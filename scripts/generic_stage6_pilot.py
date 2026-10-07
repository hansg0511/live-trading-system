"""Declarative Stage 6 pilot CLI.

Commands are intentionally asymmetric:

* ``validate`` only parses and validates the JSON configuration.
* ``dry-run`` prepares the local generic repository and prints the full
  two-intent report without contacting a broker.
* ``broker-preflight`` performs fresh account and provider market-state reads
  through the configured generic adapter, but never creates or submits an
  order.
* ``recover`` performs broker-read-only restart recovery through the existing
  ``Stage6PilotRunner``/``GenericOMS`` path.  It may write recovered local
  ledger evidence, but never submits, cancels, replaces, hedges, or flattens.
* ``sim-submit`` is the only broker-order command and requires both
  ``--arm-sim`` and the exact confirmation phrase containing the deterministic
  run correlation.  It still submits only through ``Stage6PilotRunner`` and
  ``GenericOMS``; there is no direct order path here.

REAL/LIVE and non-RTH handoffs are rejected by the configuration boundary.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
import os
from pathlib import Path
import sys
from typing import Any
import uuid

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.strategies.stat_arb.stage6_config import (  # noqa: E402
    Stage6ConfigError,
    Stage6PilotConfig,
    load_stage6_config,
)
from src.strategies.stat_arb.stage6_pilot import (  # noqa: E402
    BROKER_CLOCK_SKEW_TOLERANCE_SECONDS,
    BROKER_FACT_MAX_AGE_SECONDS,
    STAGE6_EXECUTION_COMPATIBILITY,
    Stage6RunMode,
)
from src.trading_core.repository import SQLiteTradingRepository  # noqa: E402
from src.trading_core.ports import BrokerFactSnapshot  # noqa: E402
from src.trading_core.stage6_validation import (  # noqa: E402
    Stage6SessionOutcome,
    Stage6ValidationError,
    evaluate_stage6_session,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generic Stage 6 combined-book SIM pilot")
    subparsers = parser.add_subparsers(dest="command", required=True)
    validate = subparsers.add_parser("validate", help="validate config only; no DB or broker access")
    dry_run = subparsers.add_parser("dry-run", help="prepare repository and print a no-submit report")
    broker_preflight = subparsers.add_parser(
        "broker-preflight",
        help="query fresh SIM account/RTH facts without creating or submitting orders",
    )
    baseline = subparsers.add_parser(
        "baseline",
        help="explicitly verify fresh flat SIM facts and import bounded legacy order evidence (no submit)",
    )
    sim_submit = subparsers.add_parser("sim-submit", help="explicitly arm one SIM pilot through GenericOMS")
    recover = subparsers.add_parser(
        "recover",
        help=(
            "broker-read-only restart recovery; may write recovered local ledger evidence, "
            "never submits/cancels/replaces"
        ),
    )
    compensating_exit = subparsers.add_parser(
        "compensating-exit",
        help="submit one proof-gated SIM exit for one exact filled source intent",
    )
    resolve_roundtrip = subparsers.add_parser(
        "resolve-roundtrip",
        help="persist one proof-backed entry/exit closure; never submits orders",
    )
    final_state = subparsers.add_parser(
        "final-state",
        help="obtain fresh broker/local final state without lifecycle mutation",
    )
    prepare_exit = subparsers.add_parser(
        "prepare-exit",
        help="derive normal two-book EXIT quantities from fresh broker/local exposure",
    )
    session_evidence = subparsers.add_parser(
        "session-evidence",
        help="derive validator evidence from durable repository rows and captured artifacts",
    )
    for command in (
        validate,
        dry_run,
        broker_preflight,
        baseline,
        sim_submit,
        recover,
        compensating_exit,
        resolve_roundtrip,
        final_state,
        prepare_exit,
        session_evidence,
    ):
        command.add_argument("--config", type=Path, required=True)
        command.add_argument("--json", action="store_true", dest="as_json")
    baseline.add_argument("--legacy-db", type=Path, required=True)
    baseline.add_argument("--legacy-label", default="legacy-smoke")
    baseline.add_argument(
        "--verify-flat",
        action="store_true",
        help="explicitly confirm that this command may create the audited flat baseline",
    )
    sim_submit.add_argument("--arm-sim", action="store_true", help="required explicit SIM arm")
    sim_submit.add_argument(
        "--confirm",
        help="must exactly equal: ARM STAGE6 SIM <deterministic-run-id>",
    )
    compensating_exit.add_argument("--source-intent-id", required=True)
    compensating_exit.add_argument(
        "--external-order",
        action="append",
        default=[],
        metavar="INSTRUMENT_ID=EXTERNAL_ORDER_ID",
        help="repeat for every source leg; exact durable provider identity is required",
    )
    compensating_exit.add_argument("--arm-sim", action="store_true", help="required explicit SIM arm")
    compensating_exit.add_argument(
        "--confirm",
        help="must exactly equal: ARM STAGE6 SIM COMPENSATING EXIT <source-intent-id>",
    )
    resolve_roundtrip.add_argument("--entry-intent-id", required=True)
    resolve_roundtrip.add_argument("--exit-intent-id", required=True)
    prepare_exit.add_argument("--output", type=Path, help="optional new derived artifact; existing files are never overwritten")
    session_evidence.add_argument("--session-id", required=True)
    session_evidence.add_argument("--entry-intent-id", action="append", default=[])
    session_evidence.add_argument("--exit-intent-id", action="append", default=[])
    session_evidence.add_argument("--preflight-evidence", type=Path)
    session_evidence.add_argument("--recovery-evidence", type=Path)
    session_evidence.add_argument("--final-evidence", type=Path)
    session_evidence.add_argument("--commit-sha", required=True)
    session_evidence.add_argument("--execution-compatibility", default=STAGE6_EXECUTION_COMPATIBILITY)
    session_evidence.add_argument("--trading-date")
    validation_commands = {}
    for name, help_text in (
        ("session-preflight", "record one no-submit Stage 6 preflight evidence observation"),
        ("session-recover", "record an existing fresh-process recovery observation; never submits"),
        ("session-final", "record one captured final-state Stage 6 validation observation"),
        ("session-finalize", "derive and immutably persist one Stage 6 validation result"),
        ("session-status", "show retained observations and one immutable session result"),
        ("stage-status", "show retained Stage 6 history and the three-date completion gate"),
    ):
        command = subparsers.add_parser(name, help=help_text)
        command.add_argument("--config", type=Path, required=True)
        command.add_argument("--json", action="store_true", dest="as_json")
        command.add_argument("--session-id")
        command.add_argument("--evidence", type=Path, help="JSON evidence document captured around the existing runner")
        command.add_argument("--commit-sha")
        command.add_argument("--execution-compatibility")
        command.add_argument("--trading-date", help="US trading date (YYYY-MM-DD) when absent from evidence")
        command.add_argument("--process-id")
        validation_commands[name] = command
    validation_commands["session-recover"].add_argument(
        "--fresh-process", action="store_true", help="assert that the evidence came from a separate process"
    )
    validation_commands["stage-status"].add_argument("--required-sessions", type=int, default=3)
    return parser


def _summary(config: Stage6PilotConfig) -> dict[str, Any]:
    spec = config.spec()
    return {
        "valid": True,
        "state_db": str(config.state_db),
        "account_id": config.account.id,
        "external_account_id": config.account.external_account_id,
        "environment": config.account.environment.value,
        "sleeves": [item.sleeve_id for item in config.sleeves],
        "books": [item.book_id for item in config.sleeves],
        "instrument_ids": [item.id for item in config.instruments],
        "moomoo_symbols": [item.external_symbol for item in config.mappings],
        "run_id": spec.run_id,
        "required_confirmation": config.confirmation_phrase(),
        "rth_handoff_policy": config.execution.rth_handoff_policy,
        "execution_compatibility": STAGE6_EXECUTION_COMPATIBILITY,
        "mode": "DRY_RUN",
    }


def _recovery_intent_snapshot(
    repository: SQLiteTradingRepository,
    account_id: str,
    intent_ids: list[str] | tuple[str, ...] | set[str],
) -> list[dict[str, Any]]:
    """Build an operator/evidence view from public repository facts only."""

    broker_orders = repository.book_broker_orders(account_id)
    orders_by_leg: dict[str, list[dict[str, Any]]] = {}
    for order in broker_orders:
        orders_by_leg.setdefault(str(order.get("order_leg_id")), []).append(order)

    snapshots: list[dict[str, Any]] = []
    for intent_id in sorted({str(value) for value in intent_ids if str(value).strip()}):
        intent = repository.get_intent(intent_id)
        if intent is None:
            continue
        legs: list[dict[str, Any]] = []
        for leg in intent.get("legs", ()):
            leg_id = str(leg.get("id"))
            attempts: list[dict[str, Any]] = []
            for order in orders_by_leg.get(leg_id, ()):
                order_id = str(order.get("id"))
                attempts.append(
                    {
                        "id": order_id,
                        "external_order_id": order.get("external_order_id"),
                        "status": order.get("status"),
                        "submitted_quantity": order.get("submitted_quantity"),
                        "filled_quantity": order.get("filled_quantity"),
                        "average_fill_price": order.get("average_fill_price"),
                        "submitted_at": order.get("submitted_at"),
                        "updated_at": order.get("updated_at"),
                        "fills": repository.fills_for_broker_order(order_id),
                    }
                )
            legs.append(
                {
                    "id": leg_id,
                    "instrument_id": leg.get("instrument_id"),
                    "side": leg.get("side"),
                    "requested_quantity": leg.get("quantity"),
                    "status": leg.get("status"),
                    "attempts": attempts,
                }
            )
        snapshots.append(
            {
                "intent_id": str(intent.get("id", intent_id)),
                "book_id": intent.get("book_id"),
                "action": intent.get("action"),
                "status": intent.get("status"),
                "legs": legs,
            }
        )
    return snapshots


def _recovery_order_facts(
    repository: SQLiteTradingRepository,
    account_id: str,
) -> tuple[dict[str, dict[str, Any]], dict[tuple[str, str], dict[str, Any]]]:
    """Return durable broker-order rows and fill facts keyed for delta checks."""

    orders = {
        str(row["id"]): row
        for row in repository.book_broker_orders(account_id)
        if row.get("id") is not None
    }
    fills: dict[tuple[str, str], dict[str, Any]] = {}
    for order_id in orders:
        for fill in repository.fills_for_broker_order(order_id):
            fill_key = str(fill.get("dedupe_key") or fill.get("external_fill_id") or "")
            fills[(order_id, fill_key)] = fill
    return orders, fills


def _durable_submission_process_ids(
    repository: SQLiteTradingRepository,
    intent_ids: Sequence[str],
) -> tuple[dict[str, str], list[str]]:
    """Read the immutable Stage 6 submission-process provenance on intents."""

    identities: dict[str, str] = {}
    missing: list[str] = []
    for raw_intent_id in sorted({str(value).strip() for value in intent_ids if str(value).strip()}):
        intent = repository.get_intent(raw_intent_id)
        metadata = intent.get("metadata", {}) if intent is not None else {}
        marker = metadata.get("stage6_submission") if isinstance(metadata, Mapping) else None
        process_id = marker.get("process_id") if isinstance(marker, Mapping) else None
        normalized = str(process_id).strip() if process_id is not None else ""
        if normalized:
            identities[raw_intent_id] = normalized
        else:
            missing.append(raw_intent_id)
    return identities, missing


def _recovery_report(
    *,
    config: Stage6PilotConfig,
    repository: SQLiteTradingRepository,
    started_at: datetime,
    before_intent_ids: list[str],
    before_status: list[dict[str, Any]],
    before_order_ids: set[str],
    before_external_order_ids: set[str],
    before_fill_keys: set[tuple[str, str]],
    recovered: list[dict[str, Any]],
    fresh_facts: BrokerFactSnapshot | None,
    fresh_facts_error: str | None = None,
) -> dict[str, Any]:
    """Serialize one read-only broker recovery and its durable local delta."""

    account_id = config.account.id
    after_order_rows, after_fill_rows = _recovery_order_facts(repository, account_id)
    after_intent_ids = set(before_intent_ids) | {
        str(item.get("id"))
        for item in recovered
        if isinstance(item, Mapping) and item.get("id") is not None
    }
    new_order_ids = sorted(set(after_order_rows) - before_order_ids)
    new_fill_keys = sorted(
        [
            f"{order_id}:{fill_key}"
            for order_id, fill_key in set(after_fill_rows) - before_fill_keys
        ]
    )
    attempts_by_leg: dict[str, int] = {}
    for row in after_order_rows.values():
        leg_id = str(row.get("order_leg_id"))
        attempts_by_leg[leg_id] = attempts_by_leg.get(leg_id, 0) + 1
    duplicate_legs = sorted(leg_id for leg_id, count in attempts_by_leg.items() if count > 1)
    completed_at = datetime.now(timezone.utc)
    open_issues = repository.open_reconciliation_issues(account_id)
    open_actions = repository.open_recovery_actions(account_id)
    source_submission_process_ids, missing_source_process_ids = _durable_submission_process_ids(
        repository,
        before_intent_ids,
    )
    recovery_process_id = str(os.getpid())
    source_process_values = set(source_submission_process_ids.values())
    fresh_process = bool(before_intent_ids) and not missing_source_process_ids and recovery_process_id not in source_process_values
    if not before_intent_ids:
        process_identity_reason = "no recoverable source intents were present"
    elif missing_source_process_ids:
        process_identity_reason = (
            "source intent submission process identity is missing for: "
            + ", ".join(sorted(missing_source_process_ids))
        )
    elif not fresh_process:
        process_identity_reason = "recovery process identity matches the original SIM submission process"
    else:
        process_identity_reason = "recovery process identity is distinct from every source submission process"
    after_external_order_ids = {
        str(row.get("external_order_id"))
        for row in after_order_rows.values()
        if row.get("external_order_id") not in (None, "")
    }
    after_status = _recovery_intent_snapshot(repository, account_id, after_intent_ids)
    intents_preserved = set(before_intent_ids).issubset(after_intent_ids)
    orders_preserved = before_external_order_ids.issubset(after_external_order_ids)
    no_duplicate_attempts = not duplicate_legs and not new_order_ids
    durable_exposure: dict[str, Decimal] = {}
    for allocation in repository.position_allocations(account_id):
        if str(allocation.get("ownership_class", "")).upper() != "MANAGED":
            continue
        instrument_id = str(allocation.get("instrument_id") or "").strip()
        try:
            quantity = Decimal(str(allocation.get("signed_quantity", "0")))
        except (InvalidOperation, TypeError, ValueError):
            continue
        if instrument_id and quantity.is_finite():
            durable_exposure[instrument_id] = durable_exposure.get(instrument_id, Decimal("0")) + quantity
    durable_exposure = {key: value for key, value in durable_exposure.items() if value != 0}
    broker_exposure: dict[str, Decimal] = {}
    facts_valid = bool(
        isinstance(fresh_facts, BrokerFactSnapshot)
        and fresh_facts.complete
        and not fresh_facts.error
        and fresh_facts.account_id == account_id
    )
    if facts_valid and fresh_facts is not None:
        captured_at = fresh_facts.captured_at
        if captured_at.tzinfo is None or captured_at.utcoffset() is None:
            facts_valid = False
        else:
            age_seconds = (completed_at - captured_at.astimezone(timezone.utc)).total_seconds()
            if age_seconds < -BROKER_CLOCK_SKEW_TOLERANCE_SECONDS or age_seconds > BROKER_FACT_MAX_AGE_SECONDS:
                facts_valid = False
        if fresh_facts.open_orders:
            facts_valid = False
        raw_by_instrument: dict[str, list[Decimal]] = {}
        for position in fresh_facts.positions:
            try:
                quantity = Decimal(str(position.signed_quantity))
            except (InvalidOperation, TypeError, ValueError):
                facts_valid = False
                break
            if not quantity.is_finite():
                facts_valid = False
                break
            instrument_id = str(position.instrument_id)
            raw_by_instrument.setdefault(instrument_id, []).append(quantity)
            broker_exposure[instrument_id] = broker_exposure.get(instrument_id, Decimal("0")) + quantity
        if any(
            len(values) > 1
            and any(value != 0 for value in values)
            and sum(values, Decimal("0")) == 0
            for values in raw_by_instrument.values()
        ):
            # Do not infer that contradictory/non-netted provider rows are a
            # proven flat exposure merely because their aggregate is zero.
            facts_valid = False
    broker_exposure = {key: value for key, value in broker_exposure.items() if value != 0}
    exposure_agrees = facts_valid and broker_exposure == durable_exposure
    exposure_stop_reasons: list[str] = []
    if fresh_facts_error:
        exposure_stop_reasons.append(f"fresh authoritative account facts unavailable: {fresh_facts_error}")
    if not facts_valid:
        exposure_stop_reasons.append("fresh broker facts are incomplete, stale, account-mismatched, open-order-bearing, or contradictory")
    if not exposure_agrees:
        exposure_stop_reasons.append(
            "fresh broker exposure does not exactly match durable managed exposure"
        )
    terminal_statuses = {"FILLED", "COMPLETED"}
    recovered_statuses = [str(item.get("status")) for item in after_status]
    recovery_clean = bool(before_intent_ids) and fresh_process and all(
        status in terminal_statuses for status in recovered_statuses
    ) and not open_issues and not open_actions and intents_preserved and orders_preserved and no_duplicate_attempts and exposure_agrees
    restart_recovery = {
        "performed": True,
        "broker_contacted": True,
        "fresh_process": fresh_process,
        "process_id": recovery_process_id,
        "source_intent_ids": sorted(str(value) for value in before_intent_ids),
        "source_submission_process_ids": source_submission_process_ids,
        "source_submission_process_identity_complete": bool(before_intent_ids) and not missing_source_process_ids,
        "process_identity_reason": process_identity_reason,
        "captured_at": completed_at.isoformat(),
        "result": "RECOVERED" if recovery_clean else "BLOCKED",
        "preserved_intent_ids": sorted(after_intent_ids),
        "preserved_order_ids": sorted(after_external_order_ids),
        "intents_preserved": intents_preserved,
        "orders_preserved": orders_preserved,
        "no_duplicate_attempts": no_duplicate_attempts,
        "no_resubmission": True,
        "exposure_agrees": exposure_agrees,
        "durable_managed_exposure": {key: str(value) for key, value in sorted(durable_exposure.items())},
        "broker_observed_exposure": {key: str(value) for key, value in sorted(broker_exposure.items())},
        "fresh_facts_complete": facts_valid,
        "fresh_facts_error": fresh_facts_error,
    }
    return {
        "run_id": config.spec().run_id,
        "mode": "RECOVER",
        "recovery_mode": "BROKER_READ_ONLY_LOCAL_LEDGER_WRITE_CAPABLE",
        "recovery_path": "Stage6PilotRunner->GenericOMS->MooMooGenericAdapter->OpenD",
        "account_id": account_id,
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "captured_at": completed_at.isoformat(),
        "broker_contacted": True,
        "fresh_process_recovery": fresh_process,
        "process_id": recovery_process_id,
        "source_intent_ids": sorted(str(value) for value in before_intent_ids),
        "source_submission_process_ids": source_submission_process_ids,
        "source_submission_process_identity_complete": bool(before_intent_ids) and not missing_source_process_ids,
        "recovered_intent_ids": sorted(
            {
                str(item.get("id"))
                for item in recovered
                if isinstance(item, Mapping) and item.get("id") is not None
            }
        ),
        "recovery_results": recovered,
        "before_status": before_status,
        "after_status": after_status,
        "restart_recovery": restart_recovery,
        "recovered_fill_evidence": [
            {
                "broker_order_id": order_id,
                "dedupe_key": fill_key,
                **dict(fill),
            }
            for (order_id, fill_key), fill in sorted(after_fill_rows.items())
            if (order_id, fill_key) not in before_fill_keys
        ],
        "open_reconciliation_issues": open_issues,
        "open_recovery_actions": open_actions,
        "durable_managed_exposure": {key: str(value) for key, value in sorted(durable_exposure.items())},
        "broker_observed_exposure": {key: str(value) for key, value in sorted(broker_exposure.items())},
        "fresh_facts_error": fresh_facts_error,
        "duplicate_attempt_status": {
            "detected": bool(duplicate_legs),
            "by_leg": attempts_by_leg,
            "duplicate_leg_ids": duplicate_legs,
            "new_broker_order_attempt_count": len(new_order_ids),
            "new_broker_order_attempt_ids": new_order_ids,
        },
        "broker_submission_count": 0,
        "cancel_count": 0,
        "replace_count": 0,
        "orders_submitted": 0,
        "mutations": {
            "broker_orders": len(new_order_ids),
            "fills": len(new_fill_keys),
            "order_intents": 0,
            "order_legs": 0,
            "submission_calls": 0,
            "cancel_calls": 0,
            "replace_calls": 0,
            "recovery_calls": 1,
        },
        "stop_reasons": list(dict.fromkeys(exposure_stop_reasons)),
    }


def _emit(value: Any, *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(value, sort_keys=True, indent=2, default=str))
        return
    if isinstance(value, dict) and "started_at" in value and "stop_reasons" in value:
        print(
            f"run_id={value.get('run_id')} mode={value.get('mode')} "
            f"config_preflight={'PASS' if value.get('preflight_passed') else 'STOP'} "
            f"broker_preflight={'PASS' if value.get('broker_preflight_passed') else 'NOT_RUN/STOP'}"
        )
        for plan in value.get("intent_plans", ()):
            print(
                f"intent={plan.get('intent_id')} sleeve={plan.get('sleeve_id')} "
                f"action={plan.get('action')}"
            )
        for reason in value.get("stop_reasons", ()):
            print(f"STOP: {reason}")
        if value.get("required_confirmation"):
            print(f"required_confirmation={value['required_confirmation']}")
        return
    if isinstance(value, dict) and "required_confirmation" in value and "run_id" in value:
        print(f"valid={value.get('valid', True)} run_id={value['run_id']}")
        print(f"account={value.get('account_id')} external_account={value.get('external_account_id')} environment={value.get('environment')}")
        print(f"sleeves={','.join(value.get('sleeves', []))} books={','.join(value.get('books', []))}")
        print(f"required_confirmation={value.get('required_confirmation')}")
        return
    print(json.dumps(value, sort_keys=True, indent=2, default=str))


def _read_evidence(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Stage6ConfigError(f"could not read Stage 6 evidence {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise Stage6ConfigError("Stage 6 evidence must be a JSON object")
    return value


def _read_artifact_section(path: Path | None, key: str) -> dict[str, Any] | None:
    value = _read_evidence(path)
    if value is None:
        return None
    selected = value.get(key)
    if isinstance(selected, Mapping):
        return dict(selected)
    return value


def _parse_external_order_args(values: list[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for raw in values:
        text = str(raw).strip()
        if "=" not in text:
            raise Stage6ConfigError(
                "--external-order must use INSTRUMENT_ID=EXTERNAL_ORDER_ID"
            )
        instrument_id, external_order_id = (item.strip() for item in text.split("=", 1))
        if not instrument_id or not external_order_id or instrument_id in mapping:
            raise Stage6ConfigError("--external-order identities must be non-empty and unique")
        mapping[instrument_id] = external_order_id
    return mapping


def _has_duplicate_attempts_per_leg(snapshots: Sequence[Mapping[str, Any]]) -> bool:
    """Detect repeated attempts by logical leg, not by total pair-leg count."""

    seen_leg_ids: set[str] = set()
    seen_attempt_ids: set[str] = set()
    seen_external_order_ids: set[str] = set()
    for snapshot in snapshots:
        if not isinstance(snapshot, Mapping):
            return True
        legs = snapshot.get("legs", ())
        if not isinstance(legs, (list, tuple)):
            return True
        for leg in legs:
            if not isinstance(leg, Mapping):
                return True
            leg_id = str(leg.get("id", "")).strip()
            if not leg_id or leg_id in seen_leg_ids:
                return True
            seen_leg_ids.add(leg_id)
            attempts = leg.get("attempts", ())
            if not isinstance(attempts, (list, tuple)):
                return True
            if len(attempts) > 1:
                return True
            for attempt in attempts:
                if not isinstance(attempt, Mapping):
                    return True
                attempt_id = str(attempt.get("id", "")).strip()
                if not attempt_id or attempt_id in seen_attempt_ids:
                    return True
                seen_attempt_ids.add(attempt_id)
                external_order_id = str(attempt.get("external_order_id", "")).strip()
                if external_order_id:
                    if external_order_id in seen_external_order_ids:
                        return True
                    seen_external_order_ids.add(external_order_id)
    return False


def _derive_external_order_mapping(
    repository: SQLiteTradingRepository,
    source_intent_id: str,
) -> dict[str, str]:
    intent = repository.get_intent(source_intent_id)
    if intent is None:
        raise Stage6ConfigError(f"unknown source intent: {source_intent_id}")
    mapping: dict[str, str] = {}
    for leg in intent.get("legs", ()):
        attempts = repository.broker_orders_for_leg(str(leg.get("id")))
        if len(attempts) != 1 or not attempts[0].get("external_order_id"):
            raise Stage6ConfigError(
                f"source leg {leg.get('id')} does not have exactly one durable external order identity"
            )
        instrument_id = str(leg.get("instrument_id"))
        if instrument_id in mapping:
            raise Stage6ConfigError("source intent has ambiguous instrument identities")
        mapping[instrument_id] = str(attempts[0]["external_order_id"])
    if not mapping:
        raise Stage6ConfigError("source intent has no durable broker legs")
    return mapping


def _compensating_confirmation(source_intent_id: str) -> str:
    return f"ARM STAGE6 SIM COMPENSATING EXIT {str(source_intent_id).strip()}"


def _write_new_json(path: Path | None, payload: Mapping[str, Any]) -> str | None:
    if path is None:
        return None
    if path.exists():
        raise Stage6ConfigError(f"refusing to overwrite existing artifact: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2, default=str) + "\n", encoding="utf-8")
    return str(path)


def _json_quantity(value: object, label: str) -> int | float:
    """Convert one verified exposure quantity to a JSON numeric scalar."""

    try:
        quantity = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise Stage6ConfigError(f"{label} is not a finite numeric quantity") from exc
    if not quantity.is_finite() or quantity == 0:
        raise Stage6ConfigError(f"{label} must be a non-zero finite quantity")
    if quantity == quantity.to_integral_value():
        return int(quantity)
    result = float(quantity)
    if not result or not (result == result) or result in {float("inf"), float("-inf")}:
        raise Stage6ConfigError(f"{label} cannot be represented as a JSON number")
    return result


def _derived_exit_config(
    config: Stage6PilotConfig,
    result: Mapping[str, Any],
) -> dict[str, Any]:
    """Clone the validated source config and replace targets with verified EXIT basis."""

    if config.source_path is None:
        raise Stage6ConfigError("prepare-exit requires a source config path")
    try:
        source = json.loads(config.source_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Stage6ConfigError(f"could not read source config for EXIT derivation: {exc}") from exc
    if not isinstance(source, dict) or type(source.get("targets")) is not list:
        raise Stage6ConfigError("source config targets are unavailable for EXIT derivation")
    run_id = str(result.get("run_id") or "").strip()
    captured_at = str(result.get("captured_at") or "").strip()
    quantities = result.get("derived_signed_quantities")
    if not run_id or not captured_at or not isinstance(quantities, Mapping):
        raise Stage6ConfigError("verified EXIT derivation lacks run, capture, or quantity evidence")

    derived = json.loads(json.dumps(source))
    derived["mode"] = "SIM_SUBMIT"
    token = run_id[-12:]
    for index, target in enumerate(derived["targets"]):
        if not isinstance(target, dict):
            raise Stage6ConfigError(f"targets[{index}] is malformed")
        instrument_ids = target.get("instrument_ids")
        if type(instrument_ids) is not list or len(instrument_ids) != 2:
            raise Stage6ConfigError(f"targets[{index}].instrument_ids is malformed")
        target_quantities: list[int | float] = []
        for instrument_id in instrument_ids:
            if str(instrument_id) not in quantities:
                raise Stage6ConfigError(
                    f"verified EXIT exposure lacks configured instrument {instrument_id}"
                )
            target_quantities.append(
                _json_quantity(quantities[str(instrument_id)], f"EXIT quantity {instrument_id}")
            )
        original_cycle = str(target.get("cycle_id") or "").strip()
        original_signal = str(target.get("signal_id") or "").strip()
        if not original_cycle or not original_signal:
            raise Stage6ConfigError(f"targets[{index}] lacks durable cycle/signal identity")
        target["signed_quantities"] = target_quantities
        target["action"] = "EXIT"
        target["cycle_id"] = f"{original_cycle}-exit-{token}"
        target["signal_id"] = f"{original_signal}-exit-{token}"
        target["evaluated_at"] = captured_at
        provenance = dict(target.get("provenance") or {})
        provenance.update(
            {
                "derived_from": "Stage6PilotRunner.prepare_exit",
                "source_run_id": run_id,
                "basis_captured_at": captured_at,
                "pre_negation_applied": False,
            }
        )
        target["provenance"] = provenance
    return derived


def _validation_defaults(
    config: Stage6PilotConfig,
    args: argparse.Namespace,
    evidence: dict[str, Any] | None,
) -> dict[str, Any]:
    """Fill only session identity metadata; never fill acceptance facts."""

    value = dict(evidence or {})
    run_id = config.spec().run_id
    now = datetime.now(timezone.utc).isoformat()
    value.setdefault("session_id", args.session_id or f"stage6-validation-{run_id}")
    value.setdefault("account_id", config.account.id)
    value.setdefault("environment", config.account.environment.value)
    value.setdefault("execution_path", "Stage6PilotRunner->GenericOMS")
    value.setdefault("execution_compatibility", args.execution_compatibility or STAGE6_EXECUTION_COMPATIBILITY)
    value.setdefault("commit_sha", args.commit_sha or "UNKNOWN")
    value.setdefault("us_trading_date", args.trading_date or value.get("trading_date"))
    value.setdefault("started_at", now)
    value.setdefault("completed_at", value.get("started_at", now))
    value.setdefault("captured_at", value.get("completed_at", now))
    value.setdefault("run_ids", [run_id])
    value.setdefault("supervised", True)
    return value


def _observation_payload(value: Mapping[str, Any], phase: str) -> dict[str, Any]:
    payload = dict(value)
    payload["phase"] = phase
    return payload


def _record_validation_observation(
    repository: SQLiteTradingRepository,
    *,
    config: Stage6PilotConfig,
    args: argparse.Namespace,
    phase: str,
    evidence: dict[str, Any] | None,
) -> dict[str, Any]:
    if evidence is None:
        if phase != "PREFLIGHT":
            raise Stage6ConfigError(
                f"{phase.lower()} evidence must be captured by the existing Stage6PilotRunner path; "
                "the validation command will not invent a recovery/fill observation"
            )
        dry_report = config.build_runner(repository).run(config.spec(), mode=Stage6RunMode.DRY_RUN)
        evidence = {
            "session_id": args.session_id or f"stage6-validation-{config.spec().run_id}",
            "account_id": config.account.id,
            "environment": config.account.environment.value,
            "execution_path": "Stage6PilotRunner->GenericOMS",
            "execution_mode": "DRY_RUN",
            "supervised": True,
            "commit_sha": args.commit_sha or "UNKNOWN",
            "execution_compatibility": args.execution_compatibility or "UNKNOWN",
            "us_trading_date": args.trading_date,
            "run_ids": [config.spec().run_id],
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "dry_run_report": dry_report.as_dict(),
            "preflight": {
                "source": "dry-run",
                "fresh_facts": {"complete": False, "flat": False, "open_order_count": 0},
            },
        }
    value = _validation_defaults(config, args, evidence)
    phase_evidence = value.get(
        "preflight" if phase == "PREFLIGHT" else "final" if phase == "FINAL" else "restart_recovery"
    )
    captured_at = value.get("captured_at")
    if phase_evidence is not None and isinstance(phase_evidence, Mapping):
        captured_at = phase_evidence.get("captured_at", captured_at)
    try:
        parsed = datetime.fromisoformat(str(captured_at).replace("Z", "+00:00"))
    except ValueError as exc:
        raise Stage6ConfigError("validation observation captured_at must be ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise Stage6ConfigError("validation observation captured_at must include a timezone")
    session_id = str(value["session_id"])
    observation_id = f"{session_id}:{phase.lower()}:{uuid.uuid4().hex}"
    process_id = args.process_id
    if phase == "RECOVERY":
        recovery = value.get("restart_recovery")
        if not isinstance(recovery, Mapping):
            raise Stage6ConfigError(
                "session-recover requires restart_recovery evidence emitted by recover; "
                "the recorder will not manufacture a fresh-process claim"
            )
        if recovery.get("fresh_process") is not True:
            raise Stage6ConfigError(
                "recovery artifact must prove a process identity distinct from every source submission"
            )
        artifact_process_id = str(recovery.get("process_id", "")).strip()
        if not artifact_process_id:
            raise Stage6ConfigError("recovery artifact must contain its process_id")
        top_level_process_id = str(value.get("process_id", "")).strip()
        if top_level_process_id and top_level_process_id != artifact_process_id:
            raise Stage6ConfigError("recovery process identity disagrees with the artifact envelope")
        if value.get("broker_contacted") is not True:
            raise Stage6ConfigError("recovery artifact must prove broker_contacted=true")
        source_intent_ids = recovery.get("source_intent_ids")
        if not isinstance(source_intent_ids, list) or not source_intent_ids:
            raise Stage6ConfigError("recovery artifact must identify source submission intents")
        normalized_source_intent_ids = [str(item).strip() for item in source_intent_ids]
        if any(not item for item in normalized_source_intent_ids) or len(set(normalized_source_intent_ids)) != len(normalized_source_intent_ids):
            raise Stage6ConfigError("recovery source submission intent identities are malformed")
        preserved_intent_ids = recovery.get("preserved_intent_ids")
        if not isinstance(preserved_intent_ids, list) or {
            str(item).strip() for item in preserved_intent_ids
        } != set(normalized_source_intent_ids):
            raise Stage6ConfigError(
                "recovery preserved intent identities do not exactly cover source submissions"
            )
        durable_process_ids, missing_process_ids = _durable_submission_process_ids(
            repository,
            normalized_source_intent_ids,
        )
        if missing_process_ids:
            raise Stage6ConfigError(
                "durable source submission process identity is missing for: "
                + ", ".join(sorted(missing_process_ids))
            )
        artifact_process_ids = recovery.get("source_submission_process_ids")
        if not isinstance(artifact_process_ids, Mapping):
            raise Stage6ConfigError("recovery artifact must carry source submission process identities")
        normalized_artifact_process_ids = {
            str(key).strip(): str(item).strip()
            for key, item in artifact_process_ids.items()
        }
        if normalized_artifact_process_ids != durable_process_ids:
            raise Stage6ConfigError(
                "recovery source submission process identities do not match durable intent metadata"
            )
        if artifact_process_id in set(durable_process_ids.values()):
            raise Stage6ConfigError(
                "recovery process identity matches an original SIM submission process"
            )
        if recovery.get("source_submission_process_identity_complete") is not True:
            raise Stage6ConfigError("recovery artifact must prove complete source process identity coverage")
        if args.fresh_process and recovery.get("fresh_process") is not True:
            raise Stage6ConfigError("--fresh-process does not override recovery evidence")
        if process_id is not None and str(process_id) != artifact_process_id:
            raise Stage6ConfigError("--process-id does not match the recovery artifact")
        process_id = artifact_process_id
    repository.record_stage6_validation_observation(
        observation_id=observation_id,
        session_id=session_id,
        account_id=config.account.id,
        phase=phase,
        captured_at=parsed,
        evidence=_observation_payload(value, phase),
        process_id=process_id,
        fresh_process=bool(
            phase == "RECOVERY"
            and isinstance(value.get("restart_recovery"), Mapping)
            and value["restart_recovery"].get("fresh_process") is True
        ),
    )
    return {
        "observation_id": observation_id,
        "session_id": session_id,
        "account_id": config.account.id,
        "phase": phase,
        "captured_at": parsed.isoformat(),
        "broker_contacted": False,
        "orders_submitted": 0,
        "evidence": value,
    }


def _merge_validation_observations(
    repository: SQLiteTradingRepository,
    session_id: str,
) -> dict[str, Any]:
    observations = repository.stage6_validation_observations(session_id)
    merged: dict[str, Any] = {"session_id": session_id}
    for row in observations:
        value = row.get("evidence")
        if not isinstance(value, Mapping):
            continue
        for key in (
            "account_id",
            "environment",
            "execution_path",
            "execution_compatibility",
            "commit_sha",
            "us_trading_date",
            "started_at",
            "completed_at",
            "run_ids",
            "entry_intent_ids",
            "exit_intent_ids",
            "supervised",
            "manual_intervention",
            "manual_flags",
            "audit_refs",
        ):
            if key in value:
                merged[key] = value[key]
        phase = str(row.get("phase", "")).upper()
        if phase == "PREFLIGHT" and isinstance(value.get("preflight"), Mapping):
            merged["preflight"] = value["preflight"]
        elif phase == "RECOVERY" and isinstance(value.get("restart_recovery"), Mapping):
            merged["restart_recovery"] = value["restart_recovery"]
        elif phase == "FINAL" and isinstance(value.get("final"), Mapping):
            merged["final"] = value["final"]
        for key in ("entry", "exit"):
            if isinstance(value.get(key), Mapping):
                merged[key] = value[key]
    return merged


def _run_validation_command(args: argparse.Namespace, config: Stage6PilotConfig) -> tuple[int, Any]:
    repository = SQLiteTradingRepository(config.state_db)
    config.ensure_repository(repository)
    if args.command in {"session-preflight", "session-recover", "session-final"}:
        phase = {
            "session-preflight": "PREFLIGHT",
            "session-recover": "RECOVERY",
            "session-final": "FINAL",
        }[args.command]
        return 0, _record_validation_observation(
            repository,
            config=config,
            args=args,
            phase=phase,
            evidence=_read_evidence(args.evidence),
        )
    if args.command == "session-status":
        session_id = args.session_id
        if not session_id:
            raise Stage6ConfigError("session-status requires --session-id")
        return 0, {
            "session": repository.get_stage6_validation_session(session_id),
            "observations": repository.stage6_validation_observations(session_id),
            "broker_contacted": False,
            "orders_submitted": 0,
        }
    if args.command == "session-finalize":
        evidence = _read_evidence(args.evidence)
        if evidence is None:
            session_id = args.session_id
            if not session_id:
                raise Stage6ConfigError("session-finalize requires --session-id or --evidence")
            evidence = _merge_validation_observations(repository, session_id)
        evidence = _validation_defaults(config, args, evidence)
        try:
            result = evaluate_stage6_session(evidence)
        except Stage6ValidationError as exc:
            raise Stage6ConfigError(str(exc)) from exc
        repository.save_stage6_validation_session(result)
        payload = result.as_dict()
        payload["broker_contacted"] = False
        payload["orders_submitted"] = 0
        return (0 if result.outcome is Stage6SessionOutcome.CLEAN_PASS else 2), payload
    if args.command == "stage-status":
        if args.required_sessions <= 0:
            raise Stage6ConfigError("--required-sessions must be positive")
        return 0, repository.stage6_validation_status(
            config.account.id,
            execution_compatibility=args.execution_compatibility,
            required_sessions=args.required_sessions,
        )
    raise Stage6ConfigError(f"unsupported Stage 6 validation command: {args.command}")


def _run(args: argparse.Namespace) -> tuple[int, Any]:
    config = load_stage6_config(args.config)
    if args.command == "validate":
        return 0, _summary(config)

    if args.command in {
        "session-preflight",
        "session-recover",
        "session-final",
        "session-status",
        "session-finalize",
        "stage-status",
    }:
        return _run_validation_command(args, config)

    if args.command == "session-evidence":
        repository = SQLiteTradingRepository(config.state_db)
        try:
            config.validate_repository(repository)
        except Exception as exc:
            raise Stage6ConfigError(f"existing repository validation failed: {exc}") from exc
        if not args.entry_intent_id or not args.exit_intent_id:
            raise Stage6ConfigError(
                "session-evidence requires at least one --entry-intent-id and --exit-intent-id"
            )
        preflight = _read_artifact_section(args.preflight_evidence, "preflight")
        recovery = _read_artifact_section(args.recovery_evidence, "restart_recovery")
        final = _read_artifact_section(args.final_evidence, "final")
        evidence = config.build_runner(repository).build_session_evidence(
            config.spec(),
            session_id=args.session_id,
            entry_intent_ids=args.entry_intent_id,
            exit_intent_ids=args.exit_intent_id,
            preflight=preflight,
            recovery=recovery,
            final=final,
            commit_sha=args.commit_sha,
            trading_date=args.trading_date,
            execution_compatibility=args.execution_compatibility,
        )
        return (0 if not evidence.get("evidence_builder_missing") else 2), evidence

    if args.command in {"final-state", "prepare-exit", "resolve-roundtrip", "compensating-exit"}:
        if str(config.account.environment.value).upper() != "SIM":
            raise Stage6ConfigError(f"{args.command} accepts SIM accounts only")
        repository = SQLiteTradingRepository(config.state_db)
        try:
            config.validate_repository(repository)
        except Exception as exc:
            raise Stage6ConfigError(f"existing repository validation failed: {exc}") from exc

        if args.command == "compensating-exit":
            expected_confirmation = _compensating_confirmation(args.source_intent_id)
            if not args.arm_sim:
                raise Stage6ConfigError("compensating-exit requires --arm-sim")
            if args.confirm != expected_confirmation:
                raise Stage6ConfigError(
                    f"exact SIM confirmation required: {expected_confirmation!r}; no broker connection was attempted"
                )
            mapping = _parse_external_order_args(args.external_order)
            if not mapping:
                mapping = _derive_external_order_mapping(repository, args.source_intent_id)
            adapter = config.build_moomoo_adapter()
            connected = False
            try:
                adapter.connect()
                connected = True
                runner = config.build_runner(repository, adapter=adapter)
                market_symbols = tuple(mapping.external_symbol for mapping in config.mappings)
                rth_gate = runner.compensating_exit_preflight(
                    config.spec(),
                    market_symbols=market_symbols,
                )
                if not rth_gate.get("preflight_passed"):
                    rth_gate.update(
                        {
                            "source_intent_id": str(args.source_intent_id),
                            "stop_reasons": list(rth_gate.get("stop_reasons", ())),
                            "compensating_exit_intent_id": None,
                            "orders_submitted": 0,
                        }
                    )
                    return 2, rth_gate
                pre_existing_intents = {
                    str(row.get("id"))
                    for row in repository.book_intents(config.account.id)
                    if row.get("id") is not None
                }
                result = runner.submit_verified_compensating_exit(
                    account=config.account,
                    source_intent_id=args.source_intent_id,
                    expected_external_order_ids=mapping,
                )
                raw_result = result.get("compensating_result", {})
                exit_intent_id = str(raw_result.get("intent_id") or raw_result.get("id") or "")
                if not exit_intent_id:
                    candidates = []
                    for row in repository.book_intents(config.account.id):
                        candidate = repository.get_intent(str(row.get("id")))
                        metadata = candidate.get("metadata", {}) if candidate else {}
                        if (
                            isinstance(metadata, Mapping)
                            and metadata.get("source_intent_id") == args.source_intent_id
                            and metadata.get("verified_compensating_exit") is True
                        ):
                            candidates.append(candidate)
                    if len(candidates) == 1:
                        exit_intent_id = str(candidates[0]["id"])
                if not exit_intent_id:
                    raise Stage6ConfigError("compensating exit did not return a durable intent identity")
                if exit_intent_id not in pre_existing_intents:
                    runner._record_submission_process_identity(
                        intent_id=exit_intent_id,
                        account_id=config.account.id,
                        run_id=config.spec().run_id,
                        mode="COMPENSATING_EXIT",
                        source_intent_id=args.source_intent_id,
                    )
                snapshots = _recovery_intent_snapshot(repository, config.account.id, [exit_intent_id])
                attempts = [
                    attempt
                    for snapshot in snapshots
                    for leg in snapshot.get("legs", [])
                    for attempt in leg.get("attempts", [])
                ]
                result.update(
                    {
                        "compensating_exit_intent_id": exit_intent_id,
                        "compensating_leg_ids": [
                            leg.get("id")
                            for snapshot in snapshots
                            for leg in snapshot.get("legs", [])
                        ],
                        "broker_order_attempt_ids": [attempt.get("id") for attempt in attempts],
                        "external_broker_order_ids": [attempt.get("external_order_id") for attempt in attempts],
                        "statuses": [attempt.get("status") for attempt in attempts],
                        "fill_state": [
                            {
                                "external_order_id": attempt.get("external_order_id"),
                                "filled_quantity": attempt.get("filled_quantity"),
                                "average_fill_price": attempt.get("average_fill_price"),
                            }
                            for attempt in attempts
                        ],
                        "stop_reasons": [],
                        "duplicate_attempt": _has_duplicate_attempts_per_leg(snapshots),
                        "broker_submission_count": len(attempts),
                        "rth_preflight": rth_gate,
                    }
                )
                return 0, result
            finally:
                if connected:
                    adapter.disconnect()

        adapter = config.build_moomoo_adapter()
        connected = False
        try:
            adapter.connect()
            connected = True
            runner = config.build_runner(repository, adapter=adapter)
            if args.command == "final-state":
                result = runner.final_state(
                    config.spec(),
                    market_symbols=tuple(mapping.external_symbol for mapping in config.mappings),
                )
                return (0 if result.get("final_state_passed") else 2), result
            if args.command == "prepare-exit":
                result = runner.prepare_exit(
                    config.spec(),
                    market_symbols=tuple(mapping.external_symbol for mapping in config.mappings),
                )
                if result.get("prepare_exit_passed") and args.output is not None:
                    artifact = _derived_exit_config(config, result)
                    result["artifact_path"] = _write_new_json(args.output, artifact)
                    result["derived_config"] = {
                        "mode": artifact.get("mode"),
                        "action": "EXIT",
                        "run_id": result.get("run_id"),
                        "pre_negation_applied": False,
                    }
                else:
                    result["artifact_path"] = None
                return (0 if result.get("prepare_exit_passed") else 2), result
            result = runner.resolve_verified_roundtrip(
                account=config.account,
                entry_intent_id=args.entry_intent_id,
                exit_intent_id=args.exit_intent_id,
            )
            return 0, result
        finally:
            if connected:
                adapter.disconnect()

    if args.command == "recover":
        if str(config.account.environment.value).upper() != "SIM":
            raise Stage6ConfigError("recover accepts SIM accounts only")
        repository = SQLiteTradingRepository(config.state_db)
        config.ensure_repository(repository)
        before_intent_ids = repository.recoverable_intent_ids(config.account.id)
        before_status = _recovery_intent_snapshot(repository, config.account.id, before_intent_ids)
        before_order_rows, before_fill_rows = _recovery_order_facts(repository, config.account.id)
        before_external_order_ids = {
            str(row.get("external_order_id"))
            for row in before_order_rows.values()
            if row.get("external_order_id") not in (None, "")
        }
        started_at = datetime.now(timezone.utc)
        adapter = config.build_moomoo_adapter()
        connected = False
        try:
            adapter.connect()
            connected = True
            recovered = config.build_runner(repository, adapter=adapter).recover(config.account)
            facts_getter = getattr(adapter, "get_authoritative_account_facts", None)
            fresh_facts_error = None
            if callable(facts_getter):
                try:
                    fresh_facts = facts_getter(config.account)
                except Exception as exc:
                    fresh_facts = None
                    fresh_facts_error = str(exc)
            else:
                fresh_facts = None
                fresh_facts_error = "adapter lacks authoritative fresh account-facts capability"
        finally:
            if connected:
                adapter.disconnect()
        payload = _recovery_report(
            config=config,
            repository=repository,
            started_at=started_at,
            before_intent_ids=before_intent_ids,
            before_status=before_status,
            before_order_ids=set(before_order_rows),
            before_external_order_ids=before_external_order_ids,
            before_fill_keys=set(before_fill_rows),
            recovered=recovered,
            fresh_facts=fresh_facts,
            fresh_facts_error=fresh_facts_error,
        )
        return 0, payload

    if args.command == "sim-submit":
        if not args.arm_sim:
            raise Stage6ConfigError("sim-submit requires --arm-sim")
        expected = config.confirmation_phrase()
        if args.confirm != expected:
            raise Stage6ConfigError(
                f"exact SIM confirmation required: {expected!r}; no broker connection was attempted"
            )
    if args.command == "baseline" and not args.verify_flat:
        raise Stage6ConfigError(
            "baseline requires --verify-flat; it performs a fresh read-only SIM account-facts check"
        )

    repository = SQLiteTradingRepository(config.state_db)
    if args.command == "broker-preflight":
        # This command is structurally read-only: unlike dry-run and the
        # execution command, it must not bootstrap or persist config rows.
        # ``Stage6PilotRunner.broker_preflight`` validates the already-existing
        # canonical repository objects before querying the adapter.
        spec = config.spec()
        try:
            config.validate_repository(repository)
        except Exception as exc:
            raise Stage6ConfigError(f"existing repository validation failed: {exc}") from exc
        adapter = config.build_moomoo_adapter()
        connected = True
        try:
            adapter.connect()
            mapping_details = [
                {
                    "mapping_id": mapping.id,
                    "instrument_id": mapping.instrument_id,
                    "provider": mapping.provider,
                    "purpose": mapping.purpose.value,
                    "external_symbol": mapping.external_symbol,
                    "external_id": mapping.external_id,
                }
                for mapping in config.mappings
            ]
            result = config.build_runner(repository, adapter=adapter).broker_preflight(
                spec,
                market_symbols=tuple(mapping.external_symbol for mapping in config.mappings),
                mapping_details=mapping_details,
            )
            result["config"] = _summary(config)
            return (0 if result.get("preflight_passed") else 2), result
        finally:
            if connected:
                adapter.disconnect()

    config.ensure_repository(repository)
    if args.command == "dry-run":
        report = config.build_runner(repository).run(config.spec(), mode=Stage6RunMode.DRY_RUN)
        payload = report.as_dict()
        payload["required_confirmation"] = config.confirmation_phrase()
        payload["config"] = _summary(config)
        return (0 if report.preflight_passed and not report.stop_reasons else 2), payload

    adapter = config.build_moomoo_adapter()
    connected = False
    try:
        adapter.connect()
        connected = True
        if args.command == "baseline":
            result = config.build_runner(repository, adapter=adapter).establish_verified_legacy_baseline(
                config.spec(),
                str(args.legacy_db),
                legacy_label=args.legacy_label,
            )
            result["config"] = _summary(config)
            return 0, result
        report = config.build_runner(repository, adapter=adapter).run(
            config.spec(),
            mode=Stage6RunMode.SIM_SUBMIT,
            market_symbols=tuple(mapping.external_symbol for mapping in config.mappings),
        )
    finally:
        if connected:
            adapter.disconnect()
    payload = report.as_dict()
    payload["config"] = _summary(config)
    return (0 if report.preflight_passed and not report.stop_reasons else 2), payload


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        code, payload = _run(args)
        _emit(payload, as_json=args.as_json)
        return code
    except (Stage6ConfigError, ValueError, OSError, PermissionError, RuntimeError) as exc:
        payload = {"valid": False, "status": "BLOCKED", "error": str(exc)}
        _emit(payload, as_json=args.as_json)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
