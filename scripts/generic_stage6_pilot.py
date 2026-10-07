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
from collections.abc import Mapping
from datetime import datetime, timezone
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
from src.strategies.stat_arb.stage6_pilot import Stage6RunMode  # noqa: E402
from src.trading_core.repository import SQLiteTradingRepository  # noqa: E402
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
    for command in (validate, dry_run, broker_preflight, baseline, sim_submit, recover):
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
    validation_commands = {}
    for name, help_text in (
        ("session-preflight", "record one no-submit Stage 6 preflight evidence observation"),
        ("session-recover", "record an existing fresh-process recovery observation; never submits"),
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
    after_external_order_ids = {
        str(row.get("external_order_id"))
        for row in after_order_rows.values()
        if row.get("external_order_id") not in (None, "")
    }
    after_status = _recovery_intent_snapshot(repository, account_id, after_intent_ids)
    intents_preserved = set(before_intent_ids).issubset(after_intent_ids)
    orders_preserved = before_external_order_ids.issubset(after_external_order_ids)
    no_duplicate_attempts = not duplicate_legs and not new_order_ids
    terminal_statuses = {"FILLED", "COMPLETED"}
    recovered_statuses = [str(item.get("status")) for item in after_status]
    recovery_clean = bool(before_intent_ids) and all(
        status in terminal_statuses for status in recovered_statuses
    ) and not open_issues and not open_actions and intents_preserved and orders_preserved and no_duplicate_attempts
    restart_recovery = {
        "performed": True,
        "fresh_process": True,
        "process_id": str(os.getpid()),
        "captured_at": completed_at.isoformat(),
        "result": "RECOVERED" if recovery_clean else "BLOCKED",
        "preserved_intent_ids": sorted(after_intent_ids),
        "preserved_order_ids": sorted(after_external_order_ids),
        "intents_preserved": intents_preserved,
        "orders_preserved": orders_preserved,
        "no_duplicate_attempts": no_duplicate_attempts,
        "exposure_agrees": recovery_clean,
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
        "fresh_process_recovery": True,
        "process_id": os.getpid(),
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
        "stop_reasons": [],
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
    value.setdefault("execution_compatibility", args.execution_compatibility or "UNKNOWN")
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
    phase_evidence = value.get("preflight" if phase == "PREFLIGHT" else "restart_recovery")
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
    if phase == "RECOVERY" and args.fresh_process:
        value["restart_recovery"] = {
            **dict(value.get("restart_recovery") or {}),
            "fresh_process": True,
            "process_id": process_id or value.get("process_id") or "operator-provided",
        }
        process_id = str(value["restart_recovery"]["process_id"])
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
    if args.command == "session-preflight":
        return 0, _record_validation_observation(
            repository,
            config=config,
            args=args,
            phase="PREFLIGHT",
            evidence=_read_evidence(args.evidence),
        )
    if args.command == "session-recover":
        return 0, _record_validation_observation(
            repository,
            config=config,
            args=args,
            phase="RECOVERY",
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
        "session-status",
        "session-finalize",
        "stage-status",
    }:
        return _run_validation_command(args, config)

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
