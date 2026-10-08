"""Stage 6 combined-book SIM-pilot preparation.

This module is deliberately an orchestration boundary, not a strategy or
broker implementation.  It accepts two explicitly configured Stage 5
``PairSleeve`` objects and normalized targets, applies their already validated
book allocation, and either produces a dry-run report or dispatches through
the existing broker-neutral ``GenericOMS`` in SIM only.

No symbols, quantities, mappings, prices, or signals are invented here.  A
real pilot must provide those values in the configuration and the adapter
must resolve the instrument IDs before any broker command is possible.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time
from typing import Any, Callable
from zoneinfo import ZoneInfo

from src.trading_core.domain import (
    Account,
    BrokerOrderStatus,
    ExecutionEvidenceBaseline,
    ExecutionEvidenceMode,
    ExecutionPolicy,
    ExecutionSession,
    IntentAction,
    IntentStatus,
    LegStatus,
    OwnershipClass,
    PositionSnapshot,
    RiskDecisionRecord,
    Side,
    TradingEnvironment,
)
from src.trading_core.oms import GenericOMS
from src.trading_core.ports import BrokerFactSnapshot, BrokerFill, BrokerHistoricalOrderFacts
from src.trading_core.repository import SQLiteTradingRepository

from .stage5_sleeves import (
    Clean40AllocationUpdate,
    NormalizedPairTarget,
    PairSleeve,
    Stage5AllocationCoordinator,
)


class Stage6RunMode(str, Enum):
    """The only execution modes exposed by the pilot.

    There is intentionally no LIVE mode.  A caller must opt into
    ``SIM_SUBMIT`` explicitly; all other calls are dry-run preparation.
    """

    DRY_RUN = "DRY_RUN"
    SIM_SUBMIT = "SIM_SUBMIT"


# Same-invocation market/session validation is part of the order-capable
# boundary.  Keep the identity explicit so validation rows cannot silently be
# mixed with the older execution semantics.
# V4 adds proof-gated partial cancellation/zero-fill recovery and generalized
# ENTER/EXIT compensation.  It remains broker-neutral, but its durable
# provenance and lifecycle contract is materially different from V3.
STAGE6_EXECUTION_COMPATIBILITY = "stage6-execution-v4"
BROKER_FACT_MAX_AGE_SECONDS = 60
BROKER_CLOCK_SKEW_TOLERANCE_SECONDS = 5
_US_EASTERN = ZoneInfo("America/New_York")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _required_id(value: object, name: str) -> str:
    result = str(value).strip() if value is not None else ""
    if not result:
        raise ValueError(f"{name} is required")
    return result


def _stable_value(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, Mapping):
        return {str(key): _stable_value(item) for key, item in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (tuple, list)):
        return [_stable_value(item) for item in value]
    if isinstance(value, Enum):
        return value.value
    return value


def _stable_run_id(
    account: Account,
    sleeves: Sequence[PairSleeve],
    allocation: Clean40AllocationUpdate,
    targets: Sequence[NormalizedPairTarget],
    execution_policy: ExecutionPolicy,
) -> str:
    """Create a deterministic correlation for one signal/allocation batch."""

    payload = {
        "account_id": account.id,
        "sleeves": [
            {
                "sleeve_id": sleeve.sleeve_id,
                "strategy_id": sleeve.strategy_id,
                "book_id": sleeve.book_id,
                "version": sleeve.version,
                "instrument_ids": sleeve.instrument_ids,
                "symbols": sleeve.symbols,
            }
            for sleeve in sorted(sleeves, key=lambda item: item.sleeve_id)
        ],
        "allocation": {
            "version": allocation.version,
            "effective_at": allocation.effective_at,
            "provenance": allocation.provenance,
            "targets": [
                {
                    "sleeve_id": target.sleeve_id,
                    "book_id": target.book_id,
                    "target_weight": target.target_weight,
                    "capacity": target.capacity,
                    "capacity_unit": target.capacity_unit,
                }
                for target in sorted(allocation.targets, key=lambda item: item.sleeve_id)
            ],
        },
        "targets": [
            {
                "sleeve_id": target.sleeve_id,
                "cycle_id": target.cycle_id,
                "signal_id": target.signal_id,
                "instrument_ids": target.instrument_ids,
                "signed_quantities": target.signed_quantities,
                "action": target.action,
                "evaluated_at": target.evaluated_at,
                "provenance": target.provenance,
            }
            for target in sorted(targets, key=lambda item: item.sleeve_id)
        ],
        "execution_policy": {
            "legging_policy": execution_policy.legging_policy,
            "partial_fill_policy": execution_policy.partial_fill_policy,
            "failure_policy": execution_policy.failure_policy,
            "max_attempts": execution_policy.max_attempts,
            "timeout_seconds": execution_policy.timeout_seconds,
            "require_native_atomicity": execution_policy.require_native_atomicity,
            "allow_extended_hours": execution_policy.allow_extended_hours,
            "required_capabilities": sorted(execution_policy.required_capabilities),
            "execution_session": execution_policy.execution_session,
            "stale_order_seconds": execution_policy.stale_order_seconds,
        },
    }
    encoded = json.dumps(_stable_value(payload), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "stage6-run-" + hashlib.sha256(encoded).hexdigest()[:24]


@dataclass(frozen=True, slots=True)
class Stage6PilotSpec:
    """Explicit configuration for one two-sleeve combined-book pilot batch."""

    account: Account
    sleeves: tuple[PairSleeve, PairSleeve]
    allocation_update: Clean40AllocationUpdate
    targets: tuple[NormalizedPairTarget, NormalizedPairTarget]
    require_flat_entry: bool = True
    run_id: str | None = None
    execution_policy: ExecutionPolicy = field(default_factory=ExecutionPolicy)

    def __post_init__(self) -> None:
        if not isinstance(self.account, Account):
            raise ValueError("account must be an Account")
        if self.account.environment is not TradingEnvironment.SIM:
            raise ValueError("Stage 6 pilot configuration only accepts SIM accounts")
        if not self.account.enabled:
            raise ValueError("Stage 6 pilot account must be enabled")
        sleeves = tuple(self.sleeves)
        targets = tuple(self.targets)
        if len(sleeves) != 2:
            raise ValueError("Stage 6 pilot requires exactly two configured sleeves")
        if any(not isinstance(item, PairSleeve) for item in sleeves):
            raise ValueError("sleeves must contain PairSleeve values")
        if len({item.sleeve_id for item in sleeves}) != 2:
            raise ValueError("Stage 6 pilot sleeves must have distinct identities")
        if len({item.book_id for item in sleeves}) != 2:
            raise ValueError("Stage 6 pilot sleeves must have distinct book identities")
        if any(item.account_id != self.account.id for item in sleeves):
            raise ValueError("every pilot sleeve must use the configured account")
        if len(targets) != 2 or any(not isinstance(item, NormalizedPairTarget) for item in targets):
            raise ValueError("Stage 6 pilot requires exactly one normalized target per sleeve")
        target_by_sleeve = {item.sleeve_id: item for item in targets}
        if set(target_by_sleeve) != {item.sleeve_id for item in sleeves}:
            raise ValueError("pilot targets must cover both configured sleeves exactly once")
        actions = {item.action for item in targets}
        if len(actions) != 1:
            raise ValueError("one pilot batch must use one action across both sleeves")
        for sleeve in sleeves:
            target = target_by_sleeve[sleeve.sleeve_id]
            if target.instrument_ids != sleeve.instrument_ids:
                raise ValueError(f"target instruments do not match sleeve {sleeve.sleeve_id}")
        if not isinstance(self.allocation_update, Clean40AllocationUpdate):
            raise ValueError("allocation_update must be a Clean40AllocationUpdate")
        if self.allocation_update.account_id != self.account.id:
            raise ValueError("allocation update account does not match pilot account")
        allocation_by_sleeve = {item.sleeve_id: item for item in self.allocation_update.targets}
        if set(allocation_by_sleeve) != {item.sleeve_id for item in sleeves}:
            raise ValueError("allocation update must cover both pilot sleeves exactly once")
        for sleeve in sleeves:
            if allocation_by_sleeve[sleeve.sleeve_id].book_id != sleeve.book_id:
                raise ValueError(f"allocation book does not match sleeve {sleeve.sleeve_id}")
        if not isinstance(self.require_flat_entry, bool):
            raise ValueError("require_flat_entry must be a bool")
        if not isinstance(self.execution_policy, ExecutionPolicy):
            raise ValueError("execution_policy must be an ExecutionPolicy")
        if (
            self.execution_policy.execution_session is not ExecutionSession.REGULAR
            or self.execution_policy.allow_extended_hours
        ):
            raise ValueError("Stage 6 pilot requires SIM regular-session execution with no extended hours")
        run_id = self.run_id or _stable_run_id(
            self.account,
            sleeves,
            self.allocation_update,
            targets,
            self.execution_policy,
        )
        object.__setattr__(self, "sleeves", sleeves)
        object.__setattr__(self, "targets", targets)
        object.__setattr__(self, "run_id", _required_id(run_id, "run_id"))

    @property
    def action(self) -> IntentAction:
        return self.targets[0].action

    @property
    def book_ids(self) -> tuple[str, str]:
        return tuple(item.book_id for item in self.sleeves)  # type: ignore[return-value]

    def validate_repository(self, repository: SQLiteTradingRepository) -> None:
        """Validate persisted ownership before applying any allocation update."""

        persisted = repository.get_account(self.account.id)
        if persisted is None:
            raise ValueError(f"pilot account is not persisted: {self.account.id}")
        identity = (
            persisted.broker != self.account.broker,
            persisted.environment is not self.account.environment,
            persisted.external_account_id != self.account.external_account_id,
            persisted.enabled != self.account.enabled,
        )
        if any(identity):
            raise ValueError("pilot account does not match the persisted canonical SIM account")
        for sleeve in self.sleeves:
            sleeve.validate_persistence(repository)
            if repository.get_book(sleeve.book_id) is None:
                raise ValueError(f"pilot book is not persisted: {sleeve.book_id}")


@dataclass(frozen=True, slots=True)
class Stage6PilotReport:
    """Operator-readable result; dry-run reports are the audit artifact."""

    run_id: str
    mode: Stage6RunMode
    started_at: datetime
    completed_at: datetime
    allocation_ids: tuple[str, ...] = ()
    preflight_passed: bool = False
    broker_preflight_passed: bool = False
    stop_reasons: tuple[str, ...] = ()
    intent_plans: tuple[Mapping[str, Any], ...] = ()
    intent_results: tuple[Mapping[str, Any], ...] = ()
    before_status: tuple[Mapping[str, Any], ...] = ()
    after_status: tuple[Mapping[str, Any], ...] = ()
    broker_facts: Mapping[str, Any] = field(default_factory=dict)
    market_state: Mapping[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "mode": self.mode.value,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat(),
            "allocation_ids": list(self.allocation_ids),
            "preflight_passed": self.preflight_passed,
            "broker_preflight_passed": self.broker_preflight_passed,
            "stop_reasons": list(self.stop_reasons),
            "intent_plans": [_stable_value(item) for item in self.intent_plans],
            "intent_results": [_stable_value(item) for item in self.intent_results],
            "before_status": [_stable_value(item) for item in self.before_status],
            "after_status": [_stable_value(item) for item in self.after_status],
            "broker_facts": _stable_value(self.broker_facts),
            "market_state": _stable_value(self.market_state),
        }

    def operator_text(self) -> str:
        """Render a compact report suitable for an operator log."""

        lines = [
            f"Stage 6 pilot {self.run_id} [{self.mode.value}]",
            f"config_preflight={'PASS' if self.preflight_passed else 'STOP'}",
            (
                "broker_preflight=PASS"
                if self.broker_preflight_passed
                else "broker_preflight=NOT_RUN (dry-run)"
                if self.mode is Stage6RunMode.DRY_RUN and self.preflight_passed
                else "broker_preflight=STOP"
            ),
            f"allocations={','.join(self.allocation_ids) or '-'}",
        ]
        for plan in self.intent_plans:
            lines.append(
                "intent "
                f"{plan.get('intent_id')} sleeve={plan.get('sleeve_id')} "
                f"action={plan.get('action')} status={plan.get('status', 'PLANNED')}"
            )
        for reason in self.stop_reasons:
            lines.append(f"STOP: {reason}")
        return "\n".join(lines)


class Stage6PilotRunner:
    """Prepare or explicitly dispatch one combined two-sleeve SIM batch."""

    _DISPATCHABLE_EXISTING = {
        IntentStatus.CREATED.value,
        IntentStatus.RISK_APPROVED.value,
        IntentStatus.SUBMITTING.value,
    }
    _SUCCESSFUL_SUBMIT = {
        IntentStatus.WORKING.value,
        IntentStatus.PARTIALLY_FILLED.value,
        IntentStatus.FILLED.value,
        IntentStatus.COMPLETED.value,
    }

    def __init__(
        self,
        repository: SQLiteTradingRepository,
        oms: GenericOMS,
        *,
        clock: Callable[[], datetime] | None = None,
        sleep: Callable[[float], None] | None = None,
        dispatch_wait_seconds: float = 30.0,
        dispatch_poll_seconds: float = 0.5,
    ) -> None:
        self.repository = repository
        self.oms = oms
        self.clock = clock or _utc_now
        self.sleep = sleep or time.sleep
        if dispatch_wait_seconds < 0:
            raise ValueError("dispatch_wait_seconds must be non-negative")
        if dispatch_poll_seconds <= 0:
            raise ValueError("dispatch_poll_seconds must be positive")
        self.dispatch_wait_seconds = float(dispatch_wait_seconds)
        self.dispatch_poll_seconds = float(dispatch_poll_seconds)
        self._last_submission_gate_outcome: str | None = None

    def _now(self) -> datetime:
        value = self.clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Stage 6 clock must return a timezone-aware datetime")
        return value.astimezone(timezone.utc)

    def _record_submission_process_identity(
        self,
        *,
        intent_id: str,
        account_id: str,
        run_id: str,
        mode: str = Stage6RunMode.SIM_SUBMIT.value,
        source_intent_id: str | None = None,
    ) -> None:
        """Retain immutable process provenance before broker submission.

        The intent row already exists when the Stage 6 per-leg admission hook
        calls this method, but no adapter submission has happened yet.  The
        marker is audit provenance, not part of the idempotency payload;
        old intents and resumed intents remain untagged and therefore cannot
        later claim a verified fresh-process recovery.
        """

        intent = self.repository.get_intent(intent_id)
        if intent is None or str(intent.get("account_id")) != str(account_id):
            raise ValueError("cannot record Stage 6 provenance for an unknown account intent")
        stage5 = intent.get("metadata", {}).get("stage5", {})
        if not isinstance(stage5, Mapping):
            stage5 = {}
        metadata = intent.get("metadata")
        existing_marker = metadata.get("stage6_submission") if isinstance(metadata, Mapping) else None
        if existing_marker is not None:
            if not isinstance(existing_marker, Mapping):
                raise ValueError("existing Stage 6 submission provenance is malformed")
            expected_source = str(source_intent_id).strip() if source_intent_id else ""
            existing_source = str(
                existing_marker.get("correlation", {}).get("source_intent_id", "")
                if isinstance(existing_marker.get("correlation"), Mapping)
                else ""
            ).strip()
            if (
                str(existing_marker.get("run_id", "")).strip() != str(run_id).strip()
                or str(existing_marker.get("mode", "")).strip() != str(mode).strip()
                or str(existing_marker.get("execution_compatibility", "")).strip()
                != STAGE6_EXECUTION_COMPATIBILITY
                or (expected_source and existing_source != expected_source)
            ):
                raise ValueError("immutable Stage 6 submission provenance conflicts with this dispatch")
            return
        submitted_at: datetime | None = None
        for leg in intent.get("legs", ()):
            for order in self.repository.broker_orders_for_leg(str(leg.get("id"))):
                value = order.get("submitted_at") or order.get("updated_at")
                if value:
                    try:
                        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
                    except ValueError:
                        continue
                    if parsed.tzinfo is None or parsed.utcoffset() is None:
                        continue
                    parsed = parsed.astimezone(timezone.utc)
                    submitted_at = parsed if submitted_at is None else min(submitted_at, parsed)
        if submitted_at is None:
            submitted_at = self._now()
        correlation = {
            "intent_id": str(intent_id),
            "run_id": str(run_id),
            "source_signal_id": str(intent.get("source_signal_id") or stage5.get("signal_id") or ""),
            "idempotency_key": str(intent.get("idempotency_key") or ""),
        }
        if source_intent_id:
            correlation["source_intent_id"] = str(source_intent_id)
        self.repository.append_intent_metadata(
            intent_id,
            {
                "stage6_submission": {
                    "process_id": str(os.getpid()),
                    "run_id": str(run_id),
                    "mode": str(mode),
                    "execution_compatibility": STAGE6_EXECUTION_COMPATIBILITY,
                    "submitted_at": submitted_at.isoformat(),
                    "correlation": correlation,
                }
            },
            account_id=account_id,
        )

    @staticmethod
    def _is_verified_full_fill(status: Mapping[str, Any]) -> bool:
        """Return true only after every leg has durable terminal fill evidence."""

        if str(status.get("status", "")) not in {
            IntentStatus.FILLED.value,
            IntentStatus.COMPLETED.value,
        }:
            return False
        legs = status.get("legs")
        if not isinstance(legs, Sequence) or isinstance(legs, (str, bytes)) or not legs:
            return False
        for leg in legs:
            if not isinstance(leg, Mapping) or str(leg.get("status", "")) != "FILLED":
                return False
            try:
                requested = Decimal(str(leg["quantity"]))
                filled = Decimal(str(leg.get("cumulative_filled_quantity", "0")))
            except (KeyError, InvalidOperation, TypeError, ValueError):
                return False
            if not requested.is_finite() or requested <= 0 or filled != requested:
                return False
        return True

    def _has_active_working_leg(self, status: Mapping[str, Any]) -> bool:
        """Return whether an incomplete intent still owns a live sibling."""

        for leg in status.get("legs", ()):
            if not isinstance(leg, Mapping):
                continue
            if str(leg.get("status")) not in {
                LegStatus.WORKING.value,
                LegStatus.SUBMITTING.value,
            }:
                continue
            if self.repository.broker_orders_for_leg(str(leg.get("id"))):
                return True
        return False

    def _wait_for_verified_full_fill(
        self,
        *,
        intent_id: str,
        account: Account,
    ) -> tuple[Mapping[str, Any], str]:
        """Reconcile one accepted intent before allowing the next sleeve.

        Stage 6 submits sleeves sequentially.  A provider ACK with a
        non-terminal ``WORKING`` status is not enough to expose the account to
        the next sleeve: the first intent must either become a verified full
        fill, or stop with a durable partial/rejection/unknown/timeout result.
        The clock and sleeper are injected so tests never need real waits.
        """

        deadline = self._now() + timedelta(seconds=self.dispatch_wait_seconds)
        last: Mapping[str, Any] = self.repository.get_intent(intent_id) or {
            "id": intent_id,
            "status": "UNKNOWN",
            "legs": (),
        }
        while True:
            if self._is_verified_full_fill(last):
                return last, "FULL"
            status = str(last.get("status", "UNKNOWN"))
            if status in {
                IntentStatus.PARTIALLY_FILLED.value,
                IntentStatus.REJECTED.value,
                IntentStatus.CANCELLED.value,
                IntentStatus.FAILED.value,
                IntentStatus.RECONCILIATION_REQUIRED.value,
            } and not (
                status == IntentStatus.PARTIALLY_FILLED.value
                and self._has_active_working_leg(last)
            ):
                return last, status
            now = self._now()
            if now >= deadline:
                return last, "TIMEOUT"
            try:
                last = self.oms.recover_intent(intent_id, account=account)
            except Exception as exc:
                return {
                    **dict(last),
                    "status": "RECOVERY_ERROR",
                    "error": str(exc),
                }, "RECOVERY_ERROR"
            if self._is_verified_full_fill(last):
                return last, "FULL"
            status = str(last.get("status", "UNKNOWN"))
            if status in {
                IntentStatus.PARTIALLY_FILLED.value,
                IntentStatus.REJECTED.value,
                IntentStatus.CANCELLED.value,
                IntentStatus.FAILED.value,
                IntentStatus.RECONCILIATION_REQUIRED.value,
            } and not (
                status == IntentStatus.PARTIALLY_FILLED.value
                and self._has_active_working_leg(last)
            ):
                return last, status
            remaining = max(0.0, (deadline - self._now()).total_seconds())
            if remaining <= 0:
                return last, "TIMEOUT"
            self.sleep(min(self.dispatch_poll_seconds, remaining))

    def _leg_fill_state(
        self,
        *,
        intent_id: str,
        leg_id: str,
    ) -> tuple[Mapping[str, Any], bool, str]:
        """Read one leg's durable state without inferring a broker fill.

        The per-leg Stage 6 gate needs a narrower proof than the existing
        sleeve hand-off wait.  A sibling may advance only when this exact leg
        has one broker attempt in a terminal ``FILLED`` state and the OMS can
        account for the requested quantity from durable fill rows.  Provider
        ACKs, requested quantities, and local ``FILLED`` labels without that
        attempt-scoped evidence remain blocked.
        """

        snapshot = self.repository.get_intent(intent_id)
        if snapshot is None:
            return {"id": intent_id, "legs": ()}, False, "UNKNOWN_INTENT"
        leg = next(
            (row for row in snapshot.get("legs", ()) if str(row.get("id")) == str(leg_id)),
            None,
        )
        if not isinstance(leg, Mapping):
            return snapshot, False, "MISSING_LEG"
        status = str(leg.get("status", "UNKNOWN"))
        try:
            requested = Decimal(str(leg["quantity"]))
            cumulative = Decimal(str(leg.get("cumulative_filled_quantity", "0")))
        except (KeyError, InvalidOperation, TypeError, ValueError):
            return snapshot, False, "MALFORMED_FILL_QUANTITY"
        attempts = self.repository.broker_orders_for_leg(str(leg_id))
        if (
            status == LegStatus.FILLED.value
            and requested.is_finite()
            and requested > 0
            and cumulative == requested
            and len(attempts) == 1
        ):
            try:
                verified = self.oms._attempt_has_complete_fill_evidence(attempts[0], leg)
            except (AttributeError, KeyError, InvalidOperation, TypeError, ValueError, sqlite3.Error):
                verified = False
            if verified:
                return snapshot, True, "FULL"

        if status in {
            LegStatus.REJECTED.value,
            LegStatus.CANCELLED.value,
            LegStatus.FAILED.value,
            LegStatus.RECONCILIATION_REQUIRED.value,
        }:
            return snapshot, False, status
        if not attempts and status in {
            LegStatus.PLANNED.value,
            LegStatus.SUBMITTING.value,
        }:
            return snapshot, False, "UNSUBMITTED"
        return snapshot, False, status or "UNKNOWN"

    def _wait_for_verified_leg_fill(
        self,
        *,
        intent_id: str,
        leg_id: str,
        account: Account,
    ) -> tuple[Mapping[str, Any], str]:
        """Wait for one owned sibling leg to become durably fully filled.

        This is intentionally fail-closed.  It performs only the existing
        OMS recovery/read path and never submits, cancels, or replaces an
        order.  A timeout, partial/rejected/ambiguous state, or recovery
        failure is returned to the per-leg admission gate so GenericOMS can
        persist the normal Stage 6 reconciliation stop and leave the next
        sibling unattempted.
        """

        deadline = self._now() + timedelta(seconds=self.dispatch_wait_seconds)
        last = self.repository.get_intent(intent_id) or {"id": intent_id, "legs": ()}
        while True:
            last, verified, state = self._leg_fill_state(intent_id=intent_id, leg_id=leg_id)
            if verified:
                return last, "FULL"
            if state in {
                "UNKNOWN_INTENT",
                "MISSING_LEG",
                "MALFORMED_FILL_QUANTITY",
                "UNSUBMITTED",
                LegStatus.REJECTED.value,
                LegStatus.CANCELLED.value,
                LegStatus.FAILED.value,
                LegStatus.RECONCILIATION_REQUIRED.value,
            }:
                return last, state
            now = self._now()
            if now >= deadline:
                return last, "TIMEOUT"
            try:
                last = self.oms.recover_intent(intent_id, account=account)
            except Exception as exc:
                return {
                    **dict(last),
                    "status": "RECOVERY_ERROR",
                    "error": str(exc),
                }, "RECOVERY_ERROR"
            _snapshot, verified, state = self._leg_fill_state(
                intent_id=intent_id,
                leg_id=leg_id,
            )
            if verified:
                return _snapshot, "FULL"
            if state in {
                "UNKNOWN_INTENT",
                "MISSING_LEG",
                "MALFORMED_FILL_QUANTITY",
                "UNSUBMITTED",
                LegStatus.REJECTED.value,
                LegStatus.CANCELLED.value,
                LegStatus.FAILED.value,
                LegStatus.RECONCILIATION_REQUIRED.value,
            }:
                return _snapshot, state
            remaining = max(0.0, (deadline - self._now()).total_seconds())
            if remaining <= 0:
                return _snapshot, "TIMEOUT"
            self.sleep(min(self.dispatch_poll_seconds, remaining))

    @staticmethod
    def _plan(intent: Any, sleeve: PairSleeve, run_id: str) -> dict[str, Any]:
        return {
            "intent_id": intent.id,
            "idempotency_key": intent.idempotency_key,
            "sleeve_id": sleeve.sleeve_id,
            "book_id": sleeve.book_id,
            "action": intent.action.value,
            "run_id": run_id,
            "legs": [
                {
                    "instrument_id": leg.instrument_id,
                    "side": leg.side.value,
                    "quantity": str(leg.quantity),
                    "order_type": leg.order_type,
                }
                for leg in intent.legs
            ],
        }

    def _local_blockers(self, spec: Stage6PilotSpec) -> list[str]:
        reasons: list[str] = []
        account_id = spec.account.id
        issues = self.repository.open_reconciliation_issues(account_id)
        actions = self.repository.open_recovery_actions(account_id)
        if issues:
            reasons.append(f"{len(issues)} open account reconciliation issue(s)")
        if actions:
            reasons.append(f"{len(actions)} open account recovery action(s)")

        allowed_books = set(spec.book_ids)
        baseline = self.repository.latest_execution_evidence_baseline(account_id)
        retired_book_checks: dict[str, tuple[bool, str]] = {}
        closed_historical_intents = self.oms._closed_historical_intent_ids(spec.account)
        for row in self.repository.position_allocations(account_id):
            if str(row.get("source_intent_id") or "") in closed_historical_intents:
                continue
            try:
                quantity = Decimal(str(row.get("signed_quantity", "0")))
            except (InvalidOperation, TypeError, ValueError):
                reasons.append(f"invalid persisted position allocation {row.get('id', '')}")
                continue
            if not quantity.is_finite():
                reasons.append(f"non-finite persisted position allocation {row.get('id', '')}")
                continue
            ownership = str(row.get("ownership_class", "")).upper()
            book_id = str(row.get("book_id", "")).strip()
            if book_id in allowed_books and ownership == OwnershipClass.MANAGED.value:
                if quantity == 0:
                    continue
                # Active pilot-book exposure is checked below by book_signed_exposure.
                continue
            if book_id and book_id not in allowed_books:
                if book_id not in retired_book_checks:
                    retired_book_checks[book_id] = self.repository.verified_retired_book_closure(
                        account_id,
                        book_id,
                        baseline,
                    )
                if retired_book_checks[book_id][0]:
                    continue
                reasons.append(
                    f"unknown or unrelated local position allocation {row.get('id', '')}; "
                    f"retired-book closure is not verified: {retired_book_checks[book_id][1]}"
                )
            else:
                reasons.append(
                    f"unknown or unrelated local position allocation {row.get('id', '')}"
                )

        if spec.action is IntentAction.ENTER and spec.require_flat_entry:
            for book_id in spec.book_ids:
                try:
                    exposure = self.repository.book_signed_exposure(
                        account_id,
                        book_id,
                        exclude_intent_ids=closed_historical_intents,
                    )
                except Exception as exc:  # pragma: no cover - defensive DB boundary
                    reasons.append(f"book {book_id} exposure could not be validated: {exc}")
                    continue
                if any(value != 0 for value in exposure.values()):
                    reasons.append(f"book {book_id} is not flat before entry")

        if spec.action is IntentAction.EXIT:
            # EXIT targets are the signed exposure that the strategy claims
            # is currently open.  PairSleeve.to_intent() negates that target
            # exactly once to produce the closing order sides.  Validate the
            # input against the durable book ledger here as well as against
            # the fresh broker snapshot below, so a dry-run cannot present a
            # double-negated close as a clean configuration.
            for sleeve, target in zip(spec.sleeves, spec.targets, strict=True):
                try:
                    exposure = self.repository.book_signed_exposure(
                        account_id,
                        sleeve.book_id,
                        exclude_intent_ids=closed_historical_intents,
                    )
                except Exception as exc:  # pragma: no cover - defensive DB boundary
                    reasons.append(
                        f"book {sleeve.book_id} exit exposure could not be validated: {exc}"
                    )
                    continue
                target_exposure = {
                    instrument_id: quantity
                    for instrument_id, quantity in zip(
                        target.instrument_ids,
                        target.signed_quantities,
                        strict=True,
                    )
                }
                for instrument_id in sorted(set(exposure) | set(target_exposure)):
                    observed_quantity = exposure.get(instrument_id, Decimal("0"))
                    target_quantity = target_exposure.get(instrument_id, Decimal("0"))
                    if target_quantity != observed_quantity:
                        reasons.append(
                            "EXIT target does not match the durable book exposure for "
                            f"{instrument_id} in {sleeve.book_id}: "
                            f"target {target_quantity}, durable {observed_quantity}"
                        )
        return reasons

    def _fresh_broker_snapshot(
        self,
        spec: Stage6PilotSpec,
        *,
        exit_target_books: set[str] | None = None,
        transient_intent: Any | None = None,
        allow_open_order_external_ids: set[str] | None = None,
    ) -> tuple[BrokerFactSnapshot | None, list[str]]:
        """Read and validate one fresh account-scoped fact set.

        This helper deliberately has no repository writes and no broker command
        capability.  ``run(..., SIM_SUBMIT)`` continues to use the same gate;
        the broker-preflight command uses it before constructing any intents.
        """
        try:
            facts, normalized_positions, normalized_open_orders = self.oms.strict_authoritative_account_facts(
                spec.account,
                max_age_seconds=BROKER_FACT_MAX_AGE_SECONDS,
            )
        except Exception as exc:
            return None, [f"authoritative broker facts unavailable: {exc}"]
        reasons: list[str] = []
        allowed_open_order_ids = {
            str(value).strip()
            for value in (allow_open_order_external_ids or set())
            if str(value).strip()
        }
        unexpected_open_orders = [
            item
            for item in normalized_open_orders
            if str(item.external_order_id).strip() not in allowed_open_order_ids
        ]
        if unexpected_open_orders or len(normalized_open_orders) != len(allowed_open_order_ids):
            reasons.append("unsafe outstanding broker orders are present")
        if facts.execution_evidence_mode is ExecutionEvidenceMode.UNAVAILABLE:
            reasons.append("execution evidence is unavailable; no SIM submission is safe")
        elif facts.execution_evidence_mode is ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS:
            baseline = self.repository.latest_execution_evidence_baseline(spec.account.id)
            cumulative_baseline_verified = (
                baseline is not None
                and baseline.account_id == spec.account.id
                and baseline.evidence_mode is ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS
                and baseline.verified_flat
                and baseline.status == "VERIFIED"
            )
            if not cumulative_baseline_verified:
                reasons.append(
                    "verified bounded cumulative-order baseline requires matching account, mode, VERIFIED status, and flat proof"
                )

        expected: dict[str, Decimal] = defaultdict(Decimal)
        closed_historical_intents = self.oms._closed_historical_intent_ids(spec.account)
        for row in self.repository.position_allocations(spec.account.id):
            if (
                str(row.get("ownership_class", "")).upper() == OwnershipClass.MANAGED.value
                and str(row.get("book_id", "")) in set(spec.book_ids)
                and str(row.get("source_intent_id") or "") not in closed_historical_intents
            ):
                expected[str(row["instrument_id"])] += Decimal(str(row["signed_quantity"]))
        # A sequential intent can have a verified filled sibling leg before
        # its next leg reaches the per-leg admission hook.  That exposure is
        # not a new allocation yet, but it is part of the expected fresh
        # account state for this exact in-flight intent.  Bind it to durable
        # cumulative fills; never infer it from a submitted/working status or
        # from the requested quantity alone.
        transient_local: dict[str, Decimal] = defaultdict(Decimal)
        transient_claims: dict[str, tuple[str, Decimal]] = {}
        if transient_intent is not None:
            transient_id = str(
                transient_intent.get("id", "")
                if isinstance(transient_intent, Mapping)
                else getattr(transient_intent, "id", "")
                or ""
            ).strip()
            persisted_transient = self.repository.get_intent(transient_id) if transient_id else None
            if persisted_transient is None:
                reasons.append("in-flight Stage 6 intent is not durably persisted")
            else:
                for leg in persisted_transient.get("legs", ()):
                    try:
                        cumulative = Decimal(str(leg.get("cumulative_filled_quantity", "0")))
                    except (InvalidOperation, TypeError, ValueError):
                        reasons.append(
                            f"in-flight Stage 6 leg {leg.get('id')} has malformed cumulative fill quantity"
                        )
                        continue
                    if not cumulative.is_finite() or cumulative < 0:
                        reasons.append(
                            f"in-flight Stage 6 leg {leg.get('id')} has invalid cumulative fill quantity"
                        )
                        continue
                    instrument_id = str(leg.get("instrument_id") or "").strip()
                    if not instrument_id or str(leg.get("side")) not in {Side.BUY.value, Side.SELL.value}:
                        reasons.append(
                            f"in-flight Stage 6 leg {leg.get('id')} has malformed filled identity"
                        )
                        continue
                    signed = cumulative if str(leg.get("side")) == Side.BUY.value else -cumulative
                    for attempt in self.repository.broker_orders_for_leg(str(leg.get("id"))):
                        external_id = str(attempt.get("external_order_id") or "").strip()
                        if not external_id:
                            continue
                        prior_claim = transient_claims.get(external_id)
                        claim = (
                            instrument_id,
                            Decimal("1")
                            if str(leg.get("side")) == Side.BUY.value
                            else Decimal("-1"),
                        )
                        if prior_claim is not None and prior_claim != claim:
                            reasons.append(
                                f"in-flight Stage 6 external order {external_id} has contradictory leg claims"
                            )
                        else:
                            transient_claims[external_id] = claim
                    if cumulative != 0:
                        transient_local[instrument_id] += signed
        # Provider fills may be visible before the generic OMS poll has
        # advanced the durable leg's cumulative quantity.  Reconcile that
        # exact, already-claimed external identity into the transient
        # expectation; an unclaimed provider fill is still rejected below.
        transient_provider: dict[str, Decimal] = defaultdict(Decimal)
        for fill in facts.fills:
            claim = transient_claims.get(str(fill.external_order_id).strip())
            if claim is None:
                continue
            instrument_id, sign = claim
            if fill.account_id not in (None, spec.account.id):
                reasons.append(
                    f"in-flight Stage 6 fill {fill.external_order_id} belongs to a different account"
                )
                continue
            if fill.instrument_id not in (None, instrument_id):
                reasons.append(
                    f"in-flight Stage 6 fill {fill.external_order_id} has a foreign instrument"
                )
                continue
            if not fill.quantity.is_finite() or fill.quantity <= 0:
                reasons.append(
                    f"in-flight Stage 6 fill {fill.external_order_id} has an invalid quantity"
                )
                continue
            transient_provider[instrument_id] += sign * fill.quantity
        for instrument_id, provider_quantity in transient_provider.items():
            local_quantity = transient_local.get(instrument_id, Decimal("0"))
            if local_quantity and local_quantity != provider_quantity:
                reasons.append(
                    f"in-flight Stage 6 durable/provider fill mismatch for {instrument_id}: "
                    f"durable {local_quantity}, provider {provider_quantity}"
                )
            expected[instrument_id] += provider_quantity
        for instrument_id, local_quantity in transient_local.items():
            if instrument_id not in transient_provider:
                expected[instrument_id] += local_quantity
        observed, position_reasons = self._strict_position_map(
            normalized_positions,
            account_id=spec.account.id,
        )
        reasons.extend(position_reasons)
        for instrument_id in set(expected) | set(observed):
            if expected[instrument_id] != observed.get(instrument_id, Decimal("0")):
                reasons.append(
                    "broker position does not match the durable managed allocation for "
                    f"{instrument_id}: expected {expected[instrument_id]}, observed {observed.get(instrument_id, Decimal('0'))}"
                )

        # EXIT targets are expressed as the signed exposure being closed;
        # ``PairSleeve.to_intent`` reverses that exposure into the closing
        # order sides.  Bind the target to the same fresh broker fact used by
        # the account gate so a caller cannot accidentally pass an already
        # negated delta and open a second position.  Exact equality is
        # intentional: a close may only dispatch the verified quantity that
        # is currently present for that configured instrument.
        if spec.action is IntentAction.EXIT:
            for sleeve, target in zip(spec.sleeves, spec.targets, strict=True):
                if exit_target_books is not None and sleeve.book_id not in exit_target_books:
                    continue
                for instrument_id, target_quantity in zip(
                    target.instrument_ids,
                    target.signed_quantities,
                    strict=True,
                ):
                    observed_quantity = observed.get(instrument_id, Decimal("0"))
                    if target_quantity != observed_quantity:
                        reasons.append(
                            "EXIT target does not match the fresh broker position for "
                            f"{instrument_id} in {sleeve.book_id}: "
                            f"target {target_quantity}, observed {observed_quantity}"
                        )

        for fill in facts.fills:
            if not self.repository.broker_orders_for_external_order_id(fill.external_order_id):
                reasons.append(
                    f"unattributed broker fill evidence is present for {fill.external_order_id}"
                )
        return facts, reasons

    @staticmethod
    def _strict_position_map(
        positions: Sequence[PositionSnapshot],
        *,
        account_id: str,
    ) -> tuple[dict[str, Decimal], list[str]]:
        """Normalize broker positions without silently netting contradictions.

        One instrument may have one authoritative row.  An exact repeated row
        (including provider snapshot identity) is harmless and is collapsed;
        any other duplicate is ambiguous and blocks the safety boundary.
        """

        grouped: dict[str, list[PositionSnapshot]] = defaultdict(list)
        reasons: list[str] = []
        for position in positions:
            if not isinstance(position, PositionSnapshot):
                reasons.append("authoritative positions contain an invalid row")
                continue
            if position.account_id != account_id:
                reasons.append(
                    f"authoritative position {position.instrument_id} belongs to a different account"
                )
            if not position.instrument_id.strip():
                reasons.append("authoritative position has no instrument identity")
                continue
            if not position.signed_quantity.is_finite():
                reasons.append(
                    f"authoritative position {position.instrument_id} has a non-finite quantity"
                )
                continue
            grouped[position.instrument_id].append(position)
        normalized: dict[str, Decimal] = {}
        for instrument_id, rows in grouped.items():
            first = rows[0]
            if any(row != first for row in rows[1:]):
                reasons.append(
                    f"authoritative positions contain contradictory duplicate rows for {instrument_id}"
                )
                continue
            normalized[instrument_id] = first.signed_quantity
        return normalized, reasons

    def _freshness_reasons(
        self,
        captured_at: datetime,
        *,
        label: str,
        max_age_seconds: int = BROKER_FACT_MAX_AGE_SECONDS,
    ) -> list[str]:
        """Reject stale/future safety facts at every order-capable boundary."""

        now = self._now()
        captured = captured_at
        if captured.tzinfo is None or captured.utcoffset() is None:
            return [f"{label} capture timestamp is not timezone-aware"]
        captured = captured.astimezone(timezone.utc)
        age = (now - captured).total_seconds()
        if age < -BROKER_CLOCK_SKEW_TOLERANCE_SECONDS:
            return [f"{label} capture timestamp is in the future"]
        if age > max_age_seconds:
            return [
                f"{label} is stale ({age:.1f}s old; maximum {max_age_seconds}s)"
            ]
        return []

    def _fresh_broker_facts(self, spec: Stage6PilotSpec) -> tuple[dict[str, Any], list[str]]:
        """Run the pilot's read-only account gate before any submit call."""

        facts, reasons = self._fresh_broker_snapshot(spec)
        if facts is None:
            return {}, reasons
        return {
            "account_id": facts.account_id,
            "captured_at": facts.captured_at.isoformat(),
            "complete": facts.complete,
            "execution_evidence_mode": facts.execution_evidence_mode.value,
            "execution_evidence_scope": sorted(facts.execution_evidence_scope),
            "position_count": len(facts.positions),
            "open_order_count": len(facts.open_orders),
            "fill_count": len(facts.fills),
            "error": facts.error,
        }, reasons

    @staticmethod
    def _broker_position_payload(position: PositionSnapshot) -> dict[str, Any]:
        return {
            "id": position.id,
            "broker_snapshot_id": position.broker_snapshot_id,
            "account_id": position.account_id,
            "instrument_id": position.instrument_id,
            "signed_quantity": str(position.signed_quantity),
            "average_price": str(position.average_price) if position.average_price is not None else None,
            "captured_at": position.captured_at.isoformat(),
            "metadata": _stable_value(position.metadata),
        }

    @staticmethod
    def _broker_order_payload(order: Any) -> dict[str, Any]:
        return {
            "id": order.id,
            "broker_snapshot_id": order.broker_snapshot_id,
            "account_id": order.account_id,
            "external_account_id": order.external_account_id,
            "instrument_id": order.instrument_id,
            "external_order_id": order.external_order_id,
            "client_order_id": order.client_order_id,
            "side": order.side.value,
            "quantity": str(order.quantity),
            "filled_quantity": str(order.filled_quantity),
            "status": order.status.value,
            "captured_at": order.captured_at.isoformat(),
            "order_time": order.order_time.isoformat() if order.order_time is not None else None,
            "no_fill_asserted": order.no_fill_asserted,
            "authority": order.authority,
            "metadata": _stable_value(order.metadata),
        }

    @staticmethod
    def _broker_fill_payload(fill: BrokerFill) -> dict[str, Any]:
        return {
            "external_order_id": fill.external_order_id,
            "external_fill_id": fill.external_fill_id,
            "dedupe_key": fill.dedupe_key,
            "quantity": str(fill.quantity),
            "price": str(fill.price),
            "filled_at": fill.filled_at.isoformat(),
            "received_at": fill.received_at.isoformat(),
            "account_id": fill.account_id,
            "instrument_id": fill.instrument_id,
            "evidence_reference": fill.evidence_reference,
            "evidence_mode": fill.evidence_mode.value,
            "metadata": _stable_value(fill.metadata),
        }

    def _fresh_market_state(
        self,
        spec: Stage6PilotSpec,
        symbols: Sequence[str],
        *,
        require_rth: bool = True,
    ) -> tuple[dict[str, Any], list[str]]:
        """Read authoritative provider market/session state without commands."""

        getter = getattr(self.oms.adapter, "get_authoritative_market_state", None)
        if not callable(getter):
            return {}, ["adapter lacks authoritative market/RTH state capability"]
        requested = tuple(str(symbol).strip().upper() for symbol in symbols if str(symbol).strip())
        if not requested:
            return {}, ["configured broker symbols are unavailable for market/RTH preflight"]
        try:
            value = getter(requested)
        except Exception as exc:
            return {}, [f"authoritative market/RTH state unavailable: {exc}"]
        if not isinstance(value, Mapping):
            return {}, ["authoritative market/RTH state returned an invalid report"]
        report = dict(value)
        reasons: list[str] = []
        if str(report.get("market", "")).strip().upper() != "US":
            reasons.append("authoritative market/RTH state is not for US")
        if report.get("complete") is not True:
            reasons.append("authoritative market/RTH state is incomplete")
        captured_at = report.get("captured_at")
        if not isinstance(captured_at, str) or not captured_at.strip():
            reasons.append("authoritative market/RTH state has no capture timestamp")
        else:
            try:
                parsed_capture = datetime.fromisoformat(captured_at.replace("Z", "+00:00"))
            except ValueError:
                reasons.append("authoritative market/RTH state has an invalid capture timestamp")
            else:
                if parsed_capture.tzinfo is None or parsed_capture.utcoffset() is None:
                    reasons.append("authoritative market/RTH state capture timestamp has no timezone")
                else:
                    reasons.extend(
                        self._freshness_reasons(
                            parsed_capture,
                            label="authoritative market/RTH state",
                        )
                    )
        raw_rows = report.get("rows")
        if type(raw_rows) is not list or not raw_rows:
            reasons.append("authoritative market/RTH state has no symbol rows")
            raw_rows = []
        rows: list[dict[str, Any]] = []
        observed_symbols: set[str] = set()
        rth_states = {"RTH", "REGULAR", "MORNING", "AFTERNOON"}
        for index, raw_row in enumerate(raw_rows):
            if not isinstance(raw_row, Mapping):
                reasons.append(f"authoritative market/RTH row {index} is malformed")
                continue
            row = dict(raw_row)
            symbol = str(row.get("symbol", row.get("code", ""))).strip().upper()
            state = str(row.get("market_state", row.get("session", row.get("state", "")))).strip().upper()
            if not symbol:
                reasons.append(f"authoritative market/RTH row {index} has no symbol")
            else:
                observed_symbols.add(symbol)
            if not state:
                reasons.append(f"authoritative market/RTH row {index} has no market state")
            elif require_rth and state not in rth_states:
                reasons.append(f"authoritative market state for {symbol or index} is not RTH: {state}")
            rows.append({**row, "symbol": symbol, "market_state": state})
        if observed_symbols != set(requested):
            reasons.append(
                "authoritative market/RTH symbols do not match configuration: "
                f"expected {sorted(set(requested))}, observed {sorted(observed_symbols)}"
            )
        report["market"] = str(report.get("market", "")).strip().upper()
        report["symbols"] = list(requested)
        report["rows"] = rows
        observed_rth = bool(rows) and not any(
            str(row.get("market_state", "")).upper() not in rth_states for row in rows
        )
        report["rth"] = {
            "observed": observed_rth and not reasons,
            "market_state": (
                rows[0].get("market_state")
                if rows and len({row.get("market_state") for row in rows}) == 1
                else "MIXED"
            ),
        }
        return report, reasons

    def broker_preflight(
        self,
        spec: Stage6PilotSpec,
        *,
        market_symbols: Sequence[str] = (),
        mapping_details: Sequence[Mapping[str, Any]] = (),
    ) -> dict[str, Any]:
        """Perform a connected, read-only broker/account preflight.

        This method intentionally does not call ``run``, ``recover``, or any
        GenericOMS submission/recovery method.  It creates no intents, legs,
        broker-order attempts, or fills.
        """

        started = self._now()
        stop_reasons: list[str] = []
        try:
            spec.validate_repository(self.repository)
        except Exception as exc:
            stop_reasons.append(f"configuration/repository validation failed: {exc}")

        local_blockers: list[str] = []
        book_risk_status: Mapping[str, Any] = {}
        try:
            book_risk_status = self.oms.book_risk_status(account=spec.account)
            blocked_books = [
                book_id
                for book_id, value in book_risk_status.get("books", {}).items()
                if isinstance(value, Mapping) and value.get("status") == "BLOCKED"
            ]
            if blocked_books:
                local_blockers.append("book risk status is blocked for: " + ", ".join(sorted(blocked_books)))
        except Exception as exc:
            local_blockers.append(f"book risk status unavailable: {exc}")
        try:
            local_blockers.extend(self._local_blockers(spec))
        except Exception as exc:
            local_blockers.append(f"local safety blockers unavailable: {exc}")

        unfinished_statuses = {
            IntentStatus.CREATED.value,
            IntentStatus.RISK_APPROVED.value,
            IntentStatus.SUBMITTING.value,
            IntentStatus.WORKING.value,
            IntentStatus.PARTIALLY_FILLED.value,
            IntentStatus.RECONCILIATION_REQUIRED.value,
        }
        try:
            closed_historical = self.oms._closed_historical_intent_ids(spec.account)
        except Exception as exc:
            closed_historical = set()
            local_blockers.append(f"historical intent closure state unavailable: {exc}")
        unfinished_intents = [
            {
                "intent_id": str(row.get("id")),
                "status": str(row.get("status")),
                "book_id": row.get("book_id"),
            }
            for row in self.repository.book_intents(spec.account.id)
            if str(row.get("status")) in unfinished_statuses
            and str(row.get("id")) not in closed_historical
        ]
        if unfinished_intents:
            local_blockers.append(
                f"{len(unfinished_intents)} unfinished account intent(s) block preflight"
            )
        stop_reasons.extend(local_blockers)

        facts, fact_reasons = self._fresh_broker_snapshot(spec)
        stop_reasons.extend(fact_reasons)
        market_report, market_reasons = self._fresh_market_state(spec, market_symbols)
        stop_reasons.extend(market_reasons)

        broker_facts: dict[str, Any] = {
            "account_id": facts.account_id if facts is not None else spec.account.id,
            "captured_at": facts.captured_at.isoformat() if facts is not None else None,
            "complete": facts.complete if facts is not None else False,
            "error": facts.error if facts is not None else "authoritative broker facts unavailable",
            "execution_evidence_mode": (
                facts.execution_evidence_mode.value if facts is not None else "UNAVAILABLE"
            ),
            "execution_evidence_scope": sorted(facts.execution_evidence_scope) if facts is not None else [],
            "flat": not facts.positions if facts is not None else False,
            "position_count": len(facts.positions) if facts is not None else 0,
            "open_order_count": len(facts.open_orders) if facts is not None else 0,
            "fill_count": len(facts.fills) if facts is not None else 0,
            "positions": [self._broker_position_payload(item) for item in facts.positions] if facts else [],
            "open_orders": [self._broker_order_payload(item) for item in facts.open_orders] if facts else [],
            "fills": [self._broker_fill_payload(item) for item in facts.fills] if facts else [],
            "metadata": _stable_value(facts.metadata) if facts is not None else {},
        }

        books: list[dict[str, Any]] = []
        targets_by_sleeve = {target.sleeve_id: target for target in spec.targets}
        allocation_by_sleeve = {target.sleeve_id: target for target in spec.allocation_update.targets}
        for sleeve in spec.sleeves:
            target = targets_by_sleeve[sleeve.sleeve_id]
            allocation = allocation_by_sleeve[sleeve.sleeve_id]
            books.append(
                {
                    "book_id": sleeve.book_id,
                    "sleeve_id": sleeve.sleeve_id,
                    "allocation_valid": True,
                    "mapping_valid": True,
                    "allocation": {
                        "version": spec.allocation_update.version,
                        "target_weight": str(allocation.target_weight) if allocation.target_weight is not None else None,
                        "capacity": str(allocation.capacity) if allocation.capacity is not None else None,
                        "capacity_unit": allocation.capacity_unit,
                    },
                    "expected_quantities": {
                        instrument_id: str(quantity)
                        for instrument_id, quantity in zip(
                            target.instrument_ids,
                            target.signed_quantities,
                            strict=True,
                        )
                    },
                }
            )

        open_issues = self.repository.open_reconciliation_issues(spec.account.id)
        open_actions = self.repository.open_recovery_actions(spec.account.id)
        preflight = {
            "account_identity": {
                "account_id": spec.account.id,
                "external_account_id": spec.account.external_account_id,
                "broker": spec.account.broker,
                "environment": spec.account.environment.value,
            },
            "fresh_facts": broker_facts,
            "rth": market_report.get("rth", {"observed": False}) if market_report else {"observed": False},
            "books": books,
            "mappings": [dict(item) for item in mapping_details],
            "mappings_valid": True,
            "quantities_valid": True,
            "safety_gates_passed": not stop_reasons,
            "blockers": {
                "issues": len(open_issues),
                "actions": len(open_actions),
                "unfinished_intents": len(unfinished_intents),
            },
            "reconciliation_issues": _stable_value(open_issues),
            "recovery_actions": _stable_value(open_actions),
            "unfinished_intents": unfinished_intents,
            "local_book_risk_status": _stable_value(book_risk_status),
            "local_blockers": list(local_blockers),
        }
        return {
            "run_id": spec.run_id,
            "mode": "BROKER_PREFLIGHT",
            "started_at": started.isoformat(),
            "completed_at": self._now().isoformat(),
            "account": preflight["account_identity"],
            "broker_contacted": True,
            "orders_submitted": 0,
            "preflight_passed": not stop_reasons,
            "broker_preflight_passed": not stop_reasons,
            "stop_reasons": list(stop_reasons),
            "broker_facts": broker_facts,
            "market_state": market_report,
            "preflight": preflight,
            "config": {"books": books, "mappings": [dict(item) for item in mapping_details]},
            "mutations": {
                "order_intents": 0,
                "order_legs": 0,
                "broker_orders": 0,
                "fills": 0,
                "submission_calls": 0,
                "cancel_calls": 0,
                "replace_calls": 0,
                "recovery_calls": 0,
            },
        }

    def compensating_exit_preflight(
        self,
        spec: Stage6PilotSpec,
        *,
        market_symbols: Sequence[str] = (),
    ) -> dict[str, Any]:
        """Require a fresh authoritative RTH observation before an exit.

        The proof-gated OMS primitive remains the only submit path.  This
        read-only gate is deliberately separate so a closed, stale, missing,
        or contradictory market observation returns before that primitive is
        invoked.
        """

        if spec.account.environment is not TradingEnvironment.SIM:
            raise ValueError("Stage 6 compensating exits accept SIM accounts only")
        _facts, fact_reasons = self._fresh_broker_snapshot(spec)
        report, market_reasons = self._fresh_market_state(
            spec,
            tuple(market_symbols) or self._configured_symbols(spec),
            require_rth=True,
        )
        reasons = list(fact_reasons) + list(market_reasons)
        return {
            "run_id": spec.run_id,
            "mode": "COMPENSATING_EXIT_PREFLIGHT",
            "broker_contacted": True,
            "market_state": report,
            "preflight_passed": not reasons,
            "stop_reasons": list(dict.fromkeys(reasons)),
            "broker_submission_count": 0,
            "cancel_count": 0,
            "replace_count": 0,
            "recovery_calls": 0,
            "mutations": {
                "order_intents": 0,
                "order_legs": 0,
                "broker_orders": 0,
                "fills": 0,
                "submission_calls": 0,
                "cancel_calls": 0,
                "replace_calls": 0,
                "recovery_calls": 0,
            },
        }

    @staticmethod
    def _configured_symbols(spec: Stage6PilotSpec) -> tuple[str, ...]:
        return tuple(
            symbol
            for sleeve in spec.sleeves
            for symbol in sleeve.symbols
            if str(symbol).strip()
        )

    def submit_verified_compensating_exit(
        self,
        *,
        account: Account,
        source_intent_id: str,
        expected_external_order_ids: Mapping[str, str],
        spec: Stage6PilotSpec | None = None,
        market_symbols: Sequence[str] = (),
    ) -> dict[str, Any]:
        """Expose the existing proof-gated OMS primitive without reimplementing it."""

        if account.environment is not TradingEnvironment.SIM:
            raise ValueError("Stage 6 compensating exits accept SIM accounts only")
        if spec is None:
            raise ValueError(
                "Stage 6 compensating exits require the configured pilot spec for scope and fresh-fact gates"
            )
        if account.id != spec.account.id or account.external_account_id != spec.account.external_account_id:
            raise ValueError("Stage 6 compensating exit account does not match the configured account")
        spec.validate_repository(self.repository)
        self._validate_recovery_scope(spec, (source_intent_id,))
        before_submit = self._submission_safety_gate(
            spec,
            tuple(market_symbols) or self._configured_symbols(spec),
            provenance_mode="COMPENSATING_EXIT",
            source_intent_id=str(source_intent_id),
        )
        result = self.oms.submit_verified_compensating_exit(
            source_intent_id=source_intent_id,
            expected_external_order_ids=expected_external_order_ids,
            account=account,
            _before_submit_leg=before_submit,
        )
        compensating_intent_id = str(result.get("id") or result.get("intent_id") or "")
        snapshots = self.repository.get_intent(compensating_intent_id) if compensating_intent_id else None
        attempt_rows = [
            attempt
            for leg in (snapshots or {}).get("legs", ())
            for attempt in self.repository.broker_orders_for_leg(str(leg.get("id")))
        ]
        return {
            "source_intent_id": str(source_intent_id),
            "source_external_order_ids": dict(expected_external_order_ids),
            "compensating_result": _stable_value(result),
            "compensating_exit_intent_id": compensating_intent_id,
            "compensating_leg_ids": [str(leg.get("id")) for leg in (snapshots or {}).get("legs", ())],
            "broker_order_attempt_ids": [str(row.get("id")) for row in attempt_rows],
            "external_broker_order_ids": [str(row.get("external_order_id")) for row in attempt_rows],
            "statuses": [str(row.get("status")) for row in attempt_rows],
            "fill_state": [
                {
                    "external_order_id": str(row.get("external_order_id")),
                    "filled_quantity": row.get("filled_quantity"),
                    "average_fill_price": row.get("average_fill_price"),
                }
                for row in attempt_rows
            ],
            "execution_path": "Stage6PilotRunner->GenericOMS->MooMooGenericAdapter->OpenD",
            "broker_contacted": True,
            "broker_submission_count": len(attempt_rows),
            "cancel_count": 0,
            "replace_count": 0,
            "duplicate_attempt": any(
                len(self.repository.broker_orders_for_leg(str(leg.get("id")))) > 1
                for leg in (snapshots or {}).get("legs", ())
            ),
            "stop_reasons": [],
        }

    def submit_verified_residual_exit(
        self,
        *,
        account: Account,
        source_intent_id: str,
        expected_external_order_ids: Mapping[str, str],
        spec: Stage6PilotSpec,
        market_symbols: Sequence[str] = (),
    ) -> dict[str, Any]:
        """Submit a proof-gated exit for one exact partial Stage 6 source.

        Scope is checked here, while the broker-neutral OMS owns the durable
        fill/position proof and idempotent exit construction.  Retired legacy
        evidence is intentionally excluded from this order-bearing route.
        """
        if account.environment is not TradingEnvironment.SIM:
            raise ValueError("Stage 6 residual exits accept SIM accounts only")
        if account.id != spec.account.id or account.external_account_id != spec.account.external_account_id:
            raise ValueError("Stage 6 residual exit account does not match the configured account")
        spec.validate_repository(self.repository)
        self._validate_recovery_scope(spec, (source_intent_id,))
        source = self.repository.get_intent(str(source_intent_id))
        if source is None:
            raise ValueError(f"unknown Stage 6 residual source intent: {source_intent_id}")
        metadata = source.get("metadata")
        if isinstance(metadata, Mapping) and (metadata.get("legacy_import") or metadata.get("retired_baseline")):
            raise ValueError("retired legacy evidence cannot submit a residual exit")
        allowed_strategies = {str(item.strategy_id) for item in spec.sleeves}
        allowed_books = {str(item.book_id) for item in spec.sleeves}
        allowed_instruments = {
            str(instrument_id)
            for sleeve in spec.sleeves
            for instrument_id in sleeve.instrument_ids
        }
        if str(source.get("strategy_id")) not in allowed_strategies:
            raise ValueError("Stage 6 residual source strategy is outside the configured pilot")
        if str(source.get("book_id") or "") not in allowed_books:
            raise ValueError("Stage 6 residual source book is outside the configured pilot")
        for leg in source.get("legs", ()):
            if str(leg.get("instrument_id") or "") not in allowed_instruments:
                raise ValueError("Stage 6 residual source instrument is outside the configured pilot")

        before_submit = self._submission_safety_gate(
            spec,
            tuple(market_symbols) or self._configured_symbols(spec),
            provenance_mode="RESIDUAL_EXIT",
            source_intent_id=str(source_intent_id),
        )
        result = self.oms.submit_verified_residual_exit(
            source_intent_id=str(source_intent_id),
            expected_external_order_ids=expected_external_order_ids,
            account=account,
            _before_submit_leg=before_submit,
        )
        residual_intent_id = str(result.get("id") or result.get("intent_id") or "")
        snapshot = self.repository.get_intent(residual_intent_id) if residual_intent_id else None
        attempt_rows = [
            attempt
            for leg in (snapshot or {}).get("legs", ())
            for attempt in self.repository.broker_orders_for_leg(str(leg.get("id")))
        ]
        return {
            "source_intent_id": str(source_intent_id),
            "source_external_order_ids": dict(expected_external_order_ids),
            "residual_result": _stable_value(result),
            "residual_exit_intent_id": residual_intent_id,
            "residual_leg_ids": [str(leg.get("id")) for leg in (snapshot or {}).get("legs", ())],
            "broker_order_attempt_ids": [str(row.get("id")) for row in attempt_rows],
            "external_broker_order_ids": [str(row.get("external_order_id")) for row in attempt_rows],
            "statuses": [str(row.get("status")) for row in attempt_rows],
            "fill_state": [
                {
                    "external_order_id": str(row.get("external_order_id")),
                    "filled_quantity": row.get("filled_quantity"),
                    "average_fill_price": row.get("average_fill_price"),
                }
                for row in attempt_rows
            ],
            "execution_path": "Stage6PilotRunner->GenericOMS->MooMooGenericAdapter->OpenD",
            "broker_contacted": True,
            "broker_submission_count": len(attempt_rows),
            "cancel_count": 0,
            "replace_count": 0,
            "duplicate_attempt": any(
                len(self.repository.broker_orders_for_leg(str(leg.get("id")))) > 1
                for leg in (snapshot or {}).get("legs", ())
            ),
            "stop_reasons": [],
        }

    def _submission_safety_gate(
        self,
        spec: Stage6PilotSpec,
        market_symbols: Sequence[str],
        *,
        provenance_mode: str = Stage6RunMode.SIM_SUBMIT.value,
        source_intent_id: str | None = None,
    ) -> Callable[[Any, Any, Account], None]:
        """Build a per-leg fresh account/RTH admission callback."""

        recorded_intents: set[str] = set()

        def gate(_intent: Any, _leg: Any, account: Account) -> None:
            if account.id != spec.account.id or account.environment is not TradingEnvironment.SIM:
                raise ValueError("Stage 6 submission account is not the configured SIM account")
            intent_id = str(getattr(_intent, "id", "")).strip()
            if not intent_id:
                raise ValueError("Stage 6 submission intent identity is missing")
            if intent_id not in recorded_intents:
                # Persist immutable provenance after the local intent exists,
                # but before any adapter submit call can occur.  A failure
                # here is therefore a durable pre-submit block, never a
                # post-submit provenance hole.
                self._record_submission_process_identity(
                    intent_id=intent_id,
                    account_id=account.id,
                    run_id=spec.run_id,
                    mode=provenance_mode,
                    source_intent_id=source_intent_id,
                )
                recorded_intents.add(intent_id)
            # GenericOMS invokes this hook immediately before every sibling
            # can reach the adapter.  Once a prior leg has been accepted, do
            # not treat its WORKING ACK as enough to submit the next leg: wait
            # for that exact owned leg to become durably FILLED first.  This
            # keeps the existing account/RTH gate strict while preventing the
            # first still-working order from being mistaken for a clean
            # hand-off opportunity.
            persisted_before_gate = self.repository.get_intent(intent_id)
            prior_leg_was_durably_recovered = False
            current_sequence = getattr(_leg, "sequence", None)
            if persisted_before_gate is None or current_sequence is None:
                raise ValueError("Stage 6 submission intent/leg ownership is unavailable")
            try:
                current_sequence = int(current_sequence)
            except (TypeError, ValueError) as exc:
                raise ValueError("Stage 6 submission leg sequence is invalid") from exc
            prior_legs = sorted(
                (
                    row
                    for row in persisted_before_gate.get("legs", ())
                    if int(row.get("sequence", -1)) < current_sequence
                ),
                key=lambda row: int(row.get("sequence", -1)),
            )
            for prior_leg in prior_legs:
                prior_id = str(prior_leg.get("id") or "").strip()
                if not prior_id:
                    raise ValueError("Stage 6 prior sibling leg identity is missing")
                _prior_snapshot, prior_verified, prior_state = self._leg_fill_state(
                    intent_id=intent_id,
                    leg_id=prior_id,
                )
                if not prior_verified:
                    _prior_snapshot, prior_state = self._wait_for_verified_leg_fill(
                        intent_id=intent_id,
                        leg_id=prior_id,
                        account=account,
                    )
                    if prior_state != "FULL":
                        self._last_submission_gate_outcome = prior_state
                        raise ValueError(
                            "prior Stage 6 sibling leg "
                            f"{prior_id} returned {prior_state}; next leg remains unsubmitted"
                        )
                prior_leg_was_durably_recovered = True
            exit_target_books: set[str] | None = None
            if spec.action is IntentAction.EXIT:
                # An EXIT batch is dispatched one book at a time.  The first
                # leg for this intent must prove that its book still matches
                # the configured closing target; after that leg fills, the
                # account snapshot necessarily changes before the sibling leg
                # and before the next book.  Keep the global allocation/fact
                # equality checks on every leg, while scoping this target
                # equality check to the current book and only its first
                # attempt.
                persisted = self.repository.get_intent(str(getattr(_intent, "id", "")))
                has_attempts = bool(
                    persisted
                    and any(
                        self.repository.broker_orders_for_leg(str(row.get("id")))
                        for row in persisted.get("legs", ())
                    )
                )
                exit_target_books = (
                    set()
                    if has_attempts
                    else {str(getattr(_intent, "book_id", "") or "")}
                )
            _facts, fact_reasons = self._fresh_broker_snapshot(
                spec,
                exit_target_books=exit_target_books,
                # A successful prior-leg wait has already persisted its
                # exact fill and allocation.  Do not add that same exposure a
                # second time as a transient claim while checking the next
                # leg.  The first leg of an intent still uses the existing
                # transient path because no sibling has been recovered yet.
                transient_intent=None if prior_leg_was_durably_recovered else _intent,
            )
            _market, market_reasons = self._fresh_market_state(
                spec,
                market_symbols,
                require_rth=True,
            )
            reasons = list(fact_reasons) + list(market_reasons)
            if reasons:
                raise ValueError("Stage 6 per-leg submission gate blocked: " + "; ".join(reasons))

        return gate

    def resolve_verified_roundtrip(
        self,
        *,
        account: Account,
        entry_intent_id: str,
        exit_intent_id: str,
        spec: Stage6PilotSpec,
    ) -> dict[str, Any]:
        """Expose the existing proof-backed resolver as a local-only boundary."""

        if account.environment is not TradingEnvironment.SIM:
            raise ValueError("Stage 6 round-trip resolution accepts SIM accounts only")
        if account.id != spec.account.id or account.external_account_id != spec.account.external_account_id:
            raise ValueError("Stage 6 round-trip account does not match the configured account")
        spec.validate_repository(self.repository)
        self._validate_recovery_scope(spec, (entry_intent_id, exit_intent_id))
        result = self.oms.resolve_verified_roundtrip(
            entry_intent_id=str(entry_intent_id),
            exit_intent_id=str(exit_intent_id),
            account=account,
        )
        return {
            **_stable_value(result),
            "entry_intent_id": str(entry_intent_id),
            "exit_intent_id": str(exit_intent_id),
            "execution_path": "Stage6PilotRunner->GenericOMS",
            "broker_contacted": True,
            "orders_submitted": 0,
            "submission_count": 0,
            "cancel_count": 0,
            "replace_count": 0,
        }

    def _validate_recovery_scope(
        self,
        spec: Stage6PilotSpec,
        intent_ids: Sequence[str],
    ) -> None:
        """Reject proof operations that name foreign strategy/book rows."""

        allowed_strategies = {str(item.strategy_id) for item in spec.sleeves}
        allowed_books = {str(item.book_id) for item in spec.sleeves}
        allowed_instruments = {
            str(instrument_id)
            for sleeve in spec.sleeves
            for instrument_id in sleeve.instrument_ids
        }
        for raw_id in intent_ids:
            intent_id = str(raw_id).strip()
            intent = self.repository.get_intent(intent_id)
            if intent is None:
                raise ValueError(f"unknown Stage 6 recovery intent: {intent_id}")
            if str(intent.get("account_id")) != spec.account.id:
                raise ValueError(f"recovery intent {intent_id} belongs to another account")
            if str(intent.get("strategy_id")) not in allowed_strategies:
                raise ValueError(f"recovery intent {intent_id} belongs to another strategy")
            if str(intent.get("book_id") or "") not in allowed_books:
                raise ValueError(f"recovery intent {intent_id} belongs to another book")
            for leg in intent.get("legs", ()):
                if str(leg.get("instrument_id") or "") not in allowed_instruments:
                    raise ValueError(
                        f"recovery intent {intent_id} contains an instrument outside the configured universe"
                    )

    def validate_recovery_scope(
        self,
        spec: Stage6PilotSpec,
        intent_ids: Sequence[str],
    ) -> None:
        """Validate proof/order intent ownership without contacting a broker.

        CLI commands call this boundary before constructing a connected
        adapter path.  The same check is repeated by each runner operation so
        an in-process caller cannot bypass the repository ownership gate.
        """

        self._validate_recovery_scope(spec, intent_ids)

    def resolve_unsubmitted(
        self,
        spec: Stage6PilotSpec,
        *,
        intent_id: str,
        expected_book_id: str | None = None,
    ) -> dict[str, Any]:
        if spec.account.environment is not TradingEnvironment.SIM:
            raise ValueError("Stage 6 resolution accepts SIM accounts only")
        self._validate_recovery_scope(spec, (intent_id,))
        return _stable_value(
            self.oms.resolve_unsubmitted_intent(
                str(intent_id),
                account=spec.account,
                expected_book_id=expected_book_id,
            )
        )

    def resolve_compensated_partial(
        self,
        spec: Stage6PilotSpec,
        *,
        intent_id: str,
    ) -> dict[str, Any]:
        if spec.account.environment is not TradingEnvironment.SIM:
            raise ValueError("Stage 6 resolution accepts SIM accounts only")
        self._validate_recovery_scope(spec, (intent_id,))
        return _stable_value(
            self.oms.resolve_compensated_partial_intent(str(intent_id), account=spec.account)
        )

    def cancel_known_partial(
        self,
        spec: Stage6PilotSpec,
        *,
        intent_id: str,
        external_order_id: str,
        market_symbols: Sequence[str] = (),
    ) -> dict[str, Any]:
        """Cancel one scoped partial order after a fresh SIM/RTH gate."""
        if spec.account.environment is not TradingEnvironment.SIM:
            raise ValueError("Stage 6 partial cancellation accepts SIM accounts only")
        spec.validate_repository(self.repository)
        self._validate_recovery_scope(spec, (intent_id,))
        facts, fact_reasons = self._fresh_broker_snapshot(
            spec,
            allow_open_order_external_ids={str(external_order_id)},
        )
        market_report, market_reasons = self._fresh_market_state(
            spec,
            tuple(market_symbols) or self._configured_symbols(spec),
            require_rth=True,
        )
        reasons = list(fact_reasons) + list(market_reasons)
        if reasons:
            return {
                "intent_id": str(intent_id),
                "external_order_id": str(external_order_id),
                "mode": "CANCEL_KNOWN_PARTIAL",
                "broker_contacted": True,
                "cancel_count": 0,
                "submission_count": 0,
                "preflight_passed": False,
                "market_state": market_report,
                "broker_facts": (
                    {
                        "account_id": facts.account_id,
                        "captured_at": facts.captured_at.isoformat(),
                        "complete": facts.complete,
                        "error": facts.error,
                        "positions": [self._broker_position_payload(item) for item in facts.positions],
                        "open_orders": [self._broker_order_payload(item) for item in facts.open_orders],
                    }
                    if facts is not None
                    else None
                ),
                "stop_reasons": list(dict.fromkeys(reasons)),
            }
        result = self.oms.cancel_known_partial_attempt(
            str(intent_id),
            account=spec.account,
            external_order_id=str(external_order_id),
        )
        return {
            **_stable_value(result),
            "mode": "CANCEL_KNOWN_PARTIAL",
            "market_state": market_report,
            "preflight_passed": True,
            "stop_reasons": [],
            "execution_path": "Stage6PilotRunner->GenericOMS->MooMooGenericAdapter->OpenD",
        }

    def resolve_aggregate_roundtrip(
        self,
        spec: Stage6PilotSpec,
        *,
        intent_ids: Sequence[str],
    ) -> dict[str, Any]:
        if spec.account.environment is not TradingEnvironment.SIM:
            raise ValueError("Stage 6 resolution accepts SIM accounts only")
        self._validate_recovery_scope(spec, intent_ids)
        return _stable_value(
            self.oms.resolve_verified_aggregate_roundtrip(
                intent_ids=tuple(str(value) for value in intent_ids),
                account=spec.account,
            )
        )

    def final_state(
        self,
        spec: Stage6PilotSpec,
        *,
        market_symbols: Sequence[str] = (),
    ) -> dict[str, Any]:
        """Observe fresh broker and durable local state without lifecycle writes."""

        if spec.account.environment is not TradingEnvironment.SIM:
            raise ValueError("Stage 6 final-state accepts SIM accounts only")
        spec.validate_repository(self.repository)
        facts, fact_reasons = self._fresh_broker_snapshot(spec)
        requested_symbols = tuple(market_symbols) or self._configured_symbols(spec)
        market_report, market_reasons = self._fresh_market_state(
            spec,
            requested_symbols,
            require_rth=False,
        )
        reasons = list(fact_reasons) + list(market_reasons)
        open_issues = self.repository.open_reconciliation_issues(spec.account.id)
        open_actions = self.repository.open_recovery_actions(spec.account.id)
        if open_issues:
            reasons.append("open reconciliation issues remain")
        if open_actions:
            reasons.append("open recovery actions remain")

        closed_historical = self.oms._closed_historical_intent_ids(spec.account)
        unfinished_statuses = {
            IntentStatus.CREATED.value,
            IntentStatus.RISK_APPROVED.value,
            IntentStatus.SUBMITTING.value,
            IntentStatus.WORKING.value,
            IntentStatus.PARTIALLY_FILLED.value,
            IntentStatus.RECONCILIATION_REQUIRED.value,
        }
        intents = self.repository.book_intents(spec.account.id)
        unfinished = [
            {
                "intent_id": str(row.get("id")),
                "book_id": row.get("book_id"),
                "status": row.get("status"),
            }
            for row in intents
            if str(row.get("status")) in unfinished_statuses
            and str(row.get("id")) not in closed_historical
        ]
        if unfinished:
            reasons.append(f"{len(unfinished)} unfinished intent(s) remain")

        exposures: dict[str, dict[str, str]] = {}
        exposure_totals: dict[str, str] = {}
        for book_id in spec.book_ids:
            try:
                exposure = self.repository.book_signed_exposure(spec.account.id, book_id)
                exposures[book_id] = {key: str(value) for key, value in sorted(exposure.items())}
                # The validator's durable final-state contract uses one
                # scalar per book.  Use gross signed-quantity magnitude so a
                # crossed pair cannot look flat merely because its net sum is
                # zero; retain the per-instrument map in ``book_exposures``.
                exposure_totals[book_id] = str(sum((abs(value) for value in exposure.values()), Decimal("0")))
                if any(value != 0 for value in exposure.values()):
                    reasons.append(f"book {book_id} is not flat")
            except Exception as exc:
                reasons.append(f"book {book_id} exposure unavailable: {exc}")
                exposure_totals[book_id] = "NaN"

        terminal_statuses = {
            IntentStatus.FILLED.value,
            IntentStatus.COMPLETED.value,
            IntentStatus.CANCELLED.value,
            IntentStatus.REJECTED.value,
            IntentStatus.FAILED.value,
        }
        terminal_intents = bool(intents) and all(
            str(row.get("status")) in terminal_statuses or str(row.get("id")) in closed_historical
            for row in intents
        )
        if not terminal_intents:
            reasons.append("not every managed intent is terminal")
        orders = self.repository.book_broker_orders(spec.account.id)
        configured_books = set(spec.book_ids)
        all_attributable = all(
            str(row.get("book_id")) in configured_books
            or str(row.get("intent_id")) in closed_historical
            for row in orders
        )
        if not all_attributable:
            reasons.append("account broker orders include an unattributed book")
        broker_facts = {
            "account_id": facts.account_id if facts is not None else spec.account.id,
            "captured_at": facts.captured_at.isoformat() if facts is not None else None,
            "complete": facts.complete if facts is not None else False,
            "error": facts.error if facts is not None else "authoritative broker facts unavailable",
            "flat": not facts.positions if facts is not None else False,
            "open_order_count": len(facts.open_orders) if facts is not None else 0,
            "positions": [self._broker_position_payload(item) for item in facts.positions] if facts else [],
            "open_orders": [self._broker_order_payload(item) for item in facts.open_orders] if facts else [],
            "fills": [self._broker_fill_payload(item) for item in facts.fills] if facts else [],
            "execution_evidence_mode": (
                facts.execution_evidence_mode.value if facts is not None else "UNAVAILABLE"
            ),
            "execution_evidence_scope": sorted(facts.execution_evidence_scope) if facts is not None else [],
        }
        flat = bool(facts is not None and facts.complete and not facts.positions)
        no_open_orders = bool(facts is not None and facts.complete and not facts.open_orders)
        final = {
            "account_identity": {
                "account_id": spec.account.id,
                "external_account_id": spec.account.external_account_id,
                "broker": spec.account.broker,
                "environment": spec.account.environment.value,
            },
            "captured_at": facts.captured_at.isoformat() if facts is not None else None,
            "fresh_facts": broker_facts,
            "flat": flat,
            "no_open_orders": no_open_orders,
            "open_order_count": len(facts.open_orders) if facts is not None else 0,
            "book_exposure": exposure_totals,
            "book_exposures": exposures,
            "open_reconciliation_issues": _stable_value(open_issues),
            "open_recovery_actions": _stable_value(open_actions),
            "issues": len(open_issues),
            "actions": len(open_actions),
            "unfinished_intents": len(unfinished),
            "unfinished_intent_blockers": unfinished,
            "terminal_intents": terminal_intents,
            "all_orders_attributable": all_attributable,
            "terminal_intent_status": [
                {"intent_id": str(row.get("id")), "status": str(row.get("status"))}
                for row in intents
            ],
            "session_orders": [_stable_value(row) for row in orders],
            "market_state": market_report,
        }
        return {
            "run_id": spec.run_id,
            "mode": "FINAL_STATE",
            "account": final["account_identity"],
            "broker_contacted": True,
            "execution_path": "Stage6PilotRunner->GenericOMS->MooMooGenericAdapter->OpenD",
            "started_at": self._now().isoformat(),
            "completed_at": self._now().isoformat(),
            "captured_at": final["captured_at"],
            "fresh_facts": broker_facts,
            "market_state": market_report,
            "final": final,
            "final_state_passed": not reasons,
            "stop_reasons": list(dict.fromkeys(reasons)),
            "open_reconciliation_issues": _stable_value(open_issues),
            "open_recovery_actions": _stable_value(open_actions),
            "unfinished_intents": unfinished,
            "broker_submission_count": 0,
            "cancel_count": 0,
            "replace_count": 0,
            "orders_submitted": 0,
            "mutations": {
                "order_intents": 0,
                "order_legs": 0,
                "broker_orders": 0,
                "fills": 0,
                "submission_calls": 0,
                "cancel_calls": 0,
                "replace_calls": 0,
                "recovery_calls": 0,
            },
        }

    def prepare_exit(
        self,
        spec: Stage6PilotSpec,
        *,
        market_symbols: Sequence[str] = (),
    ) -> dict[str, Any]:
        """Derive a normal two-book close from matching local and broker exposure."""

        if spec.account.environment is not TradingEnvironment.SIM:
            raise ValueError("Stage 6 exit preparation accepts SIM accounts only")
        spec.validate_repository(self.repository)
        facts, fact_reasons = self._fresh_broker_snapshot(spec)
        reasons = list(fact_reasons)
        open_issues = self.repository.open_reconciliation_issues(spec.account.id)
        open_actions = self.repository.open_recovery_actions(spec.account.id)
        if open_issues:
            reasons.append("open reconciliation issues remain")
        if open_actions:
            reasons.append("open recovery actions remain")
        expected_by_instrument: dict[str, Decimal] = defaultdict(Decimal)
        books: list[dict[str, Any]] = []
        for sleeve in spec.sleeves:
            local = self.repository.book_signed_exposure(spec.account.id, sleeve.book_id)
            target: dict[str, str] = {}
            legs: list[dict[str, Any]] = []
            for instrument_id in sleeve.instrument_ids:
                quantity = local.get(instrument_id, Decimal("0"))
                expected_by_instrument[instrument_id] += quantity
                target[instrument_id] = str(quantity)
                if quantity == 0:
                    reasons.append(f"book {sleeve.book_id} has no non-zero exposure for {instrument_id}")
                    continue
                legs.append(
                    {
                        "instrument_id": instrument_id,
                        "basis_signed_quantity": str(quantity),
                        "quantity": str(abs(quantity)),
                        "side": "SELL" if quantity > 0 else "BUY",
                    }
                )
            books.append({"book_id": sleeve.book_id, "sleeve_id": sleeve.sleeve_id, "signed_quantities": target, "closing_legs": legs})
        observed: dict[str, Decimal] = {}
        if facts is not None:
            observed, position_reasons = self._strict_position_map(
                facts.positions,
                account_id=spec.account.id,
            )
            reasons.extend(position_reasons)
        if facts is None or not facts.complete:
            reasons.append("fresh broker positions are unavailable")
        elif dict(expected_by_instrument) != {key: value for key, value in observed.items() if value != 0}:
            reasons.append(
                f"fresh broker positions do not match durable book exposure: expected {dict(expected_by_instrument)!r}, observed {dict(observed)!r}"
            )
        return {
            "run_id": spec.run_id,
            "mode": "PREPARE_EXIT",
            "broker_contacted": True,
            "execution_path": "Stage6PilotRunner->GenericOMS->MooMooGenericAdapter->OpenD",
            "account": {
                "account_id": spec.account.id,
                "external_account_id": spec.account.external_account_id,
                "environment": spec.account.environment.value,
            },
            "captured_at": facts.captured_at.isoformat() if facts is not None else None,
            "fresh_facts": {
                "complete": facts.complete if facts is not None else False,
                "positions": [self._broker_position_payload(item) for item in facts.positions] if facts else [],
                "open_order_count": len(facts.open_orders) if facts is not None else 0,
            },
            "books": books,
            "current_exposure": {key: str(value) for key, value in sorted(expected_by_instrument.items())},
            "observed_broker_exposure": {key: str(value) for key, value in sorted(observed.items()) if value != 0},
            "derived_signed_quantities": {key: str(value) for key, value in sorted(expected_by_instrument.items())},
            "pre_negation_applied": False,
            "creates_order_intent": False,
            "orders_submitted": 0,
            "broker_submission_count": 0,
            "cancel_count": 0,
            "replace_count": 0,
            "prepare_exit_passed": not reasons,
            "stop_reasons": list(dict.fromkeys(reasons)),
            "mutations": {"order_intents": 0, "order_legs": 0, "broker_orders": 0, "fills": 0},
        }

    def build_session_evidence(
        self,
        spec: Stage6PilotSpec,
        *,
        session_id: str,
        entry_intent_ids: Sequence[str],
        exit_intent_ids: Sequence[str],
        preflight: Mapping[str, Any] | None = None,
        recovery: Mapping[str, Any] | None = None,
        final: Mapping[str, Any] | None = None,
        commit_sha: str = "UNKNOWN",
        trading_date: str | None = None,
        execution_compatibility: str | None = None,
    ) -> dict[str, Any]:
        """Derive validator-shaped entry/exit evidence from durable rows only."""

        inverse_exposure_proven = False
        inverse_exposure_reason = ""
        entry_basis: dict[tuple[str, str], Decimal] = {}
        exit_basis: dict[tuple[str, str], Decimal] = {}

        def collect_basis(
            intent_ids: Sequence[str],
            destination: dict[tuple[str, str], Decimal],
            label: str,
        ) -> bool:
            valid = True
            for intent_id in intent_ids:
                intent = self.repository.get_intent(str(intent_id))
                if intent is None:
                    inverse_exposure_reason = f"{label} intent {intent_id} is not durable"
                    missing_basis_reasons.append(inverse_exposure_reason)
                    valid = False
                    continue
                expected_action = IntentAction.ENTER.value if label == "entry" else {
                    IntentAction.EXIT.value,
                    IntentAction.FLATTEN.value,
                }
                if label == "entry":
                    action_valid = str(intent.get("action")) == expected_action
                else:
                    action_valid = str(intent.get("action")) in expected_action
                if not action_valid:
                    missing_basis_reasons.append(
                        f"{label} intent {intent_id} has an incompatible action"
                    )
                    valid = False
                for leg in intent.get("legs", ()):
                    instrument_id = str(leg.get("instrument_id") or "").strip()
                    side = str(leg.get("side") or "").strip().upper()
                    if not instrument_id or side not in {Side.BUY.value, Side.SELL.value}:
                        missing_basis_reasons.append(
                            f"{label} intent {intent_id} has malformed durable leg identity"
                        )
                        valid = False
                        continue
                    try:
                        quantity = Decimal(str(leg.get("quantity")))
                    except (InvalidOperation, TypeError, ValueError):
                        quantity = Decimal("NaN")
                    key = (instrument_id, side)
                    if not quantity.is_finite() or quantity <= 0 or key in destination:
                        missing_basis_reasons.append(
                            f"{label} durable legs do not provide unique positive quantities"
                        )
                        valid = False
                        continue
                    destination[key] = quantity
            return valid

        missing_basis_reasons: list[str] = []
        entry_basis_valid = collect_basis(entry_intent_ids, entry_basis, "entry")
        exit_basis_valid = collect_basis(exit_intent_ids, exit_basis, "exit")
        if entry_basis_valid and exit_basis_valid and entry_basis and len(entry_basis) == len(exit_basis):
            expected_exit_basis = {
                (instrument_id, Side.SELL.value if side == Side.BUY.value else Side.BUY.value): quantity
                for (instrument_id, side), quantity in entry_basis.items()
            }
            inverse_exposure_proven = expected_exit_basis == exit_basis
            if not inverse_exposure_proven:
                inverse_exposure_reason = (
                    "durable exit legs do not exactly oppose every durable entry leg"
                )
                missing_basis_reasons.append(inverse_exposure_reason)
        else:
            inverse_exposure_reason = "durable entry/exit legs are incomplete for inverse exposure proof"
            missing_basis_reasons.append(inverse_exposure_reason)

        def section(intent_ids: Sequence[str], label: str) -> tuple[dict[str, Any], list[str]]:
            ids = tuple(str(value).strip() for value in intent_ids if str(value).strip())
            missing: list[str] = []
            expected: list[dict[str, Any]] = []
            actual: list[dict[str, Any]] = []
            duplicate_attempts = 0
            fills_complete = True
            attributable = True
            for intent_id in ids:
                intent = self.repository.get_intent(intent_id)
                if intent is None:
                    missing.append(f"{label} intent {intent_id} is not durable")
                    continue
                if str(intent.get("account_id")) != spec.account.id:
                    missing.append(f"{label} intent {intent_id} belongs to another account")
                for leg in intent.get("legs", ()):
                    attempts = self.repository.broker_orders_for_leg(str(leg.get("id")))
                    duplicate_attempts += max(0, len(attempts) - 1)
                    if not attempts:
                        missing.append(f"{label} leg {leg.get('id')} has no durable broker attempt")
                    for order in attempts:
                        external_id = str(order.get("external_order_id") or "").strip()
                        requested = order.get("submitted_quantity", leg.get("quantity"))
                        filled = order.get("filled_quantity", leg.get("cumulative_filled_quantity", "0"))
                        fills = self.repository.fills_for_broker_order(str(order.get("id")))
                        fill_price = fills[0].get("price") if len(fills) == 1 else order.get("average_fill_price")
                        row = {
                            "order_id": external_id,
                            "broker_order_id": str(order.get("id")),
                            "external_order_id": external_id,
                            "intent_id": intent_id,
                            "leg_id": str(leg.get("id")),
                            "account_id": str(order.get("account_id")),
                            "instrument_id": str(leg.get("instrument_id")),
                            "side": str(leg.get("side")),
                            "status": str(order.get("status")),
                            "quantity": str(requested),
                            "filled_quantity": str(filled),
                            "fill_price": str(fill_price) if fill_price is not None else None,
                            "attributable": str(order.get("account_id")) == spec.account.id and bool(external_id),
                            "fills": _stable_value(fills),
                        }
                        expected.append({"order_id": external_id, "intent_id": intent_id, "quantity": str(requested)})
                        actual.append(row)
                        try:
                            fills_complete = fills_complete and str(order.get("status")) == "FILLED" and Decimal(str(requested)) == Decimal(str(filled)) and len(fills) == 1
                        except (InvalidOperation, TypeError, ValueError):
                            fills_complete = False
                        attributable = attributable and bool(row["attributable"])
            return (
                {
                    "expected_intents": [{"intent_id": item} for item in ids],
                    "expected_orders": expected,
                    "actual_orders": actual,
                    "unexpected_attempts": len(missing),
                    "duplicate_attempts": duplicate_attempts,
                    "fills_complete": fills_complete and not missing,
                    "orders_attributable": attributable and not missing,
                    "derived_from_repository": True,
                },
                missing,
            )

        entry, entry_missing = section(entry_intent_ids, "entry")
        exit_section, exit_missing = section(exit_intent_ids, "exit")
        exit_section["current_exposure_inverse"] = inverse_exposure_proven
        exit_section["submitted_via"] = "Stage6PilotRunner->GenericOMS"
        started_candidates: list[datetime] = []
        completed_candidates: list[datetime] = []
        execution_provenance: dict[str, dict[str, dict[str, Any]]] = {
            "entry": {},
            "exit": {},
        }
        provenance_compatibilities: set[str] = set()
        provenance_dates: set[str] = set()
        provenance_run_ids: set[str] = set()
        provenance_missing: list[str] = []
        for intent_id in (*entry_intent_ids, *exit_intent_ids):
            intent = self.repository.get_intent(str(intent_id))
            if intent is None:
                provenance_missing.append(f"intent {intent_id} is not durable for execution provenance")
                continue
            metadata = intent.get("metadata")
            marker = metadata.get("stage6_submission") if isinstance(metadata, Mapping) else None
            label = "entry" if intent_id in entry_intent_ids else "exit"
            if not isinstance(marker, Mapping):
                provenance_missing.append(f"{label} intent {intent_id} lacks durable Stage 6 submission provenance")
            else:
                marker_value = {str(key): _stable_value(value) for key, value in marker.items()}
                execution_provenance[label][str(intent_id)] = marker_value
                compatibility = str(marker.get("execution_compatibility") or "").strip()
                if compatibility:
                    provenance_compatibilities.add(compatibility)
                marker_run_id = str(marker.get("run_id") or "").strip()
                if marker_run_id:
                    provenance_run_ids.add(marker_run_id)
                submitted_text = str(marker.get("submitted_at") or "").strip()
                if submitted_text:
                    try:
                        submitted = datetime.fromisoformat(submitted_text.replace("Z", "+00:00"))
                        if submitted.tzinfo is None or submitted.utcoffset() is None:
                            raise ValueError("timestamp has no timezone")
                        submitted = submitted.astimezone(timezone.utc)
                        started_candidates.append(submitted)
                        provenance_dates.add(submitted.astimezone(_US_EASTERN).date().isoformat())
                    except ValueError:
                        provenance_missing.append(
                            f"{label} intent {intent_id} has an invalid durable submission timestamp"
                        )
                else:
                    provenance_missing.append(f"{label} intent {intent_id} lacks durable submission timestamp")
            for field_name, collection in (("created_at", started_candidates), ("updated_at", completed_candidates)):
                value = intent.get(field_name)
                if value:
                    try:
                        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
                        if parsed.tzinfo is None:
                            parsed = parsed.replace(tzinfo=timezone.utc)
                        collection.append(parsed.astimezone(timezone.utc))
                    except ValueError:
                        pass
        started = min(started_candidates).isoformat() if started_candidates else self._now().isoformat()
        completed = max(completed_candidates).isoformat() if completed_candidates else started
        derived_trading_date = next(iter(provenance_dates)) if len(provenance_dates) == 1 else None
        if len(provenance_dates) > 1:
            provenance_missing.append(
                "durable Stage 6 submission timestamps span multiple US trading dates: "
                + ", ".join(sorted(provenance_dates))
            )
        derived_compatibility = next(iter(provenance_compatibilities)) if len(provenance_compatibilities) == 1 else None
        if len(provenance_compatibilities) > 1:
            provenance_missing.append("durable Stage 6 submissions use incompatible execution identities")
        evidence: dict[str, Any] = {
            "session_id": str(session_id),
            "us_trading_date": trading_date or derived_trading_date or started[:10],
            "derived_us_trading_date": derived_trading_date,
            "started_at": started,
            "completed_at": completed,
            "commit_sha": commit_sha,
            "execution_compatibility": execution_compatibility or derived_compatibility or STAGE6_EXECUTION_COMPATIBILITY,
            "derived_execution_compatibility": derived_compatibility,
            "account_id": spec.account.id,
            "environment": spec.account.environment.value,
            "execution_path": "Stage6PilotRunner->GenericOMS",
            "execution_mode": "SIM_SUBMIT",
            "supervised": True,
            "run_ids": sorted({str(spec.run_id), *provenance_run_ids}),
            "entry_intent_ids": [str(value) for value in entry_intent_ids],
            "exit_intent_ids": [str(value) for value in exit_intent_ids],
            "evidence_class": "DURABLE",
            "entry": entry,
            "exit": exit_section,
            "execution_provenance": execution_provenance,
            "provenance_complete": not provenance_missing
        }
        missing = entry_missing + exit_missing + missing_basis_reasons + provenance_missing
        if derived_trading_date is not None and trading_date is not None and str(trading_date) != derived_trading_date:
            missing.append(
                f"supplied US trading date {trading_date} disagrees with durable execution date {derived_trading_date}"
            )
        if execution_compatibility and derived_compatibility and execution_compatibility != derived_compatibility:
            missing.append(
                "supplied execution compatibility disagrees with durable submission provenance"
            )
        if preflight is not None:
            evidence["preflight"] = dict(preflight.get("preflight", preflight))
        else:
            missing.append("fresh preflight evidence artifact is required")
        if recovery is not None:
            evidence["restart_recovery"] = dict(recovery.get("restart_recovery", recovery))
        else:
            missing.append("fresh-process recovery evidence artifact is required")
        if final is not None:
            evidence["final"] = dict(final.get("final", final))
        else:
            missing.append("final-state evidence artifact is required")
        if missing:
            evidence["failure_reasons"] = list(dict.fromkeys(missing))
            evidence["evidence_builder_missing"] = list(dict.fromkeys(missing))
        return evidence

    def verify_durable_session_evidence(
        self,
        spec: Stage6PilotSpec,
        evidence: Mapping[str, Any],
    ) -> None:
        """Verify a candidate CLEAN_PASS against the durable repository graph.

        ``evaluate_stage6_session`` intentionally remains a pure document
        validator.  This boundary is the repository-backed companion used
        immediately before a clean result is persisted: it rebuilds the
        order/fill/provenance/inverse/date evidence from durable rows and
        binds the three required phase sections to the immutable observations
        retained for this session.  A caller may submit a structurally valid
        document, but it cannot turn fabricated IDs, fills, provenance, or
        phase observations into a clean persisted session.
        """

        if not isinstance(evidence, Mapping):
            raise ValueError("CLEAN_PASS durable repository verification requires an evidence object")
        session_id = str(evidence.get("session_id") or "").strip()
        if not session_id:
            raise ValueError("CLEAN_PASS durable repository verification requires session_id")

        observations = self.repository.stage6_validation_observations(session_id)
        expected_phases = {"PREFLIGHT", "RECOVERY", "FINAL"}
        observations_by_phase: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        observation_errors: list[str] = []
        for row in observations:
            row_session_id = str(row.get("session_id") or "").strip()
            row_account_id = str(row.get("account_id") or "").strip()
            phase = str(row.get("phase") or "").strip().upper()
            if row_session_id != session_id:
                observation_errors.append("retained observation has a different session identity")
            if row_account_id != spec.account.id:
                observation_errors.append(
                    f"retained {phase or 'unknown'} observation belongs to another account"
                )
            if phase not in expected_phases:
                observation_errors.append(f"retained observation has unsupported phase {phase!r}")
                continue
            if not isinstance(row.get("evidence"), Mapping):
                observation_errors.append(f"retained {phase} observation evidence is not an object")
                continue
            source = row["evidence"].get("observation_source")
            if not isinstance(source, Mapping):
                observation_errors.append(
                    f"retained {phase} observation lacks a runner source marker"
                )
            elif (
                str(source.get("kind", "")).strip().upper() != "STAGE6_RUNNER"
                or source.get("self_generated") is not True
                or source.get("broker_contacted") is not True
                or str(source.get("phase", "")).strip().upper() != phase
                or str(source.get("run_id", "")).strip() != spec.run_id
                or not str(source.get("invocation_id", "")).strip()
                or str(source.get("process_id", "")).strip()
                != str(row.get("process_id") or "").strip()
            ):
                observation_errors.append(
                    f"retained {phase} observation is not a self-generated connected runner observation"
                )
            observations_by_phase[phase].append(row)

        for phase in sorted(expected_phases):
            rows = observations_by_phase.get(phase, [])
            if len(rows) != 1:
                observation_errors.append(
                    f"session must retain exactly one {phase} observation; found {len(rows)}"
                )
        if observation_errors:
            raise ValueError(
                "CLEAN_PASS durable repository verification failed: "
                + "; ".join(dict.fromkeys(observation_errors))
            )

        phase_keys = {
            "PREFLIGHT": "preflight",
            "RECOVERY": "restart_recovery",
            "FINAL": "final",
        }
        phase_evidence: dict[str, Mapping[str, Any]] = {}
        for phase, key in phase_keys.items():
            row_evidence = observations_by_phase[phase][0]["evidence"]
            nested = row_evidence.get(key)
            if not isinstance(nested, Mapping):
                raise ValueError(
                    "CLEAN_PASS durable repository verification failed: "
                    f"{phase} observation lacks its {key} evidence"
                )
            phase_evidence[key] = nested

        entry_intent_ids = evidence.get("entry_intent_ids")
        exit_intent_ids = evidence.get("exit_intent_ids")
        if not isinstance(entry_intent_ids, (list, tuple)) or not isinstance(exit_intent_ids, (list, tuple)):
            raise ValueError(
                "CLEAN_PASS durable repository verification failed: intent identity lists are required"
            )
        canonical = self.build_session_evidence(
            spec,
            session_id=session_id,
            entry_intent_ids=tuple(str(value) for value in entry_intent_ids),
            exit_intent_ids=tuple(str(value) for value in exit_intent_ids),
            preflight=phase_evidence["preflight"],
            recovery=phase_evidence["restart_recovery"],
            final=phase_evidence["final"],
            commit_sha=str(evidence.get("commit_sha") or "UNKNOWN"),
            trading_date=str(evidence.get("us_trading_date") or "") or None,
            execution_compatibility=str(evidence.get("execution_compatibility") or "") or None,
        )
        missing = canonical.get("evidence_builder_missing")
        if isinstance(missing, list) and missing:
            raise ValueError(
                "CLEAN_PASS durable repository verification failed: "
                + "; ".join(str(item) for item in missing)
            )

        critical_fields = (
            "session_id",
            "us_trading_date",
            "derived_us_trading_date",
            "started_at",
            "completed_at",
            "execution_compatibility",
            "derived_execution_compatibility",
            "account_id",
            "environment",
            "execution_path",
            "execution_mode",
            "supervised",
            "run_ids",
            "entry_intent_ids",
            "exit_intent_ids",
            "evidence_class",
            "entry",
            "exit",
            "execution_provenance",
            "provenance_complete",
            "preflight",
            "restart_recovery",
            "final",
        )
        mismatches: list[str] = []
        for field_name in critical_fields:
            if _stable_value(evidence.get(field_name)) != _stable_value(canonical.get(field_name)):
                mismatches.append(field_name)
        if mismatches:
            raise ValueError(
                "CLEAN_PASS durable repository verification failed; evidence differs from canonical "
                "durable evidence in: "
                + ", ".join(mismatches)
            )
        if isinstance(evidence, dict):
            # This marker is written only after the canonical repository graph
            # and the retained phase observations have matched.  The
            # repository still requires its instance-scoped capability before
            # accepting a clean row, so an input document cannot self-authorize
            # this boundary.
            evidence["_stage6_durable_graph_verified"] = True

    @staticmethod
    def _parse_history_timestamp(value: object, *, field: str) -> datetime:
        text = str(value or "").strip()
        if not text:
            raise ValueError(f"legacy evidence has no {field}")
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"legacy evidence has an invalid {field}: {text!r}") from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _legacy_history_window(
        self,
        source_db_path: str,
        account_id: str,
        source_order_ids: Sequence[str],
    ) -> tuple[datetime, datetime]:
        """Derive a narrow provider-history window from source fill evidence."""

        identifiers = tuple(sorted({str(value).strip() for value in source_order_ids if str(value).strip()}))
        if not identifiers:
            raise ValueError("legacy evidence contains no source order IDs")
        placeholders = ",".join("?" for _ in identifiers)
        source_connection: sqlite3.Connection | None = None
        try:
            source_connection = sqlite3.connect(
                Path(source_db_path).expanduser().resolve().as_uri() + "?mode=ro",
                uri=True,
            )
            row = source_connection.execute(
                f"""SELECT MIN(f.filled_at) AS first_filled_at,
                                  MAX(f.filled_at) AS last_filled_at
                           FROM core_fills f
                           JOIN core_broker_orders b ON b.id = f.broker_order_id
                          WHERE b.account_id = ?
                            AND b.external_order_id IN ({placeholders})""",
                (account_id, *identifiers),
            ).fetchone()
        except sqlite3.Error as exc:
            raise ValueError(f"legacy evidence timestamps are unavailable: {exc}") from exc
        finally:
            if source_connection is not None:
                source_connection.close()
        if row is None or row[0] in (None, "") or row[1] in (None, ""):
            raise ValueError("legacy evidence has no complete fill timestamp coverage")
        first = self._parse_history_timestamp(row[0], field="first fill timestamp")
        last = self._parse_history_timestamp(row[1], field="last fill timestamp")
        if last < first:
            raise ValueError("legacy evidence fill timestamps are inverted")
        # Keep the window narrow enough to reject unrelated historical orders,
        # while allowing provider second-level timestamp rounding at either
        # boundary.
        return first - timedelta(minutes=5), last + timedelta(minutes=5)

    def _persisted_legacy_evidence(self, source_order_ids: Sequence[str]) -> dict[str, dict[str, Any]]:
        """Load the imported target claims used for exact history matching."""

        identifiers = tuple(sorted({str(value).strip() for value in source_order_ids if str(value).strip()}))
        if not identifiers:
            raise ValueError("no imported legacy order IDs are available")
        placeholders = ",".join("?" for _ in identifiers)
        with self.repository.transaction() as connection:
            order_rows = connection.execute(
                f"""SELECT b.external_order_id, l.instrument_id, l.side, l.quantity,
                                  f.quantity AS fill_quantity, f.price AS fill_price,
                                  f.filled_at AS fill_timestamp
                           FROM core_broker_orders b
                           JOIN core_order_legs l ON l.id = b.order_leg_id
                           LEFT JOIN core_fills f ON f.broker_order_id = b.id
                          WHERE b.external_order_id IN ({placeholders})
                          ORDER BY b.external_order_id, f.id""",
                identifiers,
            ).fetchall()
        evidence: dict[str, dict[str, Any]] = {}
        for row in order_rows:
            external_order_id = str(row["external_order_id"])
            if external_order_id in evidence:
                raise ValueError(f"imported legacy order {external_order_id} has duplicate fill evidence")
            if row["fill_quantity"] in (None, "") or row["fill_price"] in (None, ""):
                raise ValueError(f"imported legacy order {external_order_id} has incomplete fill evidence")
            evidence[external_order_id] = {
                "instrument_id": str(row["instrument_id"]),
                "side": str(row["side"]),
                "quantity": Decimal(str(row["quantity"])),
                "fill_quantity": Decimal(str(row["fill_quantity"])),
                "fill_price": Decimal(str(row["fill_price"])),
                "fill_timestamp": self._parse_history_timestamp(row["fill_timestamp"], field="fill timestamp"),
            }
        if set(evidence) != set(identifiers):
            raise ValueError(
                "imported legacy order claims do not exactly match source order IDs: "
                f"observed={sorted(evidence)}, expected={sorted(identifiers)}"
            )
        return evidence

    @staticmethod
    def _fill_evidence_identity(fill: Any) -> tuple[object, ...]:
        return (
            str(fill.external_order_id),
            str(fill.instrument_id or ""),
            Decimal(str(fill.quantity)),
            Decimal(str(fill.price)),
            fill.filled_at,
        )

    def _validate_historical_evidence(
        self,
        spec: Stage6PilotSpec,
        current_facts: BrokerFactSnapshot,
        history: BrokerHistoricalOrderFacts,
        expected: Mapping[str, Mapping[str, Any]],
        history_start: datetime,
        history_end: datetime,
    ) -> dict[str, Any]:
        """Require exact, complete, account-bound historical cumulative facts."""

        if not isinstance(history, BrokerHistoricalOrderFacts):
            raise ValueError("historical order provider returned an invalid fact window")
        if not history.complete or history.error:
            raise ValueError(f"historical order facts are incomplete: {history.error or 'unspecified'}")
        if history.account_id != spec.account.id:
            raise ValueError("historical order facts belong to a different account")
        if history.requested_start != history_start.astimezone(timezone.utc):
            raise ValueError("historical order facts returned a different requested start")
        if history.requested_end != history_end.astimezone(timezone.utc):
            raise ValueError("historical order facts returned a different requested end")
        if history.execution_evidence_mode is not ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS:
            raise ValueError("historical order facts are not cumulative-order evidence")

        expected_ids = set(expected)
        orders_by_id: dict[str, Any] = {}
        for order in history.orders:
            order_id = str(order.external_order_id)
            if order_id in orders_by_id:
                raise ValueError(f"historical order facts duplicate order {order_id}")
            orders_by_id[order_id] = order
        if set(orders_by_id) != expected_ids:
            raise ValueError(
                "historical order coverage does not exactly match imported legacy claims: "
                f"observed={sorted(orders_by_id)}, expected={sorted(expected_ids)}"
            )

        fills_by_id: dict[str, Any] = {}
        for fill in history.fills:
            order_id = str(fill.external_order_id)
            if order_id in fills_by_id:
                raise ValueError(f"historical fill facts duplicate order {order_id}")
            fills_by_id[order_id] = fill
        if set(fills_by_id) != expected_ids:
            raise ValueError(
                "historical cumulative fill coverage does not exactly match imported legacy claims: "
                f"observed={sorted(fills_by_id)}, expected={sorted(expected_ids)}"
            )

        for order_id, target in expected.items():
            order = orders_by_id[order_id]
            fill = fills_by_id[order_id]
            if order.account_id != spec.account.id or fill.account_id not in (None, spec.account.id):
                raise ValueError(f"historical evidence account identity mismatch for {order_id}")
            if order.status.value != "FILLED":
                raise ValueError(f"historical order {order_id} is not terminal FILLED")
            if order.instrument_id != target["instrument_id"] or fill.instrument_id != target["instrument_id"]:
                raise ValueError(f"historical evidence instrument mismatch for {order_id}")
            if order.side.value != target["side"]:
                raise ValueError(f"historical order side mismatch for {order_id}")
            if order.quantity != target["quantity"] or order.filled_quantity != target["fill_quantity"]:
                raise ValueError(f"historical order quantity mismatch for {order_id}")
            if fill.quantity != target["fill_quantity"] or fill.price != target["fill_price"]:
                raise ValueError(f"historical fill economics mismatch for {order_id}")
            if fill.filled_at != target["fill_timestamp"]:
                raise ValueError(f"historical fill timestamp mismatch for {order_id}")

        # A current-order query may overlap the bounded history window.  Keep
        # exact duplicates idempotent, but never allow a conflicting provider
        # view or a new unmatched current fill to disappear.
        current_by_id: dict[str, Any] = {}
        for fill in current_facts.fills:
            order_id = str(fill.external_order_id)
            if order_id in current_by_id:
                raise ValueError(f"current account facts duplicate fill order {order_id}")
            current_by_id[order_id] = fill
        unexpected_current_ids = set(current_by_id) - expected_ids
        closed_historical_current_ids: list[str] = []
        for order_id in sorted(unexpected_current_ids):
            if not self.oms.is_proof_backed_closed_broker_fill(
                account=spec.account,
                broker_fill=current_by_id[order_id],
            ):
                raise ValueError(
                    "current account facts contain unmatched fill evidence: "
                    + ", ".join(sorted(unexpected_current_ids))
                )
            closed_historical_current_ids.append(order_id)
        if unexpected_current_ids and not closed_historical_current_ids:
            raise ValueError(
                "current account facts contain unmatched fill evidence: "
                + ", ".join(sorted(unexpected_current_ids))
            )
        for order_id, current_fill in current_by_id.items():
            # A current account query may also expose a proof-backed closed
            # historical fill whose claim is intentionally outside the
            # imported legacy source window.  It was validated above through
            # the same durable ownership predicate used by the OMS gate; it
            # has no source-row counterpart to compare here.
            if order_id not in fills_by_id:
                continue
            if self._fill_evidence_identity(current_fill) != self._fill_evidence_identity(fills_by_id[order_id]):
                raise ValueError(f"current/history fill evidence conflicts for {order_id}")

        return {
            "requested_start": history_start.astimezone(timezone.utc).isoformat(),
            "requested_end": history_end.astimezone(timezone.utc).isoformat(),
            "history_captured_at": history.captured_at.isoformat(),
            "history_row_count": len(history.orders),
            "history_fill_count": len(history.fills),
            "history_execution_evidence_scope": sorted(history.execution_evidence_scope),
            "current_fill_count": len(current_facts.fills),
            "closed_historical_current_fill_order_ids": closed_historical_current_ids,
        }

    def establish_verified_legacy_baseline(
        self,
        spec: Stage6PilotSpec,
        source_db_path: str,
        *,
        legacy_label: str = "legacy-smoke",
    ) -> dict[str, Any]:
        """Verify fresh flat SIM facts and import the bounded legacy ledger.

        This is an explicit read-only-provider plus local-audit operation.  It
        never builds or submits an order and it refuses to create a baseline
        from configuration assumptions or from an unsupported/partial fact
        set.
        """
        if not isinstance(spec, Stage6PilotSpec):
            raise ValueError("baseline establishment requires a Stage6PilotSpec")
        if spec.account.environment is not TradingEnvironment.SIM:
            raise ValueError("execution-evidence baselines are SIM-only")
        if spec.execution_policy.execution_session is not ExecutionSession.REGULAR:
            raise ValueError("execution-evidence baseline requires regular RTH policy")
        self.repository.initialize()
        spec.validate_repository(self.repository)
        try:
            facts, normalized_positions, normalized_open_orders = self.oms.strict_authoritative_account_facts(
                spec.account,
                max_age_seconds=BROKER_FACT_MAX_AGE_SECONDS,
            )
        except Exception as exc:
            raise ValueError(f"fresh SIM account facts are unavailable: {exc}") from exc
        if facts.execution_evidence_mode is not ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS:
            raise ValueError("the bounded baseline path requires cumulative order snapshot evidence")
        if normalized_open_orders:
            raise ValueError("cannot establish a flat baseline while broker open orders are present")
        if any(position.signed_quantity != 0 for position in normalized_positions):
            raise ValueError("cannot establish a flat baseline while broker positions are nonzero")
        configured_instrument_ids = {
            instrument_id
            for sleeve in spec.sleeves
            for instrument_id in sleeve.instrument_ids
        }
        configured_mapping: dict[str, str] = {}
        with self.repository.transaction() as connection:
            mapping_rows = connection.execute(
                """SELECT instrument_id, external_symbol
                     FROM core_instrument_mappings
                    WHERE provider = 'moomoo' AND purpose = 'BROKER'"""
            ).fetchall()
        for row in mapping_rows:
            instrument_id = str(row["instrument_id"])
            if instrument_id not in configured_instrument_ids:
                continue
            source_symbol = str(row["external_symbol"]).strip().upper().split(".")[-1]
            if source_symbol in configured_mapping and configured_mapping[source_symbol] != instrument_id:
                raise ValueError(f"configured Moomoo symbol mapping is ambiguous: {source_symbol}")
            configured_mapping[source_symbol] = instrument_id
        if not configured_mapping:
            raise ValueError("configured Stage 6 Moomoo instrument mappings are missing")
        try:
            source_connection = sqlite3.connect(
                Path(source_db_path).expanduser().resolve().as_uri() + "?mode=ro",
                uri=True,
            )
            source_rows = source_connection.execute(
                """SELECT DISTINCT ins.symbol
                     FROM core_broker_orders b
                     JOIN core_order_legs l ON l.id = b.order_leg_id
                     JOIN core_instruments ins ON ins.id = l.instrument_id
                    WHERE b.account_id = ? AND b.external_order_id IS NOT NULL""",
                (spec.account.id,),
            ).fetchall()
        except sqlite3.Error as exc:
            raise ValueError(f"legacy evidence source symbols are unavailable: {exc}") from exc
        finally:
            try:
                source_connection.close()
            except UnboundLocalError:
                pass
        source_symbols = {str(row[0]).strip().upper().split(".")[-1] for row in source_rows}
        configured_mapping = {
            symbol: instrument_id
            for symbol, instrument_id in configured_mapping.items()
            if symbol in source_symbols
        }
        if not source_symbols or set(configured_mapping) != source_symbols:
            raise ValueError("configured Stage 6 mappings do not exactly cover legacy source symbols")
        imported = self.repository.import_legacy_order_evidence(
            source_db_path,
            spec.account.id,
            legacy_label=legacy_label,
            instrument_mapping=configured_mapping,
        )
        expected_orders = set(imported["source_order_ids"])
        history_start, history_end = self._legacy_history_window(
            source_db_path,
            spec.account.id,
            tuple(sorted(expected_orders)),
        )
        try:
            history = self.oms.strict_historical_order_facts(
                spec.account,
                requested_start=history_start,
                requested_end=history_end,
            )
        except Exception as exc:
            raise ValueError(f"bounded historical-order evidence is unavailable: {exc}") from exc
        expected_evidence = self._persisted_legacy_evidence(tuple(sorted(expected_orders)))
        history_metadata = self._validate_historical_evidence(
            spec,
            facts,
            history,
            expected_evidence,
            history_start,
            history_end,
        )
        # Re-running the explicit baseline check is intentionally read-only
        # once the same source ledger is already checkpointed.  The fresh
        # account/history validation above is still required; only the
        # durable checkpoint write is reused.  This avoids treating a newer
        # observation timestamp or newly recognized, proof-backed closed
        # history rows as contradictory evidence for the same immutable
        # source ledger.
        existing_baseline = self.repository.latest_execution_evidence_baseline(spec.account.id)
        if (
            existing_baseline is not None
            and existing_baseline.source_ledger_fingerprint == str(imported["source_ledger_fingerprint"])
            and set(existing_baseline.source_order_ids) == expected_orders
            and existing_baseline.verified_flat
            and existing_baseline.status == "VERIFIED"
        ):
            return {
                "baseline_id": existing_baseline.id,
                "account_id": existing_baseline.account_id,
                "captured_at": existing_baseline.captured_at.isoformat(),
                "evidence_mode": existing_baseline.evidence_mode.value,
                "coverage": list(existing_baseline.coverage),
                "source_ledger_fingerprint": existing_baseline.source_ledger_fingerprint,
                "source_order_ids": list(existing_baseline.source_order_ids),
                "legacy_strategy_id": imported["legacy_strategy_id"],
                "legacy_book_id": imported["legacy_book_id"],
                "verified_flat": True,
                "historical_order_query": history_metadata,
                "orders_sent": 0,
                "reused_existing_checkpoint": True,
            }
        position_fingerprint, open_order_fingerprint = self.oms.execution_fact_fingerprints(facts)
        baseline = ExecutionEvidenceBaseline(
            id=f"stage6-baseline:{spec.account.id}:{imported['source_ledger_fingerprint'][:24]}",
            account_id=spec.account.id,
            captured_at=facts.captured_at,
            evidence_mode=ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS,
            coverage=tuple(
                sorted(
                    set(facts.execution_evidence_scope)
                    | set(history.execution_evidence_scope)
                    | {"SOURCE_LEDGER"}
                )
            ),
            source_ledger_fingerprint=str(imported["source_ledger_fingerprint"]),
            source_order_ids=tuple(sorted(expected_orders)),
            position_fingerprint=position_fingerprint,
            open_order_fingerprint=open_order_fingerprint,
            verified_flat=True,
            status="VERIFIED",
            metadata={
                "source_database": imported["source_database"],
                "legacy_strategy_id": imported["legacy_strategy_id"],
                "legacy_book_id": imported["legacy_book_id"],
                "provider_captured_at": facts.captured_at.isoformat(),
                "provider_account_id": spec.account.id,
                "no_deal_history": True,
                "historical_order_query": history_metadata,
            },
        )
        self.repository.save_execution_evidence_baseline(baseline)
        return {
            "baseline_id": baseline.id,
            "account_id": baseline.account_id,
            "captured_at": baseline.captured_at.isoformat(),
            "evidence_mode": baseline.evidence_mode.value,
            "coverage": list(baseline.coverage),
            "source_ledger_fingerprint": baseline.source_ledger_fingerprint,
            "source_order_ids": list(baseline.source_order_ids),
            "legacy_strategy_id": imported["legacy_strategy_id"],
            "legacy_book_id": imported["legacy_book_id"],
            "verified_flat": baseline.verified_flat,
            "historical_order_query": history_metadata,
            "orders_sent": 0,
        }

    def _existing_status(
        self,
        spec: Stage6PilotSpec,
        intent_id: str,
    ) -> Mapping[str, Any]:
        try:
            return self.oms.recovery_status(intent_id, account=spec.account)
        except Exception as exc:
            return {"intent_id": intent_id, "status": "STATUS_ERROR", "error": str(exc)}

    def recover(self, account: Account) -> list[dict[str, Any]]:
        """Explicit read-only restart/recovery hook for an armed SIM pilot."""

        if account.environment is not TradingEnvironment.SIM:
            raise ValueError("Stage 6 recovery accepts SIM accounts only")
        self.repository.initialize()
        return self.oms.poll_and_recover(account=account)

    def run(
        self,
        spec: Stage6PilotSpec,
        *,
        mode: Stage6RunMode = Stage6RunMode.DRY_RUN,
        market_symbols: Sequence[str] = (),
    ) -> Stage6PilotReport:
        # Repeat the public boundary check immediately before any repository
        # initialization or allocation mutation.  This remains fail-closed if
        # an in-process caller supplies a forged/mutated spec object.
        if not isinstance(spec, Stage6PilotSpec):
            raise ValueError("Stage 6 runner requires a Stage6PilotSpec")
        if spec.account.environment is not TradingEnvironment.SIM:
            raise ValueError("Stage 6 runner accepts SIM accounts only")
        if (
            spec.execution_policy.execution_session is not ExecutionSession.REGULAR
            or spec.execution_policy.allow_extended_hours
        ):
            raise ValueError("Stage 6 runner requires regular RTH execution with no extended hours")
        try:
            selected_mode = mode if isinstance(mode, Stage6RunMode) else Stage6RunMode(mode)
        except (TypeError, ValueError) as exc:
            raise ValueError("Stage 6 mode must be DRY_RUN or SIM_SUBMIT; LIVE is unavailable") from exc
        started = self._now()
        allocation_ids: tuple[str, ...] = ()
        stop_reasons: list[str] = []
        before_status: list[Mapping[str, Any]] = []
        after_status: list[Mapping[str, Any]] = []
        intent_results: list[Mapping[str, Any]] = []
        broker_facts: dict[str, Any] = {}
        market_state: dict[str, Any] = {}
        broker_preflight_passed = False
        submission_gate: Callable[[Any, Any, Account], None] | None = None

        self.repository.initialize()
        try:
            spec.validate_repository(self.repository)
            allocation_ids = tuple(
                Stage5AllocationCoordinator(self.repository, spec.sleeves).apply(spec.allocation_update)
            )
        except Exception as exc:
            stop_reasons.append(f"configuration/allocation validation failed: {exc}")

        if not stop_reasons:
            try:
                account_book_status = self.oms.book_risk_status(account=spec.account)
                before_status.append({"scope": "account", "book_risk_status": account_book_status})
                blocked_books = [
                    book_id
                    for book_id, value in account_book_status.get("books", {}).items()
                    if isinstance(value, Mapping) and value.get("status") == "BLOCKED"
                ]
                if blocked_books:
                    stop_reasons.append(
                        "book risk status is blocked for: " + ", ".join(sorted(blocked_books))
                    )
            except Exception as exc:
                stop_reasons.append(f"book risk status unavailable: {exc}")

        intents: list[tuple[PairSleeve, Any]] = []
        if not stop_reasons:
            for sleeve, target in zip(spec.sleeves, spec.targets, strict=True):
                intent = sleeve.to_intent(target)
                intent = replace(
                    intent,
                    execution_policy=spec.execution_policy,
                    metadata={
                        **dict(intent.metadata),
                        "stage6_pilot": {
                            "run_id": spec.run_id,
                            "mode": selected_mode.value,
                            "allocation_version": spec.allocation_update.version,
                        },
                    },
                )
                intents.append((sleeve, intent))

            for sleeve, intent in intents:
                existing = self.repository.get_intent_by_idempotency_key(
                    spec.account.id,
                    intent.idempotency_key,
                )
                if existing is None:
                    before_status.append(
                        {
                            "intent_id": intent.id,
                            "status": "NOT_PERSISTED",
                            "sleeve_id": sleeve.sleeve_id,
                        }
                    )
                else:
                    status = self._existing_status(spec, str(existing["id"]))
                    before_status.append(status)
                    if status.get("status") == "STATUS_ERROR":
                        stop_reasons.append(
                            f"recovery status unavailable for existing intent {existing['id']}"
                        )

            stop_reasons.extend(self._local_blockers(spec))
            if selected_mode is Stage6RunMode.SIM_SUBMIT and not stop_reasons:
                broker_facts, fresh_reasons = self._fresh_broker_facts(spec)
                stop_reasons.extend(fresh_reasons)
                # This is intentionally performed in the same invocation,
                # immediately before the first possible submit.  A prior
                # broker-preflight/session artifact is never an execution
                # safety substitute.
                market_state, market_reasons = self._fresh_market_state(
                    spec,
                    tuple(market_symbols) or self._configured_symbols(spec),
                    require_rth=True,
                )
                stop_reasons.extend(market_reasons)
                broker_preflight_passed = not fresh_reasons and not market_reasons
                if broker_preflight_passed:
                    submission_gate = self._submission_safety_gate(
                        spec,
                        tuple(market_symbols) or self._configured_symbols(spec),
                    )
            elif selected_mode is Stage6RunMode.DRY_RUN:
                broker_facts = {"queried": False, "reason": "dry-run does not contact the broker"}

        plans = tuple(self._plan(intent, sleeve, spec.run_id) for sleeve, intent in intents)
        if stop_reasons or selected_mode is Stage6RunMode.DRY_RUN:
            try:
                after_status.append(
                    {
                        "scope": "account",
                        "book_risk_status": self.oms.book_risk_status(account=spec.account),
                    }
                )
            except Exception as exc:
                after_status.append({"scope": "account", "status": "STATUS_ERROR", "error": str(exc)})
            if selected_mode is Stage6RunMode.DRY_RUN and not stop_reasons:
                intent_results = [
                    {
                        "intent_id": intent.id,
                        "sleeve_id": sleeve.sleeve_id,
                        "status": "PLANNED_NOT_SUBMITTED",
                        "submitted": False,
                        "run_id": spec.run_id,
                    }
                    for sleeve, intent in intents
                ]
            return Stage6PilotReport(
                run_id=spec.run_id,
                mode=selected_mode,
                started_at=started,
                completed_at=self._now(),
                allocation_ids=allocation_ids,
                preflight_passed=not stop_reasons,
                broker_preflight_passed=broker_preflight_passed,
                stop_reasons=tuple(stop_reasons),
                intent_plans=plans,
                intent_results=tuple(intent_results),
                before_status=tuple(before_status),
                after_status=tuple(after_status),
                broker_facts=broker_facts,
                market_state=market_state,
            )

        for sequence, (sleeve, intent) in enumerate(intents):
            existing = self.repository.get_intent_by_idempotency_key(
                spec.account.id,
                intent.idempotency_key,
            )
            if existing is not None and str(existing.get("status")) not in self._DISPATCHABLE_EXISTING:
                intent_results.append(
                    {
                        "intent_id": str(existing["id"]),
                        "sleeve_id": sleeve.sleeve_id,
                        "status": str(existing["status"]),
                        "submitted": False,
                        "deduplicated": True,
                        "run_id": spec.run_id,
                    }
                )
                continue
            risk = RiskDecisionRecord(
                id=f"stage6-risk-{spec.run_id}-{sequence}",
                intent_id=intent.id,
                approved=True,
                reason="explicit Stage 6 SIM pilot arm",
                checks={"stage6_run_id": spec.run_id, "mode": selected_mode.value},
                evaluated_at=started,
                metadata={"pilot_run_id": spec.run_id},
            )
            try:
                self._last_submission_gate_outcome = None
                result = self.oms.submit_intent(
                    intent,
                    account=spec.account,
                    risk_decision=risk,
                    _before_submit_leg=submission_gate,
                )
                status = str(result.get("status", "UNKNOWN"))
                wait_outcome = "FULL" if self._is_verified_full_fill(result) else status
                final_result: Mapping[str, Any] = result
                if status in {
                    IntentStatus.WORKING.value,
                    IntentStatus.SUBMITTING.value,
                    IntentStatus.RISK_APPROVED.value,
                    IntentStatus.PARTIALLY_FILLED.value,
                }:
                    final_result, wait_outcome = self._wait_for_verified_full_fill(
                        intent_id=intent.id,
                        account=spec.account,
                    )
                    status = str(final_result.get("status", wait_outcome))
                intent_results.append(
                    {
                        "intent_id": intent.id,
                        "sleeve_id": sleeve.sleeve_id,
                        "status": status,
                        "initial_status": str(result.get("status", "UNKNOWN")),
                        "dispatch_outcome": wait_outcome,
                        "submitted": status in self._SUCCESSFUL_SUBMIT,
                        "run_id": spec.run_id,
                    }
                )
                # A working ACK is not a safe hand-off to the next sleeve.
                # Only a verified full fill may expose the account to the
                # next sequential dispatch.  Partial, rejected, unknown, and
                # timeout outcomes are deliberately terminal for this run;
                # any known exposure is left for the existing compensating
                # exit/reconciliation path.
                if wait_outcome != "FULL":
                    reported_outcome = self._last_submission_gate_outcome or wait_outcome
                    stop_reasons.append(
                        f"dispatch stopped after {sleeve.sleeve_id} returned {reported_outcome}"
                    )
                    break
            except Exception as exc:
                stop_reasons.append(f"dispatch stopped for {sleeve.sleeve_id}: {exc}")
                intent_results.append(
                    {
                        "intent_id": intent.id,
                        "sleeve_id": sleeve.sleeve_id,
                        "status": "DISPATCH_ERROR",
                        "submitted": False,
                        "error": str(exc),
                        "run_id": spec.run_id,
                    }
                )
                break

        for _sleeve, intent in intents:
            persisted = self.repository.get_intent_by_idempotency_key(
                spec.account.id,
                intent.idempotency_key,
            )
            if persisted is not None:
                after_status.append(self._existing_status(spec, str(persisted["id"])))
        try:
            after_status.append(
                {
                    "scope": "account",
                    "book_risk_status": self.oms.book_risk_status(account=spec.account),
                }
            )
        except Exception as exc:
            after_status.append({"scope": "account", "status": "STATUS_ERROR", "error": str(exc)})
        if not after_status and not stop_reasons:
            stop_reasons.append("SIM dispatch produced no durable intent status")
        return Stage6PilotReport(
            run_id=spec.run_id,
            mode=selected_mode,
            started_at=started,
            completed_at=self._now(),
            allocation_ids=allocation_ids,
            preflight_passed=not stop_reasons,
            broker_preflight_passed=broker_preflight_passed,
            stop_reasons=tuple(stop_reasons),
            intent_plans=plans,
            intent_results=tuple(intent_results),
            before_status=tuple(before_status),
            after_status=tuple(after_status),
            broker_facts=broker_facts,
            market_state=market_state,
        )


__all__ = [
    "STAGE6_EXECUTION_COMPATIBILITY",
    "Stage6PilotReport",
    "Stage6PilotRunner",
    "Stage6PilotSpec",
    "Stage6RunMode",
]
