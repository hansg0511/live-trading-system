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
from pathlib import Path
import sqlite3
import time
from typing import Any, Callable

from src.trading_core.domain import (
    Account,
    ExecutionEvidenceBaseline,
    ExecutionEvidenceMode,
    ExecutionPolicy,
    ExecutionSession,
    IntentAction,
    IntentStatus,
    OwnershipClass,
    PositionSnapshot,
    RiskDecisionRecord,
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

    def _now(self) -> datetime:
        value = self.clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Stage 6 clock must return a timezone-aware datetime")
        return value.astimezone(timezone.utc)

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
            }:
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
            }:
                return last, status
            remaining = max(0.0, (deadline - self._now()).total_seconds())
            if remaining <= 0:
                return last, "TIMEOUT"
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
    ) -> tuple[BrokerFactSnapshot | None, list[str]]:
        """Read and validate one fresh account-scoped fact set.

        This helper deliberately has no repository writes and no broker command
        capability.  ``run(..., SIM_SUBMIT)`` continues to use the same gate;
        the broker-preflight command uses it before constructing any intents.
        """
        getter = getattr(self.oms.adapter, "get_authoritative_account_facts", None)
        if not callable(getter):
            return None, ["adapter lacks authoritative fresh account-facts capability"]
        try:
            facts = getter(spec.account)
        except Exception as exc:
            return None, [f"authoritative broker facts unavailable: {exc}"]
        if not isinstance(facts, BrokerFactSnapshot):
            return None, ["authoritative broker facts returned an invalid snapshot"]
        reasons: list[str] = []
        if facts.account_id != spec.account.id:
            reasons.append("authoritative broker facts belong to a different account")
        if not facts.complete:
            reasons.append(f"authoritative broker facts are incomplete: {facts.error or 'unspecified'}")
        if facts.error:
            reasons.append(f"authoritative broker facts report an error: {facts.error}")
        if facts.open_orders:
            reasons.append("unsafe outstanding broker orders are present")
        if facts.execution_evidence_mode is ExecutionEvidenceMode.UNAVAILABLE:
            reasons.append("execution evidence is unavailable; no SIM submission is safe")
        elif (
            facts.execution_evidence_mode is ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS
            and self.repository.latest_execution_evidence_baseline(spec.account.id) is None
        ):
            reasons.append("verified bounded cumulative-order baseline is required before SIM submission")

        expected: dict[str, Decimal] = defaultdict(Decimal)
        closed_historical_intents = self.oms._closed_historical_intent_ids(spec.account)
        for row in self.repository.position_allocations(spec.account.id):
            if (
                str(row.get("ownership_class", "")).upper() == OwnershipClass.MANAGED.value
                and str(row.get("book_id", "")) in set(spec.book_ids)
                and str(row.get("source_intent_id") or "") not in closed_historical_intents
            ):
                expected[str(row["instrument_id"])] += Decimal(str(row["signed_quantity"]))
        observed: dict[str, Decimal] = defaultdict(Decimal)
        for position in facts.positions:
            if not isinstance(position, PositionSnapshot):
                reasons.append("authoritative positions contain an invalid row")
                continue
            observed[position.instrument_id] += position.signed_quantity
        for instrument_id in set(expected) | set(observed):
            if expected[instrument_id] != observed[instrument_id]:
                reasons.append(
                    "broker position does not match the durable managed allocation for "
                    f"{instrument_id}: expected {expected[instrument_id]}, observed {observed[instrument_id]}"
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
            elif state not in rth_states:
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
        report["rth"] = {
            "observed": not reasons,
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
        getter = getattr(self.oms.adapter, "get_authoritative_account_facts", None)
        if not callable(getter):
            raise ValueError("adapter lacks authoritative fresh account-facts capability")
        facts = getter(spec.account)
        if not isinstance(facts, BrokerFactSnapshot):
            raise ValueError("adapter returned an invalid authoritative account-facts snapshot")
        if not facts.complete or facts.error:
            raise ValueError(f"fresh SIM account facts are incomplete: {facts.error or 'unspecified'}")
        if facts.account_id != spec.account.id:
            raise ValueError("fresh SIM account facts belong to a different account")
        if facts.execution_evidence_mode is not ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS:
            raise ValueError("the bounded baseline path requires cumulative order snapshot evidence")
        if facts.open_orders:
            raise ValueError("cannot establish a flat baseline while broker open orders are present")
        if any(position.signed_quantity != 0 for position in facts.positions):
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
        history_provider = getattr(self.oms.adapter, "get_historical_order_facts", None)
        if not callable(history_provider):
            raise ValueError("adapter lacks bounded historical-order evidence capability")
        history_start, history_end = self._legacy_history_window(
            source_db_path,
            spec.account.id,
            tuple(sorted(expected_orders)),
        )
        history = history_provider(spec.account, history_start, history_end)
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
        broker_preflight_passed = False

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
                broker_preflight_passed = not fresh_reasons
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
                result = self.oms.submit_intent(intent, account=spec.account, risk_decision=risk)
                status = str(result.get("status", "UNKNOWN"))
                wait_outcome = "FULL" if self._is_verified_full_fill(result) else status
                final_result: Mapping[str, Any] = result
                if status in {
                    IntentStatus.WORKING.value,
                    IntentStatus.SUBMITTING.value,
                    IntentStatus.RISK_APPROVED.value,
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
                    stop_reasons.append(
                        f"dispatch stopped after {sleeve.sleeve_id} returned {wait_outcome}"
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
        )


__all__ = [
    "Stage6PilotReport",
    "Stage6PilotRunner",
    "Stage6PilotSpec",
    "Stage6RunMode",
]
