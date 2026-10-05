"""Stage 5 generic stat-arb sleeve contracts.

This module deliberately stops at the strategy boundary.  A sleeve owns pair
configuration and converts a normalized signed target into a generic
``OrderIntent``; the trading core owns persistence, book attribution, risk,
and broker execution.  No market-data provider or legacy strategy is used.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
import hashlib
from typing import Any

from src.trading_core.domain import (
    BookAllocation,
    ExecutionPolicy,
    FailurePolicy,
    IntentAction,
    IntentStatus,
    LegStatus,
    LeggingPolicy,
    OrderIntent,
    OrderLeg,
    PartialFillPolicy,
    QuantityUnit,
    Side,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _required_id(value: object, name: str) -> str:
    result = str(value).strip() if value is not None else ""
    if not result:
        raise ValueError(f"{name} is required")
    return result


def _decimal(value: object, name: str) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a finite Decimal")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a finite Decimal") from exc
    if not result.is_finite():
        raise ValueError(f"{name} must be finite")
    return result


def _nonnegative(value: object, name: str) -> Decimal:
    result = _decimal(value, name)
    if result < 0:
        raise ValueError(f"{name} must be non-negative")
    return result


def _timestamp(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _mapping(value: Mapping[str, Any] | None, name: str) -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be a mapping")
    return dict(value)


class AllocationUnit(str, Enum):
    """Capacity bases accepted by the Stage 5 allocator."""

    FRACTION = "FRACTION"
    CAPITAL_AMOUNT = "CAPITAL_AMOUNT"
    RISK_BUDGET = "RISK_BUDGET"


@dataclass(frozen=True, slots=True)
class PairSleeve:
    """Stable configuration for one independent pair sleeve.

    ``book_id`` is the only ownership binding.  Pair symbols, instrument IDs,
    and strategy parameters remain strategy-owned configuration and are not
    added to generic OMS/database records beyond intent metadata.
    """

    sleeve_id: str
    strategy_id: str
    account_id: str
    book_id: str
    name: str
    instrument_ids: tuple[str, str]
    symbols: tuple[str, str]
    configuration: Mapping[str, Any] = field(default_factory=dict)
    version: str = "1"
    enabled: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "sleeve_id", _required_id(self.sleeve_id, "sleeve_id"))
        object.__setattr__(self, "strategy_id", _required_id(self.strategy_id, "strategy_id"))
        object.__setattr__(self, "account_id", _required_id(self.account_id, "account_id"))
        object.__setattr__(self, "book_id", _required_id(self.book_id, "book_id"))
        object.__setattr__(self, "name", _required_id(self.name, "name"))
        object.__setattr__(self, "version", _required_id(self.version, "version"))
        if len(tuple(self.instrument_ids)) != 2:
            raise ValueError("a pair sleeve requires exactly two instrument IDs")
        if len(tuple(self.symbols)) != 2:
            raise ValueError("a pair sleeve requires exactly two symbols")
        instrument_ids = tuple(_required_id(value, "instrument_id") for value in self.instrument_ids)
        symbols = tuple(_required_id(value, "symbol") for value in self.symbols)
        if len(set(instrument_ids)) != 2 or len(set(symbol.upper() for symbol in symbols)) != 2:
            raise ValueError("pair sleeve legs must be distinct")
        object.__setattr__(self, "instrument_ids", instrument_ids)
        object.__setattr__(self, "symbols", symbols)
        object.__setattr__(self, "configuration", _mapping(self.configuration, "configuration"))
        if not isinstance(self.enabled, bool):
            raise ValueError("enabled must be a bool")

    def validate_persistence(self, repository: Any) -> None:
        """Require the account, strategy, and exactly one enabled book."""
        account = repository.get_account(self.account_id)
        if account is None or not account.enabled:
            raise ValueError(f"sleeve requires an enabled account: {self.account_id}")
        strategy = repository.get_strategy(self.strategy_id)
        if strategy is None or not strategy.enabled:
            raise ValueError(f"sleeve requires an enabled strategy: {self.strategy_id}")
        book = repository.get_book(self.book_id)
        if book is None or not bool(book.get("enabled")):
            raise ValueError(f"sleeve requires one enabled declared book: {self.book_id}")

    def to_intent(self, target: "NormalizedPairTarget") -> OrderIntent:
        """Translate a normalized pair exposure target into two generic legs.

        For ``ENTER``, signed quantities are desired entry exposure.  For
        ``EXIT``, signed quantities represent the exposure being closed and
        are reversed into the closing orders.  The cycle ID is stable across
        repeated signal evaluations, so the repository's existing idempotency
        contract deduplicates exact repeats while a later cycle creates a new
        intent.
        """
        if not self.enabled:
            raise ValueError(f"sleeve is disabled: {self.sleeve_id}")
        if target.sleeve_id != self.sleeve_id:
            raise ValueError("target sleeve does not match the configured sleeve")
        if tuple(target.instrument_ids) != tuple(self.instrument_ids):
            raise ValueError("target instruments do not match the configured pair")
        action = target.action
        if action not in {IntentAction.ENTER, IntentAction.EXIT}:
            raise ValueError("pair sleeves support only ENTER and EXIT targets")
        multiplier = Decimal("-1") if action is IntentAction.EXIT else Decimal("1")
        intent_key = f"stage5|{self.sleeve_id}|{target.cycle_id}|{action.value}"
        intent_id = "stage5-intent-" + hashlib.sha256(intent_key.encode("utf-8")).hexdigest()[:24]
        now = target.evaluated_at
        legs: list[OrderLeg] = []
        effective_quantities: list[Decimal] = []
        for sequence, (instrument_id, signed_quantity) in enumerate(
            zip(self.instrument_ids, target.signed_quantities, strict=True)
        ):
            effective = signed_quantity * multiplier
            if effective == 0:
                raise ValueError("pair target quantities must be non-zero")
            effective_quantities.append(effective)
            legs.append(
                OrderLeg(
                    id=f"{intent_id}-leg-{sequence}",
                    intent_id=intent_id,
                    sequence=sequence,
                    instrument_id=instrument_id,
                    side=Side.BUY if effective > 0 else Side.SELL,
                    quantity=abs(effective),
                    quantity_unit=QuantityUnit.UNITS,
                    order_type="MARKET",
                    status=LegStatus.PLANNED,
                    metadata={
                        "stage5_sleeve_id": self.sleeve_id,
                        "pair_symbol": self.symbols[sequence],
                        "target_signed_quantity": str(signed_quantity),
                        "effective_signed_quantity": str(effective),
                    },
                    created_at=now,
                    updated_at=now,
                )
            )
        metadata = {
            "stage5": {
                "sleeve_id": self.sleeve_id,
                "sleeve_version": self.version,
                "cycle_id": target.cycle_id,
                "signal_id": target.signal_id,
                "pair_symbols": list(self.symbols),
                "target_signed_quantities": [str(value) for value in target.signed_quantities],
                "effective_signed_quantities": [str(value) for value in effective_quantities],
                "configuration": dict(self.configuration),
                "provenance": dict(target.provenance),
            }
        }
        return OrderIntent(
            id=intent_id,
            idempotency_key=intent_key,
            strategy_id=self.strategy_id,
            account_id=self.account_id,
            book_id=self.book_id,
            action=action,
            status=IntentStatus.CREATED,
            source_signal_id=target.signal_id,
            execution_policy=ExecutionPolicy(
                legging_policy=LeggingPolicy.SEQUENTIAL,
                partial_fill_policy=PartialFillPolicy.WAIT,
                failure_policy=FailurePolicy.HOLD_AND_RECONCILE,
            ),
            legs=tuple(legs),
            metadata=metadata,
            created_at=now,
            updated_at=now,
        )


@dataclass(frozen=True, slots=True)
class NormalizedPairTarget:
    """Provider-neutral desired signed exposure supplied by a signal layer."""

    sleeve_id: str
    cycle_id: str
    signal_id: str
    instrument_ids: tuple[str, str]
    signed_quantities: tuple[Decimal, Decimal]
    action: IntentAction = IntentAction.ENTER
    evaluated_at: datetime = field(default_factory=_utc_now)
    provenance: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "sleeve_id", _required_id(self.sleeve_id, "sleeve_id"))
        object.__setattr__(self, "cycle_id", _required_id(self.cycle_id, "cycle_id"))
        object.__setattr__(self, "signal_id", _required_id(self.signal_id, "signal_id"))
        object.__setattr__(self, "action", self.action if isinstance(self.action, IntentAction) else IntentAction(self.action))
        if self.action not in {IntentAction.ENTER, IntentAction.EXIT}:
            raise ValueError("normalized pair targets support only ENTER and EXIT")
        instruments = tuple(_required_id(value, "instrument_id") for value in self.instrument_ids)
        if len(instruments) != 2 or len(set(instruments)) != 2:
            raise ValueError("normalized pair target requires two distinct instruments")
        object.__setattr__(self, "instrument_ids", instruments)
        quantities = tuple(_decimal(value, "signed_quantity") for value in self.signed_quantities)
        if len(quantities) != 2 or any(value == 0 for value in quantities):
            raise ValueError("normalized pair target requires two non-zero quantities")
        object.__setattr__(self, "signed_quantities", quantities)
        object.__setattr__(self, "evaluated_at", _timestamp(self.evaluated_at, "evaluated_at"))
        object.__setattr__(self, "provenance", _mapping(self.provenance, "provenance"))


# Descriptive alias used by callers that think in terms of signals rather
# than execution targets.
NormalizedPairSignal = NormalizedPairTarget


@dataclass(frozen=True, slots=True)
class SleeveAllocationTarget:
    """One sleeve/book capacity target in a Clean40-style update."""

    sleeve_id: str
    book_id: str
    target_weight: Decimal | None = None
    capacity: Decimal | None = None
    capacity_unit: str = AllocationUnit.FRACTION.value

    def __post_init__(self) -> None:
        object.__setattr__(self, "sleeve_id", _required_id(self.sleeve_id, "sleeve_id"))
        object.__setattr__(self, "book_id", _required_id(self.book_id, "book_id"))
        if (self.target_weight is None) == (self.capacity is None):
            raise ValueError("allocation target requires exactly one weight or capacity")
        unit = str(self.capacity_unit).strip().upper()
        if self.target_weight is not None:
            if unit not in {AllocationUnit.FRACTION.value, "WEIGHT"}:
                raise ValueError("target_weight must use FRACTION capacity units")
            value = _decimal(self.target_weight, "target_weight")
            if value < 0 or value > 1:
                raise ValueError("target_weight must be between 0 and 1")
            unit = AllocationUnit.FRACTION.value
            object.__setattr__(self, "target_weight", value)
        else:
            unit = {
                "CAPITAL": AllocationUnit.CAPITAL_AMOUNT.value,
                "CAPITAL_AMOUNT": AllocationUnit.CAPITAL_AMOUNT.value,
                "RISK": AllocationUnit.RISK_BUDGET.value,
                "RISK_BUDGET": AllocationUnit.RISK_BUDGET.value,
                "FRACTION": AllocationUnit.FRACTION.value,
            }.get(unit, unit)
            if unit not in {item.value for item in AllocationUnit}:
                raise ValueError(f"unsupported allocation capacity unit: {unit}")
            object.__setattr__(self, "capacity", _nonnegative(self.capacity, "capacity"))
        object.__setattr__(self, "capacity_unit", unit)

    @property
    def value(self) -> Decimal:
        return self.target_weight if self.target_weight is not None else self.capacity  # type: ignore[return-value]


@dataclass(frozen=True, slots=True)
class Clean40AllocationUpdate:
    """Versioned atomic allocation mapping; it never liquidates ownership."""

    account_id: str
    version: int
    targets: tuple[SleeveAllocationTarget, ...]
    effective_at: datetime
    provenance: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "account_id", _required_id(self.account_id, "account_id"))
        if isinstance(self.version, bool) or not isinstance(self.version, int) or self.version <= 0:
            raise ValueError("allocation version must be a positive integer")
        targets = tuple(self.targets)
        if not targets:
            raise ValueError("allocation update requires at least one target")
        if any(not isinstance(target, SleeveAllocationTarget) for target in targets):
            raise ValueError("targets must contain SleeveAllocationTarget values")
        if len({target.sleeve_id for target in targets}) != len(targets):
            raise ValueError("allocation update contains duplicate sleeves")
        object.__setattr__(self, "targets", targets)
        object.__setattr__(self, "effective_at", _timestamp(self.effective_at, "effective_at"))
        object.__setattr__(self, "provenance", _required_id(self.provenance, "provenance"))


class Stage5AllocationCoordinator:
    """Validate sleeve targets then update Stage 4 caps atomically."""

    def __init__(self, repository: Any, sleeves: Sequence[PairSleeve]):
        self.repository = repository
        self._sleeves: dict[str, PairSleeve] = {}
        values = tuple(sleeves)
        if not values:
            raise ValueError("at least one sleeve is required")
        for sleeve in values:
            self.register(sleeve)

    @property
    def sleeves(self) -> tuple[PairSleeve, ...]:
        return tuple(self._sleeves.values())

    def register(self, sleeve: PairSleeve) -> None:
        if not isinstance(sleeve, PairSleeve):
            raise ValueError("sleeve must be a PairSleeve")
        if sleeve.sleeve_id in self._sleeves:
            raise ValueError(f"duplicate sleeve: {sleeve.sleeve_id}")
        if self._sleeves and sleeve.account_id != next(iter(self._sleeves.values())).account_id:
            raise ValueError("registered sleeves must share exactly one account")
        sleeve.validate_persistence(self.repository)
        self._sleeves[sleeve.sleeve_id] = sleeve

    def apply(self, update: Clean40AllocationUpdate) -> tuple[str, ...]:
        if update.account_id != self._account_id:
            raise ValueError("allocation update account does not match registered sleeves")
        expected = set(self._sleeves)
        supplied = {target.sleeve_id for target in update.targets}
        if supplied != expected:
            raise ValueError("allocation update must include every registered sleeve exactly once")
        units = {target.capacity_unit for target in update.targets}
        if len(units) != 1:
            raise ValueError("allocation targets use incompatible capacity units")
        unit = next(iter(units))
        total = sum((target.value for target in update.targets), Decimal("0"))
        if unit == AllocationUnit.FRACTION.value:
            if total > 1:
                raise ValueError("allocation weights must total at most 1")
        else:
            account = self.repository.get_account(update.account_id)
            if account is None:
                raise ValueError("allocation account is not persisted")
            capacity = next(
                (
                    account.metadata[key]
                    for key in (
                        "account_capacity",
                        "aggregate_risk_budget",
                        "account_risk_budget",
                        "max_gross_exposure",
                    )
                    if key in account.metadata
                ),
                None,
            )
            if capacity is None or _decimal(capacity, "account capacity") < total:
                raise ValueError("allocation capacity exceeds the declared account budget")

        allocations: list[BookAllocation] = []
        for target in update.targets:
            sleeve = self._sleeves[target.sleeve_id]
            if target.book_id != sleeve.book_id:
                raise ValueError("allocation target book does not match sleeve book")
            metadata = {
                "allocation_source": "stage5_clean40",
                "allocation_version": update.version,
                "allocation_provenance": update.provenance,
                "sleeve_id": sleeve.sleeve_id,
                "capacity_unit": target.capacity_unit,
                "future_headroom_only": True,
            }
            allocation_id = (
                f"stage5-allocation-{update.account_id}-{update.version}-{sleeve.sleeve_id}"
            )
            if unit == AllocationUnit.FRACTION.value:
                allocation = BookAllocation(
                    id=allocation_id,
                    book_id=sleeve.book_id,
                    strategy_id=sleeve.strategy_id,
                    account_id=update.account_id,
                    effective_at=update.effective_at,
                    capital_fraction=target.value,
                    metadata=metadata,
                    created_at=update.effective_at,
                    updated_at=update.effective_at,
                )
            elif unit == AllocationUnit.CAPITAL_AMOUNT.value:
                allocation = BookAllocation(
                    id=allocation_id,
                    book_id=sleeve.book_id,
                    strategy_id=sleeve.strategy_id,
                    account_id=update.account_id,
                    effective_at=update.effective_at,
                    capital_amount=target.value,
                    metadata=metadata,
                    created_at=update.effective_at,
                    updated_at=update.effective_at,
                )
            else:
                allocation = BookAllocation(
                    id=allocation_id,
                    book_id=sleeve.book_id,
                    strategy_id=sleeve.strategy_id,
                    account_id=update.account_id,
                    effective_at=update.effective_at,
                    risk_budget=target.value,
                    metadata=metadata,
                    created_at=update.effective_at,
                    updated_at=update.effective_at,
                )
            allocations.append(allocation)
        return self.repository.apply_book_allocation_update(
            account_id=update.account_id,
            allocations=allocations,
            version=update.version,
            effective_at=update.effective_at,
            provenance=update.provenance,
        )

    @property
    def _account_id(self) -> str:
        account_ids = {sleeve.account_id for sleeve in self._sleeves.values()}
        if len(account_ids) != 1:
            raise ValueError("registered sleeves must share exactly one account")
        return next(iter(account_ids))


__all__ = [
    "AllocationUnit",
    "Clean40AllocationUpdate",
    "NormalizedPairSignal",
    "NormalizedPairTarget",
    "PairSleeve",
    "SleeveAllocationTarget",
    "Stage5AllocationCoordinator",
]
