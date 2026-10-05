"""Provider-neutral, immutable contracts for the generic trading core."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Mapping


class _ValueEnum(str, Enum):
    """String enum that accepts case-insensitive wire values."""

    @classmethod
    def _missing_(cls, value: object):  # type: ignore[override]
        if isinstance(value, str):
            normalized = value.strip().upper()
            for member in cls:
                if member.name.upper() == normalized or str(member.value).upper() == normalized:
                    return member
        return None


class TradingEnvironment(_ValueEnum):
    SIM = "SIM"
    LIVE = "LIVE"


class ExecutionSession(_ValueEnum):
    """Provider-neutral session selection for an execution request."""

    REGULAR = "REGULAR"
    EXTENDED = "EXTENDED"
    OVERNIGHT = "OVERNIGHT"


class AssetClass(_ValueEnum):
    EQUITY = "EQUITY"
    OPTION = "OPTION"
    FUTURE = "FUTURE"
    CRYPTO = "CRYPTO"
    PREDICTION_MARKET = "PREDICTION_MARKET"
    OTHER = "OTHER"


class MappingPurpose(_ValueEnum):
    BROKER = "BROKER"
    MARKET_DATA = "MARKET_DATA"


class Side(_ValueEnum):
    BUY = "BUY"
    SELL = "SELL"


class IntentAction(_ValueEnum):
    ENTER = "ENTER"
    EXIT = "EXIT"
    REBALANCE = "REBALANCE"
    FLATTEN = "FLATTEN"


class IntentStatus(_ValueEnum):
    CREATED = "CREATED"
    RISK_APPROVED = "RISK_APPROVED"
    SUBMITTING = "SUBMITTING"
    WORKING = "WORKING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    COMPLETED = "COMPLETED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"
    RECONCILIATION_REQUIRED = "RECONCILIATION_REQUIRED"


class LegStatus(_ValueEnum):
    PLANNED = "PLANNED"
    SUBMITTING = "SUBMITTING"
    WORKING = "WORKING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"
    RECONCILIATION_REQUIRED = "RECONCILIATION_REQUIRED"


class BrokerOrderStatus(_ValueEnum):
    PREPARED = "PREPARED"
    SUBMITTING = "SUBMITTING"
    WORKING = "WORKING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


class ExecutionEvidenceMode(_ValueEnum):
    """Provider-neutral description of execution evidence available.

    ``INDIVIDUAL_DEALS`` identifies immutable provider deal rows.  Some SIM
    providers cannot expose deal rows but do expose an authenticated order
    snapshot with cumulative dealt quantity; that is deliberately a separate
    mode and must never be represented as a fabricated deal ID.
    """

    INDIVIDUAL_DEALS = "INDIVIDUAL_DEALS"
    CUMULATIVE_ORDER_SNAPSHOTS = "CUMULATIVE_ORDER_SNAPSHOTS"
    UNAVAILABLE = "UNAVAILABLE"


# Set by an adapter when a normalized order snapshot came from its
# authenticated order-query path.  This provenance is kept separate from
# provider metadata so expected order identity fields in a raw row do not
# look like untrusted submission evidence.
ADAPTER_ORDER_SNAPSHOT_AUTHORITY = "ADAPTER_ORDER_SNAPSHOT"
# Set by an adapter only after it has positively validated a non-terminal
# submission acknowledgement: the bound order exists, cumulative/dealt
# quantity is exactly zero, and the raw response contains no contradictory
# fill/deal evidence.  This is intentionally distinct from a terminal order
# snapshot authority.
ADAPTER_SUBMISSION_ACK_AUTHORITY = "ADAPTER_SUBMISSION_ACK"


class LeggingPolicy(_ValueEnum):
    SEQUENTIAL = "SEQUENTIAL"
    PARALLEL = "PARALLEL"
    BEST_EFFORT = "BEST_EFFORT"


class PartialFillPolicy(_ValueEnum):
    WAIT = "WAIT"
    ACCEPT_PARTIAL = "ACCEPT_PARTIAL"
    CANCEL_REMAINDER = "CANCEL_REMAINDER"


class FailurePolicy(_ValueEnum):
    HOLD_AND_RECONCILE = "HOLD_AND_RECONCILE"
    CANCEL_WORKING_LEGS = "CANCEL_WORKING_LEGS"
    UNWIND_FILLED_LEGS = "UNWIND_FILLED_LEGS"


class QuantityUnit(_ValueEnum):
    UNITS = "UNITS"
    CONTRACTS = "CONTRACTS"


class OwnershipClass(_ValueEnum):
    MANAGED = "MANAGED"
    EXTERNAL = "EXTERNAL"
    UNKNOWN = "UNKNOWN"


class ReconciliationStatus(_ValueEnum):
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class IssueStatus(_ValueEnum):
    OPEN = "OPEN"
    RESOLVED = "RESOLVED"


class RecoveryActionStatus(_ValueEnum):
    OPEN = "OPEN"
    RESOLVED = "RESOLVED"


class IssueSeverity(_ValueEnum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_datetime(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware UTC")
    return value.astimezone(timezone.utc)


def _required_id(value: object, field_name: str) -> str:
    if value is None:
        raise ValueError(f"{field_name} is required")
    result = str(value).strip()
    if not result:
        raise ValueError(f"{field_name} must not be empty")
    return result


def _optional_id(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _required_id(value, field_name)


def _decimal(value: object, field_name: str) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a finite Decimal")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a finite Decimal") from exc
    if not result.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return result


def _positive_decimal(value: object, field_name: str) -> Decimal:
    result = _decimal(value, field_name)
    if result <= 0:
        raise ValueError(f"{field_name} must be positive")
    return result


def _nonnegative_decimal(value: object, field_name: str) -> Decimal:
    result = _decimal(value, field_name)
    if result < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return result


def _optional_nonnegative_decimal(value: object, field_name: str) -> Decimal | None:
    if value is None:
        return None
    return _nonnegative_decimal(value, field_name)


def _mapping(value: Mapping[str, Any] | None, field_name: str = "metadata") -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be a mapping")
    return dict(value)


def _enum(value: object, enum_type: type[_ValueEnum], field_name: str):
    try:
        return value if isinstance(value, enum_type) else enum_type(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid {field_name}: {value!r}") from exc


def _normalise_execution_session(
    value: object,
    allow_extended_hours: bool,
) -> tuple[ExecutionSession, bool]:
    """Resolve the session while retaining the legacy extended-hours alias."""

    session = _enum(value, ExecutionSession, "execution_session")
    if not isinstance(allow_extended_hours, bool):
        raise ValueError("allow_extended_hours must be a bool")
    if allow_extended_hours:
        if session is ExecutionSession.REGULAR:
            session = ExecutionSession.EXTENDED
        elif session is not ExecutionSession.EXTENDED:
            raise ValueError("allow_extended_hours cannot be combined with a non-extended execution_session")
    elif session is ExecutionSession.EXTENDED:
        # An explicit generic session remains compatible with adapters that
        # still inspect the legacy boolean field.
        allow_extended_hours = True
    return session, allow_extended_hours


def _nonnegative_int(value: object, field_name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a non-negative integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a non-negative integer") from exc
    if result != value or result < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return result


def _positive_int(value: object, field_name: str) -> int:
    result = _nonnegative_int(value, field_name)
    if result <= 0:
        raise ValueError(f"{field_name} must be a positive integer")
    return result


def _date(value: object, field_name: str) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value.strip())
        except ValueError as exc:
            raise ValueError(f"{field_name} must be an ISO date") from exc
    raise ValueError(f"{field_name} must be a date")


@dataclass(frozen=True, slots=True)
class Account:
    id: str
    broker: str
    environment: TradingEnvironment
    external_account_id: str
    base_currency: str
    enabled: bool = True
    metadata: Mapping[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=_utc_now)
    updated_at: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_id(self.id, "id"))
        object.__setattr__(self, "broker", _required_id(self.broker, "broker"))
        object.__setattr__(self, "environment", _enum(self.environment, TradingEnvironment, "environment"))
        object.__setattr__(self, "external_account_id", _required_id(self.external_account_id, "external_account_id"))
        object.__setattr__(self, "base_currency", _required_id(self.base_currency, "base_currency"))
        if not isinstance(self.enabled, bool):
            raise ValueError("enabled must be a bool")
        object.__setattr__(self, "metadata", _mapping(self.metadata))
        object.__setattr__(self, "created_at", _utc_datetime(self.created_at, "created_at"))
        if self.updated_at is None:
            object.__setattr__(self, "updated_at", self.created_at)
        else:
            object.__setattr__(self, "updated_at", _utc_datetime(self.updated_at, "updated_at"))


@dataclass(frozen=True, slots=True)
class Instrument:
    id: str
    asset_class: AssetClass
    symbol: str
    venue: str
    currency: str
    multiplier: Decimal = Decimal("1")
    tick_size: Decimal = Decimal("0.01")
    lot_size: Decimal = Decimal("1")
    expiry: date | None = None
    strike: Decimal | None = None
    option_right: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=_utc_now)
    updated_at: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_id(self.id, "id"))
        object.__setattr__(self, "asset_class", _enum(self.asset_class, AssetClass, "asset_class"))
        object.__setattr__(self, "symbol", _required_id(self.symbol, "symbol"))
        object.__setattr__(self, "venue", _required_id(self.venue, "venue"))
        object.__setattr__(self, "currency", _required_id(self.currency, "currency"))
        object.__setattr__(self, "multiplier", _positive_decimal(self.multiplier, "multiplier"))
        object.__setattr__(self, "tick_size", _positive_decimal(self.tick_size, "tick_size"))
        object.__setattr__(self, "lot_size", _positive_decimal(self.lot_size, "lot_size"))
        object.__setattr__(self, "expiry", _date(self.expiry, "expiry"))
        if self.strike is not None:
            object.__setattr__(self, "strike", _positive_decimal(self.strike, "strike"))
        if self.option_right is not None:
            object.__setattr__(self, "option_right", _required_id(self.option_right, "option_right").upper())
        object.__setattr__(self, "metadata", _mapping(self.metadata))
        object.__setattr__(self, "created_at", _utc_datetime(self.created_at, "created_at"))
        if self.updated_at is None:
            object.__setattr__(self, "updated_at", self.created_at)
        else:
            object.__setattr__(self, "updated_at", _utc_datetime(self.updated_at, "updated_at"))


@dataclass(frozen=True, slots=True)
class InstrumentMapping:
    id: str
    instrument_id: str
    provider: str
    purpose: MappingPurpose
    external_symbol: str
    external_id: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=_utc_now)
    updated_at: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_id(self.id, "id"))
        object.__setattr__(self, "instrument_id", _required_id(self.instrument_id, "instrument_id"))
        object.__setattr__(self, "provider", _required_id(self.provider, "provider"))
        object.__setattr__(self, "purpose", _enum(self.purpose, MappingPurpose, "purpose"))
        object.__setattr__(self, "external_symbol", _required_id(self.external_symbol, "external_symbol"))
        object.__setattr__(self, "external_id", _optional_id(self.external_id, "external_id"))
        object.__setattr__(self, "metadata", _mapping(self.metadata))
        object.__setattr__(self, "created_at", _utc_datetime(self.created_at, "created_at"))
        if self.updated_at is None:
            object.__setattr__(self, "updated_at", self.created_at)
        else:
            object.__setattr__(self, "updated_at", _utc_datetime(self.updated_at, "updated_at"))


@dataclass(frozen=True, slots=True)
class Strategy:
    id: str
    name: str
    strategy_type: str
    version: str = "1"
    enabled: bool = True
    config: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=_utc_now)
    updated_at: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_id(self.id, "id"))
        object.__setattr__(self, "name", _required_id(self.name, "name"))
        object.__setattr__(self, "strategy_type", _required_id(self.strategy_type, "strategy_type"))
        object.__setattr__(self, "version", _required_id(self.version, "version"))
        if not isinstance(self.enabled, bool):
            raise ValueError("enabled must be a bool")
        object.__setattr__(self, "config", _mapping(self.config, "config"))
        object.__setattr__(self, "metadata", _mapping(self.metadata))
        object.__setattr__(self, "created_at", _utc_datetime(self.created_at, "created_at"))
        if self.updated_at is None:
            object.__setattr__(self, "updated_at", self.created_at)
        else:
            object.__setattr__(self, "updated_at", _utc_datetime(self.updated_at, "updated_at"))


@dataclass(frozen=True, slots=True)
class Book:
    id: str
    name: str
    enabled: bool = True
    metadata: Mapping[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=_utc_now)
    updated_at: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_id(self.id, "id"))
        object.__setattr__(self, "name", _required_id(self.name, "name"))
        if not isinstance(self.enabled, bool):
            raise ValueError("enabled must be a bool")
        object.__setattr__(self, "metadata", _mapping(self.metadata))
        object.__setattr__(self, "created_at", _utc_datetime(self.created_at, "created_at"))
        if self.updated_at is None:
            object.__setattr__(self, "updated_at", self.created_at)
        else:
            object.__setattr__(self, "updated_at", _utc_datetime(self.updated_at, "updated_at"))


@dataclass(frozen=True, slots=True)
class BookAllocation:
    id: str
    book_id: str
    strategy_id: str
    account_id: str
    effective_at: datetime
    capital_fraction: Decimal | None = None
    capital_amount: Decimal | None = None
    risk_budget: Decimal | None = None
    expires_at: datetime | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=_utc_now)
    updated_at: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_id(self.id, "id"))
        object.__setattr__(self, "book_id", _required_id(self.book_id, "book_id"))
        object.__setattr__(self, "strategy_id", _required_id(self.strategy_id, "strategy_id"))
        object.__setattr__(self, "account_id", _required_id(self.account_id, "account_id"))
        if self.capital_fraction is not None:
            fraction = _nonnegative_decimal(self.capital_fraction, "capital_fraction")
            if fraction > 1:
                raise ValueError("capital_fraction must be at most 1")
            object.__setattr__(self, "capital_fraction", fraction)
        object.__setattr__(self, "capital_amount", _optional_nonnegative_decimal(self.capital_amount, "capital_amount"))
        object.__setattr__(self, "risk_budget", _optional_nonnegative_decimal(self.risk_budget, "risk_budget"))
        object.__setattr__(self, "effective_at", _utc_datetime(self.effective_at, "effective_at"))
        if self.expires_at is not None:
            object.__setattr__(self, "expires_at", _utc_datetime(self.expires_at, "expires_at"))
            if self.expires_at <= self.effective_at:
                raise ValueError("expires_at must be after effective_at")
        configured_limits = tuple(
            value is not None
            for value in (self.capital_fraction, self.capital_amount, self.risk_budget)
        )
        if not any(configured_limits):
            raise ValueError(
                "book allocation requires capital_fraction, capital_amount, or risk_budget"
            )
        if sum(configured_limits) != 1:
            raise ValueError(
                "book allocation must configure exactly one limit basis"
            )
        object.__setattr__(self, "metadata", _mapping(self.metadata))
        object.__setattr__(self, "created_at", _utc_datetime(self.created_at, "created_at"))
        if self.updated_at is None:
            object.__setattr__(self, "updated_at", self.created_at)
        else:
            object.__setattr__(self, "updated_at", _utc_datetime(self.updated_at, "updated_at"))

    @property
    def limit_basis(self) -> str:
        """Return the single generic capacity basis configured for this book."""
        configured = tuple(
            name
            for name, value in (
                ("capital_fraction", self.capital_fraction),
                ("capital_amount", self.capital_amount),
                ("risk_budget", self.risk_budget),
            )
            if value is not None
        )
        if len(configured) != 1:
            raise ValueError("book allocation must configure exactly one limit basis")
        return configured[0]


@dataclass(frozen=True, slots=True)
class ExecutionPolicy:
    legging_policy: LeggingPolicy = LeggingPolicy.SEQUENTIAL
    partial_fill_policy: PartialFillPolicy = PartialFillPolicy.WAIT
    failure_policy: FailurePolicy = FailurePolicy.HOLD_AND_RECONCILE
    max_attempts: int = 1
    timeout_seconds: int = 300
    require_native_atomicity: bool = False
    allow_extended_hours: bool = False
    required_capabilities: frozenset[str] = frozenset()
    metadata: Mapping[str, Any] = field(default_factory=dict)
    execution_session: ExecutionSession = ExecutionSession.REGULAR
    stale_order_seconds: int = 300

    def __post_init__(self) -> None:
        object.__setattr__(self, "legging_policy", _enum(self.legging_policy, LeggingPolicy, "legging_policy"))
        object.__setattr__(self, "partial_fill_policy", _enum(self.partial_fill_policy, PartialFillPolicy, "partial_fill_policy"))
        if self.partial_fill_policy is not PartialFillPolicy.WAIT:
            raise ValueError("partial_fill_policy values other than WAIT are not implemented")
        object.__setattr__(self, "failure_policy", _enum(self.failure_policy, FailurePolicy, "failure_policy"))
        object.__setattr__(self, "max_attempts", _positive_int(self.max_attempts, "max_attempts"))
        object.__setattr__(self, "timeout_seconds", _positive_int(self.timeout_seconds, "timeout_seconds"))
        object.__setattr__(self, "stale_order_seconds", _positive_int(self.stale_order_seconds, "stale_order_seconds"))
        if self.legging_policy is not LeggingPolicy.SEQUENTIAL:
            raise ValueError("legging_policy values other than SEQUENTIAL are not implemented")
        if self.failure_policy is FailurePolicy.UNWIND_FILLED_LEGS:
            raise ValueError("failure_policy UNWIND_FILLED_LEGS is not implemented")
        if self.max_attempts != 1:
            raise ValueError("max_attempts values other than 1 are not implemented")
        if not isinstance(self.require_native_atomicity, bool):
            raise ValueError("require_native_atomicity must be a bool")
        if self.require_native_atomicity:
            raise ValueError("require_native_atomicity is not implemented for separate generic legs")
        session, allow_extended_hours = _normalise_execution_session(
            self.execution_session,
            self.allow_extended_hours,
        )
        object.__setattr__(self, "execution_session", session)
        object.__setattr__(self, "allow_extended_hours", allow_extended_hours)
        object.__setattr__(self, "required_capabilities", frozenset(str(value).strip() for value in self.required_capabilities if str(value).strip()))
        if self.required_capabilities:
            raise ValueError("required_capabilities are not implemented by the generic Stage 3 executor")
        object.__setattr__(self, "metadata", _mapping(self.metadata))


@dataclass(frozen=True, slots=True)
class OrderLeg:
    id: str
    intent_id: str
    sequence: int
    instrument_id: str
    side: Side
    quantity: Decimal
    quantity_unit: QuantityUnit = QuantityUnit.UNITS
    order_type: str = "MARKET"
    limit_price: Decimal | None = None
    stop_price: Decimal | None = None
    time_in_force: str | None = None
    status: LegStatus = LegStatus.PLANNED
    cumulative_filled_quantity: Decimal = Decimal("0")
    average_fill_price: Decimal | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=_utc_now)
    updated_at: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_id(self.id, "id"))
        object.__setattr__(self, "intent_id", _required_id(self.intent_id, "intent_id"))
        object.__setattr__(self, "sequence", _nonnegative_int(self.sequence, "sequence"))
        object.__setattr__(self, "instrument_id", _required_id(self.instrument_id, "instrument_id"))
        object.__setattr__(self, "side", _enum(self.side, Side, "side"))
        object.__setattr__(self, "quantity", _positive_decimal(self.quantity, "quantity"))
        object.__setattr__(self, "quantity_unit", _enum(self.quantity_unit, QuantityUnit, "quantity_unit"))
        object.__setattr__(self, "order_type", _required_id(self.order_type, "order_type").upper())
        if self.limit_price is not None:
            object.__setattr__(self, "limit_price", _positive_decimal(self.limit_price, "limit_price"))
        if self.stop_price is not None:
            object.__setattr__(self, "stop_price", _positive_decimal(self.stop_price, "stop_price"))
        if self.time_in_force is not None:
            object.__setattr__(self, "time_in_force", _required_id(self.time_in_force, "time_in_force").upper())
        object.__setattr__(self, "status", _enum(self.status, LegStatus, "status"))
        filled = _nonnegative_decimal(self.cumulative_filled_quantity, "cumulative_filled_quantity")
        if filled > self.quantity:
            raise ValueError("cumulative_filled_quantity cannot exceed quantity")
        object.__setattr__(self, "cumulative_filled_quantity", filled)
        if self.average_fill_price is not None:
            object.__setattr__(self, "average_fill_price", _positive_decimal(self.average_fill_price, "average_fill_price"))
        object.__setattr__(self, "metadata", _mapping(self.metadata))
        object.__setattr__(self, "created_at", _utc_datetime(self.created_at, "created_at"))
        if self.updated_at is None:
            object.__setattr__(self, "updated_at", self.created_at)
        else:
            object.__setattr__(self, "updated_at", _utc_datetime(self.updated_at, "updated_at"))

@dataclass(frozen=True, slots=True)
class OrderIntent:
    id: str
    idempotency_key: str
    strategy_id: str
    account_id: str
    action: IntentAction
    legs: tuple[OrderLeg, ...]
    book_id: str | None = None
    status: IntentStatus = IntentStatus.CREATED
    source_signal_id: str | None = None
    execution_policy: ExecutionPolicy = field(default_factory=ExecutionPolicy)
    metadata: Mapping[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=_utc_now)
    updated_at: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_id(self.id, "id"))
        object.__setattr__(self, "idempotency_key", _required_id(self.idempotency_key, "idempotency_key"))
        object.__setattr__(self, "strategy_id", _required_id(self.strategy_id, "strategy_id"))
        object.__setattr__(self, "account_id", _required_id(self.account_id, "account_id"))
        object.__setattr__(self, "action", _enum(self.action, IntentAction, "action"))
        legs = tuple(self.legs)
        if not legs:
            raise ValueError("an order intent must contain at least one leg")
        if any(not isinstance(leg, OrderLeg) for leg in legs):
            raise ValueError("legs must contain OrderLeg values")
        leg_ids = [leg.id for leg in legs]
        sequences = [leg.sequence for leg in legs]
        if len(set(leg_ids)) != len(leg_ids):
            raise ValueError("order intent legs must have unique IDs")
        if len(set(sequences)) != len(sequences):
            raise ValueError("order intent legs must have unique sequences")
        if any(leg.intent_id != self.id for leg in legs):
            raise ValueError("leg intent_id does not match intent id")
        object.__setattr__(self, "legs", tuple(sorted(legs, key=lambda leg: leg.sequence)))
        object.__setattr__(self, "book_id", _optional_id(self.book_id, "book_id"))
        object.__setattr__(self, "status", _enum(self.status, IntentStatus, "status"))
        object.__setattr__(self, "source_signal_id", _optional_id(self.source_signal_id, "source_signal_id"))
        if not isinstance(self.execution_policy, ExecutionPolicy):
            raise ValueError("execution_policy must be an ExecutionPolicy")
        object.__setattr__(self, "metadata", _mapping(self.metadata))
        object.__setattr__(self, "created_at", _utc_datetime(self.created_at, "created_at"))
        if self.updated_at is None:
            object.__setattr__(self, "updated_at", self.created_at)
        else:
            object.__setattr__(self, "updated_at", _utc_datetime(self.updated_at, "updated_at"))

@dataclass(frozen=True, slots=True)
class BrokerOrder:
    id: str
    order_leg_id: str
    account_id: str
    broker: str
    attempt_number: int
    submitted_quantity: Decimal
    external_order_id: str | None = None
    client_order_id: str | None = None
    status: BrokerOrderStatus = BrokerOrderStatus.PREPARED
    submitted_at: datetime | None = None
    updated_at: datetime = field(default_factory=_utc_now)
    replaces_broker_order_id: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_id(self.id, "id"))
        object.__setattr__(self, "order_leg_id", _required_id(self.order_leg_id, "order_leg_id"))
        object.__setattr__(self, "account_id", _required_id(self.account_id, "account_id"))
        object.__setattr__(self, "broker", _required_id(self.broker, "broker"))
        object.__setattr__(self, "attempt_number", _positive_int(self.attempt_number, "attempt_number"))
        object.__setattr__(self, "external_order_id", _optional_id(self.external_order_id, "external_order_id"))
        object.__setattr__(self, "client_order_id", _optional_id(self.client_order_id, "client_order_id"))
        object.__setattr__(self, "status", _enum(self.status, BrokerOrderStatus, "status"))
        object.__setattr__(self, "submitted_quantity", _positive_decimal(self.submitted_quantity, "submitted_quantity"))
        if self.submitted_at is not None:
            object.__setattr__(self, "submitted_at", _utc_datetime(self.submitted_at, "submitted_at"))
        object.__setattr__(self, "updated_at", _utc_datetime(self.updated_at, "updated_at"))
        object.__setattr__(self, "replaces_broker_order_id", _optional_id(self.replaces_broker_order_id, "replaces_broker_order_id"))
        object.__setattr__(self, "metadata", _mapping(self.metadata))

@dataclass(frozen=True, slots=True)
class BrokerOrderEvent:
    id: str
    broker_order_id: str
    dedupe_key: str
    event_type: str
    event_at: datetime
    received_at: datetime
    broker_status: BrokerOrderStatus | None = None
    external_event_id: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    external_order_id: str | None = None
    client_order_id: str | None = None
    cumulative_filled_quantity: Decimal | None = None
    # Explicit account aliases are optional for backwards-compatible event
    # producers, but when supplied they must agree with the persisted order
    # account before an event can affect lifecycle state.
    account_id: str | None = None
    external_account_id: str | None = None
    # A provider may explicitly certify that this terminal event carried no
    # fills. False/omitted is intentionally not proof of zero.
    no_fill_asserted: bool = False
    # Set when the event status/cumulative values were projected from an
    # adapter-authenticated order snapshot.
    authority: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_id(self.id, "id"))
        object.__setattr__(self, "broker_order_id", _required_id(self.broker_order_id, "broker_order_id"))
        object.__setattr__(self, "dedupe_key", _required_id(self.dedupe_key, "dedupe_key"))
        object.__setattr__(self, "event_type", _required_id(self.event_type, "event_type").upper())
        if self.broker_status is not None:
            object.__setattr__(self, "broker_status", _enum(self.broker_status, BrokerOrderStatus, "broker_status"))
        object.__setattr__(self, "event_at", _utc_datetime(self.event_at, "event_at"))
        object.__setattr__(self, "received_at", _utc_datetime(self.received_at, "received_at"))
        object.__setattr__(self, "external_event_id", _optional_id(self.external_event_id, "external_event_id"))
        object.__setattr__(self, "external_order_id", _optional_id(self.external_order_id, "external_order_id"))
        object.__setattr__(self, "client_order_id", _optional_id(self.client_order_id, "client_order_id"))
        object.__setattr__(self, "account_id", _optional_id(self.account_id, "account_id"))
        object.__setattr__(self, "external_account_id", _optional_id(self.external_account_id, "external_account_id"))
        if not isinstance(self.no_fill_asserted, bool):
            raise ValueError("no_fill_asserted must be a bool")
        object.__setattr__(self, "authority", _optional_id(self.authority, "authority"))
        if self.cumulative_filled_quantity is not None:
            object.__setattr__(
                self,
                "cumulative_filled_quantity",
                _nonnegative_decimal(self.cumulative_filled_quantity, "cumulative_filled_quantity"),
            )
        object.__setattr__(self, "metadata", _mapping(self.metadata))

@dataclass(frozen=True, slots=True)
class Fill:
    id: str
    broker_order_id: str
    order_leg_id: str
    dedupe_key: str
    quantity: Decimal
    price: Decimal
    filled_at: datetime
    received_at: datetime
    external_fill_id: str | None = None
    fee: Decimal | None = None
    fee_currency: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    # Optional normalized account identity for repository-level validation.
    account_id: str | None = None
    # How the provider established this execution.  Order-level SIM fallback
    # evidence is explicitly distinct from individual deal evidence.
    evidence_mode: ExecutionEvidenceMode = ExecutionEvidenceMode.INDIVIDUAL_DEALS
    # Exact provider-side identity used to bind this evidence to the one
    # persisted broker-order attempt.  Older callers may omit it; the
    # repository derives the deterministic legacy reference from the
    # persisted external order ID and dedupe key, but an explicit value is
    # always checked when supplied.
    external_order_id: str | None = None
    evidence_reference: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_id(self.id, "id"))
        object.__setattr__(self, "broker_order_id", _required_id(self.broker_order_id, "broker_order_id"))
        object.__setattr__(self, "order_leg_id", _required_id(self.order_leg_id, "order_leg_id"))
        object.__setattr__(self, "dedupe_key", _required_id(self.dedupe_key, "dedupe_key"))
        object.__setattr__(self, "quantity", _positive_decimal(self.quantity, "quantity"))
        object.__setattr__(self, "price", _positive_decimal(self.price, "price"))
        object.__setattr__(self, "filled_at", _utc_datetime(self.filled_at, "filled_at"))
        object.__setattr__(self, "received_at", _utc_datetime(self.received_at, "received_at"))
        object.__setattr__(self, "external_fill_id", _optional_id(self.external_fill_id, "external_fill_id"))
        object.__setattr__(self, "fee", _optional_nonnegative_decimal(self.fee, "fee"))
        if self.fee_currency is not None:
            object.__setattr__(self, "fee_currency", _required_id(self.fee_currency, "fee_currency"))
        object.__setattr__(self, "metadata", _mapping(self.metadata))
        object.__setattr__(self, "account_id", _optional_id(self.account_id, "account_id"))
        object.__setattr__(self, "evidence_mode", _enum(self.evidence_mode, ExecutionEvidenceMode, "evidence_mode"))
        object.__setattr__(self, "external_order_id", _optional_id(self.external_order_id, "external_order_id"))
        object.__setattr__(self, "evidence_reference", _optional_id(self.evidence_reference, "evidence_reference"))


@dataclass(frozen=True, slots=True)
class RecoveryAction:
    """Durable operator-facing guidance for an unresolved order observation."""

    id: str
    intent_id: str
    account_id: str
    action_key: str
    state: str
    summary: str
    observed_positions: Mapping[str, Any] = field(default_factory=dict)
    remaining_quantities: Mapping[str, Any] = field(default_factory=dict)
    stale: bool = False
    timed_out: bool = False
    allowed_next_steps: tuple[str, ...] = ()
    status: RecoveryActionStatus = RecoveryActionStatus.OPEN
    detected_at: datetime = field(default_factory=_utc_now)
    last_seen_at: datetime | None = None
    occurrence_count: int = 1
    resolved_at: datetime | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_id(self.id, "id"))
        object.__setattr__(self, "intent_id", _required_id(self.intent_id, "intent_id"))
        object.__setattr__(self, "account_id", _required_id(self.account_id, "account_id"))
        object.__setattr__(self, "action_key", _required_id(self.action_key, "action_key"))
        object.__setattr__(self, "state", _required_id(self.state, "state").upper())
        object.__setattr__(self, "summary", str(self.summary))
        object.__setattr__(self, "observed_positions", _mapping(self.observed_positions, "observed_positions"))
        object.__setattr__(self, "remaining_quantities", _mapping(self.remaining_quantities, "remaining_quantities"))
        if not isinstance(self.stale, bool):
            raise ValueError("stale must be a bool")
        if not isinstance(self.timed_out, bool):
            raise ValueError("timed_out must be a bool")
        object.__setattr__(
            self,
            "allowed_next_steps",
            tuple(_required_id(step, "allowed_next_step") for step in self.allowed_next_steps),
        )
        object.__setattr__(self, "status", _enum(self.status, RecoveryActionStatus, "status"))
        detected_at = _utc_datetime(self.detected_at, "detected_at")
        object.__setattr__(self, "detected_at", detected_at)
        if self.last_seen_at is None:
            object.__setattr__(self, "last_seen_at", detected_at)
        else:
            object.__setattr__(self, "last_seen_at", _utc_datetime(self.last_seen_at, "last_seen_at"))
        object.__setattr__(self, "occurrence_count", _positive_int(self.occurrence_count, "occurrence_count"))
        if self.resolved_at is not None:
            object.__setattr__(self, "resolved_at", _utc_datetime(self.resolved_at, "resolved_at"))
        object.__setattr__(self, "metadata", _mapping(self.metadata))

@dataclass(frozen=True, slots=True)
class BrokerCapabilities:
    broker: str = "default"
    features: frozenset[str] = frozenset()
    supports_balance_read: bool = True
    supports_position_read: bool = True
    supports_order_read: bool = True
    supports_fill_read: bool = False
    execution_evidence_mode: ExecutionEvidenceMode = ExecutionEvidenceMode.UNAVAILABLE
    execution_evidence_scope: frozenset[str] = frozenset()
    supports_order_events: bool = False
    supports_submit: bool = True
    supports_cancel: bool = False
    supports_replace: bool = False
    supports_native_multi_leg: bool = False
    supports_client_order_id: bool = False
    supports_idempotent_submit: bool = False
    max_legs: int | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "broker", _required_id(self.broker, "broker"))
        object.__setattr__(self, "features", frozenset(str(value).strip() for value in self.features if str(value).strip()))
        if self.max_legs is not None:
            object.__setattr__(self, "max_legs", _positive_int(self.max_legs, "max_legs"))
        for name in (
            "supports_balance_read",
            "supports_position_read",
            "supports_order_read",
            "supports_fill_read",
            "supports_order_events",
            "supports_submit",
            "supports_cancel",
            "supports_replace",
            "supports_native_multi_leg",
            "supports_client_order_id",
            "supports_idempotent_submit",
        ):
            if not isinstance(getattr(self, name), bool):
                raise ValueError(f"{name} must be a bool")
        object.__setattr__(
            self,
            "execution_evidence_mode",
            _enum(self.execution_evidence_mode, ExecutionEvidenceMode, "execution_evidence_mode"),
        )
        object.__setattr__(
            self,
            "execution_evidence_scope",
            frozenset(str(value).strip() for value in self.execution_evidence_scope if str(value).strip()),
        )
        object.__setattr__(self, "metadata", _mapping(self.metadata))

    def supports(self, capability: str) -> bool:
        return str(capability).strip() in self.features


@dataclass(frozen=True, slots=True)
class BrokerSnapshot:
    id: str
    account_id: str
    captured_at: datetime
    status: str
    error: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_id(self.id, "id"))
        object.__setattr__(self, "account_id", _required_id(self.account_id, "account_id"))
        object.__setattr__(self, "captured_at", _utc_datetime(self.captured_at, "captured_at"))
        object.__setattr__(self, "status", _required_id(self.status, "status").upper())
        if self.error is not None:
            object.__setattr__(self, "error", str(self.error))
        object.__setattr__(self, "metadata", _mapping(self.metadata))


@dataclass(frozen=True, slots=True)
class AccountBalanceSnapshot:
    id: str
    broker_snapshot_id: str
    currency: str
    cash: Decimal | None = None
    buying_power: Decimal | None = None
    equity: Decimal | None = None
    initial_margin: Decimal | None = None
    maintenance_margin: Decimal | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_id(self.id, "id"))
        object.__setattr__(self, "broker_snapshot_id", _required_id(self.broker_snapshot_id, "broker_snapshot_id"))
        object.__setattr__(self, "currency", _required_id(self.currency, "currency"))
        for name in ("cash", "buying_power", "equity", "initial_margin", "maintenance_margin"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _decimal(value, name))
        object.__setattr__(self, "metadata", _mapping(self.metadata))


@dataclass(frozen=True, slots=True)
class PositionSnapshot:
    id: str
    broker_snapshot_id: str
    account_id: str
    instrument_id: str
    signed_quantity: Decimal
    average_price: Decimal | None = None
    captured_at: datetime = field(default_factory=_utc_now)
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_id(self.id, "id"))
        object.__setattr__(self, "broker_snapshot_id", _required_id(self.broker_snapshot_id, "broker_snapshot_id"))
        object.__setattr__(self, "account_id", _required_id(self.account_id, "account_id"))
        object.__setattr__(self, "instrument_id", _required_id(self.instrument_id, "instrument_id"))
        object.__setattr__(self, "signed_quantity", _decimal(self.signed_quantity, "signed_quantity"))
        if self.average_price is not None:
            object.__setattr__(self, "average_price", _positive_decimal(self.average_price, "average_price"))
        object.__setattr__(self, "captured_at", _utc_datetime(self.captured_at, "captured_at"))
        object.__setattr__(self, "metadata", _mapping(self.metadata))


@dataclass(frozen=True, slots=True)
class BrokerOrderSnapshot:
    id: str
    broker_snapshot_id: str
    account_id: str
    instrument_id: str
    external_order_id: str
    side: Side
    quantity: Decimal
    filled_quantity: Decimal
    status: BrokerOrderStatus
    captured_at: datetime
    client_order_id: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    # Broker-provided order creation/update time.  This is distinct from the
    # local query capture time and is used for stale-order policy when present.
    order_time: datetime | None = None
    external_account_id: str | None = None
    no_fill_asserted: bool = False
    # Set by an adapter-authenticated order-query path.  This is deliberately
    # separate from provider metadata so generic evidence validation can
    # distinguish expected row identity from an untrusted order claim.
    authority: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_id(self.id, "id"))
        object.__setattr__(self, "broker_snapshot_id", _required_id(self.broker_snapshot_id, "broker_snapshot_id"))
        object.__setattr__(self, "account_id", _required_id(self.account_id, "account_id"))
        object.__setattr__(self, "instrument_id", _required_id(self.instrument_id, "instrument_id"))
        object.__setattr__(self, "external_order_id", _required_id(self.external_order_id, "external_order_id"))
        object.__setattr__(self, "client_order_id", _optional_id(self.client_order_id, "client_order_id"))
        object.__setattr__(self, "side", _enum(self.side, Side, "side"))
        quantity = _positive_decimal(self.quantity, "quantity")
        filled = _nonnegative_decimal(self.filled_quantity, "filled_quantity")
        if filled > quantity:
            raise ValueError("filled_quantity cannot exceed quantity")
        object.__setattr__(self, "quantity", quantity)
        object.__setattr__(self, "filled_quantity", filled)
        object.__setattr__(self, "status", _enum(self.status, BrokerOrderStatus, "status"))
        object.__setattr__(self, "captured_at", _utc_datetime(self.captured_at, "captured_at"))
        object.__setattr__(self, "metadata", _mapping(self.metadata))
        if self.order_time is not None:
            object.__setattr__(self, "order_time", _utc_datetime(self.order_time, "order_time"))
        object.__setattr__(self, "external_account_id", _optional_id(self.external_account_id, "external_account_id"))
        if not isinstance(self.no_fill_asserted, bool):
            raise ValueError("no_fill_asserted must be a bool")
        object.__setattr__(self, "authority", _optional_id(self.authority, "authority"))


@dataclass(frozen=True, slots=True)
class PositionAllocation:
    id: str
    account_id: str
    instrument_id: str
    ownership_class: OwnershipClass
    signed_quantity: Decimal
    strategy_id: str | None = None
    book_id: str | None = None
    source_intent_id: str | None = None
    updated_at: datetime = field(default_factory=_utc_now)
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_id(self.id, "id"))
        object.__setattr__(self, "account_id", _required_id(self.account_id, "account_id"))
        object.__setattr__(self, "instrument_id", _required_id(self.instrument_id, "instrument_id"))
        object.__setattr__(self, "ownership_class", _enum(self.ownership_class, OwnershipClass, "ownership_class"))
        object.__setattr__(self, "signed_quantity", _decimal(self.signed_quantity, "signed_quantity"))
        object.__setattr__(self, "strategy_id", _optional_id(self.strategy_id, "strategy_id"))
        object.__setattr__(self, "book_id", _optional_id(self.book_id, "book_id"))
        object.__setattr__(self, "source_intent_id", _optional_id(self.source_intent_id, "source_intent_id"))
        object.__setattr__(self, "updated_at", _utc_datetime(self.updated_at, "updated_at"))
        object.__setattr__(self, "metadata", _mapping(self.metadata))


@dataclass(frozen=True, slots=True)
class RiskDecisionRecord:
    id: str
    intent_id: str
    approved: bool
    reason: str
    checks: Mapping[str, Any] = field(default_factory=dict)
    evaluated_at: datetime = field(default_factory=_utc_now)
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_id(self.id, "id"))
        object.__setattr__(self, "intent_id", _required_id(self.intent_id, "intent_id"))
        if not isinstance(self.approved, bool):
            raise ValueError("approved must be a bool")
        object.__setattr__(self, "reason", str(self.reason))
        object.__setattr__(self, "checks", _mapping(self.checks, "checks"))
        object.__setattr__(self, "evaluated_at", _utc_datetime(self.evaluated_at, "evaluated_at"))
        object.__setattr__(self, "metadata", _mapping(self.metadata))


@dataclass(frozen=True, slots=True)
class ReconciliationRun:
    id: str
    account_id: str
    broker_snapshot_id: str | None = None
    started_at: datetime = field(default_factory=_utc_now)
    completed_at: datetime | None = None
    status: ReconciliationStatus = ReconciliationStatus.RUNNING
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_id(self.id, "id"))
        object.__setattr__(self, "account_id", _required_id(self.account_id, "account_id"))
        object.__setattr__(self, "broker_snapshot_id", _optional_id(self.broker_snapshot_id, "broker_snapshot_id"))
        object.__setattr__(self, "started_at", _utc_datetime(self.started_at, "started_at"))
        if self.completed_at is not None:
            object.__setattr__(self, "completed_at", _utc_datetime(self.completed_at, "completed_at"))
        object.__setattr__(self, "status", _enum(self.status, ReconciliationStatus, "status"))
        object.__setattr__(self, "metadata", _mapping(self.metadata))


@dataclass(frozen=True, slots=True)
class ExecutionEvidenceBaseline:
    """An explicitly verified, bounded account-facts checkpoint.

    This records what was freshly observed and reconciled; it does not claim
    that a provider with cumulative order snapshots exposes historical deal
    records that it cannot return.
    """

    id: str
    account_id: str
    captured_at: datetime
    evidence_mode: ExecutionEvidenceMode
    coverage: tuple[str, ...]
    source_ledger_fingerprint: str
    source_order_ids: tuple[str, ...]
    position_fingerprint: str
    open_order_fingerprint: str
    verified_flat: bool = True
    status: str = "VERIFIED"
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_id(self.id, "id"))
        object.__setattr__(self, "account_id", _required_id(self.account_id, "account_id"))
        object.__setattr__(self, "captured_at", _utc_datetime(self.captured_at, "captured_at"))
        object.__setattr__(self, "evidence_mode", _enum(self.evidence_mode, ExecutionEvidenceMode, "evidence_mode"))
        object.__setattr__(self, "coverage", tuple(_required_id(value, "coverage") for value in self.coverage))
        object.__setattr__(self, "source_ledger_fingerprint", _required_id(self.source_ledger_fingerprint, "source_ledger_fingerprint"))
        object.__setattr__(self, "source_order_ids", tuple(_required_id(value, "source_order_id") for value in self.source_order_ids))
        object.__setattr__(self, "position_fingerprint", _required_id(self.position_fingerprint, "position_fingerprint"))
        object.__setattr__(self, "open_order_fingerprint", _required_id(self.open_order_fingerprint, "open_order_fingerprint"))
        if not isinstance(self.verified_flat, bool):
            raise ValueError("verified_flat must be a bool")
        object.__setattr__(self, "status", _required_id(self.status, "status").upper())
        object.__setattr__(self, "metadata", _mapping(self.metadata))


@dataclass(frozen=True, slots=True)
class ReconciliationIssue:
    id: str
    run_id: str
    account_id: str
    issue_key: str
    entity_type: str
    entity_key: str
    category: str
    severity: IssueSeverity
    status: IssueStatus = IssueStatus.OPEN
    sticky: bool = True
    details: Mapping[str, Any] = field(default_factory=dict)
    detected_at: datetime = field(default_factory=_utc_now)
    last_seen_at: datetime | None = None
    occurrence_count: int = 1
    resolved_at: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_id(self.id, "id"))
        object.__setattr__(self, "run_id", _required_id(self.run_id, "run_id"))
        object.__setattr__(self, "account_id", _required_id(self.account_id, "account_id"))
        object.__setattr__(self, "issue_key", _required_id(self.issue_key, "issue_key"))
        object.__setattr__(self, "entity_type", _required_id(self.entity_type, "entity_type"))
        object.__setattr__(self, "entity_key", _required_id(self.entity_key, "entity_key"))
        object.__setattr__(self, "category", _required_id(self.category, "category"))
        severity = _enum(self.severity, IssueSeverity, "severity")
        object.__setattr__(self, "severity", severity)
        object.__setattr__(self, "status", _enum(self.status, IssueStatus, "status"))
        if not isinstance(self.sticky, bool):
            raise ValueError("sticky must be a bool")
        if severity in {IssueSeverity.ERROR, IssueSeverity.CRITICAL}:
            object.__setattr__(self, "sticky", True)
        object.__setattr__(self, "details", _mapping(self.details, "details"))
        detected_at = _utc_datetime(self.detected_at, "detected_at")
        object.__setattr__(self, "detected_at", detected_at)
        if self.last_seen_at is None:
            object.__setattr__(self, "last_seen_at", detected_at)
        else:
            object.__setattr__(self, "last_seen_at", _utc_datetime(self.last_seen_at, "last_seen_at"))
        object.__setattr__(self, "occurrence_count", _positive_int(self.occurrence_count, "occurrence_count"))
        if self.resolved_at is not None:
            object.__setattr__(self, "resolved_at", _utc_datetime(self.resolved_at, "resolved_at"))


__all__ = [
    "Account",
    "AccountBalanceSnapshot",
    "AssetClass",
    "Book",
    "BookAllocation",
    "BrokerCapabilities",
    "BrokerOrder",
    "BrokerOrderEvent",
    "BrokerOrderSnapshot",
    "BrokerOrderStatus",
    "BrokerSnapshot",
    "ExecutionPolicy",
    "ExecutionEvidenceMode",
    "ExecutionEvidenceBaseline",
    "ExecutionSession",
    "FailurePolicy",
    "Fill",
    "Instrument",
    "InstrumentMapping",
    "IntentAction",
    "IntentStatus",
    "IssueSeverity",
    "IssueStatus",
    "LegStatus",
    "LeggingPolicy",
    "MappingPurpose",
    "OrderIntent",
    "OrderLeg",
    "OwnershipClass",
    "PartialFillPolicy",
    "PositionAllocation",
    "PositionSnapshot",
    "QuantityUnit",
    "ReconciliationIssue",
    "ReconciliationRun",
    "ReconciliationStatus",
    "RiskDecisionRecord",
    "RecoveryAction",
    "RecoveryActionStatus",
    "Side",
    "Strategy",
    "TradingEnvironment",
]
