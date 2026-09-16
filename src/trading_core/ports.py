"""Provider-neutral adapter and market-data ports."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, Mapping, Protocol, Sequence, runtime_checkable

from .domain import (
    Account,
    AccountBalanceSnapshot,
    BrokerCapabilities,
    BrokerOrderSnapshot,
    BrokerOrderStatus,
    BrokerSnapshot,
    ExecutionSession,
    Instrument,
    InstrumentMapping,
    MappingPurpose,
    OrderLeg,
    PositionSnapshot,
    _enum,
    _mapping,
    _normalise_execution_session,
    _nonnegative_decimal,
    _optional_id,
    _positive_int,
    _required_id,
    _utc_datetime,
)


@dataclass(frozen=True, slots=True)
class BrokerSubmitRequest:
    """One persisted broker-order attempt ready for adapter submission."""

    broker_order_id: str
    account_id: str
    broker: str
    order_leg: OrderLeg
    attempt_number: int = 1
    client_order_id: str | None = None
    allow_extended_hours: bool = False
    metadata: Mapping[str, Any] = field(default_factory=dict)
    execution_session: ExecutionSession = ExecutionSession.REGULAR

    def __post_init__(self) -> None:
        object.__setattr__(self, "broker_order_id", _required_id(self.broker_order_id, "broker_order_id"))
        object.__setattr__(self, "account_id", _required_id(self.account_id, "account_id"))
        object.__setattr__(self, "broker", _required_id(self.broker, "broker"))
        if not isinstance(self.order_leg, OrderLeg):
            raise ValueError("order_leg must be an OrderLeg")
        object.__setattr__(self, "attempt_number", _positive_int(self.attempt_number, "attempt_number"))
        object.__setattr__(self, "client_order_id", _optional_id(self.client_order_id, "client_order_id"))
        if not isinstance(self.allow_extended_hours, bool):
            raise ValueError("allow_extended_hours must be a bool")
        object.__setattr__(self, "metadata", _mapping(self.metadata))
        session, allow_extended_hours = _normalise_execution_session(
            self.execution_session,
            self.allow_extended_hours,
        )
        object.__setattr__(self, "execution_session", session)
        object.__setattr__(self, "allow_extended_hours", allow_extended_hours)

@dataclass(frozen=True, slots=True)
class BrokerSubmissionResult:
    """Normalized outcome of a submit, cancel, or replace operation."""

    broker_order_id: str
    accepted: bool | None
    status: BrokerOrderStatus
    external_order_id: str | None = None
    client_order_id: str | None = None
    submitted_at: datetime | None = None
    error_code: str | None = None
    error_message: str | None = None
    retryable: bool = False
    ambiguous: bool = False
    cumulative_filled_quantity: Decimal = Decimal("0")
    raw_payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "broker_order_id", _required_id(self.broker_order_id, "broker_order_id"))
        if self.accepted is not None and not isinstance(self.accepted, bool):
            raise ValueError("accepted must be a bool or None")
        object.__setattr__(self, "status", _enum(self.status, BrokerOrderStatus, "status"))
        object.__setattr__(self, "external_order_id", _optional_id(self.external_order_id, "external_order_id"))
        object.__setattr__(self, "client_order_id", _optional_id(self.client_order_id, "client_order_id"))
        if self.submitted_at is not None:
            object.__setattr__(self, "submitted_at", _utc_datetime(self.submitted_at, "submitted_at"))
        if self.error_code is not None:
            object.__setattr__(self, "error_code", _required_id(self.error_code, "error_code"))
        if self.error_message is not None:
            object.__setattr__(self, "error_message", str(self.error_message))
        if not isinstance(self.retryable, bool):
            raise ValueError("retryable must be a bool")
        if not isinstance(self.ambiguous, bool):
            raise ValueError("ambiguous must be a bool")
        object.__setattr__(
            self,
            "cumulative_filled_quantity",
            _nonnegative_decimal(self.cumulative_filled_quantity, "cumulative_filled_quantity"),
        )
        object.__setattr__(self, "raw_payload", _mapping(self.raw_payload, "raw_payload"))


@dataclass(frozen=True, slots=True)
class BrokerFill:
    """Normalized immutable fill fact returned by a broker adapter."""

    external_order_id: str
    dedupe_key: str
    quantity: Decimal
    price: Decimal
    filled_at: datetime
    received_at: datetime
    external_fill_id: str | None = None
    fee: Decimal | None = None
    fee_currency: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "external_order_id", _required_id(self.external_order_id, "external_order_id"))
        object.__setattr__(self, "dedupe_key", _required_id(self.dedupe_key, "dedupe_key"))
        object.__setattr__(self, "quantity", _nonnegative_decimal(self.quantity, "quantity"))
        if self.quantity == 0:
            raise ValueError("quantity must be positive")
        price = Decimal(self.price)
        if not price.is_finite() or price <= 0:
            raise ValueError("price must be positive and finite")
        object.__setattr__(self, "price", price)
        object.__setattr__(self, "filled_at", _utc_datetime(self.filled_at, "filled_at"))
        object.__setattr__(self, "received_at", _utc_datetime(self.received_at, "received_at"))
        object.__setattr__(self, "external_fill_id", _optional_id(self.external_fill_id, "external_fill_id"))
        if self.fee is not None:
            object.__setattr__(self, "fee", _nonnegative_decimal(self.fee, "fee"))
        object.__setattr__(self, "fee_currency", _optional_id(self.fee_currency, "fee_currency"))
        object.__setattr__(self, "metadata", _mapping(self.metadata, "metadata"))

@runtime_checkable
class BrokerAdapter(Protocol):
    """Capability, read, and order-command boundary for an execution adapter."""

    def get_accounts(self) -> Sequence[Account]:
        ...

    def get_capabilities(self, account: Account) -> BrokerCapabilities:
        ...

    def get_snapshot(self, account: Account) -> BrokerSnapshot:
        ...

    def get_balances(self, account: Account) -> AccountBalanceSnapshot:
        ...

    def get_positions(self, account: Account) -> Sequence[PositionSnapshot]:
        ...

    def get_open_orders(self, account: Account) -> Sequence[BrokerOrderSnapshot]:
        ...

    def get_order(self, account: Account, external_order_id: str) -> BrokerOrderSnapshot | None:
        ...

    def get_fills(self, account: Account, since: datetime | None = None) -> Sequence[BrokerFill]:
        ...

    def submit_order(self, account: Account, request: BrokerSubmitRequest) -> BrokerSubmissionResult:
        ...

    def cancel_order(self, account: Account, external_order_id: str) -> BrokerSubmissionResult:
        ...

    def replace_order(
        self, account: Account, external_order_id: str, changes: Mapping[str, Any]
    ) -> BrokerSubmissionResult:
        ...


@dataclass(frozen=True, slots=True)
class MarketQuote:
    instrument_id: str
    price: Decimal
    source_timestamp: datetime
    received_timestamp: datetime
    provider: str


@dataclass(frozen=True, slots=True)
class MarketBar:
    instrument_id: str
    timeframe: str
    source_timestamp: datetime
    received_timestamp: datetime
    provider: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal | None = None


@runtime_checkable
class MarketDataProvider(Protocol):
    """Research/execution data boundary, intentionally separate from brokers."""

    def get_instrument_mapping(
        self,
        account: Account,
        instrument: Instrument,
        purpose: MappingPurpose = MappingPurpose.MARKET_DATA,
    ) -> InstrumentMapping:
        ...

    def get_quote(self, instrument: Instrument) -> MarketQuote:
        ...

    def get_bars(
        self, instrument: Instrument, timeframe: str, start: datetime, end: datetime
    ) -> Sequence[MarketBar]:
        ...

    def get_corporate_actions(
        self, instrument: Instrument, start: datetime, end: datetime
    ) -> Sequence[Mapping[str, Any]]:
        ...

    def get_earnings(
        self, instrument: Instrument, start: datetime, end: datetime
    ) -> Sequence[Mapping[str, Any]]:
        ...

    def health(self) -> Mapping[str, Any]:
        ...


__all__ = [
    "BrokerAdapter",
    "BrokerFill",
    "BrokerSubmitRequest",
    "BrokerSubmissionResult",
    "MarketBar",
    "MarketDataProvider",
    "MarketQuote",
]
