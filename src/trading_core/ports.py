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
    ExecutionEvidenceMode,
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
    """Normalized outcome of a submit, cancel, or replace operation.

    ``cumulative_filled_quantity`` is deliberately optional.  ``None`` means
    that the provider did not give an authoritative cumulative-fill value;
    it is not equivalent to zero.  A caller may claim a clean no-submit or
    no-fill result only with the corresponding explicit assertion flag.  This
    keeps provider response parsing fail-closed instead of treating an
    unrecognized response shape as proof of no broker exposure.
    """

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
    cumulative_filled_quantity: Decimal | None = None
    raw_payload: Mapping[str, Any] = field(default_factory=dict)
    no_submit_asserted: bool = False
    no_fill_asserted: bool = False
    # Provenance for an adapter-authoritative terminal order response.  The
    # generic OMS uses this only to distinguish expected row identity from
    # untrusted nested lifecycle evidence.
    authority: str | None = None
    # Optional normalized identity facts reported by a command response.
    # These are intentionally separate from ``raw_payload`` so the generic
    # OMS can bind a terminal zero-fill claim to the persisted attempt rather
    # than accepting an unscoped provider assertion.
    submitted_quantity: Decimal | None = None
    instrument_id: str | None = None

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
        if self.cumulative_filled_quantity is not None:
            object.__setattr__(
                self,
                "cumulative_filled_quantity",
                _nonnegative_decimal(self.cumulative_filled_quantity, "cumulative_filled_quantity"),
            )
        if self.submitted_quantity is not None:
            object.__setattr__(
                self,
                "submitted_quantity",
                _nonnegative_decimal(self.submitted_quantity, "submitted_quantity"),
            )
            if self.submitted_quantity == 0:
                raise ValueError("submitted_quantity must be positive when provided")
        object.__setattr__(self, "instrument_id", _optional_id(self.instrument_id, "instrument_id"))
        if not isinstance(self.no_submit_asserted, bool):
            raise ValueError("no_submit_asserted must be a bool")
        if not isinstance(self.no_fill_asserted, bool):
            raise ValueError("no_fill_asserted must be a bool")
        object.__setattr__(self, "authority", _optional_id(self.authority, "authority"))
        if self.no_submit_asserted:
            if self.accepted is not False or self.external_order_id is not None or self.ambiguous:
                raise ValueError("no_submit_asserted requires a definite rejected result without an order ID")
        if self.no_fill_asserted:
            if self.cumulative_filled_quantity is None or self.cumulative_filled_quantity != 0:
                raise ValueError("no_fill_asserted requires an authoritative zero cumulative fill")
            if self.ambiguous:
                raise ValueError("no_fill_asserted cannot be asserted on an ambiguous result")
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
    account_id: str | None = None
    evidence_reference: str | None = None
    evidence_mode: ExecutionEvidenceMode = ExecutionEvidenceMode.INDIVIDUAL_DEALS
    # Canonical generic instrument identity supplied by the adapter.  Legacy
    # broker-neutral test/fake feeds may omit it, but a provider row carrying
    # symbol provenance must be represented here so OMS matching can reject
    # a foreign or contradictory instrument before persistence.
    instrument_id: str | None = None

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
        object.__setattr__(self, "account_id", _optional_id(self.account_id, "account_id"))
        object.__setattr__(self, "evidence_reference", _optional_id(self.evidence_reference, "evidence_reference"))
        object.__setattr__(self, "evidence_mode", _enum(self.evidence_mode, ExecutionEvidenceMode, "evidence_mode"))
        object.__setattr__(self, "instrument_id", _optional_id(self.instrument_id, "instrument_id"))
        if self.evidence_reference is None:
            # A provider deal ID is ideal, but the normalized dedupe key is
            # still an exact immutable evidence reference for legacy feeds.
            object.__setattr__(self, "evidence_reference", self.dedupe_key)


@dataclass(frozen=True, slots=True)
class BrokerFactSnapshot:
    """One complete, account-scoped broker-fact observation.

    Empty child collections are meaningful only when ``complete`` is true.
    Adapters must set ``complete=False`` (and preferably ``error``) when any
    required account-wide query is unsupported, partial, or ambiguous.  This
    prevents a safety gate from interpreting an unavailable account as flat.
    ``metadata`` is diagnostic context only; the OMS recursively validates it
    as primitive, non-execution-shaped, and account-consistent before treating
    empty child collections as safe.
    """

    account_id: str
    captured_at: datetime
    complete: bool
    positions: tuple[PositionSnapshot, ...] = ()
    open_orders: tuple[BrokerOrderSnapshot, ...] = ()
    fills: tuple[BrokerFill, ...] = ()
    error: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    # Legacy fake/adapters that predate typed execution evidence retain the
    # historical individual-deal contract when they omit this field.  A
    # provider that truly cannot establish execution evidence must set
    # ``UNAVAILABLE`` explicitly; the OMS safety gate blocks that mode.
    execution_evidence_mode: ExecutionEvidenceMode = ExecutionEvidenceMode.INDIVIDUAL_DEALS
    execution_evidence_scope: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        object.__setattr__(self, "account_id", _required_id(self.account_id, "account_id"))
        object.__setattr__(self, "captured_at", _utc_datetime(self.captured_at, "captured_at"))
        if not isinstance(self.complete, bool):
            raise ValueError("complete must be a bool")
        positions = tuple(self.positions)
        open_orders = tuple(self.open_orders)
        fills = tuple(self.fills)
        if any(not isinstance(item, PositionSnapshot) for item in positions):
            raise ValueError("positions must contain PositionSnapshot values")
        if any(not isinstance(item, BrokerOrderSnapshot) for item in open_orders):
            raise ValueError("open_orders must contain BrokerOrderSnapshot values")
        if any(not isinstance(item, BrokerFill) for item in fills):
            raise ValueError("fills must contain BrokerFill values")
        object.__setattr__(self, "positions", positions)
        object.__setattr__(self, "open_orders", open_orders)
        object.__setattr__(self, "fills", fills)
        if self.error is not None:
            object.__setattr__(self, "error", str(self.error))
        object.__setattr__(self, "metadata", _mapping(self.metadata, "metadata"))
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


@dataclass(frozen=True, slots=True)
class BrokerHistoricalOrderFacts:
    """One explicitly bounded, provider-authoritative order-history window.

    Historical order coverage is deliberately separate from the fresh
    account-facts snapshot used by the submission gate.  An adapter must
    return ``complete=False`` on an unsupported, malformed, or failed history
    query; an empty successful window is represented by ``complete=True``
    with empty ``orders``/``fills``.  This prevents a history transport error
    from being mistaken for proof that no historical executions exist.
    """

    account_id: str
    requested_start: datetime
    requested_end: datetime
    captured_at: datetime
    complete: bool
    orders: tuple[BrokerOrderSnapshot, ...] = ()
    fills: tuple[BrokerFill, ...] = ()
    error: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    execution_evidence_mode: ExecutionEvidenceMode = ExecutionEvidenceMode.UNAVAILABLE
    execution_evidence_scope: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        object.__setattr__(self, "account_id", _required_id(self.account_id, "account_id"))
        start = _utc_datetime(self.requested_start, "requested_start")
        end = _utc_datetime(self.requested_end, "requested_end")
        captured = _utc_datetime(self.captured_at, "captured_at")
        if end <= start:
            raise ValueError("requested_end must be after requested_start")
        object.__setattr__(self, "requested_start", start)
        object.__setattr__(self, "requested_end", end)
        object.__setattr__(self, "captured_at", captured)
        if not isinstance(self.complete, bool):
            raise ValueError("complete must be a bool")
        orders = tuple(self.orders)
        fills = tuple(self.fills)
        if any(not isinstance(item, BrokerOrderSnapshot) for item in orders):
            raise ValueError("orders must contain BrokerOrderSnapshot values")
        if any(not isinstance(item, BrokerFill) for item in fills):
            raise ValueError("fills must contain BrokerFill values")
        object.__setattr__(self, "orders", orders)
        object.__setattr__(self, "fills", fills)
        if self.error is not None:
            object.__setattr__(self, "error", str(self.error))
        object.__setattr__(self, "metadata", _mapping(self.metadata, "metadata"))
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

    def get_account_facts(self, account: Account) -> BrokerFactSnapshot:
        """Return account facts for ordinary reads/display paths.

        Order submission safety gates must call
        :meth:`get_authoritative_account_facts` instead.  This reader may be
        backed by a bounded display cache when an adapter supports one.
        """
        ...

    def get_authoritative_account_facts(self, account: Account) -> BrokerFactSnapshot:
        """Return a freshly queried account fact set for safety-critical gates.

        Implementations must bypass any bounded display/query cache for every
        required account-fact endpoint.  An adapter that cannot obtain a
        complete fresh set must return an incomplete snapshot or raise so the
        generic OMS can persist a blocker rather than treating cached or
        missing facts as a flat account.
        """
        ...

    def submit_order(self, account: Account, request: BrokerSubmitRequest) -> BrokerSubmissionResult:
        ...

    def cancel_order(self, account: Account, external_order_id: str) -> BrokerSubmissionResult:
        ...

    def replace_order(
        self, account: Account, external_order_id: str, changes: Mapping[str, Any]
    ) -> BrokerSubmissionResult:
        ...


@runtime_checkable
class HistoricalOrderEvidenceProvider(Protocol):
    """Optional provider capability for a bounded authoritative order window."""

    def get_historical_order_facts(
        self,
        account: Account,
        start: datetime,
        end: datetime,
    ) -> BrokerHistoricalOrderFacts:
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
    "BrokerFactSnapshot",
    "BrokerFill",
    "BrokerHistoricalOrderFacts",
    "BrokerSubmitRequest",
    "BrokerSubmissionResult",
    "HistoricalOrderEvidenceProvider",
    "MarketBar",
    "MarketDataProvider",
    "MarketQuote",
]
