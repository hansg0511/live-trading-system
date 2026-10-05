"""Broker-neutral Moomoo/OpenD adapter for the generic trading core.

The adapter owns only Moomoo protocol translation.  It never imports the
legacy pair engine or its database, and it requires an explicit mapping
between generic instrument IDs and Moomoo symbols.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import random
import time
from typing import Any, Callable, Protocol, runtime_checkable
from uuid import uuid4
from zoneinfo import ZoneInfo

from src.trading_core.domain import (
    Account,
    AccountBalanceSnapshot,
    ADAPTER_ORDER_SNAPSHOT_AUTHORITY,
    ADAPTER_SUBMISSION_ACK_AUTHORITY,
    BrokerCapabilities,
    BrokerOrderSnapshot,
    BrokerOrderStatus,
    BrokerSnapshot,
    ExecutionEvidenceMode,
    ExecutionSession,
    PositionSnapshot,
    QuantityUnit,
    Side,
    TradingEnvironment,
)
from src.trading_core.ports import (
    BrokerAdapter,
    BrokerFactSnapshot,
    BrokerFill,
    BrokerHistoricalOrderFacts,
    BrokerSubmissionResult,
    BrokerSubmitRequest,
)
from src.trading_core.provider_payload import (
    ProviderPayloadError,
    coerce_provider_payload,
    is_execution_evidence_key,
    normalize_provider_key,
)

from .common import (
    MoomooResponseShapeError,
    as_records,
    canonicalize_payload,
    get_value,
    normalise_env,
    parse_market_auth,
    status_name,
)

try:
    import moomoo as _moomoo  # type: ignore
except (ImportError, OSError, PermissionError):  # pragma: no cover - depends on local SDK install
    _moomoo = None  # type: ignore[assignment]


_ORDER_QUERY_CACHE_SECONDS = 3.5
_BROKER_NAME = "moomoo"
_SUPPORTED_MARKETS = {"US", "HK", "SG", "MY", "JP"}
_MARKET_CURRENCIES = {"US": "USD", "HK": "HKD", "SG": "SGD", "MY": "MYR", "JP": "JPY"}
_MARKET_TIMEZONES = {
    "US": "America/New_York",
    "HK": "Asia/Hong_Kong",
    "SG": "Asia/Singapore",
    "MY": "Asia/Kuala_Lumpur",
    "JP": "Asia/Tokyo",
}
_TERMINAL_STATUSES = {
    BrokerOrderStatus.FILLED,
    BrokerOrderStatus.REJECTED,
    BrokerOrderStatus.CANCELLED,
    BrokerOrderStatus.FAILED,
}
_READ_METHODS = frozenset(
    {
        "get_acc_list",
        "accinfo_query",
        "position_list_query",
        "order_list_query",
        "deal_list_query",
        "history_order_list_query",
    }
)


class MoomooAdapterError(RuntimeError):
    """A definite OpenD protocol, mapping, or account-boundary failure."""


class MoomooRateLimitError(MoomooAdapterError):
    """OpenD/provider read throttling remained after bounded safe retries."""

    retryable = True


class MoomooMappingError(MoomooAdapterError):
    """A broker symbol cannot be attributed to a generic instrument."""


@runtime_checkable
class MoomooInstrumentResolver(Protocol):
    """Explicit, two-way generic-ID to Moomoo-symbol mapping boundary."""

    def symbol_for_instrument(self, instrument_id: str) -> str:
        ...

    def instrument_id_for_symbol(self, external_symbol: str) -> str:
        ...


@dataclass(frozen=True, slots=True)
class StaticMoomooInstrumentResolver:
    """Small in-memory resolver for configuration and adapter-contract tests."""

    instrument_symbols: Mapping[str, str]
    _reverse: Mapping[str, str] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        normalized: dict[str, str] = {}
        reverse: dict[str, str] = {}
        for instrument_id, symbol in self.instrument_symbols.items():
            identifier = str(instrument_id).strip()
            external_symbol = _normalise_symbol(symbol)
            if not identifier:
                raise ValueError("instrument ID must not be empty")
            if external_symbol in reverse and reverse[external_symbol] != identifier:
                raise ValueError(f"Moomoo symbol {external_symbol!r} maps to more than one instrument")
            normalized[identifier] = external_symbol
            reverse[external_symbol] = identifier
        object.__setattr__(self, "instrument_symbols", normalized)
        object.__setattr__(self, "_reverse", reverse)

    def symbol_for_instrument(self, instrument_id: str) -> str:
        identifier = str(instrument_id).strip()
        try:
            return self.instrument_symbols[identifier]
        except KeyError as exc:
            raise MoomooMappingError(f"No Moomoo symbol mapping for instrument {identifier!r}") from exc

    def instrument_id_for_symbol(self, external_symbol: str) -> str:
        symbol = _normalise_symbol(external_symbol)
        try:
            return self._reverse[symbol]
        except KeyError as exc:
            raise MoomooMappingError(f"Unmapped Moomoo symbol {symbol!r}") from exc


def _normalise_symbol(value: Any) -> str:
    symbol = str(value or "").strip().upper()
    if not symbol:
        raise ValueError("Moomoo symbol must not be empty")
    return symbol


def _decimal(value: Any, *, field: str, allow_zero: bool = True) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise MoomooAdapterError(f"Moomoo returned invalid {field}: {value!r}") from exc
    if not result.is_finite() or result < 0 or (not allow_zero and result == 0):
        raise MoomooAdapterError(f"Moomoo returned invalid {field}: {value!r}")
    return result


def _signed_decimal(value: Any, *, field: str) -> Decimal:
    """Parse a finite broker quantity while retaining its reported sign."""

    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise MoomooAdapterError(f"Moomoo returned invalid {field}: {value!r}") from exc
    if not result.is_finite():
        raise MoomooAdapterError(f"Moomoo returned invalid {field}: {value!r}")
    return result


def _optional_decimal(value: Any, *, field: str, allow_zero: bool = True) -> Decimal | None:
    if value is None or str(value).strip() == "":
        return None
    result = _decimal(value, field=field)
    return None if not allow_zero and result == 0 else result


def _optional_balance_decimal(value: Any, *, field: str, allow_zero: bool = True) -> Decimal | None:
    """Normalize optional account-balance fields and provider sentinel text.

    OpenD can report fields that do not apply to an account as strings such as
    ``"N/A"``.  Those fields are useful when present, but their absence must
    not make an otherwise usable account balance unreadable.  Required
    balance fields are parsed with :func:`_decimal` at the call site.
    """
    if value is None or str(value).strip() == "":
        return None
    try:
        result = _decimal(value, field=field)
    except MoomooAdapterError:
        return None
    return None if not allow_zero and result == 0 else result


def _nested_lifecycle_evidence(payload: Any, *, _nested: bool = False) -> bool:
    """Find nested broker order/lifecycle claims without rejecting row identity."""
    lifecycle_statuses = {
        "SUBMITTED", "SUBMITTING", "WAITING_SUBMIT", "WAITING_VERIFY", "PENDING", "WORKING",
        "FILLED", "FILLED_ALL", "FULLY_FILLED", "PARTIALLY_FILLED", "FILLED_PART", "PARTIAL_FILLED",
        "CANCELLED", "CANCELLED_ALL", "CANCELLED_PART", "FILL_CANCELLED", "DELETED", "DISABLED",
        "REJECTED", "SUBMIT_FAILED", "FAILED", "TIMEOUT", "UNKNOWN",
    }
    order_containers = {
        "order", "orders", "broker_order", "order_list", "order_rows", "open_orders",
        "broker_orders", "order_data",
    }
    if not _nested:
        try:
            payload = coerce_provider_payload(payload)
        except ProviderPayloadError:
            return True
    if isinstance(payload, Mapping):
        for raw_key, value in payload.items():
            key = normalize_provider_key(raw_key)
            if key in order_containers and value not in (None, "", 0, "0", (), [], {}):
                return True
            if key in {"status", "state", "order_state", "order_status"} and _nested:
                token = str(value or "").strip().upper().replace("-", "_").split(".")[-1]
                if token in lifecycle_statuses:
                    return True
            if isinstance(value, (Mapping, list, tuple)) and _nested_lifecycle_evidence(value, _nested=True):
                return True
        return False
    if isinstance(payload, (list, tuple)):
        return any(_nested_lifecycle_evidence(item, _nested=True) for item in payload)
    return False


def _raw_fill_evidence(payload: Any) -> bool:
    """Detect fill-shaped provider metadata before asserting a clean zero.

    Moomoo order rows vary by endpoint/version.  Unknown or malformed fields
    that look like deal evidence must remain ambiguous; only an explicitly
    present, parseable zero (or an empty deal collection) is clean no-fill
    evidence.
    """
    try:
        payload = coerce_provider_payload(payload)
    except ProviderPayloadError:
        return True
    if _nested_lifecycle_evidence(payload):
        return True
    quantity_keys = {
        "fill_qty", "filled_qty", "dealt_qty", "filled_quantity",
        "cumulative_filled_quantity", "deal_qty", "deal_quantity",
        "executed_qty", "executed_quantity",
    }
    collection_keys = {"fills", "deals", "fill_list", "deal_list", "deal_detail"}
    if isinstance(payload, Mapping):
        for key, value in payload.items():
            normalized = normalize_provider_key(key)
            if is_execution_evidence_key(normalized) and value not in (None, ""):
                # A nonempty trade/deal/execution identity is material
                # evidence even when dealt_qty is reported as zero.
                return True
            if normalized in quantity_keys:
                try:
                    parsed = Decimal(str(value))
                except (InvalidOperation, TypeError, ValueError):
                    return True
                if not parsed.is_finite() or parsed != 0:
                    return True
                continue
            if normalized in collection_keys and value not in (None, "", 0, "0", (), [], {}):
                return True
            if normalized in collection_keys:
                continue
            if normalized in {
                "fill_price", "dealt_avg_price", "avg_fill_price", "filled_at",
                "fill_time", "deal_time", "executed_at", "execution_time",
            } and value not in (None, ""):
                if normalized in {"fill_price", "dealt_avg_price", "avg_fill_price"}:
                    try:
                        parsed_price = Decimal(str(value))
                        if parsed_price.is_finite() and parsed_price == 0:
                            continue
                    except (InvalidOperation, TypeError, ValueError):
                        pass
                return True
            if any(token in normalized for token in ("fill", "deal", "execut")):
                return True
            if _raw_fill_evidence(value):
                return True
    elif isinstance(payload, (list, tuple)):
        return any(_raw_fill_evidence(item) for item in payload)
    elif payload is not None and not isinstance(payload, (str, int, float, bool, Decimal, datetime)):
        # An opaque SDK/table object cannot be inspected for deal evidence.
        # Treat it as possible exposure; only canonicalized primitive payloads
        # may support a clean no-fill assertion.
        return True
    return False


def _valid_zero_submission_ack(
    row: Mapping[str, Any],
    *,
    expected_quantity: Decimal,
    expected_symbol: str | None,
) -> bool:
    """Validate Moomoo's non-terminal zero-fill place-order acknowledgement."""
    if not isinstance(row, Mapping):
        return False

    def values(aliases: set[str]) -> list[Any]:
        return [value for key, value in row.items() if normalize_provider_key(key) in aliases]

    def exact_decimal(items: list[Any], expected: Decimal) -> bool:
        if not items:
            return False
        parsed: list[Decimal] = []
        for item in items:
            try:
                value = Decimal(str(item))
            except (InvalidOperation, TypeError, ValueError):
                return False
            if not value.is_finite():
                return False
            parsed.append(value)
        return all(value == expected for value in parsed)

    if not exact_decimal(
        values({"dealt_qty", "filled_qty", "filled_quantity", "cumulative_filled_quantity", "fill_qty"}),
        Decimal("0"),
    ):
        return False
    if not exact_decimal(values({"dealt_avg_price", "avg_fill_price", "fill_price"}), Decimal("0")):
        return False
    if not exact_decimal(values({"qty", "quantity"}), expected_quantity):
        return False
    status_values = values({"order_status", "status"})
    status_tokens = {
        str(value or "").strip().upper().replace("-", "_").split(".")[-1]
        for value in status_values
    }
    if not status_tokens or len(status_tokens) != 1 or not status_tokens <= {
        "SUBMITTED", "SUBMITTING", "WAITING_SUBMIT", "WAITING_VERIFY", "PENDING", "WORKING",
    }:
        return False
    if expected_symbol is not None:
        symbols = values({"code", "symbol", "ticker"})
        if not symbols:
            return False
        try:
            if any(_normalise_symbol(value) != _normalise_symbol(expected_symbol) for value in symbols):
                return False
        except (TypeError, ValueError):
            return False
    # Any unrecognized fill/deal/execution field, non-empty deal collection,
    # execution identity, or nested lifecycle/order payload is ambiguous.
    allowed = {
        "dealt_qty", "filled_qty", "filled_quantity", "cumulative_filled_quantity", "fill_qty",
        "dealt_avg_price", "avg_fill_price", "fill_price", "qty", "quantity", "order_status", "status",
        "code", "symbol", "ticker", "order_id", "orderid", "external_order_id", "broker_order_id", "id",
        "fill_outside_rth",
    }
    for key, value in row.items():
        normalized = normalize_provider_key(key)
        if normalized in allowed:
            continue
        if is_execution_evidence_key(normalized):
            return False
        if normalized in {"fills", "deals", "fill_list", "deal_list", "deal_detail"}:
            if value not in (None, "", 0, "0", (), [], {}):
                return False
            continue
        if any(token in normalized for token in ("fill", "deal", "execut")):
            return False
        if _nested_lifecycle_evidence(value, _nested=True):
            return False
    return True


def _environment(value: TradingEnvironment | str) -> TradingEnvironment:
    text = str(value.value if isinstance(value, TradingEnvironment) else value).strip().upper()
    aliases = {"SIMULATE": TradingEnvironment.SIM, "REAL": TradingEnvironment.LIVE}
    if text in aliases:
        return aliases[text]
    return TradingEnvironment(text)


def _order_status(value: Any) -> BrokerOrderStatus:
    status = status_name(value)
    if status in {"FILLED_ALL", "FILLED", "FULLY_FILLED"}:
        return BrokerOrderStatus.FILLED
    if status in {"FILLED_PART", "PARTIALLY_FILLED", "PARTIAL_FILLED", "CANCELLED_PART", "FILL_CANCELLED"}:
        return BrokerOrderStatus.PARTIALLY_FILLED
    if status in {"CANCELLED_ALL", "CANCELLED", "DELETED", "DISABLED"}:
        return BrokerOrderStatus.CANCELLED
    if status in {"REJECTED", "FAILED", "SUBMIT_FAILED", "TIMEOUT"}:
        return BrokerOrderStatus.REJECTED
    if status in {"SUBMITTED", "SUBMITTING", "WAITING_SUBMIT", "WAITING_VERIFY", "PENDING", "WORKING"}:
        return BrokerOrderStatus.WORKING
    return BrokerOrderStatus.UNKNOWN


def _side(value: Any) -> Side:
    normalized = normalise_env(value)
    if normalized in {"BUY", "BUY_BACK", "COVER"}:
        return Side.BUY
    if normalized in {"SELL", "SELL_SHORT", "SHORT"}:
        return Side.SELL
    raise MoomooAdapterError(f"Moomoo returned unsupported trade side: {value!r}")


def _order_quantity_consistency_errors(
    status: BrokerOrderStatus,
    quantity: Decimal,
    filled_quantity: Decimal,
) -> tuple[str, ...]:
    """Validate status/cumulative quantity as one normalized broker fact."""
    if status is BrokerOrderStatus.UNKNOWN:
        return ("unknown order status",)
    if filled_quantity < 0 or filled_quantity > quantity:
        return ("filled quantity is outside submitted quantity",)
    if status in {
        BrokerOrderStatus.PREPARED,
        BrokerOrderStatus.SUBMITTING,
        BrokerOrderStatus.WORKING,
    }:
        return () if filled_quantity == 0 else (f"{status.value} status has nonzero filled quantity",)
    if status is BrokerOrderStatus.PARTIALLY_FILLED:
        return () if 0 < filled_quantity < quantity else ("PARTIALLY_FILLED requires 0 < filled < submitted",)
    if status is BrokerOrderStatus.FILLED:
        return () if filled_quantity == quantity else ("FILLED requires filled == submitted",)
    if status in {BrokerOrderStatus.REJECTED, BrokerOrderStatus.FAILED}:
        return () if filled_quantity == 0 else (f"{status.value} status has positive fill",)
    if status is BrokerOrderStatus.CANCELLED:
        return () if filled_quantity < quantity else ("CANCELLED cannot report a complete fill",)
    return ("unsupported order status",)


def _normalised_key(value: object) -> str:
    return normalize_provider_key(value)


class MooMooGenericAdapter(BrokerAdapter):
    """SIM-only OpenD implementation of the generic :class:`BrokerAdapter`.

    The generic core passes internal IDs.  This adapter obtains the actual
    Moomoo code only through ``instrument_resolver``; it never assumes a
    pairs-trading symbol convention or reads the legacy execution database.
    """

    def __init__(
        self,
        *,
        instrument_resolver: MoomooInstrumentResolver,
        host: str = "127.0.0.1",
        port: int = 11111,
        market: str = "US",
        external_account_id: str | int | None = None,
        environment: TradingEnvironment | str = TradingEnvironment.SIM,
        security_firm: str | None = None,
        sdk_module: Any | None = None,
        trade_context: Any | None = None,
        broker_timezone: str | None = None,
        read_clock: Callable[[], float] | None = None,
        read_sleep: Callable[[float], None] | None = None,
        read_min_interval: float = 0.20,
        read_max_retries: int = 2,
        read_backoff_base: float = 0.25,
        read_jitter: Callable[[float], float] | None = None,
    ) -> None:
        self.host = str(host)
        self.port = int(port)
        self.market = str(market).upper()
        if self.market not in _SUPPORTED_MARKETS:
            raise ValueError(f"unsupported Moomoo market: {self.market}")
        self.environment = _environment(environment)
        if self.environment is not TradingEnvironment.SIM:
            raise PermissionError("The generic Moomoo bridge is SIM-only during Stage 1")
        self.instrument_resolver = instrument_resolver
        self.external_account_id = None if external_account_id is None else str(external_account_id)
        if self.external_account_id is not None:
            try:
                if int(self.external_account_id) <= 0:
                    raise ValueError
            except ValueError as exc:
                raise ValueError("external_account_id must be a positive integer") from exc
        self.security_firm = security_firm
        self._sdk = _moomoo if sdk_module is None else sdk_module
        self.trade_context = trade_context
        self._connected = False
        self._selected_account_id: str | None = None
        self._account_rows: list[dict[str, Any]] = []
        self._order_query_cache: tuple[float, list[dict[str, Any]]] | None = None
        self._last_fill_history_unsupported = False
        self._last_execution_evidence_mode = ExecutionEvidenceMode.UNAVAILABLE
        self._last_execution_evidence_scope: frozenset[str] = frozenset()
        if read_min_interval < 0:
            raise ValueError("read_min_interval must be non-negative")
        if read_max_retries < 0:
            raise ValueError("read_max_retries must be non-negative")
        if read_backoff_base <= 0:
            raise ValueError("read_backoff_base must be positive")
        self._read_clock = read_clock or time.monotonic
        self._read_sleep = read_sleep or time.sleep
        self._read_min_interval = float(read_min_interval)
        self._read_max_retries = int(read_max_retries)
        self._read_backoff_base = float(read_backoff_base)
        self._read_jitter = read_jitter or (lambda maximum: random.uniform(0.0, maximum))
        self._last_read_at: float | None = None
        self._last_read_error: MoomooAdapterError | None = None
        try:
            self._broker_timezone = ZoneInfo(broker_timezone or _MARKET_TIMEZONES[self.market])
        except Exception as exc:
            raise ValueError(f"invalid broker_timezone: {broker_timezone!r}") from exc

    @property
    def selected_external_account_id(self) -> str | None:
        return self._selected_account_id

    def connect(self) -> bool:
        """Open OpenD and select one safe SIM margin/securities account."""
        if self.trade_context is None:
            if self._sdk is None:
                raise ImportError("moomoo-api is not installed; install it before connecting the generic adapter")
            kwargs: dict[str, Any] = {
                "host": self.host,
                "port": self.port,
                "is_encrypt": False,
                "filter_trdmarket": getattr(getattr(self._sdk, "TrdMarket", None), "NONE", "NONE"),
            }
            if self.security_firm and hasattr(self._sdk, "SecurityFirm"):
                kwargs["security_firm"] = getattr(self._sdk.SecurityFirm, self.security_firm, self.security_firm)
            try:
                self.trade_context = self._sdk.OpenSecTradeContext(**kwargs)
            except TypeError:
                kwargs.pop("security_firm", None)
                self.trade_context = self._sdk.OpenSecTradeContext(**kwargs)

        rows = self._call("get_acc_list")
        self._account_rows = rows
        eligible = self._eligible_account_rows(rows)
        if self.external_account_id is not None:
            selected = [row for row in eligible if str(get_value(row, "acc_id", "account_id", default="")) == self.external_account_id]
            if not selected:
                raise PermissionError(
                    f"Configured SIM account {self.external_account_id} is not an active authorized {self.market} margin account"
                )
            self._selected_account_id = self.external_account_id
        elif len(eligible) == 1:
            self._selected_account_id = str(get_value(eligible[0], "acc_id", "account_id"))
        elif not eligible:
            raise RuntimeError(f"No safe SIM account is authorized for Moomoo {self.market} trading")
        else:
            raise PermissionError("More than one safe SIM account is available; configure external_account_id explicitly")
        self._connected = True
        return True

    def disconnect(self) -> bool:
        if self.trade_context is not None:
            try:
                self.trade_context.close()
            except Exception:
                pass
        self._connected = False
        self._order_query_cache = None
        return True

    def get_accounts(self) -> Sequence[Account]:
        self._require_connected()
        accounts: list[Account] = []
        now = datetime.now(timezone.utc)
        for row in self._eligible_account_rows(self._account_rows):
            external_id = str(get_value(row, "acc_id", "account_id"))
            accounts.append(
                Account(
                    id=f"moomoo:sim:{external_id}",
                    broker=_BROKER_NAME,
                    environment=TradingEnvironment.SIM,
                    external_account_id=external_id,
                    base_currency=_MARKET_CURRENCIES[self.market],
                    metadata={"market": self.market, "selected": external_id == self._selected_account_id},
                    created_at=now,
                    updated_at=now,
                )
            )
        return tuple(accounts)

    def get_capabilities(self, account: Account) -> BrokerCapabilities:
        self._validate_account(account)
        features = {"EQUITY", "SIM", "OPEN_D", "ORDER_REPLACE"}
        if self.market == "US":
            features.add("EXTENDED_HOURS_LIMIT")
            features.add("OVERNIGHT_LIMIT")
        return BrokerCapabilities(
            broker=_BROKER_NAME,
            features=frozenset(features),
            supports_balance_read=True,
            supports_position_read=True,
            supports_order_read=True,
            supports_fill_read=True,
            execution_evidence_mode=ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS,
            execution_evidence_scope=frozenset({"CURRENT_ORDER_SNAPSHOTS", "HISTORICAL_ORDER_SNAPSHOTS"}),
            supports_order_events=False,
            supports_submit=True,
            supports_cancel=True,
            supports_replace=True,
            supports_native_multi_leg=False,
            supports_client_order_id=False,
            supports_idempotent_submit=False,
            metadata={"market": self.market, "environment": self.environment.value},
        )

    def get_snapshot(self, account: Account) -> BrokerSnapshot:
        self._validate_account(account)
        rows = self._call("accinfo_query", **self._account_kwargs(refresh_cache=True))
        if not rows:
            raise MoomooAdapterError("Moomoo accinfo_query returned no account data")
        return BrokerSnapshot(
            id=self._snapshot_id(account),
            account_id=account.id,
            captured_at=datetime.now(timezone.utc),
            status="COMPLETE",
            metadata={"endpoint": "accinfo_query", "row_count": len(rows), "consistency": "per-endpoint"},
        )

    def get_balances(self, account: Account) -> AccountBalanceSnapshot:
        self._validate_account(account)
        rows = self._call("accinfo_query", **self._account_kwargs(refresh_cache=True))
        if not rows:
            raise MoomooAdapterError("Moomoo accinfo_query returned no account data")
        row = rows[0]
        snapshot_id = self._snapshot_id(account)
        return AccountBalanceSnapshot(
            id=f"{snapshot_id}:balance",
            broker_snapshot_id=snapshot_id,
            currency=account.base_currency,
            cash=_decimal(get_value(row, "cash", "avail_cash", "available_cash"), field="cash"),
            buying_power=_decimal(get_value(row, "buying_power", "power", "available_power"), field="buying_power"),
            equity=_optional_balance_decimal(get_value(row, "equity", "total_assets", "asset_value"), field="equity"),
            initial_margin=_optional_balance_decimal(get_value(row, "initial_margin", "used_initial_margin", "margin_used"), field="initial_margin"),
            maintenance_margin=_optional_balance_decimal(get_value(row, "maintenance_margin", "used_maintenance_margin"), field="maintenance_margin"),
            metadata={"raw": dict(row)},
        )

    def get_positions(self, account: Account) -> Sequence[PositionSnapshot]:
        self._validate_account(account)
        snapshot_id = self._snapshot_id(account)
        captured_at = datetime.now(timezone.utc)
        rows = self._call("position_list_query", **self._account_kwargs(refresh_cache=True))
        positions: list[PositionSnapshot] = []
        for index, row in enumerate(rows):
            self._validate_position_row_aliases(row)
            symbol = _normalise_symbol(get_value(row, "code", "symbol", "ticker"))
            quantity = _signed_decimal(get_value(row, "qty", "quantity", "position"), field="position quantity")
            direction = normalise_env(get_value(row, "position_side", "position_type", "side", default=""))
            if direction in {"SHORT", "SELL", "SELL_SHORT"}:
                # OpenD has returned both unsigned SHORT quantities and
                # already-signed negative quantities across SIM endpoints.
                # Treat either representation as the same short exposure,
                # while retaining the sign for consistency checks below.
                signed_quantity = -abs(quantity)
            elif direction in {"LONG", "BUY"}:
                if quantity < 0:
                    raise MoomooAdapterError(
                        f"Moomoo position {symbol} reports a negative quantity for LONG direction"
                    )
                signed_quantity = quantity
            else:
                raise MoomooAdapterError(f"Moomoo position {symbol} has an unknown direction: {direction!r}")
            if signed_quantity == 0:
                continue
            positions.append(
                PositionSnapshot(
                    id=f"{snapshot_id}:position:{get_value(row, 'position_id', 'id', default=index)}",
                    broker_snapshot_id=snapshot_id,
                    account_id=account.id,
                    instrument_id=self.instrument_resolver.instrument_id_for_symbol(symbol),
                    signed_quantity=signed_quantity,
                    average_price=_optional_decimal(
                        get_value(row, "average_price", "position_avg_price", "cost_price"),
                        field="average_price",
                        allow_zero=False,
                    ),
                    captured_at=captured_at,
                    metadata={"external_symbol": symbol, "broker_side": direction, "raw": dict(row)},
                )
            )
        return tuple(positions)

    def get_open_orders(self, account: Account) -> Sequence[BrokerOrderSnapshot]:
        return self._get_open_orders(account, force_refresh=False)

    def _get_open_orders(
        self,
        account: Account,
        *,
        force_refresh: bool,
        rows: Sequence[Mapping[str, Any]] | None = None,
    ) -> tuple[BrokerOrderSnapshot, ...]:
        self._validate_account(account)
        snapshot_id = self._snapshot_id(account)
        captured_at = datetime.now(timezone.utc)
        validated_rows = self._validated_order_rows(
            account,
            self._query_orders(force_refresh=force_refresh) if rows is None else rows,
        )
        return tuple(
            self._order_snapshot(account, snapshot_id, captured_at, row)
            for row in validated_rows
            if _order_status(get_value(row, "order_status", "status", default="")) not in _TERMINAL_STATUSES
        )

    def get_order(self, account: Account, external_order_id: str) -> BrokerOrderSnapshot | None:
        self._validate_account(account)
        identifier = str(external_order_id).strip()
        rows = self._validated_order_rows(account, self._query_orders())
        for row in rows:
            if str(get_value(row, "order_id", "id", "orderid", default="")) == identifier:
                return self._order_snapshot(account, self._snapshot_id(account), datetime.now(timezone.utc), row)
        return None

    def get_fills(self, account: Account, since: datetime | None = None) -> Sequence[BrokerFill]:
        return self._get_fills(account, since=since, force_refresh=False)

    def get_historical_order_facts(
        self,
        account: Account,
        start: datetime,
        end: datetime,
    ) -> BrokerHistoricalOrderFacts:
        """Return a bounded, normalized historical order-evidence window.

        Moomoo's SIM deal endpoint is unavailable, but its historical order
        endpoint exposes terminal order economics.  This method keeps that
        endpoint separate from the fresh account gate: positions and current
        open orders remain queried by ``get_authoritative_account_facts``.
        History query errors are represented as an incomplete result, never
        as an empty successful window.
        """

        self._validate_account(account)
        if start.tzinfo is None or start.utcoffset() is None:
            raise ValueError("start must be timezone-aware")
        if end.tzinfo is None or end.utcoffset() is None:
            raise ValueError("end must be timezone-aware")
        start_utc = start.astimezone(timezone.utc)
        end_utc = end.astimezone(timezone.utc)
        if end_utc <= start_utc:
            raise ValueError("end must be after start")
        captured_at = datetime.now(timezone.utc)
        query_start = start_utc.astimezone(self._broker_timezone).strftime("%Y-%m-%d %H:%M:%S")
        query_end = end_utc.astimezone(self._broker_timezone).strftime("%Y-%m-%d %H:%M:%S")
        metadata = {
            "source": "history_order_list_query",
            "requested_start": start_utc.isoformat(),
            "requested_end": end_utc.isoformat(),
            "query_start": query_start,
            "query_end": query_end,
            "captured_at": captured_at.isoformat(),
        }
        try:
            rows = self._call(
                "history_order_list_query",
                **self._account_kwargs(),
                start=query_start,
                end=query_end,
            )
            validated_rows = self._validated_order_rows(account, rows)
            snapshot_id = f"moomoo:{account.external_account_id}:history:{uuid4()}"
            orders = tuple(
                self._order_snapshot(account, snapshot_id, captured_at, row)
                for row in validated_rows
            )
            fills = self._filled_order_fallback_fills(
                account=account,
                rows=validated_rows,
                since=start_utc,
                until=end_utc,
                reason="history_order_list_query",
                source="history_order_list_query",
                evidence_scope="HISTORICAL_ORDER_SNAPSHOTS",
            )
        except Exception as exc:
            return BrokerHistoricalOrderFacts(
                account_id=account.id,
                requested_start=start_utc,
                requested_end=end_utc,
                captured_at=captured_at,
                complete=False,
                error=str(exc),
                metadata=metadata,
                execution_evidence_mode=ExecutionEvidenceMode.UNAVAILABLE,
                execution_evidence_scope=frozenset({"HISTORICAL_ORDER_SNAPSHOTS"}),
            )
        return BrokerHistoricalOrderFacts(
            account_id=account.id,
            requested_start=start_utc,
            requested_end=end_utc,
            captured_at=captured_at,
            complete=True,
            orders=orders,
            fills=fills,
            metadata={**metadata, "row_count": len(orders)},
            execution_evidence_mode=ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS,
            execution_evidence_scope=frozenset({"HISTORICAL_ORDER_SNAPSHOTS"}),
        )

    def _get_fills(
        self,
        account: Account,
        since: datetime | None = None,
        *,
        force_refresh: bool,
        order_rows: Sequence[Mapping[str, Any]] | None = None,
    ) -> tuple[BrokerFill, ...]:
        self._validate_account(account)
        self._last_fill_history_unsupported = False
        self._last_execution_evidence_mode = ExecutionEvidenceMode.UNAVAILABLE
        self._last_execution_evidence_scope = frozenset()
        if since is not None and (since.tzinfo is None or since.utcoffset() is None):
            raise ValueError("since must be timezone-aware")
        try:
            rows = self._call("deal_list_query", **self._account_kwargs(refresh_cache=True))
        except MoomooAdapterError as exc:
            # Moomoo SIM currently rejects deal_list_query outright.  The
            # order query still contains authoritative terminal FILLED_ALL
            # rows, so use those rows only when the provider explicitly says
            # deal history is unsupported.  Other query failures remain hard
            # errors and therefore keep OMS recovery fail-closed.
            if not self._deal_history_unsupported(exc):
                raise
            self._last_fill_history_unsupported = True
            self._last_execution_evidence_mode = ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS
            self._last_execution_evidence_scope = frozenset({"CURRENT_ORDER_SNAPSHOTS"})
            return self._filled_order_fallback_fills(
                account=account,
                rows=order_rows,
                since=since,
                reason=str(exc),
                force_refresh=force_refresh,
            )
        received_at = datetime.now(timezone.utc)
        self._last_execution_evidence_mode = ExecutionEvidenceMode.INDIVIDUAL_DEALS
        self._last_execution_evidence_scope = frozenset({"CURRENT_DEALS"})
        fills: list[BrokerFill] = []
        for row in rows:
            self._validate_fill_row_aliases(row)
            external_order_id = str(get_value(row, "order_id", "orderid", default="")).strip()
            if not external_order_id:
                raise MoomooAdapterError("Moomoo fill did not contain an order_id")
            quantity = _decimal(get_value(row, "qty", "dealt_qty", "filled_qty"), field="fill quantity", allow_zero=False)
            price = _decimal(get_value(row, "price", "fill_price", "dealt_avg_price"), field="fill price", allow_zero=False)
            filled_at = self._timestamp(get_value(row, "create_time", "updated_time", "fill_time"))
            if since is not None and filled_at < since.astimezone(timezone.utc):
                continue
            external_fill_id = str(get_value(row, "deal_id", "fill_id", default="")).strip() or None
            dedupe_key = external_fill_id or hashlib.sha256(
                f"{external_order_id}|{filled_at.isoformat()}|{quantity}|{price}".encode("utf-8")
            ).hexdigest()
            fills.append(
                BrokerFill(
                    external_order_id=external_order_id,
                    external_fill_id=external_fill_id,
                    dedupe_key=dedupe_key,
                    quantity=quantity,
                    price=price,
                    filled_at=filled_at,
                    received_at=received_at,
                    account_id=account.id,
                    evidence_reference=f"{account.id}:deal:{external_fill_id or dedupe_key}",
                    evidence_mode=ExecutionEvidenceMode.INDIVIDUAL_DEALS,
                    instrument_id=self.instrument_resolver.instrument_id_for_symbol(
                        _normalise_symbol(get_value(row, "code", "symbol", "ticker"))
                    ),
                    metadata={"external_symbol": get_value(row, "code", "symbol", "ticker"), "raw": dict(row)},
                )
            )
        return tuple(fills)

    def get_account_facts(self, account: Account) -> BrokerFactSnapshot:
        # The generic account-facts reader is itself safety-sensitive.  Keep
        # it fresh even for callers that do not use the explicit method below.
        return self._get_account_facts(account, force_refresh=True)

    def get_authoritative_account_facts(self, account: Account) -> BrokerFactSnapshot:
        """Query account facts without using the bounded order-query cache."""
        return self._get_account_facts(account, force_refresh=True)

    def _get_account_facts(self, account: Account, *, force_refresh: bool) -> BrokerFactSnapshot:
        """Query a complete account fact set for the generic safety gate.

        The three child queries are intentionally all-or-nothing.  An empty
        tuple is safe only after every query has returned successfully; an
        unsupported or ambiguous endpoint produces an explicitly incomplete
        result instead of being mistaken for a flat account.
        """
        self._validate_account(account)
        captured_at = datetime.now(timezone.utc)
        try:
            # One authoritative order query feeds both the open-order view and
            # the bounded cumulative-fill fallback.  This keeps a recovery
            # cycle from issuing two near-identical OpenD reads and makes the
            # adapter's retry/throttle policy the single read boundary.
            order_rows = self._query_orders(force_refresh=force_refresh)
            positions = tuple(self.get_positions(account))
            open_orders = tuple(
                self._get_open_orders(account, force_refresh=force_refresh, rows=order_rows)
            )
            fills = tuple(
                self._get_fills(
                    account,
                    since=None,
                    force_refresh=force_refresh,
                    order_rows=order_rows,
                )
            )
        except Exception as exc:
            return BrokerFactSnapshot(
                account_id=account.id,
                captured_at=captured_at,
                complete=False,
                error=str(exc),
                metadata={"source": "moomoo_account_fact_queries"},
            )
        fact_metadata: dict[str, Any] = {
            "source": "moomoo_account_fact_queries",
            "query_complete": True,
        }
        if self._last_fill_history_unsupported:
            # Fallback order evidence is useful for recovery, but an
            # unsupported deal-history endpoint remains an account-wide
            # uncertainty and must block fresh submission until reconciled.
            fact_metadata["fill_history_unsupported"] = True
        return BrokerFactSnapshot(
            account_id=account.id,
            captured_at=captured_at,
            complete=True,
            positions=positions,
            open_orders=open_orders,
            fills=fills,
            metadata=fact_metadata,
            execution_evidence_mode=self._last_execution_evidence_mode,
            execution_evidence_scope=self._last_execution_evidence_scope,
        )

    @staticmethod
    def _deal_history_unsupported(error: MoomooAdapterError) -> bool:
        text = str(error).strip().lower()
        # A malformed tabular response may itself mention an "unsupported"
        # row type.  That is not the documented SIM deal-history limitation
        # and must never be converted into order-derived fill evidence.
        if "returned malformed response" in text:
            return False
        return (
            "deal_list_query" in text
            and "support" in text
            and any(marker in text for marker in ("not support", "unsupported", "does not support"))
        )

    def _filled_order_fallback_fills(
        self,
        *,
        account: Account,
        rows: Sequence[Mapping[str, Any]] | None = None,
        since: datetime | None,
        until: datetime | None = None,
        reason: str,
        source: str = "order_list_query",
        evidence_scope: str = "CURRENT_ORDER_SNAPSHOTS",
        force_refresh: bool = False,
    ) -> tuple[BrokerFill, ...]:
        """Synthesize one conservative fill fact per fully filled order.

        This is deliberately narrower than treating an order status as a
        fill: the provider must report a terminal FILLED status, positive
        dealt quantity equal to the submitted quantity, a positive dealt
        average price, and a parseable order timestamp.
        """

        received_at = datetime.now(timezone.utc)
        fills: list[BrokerFill] = []
        candidate_rows = self._query_orders(force_refresh=force_refresh) if rows is None else rows
        for row in self._validated_order_rows(account, candidate_rows):
            if _order_status(get_value(row, "order_status", "status", default="")) is not BrokerOrderStatus.FILLED:
                continue
            external_order_id = str(get_value(row, "order_id", "orderid", "id", default="")).strip()
            if not external_order_id:
                raise MoomooAdapterError("Moomoo filled order did not contain an order_id")
            quantity = _decimal(get_value(row, "qty", "quantity"), field="order quantity", allow_zero=False)
            dealt_quantity = _decimal(
                get_value(row, "dealt_qty", "filled_qty"),
                field="filled quantity",
                allow_zero=False,
            )
            if dealt_quantity != quantity:
                raise MoomooAdapterError(
                    f"Moomoo order {external_order_id} is FILLED but dealt quantity is not complete"
                )
            price = _decimal(
                get_value(row, "dealt_avg_price", "avg_fill_price", "fill_price"),
                field="average fill price",
                allow_zero=False,
            )
            filled_at = self._timestamp(
                get_value(row, "updated_time", "create_time", "fill_time", "order_time")
            )
            if since is not None and filled_at < since.astimezone(timezone.utc):
                continue
            if until is not None and filled_at >= until.astimezone(timezone.utc):
                continue
            # The generic repository binds fallback fill evidence to the
            # persisted attempt using ``external_order_id:dedupe_key``.
            # Keep provider scope in metadata, but emit that exact
            # attempt-scoped reference for the normalized fact.
            evidence_reference = f"{external_order_id}:moomoo-order-fill:{external_order_id}"
            fills.append(
                BrokerFill(
                    external_order_id=external_order_id,
                    external_fill_id=None,
                    dedupe_key=f"moomoo-order-fill:{external_order_id}",
                    quantity=dealt_quantity,
                    price=price,
                    filled_at=filled_at,
                    received_at=received_at,
                    account_id=account.id,
                    evidence_reference=evidence_reference,
                    evidence_mode=ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS,
                    instrument_id=self.instrument_resolver.instrument_id_for_symbol(
                        _normalise_symbol(get_value(row, "code", "symbol", "ticker"))
                    ),
                    metadata={
                        "source": source,
                        "synthetic": True,
                        "evidence_mode": ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS.value,
                        "evidence_reference": evidence_reference,
                        "evidence_scope": evidence_scope,
                        "reason": reason,
                        "raw": dict(row),
                    },
                )
            )
        return tuple(fills)

    def submit_order(self, account: Account, request: BrokerSubmitRequest) -> BrokerSubmissionResult:
        self._validate_request(account, request)
        leg = request.order_leg
        if leg.quantity_unit is not QuantityUnit.UNITS:
            return self._rejected(request, "UNSUPPORTED_QUANTITY_UNIT", "Stage 1 Moomoo bridge supports unit quantities only")
        if leg.stop_price is not None or leg.time_in_force is not None:
            return self._rejected(request, "UNSUPPORTED_ORDER_PARAMETER", "stop prices and time-in-force are not yet supported")
        if leg.order_type not in {"MARKET", "LIMIT"}:
            return self._rejected(request, "UNSUPPORTED_ORDER_TYPE", f"unsupported Moomoo order type {leg.order_type}")
        if leg.order_type == "LIMIT" and leg.limit_price is None:
            return self._rejected(request, "MISSING_LIMIT_PRICE", "LIMIT orders require limit_price")
        session = request.execution_session
        if session is ExecutionSession.EXTENDED and self.market != "US":
            return self._rejected(
                request,
                "EXTENDED_HOURS_UNSUPPORTED_MARKET",
                "Stage 1 extended-hours execution is implemented for US equities only",
            )
        if session is ExecutionSession.EXTENDED and leg.order_type != "LIMIT":
            return self._rejected(
                request,
                "EXTENDED_HOURS_LIMIT_ONLY",
                "Moomoo does not support market orders during US pre/post-market sessions",
            )
        if session is ExecutionSession.OVERNIGHT and self.market != "US":
            return self._rejected(
                request,
                "OVERNIGHT_UNSUPPORTED_MARKET",
                "Moomoo overnight execution is implemented for US equities only",
            )
        if session is ExecutionSession.OVERNIGHT and leg.order_type != "LIMIT":
            return self._rejected(
                request,
                "OVERNIGHT_LIMIT_ONLY",
                "Moomoo overnight execution supports US LIMIT orders only",
            )

        sdk = self._require_sdk_for_command()
        symbol = self.instrument_resolver.symbol_for_instrument(leg.instrument_id)
        kwargs: dict[str, Any] = {
            "price": 0.0 if leg.order_type == "MARKET" else float(leg.limit_price),
            "qty": float(leg.quantity),
            "code": symbol,
            "trd_side": self._sdk_side(sdk, leg.side),
            "order_type": self._sdk_order_type(sdk, leg.order_type),
            **self._account_kwargs(),
        }
        if request.client_order_id:
            kwargs["remark"] = request.client_order_id
        if session is ExecutionSession.EXTENDED:
            kwargs["fill_outside_rth"] = True
        elif session is ExecutionSession.OVERNIGHT:
            kwargs["session"] = self._sdk_session(sdk, session)
        ret, data = self.trade_context.place_order(**kwargs)
        self._invalidate_order_query_cache()
        if ret != 0:
            payload = self._as_payload(data, method="place_order")
            mismatches = self._submit_response_mismatches(
                request,
                payload,
                account=account,
                expected_symbol=symbol,
            )
            if mismatches:
                return BrokerSubmissionResult(
                    broker_order_id=request.broker_order_id,
                    accepted=None,
                    status=BrokerOrderStatus.UNKNOWN,
                    client_order_id=request.client_order_id,
                    ambiguous=True,
                    error_code="MOOMOO_SUBMIT_RESPONSE_CONFLICT",
                    error_message=(
                        "Moomoo submit failure response carried conflicting or extra order/account facts: "
                        f"{sorted(set(mismatches))}"
                    ),
                    raw_payload={"response": payload},
                )
            return self._rejected(
                request,
                "MOOMOO_PLACE_ORDER_FAILED",
                str(data),
                raw_payload={"response": payload},
                no_submit_asserted=False,
                no_fill_asserted=False,
                cumulative_filled_quantity=None,
            )
        return self._command_result(
            request,
            data,
            default_status=BrokerOrderStatus.WORKING,
            account=account,
            expected_symbol=symbol,
        )

    def cancel_order(self, account: Account, external_order_id: str) -> BrokerSubmissionResult:
        self._validate_account(account)
        sdk = self._require_sdk_for_command()
        identifier = str(external_order_id).strip()
        if not identifier:
            raise ValueError("external_order_id is required")
        ret, data = self.trade_context.modify_order(
            modify_order_op=getattr(getattr(sdk, "ModifyOrderOp", None), "CANCEL", "CANCEL"),
            order_id=self._sdk_order_id(identifier),
            qty=0,
            price=0,
            **self._account_kwargs(),
        )
        self._invalidate_order_query_cache()
        if ret != 0:
            payload = self._as_payload(data, method="modify_order")
            mismatches = self._response_account_mismatches(account, payload)
            rows = self._as_records(payload, method="modify_order")
            if rows:
                if len(rows) != 1:
                    mismatches.append(f"response.row_count={len(rows)}")
                for index, row in enumerate(rows):
                    try:
                        self._validate_order_row_aliases(row)
                    except MoomooAdapterError as exc:
                        mismatches.append(f"response[{index}].alias_conflict={exc}")
                    row_order_id = str(get_value(row, "order_id", "id", "orderid", default="")).strip()
                    if not row_order_id:
                        mismatches.append(f"response[{index}].order_id_missing")
                    elif row_order_id != identifier:
                        mismatches.append(f"response[{index}].order_id={row_order_id}")
                    mismatches.extend(
                        f"response[{index}].{item}"
                        for item in self._response_account_mismatches(account, row)
                    )
            return BrokerSubmissionResult(
                broker_order_id=f"cancel:{identifier}",
                accepted=None if mismatches else False,
                status=BrokerOrderStatus.UNKNOWN if mismatches else BrokerOrderStatus.FAILED,
                external_order_id=identifier,
                ambiguous=bool(mismatches),
                error_code="MOOMOO_CANCEL_IDENTITY_CONFLICT" if mismatches else "MOOMOO_CANCEL_FAILED",
                error_message=(f"Moomoo cancel response identity conflict: {mismatches}" if mismatches else str(payload)),
                raw_payload={"response": payload},
            )
        payload = self._as_payload(data, method="modify_order")
        rows = self._as_records(payload, method="modify_order")
        mismatches = self._response_account_mismatches(account, payload)
        returned_ids: list[str] = []
        if len(rows) != 1:
            mismatches.append(f"response.row_count={len(rows)}")
        for index, row in enumerate(rows):
            try:
                self._validate_order_row_aliases(row)
            except MoomooAdapterError as exc:
                mismatches.append(f"response[{index}].alias_conflict={exc}")
            returned_order_id = str(get_value(row, "order_id", "id", "orderid", default="")).strip()
            if not returned_order_id:
                mismatches.append(f"response[{index}].order_id_missing")
            else:
                returned_ids.append(returned_order_id)
                if returned_order_id != identifier:
                    mismatches.append(f"response[{index}].order_id={returned_order_id}")
            # Validate every row independently.  A top-level account alias
            # check is not sufficient when the SDK returns multiple records.
            mismatches.extend(
                f"response[{index}].{item}"
                for item in self._response_account_mismatches(account, row)
            )
        returned_order_id = returned_ids[0] if len(returned_ids) == 1 else ""
        if mismatches:
            return BrokerSubmissionResult(
                broker_order_id=f"cancel:{identifier}",
                accepted=None,
                status=BrokerOrderStatus.UNKNOWN,
                external_order_id=returned_order_id or identifier,
                ambiguous=True,
                error_code="MOOMOO_CANCEL_IDENTITY_CONFLICT",
                error_message=f"Moomoo cancel response could not be bound to requested account/order: {sorted(set(mismatches))}",
                raw_payload={"response": payload},
            )
        return self._command_result_for_external(f"cancel:{identifier}", identifier, payload)

    def replace_order(self, account: Account, external_order_id: str, changes: Mapping[str, Any]) -> BrokerSubmissionResult:
        self._validate_account(account)
        identifier = str(external_order_id).strip()
        unknown = set(changes) - {"quantity", "limit_price"}
        if unknown or set(changes) != {"quantity", "limit_price"}:
            return BrokerSubmissionResult(
                broker_order_id=f"replace:{identifier}", accepted=False, status=BrokerOrderStatus.REJECTED,
                external_order_id=identifier, error_code="UNSUPPORTED_REPLACE", error_message="replace requires quantity and limit_price only",
            )
        quantity = _decimal(changes["quantity"], field="replacement quantity", allow_zero=False)
        price = _decimal(changes["limit_price"], field="replacement limit_price", allow_zero=False)
        sdk = self._require_sdk_for_command()
        ret, data = self.trade_context.modify_order(
            modify_order_op=getattr(getattr(sdk, "ModifyOrderOp", None), "MODIFY", "MODIFY"),
            order_id=self._sdk_order_id(identifier), qty=float(quantity), price=float(price), **self._account_kwargs(),
        )
        self._invalidate_order_query_cache()
        if ret != 0:
            payload = self._as_payload(data, method="modify_order")
            return BrokerSubmissionResult(
                broker_order_id=f"replace:{identifier}", accepted=False, status=BrokerOrderStatus.FAILED,
                external_order_id=identifier, error_code="MOOMOO_REPLACE_FAILED", error_message=str(payload), raw_payload={"response": payload},
            )
        payload = self._as_payload(data, method="modify_order")
        return self._command_result_for_external(f"replace:{identifier}", identifier, payload)

    def _eligible_account_rows(self, rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        eligible: list[dict[str, Any]] = []
        for row in rows:
            if normalise_env(get_value(row, "trd_env", "trading_env")) != "SIMULATE":
                continue
            if not get_value(row, "acc_id", "account_id"):
                continue
            if normalise_env(get_value(row, "acc_status", "status")) != "ACTIVE":
                continue
            if normalise_env(get_value(row, "acc_role", "role")) == "MASTER":
                continue
            if self.market not in parse_market_auth(get_value(row, "trdmarket_auth", "market_auth")):
                continue
            if normalise_env(get_value(row, "acc_type", "account_type")) not in {"MARGIN", "STOCK_AND_OPTION"}:
                continue
            eligible.append(dict(row))
        return eligible

    def _validate_account(self, account: Account) -> None:
        self._require_connected()
        if account.broker.lower() != _BROKER_NAME:
            raise PermissionError(f"account broker {account.broker!r} does not match Moomoo")
        if account.environment is not TradingEnvironment.SIM:
            raise PermissionError("generic Moomoo adapter accepts SIM accounts only during Stage 1")
        if account.external_account_id != self._selected_account_id:
            raise PermissionError("generic account identity does not match selected Moomoo SIM account")

    def _validate_request(self, account: Account, request: BrokerSubmitRequest) -> None:
        self._validate_account(account)
        if request.account_id != account.id:
            raise PermissionError("broker submit request does not match generic account")
        if request.broker.lower() != _BROKER_NAME:
            raise PermissionError("broker submit request does not target Moomoo")

    def _require_connected(self) -> None:
        if not self._connected or self.trade_context is None or self._selected_account_id is None:
            raise ConnectionError("MooMooGenericAdapter is not connected")

    def _require_sdk_for_command(self) -> Any:
        self._require_connected()
        if self._sdk is None:
            # A supplied test context does not need the installed SDK.  Use
            # string enum values in that isolated offline case.
            return _StringMoomooEnums
        return self._sdk

    @staticmethod
    def _is_rate_limit_response(ret: object, data: object) -> bool:
        text = f"{ret} {data}".lower()
        return any(
            marker in text
            for marker in (
                "rate limit",
                "ratelimit",
                "too many request",
                "too frequent",
                "throttl",
                "request frequency",
                "429",
            )
        )

    def _wait_for_read_slot(self) -> None:
        if self._last_read_at is None or self._read_min_interval <= 0:
            return
        remaining = self._read_min_interval - (self._read_clock() - self._last_read_at)
        if remaining > 0:
            self._read_sleep(remaining)

    def _call(self, method: str, **kwargs: Any) -> list[dict[str, Any]]:
        if self.trade_context is None:
            raise ConnectionError("MooMooGenericAdapter is not connected")
        if method not in _READ_METHODS:
            ret, data = getattr(self.trade_context, method)(**kwargs)
            if ret != 0:
                raise MoomooAdapterError(f"Moomoo {method} failed with ret={ret}: {data}")
            return self._as_records(data, method=method)

        last_error: object = None
        for attempt in range(self._read_max_retries + 1):
            self._wait_for_read_slot()
            self._last_read_at = self._read_clock()
            try:
                ret, data = getattr(self.trade_context, method)(**kwargs)
            except Exception as exc:
                if not self._is_rate_limit_response("exception", exc):
                    raise
                ret, data = "exception", exc
            if ret == 0:
                self._last_read_error = None
                return self._as_records(data, method=method)
            last_error = data
            if not self._is_rate_limit_response(ret, data):
                raise MoomooAdapterError(f"Moomoo {method} failed with ret={ret}: {data}")
            if attempt >= self._read_max_retries:
                break
            backoff = min(5.0, self._read_backoff_base * (2**attempt))
            try:
                jitter = float(self._read_jitter(backoff * 0.25))
            except (TypeError, ValueError):
                jitter = 0.0
            self._read_sleep(max(0.0, backoff + min(backoff * 0.25, jitter)))
        error = MoomooRateLimitError(
            f"Moomoo {method} remained rate-limited after {self._read_max_retries + 1} bounded read attempt(s): {last_error}"
        )
        self._last_read_error = error
        raise error

    @staticmethod
    def _as_records(data: Any, *, method: str) -> list[dict[str, Any]]:
        try:
            return as_records(data)
        except MoomooResponseShapeError as exc:
            raise MoomooAdapterError(f"Moomoo {method} returned malformed response: {exc}") from exc

    @staticmethod
    def _as_payload(data: Any, *, method: str) -> Any:
        try:
            return coerce_provider_payload(canonicalize_payload(data))
        except (MoomooResponseShapeError, ProviderPayloadError) as exc:
            raise MoomooAdapterError(f"Moomoo {method} returned malformed response: {exc}") from exc

    def _account_kwargs(self, *, refresh_cache: bool = False) -> dict[str, Any]:
        environment = getattr(getattr(self._sdk, "TrdEnv", None), "SIMULATE", "SIMULATE") if self._sdk else "SIMULATE"
        kwargs: dict[str, Any] = {"trd_env": environment, "acc_id": int(self._selected_account_id or 0)}
        if refresh_cache:
            kwargs["refresh_cache"] = True
        return kwargs

    @staticmethod
    def _response_account_mismatches(account: Account, payload: object) -> list[str]:
        """Find conflicting account aliases anywhere in a provider response."""
        try:
            payload = coerce_provider_payload(canonicalize_payload(payload))
        except (MoomooResponseShapeError, ProviderPayloadError) as exc:
            return [f"response.malformed_shape={exc}"]
        expected_internal = str(account.id)
        expected_external = str(account.external_account_id)
        expected_broker = str(account.broker).lower()
        expected_environment = str(account.environment.value).upper()
        environment_aliases = {expected_environment}
        if expected_environment == "SIM":
            environment_aliases.add("SIMULATE")
        elif expected_environment == "LIVE":
            environment_aliases.add("REAL")
        mismatches: list[str] = []

        def walk(value: object, path: str = "response") -> None:
            if isinstance(value, Mapping):
                for raw_key, raw_value in value.items():
                    key = normalize_provider_key(raw_key)
                    current = f"{path}.{key}"
                    if raw_value in (None, ""):
                        walk(raw_value, current)
                        continue
                    if key in {"account_id", "internal_account_id", "oms_account_id"} and str(raw_value) != expected_internal:
                        mismatches.append(f"{current}={raw_value}")
                    elif key in {"external_account_id", "acc_id", "account_number", "trd_acc_id", "trade_account_id"} and str(raw_value) != expected_external:
                        mismatches.append(f"{current}={raw_value}")
                    elif key in {"broker", "broker_name", "broker_id", "provider"} and str(raw_value).lower() != expected_broker:
                        mismatches.append(f"{current}={raw_value}")
                    elif key in {"environment", "trading_environment", "trd_env", "trd_environment"} and str(raw_value).upper().split(".")[-1] not in environment_aliases:
                        mismatches.append(f"{current}={raw_value}")
                    elif key in {"account", "account_alias", "account_identifier"} and str(raw_value) not in {expected_internal, expected_external}:
                        mismatches.append(f"{current}={raw_value}")
                    walk(raw_value, current)
            elif isinstance(value, (list, tuple)):
                for index, item in enumerate(value):
                    walk(item, f"{path}[{index}]")

        walk(payload)
        return sorted(set(mismatches))

    def _snapshot_id(self, account: Account) -> str:
        return f"moomoo:{account.external_account_id}:{uuid4()}"

    def _query_orders(self, *, force_refresh: bool = False) -> list[dict[str, Any]]:
        self._require_connected()
        now = time.monotonic()
        if (
            not force_refresh
            and self._order_query_cache is not None
            and now - self._order_query_cache[0] < _ORDER_QUERY_CACHE_SECONDS
        ):
            return [dict(row) for row in self._order_query_cache[1]]
        rows = self._call("order_list_query", **self._account_kwargs(refresh_cache=True))
        self._order_query_cache = (time.monotonic(), rows)
        return [dict(row) for row in rows]

    def _invalidate_order_query_cache(self) -> None:
        self._order_query_cache = None

    @staticmethod
    def _raw_alias_values(row: Mapping[str, Any], aliases: set[str]) -> list[tuple[str, Any]]:
        values: list[tuple[str, Any]] = []
        for raw_key, value in row.items():
            key = _normalised_key(raw_key)
            if key not in aliases or value is None or (isinstance(value, str) and not value.strip()):
                continue
            values.append((key, value))
        return values

    def _validate_alias_group(
        self,
        row: Mapping[str, Any],
        *,
        name: str,
        aliases: set[str],
        normalizer,
    ) -> None:
        values = self._raw_alias_values(row, aliases)
        if not values:
            return
        normalized: list[tuple[str, object]] = []
        for key, value in values:
            try:
                normalized.append((key, normalizer(value)))
            except Exception as exc:
                raise MoomooAdapterError(
                    f"Moomoo {name} alias {key!r} is malformed: {value!r}"
                ) from exc
        if any(item[1] != normalized[0][1] for item in normalized[1:]):
            details = ", ".join(f"{key}={value!r}" for key, value in values)
            raise MoomooAdapterError(f"Moomoo {name} aliases conflict: {details}")

    def _validate_order_row_aliases(self, row: Mapping[str, Any]) -> None:
        """Reject contradictory aliases before any first-alias selection.

        OpenD has returned the same fact under several column names across
        endpoints.  Choosing the first populated alias can hide a provider
        contradiction, especially when duplicate rows are collapsed.  Every
        alias family is therefore normalized and compared before mapping.
        """
        self._validate_alias_group(
            row,
            name="order ID",
            aliases={"order_id", "orderid", "external_order_id", "broker_order_id", "id"},
            normalizer=lambda value: str(value).strip(),
        )
        self._validate_alias_group(
            row,
            name="order status",
            aliases={"order_status", "status"},
            normalizer=lambda value: (_order_status(value).value, status_name(value)),
        )
        self._validate_alias_group(
            row,
            name="symbol",
            aliases={"code", "symbol", "ticker"},
            normalizer=lambda value: _normalise_symbol(value),
        )
        self._validate_alias_group(
            row,
            name="trade side",
            aliases={"trd_side", "side"},
            normalizer=lambda value: _side(value).value,
        )
        self._validate_alias_group(
            row,
            name="order quantity",
            aliases={"qty", "quantity"},
            normalizer=lambda value: _decimal(value, field="order quantity"),
        )
        self._validate_alias_group(
            row,
            name="filled quantity",
            aliases={"dealt_qty", "filled_qty", "filled_quantity", "cumulative_filled_quantity"},
            normalizer=lambda value: _decimal(value, field="filled quantity"),
        )
        self._validate_alias_group(
            row,
            name="fill price",
            aliases={"dealt_avg_price", "avg_fill_price", "fill_price"},
            normalizer=lambda value: _decimal(value, field="fill price"),
        )
        self._validate_alias_group(
            row,
            name="client order ID",
            aliases={"remark", "client_order_id"},
            normalizer=lambda value: str(value).strip(),
        )
        self._validate_alias_group(
            row,
            name="fill ID",
            aliases={"deal_id", "fill_id", "external_fill_id"},
            normalizer=lambda value: str(value).strip(),
        )
        for name, aliases in (
            ("order time", {"order_time", "create_time"}),
            ("updated time", {"updated_time", "update_time", "modified_time"}),
            ("fill time", {"filled_at", "fill_time", "deal_time", "executed_at", "execution_time"}),
        ):
            self._validate_alias_group(
                row,
                name=name,
                aliases=aliases,
                normalizer=self._timestamp,
            )

        self._validate_raw_fill_detail(row)

    @staticmethod
    def _normalise_position_direction(value: Any) -> str:
        normalized = normalise_env(value)
        if normalized in {"SHORT", "SELL", "SELL_SHORT"}:
            return "SHORT"
        if normalized in {"LONG", "BUY"}:
            return "LONG"
        raise MoomooAdapterError(f"Moomoo position has an unknown direction: {value!r}")

    def _validate_position_row_aliases(self, row: Mapping[str, Any]) -> None:
        """Validate every populated position alias before choosing one.

        OpenD position tables have exposed the same fact as ``code``/
        ``symbol``/``ticker``, ``qty``/``quantity``/``position`` and several
        direction names.  Selecting the first populated alias can turn a
        contradictory flat-looking row into an unmapped/zero position.  A
        contradiction is an incomplete broker fact and must reach the
        account-wide safety gate as an error.
        """
        self._validate_alias_group(
            row,
            name="position symbol",
            aliases={"code", "symbol", "ticker"},
            normalizer=_normalise_symbol,
        )
        self._validate_alias_group(
            row,
            name="position quantity",
            aliases={"qty", "quantity", "position"},
            normalizer=lambda value: _signed_decimal(value, field="position quantity"),
        )
        # OpenD's position table can include ``position_type='N/A'`` as a
        # non-semantic placeholder while ``position_side`` carries the real
        # LONG/SHORT direction.  Do not treat that placeholder as a
        # contradictory populated alias, but retain fail-closed behavior when
        # no usable direction alias exists.
        direction_aliases = {"position_side", "position_type", "side", "direction"}
        direction_row = {
            key: value
            for key, value in row.items()
            if not (
                _normalised_key(key) in direction_aliases
                and normalise_env(value) in {"N/A", "NA", "NONE"}
            )
        }
        self._validate_alias_group(
            direction_row,
            name="position direction",
            aliases=direction_aliases,
            normalizer=self._normalise_position_direction,
        )

    def _validate_fill_row_aliases(self, row: Mapping[str, Any]) -> None:
        """Validate deal rows before constructing durable BrokerFill facts."""
        self._validate_alias_group(
            row,
            name="fill order ID",
            aliases={"order_id", "orderid", "external_order_id", "broker_order_id", "id"},
            normalizer=lambda value: str(value).strip(),
        )
        self._validate_alias_group(
            row,
            name="fill symbol",
            aliases={"code", "symbol", "ticker"},
            normalizer=_normalise_symbol,
        )
        self._validate_alias_group(
            row,
            name="fill quantity",
            aliases={"qty", "dealt_qty", "filled_qty", "filled_quantity", "quantity"},
            normalizer=lambda value: _decimal(value, field="fill quantity", allow_zero=False),
        )
        self._validate_alias_group(
            row,
            name="fill price",
            aliases={"price", "fill_price", "dealt_avg_price", "avg_fill_price"},
            normalizer=lambda value: _decimal(value, field="fill price", allow_zero=False),
        )
        self._validate_alias_group(
            row,
            name="fill ID",
            aliases={"deal_id", "fill_id", "external_fill_id"},
            normalizer=lambda value: str(value).strip(),
        )
        for name, aliases in (
            ("fill time", {"filled_at", "fill_time", "deal_time", "executed_at", "execution_time", "create_time"}),
        ):
            self._validate_alias_group(
                row,
                name=name,
                aliases=aliases,
                normalizer=self._timestamp,
            )

        self._validate_raw_fill_detail(row)

    def _validate_raw_fill_detail(self, row: Mapping[str, Any]) -> None:
        """Ensure nested deal/fill detail agrees with aggregate row fields."""
        detail_values = self._raw_alias_values(
            row,
            {"fills", "deals", "fill_list", "deal_list", "deal_detail"},
        )
        if not detail_values:
            return
        declared_ids = {
            str(value).strip()
            for _key, value in self._raw_alias_values(
                row,
                {"deal_id", "fill_id", "external_fill_id"},
            )
            if str(value).strip()
        }
        detail_ids: set[str] = set()
        detail_quantities: list[Decimal] = []

        def walk(value: object) -> None:
            if isinstance(value, Mapping):
                for raw_key, raw_value in value.items():
                    key = _normalised_key(raw_key)
                    if key in {"deal_id", "fill_id", "external_fill_id"} and raw_value not in (None, ""):
                        detail_ids.add(str(raw_value).strip())
                    if key in {
                        "fill_qty", "filled_qty", "dealt_qty", "filled_quantity", "quantity", "qty",
                    } and raw_value not in (None, ""):
                        try:
                            detail_quantities.append(_decimal(raw_value, field="nested fill quantity"))
                        except MoomooAdapterError as exc:
                            raise MoomooAdapterError(
                                f"Moomoo nested fill quantity is malformed: {raw_value!r}"
                            ) from exc
                    walk(raw_value)
            elif isinstance(value, (list, tuple)):
                for item in value:
                    walk(item)

        for _key, value in detail_values:
            try:
                canonical = coerce_provider_payload(canonicalize_payload(value))
            except (MoomooResponseShapeError, ProviderPayloadError) as exc:
                raise MoomooAdapterError(f"Moomoo nested fill detail is opaque: {exc}") from exc
            local_ids: set[str] = set()

            def collect_ids(item: object) -> None:
                if isinstance(item, Mapping):
                    for raw_key, raw_value in item.items():
                        key = _normalised_key(raw_key)
                        if key in {"deal_id", "fill_id", "external_fill_id"} and raw_value not in (None, ""):
                            local_ids.add(str(raw_value).strip())
                        collect_ids(raw_value)
                elif isinstance(item, (list, tuple)):
                    for child in item:
                        collect_ids(child)

            collect_ids(canonical)
            if declared_ids and local_ids and not declared_ids.intersection(local_ids):
                raise MoomooAdapterError(
                    "Moomoo aggregate fill ID conflicts with one nested deal/fill collection"
                )
            walk(canonical)
        if declared_ids and detail_ids and not declared_ids.intersection(detail_ids):
            raise MoomooAdapterError("Moomoo aggregate fill ID conflicts with nested deal/fill detail")
        aggregate_values = self._raw_alias_values(
            row,
            {"dealt_qty", "filled_qty", "filled_quantity", "cumulative_filled_quantity"},
        )
        if detail_quantities and aggregate_values:
            try:
                aggregate = _decimal(aggregate_values[0][1], field="filled quantity")
            except MoomooAdapterError as exc:
                raise MoomooAdapterError("Moomoo aggregate filled quantity is malformed") from exc
            detail_total = sum(detail_quantities, Decimal("0"))
            if aggregate == 0 or detail_total != aggregate:
                raise MoomooAdapterError(
                    "Moomoo aggregate filled quantity conflicts with nested deal/fill detail"
                )

    @staticmethod
    def _raw_row_signature(row: Mapping[str, Any]) -> str:
        try:
            canonical = canonicalize_payload(dict(row))
            return json.dumps(canonical, sort_keys=True, separators=(",", ":"), default=str)
        except MoomooResponseShapeError as exc:
            raise MoomooAdapterError(f"Moomoo order row contains uninspectable raw evidence: {exc}") from exc

    def _validated_order_rows(
        self,
        account: Account,
        rows: Sequence[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        """Collapse exact duplicate order rows and reject contradictory ones.

        OpenD can expose the same broker order more than once while caches are
        refreshed.  It is safe to collapse rows only when all lifecycle,
        identity, quantity, and fill-economics fields agree.  A conflict is
        deliberately surfaced as an adapter error so the generic account
        fact gate records a durable blocker rather than selecting the first
        row.
        """
        grouped: dict[str, list[dict[str, Any]]] = {}
        for index, raw_row in enumerate(rows):
            row = dict(raw_row)
            self._validate_order_row_aliases(row)
            external_order_id = str(get_value(row, "order_id", "id", "orderid", default="")).strip()
            if not external_order_id:
                raise MoomooAdapterError(f"Moomoo order row {index} did not contain an order_id")
            grouped.setdefault(external_order_id, []).append(row)

        validated: list[dict[str, Any]] = []
        for external_order_id, matching_rows in grouped.items():
            signatures = [self._order_row_signature(account, row) for row in matching_rows]
            if any(signature != signatures[0] for signature in signatures[1:]):
                raise MoomooAdapterError(
                    f"Moomoo order {external_order_id} returned conflicting duplicate rows"
                )
            validated.append(matching_rows[0])
        return validated

    def _order_row_signature(self, account: Account, row: Mapping[str, Any]) -> tuple[object, ...]:
        self._validate_order_row_aliases(row)
        mismatches = self._response_account_mismatches(account, row)
        if mismatches:
            raise MoomooAdapterError(
                "Moomoo order row account identity conflict: " + ", ".join(mismatches)
            )
        status = _order_status(get_value(row, "order_status", "status", default=""))
        symbol = _normalise_symbol(get_value(row, "code", "symbol", "ticker"))
        side = _side(get_value(row, "trd_side", "side"))
        quantity = _decimal(get_value(row, "qty", "quantity"), field="order quantity", allow_zero=False)
        missing = object()
        raw_filled = get_value(row, "dealt_qty", "filled_qty", default=missing)
        if raw_filled is missing:
            filled_signature: object = ("missing",)
            filled = Decimal("0")
        else:
            filled = _decimal(raw_filled, field="filled quantity")
            filled_signature = ("value", str(filled))
        consistency = _order_quantity_consistency_errors(status, quantity, filled)
        if consistency:
            if status is BrokerOrderStatus.FILLED and filled != quantity:
                raise MoomooAdapterError(
                    f"Moomoo order {get_value(row, 'order_id', 'id', 'orderid')} dealt quantity is not complete; "
                    "FILLED requires filled == submitted"
                )
            raise MoomooAdapterError(
                f"Moomoo order {get_value(row, 'order_id', 'id', 'orderid')} has inconsistent status/quantity: "
                + "; ".join(consistency)
            )
        economics: list[object] = []
        for key in ("dealt_avg_price", "avg_fill_price", "fill_price"):
            if key in row and row[key] not in (None, ""):
                try:
                    economics.append((key, str(_decimal(row[key], field=key))))
                except MoomooAdapterError:
                    raise
        client_order_id = str(get_value(row, "remark", "client_order_id", default="")).strip() or None
        return (
            status.value,
            symbol,
            side.value,
            str(quantity),
            filled_signature,
            tuple(economics),
            client_order_id,
            # Preserve all primitive provider evidence when collapsing exact
            # duplicate rows.  A second row with a different deal ID/list or
            # timestamp is not silently discarded as an equivalent order.
            self._raw_row_signature(row),
        )

    def _order_snapshot(self, account: Account, snapshot_id: str, captured_at: datetime, row: Mapping[str, Any]) -> BrokerOrderSnapshot:
        self._validate_order_row_aliases(row)
        external_order_id = str(get_value(row, "order_id", "id", "orderid", default="")).strip()
        if not external_order_id:
            raise MoomooAdapterError("Moomoo order did not contain an order_id")
        symbol = _normalise_symbol(get_value(row, "code", "symbol", "ticker"))
        quantity = _decimal(get_value(row, "qty", "quantity"), field="order quantity", allow_zero=False)
        missing = object()
        raw_filled = get_value(row, "dealt_qty", "filled_qty", default=missing)
        if raw_filled is missing:
            filled = Decimal("0")
            no_fill_asserted = False
        else:
            filled = _decimal(raw_filled, field="filled quantity")
            no_fill_asserted = filled == 0 and not _raw_fill_evidence(row)
        status = _order_status(get_value(row, "order_status", "status", default=""))
        consistency = _order_quantity_consistency_errors(status, quantity, filled)
        if consistency:
            raise MoomooAdapterError(
                f"Moomoo order {external_order_id} has inconsistent status/quantity: "
                + "; ".join(consistency)
            )
        return BrokerOrderSnapshot(
            id=f"{snapshot_id}:order:{external_order_id}",
            broker_snapshot_id=snapshot_id,
            account_id=account.id,
            instrument_id=self.instrument_resolver.instrument_id_for_symbol(symbol),
            external_order_id=external_order_id,
            client_order_id=str(get_value(row, "remark", "client_order_id", default="")).strip() or None,
            side=_side(get_value(row, "trd_side", "side")),
            quantity=quantity,
            filled_quantity=filled,
            status=status,
            captured_at=captured_at,
            metadata={"external_symbol": symbol, "raw": dict(row)},
            order_time=self._order_timestamp(row),
            external_account_id=account.external_account_id,
            no_fill_asserted=no_fill_asserted,
            authority=ADAPTER_ORDER_SNAPSHOT_AUTHORITY,
        )

    def _order_timestamp(self, row: Mapping[str, Any]) -> datetime | None:
        """Return provider order time, never the local query capture time."""
        value = get_value(row, "updated_time", "create_time", "order_time", default=None)
        if value in (None, ""):
            return None
        return self._timestamp(value)

    def _timestamp(self, value: Any) -> datetime:
        if isinstance(value, datetime):
            parsed = value
        else:
            text = str(value or "").strip()
            if not text:
                raise MoomooAdapterError("Moomoo fill did not contain a timestamp")
            try:
                parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            except ValueError:
                try:
                    parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
                except ValueError as exc:
                    raise MoomooAdapterError(f"Moomoo returned an unparseable timestamp: {text!r}") from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            parsed = parsed.replace(tzinfo=self._broker_timezone)
        return parsed.astimezone(timezone.utc)

    def _sdk_side(self, sdk: Any, side: Side) -> Any:
        name = "BUY" if side is Side.BUY else "SELL"
        return getattr(getattr(sdk, "TrdSide", None), name, name)

    def _sdk_order_type(self, sdk: Any, order_type: str) -> Any:
        name = "MARKET" if order_type == "MARKET" else "NORMAL"
        return getattr(getattr(sdk, "OrderType", None), name, name)

    def _sdk_session(self, sdk: Any, session: ExecutionSession) -> Any:
        name = "OVERNIGHT" if session is ExecutionSession.OVERNIGHT else "RTH"
        return getattr(getattr(sdk, "Session", None), name, name)

    @staticmethod
    def _sdk_order_id(identifier: str) -> int | str:
        return int(identifier) if identifier.isdigit() else identifier

    def _rejected(
        self,
        request: BrokerSubmitRequest,
        code: str,
        message: str,
        *,
        raw_payload: Mapping[str, Any] | None = None,
        no_submit_asserted: bool = True,
        no_fill_asserted: bool = True,
        cumulative_filled_quantity: Decimal | None = Decimal("0"),
    ) -> BrokerSubmissionResult:
        return BrokerSubmissionResult(
            broker_order_id=request.broker_order_id, accepted=False, status=BrokerOrderStatus.REJECTED,
            client_order_id=request.client_order_id,
            error_code=code,
            error_message=message,
            cumulative_filled_quantity=cumulative_filled_quantity,
            no_submit_asserted=no_submit_asserted,
            no_fill_asserted=no_fill_asserted,
            raw_payload=raw_payload or {},
            submitted_quantity=request.order_leg.quantity,
            instrument_id=request.order_leg.instrument_id,
        )

    def _submit_response_mismatches(
        self,
        request: BrokerSubmitRequest,
        data: Any,
        *,
        account: Account,
        expected_symbol: str | None,
    ) -> list[str]:
        """Validate every row returned by a place-order command."""
        payload = self._as_payload(data, method="place_order")
        rows = self._as_records(payload, method="place_order")
        mismatches: list[str] = []
        if len(rows) != 1:
            mismatches.append(f"response.row_count={len(rows)}")
        mismatches.extend(self._response_account_mismatches(account, payload))
        for index, row in enumerate(rows):
            try:
                self._validate_order_row_aliases(row)
            except MoomooAdapterError as exc:
                mismatches.append(f"response[{index}].alias_conflict={exc}")
            row_order_id = str(get_value(row, "order_id", "id", "orderid", default="")).strip()
            if not row_order_id:
                mismatches.append(f"response[{index}].order_id_missing")
            mismatches.extend(
                f"response[{index}].{item}"
                for item in self._response_account_mismatches(account, row)
            )
            if expected_symbol is not None:
                supplied_symbol = get_value(row, "code", "symbol", "ticker", default=None)
                if supplied_symbol not in (None, ""):
                    try:
                        if _normalise_symbol(supplied_symbol) != _normalise_symbol(expected_symbol):
                            mismatches.append(f"response[{index}].symbol={supplied_symbol}")
                    except (TypeError, ValueError):
                        mismatches.append(f"response[{index}].symbol_invalid={supplied_symbol}")
            supplied_client_id = get_value(row, "remark", "client_order_id", default=None)
            if supplied_client_id not in (None, "") and request.client_order_id is not None:
                if str(supplied_client_id) != str(request.client_order_id):
                    mismatches.append(f"response[{index}].client_order_id={supplied_client_id}")
        return mismatches

    def _command_result(
        self,
        request: BrokerSubmitRequest,
        data: Any,
        *,
        default_status: BrokerOrderStatus,
        account: Account | None = None,
        expected_symbol: str | None = None,
    ) -> BrokerSubmissionResult:
        payload = self._as_payload(data, method="place_order")
        rows = self._as_records(payload, method="place_order")
        mismatches: list[str] = []
        if account is not None:
            mismatches = self._submit_response_mismatches(
                request,
                payload,
                account=account,
                expected_symbol=expected_symbol,
            )
        elif len(rows) != 1:
            mismatches.append(f"response.row_count={len(rows)}")
        returned_ids: list[str] = []
        for index, row in enumerate(rows):
            try:
                self._validate_order_row_aliases(row)
            except MoomooAdapterError as exc:
                mismatches.append(f"response[{index}].alias_conflict={exc}")
            row_order_id = str(get_value(row, "order_id", "id", "orderid", default="")).strip()
            if not row_order_id:
                if account is None:
                    mismatches.append(f"response[{index}].order_id_missing")
            else:
                returned_ids.append(row_order_id)
        external_order_id = returned_ids[0] if len(returned_ids) == 1 else None
        if mismatches or external_order_id is None:
            return BrokerSubmissionResult(
                broker_order_id=request.broker_order_id, accepted=None, status=BrokerOrderStatus.UNKNOWN,
                client_order_id=request.client_order_id, ambiguous=True,
                error_code="MOOMOO_SUBMIT_RESPONSE_CONFLICT" if mismatches else "MOOMOO_ORDER_ID_MISSING",
                error_message=(
                    "Moomoo submit response could not be bound to exactly one expected account/order: "
                    f"{sorted(set(mismatches))}"
                    if mismatches
                    else "Moomoo accepted the request but returned no broker order ID"
                ),
                raw_payload={"response": payload},
            )
        row = rows[0]
        provider_status = _order_status(get_value(row, "order_status", "status", default="")) if row else default_status
        missing = object()
        raw_cumulative = get_value(row, "dealt_qty", "filled_qty", default=missing)
        if raw_cumulative is missing:
            cumulative = None
            no_fill_asserted = False
        else:
            cumulative = _decimal(raw_cumulative, field="filled quantity")
            no_fill_asserted = (
                cumulative == 0
                and provider_status is BrokerOrderStatus.WORKING
                and _valid_zero_submission_ack(
                    row,
                    expected_quantity=request.order_leg.quantity,
                    expected_symbol=expected_symbol,
                )
            )
        return BrokerSubmissionResult(
            broker_order_id=request.broker_order_id, accepted=True,
            status=default_status if provider_status is BrokerOrderStatus.UNKNOWN else provider_status,
            external_order_id=external_order_id, client_order_id=request.client_order_id,
            submitted_at=self._timestamp(get_value(row, "create_time", "updated_time")) if get_value(row, "create_time", "updated_time") else None,
            cumulative_filled_quantity=cumulative,
            no_fill_asserted=no_fill_asserted,
            raw_payload={"response": payload},
            authority=(ADAPTER_SUBMISSION_ACK_AUTHORITY if no_fill_asserted else None),
            submitted_quantity=request.order_leg.quantity,
            instrument_id=request.order_leg.instrument_id,
        )

    def _command_result_for_external(self, broker_order_id: str, external_order_id: str, data: Any) -> BrokerSubmissionResult:
        payload = self._as_payload(data, method="modify_order")
        rows = self._as_records(payload, method="modify_order")
        mismatches: list[str] = []
        for index, row in enumerate(rows):
            try:
                self._validate_order_row_aliases(row)
            except MoomooAdapterError as exc:
                mismatches.append(f"response[{index}].alias_conflict={exc}")
        if len(rows) != 1:
            mismatches.append(f"response.row_count={len(rows)}")
        if mismatches:
            return BrokerSubmissionResult(
                broker_order_id=broker_order_id,
                accepted=None,
                status=BrokerOrderStatus.UNKNOWN,
                external_order_id=external_order_id,
                ambiguous=True,
                error_code="MOOMOO_COMMAND_RESPONSE_CONFLICT",
                error_message=f"Moomoo command response could not be normalized: {sorted(set(mismatches))}",
                raw_payload={"response": payload},
            )
        row = rows[0] if rows else {}
        missing = object()
        raw_cumulative = get_value(row, "dealt_qty", "filled_qty", default=missing)
        if raw_cumulative is missing:
            cumulative = None
            no_fill_asserted = False
        else:
            cumulative = _decimal(raw_cumulative, field="filled quantity")
            # Inspect the canonical returned row here.  The surrounding
            # response wrapper is a transport shape, not a second order;
            # sibling/provider fields are still retained in raw_payload and
            # rejected by the generic OMS authority validator.
            no_fill_asserted = cumulative == 0 and not _raw_fill_evidence(row)
        status = _order_status(get_value(row, "order_status", "status", default="")) if row else BrokerOrderStatus.WORKING
        submitted_quantity: Decimal | None = None
        instrument_id: str | None = None
        if row:
            raw_quantity = get_value(row, "qty", "quantity", default=missing)
            if raw_quantity is not missing:
                try:
                    submitted_quantity = _decimal(raw_quantity, field="order quantity", allow_zero=False)
                except MoomooAdapterError as exc:
                    mismatches.append(f"response.order_quantity={exc}")
            raw_symbol = get_value(row, "code", "symbol", "ticker", default=missing)
            if raw_symbol is not missing:
                try:
                    instrument_id = self.instrument_resolver.instrument_id_for_symbol(
                        _normalise_symbol(raw_symbol)
                    )
                except (MoomooAdapterError, ValueError) as exc:
                    mismatches.append(f"response.instrument={exc}")
        if mismatches:
            return BrokerSubmissionResult(
                broker_order_id=broker_order_id,
                accepted=None,
                status=BrokerOrderStatus.UNKNOWN,
                external_order_id=external_order_id,
                ambiguous=True,
                error_code="MOOMOO_COMMAND_RESPONSE_CONFLICT",
                error_message=f"Moomoo command response could not be normalized: {sorted(set(mismatches))}",
                raw_payload={"response": payload},
            )
        return BrokerSubmissionResult(
            broker_order_id=broker_order_id, accepted=True,
            status=status,
            external_order_id=external_order_id,
            cumulative_filled_quantity=cumulative,
            no_fill_asserted=no_fill_asserted,
            raw_payload={"response": payload},
            authority=(
                ADAPTER_ORDER_SNAPSHOT_AUTHORITY
                if no_fill_asserted
                and status in {
                    BrokerOrderStatus.CANCELLED,
                    BrokerOrderStatus.REJECTED,
                    BrokerOrderStatus.FAILED,
                }
                and submitted_quantity is not None
                and instrument_id is not None
                else None
            ),
            submitted_quantity=submitted_quantity,
            instrument_id=instrument_id,
        )


class _StringMoomooEnums:
    """Offline fallback used only by fake adapter-contract contexts."""

    class TrdSide:
        BUY = "BUY"
        SELL = "SELL"

    class OrderType:
        MARKET = "MARKET"
        NORMAL = "NORMAL"

    class ModifyOrderOp:
        CANCEL = "CANCEL"
        MODIFY = "MODIFY"

    class Session:
        RTH = "RTH"
        ETH = "ETH"
        OVERNIGHT = "OVERNIGHT"


__all__ = [
    "MooMooGenericAdapter",
    "MoomooAdapterError",
    "MoomooRateLimitError",
    "MoomooInstrumentResolver",
    "MoomooMappingError",
    "StaticMoomooInstrumentResolver",
]
