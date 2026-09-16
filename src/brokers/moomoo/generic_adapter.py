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
import time
from typing import Any, Protocol, runtime_checkable
from uuid import uuid4
from zoneinfo import ZoneInfo

from src.trading_core.domain import (
    Account,
    AccountBalanceSnapshot,
    BrokerCapabilities,
    BrokerOrderSnapshot,
    BrokerOrderStatus,
    BrokerSnapshot,
    ExecutionSession,
    PositionSnapshot,
    QuantityUnit,
    Side,
    TradingEnvironment,
)
from src.trading_core.ports import BrokerAdapter, BrokerFill, BrokerSubmissionResult, BrokerSubmitRequest

from .common import as_records, get_value, normalise_env, parse_market_auth, status_name

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


class MoomooAdapterError(RuntimeError):
    """A definite OpenD protocol, mapping, or account-boundary failure."""


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
            symbol = _normalise_symbol(get_value(row, "code", "symbol"))
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
        self._validate_account(account)
        snapshot_id = self._snapshot_id(account)
        captured_at = datetime.now(timezone.utc)
        return tuple(
            self._order_snapshot(account, snapshot_id, captured_at, row)
            for row in self._query_orders()
            if _order_status(get_value(row, "order_status", "status", default="")) not in _TERMINAL_STATUSES
        )

    def get_order(self, account: Account, external_order_id: str) -> BrokerOrderSnapshot | None:
        self._validate_account(account)
        identifier = str(external_order_id).strip()
        for row in self._query_orders():
            if str(get_value(row, "order_id", "id", "orderid", default="")) == identifier:
                return self._order_snapshot(account, self._snapshot_id(account), datetime.now(timezone.utc), row)
        return None

    def get_fills(self, account: Account, since: datetime | None = None) -> Sequence[BrokerFill]:
        self._validate_account(account)
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
            return self._filled_order_fallback_fills(since=since, reason=str(exc))
        received_at = datetime.now(timezone.utc)
        fills: list[BrokerFill] = []
        for row in rows:
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
                    metadata={"external_symbol": get_value(row, "code", "symbol"), "raw": dict(row)},
                )
            )
        return tuple(fills)

    @staticmethod
    def _deal_history_unsupported(error: MoomooAdapterError) -> bool:
        text = str(error).strip().lower()
        return (
            "deal_list_query" in text
            and "support" in text
            and any(marker in text for marker in ("not support", "unsupported", "does not support"))
        )

    def _filled_order_fallback_fills(
        self,
        *,
        since: datetime | None,
        reason: str,
    ) -> tuple[BrokerFill, ...]:
        """Synthesize one conservative fill fact per fully filled order.

        This is deliberately narrower than treating an order status as a
        fill: the provider must report a terminal FILLED status, positive
        dealt quantity equal to the submitted quantity, a positive dealt
        average price, and a parseable order timestamp.
        """

        received_at = datetime.now(timezone.utc)
        fills: list[BrokerFill] = []
        for row in self._query_orders():
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
            fills.append(
                BrokerFill(
                    external_order_id=external_order_id,
                    external_fill_id=None,
                    dedupe_key=f"moomoo-order-fill:{external_order_id}",
                    quantity=dealt_quantity,
                    price=price,
                    filled_at=filled_at,
                    received_at=received_at,
                    metadata={
                        "source": "order_list_query",
                        "synthetic": True,
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
        kwargs: dict[str, Any] = {
            "price": 0.0 if leg.order_type == "MARKET" else float(leg.limit_price),
            "qty": float(leg.quantity),
            "code": self.instrument_resolver.symbol_for_instrument(leg.instrument_id),
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
            return self._rejected(request, "MOOMOO_PLACE_ORDER_FAILED", str(data), raw_payload={"response": data})
        return self._command_result(request, data, default_status=BrokerOrderStatus.WORKING)

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
            return BrokerSubmissionResult(
                broker_order_id=f"cancel:{identifier}", accepted=False, status=BrokerOrderStatus.FAILED,
                external_order_id=identifier, error_code="MOOMOO_CANCEL_FAILED", error_message=str(data), raw_payload={"response": data},
            )
        return self._command_result_for_external(f"cancel:{identifier}", identifier, data)

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
            return BrokerSubmissionResult(
                broker_order_id=f"replace:{identifier}", accepted=False, status=BrokerOrderStatus.FAILED,
                external_order_id=identifier, error_code="MOOMOO_REPLACE_FAILED", error_message=str(data), raw_payload={"response": data},
            )
        return self._command_result_for_external(f"replace:{identifier}", identifier, data)

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

    def _call(self, method: str, **kwargs: Any) -> list[dict[str, Any]]:
        if self.trade_context is None:
            raise ConnectionError("MooMooGenericAdapter is not connected")
        ret, data = getattr(self.trade_context, method)(**kwargs)
        if ret != 0:
            raise MoomooAdapterError(f"Moomoo {method} failed with ret={ret}: {data}")
        return as_records(data)

    def _account_kwargs(self, *, refresh_cache: bool = False) -> dict[str, Any]:
        environment = getattr(getattr(self._sdk, "TrdEnv", None), "SIMULATE", "SIMULATE") if self._sdk else "SIMULATE"
        kwargs: dict[str, Any] = {"trd_env": environment, "acc_id": int(self._selected_account_id or 0)}
        if refresh_cache:
            kwargs["refresh_cache"] = True
        return kwargs

    def _snapshot_id(self, account: Account) -> str:
        return f"moomoo:{account.external_account_id}:{uuid4()}"

    def _query_orders(self) -> list[dict[str, Any]]:
        self._require_connected()
        now = time.monotonic()
        if self._order_query_cache is not None and now - self._order_query_cache[0] < _ORDER_QUERY_CACHE_SECONDS:
            return [dict(row) for row in self._order_query_cache[1]]
        rows = self._call("order_list_query", **self._account_kwargs(refresh_cache=True))
        self._order_query_cache = (now, rows)
        return [dict(row) for row in rows]

    def _invalidate_order_query_cache(self) -> None:
        self._order_query_cache = None

    def _order_snapshot(self, account: Account, snapshot_id: str, captured_at: datetime, row: Mapping[str, Any]) -> BrokerOrderSnapshot:
        external_order_id = str(get_value(row, "order_id", "id", "orderid", default="")).strip()
        if not external_order_id:
            raise MoomooAdapterError("Moomoo order did not contain an order_id")
        symbol = _normalise_symbol(get_value(row, "code", "symbol"))
        quantity = _decimal(get_value(row, "qty", "quantity"), field="order quantity", allow_zero=False)
        filled = _decimal(get_value(row, "dealt_qty", "filled_qty", default=0), field="filled quantity")
        if filled > quantity:
            raise MoomooAdapterError(f"Moomoo order {external_order_id} reports fills above submitted quantity")
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
            status=_order_status(get_value(row, "order_status", "status", default="")),
            captured_at=captured_at,
            metadata={"external_symbol": symbol, "raw": dict(row)},
        )

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

    def _rejected(self, request: BrokerSubmitRequest, code: str, message: str, *, raw_payload: Mapping[str, Any] | None = None) -> BrokerSubmissionResult:
        return BrokerSubmissionResult(
            broker_order_id=request.broker_order_id, accepted=False, status=BrokerOrderStatus.REJECTED,
            client_order_id=request.client_order_id, error_code=code, error_message=message, raw_payload=raw_payload or {},
        )

    def _command_result(self, request: BrokerSubmitRequest, data: Any, *, default_status: BrokerOrderStatus) -> BrokerSubmissionResult:
        rows = as_records(data)
        row = rows[0] if rows else {}
        external_order_id = str(get_value(row, "order_id", "id", "orderid", default="")).strip() or None
        if external_order_id is None:
            return BrokerSubmissionResult(
                broker_order_id=request.broker_order_id, accepted=None, status=BrokerOrderStatus.UNKNOWN,
                client_order_id=request.client_order_id, ambiguous=True, error_code="MOOMOO_ORDER_ID_MISSING",
                error_message="Moomoo accepted the request but returned no broker order ID", raw_payload={"response": data},
            )
        provider_status = _order_status(get_value(row, "order_status", "status", default="")) if row else default_status
        return BrokerSubmissionResult(
            broker_order_id=request.broker_order_id, accepted=True,
            status=default_status if provider_status is BrokerOrderStatus.UNKNOWN else provider_status,
            external_order_id=external_order_id, client_order_id=request.client_order_id,
            submitted_at=self._timestamp(get_value(row, "create_time", "updated_time")) if get_value(row, "create_time", "updated_time") else None,
            cumulative_filled_quantity=_decimal(get_value(row, "dealt_qty", "filled_qty", default=0), field="filled quantity"), raw_payload={"response": data},
        )

    def _command_result_for_external(self, broker_order_id: str, external_order_id: str, data: Any) -> BrokerSubmissionResult:
        rows = as_records(data)
        row = rows[0] if rows else {}
        return BrokerSubmissionResult(
            broker_order_id=broker_order_id, accepted=True,
            status=_order_status(get_value(row, "order_status", "status", default="")) if row else BrokerOrderStatus.WORKING,
            external_order_id=external_order_id, raw_payload={"response": data},
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
    "MoomooInstrumentResolver",
    "MoomooMappingError",
    "StaticMoomooInstrumentResolver",
]
