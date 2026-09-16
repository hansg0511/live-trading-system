"""Moomoo broker adapter with explicit environment/account safety checks."""

from __future__ import annotations

import json
import time
from typing import Any, List

from src.core.interfaces import BrokerAdapter
from src.core.models import AccountBalance, Order, OrderSide, OrderStatus, OrderType, Position
from src.db.positions_db import (
    DB_PATH,
    record_aggregate_fill,
    record_fill_event,
    resolve_db_path,
    update_order_status,
)
from .common import (
    TRD_MARKET_NUMBER_TO_NAME as _TRD_MARKET_NUMBER_TO_NAME,
    as_records as _as_records,
    get_value as _get,
    normalise_env as _normalise_env,
    parse_market_auth as _parse_market_auth,
    safe_float as _safe_float,
    status_name as _status_name,
)

try:
    import moomoo as moo  # type: ignore
except (ImportError, OSError, PermissionError):  # pragma: no cover - SDK/logging unavailable
    moo = None  # type: ignore[assignment]


_TradeOrderBase = moo.TradeOrderHandlerBase if moo is not None else object
_TradeDealBase = moo.TradeDealHandlerBase if moo is not None else object

# OpenD limits ``order_list_query`` to 10 calls per 30 seconds.  A single
# reconciliation pass reads the same broker-order snapshot several times
# (recent orders, per-order status fallback, and open orders).  Keep that
# pass coherent and within the broker limit, while refreshing frequently
# enough for supervised order management.
_ORDER_QUERY_CACHE_SECONDS = 3.5


class _TradeOrderPushHandler(_TradeOrderBase):
    """Persist order status and aggregate fill deltas from broker pushes."""

    def __init__(self, db_path: str = DB_PATH):
        if moo is not None:
            super().__init__()
        self.db_path = db_path

    def on_recv_rsp(self, rsp_pb):  # pragma: no cover - SDK callback
        ret_code, ret_data = super().on_recv_rsp(rsp_pb)
        if moo is None or ret_code != moo.RET_OK:
            return ret_code, ret_data
        for row in _as_records(ret_data):
            broker_order_id = str(_get(row, "order_id", "id", default=""))
            status = _status_name(_get(row, "order_status", "status", default=""))
            if not broker_order_id:
                continue
            local = _find_local_order(broker_order_id, self.db_path)
            if local is None:
                continue
            dealt_qty = _safe_float(_get(row, "dealt_qty", "filled_qty"), 0.0) or 0.0
            if dealt_qty > 0:
                avg_price = _safe_float(_get(row, "dealt_avg_price", "avg_fill_price", "price"), 0.0) or 0.0
                if avg_price > 0:
                    try:
                        record_aggregate_fill(
                            broker_order_id=broker_order_id,
                            cumulative_quantity=dealt_qty,
                            average_price=avg_price,
                            fill_time=str(_get(row, "updated_time", "create_time", default="")),
                            broker_order_status=status,
                            db_path=self.db_path,
                        )
                    except (LookupError, ValueError):
                        # A push can race the durable broker-ID write. The
                        # startup poll will recover the order and its fills.
                        pass
            mapped = _map_broker_status(status)
            if mapped in {"cancelled", "rejected"}:
                update_order_status(local["id"], mapped, self.db_path, broker_order_status=status)
        return ret_code, ret_data


class _TradeDealPushHandler(_TradeDealBase):
    """Persist each deal exactly once using its broker deal ID."""

    def __init__(self, db_path: str = DB_PATH):
        if moo is not None:
            super().__init__()
        self.db_path = db_path

    def on_recv_rsp(self, rsp_pb):  # pragma: no cover - SDK callback
        ret_code, ret_data = super().on_recv_rsp(rsp_pb)
        if moo is None or ret_code != moo.RET_OK:
            return ret_code, ret_data
        for row in _as_records(ret_data):
            broker_order_id = str(_get(row, "order_id", default=""))
            fill_price = _safe_float(_get(row, "price", "fill_price"), 0.0) or 0.0
            fill_qty = _safe_float(_get(row, "qty", "dealt_qty", "filled_qty"), 0.0) or 0.0
            if not broker_order_id or fill_price <= 0 or fill_qty <= 0:
                continue
            try:
                record_fill_event(
                    broker_order_id=broker_order_id,
                    broker_fill_id=str(_get(row, "deal_id", "fill_id", default="")) or None,
                    fill_price=fill_price,
                    fill_quantity=fill_qty,
                    fill_time=str(_get(row, "create_time", "updated_time", default="")),
                    raw_payload=row,
                    db_path=self.db_path,
                )
            except (LookupError, ValueError):
                # Missing local correlation is retained for startup
                # reconciliation instead of killing the broker callback.
                pass
        return ret_code, ret_data


def _find_local_order(broker_order_id: str, db_path: str) -> dict | None:
    import sqlite3

    conn = sqlite3.connect(str(resolve_db_path(db_path)))
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT id, operation_id, broker_order_id FROM orders WHERE broker_order_id = ?",
            (str(broker_order_id),),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _map_broker_status(status: str) -> str:
    status = str(status).upper()
    if status in {"FILLED_ALL", "FILLED", "FULLY_FILLED"}:
        return "filled"
    if status in {"FILLED_PART", "PARTIALLY_FILLED", "PARTIAL_FILLED", "CANCELLED_PART", "FILL_CANCELLED"}:
        # A cancelled-part/fill-cancelled order may carry real exposure; keep
        # it in the partial/reconciliation path until fill evidence is read.
        return "partially_filled"
    if status in {"CANCELLED_ALL", "CANCELLED", "DELETED", "DISABLED"}:
        return "cancelled"
    if status in {"REJECTED", "FAILED", "SUBMIT_FAILED", "TIMEOUT"}:
        return "rejected"
    return "submitted"


class MooMooAdapter(BrokerAdapter):
    """Synchronous Moomoo securities adapter.

    SIMULATE is the default. REAL requires an explicit account ID, an exact
    expected account ID, and ``ENABLE_LIVE_TRADING=true``. The adapter never
    unlocks trading through the SDK; the OpenD GUI must be unlocked manually.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 11111,
        market: str = "US",
        asset_class: str = "Securities",
        trd_env: str = "SIMULATE",
        acc_id: int | None = None,
        expected_account_id: int | str | None = None,
        enable_live_trading: bool = False,
        security_firm: str | None = None,
        db_path: str = DB_PATH,
    ):
        self.host = host
        self.port = int(port)
        self.market = market.upper()
        self.asset_class = asset_class
        self.trd_env_name = str(trd_env).upper()
        if self.trd_env_name not in {"SIMULATE", "REAL"}:
            raise ValueError("trd_env must be SIMULATE or REAL")
        self.enable_live_trading = bool(enable_live_trading)
        self.expected_account_id = str(expected_account_id) if expected_account_id is not None else None
        self._acc_id = int(acc_id) if acc_id is not None else None
        if self._acc_id is not None and self._acc_id <= 0:
            raise ValueError("acc_id must be positive")
        if self.expected_account_id is not None:
            try:
                if int(self.expected_account_id) <= 0:
                    raise ValueError("expected_account_id must be positive")
            except ValueError:
                raise ValueError("expected_account_id must be a positive integer")
        self.security_firm = security_firm
        self.db_path = db_path
        self._connected = False
        self.trade_context = None
        self.quote_context = None
        # Keep the exact account rows returned during connect.  The SIM smoke
        # harness uses this evidence to display the selected account without
        # performing a second, potentially different account discovery call.
        self.account_rows: list[dict[str, Any]] = []
        self.account_row: dict[str, Any] | None = None
        self._order_query_cache: tuple[float, list[dict[str, Any]]] | None = None
        self.trd_env = getattr(moo.TrdEnv, self.trd_env_name, None) if moo else None
        if self.trd_env_name == "REAL":
            if not self.enable_live_trading:
                raise PermissionError("REAL trading is disabled; set ENABLE_LIVE_TRADING=true explicitly")
            if self.expected_account_id is None:
                raise PermissionError("REAL trading requires EXPECTED_REAL_ACCOUNT_ID")
            if self._acc_id is not None and str(self._acc_id) != self.expected_account_id:
                raise PermissionError("Configured acc_id does not match EXPECTED_REAL_ACCOUNT_ID")
            self._acc_id = int(self.expected_account_id)

    @property
    def account_id(self) -> int | None:
        return self._acc_id

    def _validate_account_rows(self, rows: list[dict[str, Any]]) -> None:
        matches = []
        for row in rows:
            env = _normalise_env(_get(row, "trd_env", "trading_env"))
            acc_id = _get(row, "acc_id", "account_id")
            if env != self.trd_env_name or acc_id is None:
                continue
            auth = _parse_market_auth(_get(row, "trdmarket_auth", "market_auth"))
            role = _normalise_env(_get(row, "acc_role", "role"))
            status = _normalise_env(_get(row, "acc_status", "status"))
            acc_type = _normalise_env(_get(row, "acc_type", "account_type"))
            # Keep the adapter fail-closed as well as the SIM smoke harness:
            # blank authorization, inactive accounts, MASTER rows, and cash
            # accounts cannot be used for a two-leg US stock/short test.
            if (
                status != "ACTIVE"
                or role == "MASTER"
                or self.market not in auth
                or acc_type not in {"MARGIN", "STOCK_AND_OPTION"}
            ):
                continue
            matches.append((str(acc_id), role, row))
        if self.trd_env_name == "REAL":
            exact = [item for item in matches if item[0] == self.expected_account_id]
            if not exact:
                raise PermissionError(
                    f"Broker account identity mismatch: expected {self.expected_account_id} "
                    f"with {self.market} permission"
                )
            if any(item[1] == "MASTER" for item in exact):
                raise PermissionError("The configured REAL account is MASTER and cannot place orders")
        elif self._acc_id is None:
            if not matches:
                raise RuntimeError(f"No {self.trd_env_name} account authorized for {self.market}")
            # Auto-selection is permitted only for SIMULATE.
            self._acc_id = int(matches[0][0])
        elif not any(item[0] == str(self._acc_id) for item in matches):
            raise PermissionError(
                f"Configured {self.trd_env_name} account {self._acc_id} is not authorized for {self.market}"
            )

    def connect(self) -> bool:
        if moo is None:
            raise ImportError("moomoo-api is not installed. Install the moomoo package before using MooMooAdapter.")
        if self.trd_env is None:
            raise RuntimeError(f"Moomoo SDK does not expose trading environment {self.trd_env_name}")
        if self.asset_class != "Securities":
            raise NotImplementedError(f"Asset class {self.asset_class} not supported by this adapter.")
        market_map = {
            "US": moo.TrdMarket.US,
            "HK": moo.TrdMarket.HK,
            "SG": moo.TrdMarket.SG,
            "MY": moo.TrdMarket.MY,
            "JP": moo.TrdMarket.JP,
        }
        if self.market not in market_map:
            raise NotImplementedError(f"Market {self.market} not supported.")
        kwargs: dict[str, Any] = {
            "host": self.host,
            "port": self.port,
            "is_encrypt": False,
            # NONE is important: filtering by US/HK can hide accounts needed
            # for identity verification.
            "filter_trdmarket": moo.TrdMarket.NONE,
        }
        if self.security_firm and hasattr(moo, "SecurityFirm"):
            kwargs["security_firm"] = getattr(moo.SecurityFirm, self.security_firm, self.security_firm)
        try:
            self.trade_context = moo.OpenSecTradeContext(**kwargs)
        except TypeError:
            kwargs.pop("security_firm", None)
            self.trade_context = moo.OpenSecTradeContext(**kwargs)
        ret, acc_data = self.trade_context.get_acc_list()
        if ret != 0 or acc_data is None:
            raise RuntimeError(f"get_acc_list failed with ret={ret}: {acc_data}")
        self.account_rows = _as_records(acc_data)
        self._validate_account_rows(self.account_rows)
        for row in self.account_rows:
            if str(_get(row, "acc_id", "account_id", default="")) == str(self._acc_id):
                self.account_row = row
                break
        self._connected = True
        print(self.startup_summary())
        return True

    def startup_summary(self) -> str:
        armed = self.trd_env_name == "REAL" and self.enable_live_trading
        return (
            f"Moomoo execution environment={self.trd_env_name} "
            f"account_id={self._acc_id or 'unselected'} "
            f"live_armed={'YES' if armed else 'NO'}"
        )

    def disconnect(self) -> bool:
        if self.quote_context is not None:
            try:
                self.quote_context.close()
            except Exception:
                pass
            self.quote_context = None
        if self.trade_context is not None:
            self.trade_context.close()
        self._connected = False
        return True

    def _require_connected(self):
        if not self._connected or self.trade_context is None:
            raise ConnectionError("MooMooAdapter is not connected")

    def get_account_balance(self) -> AccountBalance:
        self._require_connected()
        ret, data = self.trade_context.accinfo_query(
            trd_env=self.trd_env, acc_id=self._acc_id or 0, refresh_cache=True
        )
        if ret != 0:
            raise RuntimeError(f"accinfo_query failed with ret={ret}")
        row = _as_records(data)[0] if _as_records(data) else {}
        cash = _safe_float(_get(row, "cash", "avail_cash", "available_cash"), 0.0) or 0.0
        buying_power = _safe_float(_get(row, "buying_power", "power", "available_power"), cash) or cash
        equity = _safe_float(_get(row, "equity", "total_assets", "asset_value"), cash) or cash
        initial_margin = _safe_float(_get(row, "initial_margin", "used_initial_margin", "margin_used", "used_margin"), None)
        maintenance_margin = _safe_float(_get(row, "maintenance_margin", "used_maintenance_margin", "maintenance_margin_used"), None)
        return AccountBalance(
            cash=cash,
            buying_power=buying_power,
            equity=equity,
            initial_margin=initial_margin,
            maintenance_margin=maintenance_margin,
            account_id=str(self._acc_id) if self._acc_id is not None else None,
        )

    def get_positions(self) -> List[Position]:
        self._require_connected()
        ret, data = self.trade_context.position_list_query(
            trd_env=self.trd_env, acc_id=self._acc_id or 0, refresh_cache=True
        )
        if ret != 0:
            raise RuntimeError(f"position_list_query failed with ret={ret}")
        positions: List[Position] = []
        for row in _as_records(data):
            positions.append(
                Position(
                    symbol=str(_get(row, "code", "symbol", default="")),
                    quantity=_safe_float(_get(row, "qty", "quantity", "position", "can_sell_qty"), 0.0) or 0.0,
                    average_price=_safe_float(_get(row, "average_price", "position_avg_price", "cost_price"), 0.0) or 0.0,
                    current_price=_safe_float(_get(row, "current_price", "market_price", "last_price"), None),
                    pnl=_safe_float(_get(row, "unrealized_pl", "pnl", "pl_val"), None),
                    side=_normalise_env(_get(row, "position_side", "position_type", "side", default="")) or None,
                    broker_position_id=str(_get(row, "position_id", "id", default="")) or None,
                )
            )
        return positions

    def place_order(self, order: Order) -> tuple[str, str]:
        self._require_connected()
        if self.trd_env_name == "REAL" and not self.enable_live_trading:
            raise PermissionError("REAL order blocked: live trading is not armed")
        moo_order_type = moo.OrderType.MARKET if order.order_type == OrderType.MARKET else moo.OrderType.NORMAL
        trd_side = moo.TrdSide.BUY if order.side == OrderSide.BUY else moo.TrdSide.SELL
        raw_price = float(order.price) if order.price is not None else 0.0
        # Moomoo expects market orders to use a zero order price. The
        # strategy's intended/reference price remains in the local ledger for
        # risk and slippage calculations.
        price = 0.0 if order.order_type == OrderType.MARKET else (round(raw_price, 2) if raw_price > 0.0 else 0.0)
        kwargs: dict[str, Any] = {
            "price": price,
            "qty": float(order.quantity),
            "code": order.symbol,
            "trd_side": trd_side,
            "order_type": moo_order_type,
            "trd_env": self.trd_env,
            "acc_id": self._acc_id or 0,
        }
        if order.remark:
            kwargs["remark"] = order.remark
        ret, data = self.trade_context.place_order(**kwargs)
        if ret != 0:
            raise RuntimeError(f"place_order failed with ret={ret}: {data}")
        records = _as_records(data)
        row = records[0] if records else {}
        order_id = _get(row, "order_id", "id", "orderid")
        if order_id is None:
            raise RuntimeError("Broker accepted order but returned no broker order ID")
        create_time = str(_get(row, "create_time", "updated_time", default=""))
        order.order_id = str(order_id)
        order.status = OrderStatus.SUBMITTED
        self._invalidate_order_query_cache()
        return order.order_id, create_time

    def cancel_order(self, order_id: str) -> bool:
        self._require_connected()
        ret, data = self.trade_context.modify_order(
            modify_order_op=moo.ModifyOrderOp.CANCEL,
            order_id=int(order_id), qty=0, price=0,
            trd_env=self.trd_env, acc_id=self._acc_id or 0,
        )
        if ret != 0:
            raise RuntimeError(f"cancel_order failed with ret={ret}: {data}")
        self._invalidate_order_query_cache()
        return True

    def start_push(self) -> None:
        self._require_connected()
        self.trade_context.set_handler(_TradeOrderPushHandler(self.db_path))
        self.trade_context.set_handler(_TradeDealPushHandler(self.db_path))
        self.trade_context.start()

    def stop_push(self) -> None:
        if self.trade_context is not None:
            try:
                self.trade_context.close()
            except Exception:
                pass

    def _invalidate_order_query_cache(self) -> None:
        """Discard a stale order snapshot after a broker-side mutation."""
        self._order_query_cache = None

    def _query_orders(self) -> list[dict[str, Any]]:
        now = time.monotonic()
        if self._order_query_cache is not None:
            cached_at, cached_rows = self._order_query_cache
            if now - cached_at < _ORDER_QUERY_CACHE_SECONDS:
                return [dict(row) for row in cached_rows]
        ret, data = self.trade_context.order_list_query(
            trd_env=self.trd_env, acc_id=self._acc_id or 0, refresh_cache=True
        )
        if ret != 0:
            raise RuntimeError(f"order_list_query failed with ret={ret}: {data}")
        rows = _as_records(data)
        self._order_query_cache = (now, rows)
        return [dict(row) for row in rows]

    def get_open_orders(self) -> list[dict[str, Any]]:
        self._require_connected()
        rows = self._query_orders()
        result = []
        for row in rows:
            status = _status_name(_get(row, "order_status", "status", default=""))
            if _map_broker_status(status) not in {"cancelled", "rejected", "filled"}:
                result.append(row)
        return result

    def get_recent_orders(self) -> list[dict[str, Any]]:
        self._require_connected()
        return self._query_orders()

    def get_order_status(self, order_id: str) -> OrderStatus:
        self._require_connected()
        for row in self._query_orders():
            if str(_get(row, "order_id", "id", "orderid", default="")) != str(order_id):
                continue
            status = _status_name(_get(row, "order_status", "status", default=""))
            mapped = _map_broker_status(status)
            return {
                "filled": OrderStatus.FILLED,
                "cancelled": OrderStatus.CANCELLED,
                "rejected": OrderStatus.REJECTED,
                "partially_filled": OrderStatus.PARTIALLY_FILLED,
            }.get(mapped, OrderStatus.SUBMITTED)
        return OrderStatus.PENDING

    def get_order_fills(self, order_id: str) -> list[dict]:
        self._require_connected()
        ret, data = self.trade_context.deal_list_query(
            trd_env=self.trd_env, acc_id=self._acc_id or 0, refresh_cache=True
        )
        if ret == 0 and data is not None:
            result = []
            for row in _as_records(data):
                if str(_get(row, "order_id", default="")) == str(order_id):
                    result.append(
                        {
                            "order_id": str(order_id),
                            "deal_id": _get(row, "deal_id", "fill_id"),
                            "code": _get(row, "code", default=""),
                            "qty": _safe_float(_get(row, "qty", "dealt_qty"), 0.0) or 0.0,
                            "price": _safe_float(_get(row, "price", "dealt_avg_price"), 0.0) or 0.0,
                            "trd_side": _get(row, "trd_side", default=""),
                            "create_time": _get(row, "create_time", "updated_time", default=""),
                        }
                    )
            if result:
                return result
        for row in self._query_orders():
            if str(_get(row, "order_id", default="")) == str(order_id):
                qty = _safe_float(_get(row, "dealt_qty", "filled_qty"), 0.0) or 0.0
                price = _safe_float(_get(row, "dealt_avg_price", "avg_fill_price"), 0.0) or 0.0
                if qty > 0 and price > 0:
                    return [{
                        "order_id": str(order_id),
                        "qty": qty,
                        "price": price,
                        "create_time": _get(row, "updated_time", "create_time", default=""),
                    }]
        return []

    def get_market_state(self, symbols: list[str]) -> dict[str, Any]:
        """Query market/session state when the quote API is available."""
        self._require_connected()
        if self.quote_context is None:
            self.quote_context = moo.OpenQuoteContext(host=self.host, port=self.port)
        ret, data = self.quote_context.get_market_state(symbols)
        if ret != 0:
            raise RuntimeError(f"get_market_state failed with ret={ret}: {data}")
        return {"symbols": symbols, "rows": _as_records(data)}

    def get_market_snapshot(self, symbols: list[str]) -> dict[str, Any]:
        """Return fresh reference snapshots for explicit symbols.

        Snapshot data is intentionally kept as broker-shaped records so the
        smoke harness can retain ``update_time``/bid/ask evidence alongside
        the last price used as the execution engine's intended price.
        """
        self._require_connected()
        if not symbols:
            return {"symbols": [], "rows": []}
        if self.quote_context is None:
            self.quote_context = moo.OpenQuoteContext(host=self.host, port=self.port)
        ret, data = self.quote_context.get_market_snapshot(symbols)
        if ret != 0:
            raise RuntimeError(f"get_market_snapshot failed with ret={ret}: {data}")
        return {"symbols": list(symbols), "rows": _as_records(data)}

    def validate_symbols(self, symbols: list[str]) -> bool:
        self._require_connected()
        if self.quote_context is None:
            self.quote_context = moo.OpenQuoteContext(host=self.host, port=self.port)
        market_enum = getattr(getattr(moo, "Market", None), self.market, self.market)
        ret, data = self.quote_context.get_stock_basicinfo(market_enum, code_list=symbols)
        if ret != 0:
            raise RuntimeError(f"get_stock_basicinfo failed with ret={ret}: {data}")
        return len(_as_records(data)) >= len(symbols)
