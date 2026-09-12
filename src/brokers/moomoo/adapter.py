from __future__ import annotations

import sqlite3
from typing import Any, List

import pandas as pd
from src.core.interfaces import BrokerAdapter
from src.core.models import AccountBalance, Order, OrderSide, OrderStatus, OrderType, Position
from src.db.positions_db import record_order_fill, update_order_status, DB_PATH

try:
    import moomoo as moo  # type: ignore
except ImportError:  # pragma: no cover - handled at runtime
    moo = None  # type: ignore[assignment]


class _TradeOrderPushHandler(moo.TradeOrderHandlerBase):
    """Receives push notifications when order status changes (fills, cancels, rejects)."""

    def __init__(self, db_path: str = DB_PATH):
        super().__init__()
        self.db_path = db_path

    def on_recv_rsp(self, rsp_pb):
        ret_code, ret_data = super().on_recv_rsp(rsp_pb)
        if ret_code != moo.RET_OK:
            return ret_code, ret_data
        if ret_data is None or len(ret_data) == 0:
            return ret_code, ret_data
        row = ret_data.iloc[0]
        broker_order_id = str(row["order_id"])
        new_status = str(row["order_status"]).upper()

        if new_status in ("FILLED_ALL", "FILLED", "FULLY_FILLED"):
            fill_price = float(row["dealt_avg_price"])
            fill_qty = float(row["dealt_qty"])
            _record_fill(broker_order_id, fill_price, fill_qty, str(row["updated_time"]), self.db_path)
        elif new_status in ("CANCELLED_ALL", "CANCELLED"):
            _update_status(broker_order_id, "cancelled", self.db_path)
        elif new_status in ("REJECTED", "FAILED"):
            _update_status(broker_order_id, "rejected", self.db_path)
        return ret_code, ret_data


class _TradeDealPushHandler(moo.TradeDealHandlerBase):
    """Receives push notifications when deals (fills) occur."""

    def __init__(self, db_path: str = DB_PATH):
        super().__init__()
        self.db_path = db_path

    def on_recv_rsp(self, rsp_pb):
        ret_code, ret_data = super().on_recv_rsp(rsp_pb)
        if ret_code != moo.RET_OK:
            return ret_code, ret_data
        if ret_data is None or len(ret_data) == 0:
            return ret_code, ret_data
        row = ret_data.iloc[0]
        broker_order_id = str(row["order_id"])
        fill_price = float(row["price"])
        fill_qty = float(row["qty"])
        _record_fill(broker_order_id, fill_price, fill_qty, str(row["create_time"]), self.db_path)
        return ret_code, ret_data


def _find_order_by_broker_id(broker_order_id: str, db_path: str) -> dict | None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT id, intended_price, side FROM orders WHERE broker_order_id = ?",
        (broker_order_id,),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def _record_fill(broker_order_id: str, fill_price: float, fill_qty: float, fill_time: str, db_path: str):
    order = _find_order_by_broker_id(broker_order_id, db_path)
    if order is None:
        return
    record_order_fill(
        order_id=order["id"],
        fill_price=fill_price,
        fill_time=fill_time,
        fill_quantity=fill_qty,
        intended_price=float(order["intended_price"]),
        db_path=db_path,
    )


def _update_status(broker_order_id: str, status: str, db_path: str):
    order = _find_order_by_broker_id(broker_order_id, db_path)
    if order is None:
        return
    update_order_status(order["id"], status, db_path=db_path)


class MooMooAdapter(BrokerAdapter):
    """
    Synchronous BrokerAdapter implementation for MooMoo via moomoo-api.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 11111,
        market: str = "US",
        asset_class: str = "Securities",
        trd_env: str = "SIMULATE",
        acc_id: int | None = None,
    ):
        self.host = host
        self.port = port
        self.market = market.upper()
        self.asset_class = asset_class
        self.trd_env = getattr(moo.TrdEnv, trd_env, moo.TrdEnv.SIMULATE) if moo else None
        self._acc_id = acc_id
        self._connected = False
        self.trade_context = None

    def connect(self) -> bool:
        if moo is None:
            raise ImportError(
                "moomoo-api is not installed. Install the moomoo package before using MooMooAdapter."
            )

        if self.asset_class != "Securities":
            raise NotImplementedError(
                f"Asset class {self.asset_class} not supported by this adapter."
            )

        market_map = {
            "US": moo.TrdMarket.US,
            "HK": moo.TrdMarket.HK,
            "SG": moo.TrdMarket.SG,
            "MY": moo.TrdMarket.MY,
            "JP": moo.TrdMarket.JP,
        }
        if self.market not in market_map:
            raise NotImplementedError(f"Market {self.market} not supported.")

        self.trade_context = moo.OpenSecTradeContext(
            host=self.host,
            port=self.port,
            is_encrypt=False,
            filter_trdmarket=market_map[self.market],
        )

        # Auto-discover acc_id if not explicitly provided
        if self._acc_id is None:
            ret, acc_data = self.trade_context.get_acc_list()
            if ret == 0 and acc_data is not None and len(acc_data):
                for _, row in acc_data.iterrows():
                    if row["trd_env"] == self.trd_env:
                        self._acc_id = int(row["acc_id"])
                        break

        self._connected = True
        return True

    def disconnect(self) -> bool:
        if self.trade_context is not None:
            self.trade_context.close()
        self._connected = False
        return True

    def _require_connected(self):
        if not self._connected or self.trade_context is None:
            raise ConnectionError("MooMooAdapter is not connected")

    @staticmethod
    def _first_row(data: Any) -> Any:
        if data is None:
            return None
        if isinstance(data, list):
            return data[0] if data else None
        if hasattr(data, "iloc"):
            return data.iloc[0] if len(data) else None
        if hasattr(data, "to_dict"):
            try:
                records = data.to_dict("records")
                return records[0] if records else None
            except Exception:
                return data
        return data

    @staticmethod
    def _get_value(row: Any, *keys: str, default: Any = None) -> Any:
        if row is None:
            return default
        if isinstance(row, dict):
            for key in keys:
                if key in row and row[key] is not None:
                    return row[key]
            return default
        for key in keys:
            if hasattr(row, key):
                value = getattr(row, key)
                if value is not None:
                    return value
            try:
                value = row[key]  # type: ignore[index]
                if value is not None:
                    return value
            except Exception:
                continue
        return default

    def get_account_balance(self) -> AccountBalance:
        self._require_connected()
        ret, data = self.trade_context.accinfo_query(
            trd_env=self.trd_env,
            acc_id=self._acc_id or 0,
            refresh_cache=True,
        )
        if ret != 0:
            raise RuntimeError(f"accinfo_query failed with ret={ret}")

        row = self._first_row(data)
        cash = float(self._get_value(row, "cash", "avail_cash", "available_cash", default=0.0) or 0.0)
        buying_power = float(
            self._get_value(row, "buying_power", "power", "available_power", default=cash) or cash
        )
        equity = float(self._get_value(row, "equity", "total_assets", "asset_value", default=cash) or cash)
        return AccountBalance(cash=cash, buying_power=buying_power, equity=equity)

    def get_positions(self) -> List[Position]:
        self._require_connected()
        ret, data = self.trade_context.position_list_query(
            trd_env=self.trd_env,
            acc_id=self._acc_id or 0,
            refresh_cache=True,
        )
        if ret != 0:
            raise RuntimeError(f"position_list_query failed with ret={ret}")

        if data is None:
            return []

        def _safe_float(val, default=0.0):
            if val is None:
                return default
            try:
                return float(val)
            except (ValueError, TypeError):
                return default

        rows = data.to_dict("records") if hasattr(data, "to_dict") else data
        positions: List[Position] = []
        for row in rows:
            symbol = str(self._get_value(row, "code", "symbol", default=""))
            qty = _safe_float(self._get_value(row, "qty", "quantity", "position", "can_sell_qty", default=0.0))
            avg_price = _safe_float(
                self._get_value(row, "average_price", "price", "cost_price", "position_avg_price", default=0.0)
            )
            current_price = _safe_float(
                self._get_value(row, "current_price", "market_price", "last_price"), default=None
            )
            pnl = _safe_float(self._get_value(row, "pnl", "unrealized_pl", "pl_val"), default=None)
            positions.append(
                Position(
                    symbol=symbol,
                    quantity=qty,
                    average_price=avg_price,
                    current_price=current_price,
                    pnl=pnl,
                )
            )
        return positions

    def place_order(self, order: Order) -> tuple[str, str]:
        self._require_connected()

        moo_order_type = moo.OrderType.MARKET if order.order_type == OrderType.MARKET else moo.OrderType.NORMAL
        trd_side = moo.TrdSide.BUY if order.side == OrderSide.BUY else moo.TrdSide.SELL
        raw_price = float(order.price) if order.price is not None else 0.0
        price = round(raw_price, 2) if raw_price > 0.0 else 0.0

        ret, data = self.trade_context.place_order(
            price=price,
            qty=float(order.quantity),
            code=order.symbol,
            trd_side=trd_side,
            order_type=moo_order_type,
            trd_env=self.trd_env,
        )
        if ret != 0:
            raise RuntimeError(f"place_order failed with ret={ret}: {data}")

        row = self._first_row(data)
        order_id = self._get_value(row, "order_id", "id", "orderid", default=None)
        if order_id is None:
            order_id = f"{order.symbol}-{order.side.value}-{order.quantity}"
        create_time = str(self._get_value(row, "create_time", default=""))

        order.order_id = str(order_id)
        order.status = OrderStatus.SUBMITTED
        return order.order_id, create_time

    def cancel_order(self, order_id: str) -> bool:
        self._require_connected()
        ret, data = self.trade_context.modify_order(
            modify_order_op=moo.ModifyOrderOp.CANCEL,
            order_id=int(order_id), qty=0, price=0,
            trd_env=self.trd_env,
        )
        if ret != 0:
            raise RuntimeError(f"cancel_order failed with ret={ret}: {data}")
        return True

    def start_push(self) -> None:
        """Register push handlers for order/deal events and start the async listener."""
        self._require_connected()
        self.trade_context.set_handler(_TradeOrderPushHandler())
        self.trade_context.set_handler(_TradeDealPushHandler())
        self.trade_context.start()

    def stop_push(self) -> None:
        """Stop the async push listener."""
        if self.trade_context is not None:
            self.trade_context.close()

    def get_order_status(self, order_id: str) -> OrderStatus:
        self._require_connected()
        ret, data = self.trade_context.order_list_query(
            trd_env=self.trd_env,
            acc_id=self._acc_id or 0,
            refresh_cache=True,
        )
        if ret != 0:
            print(f"  [get_order_status] error: data={data}")
            return OrderStatus.PENDING

        rows = data.to_dict("records") if hasattr(data, "to_dict") else (data or [])
        for row in rows:
            current_id = str(self._get_value(row, "order_id", "id", "orderid", default=""))
            if current_id != str(order_id):
                continue
            status = str(self._get_value(row, "order_status", "status", default="")).upper()
            if status in {"FILLED_ALL", "FILLED", "FULLY_FILLED"}:
                return OrderStatus.FILLED
            if status in {"CANCELLED_ALL", "CANCELLED", "DELETED"}:
                return OrderStatus.CANCELLED
            if status in {"REJECTED", "FAILED"}:
                return OrderStatus.REJECTED
            return OrderStatus.SUBMITTED
        return OrderStatus.PENDING

    def get_order_fills(self, order_id: str) -> list[dict]:
        """Get fill records for an order.

        Tries deal_list_query first (live accounts). Falls back to
        order_list_query aggregate data (SIMULATE where deal_list_query
        returns -1).
        """
        self._require_connected()
        ret, data = self.trade_context.deal_list_query(
            trd_env=self.trd_env,
            acc_id=self._acc_id or 0,
            refresh_cache=True,
        )
        if ret == 0 and data is not None and len(data):
            if isinstance(data, pd.DataFrame):
                filtered = data[data["order_id"].astype(str) == str(order_id)]
                return filtered.to_dict("records")
            return []
        ret, data = self.trade_context.order_list_query(
            trd_env=self.trd_env,
            acc_id=self._acc_id or 0,
            refresh_cache=True,
        )
        if ret != 0 or data is None:
            return []
        for _, row in data.iterrows():
            if str(row["order_id"]) == str(order_id):
                dealt_qty = float(row.get("dealt_qty", 0))
                if dealt_qty <= 0:
                    return []
                return [
                    {
                        "order_id": row["order_id"],
                        "code": row.get("code", ""),
                        "qty": dealt_qty,
                        "price": float(row.get("dealt_avg_price", 0)),
                        "trd_side": row.get("trd_side", ""),
                        "create_time": row.get("updated_time", ""),
                    }
                ]
        return []
