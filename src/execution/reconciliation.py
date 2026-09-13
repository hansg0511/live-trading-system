from __future__ import annotations

from dataclasses import dataclass, field
import math
from typing import Any, Iterable

from src.core.models import Position


@dataclass(frozen=True)
class ReconciliationIssue:
    category: str
    entity_key: str
    details: dict[str, Any]
    severity: str = "high"


@dataclass
class ReconciliationResult:
    issues: list[ReconciliationIssue] = field(default_factory=list)
    recovered_order_ids: list[str] = field(default_factory=list)
    applied_fill_count: int = 0

    @property
    def ready(self) -> bool:
        return not self.issues


def _get(row: Any, *keys: str, default: Any = None) -> Any:
    if isinstance(row, dict):
        for key in keys:
            if key in row and row[key] is not None:
                return row[key]
        return default
    for key in keys:
        try:
            value = row[key]
            if value is not None:
                return value
        except Exception:
            pass
        if hasattr(row, key):
            value = getattr(row, key)
            if value is not None:
                return value
    return default


def _float(value: Any, default: float = 0.0) -> float:
    try:
        value = float(value)
        return value if math.isfinite(value) else default
    except (TypeError, ValueError):
        return default


def _signed_quantity(row: Any) -> float:
    quantity = _float(_get(row, "quantity", "qty", "position", default=0.0))
    side = str(_get(row, "side", "position_side", "position_type", default="")).upper()
    if "SHORT" in side or side in {"SELL", "SHORT"}:
        return -abs(quantity)
    return quantity


def _normalise_symbol(value: Any) -> str:
    symbol = str(value or "")
    if symbol and not symbol.startswith(("US.", "HK.", "SG.", "MY.", "JP.", "SH.", "SZ.")):
        return "US." + symbol
    return symbol


def _expected_local_legs(local_positions: Iterable[Any]) -> dict[str, tuple[float, str]]:
    expected: dict[str, tuple[float, str]] = {}
    for position in local_positions:
        status = str(_get(position, "status", default="open")).lower()
        if status != "open":
            continue
        for ticker_key, side_key, qty_key in (
            ("ticker1", "entry_side1", "executed_size1"),
            ("ticker2", "entry_side2", "executed_size2"),
        ):
            symbol = _normalise_symbol(_get(position, ticker_key, default=""))
            side = str(_get(position, side_key, default="")).upper()
            qty = _float(_get(position, qty_key, default=0.0))
            expected[symbol] = (qty if side == "BUY" else -qty, side)
    return expected


def compare_broker_and_local_state(
    *,
    local_positions: Iterable[Any],
    broker_positions: Iterable[Any],
    local_orders: Iterable[Any],
    broker_orders: Iterable[Any],
    tolerance: float = 1e-6,
) -> ReconciliationResult:
    """Classify discrepancies without discarding either side's evidence."""
    result = ReconciliationResult()
    local_rows = list(local_positions)
    seen_pairs: dict[str, Any] = {}
    for position in local_rows:
        status = str(_get(position, "status", default="open")).lower()
        pair = str(_get(position, "pair", default=""))
        if status != "open" or not pair:
            continue
        if pair in seen_pairs:
            result.issues.append(
                ReconciliationIssue(
                    "duplicate_local_position",
                    pair,
                    {
                        "first_position": _get(seen_pairs[pair], "id", default=""),
                        "duplicate_position": _get(position, "id", default=""),
                    },
                )
            )
        else:
            seen_pairs[pair] = position
    expected = _expected_local_legs(local_rows)
    actual: dict[str, float] = {}
    actual_rows: dict[str, Any] = {}
    for row in broker_positions:
        symbol = _normalise_symbol(_get(row, "symbol", "code", default=""))
        if not symbol:
            continue
        actual[symbol] = actual.get(symbol, 0.0) + _signed_quantity(row)
        actual_rows[symbol] = row
    for symbol, (expected_qty, side) in expected.items():
        if symbol not in actual:
            result.issues.append(
                ReconciliationIssue(
                    "local_position_missing_at_broker",
                    symbol,
                    {"expected_quantity": expected_qty, "expected_side": side},
                )
            )
            continue
        if abs(actual[symbol] - expected_qty) > tolerance:
            result.issues.append(
                ReconciliationIssue(
                    "position_quantity_mismatch",
                    symbol,
                    {"expected_quantity": expected_qty, "broker_quantity": actual[symbol]},
                )
            )
    for symbol, quantity in actual.items():
        if abs(quantity) <= tolerance:
            continue
        if symbol not in expected:
            result.issues.append(
                ReconciliationIssue(
                    "broker_position_untracked",
                    symbol,
                    {"broker_quantity": quantity, "broker_row": str(actual_rows.get(symbol))},
                )
            )

    broker_by_id = {
        str(_get(row, "order_id", "id", "broker_order_id", default="")): row
        for row in broker_orders
        if _get(row, "order_id", "id", "broker_order_id") is not None
    }
    # ``pending``/``created`` local intents are expected to have no broker ID
    # before their turn in the two-leg submit sequence. Only a leg that has
    # entered the broker-call/accepted path requires correlation evidence.
    active_statuses = {"submitted", "partially_filled", "submitting"}
    for row in local_orders:
        status = str(_get(row, "status", default="")).lower()
        if status not in active_statuses:
            continue
        local_id = str(_get(row, "id", "local_order_id", default=""))
        broker_id = _get(row, "broker_order_id")
        if not broker_id:
            result.issues.append(
                ReconciliationIssue(
                    "local_order_missing_broker_id",
                    local_id,
                    {"symbol": _get(row, "symbol", "code"), "status": status},
                )
            )
        elif str(broker_id) not in broker_by_id:
            result.issues.append(
                ReconciliationIssue(
                    "local_order_missing_at_broker",
                    str(broker_id),
                    {"local_order_id": local_id, "status": status},
                )
            )
    local_broker_ids = {
        str(_get(row, "broker_order_id"))
        for row in local_orders
        if _get(row, "broker_order_id") is not None
    }
    for broker_id, row in broker_by_id.items():
        if broker_id and broker_id not in local_broker_ids:
            result.issues.append(
                ReconciliationIssue(
                    "broker_order_untracked",
                    broker_id,
                    {"symbol": _get(row, "code", "symbol"), "status": _get(row, "order_status", "status")},
                )
            )
    return result
