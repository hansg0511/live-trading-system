"""Pure translation from the current stat-arb plan to a generic order intent."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import math
from typing import Any

from src.trading_core.domain import (
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


def _required(source: Mapping[str, Any], name: str) -> Any:
    if name not in source or source[name] is None:
        raise ValueError(f"{name} is required")
    return source[name]


def _positive_integer(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a positive integer")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if not math.isfinite(number) or number <= 0 or not number.is_integer():
        raise ValueError(f"{name} must be a positive integer")
    return int(number)


def _resolve_instrument_id(
    instrument_ids: Mapping[str, str] | Sequence[str], symbol: str, sequence: int
) -> str:
    if isinstance(instrument_ids, Mapping):
        candidates = (symbol, symbol.upper(), f"US.{symbol}", f"US.{symbol.upper()}")
        for candidate in candidates:
            if candidate in instrument_ids:
                value = str(instrument_ids[candidate]).strip()
                if value:
                    return value
        raise KeyError(f"No internal instrument ID supplied for {symbol}")
    if len(instrument_ids) != 2:
        raise ValueError("instrument_ids must contain exactly two IDs")
    value = str(instrument_ids[sequence]).strip()
    if not value:
        raise ValueError(f"Internal instrument ID for {symbol} cannot be empty")
    return value


class PairIntentTranslator:
    """Translate one current entry decision without persistence or broker calls."""

    def __init__(
        self,
        *,
        strategy_id: str = "stat_arb",
        account_id: str | int | None = None,
        book_id: str | None = None,
    ) -> None:
        self.strategy_id = str(strategy_id).strip()
        self.account_id = None if account_id is None else str(account_id).strip()
        self.book_id = None if book_id is None else str(book_id).strip()
        if not self.strategy_id:
            raise ValueError("strategy_id is required")

    def translate(
        self,
        decision: Mapping[str, Any],
        plan: Mapping[str, Any],
        *,
        instrument_ids: Mapping[str, str] | Sequence[str],
        operation_date: str,
        account_id: str | int | None = None,
        idempotency_key: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> OrderIntent:
        strategy_identifier = str(_required(decision, "pair")).strip()
        symbols = (
            str(_required(decision, "ticker1")).strip(),
            str(_required(decision, "ticker2")).strip(),
        )
        if not strategy_identifier or not all(symbols) or symbols[0] == symbols[1]:
            raise ValueError("decision must contain a non-empty pair and two distinct tickers")
        operation_date = str(operation_date).strip()
        if not operation_date:
            raise ValueError("operation_date is required")
        resolved_account_id = str(account_id if account_id is not None else self.account_id or "").strip()
        if not resolved_account_id:
            raise ValueError("account_id is required")

        key = str(idempotency_key or f"{self.strategy_id}|{strategy_identifier}|entry|{operation_date}").strip()
        if not key:
            raise ValueError("idempotency_key is required")
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:24]
        intent_id = f"intent-{digest}"
        now = datetime.now(timezone.utc)

        sides = (
            Side(str(_required(plan, "ticker1_side"))),
            Side(str(_required(plan, "ticker2_side"))),
        )
        quantities = (
            _positive_integer(_required(plan, "ticker1_qty"), "ticker1_qty"),
            _positive_integer(_required(plan, "ticker2_qty"), "ticker2_qty"),
        )
        legs = tuple(
            OrderLeg(
                id=f"{intent_id}-leg-{sequence}",
                intent_id=intent_id,
                sequence=sequence,
                instrument_id=_resolve_instrument_id(instrument_ids, symbol, sequence),
                side=sides[sequence],
                quantity=Decimal(quantities[sequence]),
                quantity_unit=QuantityUnit.UNITS,
                order_type="MARKET",
                status=LegStatus.PLANNED,
                metadata={
                    "legacy_role": f"ticker{sequence + 1}",
                    "source_symbol": symbol,
                    "intended_quantity": plan.get(f"ticker{sequence + 1}_intended_qty"),
                },
                created_at=now,
                updated_at=now,
            )
            for sequence, symbol in enumerate(symbols)
        )
        provenance = {
            "strategy_identifier": strategy_identifier,
            "operation_date": operation_date,
            "ticker1_intended_qty": plan.get("ticker1_intended_qty"),
            "ticker2_intended_qty": plan.get("ticker2_intended_qty"),
            **{
                name: decision[name]
                for name in (
                    "entry_zscore",
                    "entry_hedge_ratio",
                    "entry_alpha",
                    "entry_residual_mean",
                    "entry_residual_std",
                    "latest_price_s1",
                    "latest_price_s2",
                )
                if name in decision
            },
        }
        if metadata:
            provenance.update(dict(metadata))
        return OrderIntent(
            id=intent_id,
            idempotency_key=key,
            strategy_id=self.strategy_id,
            book_id=self.book_id,
            account_id=resolved_account_id,
            action=IntentAction.ENTER,
            status=IntentStatus.CREATED,
            source_signal_id=str(decision["source_signal_id"]) if decision.get("source_signal_id") else None,
            execution_policy=ExecutionPolicy(
                legging_policy=LeggingPolicy.SEQUENTIAL,
                partial_fill_policy=PartialFillPolicy.WAIT,
                failure_policy=FailurePolicy.HOLD_AND_RECONCILE,
            ),
            legs=legs,
            metadata=provenance,
            created_at=now,
            updated_at=now,
        )

    to_order_intent = translate
    from_entry_plan = translate


__all__ = ["PairIntentTranslator"]
