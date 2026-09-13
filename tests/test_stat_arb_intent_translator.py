"""Broker-free parity tests for the stat-arb to generic-intent boundary."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

from scripts.run_daily_signal import compute_pair_order_plan
from src.strategies.stat_arb.intent_translator import PairIntentTranslator
from src.trading_core.domain import OrderIntent, OrderLeg


def _decision(price1: float, price2: float, hedge_ratio: float, zscore: float) -> dict[str, Any]:
    return {
        "pair": "AAA-BBB",
        "ticker1": "AAA",
        "ticker2": "BBB",
        "entry_zscore": zscore,
        "entry_hedge_ratio": hedge_ratio,
        "latest_price_s1": price1,
        "latest_price_s2": price2,
    }


def _enum_label(value: Any) -> str:
    name = getattr(value, "name", None)
    if name:
        return str(name).upper()
    raw = getattr(value, "value", value)
    return str(raw).rsplit(".", 1)[-1].upper()


def _field(source: Any, *names: str) -> Any:
    if isinstance(source, Mapping):
        for name in names:
            if name in source:
                return source[name]
    for name in names:
        if hasattr(source, name):
            return getattr(source, name)
    raise AttributeError(f"{type(source).__name__} has none of {names!r}")


def _translate(decision: dict[str, Any], plan: Mapping[str, Any]):
    return PairIntentTranslator(account_id="sim-account").translate(
        decision,
        plan,
        instrument_ids={"AAA": "internal:alpha", "BBB": "internal:beta"},
        operation_date="2026-09-13",
    )


@pytest.mark.parametrize(
    ("price1", "price2", "hedge_ratio", "zscore"),
    [
        (100.0, 50.0, 1.0, 2.5),
        (100.0, 50.0, 1.0, -2.5),
        (137.25, 43.5, -0.63, 1.2),
        (137.25, 43.5, -0.63, -1.2),
    ],
)
def test_translator_orders_match_legacy_pair_plan(
    price1: float,
    price2: float,
    hedge_ratio: float,
    zscore: float,
):
    decision = _decision(price1, price2, hedge_ratio, zscore)
    plan = compute_pair_order_plan(price1, price2, hedge_ratio, zscore)

    intent = _translate(decision, plan)

    assert isinstance(intent, OrderIntent)
    legs = tuple(_field(intent, "legs"))
    assert len(legs) == 2
    assert all(isinstance(leg, OrderLeg) for leg in legs)

    expected = (
        ("internal:alpha", plan["ticker1_side"], plan["ticker1_qty"]),
        ("internal:beta", plan["ticker2_side"], plan["ticker2_qty"]),
    )
    for leg, (instrument_id, expected_side, expected_quantity) in zip(legs, expected):
        assert _field(leg, "instrument_id") == instrument_id
        assert _enum_label(_field(leg, "side")) == expected_side
        quantity = _field(leg, "requested_quantity", "quantity", "qty")
        assert quantity == expected_quantity
        assert int(quantity) == expected_quantity
        assert _enum_label(_field(leg, "order_type")) == "MARKET"
        assert _enum_label(_field(leg, "quantity_unit")) in {
            "SHARE",
            "SHARES",
            "UNIT",
            "UNITS",
        }
        if hasattr(leg, "status"):
            assert _enum_label(_field(leg, "status")) in {"PLANNED", "CREATED", "PENDING"}

    account = _field(intent, "account_id", "account")
    if hasattr(account, "account_id"):
        account = account.account_id
    assert account == "sim-account"
    assert _enum_label(_field(intent, "action")) in {"ENTRY", "ENTER", "OPEN"}
    assert _enum_label(_field(intent, "status")) == "CREATED"
    metadata = _field(intent, "metadata")
    assert metadata["ticker1_intended_qty"] == pytest.approx(plan["ticker1_intended_qty"])
    assert metadata["ticker2_intended_qty"] == pytest.approx(plan["ticker2_intended_qty"])

    policy = _field(intent, "execution_policy", "policy")
    assert _enum_label(_field(policy, "legging_policy", "legging")) == "SEQUENTIAL"
    assert _enum_label(_field(policy, "partial_fill_policy", "partial_fill")) in {
        "HOLD_AND_RECONCILE",
        "WAIT",
    }
    assert _enum_label(_field(policy, "failure_policy", "failure")) == "HOLD_AND_RECONCILE"


def test_translator_has_deterministic_idempotency_and_no_broker_side_effects():
    decision = _decision(100.0, 50.0, 1.0, 2.5)
    plan = compute_pair_order_plan(100.0, 50.0, 1.0, 2.5)
    translator = PairIntentTranslator(account_id="sim-account")
    kwargs = {
        "instrument_ids": {"AAA": "internal:alpha", "BBB": "internal:beta"},
        "operation_date": "2026-09-13",
    }

    first = translator.translate(decision, plan, **kwargs)
    second = translator.translate(decision, plan, **kwargs)
    assert _field(first, "idempotency_key") == _field(second, "idempotency_key")
    assert _field(first, "id", "intent_id") == _field(second, "id", "intent_id")

    next_day = translator.translate(
        decision,
        plan,
        instrument_ids=kwargs["instrument_ids"],
        operation_date="2026-09-14",
    )
    assert _field(next_day, "idempotency_key") != _field(first, "idempotency_key")


def test_translator_preserves_ordered_internal_ids_when_given_as_a_sequence():
    decision = _decision(100.0, 50.0, 1.0, 2.5)
    plan = compute_pair_order_plan(100.0, 50.0, 1.0, 2.5)
    intent = PairIntentTranslator(account_id="sim-account").translate(
        decision,
        plan,
        instrument_ids=("internal:alpha", "internal:beta"),
        operation_date="2026-09-13",
    )

    legs = tuple(_field(intent, "legs"))
    assert [_field(leg, "instrument_id") for leg in legs] == [
        "internal:alpha",
        "internal:beta",
    ]
