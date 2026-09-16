"""Focused tests for the schema-aligned generic trading contracts."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, fields
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from src.trading_core import (
    Account,
    ExecutionPolicy,
    ExecutionSession,
    IntentAction,
    OrderIntent,
    OrderLeg,
    Side,
    TradingEnvironment,
)


UTC = timezone.utc


def _account() -> Account:
    return Account(
        id="account-1",
        broker="broker-1",
        environment=TradingEnvironment.SIM,
        external_account_id="external-account-1",
        base_currency="USD",
    )


def _leg(intent_id: str, sequence: int, *, leg_id: str | None = None) -> OrderLeg:
    return OrderLeg(
        id=leg_id or f"leg-{sequence}",
        intent_id=intent_id,
        sequence=sequence,
        instrument_id=f"instrument-{sequence}",
        side=Side.BUY if sequence % 2 == 0 else Side.SELL,
        quantity=Decimal(sequence + 1),
    )


def _intent(legs: tuple[OrderLeg, ...], *, intent_id: str = "intent-1") -> OrderIntent:
    return OrderIntent(
        id=intent_id,
        idempotency_key=f"key-{intent_id}",
        strategy_id="strategy-1",
        account_id="account-1",
        action=IntentAction.ENTER,
        legs=legs,
    )


@pytest.mark.parametrize("leg_count", [1, 2, 3])
def test_intents_support_one_two_and_three_legs(leg_count: int) -> None:
    intent_id = f"intent-{leg_count}"
    sequences = list(reversed(range(leg_count)))
    intent = _intent(
        tuple(_leg(intent_id, sequence) for sequence in sequences),
        intent_id=intent_id,
    )

    assert isinstance(intent.legs, tuple)
    assert [leg.sequence for leg in intent.legs] == list(range(leg_count))
    assert [leg.intent_id for leg in intent.legs] == [intent.id] * leg_count
    assert all(isinstance(leg.quantity, Decimal) and leg.quantity > 0 for leg in intent.legs)


def test_schema_identity_fields_are_present() -> None:
    assert {item.name for item in fields(Account)} >= {
        "id",
        "broker",
        "environment",
        "external_account_id",
        "base_currency",
        "enabled",
        "metadata",
        "created_at",
        "updated_at",
    }
    assert {item.name for item in fields(OrderLeg)} >= {
        "id",
        "intent_id",
        "sequence",
        "instrument_id",
        "side",
        "quantity",
        "quantity_unit",
        "order_type",
        "limit_price",
        "stop_price",
        "time_in_force",
        "status",
        "cumulative_filled_quantity",
        "average_fill_price",
        "metadata",
        "created_at",
        "updated_at",
    }


def test_domain_values_are_frozen_and_slot_based() -> None:
    intent = _intent((_leg("intent-frozen", 0),), intent_id="intent-frozen")

    with pytest.raises(FrozenInstanceError):
        intent.id = "changed"  # type: ignore[misc]
    with pytest.raises((AttributeError, TypeError)):
        intent.unexpected = "value"  # type: ignore[attr-defined]


@pytest.mark.parametrize("quantity", [Decimal("0"), Decimal("-1")])
def test_order_leg_rejects_non_positive_quantity(quantity: Decimal) -> None:
    with pytest.raises(ValueError, match="quantity must be positive"):
        OrderLeg(
            id="leg-invalid-quantity",
            intent_id="intent-invalid-quantity",
            sequence=0,
            instrument_id="instrument-1",
            side=Side.BUY,
            quantity=quantity,
        )


def test_order_intent_rejects_empty_legs() -> None:
    with pytest.raises(ValueError, match="at least one leg"):
        _intent(())


def test_execution_session_is_distinct_from_legacy_extended_hours_toggle() -> None:
    assert ExecutionPolicy().execution_session is ExecutionSession.REGULAR
    assert ExecutionPolicy(allow_extended_hours=True).execution_session is ExecutionSession.EXTENDED
    assert ExecutionPolicy(execution_session=ExecutionSession.OVERNIGHT).allow_extended_hours is False

    with pytest.raises(ValueError, match="cannot be combined"):
        ExecutionPolicy(allow_extended_hours=True, execution_session=ExecutionSession.OVERNIGHT)


def test_order_intent_rejects_duplicate_leg_ids() -> None:
    legs = (
        _leg("intent-duplicate-id", 0, leg_id="duplicate"),
        _leg("intent-duplicate-id", 1, leg_id="duplicate"),
    )
    with pytest.raises(ValueError, match="unique IDs"):
        _intent(legs, intent_id="intent-duplicate-id")


def test_order_intent_rejects_duplicate_sequences() -> None:
    legs = (
        _leg("intent-duplicate-sequence", 0, leg_id="leg-a"),
        _leg("intent-duplicate-sequence", 0, leg_id="leg-b"),
    )
    with pytest.raises(ValueError, match="unique sequences"):
        _intent(legs, intent_id="intent-duplicate-sequence")


def test_order_intent_rejects_mismatched_leg_intent_id() -> None:
    with pytest.raises(ValueError, match="does not match"):
        _intent((_leg("intent-other", 0),), intent_id="intent-parent")


def test_datetimes_must_be_timezone_aware_and_are_normalized_to_utc() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        Account(
            id="account-naive",
            broker="broker-1",
            environment=TradingEnvironment.SIM,
            external_account_id="external-account-1",
            base_currency="USD",
            created_at=datetime(2026, 1, 1),
        )

    account = Account(
        id="account-utc",
        broker="broker-1",
        environment=TradingEnvironment.SIM,
        external_account_id="external-account-1",
        base_currency="USD",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert account.created_at.tzinfo is UTC
