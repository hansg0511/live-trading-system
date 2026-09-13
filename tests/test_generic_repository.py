from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
import sqlite3

import pytest

from src.db.positions_db import init_db as init_legacy_db
from src.trading_core.domain import (
    Account,
    AssetClass,
    Book,
    BrokerOrderStatus,
    ExecutionPolicy,
    Fill,
    Instrument,
    IntentAction,
    IntentStatus,
    IssueSeverity,
    IssueStatus,
    LegStatus,
    OrderIntent,
    OrderLeg,
    QuantityUnit,
    ReconciliationIssue,
    ReconciliationRun,
    ReconciliationStatus,
    Side,
    Strategy,
    TradingEnvironment,
)
from src.trading_core.repository import IdempotencyConflict, SQLiteTradingRepository


NOW = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)


def account() -> Account:
    return Account(
        id="acct",
        broker="fake",
        environment=TradingEnvironment.SIM,
        external_account_id="external-acct",
        base_currency="USD",
        enabled=True,
        metadata={},
        created_at=NOW,
        updated_at=NOW,
    )


def strategy() -> Strategy:
    return Strategy(
        id="strategy",
        name="test strategy",
        strategy_type="test",
        version="1",
        enabled=True,
        config={},
        metadata={},
        created_at=NOW,
        updated_at=NOW,
    )


def instrument(number: int) -> Instrument:
    return Instrument(
        id=f"instrument-{number}",
        asset_class=AssetClass.EQUITY,
        symbol=f"SYM{number}",
        venue="TEST",
        currency="USD",
        multiplier=Decimal("1"),
        tick_size=Decimal("0.01"),
        lot_size=Decimal("1"),
        metadata={},
        created_at=NOW,
        updated_at=NOW,
    )


def intent(count: int = 1, *, quantity: Decimal = Decimal("10")) -> OrderIntent:
    intent_id = "intent"
    legs = tuple(
        OrderLeg(
            id=f"order-leg-{index}",
            intent_id=intent_id,
            sequence=index,
            instrument_id=f"instrument-{index}",
            side=Side.BUY if index % 2 == 0 else Side.SELL,
            quantity=quantity,
            quantity_unit=QuantityUnit.UNITS,
            order_type="MARKET",
            status=LegStatus.PLANNED,
            created_at=NOW,
            updated_at=NOW,
            metadata={},
        )
        for index in range(count)
    )
    return OrderIntent(
        id=intent_id,
        idempotency_key="intent-key",
        strategy_id="strategy",
        book_id=None,
        account_id="acct",
        action=IntentAction.ENTER,
        status=IntentStatus.CREATED,
        source_signal_id="signal",
        execution_policy=ExecutionPolicy(),
        legs=legs,
        created_at=NOW,
        updated_at=NOW,
        metadata={},
    )


def ready_repository(tmp_path, *, count: int = 3) -> SQLiteTradingRepository:
    repository = SQLiteTradingRepository(tmp_path / "generic.db")
    repository.initialize()
    repository.save_account(account())
    repository.save_strategy(strategy())
    for index in range(count):
        repository.save_instrument(instrument(index))
    return repository


def test_generic_schema_coexists_with_legacy_tables(tmp_path):
    path = tmp_path / "shared.db"
    init_legacy_db(path)
    repository = SQLiteTradingRepository(path)
    repository.initialize()

    with sqlite3.connect(path) as conn:
        names = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    assert "pair_operations" in names
    assert "core_order_intents" in names


def test_intent_and_all_legs_are_inserted_atomically_and_idempotently(tmp_path):
    repository = ready_repository(tmp_path)
    original = intent(3)

    assert repository.create_intent(original) == (original.id, True)
    assert repository.create_intent(original) == (original.id, False)
    stored = repository.get_intent(original.id)
    assert stored is not None
    assert [leg["sequence"] for leg in stored["legs"]] == [0, 1, 2]

    changed_legs = (replace(original.legs[0], quantity=Decimal("11")), *original.legs[1:])
    retry_id = "retry-intent"
    retry = replace(
        original,
        id=retry_id,
        legs=tuple(
            replace(leg, id=f"retry-{leg.sequence}", intent_id=retry_id)
            for leg in original.legs
        ),
    )
    assert repository.create_intent(retry) == (original.id, False)

    changed_legs = (replace(original.legs[0], quantity=Decimal("11")), *original.legs[1:])
    with pytest.raises(IdempotencyConflict):
        repository.create_intent(replace(original, legs=changed_legs))


def test_one_logical_leg_can_have_multiple_broker_order_attempts(tmp_path):
    repository = ready_repository(tmp_path)
    original = intent()
    repository.create_intent(original)
    leg_id = original.legs[0].id

    for attempt in (1, 2):
        repository.create_broker_order(
            broker_order_id=f"attempt-{attempt}",
            order_leg_id=leg_id,
            account_id="acct",
            broker="fake",
            attempt_number=attempt,
            client_order_id=f"client-{attempt}",
            submitted_quantity=Decimal("10"),
            replaces_broker_order_id="attempt-1" if attempt == 2 else None,
        )
    assert [row["attempt_number"] for row in repository.broker_orders_for_leg(leg_id)] == [1, 2]


def test_partial_fills_accumulate_and_duplicates_do_not_double_count(tmp_path):
    repository = ready_repository(tmp_path)
    original = intent(quantity=Decimal("10"))
    repository.create_intent(original)
    repository.transition_intent(original.id, IntentStatus.RISK_APPROVED)
    repository.transition_intent(original.id, IntentStatus.SUBMITTING)
    repository.transition_leg(original.legs[0].id, LegStatus.SUBMITTING)
    repository.create_broker_order(
        broker_order_id="attempt",
        order_leg_id=original.legs[0].id,
        account_id="acct",
        broker="fake",
        attempt_number=1,
        client_order_id="client",
        submitted_quantity=Decimal("10"),
    )
    repository.transition_broker_order("attempt", BrokerOrderStatus.SUBMITTING)
    repository.record_submission(
        "attempt", status=BrokerOrderStatus.WORKING, external_order_id="external"
    )
    repository.transition_leg(original.legs[0].id, LegStatus.WORKING)
    repository.transition_intent(original.id, IntentStatus.WORKING)

    first = Fill(
        id="fill-1",
        broker_order_id="attempt",
        order_leg_id=original.legs[0].id,
        external_fill_id="deal-1",
        dedupe_key="deal-1",
        quantity=Decimal("4"),
        price=Decimal("100"),
        fee=None,
        fee_currency=None,
        filled_at=NOW,
        received_at=NOW,
        metadata={},
    )
    assert repository.record_fill(first) is True
    assert repository.record_fill(replace(first, id="duplicate")) is False
    second = replace(
        first,
        id="fill-2",
        external_fill_id="deal-2",
        dedupe_key="deal-2",
        quantity=Decimal("6"),
        price=Decimal("110"),
    )
    assert repository.record_fill(second) is True

    stored = repository.get_intent(original.id)
    assert stored is not None
    assert stored["status"] == IntentStatus.FILLED.value
    assert stored["legs"][0]["cumulative_filled_quantity"] == "10"
    assert Decimal(stored["legs"][0]["average_fill_price"]) == Decimal("106")
    assert len(repository.fills_for_leg(original.legs[0].id)) == 2
    assert repository.position_allocations("acct")[0]["signed_quantity"] == "10"

    with pytest.raises(ValueError, match="different evidence"):
        repository.record_fill(replace(first, id="conflict", price=Decimal("999")))


def test_fill_cannot_cross_logical_legs(tmp_path):
    repository = ready_repository(tmp_path)
    original = intent(2)
    repository.create_intent(original)
    repository.create_broker_order(
        broker_order_id="attempt-leg-zero",
        order_leg_id=original.legs[0].id,
        account_id="acct",
        broker="fake",
        attempt_number=1,
        client_order_id="client-zero",
        submitted_quantity=Decimal("10"),
    )
    with pytest.raises(ValueError, match="does not belong"):
        repository.record_fill(
            Fill(
                id="cross-leg-fill",
                broker_order_id="attempt-leg-zero",
                order_leg_id=original.legs[1].id,
                dedupe_key="cross-leg",
                quantity=Decimal("1"),
                price=Decimal("100"),
                filled_at=NOW,
                received_at=NOW,
            )
        )


def test_invalid_transition_fails_without_changing_state(tmp_path):
    repository = ready_repository(tmp_path)
    original = intent()
    repository.create_intent(original)

    with pytest.raises(ValueError, match="Invalid intent transition"):
        repository.transition_intent(original.id, IntentStatus.FILLED)
    assert repository.get_intent(original.id)["status"] == IntentStatus.CREATED.value


def test_reconciliation_issue_is_sticky_and_requires_explicit_resolution(tmp_path):
    repository = ready_repository(tmp_path)
    run = ReconciliationRun(
        id="run",
        account_id="acct",
        broker_snapshot_id=None,
        started_at=NOW,
        completed_at=NOW,
        status=ReconciliationStatus.COMPLETED,
        metadata={},
    )
    repository.save_reconciliation_run(run)
    issue = ReconciliationIssue(
        id="issue",
        run_id="run",
        account_id="acct",
        issue_key="unknown:instrument-0",
        entity_type="INSTRUMENT",
        entity_key="instrument-0",
        category="UNKNOWN_POSITION_RESIDUAL",
        severity=IssueSeverity.ERROR,
        status=IssueStatus.OPEN,
        sticky=True,
        details={"signed_quantity": "2"},
        detected_at=NOW,
        resolved_at=None,
    )
    assert repository.upsert_reconciliation_issue(issue) == "issue"
    assert repository.upsert_reconciliation_issue(issue) == "issue"
    opened = repository.open_reconciliation_issues("acct")
    assert opened[0]["occurrence_count"] == 2

    repository.resolve_reconciliation_issue("issue", resolved_at=NOW)
    assert repository.open_reconciliation_issues("acct") == []
