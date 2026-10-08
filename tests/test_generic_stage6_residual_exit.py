from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from src.strategies.stat_arb.stage6_pilot import Stage6RunMode
from src.trading_core.domain import (
    Book,
    BrokerOrderStatus,
    ExecutionPolicy,
    Fill,
    IntentAction,
    IntentStatus,
    LegStatus,
    OrderIntent,
    OrderLeg,
    PositionSnapshot,
    Side,
)
from tests.test_generic_stage6_pilot import (
    NOW,
    make_account,
    make_runner,
    make_sleeves,
    make_spec,
)


def _seed_partial_source(
    repository,
    account,
    *,
    action: IntentAction,
    source_id: str,
    instrument_id: str,
    book_id: str = "pilot-book-a",
    quantity: str = "2",
    filled_quantity: str = "1",
    side: Side = Side.BUY,
):
    leg_id = f"{source_id}-leg"
    intent = OrderIntent(
        id=source_id,
        idempotency_key=f"{source_id}-key",
        strategy_id="pilot-strategy",
        account_id=account.id,
        book_id=book_id,
        action=action,
        execution_policy=ExecutionPolicy(),
        legs=(
            OrderLeg(
                id=leg_id,
                intent_id=source_id,
                sequence=0,
                instrument_id=instrument_id,
                side=side,
                quantity=Decimal(quantity),
                created_at=NOW,
                updated_at=NOW,
            ),
        ),
        created_at=NOW,
        updated_at=NOW,
    )
    repository.create_intent(intent)
    repository.transition_intent(source_id, IntentStatus.RISK_APPROVED, now=NOW)
    repository.transition_intent(source_id, IntentStatus.SUBMITTING, now=NOW)
    repository.transition_leg(leg_id, LegStatus.SUBMITTING, now=NOW)
    repository.create_broker_order(
        broker_order_id=f"{source_id}-order",
        order_leg_id=leg_id,
        account_id=account.id,
        broker=account.broker,
        attempt_number=1,
        client_order_id=f"{leg_id}:1",
        submitted_quantity=Decimal(quantity),
        now=NOW,
    )
    repository.transition_broker_order(
        f"{source_id}-order",
        BrokerOrderStatus.SUBMITTING,
        now=NOW,
    )
    repository.record_submission(
        f"{source_id}-order",
        status=BrokerOrderStatus.PARTIALLY_FILLED,
        external_order_id=f"{source_id}-external",
        now=NOW,
    )
    repository.record_fill(
        Fill(
            id=f"{source_id}-fill",
            broker_order_id=f"{source_id}-order",
            order_leg_id=leg_id,
            dedupe_key=f"{source_id}-deal",
            quantity=Decimal(filled_quantity),
            price=Decimal("100"),
            filled_at=NOW,
            received_at=NOW,
            account_id=account.id,
            external_order_id=f"{source_id}-external",
            evidence_reference=f"{source_id}-deal",
        ),
        now=NOW,
        _validation_token=repository._fill_validation_capability(),
    )
    return intent, leg_id, f"{source_id}-external"


def _set_partial_facts(adapter, account, instrument_id: str, signed_quantity: str, external_order_id: str):
    adapter.facts = replace(
        adapter.facts,
        positions=(
            PositionSnapshot(
                id=f"fresh-position-{external_order_id}",
                broker_snapshot_id=f"fresh-snapshot-{external_order_id}",
                account_id=account.id,
                instrument_id=instrument_id,
                signed_quantity=Decimal(signed_quantity),
                average_price=Decimal("100"),
                captured_at=NOW,
            ),
        ),
        fills=(
            # The fake fact is intentionally provider-shaped and is matched
            # against the immutable durable source fill by external order ID.
            adapter.facts.fills[0]
            if adapter.facts.fills
            else None,
        ),
    )


def test_partial_enter_residual_exit_is_exact_and_idempotent(tmp_path):
    account, sleeves, repository, adapter, runner = make_runner(tmp_path)
    _intent, _leg_id, external_id = _seed_partial_source(
        repository,
        account,
        action=IntentAction.ENTER,
        source_id="partial-enter",
        instrument_id=sleeves[0].instrument_ids[0],
    )
    from src.trading_core.ports import BrokerFill

    adapter.facts = replace(
        adapter.facts,
        positions=(
            PositionSnapshot(
                id="fresh-partial-enter-position",
                broker_snapshot_id="fresh-partial-enter-snapshot",
                account_id=account.id,
                instrument_id=sleeves[0].instrument_ids[0],
                signed_quantity=Decimal("1"),
                average_price=Decimal("100"),
                captured_at=NOW,
            ),
        ),
        fills=(
            BrokerFill(
                external_order_id=external_id,
                dedupe_key="partial-enter-deal",
                quantity=Decimal("1"),
                price=Decimal("100"),
                filled_at=NOW,
                received_at=NOW,
                account_id=account.id,
                instrument_id=sleeves[0].instrument_ids[0],
            ),
        ),
    )
    result = runner.submit_verified_residual_exit(
        account=account,
        source_intent_id="partial-enter",
        expected_external_order_ids={sleeves[0].instrument_ids[0]: external_id},
        spec=make_spec(account, sleeves),
    )
    assert result["residual_exit_intent_id"]
    assert result["broker_contacted"] is True
    assert result["broker_submission_count"] == 1
    assert adapter.submit_calls == [result["broker_order_attempt_ids"][0]]
    residual_snapshot = repository.get_intent(result["residual_exit_intent_id"])
    assert residual_snapshot is not None
    assert residual_snapshot["metadata"]["stage6_submission"]["mode"] == "RESIDUAL_EXIT"
    assert residual_snapshot["metadata"]["stage6_submission"]["correlation"]["source_intent_id"] == "partial-enter"
    replay = runner.submit_verified_residual_exit(
        account=account,
        source_intent_id="partial-enter",
        expected_external_order_ids={sleeves[0].instrument_ids[0]: external_id},
        spec=make_spec(account, sleeves),
    )
    assert replay["residual_exit_intent_id"] == result["residual_exit_intent_id"]
    assert len(adapter.submit_calls) == 1


def test_partial_exit_residual_uses_remaining_book_basis(tmp_path):
    account, sleeves, repository, adapter, runner = make_runner(tmp_path)
    # A durable managed +2 basis exists before the partial SELL 1 source.
    _seed_partial_source(
        repository,
        account,
        action=IntentAction.ENTER,
        source_id="prior-entry",
        instrument_id=sleeves[0].instrument_ids[0],
        quantity="2",
        filled_quantity="2",
    )
    repository.transition_intent("prior-entry", IntentStatus.COMPLETED, now=NOW)
    _intent, _leg_id, external_id = _seed_partial_source(
        repository,
        account,
        action=IntentAction.EXIT,
        source_id="partial-exit",
        instrument_id=sleeves[0].instrument_ids[0],
        quantity="2",
        filled_quantity="1",
        side=Side.SELL,
    )
    from src.trading_core.ports import BrokerFill

    adapter.facts = replace(
        adapter.facts,
        positions=(
            PositionSnapshot(
                id="fresh-partial-exit-position",
                broker_snapshot_id="fresh-partial-exit-snapshot",
                account_id=account.id,
                instrument_id=sleeves[0].instrument_ids[0],
                signed_quantity=Decimal("1"),
                average_price=Decimal("100"),
                captured_at=NOW,
            ),
        ),
        fills=(
            BrokerFill(
                external_order_id="prior-entry-external",
                dedupe_key="prior-entry-deal",
                quantity=Decimal("2"),
                price=Decimal("100"),
                filled_at=NOW,
                received_at=NOW,
                account_id=account.id,
                instrument_id=sleeves[0].instrument_ids[0],
            ),
            BrokerFill(
                external_order_id=external_id,
                dedupe_key="partial-exit-deal",
                quantity=Decimal("1"),
                price=Decimal("100"),
                filled_at=NOW,
                received_at=NOW,
                account_id=account.id,
                instrument_id=sleeves[0].instrument_ids[0],
            ),
        ),
    )
    result = runner.submit_verified_residual_exit(
        account=account,
        source_intent_id="partial-exit",
        expected_external_order_ids={sleeves[0].instrument_ids[0]: external_id},
        spec=make_spec(account, sleeves),
    )
    residual = repository.get_intent(result["residual_exit_intent_id"])
    assert residual is not None
    assert residual["legs"][0]["side"] == Side.SELL.value
    assert Decimal(str(residual["legs"][0]["quantity"])) == Decimal("1")


def test_partial_residual_rth_closure_blocks_before_any_order(tmp_path):
    account, sleeves, repository, adapter, runner = make_runner(tmp_path)
    _intent, _leg_id, external_id = _seed_partial_source(
        repository,
        account,
        action=IntentAction.ENTER,
        source_id="partial-rth",
        instrument_id=sleeves[0].instrument_ids[0],
    )
    from src.trading_core.ports import BrokerFill

    adapter.facts = replace(
        adapter.facts,
        positions=(
            PositionSnapshot(
                id="fresh-partial-rth-position",
                broker_snapshot_id="fresh-partial-rth-snapshot",
                account_id=account.id,
                instrument_id=sleeves[0].instrument_ids[0],
                signed_quantity=Decimal("1"),
                average_price=Decimal("100"),
                captured_at=NOW,
            ),
        ),
        fills=(
            BrokerFill(
                external_order_id=external_id,
                dedupe_key="partial-rth-deal",
                quantity=Decimal("1"),
                price=Decimal("100"),
                filled_at=NOW,
                received_at=NOW,
                account_id=account.id,
                instrument_id=sleeves[0].instrument_ids[0],
            ),
        ),
    )
    adapter.get_authoritative_market_state = lambda symbols: {
        "market": "US",
        "captured_at": NOW.isoformat(),
        "complete": True,
        "rows": [{"symbol": str(symbol), "market_state": "CLOSED"} for symbol in symbols],
    }
    result = runner.submit_verified_residual_exit(
        account=account,
        source_intent_id="partial-rth",
        expected_external_order_ids={sleeves[0].instrument_ids[0]: external_id},
        spec=make_spec(account, sleeves),
    )
    assert result["residual_result"]["status"] == IntentStatus.REJECTED.value
    assert adapter.submit_calls == []


def test_residual_scope_rejects_foreign_source_before_submission(tmp_path):
    account, sleeves, repository, adapter, runner = make_runner(tmp_path)
    _intent, _leg_id, external_id = _seed_partial_source(
        repository,
        account,
        action=IntentAction.ENTER,
        source_id="foreign-residual",
        instrument_id=sleeves[0].instrument_ids[0],
    )
    repository.save_book(Book(id="foreign-book", name="Foreign", created_at=NOW, updated_at=NOW))
    with repository.transaction() as conn:
        conn.execute(
            "UPDATE core_order_intents SET book_id = ? WHERE id = ?",
            ("foreign-book", "foreign-residual"),
        )
    with pytest.raises(ValueError, match="book"):
        runner.submit_verified_residual_exit(
            account=account,
            source_intent_id="foreign-residual",
            expected_external_order_ids={sleeves[0].instrument_ids[0]: external_id},
            spec=make_spec(account, sleeves),
        )
    assert adapter.submit_calls == []


def test_compensating_exit_scope_rejects_foreign_same_account_book(tmp_path):
    account, sleeves, repository, adapter, runner = make_runner(tmp_path)
    _intent, _leg_id, external_id = _seed_partial_source(
        repository,
        account,
        action=IntentAction.ENTER,
        source_id="foreign-compensating-source",
        instrument_id=sleeves[0].instrument_ids[0],
    )
    repository.save_book(Book(id="foreign-compensating-book", name="Foreign", created_at=NOW, updated_at=NOW))
    with repository.transaction() as conn:
        conn.execute(
            "UPDATE core_order_intents SET book_id = ? WHERE id = ?",
            ("foreign-compensating-book", "foreign-compensating-source"),
        )
    with pytest.raises(ValueError, match="book"):
        runner.submit_verified_compensating_exit(
            account=account,
            source_intent_id="foreign-compensating-source",
            expected_external_order_ids={sleeves[0].instrument_ids[0]: external_id},
            spec=make_spec(account, sleeves),
        )
    assert adapter.submit_calls == []
