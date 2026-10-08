from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from src.trading_core.domain import (
    Account,
    AssetClass,
    Book,
    BrokerOrderSnapshot,
    BrokerOrderStatus,
    ExecutionEvidenceMode,
    ExecutionPolicy,
    Fill,
    Instrument,
    IntentAction,
    IntentStatus,
    LegStatus,
    OrderIntent,
    OrderLeg,
    PositionSnapshot,
    Side,
    Strategy,
    TradingEnvironment,
)
from src.trading_core.oms import GenericOMS, OMSExecutionError
from src.trading_core.ports import BrokerFactSnapshot, BrokerFill, BrokerHistoricalOrderFacts
from src.trading_core.repository import SQLiteTradingRepository


NOW = datetime(2026, 10, 3, 5, 20, tzinfo=timezone.utc)
ACCOUNT_ID = "aggregate:sim:1"
BOOK_ID = "aggregate-book"
INSTRUMENTS = ("aggregate-a", "aggregate-b")


def _account() -> Account:
    return Account(
        id=ACCOUNT_ID,
        broker="fake",
        environment=TradingEnvironment.SIM,
        external_account_id="1",
        base_currency="USD",
        created_at=NOW,
        updated_at=NOW,
    )


def _repository(tmp_path) -> SQLiteTradingRepository:
    repository = SQLiteTradingRepository(tmp_path / "aggregate.db")
    repository.initialize()
    repository.save_account(_account())
    repository.save_strategy(
        Strategy(
            id="aggregate-strategy",
            name="aggregate test",
            strategy_type="test",
            created_at=NOW,
            updated_at=NOW,
        )
    )
    repository.save_book(Book(id=BOOK_ID, name="aggregate book", created_at=NOW, updated_at=NOW))
    for index, instrument_id in enumerate(INSTRUMENTS):
        repository.save_instrument(
            Instrument(
                id=instrument_id,
                asset_class=AssetClass.EQUITY,
                symbol=f"AGG{index}",
                venue="US",
                currency="USD",
                created_at=NOW,
                updated_at=NOW,
            )
        )
    return repository


class _AggregateAdapter:
    def __init__(self, facts: BrokerFactSnapshot, orders, fills) -> None:
        self.facts = facts
        self.orders = tuple(orders)
        self.fills = tuple(fills)

    def get_authoritative_account_facts(self, _account: Account) -> BrokerFactSnapshot:
        return self.facts

    def get_historical_order_facts(self, account: Account, start, end) -> BrokerHistoricalOrderFacts:
        return BrokerHistoricalOrderFacts(
            account_id=account.id,
            requested_start=start,
            requested_end=end,
            captured_at=end,
            complete=True,
            orders=self.orders,
            fills=self.fills,
            execution_evidence_mode=ExecutionEvidenceMode.INDIVIDUAL_DEALS,
            execution_evidence_scope=frozenset({"HISTORICAL_ORDER_SNAPSHOTS"}),
        )


class _FailingRecoveryAdapter:
    def get_authoritative_account_facts(self, _account: Account):
        raise AssertionError("proof-closed aggregate must not be polled")

    def get_historical_order_facts(self, _account: Account, _start, _end):
        raise AssertionError("proof-closed aggregate must not query history")


def _seed(repository: SQLiteTradingRepository):
    specs = (
        ("aggregate-entry", IntentAction.ENTER, (Side.BUY, Side.SELL), (Decimal("1"), Decimal("1"))),
        ("aggregate-wrong-exit", IntentAction.EXIT, (Side.BUY, Side.SELL), (Decimal("1"), Decimal("1"))),
        ("aggregate-corrective-exit", IntentAction.EXIT, (Side.SELL, Side.BUY), (Decimal("2"), Decimal("2"))),
    )
    rows = []
    orders = []
    fills = []
    for intent_index, (intent_id, action, sides, quantities) in enumerate(specs):
        legs = tuple(
            OrderLeg(
                id=f"{intent_id}-leg-{leg_index}",
                intent_id=intent_id,
                sequence=leg_index,
                instrument_id=INSTRUMENTS[leg_index],
                side=side,
                quantity=quantity,
                created_at=NOW,
                updated_at=NOW,
            )
            for leg_index, (side, quantity) in enumerate(zip(sides, quantities))
        )
        intent = OrderIntent(
            id=intent_id,
            idempotency_key=f"{intent_id}-key",
            strategy_id="aggregate-strategy",
            account_id=ACCOUNT_ID,
            book_id=BOOK_ID,
            action=action,
            execution_policy=ExecutionPolicy(),
            legs=legs,
            created_at=NOW,
            updated_at=NOW,
        )
        repository.create_intent(intent)
        repository.transition_intent(intent_id, IntentStatus.RISK_APPROVED, now=NOW)
        repository.transition_intent(intent_id, IntentStatus.SUBMITTING, now=NOW)
        for leg_index, leg in enumerate(legs):
            attempt_id = f"{intent_id}-attempt-{leg_index}"
            external_id = f"external-{intent_index}-{leg_index}"
            repository.transition_leg(leg.id, LegStatus.SUBMITTING, now=NOW)
            repository.create_broker_order(
                broker_order_id=attempt_id,
                order_leg_id=leg.id,
                account_id=ACCOUNT_ID,
                broker="fake",
                attempt_number=1,
                client_order_id=f"{leg.id}:1",
                submitted_quantity=leg.quantity,
                now=NOW,
            )
            repository.transition_broker_order(attempt_id, BrokerOrderStatus.SUBMITTING, now=NOW)
            repository.record_submission(
                attempt_id,
                status=BrokerOrderStatus.FILLED,
                external_order_id=external_id,
                metadata={"provider_status": BrokerOrderStatus.FILLED.value},
                now=NOW,
            )
            price = Decimal("100") + Decimal(str(intent_index * 10 + leg_index))
            repository.record_fill(
                Fill(
                    id=f"{attempt_id}-fill",
                    broker_order_id=attempt_id,
                    order_leg_id=leg.id,
                    dedupe_key=f"dedupe:{external_id}",
                    quantity=leg.quantity,
                    price=price,
                    filled_at=NOW,
                    received_at=NOW,
                    account_id=ACCOUNT_ID,
                    external_order_id=external_id,
                    evidence_reference=f"{external_id}:dedupe:{external_id}",
                    metadata={
                        "source": "aggregate-test",
                        "_broker_fill_account_id": ACCOUNT_ID,
                        "_external_order_id": external_id,
                        "_instrument_id": leg.instrument_id,
                        "_evidence_reference": f"{external_id}:dedupe:{external_id}",
                    },
                ),
                now=NOW,
                _validation_token=repository._fill_validation_capability(),
            )
            rows.append((intent, leg, attempt_id, external_id, price))
            orders.append(
                BrokerOrderSnapshot(
                    id=f"history:{external_id}",
                    broker_snapshot_id="history-snapshot",
                    account_id=ACCOUNT_ID,
                    instrument_id=leg.instrument_id,
                    external_order_id=external_id,
                    side=leg.side,
                    quantity=leg.quantity,
                    filled_quantity=leg.quantity,
                    status=BrokerOrderStatus.FILLED,
                    captured_at=NOW,
                    order_time=NOW,
                )
            )
            fills.append(
                BrokerFill(
                    external_order_id=external_id,
                    dedupe_key=f"broker:{external_id}",
                    quantity=leg.quantity,
                    price=price,
                    filled_at=NOW,
                    received_at=NOW,
                    account_id=ACCOUNT_ID,
                    evidence_reference=f"broker-evidence:{external_id}",
                    evidence_mode=ExecutionEvidenceMode.INDIVIDUAL_DEALS,
                    instrument_id=leg.instrument_id,
                )
            )
    facts = BrokerFactSnapshot(
        account_id=ACCOUNT_ID,
        captured_at=NOW + timedelta(seconds=1),
        complete=True,
        fills=tuple(fills),
        execution_evidence_mode=ExecutionEvidenceMode.INDIVIDUAL_DEALS,
        execution_evidence_scope=frozenset({"CURRENT_DEALS"}),
    )
    return rows, _AggregateAdapter(facts, orders, fills)


def _intent_ids(rows):
    return tuple(dict.fromkeys(row[0].id for row in rows))


def test_aggregate_roundtrip_closes_exact_incident_and_preserves_all_proof(tmp_path):
    repository = _repository(tmp_path)
    rows, adapter = _seed(repository)
    oms = GenericOMS(repository, adapter, clock=lambda: NOW + timedelta(seconds=2))

    result = oms.resolve_verified_aggregate_roundtrip(intent_ids=_intent_ids(rows), account=_account())

    assert result["status"] == IntentStatus.COMPLETED.value
    for intent_id in _intent_ids(rows):
        persisted = repository.get_intent(intent_id)
        assert persisted["status"] == IntentStatus.COMPLETED.value
        closure = persisted["metadata"]["verified_roundtrip_closure"]
        assert closure["intent_ids"] == list(_intent_ids(rows))
        assert len(closure["fill_proof"]) == 6
    proof = result["proof"]
    assert proof["signed_quantity_totals"] == {INSTRUMENTS[0]: "0", INSTRUMENTS[1]: "0"}
    assert proof["incident"]["wrong_direction_intent_ids"] == ["aggregate-wrong-exit"]
    assert repository.operational_events(ACCOUNT_ID)[0]["event_type"] == "VERIFIED_AGGREGATE_ROUNDTRIP_CLOSED"
    assert repository.fills_for_leg("aggregate-wrong-exit-leg-0")[0]["quantity"] == "1"


def test_aggregate_closure_is_lifecycle_frozen_for_direct_and_scheduler_recovery(tmp_path):
    repository = _repository(tmp_path)
    rows, adapter = _seed(repository)
    intent_ids = _intent_ids(rows)
    proof_oms = GenericOMS(repository, adapter, clock=lambda: NOW + timedelta(seconds=2))
    proof_oms.resolve_verified_aggregate_roundtrip(intent_ids=intent_ids, account=_account())

    with repository.transaction() as conn:
        conn.execute(
            "UPDATE core_order_intents SET status = ?, updated_at = ? WHERE id IN (?, ?)",
            (
                IntentStatus.RECONCILIATION_REQUIRED.value,
                (NOW + timedelta(seconds=3)).isoformat(),
                intent_ids[0],
                intent_ids[1],
            ),
        )

    recovered_oms = GenericOMS(
        repository,
        _FailingRecoveryAdapter(),
        clock=lambda: NOW + timedelta(seconds=4),
    )
    direct = recovered_oms.recover_intent(intent_ids[0], account=_account())
    scheduled = recovered_oms.recover_pending_intents(account=_account())

    assert direct["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert {item["id"] for item in scheduled} == set(intent_ids)
    assert repository.open_reconciliation_issues(ACCOUNT_ID) == []
    assert repository.open_recovery_actions(ACCOUNT_ID) == []


@pytest.mark.parametrize("mutation", ("partial", "qty", "account", "active"))
def test_aggregate_roundtrip_rejects_incomplete_or_unsafe_evidence(tmp_path, mutation):
    repository = _repository(tmp_path)
    rows, adapter = _seed(repository)
    if mutation == "partial":
        adapter.orders = adapter.orders[:-1]
    elif mutation == "qty":
        adapter.fills = tuple(
            replace(item, quantity=Decimal("1.5")) if item.external_order_id == "external-2-0" else item
            for item in adapter.fills
        )
    elif mutation == "account":
        adapter.facts = replace(
            adapter.facts,
            fills=tuple(
                replace(item, account_id="foreign:1") if item.external_order_id == "external-0-0" else item
                for item in adapter.facts.fills
            ),
        )
    else:
        adapter.facts = replace(
            adapter.facts,
            positions=(
                PositionSnapshot(
                    id="unsafe-position",
                    broker_snapshot_id="unsafe-snapshot",
                    account_id=ACCOUNT_ID,
                    instrument_id=INSTRUMENTS[0],
                    signed_quantity=Decimal("1"),
                    captured_at=NOW,
                ),
            ),
        )
    oms = GenericOMS(repository, adapter, clock=lambda: NOW + timedelta(seconds=2))

    with pytest.raises(OMSExecutionError):
        oms.resolve_verified_aggregate_roundtrip(intent_ids=_intent_ids(rows), account=_account())
    assert all(repository.get_intent(intent_id)["status"] == IntentStatus.FILLED.value for intent_id in _intent_ids(rows))
    assert repository.operational_events(ACCOUNT_ID) == []


def test_aggregate_roundtrip_rejects_counter_fill_reuse_and_foreign_book(tmp_path):
    repository = _repository(tmp_path)
    rows, adapter = _seed(repository)
    first_id = _intent_ids(rows)[0]
    with repository.transaction() as conn:
        conn.execute(
            "UPDATE core_order_intents SET metadata_json = ? WHERE id = ?",
            (
                '{"verified_roundtrip_closure":{"account_id":"aggregate:sim:1","intent_ids":["other-closure"],"external_order_ids":["external-0-0"]}}',
                first_id,
            ),
        )
    oms = GenericOMS(repository, adapter, clock=lambda: NOW + timedelta(seconds=2))
    with pytest.raises(OMSExecutionError, match="different durable closure"):
        oms.resolve_verified_aggregate_roundtrip(intent_ids=_intent_ids(rows), account=_account())

    clean = _repository(tmp_path / "book-mismatch")
    rows, adapter = _seed(clean)
    clean.save_book(Book(id="foreign-book", name="foreign book", created_at=NOW, updated_at=NOW))
    with clean.transaction() as conn:
        conn.execute("UPDATE core_order_intents SET book_id = 'foreign-book' WHERE id = ?", (rows[1][0].id,))
    with pytest.raises(OMSExecutionError, match="share one declared book"):
        GenericOMS(clean, adapter, clock=lambda: NOW + timedelta(seconds=2)).resolve_verified_aggregate_roundtrip(
            intent_ids=_intent_ids(rows), account=_account()
        )


@pytest.mark.parametrize("mutation", ("extra", "duplicate", "stale-history"))
def test_aggregate_roundtrip_rejects_unknown_duplicate_or_stale_shared_evidence(tmp_path, mutation):
    repository = _repository(tmp_path)
    rows, adapter = _seed(repository)
    if mutation == "extra":
        extra = replace(
            adapter.facts.fills[0],
            external_order_id="unexpected-external",
            dedupe_key="broker:unexpected-external",
        )
        adapter.facts = replace(adapter.facts, fills=(*adapter.facts.fills, extra))
    elif mutation == "duplicate":
        duplicate = replace(adapter.facts.fills[0], quantity=Decimal("1.5"))
        adapter.facts = replace(adapter.facts, fills=(*adapter.facts.fills, duplicate))
    else:
        original_history = adapter.get_historical_order_facts

        def stale_history(account, start, end):
            return replace(
                original_history(account, start, end),
                captured_at=NOW - timedelta(days=2),
            )

        adapter.get_historical_order_facts = stale_history

    with pytest.raises(OMSExecutionError):
        GenericOMS(repository, adapter, clock=lambda: NOW + timedelta(seconds=2)).resolve_verified_aggregate_roundtrip(
            intent_ids=_intent_ids(rows), account=_account()
        )
    assert all(
        repository.get_intent(intent_id)["status"] == IntentStatus.FILLED.value
        for intent_id in _intent_ids(rows)
    )


def test_verified_roundtrip_does_not_clear_unrelated_position_query_blocker(tmp_path):
    repository = _repository(tmp_path)
    rows, adapter = _seed(repository)
    source = rows[0][0]
    unrelated_id = "unrelated-position-query"
    unrelated_legs = tuple(
        replace(leg, id=f"{unrelated_id}-leg-{leg.sequence}", intent_id=unrelated_id)
        for leg in source.legs
    )
    unrelated = replace(
        source,
        id=unrelated_id,
        idempotency_key=f"{unrelated_id}-key",
        legs=unrelated_legs,
    )
    repository.create_intent(unrelated)
    # Reuse the seeded graph as a simple two-intent round-trip by retaining
    # only the entry and one exact opposite-side exit.  The unrelated blocker
    # below must not be cleared by the proof route.
    with repository.transaction() as connection:
        connection.execute(
            "UPDATE core_order_legs SET side = CASE WHEN side = 'BUY' THEN 'SELL' ELSE 'BUY' END "
            "WHERE intent_id = ?",
            ("aggregate-wrong-exit",),
        )
    expected_external_ids = {"external-0-0", "external-0-1", "external-1-0", "external-1-1"}
    adapter.orders = tuple(
        replace(
            order,
            side=(Side.SELL if order.side is Side.BUY else Side.BUY)
            if str(order.external_order_id).startswith("external-1-")
            else order.side,
        )
        for order in adapter.orders
        if order.external_order_id in expected_external_ids
    )
    adapter.fills = tuple(item for item in adapter.fills if item.external_order_id in expected_external_ids)
    adapter.facts = replace(adapter.facts, fills=adapter.fills)
    oms = GenericOMS(repository, adapter, clock=lambda: NOW + timedelta(seconds=2))
    oms._require_reconciliation(
        unrelated_id,
        _account(),
        category="BROKER_QUERY_FAILED",
        entity_type="ACCOUNT",
        entity_key=f"{ACCOUNT_ID}:positions",
        details={"message": "unrelated position query failed"},
    )

    with pytest.raises(OMSExecutionError, match="blockers remain open"):
        oms.resolve_verified_roundtrip(
            entry_intent_id="aggregate-entry",
            exit_intent_id="aggregate-wrong-exit",
            account=_account(),
        )
    remaining = repository.open_reconciliation_issues(ACCOUNT_ID)
    assert any(issue["issue_key"] == f"BROKER_QUERY_FAILED:ACCOUNT:{ACCOUNT_ID}:positions" for issue in remaining)
