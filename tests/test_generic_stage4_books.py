from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import sqlite3

from src.trading_core.domain import (
    Account,
    AssetClass,
    Book,
    BookAllocation,
    BrokerOrderStatus,
    Instrument,
    IntentAction,
    IntentStatus,
    LegStatus,
    OrderIntent,
    OrderLeg,
    OwnershipClass,
    PositionAllocation,
    QuantityUnit,
    RiskDecisionRecord,
    Side,
    Strategy,
    TradingEnvironment,
)
from src.trading_core.oms import GenericOMS
from src.trading_core.ports import BrokerFactSnapshot, BrokerSubmissionResult
from src.trading_core.repository import SQLiteTradingRepository


NOW = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)


def make_account(*, metadata: dict | None = None) -> Account:
    return Account(
        id="book-acct",
        broker="fake",
        environment=TradingEnvironment.SIM,
        external_account_id="book-external",
        base_currency="USD",
        metadata=metadata or {},
        created_at=NOW,
        updated_at=NOW,
    )


def make_strategy() -> Strategy:
    return Strategy(
        id="book-strategy",
        name="book test strategy",
        strategy_type="test",
        version="1",
        created_at=NOW,
        updated_at=NOW,
    )


def make_repository(tmp_path, *, account_metadata: dict | None = None) -> SQLiteTradingRepository:
    repository = SQLiteTradingRepository(tmp_path / "stage4.db")
    repository.initialize()
    repository.save_account(make_account(metadata=account_metadata))
    repository.save_strategy(make_strategy())
    repository.save_instrument(
        Instrument(
            id="book-instrument",
            asset_class=AssetClass.EQUITY,
            symbol="BOOK",
            venue="TEST",
            currency="USD",
            created_at=NOW,
            updated_at=NOW,
        )
    )
    return repository


def declare_book(repository: SQLiteTradingRepository, book_id: str, limit: str) -> None:
    repository.save_book(Book(id=book_id, name=f"Book {book_id}", created_at=NOW, updated_at=NOW))
    repository.save_book_allocation(
        BookAllocation(
            id=f"allocation-{book_id}",
            book_id=book_id,
            strategy_id="book-strategy",
            account_id="book-acct",
            effective_at=NOW,
            risk_budget=Decimal(limit),
            created_at=NOW,
            updated_at=NOW,
        )
    )


def make_intent(intent_id: str, key: str, book_id: str | None, quantity: str) -> OrderIntent:
    return OrderIntent(
        id=intent_id,
        idempotency_key=key,
        strategy_id="book-strategy",
        account_id="book-acct",
        book_id=book_id,
        action=IntentAction.ENTER,
        legs=(
            OrderLeg(
                id=f"{intent_id}-leg",
                intent_id=intent_id,
                sequence=0,
                instrument_id="book-instrument",
                side=Side.BUY,
                quantity=Decimal(quantity),
                quantity_unit=QuantityUnit.UNITS,
                order_type="MARKET",
                status=LegStatus.PLANNED,
                created_at=NOW,
                updated_at=NOW,
            ),
        ),
        status=IntentStatus.CREATED,
        created_at=NOW,
        updated_at=NOW,
    )


def decision(intent_id: str, number: int) -> RiskDecisionRecord:
    return RiskDecisionRecord(
        id=f"risk-{intent_id}-{number}",
        intent_id=intent_id,
        approved=True,
        reason="book test",
        checks={"offline": True},
        evaluated_at=NOW,
    )


class BookAdapter:
    def __init__(self) -> None:
        self.submit_calls: list[str] = []

    def get_authoritative_account_facts(self, account: Account) -> BrokerFactSnapshot:
        return BrokerFactSnapshot(
            account_id=account.id,
            captured_at=NOW,
            complete=True,
        )

    def submit_order(self, _account: Account, request) -> BrokerSubmissionResult:
        self.submit_calls.append(request.broker_order_id)
        return BrokerSubmissionResult(
            broker_order_id=request.broker_order_id,
            accepted=True,
            status=BrokerOrderStatus.WORKING,
            external_order_id=f"book-order-{len(self.submit_calls)}",
            client_order_id=request.client_order_id,
            submitted_at=NOW,
        )


def test_two_books_share_account_and_persist_attribution(tmp_path):
    repository = make_repository(tmp_path)
    declare_book(repository, "book-a", "10")
    declare_book(repository, "book-b", "10")
    adapter = BookAdapter()
    oms = GenericOMS(repository, adapter, clock=lambda: NOW)
    account = make_account()

    first = oms.submit_intent(
        make_intent("book-intent-a", "book-key-a", "book-a", "10"),
        account=account,
        risk_decision=decision("book-intent-a", 1),
    )
    second = oms.submit_intent(
        make_intent("book-intent-b", "book-key-b", "book-b", "10"),
        account=account,
        risk_decision=decision("book-intent-b", 2),
    )

    assert first["status"] == IntentStatus.WORKING.value
    assert second["status"] == IntentStatus.WORKING.value
    assert len(adapter.submit_calls) == 2
    assert {row["book_id"] for row in repository.book_intents("book-acct")} == {"book-a", "book-b"}
    assert {row["book_id"] for row in repository.book_broker_orders("book-acct")} == {"book-a", "book-b"}

    restarted = SQLiteTradingRepository(tmp_path / "stage4.db")
    restarted.initialize()
    assert restarted.get_book("book-a")["name"] == "Book book-a"
    assert {row["book_id"] for row in restarted.book_allocations("book-acct")} == {"book-a", "book-b"}


def test_book_cap_rejects_without_submitting_and_is_sticky(tmp_path):
    repository = make_repository(tmp_path)
    declare_book(repository, "book-a", "10")
    adapter = BookAdapter()
    result = GenericOMS(repository, adapter, clock=lambda: NOW).submit_intent(
        make_intent("book-cap-intent", "book-cap-key", "book-a", "11"),
        account=make_account(),
        risk_decision=decision("book-cap-intent", 1),
    )

    assert result["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert adapter.submit_calls == []
    assert any(
        item["category"] == "BOOK_OWNERSHIP_OR_CAPACITY"
        for item in repository.open_reconciliation_issues("book-acct")
    )
    assert any(
        item["action_key"] == "BOOK_RISK_BLOCK:book-acct:book-cap-intent"
        for item in repository.open_recovery_actions("book-acct")
    )


def test_rejected_no_submit_legs_do_not_reserve_book_capacity(tmp_path):
    repository = make_repository(tmp_path)
    declare_book(repository, "book-a", "10")
    rejected = make_intent("rejected-no-submit", "rejected-no-submit-key", "book-a", "10")
    repository.create_intent(rejected)
    repository.transition_intent(rejected.id, IntentStatus.REJECTED, now=NOW)

    assert repository.active_book_intent_exposure("book-acct", "book-a") == Decimal("0")
    assert repository.book_signed_exposure("book-acct", "book-a") == {}


def test_account_capacity_can_be_tighter_than_sum_of_book_limits(tmp_path):
    repository = make_repository(tmp_path, account_metadata={"account_capacity": "15"})
    declare_book(repository, "book-a", "100")
    declare_book(repository, "book-b", "100")
    adapter = BookAdapter()
    oms = GenericOMS(repository, adapter, clock=lambda: NOW)
    account = make_account(metadata={"account_capacity": "15"})

    accepted = oms.submit_intent(
        make_intent("aggregate-a", "aggregate-key-a", "book-a", "10"),
        account=account,
        risk_decision=decision("aggregate-a", 1),
    )
    rejected = oms.submit_intent(
        make_intent("aggregate-b", "aggregate-key-b", "book-b", "6"),
        account=account,
        risk_decision=decision("aggregate-b", 2),
    )

    assert accepted["status"] == IntentStatus.WORKING.value
    assert rejected["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert len(adapter.submit_calls) == 1
    assert any(
        item["category"] == "BOOK_OWNERSHIP_OR_CAPACITY"
        for item in repository.open_reconciliation_issues("book-acct")
    )


def test_unknown_allocation_blocks_new_book_risk_and_survives_restart(tmp_path):
    repository = make_repository(tmp_path)
    declare_book(repository, "book-a", "10")
    repository.save_position_allocation(
        PositionAllocation(
            id="unknown-position",
            account_id="book-acct",
            instrument_id="book-instrument",
            ownership_class=OwnershipClass.UNKNOWN,
            signed_quantity=Decimal("3"),
            metadata={"provenance": {"source": "offline-test"}},
            updated_at=NOW,
        ),
        _validation_token=repository._allocation_validation_capability(),
    )
    adapter = BookAdapter()
    result = GenericOMS(repository, adapter, clock=lambda: NOW).submit_intent(
        make_intent("unknown-intent", "unknown-key", "book-a", "1"),
        account=make_account(),
        risk_decision=decision("unknown-intent", 1),
    )

    assert result["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert adapter.submit_calls == []

    restarted = SQLiteTradingRepository(tmp_path / "stage4.db")
    restarted.initialize()
    assert restarted.position_allocations("book-acct")[0]["ownership_class"] == OwnershipClass.UNKNOWN.value
    assert restarted.open_recovery_actions("book-acct")
    assert restarted.open_reconciliation_issues("book-acct")


def test_unknown_requested_book_is_durable_unknown_and_never_submits(tmp_path):
    repository = make_repository(tmp_path)
    declare_book(repository, "book-a", "10")
    adapter = BookAdapter()
    result = GenericOMS(repository, adapter, clock=lambda: NOW).submit_intent(
        make_intent("unknown-book-intent", "unknown-book-key", "not-declared", "1"),
        account=make_account(),
        risk_decision=decision("unknown-book-intent", 1),
    )

    assert result["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert adapter.submit_calls == []
    stored = repository.get_intent("unknown-book-intent")
    assert stored["book_id"] is None
    assert stored["metadata"][GenericOMS._UNKNOWN_BOOK_REQUEST_KEY] == "not-declared"


def test_mixed_book_capacity_bases_are_account_configuration_blockers(tmp_path):
    repository = make_repository(tmp_path)
    declare_book(repository, "book-a", "10")
    repository.save_book(Book(id="book-b", name="Book book-b", created_at=NOW, updated_at=NOW))
    repository.save_book_allocation(
        BookAllocation(
            id="allocation-book-b",
            book_id="book-b",
            strategy_id="book-strategy",
            account_id="book-acct",
            effective_at=NOW,
            capital_amount=Decimal("100"),
            created_at=NOW,
            updated_at=NOW,
        )
    )
    adapter = BookAdapter()
    result = GenericOMS(repository, adapter, clock=lambda: NOW).submit_intent(
        make_intent("mixed-basis-intent", "mixed-basis-key", "book-a", "1"),
        account=make_account(),
        risk_decision=decision("mixed-basis-intent", 1),
    )

    assert result["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert adapter.submit_calls == []
    issue = next(
        item
        for item in repository.open_reconciliation_issues("book-acct")
        if item["category"] == "BOOK_OWNERSHIP_OR_CAPACITY"
    )
    assert "book_capacity_configuration" in issue["details_json"]


def test_stage4_book_columns_migrate_idempotently_on_legacy_schema(tmp_path):
    path = tmp_path / "legacy-stage4.db"
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE core_order_intents (
                id TEXT PRIMARY KEY,
                idempotency_key TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                strategy_id TEXT NOT NULL,
                account_id TEXT NOT NULL,
                action TEXT NOT NULL,
                status TEXT NOT NULL,
                source_signal_id TEXT,
                execution_policy_json TEXT NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE (account_id, idempotency_key)
            );
            CREATE TABLE core_position_allocations (
                id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL,
                instrument_id TEXT NOT NULL,
                strategy_id TEXT,
                ownership_class TEXT NOT NULL,
                signed_quantity TEXT NOT NULL,
                source_intent_id TEXT,
                updated_at TEXT NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}'
            );
            """
        )

    repository = SQLiteTradingRepository(path)
    repository.initialize()
    repository.initialize()
    with sqlite3.connect(path) as conn:
        intent_columns = {row[1] for row in conn.execute("PRAGMA table_info(core_order_intents)")}
        allocation_columns = {row[1] for row in conn.execute("PRAGMA table_info(core_position_allocations)")}
        intent_indexes = {row[1] for row in conn.execute("PRAGMA index_list(core_order_intents)")}
        migration = conn.execute(
            "SELECT version FROM core_schema_migrations WHERE version = 4"
        ).fetchone()

    assert "book_id" in intent_columns
    assert "book_id" in allocation_columns
    assert "idx_core_intents_book" in intent_indexes
    assert migration is not None
