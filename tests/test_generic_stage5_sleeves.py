from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from src.strategies.stat_arb.stage5_sleeves import (
    Clean40AllocationUpdate,
    NormalizedPairTarget,
    PairSleeve,
    SleeveAllocationTarget,
    Stage5AllocationCoordinator,
)
from src.trading_core.domain import (
    Account,
    AssetClass,
    Book,
    BrokerOrderStatus,
    Instrument,
    IntentAction,
    IntentStatus,
    LegStatus,
    PositionAllocation,
    RiskDecisionRecord,
    Side,
    Strategy,
    TradingEnvironment,
    OwnershipClass,
)
from src.trading_core.oms import GenericOMS
from src.trading_core.ports import BrokerFactSnapshot, BrokerSubmissionResult
from src.trading_core.repository import SQLiteTradingRepository


NOW = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)


def make_account() -> Account:
    return Account(
        id="stage5-account",
        broker="fake",
        environment=TradingEnvironment.SIM,
        external_account_id="stage5-external",
        base_currency="USD",
        metadata={"allocation_capacity": "20", "account_capacity": "20"},
        created_at=NOW,
        updated_at=NOW,
    )


def make_sleeves() -> tuple[PairSleeve, PairSleeve]:
    return (
        PairSleeve(
            sleeve_id="sleeve-a",
            strategy_id="stage5-strategy",
            account_id="stage5-account",
            book_id="book-a",
            name="Sleeve A",
            instrument_ids=("a-1", "a-2"),
            symbols=("A1", "A2"),
            configuration={"lookback": 40, "signal_source": "normalized"},
        ),
        PairSleeve(
            sleeve_id="sleeve-b",
            strategy_id="stage5-strategy",
            account_id="stage5-account",
            book_id="book-b",
            name="Sleeve B",
            instrument_ids=("b-1", "b-2"),
            symbols=("B1", "B2"),
            configuration={"lookback": 40, "signal_source": "normalized"},
        ),
    )


def make_repository(tmp_path) -> SQLiteTradingRepository:
    repository = SQLiteTradingRepository(tmp_path / "stage5.db")
    repository.initialize()
    repository.save_account(make_account())
    repository.save_strategy(
        Strategy(
            id="stage5-strategy",
            name="Stage 5 strategy",
            strategy_type="generic_stat_arb",
            version="1",
            created_at=NOW,
            updated_at=NOW,
        )
    )
    for book_id in ("book-a", "book-b"):
        repository.save_book(Book(id=book_id, name=book_id, created_at=NOW, updated_at=NOW))
    for number in ("a-1", "a-2", "b-1", "b-2"):
        repository.save_instrument(
            Instrument(
                id=number,
                asset_class=AssetClass.EQUITY,
                symbol=number.upper(),
                venue="TEST",
                currency="USD",
                created_at=NOW,
                updated_at=NOW,
            )
        )
    return repository


def make_update(
    version: int,
    *,
    weights=("0.50", "0.50"),
    effective_at: datetime = NOW,
) -> Clean40AllocationUpdate:
    return Clean40AllocationUpdate(
        account_id="stage5-account",
        version=version,
        effective_at=effective_at,
        provenance=f"offline-test-v{version}",
        targets=(
            SleeveAllocationTarget("sleeve-a", "book-a", target_weight=weights[0]),
            SleeveAllocationTarget("sleeve-b", "book-b", target_weight=weights[1]),
        ),
    )


def make_target(
    sleeve_id: str,
    instruments: tuple[str, str],
    cycle: str,
    quantities=("4", "-4"),
    *,
    action=IntentAction.ENTER,
) -> NormalizedPairTarget:
    return NormalizedPairTarget(
        sleeve_id=sleeve_id,
        cycle_id=cycle,
        signal_id=f"signal-{cycle}",
        instrument_ids=instruments,
        signed_quantities=tuple(Decimal(value) for value in quantities),
        action=action,
        evaluated_at=NOW,
        provenance={"source": "offline-test", "cycle": cycle},
    )


class Stage5Adapter:
    def __init__(self) -> None:
        self.submit_calls: list[str] = []

    def get_authoritative_account_facts(self, account: Account) -> BrokerFactSnapshot:
        return BrokerFactSnapshot(account_id=account.id, captured_at=NOW, complete=True)

    def submit_order(self, _account: Account, request) -> BrokerSubmissionResult:
        self.submit_calls.append(request.broker_order_id)
        return BrokerSubmissionResult(
            broker_order_id=request.broker_order_id,
            accepted=True,
            status=BrokerOrderStatus.WORKING,
            external_order_id=f"stage5-order-{len(self.submit_calls)}",
            client_order_id=request.client_order_id,
            submitted_at=NOW,
        )


def risk(intent_id: str, number: int) -> RiskDecisionRecord:
    return RiskDecisionRecord(
        id=f"stage5-risk-{intent_id}-{number}",
        intent_id=intent_id,
        approved=True,
        reason="offline stage5 test",
        checks={"offline": True},
        evaluated_at=NOW,
    )


def test_two_independent_sleeves_emit_book_owned_entry_exit_and_cycle_idempotency(tmp_path):
    repository = make_repository(tmp_path)
    sleeves = make_sleeves()
    coordinator = Stage5AllocationCoordinator(repository, sleeves)
    coordinator.apply(make_update(1))

    entry = sleeves[0].to_intent(make_target("sleeve-a", ("a-1", "a-2"), "cycle-1"))
    exit_intent = sleeves[0].to_intent(
        make_target(
            "sleeve-a",
            ("a-1", "a-2"),
            "cycle-1",
            action=IntentAction.EXIT,
        )
    )
    other = sleeves[1].to_intent(make_target("sleeve-b", ("b-1", "b-2"), "cycle-1"))

    assert entry.book_id == "book-a"
    assert entry.action is IntentAction.ENTER
    assert [leg.side for leg in entry.legs] == [Side.BUY, Side.SELL]
    assert exit_intent.book_id == "book-a"
    assert exit_intent.action is IntentAction.EXIT
    assert [leg.side for leg in exit_intent.legs] == [Side.SELL, Side.BUY]
    assert other.book_id == "book-b"
    assert other.idempotency_key != entry.idempotency_key

    assert repository.create_intent(entry) == (entry.id, True)
    assert repository.create_intent(sleeves[0].to_intent(make_target("sleeve-a", ("a-1", "a-2"), "cycle-1"))) == (entry.id, False)
    later = sleeves[0].to_intent(make_target("sleeve-a", ("a-1", "a-2"), "cycle-2"))
    assert repository.create_intent(later) == (later.id, True)


def test_simultaneous_sleeves_use_independent_book_and_account_caps(tmp_path):
    repository = make_repository(tmp_path)
    sleeves = make_sleeves()
    Stage5AllocationCoordinator(repository, sleeves).apply(make_update(1))
    adapter = Stage5Adapter()
    oms = GenericOMS(repository, adapter, clock=lambda: NOW)
    account = make_account()

    first_intent = sleeves[0].to_intent(make_target("sleeve-a", ("a-1", "a-2"), "entry-a"))
    first = oms.submit_intent(
        first_intent,
        account=account,
        risk_decision=risk(first_intent.id, 1),
    )
    assert first["status"] == IntentStatus.WORKING.value

    second_intent = sleeves[1].to_intent(make_target("sleeve-b", ("b-1", "b-2"), "entry-b"))
    second = oms.submit_intent(second_intent, account=account, risk_decision=risk(second_intent.id, 2))
    assert second["status"] == IntentStatus.WORKING.value
    assert len(adapter.submit_calls) == 4

    over = sleeves[0].to_intent(
        make_target("sleeve-a", ("a-1", "a-2"), "entry-over", quantities=("6", "-5"))
    )
    rejected = oms.submit_intent(over, account=account, risk_decision=risk(over.id, 3))
    assert rejected["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert len(adapter.submit_calls) == 4


def test_allocation_update_is_versioned_atomic_and_does_not_reassign_positions(tmp_path):
    repository = make_repository(tmp_path)
    sleeves = make_sleeves()
    coordinator = Stage5AllocationCoordinator(repository, sleeves)
    coordinator.apply(make_update(1, weights=("0.60", "0.40")))
    entry = sleeves[0].to_intent(make_target("sleeve-a", ("a-1", "a-2"), "owned-cycle"))
    repository.create_intent(entry)
    repository.save_position_allocation(
        PositionAllocation(
            id="stage5-owned-position",
            account_id="stage5-account",
            instrument_id="a-1",
            strategy_id="stage5-strategy",
            book_id="book-a",
            ownership_class=OwnershipClass.MANAGED,
            signed_quantity=Decimal("4"),
            source_intent_id=entry.id,
            updated_at=NOW,
            metadata={"provenance": "offline-stage5"},
        ),
        _validation_token=repository._allocation_validation_capability(),
    )

    coordinator.apply(make_update(2, weights=("0.25", "0.75")))
    rows = repository.position_allocations("stage5-account")
    assert rows[0]["book_id"] == "book-a"
    assert Decimal(rows[0]["signed_quantity"]) == Decimal("4")
    active = repository.book_allocations("stage5-account", active_only=True)
    assert {row["book_id"]: Decimal(row["capital_fraction"]) for row in active} == {
        "book-a": Decimal("0.25"),
        "book-b": Decimal("0.75"),
    }

    restarted = SQLiteTradingRepository(tmp_path / "stage5.db")
    restarted.initialize()
    assert len(restarted.position_allocations("stage5-account")) == 1
    assert {row["book_id"] for row in restarted.book_allocations("stage5-account")} == {"book-a", "book-b"}


def test_invalid_total_or_mixed_allocation_fails_before_persistence(tmp_path):
    repository = make_repository(tmp_path)
    sleeves = make_sleeves()
    coordinator = Stage5AllocationCoordinator(repository, sleeves)

    with pytest.raises(ValueError, match="at most 1"):
        coordinator.apply(make_update(1, weights=("0.60", "0.50")))
    assert repository.book_allocations("stage5-account", active_only=False) == []

    mixed = Clean40AllocationUpdate(
        account_id="stage5-account",
        version=1,
        effective_at=NOW,
        provenance="mixed",
        targets=(
            SleeveAllocationTarget("sleeve-a", "book-a", target_weight="0.5"),
            SleeveAllocationTarget(
                "sleeve-b",
                "book-b",
                capacity="5",
                capacity_unit="RISK_BUDGET",
            ),
        ),
    )
    with pytest.raises(ValueError, match="incompatible"):
        coordinator.apply(mixed)
    assert repository.book_allocations("stage5-account", active_only=False) == []


def test_allocation_retry_is_idempotent_and_stale_version_cannot_rewind(tmp_path):
    repository = make_repository(tmp_path)
    coordinator = Stage5AllocationCoordinator(repository, make_sleeves())
    update = make_update(1)
    first = coordinator.apply(update)
    assert coordinator.apply(update) == first
    with pytest.raises(ValueError, match="increase|different"):
        coordinator.apply(make_update(1, weights=("0.40", "0.60")))


def test_allocation_schedule_rejects_earlier_future_version_without_capacity_inflation(tmp_path):
    repository = make_repository(tmp_path)
    coordinator = Stage5AllocationCoordinator(repository, make_sleeves())
    future = NOW + timedelta(days=1)
    coordinator.apply(make_update(1, effective_at=future))

    with pytest.raises(ValueError, match="cannot precede the latest scheduled"):
        coordinator.apply(
            make_update(
                2,
                weights=("0.75", "0.25"),
                effective_at=NOW + timedelta(hours=1),
            )
        )

    # The failed atomic update did not add a second declaration or change the
    # future allocation.  A later monotonic version supersedes both books at
    # its own effective time without overlapping capacity.
    before_later = repository.book_allocations(
        "stage5-account",
        at=future,
        active_only=True,
    )
    assert {row["book_id"]: Decimal(row["capital_fraction"]) for row in before_later} == {
        "book-a": Decimal("0.50"),
        "book-b": Decimal("0.50"),
    }
    later = future + timedelta(days=1)
    coordinator.apply(make_update(2, weights=("0.75", "0.25"), effective_at=later))
    at_later = repository.book_allocations("stage5-account", at=later, active_only=True)
    assert {row["book_id"]: Decimal(row["capital_fraction"]) for row in at_later} == {
        "book-a": Decimal("0.75"),
        "book-b": Decimal("0.25"),
    }
    assert len(repository.book_allocations("stage5-account", active_only=False)) == 4
