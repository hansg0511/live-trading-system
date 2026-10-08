from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from dataclasses import replace
import json
import sqlite3
import shutil

import pytest

from src.trading_core.domain import (
    Account,
    AssetClass,
    Book,
    BrokerOrderEvent,
    BrokerOrderSnapshot,
    BrokerOrderStatus,
    ExecutionEvidenceBaseline,
    ExecutionEvidenceMode,
    ExecutionPolicy,
    Instrument,
    InstrumentMapping,
    IntentAction,
    IntentStatus,
    LegStatus,
    Fill,
    MappingPurpose,
    OrderIntent,
    OrderLeg,
    OwnershipClass,
    PositionAllocation,
    PositionSnapshot,
    RiskDecisionRecord,
    Side,
    Strategy,
    TradingEnvironment,
)
from src.trading_core.oms import GenericOMS, OMSExecutionError
from src.trading_core.ports import BrokerFactSnapshot, BrokerFill, BrokerHistoricalOrderFacts
from src.trading_core.repository import SQLiteTradingRepository


NOW = datetime(2026, 10, 1, 3, 0, tzinfo=timezone.utc)


def _account() -> Account:
    return Account(
        id="moomoo:sim:5077333",
        broker="moomoo",
        environment=TradingEnvironment.SIM,
        external_account_id="5077333",
        base_currency="USD",
        created_at=NOW,
        updated_at=NOW,
    )


def _repository(tmp_path, *, account: Account | None = None) -> SQLiteTradingRepository:
    repository = SQLiteTradingRepository(tmp_path / "compat.db")
    repository.initialize()
    account = account or _account()
    repository.save_account(account)
    repository.save_strategy(
        Strategy(
            id="compat-strategy",
            name="compatibility tests",
            strategy_type="test",
            created_at=NOW,
            updated_at=NOW,
        )
    )
    repository.save_instrument(
        Instrument(
            id="compat-instrument",
            asset_class=AssetClass.EQUITY,
            symbol="COMPAT",
            venue="US",
            currency="USD",
            created_at=NOW,
            updated_at=NOW,
        )
    )
    return repository


def _intent() -> OrderIntent:
    return OrderIntent(
        id="compat-intent",
        idempotency_key="compat-intent-key",
        strategy_id="compat-strategy",
        account_id=_account().id,
        action=IntentAction.ENTER,
        execution_policy=ExecutionPolicy(),
        legs=(
            OrderLeg(
                id="compat-leg",
                intent_id="compat-intent",
                sequence=0,
                instrument_id="compat-instrument",
                side=Side.BUY,
                quantity=Decimal("1"),
                status=LegStatus.PLANNED,
                created_at=NOW,
                updated_at=NOW,
            ),
        ),
        created_at=NOW,
        updated_at=NOW,
    )


class _FactsAdapter:
    def __init__(self, facts: BrokerFactSnapshot) -> None:
        self.facts = facts
        self.submit_calls = 0
        self.history_override = None

    def get_authoritative_account_facts(self, _account: Account) -> BrokerFactSnapshot:
        return self.facts

    def submit_order(self, _account, _request):  # pragma: no cover - gate must prevent this
        self.submit_calls += 1
        raise AssertionError("the account gate must prevent submission")

    def get_historical_order_facts(self, account, start, end):
        if self.history_override is not None:
            return self.history_override
        return BrokerHistoricalOrderFacts(
            account_id=account.id,
            requested_start=start,
            requested_end=end,
            captured_at=end,
            complete=True,
            execution_evidence_mode=self.facts.execution_evidence_mode,
            execution_evidence_scope=frozenset({"HISTORICAL_ORDER_SNAPSHOTS"}),
            metadata={"source": "offline-test"},
        )


class _MoomooSimRecoveryAdapter(_FactsAdapter):
    """Minimal adapter-shaped SIM fixture for the recovery evidence bridge."""

    def __init__(self, facts: BrokerFactSnapshot, terminal_order: BrokerOrderSnapshot) -> None:
        super().__init__(facts)
        self.terminal_order = terminal_order

    def get_order(self, account: Account, external_order_id: str) -> BrokerOrderSnapshot | None:
        if account.id != self.facts.account_id:
            return None
        if external_order_id != self.terminal_order.external_order_id:
            return None
        return self.terminal_order

    def get_positions(self, account: Account) -> tuple[PositionSnapshot, ...]:
        return self.facts.positions if account.id == self.facts.account_id else ()


def _seed_moomoo_sim_fill_recovery(tmp_path, mode: ExecutionEvidenceMode):
    repository = _repository(tmp_path)
    account = _account()
    intent = _intent()
    repository.create_intent(intent)
    repository.transition_intent(intent.id, IntentStatus.RISK_APPROVED, now=NOW)
    repository.transition_intent(intent.id, IntentStatus.SUBMITTING, now=NOW)
    repository.transition_leg(intent.legs[0].id, LegStatus.SUBMITTING, now=NOW)
    repository.create_broker_order(
        broker_order_id="moomoo-sim-broker-order",
        order_leg_id=intent.legs[0].id,
        account_id=account.id,
        broker=account.broker,
        attempt_number=1,
        client_order_id="moomoo-sim-client-order",
        submitted_quantity=Decimal("1"),
        now=NOW,
    )
    repository.transition_broker_order("moomoo-sim-broker-order", BrokerOrderStatus.SUBMITTING, now=NOW)
    repository.record_submission(
        "moomoo-sim-broker-order",
        status=BrokerOrderStatus.WORKING,
        external_order_id="moomoo-sim-external-order",
        metadata={"provider_status": "SUBMITTED"},
        now=NOW,
    )
    repository.transition_leg(intent.legs[0].id, LegStatus.WORKING, now=NOW)
    repository.transition_intent(intent.id, IntentStatus.WORKING, now=NOW)

    external_order_id = "moomoo-sim-external-order"
    dedupe_key = "moomoo-order-fill:moomoo-sim-external-order"
    fill = BrokerFill(
        external_order_id=external_order_id,
        external_fill_id=None,
        dedupe_key=dedupe_key,
        quantity=Decimal("1"),
        price=Decimal("101"),
        filled_at=NOW + timedelta(seconds=1),
        received_at=NOW + timedelta(seconds=2),
        account_id=account.id,
        evidence_reference=f"{external_order_id}:{dedupe_key}",
        evidence_mode=mode,
        instrument_id="compat-instrument",
        metadata={
            "_external_order_id": external_order_id,
            "_evidence_reference": f"{external_order_id}:{dedupe_key}",
            "_instrument_id": "compat-instrument",
        },
    )
    position = PositionSnapshot(
        id="moomoo-sim-position",
        broker_snapshot_id="moomoo-sim-position-snapshot",
        account_id=account.id,
        instrument_id="compat-instrument",
        signed_quantity=Decimal("1"),
        average_price=Decimal("101"),
        captured_at=NOW + timedelta(seconds=2),
    )
    facts = BrokerFactSnapshot(
        account_id=account.id,
        captured_at=NOW + timedelta(seconds=2),
        complete=True,
        positions=(position,),
        open_orders=(),
        fills=(fill,),
        execution_evidence_mode=mode,
        execution_evidence_scope=frozenset({"CURRENT_ORDER_SNAPSHOTS"}),
        metadata={"source": "moomoo-sim-order-snapshot"},
    )
    terminal_order = BrokerOrderSnapshot(
        id="moomoo-sim-terminal-snapshot",
        broker_snapshot_id="moomoo-sim-terminal-order-snapshot",
        account_id=account.id,
        instrument_id="compat-instrument",
        external_order_id=external_order_id,
        client_order_id="moomoo-sim-client-order",
        side=Side.BUY,
        quantity=Decimal("1"),
        filled_quantity=Decimal("1"),
        status=BrokerOrderStatus.FILLED,
        captured_at=NOW + timedelta(seconds=2),
        order_time=NOW,
    )
    if mode is ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS:
        repository.save_execution_evidence_baseline(
            ExecutionEvidenceBaseline(
                id="moomoo-sim-flat-baseline",
                account_id=account.id,
                captured_at=NOW,
                evidence_mode=mode,
                coverage=("CURRENT_ORDER_SNAPSHOTS",),
                source_ledger_fingerprint="moomoo-sim-source-ledger",
                source_order_ids=(),
                position_fingerprint="flat-position-fingerprint",
                open_order_fingerprint="flat-order-fingerprint",
            )
        )
    adapter = _MoomooSimRecoveryAdapter(facts, terminal_order)
    return account, repository, intent, adapter


@pytest.mark.parametrize(
    "mode",
    (
        ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS,
        ExecutionEvidenceMode.INDIVIDUAL_DEALS,
    ),
)
def test_moomoo_sim_fill_recovery_persists_provider_evidence_mode(tmp_path, mode):
    account, repository, intent, adapter = _seed_moomoo_sim_fill_recovery(tmp_path, mode)
    oms = GenericOMS(repository, adapter, clock=lambda: NOW + timedelta(seconds=3))

    recovered = oms.recover_intent(intent.id, account=account)

    assert recovered["legs"][0]["status"] == LegStatus.FILLED.value
    with repository.transaction() as connection:
        row = connection.execute(
            "SELECT evidence_mode FROM core_fills WHERE broker_order_id = ?",
            ("moomoo-sim-broker-order",),
        ).fetchone()
    assert row["evidence_mode"] == mode.value

    # A fresh SIM cumulative snapshot has a new local receipt timestamp.  A
    # restart must replay the same provider evidence idempotently and retain
    # the original evidence mode instead of defaulting to individual deals.
    restarted = GenericOMS(repository, adapter, clock=lambda: NOW + timedelta(seconds=4))
    restarted.recover_intent(intent.id, account=account)
    with repository.transaction() as connection:
        rows = connection.execute(
            "SELECT evidence_mode FROM core_fills WHERE broker_order_id = ?",
            ("moomoo-sim-broker-order",),
        ).fetchall()
    assert [item["evidence_mode"] for item in rows] == [mode.value]


@pytest.mark.parametrize(
    "mode",
    (
        ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS,
        ExecutionEvidenceMode.INDIVIDUAL_DEALS,
    ),
)
def test_moomoo_sim_validated_event_persists_provider_evidence_mode(tmp_path, mode):
    account, repository, intent, adapter = _seed_moomoo_sim_fill_recovery(tmp_path, mode)
    oms = GenericOMS(repository, adapter, clock=lambda: NOW + timedelta(seconds=3))
    attempt = repository.broker_orders_for_leg(intent.legs[0].id)[0]
    event = BrokerOrderEvent(
        id="moomoo-sim-filled-event",
        broker_order_id=attempt["id"],
        dedupe_key="moomoo-sim-filled-event",
        event_type="ORDER_STATUS",
        event_at=NOW + timedelta(seconds=2),
        received_at=NOW + timedelta(seconds=2),
        broker_status=BrokerOrderStatus.FILLED,
        external_order_id=attempt["external_order_id"],
        account_id=account.id,
        cumulative_filled_quantity=Decimal("1"),
    )

    recovered = oms._ingest_validated_broker_order_event(
        event,
        account=account,
        fills=adapter.facts.fills,
    )

    assert recovered["legs"][0]["status"] == LegStatus.FILLED.value
    with repository.transaction() as connection:
        row = connection.execute(
            "SELECT evidence_mode FROM core_fills WHERE broker_order_id = ?",
            (attempt["id"],),
        ).fetchone()
    assert row["evidence_mode"] == mode.value


def _facts(
    mode: ExecutionEvidenceMode,
    *,
    positions: tuple[PositionSnapshot, ...] = (),
) -> BrokerFactSnapshot:
    return BrokerFactSnapshot(
        account_id=_account().id,
        captured_at=NOW,
        complete=True,
        positions=positions,
        execution_evidence_mode=mode,
        execution_evidence_scope=frozenset({"CURRENT_ORDER_SNAPSHOTS"}),
        metadata={"source": "offline-test"},
    )


def _imported_retired_target(tmp_path):
    target = _repository(tmp_path)
    imported = target.import_legacy_order_evidence("data/generic-sim-smoke.db", _account().id)
    baseline = ExecutionEvidenceBaseline(
        id="retired-book-baseline",
        account_id=_account().id,
        captured_at=NOW,
        evidence_mode=ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS,
        coverage=("CURRENT_ORDER_SNAPSHOTS", "HISTORICAL_ORDER_SNAPSHOTS", "SOURCE_LEDGER"),
        source_ledger_fingerprint=imported["source_ledger_fingerprint"],
        source_order_ids=imported["source_order_ids"],
        position_fingerprint="flat-position-fingerprint",
        open_order_fingerprint="empty-order-fingerprint",
        metadata={"legacy_book_id": imported["legacy_book_id"]},
    )
    target.save_execution_evidence_baseline(baseline)
    return target, imported, baseline


def test_explicitly_unavailable_execution_evidence_blocks_submit(tmp_path):
    repository = _repository(tmp_path)
    intent = _intent()
    repository.create_intent(intent)
    adapter = _FactsAdapter(_facts(ExecutionEvidenceMode.UNAVAILABLE))
    oms = GenericOMS(repository, adapter, clock=lambda: NOW)

    assert oms._account_wide_broker_fact_gate(intent.id, _account()) is False
    assert adapter.submit_calls == 0
    assert any("BROKER_FACT_GATE_BLOCKED" in row["category"] for row in repository.open_reconciliation_issues(_account().id))


def test_never_submitted_reconciliation_intent_is_proof_gated_and_cancelled(tmp_path):
    repository = _repository(tmp_path)
    intent = _intent()
    repository.create_intent(intent)
    repository.transition_intent(intent.id, IntentStatus.RECONCILIATION_REQUIRED, now=NOW)
    oms = GenericOMS(
        repository,
        _FactsAdapter(_facts(ExecutionEvidenceMode.INDIVIDUAL_DEALS)),
        clock=lambda: NOW,
    )

    result = oms.resolve_unsubmitted_intent(intent.id, account=_account())

    assert result["status"] == IntentStatus.CANCELLED.value
    persisted = repository.get_intent(intent.id)
    assert persisted["status"] == IntentStatus.CANCELLED.value
    assert {leg["status"] for leg in persisted["legs"]} == {LegStatus.CANCELLED.value}
    events = repository.operational_events(_account().id)
    assert events[0]["event_type"] == "UNSUBMITTED_INTENT_RECOVERY"


def test_never_submitted_recovery_requires_complete_bounded_history(tmp_path):
    repository = _repository(tmp_path)
    intent = _intent()
    repository.create_intent(intent)
    repository.transition_intent(intent.id, IntentStatus.RECONCILIATION_REQUIRED, now=NOW)
    adapter = _FactsAdapter(_facts(ExecutionEvidenceMode.INDIVIDUAL_DEALS))
    adapter.get_historical_order_facts = None
    oms = GenericOMS(repository, adapter, clock=lambda: NOW)

    with pytest.raises(OMSExecutionError, match="historical order facts"):
        oms.resolve_unsubmitted_intent(intent.id, account=_account())
    assert repository.get_intent(intent.id)["status"] == IntentStatus.RECONCILIATION_REQUIRED.value


def test_never_submitted_recovery_stops_on_fresh_broker_exposure(tmp_path):
    repository = _repository(tmp_path)
    intent = _intent()
    repository.create_intent(intent)
    repository.transition_intent(intent.id, IntentStatus.RECONCILIATION_REQUIRED, now=NOW)
    position = PositionSnapshot(
        id="compat-position",
        broker_snapshot_id="compat-snapshot",
        account_id=_account().id,
        instrument_id="compat-instrument",
        signed_quantity=Decimal("1"),
        captured_at=NOW,
    )
    oms = GenericOMS(
        repository,
        _FactsAdapter(_facts(ExecutionEvidenceMode.INDIVIDUAL_DEALS, positions=(position,))),
        clock=lambda: NOW,
    )

    with pytest.raises(OMSExecutionError, match="non-zero position exposure"):
        oms.resolve_unsubmitted_intent(intent.id, account=_account())

    assert repository.get_intent(intent.id)["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert repository.operational_events(_account().id) == []


def test_cumulative_snapshots_require_verified_baseline_then_allow_flat_gate(tmp_path):
    repository = _repository(tmp_path)
    intent = _intent()
    repository.create_intent(intent)
    facts = _facts(ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS)
    adapter = _FactsAdapter(facts)
    oms = GenericOMS(repository, adapter, clock=lambda: NOW)

    assert oms._account_wide_broker_fact_gate(intent.id, _account()) is False

    # Use a clean second repository because the first blocked gate is a sticky
    # account fact by design.
    clean = _repository(tmp_path / "clean")
    clean.create_intent(intent)
    clean_oms = GenericOMS(clean, _FactsAdapter(facts), clock=lambda: NOW)
    position_fp, open_order_fp = clean_oms.execution_fact_fingerprints(facts)
    clean.save_execution_evidence_baseline(
        ExecutionEvidenceBaseline(
            id="compat-baseline",
            account_id=_account().id,
            captured_at=NOW,
            evidence_mode=ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS,
            coverage=("CURRENT_ORDER_SNAPSHOTS", "SOURCE_LEDGER"),
            source_ledger_fingerprint="compat-source-fingerprint",
            source_order_ids=(),
            position_fingerprint=position_fp,
            open_order_fingerprint=open_order_fp,
        )
    )
    assert clean_oms._account_wide_broker_fact_gate(intent.id, _account()) is True


class _CompensatedPartialAdapter:
    def __init__(self, orders, *, facts=None):
        self.orders = tuple(orders)
        self.facts = facts or _facts(ExecutionEvidenceMode.INDIVIDUAL_DEALS)

    def get_authoritative_account_facts(self, _account):
        return self.facts

    def get_historical_order_facts(self, account, start, end):
        return BrokerHistoricalOrderFacts(
            account_id=account.id,
            requested_start=start,
            requested_end=end,
            captured_at=end,
            complete=True,
            orders=self.orders,
            execution_evidence_mode=ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS,
            execution_evidence_scope=frozenset({"HISTORICAL_ORDER_SNAPSHOTS"}),
            metadata={"source": "offline-compensated-proof"},
        )


class _FailingRecoveryAdapter:
    def get_open_orders(self, _account):
        raise AssertionError("proof-closed history must not be polled again")


def _seed_compensated_partial(repository: SQLiteTradingRepository):
    repository.save_book(Book(id="compat-book", name="compat book", created_at=NOW, updated_at=NOW))
    repository.save_instrument(
        Instrument(
            id="compat-instrument-2",
            asset_class=AssetClass.EQUITY,
            symbol="COMPAT2",
            venue="US",
            currency="USD",
            created_at=NOW,
            updated_at=NOW,
        )
    )
    entry_id = "compensated-entry"
    entry = OrderIntent(
        id=entry_id,
        idempotency_key="compensated-entry-key",
        strategy_id="compat-strategy",
        account_id=_account().id,
        book_id="compat-book",
        action=IntentAction.ENTER,
        execution_policy=ExecutionPolicy(),
        legs=(
            OrderLeg(
                id="compensated-entry-leg-a",
                intent_id=entry_id,
                sequence=0,
                instrument_id="compat-instrument",
                side=Side.BUY,
                quantity=Decimal("1"),
                created_at=NOW,
                updated_at=NOW,
            ),
            OrderLeg(
                id="compensated-entry-leg-b",
                intent_id=entry_id,
                sequence=1,
                instrument_id="compat-instrument-2",
                side=Side.SELL,
                quantity=Decimal("1"),
                created_at=NOW,
                updated_at=NOW,
            ),
        ),
        created_at=NOW,
        updated_at=NOW,
    )
    repository.create_intent(entry)
    repository.transition_intent(entry_id, IntentStatus.RISK_APPROVED, now=NOW)
    repository.transition_intent(entry_id, IntentStatus.SUBMITTING, now=NOW)
    for leg in entry.legs:
        repository.transition_leg(leg.id, LegStatus.SUBMITTING, now=NOW)
    repository.create_broker_order(
        broker_order_id="compensated-entry-order-a",
        order_leg_id=entry.legs[0].id,
        account_id=_account().id,
        broker=_account().broker,
        attempt_number=1,
        client_order_id="compensated-entry-leg-a:1",
        submitted_quantity=Decimal("1"),
        now=NOW,
    )
    repository.transition_broker_order("compensated-entry-order-a", BrokerOrderStatus.SUBMITTING, now=NOW)
    repository.record_submission(
        "compensated-entry-order-a",
        status=BrokerOrderStatus.FILLED,
        external_order_id="compensated-entry-external-a",
        metadata={"provider_status": "FILLED_ALL"},
        now=NOW,
    )
    repository.record_fill(
        Fill(
            id="compensated-entry-fill-a",
            broker_order_id="compensated-entry-order-a",
            order_leg_id=entry.legs[0].id,
            dedupe_key="compensated-entry-fill-a",
            quantity=Decimal("1"),
            price=Decimal("100"),
            filled_at=NOW,
            received_at=NOW,
            account_id=_account().id,
            external_order_id="compensated-entry-external-a",
            evidence_reference="compensated-entry-fill-a",
        ),
        now=NOW,
        _validation_token=repository._fill_validation_capability(),
    )
    repository.transition_leg(entry.legs[1].id, LegStatus.RECONCILIATION_REQUIRED, now=NOW)
    repository.transition_leg(entry.legs[1].id, LegStatus.CANCELLED, now=NOW)
    repository.transition_intent(entry_id, IntentStatus.RECONCILIATION_REQUIRED, now=NOW)

    exit_id = "compensated-exit"
    exit_intent = OrderIntent(
        id=exit_id,
        idempotency_key="compensated-exit-key",
        strategy_id="compat-strategy",
        account_id=_account().id,
        book_id="compat-book",
        action=IntentAction.EXIT,
        execution_policy=ExecutionPolicy(),
        metadata={"stage6_pilot": {"source_entry_intent": entry_id}},
        legs=(
            OrderLeg(
                id="compensated-exit-leg-a",
                intent_id=exit_id,
                sequence=0,
                instrument_id="compat-instrument",
                side=Side.SELL,
                quantity=Decimal("1"),
                created_at=NOW,
                updated_at=NOW,
            ),
        ),
        created_at=NOW,
        updated_at=NOW,
    )
    repository.create_intent(exit_intent)
    repository.transition_intent(exit_id, IntentStatus.RISK_APPROVED, now=NOW)
    repository.transition_intent(exit_id, IntentStatus.SUBMITTING, now=NOW)
    repository.transition_leg(exit_intent.legs[0].id, LegStatus.SUBMITTING, now=NOW)
    repository.create_broker_order(
        broker_order_id="compensated-exit-order-a",
        order_leg_id=exit_intent.legs[0].id,
        account_id=_account().id,
        broker=_account().broker,
        attempt_number=1,
        client_order_id="compensated-exit-leg-a:1",
        submitted_quantity=Decimal("1"),
        now=NOW,
    )
    repository.transition_broker_order("compensated-exit-order-a", BrokerOrderStatus.SUBMITTING, now=NOW)
    repository.record_submission(
        "compensated-exit-order-a",
        status=BrokerOrderStatus.FILLED,
        external_order_id="compensated-exit-external-a",
        metadata={"provider_status": "FILLED_ALL"},
        now=NOW,
    )
    repository.record_fill(
        Fill(
            id="compensated-exit-fill-a",
            broker_order_id="compensated-exit-order-a",
            order_leg_id=exit_intent.legs[0].id,
            dedupe_key="compensated-exit-fill-a",
            quantity=Decimal("1"),
            price=Decimal("101"),
            filled_at=NOW,
            received_at=NOW,
            account_id=_account().id,
            external_order_id="compensated-exit-external-a",
            evidence_reference="compensated-exit-fill-a",
        ),
        now=NOW,
        _validation_token=repository._fill_validation_capability(),
    )
    repository.transition_intent(exit_id, IntentStatus.COMPLETED, now=NOW)
    snapshots = (
        BrokerOrderSnapshot(
            id="proof-entry-snapshot",
            broker_snapshot_id="proof-history",
            account_id=_account().id,
            instrument_id="compat-instrument",
            external_order_id="compensated-entry-external-a",
            side=Side.BUY,
            quantity=Decimal("1"),
            filled_quantity=Decimal("1"),
            status=BrokerOrderStatus.FILLED,
            captured_at=NOW,
            order_time=NOW,
        ),
        BrokerOrderSnapshot(
            id="proof-exit-snapshot",
            broker_snapshot_id="proof-history",
            account_id=_account().id,
            instrument_id="compat-instrument",
            external_order_id="compensated-exit-external-a",
            side=Side.SELL,
            quantity=Decimal("1"),
            filled_quantity=Decimal("1"),
            status=BrokerOrderStatus.FILLED,
            captured_at=NOW,
            order_time=NOW,
        ),
    )
    return entry, exit_intent, snapshots


def _seed_stale_compensating_link(
    repository: SQLiteTradingRepository,
    entry: OrderIntent,
    *,
    nonterminal: bool = False,
) -> str:
    """Add a retry artifact that must not hide the proven compensation."""

    intent_id = "stale-compensating-exit"
    leg_id = f"{intent_id}-leg-0"
    stale = OrderIntent(
        id=intent_id,
        idempotency_key=f"{intent_id}-key",
        strategy_id=entry.strategy_id,
        account_id=_account().id,
        book_id=entry.book_id,
        action=IntentAction.EXIT,
        execution_policy=ExecutionPolicy(),
        metadata={"stage6_pilot": {"source_entry_intent": entry.id}},
        legs=(
            OrderLeg(
                id=leg_id,
                intent_id=intent_id,
                sequence=0,
                instrument_id="compat-instrument",
                side=Side.SELL,
                quantity=Decimal("1"),
                created_at=NOW,
                updated_at=NOW,
            ),
        ),
        created_at=NOW,
        updated_at=NOW,
    )
    repository.create_intent(stale)
    repository.transition_intent(intent_id, IntentStatus.RISK_APPROVED, now=NOW)
    repository.transition_intent(intent_id, IntentStatus.SUBMITTING, now=NOW)
    repository.transition_leg(leg_id, LegStatus.SUBMITTING, now=NOW)
    broker_order_id = f"{intent_id}-order"
    repository.create_broker_order(
        broker_order_id=broker_order_id,
        order_leg_id=leg_id,
        account_id=_account().id,
        broker=_account().broker,
        attempt_number=1,
        client_order_id=f"{leg_id}:1",
        submitted_quantity=Decimal("1"),
        now=NOW,
    )
    repository.transition_broker_order(broker_order_id, BrokerOrderStatus.SUBMITTING, now=NOW)
    if nonterminal:
        repository.record_submission(
            broker_order_id,
            status=BrokerOrderStatus.WORKING,
            external_order_id="stale-compensating-external",
            metadata={
                "provider_status": BrokerOrderStatus.WORKING.value,
                "accepted": True,
                "ambiguous": False,
                "cumulative_fill_known": True,
                "reported_cumulative_fill": "0",
                "no_fill_asserted": True,
                "raw_payload": {},
            },
            now=NOW,
        )
        repository.transition_leg(leg_id, LegStatus.WORKING, now=NOW)
        repository.transition_intent(intent_id, IntentStatus.WORKING, now=NOW)
    else:
        repository.record_submission(
            broker_order_id,
            status=BrokerOrderStatus.REJECTED,
            external_order_id=None,
            metadata={
                "provider_status": BrokerOrderStatus.REJECTED.value,
                "accepted": False,
                "ambiguous": False,
                "definite_no_submit": True,
                "no_submit_asserted": True,
                "no_fill_asserted": True,
                "cumulative_fill_known": True,
                "reported_cumulative_fill": "0",
                "raw_payload": {},
            },
            now=NOW,
        )
        repository.transition_leg(leg_id, LegStatus.REJECTED, now=NOW)
        repository.transition_intent(intent_id, IntentStatus.REJECTED, now=NOW)
    return intent_id


def test_compensated_partial_entry_closure_requires_fresh_exact_proof(tmp_path):
    repository = _repository(tmp_path)
    entry, _exit, snapshots = _seed_compensated_partial(repository)
    oms = GenericOMS(repository, _CompensatedPartialAdapter(snapshots), clock=lambda: NOW + timedelta(seconds=1))

    result = oms.resolve_compensated_partial_intent(entry.id, account=_account())

    assert result["status"] == IntentStatus.CANCELLED.value
    persisted = repository.get_intent(entry.id)
    assert persisted["status"] == IntentStatus.CANCELLED.value
    assert {leg["status"] for leg in persisted["legs"]} == {
        LegStatus.FILLED.value,
        LegStatus.CANCELLED.value,
    }
    assert persisted["metadata"]["compensated_partial_closure"]["unsent_leg_ids"] == [
        "compensated-entry-leg-b"
    ]
    assert repository.operational_events(_account().id)[0]["event_type"] == "COMPENSATED_PARTIAL_INTENT_CLOSED"


def test_compensated_closure_proves_capacity_neutral_intent_group(tmp_path):
    repository = _repository(tmp_path)
    entry, exit_intent, snapshots = _seed_compensated_partial(repository)
    oms = GenericOMS(repository, _CompensatedPartialAdapter(snapshots), clock=lambda: NOW + timedelta(seconds=1))

    oms.resolve_compensated_partial_intent(entry.id, account=_account())

    persisted = repository.get_intent(entry.id)
    closure = persisted["metadata"]["compensated_partial_closure"]
    assert closure["account_id"] == _account().id
    assert closure["book_id"] == "compat-book"
    assert closure["compensating_intent_ids"] == [exit_intent.id]
    closed_ids = oms._closed_historical_intent_ids(_account())
    assert {entry.id, exit_intent.id}.issubset(closed_ids)
    assert repository.book_signed_exposure(
        _account().id,
        "compat-book",
        exclude_intent_ids=closed_ids,
    ) == {}


def test_proof_closed_intent_is_not_demoted_by_restart_recovery(tmp_path):
    repository = _repository(tmp_path)
    entry, _exit_intent, snapshots = _seed_compensated_partial(repository)
    proof_oms = GenericOMS(
        repository,
        _CompensatedPartialAdapter(snapshots),
        clock=lambda: NOW + timedelta(seconds=1),
    )
    proof_oms.resolve_compensated_partial_intent(entry.id, account=_account())

    recovered_oms = GenericOMS(
        repository,
        _FailingRecoveryAdapter(),
        clock=lambda: NOW + timedelta(seconds=2),
    )
    recovered = recovered_oms.recover_intent(entry.id, account=_account())

    assert recovered["status"] == IntentStatus.CANCELLED.value
    assert repository.get_intent(entry.id)["status"] == IntentStatus.CANCELLED.value
    assert repository.open_reconciliation_issues(_account().id) == []


def test_scheduler_skips_proof_closed_intent_after_lifecycle_status_demotion(tmp_path):
    repository = _repository(tmp_path)
    entry, _exit_intent, snapshots = _seed_compensated_partial(repository)
    proof_oms = GenericOMS(
        repository,
        _CompensatedPartialAdapter(snapshots),
        clock=lambda: NOW + timedelta(seconds=1),
    )
    proof_oms.resolve_compensated_partial_intent(entry.id, account=_account())

    # Simulate the persisted status demotion that the proof-backed closure
    # must withstand after a later transient recovery path.  The immutable
    # closure envelope remains the source of truth for scheduler admission.
    with repository.transaction() as conn:
        conn.execute(
            "UPDATE core_order_intents SET status = ?, updated_at = ? WHERE id = ?",
            (IntentStatus.RECONCILIATION_REQUIRED.value, (NOW + timedelta(seconds=2)).isoformat(), entry.id),
        )

    recovered_oms = GenericOMS(
        repository,
        _FailingRecoveryAdapter(),
        clock=lambda: NOW + timedelta(seconds=2),
    )
    recovered = recovered_oms.recover_pending_intents(account=_account())

    assert {item["id"] for item in recovered} == {entry.id, "compensated-exit"}
    assert next(item for item in recovered if item["id"] == entry.id)["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert repository.get_intent(entry.id)["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert repository.open_reconciliation_issues(_account().id) == []


def test_compensated_partial_promotes_reconciled_but_exactly_filled_exit(tmp_path):
    repository = _repository(tmp_path)
    entry, exit_intent, snapshots = _seed_compensated_partial(repository)
    repository.transition_intent(exit_intent.id, IntentStatus.RECONCILIATION_REQUIRED, now=NOW)
    oms = GenericOMS(repository, _CompensatedPartialAdapter(snapshots), clock=lambda: NOW + timedelta(seconds=1))

    result = oms.resolve_compensated_partial_intent(entry.id, account=_account())

    assert result["status"] == IntentStatus.CANCELLED.value
    assert repository.get_intent(exit_intent.id)["status"] == IntentStatus.COMPLETED.value
    events = repository.operational_events(_account().id, limit=20)
    assert any(
        event["event_type"] == "VERIFIED_COMPENSATING_EXIT_CLOSED"
        and event["id"] == f"verified-compensating-exit:{exit_intent.id}"
        for event in events
    )


def test_compensated_partial_exit_accepts_current_source_link_and_restores_book_exposure(tmp_path):
    repository = _repository(tmp_path)
    repository.save_book(Book(id="compat-book", name="compat book", created_at=NOW, updated_at=NOW))
    repository.save_instrument(
        Instrument(
            id="compat-instrument-2",
            asset_class=AssetClass.EQUITY,
            symbol="COMPAT2",
            venue="US",
            currency="USD",
            created_at=NOW,
            updated_at=NOW,
        )
    )
    account = _account()
    source_id = "partial-exit-source"
    source = OrderIntent(
        id=source_id,
        idempotency_key=f"{source_id}-key",
        strategy_id="compat-strategy",
        account_id=account.id,
        book_id="compat-book",
        action=IntentAction.EXIT,
        execution_policy=ExecutionPolicy(),
        legs=(
            OrderLeg(
                id=f"{source_id}-leg-a",
                intent_id=source_id,
                sequence=0,
                instrument_id="compat-instrument",
                side=Side.SELL,
                quantity=Decimal("1"),
                created_at=NOW,
                updated_at=NOW,
            ),
            OrderLeg(
                id=f"{source_id}-leg-b",
                intent_id=source_id,
                sequence=1,
                instrument_id="compat-instrument-2",
                side=Side.BUY,
                quantity=Decimal("1"),
                created_at=NOW,
                updated_at=NOW,
            ),
        ),
        created_at=NOW,
        updated_at=NOW,
    )
    repository.create_intent(source)
    repository.transition_intent(source_id, IntentStatus.RISK_APPROVED, now=NOW)
    repository.transition_intent(source_id, IntentStatus.SUBMITTING, now=NOW)
    repository.transition_leg(source.legs[0].id, LegStatus.SUBMITTING, now=NOW)
    repository.transition_leg(source.legs[1].id, LegStatus.CANCELLED, now=NOW)

    def add_filled_leg(intent: OrderIntent, leg: OrderLeg, external_id: str, price: str) -> BrokerOrderSnapshot:
        repository.create_broker_order(
            broker_order_id=f"{intent.id}-order",
            order_leg_id=leg.id,
            account_id=account.id,
            broker=account.broker,
            attempt_number=1,
            client_order_id=f"{leg.id}:1",
            submitted_quantity=leg.quantity,
            now=NOW,
        )
        repository.transition_broker_order(f"{intent.id}-order", BrokerOrderStatus.SUBMITTING, now=NOW)
        repository.record_submission(
            f"{intent.id}-order",
            status=BrokerOrderStatus.FILLED,
            external_order_id=external_id,
            metadata={"provider_status": "FILLED"},
            now=NOW,
        )
        repository.record_fill(
            Fill(
                id=f"{intent.id}-fill",
                broker_order_id=f"{intent.id}-order",
                order_leg_id=leg.id,
                dedupe_key=f"{intent.id}-deal",
                quantity=leg.quantity,
                price=Decimal(price),
                filled_at=NOW,
                received_at=NOW,
                account_id=account.id,
                external_order_id=external_id,
                evidence_reference=f"{intent.id}-deal",
            ),
            now=NOW,
            _validation_token=repository._fill_validation_capability(),
        )
        return BrokerOrderSnapshot(
            id=f"{intent.id}-snapshot",
            broker_snapshot_id="partial-exit-history",
            account_id=account.id,
            instrument_id=leg.instrument_id,
            external_order_id=external_id,
            side=leg.side,
            quantity=leg.quantity,
            filled_quantity=leg.quantity,
            status=BrokerOrderStatus.FILLED,
            captured_at=NOW,
            order_time=NOW,
        )

    source_snapshot = add_filled_leg(source, source.legs[0], "partial-exit-source-external", "100")
    repository.transition_intent(source_id, IntentStatus.RECONCILIATION_REQUIRED, now=NOW)

    compensation_id = "partial-exit-compensation"
    compensation = OrderIntent(
        id=compensation_id,
        idempotency_key=f"{compensation_id}-key",
        strategy_id="compat-strategy",
        account_id=account.id,
        book_id="compat-book",
        action=IntentAction.ENTER,
        execution_policy=ExecutionPolicy(),
        metadata={
            "verified_partial_compensation": True,
            "source_intent_id": source_id,
        },
        legs=(
            OrderLeg(
                id=f"{compensation_id}-leg",
                intent_id=compensation_id,
                sequence=0,
                instrument_id="compat-instrument",
                side=Side.BUY,
                quantity=Decimal("1"),
                created_at=NOW,
                updated_at=NOW,
            ),
        ),
        created_at=NOW,
        updated_at=NOW,
    )
    repository.create_intent(compensation)
    repository.transition_intent(compensation_id, IntentStatus.RISK_APPROVED, now=NOW)
    repository.transition_intent(compensation_id, IntentStatus.SUBMITTING, now=NOW)
    repository.transition_leg(compensation.legs[0].id, LegStatus.SUBMITTING, now=NOW)
    compensation_snapshot = add_filled_leg(
        compensation,
        compensation.legs[0],
        "partial-exit-compensation-external",
        "101",
    )
    repository.transition_intent(compensation_id, IntentStatus.COMPLETED, now=NOW)

    for instrument_id, quantity, source_intent_id in (
        ("compat-instrument", "2", "basis-entry-a"),
        ("compat-instrument-2", "-2", "basis-entry-b"),
    ):
        repository.save_position_allocation(
            PositionAllocation(
                id=f"{source_intent_id}-allocation",
                account_id=account.id,
                instrument_id=instrument_id,
                ownership_class=OwnershipClass.MANAGED,
                signed_quantity=Decimal(quantity),
                strategy_id="compat-strategy",
                book_id="compat-book",
                source_intent_id=source_id,
                updated_at=NOW,
                metadata={"provenance": "partial-exit-test-basis"},
            ),
            _validation_token=repository._allocation_validation_capability(),
        )

    source_fill = BrokerFill(
        external_order_id="partial-exit-source-external",
        dedupe_key="partial-exit-source-deal",
        quantity=Decimal("1"),
        price=Decimal("100"),
        filled_at=NOW,
        received_at=NOW,
        account_id=account.id,
        instrument_id="compat-instrument",
    )
    compensation_fill = BrokerFill(
        external_order_id="partial-exit-compensation-external",
        dedupe_key="partial-exit-compensation-deal",
        quantity=Decimal("1"),
        price=Decimal("101"),
        filled_at=NOW,
        received_at=NOW,
        account_id=account.id,
        instrument_id="compat-instrument",
    )
    facts = replace(
        _facts(ExecutionEvidenceMode.INDIVIDUAL_DEALS),
        positions=(
            PositionSnapshot(
                id="partial-exit-current-a",
                broker_snapshot_id="partial-exit-current",
                account_id=account.id,
                instrument_id="compat-instrument",
                signed_quantity=Decimal("2"),
                average_price=Decimal("100"),
                captured_at=NOW,
            ),
            PositionSnapshot(
                id="partial-exit-current-b",
                broker_snapshot_id="partial-exit-current",
                account_id=account.id,
                instrument_id="compat-instrument-2",
                signed_quantity=Decimal("-2"),
                average_price=Decimal("100"),
                captured_at=NOW,
            ),
        ),
        fills=(source_fill, compensation_fill),
    )
    adapter = _CompensatedPartialAdapter(
        (source_snapshot, compensation_snapshot),
        facts=facts,
    )
    oms = GenericOMS(repository, adapter, clock=lambda: NOW + timedelta(seconds=1))

    result = oms.resolve_compensated_partial_intent(source_id, account=account)

    assert result["status"] == IntentStatus.CANCELLED.value
    closure = repository.get_intent(source_id)["metadata"]["compensated_partial_closure"]
    assert closure["reason"] == "fresh_broker_and_historical_proof_of_compensated_partial_exit"
    assert closure["compensating_intent_ids"] == [compensation_id]


def test_compensated_partial_ignores_definite_no_submit_retry_link(tmp_path):
    repository = _repository(tmp_path)
    entry, _exit, snapshots = _seed_compensated_partial(repository)
    stale_id = _seed_stale_compensating_link(repository, entry)
    oms = GenericOMS(
        repository,
        _CompensatedPartialAdapter(snapshots),
        clock=lambda: NOW + timedelta(seconds=1),
    )

    result = oms.resolve_compensated_partial_intent(entry.id, account=_account())

    assert result["status"] == IntentStatus.CANCELLED.value
    assert result["compensating_external_order_ids"] == ["compensated-exit-external-a"]
    assert repository.get_intent(stale_id)["status"] == IntentStatus.REJECTED.value


def test_compensated_partial_rejects_linked_nonterminal_external_retry(tmp_path):
    repository = _repository(tmp_path)
    entry, _exit, snapshots = _seed_compensated_partial(repository)
    _seed_stale_compensating_link(repository, entry, nonterminal=True)
    oms = GenericOMS(
        repository,
        _CompensatedPartialAdapter(snapshots),
        clock=lambda: NOW + timedelta(seconds=1),
    )

    with pytest.raises(OMSExecutionError, match="broker attempt is not terminal FILLED"):
        oms.resolve_compensated_partial_intent(entry.id, account=_account())
    assert repository.get_intent(entry.id)["status"] == IntentStatus.RECONCILIATION_REQUIRED.value


def test_compensated_partial_entry_accepts_provider_capture_after_query_start(tmp_path):
    repository = _repository(tmp_path)
    entry, _exit, snapshots = _seed_compensated_partial(repository)
    facts = replace(
        _facts(ExecutionEvidenceMode.INDIVIDUAL_DEALS),
        captured_at=NOW + timedelta(seconds=1, milliseconds=25),
    )
    oms = GenericOMS(
        repository,
        _CompensatedPartialAdapter(snapshots, facts=facts),
        # The provider capture is intentionally 25 ms after this clock value;
        # it is a valid in-query observation, not a materially future fact.
        clock=lambda: NOW + timedelta(seconds=1),
    )

    result = oms.resolve_compensated_partial_intent(entry.id, account=_account())

    assert result["status"] == IntentStatus.CANCELLED.value


def test_compensated_partial_entry_rejects_materially_future_provider_capture(tmp_path):
    repository = _repository(tmp_path)
    entry, _exit, snapshots = _seed_compensated_partial(repository)
    facts = replace(
        _facts(ExecutionEvidenceMode.INDIVIDUAL_DEALS),
        captured_at=NOW + timedelta(seconds=6),
    )
    oms = GenericOMS(
        repository,
        _CompensatedPartialAdapter(snapshots, facts=facts),
        clock=lambda: NOW,
    )

    with pytest.raises(OMSExecutionError, match="authoritative account facts are stale"):
        oms.resolve_compensated_partial_intent(entry.id, account=_account())
    assert repository.get_intent(entry.id)["status"] == IntentStatus.RECONCILIATION_REQUIRED.value


def test_compensated_partial_entry_rejects_stale_provider_capture(tmp_path):
    repository = _repository(tmp_path)
    entry, _exit, snapshots = _seed_compensated_partial(repository)
    facts = replace(
        _facts(ExecutionEvidenceMode.INDIVIDUAL_DEALS),
        captured_at=NOW - timedelta(seconds=61),
    )
    oms = GenericOMS(
        repository,
        _CompensatedPartialAdapter(snapshots, facts=facts),
        clock=lambda: NOW,
    )

    with pytest.raises(OMSExecutionError, match="authoritative account facts are stale"):
        oms.resolve_compensated_partial_intent(entry.id, account=_account())
    assert repository.get_intent(entry.id)["status"] == IntentStatus.RECONCILIATION_REQUIRED.value


def test_compensated_partial_entry_stays_recon_when_history_coverage_is_missing(tmp_path):
    repository = _repository(tmp_path)
    entry, _exit, snapshots = _seed_compensated_partial(repository)
    oms = GenericOMS(repository, _CompensatedPartialAdapter(snapshots[:1]), clock=lambda: NOW + timedelta(seconds=1))

    with pytest.raises(OMSExecutionError, match="historical order coverage"):
        oms.resolve_compensated_partial_intent(entry.id, account=_account())
    assert repository.get_intent(entry.id)["status"] == IntentStatus.RECONCILIATION_REQUIRED.value


def test_compensated_partial_entry_stays_recon_on_fresh_nonflat_account(tmp_path):
    repository = _repository(tmp_path)
    entry, _exit, snapshots = _seed_compensated_partial(repository)
    facts = replace(
        _facts(ExecutionEvidenceMode.INDIVIDUAL_DEALS),
        positions=(
            PositionSnapshot(
                id="proof-nonflat-position",
                broker_snapshot_id="proof-current",
                account_id=_account().id,
                instrument_id="compat-instrument",
                signed_quantity=Decimal("1"),
                captured_at=NOW,
            ),
        ),
    )
    oms = GenericOMS(repository, _CompensatedPartialAdapter(snapshots, facts=facts), clock=lambda: NOW + timedelta(seconds=1))

    with pytest.raises(OMSExecutionError, match="freshly flat"):
        oms.resolve_compensated_partial_intent(entry.id, account=_account())
    assert repository.get_intent(entry.id)["status"] == IntentStatus.RECONCILIATION_REQUIRED.value


def test_compensated_partial_entry_stays_recon_on_fresh_open_order(tmp_path):
    repository = _repository(tmp_path)
    entry, _exit, snapshots = _seed_compensated_partial(repository)
    open_order = BrokerOrderSnapshot(
        id="proof-open-order",
        broker_snapshot_id="proof-current",
        account_id=_account().id,
        instrument_id="compat-instrument-2",
        external_order_id="unrelated-open-order",
        side=Side.SELL,
        quantity=Decimal("1"),
        filled_quantity=Decimal("0"),
        status=BrokerOrderStatus.WORKING,
        captured_at=NOW,
    )
    facts = replace(
        _facts(ExecutionEvidenceMode.INDIVIDUAL_DEALS),
        open_orders=(open_order,),
    )
    oms = GenericOMS(
        repository,
        _CompensatedPartialAdapter(snapshots, facts=facts),
        clock=lambda: NOW + timedelta(seconds=1),
    )

    with pytest.raises(OMSExecutionError, match="freshly flat"):
        oms.resolve_compensated_partial_intent(entry.id, account=_account())
    assert repository.get_intent(entry.id)["status"] == IntentStatus.RECONCILIATION_REQUIRED.value


def test_compensated_partial_entry_rejects_foreign_claim_for_exact_intent(tmp_path):
    repository = _repository(tmp_path)
    entry, _exit, snapshots = _seed_compensated_partial(repository)
    foreign = replace(snapshots[0], id="foreign-claim", external_order_id="foreign-order", client_order_id=entry.id)
    oms = GenericOMS(repository, _CompensatedPartialAdapter((*snapshots, foreign)), clock=lambda: NOW + timedelta(seconds=1))

    with pytest.raises(OMSExecutionError, match="unmatched claim"):
        oms.resolve_compensated_partial_intent(entry.id, account=_account())
    assert repository.get_intent(entry.id)["status"] == IntentStatus.RECONCILIATION_REQUIRED.value


def test_account_fact_gate_accepts_durable_fill_with_repository_instrument_metadata(tmp_path):
    repository = _repository(tmp_path)
    intent = _intent()
    repository.create_intent(intent)
    repository.transition_intent(intent.id, IntentStatus.RISK_APPROVED, now=NOW)
    repository.transition_intent(intent.id, IntentStatus.SUBMITTING, now=NOW)
    repository.transition_leg(intent.legs[0].id, LegStatus.SUBMITTING, now=NOW)
    repository.transition_leg(intent.legs[0].id, LegStatus.WORKING, now=NOW)
    repository.create_broker_order(
        broker_order_id="compat-broker-order",
        order_leg_id=intent.legs[0].id,
        account_id=_account().id,
        broker=_account().broker,
        attempt_number=1,
        client_order_id="compat-client-order",
        submitted_quantity=Decimal("1"),
        now=NOW,
    )
    repository.transition_broker_order("compat-broker-order", "SUBMITTING", now=NOW)
    repository.record_submission(
        "compat-broker-order",
        status="FILLED",
        external_order_id="compat-external-order",
        metadata={"provider_status": "FILLED_ALL"},
        now=NOW,
    )
    repository.record_fill(
        Fill(
            id="compat-fill",
            broker_order_id="compat-broker-order",
            order_leg_id=intent.legs[0].id,
            dedupe_key="compat-fill",
            quantity=Decimal("1"),
            price=Decimal("101"),
            filled_at=NOW,
            received_at=NOW,
            account_id=_account().id,
            external_order_id="compat-external-order",
            evidence_reference="compat-fill",
            metadata={"source": "offline-test"},
        ),
        now=NOW,
        _validation_token=repository._fill_validation_capability(),
    )
    facts = replace(
        _facts(
            ExecutionEvidenceMode.INDIVIDUAL_DEALS,
            positions=(
                PositionSnapshot(
                    id="compat-position",
                    broker_snapshot_id="compat-position-snapshot",
                    account_id=_account().id,
                    instrument_id="compat-instrument",
                    signed_quantity=Decimal("1"),
                    captured_at=NOW,
                ),
            ),
        ),
        fills=(
            BrokerFill(
                external_order_id="compat-external-order",
                external_fill_id=None,
                dedupe_key="compat-fill",
                quantity=Decimal("1"),
                price=Decimal("101"),
                filled_at=NOW,
                received_at=NOW,
                account_id=_account().id,
                evidence_reference="compat-external-order:compat-fill",
                metadata={"source": "offline-test"},
                instrument_id="compat-instrument",
            ),
        ),
    )
    oms = GenericOMS(repository, _FactsAdapter(facts), clock=lambda: NOW)

    assert oms._account_wide_broker_fact_gate(intent.id, _account()) is True


def test_legacy_import_is_flat_distinct_and_idempotent(tmp_path):
    source = "data/generic-sim-smoke.db"
    target = _repository(tmp_path)
    before = sqlite3.connect(source).execute("SELECT COUNT(*) FROM core_broker_orders").fetchone()[0]

    first = target.import_legacy_order_evidence(source, _account().id)
    second = target.import_legacy_order_evidence(source, _account().id)

    assert first["source_ledger_fingerprint"] == second["source_ledger_fingerprint"]
    assert first["reused_existing_graph"] is False
    assert second["reused_existing_graph"] is True
    assert first["source_order_ids"] == ("3408387", "3408388", "3408464", "3408465")
    assert len(target.broker_orders_for_external_order_id("3408387")) == 1
    assert target.broker_orders_for_external_order_id("3408387")[0]["id"].startswith("legacy-smoke:order:")
    assert {row["book_id"] for row in target.position_allocations(_account().id)} == {
        first["legacy_book_id"]
    }
    assert sqlite3.connect(source).execute("SELECT COUNT(*) FROM core_broker_orders").fetchone()[0] == before


def _legacy_graph_counts(repository):
    with repository.transaction() as connection:
        return tuple(
            connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in (
                "core_strategies",
                "core_books",
                "core_order_intents",
                "core_order_legs",
                "core_broker_orders",
                "core_fills",
                "core_position_allocations",
            )
        )


def test_legacy_import_reuses_exact_graph_across_labels_and_baseline_checkpoint(tmp_path):
    target, first, baseline = _imported_retired_target(tmp_path)
    before = _legacy_graph_counts(target)

    reused = target.import_legacy_order_evidence(
        "data/generic-sim-smoke.db",
        _account().id,
        legacy_label="second-label",
    )

    assert reused["reused_existing_graph"] is True
    assert reused["legacy_strategy_id"] == first["legacy_strategy_id"]
    assert reused["legacy_book_id"] == first["legacy_book_id"]
    assert reused["imported_intents"] == first["imported_intents"]
    assert reused["imported_legs"] == first["imported_legs"]
    assert reused["imported_orders"] == first["imported_orders"]
    assert reused["imported_fills"] == first["imported_fills"]
    assert reused["imported_allocations"] == first["imported_allocations"]
    assert _legacy_graph_counts(target) == before
    assert target.verified_retired_book_closure(
        _account().id,
        baseline.metadata["legacy_book_id"],
        baseline,
    )[0] is True


def test_legacy_import_rejects_partial_existing_external_claim(tmp_path):
    target = _repository(tmp_path)
    target.create_intent(_intent())
    target.create_broker_order(
        broker_order_id="current-broker-order",
        order_leg_id="compat-leg",
        account_id=_account().id,
        broker="moomoo",
        attempt_number=1,
        client_order_id="current-client-order",
        submitted_quantity=Decimal("1"),
        now=NOW,
    )
    target.record_submission(
        "current-broker-order",
        status=BrokerOrderStatus.SUBMITTING,
        external_order_id="3408387",
        metadata={"current": True},
        now=NOW,
    )

    with pytest.raises(ValueError, match="partially overlaps"):
        target.import_legacy_order_evidence("data/generic-sim-smoke.db", _account().id)


@pytest.mark.parametrize(
    ("target_status", "reuses"),
    (
        (IntentStatus.FILLED.value, True),
        (IntentStatus.COMPLETED.value, True),
        (IntentStatus.WORKING.value, False),
        (IntentStatus.CANCELLED.value, False),
        (IntentStatus.REJECTED.value, False),
        (IntentStatus.FAILED.value, False),
    ),
)
def test_legacy_import_status_compatibility_is_narrow(tmp_path, target_status, reuses):
    target = _repository(tmp_path / target_status)
    first = target.import_legacy_order_evidence("data/generic-sim-smoke.db", _account().id)
    before = _legacy_graph_counts(target)
    with target.transaction() as connection:
        intent_id = connection.execute(
            """SELECT i.id FROM core_order_intents i
                 JOIN core_order_legs l ON l.intent_id = i.id
                 JOIN core_broker_orders b ON b.order_leg_id = l.id
                WHERE b.external_order_id = ?""",
            ("3408387",),
        ).fetchone()[0]
        connection.execute(
            "UPDATE core_order_intents SET status = ? WHERE id = ?",
            (target_status, intent_id),
        )

    if reuses:
        reused = target.import_legacy_order_evidence(
            "data/generic-sim-smoke.db",
            _account().id,
            legacy_label=f"status-{target_status}",
        )
        assert reused["reused_existing_graph"] is True
        assert reused["legacy_strategy_id"] == first["legacy_strategy_id"]
        assert _legacy_graph_counts(target) == before
    else:
        with pytest.raises(ValueError, match="existing legacy intent evidence conflicts"):
            target.import_legacy_order_evidence(
                "data/generic-sim-smoke.db",
                _account().id,
                legacy_label=f"status-{target_status}",
            )
        assert _legacy_graph_counts(target) == before


def test_legacy_import_does_not_accept_completed_to_filled(tmp_path):
    source = tmp_path / "completed-source.db"
    shutil.copyfile("data/generic-sim-smoke.db", source)
    with sqlite3.connect(source) as connection:
        connection.execute(
            """UPDATE core_order_intents
                  SET status = 'COMPLETED'
                WHERE id = 'generic-sim-smoke-entry-aapl-msft-a5a28459897a6b4e'"""
        )

    target = _repository(tmp_path / "completed-target")
    target.import_legacy_order_evidence(source, _account().id)
    with target.transaction() as connection:
        intent_id = connection.execute(
            """SELECT i.id FROM core_order_intents i
                 JOIN core_order_legs l ON l.intent_id = i.id
                 JOIN core_broker_orders b ON b.order_leg_id = l.id
                WHERE b.external_order_id = ?""",
            ("3408387",),
        ).fetchone()[0]
        connection.execute(
            "UPDATE core_order_intents SET status = 'FILLED' WHERE id = ?",
            (intent_id,),
        )

    with pytest.raises(ValueError, match="existing legacy intent evidence conflicts"):
        target.import_legacy_order_evidence(
            source,
            _account().id,
            legacy_label="completed-to-filled",
        )


@pytest.mark.parametrize(
    "mutation",
    (
        "nonlegacy",
        "instrument",
        "side",
        "quantity",
        "fill_quantity",
        "fill_price",
        "fill_timestamp",
        "status",
        "incomplete_fill",
    ),
)
def test_legacy_import_rejects_conflicting_existing_graph(tmp_path, mutation):
    target = _repository(tmp_path / mutation)
    target.import_legacy_order_evidence("data/generic-sim-smoke.db", _account().id)
    order = target.broker_orders_for_external_order_id("3408387")[0]
    order_id = str(order["id"])
    leg_id = str(order["order_leg_id"])
    with target.transaction() as connection:
        if mutation == "nonlegacy":
            metadata = dict(order["metadata"])
            metadata["legacy_import"] = False
            connection.execute(
                "UPDATE core_broker_orders SET metadata_json = ? WHERE id = ?",
                (json.dumps(metadata), order_id),
            )
        elif mutation == "instrument":
            connection.execute(
                "UPDATE core_order_legs SET instrument_id = ? WHERE id = ?",
                ("compat-instrument", leg_id),
            )
        elif mutation == "side":
            current = connection.execute(
                "SELECT side FROM core_order_legs WHERE id = ?", (leg_id,)
            ).fetchone()[0]
            connection.execute(
                "UPDATE core_order_legs SET side = ? WHERE id = ?",
                ("SELL" if str(current).upper() == "BUY" else "BUY", leg_id),
            )
        elif mutation == "quantity":
            connection.execute(
                "UPDATE core_broker_orders SET submitted_quantity = '2' WHERE id = ?",
                (order_id,),
            )
        elif mutation == "fill_quantity":
            connection.execute(
                "UPDATE core_fills SET quantity = '2' WHERE broker_order_id = ?",
                (order_id,),
            )
        elif mutation == "fill_price":
            connection.execute(
                "UPDATE core_fills SET price = '999' WHERE broker_order_id = ?",
                (order_id,),
            )
        elif mutation == "fill_timestamp":
            connection.execute(
                "UPDATE core_fills SET filled_at = '2030-01-01T00:00:00+00:00' WHERE broker_order_id = ?",
                (order_id,),
            )
        elif mutation == "status":
            connection.execute(
                "UPDATE core_broker_orders SET status = 'WORKING' WHERE id = ?",
                (order_id,),
            )
        elif mutation == "incomplete_fill":
            connection.execute("PRAGMA foreign_keys = OFF")
            connection.execute("DELETE FROM core_fills WHERE broker_order_id = ?", (order_id,))

    with pytest.raises(ValueError, match="existing legacy|retired"):
        target.import_legacy_order_evidence(
            "data/generic-sim-smoke.db",
            _account().id,
            legacy_label="conflicting-label",
        )


def test_legacy_import_rejects_source_fingerprint_change(tmp_path):
    target = _repository(tmp_path / "target")
    target.import_legacy_order_evidence("data/generic-sim-smoke.db", _account().id)
    source = tmp_path / "changed-source.db"
    shutil.copyfile("data/generic-sim-smoke.db", source)
    with sqlite3.connect(source) as connection:
        row = connection.execute(
            "SELECT id, metadata_json FROM core_fills ORDER BY id LIMIT 1"
        ).fetchone()
        metadata = json.loads(row[1])
        metadata["raw"]["offline_fingerprint_change"] = True
        connection.execute(
            "UPDATE core_fills SET metadata_json = ? WHERE id = ?",
            (json.dumps(metadata), row[0]),
        )

    with pytest.raises(ValueError, match="existing legacy"):
        target.import_legacy_order_evidence(
            source,
            _account().id,
            legacy_label="changed-source-label",
        )


def test_legacy_import_preserves_external_order_uniqueness(tmp_path):
    target = _repository(tmp_path)
    imported = target.import_legacy_order_evidence("data/generic-sim-smoke.db", _account().id)
    existing = target.broker_orders_for_external_order_id(imported["source_order_ids"][0])[0]

    with target.transaction() as connection:
        ddl = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'core_broker_orders'"
        ).fetchone()[0]
        assert "UNIQUE (account_id, external_order_id)" in ddl

    with pytest.raises(sqlite3.IntegrityError):
        with target.transaction() as connection:
            connection.execute(
                """INSERT INTO core_broker_orders
                   (id, order_leg_id, account_id, broker, attempt_number, external_order_id,
                    client_order_id, status, submitted_quantity, submitted_at, updated_at,
                    replaces_broker_order_id, metadata_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)""",
                (
                    "duplicate-external-order",
                    existing["order_leg_id"],
                    _account().id,
                    "moomoo",
                    99,
                    existing["external_order_id"],
                    "duplicate-client-order",
                    "FILLED",
                    "1",
                    existing["submitted_at"],
                    existing["updated_at"],
                    "{}",
                ),
            )


def test_legacy_import_rejects_source_account_mismatch(tmp_path):
    source = tmp_path / "foreign.db"
    shutil.copyfile("data/generic-sim-smoke.db", source)
    with sqlite3.connect(source) as connection:
        connection.execute(
            "UPDATE core_accounts SET external_account_id = 'different-account' WHERE id = ?",
            (_account().id,),
        )
    target = _repository(tmp_path / "target")
    with pytest.raises(ValueError, match="account identity"):
        target.import_legacy_order_evidence(source, _account().id)


def test_legacy_import_maps_source_symbols_to_configured_stage6_instruments(tmp_path):
    target = _repository(tmp_path)
    for instrument_id, symbol in (("stage6-aapl", "AAPL"), ("stage6-msft", "MSFT")):
        target.save_instrument(
            Instrument(
                id=instrument_id,
                asset_class=AssetClass.EQUITY,
                symbol=symbol,
                venue="US",
                currency="USD",
                created_at=NOW,
                updated_at=NOW,
            )
        )
        target.save_instrument_mapping(
            InstrumentMapping(
                id=f"mapping-{instrument_id}",
                instrument_id=instrument_id,
                provider="moomoo",
                purpose=MappingPurpose.BROKER,
                external_symbol=f"US.{symbol}",
                created_at=NOW,
                updated_at=NOW,
            )
        )

    imported = target.import_legacy_order_evidence(
        "data/generic-sim-smoke.db",
        _account().id,
        instrument_mapping={"AAPL": "stage6-aapl", "MSFT": "stage6-msft"},
    )
    assert imported["source_to_target_instrument_mapping"] == {
        "AAPL": "stage6-aapl",
        "MSFT": "stage6-msft",
    }
    with target.transaction() as connection:
        legs = connection.execute(
            "SELECT instrument_id, metadata_json FROM core_order_legs WHERE id LIKE 'legacy-smoke:leg:%'"
        ).fetchall()
    assert {str(row["instrument_id"]) for row in legs} == {"stage6-aapl", "stage6-msft"}
    assert all("legacy_source_instrument_id" in str(row["metadata_json"]) for row in legs)
    assert {row["instrument_id"] for row in target.position_allocations(_account().id)} == {
        "stage6-aapl",
        "stage6-msft",
    }


def test_legacy_import_rejects_missing_or_foreign_mapping_transactionally(tmp_path):
    target = _repository(tmp_path)
    with pytest.raises(ValueError, match="cover exactly"):
        target.import_legacy_order_evidence(
            "data/generic-sim-smoke.db",
            _account().id,
            instrument_mapping={"AAPL": "foreign-instrument"},
        )
    with target.transaction() as connection:
        assert connection.execute("SELECT COUNT(*) FROM core_strategies").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM core_order_intents").fetchone()[0] == 0


def test_verified_retired_book_closure_allows_imported_flat_history(tmp_path):
    target, imported, baseline = _imported_retired_target(tmp_path)

    verified, reason = target.verified_retired_book_closure(
        _account().id,
        imported["legacy_book_id"],
        baseline,
    )

    assert verified is True
    assert "terminal" in reason


def _seed_retired_history_blockers(target, account):
    """Create one exact aged-history chain for wrapper-link regression tests."""
    book_id = target.latest_execution_evidence_baseline(account.id).metadata["legacy_book_id"]
    order = target.book_broker_orders(account.id, book_id=book_id)[0]
    intent_id = str(order["intent_id"])
    oms = GenericOMS(target, object(), clock=lambda: NOW)
    oms._require_reconciliation(
        intent_id,
        account,
        category="TERMINAL_HISTORY_EXPIRED",
        entity_type="BROKER_ORDER",
        entity_key=str(order["id"]),
        details={"broker_order_id": str(order["id"])},
    )
    oms._record_recovery_action(
        intent=target.get_intent(intent_id),
        account=account,
        action_key=f"TERMINAL_HISTORY_EXPIRED:{order['id']}",
        state="RECONCILIATION_REQUIRED",
        summary="test aged-history blocker",
        observed_positions={},
        remaining_quantities={},
        metadata={"broker_order_id": str(order["id"])},
    )
    issue = next(
        item
        for item in target.open_reconciliation_issues(account.id)
        if item["category"] == "TERMINAL_HISTORY_EXPIRED"
    )
    action = next(
        item
        for item in target.open_recovery_actions(account.id)
        if item["action_key"] == f"TERMINAL_HISTORY_EXPIRED:{order['id']}"
    )
    baseline = target.latest_execution_evidence_baseline(account.id)
    return oms, baseline, order, intent_id, issue, action


def _seed_account_wrapper(oms, account, *, issue_links, action_links, baseline, intent_id):
    oms._require_reconciliation(
        intent_id,
        account,
        category="ACCOUNT_RECONCILIATION_BLOCK",
        entity_type="ACCOUNT",
        entity_key=account.id,
        details={
            "causal_account_id": account.id,
            "causal_intent_ids": [intent_id],
            "causal_reconciliation_issue_links": issue_links,
            "causal_recovery_action_links": action_links,
            "causal_baseline_id": baseline.id,
            "causal_source_ledger_fingerprint": baseline.source_ledger_fingerprint,
        },
    )


def test_unlinked_account_wrapper_does_not_bypass_retired_baseline(tmp_path):
    target, imported, baseline = _imported_retired_target(tmp_path)
    account = _account()
    oms, _baseline, _order, intent_id, issue, action = _seed_retired_history_blockers(target, account)
    oms._require_reconciliation(
        intent_id,
        account,
        category="ACCOUNT_RECONCILIATION_BLOCK",
        entity_type="ACCOUNT",
        entity_key=account.id,
        details={"open_issue_count": 1, "open_recovery_action_count": 1},
    )

    verified, reason = target.verified_retired_book_closure(
        account.id,
        imported["legacy_book_id"],
        baseline,
        allow_retired_baseline_blockers=True,
    )

    assert verified is False
    assert "unrelated reconciliation blocker" in reason
    assert any(item["category"] == "ACCOUNT_RECONCILIATION_BLOCK" for item in target.open_reconciliation_issues(account.id))


def test_explicitly_linked_account_wrapper_resolves_only_retired_chain(tmp_path):
    target, _imported, _baseline = _imported_retired_target(tmp_path)
    account = _account()
    oms, baseline, order, intent_id, issue, action = _seed_retired_history_blockers(target, account)
    _seed_account_wrapper(
        oms,
        account,
        intent_id=intent_id,
        baseline=baseline,
        issue_links=[{"id": issue["id"], "issue_key": issue["issue_key"]}],
        action_links=[
            {
                "id": action["id"],
                "intent_id": action["intent_id"],
                "action_key": action["action_key"],
            }
        ],
    )

    result = oms.resolve_verified_retired_baseline(account=account)

    assert result["retry_requires_new_cycle"] is True
    assert order["id"] in result["broker_order_ids"]
    assert target.open_reconciliation_issues(account.id) == []
    assert target.open_recovery_actions(account.id) == []


def test_retired_resolver_restores_only_proven_quarantined_imports(tmp_path):
    target, imported, baseline = _imported_retired_target(tmp_path)
    account = _account()
    untouched = _intent()
    target.create_intent(untouched)
    oms = GenericOMS(target, _FactsAdapter(_facts(ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS)), clock=lambda: NOW)

    retired_intents = target.book_intents(account.id, book_id=imported["legacy_book_id"])
    for row in retired_intents:
        target.transition_intent(row["id"], IntentStatus.RECONCILIATION_REQUIRED, now=NOW)
    for order in target.book_broker_orders(account.id, book_id=imported["legacy_book_id"]):
        oms._require_reconciliation(
            str(order["intent_id"]),
            account,
            category="TERMINAL_HISTORY_EXPIRED",
            entity_type="BROKER_ORDER",
            entity_key=str(order["id"]),
            details={"broker_order_id": str(order["id"])},
        )

    result = oms.resolve_verified_retired_baseline(account=account)

    assert set(result["restored_intent_ids"]) == {str(row["id"]) for row in retired_intents}
    assert {
        row["status"] for row in target.book_intents(account.id, book_id=imported["legacy_book_id"])
    } == {IntentStatus.COMPLETED.value}
    assert target.get_intent(untouched.id)["status"] == IntentStatus.CREATED.value


def test_malformed_account_wrapper_with_valid_causal_links_stays_blocking(tmp_path):
    target, imported, baseline = _imported_retired_target(tmp_path)
    account = _account()
    oms, _baseline, order, intent_id, issue, action = _seed_retired_history_blockers(target, account)
    oms._require_reconciliation(
        intent_id,
        account,
        category="ACCOUNT_RECONCILIATION_BLOCK",
        entity_type="BROKER_ORDER",
        entity_key=str(order["id"]),
        details={
            "causal_account_id": account.id,
            "causal_intent_ids": [intent_id],
            "causal_reconciliation_issue_links": [
                {"id": issue["id"], "issue_key": issue["issue_key"]}
            ],
            "causal_recovery_action_links": [
                {
                    "id": action["id"],
                    "intent_id": action["intent_id"],
                    "action_key": action["action_key"],
                }
            ],
            "causal_baseline_id": baseline.id,
            "causal_source_ledger_fingerprint": baseline.source_ledger_fingerprint,
        },
    )

    verified, reason = target.verified_retired_book_closure(
        account.id,
        imported["legacy_book_id"],
        baseline,
        allow_retired_baseline_blockers=True,
    )

    assert verified is False
    assert "unrelated reconciliation blocker" in reason
    assert any(
        item["issue_key"] == f"ACCOUNT_RECONCILIATION_BLOCK:BROKER_ORDER:{order['id']}"
        for item in target.open_reconciliation_issues(account.id)
    )


def test_account_wrapper_action_must_own_a_causal_retired_intent(tmp_path):
    target, imported, baseline = _imported_retired_target(tmp_path)
    account = _account()
    oms, _baseline, _order, intent_id, issue, action = _seed_retired_history_blockers(target, account)
    other_intent_id = next(
        row["id"]
        for row in target.book_intents(account.id, book_id=imported["legacy_book_id"])
        if row["id"] != intent_id
    )
    oms._record_recovery_action(
        intent=target.get_intent(intent_id),
        account=account,
        action_key=f"ACCOUNT_RECONCILIATION_BLOCK:ACCOUNT:{account.id}",
        state="RECONCILIATION_REQUIRED",
        summary="malformed wrapper action",
        observed_positions={},
        remaining_quantities={},
        metadata={
            "causal_account_id": account.id,
            "causal_intent_ids": [other_intent_id],
            "causal_reconciliation_issue_links": [
                {"id": issue["id"], "issue_key": issue["issue_key"]}
            ],
            "causal_recovery_action_links": [
                {
                    "id": action["id"],
                    "intent_id": action["intent_id"],
                    "action_key": action["action_key"],
                }
            ],
            "causal_baseline_id": baseline.id,
            "causal_source_ledger_fingerprint": baseline.source_ledger_fingerprint,
        },
    )

    verified, reason = target.verified_retired_book_closure(
        account.id,
        imported["legacy_book_id"],
        baseline,
        allow_retired_baseline_blockers=True,
    )

    assert verified is False
    assert "unrelated recovery blocker" in reason


def test_mixed_account_wrapper_links_remain_blocking(tmp_path):
    target, imported, baseline = _imported_retired_target(tmp_path)
    account = _account()
    oms, _baseline, _order, intent_id, issue, action = _seed_retired_history_blockers(target, account)
    _seed_account_wrapper(
        oms,
        account,
        intent_id=intent_id,
        baseline=baseline,
        issue_links=[
            {"id": issue["id"], "issue_key": issue["issue_key"]},
            {"id": "foreign-issue", "issue_key": "ACCOUNT_RECONCILIATION_BLOCK:ACCOUNT:foreign"},
        ],
        action_links=[
            {
                "id": action["id"],
                "intent_id": action["intent_id"],
                "action_key": action["action_key"],
            }
        ],
    )

    verified, reason = target.verified_retired_book_closure(
        account.id,
        imported["legacy_book_id"],
        baseline,
        allow_retired_baseline_blockers=True,
    )

    assert verified is False
    assert "unrelated reconciliation blocker" in reason


@pytest.mark.parametrize("mutation, expected", [
    ("nonzero", "not flat"),
    ("unknown", "unknown or unmanaged"),
    ("uncovered", "outside"),
    ("open", "non-terminal"),
])
def test_retired_book_closure_rejects_unverified_historical_rows(tmp_path, mutation, expected):
    target, imported, baseline = _imported_retired_target(tmp_path)
    if mutation == "nonzero":
        with target.transaction() as connection:
            connection.execute(
                "UPDATE core_position_allocations SET signed_quantity = '2' WHERE id = (SELECT id FROM core_position_allocations WHERE book_id = ? ORDER BY id LIMIT 1)",
                (imported["legacy_book_id"],),
            )
    elif mutation == "unknown":
        with target.transaction() as connection:
            connection.execute(
                "UPDATE core_position_allocations SET ownership_class = 'UNKNOWN' WHERE id = (SELECT id FROM core_position_allocations WHERE book_id = ? ORDER BY id LIMIT 1)",
                (imported["legacy_book_id"],),
            )
    elif mutation == "uncovered":
        baseline = replace(baseline, source_order_ids=(baseline.source_order_ids[0],))
    elif mutation == "open":
        with target.transaction() as connection:
            connection.execute(
                "UPDATE core_broker_orders SET status = 'WORKING' WHERE id = (SELECT id FROM core_broker_orders WHERE id LIKE 'legacy-smoke:order:%' ORDER BY id LIMIT 1)"
            )

    verified, reason = target.verified_retired_book_closure(
        _account().id,
        imported["legacy_book_id"],
        baseline,
    )

    assert verified is False
    assert expected.lower() in reason.lower()
