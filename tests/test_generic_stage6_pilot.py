from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest

from src.strategies.stat_arb.stage5_sleeves import (
    Clean40AllocationUpdate,
    NormalizedPairTarget,
    PairSleeve,
    SleeveAllocationTarget,
)
from src.strategies.stat_arb.stage6_pilot import (
    STAGE6_EXECUTION_COMPATIBILITY,
    Stage6PilotRunner,
    Stage6PilotSpec,
    Stage6RunMode,
)
from src.trading_core.domain import (
    ADAPTER_SUBMISSION_ACK_AUTHORITY,
    Account,
    AssetClass,
    Book,
    BrokerOrderSnapshot,
    BrokerOrderStatus,
    ExecutionEvidenceBaseline,
    ExecutionEvidenceMode,
    Instrument,
    IntentAction,
    IntentStatus,
    OwnershipClass,
    PositionAllocation,
    PositionSnapshot,
    Side,
    Strategy,
    TradingEnvironment,
)
from src.trading_core.oms import GenericOMS
from src.trading_core.ports import BrokerFactSnapshot, BrokerFill, BrokerSubmissionResult
from src.trading_core.repository import SQLiteTradingRepository


NOW = datetime(2026, 9, 20, 12, 0, tzinfo=timezone.utc)


def make_account(*, environment: TradingEnvironment = TradingEnvironment.SIM) -> Account:
    return Account(
        id="pilot-account",
        broker="fake",
        environment=environment,
        external_account_id="pilot-external",
        base_currency="USD",
        metadata={"allocation_capacity": "20", "account_capacity": "20"},
        created_at=NOW,
        updated_at=NOW,
    )


def make_sleeves(account_id: str = "pilot-account") -> tuple[PairSleeve, PairSleeve]:
    return (
        PairSleeve(
            sleeve_id="pilot-sleeve-a",
            strategy_id="pilot-strategy",
            account_id=account_id,
            book_id="pilot-book-a",
            name="Pilot A",
            instrument_ids=("pilot-a-1", "pilot-a-2"),
            symbols=("PILOT_A1", "PILOT_A2"),
            configuration={"signal_source": "test-driver"},
        ),
        PairSleeve(
            sleeve_id="pilot-sleeve-b",
            strategy_id="pilot-strategy",
            account_id=account_id,
            book_id="pilot-book-b",
            name="Pilot B",
            instrument_ids=("pilot-b-1", "pilot-b-2"),
            symbols=("PILOT_B1", "PILOT_B2"),
            configuration={"signal_source": "test-driver"},
        ),
    )


def make_targets(
    sleeves: tuple[PairSleeve, PairSleeve],
    *,
    quantities: tuple[str, str] = ("2", "2"),
    action: IntentAction = IntentAction.ENTER,
) -> tuple[NormalizedPairTarget, NormalizedPairTarget]:
    result = []
    for number, (sleeve, quantity) in enumerate(zip(sleeves, quantities, strict=True)):
        result.append(
            NormalizedPairTarget(
                sleeve_id=sleeve.sleeve_id,
                cycle_id=f"pilot-cycle-{number}",
                signal_id=f"pilot-signal-{number}",
                instrument_ids=sleeve.instrument_ids,
                signed_quantities=(Decimal(quantity), Decimal(f"-{quantity}")),
                action=action,
                evaluated_at=NOW,
                provenance={"source": "stage6-test-driver"},
            )
        )
    return tuple(result)  # type: ignore[return-value]


def make_spec(
    account: Account,
    sleeves: tuple[PairSleeve, PairSleeve],
    *,
    targets: tuple[NormalizedPairTarget, NormalizedPairTarget] | None = None,
    version: int = 1,
) -> Stage6PilotSpec:
    return Stage6PilotSpec(
        account=account,
        sleeves=sleeves,
        allocation_update=Clean40AllocationUpdate(
            account_id=account.id,
            version=version,
            targets=(
                SleeveAllocationTarget(sleeves[0].sleeve_id, sleeves[0].book_id, target_weight="0.50"),
                SleeveAllocationTarget(sleeves[1].sleeve_id, sleeves[1].book_id, target_weight="0.50"),
            ),
            effective_at=NOW,
            provenance="stage6-offline-test",
        ),
        targets=targets or make_targets(sleeves),
    )


class PilotFakeAdapter:
    def __init__(self, facts: BrokerFactSnapshot | None = None) -> None:
        self.facts = facts or BrokerFactSnapshot(
            account_id="pilot-account",
            captured_at=NOW,
            complete=True,
        )
        self.submit_calls: list[str] = []
        self.fact_calls = 0
        self.recovery_calls = 0
        self._filled_orders: dict[str, BrokerOrderSnapshot] = {}
        self._next_fill_price = Decimal("100")

    def get_authoritative_account_facts(self, account: Account) -> BrokerFactSnapshot:
        self.fact_calls += 1
        return self.facts

    def get_authoritative_market_state(self, symbols):
        return {
            "market": "US",
            "captured_at": NOW.isoformat(),
            "complete": True,
            "rows": [
                {"symbol": str(symbol), "market_state": "RTH"}
                for symbol in symbols
            ],
        }

    def get_positions(self, _account: Account):
        self.recovery_calls += 1
        return self.facts.positions

    def get_open_orders(self, _account: Account):
        self.recovery_calls += 1
        return self.facts.open_orders

    def get_order(self, _account: Account, external_order_id: str):
        self.recovery_calls += 1
        return self._filled_orders.get(str(external_order_id))

    def get_fills(self, _account: Account, since=None):
        self.recovery_calls += 1
        return self.facts.fills

    def submit_order(self, _account: Account, request) -> BrokerSubmissionResult:
        self.submit_calls.append(request.broker_order_id)
        external_order_id = f"pilot-order-{len(self.submit_calls)}"
        price = self._next_fill_price
        self._next_fill_price += Decimal("1")
        snapshot = BrokerOrderSnapshot(
            id=f"snapshot-{external_order_id}",
            broker_snapshot_id=f"snapshot-{external_order_id}",
            account_id=request.account_id,
            instrument_id=request.order_leg.instrument_id,
            external_order_id=external_order_id,
            client_order_id=request.client_order_id,
            side=request.order_leg.side,
            quantity=request.order_leg.quantity,
            filled_quantity=request.order_leg.quantity,
            status=BrokerOrderStatus.FILLED,
            captured_at=NOW,
            order_time=NOW,
        )
        self._filled_orders[external_order_id] = snapshot
        signed = request.order_leg.quantity if request.order_leg.side is Side.BUY else -request.order_leg.quantity
        position = PositionSnapshot(
            id=f"position-{external_order_id}",
            broker_snapshot_id=f"snapshot-{external_order_id}",
            account_id=request.account_id,
            instrument_id=request.order_leg.instrument_id,
            signed_quantity=signed,
            average_price=price,
            captured_at=NOW,
        )
        fill = BrokerFill(
            external_order_id=external_order_id,
            dedupe_key=f"deal-{external_order_id}",
            quantity=request.order_leg.quantity,
            price=price,
            filled_at=NOW,
            received_at=NOW,
            account_id=request.account_id,
            instrument_id=request.order_leg.instrument_id,
        )
        self.facts = replace(
            self.facts,
            positions=tuple((*self.facts.positions, position)),
            fills=tuple((*self.facts.fills, fill)),
        )
        return BrokerSubmissionResult(
            broker_order_id=request.broker_order_id,
            accepted=True,
            status=BrokerOrderStatus.WORKING,
            external_order_id=external_order_id,
            client_order_id=request.client_order_id,
            submitted_at=NOW,
        )


class MoomooAckPilotAdapter(PilotFakeAdapter):
    """Fake the exact normalized result produced by a Moomoo submit ACK."""

    def submit_order(self, _account: Account, request) -> BrokerSubmissionResult:
        self.submit_calls.append(request.broker_order_id)
        external_order_id = f"pilot-moomoo-order-{len(self.submit_calls)}"
        price = self._next_fill_price
        self._next_fill_price += Decimal("1")
        self._filled_orders[external_order_id] = BrokerOrderSnapshot(
            id=f"snapshot-{external_order_id}",
            broker_snapshot_id=f"snapshot-{external_order_id}",
            account_id=request.account_id,
            instrument_id=request.order_leg.instrument_id,
            external_order_id=external_order_id,
            client_order_id=request.client_order_id,
            side=request.order_leg.side,
            quantity=request.order_leg.quantity,
            filled_quantity=request.order_leg.quantity,
            status=BrokerOrderStatus.FILLED,
            captured_at=NOW,
            order_time=NOW,
        )
        signed = request.order_leg.quantity if request.order_leg.side is Side.BUY else -request.order_leg.quantity
        self.facts = replace(
            self.facts,
            positions=tuple((*self.facts.positions, PositionSnapshot(
                id=f"position-{external_order_id}",
                broker_snapshot_id=f"snapshot-{external_order_id}",
                account_id=request.account_id,
                instrument_id=request.order_leg.instrument_id,
                signed_quantity=signed,
                average_price=price,
                captured_at=NOW,
            ))),
            fills=tuple((*self.facts.fills, BrokerFill(
                external_order_id=external_order_id,
                dedupe_key=f"deal-{external_order_id}",
                quantity=request.order_leg.quantity,
                price=price,
                filled_at=NOW,
                received_at=NOW,
                account_id=request.account_id,
                instrument_id=request.order_leg.instrument_id,
            ))),
        )
        return BrokerSubmissionResult(
            broker_order_id=request.broker_order_id,
            accepted=True,
            status=BrokerOrderStatus.WORKING,
            external_order_id=external_order_id,
            client_order_id=request.client_order_id,
            submitted_at=NOW,
            cumulative_filled_quantity=Decimal("0"),
            no_fill_asserted=True,
            authority=ADAPTER_SUBMISSION_ACK_AUTHORITY,
            submitted_quantity=request.order_leg.quantity,
            instrument_id=request.order_leg.instrument_id,
            raw_payload={
                "response": [{
                    "code": request.order_leg.instrument_id,
                    "dealt_avg_price": 0.0,
                    "dealt_qty": 0.0,
                    "fill_outside_rth": False,
                    "order_id": external_order_id,
                    "order_status": "SUBMITTING",
                    "qty": float(request.order_leg.quantity),
                }],
            },
        )


class DelayedPilotAdapter(PilotFakeAdapter):
    """Keep accepted orders working until an injected recovery poll releases them."""

    def __init__(self, *, release_after_order_reads: int = 3) -> None:
        super().__init__()
        self.release_after_order_reads = release_after_order_reads
        self.order_reads = 0
        self.open_order_reads = 0
        self._pending_requests: dict[str, Any] = {}
        self._working_orders: dict[str, BrokerOrderSnapshot] = {}

    def submit_order(self, _account: Account, request) -> BrokerSubmissionResult:
        self.submit_calls.append(request.broker_order_id)
        external_order_id = f"delayed-order-{len(self.submit_calls)}"
        snapshot = BrokerOrderSnapshot(
            id=f"snapshot-{external_order_id}",
            broker_snapshot_id=f"snapshot-{external_order_id}",
            account_id=request.account_id,
            instrument_id=request.order_leg.instrument_id,
            external_order_id=external_order_id,
            client_order_id=request.client_order_id,
            side=request.order_leg.side,
            quantity=request.order_leg.quantity,
            filled_quantity=Decimal("0"),
            status=BrokerOrderStatus.WORKING,
            captured_at=NOW,
            order_time=NOW,
        )
        self._pending_requests[external_order_id] = request
        self._working_orders[external_order_id] = snapshot
        return BrokerSubmissionResult(
            broker_order_id=request.broker_order_id,
            accepted=True,
            status=BrokerOrderStatus.WORKING,
            external_order_id=external_order_id,
            client_order_id=request.client_order_id,
            submitted_at=NOW,
        )

    def _release_pending(self) -> None:
        for external_order_id, request in tuple(self._pending_requests.items()):
            if external_order_id in self._filled_orders:
                continue
            price = self._next_fill_price
            self._next_fill_price += Decimal("1")
            self._filled_orders[external_order_id] = replace(
                self._working_orders[external_order_id],
                filled_quantity=request.order_leg.quantity,
                status=BrokerOrderStatus.FILLED,
            )
            signed = request.order_leg.quantity if request.order_leg.side is Side.BUY else -request.order_leg.quantity
            position = PositionSnapshot(
                id=f"position-{external_order_id}",
                broker_snapshot_id=f"snapshot-{external_order_id}",
                account_id=request.account_id,
                instrument_id=request.order_leg.instrument_id,
                signed_quantity=signed,
                average_price=price,
                captured_at=NOW,
            )
            fill = BrokerFill(
                external_order_id=external_order_id,
                dedupe_key=f"deal-{external_order_id}",
                quantity=request.order_leg.quantity,
                price=price,
                filled_at=NOW,
                received_at=NOW,
                account_id=request.account_id,
                instrument_id=request.order_leg.instrument_id,
            )
            self.facts = replace(
                self.facts,
                positions=tuple((*self.facts.positions, position)),
                fills=tuple((*self.facts.fills, fill)),
            )

    def get_order(self, account: Account, external_order_id: str):
        self.order_reads += 1
        if self.order_reads >= self.release_after_order_reads:
            self._release_pending()
        return self._filled_orders.get(str(external_order_id), self._working_orders.get(str(external_order_id)))

    def get_open_orders(self, _account: Account):
        self.open_order_reads += 1
        if self.open_order_reads >= self.release_after_order_reads:
            self._release_pending()
        return tuple(
            snapshot
            for external_order_id, snapshot in self._working_orders.items()
            if external_order_id not in self._filled_orders
        )


class SequencedDelayedPilotAdapter(DelayedPilotAdapter):
    """Record that the second submit saw durable evidence for the first."""

    def __init__(self, *, release_after_order_reads: int = 3) -> None:
        super().__init__(release_after_order_reads=release_after_order_reads)
        self.second_submit_saw_first_fill: bool | None = None

    def submit_order(self, account: Account, request) -> BrokerSubmissionResult:
        if len(self.submit_calls) == 1:
            self.second_submit_saw_first_fill = "delayed-order-1" in self._filled_orders
        return super().submit_order(account, request)


class RejectingPilotAdapter(PilotFakeAdapter):
    """Return an explicit no-submit rejection for the first leg."""

    def submit_order(self, _account: Account, request) -> BrokerSubmissionResult:
        self.submit_calls.append(request.broker_order_id)
        return BrokerSubmissionResult(
            broker_order_id=request.broker_order_id,
            accepted=False,
            status=BrokerOrderStatus.REJECTED,
            external_order_id=None,
            client_order_id=request.client_order_id,
            submitted_at=NOW,
            cumulative_filled_quantity=Decimal("0"),
            no_submit_asserted=True,
            no_fill_asserted=True,
            submitted_quantity=request.order_leg.quantity,
            instrument_id=request.order_leg.instrument_id,
        )


class ClosedOnSiblingGateAdapter(PilotFakeAdapter):
    """Keep the first leg fillable, then close RTH before its sibling."""

    def __init__(self) -> None:
        super().__init__()
        self.market_state_calls = 0

    def get_authoritative_market_state(self, symbols):
        self.market_state_calls += 1
        state = "CLOSED" if self.market_state_calls >= 3 else "RTH"
        return {
            "market": "US",
            "captured_at": NOW.isoformat(),
            "complete": True,
            "rows": [
                {"symbol": str(symbol), "market_state": state}
                for symbol in symbols
            ],
        }


class ContradictoryAfterFirstFillAdapter(PilotFakeAdapter):
    """Return a contradictory account position at the sibling gate."""

    def get_authoritative_account_facts(self, account: Account) -> BrokerFactSnapshot:
        facts = super().get_authoritative_account_facts(account)
        # The first post-submit recovery fact set must remain coherent so the
        # prior leg can be durably terminalized.  Contradict it only on the
        # fresh account gate immediately before the sibling adapter call.
        if self.submit_calls and self.fact_calls >= 4 and facts.positions:
            first = facts.positions[0]
            contradictory = replace(first, signed_quantity=first.signed_quantity + Decimal("1"))
            return replace(facts, positions=(contradictory, *facts.positions[1:]))
        return facts


class NettedPilotAdapter(PilotFakeAdapter):
    """Model an account-level provider position row per instrument."""

    def submit_order(self, account: Account, request) -> BrokerSubmissionResult:
        result = super().submit_order(account, request)
        matching = [
            position
            for position in self.facts.positions
            if position.instrument_id == request.order_leg.instrument_id
        ]
        if len(matching) > 1:
            net_quantity = sum((position.signed_quantity for position in matching), Decimal("0"))
            retained = replace(
                matching[0],
                signed_quantity=net_quantity,
                captured_at=NOW,
            )
            self.facts = replace(
                self.facts,
                positions=tuple(
                    position
                    for position in self.facts.positions
                    if position.instrument_id != request.order_leg.instrument_id
                ) + (retained,),
            )
        return result


def make_repository(tmp_path, account: Account, sleeves: tuple[PairSleeve, PairSleeve]):
    repository = SQLiteTradingRepository(tmp_path / "stage6.db")
    repository.initialize()
    repository.save_account(account)
    repository.save_strategy(
        Strategy(
            id="pilot-strategy",
            name="Stage 6 test strategy",
            strategy_type="generic_stat_arb",
            created_at=NOW,
            updated_at=NOW,
        )
    )
    for sleeve in sleeves:
        repository.save_book(Book(id=sleeve.book_id, name=sleeve.name, created_at=NOW, updated_at=NOW))
        for instrument_id in sleeve.instrument_ids:
            repository.save_instrument(
                Instrument(
                    id=instrument_id,
                    asset_class=AssetClass.EQUITY,
                    symbol=instrument_id.upper(),
                    venue="TEST",
                    currency="USD",
                    created_at=NOW,
                    updated_at=NOW,
                )
            )
    return repository


def make_runner(tmp_path, *, facts: BrokerFactSnapshot | None = None):
    account = make_account()
    sleeves = make_sleeves()
    repository = make_repository(tmp_path, account, sleeves)
    adapter = PilotFakeAdapter(facts)
    oms = GenericOMS(repository, adapter, clock=lambda: NOW)
    return account, sleeves, repository, adapter, Stage6PilotRunner(repository, oms, clock=lambda: NOW)


def seed_verified_book_exposure(
    repository: SQLiteTradingRepository,
    account: Account,
    sleeves: tuple[PairSleeve, PairSleeve],
    quantities: tuple[str, str] = ("2", "2"),
) -> None:
    """Seed completed source intents and their durable signed allocations."""

    entry_targets = make_targets(sleeves, quantities=quantities)
    for sleeve, target in zip(sleeves, entry_targets, strict=True):
        source_intent = sleeve.to_intent(target)
        repository.create_intent(source_intent)
        for transition in (
            IntentStatus.RISK_APPROVED,
            IntentStatus.SUBMITTING,
            IntentStatus.WORKING,
            IntentStatus.FILLED,
        ):
            repository.transition_intent(source_intent.id, transition, now=NOW)
        for leg in source_intent.legs:
            signed_quantity = leg.quantity if leg.side is Side.BUY else -leg.quantity
            repository.save_position_allocation(
                PositionAllocation(
                    id=f"seed-{leg.id}",
                    account_id=account.id,
                    instrument_id=leg.instrument_id,
                    strategy_id=sleeve.strategy_id,
                    book_id=sleeve.book_id,
                    ownership_class=OwnershipClass.MANAGED,
                    signed_quantity=signed_quantity,
                    source_intent_id=source_intent.id,
                    updated_at=NOW,
                    metadata={"provenance": "stage6-exit-basis-test"},
                ),
                _validation_token=repository._allocation_validation_capability(),
            )


def make_retired_baseline_runner(tmp_path):
    account = Account(
        id="moomoo:sim:5077333",
        broker="moomoo",
        environment=TradingEnvironment.SIM,
        external_account_id="5077333",
        base_currency="USD",
        metadata={"allocation_capacity": "20", "account_capacity": "20"},
        created_at=NOW,
        updated_at=NOW,
    )
    repository = SQLiteTradingRepository(tmp_path / "retired-stage6.db")
    repository.initialize()
    repository.save_account(account)
    repository.save_strategy(
        Strategy(
            id="pilot-strategy",
            name="Stage 6 test strategy",
            strategy_type="generic_stat_arb",
            created_at=NOW,
            updated_at=NOW,
        )
    )
    sleeves = (
        PairSleeve(
            sleeve_id="pilot-sleeve-a",
            strategy_id="pilot-strategy",
            account_id=account.id,
            book_id="pilot-book-a",
            name="Pilot A",
            instrument_ids=("generic-sim-smoke:us-aapl", "generic-sim-smoke:us-msft"),
            symbols=("US.AAPL", "US.MSFT"),
            configuration={"signal_source": "test-driver"},
        ),
        PairSleeve(
            sleeve_id="pilot-sleeve-b",
            strategy_id="pilot-strategy",
            account_id=account.id,
            book_id="pilot-book-b",
            name="Pilot B",
            instrument_ids=("generic-sim-smoke:us-aapl", "generic-sim-smoke:us-msft"),
            symbols=("US.AAPL", "US.MSFT"),
            configuration={"signal_source": "test-driver"},
        ),
    )
    for sleeve in sleeves:
        repository.save_book(Book(id=sleeve.book_id, name=sleeve.name, created_at=NOW, updated_at=NOW))
    imported = repository.import_legacy_order_evidence("data/generic-sim-smoke.db", account.id)
    repository.save_execution_evidence_baseline(
        ExecutionEvidenceBaseline(
            id="pilot-retired-baseline",
            account_id=account.id,
            captured_at=NOW,
            evidence_mode=ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS,
            coverage=("CURRENT_ORDER_SNAPSHOTS", "HISTORICAL_ORDER_SNAPSHOTS", "SOURCE_LEDGER"),
            source_ledger_fingerprint=imported["source_ledger_fingerprint"],
            source_order_ids=imported["source_order_ids"],
            position_fingerprint="flat-position-fingerprint",
            open_order_fingerprint="empty-order-fingerprint",
            metadata={"legacy_book_id": imported["legacy_book_id"]},
        )
    )
    facts = BrokerFactSnapshot(
        account_id=account.id,
        captured_at=NOW,
        complete=True,
        execution_evidence_mode=ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS,
        execution_evidence_scope=frozenset({"CURRENT_ORDER_SNAPSHOTS"}),
    )
    adapter = NettedPilotAdapter(facts)
    oms = GenericOMS(repository, adapter, clock=lambda: NOW)
    return account, sleeves, repository, adapter, Stage6PilotRunner(repository, oms, clock=lambda: NOW)


def test_dry_run_builds_both_intents_and_never_submits(tmp_path):
    account, sleeves, repository, adapter, runner = make_runner(tmp_path)
    spec = make_spec(account, sleeves)

    report = runner.run(spec)
    repeated = runner.run(spec)

    assert report.mode is Stage6RunMode.DRY_RUN
    assert report.preflight_passed is True
    assert report.broker_preflight_passed is False
    assert report.stop_reasons == ()
    assert len(report.intent_plans) == 2
    assert all(item["status"] == "PLANNED_NOT_SUBMITTED" for item in report.intent_results)
    assert adapter.submit_calls == []
    assert adapter.fact_calls == 0
    assert report.run_id == repeated.run_id
    assert [item["intent_id"] for item in report.intent_plans] == [
        item["intent_id"] for item in repeated.intent_plans
    ]
    assert len(repository.book_allocations(account.id, active_only=True)) == 2


def test_exit_target_must_match_durable_book_exposure_before_dry_run(tmp_path):
    account, sleeves, repository, adapter, runner = make_runner(tmp_path)
    seed_verified_book_exposure(repository, account, sleeves)
    already_negated = tuple(
        replace(target, signed_quantities=(Decimal("-2"), Decimal("2")))
        for target in make_targets(sleeves, action=IntentAction.EXIT)
    )

    report = runner.run(
        make_spec(account, sleeves, targets=already_negated),
        mode=Stage6RunMode.DRY_RUN,
    )

    assert report.preflight_passed is False
    assert any("EXIT target does not match the durable book exposure" in reason for reason in report.stop_reasons)
    assert adapter.submit_calls == []


def test_nonzero_exit_target_is_stopped_when_book_is_flat(tmp_path):
    account, sleeves, _repository, adapter, runner = make_runner(tmp_path)
    flat_close = make_targets(sleeves, action=IntentAction.EXIT)

    report = runner.run(
        make_spec(account, sleeves, targets=flat_close),
        mode=Stage6RunMode.DRY_RUN,
    )

    assert report.preflight_passed is False
    assert any("durable book exposure" in reason for reason in report.stop_reasons)
    assert adapter.submit_calls == []


def test_exit_target_is_reversed_once_for_durable_book_exposure(tmp_path):
    account, sleeves, repository, adapter, runner = make_runner(tmp_path)
    seed_verified_book_exposure(repository, account, sleeves)
    close_basis = make_targets(
        sleeves,
        quantities=("2", "2"),
        action=IntentAction.EXIT,
    )

    report = runner.run(
        make_spec(account, sleeves, targets=close_basis),
        mode=Stage6RunMode.DRY_RUN,
    )

    assert report.preflight_passed is True
    assert adapter.submit_calls == []
    assert [leg["side"] for leg in report.intent_plans[0]["legs"]] == ["SELL", "BUY"]
    assert [leg["side"] for leg in report.intent_plans[1]["legs"]] == ["SELL", "BUY"]


def test_explicit_sim_arm_dispatches_both_through_generic_oms_and_persists_run_metadata(tmp_path):
    account, sleeves, repository, adapter, runner = make_runner(tmp_path)
    spec = make_spec(account, sleeves)

    report = runner.run(spec, mode=Stage6RunMode.SIM_SUBMIT)

    assert report.preflight_passed is True
    assert report.broker_preflight_passed is True
    assert report.stop_reasons == ()
    assert len(adapter.submit_calls) == 4
    stored = [
        repository.get_intent_by_idempotency_key(account.id, target.idempotency_key)
        for target in (sleeves[0].to_intent(make_targets(sleeves)[0]), sleeves[1].to_intent(make_targets(sleeves)[1]))
    ]
    assert all(item is not None for item in stored)
    assert all(item["metadata"]["stage6_pilot"]["run_id"] == report.run_id for item in stored if item)
    assert all(
        item["metadata"]["stage6_submission"]["process_id"]
        for item in stored
        if item
    )
    assert all(
        {
            "process_id",
            "run_id",
            "mode",
            "execution_compatibility",
            "submitted_at",
            "correlation",
        }.issubset(item["metadata"]["stage6_submission"])
        for item in stored
        if item
    )
    assert all(
        item["metadata"]["stage6_submission"]["execution_compatibility"] == STAGE6_EXECUTION_COMPATIBILITY
        for item in stored
        if item
    )
    assert len(report.after_status) >= 2


def test_stage6_provenance_failure_blocks_before_adapter_submission(tmp_path, monkeypatch):
    account, sleeves, repository, adapter, runner = make_runner(tmp_path)
    original_append = repository.append_intent_metadata

    def fail_stage6_provenance(intent_id, metadata, *, account_id=None):
        if "stage6_submission" in metadata:
            raise RuntimeError("simulated pre-submit provenance persistence failure")
        return original_append(intent_id, metadata, account_id=account_id)

    monkeypatch.setattr(repository, "append_intent_metadata", fail_stage6_provenance)
    report = runner.run(make_spec(account, sleeves), mode=Stage6RunMode.SIM_SUBMIT)

    # The callback runs after the local intent exists but before GenericOMS
    # can invoke the provider.  A provenance write failure therefore leaves a
    # durable rejected/auditable intent and zero broker submissions.
    assert adapter.submit_calls == []
    assert report.stop_reasons
    intents = repository.book_intents(account.id)
    assert intents
    assert all(item["status"] == "REJECTED" for item in intents)
    assert all(
        "stage6_submission" not in (repository.get_intent(item["id"]) or {}).get("metadata", {})
        for item in intents
    )


def test_sim_arm_dispatches_all_legs_for_moomoo_zero_fill_ack_shape(tmp_path):
    account, sleeves, repository, _adapter, _runner = make_runner(tmp_path)
    ack_adapter = MoomooAckPilotAdapter()
    oms = GenericOMS(repository, ack_adapter, clock=lambda: NOW)
    runner = Stage6PilotRunner(repository, oms, clock=lambda: NOW)

    report = runner.run(
        make_spec(account, sleeves, targets=make_targets(sleeves, quantities=("1", "1"))),
        mode=Stage6RunMode.SIM_SUBMIT,
    )

    assert report.preflight_passed is True
    assert report.broker_preflight_passed is True
    assert report.stop_reasons == ()
    assert len(ack_adapter.submit_calls) == 4
    assert len(report.intent_results) == 2
    assert all(item["submitted"] is True for item in report.intent_results)


def test_working_ack_waits_for_full_recovery_before_dispatching_next_sleeve(tmp_path):
    account = make_account()
    sleeves = make_sleeves()
    repository = make_repository(tmp_path, account, sleeves)
    adapter = SequencedDelayedPilotAdapter(release_after_order_reads=2)
    elapsed = [0.0]

    def clock() -> datetime:
        return NOW + timedelta(seconds=elapsed[0])

    def sleep(seconds: float) -> None:
        elapsed[0] += seconds

    oms = GenericOMS(repository, adapter, clock=clock)
    runner = Stage6PilotRunner(
        repository,
        oms,
        clock=clock,
        sleep=sleep,
        dispatch_wait_seconds=5.0,
        dispatch_poll_seconds=1.0,
    )

    report = runner.run(make_spec(account, sleeves), mode=Stage6RunMode.SIM_SUBMIT)

    assert report.preflight_passed is True
    assert report.stop_reasons == ()
    assert len(adapter.submit_calls) == 4
    # Recovery is admitted from repeated strict account-fact snapshots; it no
    # longer falls back to the cacheable open-order reader for position/fill
    # truth.
    assert adapter.fact_calls >= 2
    assert adapter.open_order_reads == 0
    assert adapter.second_submit_saw_first_fill is True
    assert all(item["dispatch_outcome"] == "FULL" for item in report.intent_results)


def test_partial_or_timeout_working_ack_stops_before_next_sleeve(tmp_path):
    account = make_account()
    sleeves = make_sleeves()
    repository = make_repository(tmp_path, account, sleeves)
    adapter = DelayedPilotAdapter(release_after_order_reads=100)
    elapsed = [0.0]

    def clock() -> datetime:
        return NOW + timedelta(seconds=elapsed[0])

    def sleep(seconds: float) -> None:
        elapsed[0] += seconds

    oms = GenericOMS(repository, adapter, clock=clock)
    runner = Stage6PilotRunner(
        repository,
        oms,
        clock=clock,
        sleep=sleep,
        dispatch_wait_seconds=1.0,
        dispatch_poll_seconds=0.5,
    )

    report = runner.run(make_spec(account, sleeves), mode=Stage6RunMode.SIM_SUBMIT)

    assert report.preflight_passed is False
    assert any("returned TIMEOUT" in reason for reason in report.stop_reasons)
    # Per-leg sequencing now waits for the first working order before the
    # sibling reaches the adapter.  A timeout therefore leaves the sibling
    # completely unattempted rather than creating another open exposure.
    assert len(adapter.submit_calls) == 1
    first_intent = repository.get_intent(report.intent_results[0]["intent_id"])
    assert first_intent is not None
    assert repository.broker_orders_for_leg(first_intent["legs"][1]["id"]) == []
    assert [item["sleeve_id"] for item in report.intent_results] == ["pilot-sleeve-a"]


def test_rejected_prior_leg_leaves_sibling_unsubmitted(tmp_path):
    account = make_account()
    sleeves = make_sleeves()
    repository = make_repository(tmp_path, account, sleeves)
    adapter = RejectingPilotAdapter()
    runner = Stage6PilotRunner(
        repository,
        GenericOMS(repository, adapter, clock=lambda: NOW),
        clock=lambda: NOW,
    )

    report = runner.run(make_spec(account, sleeves), mode=Stage6RunMode.SIM_SUBMIT)

    assert report.preflight_passed is False
    assert any("returned REJECTED" in reason for reason in report.stop_reasons)
    assert len(adapter.submit_calls) == 1
    first_intent = repository.get_intent(report.intent_results[0]["intent_id"])
    assert first_intent is not None
    assert repository.broker_orders_for_leg(first_intent["legs"][1]["id"]) == []


def test_rth_close_before_sibling_gate_leaves_sibling_unsubmitted(tmp_path):
    account = make_account()
    sleeves = make_sleeves()
    repository = make_repository(tmp_path, account, sleeves)
    adapter = ClosedOnSiblingGateAdapter()
    runner = Stage6PilotRunner(
        repository,
        GenericOMS(repository, adapter, clock=lambda: NOW),
        clock=lambda: NOW,
    )

    report = runner.run(make_spec(account, sleeves), mode=Stage6RunMode.SIM_SUBMIT)

    assert report.preflight_passed is False
    assert any("RECONCILIATION_REQUIRED" in reason for reason in report.stop_reasons)
    assert adapter.market_state_calls >= 3
    assert len(adapter.submit_calls) == 1
    first_intent = repository.get_intent(report.intent_results[0]["intent_id"])
    assert first_intent is not None
    assert repository.broker_orders_for_leg(first_intent["legs"][1]["id"]) == []
    issues = repository.open_reconciliation_issues(account.id)
    assert len(issues) == 1
    assert "not RTH" in issues[0]["details_json"]


def test_contradictory_account_facts_before_sibling_gate_block_dispatch(tmp_path):
    account = make_account()
    sleeves = make_sleeves()
    repository = make_repository(tmp_path, account, sleeves)
    adapter = ContradictoryAfterFirstFillAdapter()
    runner = Stage6PilotRunner(
        repository,
        GenericOMS(repository, adapter, clock=lambda: NOW),
        clock=lambda: NOW,
    )

    report = runner.run(make_spec(account, sleeves), mode=Stage6RunMode.SIM_SUBMIT)

    assert report.preflight_passed is False
    assert any("RECONCILIATION_REQUIRED" in reason for reason in report.stop_reasons)
    assert len(adapter.submit_calls) == 1
    first_intent = repository.get_intent(report.intent_results[0]["intent_id"])
    assert first_intent is not None
    assert repository.broker_orders_for_leg(first_intent["legs"][1]["id"]) == []
    issues = repository.open_reconciliation_issues(account.id)
    assert len(issues) == 1
    assert "does not match the durable managed allocation" in issues[0]["details_json"]


def test_sim_arm_dispatches_with_verified_retired_baseline_and_old_terminal_orders(tmp_path):
    account, sleeves, repository, adapter, runner = make_retired_baseline_runner(tmp_path)

    # Reproduce the real restart quarantine: aged imported terminal claims
    # are durable evidence, but the safety scan temporarily moves their
    # intents to reconciliation-required.  The explicit proof-driven
    # resolver must restore only those retired intents before the pilot can
    # dispatch through GenericOMS.
    retired_book_id = repository.latest_execution_evidence_baseline(account.id).metadata["legacy_book_id"]
    retired_intents = repository.book_intents(account.id, book_id=retired_book_id)
    for row in retired_intents:
        repository.transition_intent(row["id"], IntentStatus.RECONCILIATION_REQUIRED, now=NOW)
    for order in repository.book_broker_orders(account.id, book_id=retired_book_id):
        runner.oms._require_reconciliation(
            str(order["intent_id"]),
            account,
            category="TERMINAL_HISTORY_EXPIRED",
            entity_type="BROKER_ORDER",
            entity_key=str(order["id"]),
            details={"broker_order_id": str(order["id"])},
        )
    resolved = runner.oms.resolve_verified_retired_baseline(account=account)

    report = runner.run(
        make_spec(account, sleeves, targets=make_targets(sleeves, quantities=("1", "1"))),
        mode=Stage6RunMode.SIM_SUBMIT,
    )

    assert report.preflight_passed is True
    assert report.broker_preflight_passed is True
    assert report.stop_reasons == ()
    assert len(adapter.submit_calls) == 4
    assert set(resolved["restored_intent_ids"]) == {str(row["id"]) for row in retired_intents}
    assert repository.open_reconciliation_issues(account.id) == []
    assert repository.open_recovery_actions(account.id) == []


def test_closed_historical_fills_are_not_unmatched_broker_exposure(tmp_path):
    account, sleeves, repository, adapter, runner = make_retired_baseline_runner(tmp_path)
    baseline = repository.latest_execution_evidence_baseline(account.id)
    assert baseline is not None
    retired_book_id = str(baseline.metadata["legacy_book_id"])
    closed_fills: list[BrokerFill] = []
    for order in repository.book_broker_orders(account.id, book_id=retired_book_id):
        intent_id = repository.intent_id_for_broker_order(str(order["id"]))
        intent = repository.get_intent(str(intent_id))
        assert intent is not None
        leg = next(item for item in intent["legs"] if str(item["id"]) == str(order["order_leg_id"]))
        stored = repository.fills_for_broker_order(str(order["id"]))
        assert len(stored) == 1
        fill = stored[0]
        metadata = {
            "source": "order_list_query",
            "synthetic": True,
            "evidence_mode": ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS.value,
            "evidence_reference": f"{order['external_order_id']}:{fill['dedupe_key']}",
            "raw": {
                "order_id": str(order["external_order_id"]),
                "code": str(leg["metadata"].get("smoke_symbol", "")),
                "qty": float(fill["quantity"]),
                "dealt_qty": float(fill["quantity"]),
                "dealt_avg_price": float(fill["price"]),
                "order_status": "FILLED_ALL",
            },
        }
        closed_fills.append(
            BrokerFill(
                external_order_id=str(order["external_order_id"]),
                dedupe_key=str(fill["dedupe_key"]),
                quantity=Decimal(str(fill["quantity"])),
                price=Decimal(str(fill["price"])),
                filled_at=datetime.fromisoformat(str(fill["filled_at"])),
                received_at=datetime.fromisoformat(str(fill["received_at"])),
                account_id=account.id,
                    evidence_reference=f"{order['external_order_id']}:{fill['dedupe_key']}",
                metadata=metadata,
                instrument_id=str(leg["instrument_id"]),
            )
        )

    assert all(
        runner.oms.is_proof_backed_closed_broker_fill(account=account, broker_fill=fill)
        for fill in closed_fills
    )
    adapter.facts = replace(adapter.facts, fills=tuple(closed_fills))
    report = runner.run(
        make_spec(account, sleeves, targets=make_targets(sleeves, quantities=("1", "1"))),
        mode=Stage6RunMode.SIM_SUBMIT,
    )

    assert report.preflight_passed is True, (
        report.stop_reasons,
        [dict(item) for item in report.intent_results],
    )
    assert report.stop_reasons == ()
    assert len(adapter.submit_calls) == 4

    mismatched = replace(closed_fills[0], quantity=Decimal("2"))
    adapter.facts = replace(adapter.facts, fills=(mismatched, *closed_fills[1:]))
    blocked = runner.run(
        make_spec(
            account,
            sleeves,
            targets=make_targets(
                sleeves,
                quantities=("1", "1"),
            ),
        ),
        mode=Stage6RunMode.SIM_SUBMIT,
    )
    assert blocked.preflight_passed is False
    assert blocked.stop_reasons
    assert len(adapter.submit_calls) == 4


def test_final_state_attributes_verified_retired_baseline_orders_but_not_foreign_orders(
    tmp_path,
    monkeypatch,
):
    account, sleeves, repository, _adapter, runner = make_retired_baseline_runner(tmp_path)

    baseline_result = runner.final_state(make_spec(account, sleeves))
    assert baseline_result["final_state_passed"] is True
    assert baseline_result["final"]["all_orders_attributable"] is True

    original = repository.book_broker_orders

    def with_foreign_order(account_id, *, book_id=None):
        rows = original(account_id, book_id=book_id)
        rows.append({"id": "foreign-order", "book_id": "unverified-book", "intent_id": "foreign-intent"})
        return rows

    monkeypatch.setattr(repository, "book_broker_orders", with_foreign_order)
    foreign_result = runner.final_state(make_spec(account, sleeves))
    assert foreign_result["final_state_passed"] is False
    assert foreign_result["final"]["all_orders_attributable"] is False
    assert "unattributed book" in " ".join(foreign_result["stop_reasons"])


def test_one_sleeve_capacity_reject_stops_before_dispatching_the_other(tmp_path):
    account, sleeves, _repository, adapter, runner = make_runner(tmp_path)
    spec = make_spec(account, sleeves, targets=make_targets(sleeves, quantities=("2", "6")))

    report = runner.run(spec, mode=Stage6RunMode.SIM_SUBMIT)

    assert report.preflight_passed is False
    assert any("returned RECONCILIATION_REQUIRED" in reason for reason in report.stop_reasons)
    assert len(adapter.submit_calls) == 2
    assert [item["sleeve_id"] for item in report.intent_results] == ["pilot-sleeve-a", "pilot-sleeve-b"]


def test_duplicate_or_mixed_signal_batch_is_rejected_before_any_dispatch(tmp_path):
    account, sleeves, _repository, adapter, runner = make_runner(tmp_path)
    first = make_targets(sleeves)[0]
    duplicate = NormalizedPairTarget(
        sleeve_id=first.sleeve_id,
        cycle_id="another-cycle",
        signal_id="another-signal",
        instrument_ids=first.instrument_ids,
        signed_quantities=first.signed_quantities,
        action=IntentAction.ENTER,
        evaluated_at=NOW,
        provenance={"source": "bad-driver"},
    )
    with pytest.raises(ValueError, match="cover both configured sleeves"):
        Stage6PilotSpec(
            account=account,
            sleeves=sleeves,
            allocation_update=make_spec(account, sleeves).allocation_update,
            targets=(first, duplicate),
        )
    assert adapter.submit_calls == []


def test_restart_recovery_hook_is_explicit_and_never_submits(tmp_path):
    account, sleeves, _repository, adapter, runner = make_runner(tmp_path)
    result = runner.recover(account)

    assert result == []
    assert adapter.submit_calls == []


def test_roundtrip_resolution_rejects_same_account_foreign_strategy_or_book(tmp_path):
    account, sleeves, repository, adapter, runner = make_runner(tmp_path)
    spec = make_spec(account, sleeves)
    source = sleeves[0].to_intent(make_targets(sleeves)[0])
    repository.save_strategy(
        Strategy(
            id="foreign-strategy",
            name="Foreign strategy",
            strategy_type="generic_stat_arb",
            created_at=NOW,
            updated_at=NOW,
        )
    )
    foreign_strategy_legs = tuple(
        replace(leg, id=f"foreign-strategy-intent-{leg.sequence}", intent_id="foreign-strategy-intent")
        for leg in source.legs
    )
    foreign_strategy = replace(
        source,
        id="foreign-strategy-intent",
        idempotency_key="foreign-strategy-intent-key",
        strategy_id="foreign-strategy",
        legs=foreign_strategy_legs,
    )
    repository.create_intent(foreign_strategy)
    with pytest.raises(ValueError, match="another strategy"):
        runner.resolve_verified_roundtrip(
            account=account,
            spec=spec,
            entry_intent_id=foreign_strategy.id,
            exit_intent_id=source.id,
        )

    repository.save_book(Book(id="foreign-book", name="Foreign book", created_at=NOW, updated_at=NOW))
    foreign_book_legs = tuple(
        replace(leg, id=f"foreign-book-intent-{leg.sequence}", intent_id="foreign-book-intent")
        for leg in source.legs
    )
    foreign_book = replace(
        source,
        id="foreign-book-intent",
        idempotency_key="foreign-book-intent-key",
        book_id="foreign-book",
        legs=foreign_book_legs,
    )
    repository.create_intent(foreign_book)
    with pytest.raises(ValueError, match="another book"):
        runner.resolve_verified_roundtrip(
            account=account,
            spec=spec,
            entry_intent_id=foreign_book.id,
            exit_intent_id=source.id,
        )
    assert adapter.submit_calls == []


def test_delayed_or_partial_open_order_is_a_preflight_stop_with_facts_in_report(tmp_path):
    account = make_account()
    sleeves = make_sleeves()
    partial = BrokerOrderSnapshot(
        id="pilot-snapshot-order",
        broker_snapshot_id="pilot-snapshot",
        account_id=account.id,
        instrument_id=sleeves[0].instrument_ids[0],
        external_order_id="unrelated-working-order",
        side=Side.BUY,
        quantity=Decimal("2"),
        filled_quantity=Decimal("1"),
        status=BrokerOrderStatus.PARTIALLY_FILLED,
        captured_at=NOW,
        order_time=NOW,
    )
    facts = BrokerFactSnapshot(
        account_id=account.id,
        captured_at=NOW,
        complete=True,
        open_orders=(partial,),
    )
    account, sleeves, _repository, adapter, runner = make_runner(tmp_path, facts=facts)

    report = runner.run(make_spec(account, sleeves), mode=Stage6RunMode.SIM_SUBMIT)

    assert report.preflight_passed is False
    assert any("outstanding broker orders" in reason for reason in report.stop_reasons)
    assert report.broker_facts["open_order_count"] == 1
    assert adapter.submit_calls == []


def test_unrelated_broker_position_blocks_sim_arm(tmp_path):
    account = make_account()
    sleeves = make_sleeves()
    position = PositionSnapshot(
        id="foreign-position",
        broker_snapshot_id="position-snapshot",
        account_id=account.id,
        instrument_id="foreign-instrument",
        signed_quantity=Decimal("3"),
        captured_at=NOW,
    )
    facts = BrokerFactSnapshot(
        account_id=account.id,
        captured_at=NOW,
        complete=True,
        positions=(position,),
    )
    account, sleeves, _repository, adapter, runner = make_runner(tmp_path, facts=facts)

    report = runner.run(make_spec(account, sleeves), mode=Stage6RunMode.SIM_SUBMIT)

    assert report.preflight_passed is False
    assert any("does not match the durable managed allocation" in reason for reason in report.stop_reasons)
    assert adapter.submit_calls == []


def test_sim_arm_has_no_live_mode_and_live_account_is_rejected(tmp_path):
    live_account = make_account(environment=TradingEnvironment.LIVE)
    sleeves = make_sleeves(live_account.id)
    with pytest.raises(ValueError, match="accepts SIM accounts"):
        Stage6PilotSpec(
            account=live_account,
            sleeves=sleeves,
            allocation_update=make_spec(make_account(), make_sleeves()).allocation_update,
            targets=make_targets(sleeves),
        )

    account, sleeves, _repository, adapter, runner = make_runner(tmp_path)
    with pytest.raises(ValueError, match="DRY_RUN or SIM_SUBMIT"):
        runner.run(make_spec(account, sleeves), mode="LIVE")
    assert adapter.submit_calls == []
