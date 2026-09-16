from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
import json

import pytest

from src.trading_core.domain import (
    Account,
    AssetClass,
    BrokerOrderSnapshot,
    BrokerOrderStatus,
    ExecutionPolicy,
    ExecutionSession,
    Fill,
    Instrument,
    IntentAction,
    IntentStatus,
    LegStatus,
    OrderIntent,
    OrderLeg,
    QuantityUnit,
    RiskDecisionRecord,
    Side,
    Strategy,
    TradingEnvironment,
)
from src.trading_core.oms import GenericOMS, OMSExecutionError
from src.trading_core.ports import BrokerFill, BrokerSubmissionResult
from src.trading_core.repository import SQLiteTradingRepository


NOW = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)


def account() -> Account:
    return Account(
        id="acct",
        broker="fake",
        environment=TradingEnvironment.SIM,
        external_account_id="external-acct",
        base_currency="USD",
        created_at=NOW,
        updated_at=NOW,
    )


def strategy() -> Strategy:
    return Strategy(
        id="strategy",
        name="generic test",
        strategy_type="test",
        version="1",
        config={},
        created_at=NOW,
        updated_at=NOW,
    )


def instrument(index: int) -> Instrument:
    return Instrument(
        id=f"instrument-{index}",
        asset_class=AssetClass.EQUITY,
        symbol=f"SYM{index}",
        venue="TEST",
        currency="USD",
        created_at=NOW,
        updated_at=NOW,
    )


def make_intent(count: int, *, key: str = "intent-key") -> OrderIntent:
    intent_id = f"intent-{count}"
    return OrderIntent(
        id=intent_id,
        idempotency_key=key,
        strategy_id="strategy",
        account_id="acct",
        action=IntentAction.ENTER,
        execution_policy=ExecutionPolicy(),
        legs=tuple(
            OrderLeg(
                id=f"logical-{index}",
                intent_id=intent_id,
                sequence=index,
                instrument_id=f"instrument-{index}",
                side=Side.BUY if index % 2 == 0 else Side.SELL,
                quantity=Decimal("10"),
                quantity_unit=QuantityUnit.UNITS,
                order_type="MARKET",
                status=LegStatus.PLANNED,
                created_at=NOW,
                updated_at=NOW,
            )
            for index in range(count)
        ),
        created_at=NOW,
        updated_at=NOW,
    )


def decision(intent_id: str, approved: bool = True) -> RiskDecisionRecord:
    return RiskDecisionRecord(
        id=f"risk-{intent_id}",
        intent_id=intent_id,
        approved=approved,
        reason="approved" if approved else "blocked",
        checks={"account_wide": True},
        evaluated_at=NOW,
    )


def ready_repository(tmp_path, count: int) -> SQLiteTradingRepository:
    repository = SQLiteTradingRepository(tmp_path / "oms.db")
    repository.initialize()
    repository.save_account(account())
    repository.save_strategy(strategy())
    for index in range(count):
        repository.save_instrument(instrument(index))
    return repository


class FakeAdapter:
    def __init__(self, repository, *, reject_call=None, ambiguous_call=None, inspect_persistence=False):
        self.repository = repository
        self.reject_call = reject_call
        self.ambiguous_call = ambiguous_call
        self.inspect_persistence = inspect_persistence
        self.submit_calls = []
        self.open_orders = []
        self.fills = []

    def submit_order(self, account_value, request):
        self.submit_calls.append(request)
        call = len(self.submit_calls)
        if self.inspect_persistence:
            stored = self.repository.get_intent(request.order_leg.intent_id)
            assert stored is not None
            assert len(stored["legs"]) >= call
            with self.repository.transaction() as conn:
                assert conn.execute(
                    "SELECT COUNT(*) FROM core_risk_decisions WHERE intent_id = ?",
                    (request.order_leg.intent_id,),
                ).fetchone()[0] == 1
        if call == self.ambiguous_call:
            return BrokerSubmissionResult(
                broker_order_id=request.broker_order_id,
                accepted=True,
                status=BrokerOrderStatus.UNKNOWN,
                external_order_id=None,
                client_order_id=request.client_order_id,
                ambiguous=True,
            )
        if call == self.reject_call:
            return BrokerSubmissionResult(
                broker_order_id=request.broker_order_id,
                accepted=False,
                status=BrokerOrderStatus.REJECTED,
                client_order_id=request.client_order_id,
                error_message="rejected",
            )
        external = f"external-{call}"
        return BrokerSubmissionResult(
            broker_order_id=request.broker_order_id,
            accepted=True,
            status=BrokerOrderStatus.WORKING,
            external_order_id=external,
            client_order_id=request.client_order_id,
        )

    def get_open_orders(self, account_value):
        return tuple(self.open_orders)

    def get_fills(self, account_value, since=None):
        return tuple(self.fills)

    def get_order(self, account_value, external_order_id):
        return next(
            (item for item in self.open_orders if item.external_order_id == external_order_id),
            None,
        )

    def cancel_order(self, account_value, external_order_id):
        return BrokerSubmissionResult(
            broker_order_id="cancel",
            accepted=True,
            status=BrokerOrderStatus.CANCELLED,
            external_order_id=external_order_id,
        )


@pytest.mark.parametrize("count", [1, 2, 3])
def test_oms_persists_all_legs_before_submit_and_supports_n_legs(tmp_path, count):
    repository = ready_repository(tmp_path, count)
    adapter = FakeAdapter(repository, inspect_persistence=True)
    oms = GenericOMS(repository, adapter)
    order_intent = make_intent(count)

    result = oms.submit_intent(order_intent, account=account(), risk_decision=decision(order_intent.id))

    assert result["status"] == IntentStatus.WORKING.value
    assert len(adapter.submit_calls) == count
    assert [item.order_leg.sequence for item in adapter.submit_calls] == list(range(count))


def test_duplicate_intent_returns_existing_state_without_resubmission(tmp_path):
    repository = ready_repository(tmp_path, 1)
    adapter = FakeAdapter(repository)
    oms = GenericOMS(repository, adapter)
    order_intent = make_intent(1)

    first = oms.submit_intent(order_intent, account=account(), risk_decision=decision(order_intent.id))
    second = oms.submit_intent(order_intent, account=account(), risk_decision=decision(order_intent.id))
    assert first["id"] == second["id"]
    assert len(adapter.submit_calls) == 1


def test_oms_propagates_the_explicit_extended_hours_toggle_to_the_adapter(tmp_path):
    repository = ready_repository(tmp_path, 1)
    adapter = FakeAdapter(repository)
    order_intent = replace(make_intent(1), execution_policy=ExecutionPolicy(allow_extended_hours=True))

    GenericOMS(repository, adapter).submit_intent(order_intent, account=account(), risk_decision=decision(order_intent.id))

    assert adapter.submit_calls[0].allow_extended_hours is True


def test_oms_propagates_the_explicit_overnight_session_to_the_adapter(tmp_path):
    repository = ready_repository(tmp_path, 1)
    adapter = FakeAdapter(repository)
    order_intent = replace(make_intent(1), execution_policy=ExecutionPolicy(execution_session=ExecutionSession.OVERNIGHT))

    GenericOMS(repository, adapter).submit_intent(order_intent, account=account(), risk_decision=decision(order_intent.id))

    assert adapter.submit_calls[0].execution_session is ExecutionSession.OVERNIGHT
    assert adapter.submit_calls[0].allow_extended_hours is False


def test_mixed_leg_states_remain_visible_after_later_rejection(tmp_path):
    repository = ready_repository(tmp_path, 3)
    adapter = FakeAdapter(repository, reject_call=2)
    oms = GenericOMS(repository, adapter)
    order_intent = make_intent(3)

    result = oms.submit_intent(order_intent, account=account(), risk_decision=decision(order_intent.id))

    assert result["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert [leg["status"] for leg in result["legs"]] == [
        LegStatus.WORKING.value,
        LegStatus.REJECTED.value,
        LegStatus.PLANNED.value,
    ]
    assert len(adapter.submit_calls) == 2


def test_definite_no_submit_rejection_stops_before_later_leg_and_stays_terminal(tmp_path):
    repository = ready_repository(tmp_path, 3)
    adapter = FakeAdapter(repository, reject_call=1)
    oms = GenericOMS(repository, adapter)
    order_intent = make_intent(3)

    result = oms.submit_intent(order_intent, account=account(), risk_decision=decision(order_intent.id))

    assert result["status"] == IntentStatus.REJECTED.value
    assert [leg["status"] for leg in result["legs"]] == [
        LegStatus.REJECTED.value,
        LegStatus.PLANNED.value,
        LegStatus.PLANNED.value,
    ]
    assert len(adapter.submit_calls) == 1
    assert repository.open_reconciliation_issues("acct") == []
    attempt = repository.broker_orders_for_leg(order_intent.legs[0].id)[0]
    assert attempt["status"] == BrokerOrderStatus.REJECTED.value
    assert attempt["external_order_id"] is None
    metadata = json.loads(attempt["metadata_json"])
    assert metadata["definite_no_submit"] is True
    assert metadata["error"] == "rejected"


def test_ambiguous_submit_is_not_retried_and_requires_reconciliation(tmp_path):
    repository = ready_repository(tmp_path, 1)
    adapter = FakeAdapter(repository, ambiguous_call=1)
    oms = GenericOMS(repository, adapter)
    order_intent = make_intent(1)

    result = oms.submit_intent(order_intent, account=account(), risk_decision=decision(order_intent.id))
    assert result["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert result["legs"][0]["status"] == LegStatus.RECONCILIATION_REQUIRED.value
    assert repository.broker_orders_for_leg(order_intent.legs[0].id)[0]["status"] == BrokerOrderStatus.UNKNOWN.value
    assert len(adapter.submit_calls) == 1


def test_recovery_repairs_persisted_clean_rejection_and_only_exact_issue(tmp_path):
    repository = ready_repository(tmp_path, 2)
    order_intent = make_intent(2)
    repository.create_intent(order_intent)
    repository.transition_intent(order_intent.id, IntentStatus.RISK_APPROVED)
    repository.transition_intent(order_intent.id, IntentStatus.SUBMITTING)
    repository.transition_leg(order_intent.legs[0].id, LegStatus.SUBMITTING)
    repository.create_broker_order(
        broker_order_id="rejected-attempt",
        order_leg_id=order_intent.legs[0].id,
        account_id="acct",
        broker="fake",
        attempt_number=1,
        client_order_id="rejected-client",
        submitted_quantity=Decimal("10"),
    )
    repository.transition_broker_order("rejected-attempt", BrokerOrderStatus.SUBMITTING)
    repository.record_submission(
        "rejected-attempt",
        status=BrokerOrderStatus.REJECTED,
        external_order_id=None,
        metadata={"provider_status": "REJECTED", "error": "Paper trading does not support overnight trading sessions"},
    )
    repository.transition_leg(order_intent.legs[0].id, LegStatus.REJECTED)
    repository.transition_intent(order_intent.id, IntentStatus.RECONCILIATION_REQUIRED)

    adapter = FakeAdapter(repository)
    oms = GenericOMS(repository, adapter)
    oms._require_reconciliation(
        order_intent.id,
        account(),
        category="BROKER_QUERY_FAILED",
        entity_type="ACCOUNT",
        entity_key="acct:fills",
        details={"message": "Paper trading does not support deal data."},
    )
    oms._require_reconciliation(
        order_intent.id,
        account(),
        category="UNRELATED",
        entity_type="ACCOUNT",
        entity_key="acct:unrelated",
        details={"message": "must remain open"},
    )

    class NoFillHistoryAdapter(FakeAdapter):
        def get_fills(self, _account, since=None):
            raise AssertionError("clean no-submit rejection must not query deal history")

    recovered = GenericOMS(repository, NoFillHistoryAdapter(repository)).recover_intent(
        order_intent.id,
        account=account(),
    )

    assert recovered["status"] == IntentStatus.REJECTED.value
    assert [leg["status"] for leg in recovered["legs"]] == [
        LegStatus.REJECTED.value,
        LegStatus.PLANNED.value,
    ]
    remaining = repository.open_reconciliation_issues("acct")
    assert [issue["issue_key"] for issue in remaining] == ["UNRELATED:ACCOUNT:acct:unrelated"]


def test_restart_recovers_exact_client_order_match_without_submit(tmp_path):
    repository = ready_repository(tmp_path, 1)
    order_intent = make_intent(1)
    repository.create_intent(order_intent)
    repository.transition_intent(order_intent.id, IntentStatus.RISK_APPROVED)
    repository.transition_intent(order_intent.id, IntentStatus.SUBMITTING)
    repository.transition_leg(order_intent.legs[0].id, LegStatus.SUBMITTING)
    repository.create_broker_order(
        broker_order_id="attempt",
        order_leg_id=order_intent.legs[0].id,
        account_id="acct",
        broker="fake",
        attempt_number=1,
        client_order_id="durable-client",
        submitted_quantity=Decimal("10"),
    )
    repository.transition_broker_order("attempt", BrokerOrderStatus.SUBMITTING)
    adapter = FakeAdapter(repository)
    adapter.open_orders = [
        BrokerOrderSnapshot(
            id="snapshot-order",
            broker_snapshot_id="snapshot",
            account_id="acct",
            instrument_id="instrument-0",
            external_order_id="external-recovered",
            client_order_id="durable-client",
            side=Side.BUY,
            quantity=Decimal("10"),
            filled_quantity=Decimal("0"),
            status=BrokerOrderStatus.WORKING,
            captured_at=NOW,
        )
    ]

    recovered = GenericOMS(repository, adapter).recover_intent(order_intent.id, account=account())
    assert recovered["status"] == IntentStatus.WORKING.value
    assert recovered["legs"][0]["status"] == LegStatus.WORKING.value
    assert repository.broker_orders_for_leg(order_intent.legs[0].id)[0]["external_order_id"] == "external-recovered"
    assert adapter.submit_calls == []


def test_restart_applies_broker_fill_facts_and_broker_truth_wins(tmp_path):
    repository = ready_repository(tmp_path, 1)
    adapter = FakeAdapter(repository)
    oms = GenericOMS(repository, adapter)
    order_intent = make_intent(1)
    oms.submit_intent(order_intent, account=account(), risk_decision=decision(order_intent.id))
    attempt = repository.broker_orders_for_leg(order_intent.legs[0].id)[0]
    adapter.open_orders = [
        BrokerOrderSnapshot(
            id="filled-order-fact",
            broker_snapshot_id="snapshot",
            account_id="acct",
            instrument_id="instrument-0",
            external_order_id=attempt["external_order_id"],
            client_order_id=attempt["client_order_id"],
            side=Side.BUY,
            quantity=Decimal("10"),
            filled_quantity=Decimal("10"),
            status=BrokerOrderStatus.FILLED,
            captured_at=NOW,
        )
    ]
    adapter.fills = [
        BrokerFill(
            external_order_id="older-unrelated-order",
            external_fill_id="older-unrelated-fill",
            dedupe_key="older-unrelated-fill",
            quantity=Decimal("1"),
            price=Decimal("50"),
            filled_at=NOW,
            received_at=NOW,
        ),
        BrokerFill(
            external_order_id=attempt["external_order_id"],
            external_fill_id="deal-recovered",
            dedupe_key="deal-recovered",
            quantity=Decimal("10"),
            price=Decimal("101"),
            filled_at=NOW,
            received_at=NOW,
        )
    ]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.FILLED.value
    assert recovered["legs"][0]["status"] == LegStatus.FILLED.value
    assert len(repository.fills_for_leg(order_intent.legs[0].id)) == 1
    assert repository.position_allocations("acct")[0]["signed_quantity"] == "10"
    assert repository.open_reconciliation_issues("acct") == []


def test_recovery_keeps_reconciliation_when_one_leg_is_only_partial(tmp_path):
    repository = ready_repository(tmp_path, 2)
    adapter = FakeAdapter(repository)
    oms = GenericOMS(repository, adapter)
    order_intent = make_intent(2)
    oms.submit_intent(order_intent, account=account(), risk_decision=decision(order_intent.id))
    attempts = [repository.broker_orders_for_leg(leg.id)[0] for leg in order_intent.legs]
    adapter.open_orders = [
        BrokerOrderSnapshot(
            id="filled-snapshot",
            broker_snapshot_id="snapshot",
            account_id="acct",
            instrument_id="instrument-0",
            external_order_id=attempts[0]["external_order_id"],
            client_order_id=attempts[0]["client_order_id"],
            side=Side.BUY,
            quantity=Decimal("10"),
            filled_quantity=Decimal("10"),
            status=BrokerOrderStatus.FILLED,
            captured_at=NOW,
        ),
        BrokerOrderSnapshot(
            id="partial-snapshot",
            broker_snapshot_id="snapshot",
            account_id="acct",
            instrument_id="instrument-1",
            external_order_id=attempts[1]["external_order_id"],
            client_order_id=attempts[1]["client_order_id"],
            side=Side.SELL,
            quantity=Decimal("10"),
            filled_quantity=Decimal("5"),
            status=BrokerOrderStatus.PARTIALLY_FILLED,
            captured_at=NOW,
        ),
    ]
    adapter.fills = [
        BrokerFill(
            external_order_id=attempts[0]["external_order_id"],
            external_fill_id=None,
            dedupe_key="synthetic-filled-leg-0",
            quantity=Decimal("10"),
            price=Decimal("101"),
            filled_at=NOW,
            received_at=NOW,
        )
    ]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert [leg["status"] for leg in recovered["legs"]] == [
        LegStatus.FILLED.value,
        LegStatus.RECONCILIATION_REQUIRED.value,
    ]
    assert any(issue["category"] == "FILL_EVIDENCE_REQUIRED" for issue in repository.open_reconciliation_issues("acct"))


def test_recovery_resolves_exact_unsupported_fill_issue_after_all_legs_are_evidenced(tmp_path):
    repository = ready_repository(tmp_path, 2)
    adapter = FakeAdapter(repository)
    oms = GenericOMS(repository, adapter)
    order_intent = make_intent(2)
    oms.submit_intent(order_intent, account=account(), risk_decision=decision(order_intent.id))
    attempts = [repository.broker_orders_for_leg(leg.id)[0] for leg in order_intent.legs]
    adapter.open_orders = [
        BrokerOrderSnapshot(
            id=f"filled-snapshot-{index}",
            broker_snapshot_id="snapshot",
            account_id="acct",
            instrument_id=f"instrument-{index}",
            external_order_id=attempt["external_order_id"],
            client_order_id=attempt["client_order_id"],
            side=Side.BUY if index == 0 else Side.SELL,
            quantity=Decimal("10"),
            filled_quantity=Decimal("10"),
            status=BrokerOrderStatus.FILLED,
            captured_at=NOW,
        )
        for index, attempt in enumerate(attempts)
    ]
    adapter.fills = [
        BrokerFill(
            external_order_id=attempt["external_order_id"],
            external_fill_id=None,
            dedupe_key=f"synthetic-filled-leg-{index}",
            quantity=Decimal("10"),
            price=Decimal("101") if index == 0 else Decimal("201"),
            filled_at=NOW,
            received_at=NOW,
        )
        for index, attempt in enumerate(attempts)
    ]
    oms._require_reconciliation(
        order_intent.id,
        account(),
        category="BROKER_QUERY_FAILED",
        entity_type="ACCOUNT",
        entity_key="acct:fills",
        details={"intent_id": order_intent.id, "message": "Paper trading does not support deal data."},
    )

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.FILLED.value
    assert [leg["status"] for leg in recovered["legs"]] == [LegStatus.FILLED.value, LegStatus.FILLED.value]
    assert repository.open_reconciliation_issues("acct") == []


def test_recovery_keeps_unsupported_or_ambiguous_fill_query_reconciliation(tmp_path):
    repository = ready_repository(tmp_path, 1)
    order_intent = make_intent(1)

    class BrokenHistoryAdapter(FakeAdapter):
        def get_fills(self, _account, since=None):
            raise RuntimeError("deal history transport failure")

    adapter = BrokenHistoryAdapter(repository)
    oms = GenericOMS(repository, adapter)
    oms.submit_intent(order_intent, account=account(), risk_decision=decision(order_intent.id))
    attempt = repository.broker_orders_for_leg(order_intent.legs[0].id)[0]
    adapter.open_orders = [
        BrokerOrderSnapshot(
            id="filled-snapshot",
            broker_snapshot_id="snapshot",
            account_id="acct",
            instrument_id="instrument-0",
            external_order_id=attempt["external_order_id"],
            client_order_id=attempt["client_order_id"],
            side=Side.BUY,
            quantity=Decimal("10"),
            filled_quantity=Decimal("10"),
            status=BrokerOrderStatus.FILLED,
            captured_at=NOW,
        )
    ]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert recovered["legs"][0]["status"] == LegStatus.RECONCILIATION_REQUIRED.value
    assert any(issue["category"] == "BROKER_QUERY_FAILED" for issue in repository.open_reconciliation_issues("acct"))


def test_open_reconciliation_issue_blocks_new_submission(tmp_path):
    repository = ready_repository(tmp_path, 1)
    adapter = FakeAdapter(repository, ambiguous_call=1)
    oms = GenericOMS(repository, adapter)
    first = make_intent(1)
    oms.submit_intent(first, account=account(), risk_decision=decision(first.id))
    second_id = "intent-blocked"
    second = replace(
        make_intent(1, key="intent-blocked-key"),
        id=second_id,
        legs=(replace(first.legs[0], id="blocked-leg", intent_id=second_id),),
    )

    result = oms.submit_intent(second, account=account(), risk_decision=decision(second.id))

    assert result["status"] == IntentStatus.REJECTED.value
    assert len(adapter.submit_calls) == 1


def test_fill_evidence_drives_completion(tmp_path):
    repository = ready_repository(tmp_path, 1)
    adapter = FakeAdapter(repository)
    oms = GenericOMS(repository, adapter)
    order_intent = make_intent(1)
    oms.submit_intent(order_intent, account=account(), risk_decision=decision(order_intent.id))
    attempt = repository.broker_orders_for_leg(order_intent.legs[0].id)[0]

    oms.apply_fill(
        Fill(
            id="fill",
            broker_order_id=attempt["id"],
            order_leg_id=order_intent.legs[0].id,
            external_fill_id="deal",
            dedupe_key="deal",
            quantity=Decimal("10"),
            price=Decimal("100"),
            filled_at=NOW,
            received_at=NOW,
        )
    )
    assert repository.get_intent(order_intent.id)["status"] == IntentStatus.FILLED.value
    assert oms.complete_intent(order_intent.id)["status"] == IntentStatus.COMPLETED.value


def test_rejected_risk_is_durable_and_never_submitted(tmp_path):
    repository = ready_repository(tmp_path, 1)
    adapter = FakeAdapter(repository)
    oms = GenericOMS(repository, adapter)
    order_intent = make_intent(1)

    result = oms.submit_intent(order_intent, account=account(), risk_decision=decision(order_intent.id, False))
    assert result["status"] == IntentStatus.REJECTED.value
    assert adapter.submit_calls == []


def test_only_filled_intent_can_be_completed(tmp_path):
    repository = ready_repository(tmp_path, 1)
    adapter = FakeAdapter(repository)
    oms = GenericOMS(repository, adapter)
    order_intent = make_intent(1)
    oms.submit_intent(order_intent, account=account(), risk_decision=decision(order_intent.id))
    with pytest.raises(OMSExecutionError):
        oms.complete_intent(order_intent.id)
