from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
import json

import pytest

from src.trading_core.domain import (
    Account,
    AssetClass,
    BrokerOrderEvent,
    BrokerOrderSnapshot,
    BrokerOrderStatus,
    ExecutionEvidenceMode,
    ExecutionPolicy,
    ExecutionSession,
    Fill,
    Instrument,
    IntentAction,
    IntentStatus,
    LegStatus,
    OrderIntent,
    OrderLeg,
    PositionSnapshot,
    QuantityUnit,
    RiskDecisionRecord,
    Side,
    Strategy,
    TradingEnvironment,
)
from src.trading_core.oms import GenericOMS, OMSExecutionError
from src.trading_core.ports import BrokerFactSnapshot, BrokerFill, BrokerSubmissionResult
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


def make_oms(repository, adapter):
    """Keep fake broker timestamps aligned with the test clock."""
    return GenericOMS(repository, adapter, clock=lambda: NOW)


class FakeAdapter:
    def __init__(self, repository, *, reject_call=None, ambiguous_call=None, inspect_persistence=False):
        self.repository = repository
        self.reject_call = reject_call
        self.ambiguous_call = ambiguous_call
        self.inspect_persistence = inspect_persistence
        self.submit_calls = []
        self.open_orders = []
        self.fills = []
        self.positions = []

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
                cumulative_filled_quantity=Decimal("0"),
                no_submit_asserted=True,
                no_fill_asserted=True,
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

    def get_positions(self, account_value):
        return tuple(self.positions)

    def get_fills(self, account_value, since=None):
        return tuple(self.fills)

    def get_account_facts(self, account_value):
        return BrokerFactSnapshot(
            account_id=account_value.id,
            captured_at=NOW,
            complete=True,
            positions=tuple(self.positions),
            open_orders=tuple(self.open_orders),
            fills=tuple(self.fills),
        )

    def get_authoritative_account_facts(self, account_value):
        return self.get_account_facts(account_value)

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
            cumulative_filled_quantity=Decimal("0"),
            no_fill_asserted=True,
        )


class UncertainRejectAdapter(FakeAdapter):
    def submit_order(self, account_value, request):
        self.submit_calls.append(request)
        return BrokerSubmissionResult(
            broker_order_id=request.broker_order_id,
            accepted=False,
            status=BrokerOrderStatus.REJECTED,
            external_order_id=None,
            client_order_id=request.client_order_id,
            cumulative_filled_quantity=None,
            no_submit_asserted=False,
            no_fill_asserted=False,
        )


@pytest.mark.parametrize("count", [1, 2, 3])
def test_oms_persists_all_legs_before_submit_and_supports_n_legs(tmp_path, count):
    repository = ready_repository(tmp_path, count)
    adapter = FakeAdapter(repository, inspect_persistence=True)
    oms = make_oms(repository, adapter)
    order_intent = make_intent(count)

    result = oms.submit_intent(order_intent, account=account(), risk_decision=decision(order_intent.id))

    assert result["status"] == IntentStatus.WORKING.value
    assert len(adapter.submit_calls) == count
    assert [item.order_leg.sequence for item in adapter.submit_calls] == list(range(count))


def test_duplicate_intent_returns_existing_state_without_resubmission(tmp_path):
    repository = ready_repository(tmp_path, 1)
    adapter = FakeAdapter(repository)
    oms = make_oms(repository, adapter)
    order_intent = make_intent(1)

    first = oms.submit_intent(order_intent, account=account(), risk_decision=decision(order_intent.id))
    second = oms.submit_intent(order_intent, account=account(), risk_decision=decision(order_intent.id))
    assert first["id"] == second["id"]
    assert len(adapter.submit_calls) == 1


def test_oms_propagates_the_explicit_extended_hours_toggle_to_the_adapter(tmp_path):
    repository = ready_repository(tmp_path, 1)
    adapter = FakeAdapter(repository)
    order_intent = replace(make_intent(1), execution_policy=ExecutionPolicy(allow_extended_hours=True))

    make_oms(repository, adapter).submit_intent(order_intent, account=account(), risk_decision=decision(order_intent.id))

    assert adapter.submit_calls[0].allow_extended_hours is True


def test_oms_propagates_the_explicit_overnight_session_to_the_adapter(tmp_path):
    repository = ready_repository(tmp_path, 1)
    adapter = FakeAdapter(repository)
    order_intent = replace(make_intent(1), execution_policy=ExecutionPolicy(execution_session=ExecutionSession.OVERNIGHT))

    make_oms(repository, adapter).submit_intent(order_intent, account=account(), risk_decision=decision(order_intent.id))

    assert adapter.submit_calls[0].execution_session is ExecutionSession.OVERNIGHT
    assert adapter.submit_calls[0].allow_extended_hours is False


def test_mixed_leg_states_remain_visible_after_later_rejection(tmp_path):
    repository = ready_repository(tmp_path, 3)
    adapter = FakeAdapter(repository, reject_call=2)
    oms = make_oms(repository, adapter)
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
    oms = make_oms(repository, adapter)
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


def test_rejection_without_explicit_no_submit_contract_stays_blocked(tmp_path):
    repository = ready_repository(tmp_path, 2)
    adapter = UncertainRejectAdapter(repository)
    oms = make_oms(repository, adapter)
    order_intent = make_intent(2)

    result = oms.submit_intent(order_intent, account=account(), risk_decision=decision(order_intent.id))

    assert result["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert len(adapter.submit_calls) == 1
    assert result["legs"][0]["status"] == LegStatus.RECONCILIATION_REQUIRED.value
    assert result["legs"][1]["status"] == LegStatus.PLANNED.value
    assert repository.open_reconciliation_issues("acct")


@pytest.mark.parametrize("raw_payload", ({"fill_blob": {"provider_field": "unknown"}}, {"execution_detail": "opaque"}))
def test_unrecognized_fill_shaped_rejection_payload_is_not_clean(tmp_path, raw_payload):
    class OpaqueRejectAdapter(FakeAdapter):
        def submit_order(self, account_value, request):
            self.submit_calls.append(request)
            return BrokerSubmissionResult(
                broker_order_id=request.broker_order_id,
                accepted=False,
                status=BrokerOrderStatus.REJECTED,
                external_order_id=None,
                cumulative_filled_quantity=Decimal("0"),
                no_submit_asserted=True,
                no_fill_asserted=True,
                raw_payload=raw_payload,
            )

    repository = ready_repository(tmp_path, 1)
    adapter = OpaqueRejectAdapter(repository)
    result = make_oms(repository, adapter).submit_intent(
        make_intent(1),
        account=account(),
        risk_decision=decision("intent-1"),
    )

    assert result["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert result["legs"][0]["status"] == LegStatus.RECONCILIATION_REQUIRED.value


def test_ambiguous_submit_is_not_retried_and_requires_reconciliation(tmp_path):
    repository = ready_repository(tmp_path, 1)
    adapter = FakeAdapter(repository, ambiguous_call=1)
    oms = make_oms(repository, adapter)
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
        metadata={
            "accepted": False,
            "ambiguous": False,
            "provider_status": "REJECTED",
            "error": "Paper trading does not support overnight trading sessions",
            "reported_cumulative_fill": "0",
            "cumulative_fill_known": True,
            "no_submit_asserted": True,
            "no_fill_asserted": True,
            "definite_no_submit": True,
            "raw_payload": {},
        },
    )
    repository.transition_leg(order_intent.legs[0].id, LegStatus.REJECTED)
    repository.transition_intent(order_intent.id, IntentStatus.RECONCILIATION_REQUIRED)

    adapter = FakeAdapter(repository)
    oms = make_oms(repository, adapter)
    oms._require_reconciliation(
        order_intent.id,
        account(),
        category="BROKER_QUERY_FAILED",
        entity_type="ACCOUNT",
        entity_key="acct:fills",
        details={
            "intent_id": order_intent.id,
            "message": "Paper trading does not support deal data.",
        },
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

    recovered = make_oms(repository, NoFillHistoryAdapter(repository)).recover_intent(
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
        now=NOW,
    )
    repository.transition_broker_order("attempt", BrokerOrderStatus.SUBMITTING, now=NOW)
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

    recovered = GenericOMS(repository, adapter, clock=lambda: NOW).recover_intent(order_intent.id, account=account())
    assert recovered["status"] == IntentStatus.WORKING.value
    assert recovered["legs"][0]["status"] == LegStatus.WORKING.value
    assert repository.broker_orders_for_leg(order_intent.legs[0].id)[0]["external_order_id"] == "external-recovered"
    assert adapter.submit_calls == []


def test_restart_applies_broker_fill_facts_and_broker_truth_wins(tmp_path):
    repository = ready_repository(tmp_path, 1)
    adapter = FakeAdapter(repository)
    oms = GenericOMS(repository, adapter, clock=lambda: NOW)
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
            instrument_id="instrument-0",
        )
    ]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert recovered["legs"][0]["status"] == LegStatus.WORKING.value
    assert repository.fills_for_leg(order_intent.legs[0].id) == []
    assert repository.position_allocations("acct") == []
    assert any(
        issue["category"] == "BROKER_FACT_UNAVAILABLE"
        for issue in repository.open_reconciliation_issues("acct")
    )


def test_recovery_keeps_reconciliation_when_one_leg_is_only_partial(tmp_path):
    repository = ready_repository(tmp_path, 2)
    adapter = FakeAdapter(repository)
    oms = GenericOMS(repository, adapter, clock=lambda: NOW)
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
            instrument_id="instrument-0",
        )
    ]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert [leg["status"] for leg in recovered["legs"]] == [
        LegStatus.FILLED.value,
        LegStatus.RECONCILIATION_REQUIRED.value,
    ]
    assert any(
        issue["category"] in {"BROKER_SNAPSHOT_MISMATCH", "FILL_EVIDENCE_REQUIRED"}
        for issue in repository.open_reconciliation_issues("acct")
    )


def test_recovery_resolves_exact_unsupported_fill_issue_after_all_legs_are_evidenced(tmp_path):
    repository = ready_repository(tmp_path, 2)
    adapter = FakeAdapter(repository)
    oms = GenericOMS(repository, adapter, clock=lambda: NOW)
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
            instrument_id=f"instrument-{index}",
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
        def __init__(self, repository):
            super().__init__(repository)
            self.fail_history = False

        def get_fills(self, _account, since=None):
            if self.fail_history:
                raise RuntimeError("deal history transport failure")
            return super().get_fills(_account, since=since)

    adapter = BrokenHistoryAdapter(repository)
    oms = GenericOMS(repository, adapter, clock=lambda: NOW)
    oms.submit_intent(order_intent, account=account(), risk_decision=decision(order_intent.id))
    adapter.fail_history = True
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
    assert any(issue["category"] == "BROKER_SNAPSHOT_MISMATCH" for issue in repository.open_reconciliation_issues("acct"))


def test_retryable_position_poll_stops_one_recovery_cycle_without_duplicate_poll_blockers(tmp_path):
    class RetryableReadError(RuntimeError):
        retryable = True

    class RateLimitedAdapter(FakeAdapter):
        def __init__(self, repository):
            super().__init__(repository)
            self.rate_limited = False
            self.position_calls = 0
            self.order_calls = 0
            self.fill_calls = 0

        def get_positions(self, account_value):
            self.position_calls += 1
            if self.rate_limited:
                raise RetryableReadError("provider rate limit")
            return super().get_positions(account_value)

        def get_open_orders(self, account_value):
            self.order_calls += 1
            return super().get_open_orders(account_value)

        def get_fills(self, account_value, since=None):
            self.fill_calls += 1
            return super().get_fills(account_value, since=since)

    repository = ready_repository(tmp_path, 1)
    adapter = RateLimitedAdapter(repository)
    oms = GenericOMS(repository, adapter, clock=lambda: NOW)
    intent = make_intent(1)
    oms.submit_intent(intent, account=account(), risk_decision=decision(intent.id))
    adapter.rate_limited = True

    first = oms.recover_intent(intent.id, account=account())
    second = oms.recover_intent(intent.id, account=account())

    assert first["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert second["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert adapter.position_calls == 0
    assert adapter.order_calls == 0
    assert adapter.fill_calls == 0
    actions = repository.open_recovery_actions("acct")
    assert actions and any(
        item["action_key"].startswith(("AMBIGUOUS_ORDER:", "TERMINAL_ORDER_EVIDENCE:"))
        for item in actions
    )
    issues = repository.open_reconciliation_issues("acct")
    assert issues and any(
        item["category"] in {"AMBIGUOUS_ORDER_MATCH", "BROKER_TERMINAL_EVIDENCE_MISSING"}
        for item in issues
    )


def test_open_reconciliation_issue_blocks_new_submission(tmp_path):
    repository = ready_repository(tmp_path, 1)
    adapter = FakeAdapter(repository, ambiguous_call=1)
    oms = make_oms(repository, adapter)
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
    oms = GenericOMS(repository, adapter, clock=lambda: NOW)
    order_intent = make_intent(1)
    oms.submit_intent(order_intent, account=account(), risk_decision=decision(order_intent.id))
    attempt = repository.broker_orders_for_leg(order_intent.legs[0].id)[0]

    oms._ingest_validated_broker_order_event(
        BrokerOrderEvent(
            id="event-fill",
            broker_order_id=attempt["id"],
            dedupe_key="event-fill",
            event_type="ORDER_STATUS",
            event_at=NOW,
            received_at=NOW,
            broker_status=BrokerOrderStatus.FILLED,
            external_order_id=attempt["external_order_id"],
            cumulative_filled_quantity=Decimal("10"),
            account_id=account().id,
            external_account_id=account().external_account_id,
        ),
        account=account(),
        fills=(
            BrokerFill(
                external_order_id=attempt["external_order_id"],
                external_fill_id="deal",
                dedupe_key="deal",
                quantity=Decimal("10"),
                price=Decimal("100"),
                filled_at=NOW,
                received_at=NOW,
                account_id=account().id,
                evidence_reference="deal",
            ),
        ),
    )
    assert repository.get_intent(order_intent.id)["status"] == IntentStatus.FILLED.value
    assert oms.complete_intent(order_intent.id)["status"] == IntentStatus.COMPLETED.value


def test_rejected_risk_is_durable_and_never_submitted(tmp_path):
    repository = ready_repository(tmp_path, 1)
    adapter = FakeAdapter(repository)
    oms = make_oms(repository, adapter)
    order_intent = make_intent(1)

    result = oms.submit_intent(order_intent, account=account(), risk_decision=decision(order_intent.id, False))
    assert result["status"] == IntentStatus.REJECTED.value
    assert adapter.submit_calls == []


def test_only_filled_intent_can_be_completed(tmp_path):
    repository = ready_repository(tmp_path, 1)
    adapter = FakeAdapter(repository)
    oms = make_oms(repository, adapter)
    order_intent = make_intent(1)
    oms.submit_intent(order_intent, account=account(), risk_decision=decision(order_intent.id))
    with pytest.raises(OMSExecutionError):
        oms.complete_intent(order_intent.id)


@pytest.mark.parametrize(
    "variant",
    (
        {"external_account_id": "different-external"},
        {"broker": "other-broker"},
        {"environment": TradingEnvironment.LIVE},
        {"enabled": False},
    ),
)
def test_recovery_status_rejects_every_noncanonical_account_component(tmp_path, variant):
    repository = ready_repository(tmp_path, 1)
    adapter = FakeAdapter(repository)
    oms = make_oms(repository, adapter)
    order_intent = make_intent(1)
    oms.submit_intent(order_intent, account=account(), risk_decision=decision(order_intent.id))

    status = oms.recovery_status(order_intent.id, account=replace(account(), **variant))

    assert status["safe_to_submit"] is False
    assert any(
        issue["category"] == "ACCOUNT_CANONICAL_IDENTITY_MISMATCH"
        for issue in repository.open_reconciliation_issues("acct")
    )
    assert any(
        action["action_key"] == "ACCOUNT_CANONICAL_IDENTITY_MISMATCH:recovery_status"
        for action in repository.recovery_actions_for_intent(order_intent.id)
    )
    assert adapter.submit_calls


def test_recovery_status_is_not_safe_with_fresh_foreign_nonzero_position(tmp_path):
    repository = ready_repository(tmp_path, 1)
    adapter = FakeAdapter(repository, reject_call=1)
    oms = make_oms(repository, adapter)
    order_intent = make_intent(1)

    submitted = oms.submit_intent(order_intent, account=account(), risk_decision=decision(order_intent.id))
    assert submitted["status"] == IntentStatus.REJECTED.value

    adapter.positions = [
        PositionSnapshot(
            id="foreign-position",
            broker_snapshot_id="foreign-snapshot",
            account_id=account().id,
            instrument_id="instrument-0",
            signed_quantity=Decimal("1"),
            average_price=Decimal("100"),
            captured_at=NOW,
        )
    ]

    status = oms.recovery_status(order_intent.id, account=account())

    assert status["fresh_authoritative_facts_valid"] is False
    assert status["fresh_exposure_agrees"] is False
    assert status["fresh_broker_positions"] == {"instrument-0": "1"}
    assert status["durable_managed_exposure"] == {}
    assert "do not match durable managed exposure" in status["fresh_exposure_error"]
    assert status["safe_to_submit"] is False


def test_recovery_status_is_not_safe_without_verified_cumulative_baseline(tmp_path):
    repository = ready_repository(tmp_path, 1)
    adapter = FakeAdapter(repository)
    oms = make_oms(repository, adapter)
    order_intent = make_intent(1)
    oms.submit_intent(
        order_intent,
        account=account(),
        risk_decision=decision(order_intent.id, False),
    )

    adapter.get_authoritative_account_facts = lambda account_value: BrokerFactSnapshot(  # type: ignore[method-assign]
        account_id=account_value.id,
        captured_at=NOW,
        complete=True,
        execution_evidence_mode=ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS,
    )

    status = oms.recovery_status(order_intent.id, account=account())

    assert status["fresh_authoritative_facts_valid"] is False
    assert status["fresh_execution_evidence_ready"] is False
    assert "verified cumulative-order baseline" in status["fresh_execution_evidence_error"]
    assert status["safe_to_submit"] is False


def test_new_submit_rejects_noncanonical_account_before_persisting_or_calling_adapter(tmp_path):
    repository = ready_repository(tmp_path, 1)
    adapter = FakeAdapter(repository)
    oms = make_oms(repository, adapter)
    order_intent = make_intent(1)

    with pytest.raises(OMSExecutionError, match="canonical account"):
        oms.submit_intent(
            order_intent,
            account=replace(account(), external_account_id="wrong-external"),
            risk_decision=decision(order_intent.id),
        )

    assert repository.get_intent(order_intent.id) is None
    assert adapter.submit_calls == []


def test_persisted_disabled_account_blocks_accountless_status(tmp_path):
    repository = ready_repository(tmp_path, 1)
    adapter = FakeAdapter(repository)
    oms = make_oms(repository, adapter)
    order_intent = make_intent(1)
    oms.submit_intent(order_intent, account=account(), risk_decision=decision(order_intent.id))
    with repository.transaction() as conn:
        conn.execute("UPDATE core_accounts SET enabled = 0 WHERE id = ?", (account().id,))

    status = oms.recovery_status(order_intent.id)

    assert status["safe_to_submit"] is False
    assert any(
        issue["category"] == "ACCOUNT_CANONICAL_IDENTITY_MISMATCH"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_accountless_recovery_status_loads_canonical_account_and_quarantines_multiple_attempts(tmp_path):
    repository = ready_repository(tmp_path, 1)
    adapter = FakeAdapter(repository)
    oms = make_oms(repository, adapter)
    order_intent = make_intent(1)
    oms.submit_intent(order_intent, account=account(), risk_decision=decision(order_intent.id))
    first = repository.broker_orders_for_leg(order_intent.legs[0].id)[0]
    repository.create_broker_order(
        broker_order_id="second-attempt",
        order_leg_id=order_intent.legs[0].id,
        account_id="acct",
        broker="fake",
        attempt_number=None,
        client_order_id=None,
        submitted_quantity=Decimal("10"),
    )

    status = oms.recovery_status(order_intent.id)

    assert status["safe_to_submit"] is False
    assert any(
        issue["category"] == "MULTIPLE_ATTEMPTS_UNSUPPORTED"
        for issue in repository.open_reconciliation_issues("acct")
    )
    assert first["id"]


def test_open_action_on_other_intent_blocks_completion_account_wide(tmp_path):
    repository = ready_repository(tmp_path, 1)
    adapter = FakeAdapter(repository)
    oms = make_oms(repository, adapter)
    first = make_intent(1, key="first-key")
    oms.submit_intent(first, account=account(), risk_decision=decision(first.id))

    second_id = "second-intent"
    second_leg = replace(first.legs[0], id="second-leg", intent_id=second_id)
    second = replace(first, id=second_id, idempotency_key="second-key", legs=(second_leg,))
    oms.submit_intent(second, account=account(), risk_decision=decision(second.id))
    attempt = repository.broker_orders_for_leg(second_leg.id)[0]
    repository.record_fill(
        Fill(
            id="second-fill",
            broker_order_id=attempt["id"],
            order_leg_id=second_leg.id,
            external_fill_id="second-deal",
            dedupe_key="second-deal",
            quantity=Decimal("10"),
            price=Decimal("100"),
            filled_at=NOW,
            received_at=NOW,
            account_id=account().id,
            external_order_id=attempt["external_order_id"],
            evidence_reference="second-deal",
        ),
        _validation_token=repository._fill_validation_capability(),
    )
    first_record = repository.get_intent(first.id)
    assert first_record is not None
    oms._record_recovery_action(
        intent=first_record,
        account=account(),
        action_key="ACCOUNT_WIDE_OPERATOR_BLOCK",
        state="RECONCILIATION_REQUIRED",
        summary="account-wide blocker",
        observed_positions={"_status": "RECONCILIATION_REQUIRED"},
        remaining_quantities={"first": "10"},
    )

    with pytest.raises(OMSExecutionError, match="reconciliation issues or recovery actions"):
        oms.complete_intent(second.id)


def test_sibling_multi_attempt_leg_blocks_new_account_submit(tmp_path):
    repository = ready_repository(tmp_path, 1)
    adapter = FakeAdapter(repository)
    oms = make_oms(repository, adapter)
    first = make_intent(1, key="multi-sibling-source")
    oms.submit_intent(first, account=account(), risk_decision=decision(first.id))
    repository.create_broker_order(
        broker_order_id="sibling-second-attempt",
        order_leg_id=first.legs[0].id,
        account_id=account().id,
        broker=account().broker,
        attempt_number=None,
        client_order_id=None,
        submitted_quantity=Decimal("10"),
    )

    new_id = "multi-sibling-new"
    new_leg = replace(first.legs[0], id="multi-sibling-new-leg", intent_id=new_id)
    new_intent = replace(
        first,
        id=new_id,
        idempotency_key="multi-sibling-new-key",
        legs=(new_leg,),
    )
    result = oms.submit_intent(new_intent, account=account(), risk_decision=decision(new_id))

    assert result["status"] == IntentStatus.REJECTED.value
    assert len(adapter.submit_calls) == 1
    assert any(
        issue["category"] == "MULTIPLE_ATTEMPTS_UNSUPPORTED"
        for issue in repository.open_reconciliation_issues(account().id)
    )
