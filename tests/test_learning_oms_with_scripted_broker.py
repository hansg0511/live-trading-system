"""Focused learning test for immediate-fill submission reconciliation."""
from __future__ import annotations

from decimal import Decimal
import json

from src.trading_core.domain import (
    Account,
    AssetClass,
    BrokerOrderStatus,
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
from src.trading_core.oms import GenericOMS
from src.trading_core.repository import SQLiteTradingRepository
from tests.scripted_broker import ManualClock, ScriptedBroker


def _account(clock: ManualClock) -> Account:
    return Account(
        id="learning-account",
        broker="FAKE",
        environment=TradingEnvironment.SIM,
        external_account_id="learning-external-account",
        base_currency="USD",
        created_at=clock(),
        updated_at=clock(),
    )


def _intent(intent_id: str, idempotency_key: str, clock: ManualClock) -> OrderIntent:
    return OrderIntent(
        id=intent_id,
        idempotency_key=idempotency_key,
        strategy_id="learning-strategy",
        account_id="learning-account",
        action=IntentAction.ENTER,
        legs=(
            OrderLeg(
                id=f"{intent_id}-leg",
                intent_id=intent_id,
                sequence=0,
                instrument_id="QQQ",
                side=Side.BUY,
                quantity=Decimal("10"),
                quantity_unit=QuantityUnit.UNITS,
                order_type="MARKET",
                created_at=clock(),
                updated_at=clock(),
            ),
        ),
        created_at=clock(),
        updated_at=clock(),
    )


def _approved_decision(intent_id: str, clock: ManualClock) -> RiskDecisionRecord:
    return RiskDecisionRecord(
        id=f"risk-{intent_id}",
        intent_id=intent_id,
        approved=True,
        reason="approved for deterministic offline learning test",
        checks={"offline_test": True},
        evaluated_at=clock(),
    )


def _ready_repository(tmp_path, account: Account, clock: ManualClock) -> SQLiteTradingRepository:
    repository = SQLiteTradingRepository(tmp_path / "learning-oms.db")
    repository.initialize()
    repository.save_account(account)
    repository.save_strategy(
        Strategy(
            id="learning-strategy",
            name="learning OMS test",
            strategy_type="test",
            created_at=clock(),
            updated_at=clock(),
        )
    )
    repository.save_instrument(
        Instrument(
            id="QQQ",
            asset_class=AssetClass.EQUITY,
            symbol="QQQ",
            venue="NASDAQ",
            currency="USD",
            created_at=clock(),
            updated_at=clock(),
        )
    )
    return repository


def _submit_immediate_fill(tmp_path):
    """Create one real immediate broker fill, then submit it through the OMS."""
    clock = ManualClock()
    account = _account(clock)
    repository = _ready_repository(tmp_path, account, clock)
    broker = ScriptedBroker(account, clock=clock)
    broker.queue_submit("immediate_fill")
    oms = GenericOMS(repository, broker, clock=clock)
    order_intent = _intent("immediate-intent", "immediate-key", clock)

    submitted = oms.submit_intent(
        order_intent,
        account=account,
        risk_decision=_approved_decision(order_intent.id, clock),
    )
    return {
        "account": account,
        "broker": broker,
        "clock": clock,
        "intent": order_intent,
        "oms": oms,
        "repository": repository,
        "submitted": submitted,
    }


def test_oms_immediate_fill_during_submission_requires_reconciliation(tmp_path):
    state = _submit_immediate_fill(tmp_path)
    account = state["account"]
    broker = state["broker"]
    order_intent = state["intent"]
    repository = state["repository"]
    submitted = state["submitted"]
    leg_id = order_intent.legs[0].id
    attempts = repository.broker_orders_for_leg(leg_id)
    local_fills = repository.fills_for_leg(leg_id)
    initial_issues = repository.open_reconciliation_issues(account.id)
    initial_actions = repository.open_recovery_actions(account.id)

    # The fake broker knows the full execution and position. The submission
    # response carries only cumulative quantity, so SQLite has no allocatable
    # deal yet and the OMS quarantines the intent for reconciliation.
    assert len(broker.orders) == 1
    broker_order = next(iter(broker.orders.values()))
    assert broker_order.status is BrokerOrderStatus.FILLED
    assert broker_order.filled == Decimal("10")
    assert len(broker.deals) == 1
    assert broker.deals[0].price == Decimal("500")
    assert broker.positions["QQQ"] == Decimal("10")
    assert len(broker.get_authoritative_account_facts(account).fills) == 1
    assert len(broker.submit_calls) == 1
    assert len(attempts) == 1
    assert attempts[0]["status"] == BrokerOrderStatus.FILLED.value
    assert attempts[0]["external_order_id"] == broker_order.external_id
    assert local_fills == []
    assert submitted["legs"][0]["status"] == LegStatus.RECONCILIATION_REQUIRED.value
    assert submitted["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert {issue["category"] for issue in initial_issues} == {
        "BROKER_SUBMISSION_FILL_EVIDENCE",
        "SUBMISSION_AMBIGUITY",
    }
    submission_issue = next(
        issue for issue in initial_issues
        if issue["category"] == "BROKER_SUBMISSION_FILL_EVIDENCE"
    )
    assert json.loads(submission_issue["details_json"])["reported_cumulative_fill"] == "10"
    assert any(action["action_key"].startswith("SUBMISSION_FILL_EVIDENCE:") for action in initial_actions)


def test_oms_immediate_fill_recovery_is_idempotent_and_blocks_new_admission(tmp_path):
    state = _submit_immediate_fill(tmp_path)
    account = state["account"]
    broker = state["broker"]
    clock = state["clock"]
    order_intent = state["intent"]
    oms = state["oms"]
    repository = state["repository"]
    leg_id = order_intent.legs[0].id
    attempts = repository.broker_orders_for_leg(leg_id)

    # Recovery now sees the authoritative individual deal that the immediate
    # submission response could not turn into allocatable local fill evidence.
    recovered_once = oms.recover_intent(order_intent.id, account=account)
    recovered_fills_once = repository.fills_for_leg(leg_id)
    issues_once = repository.open_reconciliation_issues(account.id)
    actions_once = repository.open_recovery_actions(account.id)
    recovered_twice = oms.recover_intent(order_intent.id, account=account)
    recovered_fills_twice = repository.fills_for_leg(leg_id)
    issues_twice = repository.open_reconciliation_issues(account.id)
    actions_twice = repository.open_recovery_actions(account.id)

    # Recovery associates the normalized broker deal with the original
    # attempt and updates the owned fill ledger. The original submission
    # ambiguity records remain open, so the intent itself stays quarantined.
    assert recovered_once["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert recovered_once["legs"][0]["status"] == LegStatus.FILLED.value
    assert recovered_twice["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert recovered_twice["legs"][0]["status"] == LegStatus.FILLED.value
    assert len(recovered_fills_once) == 1
    assert recovered_fills_once == recovered_fills_twice
    assert recovered_fills_once[0]["broker_order_id"] == attempts[0]["id"]
    assert recovered_fills_once[0]["external_fill_id"] == broker.deals[0].external_fill_id
    assert Decimal(recovered_fills_once[0]["quantity"]) == Decimal("10")
    assert Decimal(recovered_fills_once[0]["price"]) == Decimal("500")
    assert {issue["issue_key"] for issue in issues_once} == {issue["issue_key"] for issue in issues_twice}
    assert {action["action_key"] for action in actions_once} == {action["action_key"] for action in actions_twice}
    assert {issue["category"] for issue in issues_twice} == {
        "BROKER_SUBMISSION_FILL_EVIDENCE",
        "SUBMISSION_AMBIGUITY",
    }
    assert [action["action_key"] for action in actions_twice] == [
        f"SUBMISSION_FILL_EVIDENCE:{attempts[0]['id']}"
    ]
    assert len(broker.submit_calls) == 1
    assert len(broker.orders) == 1

    second_intent = _intent("second-intent", "second-key", clock)
    second_result = oms.submit_intent(
        second_intent,
        account=account,
        risk_decision=_approved_decision(second_intent.id, clock),
    )
    final_issues = repository.open_reconciliation_issues(account.id)
    final_actions = repository.open_recovery_actions(account.id)
    # The real long position is now represented by the recovered local fill.
    # Admission is nevertheless stopped by account-wide reconciliation debt.
    assert broker.positions["QQQ"] == Decimal("10")
    assert second_result["status"] == IntentStatus.REJECTED.value
    assert second_result["legs"][0]["status"] == LegStatus.PLANNED.value
    with repository.transaction() as conn:
        blocked_decision = conn.execute(
            "SELECT reason FROM core_risk_decisions WHERE id = ?",
            (f"risk-{second_intent.id}:reconciliation-block",),
        ).fetchone()
    assert blocked_decision is not None
    assert blocked_decision["reason"] == (
        "blocked by 2 open reconciliation issue(s) and 1 open recovery action(s)"
    )
    assert {issue["issue_key"] for issue in final_issues} == {issue["issue_key"] for issue in issues_twice}
    assert {action["action_key"] for action in final_actions} == {action["action_key"] for action in actions_twice}
    assert len(broker.submit_calls) == 1
