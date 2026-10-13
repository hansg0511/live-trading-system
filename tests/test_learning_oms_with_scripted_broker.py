"""A small, readable integration exercise: real OMS + real SQLite + fake broker.

No Moomoo connection and no real orders. Start with this single test, then
add new incident scenarios only after you understand the normal lifecycle.
"""

from decimal import Decimal

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


def test_oms_submits_and_recovers_one_delayed_fill(tmp_path):
    # 1. Create an isolated database, fake clock, and test account.
    clock = ManualClock()
    account = Account(
        id="learning-account",
        broker="FAKE",
        environment=TradingEnvironment.SIM,
        external_account_id="learning-external-account",
        base_currency="USD",
    )

    repository = SQLiteTradingRepository(tmp_path / "learning_oms.db")
    repository.initialize()
    repository.save_account(account)
    repository.save_strategy(
        Strategy(
            id="learning-strategy",
            name="Learning OMS",
            strategy_type="test",
            version="1",
        )
    )
    repository.save_instrument(
        Instrument(
            id="QQQ",
            asset_class=AssetClass.EQUITY,
            symbol="QQQ",
            venue="TEST",
            currency="USD",
        )
    )

    # 2. Connect the real OMS to ScriptedBroker (not Moomoo).
    broker = ScriptedBroker(account, clock=clock)
    oms = GenericOMS(repository, broker, clock=clock)

    # 3. Define one logical intent: BUY 10 QQQ.
    leg = OrderLeg(
        id="learning-leg-1",
        intent_id="learning-intent-1",
        sequence=0,
        instrument_id="QQQ",
        side=Side.BUY,
        quantity=Decimal("10"),
        quantity_unit=QuantityUnit.UNITS,
        order_type="MARKET",
        status=LegStatus.PLANNED,
    )
    order_intent = OrderIntent(
        id="learning-intent-1",
        idempotency_key="learning-intent-key-1",
        strategy_id="learning-strategy",
        account_id=account.id,
        action=IntentAction.ENTER,
        legs=(leg,),
    )
    risk_decision = RiskDecisionRecord(
        id="learning-risk-1",
        intent_id=order_intent.id,
        approved=True,
        reason="Approved for this offline test",
        checks={"learning_exercise": True},
        evaluated_at=clock(),
    )

    # 4. The OMS submits. The broker accepts, but nothing fills yet.
    submitted = oms.submit_intent(
        order_intent,
        account=account,
        risk_decision=risk_decision,
    )
    assert submitted["status"] == IntentStatus.WORKING.value
    assert len(broker.submit_calls) == 1

    attempts = repository.broker_orders_for_leg(leg.id)
    assert len(attempts) == 1
    external_order_id = attempts[0]["external_order_id"]
    assert external_order_id in broker.orders
    assert broker.get_order(account, external_order_id).status is BrokerOrderStatus.WORKING
    assert repository.fills_for_leg(leg.id) == []

    # 5. Ten seconds later, simulate an actual broker-side execution.
    clock.advance(10)
    broker.fill_order(external_order_id, "10", "500")
    assert broker.get_order(account, external_order_id).status is BrokerOrderStatus.FILLED

    # The broker knows about the fill, but the OMS has not observed it yet.
    assert repository.fills_for_leg(leg.id) == []

    # 6. Ask the REAL OMS to reconcile against the broker's facts.
    recovered = oms.recover_intent(order_intent.id, account=account)
    assert recovered["status"] == IntentStatus.FILLED.value
    assert recovered["legs"][0]["status"] == LegStatus.FILLED.value

    persisted_fills = repository.fills_for_leg(leg.id)
    assert len(persisted_fills) == 1
    assert Decimal(str(persisted_fills[0]["quantity"])) == Decimal("10")
    assert broker.get_positions(account)[0].signed_quantity == Decimal("10")

    # 7. Recovery is repeatable: the same fill must not be persisted twice.
    recovered_again = oms.recover_intent(order_intent.id, account=account)
    assert recovered_again["status"] == IntentStatus.FILLED.value
    assert len(repository.fills_for_leg(leg.id)) == 1
    assert len(broker.submit_calls) == 1
