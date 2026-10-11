"""Offline contract tests for ScriptedBroker, no Moomoo SDK or US RTH required."""
from __future__ import annotations

from decimal import Decimal

import pytest

from src.trading_core.domain import (
    Account, BrokerOrderStatus, ExecutionEvidenceMode, OrderLeg,
    QuantityUnit, Side, TradingEnvironment,
)
from src.trading_core.ports import BrokerSubmitRequest
from tests.scripted_broker import ManualClock, ScriptedBroker


@pytest.fixture
def account() -> Account:
    return Account(
        id="fake-account",
        broker="FAKE",
        environment=TradingEnvironment.SIM,
        external_account_id="fake-external",
        base_currency="USD",
    )


@pytest.fixture
def broker(account: Account) -> ScriptedBroker:
    return ScriptedBroker(account, clock=ManualClock())


def request(account: Account, *, side: Side = Side.BUY, quantity: str = "10") -> BrokerSubmitRequest:
    leg = OrderLeg(
        id="leg-1",
        intent_id="intent-1",
        sequence=0,
        instrument_id="QQQ",
        side=side,
        quantity=Decimal(quantity),
        quantity_unit=QuantityUnit.UNITS,
        order_type="MARKET",
    )
    return BrokerSubmitRequest(
        broker_order_id="local-attempt-1",
        account_id=account.id,
        broker=account.broker,
        order_leg=leg,
        client_order_id="local-client-1",
    )


def test_late_fill_is_independent_of_observation_and_clock(broker, account):
    ack = broker.submit_order(account, request(account, side=Side.SELL, quantity="1"))
    assert ack.status is BrokerOrderStatus.WORKING
    order_id = ack.external_order_id

    broker.clock.advance(300)
    assert broker.get_order(account, order_id).status is BrokerOrderStatus.WORKING
    assert broker.get_positions(account) == ()

    broker.fill_order(order_id, "1", "600")
    assert broker.get_order(account, order_id).status is BrokerOrderStatus.FILLED
    assert broker.get_positions(account)[0].signed_quantity == Decimal("-1")
    assert len(broker.get_authoritative_account_facts(account).fills) == 1


def test_partial_fills_and_net_positions(broker, account):
    order_id = broker.submit_order(account, request(account)).external_order_id
    broker.fill_order(order_id, "4", "500")
    assert broker.get_order(account, order_id).status is BrokerOrderStatus.PARTIALLY_FILLED
    broker.fill_order(order_id, "6", "502")
    assert broker.get_order(account, order_id).status is BrokerOrderStatus.FILLED
    assert broker.get_positions(account)[0].signed_quantity == Decimal("10")
    assert len(broker.get_fills(account)) == 2


def test_broker_state_survives_simulated_oms_restart(broker, account):
    order_id = broker.submit_order(account, request(account)).external_order_id
    broker.fill_order(order_id, "10", "500")
    # The fake broker is intentionally independent of OMS and its SQLite DB.
    # The OMS-restart integration test will construct a new OMS against this fake.
    assert broker.get_order(account, order_id).status is BrokerOrderStatus.FILLED
    assert broker.submit_calls[0].broker_order_id == "local-attempt-1"


def test_lost_ack_can_hide_a_real_order(broker, account):
    broker.queue_submit("lost_ack")
    with pytest.raises(TimeoutError, match="acknowledgment"):
        broker.submit_order(account, request(account))
    assert len(broker.orders) == 1
    assert len(broker.submit_calls) == 1
    assert broker.get_open_orders(account)[0].status is BrokerOrderStatus.WORKING


def test_rejected_order_does_not_reach_venue(broker, account):
    broker.queue_submit("reject")
    result = broker.submit_order(account, request(account))
    assert result.status is BrokerOrderStatus.REJECTED
    assert result.no_submit_asserted is True
    assert broker.orders == {}


def test_duplicated_report_is_not_duplicated_execution(broker, account):
    order_id = broker.inject_external_order("QQQ", Side.BUY, "1")
    fill = broker.fill_order(order_id, "1", "500")
    broker.duplicate_deal_ids.add(fill.external_fill_id)
    facts = broker.get_authoritative_account_facts(account)
    assert len(facts.fills) == 2  # deliberately duplicated observation
    assert len(broker.deals) == 1  # one actual economic execution
    assert broker.get_positions(account)[0].signed_quantity == Decimal("1")


def test_stale_order_snapshot_disagrees_with_true_fill(broker, account):
    order_id = broker.inject_external_order("QQQ", Side.BUY, "1")
    broker.freeze_order_observation(order_id)
    broker.fill_order(order_id, "1", "500")
    assert broker.get_order(account, order_id).status is BrokerOrderStatus.WORKING
    assert broker.orders[order_id].status is BrokerOrderStatus.FILLED
    assert broker.get_positions(account)[0].signed_quantity == Decimal("1")


def test_unknown_external_order_and_broker_account_mismatch(broker, account):
    unknown_id = broker.inject_external_order("AAPL", Side.BUY, "2")
    broker.fill_order(unknown_id, "2", "200")
    assert broker.get_authoritative_account_facts(account).fills[0].external_order_id == unknown_id
    wrong = Account(id="other", broker="FAKE", environment=TradingEnvironment.SIM,
                    external_account_id="other-ext", base_currency="USD")
    with pytest.raises(ValueError, match="wrong account"):
        broker.get_authoritative_account_facts(wrong)


def test_disconnect_and_incomplete_and_stale_observations(broker, account):
    broker.query_error = "network unavailable"
    assert broker.get_authoritative_account_facts(account).complete is False
    broker.query_error = None
    broker.incomplete = True
    assert broker.get_authoritative_account_facts(account).complete is False
    broker.incomplete = False
    broker.snapshot_age_seconds = 120
    facts = broker.get_authoritative_account_facts(account)
    assert facts.captured_at < broker.clock()


def test_unknown_position_can_be_reported_independently(broker, account):
    broker.positions_override = {"MSFT": Decimal("-5")}
    assert broker.get_authoritative_account_facts(account).positions[0].signed_quantity == Decimal("-5")
    assert broker.positions == {}


def test_moomoo_like_cumulative_evidence_does_not_forge_individual_deals(account):
    broker = ScriptedBroker(
        account, clock=ManualClock(),
        evidence_mode=ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS,
    )
    order_id = broker.inject_external_order("QQQ", Side.BUY, "2")
    broker.fill_order(order_id, "2", "500")
    facts = broker.get_authoritative_account_facts(account)
    assert facts.execution_evidence_mode is ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS
    assert facts.fills[0].external_fill_id is None
    assert facts.fills[0].evidence_reference.startswith("TERMINAL_ORDER:")
    with pytest.raises(RuntimeError, match="unavailable"):
        broker.get_fills(account)


def test_two_books_orders_do_not_need_to_share_an_intent(broker, account):
    # Fake broker sees only orders and net inventory, not strategy/books.
    a = broker.inject_external_order("AAPL", Side.BUY, "2")
    b = broker.inject_external_order("MSFT", Side.SELL, "3")
    broker.fill_order(a, "2", "200")
    broker.fill_order(b, "3", "400")
    positions = {x.instrument_id: x.signed_quantity for x in broker.get_positions(account)}
    assert positions == {"AAPL": Decimal("2"), "MSFT": Decimal("-3")}
