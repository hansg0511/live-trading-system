"""Offline contract tests for the generic Moomoo/OpenD bridge."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from src.brokers.moomoo.generic_adapter import (
    MooMooGenericAdapter,
    MoomooAdapterError,
    MoomooMappingError,
    StaticMoomooInstrumentResolver,
)
from src.trading_core.domain import (
    Account,
    BrokerOrderStatus,
    ExecutionSession,
    LegStatus,
    OrderLeg,
    QuantityUnit,
    Side,
    TradingEnvironment,
)
from src.trading_core.ports import BrokerAdapter, BrokerSubmitRequest


NOW = datetime(2026, 9, 15, 12, 0, tzinfo=timezone.utc)


class FakeSdk:
    class TrdEnv:
        SIMULATE = "SIMULATE"

    class TrdSide:
        BUY = "BUY"
        SELL = "SELL"

    class OrderType:
        MARKET = "MARKET"
        NORMAL = "NORMAL"

    class ModifyOrderOp:
        CANCEL = "CANCEL"
        MODIFY = "MODIFY"

    class Session:
        RTH = "RTH"
        ETH = "ETH"
        OVERNIGHT = "OVERNIGHT"


class FakeTradeContext:
    def __init__(self) -> None:
        self.orders = [
            {
                "order_id": "101",
                "code": "US.AAPL",
                "trd_side": "BUY",
                "qty": "10",
                "dealt_qty": "3",
                "order_status": "FILLED_PART",
                "remark": "durable-client-id",
            },
            {
                "order_id": "102",
                "code": "US.MSFT",
                "trd_side": "SELL",
                "qty": "2",
                "dealt_qty": "2",
                "order_status": "FILLED_ALL",
            },
        ]
        self.positions = [
            {"position_id": "long", "code": "US.AAPL", "qty": "7", "position_side": "LONG", "average_price": "100"},
            {"position_id": "short", "code": "US.MSFT", "qty": "2", "position_side": "SHORT", "average_price": "200"},
        ]
        self.deals = [
            {
                "deal_id": "deal-1",
                "order_id": "101",
                "code": "US.AAPL",
                "qty": "3",
                "price": "101.25",
                "create_time": "2026-09-15 09:30:00",
            }
        ]
        self.balance = {"cash": "1000", "buying_power": "2500", "equity": "1200", "initial_margin": "50"}
        self.balance_rows = [self.balance]
        self.place_calls: list[dict] = []
        self.modify_calls: list[dict] = []
        self.order_query_calls = 0
        self.deal_error: tuple[int, object] | None = None

    def get_acc_list(self):
        return 0, [
            {
                "acc_id": "42",
                "trd_env": "SIMULATE",
                "acc_status": "ACTIVE",
                "acc_role": "N/A",
                "acc_type": "MARGIN",
                "trdmarket_auth": [2],
            }
        ]

    def accinfo_query(self, **_kwargs):
        return 0, self.balance_rows

    def position_list_query(self, **_kwargs):
        return 0, self.positions

    def order_list_query(self, **_kwargs):
        self.order_query_calls += 1
        return 0, self.orders

    def deal_list_query(self, **_kwargs):
        if self.deal_error is not None:
            return self.deal_error
        return 0, self.deals

    def place_order(self, **kwargs):
        self.place_calls.append(kwargs)
        return 0, [{"order_id": "103", "order_status": "SUBMITTED", "create_time": "2026-09-15T13:31:00+00:00"}]

    def modify_order(self, **kwargs):
        self.modify_calls.append(kwargs)
        return 0, [{"order_id": str(kwargs["order_id"]), "order_status": "SUBMITTED"}]


def resolver() -> StaticMoomooInstrumentResolver:
    return StaticMoomooInstrumentResolver({"instrument-aapl": "US.AAPL", "instrument-msft": "US.MSFT"})


def account() -> Account:
    return Account(
        id="generic-sim-account",
        broker="moomoo",
        environment=TradingEnvironment.SIM,
        external_account_id="42",
        base_currency="USD",
        created_at=NOW,
        updated_at=NOW,
    )


def adapter(context: FakeTradeContext | None = None) -> MooMooGenericAdapter:
    result = MooMooGenericAdapter(
        instrument_resolver=resolver(),
        external_account_id="42",
        sdk_module=FakeSdk,
        trade_context=context or FakeTradeContext(),
    )
    assert result.connect()
    return result


def request(
    *,
    order_type: str = "LIMIT",
    quantity_unit: QuantityUnit = QuantityUnit.UNITS,
    allow_extended_hours: bool = False,
    execution_session: ExecutionSession | str = ExecutionSession.REGULAR,
) -> BrokerSubmitRequest:
    leg = OrderLeg(
        id="leg-1",
        intent_id="intent-1",
        sequence=0,
        instrument_id="instrument-aapl",
        side=Side.BUY,
        quantity=Decimal("5"),
        quantity_unit=quantity_unit,
        order_type=order_type,
        limit_price=Decimal("123.45") if order_type == "LIMIT" else None,
        status=LegStatus.PLANNED,
        created_at=NOW,
        updated_at=NOW,
    )
    return BrokerSubmitRequest(
        broker_order_id="attempt-1",
        account_id="generic-sim-account",
        broker="moomoo",
        order_leg=leg,
        client_order_id="durable-client-id",
        allow_extended_hours=allow_extended_hours,
        execution_session=execution_session,
    )


def test_adapter_implements_generic_contract_and_reads_normalized_broker_facts():
    broker = adapter()
    assert isinstance(broker, BrokerAdapter)
    assert broker.get_accounts()[0].external_account_id == "42"
    assert broker.get_capabilities(account()).supports_fill_read
    assert broker.get_snapshot(account()).status == "COMPLETE"

    balance = broker.get_balances(account())
    assert balance.cash == Decimal("1000")
    assert balance.buying_power == Decimal("2500")

    positions = broker.get_positions(account())
    assert [item.signed_quantity for item in positions] == [Decimal("7"), Decimal("-2")]

    open_orders = broker.get_open_orders(account())
    assert len(open_orders) == 1
    assert open_orders[0].status is BrokerOrderStatus.PARTIALLY_FILLED
    assert open_orders[0].client_order_id == "durable-client-id"
    assert broker.get_order(account(), "102").status is BrokerOrderStatus.FILLED

    fills = broker.get_fills(account())
    assert fills[0].dedupe_key == "deal-1"
    assert fills[0].filled_at == datetime(2026, 9, 15, 13, 30, tzinfo=timezone.utc)


def test_signed_short_position_quantity_is_normalized_using_direction():
    context = FakeTradeContext()
    context.positions = [
        {"position_id": "short", "code": "US.MSFT", "qty": "-1.0", "position_side": "SHORT", "average_price": "200"}
    ]

    positions = adapter(context).get_positions(account())

    assert [item.signed_quantity for item in positions] == [Decimal("-1.0")]


def test_negative_long_position_quantity_fails_closed_as_inconsistent():
    context = FakeTradeContext()
    context.positions = [
        {"position_id": "inconsistent", "code": "US.AAPL", "qty": "-1.0", "position_side": "LONG"}
    ]

    with pytest.raises(MoomooAdapterError, match="negative quantity for LONG"):
        adapter(context).get_positions(account())


def test_unsupported_sim_deal_history_uses_only_complete_filled_order_evidence():
    context = FakeTradeContext()
    context.deal_error = (-1, "Paper trading does not support deal data.")
    context.orders = [
        {
            "order_id": "filled",
            "code": "US.AAPL",
            "trd_side": "BUY",
            "qty": "2",
            "dealt_qty": "2",
            "dealt_avg_price": "101.25",
            "order_status": "FILLED_ALL",
            "create_time": "2026-09-15 09:30:00",
        },
        {
            "order_id": "partial",
            "code": "US.MSFT",
            "trd_side": "SELL",
            "qty": "3",
            "dealt_qty": "1",
            "dealt_avg_price": "201.25",
            "order_status": "FILLED_PART",
            "create_time": "2026-09-15 09:31:00",
        },
    ]

    fills = adapter(context).get_fills(account())

    assert len(fills) == 1
    assert fills[0].external_order_id == "filled"
    assert fills[0].quantity == Decimal("2")
    assert fills[0].price == Decimal("101.25")
    assert fills[0].metadata["synthetic"] is True


def test_unsupported_sim_deal_history_with_incomplete_filled_row_fails_closed():
    context = FakeTradeContext()
    context.deal_error = (-1, "Paper trading does not support deal data.")
    context.orders = [
        {
            "order_id": "incomplete",
            "code": "US.AAPL",
            "trd_side": "BUY",
            "qty": "2",
            "dealt_qty": "1",
            "dealt_avg_price": "101.25",
            "order_status": "FILLED_ALL",
            "create_time": "2026-09-15 09:30:00",
        }
    ]

    with pytest.raises(MoomooAdapterError, match="dealt quantity is not complete"):
        adapter(context).get_fills(account())


def test_non_unsupported_deal_history_error_is_not_converted_to_order_fill():
    context = FakeTradeContext()
    context.deal_error = (-1, "temporary network failure")

    with pytest.raises(MoomooAdapterError, match="deal_list_query failed"):
        adapter(context).get_fills(account())


def test_submit_cancel_and_replace_translate_generic_commands_without_sdk_or_network():
    context = FakeTradeContext()
    broker = adapter(context)

    submitted = broker.submit_order(account(), request())
    assert submitted.accepted is True
    assert submitted.external_order_id == "103"
    assert context.place_calls == [
        {
            "price": 123.45,
            "qty": 5.0,
            "code": "US.AAPL",
            "trd_side": "BUY",
            "order_type": "NORMAL",
            "trd_env": "SIMULATE",
            "acc_id": 42,
            "remark": "durable-client-id",
        }
    ]

    cancelled = broker.cancel_order(account(), "103")
    replaced = broker.replace_order(account(), "103", {"quantity": "4", "limit_price": "124"})
    assert cancelled.accepted is True and cancelled.status is BrokerOrderStatus.WORKING
    assert replaced.accepted is True and replaced.status is BrokerOrderStatus.WORKING
    assert [call["modify_order_op"] for call in context.modify_calls] == ["CANCEL", "MODIFY"]


def test_unsupported_submit_is_rejected_before_broker_call():
    context = FakeTradeContext()
    broker = adapter(context)
    result = broker.submit_order(account(), request(quantity_unit=QuantityUnit.CONTRACTS))
    assert result.accepted is False
    assert result.status is BrokerOrderStatus.REJECTED
    assert context.place_calls == []


def test_extended_hours_is_an_explicit_limit_order_only_toggle():
    context = FakeTradeContext()
    broker = adapter(context)
    accepted = broker.submit_order(account(), request(allow_extended_hours=True))
    assert accepted.accepted is True
    assert context.place_calls[0]["fill_outside_rth"] is True
    assert "session" not in context.place_calls[0]
    assert broker.get_capabilities(account()).supports("EXTENDED_HOURS_LIMIT")

    rejected = broker.submit_order(account(), request(order_type="MARKET", allow_extended_hours=True))
    assert rejected.accepted is False
    assert rejected.error_code == "EXTENDED_HOURS_LIMIT_ONLY"
    assert len(context.place_calls) == 1


def test_overnight_maps_to_native_session_without_pre_post_flag():
    context = FakeTradeContext()
    broker = adapter(context)

    accepted = broker.submit_order(account(), request(execution_session=ExecutionSession.OVERNIGHT))

    assert accepted.accepted is True
    assert context.place_calls[0]["session"] == "OVERNIGHT"
    assert "fill_outside_rth" not in context.place_calls[0]
    assert broker.get_capabilities(account()).supports("OVERNIGHT_LIMIT")


def test_overnight_rejects_market_orders_before_broker_call():
    context = FakeTradeContext()
    broker = adapter(context)

    rejected = broker.submit_order(
        account(),
        request(order_type="MARKET", execution_session=ExecutionSession.OVERNIGHT),
    )

    assert rejected.accepted is False
    assert rejected.error_code == "OVERNIGHT_LIMIT_ONLY"
    assert context.place_calls == []


def test_overnight_rejects_non_us_markets_before_broker_call():
    context = FakeTradeContext()
    broker = adapter(context)
    broker.market = "HK"

    rejected = broker.submit_order(account(), request(execution_session=ExecutionSession.OVERNIGHT))

    assert rejected.accepted is False
    assert rejected.error_code == "OVERNIGHT_UNSUPPORTED_MARKET"
    assert context.place_calls == []


def test_zero_balance_is_preserved_as_a_real_value_and_simulate_alias_is_accepted():
    context = FakeTradeContext()
    context.balance = {"cash": "0", "buying_power": "0", "equity": "0"}
    context.balance_rows = [context.balance]
    broker = MooMooGenericAdapter(
        instrument_resolver=resolver(),
        external_account_id="42",
        environment="SIMULATE",
        sdk_module=FakeSdk,
        trade_context=context,
    )
    broker.connect()
    balance = broker.get_balances(account())
    assert balance.cash == Decimal("0")
    assert balance.buying_power == Decimal("0")
    assert balance.equity == Decimal("0")


def test_non_numeric_optional_balance_fields_are_unavailable_not_fatal():
    context = FakeTradeContext()
    context.balance = {
        "cash": "1000",
        "buying_power": "2500",
        "equity": "N/A",
        "initial_margin": "N/A",
        "maintenance_margin": "N/A",
    }
    context.balance_rows = [context.balance]

    balance = adapter(context).get_balances(account())

    assert balance.cash == Decimal("1000")
    assert balance.buying_power == Decimal("2500")
    assert balance.equity is None
    assert balance.initial_margin is None
    assert balance.maintenance_margin is None


@pytest.mark.parametrize(
    ("field", "value"),
    (("cash", "N/A"), ("cash", None), ("buying_power", "N/A"), ("buying_power", None)),
)
def test_required_cash_and_buying_power_fields_fail_closed(field, value):
    context = FakeTradeContext()
    context.balance[field] = value
    context.balance_rows = [context.balance]

    with pytest.raises(MoomooAdapterError, match=field):
        adapter(context).get_balances(account())


def test_unmapped_broker_position_fails_closed_instead_of_becoming_unknown_strategy_exposure():
    context = FakeTradeContext()
    context.positions = [{"position_id": "unknown", "code": "US.UNMAPPED", "qty": "1", "position_side": "LONG"}]
    with pytest.raises(MoomooMappingError, match="Unmapped"):
        adapter(context).get_positions(account())


def test_unmapped_zero_quantity_position_is_ignored_after_normalization():
    context = FakeTradeContext()
    context.positions = [{"position_id": "closed", "code": "US.UNMAPPED", "qty": "0", "position_side": "LONG"}]

    assert adapter(context).get_positions(account()) == ()


@pytest.mark.parametrize(
    ("row", "message"),
    (
        ({"position_id": "missing-quantity", "code": "US.UNMAPPED", "position_side": "LONG"}, "position quantity"),
        ({"position_id": "missing-direction", "code": "US.UNMAPPED", "qty": "0"}, "unknown direction"),
    ),
)
def test_zero_or_unmapped_position_with_incomplete_facts_fails_closed(row, message):
    context = FakeTradeContext()
    context.positions = [row]

    with pytest.raises(MoomooAdapterError, match=message):
        adapter(context).get_positions(account())


def test_incomplete_broker_facts_fail_closed():
    context = FakeTradeContext()
    context.balance_rows = []
    with pytest.raises(RuntimeError, match="no account data"):
        adapter(context).get_balances(account())

    context = FakeTradeContext()
    context.positions = [{"position_id": "bad-direction", "code": "US.AAPL", "qty": "1", "position_side": ""}]
    with pytest.raises(RuntimeError, match="unknown direction"):
        adapter(context).get_positions(account())


def test_adapter_rejects_live_and_wrong_account_identity():
    with pytest.raises(PermissionError, match="SIM-only"):
        MooMooGenericAdapter(instrument_resolver=resolver(), environment=TradingEnvironment.LIVE)

    wrong_account = Account(
        id="wrong",
        broker="moomoo",
        environment=TradingEnvironment.SIM,
        external_account_id="99",
        base_currency="USD",
        created_at=NOW,
    )
    with pytest.raises(PermissionError, match="identity"):
        adapter().get_balances(wrong_account)
