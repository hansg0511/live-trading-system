"""Offline contract tests for the generic Moomoo/OpenD bridge."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest
import pandas as pd

from src.brokers.moomoo.generic_adapter import (
    MooMooGenericAdapter,
    MoomooAdapterError,
    MoomooMappingError,
    MoomooRateLimitError,
    StaticMoomooInstrumentResolver,
)
from src.trading_core.domain import (
    ADAPTER_ORDER_SNAPSHOT_AUTHORITY,
    ADAPTER_SUBMISSION_ACK_AUTHORITY,
    Account,
    BrokerOrderStatus,
    ExecutionEvidenceMode,
    ExecutionSession,
    LegStatus,
    OrderLeg,
    QuantityUnit,
    Side,
    TradingEnvironment,
)
from src.trading_core.oms import GenericOMS
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
        self.position_query_calls = 0
        self.deal_query_calls = 0
        self.deal_error: tuple[int, object] | None = None
        self.history_orders: list[dict] = []
        self.history_error: tuple[int, object] | None = None
        self.history_query_kwargs: list[dict] = []

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
        self.position_query_calls += 1
        return 0, self.positions

    def order_list_query(self, **_kwargs):
        self.order_query_calls += 1
        return 0, self.orders

    def deal_list_query(self, **_kwargs):
        self.deal_query_calls += 1
        if self.deal_error is not None:
            return self.deal_error
        return 0, self.deals

    def history_order_list_query(self, **kwargs):
        self.history_query_kwargs.append(dict(kwargs))
        if self.history_error is not None:
            return self.history_error
        return 0, self.history_orders

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
    quantity: Decimal | str = Decimal("5"),
    client_order_id: str = "durable-client-id",
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
        quantity=Decimal(str(quantity)),
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
        client_order_id=client_order_id,
        allow_extended_hours=allow_extended_hours,
        execution_session=execution_session,
    )


def test_adapter_implements_generic_contract_and_reads_normalized_broker_facts():
    broker = adapter()
    assert isinstance(broker, BrokerAdapter)
    assert broker.get_accounts()[0].external_account_id == "42"
    assert broker.get_capabilities(account()).supports_fill_read
    assert broker.get_capabilities(account()).execution_evidence_mode is ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS
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


def test_order_snapshot_uses_broker_order_time_for_recovery_age_not_query_capture_time():
    context = FakeTradeContext()
    context.orders = [
        {
            "order_id": "stale-order",
            "code": "US.AAPL",
            "trd_side": "BUY",
            "qty": "1",
            "dealt_qty": "0",
            "order_status": "SUBMITTED",
            "create_time": "2026-09-15 08:00:00",
            "updated_time": "2026-09-15 09:00:00",
        }
    ]
    broker = adapter(context)

    snapshot = broker.get_order(account(), "stale-order")

    assert snapshot is not None
    assert snapshot.order_time == datetime(2026, 9, 15, 13, 0, tzinfo=timezone.utc)
    assert snapshot.captured_at > snapshot.order_time
    assert snapshot.external_account_id == account().external_account_id


def test_terminal_zero_fill_order_snapshot_is_adapter_authoritative():
    context = FakeTradeContext()
    context.orders = [
        {
            "order_id": "cancelled-zero",
            "code": "US.AAPL",
            "trd_side": "BUY",
            "qty": "10",
            "dealt_qty": "0",
            "order_status": "CANCELLED_ALL",
            "dealt_avg_price": "0",
        }
    ]

    snapshot = adapter(context).get_order(account(), "cancelled-zero")

    assert snapshot is not None
    assert snapshot.status is BrokerOrderStatus.CANCELLED
    assert snapshot.filled_quantity == Decimal("0")
    assert snapshot.no_fill_asserted is True
    assert snapshot.authority == ADAPTER_ORDER_SNAPSHOT_AUTHORITY


@pytest.mark.parametrize("execution_key", ("trade_id", "tradeId", "tradeID", "executionId"))
def test_order_zero_fill_with_execution_identity_is_not_authoritative(execution_key):
    context = FakeTradeContext()
    context.orders = [
        {
            "order_id": "cancelled-execution-id",
            "code": "US.AAPL",
            "trd_side": "BUY",
            "qty": "10",
            "dealt_qty": "0",
            "order_status": "CANCELLED_ALL",
            execution_key: "provider-execution-1",
        }
    ]

    snapshot = adapter(context).get_order(account(), "cancelled-execution-id")

    assert snapshot is not None
    assert snapshot.filled_quantity == Decimal("0")
    assert snapshot.no_fill_asserted is False
    assert snapshot.authority == ADAPTER_ORDER_SNAPSHOT_AUTHORITY


def test_duplicate_order_rows_with_conflicting_lifecycle_or_fill_economics_fail_closed():
    context = FakeTradeContext()
    context.orders = [
        {
            "order_id": "duplicate",
            "code": "US.AAPL",
            "trd_side": "BUY",
            "qty": "10",
            "dealt_qty": "0",
            "order_status": "CANCELLED_ALL",
        },
        {
            "order_id": "duplicate",
            "code": "US.AAPL",
            "trd_side": "BUY",
            "qty": "10",
            "dealt_qty": "10",
            "dealt_avg_price": "101.25",
            "order_status": "FILLED_ALL",
        },
    ]

    with pytest.raises(MoomooAdapterError, match="conflicting duplicate rows"):
        adapter(context).get_order(account(), "duplicate")


@pytest.mark.parametrize(
    ("status", "filled", "message"),
    (
        ("FILLED_PART", "0", "PARTIALLY_FILLED requires"),
        ("FILLED_ALL", "1", "FILLED requires"),
        ("CANCELLED_ALL", "10", "CANCELLED cannot"),
    ),
)
def test_order_status_and_quantity_must_be_consistent(status, filled, message):
    context = FakeTradeContext()
    context.orders = [
        {
            "order_id": "inconsistent",
            "code": "US.AAPL",
            "trd_side": "BUY",
            "qty": "10",
            "dealt_qty": filled,
            "order_status": status,
        }
    ]

    with pytest.raises(MoomooAdapterError, match=message):
        adapter(context).get_order(account(), "inconsistent")


def test_signed_short_position_quantity_is_normalized_using_direction():
    context = FakeTradeContext()
    context.positions = [
        {"position_id": "short", "code": "US.MSFT", "qty": "-1.0", "position_side": "SHORT", "average_price": "200"}
    ]

    positions = adapter(context).get_positions(account())

    assert [item.signed_quantity for item in positions] == [Decimal("-1.0")]


def test_position_type_placeholder_does_not_override_valid_position_side():
    context = FakeTradeContext()
    context.positions = [
        {
            "position_id": "long-with-placeholder",
            "code": "US.AAPL",
            "qty": "1",
            "position_side": "LONG",
            "position_type": "N/A",
            "average_price": "100",
        }
    ]

    positions = adapter(context).get_positions(account())

    assert [item.signed_quantity for item in positions] == [Decimal("1")]


def test_position_aliases_must_agree_before_zero_rows_are_discarded():
    context = FakeTradeContext()
    context.positions = [
        {
            "position_id": "contradictory-flat",
            "code": "US.AAPL",
            "symbol": "US.AAPL",
            "qty": "0",
            "quantity": "10",
            "position_side": "LONG",
            "direction": "BUY",
        }
    ]

    with pytest.raises(MoomooAdapterError, match="position quantity aliases conflict"):
        adapter(context).get_positions(account())


def test_position_symbol_and_direction_aliases_are_validated_before_mapping():
    context = FakeTradeContext()
    context.positions = [
        {
            "position_id": "contradictory-symbol",
            "code": "US.AAPL",
            "ticker": "US.MSFT",
            "qty": "1",
            "position_side": "LONG",
            "direction": "BUY",
        }
    ]

    with pytest.raises(MoomooAdapterError, match="position symbol aliases conflict"):
        adapter(context).get_positions(account())

    context.positions[0]["ticker"] = "US.AAPL"
    context.positions[0]["direction"] = "SELL"
    with pytest.raises(MoomooAdapterError, match="position direction aliases conflict"):
        adapter(context).get_positions(account())


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
    assert fills[0].evidence_reference == "filled:moomoo-order-fill:filled"
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


def test_bounded_historical_order_facts_normalize_full_fills_without_refresh_cache():
    context = FakeTradeContext()
    context.history_orders = [
        {
            "order_id": "history-filled",
            "code": "US.AAPL",
            "trd_side": "BUY",
            "qty": "1",
            "dealt_qty": "1",
            "dealt_avg_price": "101.25",
            "order_status": "FILLED_ALL",
            "create_time": "2026-09-15 11:23:11",
            "updated_time": "2026-09-15 11:23:24",
        }
    ]

    facts = adapter(context).get_historical_order_facts(
        account(),
        datetime(2026, 9, 15, 15, 0, tzinfo=timezone.utc),
        datetime(2026, 9, 15, 16, 0, tzinfo=timezone.utc),
    )

    assert facts.complete is True
    assert facts.execution_evidence_mode is ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS
    assert [row.external_order_id for row in facts.orders] == ["history-filled"]
    assert [fill.external_order_id for fill in facts.fills] == ["history-filled"]
    assert facts.fills[0].price == Decimal("101.25")
    assert facts.fills[0].account_id == account().id
    assert context.history_query_kwargs
    assert "refresh_cache" not in context.history_query_kwargs[0]


def test_bounded_historical_order_query_error_is_incomplete_not_empty_success():
    context = FakeTradeContext()
    context.history_error = (-1, "history_order_list_query unavailable")

    facts = adapter(context).get_historical_order_facts(
        account(),
        datetime(2026, 9, 15, 15, 0, tzinfo=timezone.utc),
        datetime(2026, 9, 15, 16, 0, tzinfo=timezone.utc),
    )

    assert facts.complete is False
    assert facts.orders == ()
    assert facts.fills == ()
    assert "history_order_list_query unavailable" in (facts.error or "")


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


def test_submit_zero_dealt_ack_is_not_submission_fill_evidence():
    context = FakeTradeContext()
    context.place_order = lambda **_kwargs: (
        0,
        [{
            "order_id": "103",
            "code": "US.AAPL",
            "qty": "5",
            "order_status": "SUBMITTING",
            "dealt_qty": "0",
            "dealt_avg_price": "0",
            "create_time": "2026-09-15T13:31:00+00:00",
        }],
    )
    result = adapter(context).submit_order(account(), request())

    assert result.accepted is True
    assert result.status is BrokerOrderStatus.WORKING
    assert result.cumulative_filled_quantity == Decimal("0")
    assert result.no_fill_asserted is True
    assert result.authority == ADAPTER_SUBMISSION_ACK_AUTHORITY
    assert GenericOMS._has_submission_fill_evidence(
        result,
        expected_quantity=Decimal("5"),
        expected_instrument_id="instrument-aapl",
    ) is False


def test_submit_exact_moomoo_zero_dealt_ack_with_order_metadata_is_not_fill_evidence():
    """The real SIM ``SUBMITTING`` response is an ACK, not an immediate fill."""

    context = FakeTradeContext()
    context.place_order = lambda **_kwargs: (
        0,
        [{
            "aux_price": "N/A",
            "code": "US.AAPL",
            "create_time": "2026-10-01 14:39:49",
            "currency": "USD",
            "dealt_avg_price": 0.0,
            "dealt_qty": 0.0,
            "fill_outside_rth": False,
            "last_err_msg": "",
            "order_id": "3438891",
            "order_status": "SUBMITTING",
            "order_type": "MARKET",
            "price": 0.0,
            "qty": 1.0,
            "remark": "stage5-intent-ded757a2cdd5b338ac85f6a4-leg-0:1",
            "session": "RTH",
            "stock_name": "Apple",
            "time_in_force": "DAY",
            "trd_side": "BUY",
            "updated_time": "2026-10-01 14:39:49",
        }],
    )

    result = adapter(context).submit_order(
        account(),
        request(
            order_type="MARKET",
            quantity="1",
            client_order_id="stage5-intent-ded757a2cdd5b338ac85f6a4-leg-0:1",
        ),
    )

    assert result.accepted is True
    assert result.status is BrokerOrderStatus.WORKING
    assert result.external_order_id == "3438891"
    assert result.cumulative_filled_quantity == Decimal("0")
    assert result.no_fill_asserted is True
    assert result.authority == ADAPTER_SUBMISSION_ACK_AUTHORITY
    assert GenericOMS._has_submission_fill_evidence(
        result,
        expected_quantity=Decimal("1"),
        expected_instrument_id="instrument-aapl",
    ) is False


@pytest.mark.parametrize(
    "updates",
    (
        {"dealt_qty": "1", "dealt_avg_price": "101.25"},
        {"dealt_qty": "not-a-number", "dealt_avg_price": "0"},
        {"dealt_qty": "-1", "dealt_avg_price": "101.25"},
        {"dealt_qty": "NaN", "dealt_avg_price": "0"},
        {"dealt_qty": "0", "dealt_avg_price": "NaN"},
        {"dealt_qty": "0", "dealt_avg_price": "101.25"},
        {"dealt_qty": "0", "dealt_avg_price": "0", "order_status": "FILLED_ALL"},
    ),
)
def test_submit_nonzero_or_invalid_dealt_ack_remains_fill_evidence(updates):
    context = FakeTradeContext()
    row = {
        "order_id": "103",
        "code": "US.AAPL",
        "qty": "5",
        "order_status": "SUBMITTING",
        "dealt_qty": "0",
        "dealt_avg_price": "0",
    }
    row.update(updates)
    context.place_order = lambda **_kwargs: (0, [row])
    result = adapter(context).submit_order(account(), request())
    if updates["dealt_qty"] in {"not-a-number", "NaN"}:
        assert result.accepted is None
        assert result.ambiguous is True
        assert GenericOMS._has_submission_fill_evidence(result) is True
        return
    assert result.no_fill_asserted is False
    assert result.authority is None
    assert GenericOMS._has_submission_fill_evidence(
        result,
        expected_quantity=Decimal("5"),
        expected_instrument_id="instrument-aapl",
    ) is True


def test_submit_missing_dealt_quantity_is_not_a_clean_ack():
    context = FakeTradeContext()
    row = {
        "order_id": "103",
        "code": "US.AAPL",
        "qty": "5",
        "order_status": "SUBMITTING",
        "dealt_avg_price": "0",
    }
    context.place_order = lambda **_kwargs: (0, [row])

    result = adapter(context).submit_order(account(), request())

    assert result.accepted is True
    assert result.no_fill_asserted is False
    assert GenericOMS._has_submission_fill_evidence(
        result,
        expected_quantity=Decimal("5"),
        expected_instrument_id="instrument-aapl",
    ) is True


def test_submit_with_multiple_provider_rows_is_ambiguous_instead_of_using_first_row():
    context = FakeTradeContext()
    context.place_order = lambda **_kwargs: (
        0,
        [
            {"order_id": "103", "order_status": "SUBMITTED"},
            {"order_id": "104", "order_status": "SUBMITTED"},
        ],
    )

    submitted = adapter(context).submit_order(account(), request())

    assert submitted.accepted is None
    assert submitted.status is BrokerOrderStatus.UNKNOWN
    assert submitted.ambiguous is True
    assert submitted.error_code == "MOOMOO_SUBMIT_RESPONSE_CONFLICT"


def test_submit_with_foreign_account_alias_is_ambiguous_even_when_order_id_is_present():
    context = FakeTradeContext()
    context.place_order = lambda **_kwargs: (
        0,
        [{"order_id": "103", "order_status": "SUBMITTED", "acc_id": "999"}],
    )

    submitted = adapter(context).submit_order(account(), request())

    assert submitted.accepted is None
    assert submitted.status is BrokerOrderStatus.UNKNOWN
    assert submitted.ambiguous is True
    assert submitted.error_code == "MOOMOO_SUBMIT_RESPONSE_CONFLICT"


def test_cancel_without_authoritative_cumulative_fill_is_not_a_no_fill_assertion():
    context = FakeTradeContext()
    broker = adapter(context)

    cancelled = broker.cancel_order(account(), "103")

    assert cancelled.cumulative_filled_quantity is None
    assert cancelled.no_fill_asserted is False


def test_cancel_terminal_zero_quantity_is_not_authoritative():
    context = FakeTradeContext()
    context.modify_order = lambda **_kwargs: (
        0,
        [{
            "order_id": "103",
            "code": "US.AAPL",
            "trd_side": "BUY",
            "qty": "0",
            "dealt_qty": "0",
            "order_status": "CANCELLED_ALL",
        }],
    )

    cancelled = adapter(context).cancel_order(account(), "103")

    assert cancelled.accepted is None
    assert cancelled.ambiguous is True
    assert cancelled.authority is None


def test_cancel_terminal_wrong_instrument_is_bound_as_foreign_fact():
    context = FakeTradeContext()
    context.modify_order = lambda **_kwargs: (
        0,
        [{
            "order_id": "103",
            "code": "US.MSFT",
            "trd_side": "BUY",
            "qty": "10",
            "dealt_qty": "0",
            "order_status": "CANCELLED_ALL",
        }],
    )

    cancelled = adapter(context).cancel_order(account(), "103")

    assert cancelled.accepted is True
    assert cancelled.authority == ADAPTER_ORDER_SNAPSHOT_AUTHORITY
    assert cancelled.instrument_id == "instrument-msft"
    assert cancelled.submitted_quantity == Decimal("10")


def test_cancel_terminal_expected_quantity_and_instrument_is_authoritative():
    context = FakeTradeContext()
    context.modify_order = lambda **_kwargs: (
        0,
        [{
            "order_id": "103",
            "code": "US.AAPL",
            "trd_side": "BUY",
            "qty": "10",
            "dealt_qty": "0",
            "order_status": "CANCELLED_ALL",
        }],
    )

    cancelled = adapter(context).cancel_order(account(), "103")

    assert cancelled.accepted is True
    assert cancelled.no_fill_asserted is True
    assert cancelled.authority == ADAPTER_ORDER_SNAPSHOT_AUTHORITY
    assert cancelled.instrument_id == "instrument-aapl"
    assert cancelled.submitted_quantity == Decimal("10")


@pytest.mark.parametrize("execution_key", ("trade_id", "tradeId", "tradeID", "executionId"))
def test_cancel_zero_fill_with_execution_identity_is_not_authoritative(execution_key):
    context = FakeTradeContext()
    context.modify_order = lambda **_kwargs: (
        0,
        [{
            "order_id": "103",
            "code": "US.AAPL",
            "trd_side": "BUY",
            "qty": "10",
            "dealt_qty": "0",
            "order_status": "CANCELLED_ALL",
            execution_key: "provider-execution-1",
        }],
    )

    cancelled = adapter(context).cancel_order(account(), "103")

    assert cancelled.accepted is True
    assert cancelled.cumulative_filled_quantity == Decimal("0")
    assert cancelled.no_fill_asserted is False
    assert cancelled.authority is None


def test_cancel_with_mismatched_returned_order_id_is_ambiguous():
    context = FakeTradeContext()
    context.modify_order = lambda **_kwargs: (0, [{"order_id": "999", "order_status": "CANCELLED", "acc_id": "42"}])

    cancelled = adapter(context).cancel_order(account(), "103")

    assert cancelled.accepted is None
    assert cancelled.status is BrokerOrderStatus.UNKNOWN
    assert cancelled.ambiguous is True
    assert cancelled.error_code == "MOOMOO_CANCEL_IDENTITY_CONFLICT"


def test_cancel_with_conflicting_account_alias_is_ambiguous():
    context = FakeTradeContext()
    context.modify_order = lambda **_kwargs: (0, [{"order_id": "103", "order_status": "CANCELLED", "acc_id": "99"}])

    cancelled = adapter(context).cancel_order(account(), "103")

    assert cancelled.accepted is None
    assert cancelled.status is BrokerOrderStatus.UNKNOWN
    assert cancelled.ambiguous is True
    assert cancelled.error_code == "MOOMOO_CANCEL_IDENTITY_CONFLICT"


def test_cancel_with_multiple_returned_rows_is_ambiguous_even_when_each_matches():
    context = FakeTradeContext()
    context.modify_order = lambda **_kwargs: (
        0,
        [
            {"order_id": "103", "order_status": "CANCELLED", "acc_id": "42"},
            {"order_id": "103", "order_status": "CANCELLED", "acc_id": "42"},
        ],
    )

    cancelled = adapter(context).cancel_order(account(), "103")

    assert cancelled.accepted is None
    assert cancelled.status is BrokerOrderStatus.UNKNOWN
    assert cancelled.ambiguous is True
    assert cancelled.error_code == "MOOMOO_CANCEL_IDENTITY_CONFLICT"


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


def test_account_fact_snapshot_marks_successful_empty_or_populated_queries_complete():
    context = FakeTradeContext()
    facts = adapter(context).get_account_facts(account())

    assert facts.complete is True
    assert facts.account_id == account().id
    assert len(facts.positions) == 2
    assert len(facts.open_orders) == 1
    assert len(facts.fills) == 1


def test_account_facts_force_fresh_provider_queries_bypass_cached_orders():
    context = FakeTradeContext()
    broker = adapter(context)

    # Ordinary display reads may populate the bounded order cache.
    broker.get_open_orders(account())
    assert context.order_query_calls == 1

    # A new broker/manual order arriving after that read must be visible to
    # the safety-critical account-facts query rather than hidden by cache.
    context.orders.append(
        {
            "order_id": "manual-foreign-to-oms",
            "code": "US.AAPL",
            "trd_side": "BUY",
            "qty": "1",
            "dealt_qty": "0",
            "order_status": "SUBMITTED",
        }
    )
    facts = broker.get_account_facts(account())

    assert context.order_query_calls == 2
    assert context.position_query_calls == 1
    assert context.deal_query_calls == 1
    assert {item.external_order_id for item in facts.open_orders} == {
        "101",
        "manual-foreign-to-oms",
    }


def test_account_fact_snapshot_uses_authoritative_filled_order_fallback():
    context = FakeTradeContext()
    context.orders[1]["dealt_avg_price"] = "200"
    context.orders[1]["create_time"] = "2026-09-15 09:31:00"
    context.deal_error = (-1, "deal_list_query does not support deal data")

    facts = adapter(context).get_account_facts(account())

    # The exact documented unsupported-history response is safe only because
    # the adapter can synthesize order-level evidence from terminal FILLED_ALL
    # rows; arbitrary query failures remain incomplete.
    assert facts.complete is True
    assert facts.fills
    assert facts.execution_evidence_mode is ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS
    assert all(item.evidence_mode is ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS for item in facts.fills)


def test_account_fact_snapshot_marks_arbitrary_query_failure_incomplete():
    context = FakeTradeContext()
    context.deal_error = (-1, "deal_list_query failed unexpectedly")

    facts = adapter(context).get_account_facts(account())

    assert facts.complete is False
    assert "deal_list_query failed unexpectedly" in (facts.error or "")


@pytest.mark.parametrize("malformed_endpoint", ("positions", "orders", "fills"))
def test_account_fact_snapshot_never_treats_malformed_rows_as_a_complete_empty_account(malformed_endpoint):
    context = FakeTradeContext()
    if malformed_endpoint == "positions":
        context.positions = ["not-a-record"]
    elif malformed_endpoint == "orders":
        context.orders = ["not-a-record"]
    else:
        context.deals = ["not-a-record"]

    facts = adapter(context).get_account_facts(account())

    assert facts.complete is False
    assert "malformed response" in (facts.error or "")
    assert facts.positions == ()
    assert facts.open_orders == ()
    assert facts.fills == ()


def test_account_fact_snapshot_preserves_a_recognized_true_empty_table():
    context = FakeTradeContext()
    context.positions = []
    context.orders = []
    context.deals = []

    facts = adapter(context).get_account_facts(account())

    assert facts.complete is True
    assert facts.positions == ()
    assert facts.open_orders == ()
    assert facts.fills == ()


def test_account_fact_snapshot_preserves_a_recognized_empty_dataframe():
    context = FakeTradeContext()
    context.positions = pd.DataFrame()
    context.orders = pd.DataFrame()
    context.deals = pd.DataFrame()

    facts = adapter(context).get_account_facts(account())

    assert facts.complete is True
    assert facts.positions == ()
    assert facts.open_orders == ()
    assert facts.fills == ()


def test_read_rate_limit_retries_with_injected_clock_and_bounded_backoff():
    class Clock:
        def __init__(self) -> None:
            self.value = 0.0

        def __call__(self) -> float:
            return self.value

        def sleep(self, seconds: float) -> None:
            self.value += seconds

    class RateLimitedContext(FakeTradeContext):
        def __init__(self) -> None:
            super().__init__()
            self.remaining_failures = 2

        def position_list_query(self, **kwargs):
            self.position_query_calls += 1
            if self.remaining_failures:
                self.remaining_failures -= 1
                return -1, "request frequency rate limit"
            return 0, self.positions

    clock = Clock()
    context = RateLimitedContext()
    broker = MooMooGenericAdapter(
        instrument_resolver=resolver(),
        external_account_id="42",
        sdk_module=FakeSdk,
        trade_context=context,
        read_clock=clock,
        read_sleep=clock.sleep,
        read_min_interval=1.0,
        read_max_retries=2,
        read_backoff_base=0.25,
        read_jitter=lambda _maximum: 0.0,
    )
    assert broker.connect()

    positions = broker.get_positions(account())

    assert len(positions) == 2
    assert context.position_query_calls == 3
    # Two provider backoffs plus the minimum interval before each retry are
    # driven entirely by the injected clock; no real wait is needed.
    assert clock.value >= 3.0


def test_exhausted_read_rate_limit_is_one_retryable_adapter_error():
    class RateLimitedContext(FakeTradeContext):
        def __init__(self) -> None:
            super().__init__()
            self.position_query_calls = 0

        def position_list_query(self, **kwargs):
            self.position_query_calls += 1
            return -1, "429 too many requests"

    clock = iter((0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
    context = RateLimitedContext()
    broker = MooMooGenericAdapter(
        instrument_resolver=resolver(),
        external_account_id="42",
        sdk_module=FakeSdk,
        trade_context=context,
        read_clock=lambda: next(clock),
        read_sleep=lambda _seconds: None,
        read_min_interval=0.0,
        read_max_retries=2,
        read_backoff_base=0.25,
        read_jitter=lambda _maximum: 0.0,
    )
    assert broker.connect()

    with pytest.raises(MoomooRateLimitError) as exc_info:
        broker.get_positions(account())

    assert exc_info.value.retryable is True
    assert context.position_query_calls == 3


def test_dataframe_cancel_response_is_canonicalized_and_fill_evidence_is_not_hidden():
    context = FakeTradeContext()
    context.modify_order = lambda **_kwargs: (
        0,
        pd.DataFrame(
            [
                {
                    "order_id": "103",
                    "order_status": "CANCELLED",
                    "dealt_qty": 0,
                    "dealt_avg_price": 101.25,
                    "acc_id": "42",
                }
            ]
        ),
    )

    cancelled = adapter(context).cancel_order(account(), "103")

    assert cancelled.accepted is True
    assert cancelled.cumulative_filled_quantity == Decimal("0")
    assert cancelled.no_fill_asserted is False
    assert isinstance(cancelled.raw_payload["response"], list)
    assert cancelled.raw_payload["response"][0]["acc_id"] == "42"


def test_dataframe_cancel_response_with_foreign_provenance_is_ambiguous():
    context = FakeTradeContext()
    context.modify_order = lambda **_kwargs: (
        0,
        pd.DataFrame(
            [
                {
                    "order_id": "103",
                    "order_status": "CANCELLED",
                    "dealt_qty": 0,
                    "acc_id": "999",
                }
            ]
        ),
    )

    cancelled = adapter(context).cancel_order(account(), "103")

    assert cancelled.accepted is None
    assert cancelled.ambiguous is True
    assert cancelled.error_code == "MOOMOO_CANCEL_IDENTITY_CONFLICT"


def test_repeated_fill_queries_keep_immutable_broker_identity_stable():
    context = FakeTradeContext()
    broker = adapter(context)

    first = broker.get_fills(account())
    second = broker.get_fills(account())

    assert [
        (
            item.external_order_id,
            item.external_fill_id,
            item.dedupe_key,
            item.quantity,
            item.price,
            item.filled_at,
            item.metadata,
        )
        for item in first
    ] == [
        (
            item.external_order_id,
            item.external_fill_id,
            item.dedupe_key,
            item.quantity,
            item.price,
            item.filled_at,
            item.metadata,
        )
        for item in second
    ]


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


def test_order_alias_conflict_is_rejected_before_first_alias_selection():
    context = FakeTradeContext()
    context.orders = [
        {
            "order_id": "201",
            "code": "US.AAPL",
            "trd_side": "BUY",
            "qty": "10",
            "quantity": "11",
            "dealt_qty": "0",
            "order_status": "SUBMITTED",
        }
    ]

    with pytest.raises(MoomooAdapterError, match="order quantity aliases conflict"):
        adapter(context).get_open_orders(account())


def test_status_alias_conflict_is_rejected_before_duplicate_collapse():
    context = FakeTradeContext()
    context.orders = [
        {
            "order_id": "202",
            "code": "US.AAPL",
            "trd_side": "BUY",
            "qty": "10",
            "dealt_qty": "0",
            "order_status": "SUBMITTED",
            "status": "FILLED_ALL",
        }
    ]

    with pytest.raises(MoomooAdapterError, match="order status aliases conflict"):
        adapter(context).get_open_orders(account())


def test_duplicate_normalized_order_with_different_deal_evidence_is_not_collapsed():
    context = FakeTradeContext()
    base = {
        "order_id": "203",
        "code": "US.AAPL",
        "trd_side": "BUY",
        "qty": "10",
        "dealt_qty": "0",
        "order_status": "SUBMITTED",
        "deal_id": "deal-a",
    }
    context.orders = [base, {**base, "deal_id": "deal-b"}]

    with pytest.raises(MoomooAdapterError, match="conflicting duplicate rows"):
        adapter(context).get_open_orders(account())


def test_unsupported_deal_history_is_explicit_account_fact_uncertainty():
    context = FakeTradeContext()
    context.deal_error = (-1, "deal_list_query is not supported for this SIM account")
    context.orders[1].update(
        {
            "dealt_avg_price": "201.25",
            "updated_time": "2026-09-15 13:00:00",
        }
    )

    facts = adapter(context).get_account_facts(account())

    assert facts.complete is True
    assert facts.metadata["fill_history_unsupported"] is True
    assert facts.fills
    assert facts.execution_evidence_mode is ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS


def test_nested_order_lifecycle_in_zero_fill_row_is_rejected_by_core_authority_validation():
    context = FakeTradeContext()
    context.orders = [
        {
            "order_id": "204",
            "code": "US.AAPL",
            "trd_side": "BUY",
            "qty": "10",
            "dealt_qty": "0",
            "order_status": "CANCELLED_ALL",
            "orders": [{"id": "foreign", "status": "FILLED"}],
        }
    ]

    observed = adapter(context).get_order(account(), "204")

    assert observed is not None
    assert observed.no_fill_asserted is False
    assert observed.authority == ADAPTER_ORDER_SNAPSHOT_AUTHORITY
    assert GenericOMS._snapshot_no_fill_is_authoritative(observed) is False


def test_fill_alias_conflict_is_rejected_before_constructing_broker_fill():
    context = FakeTradeContext()
    context.deals = [
        {
            "deal_id": "conflicting-deal",
            "order_id": "101",
            "qty": "3",
            "dealt_qty": "4",
            "price": "101.25",
            "create_time": "2026-09-15 09:30:00",
        }
    ]

    with pytest.raises(MoomooAdapterError, match="fill quantity aliases conflict"):
        adapter(context).get_fills(account())


def test_fill_symbol_alias_conflict_is_rejected_before_instrument_mapping():
    context = FakeTradeContext()
    context.deals = [
        {
            "deal_id": "conflicting-symbol-deal",
            "order_id": "101",
            "code": "US.AAPL",
            "ticker": "US.MSFT",
            "qty": "3",
            "price": "101.25",
            "create_time": "2026-09-15 09:30:00",
        }
    ]

    with pytest.raises(MoomooAdapterError, match="fill symbol aliases conflict"):
        adapter(context).get_fills(account())


def test_nested_deal_identity_conflict_is_rejected_before_order_normalization():
    context = FakeTradeContext()
    context.orders = [
        {
            "order_id": "205",
            "code": "US.AAPL",
            "trd_side": "BUY",
            "qty": "10",
            "dealt_qty": "10",
            "dealt_avg_price": "101.25",
            "order_status": "FILLED_ALL",
            "deal_id": "deal-a",
            "deal_list": [{"deal_id": "deal-b", "qty": "10"}],
        }
    ]

    with pytest.raises(MoomooAdapterError, match="aggregate fill ID conflicts"):
        adapter(context).get_order(account(), "205")


def test_working_zero_order_with_unsupported_fill_history_remains_account_uncertain():
    context = FakeTradeContext()
    context.orders = [
        {
            "order_id": "working-zero",
            "code": "US.AAPL",
            "trd_side": "BUY",
            "qty": "10",
            "dealt_qty": "0",
            "deal_list": [],
            "order_status": "SUBMITTED",
        }
    ]
    context.deal_error = (-1, "deal_list_query is not supported for this SIM account")

    facts = adapter(context).get_account_facts(account())

    assert facts.complete is True
    assert facts.metadata["fill_history_unsupported"] is True
    assert facts.open_orders[0].no_fill_asserted is True
