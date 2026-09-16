"""Offline tests for the supervised generic-OMS SIM smoke harness."""

from __future__ import annotations

from argparse import Namespace
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from scripts import generic_sim_smoke_test as smoke
from src.trading_core.domain import (
    AccountBalanceSnapshot,
    BrokerCapabilities,
    BrokerOrderSnapshot,
    BrokerOrderStatus,
    BrokerSnapshot,
    ExecutionSession,
    IntentStatus,
    PositionSnapshot,
    Side,
)
from src.trading_core.ports import BrokerFill, BrokerSubmissionResult
from src.trading_core.repository import SQLiteTradingRepository


NOW = datetime(2026, 9, 15, 12, 0, tzinfo=timezone.utc)


class FakeGenericMoomooAdapter:
    """Deterministic generic adapter fake; it never contacts OpenD."""

    instances: list["FakeGenericMoomooAdapter"] = []

    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.selected_external_account_id = str(kwargs["external_account_id"])
        self.connected = False
        self.requests = []
        self._orders = {}
        self._fills = []
        FakeGenericMoomooAdapter.instances.append(self)

    def connect(self):
        self.connected = True
        return True

    def disconnect(self):
        self.connected = False
        return True

    def get_capabilities(self, _account):
        return BrokerCapabilities(broker="moomoo", supports_fill_read=True, supports_submit=True, supports_cancel=True)

    def get_snapshot(self, account):
        return BrokerSnapshot(id="snapshot", account_id=account.id, captured_at=NOW, status="COMPLETE")

    def get_balances(self, _account):
        return AccountBalanceSnapshot(id="balance", broker_snapshot_id="snapshot", currency="USD", cash=Decimal("10000"), buying_power=Decimal("10000"), equity=Decimal("10000"))

    def get_positions(self, account):
        totals = {}
        for request in self.requests:
            leg = request.order_leg
            delta = leg.quantity if leg.side is Side.BUY else -leg.quantity
            totals[leg.instrument_id] = totals.get(leg.instrument_id, Decimal("0")) + delta
        return tuple(
            PositionSnapshot(
                id=f"position:{instrument_id}", broker_snapshot_id="snapshot", account_id=account.id,
                instrument_id=instrument_id, signed_quantity=quantity, average_price=Decimal("100"), captured_at=NOW,
            )
            for instrument_id, quantity in totals.items()
            if quantity != 0
        )

    def get_open_orders(self, _account):
        return ()

    def submit_order(self, _account, request):
        self.requests.append(request)
        external_id = f"external-{len(self.requests)}"
        self._orders[external_id] = request
        self._fills.append(
            BrokerFill(
                external_order_id=external_id,
                external_fill_id=f"fill-{external_id}",
                dedupe_key=f"fill-{external_id}",
                quantity=request.order_leg.quantity,
                price=request.order_leg.limit_price or Decimal("100"),
                filled_at=NOW,
                received_at=NOW,
            )
        )
        return BrokerSubmissionResult(
            broker_order_id=request.broker_order_id,
            accepted=True,
            status=BrokerOrderStatus.WORKING,
            external_order_id=external_id,
            client_order_id=request.client_order_id,
        )

    def get_order(self, account, external_order_id):
        request = self._orders[external_order_id]
        leg = request.order_leg
        return BrokerOrderSnapshot(
            id=f"snapshot:{external_order_id}", broker_snapshot_id="snapshot", account_id=account.id,
            instrument_id=leg.instrument_id, external_order_id=external_order_id, client_order_id=request.client_order_id,
            side=leg.side, quantity=leg.quantity, filled_quantity=leg.quantity,
            status=BrokerOrderStatus.FILLED, captured_at=NOW,
        )

    def get_fills(self, _account, since=None):
        return tuple(self._fills)

    def cancel_order(self, _account, external_order_id):
        return BrokerSubmissionResult(broker_order_id=f"cancel:{external_order_id}", accepted=True, status=BrokerOrderStatus.CANCELLED, external_order_id=external_order_id)

    def replace_order(self, _account, external_order_id, _changes):
        return BrokerSubmissionResult(broker_order_id=f"replace:{external_order_id}", accepted=True, status=BrokerOrderStatus.WORKING, external_order_id=external_order_id)


class RejectingGenericMoomooAdapter(FakeGenericMoomooAdapter):
    def submit_order(self, _account, request):
        self.requests.append(request)
        return BrokerSubmissionResult(
            broker_order_id=request.broker_order_id,
            accepted=False,
            status=BrokerOrderStatus.REJECTED,
            client_order_id=request.client_order_id,
            error_code="SIM_OVERNIGHT_UNSUPPORTED",
            error_message="Paper trading does not support overnight trading sessions",
        )

    def get_fills(self, _account, since=None):
        raise AssertionError("known no-submit rejection must not poll unsupported SIM deal history")


def args(tmp_path, stage="preflight", **overrides):
    values = {
        "stage": stage,
        "state_db": str(tmp_path / "generic-smoke.db"),
        "acc_id": 42,
        "symbol1": "US.AAPL",
        "symbol2": "US.MSFT",
        "quantity1": 1,
        "quantity2": 1,
        "side1": "BUY",
        "side2": "SELL",
        "limit_price1": "100",
        "limit_price2": "200",
        "allow_extended_hours": True,
        "timeout": 1,
        "submit": False,
        "host": "127.0.0.1",
        "port": 11111,
        "security_firm": None,
    }
    values.update(overrides)
    return Namespace(**values)


def test_preflight_is_read_only_and_creates_an_isolated_generic_configuration(tmp_path):
    FakeGenericMoomooAdapter.instances.clear()
    result = smoke.run_stage(args(tmp_path), adapter_factory=FakeGenericMoomooAdapter, sleep_fn=lambda _seconds: None)
    assert result["preflight"]["ready_for_submit"] is True
    assert result["orders_submitted"] is False
    assert FakeGenericMoomooAdapter.instances[0].requests == []


def test_entry_then_exit_persists_generic_attempts_fills_and_returns_broker_flat(tmp_path):
    FakeGenericMoomooAdapter.instances.clear()
    entry = smoke.run_stage(args(tmp_path, "enter", submit=True), adapter_factory=FakeGenericMoomooAdapter, sleep_fn=lambda _seconds: None)
    assert entry["completed_intent"]["status"] == "COMPLETED"
    assert len(entry["allocations"]) == 2
    assert entry["open_reconciliation_issues"] == []

    broker = FakeGenericMoomooAdapter.instances[-1]
    # Reuse the same broker truth across a new process-style harness instance.
    class RestartedFake(FakeGenericMoomooAdapter):
        def __init__(self, **kwargs):
            self.__dict__ = broker.__dict__
            self.kwargs = kwargs

    exited = smoke.run_stage(
        args(
            tmp_path,
            "exit",
            submit=True,
            limit_price1="101",
            limit_price2="201",
        ),
        adapter_factory=RestartedFake,
        sleep_fn=lambda _seconds: None,
    )
    assert exited["completed_intent"]["status"] == "COMPLETED"
    assert exited["completed_intent"]["id"] != entry["completed_intent"]["id"]
    assert exited["broker_positions"] == []
    assert exited["open_reconciliation_issues"] == []


def test_exit_accepts_reconciled_filled_entry_after_restart_before_completion_ack(tmp_path):
    FakeGenericMoomooAdapter.instances.clear()
    entry = smoke.run_stage(
        args(tmp_path, "enter", submit=True),
        adapter_factory=FakeGenericMoomooAdapter,
        sleep_fn=lambda _seconds: None,
    )
    repository = SQLiteTradingRepository(tmp_path / "generic-smoke.db")
    repository.initialize()
    intent_id = entry["completed_intent"]["id"]
    repository.transition_intent(intent_id, IntentStatus.RECONCILIATION_REQUIRED)
    repository.transition_intent(intent_id, IntentStatus.FILLED)

    broker = FakeGenericMoomooAdapter.instances[-1]

    class RestartedFake(FakeGenericMoomooAdapter):
        def __init__(self, **kwargs):
            self.__dict__ = broker.__dict__
            self.kwargs = kwargs

    exited = smoke.run_stage(
        args(tmp_path, "exit", submit=True, limit_price1="101", limit_price2="201"),
        adapter_factory=RestartedFake,
        sleep_fn=lambda _seconds: None,
    )

    assert exited["completed_intent"]["status"] == "COMPLETED"
    assert exited["broker_positions"] == []
    assert exited["open_reconciliation_issues"] == []


def test_exit_rejects_reconciliation_entry_even_when_broker_positions_match(tmp_path):
    FakeGenericMoomooAdapter.instances.clear()
    entry = smoke.run_stage(
        args(tmp_path, "enter", submit=True),
        adapter_factory=FakeGenericMoomooAdapter,
        sleep_fn=lambda _seconds: None,
    )
    repository = SQLiteTradingRepository(tmp_path / "generic-smoke.db")
    repository.initialize()
    repository.transition_intent(entry["completed_intent"]["id"], IntentStatus.RECONCILIATION_REQUIRED)
    broker = FakeGenericMoomooAdapter.instances[-1]

    class RestartedFake(FakeGenericMoomooAdapter):
        def __init__(self, **kwargs):
            self.__dict__ = broker.__dict__
            self.kwargs = kwargs

    with pytest.raises(smoke.GenericSmokeBlocked, match="preflight blocked mutation"):
        smoke.run_stage(
            args(tmp_path, "exit", submit=True, limit_price1="101", limit_price2="201"),
            adapter_factory=RestartedFake,
            sleep_fn=lambda _seconds: None,
        )

    assert len(broker.requests) == 2


def test_rejected_generic_entry_ends_immediately_without_polling_or_second_submit(tmp_path):
    RejectingGenericMoomooAdapter.instances.clear()

    with pytest.raises(smoke.GenericSmokeBlocked, match="was rejected"):
        smoke.run_stage(
            args(tmp_path, "enter", submit=True),
            adapter_factory=RejectingGenericMoomooAdapter,
            sleep_fn=lambda _seconds: pytest.fail("rejected intent must not wait for timeout"),
        )

    adapter = RejectingGenericMoomooAdapter.instances[-1]
    assert len(adapter.requests) == 1


def test_mutating_stage_refuses_to_submit_without_explicit_flag(tmp_path):
    with pytest.raises(smoke.GenericSmokeBlocked, match="requires --submit"):
        smoke.run_stage(args(tmp_path, "enter"), adapter_factory=FakeGenericMoomooAdapter)


def test_non_regular_cli_session_requires_both_limit_prices(tmp_path):
    parsed = smoke.parse_args(
        [
            "enter",
            "--state-db",
            str(tmp_path / "generic-smoke.db"),
            "--acc-id",
            "42",
            "--session",
            "OVERNIGHT",
            "--submit",
        ]
    )

    with pytest.raises(smoke.GenericSmokeBlocked, match="non-regular-session.*both limit prices"):
        smoke.run_stage(parsed, adapter_factory=FakeGenericMoomooAdapter)


def test_cli_defaults_to_regular_session(tmp_path):
    parsed = smoke.parse_args(
        ["preflight", "--state-db", str(tmp_path / "generic-smoke.db"), "--acc-id", "42"]
    )

    assert parsed.session == "REGULAR"


def test_regular_session_defaults_to_market_orders(tmp_path):
    FakeGenericMoomooAdapter.instances.clear()

    smoke.run_stage(
        args(tmp_path, "enter", submit=True, allow_extended_hours=False, limit_price1=None, limit_price2=None),
        adapter_factory=FakeGenericMoomooAdapter,
        sleep_fn=lambda _seconds: None,
    )

    request = FakeGenericMoomooAdapter.instances[-1].requests[0]
    assert request.execution_session is ExecutionSession.REGULAR
    assert request.allow_extended_hours is False
    assert request.order_leg.order_type == "MARKET"


def test_overnight_session_propagates_as_a_limit_intent(tmp_path):
    FakeGenericMoomooAdapter.instances.clear()

    smoke.run_stage(
        args(
            tmp_path,
            "enter",
            submit=True,
            allow_extended_hours=False,
            execution_session="OVERNIGHT",
        ),
        adapter_factory=FakeGenericMoomooAdapter,
        sleep_fn=lambda _seconds: None,
    )

    request = FakeGenericMoomooAdapter.instances[-1].requests[0]
    assert request.execution_session is ExecutionSession.OVERNIGHT
    assert request.allow_extended_hours is False
    assert request.order_leg.order_type == "LIMIT"


def test_smoke_identity_is_stable_and_changes_for_material_payload_fields():
    symbols = ("US.AAPL", "US.MSFT")
    quantities = (Decimal("1"), Decimal("1"))
    sides = (Side.BUY, Side.SELL)

    regular_market_key = smoke._intent_key(
        "entry",
        symbols,
        quantities,
        sides,
        prices=(None, None),
        execution_session=ExecutionSession.REGULAR,
    )
    assert regular_market_key == smoke._intent_key(
        "entry",
        symbols,
        quantities,
        sides,
        prices=(None, None),
        execution_session=ExecutionSession.REGULAR,
    )
    assert regular_market_key.startswith("generic-sim-smoke|v2|entry|US.AAPL|US.MSFT|")

    overnight_limit_key = smoke._intent_key(
        "entry",
        symbols,
        quantities,
        sides,
        prices=(Decimal("100"), Decimal("200")),
        execution_session=ExecutionSession.OVERNIGHT,
    )
    changed_side_key = smoke._intent_key(
        "entry",
        symbols,
        quantities,
        (Side.SELL, Side.BUY),
        prices=(None, None),
        execution_session=ExecutionSession.REGULAR,
    )

    assert overnight_limit_key != regular_market_key
    assert changed_side_key != regular_market_key


def test_prior_overnight_intent_remains_auditable_and_does_not_collide_with_regular(tmp_path):
    state_db = tmp_path / "generic-smoke.db"
    repository = SQLiteTradingRepository(state_db)
    repository.initialize()
    account = smoke._build_account("42")
    symbols = ("US.AAPL", "US.MSFT")
    smoke._ensure_setup(repository, account, symbols)
    quantities = (Decimal("1"), Decimal("1"))
    entry_sides = (Side.BUY, Side.SELL)

    overnight = smoke._intent(
        "entry",
        account,
        symbols,
        quantities,
        entry_sides,
        prices=(Decimal("100"), Decimal("200")),
        execution_session=ExecutionSession.OVERNIGHT,
    )
    regular = smoke._intent(
        "entry",
        account,
        symbols,
        quantities,
        entry_sides,
        prices=(None, None),
        execution_session=ExecutionSession.REGULAR,
    )

    overnight_id, overnight_created = repository.create_intent(overnight)
    overnight_retry_id, overnight_retry_created = repository.create_intent(overnight)
    regular_id, regular_created = repository.create_intent(regular)

    assert overnight_created is True
    assert overnight_retry_created is False
    assert overnight_retry_id == overnight_id
    assert regular_created is True
    assert regular_id != overnight_id
    assert regular.idempotency_key != overnight.idempotency_key
    assert repository.get_intent_by_idempotency_key(account.id, overnight.idempotency_key)["id"] == overnight_id
    assert repository.get_intent_by_idempotency_key(account.id, regular.idempotency_key)["id"] == regular_id
