from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.core.models import AccountBalance, OrderStatus, Position
from src.db.positions_db import (
    create_pair_operation,
    get_operation,
    get_open_position,
    get_open_reconciliation_issues,
    init_db,
    mark_leg_submitting,
    record_broker_submission,
    record_fill_event,
    resolve_db_path,
    transition_leg,
)
from src.execution import ExecutionConfig, ExecutionEngine, ExecutionSafetyError
from src.execution.reconciliation import compare_broker_and_local_state
from src.execution.risk import RiskLimits, RiskSnapshot, evaluate_entry_risk
from src.brokers.moomoo.adapter import MooMooAdapter
from src.brokers.moomoo.adapter import _map_broker_status


class FakeBroker:
    """Deterministic broker fake; no Moomoo SDK or network is used."""

    def __init__(self, *, fail_on_call: int | None = None, account_id: str = "123"):
        self.fail_on_call = fail_on_call
        self.account_id = account_id
        self.place_calls = 0
        self.orders: dict[str, dict] = {}
        self.fills: dict[str, list[dict]] = {}
        self.next_order_id = 100
        self.cancelled: list[str] = []
        self._net_positions: dict[str, float] = {}

    def connect(self):
        return True

    def disconnect(self):
        return True

    def get_account_balance(self):
        return AccountBalance(
            cash=100_000,
            buying_power=100_000,
            equity=100_000,
            initial_margin=0,
            maintenance_margin=0,
            account_id=self.account_id,
        )

    def get_positions(self):
        result = []
        for symbol, signed_qty in self._net_positions.items():
            if abs(signed_qty) < 1e-9:
                continue
            result.append(
                Position(
                    symbol=symbol,
                    quantity=abs(signed_qty),
                    average_price=100.0,
                    current_price=100.0,
                    side="BUY" if signed_qty > 0 else "SELL",
                )
            )
        return result

    def place_order(self, order):
        self.place_calls += 1
        if self.fail_on_call == self.place_calls:
            raise RuntimeError(f"planned failure on call {self.place_calls}")
        order_id = str(self.next_order_id)
        self.next_order_id += 1
        self.orders[order_id] = {
            "order_id": order_id,
            "code": order.symbol,
            "symbol": order.symbol,
            "side": order.side.value,
            "trd_side": order.side.value,
            "qty": float(order.quantity),
            "order_status": "SUBMITTED",
            "remark": order.remark,
            "create_time": "2026-09-12T10:00:00+00:00",
            "dealt_qty": 0.0,
            "dealt_avg_price": 0.0,
        }
        self.fills[order_id] = []
        return order_id, self.orders[order_id]["create_time"]

    def cancel_order(self, order_id):
        self.cancelled.append(str(order_id))
        if str(order_id) in self.orders:
            self.orders[str(order_id)]["order_status"] = "CANCELLED_ALL"
        return True

    def get_recent_orders(self):
        return list(self.orders.values())

    def get_open_orders(self):
        return [
            row for row in self.orders.values()
            if row["order_status"] not in {"FILLED_ALL", "CANCELLED_ALL", "REJECTED"}
        ]

    def get_order_status(self, order_id):
        status = self.orders[str(order_id)]["order_status"]
        return {
            "FILLED_ALL": OrderStatus.FILLED,
            "CANCELLED_ALL": OrderStatus.CANCELLED,
            "REJECTED": OrderStatus.REJECTED,
            "FILLED_PART": OrderStatus.PARTIALLY_FILLED,
        }.get(status, OrderStatus.SUBMITTED)

    def get_order_fills(self, order_id):
        return list(self.fills.get(str(order_id), []))

    def validate_symbols(self, symbols):
        return True

    def get_market_state(self, symbols):
        return {"rows": [{"code": symbol, "market_state": "OPEN"} for symbol in symbols]}

    def fill(self, order_id: str, *, fill_id: str, quantity: float, price: float, time: str = "2026-09-12T10:01:00+00:00"):
        order = self.orders[str(order_id)]
        self.fills[str(order_id)].append({
            "order_id": str(order_id),
            "deal_id": fill_id,
            "qty": quantity,
            "price": price,
            "create_time": time,
        })
        fills = self.fills[str(order_id)]
        cumulative = sum(float(fill["qty"]) for fill in fills)
        average = sum(float(fill["qty"]) * float(fill["price"]) for fill in fills) / cumulative
        order["dealt_qty"] = cumulative
        order["dealt_avg_price"] = average
        order["order_status"] = "FILLED_ALL" if cumulative >= order["qty"] else "FILLED_PART"
        signed = cumulative if order["side"] == "BUY" else -cumulative
        self._net_positions[order["symbol"]] = self._net_positions.get(order["symbol"], 0.0) + (signed if len(fills) == 1 else 0.0)
        if len(fills) > 1:
            # The first call already added the prior fills; add only this deal.
            self._net_positions[order["symbol"]] += (float(quantity) if order["side"] == "BUY" else -float(quantity))


class AcceptedThenLostBroker(FakeBroker):
    """Simulate an accepted order whose response is lost before the ID is returned."""

    def place_order(self, order):
        order_id, submitted_at = super().place_order(order)
        if self.place_calls == 1:
            raise RuntimeError("connection lost after broker accepted the order")
        return order_id, submitted_at


def make_config(path: Path, **overrides) -> ExecutionConfig:
    values = {
        "state_db_path": str(path),
        "max_pair_exposure": 10_000,
        "max_gross_exposure": 50_000,
        "max_account_utilization": 0.25,
        "max_margin_utilization": 0.50,
    }
    values.update(overrides)
    return ExecutionConfig(**values)


def entry_signal():
    return {
        "pair": "AAA-BBB",
        "ticker1": "AAA",
        "ticker2": "BBB",
        "entry_zscore": 2.5,
        "entry_hedge_ratio": 1.0,
        "entry_alpha": 0.1,
        "entry_residual_mean": 0.0,
        "entry_residual_std": 1.0,
        "latest_price_s1": 100.0,
        "latest_price_s2": 50.0,
    }


def entry_plan():
    return {
        "ticker1_side": "BUY",
        "ticker2_side": "SELL",
        "ticker1_qty": 10,
        "ticker2_qty": 20,
        "ticker1_intended_qty": 10.0,
        "ticker2_intended_qty": 20.0,
    }


@pytest.fixture
def state_path(tmp_path):
    path = tmp_path / "trading.db"
    init_db(path)
    return path


def submit_entry(engine: ExecutionEngine, broker: FakeBroker, path: Path):
    result = engine.execute_entry(entry_signal(), entry_plan(), operation_date="2026-09-12")
    assert len(result["legs"]) == 2
    assert broker.place_calls == 2
    return result


def fill_entry_fully(broker: FakeBroker, operation: dict):
    for leg in operation["legs"]:
        broker.fill(
            leg["broker_order_id"],
            fill_id=f"deal-{leg['leg']}",
            quantity=float(leg["requested_quantity"]),
            price=float(leg["intended_price"]),
        )


def test_normal_two_leg_entry_and_durable_intent(state_path):
    broker = FakeBroker()
    engine = ExecutionEngine(broker, config=make_config(state_path))
    result = submit_entry(engine, broker, state_path)
    assert result["status"] == "leg2_submitted"
    with engine.run_lock():
        reconciled = engine.reconcile(timeout=0)
    assert reconciled.ready
    assert get_open_position("AAA-BBB", state_path) is None
    # Intent and both order rows exist even though neither leg filled.
    operation = get_operation(result["operation_id"], state_path)
    assert operation["metadata"]["entry_hedge_ratio"] == 1.0
    assert all(leg["local_order_id"] for leg in operation["legs"])


def test_partial_fills_accumulate_and_open_only_at_full_quantity(state_path):
    broker = FakeBroker()
    engine = ExecutionEngine(broker, config=make_config(state_path))
    operation = submit_entry(engine, broker, state_path)
    leg = operation["legs"][0]
    broker.fill(leg["broker_order_id"], fill_id="a", quantity=2.5, price=100.0)
    engine.reconcile(timeout=0)
    current = get_operation(operation["operation_id"], state_path)
    current_leg = current["legs"][0]
    assert current_leg["cumulative_filled_quantity"] == pytest.approx(2.5)
    assert current_leg["status"] == "partially_filled"
    assert get_open_position("AAA-BBB", state_path) is None
    broker.fill(leg["broker_order_id"], fill_id="b", quantity=5.0, price=101.0)
    broker.fill(leg["broker_order_id"], fill_id="c", quantity=2.5, price=99.0)
    # Fill the second leg as well, then the operation can become a position.
    second = operation["legs"][1]
    broker.fill(second["broker_order_id"], fill_id="d", quantity=20.0, price=50.0)
    final = engine.reconcile(timeout=0)
    assert final.applied_fill_count >= 3
    current = get_operation(operation["operation_id"], state_path)
    assert current["status"] == "open"
    assert current["legs"][0]["cumulative_filled_quantity"] == pytest.approx(10.0)
    assert get_open_position("AAA-BBB", state_path) is not None
    duplicate = record_fill_event(
        operation_id=operation["operation_id"],
        leg="ticker1",
        broker_order_id=leg["broker_order_id"],
        broker_fill_id="c",
        fill_price=99.0,
        fill_time="2026-09-12T10:01:00+00:00",
        fill_quantity=2.5,
        db_path=state_path,
    )
    assert duplicate["duplicate"] is True


def test_out_of_order_fill_events_are_idempotent_and_weighted(state_path):
    broker = FakeBroker()
    engine = ExecutionEngine(broker, config=make_config(state_path))
    operation = submit_entry(engine, broker, state_path)
    leg = operation["legs"][0]
    first = record_fill_event(
        operation_id=operation["operation_id"], leg="ticker1", broker_order_id=leg["broker_order_id"],
        broker_fill_id="late", fill_price=110, fill_time="2026-09-12T10:03:00+00:00", fill_quantity=7.5, db_path=state_path,
    )
    second = record_fill_event(
        operation_id=operation["operation_id"], leg="ticker1", broker_order_id=leg["broker_order_id"],
        broker_fill_id="early", fill_price=90, fill_time="2026-09-12T10:01:00+00:00", fill_quantity=2.5, db_path=state_path,
    )
    assert first["cumulative_filled_quantity"] == pytest.approx(7.5)
    assert second["cumulative_filled_quantity"] == pytest.approx(10.0)
    assert second["average_fill_price"] == pytest.approx(105.0)
    duplicate = record_fill_event(
        operation_id=operation["operation_id"], leg="ticker1", broker_order_id=leg["broker_order_id"],
        broker_fill_id="late", fill_price=110, fill_time="2026-09-12T10:03:00+00:00", fill_quantity=7.5, db_path=state_path,
    )
    assert duplicate["duplicate"] is True


def test_filled_transition_requires_full_quantity(state_path):
    broker = FakeBroker()
    engine = ExecutionEngine(broker, config=make_config(state_path))
    operation = submit_entry(engine, broker, state_path)
    with pytest.raises(ValueError, match="before requested quantity"):
        transition_leg(operation["operation_id"], "ticker1", "filled", db_path=state_path)


def test_leg_one_submission_failure_does_not_submit_leg_two(state_path):
    broker = FakeBroker(fail_on_call=1)
    engine = ExecutionEngine(broker, config=make_config(state_path))
    result = engine.execute_entry(entry_signal(), entry_plan(), operation_date="2026-09-12")
    assert broker.place_calls == 1
    assert result["status"] == "requires_reconciliation"
    assert result["legs"][0]["status"] == "requires_reconciliation"
    assert result["legs"][1]["status"] == "created"


def test_restart_recovers_unknown_first_submission_and_resumes_second_leg(state_path):
    broker = AcceptedThenLostBroker()
    engine = ExecutionEngine(broker, config=make_config(state_path))
    first = engine.execute_entry(entry_signal(), entry_plan(), operation_date="2026-09-12")

    assert broker.place_calls == 1
    assert first["status"] == "requires_reconciliation"
    assert first["legs"][0]["broker_order_id"] is None

    restarted = ExecutionEngine(broker, config=make_config(state_path))
    resumed = restarted.execute_entry(entry_signal(), entry_plan(), operation_date="2026-09-12")

    assert broker.place_calls == 2
    assert resumed["status"] == "leg2_submitted"
    assert resumed["legs"][0]["broker_order_id"] == "100"
    assert resumed["legs"][1]["broker_order_id"] == "101"


def test_leg_two_failure_is_high_priority_reconciliation(state_path):
    broker = FakeBroker(fail_on_call=2)
    engine = ExecutionEngine(broker, config=make_config(state_path))
    result = engine.execute_entry(entry_signal(), entry_plan(), operation_date="2026-09-12")
    assert broker.place_calls == 2
    assert result["status"] == "requires_reconciliation"
    assert broker.cancelled == [result["legs"][0]["broker_order_id"]]
    issues = get_open_reconciliation_issues(state_path)
    assert any(issue["category"] == "one_leg_submission_failure" for issue in issues)


def test_restart_reconciles_persisted_orders_and_missed_callback(state_path):
    broker = FakeBroker()
    first_engine = ExecutionEngine(broker, config=make_config(state_path))
    operation = submit_entry(first_engine, broker, state_path)
    fill_entry_fully(broker, operation)
    restarted = ExecutionEngine(broker, config=make_config(state_path))
    result = restarted.startup_reconcile()
    assert result.applied_fill_count == 2
    assert get_operation(operation["operation_id"], state_path)["status"] == "open"
    assert get_open_position("AAA-BBB", state_path) is not None


def test_duplicate_daily_invocation_is_suppressed(state_path):
    broker = FakeBroker()
    engine = ExecutionEngine(broker, config=make_config(state_path))
    first = engine.execute_entry(entry_signal(), entry_plan(), operation_date="2026-09-12")
    second = engine.execute_entry(entry_signal(), entry_plan(), operation_date="2026-09-12")
    assert second["operation_id"] == first["operation_id"]
    assert broker.place_calls == 2
    next_day = engine.execute_entry(entry_signal(), entry_plan(), operation_date="2026-09-13")
    assert next_day["operation_id"] == first["operation_id"]
    assert broker.place_calls == 2


def test_concurrent_runner_lock_fails_closed(state_path):
    engine = ExecutionEngine(FakeBroker(), config=make_config(state_path))
    with engine.run_lock():
        with pytest.raises(ExecutionSafetyError):
            with engine.run_lock():
                pass


def test_normal_two_leg_exit_closes_position(state_path):
    broker = FakeBroker()
    engine = ExecutionEngine(broker, config=make_config(state_path))
    entry = submit_entry(engine, broker, state_path)
    fill_entry_fully(broker, entry)
    engine.reconcile(timeout=0)
    position = get_open_position("AAA-BBB", state_path)
    assert position is not None
    exit_signal = {
        "pair": "AAA-BBB",
        "ticker1": "AAA",
        "ticker2": "BBB",
        "exit_reason": "z_exit",
        "exit_zscore": 0.2,
        "latest_price_s1": 102.0,
        "latest_price_s2": 49.0,
        "open_pos": position,
    }
    exit_op = engine.execute_exit(exit_signal, operation_date="2026-09-12")
    assert exit_op["status"] == "leg2_submitted"
    for leg in exit_op["legs"]:
        broker.fill(leg["broker_order_id"], fill_id=f"exit-{leg['leg']}", quantity=leg["requested_quantity"], price=leg["intended_price"])
    engine.reconcile(timeout=0)
    assert get_open_position("AAA-BBB", state_path) is None
    assert get_operation(exit_op["operation_id"], state_path)["status"] == "closed"


def test_reconciliation_classifies_broker_local_mismatches():
    result = compare_broker_and_local_state(
        local_positions=[],
        broker_positions=[Position(symbol="US.AAA", quantity=3, average_price=100, side="BUY")],
        local_orders=[],
        broker_orders=[],
    )
    assert any(issue.category == "broker_position_untracked" for issue in result.issues)
    result = compare_broker_and_local_state(
        local_positions=[{
            "status": "open", "ticker1": "AAA", "ticker2": "BBB",
            "entry_side1": "BUY", "entry_side2": "SELL",
            "executed_size1": 10, "executed_size2": 20,
        }],
        broker_positions=[], local_orders=[], broker_orders=[],
    )
    assert sum(issue.category == "local_position_missing_at_broker" for issue in result.issues) == 2
    duplicate = compare_broker_and_local_state(
        local_positions=[
            {"id": 1, "status": "open", "pair": "AAA-BBB", "ticker1": "AAA", "ticker2": "BBB",
             "entry_side1": "BUY", "entry_side2": "SELL", "executed_size1": 10, "executed_size2": 20},
            {"id": 2, "status": "open", "pair": "AAA-BBB", "ticker1": "AAA", "ticker2": "BBB",
             "entry_side1": "BUY", "entry_side2": "SELL", "executed_size1": 10, "executed_size2": 20},
        ],
        broker_positions=[], local_orders=[], broker_orders=[],
    )
    assert any(issue.category == "duplicate_local_position" for issue in duplicate.issues)


def risk_legs():
    return [
        {"intended_price": 100, "requested_quantity": 10},
        {"intended_price": 50, "requested_quantity": 20},
    ]


def risk_snapshot(**kwargs):
    values = dict(
        equity=100_000, buying_power=100_000, initial_margin=0,
        maintenance_margin=0, gross_exposure=0, open_pairs=0,
        pending_operations=0, unresolved_reconciliation=0,
    )
    values.update(kwargs)
    return RiskSnapshot(**values)


def test_risk_gate_approves_with_capacity():
    decision = evaluate_entry_risk(risk_snapshot(), risk_legs(), RiskLimits(), environment="SIMULATE")
    assert decision.approved


def test_risk_gate_rejects_insufficient_buying_power_and_caps():
    limits = RiskLimits(max_pair_exposure=10_000, max_gross_exposure=2_100)
    decision = evaluate_entry_risk(risk_snapshot(buying_power=1_000), risk_legs(), limits, environment="SIMULATE")
    assert not decision.approved and "buying power" in decision.reason
    decision = evaluate_entry_risk(risk_snapshot(gross_exposure=1_000), risk_legs(), limits, environment="SIMULATE")
    assert not decision.approved and "gross" in decision.reason


def test_risk_gate_rejects_margin_and_real_query_failure():
    limits = RiskLimits(max_margin_utilization=0.5)
    decision = evaluate_entry_risk(risk_snapshot(initial_margin=49_000), risk_legs(), limits, environment="REAL")
    assert not decision.approved and "margin" in decision.reason
    decision = evaluate_entry_risk(risk_snapshot(risk_data_available=False), risk_legs(), limits, environment="REAL")
    assert not decision.approved and "unavailable" in decision.reason


def test_real_arming_requires_explicit_conditions():
    with pytest.raises(PermissionError):
        ExecutionConfig(trd_env="REAL", state_db_path="C:/tmp/state.db").validate()
    with pytest.raises(PermissionError):
        ExecutionConfig(
            trd_env="REAL", state_db_path="C:/tmp/state.db",
            expected_real_account_id=123, account_id=124, enable_live_trading=True,
        ).validate()
    with pytest.raises(PermissionError):
        MooMooAdapter(
            trd_env="REAL", expected_account_id=123, acc_id=123,
            enable_live_trading=False,
        )
    assert ExecutionConfig().trd_env == "SIMULATE"


def test_broker_terminal_and_partial_statuses_are_not_treated_as_submitted():
    assert _map_broker_status("SUBMIT_FAILED") == "rejected"
    assert _map_broker_status("TIMEOUT") == "rejected"
    assert _map_broker_status("CANCELLED_ALL") == "cancelled"
    assert _map_broker_status("CANCELLED_PART") == "partially_filled"


def test_adapter_account_validation_is_fail_closed_for_sim_rows():
    adapter = MooMooAdapter(trd_env="SIMULATE", acc_id=123)
    valid = {
        "acc_id": 123,
        "trd_env": "SIMULATE",
        "acc_status": "ACTIVE",
        "acc_role": "N/A",
        "acc_type": "MARGIN",
        "trdmarket_auth": [2],
    }
    adapter._validate_account_rows([valid])

    for field, value in (("trdmarket_auth", []), ("acc_status", "DISABLED"), ("acc_type", "CASH")):
        invalid = dict(valid)
        invalid[field] = value
        with pytest.raises(PermissionError):
            adapter._validate_account_rows([invalid])


def test_real_engine_allows_fake_order_only_when_armed(tmp_path):
    path = tmp_path / "real.db"
    init_db(path)
    config = make_config(
        path,
        trd_env="REAL",
        account_id=123,
        expected_real_account_id=123,
        enable_live_trading=True,
    )
    engine = ExecutionEngine(FakeBroker(), config=config)
    result = engine.execute_entry(
        entry_signal(), entry_plan(),
        signal_timestamp=datetime.now(timezone.utc), operation_date="2026-09-12",
    )
    assert result["status"] == "leg2_submitted"


def test_real_engine_rejects_account_identity_mismatch(tmp_path):
    path = tmp_path / "real-mismatch.db"
    init_db(path)
    config = make_config(
        path,
        trd_env="REAL",
        account_id=123,
        expected_real_account_id=123,
        enable_live_trading=True,
    )
    engine = ExecutionEngine(FakeBroker(account_id="999"), config=config)
    with pytest.raises(ExecutionSafetyError, match="account identity"):
        engine.execute_entry(
            entry_signal(), entry_plan(),
            signal_timestamp=datetime.now(timezone.utc), operation_date="2026-09-12",
        )


def test_state_path_is_project_relative_not_launch_cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    resolved = resolve_db_path("data/example.db")
    assert resolved == Path(__file__).resolve().parents[1] / "data" / "example.db"
    assert resolved != tmp_path / "data" / "example.db"
