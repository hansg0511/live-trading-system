from __future__ import annotations

from argparse import Namespace
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from scripts import sim_smoke_test
from src.core.models import AccountBalance, Position
from src.brokers.moomoo.adapter import _parse_market_auth


def _account_row(
    acc_id: int,
    *,
    acc_type: str = "MARGIN",
    sim_acc_type: str = "STOCK_AND_OPTION",
    acc_role: str = "N/A",
    trd_env: str = "SIMULATE",
    auth: str = "US",
    status: str = "ACTIVE",
):
    return {
        "acc_id": acc_id,
        "acc_type": acc_type,
        "sim_acc_type": sim_acc_type,
        "acc_role": acc_role,
        "trd_env": trd_env,
        "security_firm": "FUTUMY",
        "trdmarket_auth": auth,
        "acc_status": status,
    }


class FakeSmokeAdapter:
    """Small adapter fake for harness gates; it never submits an order."""

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.place_calls = 0
        self.connected = False

    def connect(self):
        self.connected = True
        return True

    def disconnect(self):
        self.connected = False
        return True

    def get_account_balance(self):
        return AccountBalance(
            cash=10_000.0,
            buying_power=10_000.0,
            equity=10_000.0,
            initial_margin=0.0,
            maintenance_margin=0.0,
            account_id=str(self.kwargs["acc_id"]),
        )

    def get_positions(self):
        return []

    def get_open_orders(self):
        return []

    def get_recent_orders(self):
        return []

    def get_order_status(self, _order_id):
        return "SUBMITTED"

    def get_order_fills(self, _order_id):
        return []

    def validate_symbols(self, _symbols):
        return True

    def get_market_state(self, symbols):
        return {"rows": [{"code": symbol, "market_state": "OPEN"} for symbol in symbols]}

    def get_market_snapshot(self, symbols):
        prices = {symbols[0]: 100.0, symbols[1]: 200.0}
        return {
            "rows": [
                {"code": symbol, "last_price": prices[symbol], "update_time": "2026-09-14 10:00:00"}
                for symbol in symbols
            ]
        }

    def place_order(self, _order):
        self.place_calls += 1
        raise AssertionError("preflight must never call the broker order endpoint")


class FakeStatusMismatchAdapter(FakeSmokeAdapter):
    def get_positions(self):
        return [
            Position(
                symbol="US.UNRELATED",
                quantity=1.0,
                average_price=10.0,
                current_price=10.0,
                side="LONG",
            )
        ]


class FakeNoTimestampAdapter(FakeSmokeAdapter):
    def get_market_snapshot(self, symbols):
        return {
            "rows": [
                {"code": symbol, "last_price": 100.0 if symbol == symbols[0] else 200.0}
                for symbol in symbols
            ]
        }


def _args(tmp_path, stage="preflight", **overrides):
    values = {
        "stage": stage,
        "state_db": str(tmp_path / "sim-smoke.db"),
        "symbol1": "US.AAPL",
        "symbol2": "US.MSFT",
        "pair": None,
        "acc_id": None,
        "security_firm": None,
        "quantity1": 1,
        "quantity2": 1,
        "side1": "BUY",
        "side2": "SELL",
        "entry_zscore": 2.5,
        "gross_cap": 1_000.0,
        "host": "127.0.0.1",
        "port": 11111,
        "timeout": 0,
        "submit": False,
        "json": True,
    }
    values.update(overrides)
    return Namespace(**values)


def _regular_monday():
    # 14:00 UTC is 10:00 America/New_York on this test date.
    return datetime(2026, 9, 14, 14, 0, tzinfo=timezone.utc)


def test_eligible_account_requires_us_sim_margin_and_excludes_real_or_master():
    rows = [
        _account_row(1, acc_type="CASH", auth="US"),
        _account_row(2, auth="HK"),
        _account_row(3, trd_env="REAL"),
        _account_row(4, acc_role="MASTER"),
        _account_row(5),
        _account_row(6, status="DISABLED"),
    ]

    selected = sim_smoke_test.eligible_sim_accounts(rows)

    assert [account.acc_id for account in selected] == [5]
    assert sim_smoke_test.select_sim_account(rows).acc_id == 5


def test_numeric_moomoo_market_authorization_is_normalized():
    assert sim_smoke_test._normalise_auth([2, 15, 111]) == {"US", "JP", "MY"}
    assert _parse_market_auth([2, 15, 111]) == {"US", "JP", "MY"}
    assert sim_smoke_test.select_sim_account([_account_row(7, auth=[2])]).acc_id == 7


def test_multiple_eligible_sim_accounts_require_explicit_selection():
    rows = [_account_row(10), _account_row(11)]

    with pytest.raises(sim_smoke_test.SmokeTestError, match="Multiple eligible"):
        sim_smoke_test.select_sim_account(rows)
    assert sim_smoke_test.select_sim_account(rows, acc_id=11).acc_id == 11


def test_regular_session_gate_rejects_weekend_and_closed_state():
    sunday = lambda: datetime(2026, 9, 13, 14, 0, tzinfo=timezone.utc)

    blockers = sim_smoke_test._regular_session_blockers(["AFTER_HOURS_END"], now_fn=sunday)

    assert any("weekends" in blocker for blocker in blockers)
    assert any("not regular US session" in blocker for blocker in blockers)
    assert sim_smoke_test._regular_session_blockers(["OPEN"], now_fn=_regular_monday) == []


def test_preflight_selects_one_account_and_never_submits(tmp_path, monkeypatch):
    monkeypatch.setenv("FUTU_TRD_ENV", "SIMULATE")
    adapter_holder = {}

    def factory(**kwargs):
        adapter = FakeSmokeAdapter(**kwargs)
        adapter_holder["adapter"] = adapter
        return adapter

    result = sim_smoke_test.run_stage(
        _args(tmp_path),
        discoverer=lambda **_kwargs: [_account_row(5077333)],
        adapter_factory=factory,
        now_fn=_regular_monday,
    )

    assert result["account"]["acc_id"] == 5077333
    assert result["ready_for_submit"] is True
    assert result["risk_admission"] == "approved"
    assert result["reference_prices"]["US.AAPL"]["last_price"] == 100.0
    assert result["proposed_gross_exposure"] == 300.0
    assert result["orders_submitted"] is False
    assert adapter_holder["adapter"].place_calls == 0


def test_blocked_preflight_does_not_arm_later_mutating_stage(tmp_path, monkeypatch):
    monkeypatch.setenv("FUTU_TRD_ENV", "SIMULATE")
    discoverer = lambda **_kwargs: [_account_row(5077333)]

    result = sim_smoke_test.run_stage(
        _args(tmp_path),
        discoverer=discoverer,
        adapter_factory=FakeStatusMismatchAdapter,
        now_fn=_regular_monday,
    )

    assert result["ready_for_submit"] is False
    assert sim_smoke_test._read_saved_selection(tmp_path / "sim-smoke.db") is None
    with pytest.raises(sim_smoke_test.SmokeTestError, match="completed preflight"):
        sim_smoke_test.run_stage(
            _args(tmp_path, stage="enter", acc_id=5077333, submit=True),
            discoverer=discoverer,
            adapter_factory=FakeSmokeAdapter,
            now_fn=_regular_monday,
        )


def test_preflight_rejects_missing_snapshot_timestamp(tmp_path, monkeypatch):
    monkeypatch.setenv("FUTU_TRD_ENV", "SIMULATE")
    result = sim_smoke_test.run_stage(
        _args(tmp_path),
        discoverer=lambda **_kwargs: [_account_row(5077333)],
        adapter_factory=FakeNoTimestampAdapter,
        now_fn=_regular_monday,
    )

    assert result["ready_for_submit"] is False
    assert any("no parseable update_time" in blocker for blocker in result["blockers"])


def test_account_discovery_does_not_hide_second_eligible_account(monkeypatch):
    rows_by_firm = {
        None: [_account_row(10)],
        "FUTUSECURITIES": [_account_row(11)],
    }

    class FakeTradeContext:
        def __init__(self, **kwargs):
            self.firm = kwargs.get("security_firm")

        def get_acc_list(self):
            return 0, rows_by_firm.get(self.firm, [])

        def close(self):
            return None

    fake_moo = SimpleNamespace(
        TrdMarket=SimpleNamespace(NONE="NONE"),
        SecurityFirm=SimpleNamespace(
            FUTUSECURITIES="FUTUSECURITIES",
            FUTUINC="FUTUINC",
            FUTUSG="FUTUSG",
            FUTUAU="FUTUAU",
            FUTUCA="FUTUCA",
            FUTUJP="FUTUJP",
            FUTUMY="FUTUMY",
        ),
        OpenSecTradeContext=FakeTradeContext,
    )
    monkeypatch.setattr(sim_smoke_test, "_import_moomoo", lambda: fake_moo)

    rows = sim_smoke_test.discover_sim_accounts()
    assert {int(row["acc_id"]) for row in rows} == {10, 11}
    with pytest.raises(sim_smoke_test.SmokeTestError, match="Multiple eligible"):
        sim_smoke_test.select_sim_account(rows)


def test_status_surfaces_unresolved_reconciliation_as_a_blocker(tmp_path, monkeypatch):
    monkeypatch.setenv("FUTU_TRD_ENV", "SIMULATE")
    discoverer = lambda **_kwargs: [_account_row(5077333)]

    sim_smoke_test.run_stage(
        _args(tmp_path),
        discoverer=discoverer,
        adapter_factory=FakeSmokeAdapter,
        now_fn=_regular_monday,
    )
    result = sim_smoke_test.run_stage(
        _args(tmp_path, stage="status"),
        discoverer=discoverer,
        adapter_factory=FakeStatusMismatchAdapter,
    )

    assert result["reconciliation_ready"] is False
    assert result["blockers"]
    assert result["orders_submitted"] is False

    with pytest.raises(sim_smoke_test.SmokeTestError, match="symbols/pair differ"):
        sim_smoke_test.run_stage(
            _args(tmp_path, stage="status", symbol2="US.NVDA"),
            discoverer=discoverer,
            adapter_factory=FakeStatusMismatchAdapter,
        )


def test_mutating_stage_requires_submit_before_account_or_broker_access(tmp_path, monkeypatch):
    monkeypatch.setenv("FUTU_TRD_ENV", "SIMULATE")
    called = False

    def discoverer(**_kwargs):
        nonlocal called
        called = True
        return [_account_row(5077333)]

    with pytest.raises(sim_smoke_test.SmokeTestError, match="requires explicit --submit"):
        sim_smoke_test.run_stage(
            _args(tmp_path, stage="enter"),
            discoverer=discoverer,
            adapter_factory=FakeSmokeAdapter,
        )
    assert called is False


def test_mutating_stage_requires_the_completed_preflight_selection(tmp_path, monkeypatch):
    monkeypatch.setenv("FUTU_TRD_ENV", "SIMULATE")

    with pytest.raises(sim_smoke_test.SmokeTestError, match="completed preflight"):
        sim_smoke_test.run_stage(
            _args(tmp_path, stage="enter", submit=True, acc_id=5077333),
            discoverer=lambda **_kwargs: [_account_row(5077333)],
            adapter_factory=FakeSmokeAdapter,
        )


def test_real_environment_is_hard_refused(monkeypatch):
    monkeypatch.setenv("FUTU_TRD_ENV", "REAL")

    with pytest.raises(sim_smoke_test.SmokeTestError, match="refuses REAL"):
        sim_smoke_test._validate_sim_environment()
