from decimal import Decimal
import json
from datetime import datetime, timezone

from scripts.generic_stage6_pilot import main
from src.strategies.stat_arb.stage6_config import Stage6PilotConfig
from src.trading_core.domain import BrokerOrderSnapshot, BrokerOrderStatus, PositionSnapshot, Side
from src.trading_core.oms import GenericOMS
from src.trading_core.ports import BrokerFactSnapshot
from src.trading_core.repository import SQLiteTradingRepository
from src.strategies.stat_arb.stage6_pilot import Stage6PilotRunner

from tests.test_generic_stage6_pilot import NOW, make_account, make_repository, make_sleeves, make_spec
from tests.test_generic_stage6_config_cli import _config_dict


SYMBOLS = ("US.AAPL", "US.MSFT", "US.TSLA", "US.NVDA")


class PreflightAdapter:
    def __init__(self, facts: BrokerFactSnapshot, *, market_state: str = "RTH") -> None:
        self.facts = facts
        self.market_state = market_state
        self.fact_calls = 0
        self.market_calls = 0
        self.submit_calls = 0
        self.cancel_calls = 0
        self.replace_calls = 0
        self.recovery_calls = 0
        self.connect_calls = 0
        self.disconnect_calls = 0

    def connect(self):
        self.connect_calls += 1
        return True

    def disconnect(self):
        self.disconnect_calls += 1
        return True

    def get_authoritative_account_facts(self, account):
        self.fact_calls += 1
        return self.facts

    def get_authoritative_market_state(self, symbols):
        self.market_calls += 1
        return {
            "market": "US",
            "captured_at": self.facts.captured_at.isoformat(),
            "complete": True,
            "rows": [{"symbol": symbol, "market_state": self.market_state} for symbol in symbols],
        }

    def submit_order(self, *_args, **_kwargs):
        self.submit_calls += 1
        raise AssertionError("broker-preflight must never submit")

    def cancel_order(self, *_args, **_kwargs):
        self.cancel_calls += 1
        raise AssertionError("broker-preflight must never cancel")

    def replace_order(self, *_args, **_kwargs):
        self.replace_calls += 1
        raise AssertionError("broker-preflight must never replace")


class NoMarketStateAdapter(PreflightAdapter):
    get_authoritative_market_state = None


def _runner(tmp_path, facts=None, adapter_type=PreflightAdapter, **kwargs):
    account = make_account()
    sleeves = make_sleeves()
    repository = make_repository(tmp_path, account, sleeves)
    facts = facts or BrokerFactSnapshot(account_id=account.id, captured_at=NOW, complete=True)
    adapter = adapter_type(facts, **kwargs)
    runner = Stage6PilotRunner(
        repository,
        GenericOMS(repository, adapter, clock=lambda: NOW),
        clock=lambda: NOW,
    )
    return account, sleeves, repository, adapter, runner


def _counts(repository):
    with repository.transaction() as connection:
        return tuple(
            connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in (
                "core_order_intents",
                "core_order_legs",
                "core_broker_orders",
                "core_fills",
            )
        )


def _run_preflight(runner, account, sleeves, *, market_symbols=SYMBOLS):
    return runner.broker_preflight(
        make_spec(account, sleeves),
        market_symbols=market_symbols,
        mapping_details=[
            {"instrument_id": instrument_id, "external_symbol": symbol}
            for instrument_id, symbol in zip(
                ("pilot-a-1", "pilot-a-2", "pilot-b-1", "pilot-b-2"),
                market_symbols,
                strict=True,
            )
        ],
    )


def test_complete_rth_flat_preflight_is_read_only_and_has_full_report(tmp_path):
    account, sleeves, repository, adapter, runner = _runner(tmp_path)
    before = _counts(repository)

    result = _run_preflight(runner, account, sleeves)

    assert result["preflight_passed"] is True
    assert result["broker_preflight_passed"] is True
    assert result["broker_contacted"] is True
    assert result["broker_facts"]["account_id"] == account.id
    assert result["broker_facts"]["complete"] is True
    assert result["broker_facts"]["flat"] is True
    assert result["preflight"]["rth"]["observed"] is True
    assert len(result["preflight"]["books"]) == 2
    assert len(result["preflight"]["mappings"]) == 4
    assert result["mutations"]["order_intents"] == 0
    assert _counts(repository) == before == (0, 0, 0, 0)
    assert (adapter.fact_calls, adapter.market_calls) == (1, 1)
    assert (adapter.submit_calls, adapter.cancel_calls, adapter.replace_calls) == (0, 0, 0)


def test_wrong_account_open_order_unexpected_position_and_incomplete_facts_block(tmp_path):
    account, sleeves, _repository, _adapter, runner = _runner(
        tmp_path / "wrong-account",
        facts=BrokerFactSnapshot(account_id="foreign-account", captured_at=NOW, complete=True),
    )
    result = _run_preflight(runner, account, sleeves)
    assert result["preflight_passed"] is False
    assert any("different account" in reason for reason in result["stop_reasons"])

    open_order = BrokerOrderSnapshot(
        id="snapshot-order",
        broker_snapshot_id="snapshot-order",
        account_id=account.id,
        instrument_id="pilot-a-1",
        external_order_id="foreign-order",
        side=Side.BUY,
        quantity=Decimal("1"),
        filled_quantity=Decimal("0"),
        status=BrokerOrderStatus.WORKING,
        captured_at=NOW,
    )
    account2, sleeves2, _repo2, _adapter2, runner2 = _runner(
        tmp_path / "open-order",
        facts=BrokerFactSnapshot(
            account_id=account.id,
            captured_at=NOW,
            complete=True,
            open_orders=(open_order,),
        ),
    )
    result2 = _run_preflight(runner2, account2, sleeves2)
    assert any("outstanding broker orders" in reason for reason in result2["stop_reasons"])

    position = PositionSnapshot(
        id="position",
        broker_snapshot_id="snapshot",
        account_id=account.id,
        instrument_id="pilot-a-1",
        signed_quantity=Decimal("1"),
        average_price=Decimal("100"),
        captured_at=NOW,
    )
    account3, sleeves3, _repo3, _adapter3, runner3 = _runner(
        tmp_path / "unexpected-position",
        facts=BrokerFactSnapshot(
            account_id=account.id,
            captured_at=NOW,
            complete=True,
            positions=(position,),
        ),
    )
    result3 = _run_preflight(runner3, account3, sleeves3)
    assert any("does not match" in reason for reason in result3["stop_reasons"])

    account4, sleeves4, _repo4, _adapter4, runner4 = _runner(
        tmp_path / "incomplete",
        facts=BrokerFactSnapshot(
            account_id=account.id,
            captured_at=NOW,
            complete=False,
            error="rate limited",
        ),
    )
    result4 = _run_preflight(runner4, account4, sleeves4)
    assert any("incomplete" in reason for reason in result4["stop_reasons"])


def test_market_state_unavailable_or_non_rth_blocks_fail_closed(tmp_path):
    account, sleeves, _repository, _adapter, runner = _runner(
        tmp_path / "unavailable", adapter_type=NoMarketStateAdapter
    )
    unavailable = _run_preflight(runner, account, sleeves)
    assert unavailable["preflight_passed"] is False
    assert any("market/RTH state capability" in reason for reason in unavailable["stop_reasons"])

    account2, sleeves2, _repo2, _adapter2, runner2 = _runner(
        tmp_path / "closed", market_state="CLOSED"
    )
    closed = _run_preflight(runner2, account2, sleeves2)
    assert closed["preflight_passed"] is False
    assert any("not RTH" in reason for reason in closed["stop_reasons"])


def test_preflight_never_invokes_legacy_baseline_import(tmp_path, monkeypatch):
    account, sleeves, repository, _adapter, runner = _runner(tmp_path)

    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("legacy baseline import must not be called by broker-preflight")

    monkeypatch.setattr(repository, "import_legacy_order_evidence", fail_if_called)
    result = _run_preflight(runner, account, sleeves)
    assert result["preflight_passed"] is True


def test_cli_broker_preflight_uses_read_only_runner_path(tmp_path, monkeypatch, capsys):
    values = _config_dict(tmp_path)
    config_path = tmp_path / "stage6-pilot.json"
    config_path.write_text(json.dumps(values), encoding="utf-8")
    config = Stage6PilotConfig.load(config_path)
    config.ensure_repository(SQLiteTradingRepository(config.state_db))
    facts = BrokerFactSnapshot(account_id=config.account.id, captured_at=datetime.now(timezone.utc), complete=True)
    adapter = PreflightAdapter(facts)
    monkeypatch.setattr(Stage6PilotConfig, "build_moomoo_adapter", lambda _self: adapter)

    assert main(["broker-preflight", "--config", str(config_path), "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "BROKER_PREFLIGHT"
    assert payload["broker_contacted"] is True
    assert payload["orders_submitted"] == 0
    assert payload["mutations"]["order_intents"] == 0
    assert adapter.submit_calls == 0
    assert (adapter.connect_calls, adapter.disconnect_calls) == (1, 1)

