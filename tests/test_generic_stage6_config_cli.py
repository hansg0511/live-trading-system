from __future__ import annotations

from datetime import datetime, timezone
from dataclasses import replace
import json
from pathlib import Path

import pytest

from scripts.generic_stage6_pilot import main
from src.strategies.stat_arb.stage6_config import Stage6ConfigError, Stage6PilotConfig
from src.trading_core.domain import ExecutionPolicy, ExecutionSession, TradingEnvironment
from src.trading_core.repository import SQLiteTradingRepository


NOW = "2026-09-20T12:00:00+00:00"


def _config_dict(tmp_path: Path) -> dict:
    return {
        "schema_version": 1,
        "state_db": str(tmp_path / "stage6-pilot.db"),
        "market": "US",
        "security_firm": "FUTUINC",
        "account": {
            "id": "pilot-account",
            "broker": "moomoo",
            "environment": "SIM",
            "external_account_id": "5077333",
            "base_currency": "USD",
            "enabled": True,
            "created_at": NOW,
            "updated_at": NOW,
            "metadata": {
                "account_capacity": "20",
                "allocation_capacity": "20",
                "capacity_unit": "FRACTION",
            },
        },
        "strategy": {
            "id": "pilot-strategy",
            "name": "Controlled pilot strategy",
            "strategy_type": "generic_stat_arb",
            "version": "1",
            "enabled": True,
            "created_at": NOW,
            "updated_at": NOW,
            "config": {"lookback": 40},
        },
        "books": [
            {"id": "pilot-book-a", "name": "Pilot A", "enabled": True, "created_at": NOW, "updated_at": NOW},
            {"id": "pilot-book-b", "name": "Pilot B", "enabled": True, "created_at": NOW, "updated_at": NOW},
        ],
        "sleeves": [
            {
                "sleeve_id": "pilot-sleeve-a",
                "strategy_id": "pilot-strategy",
                "account_id": "pilot-account",
                "book_id": "pilot-book-a",
                "name": "Pilot Sleeve A",
                "version": "1",
                "enabled": True,
                "configuration": {"z_entry": 2.0},
                "pair": [
                    {"instrument_id": "pilot-a-1", "symbol": "PILOT_A1", "moomoo_symbol": "US.AAPL", "venue": "US", "currency": "USD"},
                    {"instrument_id": "pilot-a-2", "symbol": "PILOT_A2", "moomoo_symbol": "US.MSFT", "venue": "US", "currency": "USD"},
                ],
            },
            {
                "sleeve_id": "pilot-sleeve-b",
                "strategy_id": "pilot-strategy",
                "account_id": "pilot-account",
                "book_id": "pilot-book-b",
                "name": "Pilot Sleeve B",
                "version": "1",
                "enabled": True,
                "configuration": {"z_entry": 2.0},
                "pair": [
                    {"instrument_id": "pilot-b-1", "symbol": "PILOT_B1", "moomoo_symbol": "US.TSLA", "venue": "US", "currency": "USD"},
                    {"instrument_id": "pilot-b-2", "symbol": "PILOT_B2", "moomoo_symbol": "US.NVDA", "venue": "US", "currency": "USD"},
                ],
            },
        ],
        "allocation_update": {
            "account_id": "pilot-account",
            "version": 1,
            "effective_at": NOW,
            "provenance": "operator-approved-test-allocation",
            "targets": [
                {"sleeve_id": "pilot-sleeve-a", "book_id": "pilot-book-a", "target_weight": 0.50, "capacity_unit": "FRACTION"},
                {"sleeve_id": "pilot-sleeve-b", "book_id": "pilot-book-b", "target_weight": 0.50, "capacity_unit": "FRACTION"},
            ],
        },
        "targets": [
            {
                "sleeve_id": "pilot-sleeve-a",
                "cycle_id": "pilot-cycle-a",
                "signal_id": "pilot-signal-a",
                "instrument_ids": ["pilot-a-1", "pilot-a-2"],
                "signed_quantities": [1, -1],
                "action": "ENTER",
                "evaluated_at": NOW,
                "provenance": {"source": "offline-test-driver"},
            },
            {
                "sleeve_id": "pilot-sleeve-b",
                "cycle_id": "pilot-cycle-b",
                "signal_id": "pilot-signal-b",
                "instrument_ids": ["pilot-b-1", "pilot-b-2"],
                "signed_quantities": [1, -1],
                "action": "ENTER",
                "evaluated_at": NOW,
                "provenance": {"source": "offline-test-driver"},
            },
        ],
        "require_flat_entry": True,
        "execution_policy": {
            "rth_handoff_policy": "RTH_ONLY",
            "execution_session": "REGULAR",
            "allow_extended_hours": False,
            "legging_policy": "SEQUENTIAL",
            "partial_fill_policy": "WAIT",
            "failure_policy": "HOLD_AND_RECONCILE",
            "max_attempts": 1,
            "timeout_seconds": 300,
            "stale_order_seconds": 300,
        },
    }


def _write_config(tmp_path: Path) -> Path:
    path = tmp_path / "stage6-pilot.json"
    path.write_text(json.dumps(_config_dict(tmp_path), indent=2), encoding="utf-8")
    return path


def _write_values(tmp_path: Path, values: dict, name: str) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(values, indent=2), encoding="utf-8")
    return path


def test_checked_in_template_is_invalid_and_non_runnable():
    template = Path("configs/stage6-pilot.template.json")
    with pytest.raises(Stage6ConfigError, match="placeholder"):
        Stage6PilotConfig.load(template)


def test_config_materializes_policy_resolver_and_deterministic_confirmation(tmp_path):
    config = Stage6PilotConfig.load(_write_config(tmp_path))

    assert config.account.external_account_id == "5077333"
    assert config.execution.policy.execution_session.value == "REGULAR"
    assert config.execution.rth_handoff_policy == "RTH_ONLY"
    assert config.resolver().symbol_for_instrument("pilot-a-1") == "US.AAPL"
    assert config.confirmation_phrase().startswith("ARM STAGE6 SIM stage6-run-")
    assert config.spec().execution_policy == config.execution.policy


def test_confirmation_binds_account_routing_db_and_provider_mapping(tmp_path):
    base_values = _config_dict(tmp_path)
    base = Stage6PilotConfig.load(_write_values(tmp_path, base_values, "base.json"))
    base_phrase = base.confirmation_phrase()

    mutations = (
        ("symbol", lambda values: values["sleeves"][0]["pair"][0].update({"symbol": "PILOT_A1_CHANGED"})),
        ("external account", lambda values: values["account"].update({"external_account_id": "5077334"})),
        ("security firm", lambda values: values.update({"security_firm": "FUTUINC_TEST"})),
        ("database", lambda values: values.update({"state_db": str(tmp_path / "different.db")})),
        ("Moomoo mapping", lambda values: values["sleeves"][0]["pair"][0].update({"moomoo_symbol": "US.GOOG"})),
    )
    for label, mutate in mutations:
        values = json.loads(json.dumps(base_values))
        mutate(values)
        changed = Stage6PilotConfig.load(_write_values(tmp_path, values, f"{label.replace(' ', '-')}.json"))
        assert changed.confirmation_phrase() != base_phrase, label


def test_repository_bootstrap_persists_and_rejects_divergent_moomoo_mapping(tmp_path):
    config = Stage6PilotConfig.load(_write_config(tmp_path))
    repository = SQLiteTradingRepository(config.state_db)
    config.ensure_repository(repository)

    stored_instrument = repository.get_instrument("pilot-a-1")
    stored_mapping = repository.get_instrument_mapping("stage6-moomoo-pilot-a-1")
    assert stored_instrument is not None
    assert stored_mapping is not None
    assert stored_mapping["external_symbol"] == "US.AAPL"

    values = _config_dict(tmp_path)
    values["sleeves"][0]["pair"][0]["moomoo_symbol"] = "US.GOOG"
    divergent = Stage6PilotConfig.load(_write_values(tmp_path, values, "divergent.json"))
    with pytest.raises(Stage6ConfigError, match="mapping"):
        divergent.ensure_repository(repository)


def test_signed_quantities_require_two_json_numbers(tmp_path):
    values = _config_dict(tmp_path)
    values["targets"][0]["signed_quantities"] = ["12", -2]
    with pytest.raises(Stage6ConfigError, match="JSON number"):
        Stage6PilotConfig.load(_write_values(tmp_path, values, "string-quantity.json"))

    values = _config_dict(tmp_path)
    values["targets"][0]["signed_quantities"] = [1, -2]
    config = Stage6PilotConfig.load(_write_values(tmp_path, values, "numeric-quantity.json"))
    assert config.targets[0].signed_quantities[0] == 1
    assert config.targets[0].signed_quantities[1] == -2


def test_public_stage6_spec_rejects_real_and_non_rth_policy_before_runner(tmp_path):
    config = Stage6PilotConfig.load(_write_config(tmp_path))
    overnight = ExecutionPolicy(execution_session=ExecutionSession.OVERNIGHT)
    with pytest.raises(ValueError, match="regular-session"):
        replace(config.spec(), execution_policy=overnight)

    live_account = replace(config.account, environment=TradingEnvironment.LIVE)
    with pytest.raises(ValueError, match="SIM"):
        replace(config.spec(), account=live_account)


def test_dry_run_prepares_repository_and_never_calls_fake_adapter(tmp_path):
    config = Stage6PilotConfig.load(_write_config(tmp_path))
    repository = SQLiteTradingRepository(config.state_db)
    config.ensure_repository(repository)

    class FakeAdapter:
        def __init__(self):
            self.calls: list[str] = []

        def __getattr__(self, name):
            self.calls.append(name)
            raise AssertionError(f"dry-run called adapter method {name}")

    adapter = FakeAdapter()
    report = config.build_runner(
        repository,
        adapter=adapter,
        clock=lambda: datetime(2026, 9, 20, 12, 0, tzinfo=timezone.utc),
    ).run(config.spec())

    assert report.preflight_passed is True
    assert len(report.intent_plans) == 2
    assert all(item["status"] == "PLANNED_NOT_SUBMITTED" for item in report.intent_results)
    assert adapter.calls == []
    assert repository.get_account("pilot-account") is not None
    assert repository.get_book("pilot-book-a") is not None


def test_cli_validate_and_dry_run_are_offline(tmp_path, capsys):
    path = _write_config(tmp_path)

    assert main(["validate", "--config", str(path), "--json"]) == 0
    validate_output = capsys.readouterr().out
    assert "stage6-run-" in validate_output
    assert "5077333" in validate_output

    assert main(["dry-run", "--config", str(path), "--json"]) == 0
    dry_output = capsys.readouterr().out
    assert "PLANNED_NOT_SUBMITTED" in dry_output
    assert "stage6-run-" in dry_output
    assert "config_preflight" not in dry_output or "true" in dry_output.lower()


def test_sim_command_requires_arm_and_exact_run_confirmation_without_connecting(tmp_path, capsys):
    path = _write_config(tmp_path)
    assert main(["sim-submit", "--config", str(path)]) == 2
    first = capsys.readouterr().out
    assert "requires --arm-sim" in first
    assert not Path(_config_dict(tmp_path)["state_db"]).exists()

    config = Stage6PilotConfig.load(path)
    assert main(["sim-submit", "--config", str(path), "--arm-sim", "--confirm", "WRONG"]) == 2
    second = capsys.readouterr().out
    assert "exact SIM confirmation" in second
    assert config.confirmation_phrase() not in second or "no broker connection" in second
    assert not Path(_config_dict(tmp_path)["state_db"]).exists()


def test_real_account_and_missing_rth_handoff_are_rejected(tmp_path):
    values = _config_dict(tmp_path)
    values["account"]["environment"] = "REAL"
    path = tmp_path / "real.json"
    path.write_text(json.dumps(values), encoding="utf-8")
    with pytest.raises(Stage6ConfigError, match="SIM accounts"):
        Stage6PilotConfig.load(path)

    values = _config_dict(tmp_path)
    del values["execution_policy"]["rth_handoff_policy"]
    path = tmp_path / "no-rth-policy.json"
    path.write_text(json.dumps(values), encoding="utf-8")
    with pytest.raises(Stage6ConfigError, match="rth_handoff_policy"):
        Stage6PilotConfig.load(path)
