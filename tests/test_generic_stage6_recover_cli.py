from datetime import timedelta
import json

import pytest

from scripts.generic_stage6_pilot import _parser, main
import scripts.generic_stage6_pilot as stage6_cli
from src.strategies.stat_arb.stage6_config import Stage6PilotConfig
from src.strategies.stat_arb.stage6_pilot import Stage6PilotRunner, Stage6RunMode
from src.trading_core.oms import GenericOMS
from src.trading_core.repository import SQLiteTradingRepository

from tests.test_generic_stage6_config_cli import _config_dict
from tests.test_generic_stage6_pilot import (
    DelayedPilotAdapter,
    NOW,
    make_account,
    make_repository,
    make_sleeves,
    make_spec,
)


class RecoveryAdapter(DelayedPilotAdapter):
    """Offline adapter that turns seeded working orders into broker fills."""

    def __init__(self) -> None:
        super().__init__(release_after_order_reads=100)
        self.connect_calls = 0
        self.disconnect_calls = 0
        self.cancel_calls = 0
        self.replace_calls = 0
        self.allow_submission = True

    def connect(self):
        self.connect_calls += 1
        return True

    def disconnect(self):
        self.disconnect_calls += 1
        return True

    def submit_order(self, account, request):
        if not self.allow_submission:
            raise AssertionError("recovery must never submit a broker order")
        return super().submit_order(account, request)

    def cancel_order(self, *_args, **_kwargs):
        self.cancel_calls += 1
        raise AssertionError("recovery must never cancel a broker order")

    def replace_order(self, *_args, **_kwargs):
        self.replace_calls += 1
        raise AssertionError("recovery must never replace a broker order")


def _seed_recoverable_state(tmp_path):
    account = make_account()
    sleeves = make_sleeves()
    repository = make_repository(tmp_path, account, sleeves)
    adapter = RecoveryAdapter()
    elapsed = [0.0]

    def clock():
        return NOW + timedelta(seconds=elapsed[0])

    def sleep(seconds: float):
        elapsed[0] += seconds

    runner = Stage6PilotRunner(
        repository,
        GenericOMS(repository, adapter, clock=clock),
        clock=clock,
        sleep=sleep,
        dispatch_wait_seconds=1.0,
        dispatch_poll_seconds=0.5,
    )
    report = runner.run(make_spec(account, sleeves), mode=Stage6RunMode.SIM_SUBMIT)
    assert len(adapter.submit_calls) == 2
    assert [row["book_id"] for row in repository.book_intents(account.id)] == [sleeves[0].book_id]

    # The provider now reports the already-existing attempts as filled.  The
    # subsequent recovery call is the only path allowed to persist that fact.
    adapter._release_pending()
    adapter.allow_submission = False
    return account, sleeves, repository, adapter, runner, report


def test_recover_command_is_exposed_and_parses(tmp_path):
    parsed = _parser().parse_args(["recover", "--config", str(tmp_path / "stage6.json"), "--json"])
    assert parsed.command == "recover"
    assert parsed.as_json is True


def test_recover_rejects_non_sim_before_adapter_creation(tmp_path, capsys):
    values = _config_dict(tmp_path)
    values["account"]["environment"] = "REAL"
    path = tmp_path / "real.json"
    path.write_text(json.dumps(values), encoding="utf-8")

    assert main(["recover", "--config", str(path), "--json"]) == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["valid"] is False
    assert "SIM accounts only" in payload["error"]
    assert not (tmp_path / "stage6-pilot.db").exists()


def test_runner_recover_advances_existing_stale_state_without_order_actions(tmp_path):
    account, sleeves, repository, adapter, runner, report = _seed_recoverable_state(tmp_path)
    assert report.stop_reasons
    before = tuple(len(repository.fills_for_broker_order(order["id"])) for order in repository.book_broker_orders(account.id))

    recovered = runner.recover(account)

    assert len(recovered) == 1
    assert recovered[0]["status"] in {"FILLED", "COMPLETED"}
    assert len(repository.book_intents(account.id, book_id=sleeves[0].book_id)) == 1
    assert repository.book_intents(account.id, book_id=sleeves[1].book_id) == []
    assert repository.open_recovery_actions(account.id) == []
    assert repository.open_reconciliation_issues(account.id) == []
    after = tuple(len(repository.fills_for_broker_order(order["id"])) for order in repository.book_broker_orders(account.id))
    assert after == (1, 1)
    assert before == (0, 0)
    assert len(adapter.submit_calls) == 2
    assert adapter.cancel_calls == 0
    assert adapter.replace_calls == 0
    assert adapter.recovery_calls > 0


def test_recover_is_idempotent_and_does_not_create_attempts_or_duplicate_fills(tmp_path):
    account, _sleeves, repository, adapter, runner, _report = _seed_recoverable_state(tmp_path)

    first = runner.recover(account)
    order_ids = [str(row["id"]) for row in repository.book_broker_orders(account.id)]
    fill_counts = [len(repository.fills_for_broker_order(order_id)) for order_id in order_ids]
    second = runner.recover(account)

    assert first[0]["id"] == second[0]["id"]
    assert [str(row["id"]) for row in repository.book_broker_orders(account.id)] == order_ids
    assert [len(repository.fills_for_broker_order(order_id)) for order_id in order_ids] == fill_counts == [1, 1]
    assert len(adapter.submit_calls) == 2
    assert adapter.cancel_calls == 0
    assert adapter.replace_calls == 0


def test_recover_cli_uses_runner_boundary_and_emits_recovery_evidence(tmp_path, monkeypatch, capsys):
    values = _config_dict(tmp_path)
    path = tmp_path / "stage6.json"
    path.write_text(json.dumps(values), encoding="utf-8")
    config = Stage6PilotConfig.load(path)
    config.ensure_repository(SQLiteTradingRepository(config.state_db))

    class FakeAdapter:
        connect_calls = 0
        disconnect_calls = 0

        def connect(self):
            self.connect_calls += 1

        def disconnect(self):
            self.disconnect_calls += 1

    class FakeRunner:
        def __init__(self):
            self.accounts = []

        def recover(self, account):
            self.accounts.append(account)
            return []

    adapter = FakeAdapter()
    runner = FakeRunner()
    monkeypatch.setattr(Stage6PilotConfig, "build_moomoo_adapter", lambda _self: adapter)
    monkeypatch.setattr(Stage6PilotConfig, "build_runner", lambda _self, _repository, adapter=None: runner)

    assert main(["recover", "--config", str(path), "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert runner.accounts == [config.account]
    assert (adapter.connect_calls, adapter.disconnect_calls) == (1, 1)
    assert payload["mode"] == "RECOVER"
    assert payload["recovery_mode"] == "BROKER_READ_ONLY_LOCAL_LEDGER_WRITE_CAPABLE"
    assert payload["recovery_path"] == "Stage6PilotRunner->GenericOMS->MooMooGenericAdapter->OpenD"
    assert payload["broker_contacted"] is True
    assert payload["broker_submission_count"] == 0
    assert payload["cancel_count"] == 0
    assert payload["replace_count"] == 0
    assert payload["orders_submitted"] == 0
    assert payload["restart_recovery"]["fresh_process"] is False
    assert "no recoverable source intents" in payload["restart_recovery"]["process_identity_reason"]
    assert payload["restart_recovery"]["process_id"]


def test_recover_cli_runs_existing_oms_recovery_and_keeps_book_b_untouched(tmp_path, monkeypatch, capsys):
    values = _config_dict(tmp_path)
    path = tmp_path / "stage6.json"
    path.write_text(json.dumps(values), encoding="utf-8")
    config = Stage6PilotConfig.load(path)
    repository = SQLiteTradingRepository(config.state_db)
    config.ensure_repository(repository)
    adapter = RecoveryAdapter()
    elapsed = [0.0]

    def clock():
        return NOW + timedelta(seconds=elapsed[0])

    def sleep(seconds: float):
        elapsed[0] += seconds

    seed_runner = Stage6PilotRunner(
        repository,
        GenericOMS(repository, adapter, clock=clock),
        clock=clock,
        sleep=sleep,
        dispatch_wait_seconds=1.0,
        dispatch_poll_seconds=0.5,
    )
    seed_runner.run(config.spec(), mode=Stage6RunMode.SIM_SUBMIT)
    adapter._release_pending()
    adapter.allow_submission = False
    monkeypatch.setattr(Stage6PilotConfig, "build_moomoo_adapter", lambda _self: adapter)
    monkeypatch.setattr(stage6_cli.os, "getpid", lambda: "separate-recovery-process")

    assert main(["recover", "--config", str(path), "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["recovered_intent_ids"]
    assert len(payload["recovered_fill_evidence"]) == 2
    assert payload["duplicate_attempt_status"]["new_broker_order_attempt_count"] == 0
    assert payload["duplicate_attempt_status"]["detected"] is False
    assert payload["mutations"]["broker_orders"] == 0
    assert payload["mutations"]["fills"] == 2
    assert payload["restart_recovery"]["result"] == "RECOVERED"
    assert payload["restart_recovery"]["no_duplicate_attempts"] is True
    assert payload["open_recovery_actions"] == []
    assert payload["open_reconciliation_issues"] == []
    assert len(adapter.submit_calls) == 2
    assert adapter.cancel_calls == 0
    assert adapter.replace_calls == 0
    assert repository.book_intents(config.account.id, book_id="pilot-book-b") == []


def test_recover_does_not_record_session_recover_observation_and_session_recover_stays_evidence_only(
    tmp_path, monkeypatch, capsys
):
    values = _config_dict(tmp_path)
    path = tmp_path / "stage6.json"
    path.write_text(json.dumps(values), encoding="utf-8")
    config = Stage6PilotConfig.load(path)
    repository = SQLiteTradingRepository(config.state_db)
    config.ensure_repository(repository)

    class ReadOnlyAdapter:
        def connect(self):
            return True

        def disconnect(self):
            return True

    class EmptyRunner:
        def recover(self, _account):
            return []

    adapter = ReadOnlyAdapter()
    monkeypatch.setattr(Stage6PilotConfig, "build_moomoo_adapter", lambda _self: adapter)
    monkeypatch.setattr(Stage6PilotConfig, "build_runner", lambda _self, _repository, adapter=None: EmptyRunner())
    assert main(["recover", "--config", str(path), "--json"]) == 0
    capsys.readouterr()
    assert repository.stage6_validation_observations(f"stage6-validation-{config.spec().run_id}") == []

    def fail_if_adapter_built(_self):
        raise AssertionError("session-recover must not construct or contact a broker adapter")

    monkeypatch.setattr(Stage6PilotConfig, "build_moomoo_adapter", fail_if_adapter_built)
    assert main(["session-recover", "--config", str(path), "--json"]) == 2
    blocked = json.loads(capsys.readouterr().out)
    assert "recovery evidence must be captured" in blocked["error"]
