from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import copy
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from src.strategies.stat_arb.stage6_pilot import Stage6PilotRunner, Stage6RunMode
from src.trading_core.domain import ExecutionEvidenceMode, PositionSnapshot
from src.trading_core.ports import BrokerFactSnapshot, BrokerHistoricalOrderFacts
from src.trading_core.oms import GenericOMS

from scripts.generic_stage6_pilot import main
from scripts.generic_stage6_pilot import (
    _derived_exit_config,
    _has_duplicate_attempts_per_leg,
    _recovery_report,
    _write_new_json,
)
import scripts.generic_stage6_pilot as stage6_cli
from src.strategies.stat_arb.stage6_config import Stage6PilotConfig
from src.trading_core.repository import SQLiteTradingRepository
from src.trading_core.stage6_validation import Stage6SessionOutcome, evaluate_stage6_session
from tests.test_generic_stage6_config_cli import _config_dict
from tests.test_generic_stage6_pilot import (
    NOW,
    make_repository,
    make_runner,
    make_sleeves,
    make_spec,
    make_targets,
    make_account,
    PilotFakeAdapter,
    DelayedPilotAdapter,
    make_retired_baseline_runner,
    seed_verified_book_exposure,
)


class CliWorkflowAdapter(DelayedPilotAdapter):
    """A broker-neutral fake with provider positions kept netted."""

    def __init__(self, *, release_after_order_reads: int = 1) -> None:
        super().__init__(release_after_order_reads=release_after_order_reads)
        self.connected = 0

    def connect(self) -> None:
        self.connected += 1

    def disconnect(self) -> None:
        self.connected -= 1

    def _net_positions(self) -> None:
        totals: dict[str, Decimal] = {}
        prices: dict[str, Decimal] = {}
        for position in self.facts.positions:
            totals[position.instrument_id] = totals.get(position.instrument_id, Decimal("0")) + position.signed_quantity
            if position.average_price is not None:
                prices[position.instrument_id] = position.average_price
        positions = tuple(
            PositionSnapshot(
                id=f"net-position-{instrument_id}",
                broker_snapshot_id=f"net-snapshot-{instrument_id}",
                account_id=self.facts.account_id,
                instrument_id=instrument_id,
                signed_quantity=quantity,
                average_price=prices.get(instrument_id),
                captured_at=self.facts.captured_at,
            )
            for instrument_id, quantity in sorted(totals.items())
            if quantity != 0
        )
        self.facts = replace(self.facts, positions=positions)

    def get_authoritative_account_facts(self, account):  # type: ignore[no-untyped-def]
        # The real adapter's account-facts endpoint returns one authoritative
        # row per instrument.  Keep this fake's accumulated per-order rows
        # normalized before every fresh safety read as well as before the
        # recovery/order readers below.
        self._net_positions()
        return super().get_authoritative_account_facts(account)

    def get_order(self, account, external_order_id):  # type: ignore[no-untyped-def]
        result = super().get_order(account, external_order_id)
        self._net_positions()
        return result

    def get_open_orders(self, account):  # type: ignore[no-untyped-def]
        result = super().get_open_orders(account)
        self._net_positions()
        return result


class HistoricalCliWorkflowAdapter(CliWorkflowAdapter):
    """The same fake, with bounded history for round-trip proof."""

    def get_historical_order_facts(self, account, requested_start, requested_end):  # type: ignore[no-untyped-def]
        orders = tuple(
            snapshot
            for snapshot in self._filled_orders.values()
            if snapshot.account_id == account.id and snapshot.status.value == "FILLED"
        )
        fills = tuple(
            fill
            for fill in self.facts.fills
            if fill.account_id in (None, account.id)
        )
        return BrokerHistoricalOrderFacts(
            account_id=account.id,
            requested_start=requested_start,
            requested_end=requested_end,
            captured_at=self.facts.captured_at,
            complete=True,
            orders=orders,
            fills=fills,
            execution_evidence_mode=ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS,
            execution_evidence_scope=frozenset({"HISTORICAL_ORDER_SNAPSHOTS"}),
        )


def _install_cli_fake(
    monkeypatch: pytest.MonkeyPatch,
    adapter: CliWorkflowAdapter,
    *,
    now: datetime | None = None,
) -> None:
    original_build_runner = Stage6PilotConfig.build_runner
    base_now = now or adapter.facts.captured_at
    virtual_now = [base_now]

    def build_runner(self, repository, *, adapter=None, clock=None):  # type: ignore[no-untyped-def]
        if adapter is None:
            return original_build_runner(self, repository, adapter=None, clock=lambda: base_now)
        def sleep(seconds: float) -> None:
            virtual_now[0] += timedelta(seconds=seconds)

        return Stage6PilotRunner(
            repository,
            GenericOMS(repository, adapter, clock=lambda: virtual_now[0]),
            clock=lambda: virtual_now[0],
            sleep=sleep,
            dispatch_wait_seconds=2.0,
            dispatch_poll_seconds=0.1,
        )

    monkeypatch.setattr(Stage6PilotConfig, "build_moomoo_adapter", lambda self: adapter)
    monkeypatch.setattr(Stage6PilotConfig, "build_runner", build_runner)


def _fresh_cli_values(tmp_path, monkeypatch: pytest.MonkeyPatch) -> tuple[dict, datetime]:
    """Keep fake provider timestamps inside the real recovery window."""

    import tests.test_generic_stage6_pilot as pilot_fixtures

    runtime_now = datetime.now(timezone.utc).replace(microsecond=0)
    monkeypatch.setattr(pilot_fixtures, "NOW", runtime_now)
    values = _config_dict(tmp_path)
    old_timestamp = "2026-09-20T12:00:00+00:00"
    new_timestamp = runtime_now.isoformat()

    def replace_timestamp(value):  # type: ignore[no-untyped-def]
        if isinstance(value, dict):
            return {key: replace_timestamp(item) for key, item in value.items()}
        if isinstance(value, list):
            return [replace_timestamp(item) for item in value]
        return new_timestamp if value == old_timestamp else value

    return replace_timestamp(values), runtime_now


def _write_cli_output(path: Path, output: str) -> dict:
    path.write_text(output, encoding="utf-8")
    return json.loads(output)


def _run_cli(capsys, args: list[str], artifact: Path | None = None) -> tuple[int, dict]:  # type: ignore[no-untyped-def]
    code = main(args)
    output = capsys.readouterr().out
    payload = json.loads(output)
    if artifact is not None:
        _write_cli_output(artifact, output)
    return code, payload


def test_sim_submit_rechecks_rth_and_submits_nothing_when_session_changes(tmp_path):
    account, sleeves, _repository, adapter, runner = make_runner(tmp_path)

    def closed_market(symbols):
        return {
            "market": "US",
            "captured_at": NOW.isoformat(),
            "complete": True,
            "rows": [{"symbol": symbol, "market_state": "CLOSED"} for symbol in symbols],
        }

    adapter.get_authoritative_market_state = closed_market
    report = runner.run(make_spec(account, sleeves), mode=Stage6RunMode.SIM_SUBMIT)

    assert report.preflight_passed is False
    assert any("not RTH" in reason for reason in report.stop_reasons)
    assert adapter.submit_calls == []


def test_sim_submit_rechecks_rth_before_each_leg_and_stops_on_transition(tmp_path):
    account, sleeves, repository, adapter, runner = make_runner(tmp_path)
    calls = 0

    def changing_market(symbols):
        nonlocal calls
        calls += 1
        state = "RTH" if calls < 3 else "CLOSED"
        return {
            "market": "US",
            "captured_at": NOW.isoformat(),
            "complete": True,
            "rows": [{"symbol": str(symbol), "market_state": state} for symbol in symbols],
        }

    adapter.get_authoritative_market_state = changing_market
    report = runner.run(make_spec(account, sleeves), mode=Stage6RunMode.SIM_SUBMIT)

    # Call 1 is the run-level gate, call 2 is the first leg gate, and call 3
    # closes RTH before the second leg can reach the adapter.  The first leg
    # remains durable/auditable; the blocked second leg is never submitted.
    assert calls >= 3
    assert report.preflight_passed is False
    assert any("returned RECONCILIATION_REQUIRED" in reason for reason in report.stop_reasons)
    assert any(
        "not RTH" in str(issue.get("details_json"))
        for issue in repository.open_reconciliation_issues(account.id)
    )
    assert len(adapter.submit_calls) == 1
    assert len(repository.book_intents(account.id)) == 1


@pytest.mark.parametrize("condition", ("closed", "stale", "incomplete"))
def test_compensating_exit_requires_fresh_rth_before_oms_boundary(tmp_path, condition):
    account, sleeves, _repository, adapter, runner = make_runner(tmp_path)

    def market_state(symbols):
        captured_at = NOW if condition != "stale" else NOW - timedelta(seconds=61)
        return {
            "market": "US",
            "captured_at": captured_at.isoformat(),
            "complete": condition != "incomplete",
            "rows": [
                {
                    "symbol": str(symbol),
                    "market_state": "CLOSED" if condition == "closed" else "RTH",
                }
                for symbol in symbols
            ],
        }

    adapter.get_authoritative_market_state = market_state
    gate = runner.compensating_exit_preflight(make_spec(account, sleeves))

    assert gate["preflight_passed"] is False
    assert gate["broker_submission_count"] == 0
    assert gate["mutations"]["submission_calls"] == 0
    assert adapter.submit_calls == []


def test_compensating_exit_fresh_rth_gate_passes_without_mutation(tmp_path):
    account, sleeves, _repository, adapter, runner = make_runner(tmp_path)

    gate = runner.compensating_exit_preflight(make_spec(account, sleeves))

    assert gate["preflight_passed"] is True
    assert gate["market_state"]["rth"]["observed"] is True
    assert adapter.submit_calls == []


def test_strict_position_normalizer_rejects_conflicting_duplicates_at_shared_boundaries(tmp_path):
    account = make_account()
    sleeves = make_sleeves()
    instrument_id = sleeves[0].instrument_ids[0]
    first = PositionSnapshot(
        id="position-a",
        broker_snapshot_id="snapshot-a",
        account_id=account.id,
        instrument_id=instrument_id,
        signed_quantity=Decimal("1"),
        captured_at=NOW,
    )
    conflicting = replace(first, id="position-b", broker_snapshot_id="snapshot-b", signed_quantity=Decimal("2"))
    facts = BrokerFactSnapshot(
        account_id=account.id,
        captured_at=NOW,
        complete=True,
        positions=(first, conflicting),
    )
    account, sleeves, _repository, adapter, runner = make_runner(tmp_path, facts=facts)
    spec = make_spec(account, sleeves)

    preflight = runner.broker_preflight(spec)
    assert preflight["preflight_passed"] is False
    assert any("contradictory duplicate rows" in reason for reason in preflight["stop_reasons"])

    report = runner.run(spec, mode=Stage6RunMode.SIM_SUBMIT)
    assert report.preflight_passed is False
    assert any("contradictory duplicate rows" in reason for reason in report.stop_reasons)
    assert adapter.submit_calls == []


def test_final_state_is_read_only_and_reports_local_blockers(tmp_path):
    account, sleeves, repository, adapter, runner = make_runner(tmp_path)
    result = runner.final_state(make_spec(account, sleeves))

    assert result["mode"] == "FINAL_STATE"
    assert result["broker_contacted"] is True
    assert result["orders_submitted"] == 0
    assert result["mutations"]["order_intents"] == 0
    assert set(result["final"]["book_exposure"]) == {sleeves[0].book_id, sleeves[1].book_id}
    assert repository.book_intents(account.id) == []
    assert adapter.submit_calls == []


def test_prepare_exit_derives_current_exposure_without_pre_negation(tmp_path):
    account, sleeves, repository, adapter, runner = make_runner(tmp_path)
    from tests.test_generic_stage6_pilot import seed_verified_book_exposure

    seed_verified_book_exposure(repository, account, sleeves, quantities=("1", "1"))
    positions = []
    for sleeve in sleeves:
        positions.extend(
            PositionSnapshot(
                id=f"position-{instrument_id}",
                broker_snapshot_id="snapshot-exit",
                account_id=account.id,
                instrument_id=instrument_id,
                signed_quantity=quantity,
                captured_at=NOW,
            )
            for instrument_id, quantity in zip(
                sleeve.instrument_ids,
                (Decimal("1"), Decimal("-1")),
                strict=True,
            )
        )
    adapter.facts = replace(adapter.facts, positions=tuple(positions))

    result = runner.prepare_exit(make_spec(account, sleeves))

    assert result["prepare_exit_passed"] is True
    assert result["pre_negation_applied"] is False
    assert result["creates_order_intent"] is False
    assert result["derived_signed_quantities"][sleeves[0].instrument_ids[0]] == "1"
    assert result["books"][0]["closing_legs"][0]["side"] == "SELL"
    assert result["books"][0]["closing_legs"][1]["side"] == "BUY"
    assert len(repository.book_intents(account.id)) == 2
    assert adapter.submit_calls == []


def test_session_final_records_only_supplied_final_artifact(tmp_path, capsys):
    values = _config_dict(tmp_path)
    config_path = tmp_path / "stage6.json"
    config_path.write_text(json.dumps(values), encoding="utf-8")
    final_path = tmp_path / "final.json"
    final_path.write_text(
        json.dumps(
            {
                "session_id": "final-session",
                "account_id": "pilot-account",
                "environment": "SIM",
                "final": {"captured_at": NOW.isoformat(), "flat": False},
            }
        ),
        encoding="utf-8",
    )

    assert main(
        [
            "session-final",
            "--config",
            str(config_path),
            "--evidence",
            str(final_path),
            "--session-id",
            "final-session",
            "--json",
        ]
    ) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["phase"] == "FINAL"
    assert output["broker_contacted"] is False
    assert output["evidence"]["final"]["flat"] is False


def test_session_evidence_builder_fails_closed_without_captured_artifacts(tmp_path):
    account = make_account()
    sleeves = make_sleeves()
    repository = make_repository(tmp_path, account, sleeves)
    evidence = __import__(
        "src.strategies.stat_arb.stage6_pilot",
        fromlist=["Stage6PilotRunner"],
    ).Stage6PilotRunner(
        repository,
        GenericOMS(repository, PilotFakeAdapter(), clock=lambda: NOW),
        clock=lambda: NOW,
    ).build_session_evidence(
        make_spec(account, sleeves),
        session_id="builder-session",
        entry_intent_ids=("entry-a", "entry-b"),
        exit_intent_ids=("exit-a", "exit-b"),
        commit_sha="abc123",
    )

    assert evidence["evidence_builder_missing"]
    assert "preflight" not in evidence
    assert "restart_recovery" not in evidence
    assert "final" not in evidence


def test_prepare_exit_artifact_is_a_directly_loadable_exit_config(tmp_path):
    values = _config_dict(tmp_path)
    source_path = tmp_path / "entry.json"
    source_path.write_text(json.dumps(values), encoding="utf-8")
    config = Stage6PilotConfig.load(source_path)
    result = {
        "run_id": "stage6-run-verified-exit",
        "captured_at": NOW.isoformat(),
        "derived_signed_quantities": {
            "pilot-a-1": "1",
            "pilot-a-2": "-1",
            "pilot-b-1": "2",
            "pilot-b-2": "-2",
        },
    }

    artifact = _derived_exit_config(config, result)
    output_path = tmp_path / "derived-exit.json"
    assert _write_new_json(output_path, artifact) == str(output_path)
    loaded = Stage6PilotConfig.load(output_path)

    assert loaded.spec().action.value == "EXIT"
    assert loaded.spec().targets[0].signed_quantities == (Decimal("1"), Decimal("-1"))
    assert loaded.spec().targets[1].signed_quantities == (Decimal("2"), Decimal("-2"))
    assert "-exit-" in loaded.spec().targets[0].cycle_id
    assert loaded.spec().targets[0].provenance["pre_negation_applied"] is False


def test_compensating_duplicate_attempt_flag_is_per_leg():
    normal_pair = [
        {
            "intent_id": "exit-intent",
            "legs": [
                {"id": "exit-leg-a", "attempts": [{"id": "attempt-a", "external_order_id": "external-a"}]},
                {"id": "exit-leg-b", "attempts": [{"id": "attempt-b", "external_order_id": "external-b"}]},
            ],
        }
    ]
    repeated_attempt = [
        {
            "intent_id": "exit-intent",
            "legs": [
                {"id": "exit-leg-a", "attempts": [{"id": "attempt-a", "external_order_id": "external-a"}]},
                {
                    "id": "exit-leg-b",
                    "attempts": [
                        {"id": "attempt-b-1", "external_order_id": "external-b-1"},
                        {"id": "attempt-b-2", "external_order_id": "external-b-2"},
                    ],
                },
            ],
        }
    ]

    assert _has_duplicate_attempts_per_leg(normal_pair) is False
    assert _has_duplicate_attempts_per_leg(repeated_attempt) is True


@pytest.mark.parametrize("variant", ("mismatch", "unknown", "net_zero", "missing"))
def test_recovery_exposure_proof_exposes_maps_and_blocks_ambiguous_facts(tmp_path, variant):
    account, sleeves, repository, adapter, _runner = make_runner(tmp_path)
    seed_verified_book_exposure(repository, account, sleeves, quantities=("1", "1"))
    values = _config_dict(tmp_path)
    config_path = tmp_path / f"recovery-{variant}.json"
    config_path.write_text(json.dumps(values), encoding="utf-8")
    config = Stage6PilotConfig.load(config_path)
    durable = {
        "pilot-a-1": Decimal("1"),
        "pilot-a-2": Decimal("-1"),
        "pilot-b-1": Decimal("1"),
        "pilot-b-2": Decimal("-1"),
    }
    if variant == "missing":
        facts = None
    else:
        positions = [
            PositionSnapshot(
                id=f"recovery-position-{instrument_id}",
                broker_snapshot_id="recovery-snapshot",
                account_id=account.id,
                instrument_id=instrument_id,
                signed_quantity=quantity,
                captured_at=NOW,
            )
            for instrument_id, quantity in durable.items()
        ]
        if variant == "mismatch":
            positions = positions[:-1]
        elif variant == "unknown":
            positions.append(
                PositionSnapshot(
                    id="recovery-position-unknown",
                    broker_snapshot_id="recovery-snapshot",
                    account_id=account.id,
                    instrument_id="pilot-unknown",
                    signed_quantity=Decimal("1"),
                    captured_at=NOW,
                )
            )
        elif variant == "net_zero":
            positions.append(
                PositionSnapshot(
                    id="recovery-position-net-zero",
                    broker_snapshot_id="recovery-snapshot",
                    account_id=account.id,
                    instrument_id="pilot-a-1",
                    signed_quantity=Decimal("-1"),
                    captured_at=NOW,
                )
            )
        facts = replace(adapter.facts, captured_at=datetime.now(timezone.utc), positions=tuple(positions))

    report = _recovery_report(
        config=config,
        repository=repository,
        started_at=NOW,
        before_intent_ids=[],
        before_status=[],
        before_order_ids=set(),
        before_external_order_ids=set(),
        before_fill_keys=set(),
        recovered=[],
        fresh_facts=facts,
    )
    restart = report["restart_recovery"]
    assert restart["durable_managed_exposure"] == {
        key: str(value) for key, value in sorted(durable.items())
    }
    assert restart["exposure_agrees"] is False
    assert restart["fresh_facts_complete"] is (variant not in {"missing", "net_zero"})


def test_recovery_exposure_proof_accepts_exact_fresh_maps(tmp_path):
    account, sleeves, repository, adapter, _runner = make_runner(tmp_path)
    seed_verified_book_exposure(repository, account, sleeves, quantities=("1", "1"))
    values = _config_dict(tmp_path)
    config_path = tmp_path / "recovery-match.json"
    config_path.write_text(json.dumps(values), encoding="utf-8")
    config = Stage6PilotConfig.load(config_path)
    positions = tuple(
        PositionSnapshot(
            id=f"recovery-position-{instrument_id}",
            broker_snapshot_id="recovery-snapshot",
            account_id=account.id,
            instrument_id=instrument_id,
            signed_quantity=quantity,
            captured_at=NOW,
        )
        for instrument_id, quantity in (
            ("pilot-a-1", Decimal("1")),
            ("pilot-a-2", Decimal("-1")),
            ("pilot-b-1", Decimal("1")),
            ("pilot-b-2", Decimal("-1")),
        )
    )
    report = _recovery_report(
        config=config,
        repository=repository,
        started_at=NOW,
        before_intent_ids=[],
        before_status=[],
        before_order_ids=set(),
        before_external_order_ids=set(),
        before_fill_keys=set(),
        recovered=[],
        fresh_facts=replace(adapter.facts, captured_at=datetime.now(timezone.utc), positions=positions),
    )
    assert report["restart_recovery"]["exposure_agrees"] is True
    assert report["restart_recovery"]["broker_observed_exposure"] == report["restart_recovery"]["durable_managed_exposure"]


def test_clean_preflight_dryrun_finalstate_includes_verified_retired_baseline(tmp_path):
    account, sleeves, _repository, _adapter, runner = make_retired_baseline_runner(tmp_path)
    spec = make_spec(account, sleeves, targets=make_targets(sleeves, quantities=("1", "1")))

    preflight = runner.broker_preflight(
        spec,
        market_symbols=tuple(symbol for sleeve in sleeves for symbol in sleeve.symbols),
    )
    assert preflight["preflight_passed"] is True, preflight["stop_reasons"]
    dry_run = runner.run(spec, mode=Stage6RunMode.DRY_RUN)
    assert dry_run.preflight_passed is True
    final = runner.final_state(spec)
    assert final["final_state_passed"] is True
    assert final["final"]["all_orders_attributable"] is True


def _retired_baseline_cli_values(values: dict, tmp_path: Path) -> dict:
    """Adapt the checked test config to the real imported baseline identity."""

    account_id = "moomoo:sim:5077333"
    instrument_ids = (
        ("generic-sim-smoke:us-aapl", "US.AAPL"),
        ("generic-sim-smoke:us-msft", "US.MSFT"),
    )
    values = json.loads(json.dumps(values))
    values["state_db"] = str(tmp_path / "retired-stage6.db")
    values["account"]["id"] = account_id
    values["account"]["metadata"] = {
        "allocation_capacity": "20",
        "account_capacity": "20",
    }
    values["strategy"]["name"] = "Stage 6 test strategy"
    values["strategy"]["config"] = {}
    for sleeve in values["sleeves"]:
        sleeve["account_id"] = account_id
    for pair, (instrument_id, symbol) in zip(values["sleeves"][0]["pair"], instrument_ids, strict=True):
        pair["instrument_id"] = instrument_id
        pair["symbol"] = symbol.split(".", 1)[1]
        pair["moomoo_symbol"] = symbol
    values["allocation_update"]["account_id"] = account_id
    values["targets"][0]["instrument_ids"] = [item[0] for item in instrument_ids]
    return values


@pytest.mark.parametrize("include_retired_baseline", (False, True), ids=("clean", "retired-baseline"))
def test_cli_clean_fake_broker_workflow_builds_and_finalizes_durable_evidence(
    tmp_path,
    monkeypatch,
    capsys,
    include_retired_baseline,
):
    values, runtime_now = _fresh_cli_values(tmp_path, monkeypatch)
    runtime_trading_date = runtime_now.astimezone(ZoneInfo("America/New_York")).date().isoformat()
    if include_retired_baseline:
        _baseline_account, _baseline_sleeves, _baseline_repository, baseline_adapter, _baseline_runner = (
            make_retired_baseline_runner(tmp_path)
        )
        values = _retired_baseline_cli_values(values, tmp_path)
        baseline_adapter.facts = replace(baseline_adapter.facts, captured_at=runtime_now)
        adapter = CliWorkflowAdapter()
        adapter.facts = baseline_adapter.facts
    else:
        adapter = CliWorkflowAdapter()
    config_path = tmp_path / "entry.json"
    config_path.write_text(json.dumps(values), encoding="utf-8")
    config = Stage6PilotConfig.load(config_path)
    _install_cli_fake(monkeypatch, adapter, now=runtime_now)

    # Bootstrap the exact repository/config boundary through the supported
    # CLI. Every later evidence artifact is captured from a CLI result.
    dry_code, dry_payload = _run_cli(capsys, ["dry-run", "--config", str(config_path), "--json"])
    assert dry_code == 0, json.dumps(dry_payload, indent=2, sort_keys=True)
    session_id = "clean-cli-session"
    preflight_path = tmp_path / "preflight.json"
    preflight_code, preflight = _run_cli(
        capsys,
        [
            "broker-preflight", "--config", str(config_path), "--json",
            "--session-id", session_id, "--record-observation",
        ],
        preflight_path,
    )
    assert preflight_code == 0
    assert preflight["preflight_passed"] is True
    assert preflight["orders_submitted"] == 0

    entry_code, entry = _run_cli(
        capsys,
        [
            "sim-submit", "--config", str(config_path), "--arm-sim", "--confirm",
            config.confirmation_phrase(), "--json",
        ],
    )
    assert entry_code == 0
    assert entry["preflight_passed"] is True
    assert entry["broker_contacted"] is True
    assert len(adapter.submit_calls) == 4
    entry_intent_ids = [item["intent_id"] for item in entry["intent_results"]]
    assert len(entry_intent_ids) == 2

    recovery_path = tmp_path / "recovery.json"
    original_getpid = stage6_cli.os.getpid
    monkeypatch.setattr(stage6_cli.os, "getpid", lambda: "separate-recovery-process")
    recovery_code, recovery = _run_cli(
        capsys,
        [
            "recover", "--config", str(config_path), "--json",
            "--session-id", session_id, "--record-observation",
        ],
        recovery_path,
    )
    monkeypatch.setattr(stage6_cli.os, "getpid", original_getpid)
    assert recovery_code == 0
    assert recovery["restart_recovery"]["fresh_process"] is True
    assert recovery["restart_recovery"]["no_resubmission"] is True
    assert recovery["restart_recovery"]["exposure_agrees"] is True, json.dumps(
        {
            "restart": recovery["restart_recovery"],
            "before": recovery["before_status"],
            "after": recovery["after_status"],
            "issues": recovery["open_reconciliation_issues"],
            "actions": recovery["open_recovery_actions"],
        },
        sort_keys=True,
    )
    assert recovery["broker_submission_count"] == 0

    exit_path = tmp_path / "exit.json"
    prepare_code, prepared = _run_cli(
        capsys,
        ["prepare-exit", "--config", str(config_path), "--output", str(exit_path), "--json"],
    )
    assert prepare_code == 0
    assert prepared["prepare_exit_passed"] is True
    exit_config = Stage6PilotConfig.load(exit_path)
    assert exit_config.spec().action.value == "EXIT"
    assert exit_config.targets[0].signed_quantities == (Decimal("1"), Decimal("-1"))

    exit_code, exit_report = _run_cli(
        capsys,
        [
            "sim-submit", "--config", str(exit_path), "--arm-sim", "--confirm",
            exit_config.confirmation_phrase(), "--json",
        ],
    )
    assert exit_code == 0
    assert exit_report["broker_contacted"] is True
    assert len(adapter.submit_calls) == 8
    exit_intent_ids = [item["intent_id"] for item in exit_report["intent_results"]]
    assert len(exit_intent_ids) == 2

    final_path = tmp_path / "final.json"
    final_code, final = _run_cli(
        capsys,
        [
            "final-state", "--config", str(config_path), "--json",
            "--session-id", session_id, "--record-observation",
        ],
        final_path,
    )
    assert final_code == 0, json.dumps(final, indent=2, sort_keys=True)
    assert final["final_state_passed"] is True
    assert final["final"]["flat"] is True
    assert final["final"]["no_open_orders"] is True

    evidence_path = tmp_path / "session-evidence.json"
    evidence_code, evidence = _run_cli(
        capsys,
        [
            "session-evidence", "--config", str(exit_path), "--session-id", "clean-cli-session",
            "--entry-intent-id", entry_intent_ids[0], "--entry-intent-id", entry_intent_ids[1],
            "--exit-intent-id", exit_intent_ids[0], "--exit-intent-id", exit_intent_ids[1],
            "--preflight-evidence", str(preflight_path), "--recovery-evidence", str(recovery_path),
            "--final-evidence", str(final_path), "--commit-sha", "offline-test",
                "--trading-date", runtime_trading_date, "--json",
        ],
        evidence_path,
    )
    assert evidence_code == 0
    assert "evidence_builder_missing" not in evidence
    assert evidence["entry"]["fills_complete"] is True
    assert evidence["exit"]["fills_complete"] is True
    assert evidence["exit"]["current_exposure_inverse"] is True

    # The phase rows above were persisted by the connected runner commands;
    # session-* artifact ingestion remains audit-only and cannot qualify.

    def record_phase_observations(session_id: str) -> None:
        for command, artifact in (
            ("session-preflight", preflight_path),
            ("session-recover", recovery_path),
            ("session-final", final_path),
        ):
            phase_artifact = tmp_path / f"{session_id}-{command}.json"
            phase_value = json.loads(artifact.read_text(encoding="utf-8"))
            phase_value["session_id"] = session_id
            phase_artifact.write_text(json.dumps(phase_value), encoding="utf-8")
            args = [
                command, "--config", str(config_path), "--session-id", session_id,
                "--evidence", str(phase_artifact), "--commit-sha", "offline-test",
                "--execution-compatibility", "stage6-execution-v2", "--trading-date", runtime_trading_date,
            ]
            if command == "session-recover":
                args.append("--fresh-process")
            args.append("--json")
            assert _run_cli(capsys, args)[0] == 0

    # These documents remain structurally acceptable to the pure evaluator,
    # but each mutation conflicts with the canonical durable graph.  The CLI
    # finalization boundary must reject every one before persistence.
    tamper_cases = {
        "external-order": lambda value: value["entry"]["actual_orders"][0].update(
            {"order_id": "forged-external-order", "external_order_id": "forged-external-order"}
        ),
        "fill-dedupe": lambda value: value["entry"]["actual_orders"][0]["fills"][0].update(
            {"dedupe_key": "forged-dedupe-key"}
        ),
        "fill-price": lambda value: value["entry"]["actual_orders"][0]["fills"][0].update(
            {"price": "999.99"}
        ),
        "provenance": lambda value: value["execution_provenance"]["entry"][entry_intent_ids[0]].update(
            {"process_id": "forged-submission-process"}
        ),
    }
    durable_gate_rejections = 0
    for label, mutate in tamper_cases.items():
        tampered = copy.deepcopy(evidence)
        tampered["session_id"] = f"tampered-{label}"
        mutate(tampered)
        pure_result = evaluate_stage6_session(tampered)
        # The pure document validator checks the top-level lifecycle shape;
        # nested fill/provenance tampering is intentionally rejected later by
        # the repository-backed durable graph gate.  Only the forged order
        # identity is structurally invalid at this pure boundary.
        expected_pure = (
            Stage6SessionOutcome.INVALID
            if label == "external-order"
            else Stage6SessionOutcome.CLEAN_PASS
        )
        assert pure_result.outcome is expected_pure
        tampered_path = tmp_path / f"tampered-{label}.json"
        tampered_path.write_text(json.dumps(tampered), encoding="utf-8")
        record_phase_observations(tampered["session_id"])
        tampered_code, tampered_result = _run_cli(
            capsys,
            [
                "session-finalize", "--config", str(config_path),
                "--session-id", tampered["session_id"], "--evidence", str(tampered_path), "--json",
            ],
        )
        assert tampered_code == 2
        if tampered_result.get("status") == "BLOCKED":
            durable_gate_rejections += 1
            assert "durable repository verification" in tampered_result["error"]
        else:
            assert tampered_result["result"] in {"FAILED", "INVALID"}
            assert tampered_result["counted_for_completion"] is False
    assert durable_gate_rejections >= 1

    # Observations captured under another session cannot be borrowed by a
    # candidate CLEAN_PASS document, even when all durable execution rows are
    # otherwise valid.
    observation_session = "observation-session-only"
    record_phase_observations(observation_session)
    wrong_session = copy.deepcopy(evidence)
    wrong_session["session_id"] = "candidate-session-without-observations"
    wrong_session_path = tmp_path / "wrong-session.json"
    wrong_session_path.write_text(json.dumps(wrong_session), encoding="utf-8")
    wrong_code, wrong_result = _run_cli(
        capsys,
        [
            "session-finalize", "--config", str(config_path),
            "--session-id", wrong_session["session_id"], "--evidence", str(wrong_session_path), "--json",
        ],
    )
    assert wrong_code == 2
    assert wrong_result["status"] == "BLOCKED"
    assert "exactly one PREFLIGHT observation" in wrong_result["error"]

    finalize_code, finalized = _run_cli(
        capsys,
        ["session-finalize", "--config", str(config_path), "--session-id", session_id, "--evidence", str(evidence_path), "--json"],
    )
    assert finalize_code == 0, json.dumps(finalized, indent=2, sort_keys=True)
    assert finalized["result"] == "CLEAN_PASS"


def test_same_process_recovery_cannot_count_as_fresh_process(
    tmp_path,
    monkeypatch,
    capsys,
):
    values, runtime_now = _fresh_cli_values(tmp_path, monkeypatch)
    config_path = tmp_path / "entry.json"
    config_path.write_text(json.dumps(values), encoding="utf-8")
    config = Stage6PilotConfig.load(config_path)
    adapter = CliWorkflowAdapter()
    _install_cli_fake(monkeypatch, adapter, now=runtime_now)

    assert _run_cli(capsys, ["dry-run", "--config", str(config_path), "--json"])[0] == 0
    submit_code, _submit = _run_cli(
        capsys,
        [
            "sim-submit", "--config", str(config_path), "--arm-sim", "--confirm",
            config.confirmation_phrase(), "--json",
        ],
    )
    assert submit_code == 0

    recovery_path = tmp_path / "same-process-recovery.json"
    recovery_code, recovery = _run_cli(
        capsys,
        ["recover", "--config", str(config_path), "--json"],
        recovery_path,
    )
    assert recovery_code == 2
    assert recovery["restart_recovery"]["fresh_process"] is False
    assert recovery["restart_recovery"]["result"] == "BLOCKED"
    assert "matches the original" in recovery["restart_recovery"]["process_identity_reason"]

    record_code, record = _run_cli(
        capsys,
        [
            "session-recover", "--config", str(config_path), "--session-id", "same-process-session",
            "--evidence", str(recovery_path), "--commit-sha", "offline-test",
            "--execution-compatibility", "stage6-execution-v2", "--trading-date", "2026-10-07",
            "--fresh-process", "--json",
        ],
    )
    assert record_code == 2
    assert "distinct from every source submission" in record["error"]


def test_cli_delayed_first_book_never_dispatches_second_and_cannot_finalize(
    tmp_path,
    monkeypatch,
    capsys,
):
    values, runtime_now = _fresh_cli_values(tmp_path, monkeypatch)
    config_path = tmp_path / "entry.json"
    config_path.write_text(json.dumps(values), encoding="utf-8")
    config = Stage6PilotConfig.load(config_path)
    adapter = HistoricalCliWorkflowAdapter()
    adapter.release_after_order_reads = 100
    _install_cli_fake(monkeypatch, adapter, now=runtime_now)

    assert _run_cli(capsys, ["dry-run", "--config", str(config_path), "--json"])[0] == 0
    preflight_path = tmp_path / "preflight.json"
    preflight_code, preflight = _run_cli(
        capsys,
        ["broker-preflight", "--config", str(config_path), "--json"],
        preflight_path,
    )
    assert preflight_code == 0
    assert preflight["preflight_passed"] is True
    assert preflight["orders_submitted"] == 0
    submit_code, report = _run_cli(
        capsys,
        [
            "sim-submit", "--config", str(config_path), "--arm-sim", "--confirm",
            config.confirmation_phrase(), "--json",
        ],
    )
    assert submit_code == 2
    assert len(adapter.submit_calls) == 2
    assert report["intent_results"][0]["dispatch_outcome"] == "TIMEOUT"
    assert report["intent_results"][0]["sleeve_id"] == "pilot-sleeve-a"
    assert report["intent_results"][0]["submitted"] is True
    assert report["intent_results"][0]["sleeve_id"] != "pilot-sleeve-b"
    assert report["stop_reasons"]

    repository = SQLiteTradingRepository(config.state_db)
    snapshots = repository.book_intents(config.account.id)
    assert len(snapshots) == 1
    assert snapshots[0]["book_id"] == "pilot-book-a"
    source_intent_id = str(snapshots[0]["id"])
    assert len(adapter.submit_calls) == 2
    assert repository.book_intents(config.account.id, book_id="pilot-book-b") == []

    # A separate recovery process releases the delayed Book A orders through
    # the normal broker-read-only CLI path.  The source intent then has exact
    # durable fills, while Book B still has no persisted intent.
    original_getpid = stage6_cli.os.getpid
    adapter.release_after_order_reads = 1
    monkeypatch.setattr(stage6_cli.os, "getpid", lambda: "delayed-entry-recovery-process")
    recovery_path = tmp_path / "delayed-entry-recovery.json"
    recovery_code, recovery = _run_cli(
        capsys,
        [
            "recover", "--config", str(config_path), "--json",
        ],
        recovery_path,
    )
    monkeypatch.setattr(stage6_cli.os, "getpid", original_getpid)
    assert recovery_code == 0
    assert recovery["restart_recovery"]["fresh_process"] is True
    assert recovery["restart_recovery"]["result"] == "RECOVERED"
    recovered_source = repository.get_intent(source_intent_id)
    assert recovered_source is not None
    assert recovered_source["status"] == "FILLED"
    assert all(leg["status"] == "FILLED" for leg in recovered_source["legs"])
    assert len(adapter.submit_calls) == 2
    assert all(len(repository.broker_orders_for_leg(leg["id"])) == 1 for leg in recovered_source["legs"])
    assert repository.book_intents(config.account.id, book_id="pilot-book-b") == []

    # Use the exact deterministic arm phrase.  This is the existing proof-
    # gated compensating-exit CLI, not a direct OMS helper or hand-built exit.
    compensating_code, compensating = _run_cli(
        capsys,
        [
            "compensating-exit", "--config", str(config_path),
            "--source-intent-id", source_intent_id, "--arm-sim",
            "--confirm", f"ARM STAGE6 SIM COMPENSATING EXIT {source_intent_id}", "--json",
        ],
    )
    assert compensating_code == 0, json.dumps(compensating, indent=2, sort_keys=True)
    assert compensating["broker_contacted"] is True
    exit_intent_id = compensating["compensating_exit_intent_id"]
    assert exit_intent_id
    assert compensating["broker_submission_count"] == 2
    assert compensating["duplicate_attempt"] is False
    assert len(adapter.submit_calls) == 4
    exit_snapshot = repository.get_intent(exit_intent_id)
    assert exit_snapshot is not None
    assert exit_snapshot["status"] == "WORKING"
    assert exit_snapshot["metadata"]["stage6_submission"]["mode"] == "COMPENSATING_EXIT"
    assert exit_snapshot["metadata"]["stage6_submission"]["correlation"]["source_intent_id"] == source_intent_id
    assert all(len(repository.broker_orders_for_leg(leg["id"])) == 1 for leg in exit_snapshot["legs"])
    assert repository.book_intents(config.account.id, book_id="pilot-book-b") == []

    # Before the delayed compensating orders are recovered, final-state is a
    # truthful failed observation: open orders and unfinished lifecycle remain.
    pre_resolve_final_code, pre_resolve_final = _run_cli(
        capsys,
        ["final-state", "--config", str(config_path), "--json"],
    )
    assert pre_resolve_final_code == 2
    assert pre_resolve_final["final_state_passed"] is False
    assert pre_resolve_final["final"]["unfinished_intents"] > 0

    # A second read-only recovery releases the delayed compensating exit.  The
    # recovery report may not claim a fresh process for the OMS-created exit
    # intent (it has no Stage6 submission marker), but it must still recover
    # the durable order without submitting a duplicate.
    adapter.release_after_order_reads = 1
    monkeypatch.setattr(stage6_cli.os, "getpid", lambda: "delayed-exit-recovery-process")
    exit_recovery_code, exit_recovery = _run_cli(
        capsys,
        ["recover", "--config", str(config_path), "--json"],
    )
    monkeypatch.setattr(stage6_cli.os, "getpid", original_getpid)
    assert exit_recovery_code == 0
    assert len(adapter.submit_calls) == 4
    recovered_exit = repository.get_intent(exit_intent_id)
    assert recovered_exit is not None
    assert recovered_exit["status"] == "FILLED"
    assert all(leg["status"] == "FILLED" for leg in recovered_exit["legs"])
    assert all(len(repository.broker_orders_for_leg(leg["id"])) == 1 for leg in recovered_exit["legs"])
    assert repository.book_intents(config.account.id, book_id="pilot-book-b") == []

    resolve_code, resolved = _run_cli(
        capsys,
        [
            "resolve-roundtrip", "--config", str(config_path),
            "--entry-intent-id", source_intent_id,
            "--exit-intent-id", exit_intent_id, "--json",
        ],
    )
    assert resolve_code == 0, json.dumps(resolved, indent=2, sort_keys=True)
    assert resolved["status"] == "COMPLETED"
    assert len(adapter.submit_calls) == 4

    final_path = tmp_path / "delayed-final.json"
    final_code, final = _run_cli(
        capsys,
        ["final-state", "--config", str(config_path), "--json"],
        final_path,
    )
    assert final_code == 0, json.dumps(final, indent=2, sort_keys=True)
    assert final["final_state_passed"] is True
    assert final["final"]["flat"] is True
    assert final["final"]["no_open_orders"] is True
    assert final["final"]["book_exposure"] == {"pilot-book-a": "0", "pilot-book-b": "0"}

    # The durable session evidence command must still reject this run because
    # the configured two-book contract has no Book B entry or exit intent.
    evidence_path = tmp_path / "delayed-session-evidence.json"
    evidence_code, evidence = _run_cli(
        capsys,
        [
            "session-evidence", "--config", str(config_path), "--session-id", "delayed-cli-session",
            "--entry-intent-id", source_intent_id, "--entry-intent-id", "missing-book-b-entry",
            "--exit-intent-id", exit_intent_id, "--exit-intent-id", "missing-book-b-exit",
            "--preflight-evidence", str(preflight_path),
            "--recovery-evidence", str(recovery_path),
            "--final-evidence", str(final_path), "--commit-sha", "offline-test",
            "--trading-date", "2026-10-07", "--json",
        ],
        evidence_path,
    )
    assert evidence_code == 2
    assert evidence["evidence_builder_missing"]
    assert any("entry intent missing-book-b-entry is not durable" in item for item in evidence["evidence_builder_missing"])
    assert any("exit intent missing-book-b-exit is not durable" in item for item in evidence["evidence_builder_missing"])

    finalize_code, finalized = _run_cli(
        capsys,
        [
            "session-finalize", "--config", str(config_path),
            "--session-id", "delayed-cli-session", "--evidence", str(evidence_path), "--json",
        ],
    )
    assert finalize_code == 2
    assert finalized["result"] in {"FAILED", "INVALID"}
    assert finalized["counted_for_completion"] is False
    assert any("exactly two" in reason or "not durable" in reason for reason in finalized["failure_reasons"])
    assert len(adapter.submit_calls) == 4
    assert repository.book_intents(config.account.id, book_id="pilot-book-b") == []
