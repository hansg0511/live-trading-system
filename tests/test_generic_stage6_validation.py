from __future__ import annotations

from datetime import datetime, timezone
from dataclasses import replace
import copy
import json
from pathlib import Path

import pytest

from src.trading_core.domain import Account, TradingEnvironment
from src.trading_core.repository import SQLiteTradingRepository
from src.trading_core.stage6_validation import (
    Stage6EvidenceClass,
    Stage6SessionOutcome,
    Stage6ValidationError,
    evaluate_stage6_session,
)
from src.strategies.stat_arb.stage6_config import Stage6PilotConfig
from scripts.generic_stage6_pilot import main as stage6_cli
from tests.test_generic_stage6_config_cli import _config_dict


NOW = datetime(2026, 10, 5, 13, 40, tzinfo=timezone.utc)


def _account() -> Account:
    return Account(
        id="validation-account",
        broker="moomoo",
        environment=TradingEnvironment.SIM,
        external_account_id="5077333",
        base_currency="USD",
        enabled=True,
        metadata={},
        created_at=NOW,
        updated_at=NOW,
    )


def _order(order_id: str, intent_id: str, *, side: str = "BUY") -> dict:
    return {
        "order_id": order_id,
        "intent_id": intent_id,
        "side": side,
        "attributable": True,
        "status": "FILLED",
        "quantity": 1,
        "filled_quantity": 1,
        "fill_price": "100.00",
    }


def _evidence(
    trading_date: str = "2026-10-05",
    *,
    compatibility: str = "stage6-execution-v1",
    legacy: bool = False,
) -> dict:
    entry_ids = [f"entry-a-{trading_date}", f"entry-b-{trading_date}"]
    exit_ids = [f"exit-a-{trading_date}", f"exit-b-{trading_date}"]

    def provenance(intent_id: str, *, mode: str = "SIM_SUBMIT") -> dict:
        submitted_at = (
            f"{trading_date}T13:40:00+00:00"
            if intent_id.startswith("entry-")
            else f"{trading_date}T13:50:00+00:00"
        )
        return {
            "process_id": "stage6-submit-process-1",
            "run_id": f"stage6-run-{trading_date}",
            "mode": mode,
            "execution_compatibility": compatibility,
            "submitted_at": submitted_at,
            "correlation": {
                "intent_id": intent_id,
                "run_id": f"stage6-run-{trading_date}",
                "source_signal_id": f"signal-{intent_id}",
                "idempotency_key": f"idempotency-{intent_id}",
            },
        }

    return {
        "session_id": f"session-{trading_date}",
        "us_trading_date": trading_date,
        "started_at": f"{trading_date}T13:40:00+00:00",
        "completed_at": f"{trading_date}T14:00:00+00:00",
        "commit_sha": "871cb83844ead41272bc07bb7fbf6f83c3bf2ea2",
        "execution_compatibility": compatibility,
        "account_id": "validation-account",
        "environment": "SIM",
        "broker_contacted": True,
        "execution_path": "Stage6PilotRunner->GenericOMS",
        "execution_mode": "SIM_SUBMIT",
        "supervised": True,
        "run_ids": [f"stage6-run-{trading_date}"],
        "entry_intent_ids": entry_ids,
        "exit_intent_ids": exit_ids,
        "derived_us_trading_date": trading_date,
        "provenance_complete": True,
        "execution_provenance": {
            "entry": {intent_id: provenance(intent_id) for intent_id in entry_ids},
            "exit": {intent_id: provenance(intent_id) for intent_id in exit_ids},
        },
        "legacy": legacy,
        "preflight": {
            "account_identity": {"account_id": "validation-account", "environment": "SIM"},
            "fresh_facts": {
                "complete": True,
                "captured_at": f"{trading_date}T13:40:00+00:00",
                "flat": True,
                "open_order_count": 0,
            },
            "rth": {"observed": True, "market_state": "AFTERNOON"},
            "books": [
                {"book_id": "book-a", "allocation_valid": True, "mapping_valid": True},
                {"book_id": "book-b", "allocation_valid": True, "mapping_valid": True},
            ],
            "mappings_valid": True,
            "quantities_valid": True,
            "safety_gates_passed": True,
            "issues": 0,
            "actions": 0,
            "unfinished_intents": 0,
        },
        "entry": {
            "expected_intents": [{"intent_id": f"entry-a-{trading_date}"}, {"intent_id": f"entry-b-{trading_date}"}],
            "expected_orders": [{"order_id": f"entry-order-a-{trading_date}"}, {"order_id": f"entry-order-b-{trading_date}"}],
            "actual_orders": [_order(f"entry-order-a-{trading_date}", f"entry-a-{trading_date}"), _order(f"entry-order-b-{trading_date}", f"entry-b-{trading_date}", side="SELL")],
            "unexpected_attempts": 0,
            "duplicate_attempts": 0,
            "fills_complete": True,
            "orders_attributable": True,
        },
        "restart_recovery": {
            "performed": True,
            "broker_contacted": True,
            "fresh_process": True,
            "process_id": "stage6-recovery-process-2",
            "captured_at": f"{trading_date}T13:46:40+00:00",
            "result": "PASS",
            "source_intent_ids": [f"entry-a-{trading_date}", f"entry-b-{trading_date}"],
            "source_submission_process_ids": {
                f"entry-a-{trading_date}": "stage6-submit-process-1",
                f"entry-b-{trading_date}": "stage6-submit-process-1",
            },
            "source_submission_process_identity_complete": True,
            "preserved_intent_ids": [f"entry-a-{trading_date}", f"entry-b-{trading_date}"],
            "preserved_order_ids": [f"entry-order-a-{trading_date}", f"entry-order-b-{trading_date}"],
            "intents_preserved": True,
            "orders_preserved": True,
            "no_duplicate_attempts": True,
            "no_resubmission": True,
            "exposure_agrees": True,
        },
        "exit": {
            "expected_orders": [{"order_id": f"exit-order-a-{trading_date}"}, {"order_id": f"exit-order-b-{trading_date}"}],
            "actual_orders": [_order(f"exit-order-a-{trading_date}", f"exit-a-{trading_date}", side="SELL"), _order(f"exit-order-b-{trading_date}", f"exit-b-{trading_date}", side="BUY")],
            "unexpected_attempts": 0,
            "duplicate_attempts": 0,
            "fills_complete": True,
            "orders_attributable": True,
            "current_exposure_inverse": True,
            "submitted_via": "Stage6PilotRunner->GenericOMS",
            "delayed_partial_recovery": {"occurred": True, "recovered": True, "no_duplicate_attempts": True, "no_blind_rescue": True},
        },
        "final": {
            "fresh_facts": {
                "complete": True,
                "captured_at": f"{trading_date}T13:56:00+00:00",
                "flat": True,
                "open_order_count": 0,
            },
            "flat": True,
            "no_open_orders": True,
            "open_order_count": 0,
            "book_exposure": {"book-a": 0, "book-b": 0},
            "issues": 0,
            "actions": 0,
            "unfinished_intents": 0,
            "terminal_intents": True,
            "all_orders_attributable": True,
        },
        "audit_refs": ["audit:stage6:test"],
    }


def _repository(tmp_path: Path) -> SQLiteTradingRepository:
    repository = SQLiteTradingRepository(tmp_path / "validation.db")
    repository.initialize()
    repository.save_account(_account())
    return repository


def _save_verified_clean(
    repository: SQLiteTradingRepository,
    evidence: dict,
):
    """Model the private runner proof boundary for persistence tests."""

    verified = copy.deepcopy(evidence)
    verified["_stage6_durable_graph_verified"] = True
    result = evaluate_stage6_session(verified)
    assert result.outcome is Stage6SessionOutcome.CLEAN_PASS
    return repository.save_stage6_validation_session(
        result,
        _validation_capability=repository._stage6_validation_capability(),
    )


def test_direct_clean_save_requires_verified_runner_capability_but_failed_is_retained(tmp_path):
    repository = _repository(tmp_path)
    clean = evaluate_stage6_session(_evidence())
    with pytest.raises(PermissionError, match="CLEAN_PASS capability"):
        repository.save_stage6_validation_session(clean)

    failed_evidence = _evidence("2026-10-06")
    failed_evidence["declared_result"] = "FAILED"
    failed_evidence["failure_reasons"] = ["operator stopped before qualification"]
    failed = evaluate_stage6_session(failed_evidence)
    assert failed.outcome is Stage6SessionOutcome.FAILED
    repository.save_stage6_validation_session(failed)
    assert repository.stage6_validation_status("validation-account")["retained_session_count"] == 1


def test_clean_session_result_is_derived_and_retained(tmp_path):
    repository = _repository(tmp_path)
    for trading_date in ("2026-10-05", "2026-10-06", "2026-10-07"):
        result = _save_verified_clean(repository, _evidence(trading_date))
        assert result.qualified is True

    status = repository.stage6_validation_status("validation-account")
    assert status["complete"] is True
    assert status["qualified_clean_dates"] == ["2026-10-05", "2026-10-06", "2026-10-07"]


def test_duplicate_clean_date_is_rejected_but_failed_history_is_retained(tmp_path):
    repository = _repository(tmp_path)
    _save_verified_clean(repository, _evidence())
    duplicate_evidence = copy.deepcopy(_evidence())
    duplicate_evidence["session_id"] = "different-session"
    duplicate_evidence["_stage6_durable_graph_verified"] = True
    duplicate = evaluate_stage6_session(duplicate_evidence)
    with pytest.raises(ValueError, match="CLEAN_PASS already exists"):
        repository.save_stage6_validation_session(
            duplicate,
            _validation_capability=repository._stage6_validation_capability(),
        )

    failed_evidence = _evidence("2026-10-06")
    failed_evidence["preflight"]["rth"] = {"observed": False, "market_state": "CLOSED"}
    failed = evaluate_stage6_session(failed_evidence)
    assert failed.outcome is Stage6SessionOutcome.INVALID
    repository.save_stage6_validation_session(failed)
    assert repository.stage6_validation_status("validation-account")["retained_session_count"] == 2


@pytest.mark.parametrize(
    ("mutation", "reason"),
    (
        (lambda value: value["preflight"].pop("fresh_facts"), "fresh_facts missing"),
        (lambda value: value["preflight"].update({"issues": 1}), "issues"),
        (lambda value: value["restart_recovery"].update({"fresh_process": False}), "fresh_process"),
        (lambda value: value["exit"].update({"current_exposure_inverse": False}), "current_exposure_inverse"),
        (lambda value: value["final"].update({"book_exposure": {"book-a": 1, "book-b": 0}}), "not flat"),
    ),
)
def test_missing_or_contradictory_evidence_never_counts(mutation, reason):
    value = _evidence()
    mutation(value)
    result = evaluate_stage6_session(value)
    assert result.outcome is Stage6SessionOutcome.INVALID
    assert result.qualified is False
    assert any(reason in item for item in result.failure_reasons)


def test_legacy_oct5_evidence_is_retained_but_unqualified():
    result = evaluate_stage6_session(_evidence(legacy=True))
    assert result.outcome is Stage6SessionOutcome.INVALID
    assert result.evidence_class == Stage6EvidenceClass.LEGACY_VERIFIED_EVIDENCE.value
    assert result.qualified is False
    assert any("legacy evidence" in item for item in result.failure_reasons)


def test_recovery_identity_and_numeric_evidence_are_strictly_checked():
    value = _evidence()
    for section in (value["entry"], value["exit"]):
        for order in section["actual_orders"]:
            order["quantity"] = 1.0
            order["filled_quantity"] = 1.00
    assert evaluate_stage6_session(value).outcome is Stage6SessionOutcome.CLEAN_PASS

    missing_identity = _evidence("2026-10-11")
    missing_identity["restart_recovery"].pop("preserved_order_ids")
    result = evaluate_stage6_session(missing_identity)
    assert result.outcome is Stage6SessionOutcome.INVALID
    assert any("preserved_order_ids" in item for item in result.failure_reasons)

    wrong_final_book = _evidence("2026-10-12")
    wrong_final_book["final"]["book_exposure"] = {"book-a": 0, "other-book": 0}
    result = evaluate_stage6_session(wrong_final_book)
    assert result.outcome is Stage6SessionOutcome.INVALID
    assert any("book identities" in item for item in result.failure_reasons)


@pytest.mark.parametrize(
    ("mutation", "reason"),
    (
        (lambda value: value.pop("execution_provenance"), "execution_provenance"),
        (lambda value: value["execution_provenance"]["entry"][value["entry_intent_ids"][0]].update({"execution_compatibility": "stage6-execution-v1-other"}), "incompatible execution"),
        (lambda value: value["execution_provenance"]["exit"][value["exit_intent_ids"][0]].update({"submitted_at": "2026-10-06T13:40:00+00:00"}), "derive the supplied US trading date"),
        (lambda value: value.update({"derived_us_trading_date": "2026-10-06"}), "disagrees with derived"),
    ),
)
def test_durable_provenance_date_and_compatibility_guards_fail_closed(mutation, reason):
    value = _evidence()
    mutation(value)
    result = evaluate_stage6_session(value)
    assert result.outcome is Stage6SessionOutcome.INVALID
    assert any(reason in item for item in result.failure_reasons)


def test_clean_pass_cannot_reuse_intents_or_orders_on_another_date(tmp_path):
    repository = _repository(tmp_path)
    first = _evidence("2026-10-05")
    _save_verified_clean(repository, first)

    reused = copy.deepcopy(first)
    reused["session_id"] = "reused-execution-on-new-date"
    reused["us_trading_date"] = "2026-10-06"
    reused["derived_us_trading_date"] = "2026-10-06"
    reused["started_at"] = "2026-10-06T13:40:00+00:00"
    reused["completed_at"] = "2026-10-06T14:00:00+00:00"
    reused["preflight"]["fresh_facts"]["captured_at"] = "2026-10-06T13:40:00+00:00"
    reused["restart_recovery"]["captured_at"] = "2026-10-06T13:46:40+00:00"
    reused["final"]["fresh_facts"]["captured_at"] = "2026-10-06T13:56:00+00:00"
    for marker in reused["execution_provenance"]["entry"].values():
        marker["submitted_at"] = "2026-10-06T13:40:00+00:00"
    for marker in reused["execution_provenance"]["exit"].values():
        marker["submitted_at"] = "2026-10-06T13:50:00+00:00"
    result = evaluate_stage6_session(reused)
    assert result.outcome is Stage6SessionOutcome.CLEAN_PASS
    reused["_stage6_durable_graph_verified"] = True
    result = evaluate_stage6_session(reused)
    with pytest.raises(ValueError, match="reuses durable execution identity"):
        repository.save_stage6_validation_session(
            result,
            _validation_capability=repository._stage6_validation_capability(),
        )


def test_failed_run_duplicate_attempt_and_missing_recovery_are_not_clean():
    failed_evidence = _evidence("2026-10-08")
    failed_evidence["declared_result"] = "FAILED"
    failed_evidence["failure_reasons"] = ["operator stopped after an incomplete fill"]
    failed = evaluate_stage6_session(failed_evidence)
    assert failed.outcome is Stage6SessionOutcome.FAILED
    assert failed.qualified is False

    duplicate_evidence = _evidence("2026-10-09")
    duplicate_evidence["entry"]["duplicate_attempts"] = 1
    duplicate_evidence["entry"]["unexpected_attempts"] = 1
    duplicate = evaluate_stage6_session(duplicate_evidence)
    assert duplicate.outcome is Stage6SessionOutcome.INVALID
    assert any("duplicate_attempts" in item for item in duplicate.failure_reasons)
    assert any("unexpected_attempts" in item for item in duplicate.failure_reasons)

    missing_recovery = _evidence("2026-10-10")
    missing_recovery.pop("restart_recovery")
    result = evaluate_stage6_session(missing_recovery)
    assert result.outcome is Stage6SessionOutcome.INVALID
    assert any("recovery evidence missing" in item for item in result.failure_reasons)


def test_compatibility_mismatch_does_not_complete_series(tmp_path):
    repository = _repository(tmp_path)
    _save_verified_clean(repository, _evidence("2026-10-05"))
    _save_verified_clean(repository, _evidence("2026-10-06", compatibility="different-execution-v2"))
    _save_verified_clean(repository, _evidence("2026-10-07"))
    status = repository.stage6_validation_status("validation-account")
    assert status["complete"] is False
    assert "incompatible execution identities" in " ".join(status["reasons"])


def test_session_and_observation_evidence_are_immutable(tmp_path):
    repository = _repository(tmp_path)
    value = _evidence()
    _save_verified_clean(repository, value)
    _save_verified_clean(repository, value)
    changed = dict(value)
    changed["commit_sha"] = "changed"
    changed["_stage6_durable_graph_verified"] = True
    with pytest.raises(ValueError, match="immutable"):
        repository.save_stage6_validation_session(
            evaluate_stage6_session(changed),
            _validation_capability=repository._stage6_validation_capability(),
        )

    observation = {"phase": "RECOVERY", "fresh_process": True, "process_id": "p2"}
    repository.record_stage6_validation_observation(
        observation_id="obs-1",
        session_id="session-2026-10-05",
        account_id="validation-account",
        phase="RECOVERY",
        captured_at=NOW,
        evidence=observation,
        process_id="p2",
        fresh_process=True,
    )
    assert repository.record_stage6_validation_observation(
        observation_id="obs-2",
        session_id="session-2026-10-05",
        account_id="validation-account",
        phase="RECOVERY",
        captured_at=NOW,
        evidence=observation,
        process_id="p2",
        fresh_process=True,
    ) == "obs-1"
    with pytest.raises(ValueError, match="reused with different evidence"):
        repository.record_stage6_validation_observation(
            observation_id="obs-1",
            session_id="session-2026-10-05",
            account_id="validation-account",
            phase="RECOVERY",
            captured_at=NOW,
            evidence={"phase": "RECOVERY", "fresh_process": False},
            process_id="p2",
            fresh_process=False,
        )


@pytest.mark.parametrize(
    ("mutation", "reason"),
    (
        (
            lambda value: value["preflight"]["fresh_facts"].update(
                {"captured_at": "2026-10-14T13:46:00+00:00"}
            ),
            "preflight must not follow entry submission",
        ),
        (
            lambda value: value["restart_recovery"].update(
                {"captured_at": "2026-10-14T13:30:00+00:00"}
            ),
            "entry submission must not follow recovery",
        ),
        (
            lambda value: value["restart_recovery"].update(
                {"captured_at": "2026-10-14T13:56:00+00:00"}
            ),
            "recovery must not follow exit submission",
        ),
        (
            lambda value: value["final"]["fresh_facts"].update(
                {"captured_at": "2026-10-14T13:45:00+00:00"}
            ),
            "exit submission must not follow final observation",
        ),
    ),
)
def test_phase_temporal_order_is_causal(mutation, reason):
    value = _evidence("2026-10-14")
    mutation(value)
    result = evaluate_stage6_session(value)
    assert result.outcome is Stage6SessionOutcome.INVALID
    assert reason in result.failure_reasons


def test_validation_cli_rejects_explicit_identity_conflicts_with_artifact(tmp_path, capsys):
    values = _config_dict(tmp_path)
    config_path = tmp_path / "stage6-conflict.json"
    config_path.write_text(json.dumps(values), encoding="utf-8")
    artifact = _evidence()
    artifact["session_id"] = "artifact-session"
    artifact["account_id"] = "pilot-account"
    artifact_path = tmp_path / "artifact.json"
    artifact_path.write_text(json.dumps(artifact), encoding="utf-8")

    conflicts = (
        ("--session-id", "cli-session"),
        ("--account-id", "other-account"),
        ("--commit-sha", "other-commit"),
        ("--execution-compatibility", "other-execution"),
        ("--trading-date", "2026-10-06"),
    )
    for option, value in conflicts:
        assert stage6_cli(
            [
                "session-preflight",
                "--config",
                str(config_path),
                "--evidence",
                str(artifact_path),
                option,
                value,
                "--json",
            ]
        ) == 2
        payload = json.loads(capsys.readouterr().out)
        assert "conflicts with evidence" in payload["error"]


def test_unparseable_document_fails_closed():
    with pytest.raises(Stage6ValidationError, match="session_id"):
        evaluate_stage6_session({"result": "CLEAN_PASS"})


def test_validation_cli_records_phases_finalizes_and_reports_without_broker(tmp_path, capsys):
    config_values = _config_dict(tmp_path)
    config_path = tmp_path / "stage6.json"
    config_path.write_text(json.dumps(config_values), encoding="utf-8")
    config = Stage6PilotConfig.load(config_path)
    repository = SQLiteTradingRepository(config.state_db)
    config.ensure_repository(repository)
    source_intent_ids = [f"entry-a-2026-10-05", f"entry-b-2026-10-05"]
    for sleeve, target, source_intent_id in zip(
        config.sleeves,
        config.targets,
        source_intent_ids,
        strict=True,
    ):
        intent = sleeve.to_intent(target)
        legs = tuple(
            replace(leg, id=f"{source_intent_id}-leg-{leg.sequence}", intent_id=source_intent_id)
            for leg in intent.legs
        )
        intent = replace(
            intent,
            id=source_intent_id,
            idempotency_key=f"validation|{source_intent_id}",
            legs=legs,
        )
        repository.create_intent(intent)
        repository.append_intent_metadata(
            source_intent_id,
            {
                "stage6_submission": {
                    "process_id": "stage6-submit-process-1",
                    "run_id": "validation-run",
                    "mode": "SIM_SUBMIT",
                }
            },
            account_id=config.account.id,
        )
    evidence = _evidence()
    evidence["session_id"] = "cli-session"
    evidence["account_id"] = "pilot-account"
    evidence["preflight"]["account_identity"]["account_id"] = "pilot-account"
    evidence_path = tmp_path / "evidence.json"
    evidence_path.write_text(json.dumps(evidence), encoding="utf-8")

    assert stage6_cli([
        "session-preflight", "--config", str(config_path), "--evidence", str(evidence_path), "--json",
    ]) == 0
    capsys.readouterr()
    recovery_evidence = copy.deepcopy(evidence)
    recovery_evidence["session_id"] = "cli-session"
    recovery_path = tmp_path / "recovery.json"
    recovery_path.write_text(json.dumps(recovery_evidence), encoding="utf-8")
    assert stage6_cli([
        "session-recover", "--config", str(config_path), "--evidence", str(recovery_path),
        "--fresh-process", "--process-id", "stage6-recovery-process-2", "--json",
    ]) == 0
    capsys.readouterr()

    # The document is structurally CLEAN_PASS-shaped, but it is fabricated:
    # it has no retained FINAL observation and its IDs/orders are not a
    # durable execution graph.  The supported finalization boundary must
    # retain no clean result from this input.
    assert stage6_cli([
        "session-finalize", "--config", str(config_path), "--evidence", str(evidence_path), "--json",
    ]) == 2
    finalized = capsys.readouterr().out
    assert "CLEAN_PASS durable repository verification failed" in finalized
    assert "FINAL observation" in finalized

    assert stage6_cli([
        "session-status", "--config", str(config_path), "--session-id", "cli-session", "--json",
    ]) == 0
    status = capsys.readouterr().out
    assert "PREFLIGHT" in status and "RECOVERY" in status

    assert stage6_cli(["stage-status", "--config", str(config_path), "--json"]) == 0
    stage_status = capsys.readouterr().out
    assert "STAGE_6_IN_PROGRESS" in stage_status
