from __future__ import annotations

from datetime import datetime, timezone
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
    return {
        "session_id": f"session-{trading_date}",
        "us_trading_date": trading_date,
        "started_at": "2026-10-05T13:40:00+00:00",
        "completed_at": "2026-10-05T14:00:00+00:00",
        "commit_sha": "871cb83844ead41272bc07bb7fbf6f83c3bf2ea2",
        "execution_compatibility": compatibility,
        "account_id": "validation-account",
        "environment": "SIM",
        "execution_path": "Stage6PilotRunner->GenericOMS",
        "execution_mode": "SIM_SUBMIT",
        "supervised": True,
        "run_ids": [f"stage6-run-{trading_date}"],
        "entry_intent_ids": [f"entry-a-{trading_date}", f"entry-b-{trading_date}"],
        "exit_intent_ids": [f"exit-a-{trading_date}", f"exit-b-{trading_date}"],
        "legacy": legacy,
        "preflight": {
            "account_identity": {"account_id": "validation-account", "environment": "SIM"},
            "fresh_facts": {
                "complete": True,
                "captured_at": "2026-10-05T13:40:00+00:00",
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
            "expected_orders": [{"order_id": "entry-order-a"}, {"order_id": "entry-order-b"}],
            "actual_orders": [_order("entry-order-a", f"entry-a-{trading_date}"), _order("entry-order-b", f"entry-b-{trading_date}", side="SELL")],
            "unexpected_attempts": 0,
            "duplicate_attempts": 0,
            "fills_complete": True,
            "orders_attributable": True,
        },
        "restart_recovery": {
            "performed": True,
            "fresh_process": True,
            "process_id": "stage6-recovery-process-2",
            "captured_at": "2026-10-05T13:46:40+00:00",
            "result": "PASS",
            "preserved_intent_ids": [f"entry-a-{trading_date}", f"entry-b-{trading_date}"],
            "preserved_order_ids": ["entry-order-a", "entry-order-b"],
            "intents_preserved": True,
            "orders_preserved": True,
            "no_duplicate_attempts": True,
            "exposure_agrees": True,
        },
        "exit": {
            "expected_orders": [{"order_id": "exit-order-a"}, {"order_id": "exit-order-b"}],
            "actual_orders": [_order("exit-order-a", f"exit-a-{trading_date}", side="SELL"), _order("exit-order-b", f"exit-b-{trading_date}", side="BUY")],
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
                "captured_at": "2026-10-05T13:56:00+00:00",
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


def test_clean_session_result_is_derived_and_retained(tmp_path):
    repository = _repository(tmp_path)
    for trading_date in ("2026-10-05", "2026-10-06", "2026-10-07"):
        result = evaluate_stage6_session(_evidence(trading_date))
        assert result.outcome is Stage6SessionOutcome.CLEAN_PASS
        assert result.qualified is True
        repository.save_stage6_validation_session(result)

    status = repository.stage6_validation_status("validation-account")
    assert status["complete"] is True
    assert status["qualified_clean_dates"] == ["2026-10-05", "2026-10-06", "2026-10-07"]


def test_duplicate_clean_date_is_rejected_but_failed_history_is_retained(tmp_path):
    repository = _repository(tmp_path)
    repository.save_stage6_validation_session(evaluate_stage6_session(_evidence()))
    duplicate = evaluate_stage6_session(_evidence().copy() | {"session_id": "different-session"})
    with pytest.raises(ValueError, match="CLEAN_PASS already exists"):
        repository.save_stage6_validation_session(duplicate)

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
    repository.save_stage6_validation_session(evaluate_stage6_session(_evidence("2026-10-05")))
    repository.save_stage6_validation_session(
        evaluate_stage6_session(_evidence("2026-10-06", compatibility="different-execution-v2"))
    )
    repository.save_stage6_validation_session(evaluate_stage6_session(_evidence("2026-10-07")))
    status = repository.stage6_validation_status("validation-account")
    assert status["complete"] is False
    assert "incompatible execution identities" in " ".join(status["reasons"])


def test_session_and_observation_evidence_are_immutable(tmp_path):
    repository = _repository(tmp_path)
    value = _evidence()
    result = evaluate_stage6_session(value)
    repository.save_stage6_validation_session(result)
    repository.save_stage6_validation_session(result)
    changed = dict(value)
    changed["commit_sha"] = "changed"
    with pytest.raises(ValueError, match="immutable"):
        repository.save_stage6_validation_session(evaluate_stage6_session(changed))

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


def test_unparseable_document_fails_closed():
    with pytest.raises(Stage6ValidationError, match="session_id"):
        evaluate_stage6_session({"result": "CLEAN_PASS"})


def test_validation_cli_records_phases_finalizes_and_reports_without_broker(tmp_path, capsys):
    config_values = _config_dict(tmp_path)
    config_path = tmp_path / "stage6.json"
    config_path.write_text(json.dumps(config_values), encoding="utf-8")
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
        "--fresh-process", "--process-id", "fresh-process-2", "--json",
    ]) == 0
    capsys.readouterr()

    assert stage6_cli([
        "session-finalize", "--config", str(config_path), "--evidence", str(evidence_path), "--json",
    ]) == 0
    finalized = capsys.readouterr().out
    assert "CLEAN_PASS" in finalized
    assert "orders_submitted" in finalized

    assert stage6_cli([
        "session-status", "--config", str(config_path), "--session-id", "cli-session", "--json",
    ]) == 0
    status = capsys.readouterr().out
    assert "PREFLIGHT" in status and "RECOVERY" in status

    assert stage6_cli(["stage-status", "--config", str(config_path), "--json"]) == 0
    stage_status = capsys.readouterr().out
    assert "STAGE_6_IN_PROGRESS" in stage_status
