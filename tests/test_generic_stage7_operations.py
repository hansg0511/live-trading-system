from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import json
from types import SimpleNamespace

import pytest

from src.trading_core.domain import (
    Account,
    AssetClass,
    Book,
    ExecutionPolicy,
    Instrument,
    IntentAction,
    IntentStatus,
    IssueSeverity,
    LegStatus,
    OrderIntent,
    OrderLeg,
    ReconciliationIssue,
    ReconciliationRun,
    ReconciliationStatus,
    Side,
    Strategy,
    TradingEnvironment,
)
from src.trading_core.operations import (
    CollectingAlertSink,
    OperationalAuditEvent,
    OperationalAuditError,
    OperationalConfig,
    OperationalMode,
    OperationalSafetyError,
    OperationalService,
    OperationalStatusReporter,
    ServiceState,
)
from src.trading_core.repository import SQLiteTradingRepository


NOW = datetime(2026, 10, 1, 8, 0, tzinfo=timezone.utc)


def make_account(*, enabled: bool = True) -> Account:
    return Account(
        id="stage7-account",
        broker="fake",
        environment=TradingEnvironment.SIM,
        external_account_id="stage7-external",
        base_currency="USD",
        enabled=enabled,
        metadata={"allocation_capacity": "100", "api_token": "must-not-leak"},
        created_at=NOW,
        updated_at=NOW,
    )


def make_repository(tmp_path, *, books: tuple[str, ...] = ("book-a", "book-b")):
    repository = SQLiteTradingRepository(tmp_path / "stage7.db")
    repository.initialize()
    account = make_account()
    repository.save_account(account)
    repository.save_strategy(
        Strategy(
            id="stage7-strategy",
            name="Stage 7 test strategy",
            strategy_type="generic_stat_arb",
            created_at=NOW,
            updated_at=NOW,
        )
    )
    for book_id in books:
        repository.save_book(Book(id=book_id, name=book_id.upper(), created_at=NOW, updated_at=NOW))
    return repository, account


def test_config_defaults_to_dry_run_and_sim_requires_explicit_arm(tmp_path):
    repository, account = make_repository(tmp_path)
    config = OperationalConfig.from_mapping(
        {
            "account": {
                "id": account.id,
                "broker": account.broker,
                "environment": "SIM",
                "external_account_id": account.external_account_id,
            },
            "book_ids": ["book-a", "book-b"],
        }
    )
    assert config.mode is OperationalMode.DRY_RUN
    config.validate_repository(repository)
    with pytest.raises(OperationalSafetyError, match="SIM_ARMED"):
        OperationalConfig.from_mapping(
            {
                "account": {
                    "id": account.id,
                    "broker": account.broker,
                    "environment": "SIM",
                    "external_account_id": account.external_account_id,
                },
                "mode": "SIM_ARMED",
            }
        )


def test_real_live_environment_and_mode_are_structurally_unavailable():
    account_payload = {
        "id": "real-account",
        "broker": "fake",
        "environment": "SIM",
        "external_account_id": "real-external",
    }
    with pytest.raises(OperationalSafetyError, match="REAL/LIVE"):
        OperationalConfig.from_mapping({"account": account_payload, "mode": "REAL"})
    with pytest.raises(ValueError):
        OperationalConfig.from_mapping({**{"account": {**account_payload, "environment": "LIVE"}}})


def test_status_reuses_generic_reads_and_surfaces_books_orders_fills_and_blockers(tmp_path):
    repository, account = make_repository(tmp_path)
    config = OperationalConfig(account=account, book_ids=("book-a", "book-b"), sleeve_ids=("sleeve-a", "sleeve-b"), pilot_id="pilot")
    run = ReconciliationRun(
        id="stage7-run",
        account_id=account.id,
        started_at=NOW,
        status=ReconciliationStatus.RUNNING,
    )
    repository.save_reconciliation_run(run)
    repository.upsert_reconciliation_issue(
        ReconciliationIssue(
            id="stage7-issue",
            run_id=run.id,
            account_id=account.id,
            issue_key="STAGE7_TEST_BLOCKER",
            entity_type="ACCOUNT",
            entity_key=account.id,
            category="TEST",
            severity=IssueSeverity.CRITICAL,
            details={"reason": "operator review"},
            detected_at=NOW,
        )
    )
    report = OperationalStatusReporter(repository, config=config).report()
    assert report["account"]["id"] == account.id
    assert {item["id"] for item in report["books"]} == {"book-a", "book-b"}
    assert report["intents"] == []
    assert report["orders"] == []
    assert report["fills"] == []
    assert report["pilot_runs"] == []
    assert report["reconciliation_blockers"][0]["issue_key"] == "STAGE7_TEST_BLOCKER"
    assert report["alerts"][0]["key"] == "reconciliation:STAGE7_TEST_BLOCKER"
    json.dumps(report)


def test_status_surfaces_reconciliation_required_intent_even_without_open_rows(tmp_path):
    repository, account = make_repository(tmp_path, books=("book-a",))
    repository.save_instrument(
        Instrument(
            id="stage7-instrument",
            asset_class=AssetClass.EQUITY,
            symbol="STAGE7",
            venue="TEST",
            currency="USD",
            created_at=NOW,
            updated_at=NOW,
        )
    )
    intent = OrderIntent(
        id="stage7-recon-intent",
        idempotency_key="stage7-recon-key",
        strategy_id="stage7-strategy",
        account_id=account.id,
        action=IntentAction.ENTER,
        book_id="book-a",
        execution_policy=ExecutionPolicy(),
        legs=(OrderLeg(
            id="stage7-recon-leg",
            intent_id="stage7-recon-intent",
            sequence=0,
            instrument_id="stage7-instrument",
            side=Side.BUY,
            quantity=Decimal("1"),
            status=LegStatus.PLANNED,
            created_at=NOW,
            updated_at=NOW,
        ),),
        created_at=NOW,
        updated_at=NOW,
    )
    repository.create_intent(intent)
    repository.transition_intent(intent.id, IntentStatus.RECONCILIATION_REQUIRED, now=NOW)

    report = OperationalStatusReporter(
        repository,
        config=OperationalConfig(account=account, book_ids=("book-a",)),
    ).report()

    assert report["reconciliation_blockers"] == []
    assert report["unfinished_intent_blockers"] == [{
        "intent_id": intent.id,
        "book_id": "book-a",
        "action": "ENTER",
        "status": IntentStatus.RECONCILIATION_REQUIRED.value,
        "reason": "intent remains reconciliation-required and is not a completed lifecycle",
    }]
    assert any(alert["key"] == f"unfinished-intent:{intent.id}" for alert in report["alerts"])


def test_dry_run_lifecycle_is_status_only_and_audit_is_durable(tmp_path):
    repository, account = make_repository(tmp_path, books=())
    config = OperationalConfig(account=account)
    service = OperationalService(repository, config, clock=lambda: NOW)
    assert service.state is ServiceState.STOPPED
    service.start()
    result = service.once()
    assert result["tick"]["outcome"] == "STATUS_ONLY"
    assert service.state is ServiceState.RUNNING
    service.stop()
    event_types = {row["event_type"] for row in repository.operational_events(account.id)}
    assert {"service.start", "service.tick", "service.stop"}.issubset(event_types)
    restarted = SQLiteTradingRepository(tmp_path / "stage7.db")
    restarted.initialize()
    assert len(restarted.operational_events(account.id)) >= 3


def test_sim_tick_without_explicitly_wired_oms_blocks_and_alerts_without_submit(tmp_path):
    repository, account = make_repository(tmp_path, books=())
    config = OperationalConfig(account=account, mode=OperationalMode.SIM_ARMED, sim_arm=True)
    alerts = CollectingAlertSink()
    service = OperationalService(repository, config, alert_sink=alerts, clock=lambda: NOW)
    service.start()
    result = service.once()
    assert result["tick"]["outcome"] == "BLOCKED"
    assert result["tick"]["reason"].startswith("SIM recovery")
    assert any(alert.key == "sim-recovery-unavailable" for alert in alerts.alerts)
    assert not repository.book_broker_orders(account.id)


def test_audit_event_redacts_secret_fields_and_preserves_safe_details(tmp_path):
    repository, account = make_repository(tmp_path, books=())
    event = OperationalAuditEvent(
        account_id=account.id,
        event_type="operator.note",
        mode=OperationalMode.DRY_RUN,
        outcome="RECORDED",
        summary="local note",
        details={"api_token": "super-secret", "nested": {"password": "also-secret", "safe": "ok"}},
        occurred_at=NOW,
    )
    event.persist(repository)
    row = repository.operational_events(account.id, limit=1)[0]
    assert row["details"]["api_token"] == "<redacted>"
    assert row["details"]["nested"]["password"] == "<redacted>"
    assert row["details"]["nested"]["safe"] == "ok"
    assert "super-secret" not in json.dumps(row)


def test_cli_status_json_and_default_dry_run(tmp_path, capsys):
    repository, account = make_repository(tmp_path, books=())
    # Import the script as a module so this remains an offline unit test.
    from scripts.generic_operational_cli import main

    assert main(["status", "--state-db", str(repository.db_path), "--account-id", account.id, "--json"]) == 0
    output = capsys.readouterr().out
    value = json.loads(output)
    assert value["mode"] == "DRY_RUN"
    assert value["submit_gate"] == "DRY_RUN_ONLY"


def test_config_json_load_rejects_malformed_or_real_mode(tmp_path):
    path = tmp_path / "ops.json"
    path.write_text('{"account": {"id": "x"}, "mode": "REAL"}', encoding="utf-8")
    with pytest.raises(OperationalSafetyError):
        OperationalConfig.load(path)


def test_repository_audit_boundary_redacts_direct_secret_and_private_payload_details(tmp_path):
    repository, account = make_repository(tmp_path, books=())
    repository.record_operational_event(
        event_id="direct-audit",
        account_id=account.id,
        event_type="operator.direct",
        mode="DRY_RUN",
        outcome="RECORDED",
        occurred_at=NOW,
        summary="direct test",
        details={
            "token": "do-not-store",
            "nested": {"password": "also-do-not-store"},
            "private_response_payload": {"deal_qty": "1", "secret": "hidden"},
            "safe": "visible",
        },
    )
    row = repository.operational_events(account.id, limit=1)[0]
    assert row["details"]["token"] == "<redacted>"
    assert row["details"]["nested"]["password"] == "<redacted>"
    assert row["details"]["private_response_payload"] == "<redacted>"
    assert row["details"]["safe"] == "visible"
    assert "do-not-store" not in json.dumps(row)


def test_book_risk_status_failure_emits_actionable_alert(tmp_path):
    repository, account = make_repository(tmp_path, books=())
    config = OperationalConfig(account=account)

    class BrokenOMS:
        def book_risk_status(self, *, account):
            raise RuntimeError("risk read unavailable")

    report = OperationalStatusReporter(repository, config=config, oms=BrokenOMS()).report()
    alert = next(item for item in report["alerts"] if item["key"] == "BOOK_RISK_STATUS_ERROR")
    assert alert["severity"] == "CRITICAL"
    assert "risk read unavailable" in alert["details"]["error"]


def test_lifecycle_transition_is_audited_before_state_mutation(tmp_path):
    repository, account = make_repository(tmp_path, books=())
    service = OperationalService(repository, OperationalConfig(account=account), clock=lambda: NOW)
    service.start()
    with repository.transaction() as conn:
        rows = conn.execute(
            "SELECT outcome, details_json FROM core_operational_events WHERE event_type = 'service.start' ORDER BY rowid"
        ).fetchall()
    assert [row["outcome"] for row in rows] == ["INTENT", "STARTED"]
    assert json.loads(rows[0]["details_json"])["correlation_id"] == json.loads(rows[1]["details_json"])["correlation_id"]


def test_start_terminal_audit_failure_is_explicit_after_pretransition(tmp_path):
    repository, account = make_repository(tmp_path, books=())
    service = OperationalService(repository, OperationalConfig(account=account), clock=lambda: NOW)
    original = repository.record_operational_event
    calls = 0

    def fail_terminal(**kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("audit storage unavailable")
        return original(**kwargs)

    repository.record_operational_event = fail_terminal  # type: ignore[method-assign]
    with pytest.raises(OperationalAuditError, match="terminal start audit"):
        service.start()
    assert service.state is ServiceState.RUNNING
    with repository.transaction() as conn:
        rows = conn.execute(
            "SELECT outcome FROM core_operational_events WHERE event_type = 'service.start' ORDER BY rowid"
        ).fetchall()
    assert [row["outcome"] for row in rows] == ["INTENT"]


def test_tick_persists_intent_and_terminal_with_same_correlation(tmp_path):
    repository, account = make_repository(tmp_path, books=())
    service = OperationalService(repository, OperationalConfig(account=account), clock=lambda: NOW)
    service.start()
    result = service.once()
    assert result["tick"]["outcome"] == "STATUS_ONLY"
    with repository.transaction() as conn:
        rows = conn.execute(
            "SELECT outcome, details_json FROM core_operational_events WHERE event_type = 'service.tick' ORDER BY rowid"
        ).fetchall()
    assert [row["outcome"] for row in rows] == ["INTENT", "STATUS_ONLY"]
    assert json.loads(rows[0]["details_json"])["correlation_id"] == json.loads(rows[1]["details_json"])["correlation_id"]


def test_terminal_tick_audit_failure_is_explicit_after_intent(tmp_path):
    repository, account = make_repository(tmp_path, books=())
    service = OperationalService(repository, OperationalConfig(account=account), clock=lambda: NOW)
    service.start()
    original = repository.record_operational_event
    calls = 0

    def fail_terminal(**kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("audit storage unavailable")
        return original(**kwargs)

    repository.record_operational_event = fail_terminal  # type: ignore[method-assign]
    result = service.once()
    assert result["tick"]["outcome"] == "AUDIT_FAILURE"
    assert any(alert.key == "operational-audit-failed" for alert in service.alert_sink.alerts)
    with repository.transaction() as conn:
        rows = conn.execute(
            "SELECT outcome FROM core_operational_events WHERE event_type = 'service.tick' ORDER BY rowid"
        ).fetchall()
    assert [row["outcome"] for row in rows] == ["INTENT"]


def test_explicit_pilot_handoff_has_durable_intent_and_terminal_event(tmp_path):
    repository, account = make_repository(tmp_path, books=())
    config = OperationalConfig(account=account, mode=OperationalMode.SIM_ARMED, sim_arm=True)
    service = OperationalService(repository, config, clock=lambda: NOW)
    service.start()
    calls = []

    class FakeRunner:
        def run(self, spec, *, mode):
            calls.append((spec, mode))
            return SimpleNamespace(run_id="offline-pilot")

    spec = SimpleNamespace(account=account)
    result = service.handoff_to_pilot(FakeRunner(), spec, confirm_sim=True)
    assert result.run_id == "offline-pilot"
    assert calls[0][1] == "SIM_SUBMIT"
    with repository.transaction() as conn:
        rows = conn.execute(
            "SELECT outcome, details_json FROM core_operational_events WHERE event_type = 'pilot.handoff' ORDER BY rowid"
        ).fetchall()
    assert [row["outcome"] for row in rows] == ["INTENT", "RETURNED"]
    assert json.loads(rows[0]["details_json"])["correlation_id"] == json.loads(rows[1]["details_json"])["correlation_id"]
