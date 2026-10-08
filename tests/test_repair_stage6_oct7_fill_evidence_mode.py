from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3

import pytest

from scripts.repair_stage6_oct7_fill_evidence_mode import (
    CONFIRMATION,
    EXPECTED_ACCOUNT_ID,
    EXPECTED_BOOK_ID,
    EXPECTED_DB_FILENAME,
    EXPECTED_MODE,
    EXPECTED_NONFLAT_ENTRY_BLOCKER,
    EXPECTED_SOURCE_INTENT_ID,
    OLD_MODE,
    TARGETS,
    main,
)


NOW = datetime.now(timezone.utc).replace(microsecond=0)


def _metadata(target: dict[str, str]) -> dict[str, object]:
    return {
        "_broker_fill_account_id": EXPECTED_ACCOUNT_ID,
        "_evidence_reference": target["evidence_reference"],
        "_external_order_id": target["external_order_id"],
        "_instrument_id": target["instrument_id"],
        "evidence_mode": EXPECTED_MODE,
        "evidence_reference": target["evidence_reference"],
        "evidence_scope": "CURRENT_ORDER_SNAPSHOTS",
        "reason": "Moomoo deal_list_query failed with ret=-1: Paper trading does not support deal data.",
        "source": "order_list_query",
        "synthetic": True,
        "raw": {
            "order_id": target["external_order_id"],
            "code": target["raw_code"],
            "trd_side": target["side"],
            "dealt_qty": target["quantity"],
            "dealt_avg_price": target["price"],
            "order_status": "FILLED_ALL",
        },
    }


def _broker_fill(target: dict[str, str]) -> dict[str, object]:
    return {
        "external_order_id": target["external_order_id"],
        "external_fill_id": None,
        "dedupe_key": target["evidence_reference"],
        "quantity": target["quantity"],
        "price": target["price"],
        "filled_at": target["filled_at"],
        "received_at": NOW.isoformat(),
        "account_id": EXPECTED_ACCOUNT_ID,
        "instrument_id": target["instrument_id"],
        "evidence_reference": target["evidence_reference"],
        "evidence_mode": EXPECTED_MODE,
        "metadata": _metadata(target),
    }


def _evidence(path: Path, *, mutate=None) -> None:
    payload: dict[str, object] = {
        "mode": "BROKER_PREFLIGHT",
        "broker_contacted": True,
        "preflight_passed": True,
        "account": {
            "account_id": EXPECTED_ACCOUNT_ID,
            "broker": "moomoo",
            "environment": "SIM",
        },
        "broker_facts": {
            "account_id": EXPECTED_ACCOUNT_ID,
            "captured_at": NOW.isoformat(),
            "complete": True,
            "execution_evidence_mode": EXPECTED_MODE,
            "execution_evidence_scope": ["CURRENT_ORDER_SNAPSHOTS"],
            "positions": [
                {"instrument_id": target["instrument_id"], "signed_quantity": target["quantity"]}
                for target in TARGETS
            ],
            "open_orders": [],
            "fills": [_broker_fill(target) for target in TARGETS],
        },
        "market_state": {
            "market": "US",
            "captured_at": NOW.isoformat(),
            "complete": True,
            "rows": [
                {"symbol": "AAPL", "market_state": "RTH"},
                {"symbol": "MSFT", "market_state": "RTH"},
                {"symbol": "SPY", "market_state": "RTH"},
                {"symbol": "QQQ", "market_state": "RTH"},
            ],
            "rth": {"observed": True},
        },
    }
    if mutate is not None:
        mutate(payload)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _historical_evidence(path: Path, *, mutate=None) -> None:
    orders = []
    fills = []
    for target in TARGETS:
        raw = {
            "order_id": target["external_order_id"],
            "code": target["raw_code"],
            "trd_side": target["side"],
            "qty": target["quantity"],
            "dealt_qty": target["quantity"],
            "dealt_avg_price": target["price"],
            "order_status": "FILLED_ALL",
            "updated_time": target["filled_at"],
        }
        orders.append(
            {
                "id": f"history:order:{target['external_order_id']}",
                "broker_snapshot_id": "history-snapshot",
                "account_id": EXPECTED_ACCOUNT_ID,
                "external_account_id": "5077333",
                "instrument_id": target["instrument_id"],
                "external_order_id": target["external_order_id"],
                "client_order_id": None,
                "side": target["side"],
                "quantity": target["quantity"],
                "filled_quantity": target["quantity"],
                "status": "FILLED",
                "captured_at": NOW.isoformat(),
                "order_time": target["filled_at"],
                "no_fill_asserted": False,
                "authority": "ADAPTER_ORDER_SNAPSHOT",
                "metadata": {"external_symbol": target["raw_code"], "raw": raw},
            }
        )
        fill = _broker_fill(target)
        fill["metadata"] = {
            **fill["metadata"],
            "source": "history_order_list_query",
            "evidence_scope": "HISTORICAL_ORDER_SNAPSHOTS",
            "raw": raw,
        }
        fills.append(fill)
    payload: dict[str, object] = {
        "mode": "HISTORICAL_PREFLIGHT",
        "broker_contacted": True,
        "historical_preflight_passed": True,
        "account": {
            "account_id": EXPECTED_ACCOUNT_ID,
            "broker": "moomoo",
            "environment": "SIM",
            "external_account_id": "5077333",
        },
        "historical_facts": {
            "account_id": EXPECTED_ACCOUNT_ID,
            "requested_start": "2026-10-07T17:00:00+00:00",
            "requested_end": "2026-10-07T18:00:00+00:00",
            "captured_at": NOW.isoformat(),
            "complete": True,
            "error": None,
            "execution_evidence_mode": EXPECTED_MODE,
            "execution_evidence_scope": ["HISTORICAL_ORDER_SNAPSHOTS"],
            "metadata": {"source": "history_order_list_query"},
            "provenance": {"query": "history_order_list_query"},
            "orders": orders,
            "fills": fills,
        },
    }
    if mutate is not None:
        mutate(payload)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _db(tmp_path: Path, *, mode: str = OLD_MODE) -> Path:
    path = tmp_path / EXPECTED_DB_FILENAME
    schema = Path(__file__).parents[1] / "src" / "trading_core" / "schema.sql"
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA foreign_keys = ON")
    connection.executescript(schema.read_text(encoding="utf-8"))
    now = "2026-10-01T13:25:00+00:00"
    connection.executemany(
        "INSERT INTO core_accounts (id,broker,environment,external_account_id,base_currency,enabled,metadata_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
        [(EXPECTED_ACCOUNT_ID, "moomoo", "SIM", "5077333", "USD", 1, "{}", now, now)],
    )
    connection.executemany(
        "INSERT INTO core_instruments (id,asset_class,symbol,venue,currency,multiplier,tick_size,lot_size,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
        [
            ("stage6:us-aapl", "EQUITY", "AAPL", "US", "USD", "1", "0.01", "1", now, now),
            ("stage6:us-msft", "EQUITY", "MSFT", "US", "USD", "1", "0.01", "1", now, now),
        ],
    )
    connection.executemany(
        "INSERT INTO core_instrument_mappings (id,instrument_id,provider,purpose,external_symbol,external_id,metadata_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
        [
            ("stage6-moomoo-stage6:us-aapl", "stage6:us-aapl", "moomoo", "BROKER", "US.AAPL", None, "{}", now, now),
            ("stage6-moomoo-stage6:us-msft", "stage6:us-msft", "moomoo", "BROKER", "US.MSFT", None, "{}", now, now),
        ],
    )
    connection.execute(
        "INSERT INTO core_strategies (id,name,strategy_type,version,enabled,config_json,metadata_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
        ("stage6-controlled-sim-20261001", "Stage 6", "generic_stat_arb_pilot", "1", 1, "{}", "{}", now, now),
    )
    connection.execute(
        "INSERT INTO core_books (id,name,enabled,metadata_json,created_at,updated_at) VALUES (?,?,?,?,?,?)",
        (EXPECTED_BOOK_ID, "Stage 6 Book A", 1, "{}", now, now),
    )
    connection.execute(
        "INSERT INTO core_order_intents (id,idempotency_key,payload_hash,strategy_id,book_id,account_id,action,status,source_signal_id,execution_policy_json,metadata_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (EXPECTED_SOURCE_INTENT_ID, "oct7-book-a", "hash", "stage6-controlled-sim-20261001", EXPECTED_BOOK_ID, EXPECTED_ACCOUNT_ID, "ENTER", "FILLED", "signal", "{}", "{}", now, now),
    )
    for sequence, target in enumerate(TARGETS):
        connection.execute(
            "INSERT INTO core_order_legs (id,intent_id,sequence,instrument_id,side,quantity,quantity_unit,order_type,limit_price,stop_price,time_in_force,status,cumulative_filled_quantity,average_fill_price,metadata_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (target["order_leg_id"], EXPECTED_SOURCE_INTENT_ID, sequence, target["instrument_id"], target["side"], target["quantity"], "UNITS", "MARKET", None, None, "DAY", "FILLED", target["quantity"], target["price"], "{}", now, now),
        )
        connection.execute(
            "INSERT INTO core_broker_orders (id,order_leg_id,account_id,broker,attempt_number,external_order_id,client_order_id,status,submitted_quantity,submitted_at,updated_at,replaces_broker_order_id,metadata_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (target["broker_order_id"], target["order_leg_id"], EXPECTED_ACCOUNT_ID, "moomoo", 1, target["external_order_id"], f"client-{sequence}", "FILLED", target["quantity"], target["filled_at"], now, None, "{}"),
        )
        connection.execute(
            "INSERT INTO core_fills (id,broker_order_id,order_leg_id,external_fill_id,dedupe_key,quantity,price,fee,fee_currency,filled_at,received_at,evidence_mode,metadata_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (target["fill_id"], target["broker_order_id"], target["order_leg_id"], None, target["evidence_reference"], target["quantity"], target["price"], None, None, target["filled_at"], NOW.isoformat(), mode, json.dumps(_metadata(target))),
        )
    connection.commit()
    connection.close()
    return path


def _mode_rows(path: Path) -> list[tuple[str, str]]:
    connection = sqlite3.connect(path)
    rows = connection.execute("SELECT id, evidence_mode FROM core_fills ORDER BY id").fetchall()
    connection.close()
    return rows


def test_default_dry_run_is_ready_and_does_not_write(tmp_path):
    db_path = _db(tmp_path)
    evidence_path = tmp_path / "broker-preflight.json"
    _evidence(evidence_path)
    before = hashlib.sha256(db_path.read_bytes()).hexdigest()

    assert main(["--db", str(db_path), "--evidence", str(evidence_path)]) == 0
    assert hashlib.sha256(db_path.read_bytes()).hexdigest() == before
    assert _mode_rows(db_path) == [(TARGETS[0]["fill_id"], OLD_MODE), (TARGETS[1]["fill_id"], OLD_MODE)]
    assert not list(tmp_path.glob("*-backup-*.db"))


def test_apply_updates_only_two_modes_and_creates_sqlite_backup(tmp_path):
    db_path = _db(tmp_path)
    evidence_path = tmp_path / "broker-preflight.json"
    report_path = tmp_path / "repair-report.json"
    _evidence(evidence_path)

    assert main([
        "--db", str(db_path), "--evidence", str(evidence_path), "--apply",
        "--confirm", CONFIRMATION, "--report", str(report_path),
    ]) == 0
    assert _mode_rows(db_path) == [(TARGETS[0]["fill_id"], EXPECTED_MODE), (TARGETS[1]["fill_id"], EXPECTED_MODE)]
    assert list(tmp_path.glob("*-backup-*.db"))
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "APPLIED"
    assert report["verification"]["only_target_evidence_mode_changed"] is True
    assert report["writes"]["updates"] == 2


@pytest.mark.parametrize(
    "mutate",
    [
        lambda payload: payload["account"].update(account_id="other-account"),
        lambda payload: payload["broker_facts"].update(complete=False),
        lambda payload: payload["broker_facts"].update(execution_evidence_mode="INDIVIDUAL_DEALS"),
        lambda payload: payload["broker_facts"]["fills"].pop(),
        lambda payload: payload["broker_facts"]["fills"].__getitem__(0).update(price="999"),
        lambda payload: payload["broker_facts"]["fills"].__getitem__(0)["metadata"].update(source="manual"),
    ],
)
def test_invalid_or_insufficient_independent_evidence_blocks_without_write(tmp_path, mutate):
    db_path = _db(tmp_path)
    evidence_path = tmp_path / "broker-preflight.json"
    _evidence(evidence_path, mutate=mutate)
    before = hashlib.sha256(db_path.read_bytes()).hexdigest()

    assert main(["--db", str(db_path), "--evidence", str(evidence_path)]) == 2
    assert hashlib.sha256(db_path.read_bytes()).hexdigest() == before
    assert _mode_rows(db_path) == [(TARGETS[0]["fill_id"], OLD_MODE), (TARGETS[1]["fill_id"], OLD_MODE)]


def test_already_correct_rows_are_refused(tmp_path):
    db_path = _db(tmp_path, mode=EXPECTED_MODE)
    evidence_path = tmp_path / "broker-preflight.json"
    _evidence(evidence_path)

    assert main(["--db", str(db_path), "--evidence", str(evidence_path)]) == 2
    assert _mode_rows(db_path) == [(TARGETS[0]["fill_id"], EXPECTED_MODE), (TARGETS[1]["fill_id"], EXPECTED_MODE)]


def test_active_lifecycle_blocks(tmp_path):
    db_path = _db(tmp_path)
    connection = sqlite3.connect(db_path)
    connection.execute("UPDATE core_order_intents SET status = 'WORKING'")
    connection.commit()
    connection.close()
    evidence_path = tmp_path / "broker-preflight.json"
    _evidence(evidence_path)

    assert main(["--db", str(db_path), "--evidence", str(evidence_path)]) == 2
    assert _mode_rows(db_path) == [(TARGETS[0]["fill_id"], OLD_MODE), (TARGETS[1]["fill_id"], OLD_MODE)]


def test_terminal_rejected_intent_with_unused_planned_leg_does_not_block(tmp_path, capsys):
    db_path = _db(tmp_path)
    connection = sqlite3.connect(db_path)
    now = "2026-10-01T13:25:00+00:00"
    connection.execute(
        "INSERT INTO core_order_intents (id,idempotency_key,payload_hash,strategy_id,book_id,account_id,action,status,source_signal_id,execution_policy_json,metadata_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "historical-rejected-no-attempt",
            "historical-rejected-no-attempt-key",
            "historical-rejected-no-attempt-hash",
            "stage6-controlled-sim-20261001",
            EXPECTED_BOOK_ID,
            EXPECTED_ACCOUNT_ID,
            "ENTER",
            "REJECTED",
            "historical-rejected-no-attempt-signal",
            "{}",
            "{}",
            now,
            now,
        ),
    )
    connection.execute(
        "INSERT INTO core_order_legs (id,intent_id,sequence,instrument_id,side,quantity,quantity_unit,order_type,limit_price,stop_price,time_in_force,status,cumulative_filled_quantity,average_fill_price,metadata_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "historical-rejected-no-attempt-leg",
            "historical-rejected-no-attempt",
            0,
            TARGETS[0]["instrument_id"],
            TARGETS[0]["side"],
            TARGETS[0]["quantity"],
            "UNITS",
            "MARKET",
            None,
            None,
            "DAY",
            "PLANNED",
            "0",
            None,
            "{}",
            now,
            now,
        ),
    )
    connection.commit()
    connection.close()
    evidence_path = tmp_path / "broker-preflight.json"
    _evidence(evidence_path)

    assert main(["--db", str(db_path), "--evidence", str(evidence_path)]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "READY"
    assert all(
        row["intent_id"] != "historical-rejected-no-attempt"
        for row in report["activity_gate"]["checks"]["active_lifecycle_rows"]
    )


def test_expected_nonflat_entry_blocker_is_accepted_but_missing_fill_proof_still_blocks(tmp_path, capsys):
    db_path = _db(tmp_path)
    evidence_path = tmp_path / "broker-preflight.json"

    def mutate(payload):
        payload["preflight_passed"] = False
        payload["broker_preflight_passed"] = False
        payload["stop_reasons"] = [EXPECTED_NONFLAT_ENTRY_BLOCKER]
        payload["broker_facts"]["fills"] = []
        payload["broker_facts"]["fill_count"] = 0

    _evidence(evidence_path, mutate=mutate)
    assert main(["--db", str(db_path), "--evidence", str(evidence_path)]) == 2
    output = json.loads(capsys.readouterr().out)
    assert output["errors"] == [
        "broker evidence fill identities do not exactly match the fixed repair scope: expected ['3449827', '3449828'], observed []"
    ]
    assert _mode_rows(db_path) == [(TARGETS[0]["fill_id"], OLD_MODE), (TARGETS[1]["fill_id"], OLD_MODE)]


def test_atomic_rollback_when_second_update_fails(tmp_path):
    db_path = _db(tmp_path)
    connection = sqlite3.connect(db_path)
    connection.execute(
        """
        CREATE TRIGGER fail_second_repair BEFORE UPDATE OF evidence_mode ON core_fills
        WHEN OLD.id = '7181461e-d5d5-518d-ae49-3f2b73b0dbdb'
        BEGIN SELECT RAISE(ABORT, 'test second update failure'); END
        """
    )
    connection.commit()
    connection.close()
    evidence_path = tmp_path / "broker-preflight.json"
    _evidence(evidence_path)

    assert main([
        "--db", str(db_path), "--evidence", str(evidence_path), "--apply",
        "--confirm", CONFIRMATION, "--report", str(tmp_path / "report.json"),
    ]) == 2
    assert _mode_rows(db_path) == [(TARGETS[0]["fill_id"], OLD_MODE), (TARGETS[1]["fill_id"], OLD_MODE)]


def test_apply_confirmation_is_exact_and_missing_evidence_blocks(tmp_path):
    db_path = _db(tmp_path)
    evidence_path = tmp_path / "missing.json"
    assert main([
        "--db", str(db_path), "--evidence", str(evidence_path), "--apply",
        "--confirm", "wrong", "--report", str(tmp_path / "report.json"),
    ]) == 2
    assert _mode_rows(db_path) == [(TARGETS[0]["fill_id"], OLD_MODE), (TARGETS[1]["fill_id"], OLD_MODE)]


def test_historical_preflight_evidence_is_ready_without_current_market_state(tmp_path):
    db_path = _db(tmp_path)
    evidence_path = tmp_path / "historical-preflight.json"
    _historical_evidence(evidence_path)
    before = hashlib.sha256(db_path.read_bytes()).hexdigest()

    assert main(["--db", str(db_path), "--evidence", str(evidence_path)]) == 0
    assert hashlib.sha256(db_path.read_bytes()).hexdigest() == before
    assert _mode_rows(db_path) == [(TARGETS[0]["fill_id"], OLD_MODE), (TARGETS[1]["fill_id"], OLD_MODE)]


@pytest.mark.parametrize(
    "mutate",
    [
        lambda payload: payload["historical_facts"].update(execution_evidence_scope=["CURRENT_ORDER_SNAPSHOTS"]),
        lambda payload: payload["historical_facts"].update(execution_evidence_mode="INDIVIDUAL_DEALS"),
        lambda payload: payload["historical_facts"].update(metadata={"source": "order_list_query"}),
        lambda payload: payload["historical_facts"]["fills"][0]["metadata"].update(source="order_list_query"),
        lambda payload: payload["historical_facts"]["orders"].pop(),
    ],
)
def test_historical_preflight_wrong_scope_mode_source_or_missing_target_blocks(tmp_path, mutate):
    db_path = _db(tmp_path)
    evidence_path = tmp_path / "historical-preflight.json"
    _historical_evidence(evidence_path, mutate=mutate)
    before = hashlib.sha256(db_path.read_bytes()).hexdigest()

    assert main(["--db", str(db_path), "--evidence", str(evidence_path)]) == 2
    assert hashlib.sha256(db_path.read_bytes()).hexdigest() == before
    assert _mode_rows(db_path) == [(TARGETS[0]["fill_id"], OLD_MODE), (TARGETS[1]["fill_id"], OLD_MODE)]
