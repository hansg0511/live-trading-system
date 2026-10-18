from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal
import sqlite3

import pytest

from src.trading_core.domain import BrokerOrderStatus, IntentStatus, LegStatus
from src.trading_core.repository import SQLiteTradingRepository
from tests.test_generic_oms import (
    FakeAdapter,
    account,
    decision,
    instrument,
    make_intent,
    make_oms,
    ready_repository,
    strategy,
)


NOW = datetime(2026, 10, 12, 12, 0, tzinfo=timezone.utc)


def _prepare(repository: SQLiteTradingRepository, leg_id: str) -> dict:
    return repository.prepare_leg_execution_attempt(
        order_leg_id=leg_id,
        account_id="acct",
        broker="fake",
        submitted_quantity=Decimal("10"),
        now=NOW,
    )


def test_planned_leg_atomically_becomes_executing_with_one_prepared_attempt(tmp_path):
    repository = ready_repository(tmp_path, 1)
    intent = make_intent(1)
    repository.create_intent(intent)

    prepared = _prepare(repository, intent.legs[0].id)

    stored = repository.get_intent(intent.id)
    assert stored is not None
    assert stored["legs"][0]["status"] == LegStatus.EXECUTING.value
    attempts = repository.broker_orders_for_leg(intent.legs[0].id)
    assert len(attempts) == 1
    assert attempts[0]["id"] == prepared["id"]
    assert attempts[0]["status"] == BrokerOrderStatus.PREPARED.value
    assert attempts[0]["attempt_number"] == 1
    assert attempts[0]["client_order_id"] == f"{intent.legs[0].id}:1"


def test_atomic_preparation_failure_rolls_back_leg_and_attempt(tmp_path, monkeypatch):
    repository = ready_repository(tmp_path, 1)
    intent = make_intent(1)
    repository.create_intent(intent)

    def fail_insert(*args, **kwargs):
        raise sqlite3.IntegrityError("injected prepared insert failure")

    monkeypatch.setattr(repository, "_insert_prepared_broker_order_conn", fail_insert)
    with pytest.raises(sqlite3.IntegrityError, match="injected"):
        _prepare(repository, intent.legs[0].id)

    stored = repository.get_intent(intent.id)
    assert stored is not None
    assert stored["legs"][0]["status"] == LegStatus.PLANNED.value
    assert repository.broker_orders_for_leg(intent.legs[0].id) == []


def test_restart_reuses_existing_prepared_attempt_identity(tmp_path):
    repository = ready_repository(tmp_path, 1)
    intent = make_intent(1)
    repository.create_intent(intent)

    first = _prepare(repository, intent.legs[0].id)
    second = _prepare(repository, intent.legs[0].id)

    assert first["created"] is True
    assert second["created"] is False
    assert second["id"] == first["id"]
    assert second["attempt_number"] == first["attempt_number"]
    assert second["client_order_id"] == first["client_order_id"]
    assert len(repository.broker_orders_for_leg(intent.legs[0].id)) == 1


@pytest.mark.parametrize(
    "attempt_status",
    (BrokerOrderStatus.SUBMITTING, BrokerOrderStatus.WORKING, BrokerOrderStatus.UNKNOWN),
)
def test_existing_potentially_dispatched_attempt_is_not_dispatched_again(tmp_path, attempt_status):
    repository = ready_repository(tmp_path, 1)
    adapter = FakeAdapter(repository)
    oms = make_oms(repository, adapter)
    intent = make_intent(1)
    repository.create_intent(intent)
    repository.transition_intent(intent.id, IntentStatus.RISK_APPROVED, now=NOW)
    repository.transition_intent(intent.id, IntentStatus.SUBMITTING, now=NOW)
    prepared = _prepare(repository, intent.legs[0].id)
    assert repository.begin_prepared_broker_order_submission(prepared["id"], now=NOW)
    if attempt_status is not BrokerOrderStatus.SUBMITTING:
        repository.record_submission(
            prepared["id"],
            status=attempt_status,
            external_order_id="already-dispatched" if attempt_status is BrokerOrderStatus.WORKING else None,
            now=NOW,
        )

    result = oms.submit_intent(intent, account=account(), risk_decision=decision(intent.id))

    assert adapter.submit_calls == []
    assert result["legs"][0]["status"] == LegStatus.RECONCILIATION_REQUIRED.value
    attempts = repository.broker_orders_for_leg(intent.legs[0].id)
    assert len(attempts) == 1
    assert attempts[0]["id"] == prepared["id"]
    assert attempts[0]["status"] == attempt_status.value


def test_concurrent_preparation_creates_one_attempt(tmp_path):
    repository = ready_repository(tmp_path, 1)
    intent = make_intent(1)
    repository.create_intent(intent)

    def prepare_from_independent_repository(_: int) -> dict:
        return _prepare(SQLiteTradingRepository(repository.db_path), intent.legs[0].id)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(prepare_from_independent_repository, range(2)))

    assert {item["id"] for item in results} == {results[0]["id"]}
    assert {item["attempt_number"] for item in results} == {1}
    assert {item["client_order_id"] for item in results} == {f"{intent.legs[0].id}:1"}
    assert len(repository.broker_orders_for_leg(intent.legs[0].id)) == 1


def test_legacy_persisted_submitting_leg_migrates_to_executing(tmp_path):
    database = tmp_path / "legacy.db"
    repository = SQLiteTradingRepository(database)
    repository.initialize()
    repository.save_account(account())
    repository.save_strategy(strategy())
    repository.save_instrument(instrument(0))
    intent = make_intent(1)
    repository.create_intent(intent)
    with repository.transaction() as conn:
        conn.execute(
            "UPDATE core_order_legs SET status = 'SUBMITTING' WHERE id = ?",
            (intent.legs[0].id,),
        )

    reopened = SQLiteTradingRepository(database)
    reopened.initialize()
    stored = reopened.get_intent(intent.id)
    assert stored is not None
    assert stored["legs"][0]["status"] == LegStatus.EXECUTING.value
    with reopened.transaction() as conn:
        migration = conn.execute(
            "SELECT version FROM core_schema_migrations WHERE version = 10"
        ).fetchone()
    assert migration is not None
