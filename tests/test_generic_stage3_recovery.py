from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path

import pytest
import pandas as pd

from src.trading_core.domain import (
    ADAPTER_ORDER_SNAPSHOT_AUTHORITY,
    Account,
    AssetClass,
    BrokerOrderSnapshot,
    BrokerOrderStatus,
    BrokerOrderEvent,
    ExecutionPolicy,
    ExecutionEvidenceMode,
    FailurePolicy,
    Instrument,
    IntentAction,
    IntentStatus,
    LegStatus,
    LeggingPolicy,
    Fill,
    OrderIntent,
    OrderLeg,
    PositionSnapshot,
    PartialFillPolicy,
    QuantityUnit,
    RiskDecisionRecord,
    Side,
    Strategy,
    TradingEnvironment,
)
from src.trading_core.oms import GenericOMS, OMSExecutionError
from src.trading_core.ports import BrokerFactSnapshot, BrokerFill, BrokerSubmissionResult
from src.trading_core.provider_payload import is_execution_evidence_key, normalize_provider_key
from src.trading_core.repository import SQLiteTradingRepository


T0 = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)


@dataclass
class Clock:
    value: datetime = T0

    def __call__(self) -> datetime:
        return self.value

    def advance(self, seconds: int) -> None:
        self.value += timedelta(seconds=seconds)


class OpaqueProviderValue:
    """Provider-shaped value the generic OMS cannot safely inspect."""

    pass


@pytest.mark.parametrize(
    ("provider_key", "normalized"),
    (
        ("accountId", "account_id"),
        ("instrumentId", "instrument_id"),
        ("externalOrderId", "external_order_id"),
        ("brokerOrderID", "broker_order_id"),
        ("response-payload", "response_payload"),
    ),
)
def test_provider_key_normalizer_handles_camel_case_and_acronyms(provider_key, normalized):
    assert normalize_provider_key(provider_key) == normalized


@pytest.mark.parametrize("provider_key", ("trade_id", "tradeId", "tradeID", "executionId", "dealId"))
def test_execution_identity_vocabulary_is_centralized(provider_key):
    assert is_execution_evidence_key(provider_key) is True


def account() -> Account:
    return Account(
        id="stage3-acct",
        broker="fake",
        environment=TradingEnvironment.SIM,
        external_account_id="stage3-external-acct",
        base_currency="USD",
        created_at=T0,
        updated_at=T0,
    )


def intent(policy: ExecutionPolicy | None = None) -> OrderIntent:
    return OrderIntent(
        id="stage3-intent",
        idempotency_key="stage3-intent-key",
        strategy_id="stage3-strategy",
        account_id="stage3-acct",
        action=IntentAction.ENTER,
        execution_policy=policy or ExecutionPolicy(),
        legs=tuple(
            OrderLeg(
                id=f"stage3-leg-{index}",
                intent_id="stage3-intent",
                sequence=index,
                instrument_id=f"stage3-instrument-{index}",
                side=Side.BUY if index == 0 else Side.SELL,
                quantity=Decimal("10"),
                quantity_unit=QuantityUnit.UNITS,
                order_type="MARKET",
                status=LegStatus.PLANNED,
                created_at=T0,
                updated_at=T0,
            )
            for index in range(2)
        ),
        created_at=T0,
        updated_at=T0,
    )


def decision() -> RiskDecisionRecord:
    return RiskDecisionRecord(
        id="stage3-risk",
        intent_id="stage3-intent",
        approved=True,
        reason="approved",
        checks={"stage3": True},
        evaluated_at=T0,
    )


class RecoveryAdapter:
    def __init__(self, clock: Clock):
        self.clock = clock
        self.submit_calls: list[str] = []
        self.open_orders: list[BrokerOrderSnapshot] = []
        self.fills: list[BrokerFill] = []
        self.positions: list[PositionSnapshot] = []
        self.raise_fill_history = False
        self.cancel_result: BrokerSubmissionResult | None = None
        self.submit_override = None
        self.raise_on_submit = False

    def submit_order(self, _account: Account, request) -> BrokerSubmissionResult:
        self.submit_calls.append(request.broker_order_id)
        if self.raise_on_submit:
            raise RuntimeError("simulated provider transport failure")
        if self.submit_override is not None:
            override = self.submit_override
            self.submit_override = None
            return override
        external_order_id = f"stage3-external-{len(self.submit_calls)}"
        return BrokerSubmissionResult(
            broker_order_id=request.broker_order_id,
            accepted=True,
            status=BrokerOrderStatus.WORKING,
            external_order_id=external_order_id,
            client_order_id=request.client_order_id,
            submitted_at=self.clock(),
        )

    def get_open_orders(self, _account: Account):
        return tuple(self.open_orders)

    def get_order(self, _account: Account, external_order_id: str):
        return next((item for item in self.open_orders if item.external_order_id == external_order_id), None)

    def get_fills(self, _account: Account, since=None):
        if self.raise_fill_history:
            raise RuntimeError("fill history unavailable")
        return tuple(self.fills)

    def get_positions(self, _account: Account):
        return tuple(self.positions)

    def get_account_facts(self, account_value: Account):
        if self.raise_fill_history:
            return BrokerFactSnapshot(
                account_id=account_value.id,
                captured_at=self.clock(),
                complete=False,
                error="fill history unavailable",
            )
        return BrokerFactSnapshot(
            account_id=account_value.id,
            captured_at=self.clock(),
            complete=True,
            positions=tuple(self.positions),
            open_orders=tuple(self.open_orders),
            fills=tuple(self.fills),
            execution_evidence_mode=ExecutionEvidenceMode.INDIVIDUAL_DEALS,
            execution_evidence_scope=("CURRENT_DEALS",),
        )

    def get_authoritative_account_facts(self, account_value: Account):
        return self.get_account_facts(account_value)

    def cancel_order(self, _account: Account, external_order_id: str) -> BrokerSubmissionResult:
        if self.cancel_result is not None:
            return self.cancel_result
        return BrokerSubmissionResult(
            broker_order_id=f"cancel-{external_order_id}",
            accepted=True,
            status=BrokerOrderStatus.CANCELLED,
            external_order_id=external_order_id,
            cumulative_filled_quantity=Decimal("0"),
            no_fill_asserted=True,
        )


class PositiveSubmissionAdapter(RecoveryAdapter):
    def submit_order(self, _account: Account, request) -> BrokerSubmissionResult:
        self.submit_calls.append(request.broker_order_id)
        return BrokerSubmissionResult(
            broker_order_id=request.broker_order_id,
            accepted=True,
            status=BrokerOrderStatus.WORKING,
            external_order_id=f"positive-submit-{len(self.submit_calls)}",
            client_order_id=request.client_order_id,
            submitted_at=self.clock(),
            raw_payload={"order_status": "SUBMITTED", "dealt_qty": "1"},
        )


def setup(
    tmp_path,
    *,
    policy: ExecutionPolicy | None = None,
    adapter: RecoveryAdapter | None = None,
    expected_initial_status: IntentStatus | None = IntentStatus.WORKING,
    schema_path: Path | None = None,
):
    # Pytest's legacy xunit discovery treats a top-level ``setup`` helper as
    # a module hook and passes the module object once at collection time.
    # Keep that hook harmless; normal tests still pass pathlib.Path here.
    if not isinstance(tmp_path, Path):
        return None
    if schema_path is None:
        repository = SQLiteTradingRepository(tmp_path / "stage3.db")
    else:
        repository = SQLiteTradingRepository(tmp_path / "stage3.db", schema_path=schema_path)
    repository.initialize()
    repository.save_account(account())
    repository.save_strategy(
        Strategy(
            id="stage3-strategy",
            name="stage 3 test",
            strategy_type="test",
            version="1",
            created_at=T0,
            updated_at=T0,
        )
    )
    for index in range(2):
        repository.save_instrument(
            Instrument(
                id=f"stage3-instrument-{index}",
                asset_class=AssetClass.EQUITY,
                symbol=f"S3{index}",
                venue="TEST",
                currency="USD",
                created_at=T0,
                updated_at=T0,
            )
        )
    clock = Clock()
    adapter = adapter or RecoveryAdapter(clock)
    adapter.clock = clock
    oms = GenericOMS(repository, adapter, clock=clock)
    order_intent = intent(policy)
    submitted = oms.submit_intent(order_intent, account=account(), risk_decision=decision())
    if expected_initial_status is not None:
        assert submitted["status"] == expected_initial_status.value
    attempts = [
        rows[0]
        for leg in order_intent.legs
        for rows in [repository.broker_orders_for_leg(leg.id)]
        if rows
    ]
    return repository, adapter, oms, order_intent, attempts, clock


def snapshot(
    attempt: dict,
    leg: OrderLeg,
    *,
    status: BrokerOrderStatus,
    filled: str,
    captured_at: datetime,
    order_time: datetime | None = None,
    external_account_id: str | None = None,
    metadata: dict | None = None,
    authority: str | None = None,
):
    return BrokerOrderSnapshot(
        id=f"snapshot-{attempt['id']}-{status.value}-{filled}",
        broker_snapshot_id=f"broker-snapshot-{attempt['id']}-{status.value}-{filled}",
        account_id="stage3-acct",
        instrument_id=leg.instrument_id,
        external_order_id=attempt["external_order_id"],
        client_order_id=attempt["client_order_id"],
        side=leg.side,
        quantity=Decimal("10"),
        filled_quantity=Decimal(filled),
        status=status,
        captured_at=captured_at,
        order_time=order_time,
        external_account_id=external_account_id,
        no_fill_asserted=(status in {BrokerOrderStatus.REJECTED, BrokerOrderStatus.CANCELLED} and filled == "0"),
        metadata=metadata or {},
        authority=authority,
    )


def fill(attempt: dict, *, key: str, quantity: str = "10") -> BrokerFill:
    leg_id = str(attempt.get("order_leg_id") or "")
    instrument_id = None
    if leg_id.startswith("stage3-leg-"):
        instrument_id = f"stage3-instrument-{leg_id.rsplit('-', 1)[-1]}"
    return BrokerFill(
        external_order_id=attempt["external_order_id"],
        external_fill_id=f"fill-{key}",
        dedupe_key=key,
        quantity=Decimal(quantity),
        price=Decimal("101.25"),
        filled_at=T0,
        received_at=T0,
        instrument_id=instrument_id,
    )


def test_partial_multi_leg_polling_exposes_filled_and_working_state(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    adapter.open_orders = [
        snapshot(attempts[0], order_intent.legs[0], status=BrokerOrderStatus.FILLED, filled="10", captured_at=T0),
        snapshot(attempts[1], order_intent.legs[1], status=BrokerOrderStatus.WORKING, filled="0", captured_at=T0),
    ]
    adapter.fills = [fill(attempts[0], key="leg-0")]
    adapter.positions = [
        PositionSnapshot(
            id="position-0",
            broker_snapshot_id="position-snapshot",
            account_id="stage3-acct",
            instrument_id=order_intent.legs[0].instrument_id,
            signed_quantity=Decimal("10"),
            captured_at=clock(),
        )
    ]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.PARTIALLY_FILLED.value
    assert [leg["status"] for leg in recovered["legs"]] == [
        LegStatus.FILLED.value,
        LegStatus.WORKING.value,
    ]
    status = oms.recovery_status(order_intent.id, account=account())
    assert status["safe_to_submit"] is False
    working_action = next(
        action for action in status["operator_actions"] if action["action_key"] == f"WAIT_FOR_BROKER:{attempts[1]['id']}"
    )
    assert working_action["observed_positions"] == {order_intent.legs[0].instrument_id: "10"}
    assert working_action["remaining_quantities"][order_intent.legs[1].id] == "10"
    assert len(adapter.submit_calls) == 2
    assert repository.fills_for_leg(order_intent.legs[0].id)


def test_delayed_completion_survives_restart_and_duplicate_poll(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    adapter.open_orders = [
        snapshot(attempts[0], order_intent.legs[0], status=BrokerOrderStatus.WORKING, filled="0", captured_at=T0),
        snapshot(attempts[1], order_intent.legs[1], status=BrokerOrderStatus.WORKING, filled="0", captured_at=T0),
    ]
    first = oms.poll_and_recover(account=account())
    assert first[0]["status"] == IntentStatus.WORKING.value

    clock.advance(5)
    adapter.open_orders = [
        snapshot(attempts[0], order_intent.legs[0], status=BrokerOrderStatus.FILLED, filled="10", captured_at=clock()),
        snapshot(attempts[1], order_intent.legs[1], status=BrokerOrderStatus.FILLED, filled="10", captured_at=clock()),
    ]
    adapter.fills = [fill(attempts[0], key="leg-0"), fill(attempts[1], key="leg-1")]

    restarted = GenericOMS(repository, adapter, clock=clock)
    recovered = restarted.poll_and_recover(account=account())
    assert recovered[0]["status"] == IntentStatus.FILLED.value
    assert len(adapter.submit_calls) == 2
    assert len(repository.fills_for_leg(order_intent.legs[0].id)) == 1
    assert len(repository.fills_for_leg(order_intent.legs[1].id)) == 1

    repeated = restarted.recover_intent(order_intent.id, account=account())
    assert repeated["status"] == IntentStatus.FILLED.value
    assert len(repository.fills_for_leg(order_intent.legs[0].id)) == 1
    assert len(repository.fills_for_leg(order_intent.legs[1].id)) == 1


def test_stale_working_order_is_durable_and_never_retried(tmp_path):
    policy = ExecutionPolicy(timeout_seconds=100, stale_order_seconds=10)
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path, policy=policy)
    clock.advance(20)
    adapter.open_orders = [
        snapshot(attempts[0], order_intent.legs[0], status=BrokerOrderStatus.WORKING, filled="0", captured_at=T0),
        snapshot(attempts[1], order_intent.legs[1], status=BrokerOrderStatus.WORKING, filled="0", captured_at=T0),
    ]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    actions = repository.recovery_actions_for_intent(order_intent.id)
    stale = [action for action in actions if action["state"] == "STALE"]
    assert stale and stale[0]["stale"] == 1
    assert any(issue["category"] == "STALE_ORDER" for issue in repository.open_reconciliation_issues("stage3-acct"))
    assert len(adapter.submit_calls) == 2


def test_restart_of_aged_fully_filled_order_does_not_create_timeout_blocker(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    clock.advance(1_000)
    adapter.open_orders = [
        snapshot(
            attempts[0],
            order_intent.legs[0],
            status=BrokerOrderStatus.FILLED,
            filled="10",
            captured_at=clock(),
            order_time=T0,
        ),
        snapshot(
            attempts[1],
            order_intent.legs[1],
            status=BrokerOrderStatus.WORKING,
            filled="0",
            captured_at=clock(),
        ),
    ]
    adapter.fills = [fill(attempts[0], key="aged-terminal-fill")]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["legs"][0]["status"] == LegStatus.FILLED.value
    assert not any(
        issue["category"] in {"ORDER_TIMEOUT", "STALE_ORDER"}
        and issue["entity_key"] == attempts[0]["id"]
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_timeout_is_distinct_from_stale_and_surfaces_operator_steps(tmp_path):
    policy = ExecutionPolicy(timeout_seconds=10, stale_order_seconds=100)
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path, policy=policy)
    clock.advance(11)
    adapter.open_orders = [
        snapshot(attempts[0], order_intent.legs[0], status=BrokerOrderStatus.WORKING, filled="0", captured_at=clock()),
        snapshot(attempts[1], order_intent.legs[1], status=BrokerOrderStatus.WORKING, filled="0", captured_at=clock()),
    ]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    actions = repository.recovery_actions_for_intent(order_intent.id)
    timed_out = [action for action in actions if action["state"] == "TIMED_OUT"]
    assert timed_out and timed_out[0]["timed_out"] == 1
    assert "operator_review" in timed_out[0]["allowed_next_steps"]
    assert any(issue["category"] == "ORDER_TIMEOUT" for issue in repository.open_reconciliation_issues("stage3-acct"))


def test_missing_broker_evidence_fails_closed_without_submit_or_inference(tmp_path):
    repository, adapter, oms, order_intent, attempts, _clock = setup(tmp_path)
    adapter.open_orders = []

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert recovered["legs"][0]["status"] == LegStatus.RECONCILIATION_REQUIRED.value
    assert repository.position_allocations("stage3-acct") == []
    assert len(adapter.submit_calls) == 2
    status = oms.recovery_status(order_intent.id, account=account())
    assert status["safe_to_submit"] is False
    assert any(action["state"] == "RECONCILIATION_REQUIRED" for action in status["operator_actions"])


def test_normalized_events_are_idempotent_and_late_conflicts_remain_reconciliation(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    attempt = attempts[0]
    working_event = BrokerOrderEvent(
        id="event-working",
        broker_order_id=attempt["id"],
        dedupe_key="working-1",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.WORKING,
        external_order_id=attempt["external_order_id"],
    )
    oms._ingest_validated_broker_order_event(working_event, account=account(), fills=())
    oms._ingest_validated_broker_order_event(working_event, account=account(), fills=())
    with repository.transaction() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM core_broker_order_events WHERE broker_order_id = ?",
            (attempt["id"],),
        ).fetchone()[0] == 1

    filled_event = BrokerOrderEvent(
        id="event-filled",
        broker_order_id=attempt["id"],
        dedupe_key="filled-1",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.FILLED,
        external_order_id=attempt["external_order_id"],
        cumulative_filled_quantity=Decimal("10"),
    )
    oms._ingest_validated_broker_order_event(filled_event, account=account(), fills=(fill(attempt, key="event-fill"),))
    oms._ingest_validated_broker_order_event(filled_event, account=account(), fills=(fill(attempt, key="event-fill"),))
    after_duplicate = oms.recovery_status(order_intent.id, account=account())
    assert after_duplicate["intent_status"] == IntentStatus.PARTIALLY_FILLED.value
    assert len(repository.fills_for_leg(order_intent.legs[0].id)) == 1

    late_conflict = BrokerOrderEvent(
        id="event-late-working",
        broker_order_id=attempt["id"],
        dedupe_key="late-working-1",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.WORKING,
        external_order_id=attempt["external_order_id"],
    )
    conflicted = oms._ingest_validated_broker_order_event(late_conflict, account=account(), fills=())
    assert conflicted["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(issue["category"] == "BROKER_STATE_CONFLICT" for issue in conflicted["open_reconciliation_issues"])
    assert len(adapter.submit_calls) == 2


def test_exact_working_full_fill_event_replay_is_noop_after_local_fill(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    event = BrokerOrderEvent(
        id="event-working-full-fill",
        broker_order_id=attempts[0]["id"],
        dedupe_key="working-full-fill",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.WORKING,
        external_order_id=attempts[0]["external_order_id"],
    )
    evidence = fill(attempts[0], key="working-full-fill-deal")

    first = oms._ingest_validated_broker_order_event(event, account=account(), fills=(evidence,))
    replay = oms._ingest_validated_broker_order_event(event, account=account(), fills=(evidence,))

    assert first["legs"][0]["status"] == LegStatus.FILLED.value
    assert replay["legs"][0]["status"] == LegStatus.FILLED.value
    assert replay["intent_status"] == IntentStatus.PARTIALLY_FILLED.value
    assert len(repository.fills_for_broker_order(attempts[0]["id"])) == 1
    assert not any(
        issue["category"] == "BROKER_STATE_CONFLICT"
        for issue in repository.open_reconciliation_issues(account().id)
    )


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("account_id", "wrong-account"),
        ("instrument_id", "wrong-instrument"),
        ("side", Side.SELL),
        ("quantity", Decimal("9")),
        ("client_order_id", "wrong-client"),
    ),
)
def test_poll_snapshot_identity_mismatch_is_durable_and_blocks(tmp_path, field, value):
    repository, adapter, oms, order_intent, attempts, _clock = setup(tmp_path)
    invalid = replace(
        snapshot(
            attempts[0],
            order_intent.legs[0],
            status=BrokerOrderStatus.WORKING,
            filled="0",
            captured_at=T0,
        ),
        **{field: value},
    )
    adapter.open_orders = [
        invalid,
        snapshot(
            attempts[1],
            order_intent.legs[1],
            status=BrokerOrderStatus.WORKING,
            filled="0",
            captured_at=T0,
        ),
    ]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    if field == "account_id":
        assert recovered["legs"][0]["status"] == LegStatus.WORKING.value
        assert repository.broker_orders_for_leg(order_intent.legs[0].id)[0]["status"] == BrokerOrderStatus.WORKING.value
        assert any(
            issue["category"] == "BROKER_FACT_UNAVAILABLE"
            for issue in repository.open_reconciliation_issues(account().id)
        )
    else:
        assert recovered["legs"][0]["status"] == LegStatus.RECONCILIATION_REQUIRED.value
        assert repository.broker_orders_for_leg(order_intent.legs[0].id)[0]["status"] == BrokerOrderStatus.UNKNOWN.value
        assert any(
            issue["category"] == "BROKER_SNAPSHOT_MISMATCH"
            for issue in repository.open_reconciliation_issues(account().id)
        )
        assert any(
            action["action_key"] == f"SNAPSHOT_MISMATCH:{attempts[0]['id']}"
            for action in repository.recovery_actions_for_intent(order_intent.id)
        )
    assert len(adapter.submit_calls) == 2


@pytest.mark.parametrize(
    "status",
    (
        BrokerOrderStatus.WORKING,
        BrokerOrderStatus.REJECTED,
        BrokerOrderStatus.CANCELLED,
    ),
)
def test_poll_positive_snapshot_fill_without_durable_support_fails_closed(tmp_path, status):
    repository, adapter, oms, order_intent, attempts, _clock = setup(tmp_path)
    adapter.open_orders = [
        snapshot(
            attempts[0],
            order_intent.legs[0],
            status=status,
            filled="5",
            captured_at=T0,
        ),
        snapshot(
            attempts[1],
            order_intent.legs[1],
            status=BrokerOrderStatus.WORKING,
            filled="0",
            captured_at=T0,
        ),
    ]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    if status in {BrokerOrderStatus.WORKING, BrokerOrderStatus.REJECTED}:
        assert recovered["legs"][0]["status"] == LegStatus.WORKING.value
        assert any(
            issue["category"] == "BROKER_FACT_UNAVAILABLE"
            for issue in repository.open_reconciliation_issues(account().id)
        )
        assert repository.fills_for_leg(order_intent.legs[0].id) == []
    else:
        assert recovered["legs"][0]["status"] == LegStatus.RECONCILIATION_REQUIRED.value
        assert any(
            issue["category"] == "BROKER_SNAPSHOT_MISMATCH"
            for issue in repository.open_reconciliation_issues(account().id)
        )


def test_recovery_supplied_account_mismatch_is_durable_and_does_not_poll(tmp_path):
    repository, adapter, oms, order_intent, attempts, _clock = setup(tmp_path)
    wrong_account = replace(account(), id="other-account", external_account_id="other-external")

    recovered = oms.recover_intent(order_intent.id, account=wrong_account)

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert len(adapter.open_orders) == 0
    assert any(
        issue["category"] == "ACCOUNT_IDENTITY_MISMATCH"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert any(
        action["action_key"] == "ACCOUNT_IDENTITY_MISMATCH:recover_intent"
        for action in repository.recovery_actions_for_intent(order_intent.id)
    )


def test_recovery_rejects_persisted_attempt_account_mismatch(tmp_path):
    repository, adapter, oms, order_intent, attempts, _clock = setup(tmp_path)
    repository.save_account(replace(account(), id="other-account", external_account_id="other-external"))
    with repository.transaction() as conn:
        conn.execute(
            "UPDATE core_broker_orders SET account_id = ? WHERE id = ?",
            ("other-account", attempts[0]["id"]),
        )
    adapter.open_orders = [
        snapshot(attempts[0], order_intent.legs[0], status=BrokerOrderStatus.WORKING, filled="0", captured_at=T0),
        snapshot(attempts[1], order_intent.legs[1], status=BrokerOrderStatus.WORKING, filled="0", captured_at=T0),
    ]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "ACCOUNT_IDENTITY_MISMATCH"
        and issue["details_json"]
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert repository.position_allocations(account().id) == []


@pytest.mark.parametrize("status", (BrokerOrderStatus.REJECTED, BrokerOrderStatus.CANCELLED))
def test_supported_positive_terminal_snapshot_remains_actionable(tmp_path, status):
    repository, adapter, oms, order_intent, attempts, _clock = setup(tmp_path)
    adapter.open_orders = [
        snapshot(
            attempts[0],
            order_intent.legs[0],
            status=status,
            filled="5",
            captured_at=T0,
        ),
        snapshot(
            attempts[1],
            order_intent.legs[1],
            status=BrokerOrderStatus.WORKING,
            filled="0",
            captured_at=T0,
        ),
    ]
    adapter.fills = [fill(attempts[0], key=f"terminal-{status.value}", quantity="5")]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    if status in {BrokerOrderStatus.WORKING, BrokerOrderStatus.REJECTED}:
        assert any(
            issue["category"] == "BROKER_FACT_UNAVAILABLE"
            for issue in repository.open_reconciliation_issues(account().id)
        )
        assert repository.fills_for_leg(order_intent.legs[0].id) == []
    else:
        assert any(
            issue["category"] == "BROKER_SNAPSHOT_MISMATCH"
            for issue in repository.open_reconciliation_issues(account().id)
        )
        assert any(
            action["action_key"] == f"SNAPSHOT_MISMATCH:{attempts[0]['id']}"
            and action["status"] == "OPEN"
            for action in repository.recovery_actions_for_intent(order_intent.id)
        )


@pytest.mark.parametrize("evidence_kind", ("overfill", "conflicting_dedupe"))
def test_poll_fill_recording_errors_become_durable_reconciliation(tmp_path, evidence_kind):
    repository, adapter, oms, order_intent, attempts, _clock = setup(tmp_path)
    adapter.open_orders = [
        snapshot(
            attempts[0],
            order_intent.legs[0],
            status=BrokerOrderStatus.FILLED,
            filled="10",
            captured_at=T0,
        ),
        snapshot(
            attempts[1],
            order_intent.legs[1],
            status=BrokerOrderStatus.WORKING,
            filled="0",
            captured_at=T0,
        ),
    ]
    if evidence_kind == "overfill":
        adapter.fills = [fill(attempts[0], key="overfill", quantity="11")]
    else:
        first = fill(attempts[0], key="conflicting", quantity="4")
        adapter.fills = [first, replace(first, quantity=Decimal("6"))]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    expected_category = (
        "BROKER_FACT_UNAVAILABLE"
        if evidence_kind == "conflicting_dedupe"
        else "BROKER_FILL_RECORD_FAILED"
    )
    assert any(
        issue["category"] == expected_category
        for issue in repository.open_reconciliation_issues(account().id)
    )
    if evidence_kind == "overfill":
        assert any(
            action["action_key"].startswith("FILL_RECORD_FAILED:")
            for action in repository.recovery_actions_for_intent(order_intent.id)
        )


def test_mixed_terminal_poll_legs_require_reconciliation(tmp_path):
    repository, adapter, oms, order_intent, attempts, _clock = setup(tmp_path)
    adapter.open_orders = [
        snapshot(
            attempts[0],
            order_intent.legs[0],
            status=BrokerOrderStatus.FILLED,
            filled="10",
            captured_at=T0,
        ),
        snapshot(
            attempts[1],
            order_intent.legs[1],
            status=BrokerOrderStatus.CANCELLED,
            filled="0",
            captured_at=T0,
        ),
    ]
    adapter.fills = [fill(attempts[0], key="mixed-filled")]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert [leg["status"] for leg in recovered["legs"]] == [
        LegStatus.FILLED.value,
        LegStatus.CANCELLED.value,
    ]
    assert any(
        issue["category"] == "MIXED_TERMINAL_LEGS"
        for issue in repository.open_reconciliation_issues(account().id)
    )


@pytest.mark.parametrize(
    ("broker_status", "intent_status", "leg_status"),
    (
        (BrokerOrderStatus.REJECTED, IntentStatus.REJECTED, LegStatus.REJECTED),
        (BrokerOrderStatus.CANCELLED, IntentStatus.CANCELLED, LegStatus.CANCELLED),
    ),
)
def test_clean_uniform_terminal_poll_legs_reach_terminal_outcome(
    tmp_path,
    broker_status,
    intent_status,
    leg_status,
):
    repository, adapter, oms, order_intent, attempts, _clock = setup(tmp_path)
    adapter.open_orders = [
        snapshot(
            attempts[index],
            order_intent.legs[index],
            status=broker_status,
            filled="0",
            captured_at=T0,
        )
        for index in range(2)
    ]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == intent_status.value
    assert [leg["status"] for leg in recovered["legs"]] == [leg_status.value, leg_status.value]
    assert repository.open_reconciliation_issues(account().id) == []


def test_clean_rejected_cancelled_mix_reaches_rejected_terminal_outcome(tmp_path):
    repository, adapter, oms, order_intent, attempts, _clock = setup(tmp_path)
    adapter.open_orders = [
        snapshot(
            attempts[0],
            order_intent.legs[0],
            status=BrokerOrderStatus.REJECTED,
            filled="0",
            captured_at=T0,
        ),
        snapshot(
            attempts[1],
            order_intent.legs[1],
            status=BrokerOrderStatus.CANCELLED,
            filled="0",
            captured_at=T0,
        ),
    ]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.REJECTED.value
    assert [leg["status"] for leg in recovered["legs"]] == [
        LegStatus.REJECTED.value,
        LegStatus.CANCELLED.value,
    ]
    assert repository.open_reconciliation_issues(account().id) == []


def test_conflicting_normalized_event_dedupe_is_durable(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    attempt = attempts[0]
    first = BrokerOrderEvent(
        id="event-dedupe-first",
        broker_order_id=attempt["id"],
        dedupe_key="same-event",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.WORKING,
        external_order_id=attempt["external_order_id"],
    )
    oms._ingest_validated_broker_order_event(first, account=account(), fills=())
    conflicting = replace(
        first,
        id="event-dedupe-conflict",
        broker_status=BrokerOrderStatus.FILLED,
        cumulative_filled_quantity=Decimal("10"),
    )

    recovered = oms._ingest_validated_broker_order_event(conflicting, account=account(), fills=())

    assert recovered["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "BROKER_EVENT_RECORD_FAILED"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert any(
        action["action_key"] == f"EVENT_RECORD_FAILED:{attempt['id']}:same-event"
        for action in repository.recovery_actions_for_intent(order_intent.id)
    )


def test_conflicting_normalized_event_external_order_id_is_durable(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    attempt = attempts[0]
    event = BrokerOrderEvent(
        id="event-external-conflict",
        broker_order_id=attempt["id"],
        dedupe_key="external-conflict",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.WORKING,
        external_order_id="unexpected-external-id",
    )

    recovered = oms._ingest_validated_broker_order_event(event, account=account(), fills=())

    assert recovered["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "BROKER_EVENT_EXTERNAL_ID_CONFLICT"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert any(
        action["action_key"] == f"EVENT_EXTERNAL_ID_CONFLICT:{attempt['id']}:external-conflict"
        for action in repository.recovery_actions_for_intent(order_intent.id)
    )


def test_event_positive_cumulative_fill_without_durable_support_fails_closed(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    attempt = attempts[0]
    event = BrokerOrderEvent(
        id="event-working-positive",
        broker_order_id=attempt["id"],
        dedupe_key="working-positive",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.WORKING,
        external_order_id=attempt["external_order_id"],
        cumulative_filled_quantity=Decimal("5"),
    )

    recovered = oms._ingest_validated_broker_order_event(event, account=account(), fills=())

    assert recovered["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "BROKER_EVENT_FILL_MISMATCH"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert any(
        action["action_key"] == f"EVENT_FILL_MISMATCH:{attempt['id']}:working-positive"
        for action in repository.recovery_actions_for_intent(order_intent.id)
    )


def test_event_terminal_positive_fill_remains_actionable_after_fill_arrives(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    attempt = attempts[0]
    event = BrokerOrderEvent(
        id="event-cancel-positive",
        broker_order_id=attempt["id"],
        dedupe_key="cancel-positive",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.CANCELLED,
        external_order_id=attempt["external_order_id"],
        cumulative_filled_quantity=Decimal("5"),
    )

    recovered = oms._ingest_validated_broker_order_event(
        event,
        account=account(),
        fills=(fill(attempt, key="event-cancel-positive-fill", quantity="5"),),
    )

    assert recovered["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "BROKER_EVENT_FILL_MISMATCH"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert any(
        action["action_key"] == f"EVENT_FILL_MISMATCH:{attempt['id']}:cancel-positive"
        and action["status"] == "OPEN"
        for action in repository.recovery_actions_for_intent(order_intent.id)
    )


def test_terminal_zero_fill_event_with_incoming_fill_is_contradiction_without_allocation(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    event = BrokerOrderEvent(
        id="event-cancel-zero-with-fill",
        broker_order_id=attempts[0]["id"],
        dedupe_key="cancel-zero-with-fill",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.CANCELLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=Decimal("0"),
        no_fill_asserted=True,
    )

    recovered = oms._ingest_validated_broker_order_event(
        event,
        account=account(),
        fills=(fill(attempts[0], key="cancel-zero-with-fill-deal"),),
    )

    assert recovered["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert repository.fills_for_broker_order(attempts[0]["id"]) == []
    assert repository.position_allocations(account().id) == []
    assert any(
        issue["category"] == "TERMINAL_FILL_CONTRADICTION"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert any(
        action["action_key"] == f"TERMINAL_FILL_CONTRADICTION:{attempts[0]['id']}:event:cancel-zero-with-fill"
        for action in repository.recovery_actions_for_intent(order_intent.id)
    )


def test_terminal_zero_fill_history_after_cancel_is_contradiction_without_allocation(tmp_path):
    repository, adapter, oms, order_intent, attempts, _clock = setup(tmp_path)
    adapter.open_orders = [
        snapshot(
            attempts[0],
            order_intent.legs[0],
            status=BrokerOrderStatus.CANCELLED,
            filled="0",
            captured_at=T0,
        ),
        snapshot(
            attempts[1],
            order_intent.legs[1],
            status=BrokerOrderStatus.WORKING,
            filled="0",
            captured_at=T0,
        ),
    ]
    adapter.fills = [fill(attempts[0], key="late-history-after-cancel")]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert repository.fills_for_broker_order(attempts[0]["id"]) == []
    assert repository.position_allocations(account().id) == []
    assert any(
        issue["category"] == "TERMINAL_FILL_CONTRADICTION"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert any(
        action["action_key"] == f"TERMINAL_FILL_CONTRADICTION:{attempts[0]['id']}:history:late-history-after-cancel"
        for action in repository.recovery_actions_for_intent(order_intent.id)
    )


def test_terminal_zero_snapshot_contradiction_blocks_late_history_fill_even_after_local_conflict(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    repository.record_submission(
        attempts[0]["id"],
        status=BrokerOrderStatus.FILLED,
        external_order_id=attempts[0]["external_order_id"],
        metadata={"local_status": "FILLED"},
        now=clock(),
    )
    adapter.open_orders = [
        snapshot(
            attempts[0],
            order_intent.legs[0],
            status=BrokerOrderStatus.CANCELLED,
            filled="0",
            captured_at=clock(),
        ),
        snapshot(
            attempts[1],
            order_intent.legs[1],
            status=BrokerOrderStatus.WORKING,
            filled="0",
            captured_at=clock(),
        ),
    ]
    adapter.fills = [fill(attempts[0], key="late-history-after-snapshot-conflict")]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert repository.fills_for_broker_order(attempts[0]["id"]) == []
    assert any(
        issue["category"] == "TERMINAL_FILL_CONTRADICTION"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_event_fill_fingerprint_replay_is_idempotent_but_changed_evidence_is_blocked(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    event = BrokerOrderEvent(
        id="event-fingerprint-terminal",
        broker_order_id=attempts[0]["id"],
        dedupe_key="fingerprint-terminal",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.CANCELLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=Decimal("0"),
        no_fill_asserted=True,
    )

    first = oms._ingest_validated_broker_order_event(event, account=account(), fills=())
    exact = oms._ingest_validated_broker_order_event(event, account=account(), fills=())
    changed = oms._ingest_validated_broker_order_event(
        event,
        account=account(),
        fills=(fill(attempts[0], key="fingerprint-late-fill"),),
    )

    assert first["legs"][0]["status"] == LegStatus.CANCELLED.value
    assert exact["intent_status"] == first["intent_status"]
    assert changed["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert repository.fills_for_broker_order(attempts[0]["id"]) == []
    assert any(
        issue["category"] == "BROKER_EVENT_RECORD_FAILED"
        for issue in repository.open_reconciliation_issues(account().id)
    )


@pytest.mark.parametrize(
    ("field", "value"),
    (("account_id", "wrong-replay-account"), ("evidence_reference", "wrong-replay-evidence")),
)
def test_same_event_key_replay_with_changed_fill_identity_is_durable_conflict(tmp_path, field, value):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    event = BrokerOrderEvent(
        id="event-fill-identity-replay",
        broker_order_id=attempts[0]["id"],
        dedupe_key="fill-identity-replay",
        event_type="DEAL",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.FILLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=Decimal("10"),
    )
    first_fill = fill(attempts[0], key="fill-identity-replay")
    oms._ingest_validated_broker_order_event(event, account=account(), fills=(first_fill,))

    changed_fill = replace(first_fill, **{field: value})
    recovered = oms._ingest_validated_broker_order_event(
        event,
        account=account(),
        fills=(changed_fill,),
    )

    assert recovered["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert len(repository.fills_for_broker_order(attempts[0]["id"])) == 1
    assert any(
        issue["category"] == "BROKER_EVENT_FILL_IDENTITY_CONFLICT"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_opaque_dataframe_fill_metadata_cannot_bypass_provenance_validation(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    event = BrokerOrderEvent(
        id="event-opaque-fill-metadata",
        broker_order_id=attempts[0]["id"],
        dedupe_key="opaque-fill-metadata",
        event_type="DEAL",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.FILLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=Decimal("10"),
    )
    opaque_fill = replace(
        fill(attempts[0], key="opaque-fill-metadata"),
        metadata={"raw": pd.DataFrame([{"dealt_qty": "10", "account_id": "stage3-acct"}])},
    )

    recovered = oms._ingest_validated_broker_order_event(
        event,
        account=account(),
        fills=(opaque_fill,),
    )

    assert recovered["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert repository.fills_for_broker_order(attempts[0]["id"]) == []
    assert any(
        issue["category"] == "BROKER_EVENT_FILL_IDENTITY_CONFLICT"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_wrong_account_event_returns_durable_recovery_state_without_raw_identity_error(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    wrong_account = replace(account(), id="wrong-event-account", external_account_id="wrong-event-external")
    event = BrokerOrderEvent(
        id="event-wrong-account",
        broker_order_id=attempts[0]["id"],
        dedupe_key="wrong-event-account",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.WORKING,
        external_order_id=attempts[0]["external_order_id"],
    )

    recovered = oms.ingest_broker_order_event(event, account=wrong_account)

    assert recovered["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        action["action_key"].startswith("ACCOUNT_IDENTITY_MISMATCH:event:")
        for action in repository.recovery_actions_for_intent(order_intent.id)
    )


def test_public_apply_fill_cannot_attach_new_fill_to_cancelled_attempt(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    repository.record_submission(
        attempts[0]["id"],
        status=BrokerOrderStatus.CANCELLED,
        external_order_id=attempts[0]["external_order_id"],
        metadata={"local_cancel": True},
        now=clock(),
    )
    repository.transition_leg(order_intent.legs[0].id, LegStatus.CANCELLED, now=clock())

    with pytest.raises(OMSExecutionError):
        oms.apply_fill(Fill(
            id="public-late-fill",
            broker_order_id=attempts[0]["id"],
            order_leg_id=order_intent.legs[0].id,
            external_fill_id="public-late-fill",
            dedupe_key="public-late-fill",
            quantity=Decimal("1"),
            price=Decimal("101"),
            filled_at=clock(),
            received_at=clock(),
        ))

    assert repository.fills_for_broker_order(attempts[0]["id"]) == []
    assert any(
        issue["category"] == "TERMINAL_FILL_CONTRADICTION"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_push_fill_ingestion_quarantines_multiple_attempts_before_allocation(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    repository.create_broker_order(
        broker_order_id="push-second-attempt",
        order_leg_id=order_intent.legs[0].id,
        account_id=account().id,
        broker=account().broker,
        attempt_number=None,
        client_order_id=None,
        submitted_quantity=Decimal("10"),
        now=clock(),
    )
    second = repository.get_broker_order("push-second-attempt")
    repository.transition_broker_order("push-second-attempt", BrokerOrderStatus.SUBMITTING, now=clock())
    repository.record_submission(
        "push-second-attempt",
        status=BrokerOrderStatus.WORKING,
        external_order_id="push-second-external",
        now=clock(),
    )
    event = BrokerOrderEvent(
        id="event-push-multiple-attempts",
        broker_order_id="push-second-attempt",
        dedupe_key="push-multiple-attempts",
        event_type="DEAL",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.FILLED,
        external_order_id="push-second-external",
        cumulative_filled_quantity=Decimal("10"),
    )

    recovered = oms._ingest_validated_broker_order_event(
        event,
        account=account(),
        fills=(
            BrokerFill(
                external_order_id="push-second-external",
                external_fill_id="push-second-fill",
                dedupe_key="push-second-fill",
                quantity=Decimal("10"),
                price=Decimal("101"),
                filled_at=clock(),
                received_at=clock(),
            ),
        ),
    )

    assert recovered["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert repository.fills_for_broker_order("push-second-attempt") == []
    assert any(
        issue["category"] == "MULTIPLE_ATTEMPTS_UNSUPPORTED"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_repository_fill_refresh_cannot_promote_intent_while_issue_is_open(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    repository.record_fill(
        Fill(
            id="sticky-first-fill",
            broker_order_id=attempts[0]["id"],
            order_leg_id=order_intent.legs[0].id,
            external_fill_id="sticky-first-fill",
            dedupe_key="sticky-first-fill",
            quantity=Decimal("10"),
            price=Decimal("101"),
            filled_at=clock(),
            received_at=clock(),
            account_id=account().id,
            external_order_id=attempts[0]["external_order_id"],
            evidence_reference="sticky-first-fill",
        ),
        now=clock(),
        _validation_token=repository._fill_validation_capability(),
    )
    oms._require_reconciliation(
        order_intent.id,
        account(),
        category="STICKY_TEST",
        entity_type="INTENT",
        entity_key=order_intent.id,
        details={"test": True},
    )
    repository.record_fill(
        Fill(
            id="sticky-second-fill",
            broker_order_id=attempts[1]["id"],
            order_leg_id=order_intent.legs[1].id,
            external_fill_id="sticky-second-fill",
            dedupe_key="sticky-second-fill",
            quantity=Decimal("10"),
            price=Decimal("201"),
            filled_at=clock(),
            received_at=clock(),
            account_id=account().id,
            external_order_id=attempts[1]["external_order_id"],
            evidence_reference="sticky-second-fill",
        ),
        now=clock(),
        _validation_token=repository._fill_validation_capability(),
    )

    recovered = repository.get_intent(order_intent.id)

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert all(leg["status"] == LegStatus.FILLED.value for leg in recovered["legs"])


def test_complete_intent_quarantines_multiple_attempts_before_completion(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    capability = repository._fill_validation_capability()
    for index, attempt in enumerate(attempts):
        repository.record_fill(
            Fill(
                id=f"complete-multiple-fill-{index}",
                broker_order_id=attempt["id"],
                order_leg_id=order_intent.legs[index].id,
                external_fill_id=f"complete-multiple-fill-{index}",
                dedupe_key=f"complete-multiple-fill-{index}",
                quantity=Decimal("10"),
                price=Decimal("101") if index == 0 else Decimal("201"),
                filled_at=clock(),
                received_at=clock(),
                account_id=account().id,
                external_order_id=attempt["external_order_id"],
                evidence_reference=f"complete-multiple-fill-{index}",
            ),
            now=clock(),
            _validation_token=capability,
        )
    repository.create_broker_order(
        broker_order_id="complete-multiple-second-attempt",
        order_leg_id=order_intent.legs[0].id,
        account_id=account().id,
        broker=account().broker,
        attempt_number=None,
        client_order_id=None,
        submitted_quantity=Decimal("10"),
        now=clock(),
    )

    with pytest.raises(OMSExecutionError):
        oms.complete_intent(order_intent.id)

    assert any(
        issue["category"] == "MULTIPLE_ATTEMPTS_UNSUPPORTED"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert repository.get_intent(order_intent.id)["status"] == IntentStatus.RECONCILIATION_REQUIRED.value


def test_mixed_terminal_event_legs_require_reconciliation(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    filled_event = BrokerOrderEvent(
        id="event-filled-terminal",
        broker_order_id=attempts[0]["id"],
        dedupe_key="filled-terminal",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.FILLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=Decimal("10"),
    )
    oms._ingest_validated_broker_order_event(
        filled_event,
        account=account(),
        fills=(fill(attempts[0], key="event-mixed-filled"),),
    )
    cancelled_event = BrokerOrderEvent(
        id="event-cancelled-terminal",
        broker_order_id=attempts[1]["id"],
        dedupe_key="cancelled-terminal",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.CANCELLED,
        external_order_id=attempts[1]["external_order_id"],
        cumulative_filled_quantity=Decimal("0"),
        no_fill_asserted=True,
    )

    recovered = oms._ingest_validated_broker_order_event(cancelled_event, account=account(), fills=())

    assert recovered["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "MIXED_TERMINAL_LEGS"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_clean_uniform_rejected_events_reach_rejected_intent(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    for index, attempt in enumerate(attempts):
        event = BrokerOrderEvent(
            id=f"event-rejected-{index}",
            broker_order_id=attempt["id"],
            dedupe_key=f"rejected-{index}",
            event_type="ORDER_STATUS",
            event_at=clock(),
            received_at=clock(),
            broker_status=BrokerOrderStatus.REJECTED,
            external_order_id=attempt["external_order_id"],
            cumulative_filled_quantity=Decimal("0"),
            no_fill_asserted=True,
        )
        recovered = oms._ingest_validated_broker_order_event(event, account=account(), fills=())

    assert recovered["intent_status"] == IntentStatus.REJECTED.value
    assert [leg["status"] for leg in recovered["legs"]] == [
        LegStatus.REJECTED.value,
        LegStatus.REJECTED.value,
    ]
    assert repository.open_reconciliation_issues(account().id) == []


def _durable_partial_fill(repository, order_intent, attempts):
    repository.record_fill(
        Fill(
            id="durable-partial",
            broker_order_id=attempts[0]["id"],
            order_leg_id=order_intent.legs[0].id,
            external_fill_id="durable-partial",
            dedupe_key="durable-partial",
            quantity=Decimal("4"),
            price=Decimal("101"),
            filled_at=T0,
            received_at=T0,
            account_id=account().id,
            external_order_id=attempts[0]["external_order_id"],
            evidence_reference="durable-partial",
        ),
        now=T0,
        _validation_token=repository._fill_validation_capability(),
    )


def test_partial_fill_stale_detection_is_durable(tmp_path):
    policy = ExecutionPolicy(timeout_seconds=100, stale_order_seconds=10)
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path, policy=policy)
    _durable_partial_fill(repository, order_intent, attempts)
    clock.advance(20)
    adapter.open_orders = [
        snapshot(
            attempts[0],
            order_intent.legs[0],
            status=BrokerOrderStatus.PARTIALLY_FILLED,
            filled="4",
            captured_at=T0,
        ),
        snapshot(
            attempts[1],
            order_intent.legs[1],
            status=BrokerOrderStatus.WORKING,
            filled="0",
            captured_at=clock(),
        ),
    ]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    stale = [
        action
        for action in repository.recovery_actions_for_intent(order_intent.id)
        if action["action_key"] == f"PARTIAL_STALE:{attempts[0]['id']}"
    ]
    assert stale and stale[0]["stale"] == 1
    assert any(
        issue["category"] == "STALE_ORDER"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_partial_fill_timeout_detection_is_durable(tmp_path):
    policy = ExecutionPolicy(timeout_seconds=10, stale_order_seconds=100)
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path, policy=policy)
    _durable_partial_fill(repository, order_intent, attempts)
    clock.advance(11)
    adapter.open_orders = [
        snapshot(
            attempts[0],
            order_intent.legs[0],
            status=BrokerOrderStatus.PARTIALLY_FILLED,
            filled="4",
            captured_at=clock(),
        ),
        snapshot(
            attempts[1],
            order_intent.legs[1],
            status=BrokerOrderStatus.WORKING,
            filled="0",
            captured_at=clock(),
        ),
    ]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    timed_out = [
        action
        for action in repository.recovery_actions_for_intent(order_intent.id)
        if action["action_key"] == f"PARTIAL_TIMED_OUT:{attempts[0]['id']}"
    ]
    assert timed_out and timed_out[0]["timed_out"] == 1
    assert any(
        issue["category"] == "ORDER_TIMEOUT"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_stale_age_uses_broker_order_timestamp_when_query_capture_is_fresh(tmp_path):
    policy = ExecutionPolicy(timeout_seconds=100, stale_order_seconds=10)
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path, policy=policy)
    clock.advance(20)
    adapter.open_orders = [
        snapshot(
            attempts[0],
            order_intent.legs[0],
            status=BrokerOrderStatus.WORKING,
            filled="0",
            captured_at=clock(),
            order_time=T0,
        ),
        snapshot(
            attempts[1],
            order_intent.legs[1],
            status=BrokerOrderStatus.WORKING,
            filled="0",
            captured_at=clock(),
        ),
    ]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    stale = [
        action
        for action in repository.recovery_actions_for_intent(order_intent.id)
        if action["action_key"] == f"STALE:{attempts[0]['id']}"
    ]
    assert stale and stale[0]["metadata"]["stale_time_source"] == "broker_order_time"


@pytest.mark.parametrize("policy_value", (PartialFillPolicy.ACCEPT_PARTIAL, PartialFillPolicy.CANCEL_REMAINDER))
def test_unimplemented_partial_fill_policies_are_rejected(policy_value):
    with pytest.raises(ValueError, match="not implemented"):
        ExecutionPolicy(partial_fill_policy=policy_value)


@pytest.mark.parametrize(
    "kwargs",
    (
        {"legging_policy": LeggingPolicy.PARALLEL},
        {"legging_policy": LeggingPolicy.BEST_EFFORT},
        {"failure_policy": FailurePolicy.UNWIND_FILLED_LEGS},
        {"max_attempts": 2},
        {"require_native_atomicity": True},
        {"required_capabilities": frozenset({"native_atomicity"})},
    ),
)
def test_unsupported_stage3_execution_policy_modes_are_rejected(kwargs):
    with pytest.raises(ValueError, match="not implemented"):
        ExecutionPolicy(**kwargs)


def test_clean_cancellation_result_is_durable_and_terminal(tmp_path):
    repository, adapter, oms, order_intent, attempts, _clock = setup(tmp_path)

    oms._cancel_working_attempts(order_intent, account())

    recovered = repository.get_intent(order_intent.id)
    assert recovered["status"] == IntentStatus.CANCELLED.value
    assert [leg["status"] for leg in recovered["legs"]] == [
        LegStatus.CANCELLED.value,
        LegStatus.CANCELLED.value,
    ]
    assert all(
        attempt["status"] == BrokerOrderStatus.CANCELLED.value
        for leg in order_intent.legs
        for attempt in repository.broker_orders_for_leg(leg.id)
    )
    actions = repository.recovery_actions_for_intent(order_intent.id)
    assert all(
        action["status"] == "RESOLVED"
        for action in actions
        if action["action_key"].startswith("CANCEL_RESULT:")
    )
    assert repository.open_reconciliation_issues(account().id) == []


def test_ambiguous_cancellation_is_durable_and_blocks(tmp_path):
    repository, adapter, oms, order_intent, attempts, _clock = setup(tmp_path)
    adapter.cancel_result = BrokerSubmissionResult(
        broker_order_id=attempts[0]["id"],
        accepted=False,
        status=BrokerOrderStatus.REJECTED,
        external_order_id=attempts[0]["external_order_id"],
        error_message="cancel rejected",
    )

    oms._cancel_working_attempts(order_intent, account())

    recovered = repository.get_intent(order_intent.id)
    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "CANCEL_AMBIGUITY"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert any(
        action["action_key"] == f"CANCEL_AMBIGUITY:{attempts[0]['id']}"
        and action["status"] == "OPEN"
        for action in repository.recovery_actions_for_intent(order_intent.id)
    )
    assert adapter.submit_calls == [attempt["id"] for attempt in attempts]


def test_cancelled_without_authoritative_zero_fill_is_not_clean(tmp_path):
    repository, adapter, oms, order_intent, attempts, _clock = setup(tmp_path)
    adapter.cancel_result = BrokerSubmissionResult(
        broker_order_id=attempts[0]["id"],
        accepted=True,
        status=BrokerOrderStatus.CANCELLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=None,
        no_fill_asserted=False,
    )

    oms._cancel_working_attempts(order_intent, account())

    assert any(
        issue["category"] == "CANCEL_AMBIGUITY"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert repository.get_intent(order_intent.id)["status"] == IntentStatus.RECONCILIATION_REQUIRED.value


def test_ambiguous_no_fill_assertion_is_rejected_by_normalized_contract():
    with pytest.raises(ValueError, match="no_fill_asserted cannot be asserted"):
        BrokerSubmissionResult(
            broker_order_id="ambiguous-cancel",
            accepted=True,
            status=BrokerOrderStatus.CANCELLED,
            ambiguous=True,
            cumulative_filled_quantity=Decimal("0"),
            no_fill_asserted=True,
        )


def test_positive_raw_submission_evidence_blocks_later_leg_submission(tmp_path):
    adapter = PositiveSubmissionAdapter(Clock())
    repository, _adapter, oms, order_intent, attempts, _clock = setup(
        tmp_path,
        adapter=adapter,
        expected_initial_status=None,
    )

    recovered = repository.get_intent(order_intent.id)

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert len(adapter.submit_calls) == 1
    assert recovered["legs"][0]["status"] == LegStatus.RECONCILIATION_REQUIRED.value
    assert recovered["legs"][1]["status"] == LegStatus.PLANNED.value
    assert any(
        issue["category"] == "BROKER_SUBMISSION_FILL_EVIDENCE"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert any(
        action["action_key"] == f"SUBMISSION_FILL_EVIDENCE:{attempts[0]['id']}"
        for action in repository.recovery_actions_for_intent(order_intent.id)
    )
    assert repository.position_allocations(account().id) == []


def test_raw_cancellation_fill_evidence_is_actionable(tmp_path):
    repository, adapter, oms, order_intent, attempts, _clock = setup(tmp_path)
    adapter.cancel_result = BrokerSubmissionResult(
        broker_order_id=attempts[0]["id"],
        accepted=True,
        status=BrokerOrderStatus.CANCELLED,
        external_order_id=attempts[0]["external_order_id"],
        raw_payload={"deal_list": [{"dealt_qty": "1", "dealt_avg_price": "101.2"}]},
    )

    oms._cancel_working_attempts(order_intent, account())

    recovered = repository.get_intent(order_intent.id)
    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "CANCEL_FILL_EVIDENCE"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert any(
        action["action_key"] == f"CANCEL_FILL_EVIDENCE:{attempts[0]['id']}"
        for action in repository.recovery_actions_for_intent(order_intent.id)
    )
    assert repository.position_allocations(account().id) == []


def test_nested_order_lifecycle_evidence_blocks_no_submit_rejection_and_later_leg(tmp_path):
    class NestedLifecycleAdapter(RecoveryAdapter):
        def submit_order(self, _account: Account, request) -> BrokerSubmissionResult:
            self.submit_calls.append(request.broker_order_id)
            return BrokerSubmissionResult(
                broker_order_id=request.broker_order_id,
                accepted=False,
                status=BrokerOrderStatus.REJECTED,
                error_code="BROKER_REJECTED",
                cumulative_filled_quantity=Decimal("0"),
                no_submit_asserted=True,
                no_fill_asserted=True,
                raw_payload={
                    "response": {
                        "diagnostic": {
                            "orders": [{"id": "nested-order", "order_status": "FILLED"}]
                        }
                    }
                },
            )

    adapter = NestedLifecycleAdapter(Clock())
    repository, _adapter, oms, order_intent, attempts, _clock = setup(
        tmp_path,
        adapter=adapter,
        expected_initial_status=None,
    )

    assert len(adapter.submit_calls) == 1
    assert repository.get_intent(order_intent.id)["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert repository.get_intent(order_intent.id)["legs"][1]["status"] == LegStatus.PLANNED.value
    assert any(
        issue["category"] == "SUBMISSION_AMBIGUITY"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_nested_order_lifecycle_evidence_blocks_clean_cancel(tmp_path):
    repository, adapter, oms, order_intent, attempts, _clock = setup(tmp_path)
    adapter.cancel_result = BrokerSubmissionResult(
        broker_order_id=attempts[0]["id"],
        accepted=True,
        status=BrokerOrderStatus.CANCELLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=Decimal("0"),
        no_fill_asserted=True,
        raw_payload={"response": {"orders": [{"id": "nested-order", "order_status": "FILLED"}]}},
    )

    oms._cancel_working_attempts(order_intent, account())

    assert repository.get_intent(order_intent.id)["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "CANCEL_FILL_EVIDENCE"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_adapter_authoritative_zero_fill_row_allows_no_fill_but_generic_identity_does_not(tmp_path):
    _repository, _adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    authoritative = snapshot(
        attempts[0],
        order_intent.legs[0],
        status=BrokerOrderStatus.CANCELLED,
        filled="0",
        captured_at=clock(),
        metadata={
            "raw": {
                "order_id": attempts[0]["external_order_id"],
                "code": "S30",
                "trd_side": "BUY",
                "qty": "10",
                "dealt_qty": "0",
                "order_status": "CANCELLED_ALL",
            }
        },
        authority=ADAPTER_ORDER_SNAPSHOT_AUTHORITY,
    )
    generic = replace(authoritative, authority=None)

    assert oms._snapshot_no_fill_is_authoritative(authoritative) is True
    assert oms._snapshot_no_fill_is_authoritative(generic) is False


def test_authority_fast_paths_reject_conflicting_sibling_provider_payloads(tmp_path):
    repository, _adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    raw = {
        "order_id": attempts[0]["external_order_id"],
        "code": "S30",
        "trd_side": "BUY",
        "qty": "10",
        "dealt_qty": "0",
        "order_status": "CANCELLED_ALL",
    }
    observed = snapshot(
        attempts[0],
        order_intent.legs[0],
        status=BrokerOrderStatus.CANCELLED,
        filled="0",
        captured_at=clock(),
        metadata={"raw": raw, "response": {"dealt_qty": "1"}},
        authority=ADAPTER_ORDER_SNAPSHOT_AUTHORITY,
    )
    assert oms._snapshot_no_fill_is_authoritative(observed) is False

    event = BrokerOrderEvent(
        id="sibling-authority-event",
        broker_order_id=attempts[0]["id"],
        dedupe_key="sibling-authority-event",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.CANCELLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=Decimal("0"),
        no_fill_asserted=True,
        authority=ADAPTER_ORDER_SNAPSHOT_AUTHORITY,
        metadata={"authoritative_snapshot": {"raw": raw}, "payload": {"status": "FILLED"}},
    )
    assert oms._event_no_fill_is_authoritative(
        event,
        expected_quantity=Decimal("10"),
        expected_instrument_id=order_intent.legs[0].instrument_id,
    ) is False

    command = BrokerSubmissionResult(
        broker_order_id=attempts[0]["id"],
        accepted=True,
        status=BrokerOrderStatus.CANCELLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=Decimal("0"),
        no_fill_asserted=True,
        authority=ADAPTER_ORDER_SNAPSHOT_AUTHORITY,
        submitted_quantity=Decimal("10"),
        instrument_id=order_intent.legs[0].instrument_id,
        raw_payload={"response": raw, "payload": {"dealt_qty": "1"}},
    )
    assert oms._has_cancel_evidence(
        command,
        expected_quantity=Decimal("10"),
        expected_instrument_id=order_intent.legs[0].instrument_id,
    ) is True


def test_command_authority_scans_all_response_aliases(tmp_path):
    repository, adapter, oms, order_intent, attempts, _clock = setup(tmp_path)
    raw = {
        "order_id": attempts[0]["external_order_id"],
        "order_status": "CANCELLED_ALL",
        "code": "S30",
        "instrument_id": order_intent.legs[0].instrument_id,
        "qty": "10",
        "dealt_qty": "0",
    }
    adapter.cancel_result = BrokerSubmissionResult(
        broker_order_id=attempts[0]["id"],
        accepted=True,
        status=BrokerOrderStatus.CANCELLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=Decimal("0"),
        no_fill_asserted=True,
        authority=ADAPTER_ORDER_SNAPSHOT_AUTHORITY,
        submitted_quantity=Decimal("10"),
        instrument_id=order_intent.legs[0].instrument_id,
        raw_payload={
            "response": raw,
            # A case-only duplicate must not be skipped in favor of the
            # lowercase branch; this sibling proves possible fill evidence.
            "Response": {"dealt_qty": "1"},
        },
    )

    assert oms._has_cancel_evidence(
        adapter.cancel_result,
        expected_quantity=Decimal("10"),
        expected_instrument_id=order_intent.legs[0].instrument_id,
    ) is True
    oms._cancel_working_attempts(order_intent, account())

    recovered = repository.get_intent(order_intent.id)
    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert recovered["legs"][0]["status"] != LegStatus.CANCELLED.value
    assert any(
        issue["category"] == "CANCEL_FILL_EVIDENCE"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_public_authoritative_event_rejects_case_insensitive_sibling_fill_payload(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    raw = {
        "order_id": attempts[0]["external_order_id"],
        "order_status": "CANCELLED_ALL",
        "code": "S30",
        "qty": "10",
        "dealt_qty": "0",
    }
    adapter.open_orders = [
        snapshot(
            attempts[0],
            order_intent.legs[0],
            status=BrokerOrderStatus.CANCELLED,
            filled="0",
            captured_at=clock(),
            authority=ADAPTER_ORDER_SNAPSHOT_AUTHORITY,
            metadata={"RAW": raw, "Response": {"dealt_qty": "1"}},
        )
    ]
    event = BrokerOrderEvent(
        id="public-authority-sibling-fill",
        broker_order_id=attempts[0]["id"],
        dedupe_key="public-authority-sibling-fill",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.CANCELLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=Decimal("0"),
        no_fill_asserted=True,
    )

    recovered = oms.ingest_broker_order_event(event, account=account())

    assert recovered["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert repository.fills_for_broker_order(attempts[0]["id"]) == []
    assert any(
        issue["category"] == "BROKER_EVENT_STATUS_UNAVAILABLE"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_public_event_reenvelope_validates_event_sibling_before_authority(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    raw = {
        "order_id": attempts[0]["external_order_id"],
        "order_status": "CANCELLED_ALL",
        "code": "S30",
        "qty": "10",
        "dealt_qty": "0",
    }
    adapter.open_orders = [
        snapshot(
            attempts[0],
            order_intent.legs[0],
            status=BrokerOrderStatus.CANCELLED,
            filled="0",
            captured_at=clock(),
            authority=ADAPTER_ORDER_SNAPSHOT_AUTHORITY,
            metadata={"raw": raw},
        )
    ]
    event = BrokerOrderEvent(
        id="public-reenvelope-sibling-fill",
        broker_order_id=attempts[0]["id"],
        dedupe_key="public-reenvelope-sibling-fill",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.CANCELLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=Decimal("0"),
        no_fill_asserted=True,
        metadata={"RAW": raw, "Response": {"dealt_qty": "1"}},
    )

    recovered = oms.ingest_broker_order_event(event, account=account())

    assert recovered["legs"][0]["status"] == LegStatus.WORKING.value
    assert recovered["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert repository.fills_for_broker_order(attempts[0]["id"]) == []
    assert any(
        issue["category"] == "BROKER_EVENT_STATUS_UNAVAILABLE"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_serialized_provider_payload_is_parsed_for_fill_and_account_validation(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    serialized_foreign = json.dumps(
        {
            "account_id": "foreign-account",
            "instrument_id": order_intent.legs[1].instrument_id,
            "dealt_qty": "10",
        }
    )
    event = BrokerOrderEvent(
        id="serialized-foreign-fill",
        broker_order_id=attempts[0]["id"],
        dedupe_key="serialized-foreign-fill",
        event_type="DEAL",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.FILLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=Decimal("10"),
    )
    serialized_fill = replace(
        fill(attempts[0], key="serialized-foreign-fill"),
        account_id=account().id,
        evidence_reference="serialized-foreign-fill",
        metadata={"raw": serialized_foreign},
    )

    recovered = oms._ingest_validated_broker_order_event(
        event,
        account=account(),
        fills=(serialized_fill,),
    )

    assert recovered["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert repository.fills_for_broker_order(attempts[0]["id"]) == []
    assert any(
        issue["category"] in {
            "BROKER_EVENT_ACCOUNT_ID_CONFLICT",
            "BROKER_EVENT_FILL_IDENTITY_CONFLICT",
            "BROKER_STATE_CONFLICT",
        }
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_camel_case_nested_fill_metadata_blocks_foreign_identity_before_allocation(tmp_path):
    repository, _adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    event = BrokerOrderEvent(
        id="camel-case-foreign-fill",
        broker_order_id=attempts[0]["id"],
        dedupe_key="camel-case-foreign-fill",
        event_type="DEAL",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.FILLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=Decimal("10"),
    )
    camel_case_fill = replace(
        fill(attempts[0], key="camel-case-foreign-fill"),
        account_id=account().id,
        instrument_id=order_intent.legs[0].instrument_id,
        evidence_reference="camel-case-foreign-fill",
        metadata={
            "providerEnvelope": {
                "accountId": "foreign-account",
                "instrumentId": order_intent.legs[1].instrument_id,
                "dealtQty": "10",
            }
        },
    )

    recovered = oms._ingest_validated_broker_order_event(
        event,
        account=account(),
        fills=(camel_case_fill,),
    )

    assert recovered["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert repository.fills_for_broker_order(attempts[0]["id"]) == []
    assert repository.position_allocations(account().id) == []
    assert any(
        issue["category"]
        in {"BROKER_EVENT_ACCOUNT_ID_CONFLICT", "BROKER_EVENT_FILL_IDENTITY_CONFLICT"}
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_account_fact_gate_blocks_contradictory_duplicate_order_rows(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    adapter.open_orders = [
        snapshot(
            attempts[0],
            order_intent.legs[0],
            status=BrokerOrderStatus.WORKING,
            filled="0",
            captured_at=clock(),
        ),
        snapshot(
            attempts[0],
            order_intent.legs[0],
            status=BrokerOrderStatus.CANCELLED,
            filled="0",
            captured_at=clock(),
        ),
    ]

    assert oms._account_wide_broker_fact_gate(order_intent.id, account()) is False
    actions = repository.recovery_actions_for_intent(order_intent.id)
    assert any(
        action["state"] == "RECONCILIATION_REQUIRED"
        and "duplicate_broker_order_fact" in json.dumps(action["metadata"], sort_keys=True)
        for action in actions
    )


def test_account_fact_gate_prefers_dedicated_authoritative_reader(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    manual_order = replace(
        snapshot(
            attempts[0],
            order_intent.legs[0],
            status=BrokerOrderStatus.WORKING,
            filled="0",
            captured_at=clock(),
        ),
        external_order_id="manual-foreign-to-oms",
    )
    calls: list[str] = []

    def authoritative(account_value: Account) -> BrokerFactSnapshot:
        calls.append(account_value.id)
        return BrokerFactSnapshot(
            account_id=account_value.id,
            captured_at=clock(),
            complete=True,
            open_orders=(manual_order,),
        )

    adapter.get_account_facts = lambda _account: (_ for _ in ()).throw(
        AssertionError("stale compatibility reader must not be used by the safety gate")
    )
    adapter.get_authoritative_account_facts = authoritative

    assert oms._account_wide_broker_fact_gate(order_intent.id, account()) is False
    assert calls == [account().id]
    assert any(
        action["state"] == "RECONCILIATION_REQUIRED"
        and any(
            item.get("kind") == "unattributed_open_order"
            for item in (action["metadata"].get("blockers") or [])
        )
        for action in repository.recovery_actions_for_intent(order_intent.id)
    )


def test_account_fact_gate_dedupes_exact_duplicate_order_rows(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    raw = {
        "order_id": attempts[0]["external_order_id"],
        "order_status": "CANCELLED_ALL",
        "code": "S30",
        "qty": "10",
        "dealt_qty": "0",
    }
    cancelled = snapshot(
        attempts[0],
        order_intent.legs[0],
        status=BrokerOrderStatus.CANCELLED,
        filled="0",
        captured_at=clock(),
        authority=ADAPTER_ORDER_SNAPSHOT_AUTHORITY,
        metadata={"raw": raw, "external_symbol": "S30"},
    )
    adapter.open_orders = [cancelled, cancelled]

    assert oms._account_wide_broker_fact_gate(order_intent.id, account()) is True


def test_poll_recovery_quarantines_contradictory_duplicate_order_rows(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    adapter.open_orders = [
        snapshot(
            attempts[0],
            order_intent.legs[0],
            status=BrokerOrderStatus.WORKING,
            filled="0",
            captured_at=clock(),
        ),
        snapshot(
            attempts[0],
            order_intent.legs[0],
            status=BrokerOrderStatus.CANCELLED,
            filled="0",
            captured_at=clock(),
        ),
        snapshot(
            attempts[1],
            order_intent.legs[1],
            status=BrokerOrderStatus.WORKING,
            filled="0",
            captured_at=clock(),
        ),
    ]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "BROKER_FACT_UNAVAILABLE"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_poll_recovery_dedupes_exact_duplicate_order_rows(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    first = snapshot(
        attempts[0],
        order_intent.legs[0],
        status=BrokerOrderStatus.FILLED,
        filled="10",
        captured_at=clock(),
    )
    second = snapshot(
        attempts[1],
        order_intent.legs[1],
        status=BrokerOrderStatus.FILLED,
        filled="10",
        captured_at=clock(),
    )
    adapter.open_orders = [first, first, second]
    adapter.fills = [fill(attempts[0], key="poll-duplicate-0"), fill(attempts[1], key="poll-duplicate-1")]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.FILLED.value
    assert len(repository.fills_for_leg(order_intent.legs[0].id)) == 1
    assert len(repository.fills_for_leg(order_intent.legs[1].id)) == 1


def test_same_account_duplicate_external_order_claim_blocks_normalized_fill_before_recording(tmp_path, monkeypatch):
    repository, _adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    current = attempts[0]
    duplicate = dict(current)
    duplicate["id"] = "legacy-same-account-duplicate"

    def claims(_external_order_id):
        return [current, duplicate]

    monkeypatch.setattr(repository, "broker_orders_for_external_order_id", claims)
    with pytest.raises(ValueError, match="duplicate broker order claims"):
        oms._apply_normalized_fill(
            account=account(),
            intent_id=order_intent.id,
            broker_order_id=current["id"],
            broker_fill=fill(current, key="duplicate-claim-fill"),
        )

    assert repository.fills_for_broker_order(current["id"]) == []
    assert any(
        issue["category"] == "DUPLICATE_BROKER_ORDER_CLAIM"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert any(
        action["action_key"] == f"DUPLICATE_BROKER_ORDER_CLAIM:{current['external_order_id']}"
        for action in repository.open_recovery_actions(account().id)
    )


def test_legacy_same_account_duplicate_external_order_claim_is_enforced_before_fill(tmp_path):
    """A pre-constraint same-account duplicate cannot be matched or allocated."""
    schema_source = Path(__file__).resolve().parents[1] / "src" / "trading_core" / "schema.sql"
    legacy_schema = tmp_path / "legacy-stage3-schema.sql"
    legacy_schema.write_text(
        schema_source.read_text(encoding="utf-8").replace(
            "    UNIQUE (account_id, external_order_id),\n", ""
        ),
        encoding="utf-8",
    )
    repository, adapter, oms, order_intent, attempts, clock = setup(
        tmp_path,
        schema_path=legacy_schema,
    )

    # Simulate an old database in which a second same-account attempt claimed
    # the provider order before the current schema's uniqueness constraint was
    # introduced.
    repository.record_submission(
        attempts[1]["id"],
        status=BrokerOrderStatus.WORKING,
        external_order_id=attempts[0]["external_order_id"],
        now=clock(),
    )
    repository.audit_legacy_duplicate_broker_orders()

    with pytest.raises(ValueError, match="duplicate broker order claims"):
        oms._apply_normalized_fill(
            account=account(),
            intent_id=order_intent.id,
            broker_order_id=attempts[0]["id"],
            broker_fill=fill(attempts[0], key="legacy-same-account-duplicate"),
        )

    assert repository.fills_for_broker_order(attempts[0]["id"]) == []
    assert any(
        issue["category"] == "LEGACY_DUPLICATE_BROKER_ORDER"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert any(
        action["action_key"]
        == f"LEGACY_DUPLICATE_BROKER_ORDER:{attempts[0]['external_order_id']}"
        for action in repository.recovery_actions_for_intent(order_intent.id)
    )
def test_fill_instrument_provenance_mismatch_blocks_event_allocation(tmp_path):
    repository, _adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    event = BrokerOrderEvent(
        id="foreign-instrument-fill-event",
        broker_order_id=attempts[0]["id"],
        dedupe_key="foreign-instrument-fill-event",
        event_type="DEAL",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.FILLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=Decimal("10"),
    )
    foreign_fill = replace(
        fill(attempts[0], key="foreign-instrument-fill"),
        instrument_id=order_intent.legs[1].instrument_id,
    )

    recovered = oms._ingest_validated_broker_order_event(
        event,
        account=account(),
        fills=(foreign_fill,),
    )

    assert recovered["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert repository.fills_for_broker_order(attempts[0]["id"]) == []
    assert any(
        issue["category"] == "BROKER_EVENT_FILL_IDENTITY_CONFLICT"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_account_fact_gate_rejects_status_quantity_contradiction(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    adapter.open_orders = [
        snapshot(
            attempts[0],
            order_intent.legs[0],
            status=BrokerOrderStatus.FILLED,
            filled="0",
            captured_at=clock(),
        )
    ]

    assert oms._account_wide_broker_fact_gate(order_intent.id, account()) is False
    assert any(
        action["state"] == "RECONCILIATION_REQUIRED"
        and "filled_status_requires_complete_fill" in json.dumps(action["metadata"], sort_keys=True)
        for action in repository.recovery_actions_for_intent(order_intent.id)
    )


def test_opaque_cancellation_payload_cannot_resolve_clean_no_fill(tmp_path):
    repository, adapter, oms, order_intent, attempts, _clock = setup(tmp_path)
    adapter.cancel_result = BrokerSubmissionResult(
        broker_order_id=attempts[0]["id"],
        accepted=True,
        status=BrokerOrderStatus.CANCELLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=Decimal("0"),
        no_fill_asserted=True,
        raw_payload={"diagnostic": OpaqueProviderValue()},
    )

    oms._cancel_working_attempts(order_intent, account())

    recovered = repository.get_intent(order_intent.id)
    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "CANCEL_FILL_EVIDENCE"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert any(
        action["action_key"] == f"CANCEL_FILL_EVIDENCE:{attempts[0]['id']}"
        and action["status"] == "OPEN"
        for action in repository.recovery_actions_for_intent(order_intent.id)
    )
    assert all(
        leg["status"] != LegStatus.CANCELLED.value
        for leg in recovered["legs"][:1]
    )


def test_unknown_raw_fill_metadata_on_zero_terminal_snapshot_is_not_clean(tmp_path):
    repository, adapter, oms, order_intent, attempts, _clock = setup(tmp_path)
    adapter.open_orders = [
        snapshot(
            attempts[0],
            order_intent.legs[0],
            status=BrokerOrderStatus.CANCELLED,
            filled="0",
            captured_at=T0,
            metadata={"raw": {"fill_blob": {}}},
        ),
        snapshot(
            attempts[1],
            order_intent.legs[1],
            status=BrokerOrderStatus.WORKING,
            filled="0",
            captured_at=T0,
        ),
    ]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "BROKER_SNAPSHOT_MISMATCH"
        and "terminal_no_fill_evidence" in issue["details_json"]
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_unknown_raw_fill_metadata_on_zero_terminal_event_is_not_clean(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    event = BrokerOrderEvent(
        id="event-unknown-zero-metadata",
        broker_order_id=attempts[0]["id"],
        dedupe_key="unknown-zero-metadata",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.CANCELLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=Decimal("0"),
        no_fill_asserted=True,
        metadata={"raw_payload": {"fill_blob": {}}},
    )

    recovered = oms._ingest_validated_broker_order_event(event, account=account(), fills=())

    assert recovered["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "TERMINAL_STATUS_WITHOUT_FILL_EVIDENCE"
        for issue in repository.open_reconciliation_issues(account().id)
    )
def test_partial_filled_push_event_with_durable_partial_fill_is_accepted(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    _durable_partial_fill(repository, order_intent, attempts)
    event = BrokerOrderEvent(
        id="event-partial-durable",
        broker_order_id=attempts[0]["id"],
        dedupe_key="partial-durable",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.PARTIALLY_FILLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=Decimal("4"),
    )

    recovered = oms._ingest_validated_broker_order_event(event, account=account(), fills=())

    assert recovered["intent_status"] == IntentStatus.PARTIALLY_FILLED.value
    assert recovered["legs"][0]["status"] == LegStatus.PARTIALLY_FILLED.value
    assert not any(
        issue["category"] == "FILL_EVIDENCE_REQUIRED"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert repository.position_allocations(account().id)[0]["signed_quantity"] == "4"


def test_multiple_conflicting_event_fill_observations_remain_separately_owned(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    for index, quantity in enumerate(("5", "6")):
        event = BrokerOrderEvent(
            id=f"event-fill-conflict-{index}",
            broker_order_id=attempts[0]["id"],
            dedupe_key=f"fill-conflict-{index}",
            event_type="ORDER_STATUS",
            event_at=clock(),
            received_at=clock(),
            broker_status=BrokerOrderStatus.PARTIALLY_FILLED,
            external_order_id=attempts[0]["external_order_id"],
            cumulative_filled_quantity=Decimal(quantity),
        )
        recovered = oms._ingest_validated_broker_order_event(event, account=account(), fills=())

    actions = repository.recovery_actions_for_intent(order_intent.id)
    open_conflicts = {
        action["action_key"]
        for action in actions
        if action["action_key"].startswith(f"EVENT_FILL_MISMATCH:{attempts[0]['id']}:")
        and action["status"] == "OPEN"
    }
    assert open_conflicts == {
        f"EVENT_FILL_MISMATCH:{attempts[0]['id']}:fill-conflict-0",
        f"EVENT_FILL_MISMATCH:{attempts[0]['id']}:fill-conflict-1",
    }
    assert recovered["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value


def test_recovery_rejects_cross_account_fill_metadata_without_allocating(tmp_path):
    repository, adapter, oms, order_intent, attempts, _clock = setup(tmp_path)
    adapter.open_orders = [
        snapshot(attempts[0], order_intent.legs[0], status=BrokerOrderStatus.WORKING, filled="0", captured_at=T0),
        snapshot(attempts[1], order_intent.legs[1], status=BrokerOrderStatus.WORKING, filled="0", captured_at=T0),
    ]
    adapter.fills = [replace(fill(attempts[0], key="cross-account"), metadata={"account_id": "other-account"})]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "BROKER_FACT_UNAVAILABLE"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert repository.position_allocations(account().id) == []


def test_recovery_revisits_recent_local_terminal_order_for_broker_contradiction(tmp_path):
    repository, adapter, oms, order_intent, attempts, _clock = setup(tmp_path)
    repository.record_submission(
        attempts[0]["id"],
        status=BrokerOrderStatus.CANCELLED,
        external_order_id=attempts[0]["external_order_id"],
        metadata={"local_cancel": True},
        now=T0,
    )
    repository.transition_leg(order_intent.legs[0].id, LegStatus.CANCELLED, now=T0)
    adapter.open_orders = [
        snapshot(attempts[0], order_intent.legs[0], status=BrokerOrderStatus.WORKING, filled="0", captured_at=T0),
        snapshot(attempts[1], order_intent.legs[1], status=BrokerOrderStatus.WORKING, filled="0", captured_at=T0),
    ]

    recovered = oms.recover_pending_intents(account=account())

    assert recovered and recovered[0]["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        "local_terminal_status_conflict" in issue["details_json"]
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert len(adapter.submit_calls) == 2


def test_terminal_restart_discovery_is_bounded_to_recent_attempts(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    oms._cancel_working_attempts(order_intent, account())
    old_timestamp = (T0 - timedelta(days=2)).isoformat()
    with repository.transaction() as conn:
        conn.execute("UPDATE core_broker_orders SET updated_at = ?", (old_timestamp,))
        conn.execute(
            "UPDATE core_order_intents SET updated_at = ? WHERE id = ?",
            (old_timestamp, order_intent.id),
        )

    assert repository.recoverable_intent_ids(account().id, now=T0, terminal_window_seconds=86_400) == []

    with repository.transaction() as conn:
        conn.execute(
            "UPDATE core_broker_orders SET updated_at = ? WHERE id = ?",
            (T0.isoformat(), attempts[0]["id"]),
        )
    assert repository.recoverable_intent_ids(account().id, now=T0, terminal_window_seconds=86_400) == [order_intent.id]


def test_direct_recover_intent_scans_aged_terminal_history_before_polling(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    oms._cancel_working_attempts(order_intent, account())
    old_timestamp = (T0 - timedelta(days=2)).isoformat()
    with repository.transaction() as conn:
        conn.execute("UPDATE core_broker_orders SET updated_at = ?", (old_timestamp,))
        conn.execute("UPDATE core_order_intents SET updated_at = ? WHERE id = ?", (old_timestamp, order_intent.id))

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "TERMINAL_HISTORY_EXPIRED"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert any(
        action["action_key"].startswith("TERMINAL_HISTORY_EXPIRED:")
        for action in repository.recovery_actions_for_intent(order_intent.id)
    )


def test_event_client_order_id_conflict_is_durable(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    event = BrokerOrderEvent(
        id="event-client-conflict",
        broker_order_id=attempts[0]["id"],
        dedupe_key="client-conflict",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.WORKING,
        external_order_id=attempts[0]["external_order_id"],
        client_order_id="unexpected-client-id",
    )

    recovered = oms._ingest_validated_broker_order_event(event, account=account(), fills=())

    assert recovered["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "BROKER_EVENT_CLIENT_ID_CONFLICT"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_open_recovery_action_blocks_completion_until_explicitly_resolved(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    repository.record_fill(
        Fill(
            id="complete-block-fill",
            broker_order_id=attempts[0]["id"],
            order_leg_id=order_intent.legs[0].id,
            external_fill_id="complete-block-fill",
            dedupe_key="complete-block-fill",
            quantity=Decimal("10"),
            price=Decimal("101"),
            filled_at=clock(),
            received_at=clock(),
            account_id=account().id,
            external_order_id=attempts[0]["external_order_id"],
            evidence_reference="complete-block-fill",
        ),
        now=clock(),
        _validation_token=repository._fill_validation_capability(),
    )
    repository.record_fill(
        Fill(
            id="complete-block-fill-2",
            broker_order_id=attempts[1]["id"],
            order_leg_id=order_intent.legs[1].id,
            external_fill_id="complete-block-fill-2",
            dedupe_key="complete-block-fill-2",
            quantity=Decimal("10"),
            price=Decimal("201"),
            filled_at=clock(),
            received_at=clock(),
            account_id=account().id,
            external_order_id=attempts[1]["external_order_id"],
            evidence_reference="complete-block-fill-2",
        ),
        now=clock(),
        _validation_token=repository._fill_validation_capability(),
    )
    repository.transition_intent(order_intent.id, IntentStatus.FILLED, now=clock())
    oms._record_recovery_action(
        intent=repository.get_intent(order_intent.id),
        account=account(),
        action_key="WAIT_FOR_BROKER:completion-block",
        state="WORKING",
        summary="test blocker",
        observed_positions={},
        remaining_quantities={},
    )

    with pytest.raises(OMSExecutionError, match="open"):
        oms.complete_intent(order_intent.id)


def _derived_intent(order_intent: OrderIntent, suffix: str) -> tuple[OrderIntent, RiskDecisionRecord]:
    derived_id = f"{order_intent.id}-{suffix}"
    derived_legs = tuple(
        replace(leg, id=f"{leg.id}-{suffix}", intent_id=derived_id)
        for leg in order_intent.legs
    )
    derived = replace(
        order_intent,
        id=derived_id,
        idempotency_key=f"{order_intent.idempotency_key}-{suffix}",
        legs=derived_legs,
        status=IntentStatus.CREATED,
    )
    derived_decision = replace(
        decision(),
        id=f"{decision().id}-{suffix}",
        intent_id=derived_id,
    )
    return derived, derived_decision


@pytest.mark.parametrize(
    "raw_payload",
    (
        {"dealt_qty": "-1"},
        {"dealt_qty": "NaN"},
        {"dealt_avg_price": "N/A"},
        {"deal_time": "not-a-timestamp"},
    ),
)
def test_malformed_submit_fill_payload_is_reconciliation_evidence_not_clean_rejection(tmp_path, raw_payload):
    repository, adapter, oms, order_intent, _attempts, _clock = setup(tmp_path)
    derived, derived_decision = _derived_intent(order_intent, "malformed-submit")
    adapter.submit_override = BrokerSubmissionResult(
        broker_order_id="placeholder",
        accepted=False,
        status=BrokerOrderStatus.REJECTED,
        external_order_id=None,
        raw_payload=raw_payload,
    )

    recovered = oms.submit_intent(derived, account=account(), risk_decision=derived_decision)

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "BROKER_SUBMISSION_FILL_EVIDENCE"
        for issue in repository.open_reconciliation_issues(account().id)
    )


@pytest.mark.parametrize("raw_payload", ({"dealt_qty": "-1"}, {"dealt_qty": "NaN"}, {"deal_time": "bad"}))
def test_malformed_cancel_fill_payload_is_reconciliation_evidence(tmp_path, raw_payload):
    repository, adapter, oms, order_intent, attempts, _clock = setup(tmp_path)
    adapter.cancel_result = BrokerSubmissionResult(
        broker_order_id=attempts[0]["id"],
        accepted=True,
        status=BrokerOrderStatus.CANCELLED,
        external_order_id=attempts[0]["external_order_id"],
        raw_payload=raw_payload,
    )

    oms._cancel_working_attempts(order_intent, account())

    assert any(
        issue["category"] == "CANCEL_FILL_EVIDENCE"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_post_submit_exception_blocks_remaining_legs_and_is_durable(tmp_path):
    repository, adapter, oms, order_intent, _attempts, _clock = setup(tmp_path)
    derived, derived_decision = _derived_intent(order_intent, "post-submit-exception")
    adapter.submit_override = object()

    recovered = oms.submit_intent(derived, account=account(), risk_decision=derived_decision)

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert len(adapter.submit_calls) == 3
    derived_attempts = [
        item
        for leg in derived.legs
        for item in repository.broker_orders_for_leg(leg.id)
    ]
    assert len(derived_attempts) == 1
    assert derived_attempts[0]["status"] == BrokerOrderStatus.UNKNOWN.value
    assert any(
        issue["category"] == "BROKER_SUBMISSION_EXCEPTION"
        and issue["entity_key"] == derived_attempts[0]["id"]
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert any(
        action["action_key"] == f"SUBMISSION_EXCEPTION:{derived_attempts[0]['id']}"
        for action in repository.recovery_actions_for_intent(derived.id)
    )


def test_submit_transport_exception_records_recovery_action(tmp_path):
    repository, adapter, oms, order_intent, _attempts, _clock = setup(tmp_path)
    derived, derived_decision = _derived_intent(order_intent, "transport-exception")
    adapter.raise_on_submit = True

    recovered = oms.submit_intent(derived, account=account(), risk_decision=derived_decision)

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    derived_attempts = [
        item
        for leg in derived.legs
        for item in repository.broker_orders_for_leg(leg.id)
    ]
    assert len(derived_attempts) == 1
    assert any(
        action["action_key"] == f"SUBMISSION_EXCEPTION:{derived_attempts[0]['id']}"
        for action in repository.recovery_actions_for_intent(derived.id)
    )


def test_aged_terminal_history_creates_durable_account_safety_block(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    oms._cancel_working_attempts(order_intent, account())
    old = (T0 - timedelta(days=2)).isoformat()
    with repository.transaction() as conn:
        conn.execute("UPDATE core_broker_orders SET updated_at = ?", (old,))
        conn.execute("UPDATE core_order_intents SET updated_at = ?", (old,))

    status = oms.recovery_status(order_intent.id, account=account())

    assert status["safe_to_submit"] is False
    assert any(
        issue["category"] == "TERMINAL_HISTORY_EXPIRED"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert any(
        action["action_key"].startswith("TERMINAL_HISTORY_EXPIRED:")
        for action in repository.recovery_actions_for_intent(order_intent.id)
    )


def test_aged_definite_no_submit_rejection_does_not_create_history_blocker(tmp_path):
    class DefiniteRejectAdapter(RecoveryAdapter):
        def submit_order(self, _account: Account, request) -> BrokerSubmissionResult:
            self.submit_calls.append(request.broker_order_id)
            return BrokerSubmissionResult(
                broker_order_id=request.broker_order_id,
                accepted=False,
                status=BrokerOrderStatus.REJECTED,
                external_order_id=None,
                cumulative_filled_quantity=Decimal("0"),
                no_submit_asserted=True,
                no_fill_asserted=True,
            )

    adapter = DefiniteRejectAdapter(Clock())
    repository, adapter, oms, order_intent, attempts, _clock = setup(
        tmp_path,
        adapter=adapter,
        expected_initial_status=IntentStatus.REJECTED,
    )
    old = (T0 - timedelta(days=2)).isoformat()
    with repository.transaction() as conn:
        conn.execute("UPDATE core_broker_orders SET updated_at = ?", (old,))
        conn.execute("UPDATE core_order_intents SET updated_at = ?", (old,))

    status = oms.recovery_status(order_intent.id, account=account())

    assert status["intent_status"] == IntentStatus.REJECTED.value
    assert status["safe_to_submit"] is True
    assert not any(
        issue["category"] == "TERMINAL_HISTORY_EXPIRED"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert not any(
        action["action_key"].startswith("TERMINAL_HISTORY_EXPIRED:")
        for action in repository.recovery_actions_for_intent(order_intent.id)
    )


def test_aged_ambiguous_rejection_still_creates_history_blocker(tmp_path):
    class DefiniteRejectAdapter(RecoveryAdapter):
        def submit_order(self, _account: Account, request) -> BrokerSubmissionResult:
            self.submit_calls.append(request.broker_order_id)
            return BrokerSubmissionResult(
                broker_order_id=request.broker_order_id,
                accepted=False,
                status=BrokerOrderStatus.REJECTED,
                external_order_id=None,
                cumulative_filled_quantity=Decimal("0"),
                no_submit_asserted=True,
                no_fill_asserted=True,
            )

    adapter = DefiniteRejectAdapter(Clock())
    repository, adapter, oms, order_intent, attempts, _clock = setup(
        tmp_path,
        adapter=adapter,
        expected_initial_status=IntentStatus.REJECTED,
    )
    attempt = attempts[0]
    metadata = json.loads(attempt["metadata_json"])
    metadata.update(
        {
            "accepted": None,
            "ambiguous": True,
            "definite_no_submit": False,
            "no_submit_asserted": False,
            "no_fill_asserted": False,
            "cumulative_fill_known": False,
            "reported_cumulative_fill": None,
        }
    )
    old = (T0 - timedelta(days=2)).isoformat()
    with repository.transaction() as conn:
        conn.execute(
            "UPDATE core_broker_orders SET metadata_json = ?, updated_at = ? WHERE id = ?",
            (json.dumps(metadata, sort_keys=True), old, attempt["id"]),
        )
        conn.execute("UPDATE core_order_intents SET updated_at = ?", (old,))

    status = oms.recovery_status(order_intent.id, account=account())

    assert status["safe_to_submit"] is False
    assert any(
        issue["category"] == "TERMINAL_HISTORY_EXPIRED"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert any(
        action["action_key"].startswith("TERMINAL_HISTORY_EXPIRED:")
        for action in repository.recovery_actions_for_intent(order_intent.id)
    )


def test_aged_terminal_order_is_still_polled_for_broker_contradiction(tmp_path):
    repository, adapter, oms, order_intent, attempts, _clock = setup(tmp_path)
    repository.record_submission(
        attempts[0]["id"],
        status=BrokerOrderStatus.CANCELLED,
        external_order_id=attempts[0]["external_order_id"],
        metadata={"local_cancel": True},
        now=T0,
    )
    repository.transition_leg(order_intent.legs[0].id, LegStatus.CANCELLED, now=T0)
    old = (T0 - timedelta(days=2)).isoformat()
    with repository.transaction() as conn:
        conn.execute("UPDATE core_broker_orders SET updated_at = ? WHERE id = ?", (old, attempts[0]["id"]))
    adapter.open_orders = [
        snapshot(attempts[0], order_intent.legs[0], status=BrokerOrderStatus.WORKING, filled="0", captured_at=T0),
        snapshot(attempts[1], order_intent.legs[1], status=BrokerOrderStatus.WORKING, filled="0", captured_at=T0),
    ]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        "local_terminal_status_conflict" in issue["details_json"]
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_filled_status_requires_fill_evidence_for_that_attempt_and_quarantines_multiple_attempts(tmp_path):
    repository, adapter, oms, order_intent, attempts, _clock = setup(tmp_path)
    repository.record_fill(
        Fill(
            id="first-partial-fill",
            broker_order_id=attempts[0]["id"],
            order_leg_id=order_intent.legs[0].id,
            external_fill_id="first-partial-fill",
            dedupe_key="first-partial",
            quantity=Decimal("4"),
            price=Decimal("101.25"),
            filled_at=T0,
            received_at=T0,
            account_id=account().id,
            external_order_id=attempts[0]["external_order_id"],
            evidence_reference="first-partial",
        ),
        now=T0,
        _validation_token=repository._fill_validation_capability(),
    )
    repository.create_broker_order(
        broker_order_id="second-attempt",
        order_leg_id=order_intent.legs[0].id,
        account_id=account().id,
        broker=account().broker,
        attempt_number=None,
        client_order_id=None,
        submitted_quantity=Decimal("10"),
        now=T0,
    )
    second_id = "second-attempt"
    repository.transition_broker_order(second_id, BrokerOrderStatus.SUBMITTING, now=T0)
    repository.record_submission(
        second_id,
        status=BrokerOrderStatus.FILLED,
        external_order_id="second-external",
        metadata={"test": True},
        now=T0,
    )
    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "MULTIPLE_ATTEMPTS_UNSUPPORTED"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert repository.fills_for_broker_order(attempts[0]["id"])[0]["quantity"] == "4"
    assert repository.fills_for_broker_order(second_id) == []


def test_terminal_event_for_second_attempt_cannot_erase_first_attempt_partial_fill(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    repository.record_fill(
        Fill(
            id="first-event-partial-fill",
            broker_order_id=attempts[0]["id"],
            order_leg_id=order_intent.legs[0].id,
            external_fill_id="first-event-partial-fill",
            dedupe_key="first-event-partial",
            quantity=Decimal("4"),
            price=Decimal("101.25"),
            filled_at=T0,
            received_at=T0,
            account_id=account().id,
            external_order_id=attempts[0]["external_order_id"],
            evidence_reference="first-event-partial",
        ),
        now=T0,
        _validation_token=repository._fill_validation_capability(),
    )
    repository.create_broker_order(
        broker_order_id="second-event-attempt",
        order_leg_id=order_intent.legs[0].id,
        account_id=account().id,
        broker=account().broker,
        attempt_number=None,
        client_order_id=None,
        submitted_quantity=Decimal("10"),
        now=T0,
    )
    repository.transition_broker_order("second-event-attempt", BrokerOrderStatus.SUBMITTING, now=T0)
    repository.record_submission(
        "second-event-attempt",
        status=BrokerOrderStatus.FILLED,
        external_order_id="second-event-external",
        metadata={"test": True},
        now=T0,
    )
    event = BrokerOrderEvent(
        id="second-attempt-filled-without-evidence",
        broker_order_id="second-event-attempt",
        dedupe_key="second-attempt-filled-without-evidence",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.FILLED,
        external_order_id="second-event-external",
        cumulative_filled_quantity=Decimal("10"),
    )

    recovered = oms._ingest_validated_broker_order_event(event, account=account(), fills=())

    assert recovered["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert repository.fills_for_broker_order(attempts[0]["id"])[0]["quantity"] == "4"
    assert repository.fills_for_broker_order("second-event-attempt") == []
    assert any(
        issue["category"] == "BROKER_EVENT_FILL_MISMATCH"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_late_fill_only_event_after_local_terminal_state_requires_review(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    repository.record_submission(
        attempts[0]["id"],
        status=BrokerOrderStatus.CANCELLED,
        external_order_id=attempts[0]["external_order_id"],
        metadata={"local_cancel": True},
        now=clock(),
    )
    repository.transition_leg(order_intent.legs[0].id, LegStatus.CANCELLED, now=clock())
    event = BrokerOrderEvent(
        id="late-fill-only",
        broker_order_id=attempts[0]["id"],
        dedupe_key="late-fill-only",
        event_type="DEAL",
        event_at=clock(),
        received_at=clock(),
        external_order_id=attempts[0]["external_order_id"],
    )

    recovered = oms._ingest_validated_broker_order_event(event, account=account(), fills=(fill(attempts[0], key="late-fill"),))

    assert recovered["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "LATE_FILL_AFTER_TERMINAL"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert any(
        action["action_key"] == f"LATE_FILL_AFTER_TERMINAL:{attempts[0]['id']}:late-fill-only"
        for action in repository.recovery_actions_for_intent(order_intent.id)
    )


def test_snapshot_account_alias_conflict_is_fail_closed(tmp_path):
    repository, adapter, oms, order_intent, attempts, _clock = setup(tmp_path)
    adapter.open_orders = [
        replace(
            snapshot(attempts[0], order_intent.legs[0], status=BrokerOrderStatus.WORKING, filled="0", captured_at=T0),
            metadata={"raw": {"acc_id": "wrong-account"}},
        ),
        snapshot(attempts[1], order_intent.legs[1], status=BrokerOrderStatus.WORKING, filled="0", captured_at=T0),
    ]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "BROKER_FACT_UNAVAILABLE"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_event_account_alias_conflict_is_fail_closed_before_state_or_fill(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    event = BrokerOrderEvent(
        id="event-account-alias-conflict",
        broker_order_id=attempts[0]["id"],
        dedupe_key="event-account-alias-conflict",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.WORKING,
        external_order_id=attempts[0]["external_order_id"],
        account_id=account().id,
        external_account_id="wrong-external-account",
    )

    recovered = oms._ingest_validated_broker_order_event(event, account=account(), fills=())

    assert recovered["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "BROKER_EVENT_ACCOUNT_ID_CONFLICT"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert repository.fills_for_leg(order_intent.legs[0].id) == []


def test_zero_terminal_snapshot_inspects_all_sibling_evidence_envelopes(tmp_path):
    repository, adapter, oms, order_intent, attempts, _clock = setup(tmp_path)
    adapter.open_orders = [
        snapshot(
            attempts[0],
            order_intent.legs[0],
            status=BrokerOrderStatus.CANCELLED,
            filled="0",
            captured_at=T0,
            metadata={"raw": {}, "response": {"dealt_qty": "1"}},
        ),
        snapshot(attempts[1], order_intent.legs[1], status=BrokerOrderStatus.WORKING, filled="0", captured_at=T0),
    ]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert recovered["legs"][0]["status"] == LegStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "BROKER_SNAPSHOT_MISMATCH"
        and "terminal_no_fill_evidence" in issue["details_json"]
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_zero_terminal_event_inspects_all_sibling_evidence_envelopes(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    event = BrokerOrderEvent(
        id="event-sibling-response-fill",
        broker_order_id=attempts[0]["id"],
        dedupe_key="event-sibling-response-fill",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.CANCELLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=Decimal("0"),
        no_fill_asserted=True,
        metadata={"raw": {}, "response": {"dealt_qty": "1"}},
    )

    recovered = oms._ingest_validated_broker_order_event(event, account=account(), fills=())

    assert recovered["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "TERMINAL_STATUS_WITHOUT_FILL_EVIDENCE"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert repository.fills_for_broker_order(attempts[0]["id"]) == []


def test_zero_terminal_event_does_not_trust_valid_looking_oms_envelope_hashes(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    event = BrokerOrderEvent(
        id="event-valid-looking-envelope-fill",
        broker_order_id=attempts[0]["id"],
        dedupe_key="event-valid-looking-envelope-fill",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.CANCELLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=Decimal("0"),
        no_fill_asserted=True,
        metadata={
            "_oms_core_evidence_envelope": {
                "fill_fingerprint": "a" * 64,
                "event_fingerprint": "b" * 64,
                "raw": {"dealt_qty": "1"},
            }
        },
    )

    recovered = oms._ingest_validated_broker_order_event(event, account=account(), fills=())

    assert recovered["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "TERMINAL_STATUS_WITHOUT_FILL_EVIDENCE"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert repository.position_allocations(account().id) == []


def test_public_fill_rejects_mismatched_evidence_reference(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)

    with pytest.raises(OMSExecutionError, match="durable fill recording failed"):
        oms.apply_fill(
            Fill(
                id="forged-evidence",
                broker_order_id=attempts[0]["id"],
                order_leg_id=order_intent.legs[0].id,
                dedupe_key="forged-evidence",
                quantity=Decimal("10"),
                price=Decimal("101"),
                filled_at=clock(),
                received_at=clock(),
                external_order_id=attempts[0]["external_order_id"],
                evidence_reference="not-the-provider-fact",
                account_id=account().id,
            )
        )

    assert repository.position_allocations(account().id) == []
    assert any(
        issue["category"] == "PUBLIC_FILL_RECORD_FAILED"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_public_fill_rejects_missing_normalized_provenance(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)

    with pytest.raises(OMSExecutionError, match="missing exact broker evidence"):
        oms.apply_fill(
            Fill(
                id="missing-evidence",
                broker_order_id=attempts[0]["id"],
                order_leg_id=order_intent.legs[0].id,
                dedupe_key="missing-evidence",
                quantity=Decimal("10"),
                price=Decimal("101"),
                filled_at=clock(),
                received_at=clock(),
            )
        )

    assert repository.position_allocations(account().id) == []
    assert any(
        issue["category"] == "PUBLIC_FILL_PROVENANCE_MISSING"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_public_fill_with_bound_evidence_is_disabled_without_allocation(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    forged = Fill(
        id="forged-bound-fill",
        broker_order_id=attempts[0]["id"],
        order_leg_id=order_intent.legs[0].id,
        external_fill_id="forged-bound-fill",
        dedupe_key="forged-bound-fill",
        quantity=Decimal("10"),
        price=Decimal("101"),
        filled_at=clock(),
        received_at=clock(),
        account_id=account().id,
        external_order_id=attempts[0]["external_order_id"],
        evidence_reference="forged-bound-fill",
    )

    with pytest.raises(OMSExecutionError, match="public direct fill path is disabled"):
        oms.apply_fill(forged)

    assert repository.position_allocations(account().id) == []
    assert any(
        issue["category"] == "PUBLIC_APPLY_FILL_DISABLED"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_reserved_fingerprint_key_inside_provider_list_is_fill_evidence(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    event = BrokerOrderEvent(
        id="event-nested-reserved-fill",
        broker_order_id=attempts[0]["id"],
        dedupe_key="nested-reserved-fill",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.CANCELLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=Decimal("0"),
        no_fill_asserted=True,
        metadata={"raw": [{"_fill_evidence_fingerprint": {"dealt_qty": "1"}}]},
    )

    recovered = oms._ingest_validated_broker_order_event(event, account=account(), fills=())

    assert recovered["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "TERMINAL_STATUS_WITHOUT_FILL_EVIDENCE"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert repository.position_allocations(account().id) == []


def test_unknown_position_is_account_wide_submit_blocker(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    derived, derived_decision = _derived_intent(order_intent, "unknown-position")
    adapter.positions = [
        PositionSnapshot(
            id="unknown-position",
            broker_snapshot_id="unknown-position-snapshot",
            account_id=account().id,
            instrument_id="unmanaged-instrument",
            signed_quantity=Decimal("1"),
            captured_at=clock(),
        )
    ]

    recovered = oms.submit_intent(derived, account=account(), risk_decision=derived_decision)

    assert recovered["status"] == IntentStatus.REJECTED.value
    assert len(adapter.submit_calls) == 2
    assert any(
        issue["category"] == "BROKER_FACT_GATE_BLOCKED"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert any(
        action["action_key"].startswith(f"BROKER_FACT_GATE:{account().id}:")
        for action in repository.open_recovery_actions(account().id)
    )


def test_unknown_broker_fill_is_account_wide_submit_blocker(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    derived, derived_decision = _derived_intent(order_intent, "unknown-fill")
    adapter.fills = [
        BrokerFill(
            external_order_id="unmanaged-external-order",
            external_fill_id="unmanaged-fill",
            dedupe_key="unmanaged-fill",
            quantity=Decimal("1"),
            price=Decimal("101"),
            filled_at=clock(),
            received_at=clock(),
        )
    ]

    recovered = oms.submit_intent(derived, account=account(), risk_decision=derived_decision)

    assert recovered["status"] == IntentStatus.REJECTED.value
    assert len(adapter.submit_calls) == 2
    assert any(
        issue["category"] == "BROKER_FACT_GATE_BLOCKED"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_pre_stage3_event_replay_is_compatible_but_changed_evidence_conflicts(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    attempt = attempts[0]
    event = BrokerOrderEvent(
        id="legacy-event-row",
        broker_order_id=attempt["id"],
        dedupe_key="legacy-event-key",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.WORKING,
        external_order_id=attempt["external_order_id"],
        client_order_id=attempt["client_order_id"],
        no_fill_asserted=False,
        metadata={"source": "legacy"},
    )
    repository.append_broker_event(
        event_id=event.id,
        broker_order_id=event.broker_order_id,
        dedupe_key=event.dedupe_key,
        event_type=event.event_type,
        broker_status=event.broker_status.value,
        event_at=event.event_at,
        received_at=event.received_at,
        external_event_id=event.external_event_id,
        metadata={
            "external_order_id": event.external_order_id,
            "client_order_id": event.client_order_id,
            "no_fill_asserted": False,
            "source": "legacy",
        },
    )

    replay = oms._ingest_validated_broker_order_event(event, account=account(), fills=())
    assert replay["intent_status"] == IntentStatus.WORKING.value
    assert repository.open_reconciliation_issues(account().id) == []

    changed = replace(event, id="legacy-event-changed", event_at=clock() + timedelta(seconds=1))
    conflicted = oms._ingest_validated_broker_order_event(changed, account=account(), fills=())
    assert conflicted["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "BROKER_EVENT_RECORD_FAILED"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_pre_stage3_event_replay_with_new_fill_evidence_conflicts_before_allocation(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    attempt = attempts[0]
    event = BrokerOrderEvent(
        id="legacy-event-no-fill",
        broker_order_id=attempt["id"],
        dedupe_key="legacy-event-no-fill",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.WORKING,
        external_order_id=attempt["external_order_id"],
        client_order_id=attempt["client_order_id"],
        no_fill_asserted=False,
        metadata={"source": "legacy"},
    )
    repository.append_broker_event(
        event_id=event.id,
        broker_order_id=event.broker_order_id,
        dedupe_key=event.dedupe_key,
        event_type=event.event_type,
        broker_status=event.broker_status.value,
        event_at=event.event_at,
        received_at=event.received_at,
        external_event_id=event.external_event_id,
        metadata={
            "external_order_id": event.external_order_id,
            "client_order_id": event.client_order_id,
            "no_fill_asserted": False,
            "source": "legacy",
        },
    )

    changed = replace(
        event,
        id="legacy-event-with-new-fill",
        metadata={"source": "legacy", "response": {"dealt_qty": "10"}},
    )
    conflicted = oms._ingest_validated_broker_order_event(
        changed,
        account=account(),
        fills=(fill(attempt, key="new-fill-evidence"),),
    )

    assert conflicted["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert repository.fills_for_broker_order(attempt["id"]) == []
    assert repository.position_allocations(account().id) == []
    assert any(
        issue["category"] == "BROKER_EVENT_RECORD_FAILED"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_old_terminal_snapshot_timestamp_is_reconciliation_evidence(tmp_path):
    repository, adapter, oms, order_intent, attempts, _clock = setup(tmp_path)
    adapter.open_orders = [
        snapshot(
            attempts[0],
            order_intent.legs[0],
            status=BrokerOrderStatus.CANCELLED,
            filled="0",
            captured_at=T0,
            order_time=T0 - timedelta(seconds=30),
        ),
        snapshot(attempts[1], order_intent.legs[1], status=BrokerOrderStatus.WORKING, filled="0", captured_at=T0),
    ]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "BROKER_SNAPSHOT_MISMATCH"
        and "temporal_order" in issue["details_json"]
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_old_terminal_event_timestamp_is_reconciliation_evidence(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    event = BrokerOrderEvent(
        id="old-event-time",
        broker_order_id=attempts[0]["id"],
        dedupe_key="old-event-time",
        event_type="ORDER_STATUS",
        event_at=T0 - timedelta(seconds=30),
        received_at=clock(),
        broker_status=BrokerOrderStatus.CANCELLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=Decimal("0"),
        no_fill_asserted=True,
    )

    recovered = oms._ingest_validated_broker_order_event(event, account=account(), fills=())

    assert recovered["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "BROKER_EVENT_TIME_CONFLICT"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_event_replay_with_changed_timestamp_or_identity_is_not_a_noop(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    first = BrokerOrderEvent(
        id="replay-first",
        broker_order_id=attempts[0]["id"],
        dedupe_key="replay-key",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.WORKING,
        external_event_id="provider-event-1",
        external_order_id=attempts[0]["external_order_id"],
    )
    oms._ingest_validated_broker_order_event(first, account=account(), fills=())
    changed = replace(first, id="replay-changed", event_at=clock() + timedelta(seconds=1), external_event_id="provider-event-2")

    recovered = oms._ingest_validated_broker_order_event(changed, account=account(), fills=())

    assert recovered["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "BROKER_EVENT_RECORD_FAILED"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_invalid_persisted_policy_is_durable_and_blocks_status(tmp_path):
    repository, adapter, oms, order_intent, attempts, _clock = setup(tmp_path)
    with repository.transaction() as conn:
        conn.execute(
            "UPDATE core_order_intents SET execution_policy_json = ? WHERE id = ?",
            ('{"partial_fill_policy":"CANCEL"}', order_intent.id),
        )

    status = oms.recovery_status(order_intent.id)

    assert status["safe_to_submit"] is False
    assert any(
        issue["category"] == "INVALID_PERSISTED_EXECUTION_POLICY"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_same_account_sibling_claimed_fill_is_not_silently_skipped(tmp_path):
    repository, adapter, oms, first, attempts, clock = setup(tmp_path)
    sibling_id = "stage3-sibling-intent"
    sibling_legs = tuple(
        replace(leg, id=f"{sibling_id}-leg-{index}", intent_id=sibling_id)
        for index, leg in enumerate(first.legs)
    )
    sibling = replace(
        first,
        id=sibling_id,
        idempotency_key="stage3-sibling-key",
        legs=sibling_legs,
    )
    sibling_decision = replace(decision(), id="stage3-sibling-risk", intent_id=sibling_id)
    oms.submit_intent(sibling, account=account(), risk_decision=sibling_decision)
    sibling_attempt = repository.broker_orders_for_leg(sibling_legs[0].id)[0]
    adapter.fills = [
        BrokerFill(
            external_order_id=sibling_attempt["external_order_id"],
            external_fill_id="sibling-fill",
            dedupe_key="sibling-fill",
            quantity=Decimal("10"),
            price=Decimal("101"),
            filled_at=clock(),
            received_at=clock(),
            instrument_id=sibling_legs[0].instrument_id,
        )
    ]

    recovered = oms.recover_intent(first.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "SIBLING_INTENT_BROKER_FILL_UNMATCHED"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_public_event_fill_arguments_are_untrusted_and_cannot_allocate(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    event = BrokerOrderEvent(
        id="public-untrusted-fill-event",
        broker_order_id=attempts[0]["id"],
        dedupe_key="public-untrusted-fill",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.FILLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=Decimal("10"),
    )

    recovered = oms.ingest_broker_order_event(
        event,
        account=account(),
        fills=(fill(attempts[0], key="caller-fabricated-fill"),),
    )

    assert recovered["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert repository.fills_for_broker_order(attempts[0]["id"]) == []
    assert repository.position_allocations(account().id) == []
    assert any(
        issue["category"] == "UNTRUSTED_EVENT_FILL_EVIDENCE"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_public_event_uses_adapter_authoritative_fill_lookup(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    adapter.fills = [fill(attempts[0], key="adapter-authoritative-fill")]
    event = BrokerOrderEvent(
        id="adapter-authoritative-event",
        broker_order_id=attempts[0]["id"],
        dedupe_key="adapter-authoritative-event",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.FILLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=Decimal("10"),
    )

    recovered = oms.ingest_broker_order_event(event, account=account())

    assert recovered["legs"][0]["status"] == LegStatus.FILLED.value
    assert repository.fills_for_broker_order(attempts[0]["id"])


def test_top_level_legacy_fingerprint_collision_cannot_hide_provider_fill_metadata(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    event = BrokerOrderEvent(
        id="fingerprint-collision-event",
        broker_order_id=attempts[0]["id"],
        dedupe_key="fingerprint-collision",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.CANCELLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=Decimal("0"),
        no_fill_asserted=True,
        metadata={
            "_fill_evidence_fingerprint": {"dealt_qty": "1"},
            "_oms_evidence_envelope": {"dealt_qty": "1"},
        },
    )

    recovered = oms._ingest_validated_broker_order_event(event, account=account(), fills=())

    assert recovered["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert repository.position_allocations(account().id) == []
    assert any(
        issue["category"] == "TERMINAL_STATUS_WITHOUT_FILL_EVIDENCE"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    stored = repository.broker_order_event(attempts[0]["id"], event.dedupe_key)
    assert stored is not None
    assert stored["metadata"]["_fill_evidence_fingerprint"] == {"dealt_qty": "1"}
    assert stored["metadata"]["_oms_evidence_envelope"] == {"dealt_qty": "1"}
    assert stored["oms_event_fingerprint"]


def test_missing_required_authoritative_broker_fact_reader_is_durable_unavailable_blocker(tmp_path):
    repository, adapter, oms, first, attempts, clock = setup(tmp_path)
    initial_submit_calls = len(adapter.submit_calls)
    # A cached/display-only reader is not an acceptable submit capability.
    adapter.get_authoritative_account_facts = None
    new_id = "stage3-missing-facts"
    new_leg = replace(first.legs[0], id=f"{new_id}-leg", intent_id=new_id)
    new_intent = replace(first, id=new_id, idempotency_key=f"{new_id}-key", legs=(new_leg,))
    result = oms.submit_intent(
        new_intent,
        account=account(),
        risk_decision=replace(decision(), id=f"{new_id}-risk", intent_id=new_id),
    )

    assert result["status"] == IntentStatus.REJECTED.value
    assert len(adapter.submit_calls) == initial_submit_calls
    assert any(
        issue["category"] == "BROKER_FACT_UNAVAILABLE"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert any(
        action["action_key"] == f"BROKER_FACT_UNAVAILABLE:{account().id}:QUERY"
        for action in repository.recovery_actions_for_intent(new_id)
    )


def test_public_event_status_is_advisory_until_authoritative_order_snapshot(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    adapter.open_orders = [
        snapshot(
            attempts[0],
            order_intent.legs[0],
            status=BrokerOrderStatus.WORKING,
            filled="0",
            captured_at=clock(),
        )
    ]
    forged = BrokerOrderEvent(
        id="public-forged-terminal-status",
        broker_order_id=attempts[0]["id"],
        dedupe_key="public-forged-terminal-status",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.FILLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=Decimal("10"),
        no_fill_asserted=False,
    )

    recovered = oms.ingest_broker_order_event(forged, account=account())

    assert recovered["legs"][0]["status"] == LegStatus.WORKING.value
    assert repository.fills_for_broker_order(attempts[0]["id"]) == []
    assert not any(
        issue["category"] == "BROKER_EVENT_STATUS_UNAVAILABLE"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_public_event_without_authoritative_snapshot_cannot_terminalize(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    forged = BrokerOrderEvent(
        id="public-forged-no-snapshot",
        broker_order_id=attempts[0]["id"],
        dedupe_key="public-forged-no-snapshot",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.CANCELLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=Decimal("0"),
        no_fill_asserted=True,
    )

    recovered = oms.ingest_broker_order_event(forged, account=account())

    assert recovered["legs"][0]["status"] == LegStatus.WORKING.value
    assert any(
        issue["category"] == "BROKER_EVENT_STATUS_UNAVAILABLE"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_incomplete_account_fact_snapshot_is_not_a_flat_account(tmp_path):
    repository, adapter, oms, first, attempts, clock = setup(tmp_path)
    adapter.get_account_facts = lambda _account: BrokerFactSnapshot(
        account_id=account().id,
        captured_at=clock(),
        complete=False,
        error="positions endpoint incomplete",
    )
    derived, risk = _derived_intent(first, "incomplete-facts")

    result = oms.submit_intent(derived, account=account(), risk_decision=risk)

    assert result["status"] == IntentStatus.REJECTED.value
    assert any(
        issue["category"] == "BROKER_FACT_UNAVAILABLE"
        for issue in repository.open_reconciliation_issues(account().id)
    )


@pytest.mark.parametrize(
    "metadata",
    (
        {"diagnostic": OpaqueProviderValue()},
        {"nested": {"account_id": "foreign-account"}},
        {"nested": {"accountId": "foreign-account"}},
        {"nested": {"account": {"id": "foreign-account"}}},
        {"raw": {"dealt_qty": "1"}},
        {"raw": '{"dealt_qty": "1"}'},
    ),
)
def test_account_fact_metadata_cannot_hide_opaque_foreign_or_execution_facts(tmp_path, metadata):
    repository, adapter, oms, first, _attempts, clock = setup(tmp_path)
    adapter.get_account_facts = lambda _account: BrokerFactSnapshot(
        account_id=account().id,
        captured_at=clock(),
        complete=True,
        metadata=metadata,
    )
    derived, risk = _derived_intent(first, "unsafe-fact-metadata")

    result = oms.submit_intent(derived, account=account(), risk_decision=risk)

    assert result["status"] == IntentStatus.REJECTED.value
    issues = repository.open_reconciliation_issues(account().id)
    assert any(
        issue["category"] in {"BROKER_FACT_UNAVAILABLE", "BROKER_FACT_GATE_BLOCKED"}
        and "metadata" in issue["details_json"]
        for issue in issues
    )


def test_benign_primitive_account_fact_diagnostics_remain_compatible(tmp_path):
    repository, adapter, oms, first, _attempts, clock = setup(tmp_path)
    adapter.get_account_facts = lambda _account: BrokerFactSnapshot(
        account_id=account().id,
        captured_at=clock(),
        complete=True,
        metadata={
            "source": "offline-test",
            "query_complete": True,
            "row_count": 0,
            "consistency": "per-endpoint",
        },
    )
    derived, risk = _derived_intent(first, "benign-fact-metadata")

    result = oms.submit_intent(derived, account=account(), risk_decision=risk)

    assert result["status"] == IntentStatus.WORKING.value
    assert not any(
        issue["category"] == "BROKER_FACT_GATE_BLOCKED"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_unknown_allocation_is_sticky_and_excluded_from_managed_net_exposure(tmp_path):
    repository, adapter, oms, first, attempts, clock = setup(tmp_path)
    with repository.transaction() as conn:
        conn.execute(
            """INSERT INTO core_position_allocations
               (id, account_id, instrument_id, strategy_id, book_id, ownership_class,
                signed_quantity, source_intent_id, updated_at, metadata_json)
               VALUES (?, ?, ?, ?, NULL, 'MANAGED', ?, ?, ?, '{}')""",
            (
                "managed-allocation",
                account().id,
                first.legs[0].instrument_id,
                "stage3-strategy",
                "10",
                first.id,
                clock().isoformat(),
            ),
        )
        conn.execute(
            """INSERT INTO core_position_allocations
               (id, account_id, instrument_id, strategy_id, book_id, ownership_class,
                signed_quantity, source_intent_id, updated_at, metadata_json)
               VALUES (?, ?, ?, NULL, NULL, 'UNKNOWN', ?, NULL, ?, ?)""",
            (
                "unknown-allocation",
                account().id,
                first.legs[0].instrument_id,
                "-10",
                clock().isoformat(),
                '{"source":"legacy-operator-row"}',
            ),
        )
    adapter.positions = [
        PositionSnapshot(
            id="managed-position",
            broker_snapshot_id="managed-position-snapshot",
            account_id=account().id,
            instrument_id=first.legs[0].instrument_id,
            signed_quantity=Decimal("10"),
            captured_at=clock(),
        )
    ]
    derived, risk = _derived_intent(first, "unknown-allocation")

    result = oms.submit_intent(derived, account=account(), risk_decision=risk)

    assert result["status"] == IntentStatus.REJECTED.value
    assert any(
        issue["category"] == "BROKER_FACT_GATE_BLOCKED"
        and "unknown_allocation" in issue["details_json"]
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_legacy_duplicate_external_fill_rows_are_quarantined_before_recovery(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    with repository.transaction() as conn:
        for row_id, dedupe in (("legacy-dup-1", "legacy-dup-key-1"), ("legacy-dup-2", "legacy-dup-key-2")):
            conn.execute(
                """INSERT INTO core_fills
                   (id, broker_order_id, order_leg_id, external_fill_id, dedupe_key,
                    quantity, price, fee, fee_currency, filled_at, received_at, metadata_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, '{}')""",
                (
                    row_id,
                    attempts[0]["id"],
                    order_intent.legs[0].id,
                    "legacy-duplicate-deal",
                    dedupe,
                    "1",
                    "100",
                    clock().isoformat(),
                    clock().isoformat(),
                ),
            )

    recovered = oms.recovery_status(order_intent.id, account=account())

    assert recovered["safe_to_submit"] is False
    assert any(
        issue["category"] == "LEGACY_DUPLICATE_EXTERNAL_FILL"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert any(
        action["action_key"].startswith("LEGACY_DUPLICATE_EXTERNAL_FILL:")
        for action in repository.open_recovery_actions(account().id)
    )


def test_unknown_broker_order_status_is_not_treated_as_an_empty_open_order_book(tmp_path):
    repository, adapter, oms, first, attempts, clock = setup(tmp_path)
    adapter.open_orders = [
        snapshot(
            attempts[0],
            first.legs[0],
            status=BrokerOrderStatus.UNKNOWN,
            filled="0",
            captured_at=clock(),
        )
    ]
    new_id = "stage3-unknown-open-order"
    new_leg = replace(first.legs[0], id=f"{new_id}-leg", intent_id=new_id)
    new_intent = replace(first, id=new_id, idempotency_key=f"{new_id}-key", legs=(new_leg,))

    result = oms.submit_intent(
        new_intent,
        account=account(),
        risk_decision=replace(decision(), id=f"{new_id}-risk", intent_id=new_id),
    )

    assert result["status"] == IntentStatus.REJECTED.value
    actions = repository.recovery_actions_for_intent(new_id)
    assert any(
        action["state"] == "RECONCILIATION_REQUIRED"
        and "unknown_broker_order_status" in json.dumps(action["metadata"], sort_keys=True)
        for action in actions
    )


def test_reverse_reconciliation_blocks_when_managed_allocation_is_missing_from_broker_positions(tmp_path):
    repository, adapter, oms, first, attempts, clock = setup(tmp_path)
    event = BrokerOrderEvent(
        id="reverse-reconciliation-fill",
        broker_order_id=attempts[0]["id"],
        dedupe_key="reverse-reconciliation-fill",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.FILLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=Decimal("10"),
    )
    oms._ingest_validated_broker_order_event(
        event,
        account=account(),
        fills=(fill(attempts[0], key="reverse-reconciliation-fill"),),
    )
    adapter.positions = []
    new_id = "stage3-reverse-reconciliation"
    new_leg = replace(first.legs[0], id=f"{new_id}-leg", intent_id=new_id)
    new_intent = replace(first, id=new_id, idempotency_key=f"{new_id}-key", legs=(new_leg,))

    result = oms.submit_intent(
        new_intent,
        account=account(),
        risk_decision=replace(decision(), id=f"{new_id}-risk", intent_id=new_id),
    )

    assert result["status"] == IntentStatus.REJECTED.value
    assert any(
        item.get("kind") == "managed_position_missing_or_mismatched"
        for action in repository.recovery_actions_for_intent(new_id)
        for item in (action["metadata"].get("blockers") or [])
    )


def test_repository_rejects_same_external_fill_id_with_changed_dedupe_key(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    first = fill(attempts[0], key="external-fill-first", quantity="5")
    first_value = Fill(
        id="repository-fill-first",
        broker_order_id=attempts[0]["id"],
        order_leg_id=order_intent.legs[0].id,
        external_fill_id="same-provider-deal",
        dedupe_key=first.dedupe_key,
        quantity=first.quantity,
        price=first.price,
        filled_at=first.filled_at,
        received_at=first.received_at,
        account_id=account().id,
        external_order_id=first.external_order_id,
        evidence_reference=first.evidence_reference,
    )
    repository.record_fill(
        first_value,
        now=clock(),
        _validation_token=repository._fill_validation_capability(),
    )
    changed_value = replace(
        first_value,
        id="repository-fill-second",
        dedupe_key="external-fill-second-key",
        evidence_reference="external-fill-second-key",
    )

    with pytest.raises(ValueError, match="external fill ID"):
        repository.record_fill(
            changed_value,
            now=clock(),
            _validation_token=repository._fill_validation_capability(),
        )
    assert len(repository.fills_for_broker_order(attempts[0]["id"])) == 1


def test_changed_external_fill_identity_is_durable_reconciliation_not_second_allocation(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    first_fill = replace(
        fill(attempts[0], key="same-deal-first", quantity="5"),
        external_fill_id="same-provider-deal",
    )
    first_event = BrokerOrderEvent(
        id="same-deal-first-event",
        broker_order_id=attempts[0]["id"],
        dedupe_key="same-deal-first-event",
        event_type="DEAL",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.PARTIALLY_FILLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=Decimal("5"),
    )
    oms._ingest_validated_broker_order_event(first_event, account=account(), fills=(first_fill,))
    changed_fill = replace(
        first_fill,
        dedupe_key="same-deal-second-key",
        evidence_reference="same-deal-second-key",
        quantity=Decimal("1"),
    )
    changed_event = replace(
        first_event,
        id="same-deal-second-event",
        dedupe_key="same-deal-second-event",
        event_at=clock() + timedelta(seconds=1),
        received_at=clock() + timedelta(seconds=1),
        cumulative_filled_quantity=Decimal("6"),
    )

    recovered = oms._ingest_validated_broker_order_event(
        changed_event,
        account=account(),
        fills=(changed_fill,),
    )

    assert recovered["intent_status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert len(repository.fills_for_broker_order(attempts[0]["id"])) == 1
    assert any(
        issue["category"] == "AMBIGUOUS_FILL_MATCH"
        for issue in repository.open_reconciliation_issues(account().id)
    )


@pytest.mark.parametrize(
    "payload",
    (
        {"status": "FILLED"},
        {"result": [{"id": "broker-order-1", "status": "FILLED"}]},
        {"wrapper": {"details": {"state": "PARTIALLY_FILLED"}}},
        {"orders": {"unexpected": object()}},
    ),
)
def test_plain_or_malformed_lifecycle_payload_is_submission_evidence(payload):
    assert GenericOMS._payload_has_submission_evidence(payload) is True


def test_plain_benign_status_diagnostic_remains_non_evidence():
    assert GenericOMS._payload_has_submission_evidence({"status": "OK", "query_complete": True}) is False


def test_nested_lifecycle_claim_invalidates_authoritative_zero_fill_snapshot(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    observed = snapshot(
        attempts[0],
        order_intent.legs[0],
        status=BrokerOrderStatus.CANCELLED,
        filled="0",
        captured_at=clock(),
        authority=ADAPTER_ORDER_SNAPSHOT_AUTHORITY,
        metadata={
            "raw": {
                "order_id": attempts[0]["external_order_id"],
                "order_status": "CANCELLED_ALL",
                "qty": "10",
                "dealt_qty": "0",
                "orders": [{"id": "sibling-order", "status": "FILLED"}],
            }
        },
    )

    assert oms._snapshot_no_fill_is_authoritative(observed) is False


@pytest.mark.parametrize("execution_key", ("trade_id", "tradeId", "tradeID", "executionId"))
def test_execution_identity_blocks_zero_fill_snapshot_event_and_command(tmp_path, execution_key):
    repository, _adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    raw = {
        "order_id": attempts[0]["external_order_id"],
        "order_status": "CANCELLED_ALL",
        "code": "S30",
        "instrument_id": order_intent.legs[0].instrument_id,
        "qty": "10",
        "dealt_qty": "0",
        execution_key: "provider-execution-1",
    }
    observed = snapshot(
        attempts[0],
        order_intent.legs[0],
        status=BrokerOrderStatus.CANCELLED,
        filled="0",
        captured_at=clock(),
        authority=ADAPTER_ORDER_SNAPSHOT_AUTHORITY,
        metadata={"raw": raw},
    )
    assert oms._snapshot_no_fill_is_authoritative(observed) is False

    event = BrokerOrderEvent(
        id=f"execution-identity-event-{execution_key}",
        broker_order_id=attempts[0]["id"],
        dedupe_key=f"execution-identity-event-{execution_key}",
        event_type="ORDER_STATUS",
        event_at=clock(),
        received_at=clock(),
        broker_status=BrokerOrderStatus.CANCELLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=Decimal("0"),
        no_fill_asserted=True,
        authority=ADAPTER_ORDER_SNAPSHOT_AUTHORITY,
        metadata={"authoritative_snapshot": {"raw": raw}},
    )
    assert oms._event_no_fill_is_authoritative(
        event,
        expected_quantity=Decimal("10"),
        expected_instrument_id=order_intent.legs[0].instrument_id,
    ) is False

    command = BrokerSubmissionResult(
        broker_order_id=attempts[0]["id"],
        accepted=True,
        status=BrokerOrderStatus.CANCELLED,
        external_order_id=attempts[0]["external_order_id"],
        cumulative_filled_quantity=Decimal("0"),
        no_fill_asserted=True,
        authority=ADAPTER_ORDER_SNAPSHOT_AUTHORITY,
        submitted_quantity=Decimal("10"),
        instrument_id=order_intent.legs[0].instrument_id,
        raw_payload={"response": raw},
    )
    assert oms._has_cancel_evidence(
        command,
        expected_quantity=Decimal("10"),
        expected_instrument_id=order_intent.legs[0].instrument_id,
    ) is True
    assert oms._has_submission_fill_evidence(command) is True


def test_account_gate_blocks_working_zero_fill_without_authoritative_no_fill(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    adapter.open_orders = [
        snapshot(
            attempts[0],
            order_intent.legs[0],
            status=BrokerOrderStatus.WORKING,
            filled="0",
            captured_at=clock(),
        )
    ]

    assert oms._account_wide_broker_fact_gate(order_intent.id, account()) is False
    assert any(
        action["state"] == "RECONCILIATION_REQUIRED"
        and any(item.get("kind") == "open_order_identity_mismatch" for item in (action["metadata"].get("blockers") or []))
        for action in repository.recovery_actions_for_intent(order_intent.id)
    )


def test_legacy_duplicate_external_order_claim_is_durable_account_blocker(tmp_path):
    repository, adapter, oms, first, attempts, clock = setup(tmp_path)
    second_account = replace(account(), id="stage3-sibling-acct", external_account_id="stage3-sibling-external")
    repository.save_account(second_account)
    second_id = "stage3-sibling-intent"
    second_leg = replace(first.legs[0], id=f"{second_id}-leg", intent_id=second_id)
    second_intent = replace(
        first,
        id=second_id,
        idempotency_key=f"{second_id}-key",
        account_id=second_account.id,
        legs=(second_leg,),
    )
    repository.create_intent(second_intent)
    repository.create_broker_order(
        broker_order_id="legacy-sibling-attempt",
        order_leg_id=second_leg.id,
        account_id=second_account.id,
        broker=second_account.broker,
        attempt_number=1,
        client_order_id="legacy-sibling-client",
        submitted_quantity=second_leg.quantity,
        now=clock(),
    )
    repository.transition_broker_order("legacy-sibling-attempt", BrokerOrderStatus.SUBMITTING, now=clock())
    repository.record_submission(
        "legacy-sibling-attempt",
        status=BrokerOrderStatus.WORKING,
        external_order_id=attempts[0]["external_order_id"],
        now=clock(),
    )
    repository.audit_legacy_duplicate_broker_orders()

    assert any(
        issue["category"] == "LEGACY_DUPLICATE_BROKER_ORDER"
        for issue in repository.open_reconciliation_issues(account().id)
    )
    assert any(
        action["action_key"] == f"LEGACY_DUPLICATE_BROKER_ORDER:{attempts[0]['external_order_id']}"
        for action in repository.recovery_actions_for_intent(first.id)
    )


def test_recovery_missing_authoritative_facts_blocks_before_order_or_fill_reads(tmp_path):
    repository, adapter, oms, order_intent, attempts, _clock = setup(tmp_path)
    before_orders = repository.broker_orders_for_leg(order_intent.legs[0].id)
    adapter.get_authoritative_account_facts = None
    adapter.get_order = lambda *_args: pytest.fail("recovery must not read a raw order after fact failure")
    adapter.get_fills = lambda *_args, **_kwargs: pytest.fail("recovery must not read raw fills after fact failure")
    adapter.fills = [fill(attempts[0], key="blocked-fill")]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert all(leg["status"] == LegStatus.WORKING.value for leg in recovered["legs"])
    assert repository.fills_for_leg(order_intent.legs[0].id) == []
    assert repository.broker_orders_for_leg(order_intent.legs[0].id) == before_orders
    status = oms.recovery_status(order_intent.id, account=account())
    assert status["safe_to_submit"] is False
    assert status["fresh_authoritative_facts_valid"] is False


def test_recovery_stale_or_foreign_authoritative_facts_do_not_promote_or_allocate(tmp_path):
    def _case(mode, case_path):
        case_path.mkdir()
        repository, adapter, oms, order_intent, attempts, clock = setup(case_path)
        if mode == "stale":
            captured_at = T0 - timedelta(seconds=GenericOMS.BROKER_FACT_MAX_AGE_SECONDS + 1)
            account_id = account().id
        else:
            captured_at = clock()
            account_id = "foreign-account"
        adapter.get_authoritative_account_facts = lambda _account: BrokerFactSnapshot(
            account_id=account_id,
            captured_at=captured_at,
            complete=True,
            open_orders=tuple(adapter.open_orders),
            fills=tuple(adapter.fills),
        )
        adapter.get_order = lambda *_args: pytest.fail("invalid facts must stop before raw order reads")
        adapter.get_fills = lambda *_args, **_kwargs: pytest.fail("invalid facts must stop before raw fill reads")
        before = repository.broker_orders_for_leg(order_intent.legs[0].id)

        recovered = oms.recover_intent(order_intent.id, account=account())

        assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
        assert all(leg["status"] == LegStatus.WORKING.value for leg in recovered["legs"])
        assert repository.fills_for_leg(order_intent.legs[0].id) == []
        assert repository.broker_orders_for_leg(order_intent.legs[0].id) == before

    _case("stale", tmp_path / "stale")
    _case("foreign", tmp_path / "foreign")


def test_recovery_reuses_validated_fact_fills_without_cacheable_fill_query(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    adapter.open_orders = [
        snapshot(
            attempts[0],
            order_intent.legs[0],
            status=BrokerOrderStatus.FILLED,
            filled="10",
            captured_at=clock(),
        ),
        snapshot(
            attempts[1],
            order_intent.legs[1],
            status=BrokerOrderStatus.WORKING,
            filled="0",
            captured_at=clock(),
        ),
    ]
    adapter.fills = [fill(attempts[0], key="validated-fact-fill")]
    adapter.get_fills = lambda *_args, **_kwargs: pytest.fail("recovery must use validated snapshot fills")

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["legs"][0]["status"] == LegStatus.FILLED.value
    assert repository.fills_for_leg(order_intent.legs[0].id)


@pytest.mark.parametrize("invalid_kind", ("wrong_instrument", "unavailable_evidence"))
def test_recovery_rejects_invalid_matched_fill_provenance_before_persistence(tmp_path, invalid_kind):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    adapter.open_orders = [
        snapshot(
            attempts[0],
            order_intent.legs[0],
            status=BrokerOrderStatus.FILLED,
            filled="10",
            captured_at=clock(),
        ),
        snapshot(
            attempts[1],
            order_intent.legs[1],
            status=BrokerOrderStatus.WORKING,
            filled="0",
            captured_at=clock(),
        ),
    ]
    evidence = fill(attempts[0], key=f"invalid-{invalid_kind}")
    if invalid_kind == "wrong_instrument":
        evidence = replace(evidence, instrument_id="stage3-instrument-foreign")
    else:
        evidence = replace(evidence, evidence_mode=ExecutionEvidenceMode.UNAVAILABLE)
    adapter.fills = [evidence]

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert all(leg["status"] == LegStatus.WORKING.value for leg in recovered["legs"])
    assert repository.fills_for_leg(order_intent.legs[0].id) == []
    assert repository.position_allocations(account().id) == []
    assert any(
        issue["category"] == "BROKER_FACT_UNAVAILABLE"
        for issue in repository.open_reconciliation_issues(account().id)
    )


def test_recovery_status_is_not_safe_when_matched_fill_provenance_is_invalid(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    adapter.open_orders = []
    adapter.fills = [fill(attempts[0], key="status-invalid-evidence")]

    def unavailable_facts(account_value):
        facts = adapter.get_account_facts(account_value)
        return replace(
            facts,
            execution_evidence_mode=ExecutionEvidenceMode.UNAVAILABLE,
            execution_evidence_scope=(),
        )

    adapter.get_authoritative_account_facts = unavailable_facts

    status = oms.recovery_status(order_intent.id, account=account())

    assert status["safe_to_submit"] is False
    assert status["fresh_authoritative_facts_valid"] is False
    assert "execution evidence is unavailable" in status["fresh_authoritative_facts_error"]
    assert repository.fills_for_leg(order_intent.legs[0].id) == []


def test_recovery_contradictory_duplicate_fact_fills_block_before_persistence(tmp_path):
    repository, adapter, oms, order_intent, attempts, clock = setup(tmp_path)
    first = fill(attempts[0], key="duplicate-fact")
    conflicting = replace(first, price=Decimal("202.50"))
    adapter.get_authoritative_account_facts = lambda _account: BrokerFactSnapshot(
        account_id=account().id,
        captured_at=clock(),
        complete=True,
        fills=(first, conflicting),
    )
    adapter.get_order = lambda *_args: pytest.fail("contradictory facts must stop before raw order reads")
    adapter.get_fills = lambda *_args, **_kwargs: pytest.fail("contradictory facts must stop before raw fill reads")

    recovered = oms.recover_intent(order_intent.id, account=account())

    assert recovered["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert all(leg["status"] == LegStatus.WORKING.value for leg in recovered["legs"])
    assert repository.fills_for_leg(order_intent.legs[0].id) == []


def test_pending_recovery_missing_facts_does_not_mutate_attempts_or_fills(tmp_path):
    repository, adapter, oms, order_intent, attempts, _clock = setup(tmp_path)
    adapter.get_authoritative_account_facts = None
    adapter.get_order = lambda *_args: pytest.fail("pending recovery must not read raw orders")
    adapter.get_fills = lambda *_args, **_kwargs: pytest.fail("pending recovery must not read raw fills")
    before_intent = repository.get_intent(order_intent.id)
    before_attempts = repository.broker_orders_for_leg(order_intent.legs[0].id)

    recovered = oms.poll_and_recover(account=account())

    assert recovered and recovered[0]["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert repository.fills_for_leg(order_intent.legs[0].id) == []
    assert repository.broker_orders_for_leg(order_intent.legs[0].id) == before_attempts
    assert repository.get_intent(order_intent.id)["legs"] == before_intent["legs"]
