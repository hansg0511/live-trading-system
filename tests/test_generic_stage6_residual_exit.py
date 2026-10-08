from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

import pytest

from src.strategies.stat_arb.stage6_pilot import Stage6PilotRunner, Stage6RunMode
from src.trading_core.domain import (
    ADAPTER_ORDER_SNAPSHOT_AUTHORITY,
    Book,
    BrokerOrderSnapshot,
    BrokerOrderStatus,
    ExecutionEvidenceMode,
    ExecutionPolicy,
    Fill,
    IntentAction,
    IntentStatus,
    LegStatus,
    OrderIntent,
    OrderLeg,
    PositionSnapshot,
    Side,
)
from src.trading_core.oms import GenericOMS, OMSExecutionError
from src.trading_core.ports import BrokerFill, BrokerHistoricalOrderFacts, BrokerSubmissionResult
from tests.test_generic_stage6_pilot import (
    MoomooAckPilotAdapter,
    NOW,
    NettedPilotAdapter,
    PilotFakeAdapter,
    make_account,
    make_repository,
    make_runner,
    make_sleeves,
    make_spec,
    make_targets,
)


def _seed_partial_source(
    repository,
    account,
    *,
    action: IntentAction,
    source_id: str,
    instrument_id: str,
    book_id: str = "pilot-book-a",
    quantity: str = "2",
    filled_quantity: str = "1",
    side: Side = Side.BUY,
):
    leg_id = f"{source_id}-leg"
    intent = OrderIntent(
        id=source_id,
        idempotency_key=f"{source_id}-key",
        strategy_id="pilot-strategy",
        account_id=account.id,
        book_id=book_id,
        action=action,
        execution_policy=ExecutionPolicy(),
        legs=(
            OrderLeg(
                id=leg_id,
                intent_id=source_id,
                sequence=0,
                instrument_id=instrument_id,
                side=side,
                quantity=Decimal(quantity),
                created_at=NOW,
                updated_at=NOW,
            ),
        ),
        created_at=NOW,
        updated_at=NOW,
    )
    repository.create_intent(intent)
    repository.transition_intent(source_id, IntentStatus.RISK_APPROVED, now=NOW)
    repository.transition_intent(source_id, IntentStatus.SUBMITTING, now=NOW)
    repository.transition_leg(leg_id, LegStatus.SUBMITTING, now=NOW)
    repository.create_broker_order(
        broker_order_id=f"{source_id}-order",
        order_leg_id=leg_id,
        account_id=account.id,
        broker=account.broker,
        attempt_number=1,
        client_order_id=f"{leg_id}:1",
        submitted_quantity=Decimal(quantity),
        now=NOW,
    )
    repository.transition_broker_order(
        f"{source_id}-order",
        BrokerOrderStatus.SUBMITTING,
        now=NOW,
    )
    repository.record_submission(
        f"{source_id}-order",
        status=BrokerOrderStatus.PARTIALLY_FILLED,
        external_order_id=f"{source_id}-external",
        now=NOW,
    )
    repository.record_fill(
        Fill(
            id=f"{source_id}-fill",
            broker_order_id=f"{source_id}-order",
            order_leg_id=leg_id,
            dedupe_key=f"{source_id}-deal",
            quantity=Decimal(filled_quantity),
            price=Decimal("100"),
            filled_at=NOW,
            received_at=NOW,
            account_id=account.id,
            external_order_id=f"{source_id}-external",
            evidence_reference=f"{source_id}-deal",
        ),
        now=NOW,
        _validation_token=repository._fill_validation_capability(),
    )
    return intent, leg_id, f"{source_id}-external"


def _set_partial_facts(adapter, account, instrument_id: str, signed_quantity: str, external_order_id: str):
    adapter.facts = replace(
        adapter.facts,
        positions=(
            PositionSnapshot(
                id=f"fresh-position-{external_order_id}",
                broker_snapshot_id=f"fresh-snapshot-{external_order_id}",
                account_id=account.id,
                instrument_id=instrument_id,
                signed_quantity=Decimal(signed_quantity),
                average_price=Decimal("100"),
                captured_at=NOW,
            ),
        ),
        fills=(
            # The fake fact is intentionally provider-shaped and is matched
            # against the immutable durable source fill by external order ID.
            adapter.facts.fills[0]
            if adapter.facts.fills
            else None,
        ),
    )


def _seed_partial_enter_with_terminal_zero_sibling(
    repository,
    account,
    sleeves,
    *,
    terminal_zero_proof: bool,
    first_quantity: str = "2",
    first_filled_quantity: str = "1",
    include_terminal_zero_attempt: bool = True,
    terminal_zero_status: BrokerOrderStatus = BrokerOrderStatus.CANCELLED,
):
    """Build one filled source leg plus a proven or unsubmitted sibling.

    All setup goes through repository/domain APIs so the test exercises the
    same durable graph that residual proof consumes.
    """
    first_instrument, second_instrument = sleeves[0].instrument_ids
    source_id = "partial-enter-zero-sibling"
    first_leg_id = f"{source_id}-leg-a"
    second_leg_id = f"{source_id}-leg-b"
    source = OrderIntent(
        id=source_id,
        idempotency_key=f"{source_id}-key",
        strategy_id="pilot-strategy",
        account_id=account.id,
        book_id=sleeves[0].book_id,
        action=IntentAction.ENTER,
        execution_policy=ExecutionPolicy(),
        legs=(
            OrderLeg(
                id=first_leg_id,
                intent_id=source_id,
                sequence=0,
                instrument_id=first_instrument,
                side=Side.BUY,
                quantity=Decimal(first_quantity),
                created_at=NOW,
                updated_at=NOW,
            ),
            OrderLeg(
                id=second_leg_id,
                intent_id=source_id,
                sequence=1,
                instrument_id=second_instrument,
                side=Side.SELL,
                quantity=Decimal("1"),
                created_at=NOW,
                updated_at=NOW,
            ),
        ),
        created_at=NOW,
        updated_at=NOW,
    )
    repository.create_intent(source)
    repository.transition_intent(source_id, IntentStatus.RISK_APPROVED, now=NOW)
    repository.transition_intent(source_id, IntentStatus.SUBMITTING, now=NOW)
    for leg in source.legs:
        repository.transition_leg(leg.id, LegStatus.SUBMITTING, now=NOW)
    for leg in source.legs:
        if leg.id == second_leg_id and not include_terminal_zero_attempt:
            continue
        broker_order_id = f"{leg.id}-order"
        repository.create_broker_order(
            broker_order_id=broker_order_id,
            order_leg_id=leg.id,
            account_id=account.id,
            broker=account.broker,
            attempt_number=1,
            client_order_id=f"{leg.id}:1",
            submitted_quantity=leg.quantity,
            now=NOW,
        )
        repository.transition_broker_order(broker_order_id, BrokerOrderStatus.SUBMITTING, now=NOW)

    first_external = f"{source_id}-external-a"
    first_status = (
        BrokerOrderStatus.FILLED
        if Decimal(first_filled_quantity) == Decimal(first_quantity)
        else BrokerOrderStatus.PARTIALLY_FILLED
    )
    repository.record_submission(
        f"{first_leg_id}-order",
        status=first_status,
        external_order_id=first_external,
        now=NOW,
    )
    repository.record_fill(
        Fill(
            id=f"{source_id}-fill-a",
            broker_order_id=f"{first_leg_id}-order",
            order_leg_id=first_leg_id,
            dedupe_key=f"{source_id}-deal-a",
            quantity=Decimal(first_filled_quantity),
            price=Decimal("100"),
            filled_at=NOW,
            received_at=NOW,
            account_id=account.id,
            external_order_id=first_external,
            evidence_reference=f"{source_id}-deal-a",
            metadata={
                "_broker_fill_account_id": account.id,
                "_instrument_id": first_instrument,
            },
        ),
        now=NOW,
        _validation_token=repository._fill_validation_capability(),
    )

    second_external = ""
    if include_terminal_zero_attempt:
        second_external = f"{source_id}-external-b"
        zero_result = BrokerSubmissionResult(
            broker_order_id=f"{second_leg_id}-order",
            accepted=True,
            status=terminal_zero_status,
            external_order_id=second_external,
            cumulative_filled_quantity=Decimal("0"),
            no_fill_asserted=True,
            authority=ADAPTER_ORDER_SNAPSHOT_AUTHORITY,
            submitted_quantity=Decimal("1"),
            instrument_id=second_instrument,
            raw_payload={
                "response": [{
                    "order_id": second_external,
                    "order_status": terminal_zero_status.value,
                    "code": second_instrument,
                    "qty": 1,
                    "dealt_qty": 0,
                    "dealt_avg_price": 0,
                }],
            },
        )
        zero_metadata = GenericOMS._submission_metadata(zero_result)
        zero_metadata["terminal_zero_fill_proof"] = terminal_zero_proof
        repository.transition_broker_order(
            f"{second_leg_id}-order",
            BrokerOrderStatus.WORKING,
            now=NOW,
        )
        repository.record_submission(
            f"{second_leg_id}-order",
            status=terminal_zero_status,
            external_order_id=second_external,
            metadata=zero_metadata,
            now=NOW,
        )
        repository.transition_leg(second_leg_id, LegStatus.WORKING, now=NOW)
        repository.transition_leg(
            second_leg_id,
            LegStatus.CANCELLED if terminal_zero_status is BrokerOrderStatus.CANCELLED else LegStatus.REJECTED,
            now=NOW,
        )
    else:
        # The compensated-partial resolver deliberately accepts only a
        # sibling with no broker attempt.  Keep that supported shape distinct
        # from the attempted terminal-zero proof used by residual validation.
        repository.transition_leg(second_leg_id, LegStatus.RECONCILIATION_REQUIRED, now=NOW)
        repository.transition_leg(second_leg_id, LegStatus.CANCELLED, now=NOW)
    current = repository.get_intent(source_id)
    if current is not None and current["status"] != IntentStatus.RECONCILIATION_REQUIRED.value:
        repository.transition_intent(source_id, IntentStatus.RECONCILIATION_REQUIRED, now=NOW)
    return source_id, first_instrument, second_instrument, first_external, second_external


def _seed_partial_enter_with_partial_sibling(
    repository,
    account,
    sleeves,
    *,
    source_id: str = "partial-enter-cancel-pair",
    first_quantity: str = "1",
    second_quantity: str = "1",
    second_filled_quantity: str = "0.4",
):
    """Seed two durable source attempts: one full and one positive partial."""
    first_instrument, second_instrument = sleeves[0].instrument_ids
    first_leg_id = f"{source_id}-leg-a"
    second_leg_id = f"{source_id}-leg-b"
    source = OrderIntent(
        id=source_id,
        idempotency_key=f"{source_id}-key",
        strategy_id="pilot-strategy",
        account_id=account.id,
        book_id=sleeves[0].book_id,
        action=IntentAction.ENTER,
        execution_policy=ExecutionPolicy(),
        legs=(
            OrderLeg(
                id=first_leg_id,
                intent_id=source_id,
                sequence=0,
                instrument_id=first_instrument,
                side=Side.BUY,
                quantity=Decimal(first_quantity),
                created_at=NOW,
                updated_at=NOW,
            ),
            OrderLeg(
                id=second_leg_id,
                intent_id=source_id,
                sequence=1,
                instrument_id=second_instrument,
                side=Side.SELL,
                quantity=Decimal(second_quantity),
                created_at=NOW,
                updated_at=NOW,
            ),
        ),
        created_at=NOW,
        updated_at=NOW,
    )
    repository.create_intent(source)
    repository.transition_intent(source_id, IntentStatus.RISK_APPROVED, now=NOW)
    repository.transition_intent(source_id, IntentStatus.SUBMITTING, now=NOW)
    for leg in source.legs:
        repository.transition_leg(leg.id, LegStatus.SUBMITTING, now=NOW)
        repository.create_broker_order(
            broker_order_id=f"{leg.id}-order",
            order_leg_id=leg.id,
            account_id=account.id,
            broker=account.broker,
            attempt_number=1,
            client_order_id=f"{leg.id}:1",
            submitted_quantity=leg.quantity,
            now=NOW,
        )
        repository.transition_broker_order(
            f"{leg.id}-order", BrokerOrderStatus.SUBMITTING, now=NOW
        )

    first_external = f"{source_id}-external-a"
    repository.record_submission(
        f"{first_leg_id}-order",
        status=BrokerOrderStatus.FILLED,
        external_order_id=first_external,
        now=NOW,
    )
    repository.record_fill(
        Fill(
            id=f"{source_id}-fill-a",
            broker_order_id=f"{first_leg_id}-order",
            order_leg_id=first_leg_id,
            dedupe_key=f"{source_id}-deal-a",
            quantity=Decimal(first_quantity),
            price=Decimal("100"),
            filled_at=NOW,
            received_at=NOW,
            account_id=account.id,
            external_order_id=first_external,
            evidence_reference=f"{source_id}-deal-a",
            metadata={
                "_broker_fill_account_id": account.id,
                "_instrument_id": first_instrument,
            },
        ),
        now=NOW,
        _validation_token=repository._fill_validation_capability(),
    )

    second_external = f"{source_id}-external-b"
    repository.record_submission(
        f"{second_leg_id}-order",
        status=BrokerOrderStatus.PARTIALLY_FILLED,
        external_order_id=second_external,
        now=NOW,
    )
    repository.record_fill(
        Fill(
            id=f"{source_id}-fill-b",
            broker_order_id=f"{second_leg_id}-order",
            order_leg_id=second_leg_id,
            dedupe_key=f"{source_id}-deal-b",
            quantity=Decimal(second_filled_quantity),
            price=Decimal("100"),
            filled_at=NOW,
            received_at=NOW,
            account_id=account.id,
            external_order_id=second_external,
            evidence_reference=f"{source_id}-deal-b",
            metadata={
                "_broker_fill_account_id": account.id,
                "_instrument_id": second_instrument,
            },
        ),
        now=NOW,
        _validation_token=repository._fill_validation_capability(),
    )
    current = repository.get_intent(source_id)
    if current is not None and current["status"] != IntentStatus.RECONCILIATION_REQUIRED.value:
        repository.transition_intent(source_id, IntentStatus.RECONCILIATION_REQUIRED, now=NOW)
    return source_id, first_instrument, second_instrument, first_external, second_external


def _seed_partial_exit_with_terminal_zero_sibling(
    repository,
    account,
    sleeves,
    *,
    source_id: str = "partial-exit-zero-sibling",
):
    """Seed a completed pair followed by a partial EXIT with a proven zero-fill sibling."""
    first_instrument, second_instrument = sleeves[0].instrument_ids
    entry_id = f"{source_id}-entry"
    entry_legs = (
        OrderLeg(
            id=f"{entry_id}-leg-a",
            intent_id=entry_id,
            sequence=0,
            instrument_id=first_instrument,
            side=Side.BUY,
            quantity=Decimal("1"),
            created_at=NOW,
            updated_at=NOW,
        ),
        OrderLeg(
            id=f"{entry_id}-leg-b",
            intent_id=entry_id,
            sequence=1,
            instrument_id=second_instrument,
            side=Side.SELL,
            quantity=Decimal("1"),
            created_at=NOW,
            updated_at=NOW,
        ),
    )
    entry = OrderIntent(
        id=entry_id,
        idempotency_key=f"{entry_id}-key",
        strategy_id="pilot-strategy",
        account_id=account.id,
        book_id=sleeves[0].book_id,
        action=IntentAction.ENTER,
        execution_policy=ExecutionPolicy(),
        legs=entry_legs,
        created_at=NOW,
        updated_at=NOW,
    )
    repository.create_intent(entry)
    repository.transition_intent(entry_id, IntentStatus.RISK_APPROVED, now=NOW)
    repository.transition_intent(entry_id, IntentStatus.SUBMITTING, now=NOW)
    for leg in entry_legs:
        repository.transition_leg(leg.id, LegStatus.SUBMITTING, now=NOW)
        order_id = f"{leg.id}-order"
        repository.create_broker_order(
            broker_order_id=order_id,
            order_leg_id=leg.id,
            account_id=account.id,
            broker=account.broker,
            attempt_number=1,
            client_order_id=f"{leg.id}:1",
            submitted_quantity=leg.quantity,
            now=NOW,
        )
        repository.transition_broker_order(order_id, BrokerOrderStatus.SUBMITTING, now=NOW)
        external_id = f"{leg.id}-external"
        repository.record_submission(
            order_id,
            status=BrokerOrderStatus.FILLED,
            external_order_id=external_id,
            now=NOW,
        )
        repository.record_fill(
            Fill(
                id=f"{leg.id}-fill",
                broker_order_id=order_id,
                order_leg_id=leg.id,
                dedupe_key=f"{leg.id}-deal",
                quantity=leg.quantity,
                price=Decimal("100"),
                filled_at=NOW,
                received_at=NOW,
                account_id=account.id,
                external_order_id=external_id,
                evidence_reference=f"{leg.id}-deal",
                metadata={
                    "_broker_fill_account_id": account.id,
                    "_instrument_id": leg.instrument_id,
                },
            ),
            now=NOW,
            _validation_token=repository._fill_validation_capability(),
        )
    repository.transition_intent(entry_id, IntentStatus.COMPLETED, now=NOW)

    exit_legs = (
        OrderLeg(
            id=f"{source_id}-leg-a",
            intent_id=source_id,
            sequence=0,
            instrument_id=first_instrument,
            side=Side.SELL,
            quantity=Decimal("1"),
            created_at=NOW,
            updated_at=NOW,
        ),
        OrderLeg(
            id=f"{source_id}-leg-b",
            intent_id=source_id,
            sequence=1,
            instrument_id=second_instrument,
            side=Side.BUY,
            quantity=Decimal("1"),
            created_at=NOW,
            updated_at=NOW,
        ),
    )
    source = OrderIntent(
        id=source_id,
        idempotency_key=f"{source_id}-key",
        strategy_id="pilot-strategy",
        account_id=account.id,
        book_id=sleeves[0].book_id,
        action=IntentAction.EXIT,
        execution_policy=ExecutionPolicy(),
        legs=exit_legs,
        created_at=NOW,
        updated_at=NOW,
    )
    repository.create_intent(source)
    repository.transition_intent(source_id, IntentStatus.RISK_APPROVED, now=NOW)
    repository.transition_intent(source_id, IntentStatus.SUBMITTING, now=NOW)
    for leg in exit_legs:
        repository.transition_leg(leg.id, LegStatus.SUBMITTING, now=NOW)
        order_id = f"{leg.id}-order"
        repository.create_broker_order(
            broker_order_id=order_id,
            order_leg_id=leg.id,
            account_id=account.id,
            broker=account.broker,
            attempt_number=1,
            client_order_id=f"{leg.id}:1",
            submitted_quantity=leg.quantity,
            now=NOW,
        )
        repository.transition_broker_order(order_id, BrokerOrderStatus.SUBMITTING, now=NOW)

    first_order_id = f"{source_id}-leg-a-order"
    first_external = f"{source_id}-external-a"
    repository.record_submission(
        first_order_id,
        status=BrokerOrderStatus.FILLED,
        external_order_id=first_external,
        now=NOW,
    )
    repository.record_fill(
        Fill(
            id=f"{source_id}-fill-a",
            broker_order_id=first_order_id,
            order_leg_id=f"{source_id}-leg-a",
            dedupe_key=f"{source_id}-deal-a",
            quantity=Decimal("1"),
            price=Decimal("101"),
            filled_at=NOW,
            received_at=NOW,
            account_id=account.id,
            external_order_id=first_external,
            evidence_reference=f"{source_id}-deal-a",
            metadata={
                "_broker_fill_account_id": account.id,
                "_instrument_id": first_instrument,
            },
        ),
        now=NOW,
        _validation_token=repository._fill_validation_capability(),
    )

    second_order_id = f"{source_id}-leg-b-order"
    second_external = f"{source_id}-external-b"
    zero_result = BrokerSubmissionResult(
        broker_order_id=second_order_id,
        # The provider created an external order and then terminally rejected
        # it; ``accepted`` describes the transport admission, not a fill.
        accepted=True,
        status=BrokerOrderStatus.REJECTED,
        external_order_id=second_external,
        cumulative_filled_quantity=Decimal("0"),
        no_fill_asserted=True,
        authority=ADAPTER_ORDER_SNAPSHOT_AUTHORITY,
        submitted_quantity=Decimal("1"),
        instrument_id=second_instrument,
        raw_payload={
            "response": [{
                "order_id": second_external,
                "order_status": BrokerOrderStatus.REJECTED.value,
                "code": second_instrument,
                "qty": 1,
                "dealt_qty": 0,
                "dealt_avg_price": 0,
            }],
        },
    )
    zero_metadata = GenericOMS._submission_metadata(zero_result)
    zero_metadata["terminal_zero_fill_proof"] = True
    repository.transition_broker_order(second_order_id, BrokerOrderStatus.WORKING, now=NOW)
    repository.record_submission(
        second_order_id,
        status=BrokerOrderStatus.REJECTED,
        external_order_id=second_external,
        metadata=zero_metadata,
        now=NOW,
    )
    repository.transition_leg(f"{source_id}-leg-b", LegStatus.REJECTED, now=NOW)
    current = repository.get_intent(source_id)
    if current is not None and current["status"] != IntentStatus.RECONCILIATION_REQUIRED.value:
        repository.transition_intent(source_id, IntentStatus.RECONCILIATION_REQUIRED, now=NOW)
    return {
        "entry_id": entry_id,
        "source_id": source_id,
        "first_instrument": first_instrument,
        "second_instrument": second_instrument,
        "first_external": first_external,
        "second_external": second_external,
        "entry_external_ids": (
            f"{entry_legs[0].id}-external",
            f"{entry_legs[1].id}-external",
        ),
    }


def _install_partial_facts(
    adapter,
    account,
    instrument_id,
    external_order_id,
    *,
    quantity: str = "1",
    dedupe_key: str | None = None,
    extra_fills=(),
):
    observed_quantity = Decimal(quantity)
    if dedupe_key is None:
        # Keep the fake provider row bound to the durable source evidence.
        # The strict proof route intentionally rejects a convenient fill with
        # the right order/quantity but a different deal identity.
        dedupe_key = external_order_id.replace("-external", "-deal")
    adapter.facts = replace(
        adapter.facts,
        positions=(
            PositionSnapshot(
                id="partial-source-position",
                broker_snapshot_id="partial-source-snapshot",
                account_id=account.id,
                instrument_id=instrument_id,
                signed_quantity=observed_quantity,
                average_price=Decimal("100"),
                captured_at=NOW,
            ),
        ),
        open_orders=(),
        fills=(
            BrokerFill(
                external_order_id=external_order_id,
                dedupe_key=dedupe_key,
                quantity=observed_quantity,
                price=Decimal("100"),
                filled_at=NOW,
                received_at=NOW,
                account_id=account.id,
                evidence_reference=f"{external_order_id}:{dedupe_key}",
                instrument_id=instrument_id,
            ),
            *tuple(extra_fills),
        ),
    )


def _install_terminal_zero_history(
    adapter,
    account,
    *,
    instrument_id: str,
    external_order_id: str,
    status: BrokerOrderStatus = BrokerOrderStatus.CANCELLED,
    fills=(),
    additional_orders=(),
):
    """Install a bounded history response for an attempted zero-fill order."""
    zero_order = BrokerOrderSnapshot(
        id=f"history-{external_order_id}",
        broker_snapshot_id=f"history-snapshot-{external_order_id}",
        account_id=account.id,
        instrument_id=instrument_id,
        external_order_id=external_order_id,
        side=Side.SELL,
        quantity=Decimal("1"),
        filled_quantity=Decimal("0"),
        status=status,
        captured_at=NOW,
        order_time=NOW,
    )

    def get_historical_order_facts(_account, requested_start, requested_end):  # type: ignore[no-untyped-def]
        return BrokerHistoricalOrderFacts(
            account_id=account.id,
            requested_start=requested_start,
            requested_end=requested_end,
            captured_at=NOW,
            complete=True,
            orders=(zero_order, *tuple(additional_orders)),
            fills=tuple(fills),
            execution_evidence_mode=ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS,
            execution_evidence_scope=frozenset({"HISTORICAL_ORDER_SNAPSHOTS"}),
        )

    adapter.get_historical_order_facts = get_historical_order_facts


class _CancelPartialAdapter(PilotFakeAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.cancel_calls: list[str] = []
        self._cancelled: dict[str, BrokerOrderSnapshot] = {}

    def cancel_order(self, account, external_order_id):  # type: ignore[no-untyped-def]
        self.cancel_calls.append(str(external_order_id))
        current = next(
            item
            for item in self.facts.open_orders
            if item.external_order_id == str(external_order_id)
        )
        cancelled = replace(current, status=BrokerOrderStatus.CANCELLED)
        self._cancelled[str(external_order_id)] = cancelled
        self.facts = replace(self.facts, open_orders=())
        return BrokerSubmissionResult(
            broker_order_id="cancel-partial-order",
            accepted=True,
            status=BrokerOrderStatus.CANCELLED,
            external_order_id=str(external_order_id),
            cumulative_filled_quantity=Decimal("1"),
            submitted_quantity=current.quantity,
            instrument_id=current.instrument_id,
        )

    def get_order(self, account, external_order_id):  # type: ignore[no-untyped-def]
        return self._cancelled.get(str(external_order_id)) or super().get_order(account, external_order_id)


class _FillBeforeCancelAdapter(_CancelPartialAdapter):
    """Cancellation ACK is followed by an authoritative full-fill snapshot."""

    def cancel_order(self, account, external_order_id):  # type: ignore[no-untyped-def]
        external_order_id = str(external_order_id)
        self.cancel_calls.append(external_order_id)
        current = next(
            item for item in self.facts.open_orders
            if item.external_order_id == external_order_id
        )
        self._cancelled[external_order_id] = replace(
            current,
            status=BrokerOrderStatus.FILLED,
            filled_quantity=current.quantity,
        )
        self.facts = replace(self.facts, open_orders=())
        return BrokerSubmissionResult(
            broker_order_id="cancel-fill-race-order",
            accepted=True,
            status=BrokerOrderStatus.CANCELLED,
            external_order_id=external_order_id,
            cumulative_filled_quantity=current.filled_quantity,
            submitted_quantity=current.quantity,
            instrument_id=current.instrument_id,
        )


class _AmbiguousCancelAdapter(_CancelPartialAdapter):
    """The provider cannot establish whether the cancel took effect."""

    def cancel_order(self, account, external_order_id):  # type: ignore[no-untyped-def]
        external_order_id = str(external_order_id)
        self.cancel_calls.append(external_order_id)
        current = next(
            item for item in self.facts.open_orders
            if item.external_order_id == external_order_id
        )
        return BrokerSubmissionResult(
            broker_order_id="cancel-ambiguous-order",
            accepted=None,
            status=BrokerOrderStatus.UNKNOWN,
            external_order_id=external_order_id,
            cumulative_filled_quantity=None,
            submitted_quantity=current.quantity,
            instrument_id=current.instrument_id,
            ambiguous=True,
        )


def _install_cancel_partial_facts(adapter, account, instrument_id, external_id):
    snapshot = BrokerOrderSnapshot(
        id="fresh-partial-order",
        broker_snapshot_id="fresh-partial-snapshot",
        account_id=account.id,
        instrument_id=instrument_id,
        external_order_id=external_id,
        side=Side.BUY,
        quantity=Decimal("2"),
        filled_quantity=Decimal("1"),
        status=BrokerOrderStatus.PARTIALLY_FILLED,
        captured_at=NOW,
        order_time=NOW,
    )
    adapter.facts = replace(
        adapter.facts,
        positions=(
            PositionSnapshot(
                id="cancel-partial-position",
                broker_snapshot_id="cancel-partial-snapshot",
                account_id=account.id,
                instrument_id=instrument_id,
                signed_quantity=Decimal("1"),
                average_price=Decimal("100"),
                captured_at=NOW,
            ),
        ),
        open_orders=(snapshot,),
        fills=(
            BrokerFill(
                external_order_id=external_id,
                dedupe_key="cancel-partial-deal",
                quantity=Decimal("1"),
                price=Decimal("100"),
                filled_at=NOW,
                received_at=NOW,
                account_id=account.id,
                instrument_id=instrument_id,
            ),
        ),
    )


def test_partial_enter_residual_exit_is_exact_and_idempotent(tmp_path):
    account, sleeves, repository, adapter, runner = make_runner(tmp_path)
    _intent, _leg_id, external_id = _seed_partial_source(
        repository,
        account,
        action=IntentAction.ENTER,
        source_id="partial-enter",
        instrument_id=sleeves[0].instrument_ids[0],
    )
    from src.trading_core.ports import BrokerFill

    adapter.facts = replace(
        adapter.facts,
        positions=(
            PositionSnapshot(
                id="fresh-partial-enter-position",
                broker_snapshot_id="fresh-partial-enter-snapshot",
                account_id=account.id,
                instrument_id=sleeves[0].instrument_ids[0],
                signed_quantity=Decimal("1"),
                average_price=Decimal("100"),
                captured_at=NOW,
            ),
        ),
        fills=(
            BrokerFill(
                external_order_id=external_id,
                dedupe_key="partial-enter-deal",
                quantity=Decimal("1"),
                price=Decimal("100"),
                filled_at=NOW,
                received_at=NOW,
                account_id=account.id,
                instrument_id=sleeves[0].instrument_ids[0],
            ),
        ),
    )
    result = runner.submit_verified_residual_exit(
        account=account,
        source_intent_id="partial-enter",
        expected_external_order_ids={sleeves[0].instrument_ids[0]: external_id},
        spec=make_spec(account, sleeves),
    )
    assert result["residual_exit_intent_id"]
    assert result["broker_contacted"] is True
    assert result["broker_submission_count"] == 1
    assert adapter.submit_calls == [result["broker_order_attempt_ids"][0]]
    residual_snapshot = repository.get_intent(result["residual_exit_intent_id"])
    assert residual_snapshot is not None
    assert residual_snapshot["metadata"]["stage6_submission"]["mode"] == "RESIDUAL_EXIT"
    assert residual_snapshot["metadata"]["stage6_submission"]["correlation"]["source_intent_id"] == "partial-enter"
    replay = runner.submit_verified_residual_exit(
        account=account,
        source_intent_id="partial-enter",
        expected_external_order_ids={sleeves[0].instrument_ids[0]: external_id},
        spec=make_spec(account, sleeves),
    )
    assert replay["residual_exit_intent_id"] == result["residual_exit_intent_id"]
    assert len(adapter.submit_calls) == 1


@pytest.mark.parametrize(
    "terminal_zero_status",
    (BrokerOrderStatus.CANCELLED, BrokerOrderStatus.REJECTED),
    ids=("cancelled", "rejected"),
)
def test_residual_accepts_proven_terminal_zero_fill_sibling_but_rejects_ambiguous_attempt(
    tmp_path, terminal_zero_status
):
    account, sleeves, repository, adapter, runner = make_runner(tmp_path / "proven")
    source_id, first_instrument, second_instrument, first_external, second_external = (
        _seed_partial_enter_with_terminal_zero_sibling(
            repository,
            account,
            sleeves,
            terminal_zero_proof=True,
            terminal_zero_status=terminal_zero_status,
        )
    )
    _install_partial_facts(adapter, account, first_instrument, first_external)
    _install_terminal_zero_history(
        adapter,
        account,
        instrument_id=second_instrument,
        external_order_id=second_external,
        status=terminal_zero_status,
    )

    accepted = runner.submit_verified_residual_exit(
        account=account,
        source_intent_id=source_id,
        expected_external_order_ids={first_instrument: first_external},
        spec=make_spec(account, sleeves),
    )
    assert accepted["residual_exit_intent_id"]
    assert adapter.submit_calls == [accepted["broker_order_attempt_ids"][0]]

    ambiguous_account, ambiguous_sleeves, ambiguous_repository, ambiguous_adapter, ambiguous_runner = make_runner(
        tmp_path / "ambiguous"
    )
    ambiguous_source, ambiguous_instrument, _instrument, ambiguous_external, _external = (
        _seed_partial_enter_with_terminal_zero_sibling(
            ambiguous_repository,
            ambiguous_account,
            ambiguous_sleeves,
            terminal_zero_proof=False,
        )
    )
    _install_partial_facts(
        ambiguous_adapter,
        ambiguous_account,
        ambiguous_instrument,
        ambiguous_external,
    )
    with pytest.raises(OMSExecutionError, match="no-fill or ambiguous submission evidence"):
        ambiguous_runner.submit_verified_residual_exit(
            account=ambiguous_account,
            source_intent_id=ambiguous_source,
            expected_external_order_ids={ambiguous_instrument: ambiguous_external},
            spec=make_spec(ambiguous_account, ambiguous_sleeves),
        )
    assert ambiguous_adapter.submit_calls == []


def test_residual_rejects_historical_late_fill_for_proven_zero_fill_sibling(tmp_path):
    account, sleeves, repository, adapter, runner = make_runner(tmp_path)
    source_id, first_instrument, second_instrument, first_external, second_external = (
        _seed_partial_enter_with_terminal_zero_sibling(
            repository,
            account,
            sleeves,
            terminal_zero_proof=True,
        )
    )
    late_fill = BrokerFill(
        external_order_id=second_external,
        dedupe_key="late-zero-sibling-deal",
        quantity=Decimal("1"),
        price=Decimal("101"),
        filled_at=NOW,
        received_at=NOW,
        account_id=account.id,
        instrument_id=second_instrument,
    )
    _install_partial_facts(adapter, account, first_instrument, first_external)
    _install_terminal_zero_history(
        adapter,
        account,
        instrument_id=second_instrument,
        external_order_id=second_external,
        fills=(late_fill,),
    )
    with pytest.raises(OMSExecutionError, match="late fill for a durably proven zero-fill"):
        runner.submit_verified_residual_exit(
            account=account,
            source_intent_id=source_id,
            expected_external_order_ids={first_instrument: first_external},
            spec=make_spec(account, sleeves),
        )
    assert adapter.submit_calls == []


def test_compensated_resolver_accepts_partial_source_leg_after_terminal_cancel(tmp_path):
    """A positive cancelled partial source is compensated, never treated as full."""
    account = make_account()
    sleeves = make_sleeves()
    repository = make_repository(tmp_path, account, sleeves)
    adapter = NettedPilotAdapter()
    run_now = NOW + timedelta(seconds=1)
    runner = Stage6PilotRunner(
        repository,
        GenericOMS(repository, adapter, clock=lambda: run_now),
        clock=lambda: run_now,
    )
    source_id, first_instrument, _second_instrument, first_external, _second_external = (
        _seed_partial_enter_with_terminal_zero_sibling(
            repository,
            account,
            sleeves,
            terminal_zero_proof=False,
            first_quantity="2",
            first_filled_quantity="1",
            include_terminal_zero_attempt=False,
        )
    )
    _install_partial_facts(
        adapter,
        account,
        first_instrument,
        first_external,
        quantity="1",
        dedupe_key=f"{source_id}-deal-a",
    )
    source_snapshot = BrokerOrderSnapshot(
        id="history-partial-source",
        broker_snapshot_id="history-partial-source",
        account_id=account.id,
        instrument_id=first_instrument,
        external_order_id=first_external,
        side=Side.BUY,
        quantity=Decimal("2"),
        filled_quantity=Decimal("1"),
        status=BrokerOrderStatus.PARTIALLY_FILLED,
        captured_at=NOW,
        order_time=NOW,
    )
    adapter._filled_orders[first_external] = replace(
        source_snapshot,
        status=BrokerOrderStatus.CANCELLED,
    )

    def history_without_zero(_account, requested_start, requested_end):  # type: ignore[no-untyped-def]
        snapshots = tuple(
            item for external_id, item in adapter._filled_orders.items()
            if external_id != first_external
        ) + (source_snapshot,)
        return BrokerHistoricalOrderFacts(
            account_id=account.id,
            requested_start=requested_start,
            requested_end=requested_end,
            captured_at=NOW,
            complete=True,
            orders=snapshots,
            fills=tuple(adapter.facts.fills),
            execution_evidence_mode=ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS,
            execution_evidence_scope=frozenset({"HISTORICAL_ORDER_SNAPSHOTS"}),
        )

    adapter.get_historical_order_facts = history_without_zero
    residual = runner.submit_verified_residual_exit(
        account=account,
        source_intent_id=source_id,
        expected_external_order_ids={first_instrument: first_external},
        spec=make_spec(account, sleeves),
    )
    residual_id = str(residual["residual_exit_intent_id"])
    recovered = runner.recover(account)
    assert recovered
    assert repository.get_intent(residual_id)["status"] == IntentStatus.FILLED.value
    resolved = runner.resolve_compensated_partial(make_spec(account, sleeves), intent_id=source_id)
    assert resolved["status"] == IntentStatus.CANCELLED.value
    assert repository.get_intent(source_id)["status"] == IntentStatus.CANCELLED.value


def test_cancel_known_partial_performs_one_exact_cancel_then_requeries(tmp_path):
    account = make_account()
    sleeves = make_sleeves()
    repository = make_repository(tmp_path, account, sleeves)
    adapter = _CancelPartialAdapter()
    oms = GenericOMS(repository, adapter, clock=lambda: NOW)
    runner = Stage6PilotRunner(repository, oms, clock=lambda: NOW)
    source, leg_id, external_id = _seed_partial_source(
        repository,
        account,
        action=IntentAction.ENTER,
        source_id="cancel-known-partial",
        instrument_id=sleeves[0].instrument_ids[0],
    )
    snapshot = BrokerOrderSnapshot(
        id="fresh-partial-order",
        broker_snapshot_id="fresh-partial-snapshot",
        account_id=account.id,
        instrument_id=sleeves[0].instrument_ids[0],
        external_order_id=external_id,
        side=Side.BUY,
        quantity=Decimal("2"),
        filled_quantity=Decimal("1"),
        status=BrokerOrderStatus.PARTIALLY_FILLED,
        captured_at=NOW,
        order_time=NOW,
    )
    adapter.facts = replace(
        adapter.facts,
        positions=(
            PositionSnapshot(
                id="cancel-partial-position",
                broker_snapshot_id="cancel-partial-snapshot",
                account_id=account.id,
                instrument_id=sleeves[0].instrument_ids[0],
                signed_quantity=Decimal("1"),
                average_price=Decimal("100"),
                captured_at=NOW,
            ),
        ),
        open_orders=(snapshot,),
        fills=(
            BrokerFill(
                external_order_id=external_id,
                dedupe_key="cancel-partial-deal",
                quantity=Decimal("1"),
                price=Decimal("100"),
                filled_at=NOW,
                received_at=NOW,
                account_id=account.id,
                instrument_id=sleeves[0].instrument_ids[0],
            ),
        ),
    )

    result = runner.cancel_known_partial(
        make_spec(account, sleeves),
        intent_id=source.id,
        external_order_id=external_id,
    )

    assert result["cancel_count"] == 1, result
    assert result["external_order_id"] == external_id
    assert adapter.cancel_calls == [external_id]
    assert adapter.submit_calls == []
    assert len(repository.broker_orders_for_leg(leg_id)) == 1
    assert len(repository.fills_for_leg(leg_id)) == 1
    assert result["recovered"]["id"] == source.id


def test_cancel_known_partial_blocks_fill_before_cancel_race(tmp_path):
    account = make_account()
    sleeves = make_sleeves()
    repository = make_repository(tmp_path, account, sleeves)
    adapter = _FillBeforeCancelAdapter()
    runner = Stage6PilotRunner(
        repository,
        GenericOMS(repository, adapter, clock=lambda: NOW),
        clock=lambda: NOW,
    )
    source, leg_id, external_id = _seed_partial_source(
        repository,
        account,
        action=IntentAction.ENTER,
        source_id="cancel-fill-before-cancel",
        instrument_id=sleeves[0].instrument_ids[0],
    )
    _install_cancel_partial_facts(
        adapter,
        account,
        sleeves[0].instrument_ids[0],
        external_id,
    )

    result = runner.cancel_known_partial(
        make_spec(account, sleeves),
        intent_id=source.id,
        external_order_id=external_id,
    )

    assert result["cancel_count"] == 1
    assert adapter.cancel_calls == [external_id]
    assert adapter.submit_calls == []
    assert len(repository.broker_orders_for_leg(leg_id)) == 1
    assert repository.get_intent(source.id)["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] in {"BROKER_SNAPSHOT_MISMATCH", "TERMINAL_FILL_CONTRADICTION", "FILL_EVIDENCE_REQUIRED"}
        for issue in repository.open_reconciliation_issues(account.id)
    )


def test_cancel_known_partial_blocks_ambiguous_cancel_without_retry(tmp_path):
    account = make_account()
    sleeves = make_sleeves()
    repository = make_repository(tmp_path, account, sleeves)
    adapter = _AmbiguousCancelAdapter()
    runner = Stage6PilotRunner(
        repository,
        GenericOMS(repository, adapter, clock=lambda: NOW),
        clock=lambda: NOW,
    )
    source, leg_id, external_id = _seed_partial_source(
        repository,
        account,
        action=IntentAction.ENTER,
        source_id="cancel-ambiguous-outcome",
        instrument_id=sleeves[0].instrument_ids[0],
    )
    _install_cancel_partial_facts(
        adapter,
        account,
        sleeves[0].instrument_ids[0],
        external_id,
    )

    with pytest.raises(OMSExecutionError, match="cancellation outcome is ambiguous"):
        runner.cancel_known_partial(
            make_spec(account, sleeves),
            intent_id=source.id,
            external_order_id=external_id,
        )

    assert adapter.cancel_calls == [external_id]
    assert adapter.submit_calls == []
    assert len(repository.broker_orders_for_leg(leg_id)) == 1
    assert repository.get_intent(source.id)["status"] == IntentStatus.RECONCILIATION_REQUIRED.value
    assert any(
        issue["category"] == "CANCEL_AMBIGUITY"
        for issue in repository.open_reconciliation_issues(account.id)
    )
    assert any(
        action["action_key"].startswith("CANCEL_AMBIGUITY:")
        for action in repository.open_recovery_actions(account.id)
    )


def test_direct_oms_residual_exit_rejects_stale_authoritative_facts(tmp_path):
    account, sleeves, repository, adapter, runner = make_runner(tmp_path)
    _intent, _leg_id, external_id = _seed_partial_source(
        repository,
        account,
        action=IntentAction.ENTER,
        source_id="stale-residual",
        instrument_id=sleeves[0].instrument_ids[0],
    )
    _install_partial_facts(
        adapter,
        account,
        sleeves[0].instrument_ids[0],
        external_id,
    )
    adapter.facts = replace(adapter.facts, captured_at=NOW - timedelta(seconds=61))

    with pytest.raises(OMSExecutionError, match="authoritative account facts are stale"):
        runner.oms.submit_verified_residual_exit(
            account=account,
            source_intent_id="stale-residual",
            expected_external_order_ids={sleeves[0].instrument_ids[0]: external_id},
            _before_submit_leg=lambda _intent, _leg, _account: None,
        )
    assert adapter.submit_calls == []


def test_residual_exit_uses_shared_fact_metadata_and_fill_identity_gate(tmp_path):
    account, sleeves, repository, adapter, runner = make_runner(tmp_path / "metadata")
    _intent, _leg_id, external_id = _seed_partial_source(
        repository,
        account,
        action=IntentAction.ENTER,
        source_id="unsafe-residual-metadata",
        instrument_id=sleeves[0].instrument_ids[0],
    )
    _install_partial_facts(adapter, account, sleeves[0].instrument_ids[0], external_id)
    adapter.facts = replace(adapter.facts, metadata={"raw": {"dealt_qty": "1"}})

    with pytest.raises(OMSExecutionError, match="metadata"):
        runner.submit_verified_residual_exit(
            account=account,
            source_intent_id="unsafe-residual-metadata",
            expected_external_order_ids={sleeves[0].instrument_ids[0]: external_id},
            spec=make_spec(account, sleeves),
        )
    assert adapter.submit_calls == []

    account2, sleeves2, repository2, adapter2, runner2 = make_runner(tmp_path / "fill-conflict")
    _intent2, _leg_id2, external_id2 = _seed_partial_source(
        repository2,
        account2,
        action=IntentAction.ENTER,
        source_id="unsafe-residual-fill",
        instrument_id=sleeves2[0].instrument_ids[0],
    )
    _install_partial_facts(adapter2, account2, sleeves2[0].instrument_ids[0], external_id2)
    original_fill = adapter2.facts.fills[0]
    adapter2.facts = replace(
        adapter2.facts,
        fills=(
            original_fill,
            replace(
                original_fill,
                external_fill_id="conflicting-provider-fill",
                dedupe_key="conflicting-provider-deal",
                evidence_reference="conflicting-provider-deal",
                price=Decimal("101"),
            ),
        ),
    )

    with pytest.raises(OMSExecutionError, match="conflicts with durable evidence"):
        runner2.submit_verified_residual_exit(
            account=account2,
            source_intent_id="unsafe-residual-fill",
            expected_external_order_ids={sleeves2[0].instrument_ids[0]: external_id2},
            spec=make_spec(account2, sleeves2),
        )
    assert adapter2.submit_calls == []


@pytest.mark.parametrize(
    "evidence_mode",
    (
        ExecutionEvidenceMode.UNAVAILABLE,
        ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS,
    ),
)
def test_direct_oms_compensating_exit_rejects_unusable_execution_evidence(tmp_path, evidence_mode):
    """The direct proof primitive must enforce the same evidence gate as Stage 6."""

    account, sleeves, repository, adapter, runner = make_runner(tmp_path)
    report = runner.run(
        make_spec(account, sleeves, targets=make_targets(sleeves, quantities=("1", "1"))),
        mode=Stage6RunMode.SIM_SUBMIT,
    )
    assert report.preflight_passed is True
    source_row = repository.book_intents(account.id, book_id=sleeves[0].book_id)[0]
    source_id = str(source_row["id"])
    source = repository.get_intent(source_id)
    assert source is not None
    expected_external_order_ids = {
        str(leg["instrument_id"]): str(
            repository.broker_orders_for_leg(str(leg["id"]))[0]["external_order_id"]
        )
        for leg in source["legs"]
    }
    adapter.facts = replace(
        adapter.facts,
        execution_evidence_mode=evidence_mode,
    )
    before_submit_count = len(adapter.submit_calls)

    with pytest.raises(OMSExecutionError, match="usable execution evidence"):
        runner.oms.submit_verified_compensating_exit(
            source_intent_id=source_id,
            expected_external_order_ids=expected_external_order_ids,
            account=account,
        )

    assert len(adapter.submit_calls) == before_submit_count


def test_direct_oms_compensating_exit_rejects_stale_authoritative_facts(tmp_path):
    """The full-fill compensating primitive must fail before any exit attempt."""

    account = make_account()
    sleeves = make_sleeves()
    repository = make_repository(tmp_path, account, sleeves)
    adapter = MoomooAckPilotAdapter()
    oms = GenericOMS(repository, adapter, clock=lambda: NOW)
    runner = Stage6PilotRunner(repository, oms, clock=lambda: NOW)

    report = runner.run(make_spec(account, sleeves), mode=Stage6RunMode.SIM_SUBMIT)
    assert report.preflight_passed is True
    source_row = repository.book_intents(account.id, book_id=sleeves[0].book_id)[0]
    source_id = str(source_row["id"])
    source = repository.get_intent(source_id)
    assert source is not None
    expected_external_order_ids = {
        str(leg["instrument_id"]): str(
            repository.broker_orders_for_leg(str(leg["id"]))[0]["external_order_id"]
        )
        for leg in source["legs"]
    }
    before_submit_count = len(adapter.submit_calls)
    adapter.facts = replace(adapter.facts, captured_at=NOW - timedelta(seconds=61))

    with pytest.raises(OMSExecutionError, match="authoritative account facts are stale"):
        oms.submit_verified_compensating_exit(
            source_intent_id=source_id,
            expected_external_order_ids=expected_external_order_ids,
            account=account,
        )

    assert len(adapter.submit_calls) == before_submit_count


def test_direct_oms_compensating_exit_rejects_changed_selected_fill_identity(tmp_path):
    """A changed provider row cannot be hidden behind the selected order ID."""

    account, sleeves, repository, adapter, runner = make_runner(tmp_path)
    report = runner.run(make_spec(account, sleeves), mode=Stage6RunMode.SIM_SUBMIT)
    assert report.preflight_passed is True
    source_row = repository.book_intents(account.id, book_id=sleeves[0].book_id)[0]
    source_id = str(source_row["id"])
    source = repository.get_intent(source_id)
    assert source is not None
    expected_external_order_ids = {
        str(leg["instrument_id"]): str(
            repository.broker_orders_for_leg(str(leg["id"]))[0]["external_order_id"]
        )
        for leg in source["legs"]
    }
    selected_external_ids = set(expected_external_order_ids.values())
    selected_instruments = {str(leg["instrument_id"]) for leg in source["legs"]}
    selected_fills = tuple(
        fill for fill in adapter.facts.fills if str(fill.external_order_id) in selected_external_ids
    )
    assert len(selected_fills) == len(source["legs"])
    changed = replace(
        selected_fills[0],
        external_fill_id="conflicting-selected-fill",
        dedupe_key="conflicting-selected-deal",
        evidence_reference="conflicting-selected-deal",
        price=selected_fills[0].price + Decimal("1"),
    )
    adapter.facts = replace(
        adapter.facts,
        positions=tuple(
            position
            for position in adapter.facts.positions
            if position.instrument_id in selected_instruments
        ),
        fills=(selected_fills[0], changed, *selected_fills[1:]),
    )
    before_submit_count = len(adapter.submit_calls)

    with pytest.raises(OMSExecutionError, match="conflicts with durable evidence"):
        runner.oms.submit_verified_compensating_exit(
            source_intent_id=source_id,
            expected_external_order_ids=expected_external_order_ids,
            account=account,
        )

    assert len(adapter.submit_calls) == before_submit_count
    assert repository.book_intents(account.id, book_id=sleeves[0].book_id) == [source_row]


def test_partial_exit_residual_uses_remaining_book_basis(tmp_path):
    account, sleeves, repository, adapter, runner = make_runner(tmp_path)
    # A durable managed +2 basis exists before the partial SELL 1 source.
    _seed_partial_source(
        repository,
        account,
        action=IntentAction.ENTER,
        source_id="prior-entry",
        instrument_id=sleeves[0].instrument_ids[0],
        quantity="2",
        filled_quantity="2",
    )
    repository.transition_intent("prior-entry", IntentStatus.COMPLETED, now=NOW)
    _intent, _leg_id, external_id = _seed_partial_source(
        repository,
        account,
        action=IntentAction.EXIT,
        source_id="partial-exit",
        instrument_id=sleeves[0].instrument_ids[0],
        quantity="2",
        filled_quantity="1",
        side=Side.SELL,
    )
    from src.trading_core.ports import BrokerFill

    adapter.facts = replace(
        adapter.facts,
        positions=(
            PositionSnapshot(
                id="fresh-partial-exit-position",
                broker_snapshot_id="fresh-partial-exit-snapshot",
                account_id=account.id,
                instrument_id=sleeves[0].instrument_ids[0],
                signed_quantity=Decimal("1"),
                average_price=Decimal("100"),
                captured_at=NOW,
            ),
        ),
        fills=(
            BrokerFill(
                external_order_id=external_id,
                dedupe_key="partial-exit-deal",
                quantity=Decimal("1"),
                price=Decimal("100"),
                filled_at=NOW,
                received_at=NOW,
                account_id=account.id,
                instrument_id=sleeves[0].instrument_ids[0],
            ),
        ),
    )
    result = runner.submit_verified_residual_exit(
        account=account,
        source_intent_id="partial-exit",
        expected_external_order_ids={sleeves[0].instrument_ids[0]: external_id},
        spec=make_spec(account, sleeves),
    )
    residual = repository.get_intent(result["residual_exit_intent_id"])
    assert residual is not None
    assert residual["legs"][0]["side"] == Side.SELL.value
    assert Decimal(str(residual["legs"][0]["quantity"])) == Decimal("1")


def test_partial_residual_rth_closure_blocks_before_any_order(tmp_path):
    account, sleeves, repository, adapter, runner = make_runner(tmp_path)
    _intent, _leg_id, external_id = _seed_partial_source(
        repository,
        account,
        action=IntentAction.ENTER,
        source_id="partial-rth",
        instrument_id=sleeves[0].instrument_ids[0],
    )
    from src.trading_core.ports import BrokerFill

    adapter.facts = replace(
        adapter.facts,
        positions=(
            PositionSnapshot(
                id="fresh-partial-rth-position",
                broker_snapshot_id="fresh-partial-rth-snapshot",
                account_id=account.id,
                instrument_id=sleeves[0].instrument_ids[0],
                signed_quantity=Decimal("1"),
                average_price=Decimal("100"),
                captured_at=NOW,
            ),
        ),
        fills=(
            BrokerFill(
                external_order_id=external_id,
                dedupe_key="partial-rth-deal",
                quantity=Decimal("1"),
                price=Decimal("100"),
                filled_at=NOW,
                received_at=NOW,
                account_id=account.id,
                instrument_id=sleeves[0].instrument_ids[0],
            ),
        ),
    )
    adapter.get_authoritative_market_state = lambda symbols: {
        "market": "US",
        "captured_at": NOW.isoformat(),
        "complete": True,
        "rows": [{"symbol": str(symbol), "market_state": "CLOSED"} for symbol in symbols],
    }
    result = runner.submit_verified_residual_exit(
        account=account,
        source_intent_id="partial-rth",
        expected_external_order_ids={sleeves[0].instrument_ids[0]: external_id},
        spec=make_spec(account, sleeves),
    )
    assert result["residual_result"]["status"] == IntentStatus.REJECTED.value
    assert adapter.submit_calls == []


def test_residual_scope_rejects_foreign_source_before_submission(tmp_path):
    account, sleeves, repository, adapter, runner = make_runner(tmp_path)
    _intent, _leg_id, external_id = _seed_partial_source(
        repository,
        account,
        action=IntentAction.ENTER,
        source_id="foreign-residual",
        instrument_id=sleeves[0].instrument_ids[0],
    )
    repository.save_book(Book(id="foreign-book", name="Foreign", created_at=NOW, updated_at=NOW))
    with repository.transaction() as conn:
        conn.execute(
            "UPDATE core_order_intents SET book_id = ? WHERE id = ?",
            ("foreign-book", "foreign-residual"),
        )
    with pytest.raises(ValueError, match="book"):
        runner.submit_verified_residual_exit(
            account=account,
            source_intent_id="foreign-residual",
            expected_external_order_ids={sleeves[0].instrument_ids[0]: external_id},
            spec=make_spec(account, sleeves),
        )
    assert adapter.submit_calls == []


def test_compensating_exit_scope_rejects_foreign_same_account_book(tmp_path):
    account, sleeves, repository, adapter, runner = make_runner(tmp_path)
    _intent, _leg_id, external_id = _seed_partial_source(
        repository,
        account,
        action=IntentAction.ENTER,
        source_id="foreign-compensating-source",
        instrument_id=sleeves[0].instrument_ids[0],
    )
    repository.save_book(Book(id="foreign-compensating-book", name="Foreign", created_at=NOW, updated_at=NOW))
    with repository.transaction() as conn:
        conn.execute(
            "UPDATE core_order_intents SET book_id = ? WHERE id = ?",
            ("foreign-compensating-book", "foreign-compensating-source"),
        )
    with pytest.raises(ValueError, match="book"):
        runner.submit_verified_compensating_exit(
            account=account,
            source_intent_id="foreign-compensating-source",
            expected_external_order_ids={sleeves[0].instrument_ids[0]: external_id},
            spec=make_spec(account, sleeves),
        )
    assert adapter.submit_calls == []
