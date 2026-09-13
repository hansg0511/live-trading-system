"""Explicit lifecycle rules for the broker-neutral execution core."""

from __future__ import annotations

from collections.abc import Mapping
from enum import Enum
from typing import TypeVar

from .domain import BrokerOrderStatus, IntentStatus, LegStatus


StatusT = TypeVar("StatusT", bound=Enum)


INTENT_TRANSITIONS: Mapping[IntentStatus, frozenset[IntentStatus]] = {
    IntentStatus.CREATED: frozenset({IntentStatus.RISK_APPROVED, IntentStatus.REJECTED, IntentStatus.FAILED}),
    IntentStatus.RISK_APPROVED: frozenset({IntentStatus.SUBMITTING, IntentStatus.REJECTED, IntentStatus.FAILED}),
    IntentStatus.SUBMITTING: frozenset(
        {
            IntentStatus.WORKING,
            IntentStatus.PARTIALLY_FILLED,
            IntentStatus.FILLED,
            IntentStatus.REJECTED,
            IntentStatus.FAILED,
            IntentStatus.RECONCILIATION_REQUIRED,
        }
    ),
    IntentStatus.WORKING: frozenset(
        {
            IntentStatus.PARTIALLY_FILLED,
            IntentStatus.FILLED,
            IntentStatus.CANCELLED,
            IntentStatus.FAILED,
            IntentStatus.RECONCILIATION_REQUIRED,
        }
    ),
    IntentStatus.PARTIALLY_FILLED: frozenset(
        {
            IntentStatus.FILLED,
            IntentStatus.CANCELLED,
            IntentStatus.FAILED,
            IntentStatus.RECONCILIATION_REQUIRED,
        }
    ),
    IntentStatus.FILLED: frozenset({IntentStatus.COMPLETED, IntentStatus.RECONCILIATION_REQUIRED}),
    IntentStatus.COMPLETED: frozenset({IntentStatus.RECONCILIATION_REQUIRED}),
    IntentStatus.REJECTED: frozenset(
        {IntentStatus.PARTIALLY_FILLED, IntentStatus.FILLED, IntentStatus.RECONCILIATION_REQUIRED}
    ),
    IntentStatus.CANCELLED: frozenset(
        {IntentStatus.PARTIALLY_FILLED, IntentStatus.FILLED, IntentStatus.RECONCILIATION_REQUIRED}
    ),
    IntentStatus.FAILED: frozenset({IntentStatus.RECONCILIATION_REQUIRED}),
    IntentStatus.RECONCILIATION_REQUIRED: frozenset(
        {
            IntentStatus.SUBMITTING,
            IntentStatus.WORKING,
            IntentStatus.PARTIALLY_FILLED,
            IntentStatus.FILLED,
            IntentStatus.COMPLETED,
            IntentStatus.REJECTED,
            IntentStatus.CANCELLED,
            IntentStatus.FAILED,
        }
    ),
}

LEG_TRANSITIONS: Mapping[LegStatus, frozenset[LegStatus]] = {
    LegStatus.PLANNED: frozenset({LegStatus.SUBMITTING, LegStatus.CANCELLED, LegStatus.FAILED}),
    LegStatus.SUBMITTING: frozenset(
        {
            LegStatus.WORKING,
            LegStatus.PARTIALLY_FILLED,
            LegStatus.FILLED,
            LegStatus.REJECTED,
            LegStatus.FAILED,
            LegStatus.RECONCILIATION_REQUIRED,
        }
    ),
    LegStatus.WORKING: frozenset(
        {
            LegStatus.PARTIALLY_FILLED,
            LegStatus.FILLED,
            LegStatus.CANCELLED,
            LegStatus.FAILED,
            LegStatus.RECONCILIATION_REQUIRED,
        }
    ),
    LegStatus.PARTIALLY_FILLED: frozenset(
        {
            LegStatus.FILLED,
            LegStatus.CANCELLED,
            LegStatus.FAILED,
            LegStatus.RECONCILIATION_REQUIRED,
        }
    ),
    LegStatus.FILLED: frozenset({LegStatus.RECONCILIATION_REQUIRED}),
    LegStatus.REJECTED: frozenset(
        {LegStatus.PARTIALLY_FILLED, LegStatus.FILLED, LegStatus.RECONCILIATION_REQUIRED}
    ),
    LegStatus.CANCELLED: frozenset(
        {LegStatus.PARTIALLY_FILLED, LegStatus.FILLED, LegStatus.RECONCILIATION_REQUIRED}
    ),
    LegStatus.FAILED: frozenset({LegStatus.RECONCILIATION_REQUIRED}),
    LegStatus.RECONCILIATION_REQUIRED: frozenset(
        {
            LegStatus.WORKING,
            LegStatus.PARTIALLY_FILLED,
            LegStatus.FILLED,
            LegStatus.REJECTED,
            LegStatus.CANCELLED,
            LegStatus.FAILED,
        }
    ),
}

BROKER_ORDER_TRANSITIONS: Mapping[BrokerOrderStatus, frozenset[BrokerOrderStatus]] = {
    BrokerOrderStatus.PREPARED: frozenset(
        {BrokerOrderStatus.SUBMITTING, BrokerOrderStatus.CANCELLED, BrokerOrderStatus.FAILED}
    ),
    BrokerOrderStatus.SUBMITTING: frozenset(
        {
            BrokerOrderStatus.WORKING,
            BrokerOrderStatus.PARTIALLY_FILLED,
            BrokerOrderStatus.FILLED,
            BrokerOrderStatus.REJECTED,
            BrokerOrderStatus.FAILED,
            BrokerOrderStatus.UNKNOWN,
        }
    ),
    BrokerOrderStatus.WORKING: frozenset(
        {
            BrokerOrderStatus.PARTIALLY_FILLED,
            BrokerOrderStatus.FILLED,
            BrokerOrderStatus.CANCELLED,
            BrokerOrderStatus.FAILED,
            BrokerOrderStatus.UNKNOWN,
        }
    ),
    BrokerOrderStatus.PARTIALLY_FILLED: frozenset(
        {
            BrokerOrderStatus.FILLED,
            BrokerOrderStatus.CANCELLED,
            BrokerOrderStatus.FAILED,
            BrokerOrderStatus.UNKNOWN,
        }
    ),
    BrokerOrderStatus.FILLED: frozenset(),
    BrokerOrderStatus.REJECTED: frozenset(
        {BrokerOrderStatus.PARTIALLY_FILLED, BrokerOrderStatus.FILLED, BrokerOrderStatus.UNKNOWN}
    ),
    BrokerOrderStatus.CANCELLED: frozenset({BrokerOrderStatus.PARTIALLY_FILLED, BrokerOrderStatus.FILLED}),
    BrokerOrderStatus.FAILED: frozenset({BrokerOrderStatus.UNKNOWN}),
    BrokerOrderStatus.UNKNOWN: frozenset(
        {
            BrokerOrderStatus.WORKING,
            BrokerOrderStatus.PARTIALLY_FILLED,
            BrokerOrderStatus.FILLED,
            BrokerOrderStatus.REJECTED,
            BrokerOrderStatus.CANCELLED,
            BrokerOrderStatus.FAILED,
        }
    ),
}


def validate_transition(
    current: StatusT,
    target: StatusT,
    allowed: Mapping[StatusT, frozenset[StatusT]],
    *,
    entity: str,
) -> None:
    """Reject impossible state changes instead of silently coercing them."""
    if current == target:
        return
    if target not in allowed[current]:
        raise ValueError(f"Invalid {entity} transition: {current.value} -> {target.value}")
