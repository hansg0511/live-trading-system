"""Broker-truth comparisons independent of strategy and adapter details."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Iterable, Mapping

from .domain import OwnershipClass, PositionAllocation, PositionSnapshot


ZERO = Decimal("0")


@dataclass(frozen=True, slots=True)
class ReconciliationFinding:
    issue_key: str
    category: str
    entity_type: str
    entity_key: str
    severity: str
    sticky: bool
    details: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PositionReconciliationResult:
    snapshot_complete: bool
    ready_for_submission: bool
    unknown_residuals: Mapping[str, Decimal]
    findings: tuple[ReconciliationFinding, ...]


def compute_unknown_residuals(
    broker_positions: Iterable[PositionSnapshot],
    allocations: Iterable[PositionAllocation],
) -> dict[str, Decimal]:
    """Return broker quantity less all explicitly owned virtual allocations."""
    broker_totals: dict[str, Decimal] = {}
    allocation_totals: dict[str, Decimal] = {}
    for position in broker_positions:
        broker_totals[position.instrument_id] = broker_totals.get(position.instrument_id, ZERO) + Decimal(
            position.signed_quantity
        )
    for allocation in allocations:
        if allocation.ownership_class is OwnershipClass.UNKNOWN:
            continue
        allocation_totals[allocation.instrument_id] = allocation_totals.get(allocation.instrument_id, ZERO) + Decimal(
            allocation.signed_quantity
        )
    instruments = set(broker_totals) | set(allocation_totals)
    return {
        instrument_id: broker_totals.get(instrument_id, ZERO)
        - allocation_totals.get(instrument_id, ZERO)
        for instrument_id in instruments
        if broker_totals.get(instrument_id, ZERO) - allocation_totals.get(instrument_id, ZERO) != ZERO
    }


def reconcile_positions(
    *,
    snapshot_complete: bool,
    broker_positions: Iterable[PositionSnapshot],
    allocations: Iterable[PositionAllocation],
) -> PositionReconciliationResult:
    """Compare complete broker truth with virtual ownership.

    An incomplete snapshot is never interpreted as a flat account and is a
    sticky blocking finding. Unexplained quantities are UNKNOWN, not assumed
    to be manual/external positions.
    """
    if not snapshot_complete:
        finding = ReconciliationFinding(
            issue_key="position-snapshot-incomplete",
            category="SNAPSHOT_INCOMPLETE",
            entity_type="ACCOUNT",
            entity_key="positions",
            severity="CRITICAL",
            sticky=True,
            details={"reason": "broker position snapshot was not complete"},
        )
        return PositionReconciliationResult(
            snapshot_complete=False,
            ready_for_submission=False,
            unknown_residuals={},
            findings=(finding,),
        )

    residuals = compute_unknown_residuals(broker_positions, allocations)
    findings = tuple(
        ReconciliationFinding(
            issue_key=f"position-residual:{instrument_id}",
            category="UNKNOWN_POSITION_RESIDUAL",
            entity_type="INSTRUMENT",
            entity_key=instrument_id,
            severity="ERROR",
            sticky=True,
            details={"signed_quantity": format(quantity, "f")},
        )
        for instrument_id, quantity in sorted(residuals.items())
    )
    return PositionReconciliationResult(
        snapshot_complete=True,
        ready_for_submission=not findings,
        unknown_residuals=residuals,
        findings=findings,
    )
