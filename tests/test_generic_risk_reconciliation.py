from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from src.trading_core.domain import (
    LegStatus,
    OrderLeg,
    OwnershipClass,
    PositionAllocation,
    PositionSnapshot,
    QuantityUnit,
    Side,
)
from src.trading_core.reconciliation import compute_unknown_residuals, reconcile_positions
from src.trading_core.risk import WorkingOrder, calculate_projection, project_signed_positions


NOW = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)


def position(quantity: str) -> PositionSnapshot:
    return PositionSnapshot(
        id="position-snapshot",
        broker_snapshot_id="snapshot",
        account_id="acct",
        instrument_id="instrument",
        signed_quantity=Decimal(quantity),
        average_price=Decimal("100"),
        captured_at=NOW,
        metadata={},
    )


def allocation(quantity: str, ownership: OwnershipClass = OwnershipClass.MANAGED) -> PositionAllocation:
    return PositionAllocation(
        id=f"allocation-{ownership.value}",
        account_id="acct",
        instrument_id="instrument",
        strategy_id="strategy" if ownership is OwnershipClass.MANAGED else None,
        book_id=None,
        ownership_class=ownership,
        signed_quantity=Decimal(quantity),
        source_intent_id=None,
        updated_at=NOW,
        metadata={},
    )


def proposed(side: Side, quantity: str) -> OrderLeg:
    return OrderLeg(
        id="proposed",
        intent_id="intent",
        sequence=0,
        instrument_id="instrument",
        side=side,
        quantity=Decimal(quantity),
        quantity_unit=QuantityUnit.UNITS,
        order_type="MARKET",
        status=LegStatus.PLANNED,
        created_at=NOW,
        updated_at=NOW,
        metadata={},
    )


def test_signed_working_sell_reduces_an_existing_long():
    projected = project_signed_positions(
        [position("100")],
        [WorkingOrder("instrument", Side.SELL, Decimal("30"))],
        [proposed(Side.SELL, "20")],
    )
    assert projected == {"instrument": Decimal("50")}


def test_broker_truth_minus_managed_allocation_becomes_unknown_residual():
    residuals = compute_unknown_residuals([position("70")], [allocation("60")])
    assert residuals == {"instrument": Decimal("10")}

    result = reconcile_positions(
        snapshot_complete=True,
        broker_positions=[position("70")],
        allocations=[allocation("60")],
    )
    assert result.ready_for_submission is False
    assert result.findings[0].category == "UNKNOWN_POSITION_RESIDUAL"


def test_unknown_allocation_does_not_mask_unexplained_broker_truth():
    residuals = compute_unknown_residuals(
        [position("70")],
        [allocation("60"), allocation("10", OwnershipClass.UNKNOWN)],
    )
    assert residuals == {"instrument": Decimal("10")}


def test_incomplete_snapshot_is_never_interpreted_as_flat():
    result = reconcile_positions(
        snapshot_complete=False,
        broker_positions=[],
        allocations=[],
    )
    assert result.snapshot_complete is False
    assert result.ready_for_submission is False
    assert result.findings[0].category == "SNAPSHOT_INCOMPLETE"


@pytest.mark.parametrize("mark", [Decimal("0"), Decimal("-1"), Decimal("NaN")])
def test_risk_rejects_non_executable_marks(mark):
    with pytest.raises(ValueError, match="invalid executable mark"):
        calculate_projection({"instrument": Decimal("1")}, {"instrument": mark})
