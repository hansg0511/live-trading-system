"""Small declarative Stage 5 sleeve example.

The example shows where a future Clean40/pairs signal producer plugs in.  It
contains configuration only; it does not fetch prices, generate signals, or
submit broker orders.
"""

from __future__ import annotations

from datetime import datetime, timezone

from .stage5_sleeves import (
    Clean40AllocationUpdate,
    PairSleeve,
    SleeveAllocationTarget,
)


def build_example_sleeves(
    *,
    account_id: str = "stage5-account",
    strategy_id: str = "stage5-stat-arb",
) -> tuple[PairSleeve, PairSleeve]:
    """Build two independent sleeves with separate pair configuration."""
    return (
        PairSleeve(
            sleeve_id="stat-arb-sleeve-a",
            strategy_id=strategy_id,
            account_id=account_id,
            book_id="stat-arb-book-a",
            name="Stat Arb Sleeve A",
            instrument_ids=("stage5-a-leg-1", "stage5-a-leg-2"),
            symbols=("STAGE5_A1", "STAGE5_A2"),
            configuration={
                "lookback": 40,
                "entry_zscore": "2.0",
                "exit_zscore": "0.5",
                "signal_source": "normalized_pair_target",
            },
        ),
        PairSleeve(
            sleeve_id="stat-arb-sleeve-b",
            strategy_id=strategy_id,
            account_id=account_id,
            book_id="stat-arb-book-b",
            name="Stat Arb Sleeve B",
            instrument_ids=("stage5-b-leg-1", "stage5-b-leg-2"),
            symbols=("STAGE5_B1", "STAGE5_B2"),
            configuration={
                "lookback": 40,
                "entry_zscore": "2.25",
                "exit_zscore": "0.6",
                "signal_source": "normalized_pair_target",
            },
        ),
    )


def build_example_allocation_update(
    *,
    account_id: str = "stage5-account",
    version: int = 1,
    effective_at: datetime | None = None,
    provenance: str = "stage5-example-config",
) -> Clean40AllocationUpdate:
    """Return a balanced example update for the two independent books."""
    moment = effective_at or datetime.now(timezone.utc)
    return Clean40AllocationUpdate(
        account_id=account_id,
        version=version,
        effective_at=moment,
        provenance=provenance,
        targets=(
            SleeveAllocationTarget(
                sleeve_id="stat-arb-sleeve-a",
                book_id="stat-arb-book-a",
                target_weight="0.50",
            ),
            SleeveAllocationTarget(
                sleeve_id="stat-arb-sleeve-b",
                book_id="stat-arb-book-b",
                target_weight="0.50",
            ),
        ),
    )


__all__ = ["build_example_allocation_update", "build_example_sleeves"]
