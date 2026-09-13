from __future__ import annotations

from dataclasses import dataclass, field
import math
from typing import Any

from src.core.models import AccountBalance, Position


@dataclass(frozen=True)
class RiskLimits:
    max_account_utilization: float = 0.25
    max_margin_utilization: float = 0.50
    max_gross_exposure: float = 50_000.0
    max_pair_exposure: float = 10_000.0
    max_open_pairs: int = 5
    max_pending_operations: int = 5
    estimated_margin_rate: float = 1.0


@dataclass
class RiskSnapshot:
    equity: float | None
    buying_power: float | None
    initial_margin: float | None = None
    maintenance_margin: float | None = None
    gross_exposure: float | None = None
    open_pairs: int = 0
    pending_operations: int = 0
    pending_orders: int = 0
    unresolved_reconciliation: int = 0
    risk_data_available: bool = True
    warnings: list[str] = field(default_factory=list)

    @classmethod
    def from_account(
        cls,
        account: AccountBalance,
        *,
        positions: list[Position] | None = None,
        open_pairs: int = 0,
        pending_operations: int = 0,
        pending_orders: int = 0,
        unresolved_reconciliation: int = 0,
        risk_data_available: bool = True,
    ) -> "RiskSnapshot":
        gross = None
        if positions is not None:
            gross = 0.0
            for position in positions:
                price = position.current_price or position.average_price
                try:
                    if price is not None and math.isfinite(float(price)):
                        gross += abs(float(position.quantity) * float(price))
                except (TypeError, ValueError):
                    continue
        return cls(
            equity=account.equity,
            buying_power=account.buying_power,
            initial_margin=account.initial_margin,
            maintenance_margin=account.maintenance_margin,
            gross_exposure=gross,
            open_pairs=open_pairs,
            pending_operations=pending_operations,
            pending_orders=pending_orders,
            unresolved_reconciliation=unresolved_reconciliation,
            risk_data_available=risk_data_available,
        )


@dataclass(frozen=True)
class RiskDecision:
    approved: bool
    reason: str
    proposed_pair_exposure: float
    estimated_incremental_margin: float
    checks: dict[str, Any] = field(default_factory=dict)
    warnings: tuple[str, ...] = ()


def _finite_nonnegative(value: float | None) -> bool:
    return value is not None and math.isfinite(float(value)) and float(value) >= 0


def proposed_exposure(legs: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> float:
    total = 0.0
    for leg in legs:
        price = float(leg["intended_price"])
        quantity = float(leg["requested_quantity"])
        if not math.isfinite(price) or price <= 0 or not math.isfinite(quantity) or quantity <= 0:
            raise ValueError("Proposed legs require finite positive prices and quantities")
        total += abs(price * quantity)
    return total


def evaluate_entry_risk(
    snapshot: RiskSnapshot,
    legs: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    limits: RiskLimits,
    *,
    environment: str = "SIMULATE",
) -> RiskDecision:
    """Central admission check for an entry before either leg is submitted."""
    env = str(environment).upper()
    exposure = proposed_exposure(list(legs))
    incremental_margin = exposure * float(limits.estimated_margin_rate)
    warnings = list(snapshot.warnings)
    checks: dict[str, Any] = {
        "environment": env,
        "proposed_pair_exposure": exposure,
        "estimated_incremental_margin": incremental_margin,
    }

    def reject(reason: str) -> RiskDecision:
        return RiskDecision(False, reason, exposure, incremental_margin, checks, tuple(warnings))

    if (
        not math.isfinite(float(limits.max_account_utilization))
        or not math.isfinite(float(limits.max_margin_utilization))
        or not math.isfinite(float(limits.max_gross_exposure))
        or not math.isfinite(float(limits.max_pair_exposure))
        or not math.isfinite(float(limits.estimated_margin_rate))
        or limits.max_account_utilization <= 0
        or limits.max_account_utilization > 1
        or limits.max_margin_utilization <= 0
        or limits.max_margin_utilization > 1
        or limits.max_gross_exposure <= 0
        or limits.max_pair_exposure <= 0
        or limits.estimated_margin_rate <= 0
    ):
        return reject("risk limits are invalid")

    if env == "REAL" and not snapshot.risk_data_available:
        return reject("required account/risk data was unavailable for REAL trading")
    if snapshot.unresolved_reconciliation:
        if env == "REAL":
            return reject("unresolved broker/local reconciliation issues block REAL entries")
        warnings.append("unresolved reconciliation issues present; SIMULATE entry only")
    if not _finite_nonnegative(snapshot.equity) or float(snapshot.equity) <= 0:
        return reject("account equity is unavailable or non-positive")
    equity = float(snapshot.equity)
    checks["equity"] = equity
    if exposure > limits.max_pair_exposure + 1e-9:
        return reject(f"proposed pair exposure {exposure:.2f} exceeds cap {limits.max_pair_exposure:.2f}")
    gross = snapshot.gross_exposure
    if gross is None:
        if env == "REAL":
            return reject("current gross exposure could not be determined")
        gross = 0.0
        warnings.append("gross exposure unavailable; treated as zero in SIMULATE")
    if not _finite_nonnegative(gross):
        return reject("gross exposure is invalid")
    gross = float(gross)
    checks["gross_exposure_before"] = gross
    checks["gross_exposure_after"] = gross + exposure
    if gross + exposure > limits.max_gross_exposure + 1e-9:
        return reject("gross exposure cap would be exceeded")
    account_utilization = (gross + exposure) / equity
    checks["account_utilization_after"] = account_utilization
    if account_utilization > limits.max_account_utilization + 1e-9:
        return reject("account-level utilization cap would be exceeded")
    if snapshot.buying_power is not None:
        buying_power = float(snapshot.buying_power)
        checks["buying_power"] = buying_power
        if not math.isfinite(buying_power) or buying_power < incremental_margin - 1e-9:
            return reject("insufficient buying power for proposed incremental margin")
    elif env == "REAL":
        return reject("buying power was unavailable for REAL trading")
    margin_used = snapshot.initial_margin
    if margin_used is None:
        if env == "REAL":
            return reject("initial-margin usage was unavailable for REAL trading")
        margin_used = 0.0
        warnings.append("initial-margin usage unavailable; treated as zero in SIMULATE")
    margin_used = float(margin_used)
    if not _finite_nonnegative(margin_used):
        return reject("initial-margin usage is invalid")
    margin_utilization = (margin_used + incremental_margin) / equity
    checks["margin_utilization_after"] = margin_utilization
    if margin_utilization > limits.max_margin_utilization + 1e-9:
        return reject("margin-utilization cap would be exceeded")
    if snapshot.open_pairs + 1 > limits.max_open_pairs:
        return reject("maximum open-pair count would be exceeded")
    if snapshot.pending_operations >= limits.max_pending_operations:
        return reject("maximum pending-operation count would be exceeded")
    checks["pending_operations"] = snapshot.pending_operations
    checks["pending_orders"] = snapshot.pending_orders
    # Each pair operation reserves two local order intents. Keep the order
    # count bounded as well as the distinct-operation count.
    if snapshot.pending_orders >= limits.max_pending_operations * 2:
        return reject("maximum pending-order count would be exceeded")
    return RiskDecision(True, "risk checks approved", exposure, incremental_margin, checks, tuple(warnings))
