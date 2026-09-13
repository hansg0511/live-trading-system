"""Broker-account-wide risk projection primitives."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable, Mapping

from .domain import OrderLeg, PositionSnapshot, Side


ZERO = Decimal("0")


@dataclass(frozen=True, slots=True)
class WorkingOrder:
    instrument_id: str
    side: Side
    remaining_quantity: Decimal
    account_id: str | None = None

    def __post_init__(self) -> None:
        if self.remaining_quantity < ZERO:
            raise ValueError("remaining_quantity cannot be negative")


@dataclass(frozen=True, slots=True)
class AccountRiskPolicy:
    max_gross_notional: Decimal | None = None
    max_net_notional: Decimal | None = None
    max_instrument_notional: Decimal | None = None


@dataclass(frozen=True, slots=True)
class AccountRiskProjection:
    signed_quantities: Mapping[str, Decimal]
    gross_notional: Decimal
    net_notional: Decimal
    instrument_notionals: Mapping[str, Decimal]


def signed_quantity(side: Side, quantity: Decimal) -> Decimal:
    return quantity if side is Side.BUY else -quantity


def project_signed_positions(
    broker_positions: Iterable[PositionSnapshot],
    working_orders: Iterable[WorkingOrder],
    proposed_legs: Iterable[OrderLeg],
    *,
    account_id: str | None = None,
) -> dict[str, Decimal]:
    """Project net instrument quantities using signed order effects.

    A sell against an existing long therefore reduces exposure naturally; it
    is not incorrectly added as another positive absolute exposure.
    """
    projected: dict[str, Decimal] = {}
    for position in broker_positions:
        if account_id is not None and position.account_id != account_id:
            raise ValueError("broker position belongs to a different account")
        projected[position.instrument_id] = projected.get(position.instrument_id, ZERO) + Decimal(
            position.signed_quantity
        )
    for order in working_orders:
        if account_id is not None and order.account_id not in {None, account_id}:
            raise ValueError("working order belongs to a different account")
        projected[order.instrument_id] = projected.get(order.instrument_id, ZERO) + signed_quantity(
            order.side, Decimal(order.remaining_quantity)
        )
    for leg in proposed_legs:
        projected[leg.instrument_id] = projected.get(leg.instrument_id, ZERO) + signed_quantity(
            leg.side, Decimal(leg.quantity)
        )
    return projected


def calculate_projection(
    signed_quantities: Mapping[str, Decimal],
    marks: Mapping[str, Decimal],
    multipliers: Mapping[str, Decimal] | None = None,
) -> AccountRiskProjection:
    multipliers = multipliers or {}
    notionals: dict[str, Decimal] = {}
    for instrument_id, quantity in signed_quantities.items():
        if instrument_id not in marks:
            raise ValueError(f"missing executable mark for instrument {instrument_id}")
        mark = Decimal(marks[instrument_id])
        multiplier = Decimal(multipliers.get(instrument_id, Decimal("1")))
        if not mark.is_finite() or mark <= ZERO:
            raise ValueError(f"invalid executable mark for instrument {instrument_id}")
        if not multiplier.is_finite() or multiplier <= ZERO:
            raise ValueError(f"invalid multiplier for instrument {instrument_id}")
        notionals[instrument_id] = Decimal(quantity) * mark * multiplier
    return AccountRiskProjection(
        signed_quantities=dict(signed_quantities),
        gross_notional=sum((abs(value) for value in notionals.values()), ZERO),
        net_notional=sum(notionals.values(), ZERO),
        instrument_notionals=notionals,
    )


def policy_violations(
    projection: AccountRiskProjection,
    policy: AccountRiskPolicy,
) -> tuple[str, ...]:
    violations: list[str] = []
    if policy.max_gross_notional is not None and projection.gross_notional > policy.max_gross_notional:
        violations.append("gross_notional")
    if policy.max_net_notional is not None and abs(projection.net_notional) > policy.max_net_notional:
        violations.append("net_notional")
    if policy.max_instrument_notional is not None and any(
        abs(value) > policy.max_instrument_notional for value in projection.instrument_notionals.values()
    ):
        violations.append("instrument_concentration")
    return tuple(violations)
