"""Deterministic fake broker for offline OMS tests.

It implements the normalized broker boundary, NOT Moomoo/OpenD wire responses.
Actual broker truth is independent of the SQLite repository and persists if
the test creates a new OMS against the same fake instance.
"""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from src.trading_core.domain import (
    Account, BrokerCapabilities, BrokerOrderSnapshot, BrokerOrderStatus,
    ExecutionEvidenceMode, PositionSnapshot, Side,
)
from src.trading_core.ports import (
    BrokerFactSnapshot, BrokerFill, BrokerSubmissionResult, BrokerSubmitRequest,
)


@dataclass
class ManualClock:
    now: datetime = datetime(2026, 10, 9, 15, 0, tzinfo=timezone.utc)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("cannot move clock backwards")
        self.now += timedelta(seconds=seconds)


@dataclass
class FakeOrder:
    external_id: str
    client_id: str | None
    instrument: str
    side: Side
    quantity: Decimal
    created_at: datetime
    status: BrokerOrderStatus = BrokerOrderStatus.WORKING
    filled: Decimal = Decimal("0")
    last_fill_price: Decimal | None = None


class ScriptedBroker:
    """Controllable orders, fills, positions and unreliable broker observations.

    Do not use for price matching or profitability backtests: fills occur only
    when the test explicitly calls fill_order.
    """

    def __init__(
        self,
        account: Account,
        *,
        clock: ManualClock | None = None,
        evidence_mode: ExecutionEvidenceMode = ExecutionEvidenceMode.INDIVIDUAL_DEALS,
    ) -> None:
        self.account = account
        self.clock = clock or ManualClock()
        self.evidence_mode = evidence_mode
        self.orders: dict[str, FakeOrder] = {}
        self.deals: list[BrokerFill] = []
        self.positions: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
        self.submit_calls: list[BrokerSubmitRequest] = []
        self.submit_behaviors: deque[str] = deque()
        self.hidden_orders: set[str] = set()
        self.stale_snapshots: dict[str, BrokerOrderSnapshot] = {}
        self.duplicate_deal_ids: set[str] = set()
        self.positions_override: dict[str, Decimal] | None = None
        self.incomplete = False
        self.query_error: str | None = None
        self.snapshot_age_seconds = 0
        self.omit_execution_evidence = False
        self._next_order = 0
        self._next_deal = 0

    def _account(self, account: Account) -> None:
        if account.id != self.account.id:
            raise ValueError("wrong account")

    def queue_submit(self, behavior: str) -> None:
        """Behaviors: accept, reject, pre_send_failure, or lost_ack."""
        if behavior not in {"accept", "reject", "pre_send_failure", "lost_ack"}:
            raise ValueError("invalid scripted submission behavior")
        self.submit_behaviors.append(behavior)

    def _new_order(
        self, instrument: str, side: Side, quantity: Decimal | str,
        client_id: str | None = None,
    ) -> FakeOrder:
        qty = Decimal(str(quantity))
        if not qty.is_finite() or qty <= 0:
            raise ValueError("invalid order quantity")
        self._next_order += 1
        order = FakeOrder(
            external_id=f"FAKE-{self._next_order:06d}",
            client_id=client_id,
            instrument=instrument,
            side=side,
            quantity=qty,
            created_at=self.clock(),
        )
        self.orders[order.external_id] = order
        return order

    def submit_order(self, account: Account, request: BrokerSubmitRequest) -> BrokerSubmissionResult:
        self._account(account)
        self.submit_calls.append(request)
        behavior = self.submit_behaviors.popleft() if self.submit_behaviors else "accept"
        if behavior == "pre_send_failure":
            raise ConnectionError("scripted failure before sending")
        if behavior == "reject":
            return BrokerSubmissionResult(
                broker_order_id=request.broker_order_id,
                accepted=False,
                status=BrokerOrderStatus.REJECTED,
                no_submit_asserted=True,
                client_order_id=request.client_order_id,
            )
        order = self._new_order(
            request.order_leg.instrument_id, request.order_leg.side,
            request.order_leg.quantity, request.client_order_id,
        )
        if behavior == "lost_ack":
            raise TimeoutError("broker accepted order but its acknowledgment was lost")
        return BrokerSubmissionResult(
            broker_order_id=request.broker_order_id,
            accepted=True,
            status=BrokerOrderStatus.WORKING,
            external_order_id=order.external_id,
            client_order_id=order.client_id,
            submitted_at=self.clock(),
        )

    def inject_external_order(
        self, instrument: str, side: Side, quantity: Decimal | str,
    ) -> str:
        """Simulate an order unknown to the OMS (e.g. manual trading)."""
        return self._new_order(instrument, side, quantity).external_id

    def fill_order(
        self, external_id: str, quantity: Decimal | str, price: Decimal | str,
    ) -> BrokerFill:
        order = self.orders[external_id]
        qty, px = Decimal(str(quantity)), Decimal(str(price))
        if qty <= 0 or not qty.is_finite() or px <= 0 or not px.is_finite():
            raise ValueError("invalid execution")
        if order.status not in {BrokerOrderStatus.WORKING, BrokerOrderStatus.PARTIALLY_FILLED}:
            raise ValueError("order is no longer working")
        if order.filled + qty > order.quantity:
            raise ValueError("fill exceeds remaining quantity")
        self._next_deal += 1
        deal_id = f"DEAL-{self._next_deal:06d}"
        fill = BrokerFill(
            external_order_id=external_id,
            external_fill_id=deal_id,
            dedupe_key=deal_id,
            account_id=self.account.id,
            instrument_id=order.instrument,
            quantity=qty,
            price=px,
            filled_at=self.clock(),
            received_at=self.clock(),
        )
        self.deals.append(fill)
        order.filled += qty
        order.last_fill_price = px
        order.status = (
            BrokerOrderStatus.FILLED if order.filled == order.quantity
            else BrokerOrderStatus.PARTIALLY_FILLED
        )
        sign = Decimal("1") if order.side is Side.BUY else Decimal("-1")
        self.positions[order.instrument] += qty * sign
        return fill

    def cancel_order(self, account: Account, external_order_id: str) -> BrokerSubmissionResult:
        self._account(account)
        order = self.orders[external_order_id]
        if order.status not in {BrokerOrderStatus.WORKING, BrokerOrderStatus.PARTIALLY_FILLED}:
            raise ValueError("cannot cancel completed order")
        order.status = BrokerOrderStatus.CANCELLED
        return BrokerSubmissionResult(
            broker_order_id=f"cancel-{external_order_id}",
            accepted=True,
            status=BrokerOrderStatus.CANCELLED,
            external_order_id=external_order_id,
            client_order_id=order.client_id,
            cumulative_filled_quantity=order.filled,
            no_fill_asserted=order.filled == 0,
        )

    def _snapshot(self, order: FakeOrder) -> BrokerOrderSnapshot:
        return BrokerOrderSnapshot(
            id=f"snapshot-{order.external_id}",
            broker_snapshot_id=f"broker-snapshot-{order.external_id}",
            account_id=self.account.id,
            external_account_id=self.account.external_account_id,
            instrument_id=order.instrument,
            external_order_id=order.external_id,
            client_order_id=order.client_id,
            side=order.side,
            quantity=order.quantity,
            filled_quantity=order.filled,
            status=order.status,
            captured_at=self.clock(),
            order_time=order.created_at,
        )

    def freeze_order_observation(self, external_id: str) -> None:
        """Keep reporting the current snapshot even after the real order fills."""
        self.stale_snapshots[external_id] = self._snapshot(self.orders[external_id])

    def get_order(self, account: Account, external_order_id: str) -> BrokerOrderSnapshot | None:
        self._account(account)
        if self.query_error:
            raise ConnectionError(self.query_error)
        if external_order_id in self.hidden_orders:
            return None
        if external_order_id in self.stale_snapshots:
            return self.stale_snapshots[external_order_id]
        order = self.orders.get(external_order_id)
        return self._snapshot(order) if order else None

    def get_open_orders(self, account: Account) -> tuple[BrokerOrderSnapshot, ...]:
        self._account(account)
        if self.query_error:
            raise ConnectionError(self.query_error)
        # Filtering by *reported* state allows a stale WORKING order to remain
        # observable after the venue has actually filled it.
        reports = (
            self.stale_snapshots.get(o.external_id) or self._snapshot(o)
            for o in self.orders.values() if o.external_id not in self.hidden_orders
        )
        return tuple(s for s in reports if s.status in {
            BrokerOrderStatus.WORKING, BrokerOrderStatus.PARTIALLY_FILLED
        })

    def _visible_deals(self) -> tuple[BrokerFill, ...]:
        if self.omit_execution_evidence or self.evidence_mode is ExecutionEvidenceMode.UNAVAILABLE:
            return ()
        if self.evidence_mode is ExecutionEvidenceMode.INDIVIDUAL_DEALS:
            return tuple(
                row for deal in self.deals
                for row in ((deal, deal) if deal.external_fill_id in self.duplicate_deal_ids else (deal,))
            )
        # Simulate normalized *terminal cumulative snapshot* evidence, not
        # fabricated exchange deal IDs. Exact economics of multiple partial
        # fills cannot be recovered from a cumulative snapshot alone.
        rows = []
        for order in self.orders.values():
            if order.status not in {BrokerOrderStatus.FILLED, BrokerOrderStatus.CANCELLED}:
                continue
            if order.filled <= 0 or order.last_fill_price is None:
                continue
            last = next(
                deal for deal in reversed(self.deals) if deal.external_order_id == order.external_id
            )
            rows.append(BrokerFill(
                external_order_id=order.external_id,
                dedupe_key=f"CUMULATIVE:{order.external_id}",
                external_fill_id=None,
                account_id=self.account.id,
                instrument_id=order.instrument,
                quantity=order.filled,
                price=order.last_fill_price,
                filled_at=last.filled_at,
                received_at=self.clock(),
                evidence_reference=f"TERMINAL_ORDER:{order.external_id}",
                evidence_mode=ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS,
                metadata={"source": "terminal_order_snapshot"},
            ))
        return tuple(rows)

    def get_fills(self, account: Account, since: datetime | None = None) -> tuple[BrokerFill, ...]:
        self._account(account)
        if self.query_error:
            raise ConnectionError(self.query_error)
        if self.evidence_mode is not ExecutionEvidenceMode.INDIVIDUAL_DEALS:
            raise RuntimeError("individual-deal history unavailable")
        return tuple(f for f in self._visible_deals() if since is None or f.filled_at >= since)

    def get_positions(self, account: Account) -> tuple[PositionSnapshot, ...]:
        self._account(account)
        if self.query_error:
            raise ConnectionError(self.query_error)
        values = self.positions_override if self.positions_override is not None else self.positions
        return tuple(
            PositionSnapshot(
                id=f"position-{symbol}",
                broker_snapshot_id="fake-account-snapshot",
                account_id=account.id,
                instrument_id=symbol,
                signed_quantity=qty,
                captured_at=self.clock(),
            )
            for symbol, raw in sorted(values.items())
            if (qty := Decimal(str(raw))) != 0
        )

    def get_authoritative_account_facts(self, account: Account) -> BrokerFactSnapshot:
        self._account(account)
        if self.query_error:
            return BrokerFactSnapshot(
                account_id=account.id,
                captured_at=self.clock(),
                complete=False,
                error=self.query_error,
                execution_evidence_mode=ExecutionEvidenceMode.UNAVAILABLE,
            )
        complete = not self.incomplete and self.evidence_mode is not ExecutionEvidenceMode.UNAVAILABLE
        return BrokerFactSnapshot(
            account_id=account.id,
            captured_at=self.clock() - timedelta(seconds=self.snapshot_age_seconds),
            complete=complete,
            error=None if complete else "incomplete evidence",
            positions=self.get_positions(account),
            open_orders=self.get_open_orders(account),
            fills=self._visible_deals(),
            execution_evidence_mode=self.evidence_mode,
            execution_evidence_scope=frozenset(
                {"CURRENT_DEALS"} if self.evidence_mode is ExecutionEvidenceMode.INDIVIDUAL_DEALS
                else {"TERMINAL_ORDER_SNAPSHOTS"} if self.evidence_mode is ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS
                else ()
            ),
        )

    def get_account_facts(self, account: Account) -> BrokerFactSnapshot:
        return self.get_authoritative_account_facts(account)

    def get_capabilities(self, account: Account) -> BrokerCapabilities:
        self._account(account)
        return BrokerCapabilities(
            broker=account.broker,
            supports_fill_read=self.evidence_mode is ExecutionEvidenceMode.INDIVIDUAL_DEALS,
            execution_evidence_mode=self.evidence_mode,
            supports_submit=True,
            supports_cancel=True,
            supports_client_order_id=True,
        )

    def get_accounts(self) -> tuple[Account, ...]:
        return (self.account,)
