"""SQLite persistence for the side-by-side broker-neutral trading core."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any, Iterator
import uuid

from .domain import (
    Account,
    AccountBalanceSnapshot,
    Book,
    BookAllocation,
    BrokerOrderSnapshot,
    BrokerOrderStatus,
    BrokerSnapshot,
    Fill,
    Instrument,
    InstrumentMapping,
    IntentStatus,
    LegStatus,
    OrderIntent,
    PositionAllocation,
    PositionSnapshot,
    ReconciliationIssue,
    ReconciliationRun,
    RiskDecisionRecord,
    Strategy,
)
from .state_machine import (
    BROKER_ORDER_TRANSITIONS,
    INTENT_TRANSITIONS,
    LEG_TRANSITIONS,
    validate_transition,
)


SCHEMA_PATH = Path(__file__).with_name("schema.sql")


class IdempotencyConflict(ValueError):
    """An idempotency key was reused for a materially different intent."""


class DuplicateFill(ValueError):
    """Retained for callers that prefer exception-based duplicate handling."""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _enum(value: Any) -> Any:
    return value.value if isinstance(value, Enum) else value


def _decimal(value: Decimal | int | str | None) -> str | None:
    return None if value is None else format(Decimal(value), "f")


def _timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value if value is not None else {}, sort_keys=True, separators=(",", ":"), default=str)


def _decode(value: str | None) -> Any:
    return json.loads(value or "{}")


def _payload_hash(intent: OrderIntent) -> str:
    policy = asdict(intent.execution_policy)
    policy["required_capabilities"] = sorted(intent.execution_policy.required_capabilities)
    payload = {
        "account_id": intent.account_id,
        "strategy_id": intent.strategy_id,
        "book_id": intent.book_id,
        "action": _enum(intent.action),
        "source_signal_id": intent.source_signal_id,
        "execution_policy": policy,
        "metadata": intent.metadata,
        "legs": [
            {
                "sequence": leg.sequence,
                "instrument_id": leg.instrument_id,
                "side": _enum(leg.side),
                "quantity": _decimal(leg.quantity),
                "quantity_unit": _enum(leg.quantity_unit),
                "order_type": leg.order_type,
                "limit_price": _decimal(leg.limit_price),
                "stop_price": _decimal(leg.stop_price),
                "time_in_force": leg.time_in_force,
                "metadata": leg.metadata,
            }
            for leg in sorted(intent.legs, key=lambda item: item.sequence)
        ],
    }
    encoded = _json(payload).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class SQLiteTradingRepository:
    """Durable core store which never reads or writes legacy execution tables."""

    def __init__(self, db_path: str | Path, *, schema_path: str | Path = SCHEMA_PATH):
        self.db_path = Path(db_path)
        self.schema_path = Path(schema_path)

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.db_path), timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = FULL")
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def initialize(self) -> None:
        with self.transaction() as conn:
            conn.executescript(self.schema_path.read_text(encoding="utf-8"))

    def save_account(self, account: Account) -> None:
        with self.transaction() as conn:
            conn.execute(
                """INSERT INTO core_accounts
                   (id, broker, environment, external_account_id, base_currency, enabled,
                    metadata_json, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    account.id,
                    account.broker,
                    _enum(account.environment),
                    account.external_account_id,
                    account.base_currency,
                    int(account.enabled),
                    _json(account.metadata),
                    _timestamp(account.created_at),
                    _timestamp(account.updated_at),
                ),
            )

    def save_instrument(self, instrument: Instrument) -> None:
        with self.transaction() as conn:
            conn.execute(
                """INSERT INTO core_instruments
                   (id, asset_class, symbol, venue, currency, multiplier, tick_size, lot_size,
                    expiry, strike, option_right, metadata_json, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    instrument.id,
                    _enum(instrument.asset_class),
                    instrument.symbol,
                    instrument.venue,
                    instrument.currency,
                    _decimal(instrument.multiplier),
                    _decimal(instrument.tick_size),
                    _decimal(instrument.lot_size),
                    instrument.expiry.isoformat() if instrument.expiry else None,
                    _decimal(instrument.strike),
                    _enum(instrument.option_right),
                    _json(instrument.metadata),
                    _timestamp(instrument.created_at),
                    _timestamp(instrument.updated_at),
                ),
            )

    def save_instrument_mapping(self, mapping: InstrumentMapping) -> None:
        with self.transaction() as conn:
            conn.execute(
                """INSERT INTO core_instrument_mappings
                   (id, instrument_id, provider, purpose, external_symbol, external_id,
                    metadata_json, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    mapping.id,
                    mapping.instrument_id,
                    mapping.provider,
                    _enum(mapping.purpose),
                    mapping.external_symbol,
                    mapping.external_id,
                    _json(mapping.metadata),
                    _timestamp(mapping.created_at),
                    _timestamp(mapping.updated_at),
                ),
            )

    def save_strategy(self, strategy: Strategy) -> None:
        with self.transaction() as conn:
            conn.execute(
                """INSERT INTO core_strategies
                   (id, name, strategy_type, version, enabled, config_json, metadata_json,
                    created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    strategy.id,
                    strategy.name,
                    strategy.strategy_type,
                    strategy.version,
                    int(strategy.enabled),
                    _json(strategy.config),
                    _json(strategy.metadata),
                    _timestamp(strategy.created_at),
                    _timestamp(strategy.updated_at),
                ),
            )

    def save_book(self, book: Book) -> None:
        with self.transaction() as conn:
            conn.execute(
                """INSERT INTO core_books
                   (id, name, enabled, metadata_json, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    book.id,
                    book.name,
                    int(book.enabled),
                    _json(book.metadata),
                    _timestamp(book.created_at),
                    _timestamp(book.updated_at),
                ),
            )

    def save_book_allocation(self, allocation: BookAllocation) -> None:
        with self.transaction() as conn:
            conn.execute(
                """INSERT INTO core_book_allocations
                   (id, book_id, strategy_id, account_id, capital_fraction, capital_amount,
                    risk_budget, effective_at, expires_at, metadata_json, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    allocation.id,
                    allocation.book_id,
                    allocation.strategy_id,
                    allocation.account_id,
                    _decimal(allocation.capital_fraction),
                    _decimal(allocation.capital_amount),
                    _decimal(allocation.risk_budget),
                    _timestamp(allocation.effective_at),
                    _timestamp(allocation.expires_at),
                    _json(allocation.metadata),
                    _timestamp(allocation.created_at),
                    _timestamp(allocation.updated_at),
                ),
            )

    def create_intent(self, intent: OrderIntent) -> tuple[str, bool]:
        """Persist an intent and every logical leg atomically before submission."""
        fingerprint = _payload_hash(intent)
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT id, payload_hash FROM core_order_intents WHERE account_id = ? AND idempotency_key = ?",
                (intent.account_id, intent.idempotency_key),
            ).fetchone()
            if existing:
                if str(existing["payload_hash"]) != fingerprint:
                    raise IdempotencyConflict(
                        f"idempotency key {intent.idempotency_key!r} was reused with a different payload"
                    )
                return str(existing["id"]), False

            policy = asdict(intent.execution_policy)
            conn.execute(
                """INSERT INTO core_order_intents
                   (id, idempotency_key, payload_hash, strategy_id, book_id, account_id,
                    action, status, source_signal_id, execution_policy_json, metadata_json,
                    created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    intent.id,
                    intent.idempotency_key,
                    fingerprint,
                    intent.strategy_id,
                    intent.book_id,
                    intent.account_id,
                    _enum(intent.action),
                    _enum(intent.status),
                    intent.source_signal_id,
                    _json(policy),
                    _json(intent.metadata),
                    _timestamp(intent.created_at),
                    _timestamp(intent.updated_at),
                ),
            )
            for leg in sorted(intent.legs, key=lambda item: item.sequence):
                conn.execute(
                    """INSERT INTO core_order_legs
                       (id, intent_id, sequence, instrument_id, side, quantity, quantity_unit,
                        order_type, limit_price, stop_price, time_in_force, status,
                        cumulative_filled_quantity, average_fill_price, metadata_json,
                        created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '0', NULL, ?, ?, ?)""",
                    (
                        leg.id,
                        intent.id,
                        leg.sequence,
                        leg.instrument_id,
                        _enum(leg.side),
                        _decimal(leg.quantity),
                        _enum(leg.quantity_unit),
                        leg.order_type,
                        _decimal(leg.limit_price),
                        _decimal(leg.stop_price),
                        leg.time_in_force,
                        _enum(leg.status),
                        _json(leg.metadata),
                        _timestamp(leg.created_at),
                        _timestamp(leg.updated_at),
                    ),
                )
        return intent.id, True

    def get_intent(self, intent_id: str) -> dict[str, Any] | None:
        with self.transaction() as conn:
            row = conn.execute("SELECT * FROM core_order_intents WHERE id = ?", (intent_id,)).fetchone()
            if row is None:
                return None
            result = dict(row)
            result["metadata"] = _decode(result.pop("metadata_json"))
            result["execution_policy"] = _decode(result.pop("execution_policy_json"))
            result["legs"] = [
                dict(item)
                for item in conn.execute(
                    "SELECT * FROM core_order_legs WHERE intent_id = ? ORDER BY sequence", (intent_id,)
                ).fetchall()
            ]
            for leg in result["legs"]:
                leg["metadata"] = _decode(leg.pop("metadata_json"))
            return result

    def get_intent_by_idempotency_key(self, account_id: str, key: str) -> dict[str, Any] | None:
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT id FROM core_order_intents WHERE account_id = ? AND idempotency_key = ?",
                (account_id, key),
            ).fetchone()
        return self.get_intent(str(row["id"])) if row else None

    def transition_intent(self, intent_id: str, target: IntentStatus) -> None:
        self._transition("core_order_intents", intent_id, target, IntentStatus, INTENT_TRANSITIONS, "intent")

    def transition_leg(self, leg_id: str, target: LegStatus) -> None:
        self._transition("core_order_legs", leg_id, target, LegStatus, LEG_TRANSITIONS, "leg")

    def transition_broker_order(self, broker_order_id: str, target: BrokerOrderStatus) -> None:
        self._transition(
            "core_broker_orders",
            broker_order_id,
            target,
            BrokerOrderStatus,
            BROKER_ORDER_TRANSITIONS,
            "broker order",
        )

    def _transition(
        self,
        table: str,
        entity_id: str,
        target: Enum,
        enum_type: type[Enum],
        allowed: Any,
        entity: str,
    ) -> None:
        now = _timestamp(utc_now())
        with self.transaction() as conn:
            row = conn.execute(f"SELECT status FROM {table} WHERE id = ?", (entity_id,)).fetchone()
            if row is None:
                raise KeyError(f"Unknown {entity}: {entity_id}")
            current = enum_type(str(row["status"]))
            validate_transition(current, target, allowed, entity=entity)
            conn.execute(f"UPDATE {table} SET status = ?, updated_at = ? WHERE id = ?", (_enum(target), now, entity_id))

    def create_broker_order(
        self,
        *,
        broker_order_id: str,
        order_leg_id: str,
        account_id: str,
        broker: str,
        attempt_number: int | None,
        client_order_id: str | None,
        submitted_quantity: Decimal,
        replaces_broker_order_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> tuple[int, str]:
        with self.transaction() as conn:
            leg = conn.execute(
                """SELECT l.intent_id, i.account_id
                   FROM core_order_legs l
                   JOIN core_order_intents i ON i.id = l.intent_id
                   WHERE l.id = ?""",
                (order_leg_id,),
            ).fetchone()
            if leg is None:
                raise KeyError(f"Unknown leg: {order_leg_id}")
            if str(leg["account_id"]) != account_id:
                raise ValueError("broker order account does not match intent account")
            if attempt_number is None:
                attempt_number = int(
                    conn.execute(
                        "SELECT COALESCE(MAX(attempt_number), 0) + 1 FROM core_broker_orders WHERE order_leg_id = ?",
                        (order_leg_id,),
                    ).fetchone()[0]
                )
            if attempt_number <= 0:
                raise ValueError("attempt_number must be positive")
            resolved_client_order_id = client_order_id or f"{order_leg_id}:{attempt_number}"
            conn.execute(
                """INSERT INTO core_broker_orders
                   (id, order_leg_id, account_id, broker, attempt_number, external_order_id,
                    client_order_id, status, submitted_quantity, submitted_at, updated_at,
                    replaces_broker_order_id, metadata_json)
                   VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, NULL, ?, ?, ?)""",
                (
                    broker_order_id,
                    order_leg_id,
                    account_id,
                    broker,
                    attempt_number,
                    resolved_client_order_id,
                    BrokerOrderStatus.PREPARED.value,
                    _decimal(submitted_quantity),
                    _timestamp(utc_now()),
                    replaces_broker_order_id,
                    _json(metadata),
                ),
            )
        return attempt_number, resolved_client_order_id

    def record_submission(
        self,
        broker_order_id: str,
        *,
        status: BrokerOrderStatus,
        external_order_id: str | None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        now = _timestamp(utc_now())
        with self.transaction() as conn:
            row = conn.execute("SELECT status FROM core_broker_orders WHERE id = ?", (broker_order_id,)).fetchone()
            if row is None:
                raise KeyError(f"Unknown broker order: {broker_order_id}")
            current = BrokerOrderStatus(str(row["status"]))
            validate_transition(current, status, BROKER_ORDER_TRANSITIONS, entity="broker order")
            conn.execute(
                """UPDATE core_broker_orders
                   SET status = ?, external_order_id = COALESCE(?, external_order_id),
                       submitted_at = COALESCE(submitted_at, ?), updated_at = ?, metadata_json = ?
                   WHERE id = ?""",
                (_enum(status), external_order_id, now, now, _json(metadata), broker_order_id),
            )

    def get_broker_order(self, broker_order_id: str) -> dict[str, Any] | None:
        with self.transaction() as conn:
            row = conn.execute("SELECT * FROM core_broker_orders WHERE id = ?", (broker_order_id,)).fetchone()
            if row is None:
                return None
            result = dict(row)
            result["metadata"] = _decode(result.pop("metadata_json"))
            return result

    def broker_orders_for_leg(self, leg_id: str) -> list[dict[str, Any]]:
        with self.transaction() as conn:
            rows = conn.execute(
                "SELECT * FROM core_broker_orders WHERE order_leg_id = ? ORDER BY attempt_number", (leg_id,)
            ).fetchall()
            return [dict(row) for row in rows]

    def append_broker_event(
        self,
        *,
        event_id: str,
        broker_order_id: str,
        dedupe_key: str,
        event_type: str,
        broker_status: str | None,
        event_at: datetime,
        received_at: datetime,
        external_event_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        with self.transaction() as conn:
            values = (
                external_event_id,
                event_type,
                broker_status,
                _timestamp(event_at),
                _json(metadata),
            )
            existing = conn.execute(
                """SELECT external_event_id, event_type, broker_status, event_at, metadata_json
                   FROM core_broker_order_events
                   WHERE broker_order_id = ? AND dedupe_key = ?""",
                (broker_order_id, dedupe_key),
            ).fetchone()
            if existing:
                if tuple(existing) != values:
                    raise ValueError("broker event dedupe key was reused with different evidence")
                return False
            conn.execute(
                """INSERT INTO core_broker_order_events
                   (id, broker_order_id, dedupe_key, external_event_id, event_type,
                    broker_status, event_at, received_at, metadata_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    event_id,
                    broker_order_id,
                    dedupe_key,
                    external_event_id,
                    event_type,
                    broker_status,
                    _timestamp(event_at),
                    _timestamp(received_at),
                    _json(metadata),
                ),
            )
            return True

    def record_fill(self, fill: Fill) -> bool:
        """Append one fill exactly once and update its logical leg monotonically."""
        quantity = Decimal(fill.quantity)
        price = Decimal(fill.price)
        if quantity <= 0 or price <= 0:
            raise ValueError("fill quantity and price must be positive")
        now = _timestamp(utc_now())
        with self.transaction() as conn:
            leg = conn.execute(
                """SELECT l.quantity, l.cumulative_filled_quantity, l.average_fill_price,
                          l.status, l.instrument_id, l.side, l.intent_id,
                          i.account_id, i.strategy_id, i.book_id
                   FROM core_order_legs l
                   JOIN core_order_intents i ON i.id = l.intent_id
                   WHERE l.id = ?""",
                (fill.order_leg_id,),
            ).fetchone()
            if leg is None:
                raise KeyError(f"Unknown leg: {fill.order_leg_id}")
            attempt = conn.execute(
                "SELECT order_leg_id, account_id FROM core_broker_orders WHERE id = ?",
                (fill.broker_order_id,),
            ).fetchone()
            if attempt is None:
                raise KeyError(f"Unknown broker order: {fill.broker_order_id}")
            if str(attempt["order_leg_id"]) != fill.order_leg_id:
                raise ValueError("fill broker order does not belong to the supplied logical leg")
            if str(attempt["account_id"]) != str(leg["account_id"]):
                raise ValueError("fill broker order account does not match intent account")
            values = (
                fill.external_fill_id,
                _decimal(quantity),
                _decimal(price),
                _decimal(fill.fee),
                fill.fee_currency,
                _timestamp(fill.filled_at),
                _json(fill.metadata),
            )
            existing = conn.execute(
                """SELECT external_fill_id, quantity, price, fee, fee_currency, filled_at, metadata_json
                   FROM core_fills WHERE broker_order_id = ? AND dedupe_key = ?""",
                (fill.broker_order_id, fill.dedupe_key),
            ).fetchone()
            if existing:
                if tuple(existing) != values:
                    raise ValueError("fill dedupe key was reused with different evidence")
                return False
            conn.execute(
                """INSERT INTO core_fills
                   (id, broker_order_id, order_leg_id, external_fill_id, dedupe_key,
                    quantity, price, fee, fee_currency, filled_at, received_at, metadata_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    fill.id,
                    fill.broker_order_id,
                    fill.order_leg_id,
                    fill.external_fill_id,
                    fill.dedupe_key,
                    _decimal(quantity),
                    _decimal(price),
                    _decimal(fill.fee),
                    fill.fee_currency,
                    _timestamp(fill.filled_at),
                    _timestamp(fill.received_at),
                    _json(fill.metadata),
                ),
            )

            requested = Decimal(str(leg["quantity"]))
            previous = Decimal(str(leg["cumulative_filled_quantity"]))
            cumulative = previous + quantity
            if cumulative > requested:
                raise ValueError(f"fill would overfill leg {fill.order_leg_id}: {cumulative} > {requested}")
            old_average = Decimal(str(leg["average_fill_price"])) if leg["average_fill_price"] else Decimal("0")
            average = ((old_average * previous) + (price * quantity)) / cumulative
            target = LegStatus.FILLED if cumulative == requested else LegStatus.PARTIALLY_FILLED
            current = LegStatus(str(leg["status"]))
            validate_transition(current, target, LEG_TRANSITIONS, entity="leg")
            conn.execute(
                """UPDATE core_order_legs
                   SET cumulative_filled_quantity = ?, average_fill_price = ?, status = ?, updated_at = ?
                   WHERE id = ?""",
                (_decimal(cumulative), _decimal(average), target.value, now, fill.order_leg_id),
            )
            signed_delta = quantity if str(leg["side"]) == "BUY" else -quantity
            allocation_id = (
                f"managed:{leg['account_id']}:{leg['instrument_id']}:{leg['strategy_id']}:"
                f"{leg['book_id'] or ''}:{leg['intent_id']}"
            )
            existing_allocation = conn.execute(
                "SELECT signed_quantity FROM core_position_allocations WHERE id = ?",
                (allocation_id,),
            ).fetchone()
            allocation_quantity = signed_delta
            if existing_allocation:
                allocation_quantity += Decimal(str(existing_allocation["signed_quantity"]))
                conn.execute(
                    "UPDATE core_position_allocations SET signed_quantity = ?, updated_at = ? WHERE id = ?",
                    (_decimal(allocation_quantity), now, allocation_id),
                )
            else:
                conn.execute(
                    """INSERT INTO core_position_allocations
                       (id, account_id, instrument_id, strategy_id, book_id, ownership_class,
                        signed_quantity, source_intent_id, updated_at, metadata_json)
                       VALUES (?, ?, ?, ?, ?, 'MANAGED', ?, ?, ?, '{}')""",
                    (
                        allocation_id,
                        leg["account_id"],
                        leg["instrument_id"],
                        leg["strategy_id"],
                        leg["book_id"],
                        _decimal(allocation_quantity),
                        leg["intent_id"],
                        now,
                    ),
                )
            broker_row = conn.execute(
                "SELECT status FROM core_broker_orders WHERE id = ?", (fill.broker_order_id,)
            ).fetchone()
            if broker_row:
                broker_current = BrokerOrderStatus(str(broker_row["status"]))
                broker_target = BrokerOrderStatus.FILLED if cumulative == requested else BrokerOrderStatus.PARTIALLY_FILLED
                # A terminal broker status can arrive before the corresponding
                # deal stream. Preserve that broker fact while fills rebuild
                # the logical leg and allocation ledger.
                if broker_current is not BrokerOrderStatus.FILLED:
                    validate_transition(broker_current, broker_target, BROKER_ORDER_TRANSITIONS, entity="broker order")
                    conn.execute(
                        "UPDATE core_broker_orders SET status = ?, updated_at = ? WHERE id = ?",
                        (broker_target.value, now, fill.broker_order_id),
                    )
            self._refresh_intent_status_conn(conn, fill.order_leg_id, now)
        return True

    def _refresh_intent_status_conn(self, conn: sqlite3.Connection, leg_id: str, now: str) -> None:
        row = conn.execute("SELECT intent_id FROM core_order_legs WHERE id = ?", (leg_id,)).fetchone()
        if row is None:
            return
        intent_id = str(row["intent_id"])
        statuses = [
            LegStatus(str(item["status"]))
            for item in conn.execute("SELECT status FROM core_order_legs WHERE intent_id = ?", (intent_id,)).fetchall()
        ]
        current_row = conn.execute("SELECT status FROM core_order_intents WHERE id = ?", (intent_id,)).fetchone()
        if current_row is None:
            return
        current = IntentStatus(str(current_row["status"]))
        if any(status is LegStatus.RECONCILIATION_REQUIRED for status in statuses):
            target = IntentStatus.RECONCILIATION_REQUIRED
        elif all(status is LegStatus.FILLED for status in statuses):
            target = IntentStatus.FILLED
        elif any(status in {LegStatus.PARTIALLY_FILLED, LegStatus.FILLED} for status in statuses):
            target = IntentStatus.PARTIALLY_FILLED
        else:
            return
        validate_transition(current, target, INTENT_TRANSITIONS, entity="intent")
        conn.execute("UPDATE core_order_intents SET status = ?, updated_at = ? WHERE id = ?", (target.value, now, intent_id))

    def fills_for_leg(self, leg_id: str) -> list[dict[str, Any]]:
        with self.transaction() as conn:
            return [
                dict(row)
                for row in conn.execute("SELECT * FROM core_fills WHERE order_leg_id = ? ORDER BY filled_at", (leg_id,)).fetchall()
            ]

    def save_broker_snapshot(
        self,
        snapshot: BrokerSnapshot,
        *,
        balances: tuple[AccountBalanceSnapshot, ...] = (),
        positions: tuple[PositionSnapshot, ...] = (),
        orders: tuple[BrokerOrderSnapshot, ...] = (),
    ) -> None:
        """Persist one immutable broker-truth observation and its child rows."""
        for item in (*balances, *positions, *orders):
            if item.broker_snapshot_id != snapshot.id:
                raise ValueError("snapshot child does not belong to broker snapshot")
        for item in (*positions, *orders):
            if item.account_id != snapshot.account_id:
                raise ValueError("snapshot child account does not match broker snapshot account")
        with self.transaction() as conn:
            conn.execute(
                """INSERT INTO core_broker_snapshots
                   (id, account_id, captured_at, status, error, metadata_json)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    snapshot.id,
                    snapshot.account_id,
                    _timestamp(snapshot.captured_at),
                    snapshot.status,
                    snapshot.error,
                    _json(snapshot.metadata),
                ),
            )
            for item in balances:
                conn.execute(
                    """INSERT INTO core_account_balance_snapshots
                       (id, broker_snapshot_id, currency, cash, buying_power, equity,
                        initial_margin, maintenance_margin, metadata_json)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        item.id,
                        item.broker_snapshot_id,
                        item.currency,
                        _decimal(item.cash),
                        _decimal(item.buying_power),
                        _decimal(item.equity),
                        _decimal(item.initial_margin),
                        _decimal(item.maintenance_margin),
                        _json(item.metadata),
                    ),
                )
            for item in positions:
                conn.execute(
                    """INSERT INTO core_position_snapshots
                       (id, broker_snapshot_id, account_id, instrument_id, signed_quantity,
                        average_price, captured_at, metadata_json)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        item.id,
                        item.broker_snapshot_id,
                        item.account_id,
                        item.instrument_id,
                        _decimal(item.signed_quantity),
                        _decimal(item.average_price),
                        _timestamp(item.captured_at),
                        _json(item.metadata),
                    ),
                )
            for item in orders:
                conn.execute(
                    """INSERT INTO core_broker_order_snapshots
                       (id, broker_snapshot_id, account_id, instrument_id, external_order_id,
                        client_order_id, side, quantity, filled_quantity, status, captured_at,
                        metadata_json)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        item.id,
                        item.broker_snapshot_id,
                        item.account_id,
                        item.instrument_id,
                        item.external_order_id,
                        item.client_order_id,
                        _enum(item.side),
                        _decimal(item.quantity),
                        _decimal(item.filled_quantity),
                        _enum(item.status),
                        _timestamp(item.captured_at),
                        _json(item.metadata),
                    ),
                )

    def save_position_allocation(self, allocation: PositionAllocation) -> None:
        with self.transaction() as conn:
            conn.execute(
                """INSERT INTO core_position_allocations
                   (id, account_id, instrument_id, strategy_id, book_id, ownership_class,
                    signed_quantity, source_intent_id, updated_at, metadata_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(id) DO UPDATE SET
                       strategy_id = excluded.strategy_id,
                       book_id = excluded.book_id,
                       ownership_class = excluded.ownership_class,
                       signed_quantity = excluded.signed_quantity,
                       source_intent_id = excluded.source_intent_id,
                       updated_at = excluded.updated_at,
                       metadata_json = excluded.metadata_json""",
                (
                    allocation.id,
                    allocation.account_id,
                    allocation.instrument_id,
                    allocation.strategy_id,
                    allocation.book_id,
                    _enum(allocation.ownership_class),
                    _decimal(allocation.signed_quantity),
                    allocation.source_intent_id,
                    _timestamp(allocation.updated_at),
                    _json(allocation.metadata),
                ),
            )

    def position_allocations(self, account_id: str) -> list[dict[str, Any]]:
        with self.transaction() as conn:
            return [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM core_position_allocations WHERE account_id = ? ORDER BY instrument_id, id",
                    (account_id,),
                ).fetchall()
            ]

    def save_risk_decision(self, decision: RiskDecisionRecord) -> None:
        with self.transaction() as conn:
            values = (
                decision.intent_id,
                int(decision.approved),
                decision.reason,
                _json(decision.checks),
                _timestamp(decision.evaluated_at),
                _json(decision.metadata),
            )
            existing = conn.execute(
                """SELECT intent_id, approved, reason, checks_json, evaluated_at, metadata_json
                   FROM core_risk_decisions WHERE id = ?""",
                (decision.id,),
            ).fetchone()
            if existing:
                if tuple(existing) != values:
                    raise ValueError("risk decision ID was reused with different evidence")
                return
            conn.execute(
                """INSERT INTO core_risk_decisions
                   (id, intent_id, approved, reason, checks_json, evaluated_at, metadata_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    decision.id,
                    *values,
                ),
            )

    def save_reconciliation_run(self, run: ReconciliationRun) -> None:
        with self.transaction() as conn:
            conn.execute(
                """INSERT INTO core_reconciliation_runs
                   (id, account_id, broker_snapshot_id, started_at, completed_at, status, metadata_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    run.id,
                    run.account_id,
                    run.broker_snapshot_id,
                    _timestamp(run.started_at),
                    _timestamp(run.completed_at),
                    _enum(run.status),
                    _json(run.metadata),
                ),
            )

    def upsert_reconciliation_issue(self, issue: ReconciliationIssue) -> str:
        """Keep severe discrepancies sticky across otherwise clean snapshots."""
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT id FROM core_reconciliation_issues WHERE account_id = ? AND issue_key = ?",
                (issue.account_id, issue.issue_key),
            ).fetchone()
            if existing:
                conn.execute(
                    """UPDATE core_reconciliation_issues
                       SET run_id = ?, entity_type = ?, entity_key = ?, category = ?, severity = ?,
                           status = 'OPEN', sticky = ?, details_json = ?, last_seen_at = ?,
                           occurrence_count = occurrence_count + 1, resolved_at = NULL
                       WHERE id = ?""",
                    (
                        issue.run_id,
                        issue.entity_type,
                        issue.entity_key,
                        issue.category,
                        _enum(issue.severity),
                        int(issue.sticky),
                        _json(issue.details),
                        _timestamp(issue.detected_at),
                        str(existing["id"]),
                    ),
                )
                return str(existing["id"])
            conn.execute(
                """INSERT INTO core_reconciliation_issues
                   (id, run_id, account_id, issue_key, entity_type, entity_key, category,
                    severity, status, sticky, details_json, detected_at, last_seen_at,
                    occurrence_count, resolved_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, NULL)""",
                (
                    issue.id,
                    issue.run_id,
                    issue.account_id,
                    issue.issue_key,
                    issue.entity_type,
                    issue.entity_key,
                    issue.category,
                    _enum(issue.severity),
                    _enum(issue.status),
                    int(issue.sticky),
                    _json(issue.details),
                    _timestamp(issue.detected_at),
                    _timestamp(issue.detected_at),
                ),
            )
            return issue.id

    def resolve_reconciliation_issue(self, issue_id: str, *, resolved_at: datetime | None = None) -> None:
        with self.transaction() as conn:
            row = conn.execute("SELECT id FROM core_reconciliation_issues WHERE id = ?", (issue_id,)).fetchone()
            if row is None:
                raise KeyError(f"Unknown reconciliation issue: {issue_id}")
            conn.execute(
                "UPDATE core_reconciliation_issues SET status = 'RESOLVED', resolved_at = ? WHERE id = ?",
                (_timestamp(resolved_at or utc_now()), issue_id),
            )

    def open_reconciliation_issues(self, account_id: str) -> list[dict[str, Any]]:
        with self.transaction() as conn:
            return [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM core_reconciliation_issues WHERE account_id = ? AND status = 'OPEN' ORDER BY detected_at",
                    (account_id,),
                ).fetchall()
            ]

    @staticmethod
    def new_id() -> str:
        return str(uuid.uuid4())
