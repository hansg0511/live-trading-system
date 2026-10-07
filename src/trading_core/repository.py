"""SQLite persistence for the side-by-side broker-neutral trading core."""

from __future__ import annotations

from contextlib import contextmanager
from collections.abc import Mapping
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
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
    BrokerOrderEvent,
    BrokerOrderStatus,
    BrokerSnapshot,
    ExecutionPolicy,
    ExecutionEvidenceMode,
    ExecutionEvidenceBaseline,
    Fill,
    Instrument,
    InstrumentMapping,
    IntentStatus,
    LegStatus,
    OrderIntent,
    PositionAllocation,
    OwnershipClass,
    PositionSnapshot,
    ReconciliationIssue,
    ReconciliationRun,
    RecoveryAction,
    RecoveryActionStatus,
    RiskDecisionRecord,
    Strategy,
    TradingEnvironment,
)
from .state_machine import (
    BROKER_ORDER_TRANSITIONS,
    INTENT_TRANSITIONS,
    LEG_TRANSITIONS,
    validate_transition,
)
from .provider_payload import ProviderPayloadError, coerce_provider_payload, normalize_provider_key
from .stage6_validation import (
    Stage6EvidenceClass,
    Stage6SessionOutcome,
    Stage6SessionResult,
    Stage6ValidationError,
    canonical_evidence_json,
    evaluate_stage6_session,
    stage6_completion_status,
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


def _fingerprint(value: Any) -> str:
    """Stable hash for durable reconciliation evidence."""

    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def _redact_operational_value(value: Any, *, key: str | None = None) -> Any:
    """Redact sensitive operational details at the persistence boundary.

    This intentionally lives in the repository rather than importing the
    Stage 7 operations module, so direct repository callers cannot bypass the
    audit-record redaction contract or introduce a circular dependency.
    """
    normalized = str(key or "").lower().replace("-", "_")
    sensitive_terms = (
        "password",
        "secret",
        "token",
        "api_key",
        "apikey",
        "access_key",
        "authorization",
        "credential",
        "private_key",
    )
    if any(term in normalized for term in sensitive_terms):
        return "<redacted>"
    if "payload" in normalized and any(term in normalized for term in ("private", "response", "raw")):
        return "<redacted>"
    if normalized in {"private_response", "response_body", "raw_body"}:
        return "<redacted>"
    if isinstance(value, Mapping):
        return {str(item_key): _redact_operational_value(item, key=str(item_key)) for item_key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_redact_operational_value(item) for item in value]
    return value


def _payload_hash(intent: OrderIntent, *, include_stale_order_seconds: bool = True) -> str:
    policy = asdict(intent.execution_policy)
    if not include_stale_order_seconds:
        # Stage 2 persisted hashes predate stale-order policy.  Keep this
        # compatibility fingerprint private to idempotency lookup.
        policy.pop("stale_order_seconds", None)
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
        # Trusted paths use capabilities bound to this repository instance.
        # A module-level marker could be replayed against another database.
        self.__fill_validation_capability = object()
        self.__resolution_capability = object()
        self.__allocation_validation_capability = object()

    def _fill_validation_capability(self) -> object:
        """Return the private capability used by validated OMS ingestion."""
        return self.__fill_validation_capability

    def _resolution_capability(self) -> object:
        """Return the private capability used by validated OMS resolution."""
        return self.__resolution_capability

    def _allocation_validation_capability(self) -> object:
        """Return the instance-scoped capability for validated ledger writes."""
        return self.__allocation_validation_capability

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
            self._migrate_stage4_book_columns_conn(conn)
            # Additive migration for databases created before event evidence
            # fingerprints had dedicated columns.  Provider metadata remains
            # untouched, including any values that happen to look like OMS
            # envelope keys.
            columns = {
                str(row["name"])
                for row in conn.execute("PRAGMA table_info(core_broker_order_events)").fetchall()
            }
            if "oms_fill_fingerprint" not in columns:
                conn.execute(
                    "ALTER TABLE core_broker_order_events ADD COLUMN oms_fill_fingerprint TEXT"
                )
            if "oms_event_fingerprint" not in columns:
                conn.execute(
                    "ALTER TABLE core_broker_order_events ADD COLUMN oms_event_fingerprint TEXT"
                )
            fill_columns = {
                str(row["name"])
                for row in conn.execute("PRAGMA table_info(core_fills)").fetchall()
            }
            if "evidence_mode" not in fill_columns:
                conn.execute(
                    "ALTER TABLE core_fills ADD COLUMN evidence_mode TEXT NOT NULL DEFAULT 'INDIVIDUAL_DEALS'"
                )
            conn.execute(
                """INSERT OR IGNORE INTO core_schema_migrations(version, applied_at, description)
                   VALUES (3, ?, ?)""",
                (_timestamp(utc_now()), "Dedicated broker-event evidence fingerprints"),
            )
            conn.execute(
                """INSERT OR IGNORE INTO core_schema_migrations(version, applied_at, description)
                   VALUES (7, ?, ?)""",
                (_timestamp(utc_now()), "Stage 7 local operational audit events"),
            )
            conn.execute(
                """INSERT OR IGNORE INTO core_schema_migrations(version, applied_at, description)
                   VALUES (8, ?, ?)""",
                (_timestamp(utc_now()), "Typed execution-evidence mode and bounded SIM baselines"),
            )
            conn.execute(
                """INSERT OR IGNORE INTO core_schema_migrations(version, applied_at, description)
                   VALUES (9, ?, ?)""",
                (_timestamp(utc_now()), "Durable Stage 6 supervised SIM validation evidence"),
            )
            self._audit_legacy_duplicate_fills_conn(conn)
            self._audit_legacy_duplicate_broker_orders_conn(conn)

    @staticmethod
    def _migrate_stage4_book_columns_conn(conn: sqlite3.Connection) -> None:
        """Add Stage 4 ownership columns/indexes to pre-Stage 4 databases.

        ``CREATE TABLE IF NOT EXISTS`` cannot alter an existing legacy table,
        and the schema file must remain runnable against those tables.  Keep
        this migration additive, deterministic, and safe to repeat on every
        restart; historical rows receive NULL/UNKNOWN ownership and are
        quarantined by the Stage 4 OMS gate when the account opts in.
        """
        for table in ("core_order_intents", "core_position_allocations"):
            columns = {
                str(row["name"])
                for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
            }
            if "book_id" not in columns:
                conn.execute(
                    f"ALTER TABLE {table} ADD COLUMN book_id TEXT REFERENCES core_books(id) ON DELETE RESTRICT"
                )
        # These indexes intentionally run after the ALTER TABLE statements so
        # initialization also succeeds for databases created before book_id.
        conn.execute(
            """CREATE INDEX IF NOT EXISTS idx_core_book_allocations_account
               ON core_book_allocations(account_id, book_id, strategy_id, effective_at, expires_at)"""
        )
        conn.execute(
            """CREATE INDEX IF NOT EXISTS idx_core_intents_book
               ON core_order_intents(account_id, book_id, status, created_at)"""
        )
        conn.execute(
            """INSERT OR IGNORE INTO core_schema_migrations(version, applied_at, description)
               VALUES (4, ?, ?)""",
            (_timestamp(utc_now()), "Stage 4 shared book ownership and capacity indexes"),
        )

    def audit_legacy_duplicate_fills(self) -> None:
        """Quarantine pre-existing duplicate external deal identities."""
        with self.transaction() as conn:
            self._audit_legacy_duplicate_fills_conn(conn)

    def audit_legacy_duplicate_broker_orders(self) -> None:
        """Quarantine pre-existing claims of one provider order by multiple attempts.

        Current schemas constrain ``(account_id, external_order_id)``.  Older
        databases may predate that constraint, and cross-account duplicate
        claims are unsafe even when the current constraint is present.  The
        audit is intentionally additive and durable so a matching current
        attempt cannot silently bypass a sibling legacy claim.
        """
        with self.transaction() as conn:
            self._audit_legacy_duplicate_broker_orders_conn(conn)

    def _audit_legacy_duplicate_broker_orders_conn(self, conn: sqlite3.Connection) -> None:
        duplicated = conn.execute(
            """SELECT external_order_id, COUNT(*) AS duplicate_count
                 FROM core_broker_orders
                WHERE external_order_id IS NOT NULL
                  AND trim(external_order_id) <> ''
                GROUP BY external_order_id
               HAVING COUNT(*) > 1
                ORDER BY external_order_id"""
        ).fetchall()
        now = _timestamp(utc_now())
        for duplicate in duplicated:
            external_order_id = str(duplicate["external_order_id"])
            claims = conn.execute(
                """SELECT b.id AS broker_order_id, b.account_id, l.intent_id,
                          b.order_leg_id, b.status, b.updated_at
                     FROM core_broker_orders b
                     JOIN core_order_legs l ON l.id = b.order_leg_id
                    WHERE b.external_order_id = ?
                    ORDER BY b.account_id, l.intent_id, b.id""",
                (external_order_id,),
            ).fetchall()
            by_account: dict[str, list[sqlite3.Row]] = {}
            for claim in claims:
                by_account.setdefault(str(claim["account_id"]), []).append(claim)
            for account_id, account_claims in by_account.items():
                claim_details = [
                    {
                        "broker_order_id": str(item["broker_order_id"]),
                        "intent_id": str(item["intent_id"]),
                        "order_leg_id": str(item["order_leg_id"]),
                        "status": str(item["status"]),
                        "updated_at": item["updated_at"],
                    }
                    for item in account_claims
                ]
                token = f"legacy-duplicate-broker-order:{account_id}:{external_order_id}"
                run_id = str(uuid.uuid5(uuid.NAMESPACE_URL, token + ":run"))
                issue_id = str(uuid.uuid5(uuid.NAMESPACE_URL, token + ":issue"))
                details = _json(
                    {
                        "external_order_id": external_order_id,
                        "duplicate_count": int(duplicate["duplicate_count"]),
                        "claims": claim_details,
                        "source": "startup_legacy_broker_order_audit",
                    }
                )
                conn.execute(
                    """INSERT OR IGNORE INTO core_reconciliation_runs
                       (id, account_id, broker_snapshot_id, started_at, completed_at,
                        status, metadata_json)
                       VALUES (?, ?, NULL, ?, ?, 'COMPLETED', ?)""",
                    (run_id, account_id, now, now, details),
                )
                issue_key = f"LEGACY_DUPLICATE_BROKER_ORDER:{external_order_id}"
                conn.execute(
                    """INSERT INTO core_reconciliation_issues
                       (id, run_id, account_id, issue_key, entity_type, entity_key,
                        category, severity, status, sticky, details_json, detected_at,
                        last_seen_at, occurrence_count, resolved_at)
                       VALUES (?, ?, ?, ?, 'BROKER_ORDER', ?,
                               'LEGACY_DUPLICATE_BROKER_ORDER', 'CRITICAL', 'OPEN', 1,
                               ?, ?, ?, 1, NULL)
                       ON CONFLICT(account_id, issue_key) DO UPDATE SET
                           run_id = excluded.run_id,
                           details_json = excluded.details_json,
                           status = 'OPEN', sticky = 1,
                           last_seen_at = excluded.last_seen_at,
                           occurrence_count = core_reconciliation_issues.occurrence_count + 1,
                           resolved_at = NULL""",
                    (issue_id, run_id, account_id, issue_key, external_order_id, details, now, now),
                )
                for claim in account_claims:
                    intent_id = str(claim["intent_id"])
                    action_key = f"LEGACY_DUPLICATE_BROKER_ORDER:{external_order_id}"
                    action_id = str(uuid.uuid5(uuid.NAMESPACE_URL, token + f":action:{intent_id}"))
                    conn.execute(
                        """INSERT INTO core_recovery_actions
                           (id, intent_id, account_id, action_key, state, summary,
                            observed_positions_json, remaining_quantities_json, stale,
                            timed_out, allowed_next_steps_json, status, detected_at,
                            last_seen_at, occurrence_count, resolved_at, metadata_json)
                           VALUES (?, ?, ?, ?, 'RECONCILIATION_REQUIRED', ?, '{}', '{}', 0,
                                   0, ?, 'OPEN', ?, ?, 1, NULL, ?)
                           ON CONFLICT(account_id, intent_id, action_key) DO UPDATE SET
                               state = 'RECONCILIATION_REQUIRED', status = 'OPEN',
                               summary = excluded.summary,
                               last_seen_at = excluded.last_seen_at,
                               occurrence_count = core_recovery_actions.occurrence_count + 1,
                               resolved_at = NULL, metadata_json = excluded.metadata_json""",
                        (
                            action_id,
                            intent_id,
                            account_id,
                            action_key,
                            "Multiple durable attempts claim the same provider order; explicit broker-order reconciliation is required before any lifecycle or submission.",
                            _json(["refresh_broker_orders", "reconcile_broker_order", "operator_review"]),
                            now,
                            now,
                            details,
                        ),
                    )

    def _audit_legacy_duplicate_fills_conn(self, conn: sqlite3.Connection) -> None:
        rows = conn.execute(
            """SELECT f.broker_order_id, f.external_fill_id, COUNT(*) AS duplicate_count,
                      o.account_id, l.intent_id
                 FROM core_fills f
                 JOIN core_broker_orders o ON o.id = f.broker_order_id
                 JOIN core_order_legs l ON l.id = f.order_leg_id
                WHERE f.external_fill_id IS NOT NULL AND trim(f.external_fill_id) <> ''
                GROUP BY f.broker_order_id, f.external_fill_id, o.account_id, l.intent_id
               HAVING COUNT(*) > 1"""
        ).fetchall()
        now = _timestamp(utc_now())
        for row in rows:
            account_id = str(row["account_id"])
            broker_order_id = str(row["broker_order_id"])
            external_fill_id = str(row["external_fill_id"])
            intent_id = str(row["intent_id"])
            token = f"legacy-duplicate-fill:{account_id}:{broker_order_id}:{external_fill_id}"
            run_id = str(uuid.uuid5(uuid.NAMESPACE_URL, token + ":run"))
            issue_id = str(uuid.uuid5(uuid.NAMESPACE_URL, token + ":issue"))
            action_id = str(uuid.uuid5(uuid.NAMESPACE_URL, token + ":action"))
            issue_key = f"LEGACY_DUPLICATE_EXTERNAL_FILL:{broker_order_id}:{external_fill_id}"
            action_key = f"LEGACY_DUPLICATE_EXTERNAL_FILL:{broker_order_id}:{external_fill_id}"
            details = _json(
                {
                    "broker_order_id": broker_order_id,
                    "external_fill_id": external_fill_id,
                    "duplicate_count": int(row["duplicate_count"]),
                    "source": "startup_legacy_fill_audit",
                }
            )
            conn.execute(
                """INSERT OR IGNORE INTO core_reconciliation_runs
                   (id, account_id, broker_snapshot_id, started_at, completed_at,
                    status, metadata_json)
                   VALUES (?, ?, NULL, ?, ?, 'COMPLETED', ?)""",
                (run_id, account_id, now, now, details),
            )
            conn.execute(
                """INSERT INTO core_reconciliation_issues
                   (id, run_id, account_id, issue_key, entity_type, entity_key,
                    category, severity, status, sticky, details_json, detected_at,
                    last_seen_at, occurrence_count, resolved_at)
                   VALUES (?, ?, ?, ?, 'BROKER_FILL', ?,
                           'LEGACY_DUPLICATE_EXTERNAL_FILL', 'CRITICAL', 'OPEN', 1,
                           ?, ?, ?, 1, NULL)
                   ON CONFLICT(account_id, issue_key) DO UPDATE SET
                       run_id = excluded.run_id,
                       details_json = excluded.details_json,
                       status = 'OPEN', sticky = 1,
                       last_seen_at = excluded.last_seen_at,
                       occurrence_count = core_reconciliation_issues.occurrence_count + 1,
                       resolved_at = NULL""",
                (issue_id, run_id, account_id, issue_key, broker_order_id, details, now, now),
            )
            conn.execute(
                """INSERT INTO core_recovery_actions
                   (id, intent_id, account_id, action_key, state, summary,
                    observed_positions_json, remaining_quantities_json, stale,
                    timed_out, allowed_next_steps_json, status, detected_at,
                    last_seen_at, occurrence_count, resolved_at, metadata_json)
                   VALUES (?, ?, ?, ?, 'RECONCILIATION_REQUIRED', ?, '{}', '{}', 0,
                           0, ?, 'OPEN', ?, ?, 1, NULL, ?)
                   ON CONFLICT(account_id, intent_id, action_key) DO UPDATE SET
                       state = 'RECONCILIATION_REQUIRED', status = 'OPEN',
                       summary = excluded.summary,
                       last_seen_at = excluded.last_seen_at,
                       occurrence_count = core_recovery_actions.occurrence_count + 1,
                       resolved_at = NULL, metadata_json = excluded.metadata_json""",
                (
                    action_id,
                    intent_id,
                    account_id,
                    action_key,
                    "Duplicate legacy broker fill identity requires explicit reconciliation before lifecycle or submission.",
                    _json(["refresh_broker_fills", "reconcile_broker_order", "operator_review"]),
                    now,
                    now,
                    details,
                ),
            )

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

    def get_account(self, account_id: str) -> Account | None:
        """Load the persisted account identity for safe OMS-side validation."""
        with self.transaction() as conn:
            row = conn.execute("SELECT * FROM core_accounts WHERE id = ?", (account_id,)).fetchone()
        if row is None:
            return None

        def parse_timestamp(value: object) -> datetime:
            if not isinstance(value, str):
                raise ValueError(f"invalid account timestamp for {account_id}")
            parsed = datetime.fromisoformat(value)
            if parsed.tzinfo is None or parsed.utcoffset() is None:
                raise ValueError(f"account timestamp must be timezone-aware for {account_id}")
            return parsed

        return Account(
            id=str(row["id"]),
            broker=str(row["broker"]),
            environment=TradingEnvironment(str(row["environment"])),
            external_account_id=str(row["external_account_id"]),
            base_currency=str(row["base_currency"]),
            enabled=bool(row["enabled"]),
            metadata=_decode(row["metadata_json"]),
            created_at=parse_timestamp(row["created_at"]),
            updated_at=parse_timestamp(row["updated_at"]),
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

    def get_instrument(self, instrument_id: str) -> dict[str, Any] | None:
        """Load one instrument declaration for canonical config comparison."""

        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM core_instruments WHERE id = ?",
                (str(instrument_id),),
            ).fetchone()
        if row is None:
            return None
        result = dict(row)
        result["metadata"] = _decode(result.pop("metadata_json"))
        return result

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

    def get_instrument_mapping(self, mapping_id: str) -> dict[str, Any] | None:
        """Load one provider mapping by durable identity."""

        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM core_instrument_mappings WHERE id = ?",
                (str(mapping_id),),
            ).fetchone()
        if row is None:
            return None
        result = dict(row)
        result["metadata"] = _decode(result.pop("metadata_json"))
        return result

    def find_instrument_mapping(
        self,
        *,
        instrument_id: str,
        provider: str,
        purpose: str,
    ) -> dict[str, Any] | None:
        """Load a mapping by its unique instrument/provider/purpose key."""

        with self.transaction() as conn:
            row = conn.execute(
                """SELECT * FROM core_instrument_mappings
                   WHERE instrument_id = ? AND provider = ? AND purpose = ?""",
                (str(instrument_id), str(provider), str(purpose)),
            ).fetchone()
        if row is None:
            return None
        result = dict(row)
        result["metadata"] = _decode(result.pop("metadata_json"))
        return result

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

    def get_strategy(self, strategy_id: str) -> Strategy | None:
        """Load a declared strategy for generic configuration validation."""
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM core_strategies WHERE id = ?",
                (str(strategy_id),),
            ).fetchone()
        if row is None:
            return None

        def parse_timestamp(value: object) -> datetime:
            if not isinstance(value, str):
                raise ValueError(f"invalid strategy timestamp for {strategy_id}")
            parsed = datetime.fromisoformat(value)
            if parsed.tzinfo is None or parsed.utcoffset() is None:
                raise ValueError(f"strategy timestamp must be timezone-aware for {strategy_id}")
            return parsed

        return Strategy(
            id=str(row["id"]),
            name=str(row["name"]),
            strategy_type=str(row["strategy_type"]),
            version=str(row["version"]),
            enabled=bool(row["enabled"]),
            config=_decode(row["config_json"]),
            metadata=_decode(row["metadata_json"]),
            created_at=parse_timestamp(row["created_at"]),
            updated_at=parse_timestamp(row["updated_at"]),
        )

    def get_book(self, book_id: str) -> dict[str, Any] | None:
        """Load one declared book, preserving its durable ownership metadata."""
        with self.transaction() as conn:
            row = conn.execute("SELECT * FROM core_books WHERE id = ?", (str(book_id),)).fetchone()
        if row is None:
            return None
        result = dict(row)
        result["metadata"] = _decode(result.pop("metadata_json"))
        result["enabled"] = bool(result["enabled"])
        return result

    def list_books(self, *, enabled_only: bool = False) -> list[dict[str, Any]]:
        """Return declared books for operator/configuration views."""
        query = "SELECT * FROM core_books"
        params: tuple[Any, ...] = ()
        if enabled_only:
            query += " WHERE enabled = 1"
        query += " ORDER BY id"
        with self.transaction() as conn:
            rows = conn.execute(query, params).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["metadata"] = _decode(item.pop("metadata_json"))
            item["enabled"] = bool(item["enabled"])
            result.append(item)
        return result

    def save_book_allocation(self, allocation: BookAllocation) -> None:
        # Constructing a BookAllocation validates the shape.  Resolve the
        # basis here as well so rows loaded from old databases cannot create
        # an unbounded or mixed-unit risk configuration.
        allocation.limit_basis
        with self.transaction() as conn:
            book = conn.execute(
                "SELECT enabled FROM core_books WHERE id = ?",
                (allocation.book_id,),
            ).fetchone()
            if book is None:
                raise ValueError(f"unknown book: {allocation.book_id}")
            account = conn.execute(
                "SELECT enabled FROM core_accounts WHERE id = ?",
                (allocation.account_id,),
            ).fetchone()
            if account is None or not bool(account["enabled"]):
                raise ValueError("book allocation requires an enabled account")
            strategy = conn.execute(
                "SELECT enabled FROM core_strategies WHERE id = ?",
                (allocation.strategy_id,),
            ).fetchone()
            if strategy is None or not bool(strategy["enabled"]):
                raise ValueError("book allocation requires an enabled strategy")
            # Fractional allocations share one account-level unit interval.
            # Reject an overlapping over-allocation at persistence time; the
            # OMS repeats this check on every submission for legacy rows.
            if allocation.limit_basis == "capital_fraction":
                existing = conn.execute(
                    """SELECT capital_fraction, effective_at, expires_at
                         FROM core_book_allocations
                        WHERE account_id = ? AND capital_fraction IS NOT NULL
                          AND effective_at < COALESCE(?, '9999-12-31T23:59:59+00:00')
                          AND (expires_at IS NULL OR expires_at > ?)""",
                    (
                        allocation.account_id,
                        _timestamp(allocation.expires_at),
                        _timestamp(allocation.effective_at),
                    ),
                ).fetchall()
                total = allocation.capital_fraction or Decimal("0")
                for row in existing:
                    total += Decimal(str(row["capital_fraction"]))
                if total > 1:
                    raise ValueError("active book capital fractions exceed the account allocation")
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

    def apply_book_allocation_update(
        self,
        *,
        account_id: str,
        allocations: tuple[BookAllocation, ...] | list[BookAllocation],
        version: int,
        effective_at: datetime,
        provenance: str,
    ) -> tuple[str, ...]:
        """Atomically replace Stage 5 caps for the supplied books.

        This is the approved mutation path for a sleeve allocation update.
        Existing ownership, position allocations, and intents are never
        changed; only prior declarations for the same books are expired at
        ``effective_at`` before the new declarations are inserted.  The
        timeline policy is monotonic per account/book: a new version may be
        effective at the latest scheduled time or later, but never earlier.
        Rejecting an earlier schedule is fail-closed and prevents a future
        declaration from overlapping or inflating capacity.
        caller is responsible for validating the sleeve-to-book mapping and
        capacity units; this method repeats persistence and version checks in
        the same transaction so a direct stale update cannot partially apply.
        """
        if isinstance(version, bool) or not isinstance(version, int) or version <= 0:
            raise ValueError("allocation version must be a positive integer")
        if not isinstance(provenance, str) or not provenance.strip():
            raise ValueError("allocation provenance is required")
        if effective_at.tzinfo is None or effective_at.utcoffset() is None:
            raise ValueError("allocation effective_at must be timezone-aware")
        rows = tuple(allocations)
        if not rows:
            raise ValueError("at least one allocation is required")
        if any(not isinstance(item, BookAllocation) for item in rows):
            raise ValueError("allocations must contain BookAllocation values")
        book_ids = tuple(item.book_id for item in rows)
        if len(set(book_ids)) != len(book_ids):
            raise ValueError("allocation update contains duplicate books")
        timestamp = _timestamp(effective_at)
        account_identifier = str(account_id)
        with self.transaction() as conn:
            account = conn.execute(
                "SELECT enabled FROM core_accounts WHERE id = ?",
                (account_identifier,),
            ).fetchone()
            if account is None or not bool(account["enabled"]):
                raise ValueError("allocation update requires an enabled account")

            for allocation in rows:
                allocation.limit_basis
                if allocation.account_id != account_identifier:
                    raise ValueError("allocation account does not match update account")
                if allocation.effective_at != effective_at:
                    raise ValueError("allocation effective_at does not match update effective_at")
                book = conn.execute(
                    "SELECT enabled FROM core_books WHERE id = ?",
                    (allocation.book_id,),
                ).fetchone()
                if book is None or not bool(book["enabled"]):
                    raise ValueError(f"allocation requires an enabled book: {allocation.book_id}")
                strategy = conn.execute(
                    "SELECT enabled FROM core_strategies WHERE id = ?",
                    (allocation.strategy_id,),
                ).fetchone()
                if strategy is None or not bool(strategy["enabled"]):
                    raise ValueError(f"allocation requires an enabled strategy: {allocation.strategy_id}")
                metadata = dict(allocation.metadata)
                if metadata.get("allocation_source") != "stage5_clean40":
                    raise ValueError("allocation update is missing the Stage 5 source marker")
                if str(metadata.get("allocation_provenance", "")).strip() != provenance.strip():
                    raise ValueError("allocation provenance does not match update provenance")
                try:
                    stored_version = int(metadata.get("allocation_version", 0))
                except (TypeError, ValueError) as exc:
                    raise ValueError("allocation version metadata is invalid") from exc
                if stored_version != version:
                    raise ValueError("allocation version metadata does not match update version")

            # An exact retry is a no-op.  This is deliberately checked before
            # expiring rows so a repeated signal evaluation cannot move the
            # effective timestamp or create a second cap declaration.
            existing_by_id = {
                str(row["id"]): row
                for row in conn.execute(
                    "SELECT * FROM core_book_allocations WHERE id IN (%s)"
                    % ",".join("?" for _ in rows),
                    tuple(item.id for item in rows),
                ).fetchall()
            }
            exact = True
            for allocation in rows:
                current = existing_by_id.get(allocation.id)
                if current is None:
                    exact = False
                    break
                current_metadata = _decode(current["metadata_json"])
                if (
                    str(current["book_id"]) != allocation.book_id
                    or str(current["strategy_id"]) != allocation.strategy_id
                    or str(current["account_id"]) != allocation.account_id
                    or str(current["effective_at"]) != timestamp
                    or str(current["capital_fraction"] or "") != str(_decimal(allocation.capital_fraction) or "")
                    or str(current["capital_amount"] or "") != str(_decimal(allocation.capital_amount) or "")
                    or str(current["risk_budget"] or "") != str(_decimal(allocation.risk_budget) or "")
                    or current_metadata != dict(allocation.metadata)
                ):
                    raise ValueError("allocation version already exists with different content")
            if exact:
                return tuple(item.id for item in rows)

            # A new version must be newer than every Stage 5 declaration and
            # must not be scheduled before the latest declaration for any
            # affected book.  Missing metadata is treated as version 0,
            # preserving safe compatibility with pre-Stage 5 Stage 4 rows;
            # their effective time still participates in the conservative
            # timeline check.
            placeholders = ",".join("?" for _ in book_ids)
            prior_rows = conn.execute(
                f"""SELECT book_id, metadata_json, effective_at, expires_at
                       FROM core_book_allocations
                      WHERE account_id = ? AND book_id IN ({placeholders})""",
                (account_identifier, *book_ids),
            ).fetchall()
            highest = 0
            for prior in prior_rows:
                prior_effective_raw = prior["effective_at"]
                try:
                    prior_effective = datetime.fromisoformat(str(prior_effective_raw))
                except (TypeError, ValueError) as exc:
                    raise ValueError("existing allocation effective_at is invalid") from exc
                if prior_effective.tzinfo is None or prior_effective.utcoffset() is None:
                    raise ValueError("existing allocation effective_at must be timezone-aware")
                if prior_effective.astimezone(timezone.utc) > effective_at.astimezone(timezone.utc):
                    raise ValueError(
                        "allocation effective_at cannot precede the latest scheduled declaration "
                        f"for book {prior['book_id']}"
                    )
                prior_metadata = _decode(prior["metadata_json"])
                try:
                    highest = max(highest, int(prior_metadata.get("allocation_version", 0)))
                except (TypeError, ValueError) as exc:
                    raise ValueError("existing allocation version metadata is invalid") from exc
            if version <= highest:
                raise ValueError("allocation version must increase monotonically")

            conn.execute(
                f"""UPDATE core_book_allocations
                       SET expires_at = ?, updated_at = ?
                     WHERE account_id = ? AND book_id IN ({placeholders})
                       AND effective_at <= ?
                       AND (expires_at IS NULL OR expires_at > ?)""",
                (timestamp, timestamp, account_identifier, *book_ids, timestamp, timestamp),
            )
            for allocation in rows:
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
        return tuple(item.id for item in rows)

    def book_allocations(
        self,
        account_id: str,
        *,
        book_id: str | None = None,
        strategy_id: str | None = None,
        at: datetime | None = None,
        active_only: bool = True,
    ) -> list[dict[str, Any]]:
        """Return book limits and ownership declarations for an account.

        ``active_only=False`` is intentionally available to safety audits: an
        expired or disabled declaration remains evidence that Stage 4 book
        mode was enabled and must not be silently treated as legacy unowned
        execution.
        """
        clauses = ["a.account_id = ?"]
        params: list[Any] = [str(account_id)]
        if book_id is not None:
            clauses.append("a.book_id = ?")
            params.append(str(book_id))
        if strategy_id is not None:
            clauses.append("a.strategy_id = ?")
            params.append(str(strategy_id))
        if active_only:
            moment = _timestamp(at or utc_now())
            clauses.extend([
                "a.effective_at <= ?",
                "(a.expires_at IS NULL OR a.expires_at > ?)",
            ])
            params.extend([moment, moment])
        query = f"""SELECT a.*, b.name AS book_name, b.enabled AS book_enabled,
                          s.name AS strategy_name, s.enabled AS strategy_enabled
                     FROM core_book_allocations a
                     JOIN core_books b ON b.id = a.book_id
                     JOIN core_strategies s ON s.id = a.strategy_id
                    WHERE {' AND '.join(clauses)}
                    ORDER BY a.book_id, a.strategy_id, a.effective_at, a.id"""
        with self.transaction() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["metadata"] = _decode(item.pop("metadata_json"))
            item["book_enabled"] = bool(item["book_enabled"])
            item["strategy_enabled"] = bool(item["strategy_enabled"])
            result.append(item)
        return result

    def book_mode_active(self, account_id: str) -> bool:
        """Whether the account opted into explicit Stage 4 book ownership."""
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT 1 FROM core_book_allocations WHERE account_id = ? LIMIT 1",
                (str(account_id),),
            ).fetchone()
        return row is not None

    def book_position_allocations(
        self,
        account_id: str,
        *,
        book_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return durable managed/unknown position ownership rows by book."""
        clauses = ["account_id = ?"]
        params: list[Any] = [str(account_id)]
        if book_id is not None:
            clauses.append("book_id = ?")
            params.append(str(book_id))
        with self.transaction() as conn:
            rows = conn.execute(
                f"SELECT * FROM core_position_allocations WHERE {' AND '.join(clauses)} ORDER BY book_id, instrument_id, id",
                tuple(params),
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["metadata"] = _decode(item.pop("metadata_json"))
            result.append(item)
        return result

    def active_book_intent_exposure(
        self,
        account_id: str,
        book_id: str,
        *,
        exclude_intent_id: str | None = None,
        exclude_intent_ids: set[str] | frozenset[str] | None = None,
    ) -> Decimal:
        """Gross remaining quantity reserved by non-terminal book intents."""
        return sum(
            (abs(quantity) for quantity in self.active_book_intent_signed_exposure(
                account_id,
                book_id,
                exclude_intent_id=exclude_intent_id,
                exclude_intent_ids=exclude_intent_ids,
            ).values()),
            Decimal("0"),
        )

    def active_book_intent_signed_exposure(
        self,
        account_id: str,
        book_id: str,
        *,
        exclude_intent_id: str | None = None,
        exclude_intent_ids: set[str] | frozenset[str] | None = None,
    ) -> dict[str, Decimal]:
        """Return signed remaining quantity reserved by active book intents."""
        active_legs = tuple(
            status.value
            for status in (
                LegStatus.PLANNED,
                LegStatus.SUBMITTING,
                LegStatus.WORKING,
                LegStatus.PARTIALLY_FILLED,
                LegStatus.RECONCILIATION_REQUIRED,
            )
        )
        active_intents = tuple(
            status.value
            for status in (
                IntentStatus.CREATED,
                IntentStatus.RISK_APPROVED,
                IntentStatus.SUBMITTING,
                IntentStatus.WORKING,
                IntentStatus.PARTIALLY_FILLED,
                IntentStatus.RECONCILIATION_REQUIRED,
            )
        )
        placeholders_legs = ",".join("?" for _ in active_legs)
        placeholders_intents = ",".join("?" for _ in active_intents)
        totals: dict[str, Decimal] = {}
        # Keep instrument and side in the same query so the signed exposure
        # contract is reusable by OMS risk checks and restart/status views.
        query = f"""SELECT l.instrument_id, l.side, l.quantity, l.cumulative_filled_quantity
                      FROM core_order_intents i
                      JOIN core_order_legs l ON l.intent_id = i.id
                     WHERE i.account_id = ? AND i.book_id = ?
                       AND i.status IN ({placeholders_intents})
                       AND l.status IN ({placeholders_legs})"""
        params = [str(account_id), str(book_id), *active_intents, *active_legs]
        if exclude_intent_id is not None:
            query += " AND i.id <> ?"
            params.append(str(exclude_intent_id))
        excluded = {str(value) for value in (exclude_intent_ids or ()) if str(value).strip()}
        if excluded:
            placeholders = ",".join("?" for _ in excluded)
            query += f" AND i.id NOT IN ({placeholders})"
            params.extend(sorted(excluded))
        with self.transaction() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        for row in rows:
            remaining = Decimal(str(row["quantity"])) - Decimal(str(row["cumulative_filled_quantity"]))
            if not remaining.is_finite() or remaining <= 0:
                continue
            signed = remaining if str(row["side"]) == "BUY" else -remaining
            instrument_id = str(row["instrument_id"])
            totals[instrument_id] = totals.get(instrument_id, Decimal("0")) + signed
        return totals

    def book_signed_exposure(
        self,
        account_id: str,
        book_id: str,
        *,
        exclude_intent_id: str | None = None,
        exclude_intent_ids: set[str] | frozenset[str] | None = None,
    ) -> dict[str, Decimal]:
        """Return signed managed position plus active-intent exposure."""
        totals: dict[str, Decimal] = {}
        excluded = {str(value) for value in (exclude_intent_ids or ()) if str(value).strip()}
        for row in self.book_position_allocations(account_id, book_id=book_id):
            if str(row.get("ownership_class", "")).upper() != OwnershipClass.MANAGED.value:
                continue
            if str(row.get("source_intent_id") or "") in excluded:
                continue
            quantity = Decimal(str(row.get("signed_quantity", "0")))
            if not quantity.is_finite():
                raise ValueError("book allocation quantity is not finite")
            instrument_id = str(row.get("instrument_id", ""))
            totals[instrument_id] = totals.get(instrument_id, Decimal("0")) + quantity
        for instrument_id, quantity in self.active_book_intent_signed_exposure(
            account_id,
            book_id,
            exclude_intent_id=exclude_intent_id,
            exclude_intent_ids=excluded,
        ).items():
            totals[instrument_id] = totals.get(instrument_id, Decimal("0")) + quantity
        return totals

    def book_intents(self, account_id: str, *, book_id: str | None = None) -> list[dict[str, Any]]:
        """Return durable intent ownership claims for operator/restart views."""
        clauses = ["account_id = ?"]
        params: list[Any] = [str(account_id)]
        if book_id is not None:
            clauses.append("book_id = ?")
            params.append(str(book_id))
        with self.transaction() as conn:
            rows = conn.execute(
                f"SELECT * FROM core_order_intents WHERE {' AND '.join(clauses)} ORDER BY created_at, id",
                tuple(params),
            ).fetchall()
        return [dict(row) for row in rows]

    def book_broker_orders(self, account_id: str, *, book_id: str | None = None) -> list[dict[str, Any]]:
        """Return broker-order claims through their owning intent/book."""
        clauses = ["b.account_id = ?"]
        params: list[Any] = [str(account_id)]
        if book_id is not None:
            clauses.append("i.book_id = ?")
            params.append(str(book_id))
        with self.transaction() as conn:
            rows = conn.execute(
                f"""SELECT b.*, i.book_id, i.strategy_id, l.intent_id
                      FROM core_broker_orders b
                      JOIN core_order_legs l ON l.id = b.order_leg_id
                      JOIN core_order_intents i ON i.id = l.intent_id
                     WHERE {' AND '.join(clauses)}
                     ORDER BY i.book_id, b.id""",
                tuple(params),
            ).fetchall()
        return [dict(row) for row in rows]

    def create_intent(self, intent: OrderIntent) -> tuple[str, bool]:
        """Persist an intent and every logical leg atomically before submission."""
        fingerprint = _payload_hash(intent)
        legacy_fingerprint = _payload_hash(intent, include_stale_order_seconds=False)
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT id, payload_hash FROM core_order_intents WHERE account_id = ? AND idempotency_key = ?",
                (intent.account_id, intent.idempotency_key),
            ).fetchone()
            if existing:
                stored_fingerprint = str(existing["payload_hash"])
                legacy_compatible = (
                    intent.execution_policy.stale_order_seconds == ExecutionPolicy().stale_order_seconds
                    and stored_fingerprint == legacy_fingerprint
                )
                if stored_fingerprint not in {fingerprint} and not legacy_compatible:
                    raise IdempotencyConflict(
                        f"idempotency key {intent.idempotency_key!r} was reused with a different payload"
                    )
                return str(existing["id"]), False

            policy = asdict(intent.execution_policy)
            # Persist the canonical JSON shape rather than ``str(Enum)`` or
            # ``str(frozenset)``.  Recovery reconstructs ExecutionPolicy from
            # this row and must not quarantine every valid legacy intent.
            for key in ("legging_policy", "partial_fill_policy", "failure_policy", "execution_session"):
                policy[key] = _enum(policy[key])
            policy["required_capabilities"] = sorted(intent.execution_policy.required_capabilities)
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

    def cancel_unsubmitted_intent(
        self,
        intent_id: str,
        *,
        now: datetime | None = None,
    ) -> bool:
        """Atomically terminalize a proven never-submitted intent.

        The OMS performs the account/broker-evidence proof.  This repository
        operation only permits the narrow state transition once every leg is
        still planned or quarantined with zero cumulative fill.  Keeping the
        leg and intent updates in one transaction prevents a partially
        restored local lifecycle if a process stops between transitions.
        """
        timestamp = _timestamp(now or utc_now())
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT status FROM core_order_intents WHERE id = ?",
                (intent_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown intent: {intent_id}")
            current_intent = IntentStatus(str(row["status"]))
            if current_intent is not IntentStatus.RECONCILIATION_REQUIRED:
                raise ValueError(
                    "only a reconciliation-required intent can be marked never-submitted"
                )
            legs = conn.execute(
                """SELECT id, status, cumulative_filled_quantity
                   FROM core_order_legs WHERE intent_id = ? ORDER BY sequence""",
                (intent_id,),
            ).fetchall()
            if not legs:
                raise ValueError("never-submitted intent must contain at least one leg")
            validate_transition(
                current_intent,
                IntentStatus.CANCELLED,
                INTENT_TRANSITIONS,
                entity="intent",
            )
            for leg in legs:
                current_leg = LegStatus(str(leg["status"]))
                if current_leg not in {LegStatus.PLANNED, LegStatus.RECONCILIATION_REQUIRED}:
                    raise ValueError(
                        f"never-submitted leg {leg['id']} is already {current_leg.value}"
                    )
                try:
                    cumulative = Decimal(str(leg["cumulative_filled_quantity"] or "0"))
                except (InvalidOperation, TypeError, ValueError) as exc:
                    raise ValueError(
                        f"never-submitted leg {leg['id']} has invalid cumulative fill"
                    ) from exc
                if not cumulative.is_finite() or cumulative != 0:
                    raise ValueError(
                        f"never-submitted leg {leg['id']} has non-zero cumulative fill"
                    )
                validate_transition(current_leg, LegStatus.CANCELLED, LEG_TRANSITIONS, entity="leg")

            for leg in legs:
                conn.execute(
                    "UPDATE core_order_legs SET status = ?, updated_at = ? WHERE id = ?",
                    (LegStatus.CANCELLED.value, timestamp, str(leg["id"])),
                )
            conn.execute(
                "UPDATE core_order_intents SET status = ?, updated_at = ? WHERE id = ?",
                (IntentStatus.CANCELLED.value, timestamp, intent_id),
            )
        return True

    def mark_compensated_partial_intent(
        self,
        intent_id: str,
        *,
        closure_metadata: Mapping[str, Any],
        now: datetime | None = None,
        _resolution_capability: object | None = None,
    ) -> bool:
        """Terminalize a proof-verified, compensated partial intent.

        This is intentionally narrower than ``cancel_unsubmitted_intent``:
        filled legs and their attempt-scoped fills remain immutable, while
        only planned/reconciliation-required zero-evidence legs are moved to
        ``CANCELLED``.  The OMS owns the broker/history proof; this method is
        the atomic local lifecycle commit and requires its private capability.
        """
        if _resolution_capability is not self.__resolution_capability:
            raise PermissionError("validated compensated-intent capability is required")
        timestamp = _timestamp(now or utc_now())
        metadata = dict(closure_metadata)
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT status, metadata_json FROM core_order_intents WHERE id = ?",
                (intent_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown intent: {intent_id}")
            if IntentStatus(str(row["status"])) is not IntentStatus.RECONCILIATION_REQUIRED:
                raise ValueError("only a reconciliation-required intent can be marked compensated")
            legs = conn.execute(
                """SELECT id, status, cumulative_filled_quantity
                   FROM core_order_legs WHERE intent_id = ? ORDER BY sequence""",
                (intent_id,),
            ).fetchall()
            if not legs:
                raise ValueError("compensated intent must contain at least one leg")
            validate_transition(
                IntentStatus.RECONCILIATION_REQUIRED,
                IntentStatus.CANCELLED,
                INTENT_TRANSITIONS,
                entity="intent",
            )
            for leg in legs:
                current_leg = LegStatus(str(leg["status"]))
                if current_leg is LegStatus.FILLED:
                    continue
                if current_leg not in {
                    LegStatus.PLANNED,
                    LegStatus.RECONCILIATION_REQUIRED,
                    LegStatus.CANCELLED,
                    LegStatus.REJECTED,
                }:
                    raise ValueError(f"compensated leg {leg['id']} is not terminal or unsubmitted")
                cumulative = Decimal(str(leg["cumulative_filled_quantity"] or "0"))
                if not cumulative.is_finite() or cumulative != 0:
                    raise ValueError(f"compensated leg {leg['id']} has non-zero cumulative fill")
                if current_leg in {LegStatus.PLANNED, LegStatus.RECONCILIATION_REQUIRED}:
                    validate_transition(current_leg, LegStatus.CANCELLED, LEG_TRANSITIONS, entity="leg")

            existing = _decode(row["metadata_json"])
            if not isinstance(existing, Mapping):
                existing = {}
            merged = dict(existing)
            merged["compensated_partial_closure"] = metadata
            for leg in legs:
                if LegStatus(str(leg["status"])) in {
                    LegStatus.PLANNED,
                    LegStatus.RECONCILIATION_REQUIRED,
                }:
                    conn.execute(
                        "UPDATE core_order_legs SET status = ?, updated_at = ? WHERE id = ?",
                        (LegStatus.CANCELLED.value, timestamp, str(leg["id"])),
                    )
            conn.execute(
                "UPDATE core_order_intents SET status = ?, metadata_json = ?, updated_at = ? WHERE id = ?",
                (IntentStatus.CANCELLED.value, _json(merged), timestamp, intent_id),
            )
        return True

    def mark_verified_compensating_intent(
        self,
        intent_id: str,
        *,
        closure_metadata: Mapping[str, Any],
        now: datetime | None = None,
        _resolution_capability: object | None = None,
    ) -> bool:
        """Terminalize one proof-verified compensating exit intent.

        The OMS supplies fresh account/history proof.  This repository
        boundary rechecks the immutable local shape: a reconciliation-required
        intent whose every leg has exactly one terminal filled broker attempt
        and one exact durable fill may become ``COMPLETED``.  No order, fill,
        or allocation row is deleted or rewritten.
        """
        if _resolution_capability is not self.__resolution_capability:
            raise PermissionError("validated compensating-intent capability is required")
        timestamp = _timestamp(now or utc_now())
        metadata = dict(closure_metadata)
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT status, metadata_json FROM core_order_intents WHERE id = ?",
                (intent_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown compensating intent: {intent_id}")
            current = IntentStatus(str(row["status"]))
            if current is IntentStatus.COMPLETED:
                return True
            if current is not IntentStatus.RECONCILIATION_REQUIRED:
                raise ValueError("only a reconciliation-required compensating intent can be terminalized")
            legs = conn.execute(
                """SELECT id, status, quantity, cumulative_filled_quantity
                   FROM core_order_legs WHERE intent_id = ? ORDER BY sequence""",
                (intent_id,),
            ).fetchall()
            if not legs:
                raise ValueError("compensating intent must contain at least one leg")
            for leg in legs:
                if LegStatus(str(leg["status"])) is not LegStatus.FILLED:
                    raise ValueError(f"compensating leg {leg['id']} is not FILLED")
                requested = Decimal(str(leg["quantity"]))
                cumulative = Decimal(str(leg["cumulative_filled_quantity"]))
                if not requested.is_finite() or requested <= 0 or cumulative != requested:
                    raise ValueError(f"compensating leg {leg['id']} has incomplete fill quantity")
                attempts = conn.execute(
                    """SELECT id, status, submitted_quantity
                       FROM core_broker_orders WHERE order_leg_id = ?""",
                    (str(leg["id"]),),
                ).fetchall()
                if len(attempts) != 1 or BrokerOrderStatus(str(attempts[0]["status"])) is not BrokerOrderStatus.FILLED:
                    raise ValueError(f"compensating leg {leg['id']} lacks one terminal filled attempt")
                submitted = Decimal(str(attempts[0]["submitted_quantity"]))
                if submitted != requested:
                    raise ValueError(f"compensating leg {leg['id']} submitted quantity mismatches leg")
                fills = conn.execute(
                    """SELECT quantity, price FROM core_fills
                       WHERE broker_order_id = ? ORDER BY id""",
                    (str(attempts[0]["id"]),),
                ).fetchall()
                if len(fills) != 1:
                    raise ValueError(f"compensating leg {leg['id']} lacks exactly one durable fill")
                fill_quantity = Decimal(str(fills[0]["quantity"]))
                fill_price = Decimal(str(fills[0]["price"]))
                if (
                    not fill_quantity.is_finite()
                    or fill_quantity != requested
                    or not fill_price.is_finite()
                    or fill_price <= 0
                ):
                    raise ValueError(f"compensating leg {leg['id']} has invalid durable fill economics")
            validate_transition(current, IntentStatus.COMPLETED, INTENT_TRANSITIONS, entity="intent")
            existing = _decode(row["metadata_json"])
            if not isinstance(existing, Mapping):
                existing = {}
            merged = dict(existing)
            merged["verified_compensating_exit_closure"] = metadata
            conn.execute(
                "UPDATE core_order_intents SET status = ?, metadata_json = ?, updated_at = ? WHERE id = ?",
                (IntentStatus.COMPLETED.value, _json(merged), timestamp, intent_id),
            )
        return True

    def mark_verified_roundtrip_intents(
        self,
        intent_ids: tuple[str, ...] | list[str],
        *,
        closure_metadata: Mapping[str, Any],
        now: datetime | None = None,
        _resolution_capability: object | None = None,
    ) -> bool:
        """Atomically terminalize one proof-verified flat round-trip group.

        The OMS supplies fresh broker/history proof; this repository boundary
        still rechecks account ownership, one terminal filled attempt per leg,
        and exact durable fill quantities before persisting the closure marker.
        No order, fill, or allocation row is deleted or rewritten.
        """
        if _resolution_capability is not self.__resolution_capability:
            raise PermissionError("validated round-trip capability is required")
        unique_ids = tuple(dict.fromkeys(str(value) for value in intent_ids if str(value).strip()))
        if not unique_ids:
            raise ValueError("round-trip closure requires at least one intent")
        timestamp = _timestamp(now or utc_now())
        metadata = dict(closure_metadata)
        with self.transaction() as conn:
            rows = []
            for intent_id in unique_ids:
                row = conn.execute(
                    "SELECT * FROM core_order_intents WHERE id = ?",
                    (intent_id,),
                ).fetchone()
                if row is None:
                    raise KeyError(f"Unknown round-trip intent: {intent_id}")
                if IntentStatus(str(row["status"])) not in {
                    IntentStatus.RECONCILIATION_REQUIRED,
                    IntentStatus.FILLED,
                    IntentStatus.COMPLETED,
                }:
                    raise ValueError(f"round-trip intent {intent_id} is not safely terminalizable")
                rows.append(row)
            account_ids = {str(row["account_id"]) for row in rows}
            if len(account_ids) != 1 or str(metadata.get("account_id", "")) not in account_ids:
                raise ValueError("round-trip closure account provenance is inconsistent")
            for row in rows:
                legs = conn.execute(
                    "SELECT id, status, quantity, cumulative_filled_quantity FROM core_order_legs WHERE intent_id = ? ORDER BY sequence",
                    (str(row["id"]),),
                ).fetchall()
                if not legs:
                    raise ValueError("round-trip intent has no legs")
                for leg in legs:
                    if LegStatus(str(leg["status"])) is not LegStatus.FILLED:
                        raise ValueError(f"round-trip leg {leg['id']} is not FILLED")
                    requested = Decimal(str(leg["quantity"]))
                    cumulative = Decimal(str(leg["cumulative_filled_quantity"]))
                    attempts = conn.execute(
                        "SELECT id, status, submitted_quantity FROM core_broker_orders WHERE order_leg_id = ?",
                        (str(leg["id"]),),
                    ).fetchall()
                    if len(attempts) != 1 or BrokerOrderStatus(str(attempts[0]["status"])) is not BrokerOrderStatus.FILLED:
                        raise ValueError(f"round-trip leg {leg['id']} lacks one terminal filled attempt")
                    if requested <= 0 or cumulative != requested:
                        raise ValueError(f"round-trip leg {leg['id']} has incomplete cumulative fill")
                    fills = conn.execute(
                        "SELECT quantity FROM core_fills WHERE broker_order_id = ?",
                        (str(attempts[0]["id"]),),
                    ).fetchall()
                    if len(fills) != 1 or Decimal(str(fills[0]["quantity"])) != requested:
                        raise ValueError(f"round-trip leg {leg['id']} lacks exact durable fill")
                current = IntentStatus(str(row["status"]))
                if current is not IntentStatus.COMPLETED:
                    validate_transition(current, IntentStatus.COMPLETED, INTENT_TRANSITIONS, entity="intent")
                existing = _decode(row["metadata_json"])
                if not isinstance(existing, Mapping):
                    existing = {}
                merged = dict(existing)
                merged["verified_roundtrip_closure"] = metadata
                conn.execute(
                    "UPDATE core_order_intents SET status = ?, metadata_json = ?, updated_at = ? WHERE id = ?",
                    (IntentStatus.COMPLETED.value, _json(merged), timestamp, str(row["id"])),
                )
        return True

    def get_intent_by_idempotency_key(self, account_id: str, key: str) -> dict[str, Any] | None:
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT id FROM core_order_intents WHERE account_id = ? AND idempotency_key = ?",
                (account_id, key),
            ).fetchone()
        return self.get_intent(str(row["id"])) if row else None

    def recoverable_intent_ids(
        self,
        account_id: str,
        *,
        now: datetime | None = None,
        terminal_window_seconds: int = 86_400,
    ) -> list[str]:
        """Return active and recently terminal intents for focused contradiction checks."""
        if terminal_window_seconds <= 0:
            raise ValueError("terminal_window_seconds must be positive")
        active_intents = tuple(status.value for status in (
            IntentStatus.SUBMITTING,
            IntentStatus.WORKING,
            IntentStatus.PARTIALLY_FILLED,
            IntentStatus.RECONCILIATION_REQUIRED,
        ))
        active_legs = tuple(status.value for status in (
            LegStatus.SUBMITTING,
            LegStatus.WORKING,
            LegStatus.PARTIALLY_FILLED,
            LegStatus.RECONCILIATION_REQUIRED,
        ))
        active_orders = tuple(status.value for status in (
            BrokerOrderStatus.PREPARED,
            BrokerOrderStatus.SUBMITTING,
            BrokerOrderStatus.WORKING,
            BrokerOrderStatus.PARTIALLY_FILLED,
            BrokerOrderStatus.UNKNOWN,
        ))
        terminal_intents = tuple(status.value for status in (
            IntentStatus.FILLED,
            IntentStatus.COMPLETED,
            IntentStatus.REJECTED,
            IntentStatus.CANCELLED,
            IntentStatus.FAILED,
        ))
        terminal_orders = tuple(status.value for status in (
            BrokerOrderStatus.FILLED,
            BrokerOrderStatus.REJECTED,
            BrokerOrderStatus.CANCELLED,
            BrokerOrderStatus.FAILED,
        ))
        placeholders_intents = ",".join("?" for _ in active_intents)
        placeholders_legs = ",".join("?" for _ in active_legs)
        placeholders_orders = ",".join("?" for _ in active_orders)
        placeholders_terminal_intents = ",".join("?" for _ in terminal_intents)
        placeholders_terminal_orders = ",".join("?" for _ in terminal_orders)
        cutoff = _timestamp((now or utc_now()) - timedelta(seconds=terminal_window_seconds))
        with self.transaction() as conn:
            rows = conn.execute(
                f"""SELECT DISTINCT i.id
                    FROM core_order_intents i
                    LEFT JOIN core_order_legs l ON l.intent_id = i.id
                    LEFT JOIN core_broker_orders b ON b.order_leg_id = l.id
                    WHERE i.account_id = ?
                      AND (i.status IN ({placeholders_intents})
                           OR l.status IN ({placeholders_legs})
                           OR b.status IN ({placeholders_orders})
                           OR (i.status IN ({placeholders_terminal_intents})
                               AND b.status IN ({placeholders_terminal_orders})
                               AND b.updated_at >= ?))
                    ORDER BY i.created_at, i.id""",
                (
                    account_id,
                    *active_intents,
                    *active_legs,
                    *active_orders,
                    *terminal_intents,
                    *terminal_orders,
                    cutoff,
                ),
            ).fetchall()
        return [str(row["id"]) for row in rows]

    def expired_terminal_attempts(
        self,
        account_id: str,
        *,
        now: datetime | None = None,
        terminal_window_seconds: int = 86_400,
    ) -> list[dict[str, Any]]:
        """Find terminal broker attempts outside the focused restart window.

        These rows are deliberately not returned by ``recoverable_intent_ids``
        for automatic polling. Callers must persist a safety blocker so an
        aged terminal attempt can never silently make a new submission safe.
        """
        if terminal_window_seconds <= 0:
            raise ValueError("terminal_window_seconds must be positive")
        terminal_intents = tuple(status.value for status in (
            IntentStatus.FILLED,
            IntentStatus.COMPLETED,
            IntentStatus.REJECTED,
            IntentStatus.CANCELLED,
            IntentStatus.FAILED,
        ))
        terminal_orders = tuple(status.value for status in (
            BrokerOrderStatus.FILLED,
            BrokerOrderStatus.REJECTED,
            BrokerOrderStatus.CANCELLED,
            BrokerOrderStatus.FAILED,
        ))
        intent_placeholders = ",".join("?" for _ in terminal_intents)
        order_placeholders = ",".join("?" for _ in terminal_orders)
        cutoff = _timestamp((now or utc_now()) - timedelta(seconds=terminal_window_seconds))
        with self.transaction() as conn:
            rows = conn.execute(
                f"""SELECT i.id AS intent_id, i.account_id, i.status AS intent_status,
                          b.id AS broker_order_id, b.status AS broker_order_status,
                          b.external_order_id, b.updated_at
                   FROM core_order_intents i
                   JOIN core_order_legs l ON l.intent_id = i.id
                   JOIN core_broker_orders b ON b.order_leg_id = l.id
                   WHERE i.account_id = ?
                     AND i.status IN ({intent_placeholders})
                     AND b.status IN ({order_placeholders})
                     AND b.updated_at < ?
                   ORDER BY b.updated_at, b.id""",
                (
                    account_id,
                    *terminal_intents,
                    *terminal_orders,
                    cutoff,
                ),
            ).fetchall()
        return [dict(row) for row in rows]

    def transition_intent(
        self,
        intent_id: str,
        target: IntentStatus,
        *,
        now: datetime | None = None,
    ) -> None:
        self._transition("core_order_intents", intent_id, target, IntentStatus, INTENT_TRANSITIONS, "intent", now=now)

    def transition_leg(
        self,
        leg_id: str,
        target: LegStatus,
        *,
        now: datetime | None = None,
    ) -> None:
        self._transition("core_order_legs", leg_id, target, LegStatus, LEG_TRANSITIONS, "leg", now=now)

    def transition_broker_order(
        self,
        broker_order_id: str,
        target: BrokerOrderStatus,
        *,
        now: datetime | None = None,
    ) -> None:
        self._transition(
            "core_broker_orders",
            broker_order_id,
            target,
            BrokerOrderStatus,
            BROKER_ORDER_TRANSITIONS,
            "broker order",
            now=now,
        )

    def _transition(
        self,
        table: str,
        entity_id: str,
        target: Enum,
        enum_type: type[Enum],
        allowed: Any,
        entity: str,
        now: datetime | None = None,
    ) -> None:
        timestamp = _timestamp(now or utc_now())
        with self.transaction() as conn:
            row = conn.execute(f"SELECT status FROM {table} WHERE id = ?", (entity_id,)).fetchone()
            if row is None:
                raise KeyError(f"Unknown {entity}: {entity_id}")
            current = enum_type(str(row["status"]))
            validate_transition(current, target, allowed, entity=entity)
            conn.execute(f"UPDATE {table} SET status = ?, updated_at = ? WHERE id = ?", (_enum(target), timestamp, entity_id))

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
        now: datetime | None = None,
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
                    _timestamp(now or utc_now()),
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
        now: datetime | None = None,
        submitted_at: datetime | None = None,
    ) -> None:
        timestamp = _timestamp(now or utc_now())
        submitted_timestamp = _timestamp(submitted_at or now or utc_now())
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
                (_enum(status), external_order_id, submitted_timestamp, timestamp, _json(metadata), broker_order_id),
            )

    def get_broker_order(self, broker_order_id: str) -> dict[str, Any] | None:
        with self.transaction() as conn:
            row = conn.execute("SELECT * FROM core_broker_orders WHERE id = ?", (broker_order_id,)).fetchone()
            if row is None:
                return None
            result = dict(row)
            result["metadata"] = _decode(result.pop("metadata_json"))
            return result

    def broker_orders_for_external_order_id(self, external_order_id: str) -> list[dict[str, Any]]:
        """Return every durable account claim for one provider order ID."""
        with self.transaction() as conn:
            rows = conn.execute(
                "SELECT * FROM core_broker_orders WHERE external_order_id = ? ORDER BY account_id, id",
                (external_order_id,),
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["metadata"] = _decode(item.pop("metadata_json"))
            result.append(item)
        return result

    def intent_id_for_broker_order(self, broker_order_id: str) -> str | None:
        with self.transaction() as conn:
            row = conn.execute(
                """SELECT l.intent_id
                   FROM core_broker_orders b
                   JOIN core_order_legs l ON l.id = b.order_leg_id
                   WHERE b.id = ?""",
                (broker_order_id,),
            ).fetchone()
        return str(row["intent_id"]) if row else None

    def broker_orders_for_leg(self, leg_id: str) -> list[dict[str, Any]]:
        with self.transaction() as conn:
            rows = conn.execute(
                "SELECT * FROM core_broker_orders WHERE order_leg_id = ? ORDER BY attempt_number", (leg_id,)
            ).fetchall()
            return [dict(row) for row in rows]

    def multi_attempt_legs_for_account(self, account_id: str) -> list[dict[str, Any]]:
        """Return every managed leg with more than one durable broker attempt.

        Stage 3 deliberately supports one attempt per logical leg.  This scan
        is account-wide so a sibling intent cannot hide an unsupported
        replacement history from a new submit or recovery route.
        """
        with self.transaction() as conn:
            rows = conn.execute(
                """SELECT l.id AS leg_id, l.intent_id
                   FROM core_order_legs l
                   JOIN core_order_intents i ON i.id = l.intent_id
                   JOIN core_broker_orders b ON b.order_leg_id = l.id
                   WHERE i.account_id = ?
                   GROUP BY l.id, l.intent_id
                   HAVING COUNT(b.id) > 1
                   ORDER BY l.intent_id, l.sequence, l.id""",
                (account_id,),
            ).fetchall()
            result: list[dict[str, Any]] = []
            for row in rows:
                attempts = conn.execute(
                    "SELECT * FROM core_broker_orders WHERE order_leg_id = ? ORDER BY attempt_number, id",
                    (str(row["leg_id"]),),
                ).fetchall()
                result.append(
                    {
                        "leg_id": str(row["leg_id"]),
                        "intent_id": str(row["intent_id"]),
                        "attempts": [dict(attempt) for attempt in attempts],
                    }
                )
            return result

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
        oms_fill_fingerprint: str | None = None,
        oms_event_fingerprint: str | None = None,
    ) -> bool:
        with self.transaction() as conn:
            values = (
                external_event_id,
                event_type,
                broker_status,
                _timestamp(event_at),
                _timestamp(received_at),
                _json(metadata),
                oms_fill_fingerprint,
                oms_event_fingerprint,
            )
            existing = conn.execute(
                """SELECT external_event_id, event_type, broker_status, event_at, received_at, metadata_json,
                          oms_fill_fingerprint, oms_event_fingerprint
                   FROM core_broker_order_events
                   WHERE broker_order_id = ? AND dedupe_key = ?""",
                (broker_order_id, dedupe_key),
            ).fetchone()
            if existing:
                if tuple(existing) != values:
                    # Rows created before the dedicated fingerprint columns
                    # are valid historical facts.  Keep direct repository
                    # replay idempotent while allowing the OMS to perform
                    # its stricter legacy evidence comparison.
                    if (
                        tuple(existing[:6]) == tuple(values[:6])
                        and existing[6] is None
                        and existing[7] is None
                        and oms_fill_fingerprint is None
                        and oms_event_fingerprint is None
                    ):
                        return False
                    raise ValueError("broker event dedupe key was reused with different evidence")
                return False
            conn.execute(
                """INSERT INTO core_broker_order_events
                   (id, broker_order_id, dedupe_key, external_event_id, event_type,
                   broker_status, event_at, received_at, metadata_json,
                   oms_fill_fingerprint, oms_event_fingerprint)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
                    oms_fill_fingerprint,
                    oms_event_fingerprint,
                ),
            )
            return True

    def record_broker_order_event(
        self,
        event: BrokerOrderEvent,
        *,
        oms_fill_fingerprint: str | None = None,
        oms_event_fingerprint: str | None = None,
    ) -> bool:
        """Persist one normalized broker event idempotently.

        The repository records the provider fact only.  OMS state transitions
        remain in the coordinator so an event cannot silently manufacture a
        fill or position allocation.
        """
        metadata = dict(event.metadata)
        if event.external_order_id is not None:
            metadata.setdefault("external_order_id", event.external_order_id)
        if event.client_order_id is not None:
            metadata.setdefault("client_order_id", event.client_order_id)
        if event.cumulative_filled_quantity is not None:
            metadata.setdefault("cumulative_filled_quantity", str(event.cumulative_filled_quantity))
        if event.account_id is not None:
            metadata.setdefault("account_id", event.account_id)
        if event.external_account_id is not None:
            metadata.setdefault("external_account_id", event.external_account_id)
        metadata.setdefault("no_fill_asserted", event.no_fill_asserted)
        return self.append_broker_event(
            event_id=event.id,
            broker_order_id=event.broker_order_id,
            dedupe_key=event.dedupe_key,
            event_type=event.event_type,
            broker_status=event.broker_status.value if event.broker_status is not None else None,
            event_at=event.event_at,
            received_at=event.received_at,
            external_event_id=event.external_event_id,
            metadata=metadata,
            oms_fill_fingerprint=oms_fill_fingerprint,
            oms_event_fingerprint=oms_event_fingerprint,
        )

    def broker_order_event(self, broker_order_id: str, dedupe_key: str) -> dict[str, Any] | None:
        """Load one persisted provider event and its normalized metadata."""
        with self.transaction() as conn:
            row = conn.execute(
                """SELECT * FROM core_broker_order_events
                   WHERE broker_order_id = ? AND dedupe_key = ?""",
                (broker_order_id, dedupe_key),
            ).fetchone()
        if row is None:
            return None
        result = dict(row)
        result["metadata"] = _decode(result.pop("metadata_json"))
        return result

    def record_fill(
        self,
        fill: Fill,
        *,
        now: datetime | None = None,
        _validation_token: object | None = None,
    ) -> bool:
        """Append one fill exactly once and update its logical leg monotonically.

        Direct repository callers cannot use this method to bypass an open
        account blocker.  OMS recovery passes the explicit private trust
        marker only after it has performed broker/attempt/evidence validation;
        the repository still enforces account enablement and the one-attempt
        invariant in either mode.
        """
        quantity = Decimal(fill.quantity)
        price = Decimal(fill.price)
        if quantity <= 0 or price <= 0:
            raise ValueError("fill quantity and price must be positive")
        timestamp = _timestamp(now or utc_now())
        with self.transaction() as conn:
            leg = conn.execute(
                """SELECT l.quantity, l.cumulative_filled_quantity, l.average_fill_price,
                          l.status, l.instrument_id, l.side, l.intent_id, i.status AS intent_status,
                          i.account_id, i.strategy_id, i.book_id
                   FROM core_order_legs l
                   JOIN core_order_intents i ON i.id = l.intent_id
                   WHERE l.id = ?""",
                (fill.order_leg_id,),
            ).fetchone()
            if leg is None:
                raise KeyError(f"Unknown leg: {fill.order_leg_id}")
            account = conn.execute(
                "SELECT broker, environment, external_account_id, enabled FROM core_accounts WHERE id = ?",
                (str(leg["account_id"]),),
            ).fetchone()
            if account is None:
                raise ValueError("fill intent account is not persisted")
            if not bool(account["enabled"]):
                raise ValueError("fill intent account is disabled")
            if fill.account_id is not None and str(fill.account_id) != str(leg["account_id"]):
                raise ValueError("fill account identity does not match intent account")

            expected_internal = str(leg["account_id"])
            expected_external = str(account["external_account_id"])
            expected_broker = str(account["broker"])
            expected_environment = str(account["environment"]).upper()
            environment_aliases = {expected_environment}
            if expected_environment == "SIM":
                environment_aliases.add("SIMULATE")
            elif expected_environment == "LIVE":
                environment_aliases.add("REAL")
            try:
                fill_metadata = coerce_provider_payload(fill.metadata)
            except ProviderPayloadError as exc:
                raise ValueError(f"fill metadata is not safely inspectable: {exc}") from exc

            def validate_fill_aliases(value: object) -> None:
                if isinstance(value, Mapping):
                    for raw_key, raw_value in value.items():
                        key = normalize_provider_key(raw_key)
                        if key in {"account_id", "internal_account_id", "oms_account_id"} and raw_value not in (None, ""):
                            if str(raw_value) != expected_internal:
                                raise ValueError("fill metadata account identity does not match intent account")
                        elif key in {
                            "external_account_id", "acc_id", "account_number", "trd_acc_id", "trade_account_id"
                        } and raw_value not in (None, ""):
                            if str(raw_value) != expected_external:
                                raise ValueError("fill metadata external account identity does not match intent account")
                        elif key in {"broker", "broker_name", "broker_id", "provider"} and raw_value not in (None, ""):
                            if str(raw_value) != expected_broker:
                                raise ValueError("fill metadata broker identity does not match intent account")
                        elif key in {"environment", "trading_environment", "trd_env", "trading_env"} and raw_value not in (None, ""):
                            if str(raw_value).upper().split(".")[-1] not in environment_aliases:
                                raise ValueError("fill metadata environment does not match intent account")
                        elif key in {"account", "account_alias", "account_identifier"}:
                            if raw_value not in (None, "") and str(raw_value) not in {expected_internal, expected_external}:
                                raise ValueError("fill metadata account alias does not match intent account")
                        validate_fill_aliases(raw_value)
                elif isinstance(value, (list, tuple)):
                    for item in value:
                        validate_fill_aliases(item)
                elif value is not None and not isinstance(value, (str, int, float, bool, Decimal, datetime)):
                    raise ValueError(
                        f"opaque fill metadata type {type(value).__name__!r} cannot be account-validated"
                    )

            validate_fill_aliases(fill_metadata)
            if _validation_token is not self.__fill_validation_capability:
                blocker = conn.execute(
                    """SELECT 1 FROM core_reconciliation_issues
                       WHERE account_id = ? AND status = 'OPEN'
                       UNION ALL
                       SELECT 1 FROM core_recovery_actions
                       WHERE account_id = ? AND status = 'OPEN'
                       LIMIT 1""",
                    (str(leg["account_id"]), str(leg["account_id"])),
                ).fetchone()
                if blocker is not None:
                    raise ValueError("cannot record fill while account reconciliation/action blockers are open")
            attempt = conn.execute(
                "SELECT order_leg_id, account_id, status, external_order_id, broker FROM core_broker_orders WHERE id = ?",
                (fill.broker_order_id,),
            ).fetchone()
            if attempt is None:
                raise KeyError(f"Unknown broker order: {fill.broker_order_id}")
            if str(attempt["order_leg_id"]) != fill.order_leg_id:
                raise ValueError("fill broker order does not belong to the supplied logical leg")
            if str(attempt["account_id"]) != str(leg["account_id"]):
                raise ValueError("fill broker order account does not match intent account")
            # Enforce the legacy duplicate-order quarantine at the lowest
            # persistence boundary too.  OMS recovery performs the same
            # check before matching, but a direct repository caller must not
            # be able to record/allocate a fill against one row while a
            # sibling claim of the same provider order remains hidden.
            self._audit_legacy_duplicate_broker_orders_conn(conn)
            external_claim_count = conn.execute(
                """SELECT COUNT(*) FROM core_broker_orders
                   WHERE external_order_id = ?""",
                (str(attempt["external_order_id"] or ""),),
            ).fetchone()[0]
            if int(external_claim_count) > 1:
                raise ValueError("duplicate broker order claims require reconciliation before recording a fill")
            attempt_count = conn.execute(
                "SELECT COUNT(*) FROM core_broker_orders WHERE order_leg_id = ?",
                (fill.order_leg_id,),
            ).fetchone()[0]
            if int(attempt_count) != 1:
                raise ValueError("exactly one broker attempt is required to record a fill")
            persisted_external_order_id = str(attempt["external_order_id"] or "").strip()
            if not persisted_external_order_id:
                raise ValueError("fill broker order has no persisted external order identity")
            if fill.external_order_id not in (None, persisted_external_order_id):
                raise ValueError("fill external order identity does not match persisted broker order")
            expected_evidence_reference = f"{persisted_external_order_id}:{fill.dedupe_key}"
            metadata_evidence_reference = None
            if isinstance(fill_metadata, Mapping):
                for key in ("evidence_reference", "_evidence_reference"):
                    if fill_metadata.get(key) not in (None, ""):
                        metadata_evidence_reference = str(fill_metadata[key])
                        break
            supplied_evidence_reference = fill.evidence_reference or metadata_evidence_reference
            if supplied_evidence_reference not in (None, expected_evidence_reference, fill.dedupe_key):
                raise ValueError("fill evidence reference does not match persisted broker order attempt")

            def validate_fill_provenance(value: object) -> None:
                if isinstance(value, Mapping):
                    for raw_key, raw_value in value.items():
                        key = normalize_provider_key(raw_key)
                        if key in {"external_order_id", "broker_order_id", "order_id", "orderid"}:
                            if raw_value not in (None, "") and str(raw_value) != persisted_external_order_id:
                                raise ValueError("fill external order evidence does not match persisted broker order")
                        elif key in {"evidence_reference", "_evidence_reference"}:
                            if raw_value not in (None, "") and str(raw_value) not in {
                                expected_evidence_reference,
                                fill.dedupe_key,
                            }:
                                raise ValueError("fill evidence reference does not match persisted broker order")
                        elif key in {"instrument_id", "internal_instrument_id", "oms_instrument_id", "_instrument_id"}:
                            if raw_value not in (None, "") and str(raw_value) != str(leg["instrument_id"]):
                                raise ValueError("fill instrument identity does not match intent leg")
                        validate_fill_provenance(raw_value)
                elif isinstance(value, (list, tuple)):
                    for item in value:
                        validate_fill_provenance(item)
                elif value is not None and not isinstance(value, (str, int, float, bool, Decimal, datetime)):
                    raise ValueError(
                        f"opaque fill metadata type {type(value).__name__!r} cannot be provenance-validated"
                    )

            validate_fill_provenance(fill_metadata)
            symbol_aliases: list[str] = []
            canonical_instruments: list[str] = []

            def collect_instrument_provenance(value: object) -> None:
                if isinstance(value, Mapping):
                    for raw_key, raw_value in value.items():
                        key = normalize_provider_key(raw_key)
                        if key in {"code", "symbol", "ticker", "external_symbol"} and raw_value not in (None, ""):
                            symbol_aliases.append(str(raw_value).strip().upper())
                        elif key in {"instrument_id", "internal_instrument_id", "oms_instrument_id", "_instrument_id"} and raw_value not in (None, ""):
                            canonical_instruments.append(str(raw_value).strip())
                        collect_instrument_provenance(raw_value)
                elif isinstance(value, (list, tuple)):
                    for item in value:
                        collect_instrument_provenance(item)
                elif value is not None and not isinstance(value, (str, int, float, bool, Decimal, datetime)):
                    raise ValueError(
                        f"opaque fill instrument provenance type {type(value).__name__!r} cannot be validated"
                    )

            collect_instrument_provenance(fill_metadata)
            if symbol_aliases and any(value != symbol_aliases[0] for value in symbol_aliases[1:]):
                raise ValueError("fill instrument symbol aliases conflict")
            if canonical_instruments and any(
                value != canonical_instruments[0] for value in canonical_instruments[1:]
            ):
                raise ValueError("fill canonical instrument aliases conflict")
            if canonical_instruments and any(value != str(leg["instrument_id"]) for value in canonical_instruments):
                raise ValueError("fill instrument identity does not match intent leg")
            if symbol_aliases and not canonical_instruments:
                raise ValueError("fill symbol provenance lacks a canonical instrument identity")
            stored_metadata = dict(fill_metadata)
            stored_metadata.setdefault("_external_order_id", persisted_external_order_id)
            stored_metadata.setdefault("_evidence_reference", expected_evidence_reference)
            evidence_mode = ExecutionEvidenceMode(fill.evidence_mode).value
            values = (
                fill.external_fill_id,
                _decimal(quantity),
                _decimal(price),
                _decimal(fill.fee),
                fill.fee_currency,
                _timestamp(fill.filled_at),
                evidence_mode,
                _json(stored_metadata),
            )
            existing = conn.execute(
                """SELECT external_fill_id, quantity, price, fee, fee_currency, filled_at,
                          evidence_mode, metadata_json
                   FROM core_fills WHERE broker_order_id = ? AND dedupe_key = ?""",
                (fill.broker_order_id, fill.dedupe_key),
            ).fetchone()
            if existing:
                if tuple(existing) == values:
                    return False
                # A pre-Stage-3 row does not contain the internal provenance
                # envelope.  Accept only an exact historical replay.
                existing_metadata = _decode(existing["metadata_json"])
                if isinstance(existing_metadata, Mapping):
                    internal_keys = {"_external_order_id", "_evidence_reference"}
                    historical_user = {
                        key: value for key, value in existing_metadata.items() if key not in internal_keys
                    }
                    current_user = {
                        key: value for key, value in stored_metadata.items() if key not in internal_keys
                    }
                    if (
                        tuple(existing[:7]) == tuple(values[:7])
                        and dict(historical_user) == dict(current_user)
                    ):
                        return False
                raise ValueError("fill dedupe key was reused with different evidence")
            if fill.external_fill_id not in (None, ""):
                existing_external = conn.execute(
                    """SELECT dedupe_key FROM core_fills
                       WHERE broker_order_id = ? AND external_fill_id = ?
                       LIMIT 1""",
                    (fill.broker_order_id, fill.external_fill_id),
                ).fetchone()
                if existing_external is not None:
                    # An external deal ID is a second, independent identity
                    # boundary.  Reusing it with a different dedupe key must
                    # never create a second allocation or be silently treated
                    # as a duplicate.
                    raise ValueError(
                        "external fill ID was reused with a different dedupe key"
                    )
            if _validation_token is not self.__fill_validation_capability:
                raise PermissionError("validated broker fill capability is required")
            # New allocations must carry the complete normalized provenance
            # envelope.  A private capability is necessary but not
            # sufficient: it cannot turn a caller-fabricated Fill that lacks
            # the exact persisted account/order/evidence identity into a
            # trusted provider fact.  The legacy replay branch above remains
            # deliberately compatible with rows written before these fields
            # existed, but every new fill is strict.
            if fill.account_id != expected_internal:
                raise ValueError("new fill requires the persisted internal account identity")
            if fill.external_order_id != persisted_external_order_id:
                raise ValueError("new fill requires the persisted external order identity")
            if supplied_evidence_reference not in {expected_evidence_reference, fill.dedupe_key}:
                raise ValueError("new fill requires an exact broker evidence reference")
            if str(leg["status"]) in {
                LegStatus.FILLED.value,
                LegStatus.REJECTED.value,
                LegStatus.CANCELLED.value,
                LegStatus.FAILED.value,
            } or str(leg["intent_status"]) in {
                IntentStatus.FILLED.value,
                IntentStatus.COMPLETED.value,
                IntentStatus.REJECTED.value,
                IntentStatus.CANCELLED.value,
                IntentStatus.FAILED.value,
            } or str(attempt["status"]) in {
                BrokerOrderStatus.REJECTED.value,
                BrokerOrderStatus.CANCELLED.value,
                BrokerOrderStatus.FAILED.value,
            }:
                raise ValueError("cannot record a new fill for terminal broker/order state")
            conn.execute(
                """INSERT INTO core_fills
                   (id, broker_order_id, order_leg_id, external_fill_id, dedupe_key,
                    quantity, price, fee, fee_currency, filled_at, received_at,
                    evidence_mode, metadata_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
                    evidence_mode,
                    _json(stored_metadata),
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
                (_decimal(cumulative), _decimal(average), target.value, timestamp, fill.order_leg_id),
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
                    (_decimal(allocation_quantity), timestamp, allocation_id),
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
                        timestamp,
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
                        (broker_target.value, timestamp, fill.broker_order_id),
                    )
            self._refresh_intent_status_conn(conn, fill.order_leg_id, timestamp)
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
        current_row = conn.execute(
            "SELECT status, account_id FROM core_order_intents WHERE id = ?",
            (intent_id,),
        ).fetchone()
        if current_row is None:
            return
        current = IntentStatus(str(current_row["status"]))
        open_issue = conn.execute(
            """SELECT 1 FROM core_reconciliation_issues i
               JOIN core_order_intents intent ON intent.account_id = i.account_id
               WHERE intent.id = ? AND i.status = 'OPEN' LIMIT 1""",
            (intent_id,),
        ).fetchone()
        open_action = conn.execute(
            "SELECT 1 FROM core_recovery_actions WHERE account_id = ? AND status = 'OPEN' LIMIT 1",
            (str(current_row["account_id"]),),
        ).fetchone()
        if open_issue or open_action or any(status is LegStatus.RECONCILIATION_REQUIRED for status in statuses):
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

    def fills_for_broker_order(self, broker_order_id: str) -> list[dict[str, Any]]:
        with self.transaction() as conn:
            rows = conn.execute(
                "SELECT * FROM core_fills WHERE broker_order_id = ? ORDER BY filled_at, dedupe_key",
                (broker_order_id,),
            ).fetchall()
            result: list[dict[str, Any]] = []
            for row in rows:
                item = dict(row)
                item["metadata"] = _decode(item.pop("metadata_json"))
                result.append(item)
            return result

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

    def save_execution_evidence_baseline(self, baseline: ExecutionEvidenceBaseline) -> None:
        """Persist one explicitly verified bounded execution-evidence checkpoint.

        A baseline is an operator/audit fact, not a configuration default.  It
        may only be written for an enabled persisted account and a verified
        flat observation.  Repeating the exact checkpoint is idempotent;
        changing any evidence is rejected so a SIM history gap cannot be
        silently papered over.
        """
        if baseline.status != "VERIFIED" or not baseline.verified_flat:
            raise ValueError("only VERIFIED flat execution-evidence baselines may be persisted")
        if baseline.evidence_mode is not ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS:
            raise ValueError("bounded SIM baseline requires cumulative order snapshot evidence")
        values = (
            baseline.account_id,
            _timestamp(baseline.captured_at),
            baseline.evidence_mode.value,
            _json(list(baseline.coverage)),
            baseline.source_ledger_fingerprint,
            _json(list(baseline.source_order_ids)),
            baseline.position_fingerprint,
            baseline.open_order_fingerprint,
            int(baseline.verified_flat),
            baseline.status,
            _json(baseline.metadata),
        )
        with self.transaction() as conn:
            account = conn.execute(
                "SELECT enabled FROM core_accounts WHERE id = ?",
                (baseline.account_id,),
            ).fetchone()
            if account is None or not bool(account["enabled"]):
                raise ValueError("execution-evidence baseline requires an enabled persisted account")
            existing = conn.execute(
                "SELECT * FROM core_execution_evidence_baselines WHERE id = ?",
                (baseline.id,),
            ).fetchone()
            if existing is None:
                existing = conn.execute(
                    """SELECT * FROM core_execution_evidence_baselines
                       WHERE account_id = ? AND source_ledger_fingerprint = ?""",
                    (baseline.account_id, baseline.source_ledger_fingerprint),
                ).fetchone()
            if existing is not None:
                current = (
                    str(existing["account_id"]),
                    str(existing["captured_at"]),
                    str(existing["evidence_mode"]),
                    str(existing["coverage_json"]),
                    str(existing["source_ledger_fingerprint"]),
                    str(existing["source_order_ids_json"]),
                    str(existing["position_fingerprint"]),
                    str(existing["open_order_fingerprint"]),
                    int(existing["verified_flat"]),
                    str(existing["status"]),
                    str(existing["metadata_json"]),
                )
                expected = (
                    values[0], str(values[1]), str(values[2]), str(values[3]), str(values[4]),
                    str(values[5]), str(values[6]), str(values[7]), int(values[8]), str(values[9]), str(values[10]),
                )
                if current != expected or str(existing["id"]) != baseline.id:
                    raise ValueError("execution-evidence baseline ID/fingerprint was reused with different evidence")
                return
            conn.execute(
                """INSERT INTO core_execution_evidence_baselines
                   (id, account_id, captured_at, evidence_mode, coverage_json,
                    source_ledger_fingerprint, source_order_ids_json,
                    position_fingerprint, open_order_fingerprint, verified_flat,
                    status, metadata_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (baseline.id, *values),
            )

    def latest_execution_evidence_baseline(self, account_id: str) -> ExecutionEvidenceBaseline | None:
        """Load the latest verified bounded baseline for an account."""
        with self.transaction() as conn:
            row = conn.execute(
                """SELECT * FROM core_execution_evidence_baselines
                   WHERE account_id = ? AND verified_flat = 1 AND status = 'VERIFIED'
                   ORDER BY captured_at DESC, id DESC LIMIT 1""",
                (str(account_id),),
            ).fetchone()
        if row is None:
            return None
        return ExecutionEvidenceBaseline(
            id=str(row["id"]),
            account_id=str(row["account_id"]),
            captured_at=datetime.fromisoformat(str(row["captured_at"])),
            evidence_mode=ExecutionEvidenceMode(str(row["evidence_mode"])),
            coverage=tuple(str(item) for item in _decode(row["coverage_json"])),
            source_ledger_fingerprint=str(row["source_ledger_fingerprint"]),
            source_order_ids=tuple(str(item) for item in _decode(row["source_order_ids_json"])),
            position_fingerprint=str(row["position_fingerprint"]),
            open_order_fingerprint=str(row["open_order_fingerprint"]),
            verified_flat=bool(row["verified_flat"]),
            status=str(row["status"]),
            metadata=_decode(row["metadata_json"]),
        )

    # ------------------------------------------------------------------
    # Stage 6 validation evidence
    # ------------------------------------------------------------------
    def record_stage6_validation_observation(
        self,
        *,
        observation_id: str,
        session_id: str,
        account_id: str,
        phase: str,
        captured_at: datetime,
        evidence: Mapping[str, Any],
        process_id: str | None = None,
        fresh_process: bool = False,
    ) -> str:
        """Append one immutable validation-phase observation.

        This is intentionally not an execution-ledger write.  A repeated
        observation ID is accepted only when every persisted field is byte
        equivalent after canonicalization; changed evidence is rejected.
        """

        observation_id = str(observation_id).strip()
        session_id = str(session_id).strip()
        account_id = str(account_id).strip()
        phase = str(phase).strip().upper()
        if not observation_id or not session_id or not account_id:
            raise ValueError("observation_id, session_id, and account_id are required")
        if phase not in {"PREFLIGHT", "RECOVERY", "FINAL"}:
            raise ValueError("Stage 6 validation phase must be PREFLIGHT, RECOVERY, or FINAL")
        if captured_at.tzinfo is None or captured_at.utcoffset() is None:
            raise ValueError("captured_at must be timezone-aware")
        if type(fresh_process) is not bool:
            raise ValueError("fresh_process must be a bool")
        try:
            evidence_json = canonical_evidence_json(evidence)
        except Stage6ValidationError as exc:
            raise ValueError(str(exc)) from exc
        evidence_session_id = evidence.get("session_id")
        if evidence_session_id is not None and str(evidence_session_id).strip() != session_id:
            raise ValueError("Stage 6 observation evidence session_id does not match the row")
        evidence_account_id = evidence.get("account_id")
        if evidence_account_id is not None and str(evidence_account_id).strip() != account_id:
            raise ValueError("Stage 6 observation evidence account_id does not match the row")
        evidence_hash = _fingerprint(json.loads(evidence_json))
        values = (
            observation_id,
            session_id,
            account_id,
            phase,
            _timestamp(captured_at),
            str(process_id).strip() if process_id is not None else None,
            int(fresh_process),
            evidence_json,
            evidence_hash,
        )
        with self.transaction() as conn:
            if conn.execute("SELECT 1 FROM core_accounts WHERE id = ?", (account_id,)).fetchone() is None:
                raise ValueError(f"unknown account for Stage 6 validation observation: {account_id}")
            existing = conn.execute(
                "SELECT * FROM core_stage6_validation_observations WHERE id = ?",
                (observation_id,),
            ).fetchone()
            if existing is not None:
                current = (
                    str(existing["id"]),
                    str(existing["session_id"]),
                    str(existing["account_id"]),
                    str(existing["phase"]),
                    str(existing["captured_at"]),
                    existing["process_id"],
                    int(existing["fresh_process"]),
                    str(existing["evidence_json"]),
                    str(existing["evidence_hash"]),
                )
                expected = tuple(None if value is None else str(value) for value in values)
                normalized_current = tuple(None if value is None else str(value) for value in current)
                if normalized_current != expected:
                    raise ValueError("Stage 6 validation observation ID was reused with different evidence")
                return observation_id
            conn.execute(
                """INSERT INTO core_stage6_validation_observations
                   (id, session_id, account_id, phase, captured_at, process_id,
                    fresh_process, evidence_json, evidence_hash, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (*values, _timestamp(utc_now())),
            )
        return observation_id

    def stage6_validation_observations(
        self,
        session_id: str,
        *,
        phase: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return retained validation observations in capture order."""

        clauses = ["session_id = ?"]
        params: list[Any] = [str(session_id)]
        if phase is not None:
            normalized_phase = str(phase).strip().upper()
            if normalized_phase not in {"PREFLIGHT", "RECOVERY", "FINAL"}:
                raise ValueError("invalid Stage 6 validation phase")
            clauses.append("phase = ?")
            params.append(normalized_phase)
        with self.transaction() as conn:
            rows = conn.execute(
                f"""SELECT * FROM core_stage6_validation_observations
                     WHERE {' AND '.join(clauses)}
                     ORDER BY captured_at, id""",
                params,
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["evidence"] = _decode(item.pop("evidence_json"))
            result.append(item)
        return result

    @staticmethod
    def _stage6_result_values(result: Stage6SessionResult) -> tuple[Any, ...]:
        evidence_json = canonical_evidence_json(result.evidence)
        return (
            result.session_id,
            result.us_trading_date,
            _timestamp(result.started_at),
            _timestamp(result.completed_at),
            result.commit_sha,
            result.execution_compatibility,
            result.account_id,
            "SIM",
            result.outcome.value,
            int(result.qualified),
            int(result.counted_for_completion),
            result.evidence_class,
            int(result.expected_entry_order_count),
            int(result.actual_entry_order_count),
            int(result.expected_exit_order_count),
            int(result.actual_exit_order_count),
            int(result.duplicate_attempt_count),
            _json(list(result.run_ids)),
            _json(list(result.entry_intent_ids)),
            _json(list(result.exit_intent_ids)),
            _json(list(result.failure_reasons)),
            _json(list(result.audit_refs)),
            _json(result.preflight),
            _json(result.restart_recovery),
            _json(result.final),
            evidence_json,
            _fingerprint(json.loads(evidence_json)),
        )

    def save_stage6_validation_session(
        self,
        result: Stage6SessionResult | Mapping[str, Any],
    ) -> Stage6SessionResult:
        """Persist one immutable derived Stage 6 validation result.

        Mapping inputs are evaluated through the same strict validator as the
        CLI.  Callers cannot set ``qualified`` or completion counts to bypass
        missing evidence.  A clean date collision is rejected while all
        failed/invalid rows remain retained.
        """

        if isinstance(result, Stage6SessionResult):
            try:
                derived = evaluate_stage6_session(result.evidence)
            except Stage6ValidationError as exc:
                raise ValueError(str(exc)) from exc
            if (
                derived.session_id != result.session_id
                or derived.outcome is not result.outcome
                or derived.qualified != result.qualified
                or derived.evidence_class != result.evidence_class
                or derived.failure_reasons != result.failure_reasons
            ):
                raise ValueError("Stage 6 validation result fields do not match derived evidence")
            normalized = derived
        elif isinstance(result, Mapping):
            try:
                normalized = evaluate_stage6_session(result)
            except Stage6ValidationError as exc:
                raise ValueError(str(exc)) from exc
        else:
            raise TypeError("Stage 6 validation result must be Stage6SessionResult or a mapping")
        if normalized.outcome is Stage6SessionOutcome.CLEAN_PASS:
            if not normalized.qualified or not normalized.counted_for_completion:
                raise ValueError("CLEAN_PASS must be qualified and initially countable")
            if normalized.evidence_class != Stage6EvidenceClass.DURABLE.value:
                raise ValueError("legacy evidence cannot be persisted as CLEAN_PASS")
            if normalized.manual_intervention:
                raise ValueError("manual intervention cannot be persisted as CLEAN_PASS")
        elif normalized.qualified or normalized.counted_for_completion:
            raise ValueError("FAILED/INVALID Stage 6 validation rows cannot be qualified")
        values = self._stage6_result_values(normalized)
        with self.transaction() as conn:
            if conn.execute("SELECT 1 FROM core_accounts WHERE id = ?", (normalized.account_id,)).fetchone() is None:
                raise ValueError(f"unknown account for Stage 6 validation session: {normalized.account_id}")
            existing = conn.execute(
                "SELECT * FROM core_stage6_validation_sessions WHERE session_id = ?",
                (normalized.session_id,),
            ).fetchone()
            if existing is not None:
                existing_hash = str(existing["evidence_hash"])
                if existing_hash != str(values[-1]):
                    raise ValueError("Stage 6 validation session is immutable; evidence changed")
                return normalized
            try:
                placeholders = ", ".join("?" for _ in range(28))
                conn.execute(
                    f"""INSERT INTO core_stage6_validation_sessions
                       (session_id, us_trading_date, started_at, completed_at,
                        commit_sha, execution_compatibility, account_id, environment,
                        result, qualified, counted_for_completion, evidence_class,
                        expected_entry_order_count, actual_entry_order_count,
                        expected_exit_order_count, actual_exit_order_count,
                        duplicate_attempt_count, run_ids_json, entry_intent_ids_json,
                        exit_intent_ids_json, failure_reasons_json, audit_refs_json,
                        preflight_json, recovery_json, final_json, evidence_json,
                        evidence_hash, created_at)
                       VALUES ({placeholders})""",
                    (*values, _timestamp(utc_now())),
                )
            except sqlite3.IntegrityError as exc:
                if normalized.outcome is Stage6SessionOutcome.CLEAN_PASS:
                    raise ValueError(
                        f"a CLEAN_PASS already exists for US trading date {normalized.us_trading_date}"
                    ) from exc
                raise
        return normalized

    def get_stage6_validation_session(self, session_id: str) -> dict[str, Any] | None:
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM core_stage6_validation_sessions WHERE session_id = ?",
                (str(session_id),),
            ).fetchone()
        if row is None:
            return None
        item = dict(row)
        for column, target in (
            ("run_ids_json", "run_ids"),
            ("entry_intent_ids_json", "entry_intent_ids"),
            ("exit_intent_ids_json", "exit_intent_ids"),
            ("failure_reasons_json", "failure_reasons"),
            ("audit_refs_json", "audit_refs"),
            ("preflight_json", "preflight"),
            ("recovery_json", "restart_recovery"),
            ("final_json", "final"),
            ("evidence_json", "evidence"),
        ):
            item[target] = _decode(item.pop(column))
        item["qualified"] = bool(item["qualified"])
        item["counted_for_completion"] = bool(item["counted_for_completion"])
        return item

    def list_stage6_validation_sessions(
        self,
        account_id: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if account_id is not None:
            clauses.append("account_id = ?")
            params.append(str(account_id))
        query = "SELECT session_id FROM core_stage6_validation_sessions"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY us_trading_date, completed_at, session_id"
        with self.transaction() as conn:
            ids = [str(row["session_id"]) for row in conn.execute(query, params).fetchall()]
        rows: list[dict[str, Any]] = []
        for session_id in ids:
            row = self.get_stage6_validation_session(session_id)
            if row is not None:
                rows.append(row)
        return rows

    def stage6_validation_status(
        self,
        account_id: str | None = None,
        *,
        execution_compatibility: str | None = None,
        required_sessions: int = 3,
    ) -> dict[str, Any]:
        rows = self.list_stage6_validation_sessions(account_id)
        status = stage6_completion_status(
            rows,
            required_sessions=required_sessions,
            execution_compatibility=execution_compatibility,
        )
        status["account_id"] = account_id
        status["sessions"] = rows
        return status

    # Short aliases keep the public repository surface discoverable for CLI
    # callers while retaining the explicit validation name in documentation.
    record_stage6_observation = record_stage6_validation_observation
    stage6_validation_session = get_stage6_validation_session

    def import_legacy_order_evidence(
        self,
        source_db_path: str | Path,
        account_id: str,
        *,
        legacy_label: str = "legacy-smoke",
        instrument_mapping: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Import a verified bounded order-ledger checkpoint without mutation of its source.

        Every imported claim must be a complete FILLED order with one
        cumulative-order fill and exact raw order identity. Unknown, partial,
        foreign, duplicate, or non-flat source state is rejected before the
        target transaction begins. Imported rows live in a retired
        legacy-smoke strategy/book and are never attributed to a current
        Stage 6 sleeve.
        """
        source_path = Path(source_db_path).expanduser().resolve()
        if not source_path.is_file():
            raise FileNotFoundError(f"legacy evidence database does not exist: {source_path}")
        label = str(legacy_label).strip()
        if not label or any(char in label for char in "\\/\x00"):
            raise ValueError("legacy_label must be a simple non-empty label")
        target_account = self.get_account(str(account_id))
        if target_account is None or not target_account.enabled:
            raise ValueError("legacy evidence import requires an enabled target account")

        source_uri = source_path.as_uri() + "?mode=ro"
        source = sqlite3.connect(source_uri, uri=True)
        source.row_factory = sqlite3.Row
        try:
            tables = {
                str(row["name"])
                for row in source.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            required_tables = {
                "core_accounts", "core_strategies", "core_instruments",
                "core_order_intents", "core_order_legs", "core_broker_orders",
                "core_fills", "core_position_allocations",
            }
            missing = sorted(required_tables - tables)
            if missing:
                raise ValueError(f"legacy evidence database is missing tables: {missing}")
            source_account = source.execute(
                "SELECT * FROM core_accounts WHERE id = ?", (str(account_id),)
            ).fetchone()
            if source_account is None:
                raise ValueError("legacy evidence account is not present in source database")
            if (
                str(source_account["broker"]) != target_account.broker
                or str(source_account["environment"]).upper() != target_account.environment.value
                or str(source_account["external_account_id"]) != target_account.external_account_id
                or not bool(source_account["enabled"])
            ):
                raise ValueError("legacy evidence account identity does not match the enabled target account")
            for table in ("core_reconciliation_issues", "core_recovery_actions"):
                if table in tables:
                    open_row = source.execute(
                        f"SELECT 1 FROM {table} WHERE account_id = ? AND status = 'OPEN' LIMIT 1",
                        (str(account_id),),
                    ).fetchone()
                    if open_row is not None:
                        raise ValueError(f"legacy evidence source has open {table} blockers")

            order_rows = [
                dict(row)
                for row in source.execute(
                    """SELECT b.*, l.intent_id, l.sequence, l.instrument_id, l.side,
                              l.quantity AS leg_quantity, l.status AS leg_status,
                              i.strategy_id, i.status AS intent_status,
                              ins.symbol AS instrument_symbol
                         FROM core_broker_orders b
                         JOIN core_order_legs l ON l.id = b.order_leg_id
                         JOIN core_order_intents i ON i.id = l.intent_id
                         JOIN core_instruments ins ON ins.id = l.instrument_id
                        WHERE b.account_id = ? AND b.external_order_id IS NOT NULL
                          AND trim(b.external_order_id) <> ''
                        ORDER BY b.external_order_id, b.id""",
                    (str(account_id),),
                ).fetchall()
            ]
            if not order_rows:
                raise ValueError("legacy evidence source contains no broker-order claims")
            by_external: dict[str, dict[str, Any]] = {}
            for row in order_rows:
                external_id = str(row["external_order_id"]).strip()
                if external_id in by_external:
                    raise ValueError(f"legacy evidence contains duplicate broker-order claim: {external_id}")
                by_external[external_id] = row

            candidate_orders: list[dict[str, Any]] = []
            for row in order_rows:
                external_id = str(row["external_order_id"]).strip()
                if str(row["broker"]).strip() != target_account.broker:
                    raise ValueError(f"legacy evidence order {external_id} belongs to a different broker")
                if str(row["status"]).upper() != BrokerOrderStatus.FILLED.value:
                    raise ValueError(f"legacy evidence order {external_id} is not a complete FILLED order")
                if str(row["leg_status"]).upper() != LegStatus.FILLED.value:
                    raise ValueError(f"legacy evidence order {external_id} has a non-FILLED leg")
                try:
                    quantity = Decimal(str(row["submitted_quantity"]))
                    leg_quantity = Decimal(str(row["leg_quantity"]))
                except (InvalidOperation, TypeError, ValueError) as exc:
                    raise ValueError(f"legacy evidence order {external_id} has invalid quantity") from exc
                if not quantity.is_finite() or quantity <= 0 or quantity != leg_quantity:
                    raise ValueError(f"legacy evidence order {external_id} has incomplete quantity evidence")
                fills = [
                    dict(fill)
                    for fill in source.execute(
                        "SELECT * FROM core_fills WHERE broker_order_id = ? ORDER BY id",
                        (str(row["id"]),),
                    ).fetchall()
                ]
                if len(fills) != 1:
                    raise ValueError(f"legacy evidence order {external_id} must have exactly one fill")
                fill = fills[0]
                try:
                    fill_quantity = Decimal(str(fill["quantity"]))
                    fill_price = Decimal(str(fill["price"]))
                except (InvalidOperation, TypeError, ValueError) as exc:
                    raise ValueError(f"legacy evidence fill {external_id} has invalid economics") from exc
                if not fill_quantity.is_finite() or fill_quantity != quantity or not fill_price.is_finite() or fill_price <= 0:
                    raise ValueError(f"legacy evidence fill {external_id} is not a complete fill")
                metadata = _decode(fill.get("metadata_json"))
                if not isinstance(metadata, Mapping) or metadata.get("source") != "order_list_query" or metadata.get("synthetic") is not True:
                    raise ValueError(f"legacy evidence fill {external_id} is not typed order-snapshot evidence")
                raw = metadata.get("raw")
                if not isinstance(raw, Mapping):
                    raise ValueError(f"legacy evidence fill {external_id} has no inspectable raw order row")
                raw_order_id = str(raw.get("order_id", raw.get("id", ""))).strip()
                raw_status = str(raw.get("order_status", raw.get("status", ""))).upper()
                try:
                    raw_quantity = Decimal(str(raw.get("qty", raw.get("quantity"))))
                    raw_dealt = Decimal(str(raw.get("dealt_qty", raw.get("filled_qty"))))
                except (InvalidOperation, TypeError, ValueError) as exc:
                    raise ValueError(f"legacy evidence raw order {external_id} has invalid quantity") from exc
                raw_symbol = str(raw.get("code", raw.get("symbol", raw.get("ticker", "")))).strip()
                expected_symbol = str(row["instrument_symbol"]).strip().upper()
                if (
                    raw_order_id != external_id
                    or raw_status not in {"FILLED", "FILLED_ALL", "FULLY_FILLED"}
                    or raw_quantity != quantity
                    or raw_dealt != quantity
                    or raw_symbol.upper().split(".")[-1] != expected_symbol
                ):
                    raise ValueError(f"legacy evidence raw order {external_id} conflicts with durable order/leg")
                side = str(row["side"]).upper()
                if side not in {"BUY", "SELL"}:
                    raise ValueError(f"legacy evidence order {external_id} has unsupported side")
                metadata = dict(metadata)
                metadata.setdefault("evidence_mode", ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS.value)
                metadata.setdefault("evidence_reference", f"{account_id}:order:{external_id}")
                row["_source_broker_symbol"] = raw_symbol
                row["_fill"] = fill
                row["_fill_metadata"] = metadata
                row["_net_quantity"] = quantity if side == "BUY" else -quantity
                candidate_orders.append(row)

            strategy_ids = {str(row["strategy_id"]) for row in candidate_orders}
            if len(strategy_ids) != 1:
                raise ValueError("legacy evidence must belong to one source strategy")
            source_strategy_id = next(iter(strategy_ids))
            source_strategy = source.execute(
                "SELECT * FROM core_strategies WHERE id = ?", (source_strategy_id,)
            ).fetchone()
            if source_strategy is None:
                raise ValueError("legacy evidence source strategy is missing")
            source_instrument_ids = {str(row["instrument_id"]) for row in candidate_orders}
            source_instruments = {
                str(row["id"]): dict(row)
                for row in source.execute(
                    "SELECT * FROM core_instruments WHERE id IN (%s)"
                    % ",".join("?" for _ in source_instrument_ids),
                    tuple(source_instrument_ids),
                ).fetchall()
            }
            if set(source_instruments) != source_instrument_ids:
                raise ValueError("legacy evidence source instrument registry is incomplete")
            source_symbols = {
                str(instrument["symbol"]).strip().upper()
                for instrument in source_instruments.values()
            }
            target_instrument_ids = {item: item for item in source_instrument_ids}
            mapping_metadata: dict[str, str] = {}
            if instrument_mapping is not None:
                normalized_mapping: dict[str, str] = {}
                for raw_source_symbol, raw_target_id in instrument_mapping.items():
                    source_symbol = str(raw_source_symbol).strip().upper().split(".")[-1]
                    target_id = str(raw_target_id).strip()
                    if not source_symbol or not target_id or source_symbol in normalized_mapping:
                        raise ValueError("legacy instrument mapping is empty or ambiguous")
                    normalized_mapping[source_symbol] = target_id
                if set(normalized_mapping) != source_symbols:
                    raise ValueError(
                        "legacy instrument mapping must cover exactly every source symbol"
                    )
                if len(set(normalized_mapping.values())) != len(normalized_mapping):
                    raise ValueError("legacy instrument mapping maps multiple source symbols to one instrument")
                for source_id, instrument in source_instruments.items():
                    source_symbol = str(instrument["symbol"]).strip().upper()
                    target_id = normalized_mapping[source_symbol]
                    destination = self.get_instrument(target_id)
                    if destination is None:
                        raise ValueError(f"legacy instrument mapping destination is not persisted: {target_id}")
                    for field in ("asset_class", "symbol", "venue", "currency", "multiplier", "tick_size", "lot_size"):
                        expected = str(instrument[field]).upper() if field in {"symbol", "venue", "currency"} else str(instrument[field])
                        observed = str(destination.get(field)).upper() if field in {"symbol", "venue", "currency"} else str(destination.get(field))
                        if expected != observed:
                            raise ValueError(
                                f"legacy instrument mapping destination {target_id} conflicts on {field}"
                            )
                    provider_mapping = self.find_instrument_mapping(
                        instrument_id=target_id,
                        provider="moomoo",
                        purpose="BROKER",
                    )
                    if provider_mapping is None:
                        raise ValueError(f"legacy instrument mapping destination lacks Moomoo mapping: {target_id}")
                    source_codes = {
                        str(row["_source_broker_symbol"]).strip().upper()
                        for row in candidate_orders
                        if str(row["instrument_id"]) == source_id
                    }
                    destination_code = str(provider_mapping["external_symbol"]).strip().upper()
                    if any(code != destination_code for code in source_codes):
                        raise ValueError(
                            f"legacy instrument mapping destination Moomoo code conflicts for {source_symbol}"
                        )
                    target_instrument_ids[source_id] = target_id
                    mapping_metadata[source_symbol] = target_id
            net_by_instrument: dict[str, Decimal] = {}
            for row in candidate_orders:
                instrument_id = str(row["instrument_id"])
                net_by_instrument[instrument_id] = net_by_instrument.get(instrument_id, Decimal("0")) + row["_net_quantity"]
            allocations = [
                dict(row)
                for row in source.execute(
                    "SELECT * FROM core_position_allocations WHERE account_id = ?",
                    (str(account_id),),
                ).fetchall()
            ]
            allocated_by_instrument: dict[str, Decimal] = {}
            for row in allocations:
                ownership = str(row["ownership_class"]).upper()
                quantity = Decimal(str(row["signed_quantity"]))
                if not quantity.is_finite():
                    raise ValueError("legacy evidence contains non-finite position allocation")
                if quantity == 0:
                    continue
                if ownership != OwnershipClass.MANAGED.value:
                    raise ValueError("legacy evidence contains non-managed nonzero allocation")
                instrument_id = str(row["instrument_id"])
                if instrument_id not in net_by_instrument:
                    raise ValueError("legacy evidence contains an allocation for an unknown instrument")
                allocated_by_instrument[instrument_id] = allocated_by_instrument.get(instrument_id, Decimal("0")) + quantity
            if allocated_by_instrument != net_by_instrument:
                raise ValueError(
                    f"legacy evidence position allocations do not reconcile to order claims: "
                    f"allocations={allocated_by_instrument}, orders={net_by_instrument}"
                )
            if any(value != 0 for value in net_by_instrument.values()):
                raise ValueError("legacy evidence source is not flat after durable order/allocation reconciliation")

            fingerprint_payload = [
                {
                    "external_order_id": str(row["external_order_id"]),
                    "account_id": str(account_id),
                    "instrument_id": str(row["instrument_id"]),
                    "side": str(row["side"]),
                    "quantity": str(row["submitted_quantity"]),
                    "fill": {
                        "quantity": str(row["_fill"]["quantity"]),
                        "price": str(row["_fill"]["price"]),
                        "filled_at": str(row["_fill"]["filled_at"]),
                        "raw": row["_fill_metadata"].get("raw"),
                    },
                }
                for row in candidate_orders
            ]
            source_fingerprint = _fingerprint(fingerprint_payload)
            selected_intent_ids = {str(row["intent_id"]) for row in candidate_orders}
            source_intents = {
                str(row["id"]): dict(row)
                for row in source.execute(
                    "SELECT * FROM core_order_intents WHERE id IN (%s)"
                    % ",".join("?" for _ in selected_intent_ids),
                    tuple(selected_intent_ids),
                ).fetchall()
            }
            if set(source_intents) != selected_intent_ids:
                raise ValueError("legacy evidence intent registry is incomplete")
            source_legs = {
                str(row["id"]): dict(row)
                for row in source.execute(
                    "SELECT * FROM core_order_legs WHERE intent_id IN (%s)"
                    % ",".join("?" for _ in selected_intent_ids),
                    tuple(selected_intent_ids),
                ).fetchall()
            }
            source_order_ids = tuple(sorted(str(row["external_order_id"]) for row in candidate_orders))
        finally:
            source.close()

        def _same_text(left: Any, right: Any) -> bool:
            return ("" if left is None else str(left)) == ("" if right is None else str(right))

        def _same_decimal(left: Any, right: Any) -> bool:
            try:
                return Decimal(str(left)) == Decimal(str(right))
            except (InvalidOperation, TypeError, ValueError):
                return False

        def _same_optional_decimal(left: Any, right: Any) -> bool:
            if left is None or right is None:
                return left is None and right is None
            return _same_decimal(left, right)

        def _metadata(value: Any) -> dict[str, Any]:
            decoded = _decode(value) if isinstance(value, str) or value is None else value
            return dict(decoded) if isinstance(decoded, Mapping) else {}

        def _without_source_database(value: Mapping[str, Any]) -> dict[str, Any]:
            result = dict(value)
            result.pop("source_database", None)
            return result

        def _reuse_existing_graph() -> dict[str, Any] | None:
            """Return the exact retired graph, or None when no claim exists."""

            placeholders = ",".join("?" for _ in source_order_ids)
            with self.transaction() as conn:
                existing_rows = [
                    dict(row)
                    for row in conn.execute(
                        f"SELECT * FROM core_broker_orders "
                        f"WHERE account_id = ? AND external_order_id IN ({placeholders}) "
                        "ORDER BY external_order_id, id",
                        (str(account_id), *source_order_ids),
                    ).fetchall()
                ]
                if not existing_rows:
                    # A prior transaction should create the whole retired
                    # graph atomically.  If a matching provenance marker is
                    # nevertheless present without any of the source order
                    # claims, treat it as an incomplete graph rather than
                    # silently rebuilding it under a new label.
                    provenance_rows = [
                        dict(row)
                        for row in conn.execute(
                            "SELECT id, metadata_json FROM core_strategies "
                            "UNION ALL SELECT id, metadata_json FROM core_books"
                        ).fetchall()
                    ]
                    for row in provenance_rows:
                        metadata = _metadata(row.get("metadata_json"))
                        if (
                            metadata.get("legacy_import") is True
                            and metadata.get("retired") is True
                            and str(metadata.get("source_ledger_fingerprint", "")) == source_fingerprint
                        ):
                            raise ValueError(
                                "existing legacy graph has provenance but no broker-order claims"
                            )
                    return None
                existing_by_external: dict[str, list[dict[str, Any]]] = {}
                for row in existing_rows:
                    existing_by_external.setdefault(str(row["external_order_id"]), []).append(row)
                if set(existing_by_external) != set(source_order_ids):
                    raise ValueError(
                        "legacy evidence partially overlaps existing target broker-order claims"
                    )
                if any(len(rows) != 1 for rows in existing_by_external.values()):
                    raise ValueError("legacy evidence has ambiguous existing broker-order claims")

                imported_intents: dict[str, str] = {}
                imported_legs: dict[str, str] = {}
                imported_orders: dict[str, str] = {}
                imported_fills: dict[str, str] = {}
                imported_allocations: dict[str, str] = {}
                target_strategy_ids: set[str] = set()
                target_book_ids: set[str] = set()
                mapped_source_legs: set[str] = set()

                for external_id in source_order_ids:
                    source_order = by_external[external_id]
                    existing_order = existing_by_external[external_id][0]
                    existing_metadata = _metadata(existing_order.get("metadata_json"))
                    expected_order_metadata = _metadata(source_order.get("metadata_json"))
                    expected_order_metadata.update(
                        {
                            "legacy_import": True,
                            "source_broker_order_id": str(source_order["id"]),
                            "source_ledger_fingerprint": source_fingerprint,
                            "retired": True,
                        }
                    )
                    if (
                        str(existing_order["account_id"]) != str(account_id)
                        or str(existing_order["broker"]) != target_account.broker
                        or str(existing_order["external_order_id"]) != external_id
                        or str(existing_order["status"]).upper() != BrokerOrderStatus.FILLED.value
                        or int(existing_order["attempt_number"]) != int(source_order["attempt_number"])
                        or not _same_text(existing_order["client_order_id"], source_order["client_order_id"])
                        or not _same_decimal(existing_order["submitted_quantity"], source_order["submitted_quantity"])
                        or existing_metadata != expected_order_metadata
                    ):
                        raise ValueError(f"existing legacy broker-order evidence conflicts for {external_id}")

                    source_leg_id = str(source_order["order_leg_id"])
                    source_leg = source_legs.get(source_leg_id)
                    if source_leg is None:
                        raise ValueError(f"existing legacy graph is missing source leg {source_leg_id}")
                    target_leg_id = str(existing_order["order_leg_id"])
                    if source_leg_id in imported_legs and imported_legs[source_leg_id] != target_leg_id:
                        raise ValueError(f"existing legacy graph maps source leg {source_leg_id} ambiguously")
                    imported_legs[source_leg_id] = target_leg_id
                    mapped_source_legs.add(source_leg_id)
                    target_leg = conn.execute(
                        "SELECT * FROM core_order_legs WHERE id = ?", (target_leg_id,)
                    ).fetchone()
                    if target_leg is None:
                        raise ValueError(f"existing legacy graph is missing target leg {target_leg_id}")
                    target_leg = dict(target_leg)
                    target_instrument_id = target_instrument_ids[str(source_leg["instrument_id"])]
                    expected_leg_metadata = _metadata(source_leg.get("metadata_json"))
                    expected_leg_metadata.setdefault(
                        "legacy_source_instrument_id", str(source_leg["instrument_id"])
                    )
                    if (
                        str(target_leg["instrument_id"]) != target_instrument_id
                        or int(target_leg["sequence"]) != int(source_leg["sequence"])
                        or str(target_leg["side"]).upper() != str(source_leg["side"]).upper()
                        or not _same_decimal(target_leg["quantity"], source_leg["quantity"])
                        or not _same_text(target_leg["quantity_unit"], source_leg["quantity_unit"])
                        or not _same_text(target_leg["order_type"], source_leg["order_type"])
                        or not _same_text(target_leg["limit_price"], source_leg["limit_price"])
                        or not _same_text(target_leg["stop_price"], source_leg["stop_price"])
                        or not _same_text(target_leg["time_in_force"], source_leg["time_in_force"])
                        or str(target_leg["status"]).upper() != str(source_leg["status"]).upper()
                        or not _same_decimal(
                            target_leg["cumulative_filled_quantity"], source_leg["cumulative_filled_quantity"]
                        )
                        or not _same_optional_decimal(target_leg["average_fill_price"], source_leg["average_fill_price"])
                        or _metadata(target_leg.get("metadata_json")) != expected_leg_metadata
                    ):
                        raise ValueError(f"existing legacy leg evidence conflicts for {external_id}")

                    source_intent_id = str(source_order["intent_id"])
                    target_intent_id = str(target_leg["intent_id"])
                    if source_intent_id in imported_intents and imported_intents[source_intent_id] != target_intent_id:
                        raise ValueError(f"existing legacy graph maps source intent {source_intent_id} ambiguously")
                    imported_intents[source_intent_id] = target_intent_id
                    source_intent = source_intents.get(source_intent_id)
                    if source_intent is None:
                        raise ValueError(f"existing legacy graph is missing source intent {source_intent_id}")
                    target_intent = conn.execute(
                        "SELECT * FROM core_order_intents WHERE id = ?", (target_intent_id,)
                    ).fetchone()
                    if target_intent is None:
                        raise ValueError(f"existing legacy graph is missing target intent {target_intent_id}")
                    target_intent = dict(target_intent)
                    expected_intent_metadata = _metadata(source_intent.get("metadata_json"))
                    expected_intent_metadata.update(
                        {
                            "legacy_import": True,
                            "source_intent_id": source_intent_id,
                            "source_ledger_fingerprint": source_fingerprint,
                            "retired": True,
                        }
                    )
                    if (
                        str(target_intent["account_id"]) != str(account_id)
                        or not _same_text(target_intent["action"], source_intent["action"])
                        or not _same_text(target_intent["status"], source_intent["status"])
                        or not _same_text(target_intent["source_signal_id"], source_intent["source_signal_id"])
                        or not _same_text(target_intent["payload_hash"], source_intent["payload_hash"])
                        or not _same_text(
                            target_intent["execution_policy_json"], source_intent["execution_policy_json"]
                        )
                        or _metadata(target_intent.get("metadata_json")) != expected_intent_metadata
                    ):
                        raise ValueError(f"existing legacy intent evidence conflicts for {external_id}")
                    target_strategy_ids.add(str(target_intent["strategy_id"]))
                    target_book_ids.add(str(target_intent["book_id"]))

                    target_fills = [
                        dict(row)
                        for row in conn.execute(
                            "SELECT * FROM core_fills WHERE broker_order_id = ? ORDER BY id",
                            (str(existing_order["id"]),),
                        ).fetchall()
                    ]
                    if len(target_fills) != 1:
                        raise ValueError(f"existing legacy graph has incomplete fills for {external_id}")
                    target_fill = target_fills[0]
                    source_fill = source_order["_fill"]
                    if (
                        str(target_fill["order_leg_id"]) != target_leg_id
                        or not _same_text(target_fill["external_fill_id"], source_fill["external_fill_id"])
                        or not _same_text(target_fill["dedupe_key"], source_fill["dedupe_key"])
                        or not _same_decimal(target_fill["quantity"], source_fill["quantity"])
                        or not _same_decimal(target_fill["price"], source_fill["price"])
                        or not _same_text(target_fill["fee"], source_fill["fee"])
                        or not _same_text(target_fill["fee_currency"], source_fill["fee_currency"])
                        or not _same_text(target_fill["filled_at"], source_fill["filled_at"])
                        or str(target_fill["evidence_mode"]) != ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS.value
                        or _metadata(target_fill.get("metadata_json")) != source_order["_fill_metadata"]
                    ):
                        raise ValueError(f"existing legacy fill evidence conflicts for {external_id}")
                    imported_orders[external_id] = str(existing_order["id"])
                    imported_fills[str(source_fill["id"])] = str(target_fill["id"])

                if mapped_source_legs != set(source_legs):
                    raise ValueError("existing legacy graph is incomplete: not every source leg is represented")
                if len(target_strategy_ids) != 1 or len(target_book_ids) != 1:
                    raise ValueError("existing legacy graph has ambiguous strategy or book ownership")
                target_strategy_id = next(iter(target_strategy_ids))
                target_book_id = next(iter(target_book_ids))
                strategy_row = conn.execute(
                    "SELECT * FROM core_strategies WHERE id = ?", (target_strategy_id,)
                ).fetchone()
                book_row = conn.execute("SELECT * FROM core_books WHERE id = ?", (target_book_id,)).fetchone()
                if strategy_row is None or book_row is None:
                    raise ValueError("existing legacy graph is missing its strategy or book")
                expected_strategy_metadata = {
                    "legacy_import": True,
                    "source_database": str(source_path),
                    "source_strategy_id": source_strategy_id,
                    "source_ledger_fingerprint": source_fingerprint,
                    "source_to_target_instrument_mapping": mapping_metadata,
                    "retired": True,
                }
                expected_book_metadata = {
                    "legacy_import": True,
                    "source_database": str(source_path),
                    "source_ledger_fingerprint": source_fingerprint,
                    "source_to_target_instrument_mapping": mapping_metadata,
                    "retired": True,
                }
                if (
                    _without_source_database(_metadata(strategy_row["metadata_json"]))
                    != _without_source_database(expected_strategy_metadata)
                    or _without_source_database(_metadata(book_row["metadata_json"]))
                    != _without_source_database(expected_book_metadata)
                ):
                    raise ValueError("existing legacy strategy/book provenance conflicts")

                graph_intents = {
                    str(row["id"])
                    for row in conn.execute(
                        f"SELECT id FROM core_order_intents WHERE account_id = ? AND strategy_id = ? "
                        "AND book_id = ?",
                        (str(account_id), target_strategy_id, target_book_id),
                    ).fetchall()
                }
                if graph_intents != set(imported_intents.values()):
                    raise ValueError("existing legacy graph has incomplete or extra intent rows")
                graph_intent_placeholders = ",".join("?" for _ in graph_intents)
                graph_legs = {
                    str(row["id"])
                    for row in conn.execute(
                        f"SELECT l.id FROM core_order_legs l JOIN core_order_intents i ON i.id = l.intent_id "
                        f"WHERE i.account_id = ? AND i.strategy_id = ? AND i.book_id = ? "
                        f"AND i.id IN ({graph_intent_placeholders})",
                        (str(account_id), target_strategy_id, target_book_id, *graph_intents),
                    ).fetchall()
                }
                if graph_legs != set(imported_legs.values()):
                    raise ValueError("existing legacy graph has incomplete or extra leg rows")
                graph_leg_placeholders = ",".join("?" for _ in graph_legs)
                graph_orders = [
                    dict(row)
                    for row in conn.execute(
                        f"SELECT b.* FROM core_broker_orders b JOIN core_order_legs l ON l.id = b.order_leg_id "
                        f"WHERE l.id IN ({graph_leg_placeholders})",
                        tuple(graph_legs),
                    ).fetchall()
                ]
                if len(graph_orders) != len(source_order_ids) or {
                    str(row["external_order_id"]) for row in graph_orders
                } != set(source_order_ids):
                    raise ValueError("existing legacy graph has incomplete or extra broker-order rows")

                nonzero_source_allocations = []
                for allocation in allocations:
                    if Decimal(str(allocation["signed_quantity"])) != 0:
                        nonzero_source_allocations.append(allocation)
                allocation_rows = [
                    dict(row)
                    for row in conn.execute(
                        "SELECT * FROM core_position_allocations WHERE account_id = ? AND strategy_id = ? AND book_id = ?",
                        (str(account_id), target_strategy_id, target_book_id),
                    ).fetchall()
                ]
                source_allocation_ids = {str(row["id"]) for row in nonzero_source_allocations}
                existing_allocation_by_source: dict[str, dict[str, Any]] = {}
                for row in allocation_rows:
                    metadata = _metadata(row.get("metadata_json"))
                    source_allocation_id = str(metadata.get("source_allocation_id", "")).strip()
                    if not source_allocation_id or source_allocation_id in existing_allocation_by_source:
                        raise ValueError("existing legacy graph has ambiguous allocation provenance")
                    existing_allocation_by_source[source_allocation_id] = row
                if set(existing_allocation_by_source) != source_allocation_ids:
                    raise ValueError("existing legacy graph has incomplete or extra allocation rows")
                for source_allocation in nonzero_source_allocations:
                    source_allocation_id = str(source_allocation["id"])
                    target_allocation = existing_allocation_by_source[source_allocation_id]
                    target_source_intent = imported_intents.get(str(source_allocation.get("source_intent_id")))
                    expected_allocation_metadata = _metadata(source_allocation.get("metadata_json"))
                    expected_allocation_metadata.update(
                        {
                            "legacy_import": True,
                            "source_allocation_id": source_allocation_id,
                            "legacy_source_instrument_id": str(source_allocation["instrument_id"]),
                            "retired": True,
                        }
                    )
                    if (
                        str(target_allocation["account_id"]) != str(account_id)
                        or str(target_allocation["instrument_id"])
                        != target_instrument_ids[str(source_allocation["instrument_id"])]
                        or str(target_allocation["strategy_id"]) != target_strategy_id
                        or str(target_allocation["book_id"]) != target_book_id
                        or str(target_allocation["ownership_class"]) != str(source_allocation["ownership_class"])
                        or not _same_decimal(target_allocation["signed_quantity"], source_allocation["signed_quantity"])
                        or str(target_allocation["source_intent_id"]) != str(target_source_intent)
                        or not _same_text(target_allocation["updated_at"], source_allocation["updated_at"])
                        or _metadata(target_allocation.get("metadata_json")) != expected_allocation_metadata
                    ):
                        raise ValueError(f"existing legacy allocation evidence conflicts for {source_allocation_id}")
                    imported_allocations[source_allocation_id] = str(target_allocation["id"])

                return {
                    "source_database": str(source_path),
                    "source_ledger_fingerprint": source_fingerprint,
                    "source_order_ids": source_order_ids,
                    "legacy_strategy_id": target_strategy_id,
                    "legacy_book_id": target_book_id,
                    "source_to_target_instrument_mapping": mapping_metadata,
                    "imported_intents": imported_intents,
                    "imported_legs": imported_legs,
                    "imported_orders": imported_orders,
                    "imported_fills": imported_fills,
                    "imported_allocations": imported_allocations,
                    "verified_flat": True,
                    "evidence_mode": ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS.value,
                    "reused_existing_graph": True,
                }

        legacy_strategy_id = f"{label}:{account_id}:strategy"
        legacy_book_id = f"{label}:{account_id}:book"
        reused = _reuse_existing_graph()
        if reused is not None:
            return reused
        now = _timestamp(utc_now())
        imported_intents: dict[str, str] = {}
        imported_legs: dict[str, str] = {}
        imported_orders: dict[str, str] = {}
        imported_fills: dict[str, str] = {}
        imported_allocations: dict[str, str] = {}
        with self.transaction() as conn:
            strategy_row = conn.execute("SELECT * FROM core_strategies WHERE id = ?", (legacy_strategy_id,)).fetchone()
            strategy_metadata = {
                "legacy_import": True,
                "source_database": str(source_path),
                "source_strategy_id": source_strategy_id,
                "source_ledger_fingerprint": source_fingerprint,
                "source_to_target_instrument_mapping": mapping_metadata,
                "retired": True,
            }
            if strategy_row is None:
                conn.execute(
                    """INSERT INTO core_strategies
                       (id, name, strategy_type, version, enabled, config_json,
                        metadata_json, created_at, updated_at)
                       VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?)""",
                    (legacy_strategy_id, f"{label} retired ledger", str(source_strategy["strategy_type"]),
                     str(source_strategy["version"]), source_strategy["config_json"] or "{}",
                     _json(strategy_metadata), str(source_strategy["created_at"] or now), now),
                )
            elif _decode(strategy_row["metadata_json"]) != strategy_metadata:
                raise ValueError("legacy strategy ID exists with different source fingerprint")
            book_row = conn.execute("SELECT * FROM core_books WHERE id = ?", (legacy_book_id,)).fetchone()
            book_metadata = {
                "legacy_import": True,
                "source_database": str(source_path),
                "source_ledger_fingerprint": source_fingerprint,
                "source_to_target_instrument_mapping": mapping_metadata,
                "retired": True,
            }
            if book_row is None:
                conn.execute(
                    """INSERT INTO core_books
                       (id, name, enabled, metadata_json, created_at, updated_at)
                       VALUES (?, ?, 1, ?, ?, ?)""",
                    (legacy_book_id, f"{label} retired ledger", _json(book_metadata), now, now),
                )
            elif _decode(book_row["metadata_json"]) != book_metadata:
                raise ValueError("legacy book ID exists with different source fingerprint")

            for instrument_id, instrument in source_instruments.items():
                target_instrument_id = target_instrument_ids[instrument_id]
                existing = conn.execute(
                    """SELECT asset_class, symbol, venue, currency, multiplier, tick_size,
                              lot_size, expiry, strike, option_right
                         FROM core_instruments WHERE id = ?""",
                    (target_instrument_id,),
                ).fetchone()
                comparable = tuple(instrument.get(key) for key in (
                    "asset_class", "symbol", "venue", "currency", "multiplier", "tick_size", "lot_size", "expiry", "strike", "option_right"
                ))
                if existing is not None and tuple(existing) != comparable:
                    raise ValueError(f"target instrument {target_instrument_id} conflicts with legacy source")
                if existing is None and target_instrument_id != instrument_id:
                    raise ValueError(f"mapped target instrument is missing: {target_instrument_id}")
                if existing is None:
                    conn.execute(
                        """INSERT INTO core_instruments
                           (id, asset_class, symbol, venue, currency, multiplier, tick_size, lot_size,
                            expiry, strike, option_right, metadata_json, created_at, updated_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (target_instrument_id, instrument["asset_class"], instrument["symbol"], instrument["venue"], instrument["currency"],
                         instrument["multiplier"], instrument["tick_size"], instrument["lot_size"], instrument["expiry"], instrument["strike"],
                         instrument["option_right"], instrument.get("metadata_json") or "{}", instrument["created_at"], instrument["updated_at"]),
                    )

            for source_intent_id, source_intent in source_intents.items():
                target_intent_id = f"{label}:intent:{source_intent_id}"
                imported_intents[source_intent_id] = target_intent_id
                intent_metadata = _decode(source_intent.get("metadata_json"))
                intent_metadata = dict(intent_metadata) if isinstance(intent_metadata, Mapping) else {}
                intent_metadata.update({"legacy_import": True, "source_intent_id": source_intent_id,
                                        "source_ledger_fingerprint": source_fingerprint, "retired": True})
                existing = conn.execute("SELECT * FROM core_order_intents WHERE id = ?", (target_intent_id,)).fetchone()
                if existing is None:
                    conn.execute(
                        """INSERT INTO core_order_intents
                           (id, idempotency_key, payload_hash, strategy_id, book_id, account_id,
                            action, status, source_signal_id, execution_policy_json, metadata_json,
                            created_at, updated_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (target_intent_id, f"{label}|{source_intent_id}|{source_fingerprint[:16]}", str(source_intent["payload_hash"]),
                         legacy_strategy_id, legacy_book_id, str(account_id), str(source_intent["action"]), str(source_intent["status"]),
                         source_intent["source_signal_id"], source_intent["execution_policy_json"], _json(intent_metadata),
                         source_intent["created_at"], source_intent["updated_at"]),
                    )
                elif _decode(existing["metadata_json"]) != intent_metadata:
                    raise ValueError(f"legacy intent ID exists with different source evidence: {source_intent_id}")

            for source_leg_id, source_leg in source_legs.items():
                source_intent_id = str(source_leg["intent_id"])
                if source_intent_id not in imported_intents:
                    continue
                target_leg_id = f"{label}:leg:{source_leg_id}"
                imported_legs[source_leg_id] = target_leg_id
                if conn.execute("SELECT 1 FROM core_order_legs WHERE id = ?", (target_leg_id,)).fetchone() is None:
                    leg_metadata = _decode(source_leg.get("metadata_json"))
                    leg_metadata = dict(leg_metadata) if isinstance(leg_metadata, Mapping) else {}
                    leg_metadata.setdefault("legacy_source_instrument_id", str(source_leg["instrument_id"]))
                    conn.execute(
                        """INSERT INTO core_order_legs
                           (id, intent_id, sequence, instrument_id, side, quantity, quantity_unit,
                            order_type, limit_price, stop_price, time_in_force, status,
                            cumulative_filled_quantity, average_fill_price, metadata_json,
                            created_at, updated_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (target_leg_id, imported_intents[source_intent_id], int(source_leg["sequence"]),
                         target_instrument_ids[str(source_leg["instrument_id"])],
                         source_leg["side"], source_leg["quantity"], source_leg["quantity_unit"], source_leg["order_type"], source_leg["limit_price"],
                         source_leg["stop_price"], source_leg["time_in_force"], source_leg["status"], source_leg["cumulative_filled_quantity"],
                         source_leg["average_fill_price"], _json(leg_metadata), source_leg["created_at"], source_leg["updated_at"]),
                    )

            for source_order in candidate_orders:
                source_order_id = str(source_order["id"])
                target_order_id = f"{label}:order:{source_order_id}"
                imported_orders[str(source_order["external_order_id"])] = target_order_id
                metadata = _decode(source_order.get("metadata_json"))
                metadata = dict(metadata) if isinstance(metadata, Mapping) else {}
                metadata.update({"legacy_import": True, "source_broker_order_id": source_order_id,
                                 "source_ledger_fingerprint": source_fingerprint, "retired": True})
                existing = conn.execute("SELECT * FROM core_broker_orders WHERE id = ?", (target_order_id,)).fetchone()
                if existing is None:
                    conn.execute(
                        """INSERT INTO core_broker_orders
                           (id, order_leg_id, account_id, broker, attempt_number, external_order_id,
                            client_order_id, status, submitted_quantity, submitted_at, updated_at,
                            replaces_broker_order_id, metadata_json)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)""",
                        (target_order_id, imported_legs[str(source_order["order_leg_id"])], str(account_id), source_order["broker"],
                         int(source_order["attempt_number"]), source_order["external_order_id"], source_order["client_order_id"], source_order["status"],
                         source_order["submitted_quantity"], source_order["submitted_at"], source_order["updated_at"], _json(metadata)),
                    )
                elif str(existing["external_order_id"]) != str(source_order["external_order_id"]):
                    raise ValueError("legacy broker order ID exists with different external order")
                source_fill = source_order["_fill"]
                target_fill_id = f"{label}:fill:{source_fill['id']}"
                imported_fills[str(source_fill["id"])] = target_fill_id
                existing_fill = conn.execute("SELECT 1 FROM core_fills WHERE id = ?", (target_fill_id,)).fetchone()
                if existing_fill is None:
                    conn.execute(
                        """INSERT INTO core_fills
                           (id, broker_order_id, order_leg_id, external_fill_id, dedupe_key,
                            quantity, price, fee, fee_currency, filled_at, received_at,
                            evidence_mode, metadata_json)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (target_fill_id, target_order_id, imported_legs[str(source_order["order_leg_id"])], source_fill["external_fill_id"],
                         source_fill["dedupe_key"], source_fill["quantity"], source_fill["price"], source_fill["fee"], source_fill["fee_currency"],
                         source_fill["filled_at"], source_fill["received_at"], ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS.value,
                         _json(source_order["_fill_metadata"])),
                    )

            for allocation in allocations:
                quantity = Decimal(str(allocation["signed_quantity"]))
                if quantity == 0:
                    continue
                source_intent_id = allocation.get("source_intent_id")
                target_source_intent = imported_intents.get(str(source_intent_id)) if source_intent_id else None
                if target_source_intent is None:
                    raise ValueError("legacy allocation is not attributable to an imported intent")
                target_allocation_id = f"{label}:allocation:{allocation['id']}"
                imported_allocations[str(allocation["id"])] = target_allocation_id
                metadata = _decode(allocation.get("metadata_json"))
                metadata = dict(metadata) if isinstance(metadata, Mapping) else {}
                metadata.update({"legacy_import": True, "source_allocation_id": str(allocation["id"]),
                                 "legacy_source_instrument_id": str(allocation["instrument_id"]), "retired": True})
                if conn.execute("SELECT 1 FROM core_position_allocations WHERE id = ?", (target_allocation_id,)).fetchone() is None:
                    conn.execute(
                        """INSERT INTO core_position_allocations
                           (id, account_id, instrument_id, strategy_id, book_id,
                            ownership_class, signed_quantity, source_intent_id, updated_at, metadata_json)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (target_allocation_id, str(account_id), target_instrument_ids[str(allocation["instrument_id"])], legacy_strategy_id, legacy_book_id,
                         allocation["ownership_class"], allocation["signed_quantity"], target_source_intent, allocation["updated_at"], _json(metadata)),
                    )

        return {
            "source_database": str(source_path),
            "source_ledger_fingerprint": source_fingerprint,
            "source_order_ids": source_order_ids,
            "legacy_strategy_id": legacy_strategy_id,
            "legacy_book_id": legacy_book_id,
            "source_to_target_instrument_mapping": mapping_metadata,
            "imported_intents": imported_intents,
            "imported_legs": imported_legs,
            "imported_orders": imported_orders,
            "imported_fills": imported_fills,
            "imported_allocations": imported_allocations,
            "verified_flat": True,
            "evidence_mode": ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS.value,
            "reused_existing_graph": False,
        }

    def save_position_allocation(
        self,
        allocation: PositionAllocation,
        *,
        _validation_token: object | None = None,
    ) -> None:
        """Persist a validated ledger row, never an arbitrary public claim.

        Allocation rows are broker-accounting facts, not user preferences.
        The generic fill path writes its own validated managed rows inside
        ``record_fill``; callers that need this lower-level operation must
        use this repository instance's private capability and provide a
        persisted account/source/provenance envelope.
        """
        if _validation_token is not self.__allocation_validation_capability:
            raise PermissionError("validated position-allocation capability is required")
        provenance = allocation.metadata.get("provenance", allocation.metadata.get("source"))
        if provenance in (None, "", {}):
            raise ValueError("position allocation requires explicit provenance")
        with self.transaction() as conn:
            account_row = conn.execute(
                "SELECT broker, environment, external_account_id, enabled FROM core_accounts WHERE id = ?",
                (allocation.account_id,),
            ).fetchone()
            if account_row is None or not bool(account_row["enabled"]):
                raise ValueError("position allocation requires an enabled persisted account")
            if allocation.ownership_class is OwnershipClass.MANAGED:
                if allocation.source_intent_id is None:
                    raise ValueError("managed position allocation requires source_intent_id")
                intent_row = conn.execute(
                    "SELECT account_id, strategy_id, book_id FROM core_order_intents WHERE id = ?",
                    (allocation.source_intent_id,),
                ).fetchone()
                if intent_row is None or str(intent_row["account_id"]) != allocation.account_id:
                    raise ValueError("managed position allocation source intent does not match account")
                if allocation.strategy_id is not None and str(intent_row["strategy_id"]) != allocation.strategy_id:
                    raise ValueError("managed position allocation strategy does not match source intent")
                if allocation.book_id is not None and str(intent_row["book_id"]) != allocation.book_id:
                    raise ValueError("managed position allocation book does not match source intent")
            existing_row = conn.execute(
                "SELECT account_id, instrument_id FROM core_position_allocations WHERE id = ?",
                (allocation.id,),
            ).fetchone()
            if existing_row is not None:
                if str(existing_row["account_id"]) != allocation.account_id:
                    raise ValueError("position allocation ID is owned by a different account")
                if str(existing_row["instrument_id"]) != allocation.instrument_id:
                    raise ValueError("position allocation ID is owned by a different instrument")
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

    def verified_retired_book_closure(
        self,
        account_id: str,
        book_id: str,
        baseline: ExecutionEvidenceBaseline | None,
        *,
        allow_retired_baseline_blockers: bool = False,
        capacity_only: bool = False,
    ) -> tuple[bool, str]:
        """Validate a flat, imported book before it is ignored by a pilot gate.

        A retired book is historical ownership, not an exemption based on its
        label or on a zero broker position.  The durable book metadata,
        baseline fingerprint, terminal order claims, typed fills, and per-book
        allocation ledger must all agree before its rows can be excluded from
        the active pilot books.
        """
        if baseline is None:
            return False, "verified execution baseline is missing"
        if baseline.account_id != str(account_id) or not baseline.verified_flat or baseline.status != "VERIFIED":
            return False, "verified flat baseline does not belong to this account"
        baseline_metadata = baseline.metadata
        if not isinstance(baseline_metadata, Mapping):
            return False, "baseline metadata is not inspectable"
        if str(baseline_metadata.get("legacy_book_id", "")) != str(book_id):
            return False, "baseline does not cover this retired book"
        if not baseline.source_ledger_fingerprint or not baseline.source_order_ids:
            return False, "baseline has no complete source ledger coverage"
        if not {"SOURCE_LEDGER", "HISTORICAL_ORDER_SNAPSHOTS"}.issubset(set(baseline.coverage)):
            return False, "baseline coverage does not include source and historical order evidence"
        expected_orders = {str(value).strip() for value in baseline.source_order_ids if str(value).strip()}
        if len(expected_orders) != len(baseline.source_order_ids):
            return False, "baseline source order coverage is malformed"

        terminal_order_statuses = {
            BrokerOrderStatus.FILLED.value,
            BrokerOrderStatus.REJECTED.value,
            BrokerOrderStatus.CANCELLED.value,
            BrokerOrderStatus.FAILED.value,
        }
        terminal_leg_statuses = {
            LegStatus.FILLED.value,
            LegStatus.REJECTED.value,
            LegStatus.CANCELLED.value,
            LegStatus.FAILED.value,
        }
        terminal_intent_statuses = {
            IntentStatus.FILLED.value,
            IntentStatus.COMPLETED.value,
            IntentStatus.REJECTED.value,
            IntentStatus.CANCELLED.value,
            IntentStatus.FAILED.value,
        }
        with self.transaction() as conn:
            account = conn.execute(
                "SELECT enabled FROM core_accounts WHERE id = ?",
                (str(account_id),),
            ).fetchone()
            if account is None or not bool(account["enabled"]):
                return False, "account is missing or disabled"
            book = conn.execute(
                "SELECT metadata_json FROM core_books WHERE id = ?",
                (str(book_id),),
            ).fetchone()
            if book is None:
                return False, "retired book is not persisted"
            book_metadata = _decode(book["metadata_json"])
            if (
                not isinstance(book_metadata, Mapping)
                or book_metadata.get("legacy_import") is not True
                or book_metadata.get("retired") is not True
                or str(book_metadata.get("source_ledger_fingerprint", "")) != baseline.source_ledger_fingerprint
            ):
                return False, "book is not a verified retired import"

            allocation_rows = conn.execute(
                "SELECT * FROM core_position_allocations WHERE account_id = ? AND book_id = ? ORDER BY instrument_id, id",
                (str(account_id), str(book_id)),
            ).fetchall()
            if not allocation_rows:
                return False, "retired book has no durable allocation evidence"
            net_by_instrument: dict[str, Decimal] = {}
            source_intent_ids: set[str] = set()
            for row in allocation_rows:
                if str(row["account_id"]) != str(account_id):
                    return False, "retired allocation belongs to a different account"
                if str(row["ownership_class"]).upper() != OwnershipClass.MANAGED.value:
                    return False, "retired book contains unknown or unmanaged allocation"
                source_intent_id = str(row["source_intent_id"] or "").strip()
                if not source_intent_id or not str(row["strategy_id"] or "").strip():
                    return False, "retired allocation is not fully attributed"
                metadata = _decode(row["metadata_json"])
                if (
                    not isinstance(metadata, Mapping)
                    or metadata.get("legacy_import") is not True
                    or metadata.get("retired") is not True
                    or not str(metadata.get("source_allocation_id", "")).strip()
                ):
                    return False, "retired allocation lacks immutable source provenance"
                try:
                    quantity = Decimal(str(row["signed_quantity"]))
                except (InvalidOperation, TypeError, ValueError):
                    return False, "retired allocation quantity is invalid"
                if not quantity.is_finite():
                    return False, "retired allocation quantity is non-finite"
                instrument_id = str(row["instrument_id"]).strip()
                if not instrument_id:
                    return False, "retired allocation instrument is missing"
                source_intent_ids.add(source_intent_id)
                net_by_instrument[instrument_id] = net_by_instrument.get(instrument_id, Decimal("0")) + quantity
            if any(quantity != 0 for quantity in net_by_instrument.values()):
                return False, "retired book is not flat per instrument"

            intent_rows = conn.execute(
                "SELECT * FROM core_order_intents WHERE account_id = ? AND book_id = ? ORDER BY id",
                (str(account_id), str(book_id)),
            ).fetchall()
            if not intent_rows or {str(row["id"]) for row in intent_rows} != source_intent_ids:
                return False, "retired allocation claims do not exactly match persisted intents"
            for row in intent_rows:
                intent_status = str(row["status"]).upper()
                if intent_status not in terminal_intent_statuses and not (
                    allow_retired_baseline_blockers
                    and intent_status == IntentStatus.RECONCILIATION_REQUIRED.value
                ):
                    return False, "retired book has a non-terminal intent"
                metadata = _decode(row["metadata_json"])
                if (
                    not isinstance(metadata, Mapping)
                    or metadata.get("legacy_import") is not True
                    or metadata.get("retired") is not True
                    or str(metadata.get("source_ledger_fingerprint", "")) != baseline.source_ledger_fingerprint
                ):
                    return False, "retired intent provenance is not covered by the baseline"

            order_rows = conn.execute(
                """SELECT b.*, l.intent_id, l.instrument_id AS leg_instrument_id,
                          l.status AS leg_status, l.quantity AS leg_quantity,
                          l.cumulative_filled_quantity AS leg_filled_quantity
                     FROM core_broker_orders b
                     JOIN core_order_legs l ON l.id = b.order_leg_id
                    WHERE b.account_id = ? AND l.intent_id IN
                          (SELECT id FROM core_order_intents WHERE account_id = ? AND book_id = ?)
                    ORDER BY b.external_order_id, b.id""",
                (str(account_id), str(account_id), str(book_id)),
            ).fetchall()
            if not order_rows:
                return False, "retired book has no durable broker-order claims"
            observed_orders: set[str] = set()
            for row in order_rows:
                external_order_id = str(row["external_order_id"] or "").strip()
                if not external_order_id or external_order_id in observed_orders:
                    return False, "retired book has missing or duplicate broker-order claims"
                observed_orders.add(external_order_id)
                if external_order_id not in expected_orders:
                    return False, "retired broker-order claim is outside the verified baseline"
                if str(row["status"]).upper() not in terminal_order_statuses:
                    return False, "retired book has a non-terminal broker order"
                if str(row["leg_status"]).upper() not in terminal_leg_statuses:
                    return False, "retired book has a non-terminal order leg"
                metadata = _decode(row["metadata_json"])
                if (
                    not isinstance(metadata, Mapping)
                    or metadata.get("legacy_import") is not True
                    or metadata.get("retired") is not True
                    or str(metadata.get("source_ledger_fingerprint", "")) != baseline.source_ledger_fingerprint
                ):
                    return False, "retired broker-order provenance is not covered by the baseline"
                fills = conn.execute(
                    "SELECT * FROM core_fills WHERE broker_order_id = ? ORDER BY id",
                    (str(row["id"]),),
                ).fetchall()
                if str(row["status"]).upper() == BrokerOrderStatus.FILLED.value:
                    if len(fills) != 1:
                        return False, "retired FILLED order lacks exactly one typed fill"
                    fill = fills[0]
                    if str(fill["order_leg_id"]) != str(row["order_leg_id"]):
                        return False, "retired fill is attached to a different order leg"
                    try:
                        submitted = Decimal(str(row["submitted_quantity"]))
                        filled = Decimal(str(fill["quantity"]))
                        price = Decimal(str(fill["price"]))
                        leg_quantity = Decimal(str(row["leg_quantity"]))
                        leg_filled = Decimal(str(row["leg_filled_quantity"]))
                    except (InvalidOperation, TypeError, ValueError):
                        return False, "retired fill economics are invalid"
                    if (
                        not submitted.is_finite()
                        or not filled.is_finite()
                        or filled != submitted
                        or leg_quantity != submitted
                        or not leg_filled.is_finite()
                        or leg_filled != leg_quantity
                        or not price.is_finite()
                        or price <= 0
                    ):
                        return False, "retired fill does not prove a complete order fill"
                    fill_metadata = _decode(fill["metadata_json"])
                    if (
                        not isinstance(fill_metadata, Mapping)
                        or fill_metadata.get("evidence_mode") != ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS.value
                        or not str(fill_metadata.get("evidence_reference", "")).strip()
                    ):
                        return False, "retired fill lacks typed cumulative-order evidence"
                elif fills:
                    return False, "retired non-filled order has unexpected fill evidence"
            if observed_orders != expected_orders:
                return False, "verified baseline order coverage does not match retired claims"

            open_issues = [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM core_reconciliation_issues WHERE account_id = ? AND status = 'OPEN'",
                    (str(account_id),),
                ).fetchall()
            ]
            open_actions = [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM core_recovery_actions WHERE account_id = ? AND status = 'OPEN'",
                    (str(account_id),),
                ).fetchall()
            ]
        if open_issues or open_actions:
            if not allow_retired_baseline_blockers:
                return False, "account has an unresolved reconciliation or recovery blocker"
            # Capacity attribution has a deliberately narrower meaning than
            # operator reconciliation.  Once this book's own account,
            # source-ledger, terminal-order, typed-fill, and per-instrument
            # flatness proof has passed, unrelated current blockers must not
            # make its historical allocations reserve capacity forever.  The
            # caller must still use the normal account-wide broker-fact gate
            # for current positions/orders, and the explicit resolver below
            # retains its strict causal-link checks by leaving ``capacity_only``
            # false.
            if capacity_only:
                return True, "verified retired book is terminal, reconciled, and flat for capacity attribution"
            else:
                allocation_ids = {str(row["id"]) for row in allocation_rows}
                order_row_ids = {str(row["id"]) for row in order_rows}
                intent_ids = {str(row["id"]) for row in intent_rows}
                with self.transaction() as blocker_conn:
                    account_intent_ids = {
                        str(row["id"])
                        for row in blocker_conn.execute(
                            "SELECT id FROM core_order_intents WHERE account_id = ?",
                            (str(account_id),),
                        ).fetchall()
                    }

                def retired_derived_issue_base(row: Mapping[str, Any]) -> bool:
                    category = str(row.get("category", ""))
                    entity_key = str(row.get("entity_key", ""))
                    details = _decode(row.get("details_json", {}))
                    if category == "TERMINAL_HISTORY_EXPIRED":
                        return entity_key in order_row_ids
                    if category == "BOOK_OWNERSHIP_OR_CAPACITY":
                        blockers = details.get("blockers") if isinstance(details, Mapping) else None
                        return bool(blockers) and all(
                            isinstance(item, Mapping)
                            and item.get("kind") == "unknown_book_allocation"
                            and str(item.get("book_id", "")) == str(book_id)
                            and str(item.get("allocation_id", "")) in allocation_ids
                            for item in blockers
                        )
                    return False

                def retired_derived_action_base(row: Mapping[str, Any]) -> bool:
                    action_key = str(row.get("action_key", ""))
                    if action_key.startswith("TERMINAL_HISTORY_EXPIRED:"):
                        return action_key.split(":", 1)[1] in order_row_ids and str(row.get("intent_id", "")) in intent_ids
                    if action_key.startswith("BOOK_RISK_BLOCK:"):
                        metadata = _decode(row.get("metadata_json", {}))
                        blockers = metadata.get("blockers") if isinstance(metadata, Mapping) else None
                        return bool(blockers) and all(
                            isinstance(item, Mapping)
                            and item.get("kind") == "unknown_book_allocation"
                            and str(item.get("book_id", "")) == str(book_id)
                            and str(item.get("allocation_id", "")) in allocation_ids
                            for item in blockers
                        )
                    return False

                direct_issue_links = {
                    (str(row.get("id", "")), str(row.get("issue_key", "")))
                    for row in open_issues
                    if retired_derived_issue_base(row)
                }
                direct_action_links = {
                    (
                        str(row.get("id", "")),
                        str(row.get("intent_id", "")),
                        str(row.get("action_key", "")),
                    )
                    for row in open_actions
                    if retired_derived_action_base(row)
                }

                def linked_account_wrapper(row: Mapping[str, Any], details: object) -> bool:
                    # The causal links are evidence about the wrapper's
                    # *cause*, not a substitute for validating the wrapper
                    # row itself.  A copied details payload must not make a
                    # malformed account issue/action look like a covered
                    # retired-baseline blocker.
                    if str(row.get("category", "")) == "ACCOUNT_RECONCILIATION_BLOCK":
                        if (
                            str(row.get("entity_type", "")) != "ACCOUNT"
                            or str(row.get("entity_key", "")) != str(account_id)
                            or str(row.get("issue_key", ""))
                            != f"ACCOUNT_RECONCILIATION_BLOCK:ACCOUNT:{account_id}"
                        ):
                            return False
                    else:
                        expected_action_key = f"ACCOUNT_RECONCILIATION_BLOCK:ACCOUNT:{account_id}"
                        if str(row.get("action_key", "")) != expected_action_key:
                            return False
                    if not isinstance(details, Mapping):
                        return False
                    if str(details.get("causal_account_id", "")) != str(account_id):
                        return False
                    if str(details.get("causal_baseline_id", "")) != str(baseline.id):
                        return False
                    if str(details.get("causal_source_ledger_fingerprint", "")) != str(baseline.source_ledger_fingerprint):
                        return False
                    issue_links = details.get("causal_reconciliation_issue_links")
                    action_links = details.get("causal_recovery_action_links")
                    causal_intents = details.get("causal_intent_ids")
                    if not isinstance(issue_links, list) or not issue_links:
                        return False
                    if not isinstance(action_links, list) or not action_links:
                        return False
                    if not isinstance(causal_intents, list) or not causal_intents:
                        return False
                    causal_intent_ids = {str(value) for value in causal_intents}
                    if not causal_intent_ids or not causal_intent_ids.issubset(account_intent_ids):
                        return False
                    if str(row.get("account_id", "")) != str(account_id):
                        return False
                    if "action_key" in row:
                        wrapper_intent_id = str(row.get("intent_id", ""))
                        if (
                            not wrapper_intent_id
                            or wrapper_intent_id not in causal_intent_ids
                            or wrapper_intent_id not in intent_ids
                        ):
                            return False
                    normalized_issues: set[tuple[str, str]] = set()
                    for link in issue_links:
                        if not isinstance(link, Mapping):
                            return False
                        normalized_issues.add((str(link.get("id", "")), str(link.get("issue_key", ""))))
                    normalized_actions: set[tuple[str, str, str]] = set()
                    for link in action_links:
                        if not isinstance(link, Mapping):
                            return False
                        normalized_actions.add(
                            (
                                str(link.get("id", "")),
                                str(link.get("intent_id", "")),
                                str(link.get("action_key", "")),
                            )
                        )
                    if not normalized_issues.issubset(direct_issue_links):
                        return False
                    if not normalized_actions.issubset(direct_action_links):
                        return False
                    if not all(intent_id in intent_ids for _, intent_id, _ in normalized_actions):
                        return False
                    if not all(intent_id in causal_intent_ids for _, intent_id, _ in normalized_actions):
                        return False
                    # A wrapper may point at several covered terminal/order
                    # blockers, but it may not point at another account
                    # wrapper.  This explicit one-hop chain prevents cycles
                    # and prevents generic account blockers from being
                    # reclassified by label alone.
                    return bool(normalized_issues and normalized_actions)

                def retired_derived_issue(row: Mapping[str, Any]) -> bool:
                    if retired_derived_issue_base(row):
                        return True
                    if str(row.get("category", "")) == "ACCOUNT_RECONCILIATION_BLOCK":
                        return linked_account_wrapper(row, _decode(row.get("details_json", {})))
                    return False

                def retired_derived_action(row: Mapping[str, Any]) -> bool:
                    if retired_derived_action_base(row):
                        return True
                    if str(row.get("action_key", "")).startswith("ACCOUNT_RECONCILIATION_BLOCK:"):
                        return linked_account_wrapper(row, _decode(row.get("metadata_json", {})))
                    return False

                if not all(retired_derived_issue(row) for row in open_issues):
                    return False, "account has an unrelated reconciliation blocker"
                if not all(retired_derived_action(row) for row in open_actions):
                    return False, "account has an unrelated recovery blocker"
        return True, "verified retired book is terminal, reconciled, and flat"

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

    def resolve_reconciliation_issue(
        self,
        issue_id: str,
        *,
        resolved_at: datetime | None = None,
        _resolution_capability: object | None = None,
    ) -> None:
        if _resolution_capability is not self.__resolution_capability:
            raise PermissionError("validated reconciliation resolution capability is required")
        with self.transaction() as conn:
            row = conn.execute("SELECT id FROM core_reconciliation_issues WHERE id = ?", (issue_id,)).fetchone()
            if row is None:
                raise KeyError(f"Unknown reconciliation issue: {issue_id}")
            conn.execute(
                "UPDATE core_reconciliation_issues SET status = 'RESOLVED', resolved_at = ? WHERE id = ?",
                (_timestamp(resolved_at or utc_now()), issue_id),
            )

    def resolve_reconciliation_issue_by_key(
        self,
        account_id: str,
        issue_key: str,
        *,
        resolved_at: datetime | None = None,
        _resolution_capability: object | None = None,
    ) -> bool:
        """Resolve one known evidence-gap issue after its evidence is durable."""
        if _resolution_capability is not self.__resolution_capability:
            raise PermissionError("validated reconciliation resolution capability is required")
        with self.transaction() as conn:
            cursor = conn.execute(
                """UPDATE core_reconciliation_issues
                   SET status = 'RESOLVED', resolved_at = ?
                   WHERE account_id = ? AND issue_key = ? AND status = 'OPEN'""",
                (_timestamp(resolved_at or utc_now()), account_id, issue_key),
            )
            return cursor.rowcount > 0

    def open_reconciliation_issues(self, account_id: str) -> list[dict[str, Any]]:
        with self.transaction() as conn:
            return [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM core_reconciliation_issues WHERE account_id = ? AND status = 'OPEN' ORDER BY detected_at",
                    (account_id,),
                ).fetchall()
            ]

    def upsert_recovery_action(
        self,
        action: RecoveryAction,
        *,
        _resolution_capability: object | None = None,
    ) -> str:
        """Persist one deterministic operator action without losing history."""
        if (
            action.status is RecoveryActionStatus.RESOLVED
            and _resolution_capability is not self.__resolution_capability
        ):
            raise PermissionError("validated recovery-action resolution capability is required")
        with self.transaction() as conn:
            existing = conn.execute(
                """SELECT id FROM core_recovery_actions
                   WHERE account_id = ? AND intent_id = ? AND action_key = ?""",
                (action.account_id, action.intent_id, action.action_key),
            ).fetchone()
            values = (
                action.state,
                action.summary,
                _json(action.observed_positions),
                _json(action.remaining_quantities),
                int(action.stale),
                int(action.timed_out),
                _json(list(action.allowed_next_steps)),
                action.status.value,
                _timestamp(action.detected_at),
                _timestamp(action.last_seen_at),
                action.resolved_at and _timestamp(action.resolved_at),
                _json(action.metadata),
            )
            if existing:
                conn.execute(
                    """UPDATE core_recovery_actions
                       SET state = ?, summary = ?, observed_positions_json = ?,
                           remaining_quantities_json = ?, stale = ?, timed_out = ?,
                           allowed_next_steps_json = ?, status = ?, last_seen_at = ?,
                           occurrence_count = occurrence_count + 1, resolved_at = ?,
                           metadata_json = ?
                       WHERE id = ?""",
                    (*values[:8], values[9], values[10], values[11], str(existing["id"])),
                )
                return str(existing["id"])
            conn.execute(
                """INSERT INTO core_recovery_actions
                   (id, intent_id, account_id, action_key, state, summary,
                    observed_positions_json, remaining_quantities_json, stale, timed_out,
                    allowed_next_steps_json, status, detected_at, last_seen_at,
                    occurrence_count, resolved_at, metadata_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)""",
                (
                    action.id,
                    action.intent_id,
                    action.account_id,
                    action.action_key,
                    *values,
                ),
            )

            return action.id

    def recovery_actions_for_intent(
        self,
        intent_id: str,
        *,
        status: RecoveryActionStatus | None = None,
    ) -> list[dict[str, Any]]:
        with self.transaction() as conn:
            query = "SELECT * FROM core_recovery_actions WHERE intent_id = ?"
            params: list[Any] = [intent_id]
            if status is not None:
                query += " AND status = ?"
                params.append(status.value)
            query += " ORDER BY detected_at, action_key"
            rows = conn.execute(query, params).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["observed_positions"] = _decode(item.pop("observed_positions_json"))
            item["remaining_quantities"] = _decode(item.pop("remaining_quantities_json"))
            item["allowed_next_steps"] = _decode(item.pop("allowed_next_steps_json"))
            item["metadata"] = _decode(item.pop("metadata_json"))
            result.append(item)
        return result

    def open_recovery_actions(self, account_id: str) -> list[dict[str, Any]]:
        with self.transaction() as conn:
            rows = conn.execute(
                """SELECT * FROM core_recovery_actions
                   WHERE account_id = ? AND status = 'OPEN'
                   ORDER BY last_seen_at, action_key""",
                (account_id,),
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["observed_positions"] = _decode(item.pop("observed_positions_json"))
            item["remaining_quantities"] = _decode(item.pop("remaining_quantities_json"))
            item["allowed_next_steps"] = _decode(item.pop("allowed_next_steps_json"))
            item["metadata"] = _decode(item.pop("metadata_json"))
            result.append(item)
        return result

    def record_operational_event(
        self,
        *,
        event_id: str,
        account_id: str,
        event_type: str,
        mode: str,
        outcome: str,
        occurred_at: datetime,
        summary: str,
        details: Mapping[str, Any] | None = None,
    ) -> str:
        """Persist one local operator/service event without touching the ledger."""
        if not str(event_id).strip():
            raise ValueError("event_id is required")
        if not str(account_id).strip():
            raise ValueError("account_id is required")
        if not str(event_type).strip() or not str(mode).strip() or not str(outcome).strip():
            raise ValueError("event_type, mode, and outcome are required")
        if not str(summary).strip():
            raise ValueError("summary is required")
        with self.transaction() as conn:
            account = conn.execute(
                "SELECT 1 FROM core_accounts WHERE id = ?",
                (str(account_id),),
            ).fetchone()
            if account is None:
                raise ValueError(f"unknown account for operational event: {account_id}")
            conn.execute(
                """INSERT INTO core_operational_events
                   (id, account_id, event_type, mode, outcome, occurred_at, summary, details_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    str(event_id),
                    str(account_id),
                    str(event_type),
                    str(mode),
                    str(outcome),
                    _timestamp(occurred_at),
                    str(summary),
                    _json(_redact_operational_value(details or {})),
                ),
            )
        return str(event_id)

    def operational_events(
        self,
        account_id: str,
        *,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Return the newest local operational events for an account."""
        if isinstance(limit, bool) or int(limit) <= 0:
            raise ValueError("limit must be positive")
        with self.transaction() as conn:
            rows = conn.execute(
                """SELECT * FROM core_operational_events
                   WHERE account_id = ?
                   ORDER BY occurred_at DESC, id DESC
                   LIMIT ?""",
                (str(account_id), int(limit)),
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["details"] = _decode(item.pop("details_json"))
            result.append(item)
        return result

    def resolve_recovery_action_by_key(
        self,
        account_id: str,
        intent_id: str,
        action_key: str,
        *,
        resolved_at: datetime | None = None,
        _resolution_capability: object | None = None,
    ) -> bool:
        if _resolution_capability is not self.__resolution_capability:
            raise PermissionError("validated recovery-action resolution capability is required")
        with self.transaction() as conn:
            cursor = conn.execute(
                """UPDATE core_recovery_actions
                   SET status = 'RESOLVED', resolved_at = ?
                   WHERE account_id = ? AND intent_id = ? AND action_key = ? AND status = 'OPEN'""",
                (_timestamp(resolved_at or utc_now()), account_id, intent_id, action_key),
            )
            return cursor.rowcount > 0

    @staticmethod
    def new_id() -> str:
        return str(uuid.uuid4())
