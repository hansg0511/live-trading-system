"""Fake-testable broker-neutral order-management coordinator."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import sqlite3
from typing import Callable, Sequence
import uuid

from .domain import (
    Account,
    ADAPTER_ORDER_SNAPSHOT_AUTHORITY,
    ADAPTER_SUBMISSION_ACK_AUTHORITY,
    BrokerOrderEvent,
    BrokerOrderSnapshot,
    BrokerOrderStatus,
    FailurePolicy,
    Fill,
    IntentAction,
    IntentStatus,
    IssueSeverity,
    IssueStatus,
    LegStatus,
    OrderIntent,
    OrderLeg,
    ReconciliationIssue,
    ReconciliationRun,
    ReconciliationStatus,
    RecoveryAction,
    RecoveryActionStatus,
    RiskDecisionRecord,
    ExecutionPolicy,
    ExecutionEvidenceMode,
    PositionSnapshot,
    OwnershipClass,
    Side,
)
from .ports import (
    BrokerAdapter,
    BrokerFactSnapshot,
    BrokerFill,
    BrokerHistoricalOrderFacts,
    BrokerSubmissionResult,
    BrokerSubmitRequest,
)
from .provider_payload import (
    ProviderPayloadError,
    coerce_provider_payload,
    is_execution_evidence_key,
    normalize_provider_key,
)
from .repository import SQLiteTradingRepository


class OMSExecutionError(RuntimeError):
    pass


class GenericOMS:
    """Coordinates durable logical intents without importing an adapter SDK."""

    TERMINAL_RECOVERY_WINDOW_SECONDS = 86_400
    # New fingerprints are stored in dedicated repository columns.  These
    # names remain readable only for exact pre-migration replay compatibility;
    # no new provider metadata is written under this namespace.
    _EVENT_EVIDENCE_ENVELOPE_KEY = "_oms_core_evidence_envelope"
    _LEGACY_FILL_FINGERPRINT_KEY = "_fill_evidence_fingerprint"
    _LEGACY_EVENT_FINGERPRINT_KEY = "_event_evidence_fingerprint"
    _BROKER_FILL_ACCOUNT_ID_KEY = "_broker_fill_account_id"
    _UNKNOWN_BOOK_REQUEST_KEY = "_stage4_requested_book_id"
    # Provider clocks can differ by a few seconds, but a broker fact that
    # predates the durable submit evidence by more than this tolerance cannot
    # safely describe the current attempt.
    BROKER_TIME_ORDER_TOLERANCE_SECONDS = 5
    # A provider captures a fact during the query, so its timestamp can
    # naturally be a few milliseconds after the OMS clock observed the query
    # boundary.  A materially future timestamp is still impossible evidence.
    BROKER_CLOCK_SKEW_TOLERANCE_SECONDS = 5
    # Residual compensation is an order-bearing safety route, so a successful
    # account-facts query must describe the current account rather than a
    # cached/restarted observation.
    BROKER_FACT_MAX_AGE_SECONDS = 60

    def __init__(
        self,
        repository: SQLiteTradingRepository,
        adapter: BrokerAdapter,
        *,
        clock: Callable[[], datetime] | None = None,
    ):
        self.repository = repository
        self.adapter = adapter
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        # A normalized push event is not itself trusted fill evidence.  This
        # capability is kept on the OMS instance so ordinary callers cannot
        # pass arbitrary fills through the public event method and allocate
        # position quantity.  Adapter bridges use the private helper below;
        # the public route performs an authoritative fill lookup instead.
        self.__broker_event_validation_capability = object()
        # A compensating exit is a narrowly proof-gated, risk-reducing path.
        # Keep its capability instance-scoped so ordinary submit callers
        # cannot opt out of account-wide safety gates by supplying metadata or
        # a public boolean flag.  The method that owns this token validates
        # fresh broker facts and exact source-order evidence first.
        self.__verified_compensating_exit_capability = object()
        # Recovery keeps one observed position view for the current poll
        # cycle.  In particular, a provider rate-limit failure must not
        # trigger a second position query merely to populate an error action.
        self._last_observed_positions: dict[str, str] = {}
        self._last_broker_read_retryable = False
        # A recovery cycle may only use the one strict, fresh account-fact
        # snapshot validated at its boundary.  Keep it private and scoped to
        # the current call so nested recovery helpers cannot fall back to a
        # cacheable position/fill view after the safety gate has passed.
        self._recovery_fact_context: dict[str, object] | None = None

    def _now(self) -> datetime:
        now = self.clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("OMS clock must return a timezone-aware datetime")
        return now.astimezone(timezone.utc)

    def _startup_safety_audit(self) -> None:
        """Re-run additive legacy audits before every public safety route."""
        self.repository.audit_legacy_duplicate_fills()
        self.repository.audit_legacy_duplicate_broker_orders()

    @staticmethod
    def _account_contract_mismatches(canonical: Account, supplied: Account | None) -> list[str]:
        """Compare every persisted account identity component at the OMS boundary."""
        if supplied is None:
            return []
        mismatches: list[str] = []
        if supplied.id != canonical.id:
            mismatches.append(f"internal_id:{supplied.id}")
        if supplied.broker != canonical.broker:
            mismatches.append(f"broker:{supplied.broker}")
        if supplied.environment is not canonical.environment:
            mismatches.append(f"environment:{supplied.environment.value}")
        if supplied.external_account_id != canonical.external_account_id:
            mismatches.append(f"external_account_id:{supplied.external_account_id}")
        if supplied.enabled is not canonical.enabled:
            mismatches.append(f"enabled:{supplied.enabled}")
        return mismatches

    def _canonical_account_for_intent(
        self,
        intent: Mapping[str, object],
        *,
        supplied: Account | None,
        source: str,
    ) -> Account | None:
        """Load and validate the persisted account before any OMS mutation."""
        expected_id = str(intent["account_id"])
        canonical = self.repository.get_account(expected_id)
        mismatches: list[str] = []
        if canonical is None:
            mismatches.append("persisted_account_missing")
        else:
            mismatches.extend(self._account_contract_mismatches(canonical, supplied))
            if not canonical.enabled:
                mismatches.append("enabled:False")
        if not mismatches:
            return canonical

        details = {
            "intent_id": str(intent["id"]),
            "source": source,
            "expected_internal_id": expected_id,
            "expected_broker": canonical.broker if canonical is not None else None,
            "expected_environment": canonical.environment.value if canonical is not None else None,
            "expected_external_account_id": canonical.external_account_id if canonical is not None else None,
            "expected_enabled": canonical.enabled if canonical is not None else None,
            "supplied_internal_id": supplied.id if supplied is not None else None,
            "supplied_broker": supplied.broker if supplied is not None else None,
            "supplied_environment": supplied.environment.value if supplied is not None else None,
            "supplied_external_account_id": supplied.external_account_id if supplied is not None else None,
            "supplied_enabled": supplied.enabled if supplied is not None else None,
            "mismatches": mismatches,
        }
        # A corrupt database can lack the account row required by the foreign
        # keys on issue/action tables.  Keep the public route fail-closed even
        # in that case; normal disabled/identity mismatches are durable.
        if canonical is not None and self.repository.get_intent(str(intent["id"])) is not None:
            self._require_reconciliation_for_account_id(
                str(intent["id"]),
                expected_id,
                category="ACCOUNT_CANONICAL_IDENTITY_MISMATCH",
                entity_type="ACCOUNT",
                entity_key=expected_id,
                details=details,
            )
            self._record_recovery_action_for_account_id(
                intent=intent,
                account_id=expected_id,
                action_key=f"ACCOUNT_CANONICAL_IDENTITY_MISMATCH:{source}",
                state="RECONCILIATION_REQUIRED",
                summary="The supplied or persisted account identity is not an enabled canonical account; no broker facts or lifecycle mutation is allowed.",
                observed_positions={"_status": "ACCOUNT_CANONICAL_IDENTITY_MISMATCH", **details},
                remaining_quantities=self._remaining_quantities(intent),
                allowed_next_steps=("use_persisted_account", "reconcile_account_identity", "operator_review"),
                metadata=details,
            )
        return None

    def _canonical_account_for_boundary(self, account: Account, *, source: str) -> Account:
        """Validate a multi-intent route whose account is supplied directly."""
        canonical = self.repository.get_account(account.id)
        if canonical is None:
            raise OMSExecutionError(f"no persisted canonical account for {source}: {account.id}")
        mismatches = self._account_contract_mismatches(canonical, account)
        if not canonical.enabled:
            mismatches.append("enabled:False")
        if mismatches:
            raise OMSExecutionError(
                f"account identity is not an enabled canonical account for {source}: {mismatches}"
            )
        return canonical

    def _validate_persisted_execution_policy(
        self,
        intent: Mapping[str, object],
        account: Account,
        *,
        source: str,
    ) -> bool:
        """Reconstruct the policy before any recovery/lifecycle mutation."""
        raw_policy = intent.get("execution_policy")
        try:
            if not isinstance(raw_policy, Mapping):
                raise ValueError("persisted execution policy is not a mapping")
            # Construction is deliberately the canonical validator.  It
            # rejects every unsupported Stage 3 mode instead of letting a
            # hand-edited/legacy JSON row silently use a dangerous default.
            normalized_policy = dict(raw_policy)
            # Very early Stage 3 rows were written with ``default=str`` for
            # enum values.  Preserve that narrow, unambiguous compatibility
            # while still rejecting every other malformed/unsupported value.
            legacy_enum_prefixes = {
                "legging_policy": "LeggingPolicy.",
                "partial_fill_policy": "PartialFillPolicy.",
                "failure_policy": "FailurePolicy.",
                "execution_session": "ExecutionSession.",
            }
            for key, prefix in legacy_enum_prefixes.items():
                value = normalized_policy.get(key)
                if isinstance(value, str) and "." in value:
                    if not value.startswith(prefix):
                        raise ValueError(f"invalid legacy persisted {key} value")
                    normalized_policy[key] = value[len(prefix):]
            if normalized_policy.get("required_capabilities") == "frozenset()":
                normalized_policy["required_capabilities"] = []
            ExecutionPolicy(**normalized_policy)
        except Exception as exc:
            details = {
                "intent_id": str(intent["id"]),
                "source": source,
                "error": str(exc),
                "execution_policy": self._safe_provider_payload(raw_policy),
            }
            self._require_reconciliation(
                str(intent["id"]),
                account,
                category="INVALID_PERSISTED_EXECUTION_POLICY",
                entity_type="INTENT",
                entity_key=str(intent["id"]),
                details=details,
            )
            self._record_recovery_action(
                intent=intent,
                account=account,
                action_key=f"INVALID_EXECUTION_POLICY:{source}",
                state="RECONCILIATION_REQUIRED",
                summary="Persisted execution policy is invalid or unsupported; no recovery, completion, or submission is allowed.",
                observed_positions={"_status": "INVALID_EXECUTION_POLICY"},
                remaining_quantities=self._remaining_quantities(intent),
                allowed_next_steps=("reconcile_execution_policy", "reconcile_broker_order", "operator_review"),
                metadata=details,
            )
            return False
        return True

    @staticmethod
    def _book_exposure_value(value: object, *, label: str) -> Decimal:
        """Parse a finite non-negative generic exposure value."""
        try:
            parsed = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError(f"{label} is not a valid Decimal") from exc
        if not parsed.is_finite() or parsed < 0:
            raise ValueError(f"{label} must be finite and non-negative")
        return parsed

    @classmethod
    def _book_limit_basis(cls, row: Mapping[str, object]) -> tuple[str, Decimal]:
        configured = []
        for key in ("capital_fraction", "capital_amount", "risk_budget"):
            value = row.get(key)
            if value is not None and str(value).strip() != "":
                configured.append((key, cls._book_exposure_value(value, label=key)))
        if len(configured) != 1:
            raise ValueError("book allocation has no single supported limit basis")
        return configured[0]

    @staticmethod
    def _book_capacity_unit(row: Mapping[str, object], basis: str) -> str:
        metadata = row.get("metadata")
        declared = (
            metadata.get("capacity_unit", metadata.get("exposure_unit"))
            if isinstance(metadata, Mapping)
            else None
        )
        unit = str(declared).strip().upper() if declared not in (None, "") else basis.upper()
        if not unit:
            raise ValueError("book allocation capacity unit is empty")
        return unit

    @classmethod
    def _book_limit(
        cls,
        rows: Sequence[Mapping[str, object]],
        account: Account,
    ) -> tuple[str, Decimal]:
        """Aggregate one book/account's declarations without mixing units."""
        if not rows:
            raise ValueError("no active book allocation exists")
        bases = [cls._book_limit_basis(row) for row in rows]
        names = {name for name, _ in bases}
        if len(names) != 1:
            raise ValueError("book allocations use mixed capacity units")
        basis = bases[0][0]
        units = {cls._book_capacity_unit(row, basis) for row in rows}
        if len(units) != 1:
            raise ValueError("book allocations use mixed exposure units")
        amount = sum((value for _, value in bases), Decimal("0"))
        if basis == "capital_fraction":
            # Fractions are relative to an account-declared generic capacity.
            # Without that anchor a fraction cannot be compared to an order's
            # exposure units, so the safe result is a durable configuration
            # blocker rather than an implicit dollar/quantity conversion.
            metadata = account.metadata
            capacity_value = next(
                (
                    metadata[key]
                    for key in (
                        "allocation_capacity",
                        "capital_capacity",
                        "book_capacity",
                        "aggregate_capacity",
                    )
                    if key in metadata
                ),
                None,
            )
            if capacity_value is None:
                raise ValueError("capital_fraction requires an account allocation capacity")
            capacity = cls._book_exposure_value(capacity_value, label="account allocation capacity")
            amount *= capacity
        return basis, amount

    @classmethod
    def _requested_book_exposure(cls, intent: Mapping[str, object]) -> Decimal:
        metadata = intent.get("metadata")
        if isinstance(metadata, Mapping):
            for key in ("estimated_exposure", "estimated_capital", "notional"):
                if key in metadata:
                    return cls._book_exposure_value(metadata[key], label=key)
        total = Decimal("0")
        for leg in intent.get("legs", ()):
            try:
                quantity = cls._book_exposure_value(leg["quantity"], label="leg quantity")
            except (KeyError, TypeError) as exc:
                raise ValueError("book exposure requires valid leg quantities") from exc
            total += quantity
        return total

    def _book_guard(self, intent_id: str, account: Account) -> bool:
        """Enforce explicit Stage 4 ownership and shared capacity limits.

        Accounts with no declared ``BookAllocation`` retain the pre-Stage-4
        generic behavior for compatibility.  Once a book is declared, a
        missing/disabled/expired book is explicit UNKNOWN ownership and is a
        sticky account blocker; it is never netted into another book.
        """
        intent = self._required_intent(intent_id)
        all_rows = self.repository.book_allocations(account.id, active_only=False)
        metadata = intent.get("metadata")
        requested_unknown_book = (
            metadata.get(self._UNKNOWN_BOOK_REQUEST_KEY)
            if isinstance(metadata, Mapping)
            else None
        )
        if (
            not all_rows
            and requested_unknown_book in (None, "")
            and intent.get("book_id") in (None, "")
        ):
            return True

        active_rows = self.repository.book_allocations(account.id, active_only=True)
        blockers: list[dict[str, object]] = []
        requested_book_id = requested_unknown_book or intent.get("book_id")
        if requested_book_id in (None, ""):
            blockers.append(
                {
                    "kind": "unknown_book_ownership",
                    "intent_id": intent_id,
                    "reason": "risk-bearing intent has no declared book",
                }
            )
        elif requested_unknown_book not in (None, ""):
            blockers.append(
                {
                    "kind": "unknown_book_ownership",
                    "intent_id": intent_id,
                    "book_id": str(requested_unknown_book),
                    "reason": "requested book is not declared",
                }
            )
        matching_rows = [
            row
            for row in active_rows
            if str(row.get("book_id")) == str(requested_book_id)
            and str(row.get("strategy_id")) == str(intent.get("strategy_id"))
            and bool(row.get("book_enabled"))
            and bool(row.get("strategy_enabled"))
        ]
        if requested_book_id not in (None, "") and not matching_rows:
            blockers.append(
                {
                    "kind": "book_allocation_missing_or_disabled",
                    "book_id": str(requested_book_id),
                    "strategy_id": str(intent.get("strategy_id")),
                }
            )

        # Existing ledger rows are the durable attribution of positions.  A
        # nonzero row without a valid known book is UNKNOWN exposure and must
        # stop all new risk-bearing allocations on the shared account.
        try:
            verified_retired_books = self._verified_retired_baseline_books(account)
            closed_historical_intents = self._closed_historical_intent_ids(account)
            for row in self.repository.position_allocations(account.id):
                if str(row.get("source_intent_id") or "") in closed_historical_intents:
                    continue
                quantity = self._book_exposure_value(
                    abs(Decimal(str(row.get("signed_quantity", "0")))),
                    label="position allocation quantity",
                )
                if quantity == 0:
                    continue
                ownership = str(row.get("ownership_class", "")).upper()
                row_book_id = row.get("book_id")
                known = any(
                    str(candidate.get("book_id")) == str(row_book_id)
                    and bool(candidate.get("book_enabled"))
                    and bool(candidate.get("strategy_enabled"))
                    for candidate in active_rows
                )
                if ownership != OwnershipClass.MANAGED.value or not row_book_id or not known:
                    if str(row_book_id or "") in verified_retired_books:
                        continue
                    blockers.append(
                        {
                            "kind": "unknown_book_allocation",
                            "allocation_id": str(row.get("id", "")),
                            "book_id": row_book_id,
                            "ownership_class": ownership,
                            "signed_quantity": str(row.get("signed_quantity", "0")),
                        }
                    )
        except (InvalidOperation, TypeError, ValueError) as exc:
            blockers.append({"kind": "book_allocation_evidence_invalid", "error": str(exc)})

        exposure_by_book: dict[str, Decimal] = {}
        limits_by_book: dict[str, Decimal] = {}
        limit_bases: set[str] = set()
        limit_units: set[str] = set()
        grouped: dict[str, list[Mapping[str, object]]] = {}
        for row in active_rows:
            grouped.setdefault(str(row["book_id"]), []).append(row)
        for book_id, rows in grouped.items():
            try:
                basis, limit = self._book_limit(rows, account)
                limit_bases.add(basis)
                limit_units.update(self._book_capacity_unit(row, basis) for row in rows)
                limits_by_book[book_id] = limit
                signed = self.repository.book_signed_exposure(
                    account.id,
                    book_id,
                    exclude_intent_id=intent_id,
                    exclude_intent_ids=closed_historical_intents,
                )
                if book_id == str(requested_book_id):
                    for leg in intent.get("legs", ()):
                        quantity = self._book_exposure_value(leg["quantity"], label="leg quantity")
                        instrument_id = str(leg["instrument_id"])
                        delta = quantity if str(leg["side"]).upper() == "BUY" else -quantity
                        signed[instrument_id] = signed.get(instrument_id, Decimal("0")) + delta
                exposure_by_book[book_id] = sum((abs(value) for value in signed.values()), Decimal("0"))
            except (KeyError, InvalidOperation, TypeError, ValueError, sqlite3.Error) as exc:
                blockers.append({"kind": "book_capacity_configuration", "book_id": book_id, "error": str(exc)})

        if len(limit_bases) > 1:
            # Risk units, capital amounts, and fractions are not implicitly
            # interchangeable.  A requested book may have room in its own
            # unit while the account aggregate is otherwise unknowable, so
            # persist an account-level configuration blocker instead of
            # silently skipping the aggregate guard.
            blockers.append(
                {
                    "kind": "book_capacity_configuration",
                    "scope": "ACCOUNT",
                    "error": "declared books use incompatible capacity bases",
                    "bases": sorted(limit_bases),
                }
            )
        if len(limit_units) > 1:
            blockers.append(
                {
                    "kind": "book_capacity_configuration",
                    "scope": "ACCOUNT",
                    "error": "declared books use incompatible exposure units",
                    "units": sorted(limit_units),
                }
            )

        if matching_rows and str(requested_book_id) in limits_by_book:
            book_exposure = exposure_by_book.get(str(requested_book_id), Decimal("0"))
            book_limit = limits_by_book[str(requested_book_id)]
            if book_exposure > book_limit:
                blockers.append(
                    {
                        "kind": "book_capacity_exceeded",
                        "book_id": str(requested_book_id),
                        "exposure": str(book_exposure),
                        "limit": str(book_limit),
                    }
                )

        if len(limit_bases) == 1 and len(limits_by_book) == len(grouped):
            account_metadata = account.metadata
            account_capacity_value = next(
                (
                    account_metadata[key]
                    for key in (
                        "account_capacity",
                        "aggregate_risk_budget",
                        "account_risk_budget",
                        "max_gross_exposure",
                    )
                    if key in account_metadata
                ),
                None,
            )
            account_limit = (
                self._book_exposure_value(account_capacity_value, label="account capacity")
                if account_capacity_value is not None
                else sum(limits_by_book.values(), Decimal("0"))
            )
            account_exposure = sum(exposure_by_book.values(), Decimal("0"))
            if account_exposure > account_limit:
                blockers.append(
                    {
                        "kind": "account_capacity_exceeded",
                        "exposure": str(account_exposure),
                        "limit": str(account_limit),
                        "basis": next(iter(limit_bases)),
                    }
                )

        if not blockers:
            return True
        try:
            requested_exposure = str(self._requested_book_exposure(intent))
        except (KeyError, InvalidOperation, TypeError, ValueError) as exc:
            requested_exposure = "unknown"
            blockers.append({"kind": "book_request_exposure_invalid", "error": str(exc)})
        details = {
            "intent_id": intent_id,
            "account_id": account.id,
            "book_id": requested_book_id,
            "requested_exposure": requested_exposure,
            "exposure_by_book": {key: str(value) for key, value in exposure_by_book.items()},
            "limits_by_book": {key: str(value) for key, value in limits_by_book.items()},
            "blockers": blockers,
        }
        self._require_reconciliation(
            intent_id,
            account,
            category="BOOK_OWNERSHIP_OR_CAPACITY",
            entity_type="ACCOUNT",
            entity_key=account.id,
            details=details,
        )
        self._record_recovery_action(
            intent=self._required_intent(intent_id),
            account=account,
            action_key=f"BOOK_RISK_BLOCK:{account.id}:{intent_id}",
            state="RECONCILIATION_REQUIRED",
            summary="Book ownership or shared account capacity is unknown/exceeded; no risk-bearing submission is safe.",
            observed_positions={"_status": "BOOK_RISK_BLOCKED", "exposure_by_book": details["exposure_by_book"]},
            remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
            allowed_next_steps=("register_book_allocation", "reconcile_unknown_exposure", "refresh_broker_facts", "operator_review"),
            metadata=details,
        )
        return False

    def _book_claim_is_known(self, intent: Mapping[str, object], account: Account) -> bool:
        """Check attribution of an existing broker order/fill claim."""
        if not self.repository.book_mode_active(account.id):
            return True
        if str(intent.get("id", "")) in self._closed_historical_intent_ids(account):
            return True
        book_id = intent.get("book_id")
        if book_id in (None, ""):
            return False
        rows = self.repository.book_allocations(
            account.id,
            book_id=str(book_id),
            strategy_id=str(intent.get("strategy_id")),
            active_only=False,
        )
        return any(bool(row.get("book_enabled")) and str(row.get("book_id")) == str(book_id) for row in rows)

    def book_risk_status(self, *, account: Account) -> dict[str, object]:
        """Return durable book attribution/capacity state without broker calls."""
        account = self._canonical_account_for_boundary(account, source="book_risk_status")
        rows = self.repository.book_allocations(account.id, active_only=True)
        grouped: dict[str, list[Mapping[str, object]]] = {}
        for row in rows:
            grouped.setdefault(str(row["book_id"]), []).append(row)
        books: dict[str, object] = {}
        for book_id, declarations in grouped.items():
            try:
                basis, limit = self._book_limit(declarations, account)
                signed = self.repository.book_signed_exposure(account.id, book_id)
                exposure = sum((abs(value) for value in signed.values()), Decimal("0"))
                books[book_id] = {
                    "basis": basis,
                    "limit": str(limit),
                    "exposure": str(exposure),
                    "remaining": str(max(Decimal("0"), limit - exposure)),
                    "allocations": declarations,
                }
            except (InvalidOperation, TypeError, ValueError, sqlite3.Error) as exc:
                books[book_id] = {"status": "BLOCKED", "error": str(exc), "allocations": declarations}
        unknown = [
            row
            for row in self.repository.position_allocations(account.id)
            if str(row.get("ownership_class", "")).upper() != OwnershipClass.MANAGED.value
            and Decimal(str(row.get("signed_quantity", "0"))) != 0
        ]
        return {
            "account_id": account.id,
            "book_mode_active": self.repository.book_mode_active(account.id),
            "books": books,
            "unknown_allocations": unknown,
            "account_limit": str(
                sum(
                    (
                        Decimal(str(item["limit"]))
                        for item in books.values()
                        if isinstance(item, Mapping) and "limit" in item
                    ),
                    Decimal("0"),
                )
            ),
        }

    def submit_intent(
        self,
        intent: OrderIntent,
        *,
        account: Account,
        risk_decision: RiskDecisionRecord,
        _internal_capability: object | None = None,
        _before_submit_leg: Callable[[OrderIntent, OrderLeg, Account], None] | None = None,
    ) -> dict:
        self._startup_safety_audit()
        verified_compensating_exit = (
            _internal_capability is self.__verified_compensating_exit_capability
        )
        if intent.account_id != account.id:
            raise ValueError("intent and account identity must match")
        if risk_decision.intent_id != intent.id:
            raise ValueError("risk decision and intent identity must match")

        existing_prior = self.repository.get_intent_by_idempotency_key(
            intent.account_id,
            intent.idempotency_key,
        )
        validation_record: Mapping[str, object] = existing_prior or {
            "id": intent.id,
            "account_id": intent.account_id,
            "legs": tuple(
                {
                    "id": leg.id,
                    "quantity": leg.quantity,
                    "status": leg.status.value,
                    "cumulative_filled_quantity": Decimal("0"),
                }
                for leg in intent.legs
            ),
        }
        canonical = self._canonical_account_for_intent(
            validation_record,
            supplied=account,
            source="submit_intent",
        )
        if canonical is None:
            if existing_prior is not None:
                return existing_prior
            raise OMSExecutionError("submit_intent requires an enabled persisted canonical account")
        account = canonical
        if existing_prior is not None and not self._validate_persisted_execution_policy(
            existing_prior,
            account,
            source="submit_intent",
        ):
            return self._required_intent(str(existing_prior["id"]))

        # ``core_order_intents.book_id`` is a foreign key.  Preserve an
        # explicitly requested but unknown book as durable UNKNOWN ownership
        # metadata so the normal Stage 4 blocker path can quarantine it,
        # rather than leaking a raw SQLite constraint error or submitting it
        # as an unowned legacy intent.
        persist_intent = intent
        if intent.book_id is not None and self.repository.get_book(intent.book_id) is None:
            persist_intent = replace(
                intent,
                book_id=None,
                metadata={
                    **dict(intent.metadata),
                    self._UNKNOWN_BOOK_REQUEST_KEY: intent.book_id,
                },
            )
        intent_id, created = self.repository.create_intent(persist_intent)
        existing = self._required_intent(intent_id)
        if not self._validate_persisted_execution_policy(existing, account, source="submit_intent"):
            return existing
        if not verified_compensating_exit:
            self._ensure_terminal_history_safety(account)
            self._quarantine_multiple_attempts(intent_id, account)
            # Query account-wide broker truth before any leg can reach the
            # adapter.  Unknown historical exposure is a durable blocker,
            # not a reason to assume the new intent is independent.
            self._account_wide_broker_fact_gate(intent_id, account)
            # Stage 4 ownership and capacity is account-wide.  It runs after
            # the fresh broker-fact gate but before any risk-approved leg can
            # invoke the adapter, and it is a no-op for accounts with no
            # declarations.
            book_guard_passed = self._book_guard(intent_id, account)
        else:
            # submit_verified_compensating_exit has already performed the
            # fresh account-fact, source-attempt, position, and ownership
            # proof.  Re-running the ordinary account-wide admission gates
            # would treat historical retired claims as new risk and could
            # prevent the only safe action: reducing the verified exposure.
            # No ordinary caller can reach this branch without the private
            # instance capability above.
            book_guard_passed = True
        # Preserve the established Stage 3 rejection contract for generic
        # account-fact blockers.  Stage 4 book failures are explicitly
        # reconciliation states, so refresh only for that new guard.
        if not book_guard_passed:
            existing = self._required_intent(intent_id)
        resumable = {
            IntentStatus.CREATED.value,
            IntentStatus.RISK_APPROVED.value,
            IntentStatus.SUBMITTING.value,
        }
        if not created and existing["status"] not in resumable:
            return existing

        blockers = self.repository.open_reconciliation_issues(account.id)
        open_actions = self.repository.open_recovery_actions(account.id)
        if (blockers or open_actions) and not verified_compensating_exit:
            blocked_decision = replace(
                risk_decision,
                id=f"{risk_decision.id}:reconciliation-block",
                approved=False,
                reason=(
                    f"blocked by {len(blockers)} open reconciliation issue(s) and "
                    f"{len(open_actions)} open recovery action(s)"
                ),
            )
            self.repository.save_risk_decision(blocked_decision)
            if existing["status"] == IntentStatus.CREATED.value:
                self.repository.transition_intent(intent_id, IntentStatus.REJECTED, now=self._now())
            else:
                baseline = self.repository.latest_execution_evidence_baseline(account.id)
                causal_issue_links = [
                    {
                        "id": str(issue["id"]),
                        "issue_key": str(issue["issue_key"]),
                    }
                    for issue in blockers
                ]
                causal_action_links = [
                    {
                        "id": str(action["id"]),
                        "intent_id": str(action["intent_id"]),
                        "action_key": str(action["action_key"]),
                    }
                    for action in open_actions
                ]
                self._require_reconciliation(
                    intent_id,
                    account,
                    category="ACCOUNT_RECONCILIATION_BLOCK",
                    entity_type="ACCOUNT",
                    entity_key=account.id,
                    details={
                        "open_issue_count": len(blockers),
                        "open_recovery_action_count": len(open_actions),
                        # Account-wide wrappers are only eligible for the
                        # retired-baseline exception when they retain the
                        # exact causal chain that produced them.  Missing or
                        # generic links remain sticky blockers.
                        "causal_account_id": account.id,
                        "causal_intent_ids": sorted(
                            {intent_id, *(str(action["intent_id"]) for action in open_actions)}
                        ),
                        "causal_reconciliation_issue_links": causal_issue_links,
                        "causal_recovery_action_links": causal_action_links,
                        "causal_baseline_id": getattr(baseline, "id", None),
                        "causal_source_ledger_fingerprint": (
                            getattr(baseline, "source_ledger_fingerprint", None)
                        ),
                    },
                )
            return self._required_intent(intent_id)

        if existing["status"] == IntentStatus.CREATED.value:
            self.repository.save_risk_decision(risk_decision)
            if not risk_decision.approved:
                self.repository.transition_intent(intent_id, IntentStatus.REJECTED, now=self._now())
                return self._required_intent(intent_id)
            self.repository.transition_intent(intent_id, IntentStatus.RISK_APPROVED, now=self._now())
            existing = self._required_intent(intent_id)
        if existing["status"] == IntentStatus.RISK_APPROVED.value:
            self.repository.transition_intent(intent_id, IntentStatus.SUBMITTING, now=self._now())

        accepted_any = False
        for leg in intent.legs:
            stored = {item["id"]: item for item in self._required_intent(intent_id)["legs"]}[leg.id]
            if stored["status"] in {
                LegStatus.WORKING.value,
                LegStatus.PARTIALLY_FILLED.value,
                LegStatus.FILLED.value,
            }:
                accepted_any = True
                continue
            if stored["status"] in {
                LegStatus.REJECTED.value,
                LegStatus.FAILED.value,
                LegStatus.CANCELLED.value,
                LegStatus.RECONCILIATION_REQUIRED.value,
            }:
                self._require_reconciliation(
                    intent_id,
                    account,
                    category="INCOMPLETE_INTENT",
                    entity_type="ORDER_LEG",
                    entity_key=leg.id,
                    details={"status": stored["status"]},
                )
                return self._required_intent(intent_id)

            if _before_submit_leg is not None:
                try:
                    # Stage-specific admission hooks are called immediately
                    # before each leg can invoke the adapter.  Generic OMS
                    # callers do not provide a hook, preserving their path.
                    _before_submit_leg(intent, leg, account)
                except Exception as exc:
                    if accepted_any:
                        self._require_reconciliation(
                            intent_id,
                            account,
                            category="STAGE6_SUBMISSION_GATE_BLOCKED",
                            entity_type="ORDER_LEG",
                            entity_key=leg.id,
                            details={"error": str(exc), "adapter_invoked": False},
                        )
                    else:
                        self.repository.transition_intent(
                            intent_id,
                            IntentStatus.REJECTED,
                            now=self._now(),
                        )
                    return self._required_intent(intent_id)
            outcome = self._submit_leg(intent, leg, account, stored_status=stored["status"])
            if outcome == "accepted":
                accepted_any = True
                continue
            if outcome == "ambiguous" or accepted_any:
                self._require_reconciliation(
                    intent_id,
                    account,
                    category="SUBMISSION_AMBIGUITY" if outcome == "ambiguous" else "INCOMPLETE_INTENT",
                    entity_type="ORDER_LEG",
                    entity_key=leg.id,
                    details={"outcome": outcome},
                )
            else:
                self.repository.transition_intent(intent_id, IntentStatus.REJECTED, now=self._now())
            if intent.execution_policy.failure_policy is FailurePolicy.CANCEL_WORKING_LEGS:
                self._cancel_working_attempts(intent, account)
            return self._required_intent(intent_id)

        current = self._required_intent(intent_id)["status"]
        if current == IntentStatus.SUBMITTING.value:
            self.repository.transition_intent(intent_id, IntentStatus.WORKING, now=self._now())
        return self._required_intent(intent_id)

    def submit_verified_compensating_exit(
        self,
        *,
        source_intent_id: str,
        expected_external_order_ids: Mapping[str, str],
        account: Account,
        risk_decision: RiskDecisionRecord | None = None,
        _before_submit_leg: Callable[[OrderIntent, OrderLeg, Account], None] | None = None,
    ) -> dict:
        """Submit a strictly verified exit for one exact filled source intent.

        This is intentionally not a general ``flatten`` or blocker bypass.
        It accepts only a persisted entry intent whose every submitted leg is
        fully filled, a fresh complete account fact set with no open orders,
        and positions exactly equal to those durable fills.  The exact broker
        external-order IDs and instrument IDs are supplied so an unrelated or
        sibling claim can never be silently netted into the exit.  The normal
        ``_submit_leg`` path still creates durable exit attempts and records
        all broker responses.
        """
        source_id = str(source_intent_id).strip()
        if not source_id:
            raise OMSExecutionError("source_intent_id is required")
        if not expected_external_order_ids:
            raise OMSExecutionError("expected_external_order_ids is required")
        canonical = self.repository.get_account(account.id)
        if canonical is None or not canonical.enabled:
            raise OMSExecutionError("compensating exit requires an enabled persisted account")
        mismatches = self._account_contract_mismatches(canonical, account)
        if mismatches:
            raise OMSExecutionError(
                "compensating exit account identity mismatch: " + ",".join(mismatches)
            )
        account = canonical
        source = self.repository.get_intent(source_id)
        if source is None:
            raise OMSExecutionError(f"unknown source intent: {source_id}")
        if str(source.get("account_id")) != account.id:
            raise OMSExecutionError("source intent account does not match supplied account")
        if str(source.get("action")) != IntentAction.ENTER.value:
            raise OMSExecutionError("compensating exit source must be an ENTER intent")
        legs = list(source.get("legs", ()))
        if not legs:
            raise OMSExecutionError("source intent has no legs")

        expected_by_instrument = {
            str(instrument_id): str(external_order_id)
            for instrument_id, external_order_id in expected_external_order_ids.items()
        }
        if len(expected_by_instrument) != len(expected_external_order_ids):
            raise OMSExecutionError("compensating exit external-order mapping is ambiguous")
        expected_positions: dict[str, Decimal] = {}
        verified_rows: list[dict[str, object]] = []
        for leg in legs:
            instrument_id = str(leg["instrument_id"])
            external_order_id = expected_by_instrument.get(instrument_id)
            if external_order_id is None:
                raise OMSExecutionError(
                    f"source leg {leg['id']} has no exact external-order mapping"
                )
            if str(leg.get("status")) != LegStatus.FILLED.value:
                raise OMSExecutionError(f"source leg {leg['id']} is not durably FILLED")
            try:
                quantity = Decimal(str(leg["quantity"]))
                cumulative = Decimal(str(leg.get("cumulative_filled_quantity") or "0"))
            except (InvalidOperation, TypeError, ValueError) as exc:
                raise OMSExecutionError("source leg quantity evidence is malformed") from exc
            if not quantity.is_finite() or quantity <= 0 or cumulative != quantity:
                raise OMSExecutionError(f"source leg {leg['id']} lacks exact full-fill evidence")
            attempts = self.repository.broker_orders_for_leg(str(leg["id"]))
            if len(attempts) != 1:
                raise OMSExecutionError(
                    f"source leg {leg['id']} has unsupported attempt count {len(attempts)}"
                )
            order = attempts[0]
            if str(order.get("account_id")) != account.id:
                raise OMSExecutionError("source broker order account mismatch")
            if str(order.get("external_order_id")) != external_order_id:
                raise OMSExecutionError(
                    f"source leg {leg['id']} external order mismatch"
                )
            if str(order.get("status")) != BrokerOrderStatus.FILLED.value:
                raise OMSExecutionError("source broker order is not durably FILLED")
            try:
                submitted = Decimal(str(order["submitted_quantity"]))
            except (InvalidOperation, TypeError, ValueError) as exc:
                raise OMSExecutionError("source submitted quantity evidence is malformed") from exc
            if submitted != quantity:
                raise OMSExecutionError("source submitted quantity differs from leg quantity")
            fills = self.repository.fills_for_broker_order(str(order["id"]))
            if len(fills) != 1:
                raise OMSExecutionError(
                    f"source broker order {external_order_id} lacks exactly one durable fill"
                )
            fill = fills[0]
            try:
                fill_quantity = Decimal(str(fill["quantity"]))
                fill_price = Decimal(str(fill["price"]))
            except (InvalidOperation, TypeError, ValueError) as exc:
                raise OMSExecutionError("source fill evidence is malformed") from exc
            if (
                not fill_quantity.is_finite()
                or fill_quantity != quantity
                or not fill_price.is_finite()
                or fill_price <= 0
            ):
                raise OMSExecutionError("source fill evidence is not an exact full fill")
            metadata = fill.get("metadata")
            if isinstance(metadata, Mapping):
                if str(metadata.get(self._BROKER_FILL_ACCOUNT_ID_KEY, account.id)) != account.id:
                    raise OMSExecutionError("source fill account provenance mismatch")
                if str(metadata.get("_external_order_id", external_order_id)) != external_order_id:
                    raise OMSExecutionError("source fill external-order provenance mismatch")
                if str(metadata.get("_instrument_id", instrument_id)) != instrument_id:
                    raise OMSExecutionError("source fill instrument provenance mismatch")
            side = str(leg["side"])
            if side == Side.BUY.value:
                signed = quantity
            elif side == Side.SELL.value:
                signed = -quantity
            else:
                raise OMSExecutionError("source leg side is invalid")
            expected_positions[instrument_id] = expected_positions.get(instrument_id, Decimal("0")) + signed
            verified_rows.append(
                {
                    "instrument_id": instrument_id,
                    "external_order_id": external_order_id,
                    "quantity": quantity,
                    "price": fill_price,
                    "side": side,
                    "order": order,
                    "attempt": order,
                    "fill": fill,
                }
            )
        if set(expected_by_instrument) != {str(row["instrument_id"]) for row in verified_rows}:
            raise OMSExecutionError("external-order mapping contains an unrelated instrument")

        facts, normalized_positions, normalized_open_orders = self._strict_authoritative_account_facts(
            account,
            max_age_seconds=self.BROKER_FACT_MAX_AGE_SECONDS,
            require_no_open_orders=True,
        )
        self._require_usable_execution_evidence(
            account,
            facts,
            source="compensating exit",
        )
        observed: dict[str, Decimal] = {}
        for position in normalized_positions:
            quantity = Decimal(str(position.signed_quantity))
            if not quantity.is_finite():
                raise OMSExecutionError("broker position quantity is invalid")
            if quantity != 0:
                observed[position.instrument_id] = observed.get(position.instrument_id, Decimal("0")) + quantity
        if observed != {key: value for key, value in expected_positions.items() if value != 0}:
            raise OMSExecutionError(
                f"broker positions do not exactly match verified source exposure: {observed!r}"
            )
        self._validate_exact_selected_fill_facts(
            account=account,
            facts=facts,
            expected_rows=verified_rows,
            source="compensating exit",
        )
        now = self._now()
        digest_material = "|".join(
            [account.id, source_id]
            + [
                f"{row['instrument_id']}:{row['external_order_id']}:{row['side']}:{row['quantity']}"
                for row in verified_rows
            ]
        )
        digest = hashlib.sha256(digest_material.encode("utf-8")).hexdigest()[:24]
        exit_id = f"stage6-compensating-exit-{digest}"
        exit_key = f"stage6-compensating-exit|{source_id}|{digest}"
        exit_legs = []
        for sequence, row in enumerate(verified_rows):
            source_leg = legs[sequence]
            exit_side = Side.SELL if row["side"] == Side.BUY.value else Side.BUY
            exit_legs.append(
                OrderLeg(
                    id=f"{exit_id}-leg-{sequence}",
                    intent_id=exit_id,
                    sequence=sequence,
                    instrument_id=str(row["instrument_id"]),
                    side=exit_side,
                    quantity=row["quantity"],
                    quantity_unit=source_leg.get("quantity_unit", "UNITS"),
                    order_type=str(source_leg.get("order_type", "MARKET")),
                    time_in_force=source_leg.get("time_in_force"),
                    metadata={
                        "verified_source_intent_id": source_id,
                        "verified_source_external_order_id": str(row["external_order_id"]),
                        "verified_source_fill_quantity": str(row["quantity"]),
                        "verified_compensating_exit": True,
                    },
                    created_at=now,
                    updated_at=now,
                )
            )
        exit_intent = OrderIntent(
            id=exit_id,
            idempotency_key=exit_key,
            strategy_id=str(source["strategy_id"]),
            account_id=account.id,
            action=IntentAction.EXIT,
            legs=tuple(exit_legs),
            book_id=source.get("book_id"),
            source_signal_id=f"verified-compensating-exit:{source_id}",
            execution_policy=ExecutionPolicy(),
            metadata={
                "verified_compensating_exit": True,
                "source_intent_id": source_id,
                "source_external_order_ids": [str(row["external_order_id"]) for row in verified_rows],
                "source_fill_evidence": [
                    {
                        "instrument_id": str(row["instrument_id"]),
                        "external_order_id": str(row["external_order_id"]),
                        "quantity": str(row["quantity"]),
                        "price": str(row["price"]),
                    }
                    for row in verified_rows
                ],
                "fresh_facts_captured_at": facts.captured_at.isoformat(),
            },
            created_at=now,
            updated_at=now,
        )
        decision = risk_decision or RiskDecisionRecord(
            id=f"stage6-compensating-exit-risk-{digest}",
            intent_id=exit_id,
            approved=True,
            reason="explicit approved SIM compensating exit for exact verified exposure",
            checks={
                "source_intent_id": source_id,
                "external_order_ids": [str(row["external_order_id"]) for row in verified_rows],
                "fresh_positions_exact": True,
                "fresh_open_orders_empty": True,
            },
            evaluated_at=now,
            metadata={"verified_compensating_exit": True},
        )
        if decision.intent_id != exit_id:
            raise OMSExecutionError("compensating exit risk decision identity mismatch")
        return self.submit_intent(
            exit_intent,
            account=account,
            risk_decision=decision,
            _internal_capability=self.__verified_compensating_exit_capability,
            _before_submit_leg=_before_submit_leg,
        )

    def submit_verified_residual_exit(
        self,
        *,
        source_intent_id: str,
        expected_external_order_ids: Mapping[str, str],
        account: Account,
        risk_decision: RiskDecisionRecord | None = None,
        _before_submit_leg: Callable[[OrderIntent, OrderLeg, Account], None] | None = None,
    ) -> dict:
        """Submit one exact residual exit for a durably partial source.

        This is deliberately separate from ``submit_verified_compensating_exit``:
        the latter remains a full-fill-only primitive.  This route accepts a
        source with one or more proven positive fills and no ambiguous/active
        attempts, then requires a fresh account snapshot to equal the
        source-derived residual before creating any exit intent.  It never
        retries or cancels the source and requires the Stage 6 per-leg safety
        callback before each broker submission.
        """
        source_id = str(source_intent_id).strip()
        if not source_id:
            raise OMSExecutionError("source_intent_id is required")
        if not expected_external_order_ids:
            raise OMSExecutionError("expected_external_order_ids is required")
        if _before_submit_leg is None:
            raise OMSExecutionError("residual exit requires a per-leg safety callback")

        canonical = self.repository.get_account(account.id)
        if canonical is None or not canonical.enabled:
            raise OMSExecutionError("residual exit requires an enabled persisted account")
        mismatches = self._account_contract_mismatches(canonical, account)
        if mismatches:
            raise OMSExecutionError(
                "residual exit account identity mismatch: " + ",".join(mismatches)
            )
        account = canonical

        source = self.repository.get_intent(source_id)
        if source is None:
            raise OMSExecutionError(f"unknown source intent: {source_id}")
        if str(source.get("account_id")) != account.id:
            raise OMSExecutionError("source intent account does not match supplied account")
        action = str(source.get("action"))
        if action not in {IntentAction.ENTER.value, IntentAction.EXIT.value}:
            raise OMSExecutionError("residual exit source must be an ENTER or EXIT intent")
        if not str(source.get("book_id") or "").strip() or not str(source.get("strategy_id") or "").strip():
            raise OMSExecutionError("residual exit source lacks durable book/strategy ownership")
        if str(source.get("status")) not in {
            IntentStatus.PARTIALLY_FILLED.value,
            IntentStatus.RECONCILIATION_REQUIRED.value,
            IntentStatus.FILLED.value,
            IntentStatus.CANCELLED.value,
            IntentStatus.FAILED.value,
        }:
            raise OMSExecutionError("residual exit source is not durably terminal/partial")

        expected_by_instrument = {
            str(instrument_id).strip(): str(external_order_id).strip()
            for instrument_id, external_order_id in expected_external_order_ids.items()
        }
        if any(not instrument or not external for instrument, external in expected_by_instrument.items()):
            raise OMSExecutionError("residual external-order mapping contains an empty identity")
        if len(set(expected_by_instrument.values())) != len(expected_by_instrument):
            raise OMSExecutionError("residual external-order mapping reuses one provider order")
        prior_residuals: list[dict[str, object]] = []
        for candidate_row in self.repository.book_intents(account.id, book_id=str(source.get("book_id"))):
            candidate = self.repository.get_intent(str(candidate_row.get("id")))
            if candidate is None:
                continue
            candidate_metadata = candidate.get("metadata")
            if not isinstance(candidate_metadata, Mapping):
                continue
            if (
                candidate_metadata.get("verified_residual_exit") is True
                and str(candidate_metadata.get("source_intent_id")) == source_id
            ):
                prior_residuals.append(candidate)
        if len(prior_residuals) > 1:
            raise OMSExecutionError("source already has conflicting residual exit attempts")
        if prior_residuals:
            # A deterministic prior residual is already the durable answer.
            # Do not re-query or net its now-changed position into a second
            # exit; verify the immutable source-order mapping before replay.
            prior_metadata = prior_residuals[0]["metadata"]
            prior_external_ids = {
                str(value)
                for value in prior_metadata.get("source_external_order_ids", ())
            } if isinstance(prior_metadata, Mapping) else set()
            if prior_external_ids != set(expected_by_instrument.values()):
                raise OMSExecutionError("existing residual exit provenance conflicts with source mapping")
            return prior_residuals[0]

        def decimal(value: object, label: str, *, positive: bool = False) -> Decimal:
            try:
                parsed = Decimal(str(value))
            except (InvalidOperation, TypeError, ValueError) as exc:
                raise OMSExecutionError(f"{label} is not numeric") from exc
            if not parsed.is_finite() or (positive and parsed <= 0) or (not positive and parsed < 0):
                raise OMSExecutionError(f"{label} is invalid")
            return parsed

        def timestamp(value: object, label: str) -> datetime:
            parsed = self._parse_timestamp(value)
            if parsed is None:
                raise OMSExecutionError(f"{label} is missing or invalid")
            return parsed

        legs = list(source.get("legs", ()))
        if not legs:
            raise OMSExecutionError("residual exit source has no legs")
        durable_signed: dict[str, Decimal] = {}
        requested_signed: dict[str, Decimal] = {}
        verified_rows: list[dict[str, object]] = []
        filled_instruments: set[str] = set()
        for leg in legs:
            instrument_id = str(leg.get("instrument_id") or "").strip()
            if not instrument_id:
                raise OMSExecutionError("residual source leg lacks instrument identity")
            side = str(leg.get("side") or "").upper()
            if side not in {Side.BUY.value, Side.SELL.value}:
                raise OMSExecutionError(f"residual source leg {leg.get('id')} has invalid side")
            quantity = decimal(leg.get("quantity"), f"source leg {leg.get('id')} quantity", positive=True)
            cumulative = decimal(
                leg.get("cumulative_filled_quantity", "0"),
                f"source leg {leg.get('id')} cumulative fill",
            )
            if cumulative > quantity:
                raise OMSExecutionError(f"source leg {leg.get('id')} cumulative fill exceeds quantity")
            if instrument_id in requested_signed:
                raise OMSExecutionError("residual source contains duplicate instrument legs")
            signed_factor = Decimal("1") if side == Side.BUY.value else Decimal("-1")
            requested_signed[instrument_id] = signed_factor * quantity
            attempts = self.repository.broker_orders_for_leg(str(leg.get("id")))
            if cumulative == 0:
                # A planned sibling is acceptable for a partial source.  An
                # attempted sibling is also acceptable only when its durable
                # broker evidence proves a terminal zero-fill outcome.  A
                # merely terminal local row is not enough: a late fill can
                # otherwise be hidden by the residual proof.
                if str(leg.get("status")) not in {
                    LegStatus.PLANNED.value,
                    LegStatus.REJECTED.value,
                    LegStatus.CANCELLED.value,
                    LegStatus.FAILED.value,
                    LegStatus.RECONCILIATION_REQUIRED.value,
                }:
                    raise OMSExecutionError(
                        f"source leg {leg.get('id')} has no-fill or ambiguous submission evidence"
                    )
                if len(attempts) > 1:
                    raise OMSExecutionError(
                        f"source leg {leg.get('id')} has unsupported attempt count {len(attempts)}"
                    )
                if attempts:
                    attempt = attempts[0]
                    no_submit = self._is_durable_no_submit_rejection(attempt, leg, ())
                    terminal_zero = self._attempt_has_terminal_zero_fill_evidence(attempt, leg)
                    if not (no_submit or terminal_zero):
                        raise OMSExecutionError(
                            f"source leg {leg.get('id')} has no-fill or ambiguous submission evidence"
                        )
                continue
            if str(leg.get("status")) not in {
                LegStatus.PARTIALLY_FILLED.value,
                LegStatus.FILLED.value,
                LegStatus.CANCELLED.value,
                LegStatus.FAILED.value,
                LegStatus.RECONCILIATION_REQUIRED.value,
            }:
                raise OMSExecutionError(f"source leg {leg.get('id')} is not durably partial/terminal")
            if len(attempts) != 1:
                raise OMSExecutionError(
                    f"source leg {leg.get('id')} has unsupported attempt count {len(attempts)}"
                )
            order = attempts[0]
            external_order_id = str(order.get("external_order_id") or "").strip()
            if expected_by_instrument.get(instrument_id) != external_order_id:
                raise OMSExecutionError(f"source leg {leg.get('id')} external-order mapping mismatch")
            if str(order.get("account_id")) != account.id:
                raise OMSExecutionError("source broker order account mismatch")
            submitted = decimal(order.get("submitted_quantity"), "source submitted quantity", positive=True)
            if cumulative > submitted:
                raise OMSExecutionError("source cumulative fill exceeds submitted quantity")
            order_status = str(order.get("status"))
            if order_status == BrokerOrderStatus.FILLED.value and cumulative != submitted:
                raise OMSExecutionError("FILLED source order lacks complete durable fill")
            if order_status in {BrokerOrderStatus.PARTIALLY_FILLED.value, BrokerOrderStatus.CANCELLED.value}:
                if not (Decimal("0") < cumulative < submitted):
                    raise OMSExecutionError("partial source order quantity is not strictly partial")
            elif order_status in {BrokerOrderStatus.REJECTED.value, BrokerOrderStatus.FAILED.value}:
                raise OMSExecutionError("rejected/failed source order cannot carry residual fill evidence")
            elif order_status != BrokerOrderStatus.FILLED.value:
                raise OMSExecutionError("source broker order is active or unknown")
            fills = self.repository.fills_for_broker_order(str(order.get("id")))
            if not fills:
                raise OMSExecutionError("source broker order lacks durable fill evidence")
            fill_total = Decimal("0")
            fill_identities: set[str] = set()
            for fill in fills:
                fill_quantity = decimal(fill.get("quantity"), "source durable fill quantity", positive=True)
                fill_price = decimal(fill.get("price"), "source durable fill price", positive=True)
                fill_identity = str(fill.get("external_fill_id") or fill.get("dedupe_key") or "").strip()
                if not fill_identity or fill_identity in fill_identities:
                    raise OMSExecutionError("source durable fills contain duplicate/unknown identities")
                fill_identities.add(fill_identity)
                if str(fill.get("account_id") or account.id) != account.id:
                    raise OMSExecutionError("source durable fill account mismatch")
                if str(fill.get("external_order_id") or external_order_id) != external_order_id:
                    raise OMSExecutionError("source durable fill external-order mismatch")
                filled_at = timestamp(fill.get("filled_at"), "source durable fill timestamp")
                received_at = timestamp(fill.get("received_at"), "source durable fill receipt timestamp")
                if filled_at < timestamp(order.get("submitted_at") or order.get("updated_at"), "source submit timestamp") - timedelta(seconds=self.BROKER_TIME_ORDER_TOLERANCE_SECONDS):
                    raise OMSExecutionError("source durable fill predates durable submission")
                if received_at < filled_at - timedelta(seconds=self.BROKER_TIME_ORDER_TOLERANCE_SECONDS):
                    raise OMSExecutionError("source durable fill receipt predates fill")
                fill_total += fill_quantity
                verified_rows.append(
                    {
                        "instrument_id": instrument_id,
                        "external_order_id": external_order_id,
                        "quantity": fill_quantity,
                        "price": fill_price,
                        "fill_identity": fill_identity,
                        "side": side,
                        "attempt": order,
                        "fill": fill,
                    }
                )
            if fill_total != cumulative:
                raise OMSExecutionError("source durable fills do not equal leg cumulative fill")
            filled_instruments.add(instrument_id)
            durable_signed[instrument_id] = durable_signed.get(instrument_id, Decimal("0")) + signed_factor * cumulative

        if set(expected_by_instrument) != filled_instruments:
            raise OMSExecutionError("residual external-order mapping must cover only filled source legs")
        if not filled_instruments:
            raise OMSExecutionError("source has no positive durable fill for residual compensation")

        # Prove the source-derived expected residual from the durable book,
        # rather than treating any current position as an eligible flatten.
        try:
            book_basis = self.repository.book_signed_exposure(
                account.id,
                str(source["book_id"]),
                exclude_intent_id=source_id,
                # ``book_signed_exposure`` historically applies the
                # exclusion to active intents but not the managed allocation
                # rows.  Supply both forms so the source's own partial fill
                # is never mistaken for unrelated book basis.
                exclude_intent_ids={source_id},
            )
        except Exception as exc:
            raise OMSExecutionError("durable source book exposure is unavailable") from exc
        if action == IntentAction.ENTER.value:
            if any(value != 0 for value in book_basis.values()):
                raise OMSExecutionError("partial ENTER has unrelated durable book exposure")
            expected_positions = {key: value for key, value in durable_signed.items() if value != 0}
        else:
            expected_positions = dict(book_basis)
            for instrument_id, requested in requested_signed.items():
                basis = book_basis.get(instrument_id, Decimal("0"))
                if basis == 0 or requested != -basis:
                    raise OMSExecutionError("partial EXIT does not exactly match durable book basis")
                expected_positions[instrument_id] = basis + durable_signed.get(instrument_id, Decimal("0"))
            expected_positions = {key: value for key, value in expected_positions.items() if value != 0}
        if not expected_positions:
            raise OMSExecutionError("source does not leave a residual exposure")

        facts, normalized_positions, normalized_open_orders = self._strict_authoritative_account_facts(
            account,
            max_age_seconds=self.BROKER_FACT_MAX_AGE_SECONDS,
            require_no_open_orders=True,
        )
        self._require_usable_execution_evidence(
            account,
            facts,
            source="residual exit",
        )
        observed: dict[str, Decimal] = {}
        for position in normalized_positions:
            if position.account_id != account.id or self._account_alias_mismatches(account, metadata=position.metadata):
                raise OMSExecutionError("fresh broker position account provenance mismatch")
            # Account positions are signed: a source SELL leg is a negative
            # quantity.  Use the strict finite-number contract here without
            # applying the positive-only order/fill quantity rule.
            try:
                quantity = Decimal(str(position.signed_quantity))
            except (InvalidOperation, TypeError, ValueError) as exc:
                raise OMSExecutionError("fresh broker position quantity is invalid") from exc
            if not quantity.is_finite():
                raise OMSExecutionError("fresh broker position quantity is invalid")
            if quantity != 0:
                observed[position.instrument_id] = observed.get(position.instrument_id, Decimal("0")) + quantity
        if observed != expected_positions:
            raise OMSExecutionError(
                f"fresh broker positions do not equal the proven residual exposure: {observed!r}"
            )
        if self._account_alias_mismatches(account, metadata=facts.metadata):
            raise OMSExecutionError("fresh broker facts contain foreign account aliases")

        # Fresh fills must prove every durable source fill and may not contain
        # an account fill with no durable owner.  Duplicate identity is never
        # netted, even when quantities happen to sum to the expected amount.
        # A source leg that was attempted and durably proven terminal with
        # zero fill is still an execution claim.  A later provider fill for
        # that exact external order is therefore contradictory even when the
        # current position snapshot has not reflected it yet.  Do not let a
        # delayed/history-only fill disappear behind the zero-fill residual
        # proof.
        terminal_zero_proofs: dict[str, tuple[Mapping[str, object], Mapping[str, object]]] = {}
        for leg in source.get("legs", ()):
            for attempt in self.repository.broker_orders_for_leg(str(leg.get("id"))):
                external_id = str(attempt.get("external_order_id") or "").strip()
                if (
                    external_id
                    and Decimal(str(leg.get("cumulative_filled_quantity", "0"))) == 0
                    and self._attempt_has_terminal_zero_fill_evidence(attempt, leg)
                ):
                    terminal_zero_proofs[external_id] = (leg, attempt)
        terminal_zero_external_ids = set(terminal_zero_proofs)
        if terminal_zero_proofs:
            source_created_at = self._parse_timestamp(source.get("created_at"))
            if source_created_at is None:
                raise OMSExecutionError("residual source has no valid creation timestamp for history proof")
            history_end = self._now()
            if history_end <= source_created_at:
                history_end = source_created_at + timedelta(microseconds=1)
            history = self._strict_historical_order_facts(
                account,
                requested_start=source_created_at,
                requested_end=history_end,
            )
            history_by_external: dict[str, BrokerOrderSnapshot] = {}
            for snapshot in history.orders:
                if snapshot.account_id != account.id:
                    raise OMSExecutionError("historical order facts contain a foreign account row")
                aliases = self._account_alias_mismatches(
                    account,
                    account_id=snapshot.account_id,
                    metadata=snapshot.metadata,
                )
                if aliases:
                    raise OMSExecutionError(
                        f"historical order {snapshot.external_order_id} has foreign account aliases: {aliases}"
                    )
                external_id = str(snapshot.external_order_id)
                if external_id in history_by_external:
                    raise OMSExecutionError("historical order facts contain duplicate order identities")
                history_by_external[external_id] = snapshot
            for external_id, (zero_leg, zero_attempt) in terminal_zero_proofs.items():
                snapshot = history_by_external.get(external_id)
                if snapshot is None:
                    raise OMSExecutionError(
                        f"historical order facts lack terminal zero-fill proof for {external_id}"
                    )
                submitted = Decimal(str(zero_attempt.get("submitted_quantity")))
                if (
                    snapshot.account_id != account.id
                    or snapshot.instrument_id != str(zero_leg.get("instrument_id"))
                    or snapshot.side.value != str(zero_leg.get("side"))
                    or snapshot.quantity != submitted
                    or snapshot.filled_quantity != 0
                    or snapshot.status
                    not in {
                        BrokerOrderStatus.CANCELLED,
                        BrokerOrderStatus.REJECTED,
                        BrokerOrderStatus.FAILED,
                    }
                ):
                    raise OMSExecutionError(
                        f"historical order {external_id} contradicts terminal zero-fill proof"
                    )
                submitted_at = self._parse_timestamp(zero_attempt.get("submitted_at"))
                observed_at = snapshot.order_time or snapshot.captured_at
                if submitted_at is not None and observed_at < submitted_at - timedelta(
                    seconds=self.BROKER_TIME_ORDER_TOLERANCE_SECONDS
                ):
                    raise OMSExecutionError(
                        f"historical order {external_id} predates durable submission"
                    )
                late_fills = [
                    fill
                    for fill in history.fills
                    if str(fill.external_order_id) == external_id
                ]
                if late_fills:
                    raise OMSExecutionError(
                        f"historical order facts contain a late fill for a durably proven zero-fill order {external_id}"
                    )
        self._validate_exact_selected_fill_facts(
            account=account,
            facts=facts,
            expected_rows=verified_rows,
            source="residual exit",
        )

        digest_material = [account.id, str(source.get("book_id")), str(source.get("strategy_id")), source_id]
        digest_material.extend(
            f"{row['instrument_id']}:{row['external_order_id']}:{row['fill_identity']}:{row['quantity']}:{row['price']}"
            for row in sorted(verified_rows, key=lambda item: (str(item["instrument_id"]), str(item["fill_identity"])))
        )
        digest_material.extend(f"{key}:{value}" for key, value in sorted(expected_positions.items()))
        digest = hashlib.sha256("|".join(digest_material).encode("utf-8")).hexdigest()[:24]
        exit_id = f"stage6-residual-exit-{digest}"
        exit_key = f"stage6-residual-exit|{source_id}|{digest}"
        existing = self.repository.get_intent_by_idempotency_key(account.id, exit_key)
        if existing is not None:
            metadata = existing.get("metadata")
            if not isinstance(metadata, Mapping) or metadata.get("verified_residual_exit") is not True or str(metadata.get("source_intent_id")) != source_id or str(metadata.get("proof_digest")) != digest:
                raise OMSExecutionError("residual exit idempotency key has conflicting durable provenance")
            return existing
        now = self._now()
        exit_legs: list[OrderLeg] = []
        for sequence, (instrument_id, residual_quantity) in enumerate(sorted(expected_positions.items())):
            exit_legs.append(
                OrderLeg(
                    id=f"{exit_id}-leg-{sequence}",
                    intent_id=exit_id,
                    sequence=sequence,
                    instrument_id=instrument_id,
                    side=Side.SELL if residual_quantity > 0 else Side.BUY,
                    quantity=abs(residual_quantity),
                    order_type="MARKET",
                    metadata={
                        "verified_residual_exit": True,
                        "source_intent_id": source_id,
                        "proof_digest": digest,
                    },
                    created_at=now,
                    updated_at=now,
                )
            )
        exit_intent = OrderIntent(
            id=exit_id,
            idempotency_key=exit_key,
            strategy_id=str(source["strategy_id"]),
            account_id=account.id,
            action=IntentAction.EXIT,
            legs=tuple(exit_legs),
            book_id=str(source["book_id"]),
            source_signal_id=f"verified-residual-exit:{source_id}",
            execution_policy=ExecutionPolicy(),
            metadata={
                "verified_residual_exit": True,
                "source_intent_id": source_id,
                "source_external_order_ids": sorted(expected_by_instrument.values()),
                "source_fill_evidence": [
                    {
                        "instrument_id": str(row["instrument_id"]),
                        "external_order_id": str(row["external_order_id"]),
                        "fill_identity": str(row["fill_identity"]),
                        "quantity": str(row["quantity"]),
                        "price": str(row["price"]),
                    }
                    for row in verified_rows
                ],
                "residual_positions": {key: str(value) for key, value in sorted(expected_positions.items())},
                "proof_digest": digest,
                "fresh_facts_captured_at": facts.captured_at.isoformat(),
            },
            created_at=now,
            updated_at=now,
        )
        decision = risk_decision or RiskDecisionRecord(
            id=f"stage6-residual-exit-risk-{digest}",
            intent_id=exit_id,
            approved=True,
            reason="explicit approved SIM residual exit for exact partial exposure",
            checks={
                "source_intent_id": source_id,
                "fresh_positions_exact": True,
                "fresh_open_orders_empty": True,
                "partial_source_proven": True,
            },
            evaluated_at=now,
            metadata={"verified_residual_exit": True, "proof_digest": digest},
        )
        if decision.intent_id != exit_id:
            raise OMSExecutionError("residual exit risk decision identity mismatch")
        return self.submit_intent(
            exit_intent,
            account=account,
            risk_decision=decision,
            _internal_capability=self.__verified_compensating_exit_capability,
            _before_submit_leg=_before_submit_leg,
        )

    def resolve_verified_roundtrip(
        self,
        *,
        entry_intent_id: str,
        exit_intent_id: str,
        account: Account,
    ) -> dict[str, object]:
        """Close one exact filled entry/exit group after fresh broker proof.

        This is a generic local lifecycle operation, not a broker command.  It
        requires both intents' terminal attempt-scoped fills, fresh flat
        account facts, and a complete historical order window covering both
        intents.  Only stale issues whose evidence names these exact broker
        orders are resolved; unrelated blockers remain sticky.
        """
        self._startup_safety_audit()
        account = self._canonical_account_for_boundary(account, source="resolve_verified_roundtrip")
        if entry_intent_id == exit_intent_id:
            raise OMSExecutionError("round-trip entry and exit intents must differ")
        entry = self._required_intent(entry_intent_id)
        exit_intent = self._required_intent(exit_intent_id)
        for item, label in ((entry, "entry"), (exit_intent, "exit")):
            if str(item.get("account_id")) != account.id:
                raise OMSExecutionError(f"round-trip {label} intent account mismatch")
            if str(item.get("book_id") or "") != str(entry.get("book_id") or ""):
                raise OMSExecutionError("round-trip intents must share one declared book")
        if str(entry.get("action")) != IntentAction.ENTER.value:
            raise OMSExecutionError("round-trip entry intent must be ENTER")
        if str(exit_intent.get("action")) not in {IntentAction.EXIT.value, IntentAction.FLATTEN.value}:
            raise OMSExecutionError("round-trip compensating intent must be EXIT or FLATTEN")

        def parse_decimal(value: object, label: str) -> Decimal:
            try:
                result = Decimal(str(value))
            except (InvalidOperation, TypeError, ValueError) as exc:
                raise OMSExecutionError(f"{label} is not numeric") from exc
            if not result.is_finite() or result <= 0:
                raise OMSExecutionError(f"{label} is invalid")
            return result

        def parse_time(value: object, label: str) -> datetime:
            result = self._parse_timestamp(value)
            if result is None:
                raise OMSExecutionError(f"{label} is missing or invalid")
            return result

        def filled_rows(intent: Mapping[str, object], label: str) -> list[dict[str, object]]:
            rows: list[dict[str, object]] = []
            for leg in intent.get("legs", ()):
                if str(leg.get("status")) != LegStatus.FILLED.value:
                    raise OMSExecutionError(f"round-trip {label} leg {leg.get('id')} is not FILLED")
                quantity = parse_decimal(leg.get("quantity"), f"{label} leg quantity")
                cumulative = parse_decimal(leg.get("cumulative_filled_quantity"), f"{label} cumulative quantity")
                if cumulative != quantity:
                    raise OMSExecutionError(f"round-trip {label} leg cumulative fill is incomplete")
                attempts = self.repository.broker_orders_for_leg(str(leg["id"]))
                if len(attempts) != 1 or str(attempts[0].get("status")) != BrokerOrderStatus.FILLED.value:
                    raise OMSExecutionError(f"round-trip {label} leg lacks one terminal filled attempt")
                attempt = attempts[0]
                if str(attempt.get("account_id")) != account.id:
                    raise OMSExecutionError(f"round-trip {label} attempt account mismatch")
                external_id = str(attempt.get("external_order_id") or "").strip()
                if not external_id:
                    raise OMSExecutionError(f"round-trip {label} attempt lacks external identity")
                if parse_decimal(attempt.get("submitted_quantity"), f"{label} submitted quantity") != quantity:
                    raise OMSExecutionError(f"round-trip {label} submitted quantity mismatch")
                fills = self.repository.fills_for_broker_order(str(attempt["id"]))
                if len(fills) != 1 or parse_decimal(fills[0].get("quantity"), f"{label} durable fill quantity") != quantity:
                    raise OMSExecutionError(f"round-trip {label} lacks exact durable fill")
                metadata = fills[0].get("metadata")
                if not isinstance(metadata, Mapping):
                    raise OMSExecutionError(f"round-trip {label} fill provenance is missing")
                if str(metadata.get(self._BROKER_FILL_ACCOUNT_ID_KEY, account.id)) != account.id:
                    raise OMSExecutionError(f"round-trip {label} fill account provenance mismatch")
                if str(metadata.get("_external_order_id", external_id)) != external_id:
                    raise OMSExecutionError(f"round-trip {label} fill order provenance mismatch")
                if str(metadata.get("_instrument_id", leg.get("instrument_id"))) != str(leg.get("instrument_id")):
                    raise OMSExecutionError(f"round-trip {label} fill instrument provenance mismatch")
                rows.append({
                    "leg": leg,
                    "attempt": attempt,
                    "external_order_id": external_id,
                    "quantity": quantity,
                    "fill": fills[0],
                })
            return rows

        entry_rows = filled_rows(entry, "entry")
        exit_rows = filled_rows(exit_intent, "exit")
        if len(entry_rows) != len(exit_rows):
            raise OMSExecutionError("round-trip leg counts do not match")
        exit_by_instrument = {str(row["leg"]["instrument_id"]): row for row in exit_rows}
        if len(exit_by_instrument) != len(exit_rows):
            raise OMSExecutionError("round-trip exit instrument identities are ambiguous")
        for row in entry_rows:
            instrument = str(row["leg"]["instrument_id"])
            counterpart = exit_by_instrument.get(instrument)
            if counterpart is None or counterpart["quantity"] != row["quantity"]:
                raise OMSExecutionError("round-trip exit does not exactly offset entry quantity")
            entry_side = str(row["leg"]["side"])
            exit_side = str(counterpart["leg"]["side"])
            if not ((entry_side == Side.BUY.value and exit_side == Side.SELL.value) or (entry_side == Side.SELL.value and exit_side == Side.BUY.value)):
                raise OMSExecutionError("round-trip exit side does not offset entry side")

        start = min(
            parse_time(entry.get("created_at"), "entry created_at"),
            parse_time(exit_intent.get("created_at"), "exit created_at"),
        )
        observation_time = self._now()
        expected_external_ids = {
            str(row["external_order_id"])
            for row in (*entry_rows, *exit_rows)
        }
        facts, history = self._strict_roundtrip_broker_evidence(
            account,
            expected_rows=(*entry_rows, *exit_rows),
            requested_start=start,
            requested_end=observation_time,
        )

        closure = {
            "version": 1,
            "reason": "fresh_broker_and_historical_proof_of_verified_flat_roundtrip",
            "account_id": account.id,
            "book_id": str(entry.get("book_id") or ""),
            "intent_ids": [entry_intent_id, exit_intent_id],
            "entry_external_order_ids": sorted(str(row["external_order_id"]) for row in entry_rows),
            "exit_external_order_ids": sorted(str(row["external_order_id"]) for row in exit_rows),
            "account_facts_captured_at": facts.captured_at.isoformat(),
            "historical_window": {
                "requested_start": history.requested_start.isoformat(),
                "requested_end": history.requested_end.isoformat(),
                "captured_at": history.captured_at.isoformat(),
                "order_count": len(history.orders),
                "evidence_mode": history.execution_evidence_mode.value,
            },
        }
        resolved_issue_keys: list[str] = []
        expected_intent_ids = {str(entry_intent_id), str(exit_intent_id)}
        for issue in self.repository.open_reconciliation_issues(account.id):
            details = self._issue_details(issue)
            category = str(issue.get("category", ""))
            external_id = str(details.get("external_order_id", ""))
            issue_intent_id = str(issue.get("intent_id") or details.get("intent_id") or "").strip()
            if issue_intent_id not in expected_intent_ids:
                continue
            if category == "SIBLING_INTENT_BROKER_FILL_UNMATCHED" and external_id in expected_external_ids:
                if self._resolve_reconciliation_issue(account.id, str(issue["issue_key"]), resolved_at=observation_time):
                    resolved_issue_keys.append(str(issue["issue_key"]))
            elif category == "BROKER_QUERY_FAILED" and str(issue.get("entity_key", "")) == f"{account.id}:positions":
                if self._resolve_reconciliation_issue(account.id, str(issue["issue_key"]), resolved_at=observation_time):
                    resolved_issue_keys.append(str(issue["issue_key"]))
            elif category == "BROKER_FACT_GATE_BLOCKED":
                blockers = details.get("blockers")
                if isinstance(blockers, list) and blockers and all(
                    isinstance(item, Mapping)
                    and str(item.get("external_order_id", "")) in expected_external_ids
                    for item in blockers
                ):
                    if self._resolve_reconciliation_issue(account.id, str(issue["issue_key"]), resolved_at=observation_time):
                        resolved_issue_keys.append(str(issue["issue_key"]))
        resolved_action_keys: list[str] = []
        expected_fill_keys = {f"SIBLING_INTENT_FILL:moomoo-order-fill:{external_id}" for external_id in expected_external_ids}

        def stale_unsubmitted_fact_action(action: Mapping[str, object]) -> bool:
            """Recognize one superseded pre-fill gate only after exact proof.

            A sequential pilot can leave a sibling intent with no attempts
            after the first sleeve was accepted.  Its account-fact gate may
            still name those now-completed orders as ``WORKING``/zero-fill.
            The round-trip proof above authenticates every named order and
            fresh flatness; only then may this stale, never-submitted action
            be resolved.  Any different IDs, account, local claim, or intent
            state remains an account-wide blocker.
            """

            action_key = str(action.get("action_key", ""))
            if not action_key.startswith("BROKER_FACT_GATE:"):
                return False
            if str(action.get("account_id", "")) != account.id:
                return False
            sibling_id = str(action.get("intent_id", ""))
            sibling = self.repository.get_intent(sibling_id)
            if sibling is None or str(sibling.get("account_id", "")) != account.id:
                return False
            if str(sibling.get("status", "")) not in {
                IntentStatus.RECONCILIATION_REQUIRED.value,
                IntentStatus.REJECTED.value,
                IntentStatus.CANCELLED.value,
            }:
                return False
            if any(
                self.repository.broker_orders_for_leg(str(leg["id"]))
                or self.repository.fills_for_leg(str(leg["id"]))
                for leg in sibling.get("legs", ())
            ):
                return False
            if any(
                str(row.get("source_intent_id", "")) == sibling_id
                for row in self.repository.position_allocations(account.id)
            ):
                return False
            metadata = action.get("metadata")
            blockers = metadata.get("blockers") if isinstance(metadata, Mapping) else None
            if not isinstance(blockers, list) or not blockers:
                return False
            blocked_external_ids = {
                str(item.get("external_order_id", "")).strip()
                for item in blockers
                if isinstance(item, Mapping) and str(item.get("external_order_id", "")).strip()
            }
            # A sequential pilot's stale gate is created while the entry
            # sleeve is working, so it can name only the entry orders even
            # though the proof necessarily also contains their later
            # compensating exits.  Keep this exact-proof scoped: every
            # blocked order must be one of the authenticated entry orders;
            # arbitrary subsets or unrelated historical IDs remain blocked.
            entry_external_ids = {
                str(row["external_order_id"])
                for row in entry_rows
            }
            return bool(blocked_external_ids) and blocked_external_ids.issubset(entry_external_ids)

        for action in self.repository.open_recovery_actions(account.id):
            action_key = str(action.get("action_key", ""))
            action_intent_id = str(action.get("intent_id") or "").strip()
            if action_intent_id not in expected_intent_ids:
                continue
            if action_key in expected_fill_keys or action_key == f"POLL_ERROR:{account.id}:positions":
                if self._resolve_recovery_action(account.id, str(action.get("intent_id")), action_key, resolved_at=observation_time):
                    resolved_action_keys.append(action_key)
            elif stale_unsubmitted_fact_action(action):
                if self._resolve_recovery_action(
                    account.id,
                    str(action.get("intent_id")),
                    action_key,
                    resolved_at=observation_time,
                ):
                    resolved_action_keys.append(action_key)
        if self.repository.open_reconciliation_issues(account.id) or self.repository.open_recovery_actions(account.id):
            raise OMSExecutionError("cannot close round-trip while reconciliation blockers remain open")
        self.repository.mark_verified_roundtrip_intents(
            (entry_intent_id, exit_intent_id),
            closure_metadata=closure,
            now=observation_time,
            _resolution_capability=self.repository._resolution_capability(),
        )
        event_id = f"verified-roundtrip-closure:{entry_intent_id}:{exit_intent_id}"
        if not any(event.get("id") == event_id for event in self.repository.operational_events(account.id, limit=1000)):
            self.repository.record_operational_event(
                event_id=event_id,
                account_id=account.id,
                event_type="VERIFIED_ROUNDTRIP_CLOSED",
                mode=account.environment.value,
                outcome="COMPLETED",
                occurred_at=observation_time,
                summary="Closed one entry/exit group after fresh flat-account and complete historical-order proof.",
                details={**closure, "resolved_issue_keys": resolved_issue_keys, "resolved_action_keys": resolved_action_keys},
            )
        return {
            "entry_intent_id": entry_intent_id,
            "exit_intent_id": exit_intent_id,
            "status": IntentStatus.COMPLETED.value,
            "resolved_issue_keys": tuple(resolved_issue_keys),
            "resolved_action_keys": tuple(resolved_action_keys),
            "proof": closure,
        }

    def resolve_verified_aggregate_roundtrip(
        self,
        *,
        intent_ids: Sequence[str],
        account: Account,
    ) -> dict[str, object]:
        """Close one explicitly named multi-intent flat round-trip group.

        This is the narrow recovery path for a sequential pilot that emitted
        more than one exit lifecycle for the same source position (for
        example, an exit with the source direction followed by a corrective
        opposite-side exit).  The method never infers ownership from labels:
        every intent, broker attempt, durable fill, provider fill, and
        historical order must be named and match the same canonical account,
        book, instrument, quantity, side, and price evidence.  Local fills
        and the incident metadata remain immutable; only the named intents
        receive the proof-backed terminal promotion.
        """
        self._startup_safety_audit()
        account = self._canonical_account_for_boundary(
            account,
            source="resolve_verified_aggregate_roundtrip",
        )
        requested_ids = tuple(str(value).strip() for value in intent_ids if str(value).strip())
        if len(requested_ids) < 3 or len(set(requested_ids)) != len(requested_ids):
            raise OMSExecutionError("aggregate round-trip requires at least three distinct intents")

        intents = [self._required_intent(intent_id) for intent_id in requested_ids]
        if any(str(intent.get("account_id")) != account.id for intent in intents):
            raise OMSExecutionError("aggregate round-trip intent account mismatch")
        book_ids = {str(intent.get("book_id") or "").strip() for intent in intents}
        if len(book_ids) != 1 or not next(iter(book_ids)):
            raise OMSExecutionError("aggregate round-trip intents must share one declared book")
        book_id = next(iter(book_ids))
        actions = [str(intent.get("action", "")) for intent in intents]
        if actions.count(IntentAction.ENTER.value) != 1 or any(
            action not in {IntentAction.ENTER.value, IntentAction.EXIT.value, IntentAction.FLATTEN.value}
            for action in actions
        ):
            raise OMSExecutionError("aggregate round-trip requires one ENTER and only EXIT/FLATTEN companions")
        if not any(action in {IntentAction.EXIT.value, IntentAction.FLATTEN.value} for action in actions):
            raise OMSExecutionError("aggregate round-trip requires a corrective exit intent")
        allowed_intent_statuses = {
            IntentStatus.FILLED.value,
            IntentStatus.RECONCILIATION_REQUIRED.value,
            IntentStatus.COMPLETED.value,
        }
        if any(str(intent.get("status")) not in allowed_intent_statuses for intent in intents):
            raise OMSExecutionError("aggregate round-trip contains an active or non-terminal intent")

        def parse_decimal(value: object, label: str, *, positive: bool = True) -> Decimal:
            try:
                result = Decimal(str(value))
            except (InvalidOperation, TypeError, ValueError) as exc:
                raise OMSExecutionError(f"{label} is not numeric") from exc
            if not result.is_finite() or (result <= 0 if positive else result < 0):
                raise OMSExecutionError(f"{label} is invalid")
            return result

        def parse_time(value: object, label: str) -> datetime:
            result = self._parse_timestamp(value)
            if result is None:
                raise OMSExecutionError(f"{label} is missing or invalid")
            return result

        rows: list[dict[str, object]] = []
        external_order_ids: set[str] = set()
        attempt_ids: set[str] = set()
        durable_fill_ids: set[str] = set()
        durable_dedupe_keys: set[str] = set()
        for intent in intents:
            intent_label = str(intent["id"])
            for leg in intent.get("legs", ()):
                if str(leg.get("status")) != LegStatus.FILLED.value:
                    raise OMSExecutionError(f"aggregate leg {leg.get('id')} is not FILLED")
                requested = parse_decimal(leg.get("quantity"), f"{intent_label} leg quantity")
                cumulative = parse_decimal(
                    leg.get("cumulative_filled_quantity"),
                    f"{intent_label} cumulative quantity",
                )
                if cumulative != requested:
                    raise OMSExecutionError(f"aggregate leg {leg.get('id')} has incomplete fill quantity")
                attempts = self.repository.broker_orders_for_leg(str(leg["id"]))
                if len(attempts) != 1:
                    raise OMSExecutionError(f"aggregate leg {leg.get('id')} has multiple or missing attempts")
                attempt = attempts[0]
                attempt_id = str(attempt.get("id") or "").strip()
                external_order_id = str(attempt.get("external_order_id") or "").strip()
                if not attempt_id or attempt_id in attempt_ids:
                    raise OMSExecutionError("aggregate attempts must have unique durable identities")
                if not external_order_id or external_order_id in external_order_ids:
                    raise OMSExecutionError("aggregate fills must have unique external order identities")
                if str(attempt.get("account_id")) != account.id:
                    raise OMSExecutionError("aggregate attempt account provenance mismatch")
                attempt_alias_mismatches = self._account_alias_mismatches(
                    account,
                    metadata=self._attempt_metadata(dict(attempt)),
                )
                if attempt_alias_mismatches:
                    raise OMSExecutionError(
                        f"aggregate attempt {attempt_id} has foreign account aliases: {attempt_alias_mismatches}"
                    )
                external_claims = self.repository.broker_orders_for_external_order_id(external_order_id)
                if len(external_claims) != 1 or str(external_claims[0].get("id")) != attempt_id:
                    raise OMSExecutionError(
                        f"aggregate external order {external_order_id} has an ambiguous or foreign durable claim"
                    )
                if str(attempt.get("status")) != BrokerOrderStatus.FILLED.value:
                    raise OMSExecutionError(f"aggregate attempt {attempt_id} is not terminal FILLED")
                submitted = parse_decimal(
                    attempt.get("submitted_quantity"),
                    f"aggregate attempt {attempt_id} submitted quantity",
                )
                if submitted != requested:
                    raise OMSExecutionError(f"aggregate attempt {attempt_id} quantity mismatch")
                fills = self.repository.fills_for_broker_order(attempt_id)
                if len(fills) != 1:
                    raise OMSExecutionError(f"aggregate attempt {attempt_id} lacks one exact durable fill")
                fill = fills[0]
                fill_quantity = parse_decimal(
                    fill.get("quantity"),
                    f"aggregate durable fill {attempt_id} quantity",
                )
                fill_price = parse_decimal(
                    fill.get("price"),
                    f"aggregate durable fill {attempt_id} price",
                )
                if fill_quantity != requested:
                    raise OMSExecutionError(f"aggregate durable fill {attempt_id} quantity mismatch")
                fill_id = str(fill.get("id") or "").strip()
                dedupe_key = str(fill.get("dedupe_key") or "").strip()
                if not fill_id or fill_id in durable_fill_ids:
                    raise OMSExecutionError("aggregate durable fills must have unique row identities")
                if not dedupe_key or dedupe_key in durable_dedupe_keys:
                    raise OMSExecutionError("aggregate durable fills must have unique dedupe identities")
                metadata = fill.get("metadata")
                if not isinstance(metadata, Mapping):
                    raise OMSExecutionError(f"aggregate durable fill {fill_id} lacks provenance metadata")
                if str(metadata.get(self._BROKER_FILL_ACCOUNT_ID_KEY, "")) != account.id:
                    raise OMSExecutionError(f"aggregate durable fill {fill_id} account provenance mismatch")
                if str(metadata.get("_external_order_id", "")) != external_order_id:
                    raise OMSExecutionError(f"aggregate durable fill {fill_id} order provenance mismatch")
                if str(metadata.get("_instrument_id", "")) != str(leg.get("instrument_id")):
                    raise OMSExecutionError(f"aggregate durable fill {fill_id} instrument provenance mismatch")
                if str(fill.get("external_order_id") or external_order_id) != external_order_id:
                    raise OMSExecutionError(f"aggregate durable fill {fill_id} external order mismatch")
                if str(fill.get("account_id") or account.id) != account.id:
                    raise OMSExecutionError(f"aggregate durable fill {fill_id} account mismatch")
                if str(fill.get("order_leg_id")) != str(leg.get("id")):
                    raise OMSExecutionError(f"aggregate durable fill {fill_id} leg ownership mismatch")
                alias_mismatches = self._account_alias_mismatches(account, metadata=metadata)
                if alias_mismatches:
                    raise OMSExecutionError(
                        f"aggregate durable fill {fill_id} has foreign account aliases: {alias_mismatches}"
                    )
                attempt_ids.add(attempt_id)
                external_order_ids.add(external_order_id)
                durable_fill_ids.add(fill_id)
                durable_dedupe_keys.add(dedupe_key)
                rows.append(
                    {
                        "intent": intent,
                        "leg": leg,
                        "attempt": attempt,
                        "fill": fill,
                        "quantity": requested,
                        "price": fill_price,
                        "attempt_id": attempt_id,
                        "external_order_id": external_order_id,
                        "fill_id": fill_id,
                        "dedupe_key": dedupe_key,
                    }
                )

        if not rows:
            raise OMSExecutionError("aggregate round-trip has no filled legs")

        # A fill/order identity already named by a different durable closure
        # cannot be re-used as a counter-fill.  The same exact group is
        # idempotent; every other overlap is a hard contradiction.
        def closure_tokens(value: object, key: str = "") -> set[str]:
            tokens: set[str] = set()
            normalized_key = key.lower()
            is_identity = any(
                marker in normalized_key
                for marker in (
                    "external_order_id",
                    "external_fill_id",
                    "attempt_id",
                    "broker_order_id",
                    "fill_id",
                    "dedupe_key",
                )
            )
            if isinstance(value, Mapping):
                for raw_key, raw_value in value.items():
                    tokens.update(closure_tokens(raw_value, str(raw_key)))
            elif isinstance(value, (list, tuple, set, frozenset)):
                for item in value:
                    tokens.update(closure_tokens(item, key))
            elif is_identity and value not in (None, ""):
                tokens.add(str(value))
            return tokens

        closure_claims: list[tuple[set[str], set[str]]] = []
        candidate_id_set = set(requested_ids)
        for existing_row in self.repository.book_intents(account.id):
            existing_id = str(existing_row.get("id") or "")
            existing = self.repository.get_intent(existing_id)
            if existing is None:
                continue
            existing_metadata = existing.get("metadata")
            if not isinstance(existing_metadata, Mapping):
                continue
            for closure_key in (
                "verified_roundtrip_closure",
                "aggregate_roundtrip_closure",
                "compensated_partial_closure",
            ):
                closure = existing_metadata.get(closure_key)
                if not isinstance(closure, Mapping):
                    continue
                group_value = closure.get("intent_ids")
                if not isinstance(group_value, (list, tuple)):
                    group_value = closure.get("compensating_intent_ids")
                group = {str(value) for value in group_value} if isinstance(group_value, (list, tuple)) else set()
                tokens = closure_tokens(closure)
                closure_claims.append((group, tokens))
                if existing_id in candidate_id_set and group and group != candidate_id_set:
                    raise OMSExecutionError(
                        f"intent {existing_id} already belongs to a different durable closure"
                    )
        candidate_tokens = external_order_ids | attempt_ids | durable_fill_ids | durable_dedupe_keys
        for group, tokens in closure_claims:
            overlap = candidate_tokens.intersection(tokens)
            if overlap and group != candidate_id_set:
                raise OMSExecutionError(
                    f"aggregate counter-fill identity is already claimed by another closure: {sorted(overlap)}"
                )

        # Compute economic netting from actual leg sides, never from the
        # intent's desired/effective metadata.  This catches the historical
        # wrong-direction exit while requiring an opposite-side correction.
        totals: dict[str, Decimal] = {}
        entry_totals: dict[str, Decimal] = {}
        exit_rows_by_instrument: dict[str, list[dict[str, object]]] = {}
        for row in rows:
            leg = row["leg"]
            instrument_id = str(leg.get("instrument_id") or "").strip()
            if not instrument_id:
                raise OMSExecutionError("aggregate leg instrument identity is missing")
            quantity = row["quantity"]
            signed = quantity if str(leg.get("side")) == Side.BUY.value else -quantity
            totals[instrument_id] = totals.get(instrument_id, Decimal("0")) + signed
            intent_action = str(row["intent"].get("action"))
            if intent_action == IntentAction.ENTER.value:
                entry_totals[instrument_id] = entry_totals.get(instrument_id, Decimal("0")) + signed
            else:
                exit_rows_by_instrument.setdefault(instrument_id, []).append(row)
        if not totals or any(value != 0 for value in totals.values()):
            raise OMSExecutionError(
                f"aggregate economic signed quantities do not net flat: { {key: str(value) for key, value in totals.items()} }"
            )
        for instrument_id, entry_signed in entry_totals.items():
            if entry_signed == 0:
                raise OMSExecutionError(f"aggregate entry basis is zero for {instrument_id}")
            exits = exit_rows_by_instrument.get(instrument_id, [])
            if not exits:
                raise OMSExecutionError(f"aggregate has no corrective exit for {instrument_id}")
            entry_side = Side.BUY.value if entry_signed > 0 else Side.SELL.value
            if not any(str(row["leg"].get("side")) != entry_side for row in exits):
                raise OMSExecutionError(f"aggregate exits do not contain an opposite-side correction for {instrument_id}")
        if set(entry_totals) != set(exit_rows_by_instrument):
            raise OMSExecutionError("aggregate contains an instrument without both entry and exit evidence")

        observation_time = self._now()
        expected_start = min(parse_time(intent.get("created_at"), "aggregate intent created_at") for intent in intents)
        facts, history = self._strict_roundtrip_broker_evidence(
            account,
            expected_rows=rows,
            requested_start=expected_start,
            requested_end=observation_time,
        )

        open_issues = self.repository.open_reconciliation_issues(account.id)
        open_actions = self.repository.open_recovery_actions(account.id)
        if open_issues or open_actions:
            raise OMSExecutionError("cannot close aggregate round-trip while reconciliation blockers remain open")

        entry_intent_ids = {
            str(intent["id"])
            for intent in intents
            if str(intent.get("action")) == IntentAction.ENTER.value
        }
        wrong_direction_intent_ids: set[str] = set()
        corrective_intent_ids: set[str] = set()
        for instrument_id, exits in exit_rows_by_instrument.items():
            entry_signed = entry_totals[instrument_id]
            entry_side = Side.BUY.value if entry_signed > 0 else Side.SELL.value
            for row in exits:
                intent_id = str(row["intent"]["id"])
                if str(row["leg"].get("side")) == entry_side:
                    wrong_direction_intent_ids.add(intent_id)
                else:
                    corrective_intent_ids.add(intent_id)
        intent_roles = []
        for intent in intents:
            intent_rows = [row for row in rows if row["intent"]["id"] == intent["id"]]
            intent_roles.append(
                {
                    "intent_id": str(intent["id"]),
                    "action": str(intent["action"]),
                    "status_before": str(intent["status"]),
                    "book_id": book_id,
                    "leg_ids": [str(row["leg"]["id"]) for row in intent_rows],
                    "attempt_ids": [str(row["attempt_id"]) for row in intent_rows],
                    "external_order_ids": [str(row["external_order_id"]) for row in intent_rows],
                    "economic_role": (
                        "source_entry"
                        if str(intent["id"]) in entry_intent_ids
                        else "wrong_direction_incident"
                        if str(intent["id"]) in wrong_direction_intent_ids
                        else "corrective_exit"
                        if str(intent["id"]) in corrective_intent_ids
                        else "exit"
                    ),
                }
            )
        fill_proof = [
            {
                "intent_id": str(row["intent"]["id"]),
                "leg_id": str(row["leg"]["id"]),
                "attempt_id": str(row["attempt_id"]),
                "external_order_id": str(row["external_order_id"]),
                "durable_fill_id": str(row["fill_id"]),
                "dedupe_key": str(row["dedupe_key"]),
                "instrument_id": str(row["leg"]["instrument_id"]),
                "side": str(row["leg"]["side"]),
                "quantity": str(row["quantity"]),
                "price": str(row["price"]),
            }
            for row in rows
        ]
        closure = {
            "version": 1,
            "reason": "fresh_broker_and_historical_proof_of_verified_aggregate_roundtrip",
            "account_id": account.id,
            "book_id": book_id,
            "intent_ids": list(requested_ids),
            "intent_roles": intent_roles,
            "external_order_ids": sorted(external_order_ids),
            "attempt_ids": sorted(attempt_ids),
            "durable_fill_ids": sorted(durable_fill_ids),
            "dedupe_keys": sorted(durable_dedupe_keys),
            "fill_proof": fill_proof,
            "signed_quantity_totals": {key: str(value) for key, value in sorted(totals.items())},
            "incident": {
                "wrong_direction_intent_ids": sorted(wrong_direction_intent_ids),
                "corrective_intent_ids": sorted(corrective_intent_ids),
                "source_fills_retained": True,
            },
            "account_facts_captured_at": facts.captured_at.isoformat(),
            "historical_window": {
                "requested_start": history.requested_start.isoformat(),
                "requested_end": history.requested_end.isoformat(),
                "captured_at": history.captured_at.isoformat(),
                "order_count": len(history.orders),
                "fill_count": len(history.fills),
                "evidence_mode": history.execution_evidence_mode.value,
            },
        }
        self.repository.mark_verified_roundtrip_intents(
            requested_ids,
            closure_metadata=closure,
            now=observation_time,
            _resolution_capability=self.repository._resolution_capability(),
        )
        event_id = "verified-aggregate-roundtrip-closure:" + hashlib.sha256(
            "|".join(requested_ids).encode("utf-8")
        ).hexdigest()[:32]
        if not any(event.get("id") == event_id for event in self.repository.operational_events(account.id, limit=1000)):
            self.repository.record_operational_event(
                event_id=event_id,
                account_id=account.id,
                event_type="VERIFIED_AGGREGATE_ROUNDTRIP_CLOSED",
                mode=account.environment.value,
                outcome="COMPLETED",
                occurred_at=observation_time,
                summary="Closed one explicitly proven aggregate round-trip; incident fills and corrective fills remain auditable.",
                details=closure,
            )
        if self.repository.open_reconciliation_issues(account.id) or self.repository.open_recovery_actions(account.id):
            raise OMSExecutionError("aggregate closure left reconciliation blockers open")
        return {
            "intent_ids": requested_ids,
            "status": IntentStatus.COMPLETED.value,
            "book_id": book_id,
            "resolved_issue_keys": (),
            "resolved_action_keys": (),
            "proof": closure,
        }

    def _submit_leg(
        self,
        intent: OrderIntent,
        leg: OrderLeg,
        account: Account,
        *,
        stored_status: str,
    ) -> str:
        """Submit one leg, converting every post-submit exception to recon."""
        try:
            return self._submit_leg_impl(intent, leg, account, stored_status=stored_status)
        except Exception as exc:
            # Once the adapter call has been attempted, even a malformed
            # response or a durable-write failure may hide a broker-side
            # order.  Persist a blocker rather than allowing the caller to
            # continue with a later leg or retry blindly.
            self._record_post_submit_exception(intent, leg, account, exc)
            return "ambiguous"

    def _submit_leg_impl(
        self,
        intent: OrderIntent,
        leg: OrderLeg,
        account: Account,
        *,
        stored_status: str,
    ) -> str:
        attempts = self.repository.broker_orders_for_leg(leg.id)
        if attempts:
            last = attempts[-1]
            if last["status"] != BrokerOrderStatus.PREPARED.value:
                if stored_status != LegStatus.RECONCILIATION_REQUIRED.value:
                    self.repository.transition_leg(leg.id, LegStatus.RECONCILIATION_REQUIRED, now=self._now())
                return "ambiguous"
            broker_order_id = str(last["id"])
            attempt_number = int(last["attempt_number"])
            client_order_id = str(last["client_order_id"])
        else:
            if stored_status == LegStatus.PLANNED.value:
                self.repository.transition_leg(leg.id, LegStatus.SUBMITTING, now=self._now())
            elif stored_status != LegStatus.SUBMITTING.value:
                raise OMSExecutionError(f"cannot prepare attempt from leg status {stored_status}")
            broker_order_id = str(uuid.uuid4())
            attempt_number, client_order_id = self.repository.create_broker_order(
                broker_order_id=broker_order_id,
                order_leg_id=leg.id,
                account_id=account.id,
                broker=account.broker,
                attempt_number=None,
                client_order_id=None,
                submitted_quantity=leg.quantity,
                now=self._now(),
            )

        self.repository.transition_broker_order(broker_order_id, BrokerOrderStatus.SUBMITTING, now=self._now())
        request = BrokerSubmitRequest(
            broker_order_id=broker_order_id,
            account_id=account.id,
            broker=account.broker,
            order_leg=leg,
            client_order_id=client_order_id,
            attempt_number=attempt_number,
            allow_extended_hours=intent.execution_policy.allow_extended_hours,
            execution_session=intent.execution_policy.execution_session,
        )
        try:
            result = self.adapter.submit_order(account, request)
        except Exception as exc:
            self.repository.record_submission(
                broker_order_id,
                status=BrokerOrderStatus.UNKNOWN,
                external_order_id=None,
                metadata={"exception": type(exc).__name__, "message": str(exc)},
                now=self._now(),
            )
            self.repository.transition_leg(leg.id, LegStatus.RECONCILIATION_REQUIRED, now=self._now())
            self._require_reconciliation(
                intent.id,
                account,
                category="BROKER_SUBMISSION_EXCEPTION",
                entity_type="BROKER_ORDER",
                entity_key=broker_order_id,
                details={
                    "exception_type": type(exc).__name__,
                    "message": str(exc),
                    "after_submit_invocation": True,
                },
            )
            self._record_recovery_action(
                intent=self._required_intent(intent.id),
                account=account,
                action_key=f"SUBMISSION_EXCEPTION:{broker_order_id}",
                state="RECONCILIATION_REQUIRED",
                summary="Broker submission raised after invocation; order exposure is unknown and operator reconciliation is required.",
                observed_positions=self._observe_positions(account)[0],
                remaining_quantities=self._remaining_quantities(self._required_intent(intent.id)),
                allowed_next_steps=("refresh_broker_facts", "reconcile_broker_order", "operator_review"),
                metadata={"broker_order_id": broker_order_id, "exception_type": type(exc).__name__, "message": str(exc)},
            )
            return "ambiguous"

        if result.broker_order_id != broker_order_id:
            self.repository.record_submission(
                broker_order_id,
                status=BrokerOrderStatus.UNKNOWN,
                external_order_id=result.external_order_id,
                metadata=self._submission_metadata(result),
                now=self._now(),
            )
            self.repository.transition_leg(leg.id, LegStatus.RECONCILIATION_REQUIRED, now=self._now())
            self._require_reconciliation(
                intent.id,
                account,
                category="BROKER_SUBMISSION_ID_CONFLICT",
                entity_type="BROKER_ORDER",
                entity_key=broker_order_id,
                details={
                    "expected_broker_order_id": broker_order_id,
                    "observed_broker_order_id": result.broker_order_id,
                },
            )
            self._record_recovery_action(
                intent=self._required_intent(intent.id),
                account=account,
                action_key=f"SUBMISSION_ID_CONFLICT:{broker_order_id}",
                state="RECONCILIATION_REQUIRED",
                summary="Adapter returned a broker order ID different from the durable attempt; broker exposure is ambiguous.",
                observed_positions=self._observe_positions(account)[0],
                remaining_quantities=self._remaining_quantities(self._required_intent(intent.id)),
                allowed_next_steps=("refresh_broker_facts", "reconcile_broker_order", "operator_review"),
                metadata={"expected_broker_order_id": broker_order_id, "observed_broker_order_id": result.broker_order_id},
            )
            if self._has_submission_fill_evidence(
                result,
                expected_quantity=leg.quantity,
                expected_instrument_id=leg.instrument_id,
            ):
                self._record_submission_fill_evidence(
                    intent=intent,
                    leg=leg,
                    account=account,
                    broker_order_id=broker_order_id,
                    result=result,
                    status=BrokerOrderStatus.UNKNOWN,
                )
            return "ambiguous"

        # A submit response is not a normalized fill feed. Even when the
        # provider claims WORKING/FILLED, any positive or malformed deal
        # payload must stop the leg and await durable fill evidence.
        if self._has_submission_fill_evidence(
            result,
            expected_quantity=leg.quantity,
            expected_instrument_id=leg.instrument_id,
        ):
            self._record_submission_fill_evidence(
                intent=intent,
                leg=leg,
                account=account,
                broker_order_id=broker_order_id,
                result=result,
            )
            return "ambiguous"

        # A provider can reject a request before creating any broker-side
        # order.  Only this narrow, positively identified outcome is safe to
        # finish as a rejection; all other rejection-shaped responses remain
        # reconciliation-required because they may hide a submitted order or
        # fill.
        if self._is_definite_no_submit_rejection(result):
            self.repository.record_submission(
                broker_order_id,
                status=BrokerOrderStatus.REJECTED,
                external_order_id=result.external_order_id,
                metadata=self._submission_metadata(result, definite_no_submit=True),
                now=self._now(),
            )
            self.repository.transition_leg(leg.id, LegStatus.REJECTED, now=self._now())
            return "rejected"

        if self._is_ambiguous(result) or result.accepted is False or result.status is BrokerOrderStatus.REJECTED:
            self.repository.record_submission(
                broker_order_id,
                status=BrokerOrderStatus.UNKNOWN,
                external_order_id=result.external_order_id,
                metadata=self._submission_metadata(result),
                now=self._now(),
            )
            self.repository.transition_leg(leg.id, LegStatus.RECONCILIATION_REQUIRED, now=self._now())
            return "ambiguous"

        accepted_status = result.status
        if accepted_status not in {
            BrokerOrderStatus.WORKING,
            BrokerOrderStatus.PARTIALLY_FILLED,
            BrokerOrderStatus.FILLED,
        }:
            accepted_status = BrokerOrderStatus.WORKING
        self.repository.record_submission(
            broker_order_id,
            status=accepted_status,
            external_order_id=result.external_order_id,
            metadata={
                "provider_status": result.status.value,
                "reported_cumulative_fill": (
                    str(result.cumulative_filled_quantity)
                    if result.cumulative_filled_quantity is not None
                    else None
                ),
                "cumulative_fill_known": result.cumulative_filled_quantity is not None,
                "no_submit_asserted": result.no_submit_asserted,
                "no_fill_asserted": result.no_fill_asserted,
                "raw_payload": self._safe_provider_payload(result.raw_payload),
            },
            now=self._now(),
            submitted_at=result.submitted_at,
        )
        if accepted_status is BrokerOrderStatus.WORKING:
            self.repository.transition_leg(leg.id, LegStatus.WORKING, now=self._now())
            return "accepted"

        # Quantity without fill price/deal evidence cannot safely update the
        # owned position ledger. Preserve the broker status and reconcile.
        self.repository.transition_leg(leg.id, LegStatus.RECONCILIATION_REQUIRED, now=self._now())
        return "ambiguous"

    def _record_post_submit_exception(
        self,
        intent: OrderIntent,
        leg: OrderLeg,
        account: Account,
        error: Exception,
    ) -> None:
        """Durably quarantine an exception raised after submit invocation."""
        broker_order_id = leg.id
        try:
            attempts = self.repository.broker_orders_for_leg(leg.id)
            if attempts:
                broker_order_id = str(attempts[-1]["id"])
                current = str(attempts[-1]["status"])
                if current not in {
                    BrokerOrderStatus.FILLED.value,
                    BrokerOrderStatus.REJECTED.value,
                    BrokerOrderStatus.CANCELLED.value,
                    BrokerOrderStatus.FAILED.value,
                }:
                    self.repository.transition_broker_order(
                        broker_order_id,
                        BrokerOrderStatus.UNKNOWN,
                        now=self._now(),
                    )
        except Exception:
            # The durable reconciliation write below is the safety boundary;
            # an additional best-effort order transition must not mask it.
            pass
        try:
            current_intent = self._required_intent(intent.id)
            current_leg = next(item for item in current_intent["legs"] if item["id"] == leg.id)
            self._force_leg_reconciliation(leg.id, str(current_leg["status"]))
        except Exception:
            pass
        details = {
            "broker_order_id": broker_order_id,
            "exception_type": type(error).__name__,
            "message": str(error),
            "after_submit_invocation": True,
        }
        try:
            self._require_reconciliation(
                intent.id,
                account,
                category="BROKER_SUBMISSION_EXCEPTION",
                entity_type="BROKER_ORDER",
                entity_key=broker_order_id,
                details=details,
            )
        except Exception:
            # Never re-raise the original provider/DB exception; the caller
            # receives an ambiguous outcome and the surrounding intent is
            # already prevented from submitting subsequent legs.
            pass
        try:
            self._record_recovery_action(
                intent=self._required_intent(intent.id),
                account=account,
                action_key=f"SUBMISSION_EXCEPTION:{broker_order_id}",
                state="RECONCILIATION_REQUIRED",
                summary="An exception occurred after broker submission was invoked; order exposure is unknown and operator reconciliation is required.",
                observed_positions=self._observe_positions(account)[0],
                remaining_quantities=self._remaining_quantities(self._required_intent(intent.id)),
                allowed_next_steps=("refresh_broker_facts", "reconcile_broker_order", "operator_review"),
                metadata=details,
            )
        except Exception:
            pass

    @staticmethod
    def _is_ambiguous(result: BrokerSubmissionResult) -> bool:
        return bool(result.ambiguous or result.accepted is None or (result.accepted and not result.external_order_id))

    @classmethod
    def _is_definite_no_submit_rejection(cls, result: BrokerSubmissionResult) -> bool:
        return bool(
            result.accepted is False
            and result.status is BrokerOrderStatus.REJECTED
            and not result.ambiguous
            and result.external_order_id is None
            and result.no_submit_asserted
            and result.no_fill_asserted
            and result.cumulative_filled_quantity is not None
            and result.cumulative_filled_quantity == 0
            and not cls._payload_has_submission_evidence(result.raw_payload)
        )

    @classmethod
    def _authoritative_command_no_fill(
        cls,
        result: BrokerSubmissionResult,
        *,
        expected_quantity: Decimal | None,
        expected_instrument_id: str | None,
        evidence_detector: Callable[[object], bool],
    ) -> bool:
        """Validate one adapter-authoritative terminal command response.

        Command adapters commonly wrap their normalized row in a ``response``
        field.  Treating that field as a case-sensitive dictionary lookup is
        unsafe: a provider or SDK can return ``Response``, ``responsePayload``
        or another formatting of the same alias alongside a contradictory
        sibling.  Normalize and inspect every branch, and only accept a
        single response row after all non-response siblings are proven free of
        order/fill evidence.
        """
        if (
            result.authority != ADAPTER_ORDER_SNAPSHOT_AUTHORITY
            or not result.no_fill_asserted
            or result.status
            not in {
                BrokerOrderStatus.CANCELLED,
                BrokerOrderStatus.REJECTED,
                BrokerOrderStatus.FAILED,
            }
        ):
            return False
        if expected_quantity is not None and result.submitted_quantity != expected_quantity:
            return False
        if expected_instrument_id is not None and result.instrument_id != expected_instrument_id:
            return False
        try:
            payload = coerce_provider_payload(result.raw_payload)
        except ProviderPayloadError:
            return False
        if not isinstance(payload, Mapping):
            return False

        response_aliases = {
            "response",
            "response_payload",
            "response_data",
            "response_body",
            "response_result",
        }
        response_values: list[object] = []
        for raw_key, value in payload.items():
            if normalize_provider_key(raw_key) in response_aliases:
                response_values.append(value)
            elif evidence_detector(value):
                # Sibling branches are never ignored merely because a clean
                # response branch exists.  In particular, Response + response
                # must not allow a positive dealt_qty to be hidden.
                return False
        if len(response_values) != 1:
            # Multiple aliases are ambiguous even when they happen to look
            # similar; accepting one would make the result depend on mapping
            # insertion order.  A single branch is the only safe authority.
            return False
        response = response_values[0]
        if isinstance(response, (list, tuple)):
            if len(response) != 1:
                return False
            response = response[0]
        return cls._authoritative_raw_no_fill(
            raw=response,
            expected_order_id=result.external_order_id,
            expected_status=result.status,
            expected_quantity=(result.submitted_quantity or expected_quantity),
            expected_instrument_id=(result.instrument_id or expected_instrument_id),
        )

    @staticmethod
    def _payload_has_submission_evidence(
        payload: object,
        *,
        _depth: int = 0,
        _order_context: bool = False,
    ) -> bool:
        """Return true when optional provider payload contains order/fill facts.

        Provider responses often wrap order rows several levels down under an
        ``orders``/``order_list`` key.  A top-level diagnostic ``status=OK`` is
        benign, but an order-shaped row carrying an ``id`` or lifecycle status
        is not a no-submit/no-fill proof.  Keep that distinction explicit so
        ordinary primitive diagnostics remain backwards compatible.
        """
        order_containers = {
            "order",
            "orders",
            "broker_order",
            "order_list",
            "order_rows",
            "open_orders",
            "broker_orders",
            "order_data",
        }
        order_id_keys = {"order_id", "orderid", "external_order_id", "broker_order_id"}
        lifecycle_keys = {
            "order_status",
            "submission_status",
            "submit_status",
            "broker_status",
        }
        broker_lifecycle_statuses = {
            "SUBMITTED", "SUBMITTING", "WAITING_SUBMIT", "WAITING_VERIFY", "PENDING", "WORKING",
            "FILLED", "FILLED_ALL", "FULLY_FILLED", "PARTIALLY_FILLED", "FILLED_PART", "PARTIAL_FILLED",
            "CANCELLED", "CANCELLED_ALL", "CANCELLED_PART", "FILL_CANCELLED", "DELETED", "DISABLED",
            "REJECTED", "SUBMIT_FAILED", "FAILED", "TIMEOUT", "UNKNOWN",
        }
        if _depth == 0:
            try:
                payload = coerce_provider_payload(payload)
            except ProviderPayloadError:
                return True
        if isinstance(payload, Mapping):
            for key, value in payload.items():
                normalized = normalize_provider_key(key)
                if is_execution_evidence_key(normalized) and value not in (None, ""):
                    # A stable provider execution identity is material even
                    # when cumulative dealt quantity is reported as zero.
                    # It may identify a late/hidden fill, so no clean
                    # no-submit/no-fill assertion can ignore it.
                    return True
                if normalized in order_containers:
                    if value not in (None, "", 0, "0", (), [], {}):
                        nested_evidence = GenericOMS._payload_has_submission_evidence(
                            value,
                            _depth=_depth + 1,
                            _order_context=True,
                        )
                        # A non-empty order-shaped container that cannot be
                        # parsed into lifecycle evidence is itself ambiguous.
                        if nested_evidence or isinstance(value, (Mapping, list, tuple)) or value is not None:
                            return True
                    continue
                if normalized in order_id_keys or (_order_context and normalized == "id"):
                    if value not in (None, "", 0, "0"):
                        return True
                if normalized in lifecycle_keys or (
                    normalized in {"status", "state", "order_state"}
                ):
                    token = str(value or "").strip().upper().replace("-", "_").split(".")[-1]
                    # Plain diagnostic status values such as OK/SUCCESS stay
                    # benign, while recognizable broker lifecycle values are
                    # evidence even under arbitrary result/list wrappers.
                    if value not in (None, "") and (
                        normalized in lifecycle_keys
                        or _order_context
                        or token in broker_lifecycle_statuses
                    ):
                        return True
                if normalized in {
                    "fill_qty",
                    "filled_qty",
                    "dealt_qty",
                    "filled_quantity",
                    "cumulative_filled_quantity",
                    "deal_qty",
                    "deal_quantity",
                    "executed_qty",
                    "executed_quantity",
                }:
                    try:
                        parsed = Decimal(str(value))
                        if not parsed.is_finite() or parsed != 0:
                            return True
                    except (InvalidOperation, TypeError, ValueError):
                        return True
                    continue
                if normalized in {"fills", "deals", "fill_list", "deal_list"} and value:
                    return True
                if normalized in {"fills", "deals", "fill_list", "deal_list"}:
                    continue
                if normalized in {
                    "fill_price",
                    "dealt_avg_price",
                    "avg_fill_price",
                    "filled_at",
                    "fill_time",
                    "deal_time",
                    "executed_at",
                    "execution_time",
                } and value not in (None, ""):
                    # Even malformed price/time fields are evidence that the
                    # provider may have seen a fill; they are not a clean
                    # no-submit rejection.
                    return True
                if any(token in normalized for token in ("fill", "deal", "execut")):
                    # Provider-specific fill-shaped fields are evidence even
                    # when this core does not know their exact schema.  An
                    # empty/odd value is still an unparsed provider claim,
                    # unlike a recognized zero quantity or empty collection.
                    return True
                if GenericOMS._payload_has_submission_evidence(
                    value,
                    _depth=_depth + 1,
                    _order_context=_order_context,
                ):
                    return True
        elif isinstance(payload, (list, tuple)):
            return any(
                GenericOMS._payload_has_submission_evidence(
                    item,
                    _depth=_depth + 1,
                    _order_context=_order_context,
                )
                for item in payload
            )
        elif payload is not None and not isinstance(payload, (str, int, float, bool, Decimal, datetime)):
            # Provider payloads must be recursively inspectable before a
            # terminal no-fill/no-submit claim can be trusted.  Opaque table
            # or SDK objects are possible evidence, never clean absence.
            return True
        return False

    @classmethod
    def _authoritative_raw_no_fill(
        cls,
        *,
        raw: object,
        expected_order_id: str | None,
        expected_status: BrokerOrderStatus,
        expected_quantity: Decimal | None = None,
        expected_instrument_id: str | None = None,
        expected_external_symbol: str | None = None,
    ) -> bool:
        """Validate a source-bound terminal zero-fill order row.

        Moomoo's normalized order row necessarily contains its own order ID
        and terminal status.  Those identity fields are not submission
        evidence when the row has come through the adapter-authoritative
        snapshot path.  Every other nested provider field remains visible to
        the normal evidence scanner, so a deal/fill claim still blocks clean
        cancellation.
        """
        try:
            raw = coerce_provider_payload(raw)
        except ProviderPayloadError:
            return False
        if not isinstance(raw, Mapping):
            return False
        def first_alias(names: set[str]) -> object | None:
            return next(
                (
                    value
                    for raw_key, value in raw.items()
                    if normalize_provider_key(raw_key) in names and value not in (None, "")
                ),
                None,
            )

        order_id = first_alias({"order_id", "orderid", "external_order_id", "broker_order_id", "id"})
        if expected_order_id not in (None, "") and str(order_id) != str(expected_order_id):
            return False
        def alias_values(names: set[str]) -> list[object]:
            values: list[object] = []
            for raw_key, value in raw.items():
                if normalize_provider_key(raw_key) in names and value not in (None, ""):
                    values.append(value)
            return values

        order_ids = alias_values({"order_id", "orderid", "external_order_id", "broker_order_id", "id"})
        if order_ids and any(str(value).strip() != str(order_ids[0]).strip() for value in order_ids[1:]):
            return False
        raw_status = first_alias({"order_status", "status"})
        status_token = str(raw_status or "").strip().upper().replace("-", "_").split(".")[-1]
        status_values = [
            str(value).strip().upper().replace("-", "_").split(".")[-1]
            for value in alias_values({"order_status", "status"})
        ]
        if status_values and any(value != status_values[0] for value in status_values[1:]):
            return False
        status_aliases = {
            BrokerOrderStatus.PREPARED: {"PREPARED"},
            BrokerOrderStatus.SUBMITTING: {"SUBMITTING"},
            BrokerOrderStatus.WORKING: {
                "SUBMITTED", "SUBMITTING", "WAITING_SUBMIT", "WAITING_VERIFY", "PENDING", "WORKING",
            },
            BrokerOrderStatus.CANCELLED: {"CANCELLED", "CANCELLED_ALL", "DELETED", "DISABLED"},
            BrokerOrderStatus.REJECTED: {"REJECTED", "SUBMIT_FAILED", "FAILED", "TIMEOUT"},
            BrokerOrderStatus.FAILED: {"FAILED", "SUBMIT_FAILED", "TIMEOUT"},
        }
        if status_token not in status_aliases.get(expected_status, set()):
            return False
        filled_values = alias_values(
            {"dealt_qty", "filled_qty", "filled_quantity", "cumulative_filled_quantity"}
        )
        filled = filled_values[0] if filled_values else None
        if filled is None:
            return False
        try:
            parsed_filled = Decimal(str(filled))
            parsed_filled_values = [Decimal(str(value)) for value in filled_values]
        except (InvalidOperation, TypeError, ValueError):
            return False
        if (
            not parsed_filled.is_finite()
            or parsed_filled != 0
            or any(not value.is_finite() or value != parsed_filled for value in parsed_filled_values)
        ):
            return False
        quantity_values = alias_values({"qty", "quantity"})
        if quantity_values:
            try:
                parsed_quantities = [Decimal(str(value)) for value in quantity_values]
            except (InvalidOperation, TypeError, ValueError):
                return False
            if any(not value.is_finite() or value != parsed_quantities[0] for value in parsed_quantities[1:]):
                return False
        if expected_quantity is not None:
            if not quantity_values:
                return False
            try:
                expected = Decimal(str(expected_quantity))
            except (InvalidOperation, TypeError, ValueError):
                return False
            if not expected.is_finite() or expected <= 0 or any(value != expected for value in parsed_quantities):
                return False
        instrument_values = alias_values({"instrument_id", "internal_instrument_id", "oms_instrument_id"})
        if instrument_values and any(str(value).strip() != str(instrument_values[0]).strip() for value in instrument_values[1:]):
            return False
        # A terminal command/snapshot authority is only meaningful when the
        # returned provider row binds to an instrument.  The adapter maps
        # external symbols to the canonical ID before exposing the result;
        # this raw validator still requires all populated symbol aliases to
        # agree and, when a canonical ID is present, to match the expected
        # persisted leg.
        symbol_values = alias_values({"code", "symbol", "ticker"})
        if expected_instrument_id is not None:
            if not symbol_values and not instrument_values:
                return False
            if instrument_values and str(instrument_values[0]).strip() != str(expected_instrument_id).strip():
                return False
        if expected_external_symbol is not None:
            expected_symbol = str(expected_external_symbol).strip().upper()
            if not expected_symbol or not symbol_values or any(value != expected_symbol for value in symbol_values):
                return False
        identity_keys = {
            "order_id", "orderid", "external_order_id", "broker_order_id", "id",
            "order_status", "status", "code", "symbol", "ticker", "trd_side", "side",
            "qty", "quantity", "remark", "client_order_id", "acc_id", "account_id",
            "external_account_id", "trd_env", "trading_env", "create_time", "updated_time",
            "order_time",
        }
        remaining: dict[object, object] = {}
        zero_fill_price_keys = {"fill_price", "dealt_avg_price", "avg_fill_price"}
        for key, value in raw.items():
            normalized = normalize_provider_key(key)
            if normalized in identity_keys:
                continue
            if normalized in zero_fill_price_keys and value not in (None, ""):
                try:
                    if Decimal(str(value)).is_finite() and Decimal(str(value)) == 0:
                        continue
                except (InvalidOperation, TypeError, ValueError):
                    pass
            remaining[key] = value
        # The raw source-bound row is allowed to omit only its expected
        # identity fields.  Scan all remaining primitive content with the
        # lifecycle-aware detector so nested order/status claims (not just
        # numeric deals) invalidate the zero-fill exception.
        return not cls._payload_has_submission_evidence(remaining)

    @classmethod
    def _authoritative_snapshot_metadata_no_fill(
        cls,
        *,
        metadata: object,
        expected_order_id: str,
        expected_status: BrokerOrderStatus,
        expected_quantity: Decimal | None,
        expected_instrument_id: str | None,
    ) -> bool:
        """Validate the complete adapter-authenticated snapshot envelope.

        The raw row is the one validated provider branch, but wrapper aliases
        and every sibling branch remain part of the evidence boundary.  A
        second ``RAW``/``raw`` branch or a serialized ``Response`` branch is
        ambiguous rather than silently ignored.
        """
        try:
            normalized_metadata = coerce_provider_payload(metadata)
        except ProviderPayloadError:
            return False
        if not isinstance(normalized_metadata, Mapping):
            return False
        raw_values = [
            value
            for key, value in normalized_metadata.items()
            if normalize_provider_key(key) == "raw"
        ]
        if len(raw_values) != 1:
            return False
        raw = raw_values[0]
        if any(
            cls._payload_has_submission_evidence(value)
            for key, value in normalized_metadata.items()
            if normalize_provider_key(key) != "raw"
        ):
            return False
        external_symbol = next(
            (
                str(value)
                for key, value in normalized_metadata.items()
                if normalize_provider_key(key) == "external_symbol" and value not in (None, "")
            ),
            None,
        )
        return cls._authoritative_raw_no_fill(
            raw=raw,
            expected_order_id=expected_order_id,
            expected_status=expected_status,
            expected_quantity=expected_quantity,
            expected_instrument_id=expected_instrument_id,
            expected_external_symbol=external_symbol,
        )

    @classmethod
    def _authoritative_snapshot_no_fill(cls, snapshot: BrokerOrderSnapshot) -> bool:
        if snapshot.authority != ADAPTER_ORDER_SNAPSHOT_AUTHORITY:
            return False
        if snapshot.status not in {
            BrokerOrderStatus.PREPARED,
            BrokerOrderStatus.SUBMITTING,
            BrokerOrderStatus.WORKING,
            BrokerOrderStatus.CANCELLED,
            BrokerOrderStatus.REJECTED,
            BrokerOrderStatus.FAILED,
        }:
            return False
        return cls._authoritative_snapshot_metadata_no_fill(
            metadata=snapshot.metadata,
            expected_order_id=snapshot.external_order_id,
            expected_status=snapshot.status,
            expected_quantity=snapshot.quantity,
            expected_instrument_id=snapshot.instrument_id,
        )

    @classmethod
    def _snapshot_no_fill_is_authoritative(
        cls,
        snapshot: BrokerOrderSnapshot,
        *,
        expected_quantity: Decimal | None = None,
        expected_instrument_id: str | None = None,
    ) -> bool:
        """Return whether a terminal snapshot proves cumulative zero safely."""
        if snapshot.filled_quantity != 0:
            return False
        if snapshot.authority == ADAPTER_ORDER_SNAPSHOT_AUTHORITY:
            if expected_quantity is not None and snapshot.quantity != expected_quantity:
                return False
            if expected_instrument_id is not None and snapshot.instrument_id != expected_instrument_id:
                return False
            return bool(snapshot.no_fill_asserted and cls._authoritative_snapshot_no_fill(snapshot))
        metadata = snapshot.metadata if isinstance(snapshot.metadata, Mapping) else {}
        # Inspect the complete evidence envelope.  Providers and adapters
        # commonly retain both a normalized ``raw`` row and a sibling
        # ``response``/``payload`` object; selecting only one branch can turn
        # a positive deal claim in the other branch into a false clean
        # cancellation/rejection.
        if cls._payload_has_submission_evidence(metadata):
            return False
        # A terminal provider fact must carry the explicit normalized no-fill
        # assertion. Raw metadata may be absent, but its presence is parsed
        # above and any unknown fill-shaped field remains contradictory.
        return bool(snapshot.no_fill_asserted)

    @classmethod
    def _event_no_fill_is_authoritative(
        cls,
        event: BrokerOrderEvent,
        *,
        expected_quantity: Decimal | None = None,
        expected_instrument_id: str | None = None,
    ) -> bool:
        """Return whether a terminal event proves cumulative zero safely."""
        if event.cumulative_filled_quantity != 0:
            return False
        try:
            metadata = coerce_provider_payload(event.metadata)
        except ProviderPayloadError:
            return False
        if not isinstance(metadata, Mapping):
            return False
        if event.authority == ADAPTER_ORDER_SNAPSHOT_AUTHORITY:
            snapshot_values = [
                value
                for key, value in metadata.items()
                if normalize_provider_key(key) == "authoritative_snapshot"
            ]
            if len(snapshot_values) != 1:
                return False
            snapshot_metadata = snapshot_values[0]
            if any(
                cls._payload_has_submission_evidence(value)
                for key, value in metadata.items()
                if normalize_provider_key(key) != "authoritative_snapshot"
            ):
                return False
            if event.broker_status in {
                BrokerOrderStatus.CANCELLED,
                BrokerOrderStatus.REJECTED,
                BrokerOrderStatus.FAILED,
            }:
                return bool(
                    event.no_fill_asserted
                    and cls._authoritative_snapshot_metadata_no_fill(
                        metadata=snapshot_metadata,
                        expected_order_id=event.external_order_id,
                        expected_status=event.broker_status,
                        expected_quantity=expected_quantity,
                        expected_instrument_id=expected_instrument_id,
                    )
                )
        if cls._payload_has_submission_evidence(metadata):
            return False
        return bool(event.no_fill_asserted)

    @staticmethod
    def _payload_has_positive_fill_evidence(
        payload: object,
        *,
        _order_context: bool = False,
        _depth: int = 0,
    ) -> bool:
        """Detect provider fill/deal facts without trusting them as fills.

        Submission and cancellation responses are not normalized fill feeds.
        A positive or malformed deal-shaped value therefore only creates a
        durable blocker; it never allocates position quantity here.
        """
        quantity_keys = {
            "fill_qty",
            "filled_qty",
            "dealt_qty",
            "filled_quantity",
            "cumulative_filled_quantity",
            "deal_qty",
            "deal_quantity",
            "executed_qty",
            "executed_quantity",
        }
        collection_keys = {"fills", "deals", "fill_list", "deal_list", "deal_detail"}
        if _depth == 0:
            try:
                payload = coerce_provider_payload(payload)
            except ProviderPayloadError:
                return True
        if isinstance(payload, Mapping):
            for key, value in payload.items():
                normalized = normalize_provider_key(key)
                if is_execution_evidence_key(normalized) and value not in (None, ""):
                    return True
                if normalized in quantity_keys:
                    try:
                        quantity = Decimal(str(value))
                    except (InvalidOperation, TypeError, ValueError):
                        # A present but malformed provider quantity is
                        # evidence ambiguity, including null/empty sentinels;
                        # only an absent key means no evidence.
                        return True
                    if not quantity.is_finite() or quantity != 0:
                        return True
                    continue
                if normalized in collection_keys and value not in (None, "", 0, "0", (), [], {}):
                    # A non-empty provider deal collection is possible fill
                    # evidence even when its schema is not understood.
                    return True
                if normalized in collection_keys:
                    continue
                if normalized in {
                    "fill_price",
                    "dealt_avg_price",
                    "avg_fill_price",
                    "filled_at",
                    "fill_time",
                    "deal_time",
                    "executed_at",
                    "execution_time",
                } and value not in (None, ""):
                    return True
                if any(token in normalized for token in ("fill", "deal", "execut")):
                    return True
                if GenericOMS._payload_has_positive_fill_evidence(
                    value,
                    _order_context=_order_context,
                    _depth=_depth + 1,
                ):
                    return True
        elif isinstance(payload, (list, tuple)):
            return any(
                GenericOMS._payload_has_positive_fill_evidence(
                    item,
                    _order_context=_order_context,
                    _depth=_depth + 1,
                )
                for item in payload
            )
        elif payload is not None and not isinstance(payload, (str, int, float, bool, Decimal, datetime)):
            # An opaque provider value cannot prove that a cancellation or
            # rejection carried no deal.  Treat it as possible evidence so
            # the caller records a durable reconciliation blocker instead of
            # silently resolving a clean terminal action.
            return True
        return False

    @staticmethod
    def _broker_fact_metadata_blockers(
        account: Account,
        metadata: object,
        *,
        allow_cumulative_order_evidence: bool = False,
    ) -> list[str]:
        """Validate account-fact diagnostics before allowing an empty result.

        ``BrokerFactSnapshot.metadata`` is diagnostic only.  It is not a
        second, untyped provider-facts channel: opaque values, account aliases
        that disagree with the canonical account, and execution-shaped fields
        are all blockers even when the normalized child collections are empty.
        Primitive diagnostics such as ``source`` and ``query_complete`` remain
        compatible with existing adapters.
        """
        try:
            metadata = coerce_provider_payload(metadata)
        except ProviderPayloadError as exc:
            return [f"metadata.provider_payload={exc}"]

        expected_internal = str(account.id)
        expected_external = str(account.external_account_id)
        expected_broker = str(account.broker)
        expected_environment = str(account.environment.value).upper()
        environment_aliases = {expected_environment}
        if expected_environment == "SIM":
            environment_aliases.add("SIMULATE")
        elif expected_environment == "LIVE":
            environment_aliases.add("REAL")

        account_keys = {
            "account_id",
            "internal_account_id",
            "oms_account_id",
            "external_account_id",
            "acc_id",
            "account_number",
            "trd_acc_id",
            "trade_account_id",
            "account",
            "account_alias",
            "account_identifier",
            "broker",
            "broker_name",
            "broker_id",
            "provider",
            "environment",
            "trading_environment",
            "trd_env",
            "trading_env",
        }
        execution_keys = {
            "order_id",
            "orderid",
            "external_order_id",
            "broker_order_id",
            "client_order_id",
            "fill_qty",
            "filled_qty",
            "dealt_qty",
            "filled_quantity",
            "cumulative_filled_quantity",
            "deal_qty",
            "deal_quantity",
            "executed_qty",
            "executed_quantity",
            "fill_price",
            "dealt_avg_price",
            "avg_fill_price",
            "filled_at",
            "fill_time",
            "deal_time",
            "executed_at",
            "execution_time",
            "fill_list",
            "deal_list",
            "fills",
            "deals",
            "position",
            "position_id",
            "position_qty",
            "position_quantity",
            "side",
            "trd_side",
            "code",
            "symbol",
            "instrument",
            "instrument_id",
            "dedupe_key",
            "order_status",
            "status",
        }
        provider_container_keys = {
            "raw",
            "response",
            "payload",
            "provider_payload",
            "provider_response",
            "data",
        }
        mismatches: set[str] = set()

        def scalar(value: object) -> bool:
            if value is None or isinstance(value, (str, int, bool, datetime)):
                return True
            if isinstance(value, float):
                return value == value and value not in (float("inf"), float("-inf"))
            if isinstance(value, Decimal):
                return value.is_finite()
            return False

        def empty_provider_value(value: object) -> bool:
            if value is None:
                return True
            if isinstance(value, str):
                return value == "" or value == "0"
            if isinstance(value, (int, float, Decimal, bool)):
                return value == 0
            if isinstance(value, (Mapping, list, tuple)):
                try:
                    return len(value) == 0
                except Exception:
                    return False
            return False

        def check_account(key: str, value: object, path: str) -> None:
            if value is None or (isinstance(value, str) and value == ""):
                return
            if not scalar(value):
                # Nested mappings/lists are still walked by the caller; an
                # opaque value at an identity key is never trustworthy.
                if isinstance(value, (Mapping, list, tuple)):
                    return
                mismatches.add(f"{path}.opaque_type={type(value).__name__}")
                return
            expected: set[str]
            if key in {"account_id", "internal_account_id", "oms_account_id"}:
                expected = {expected_internal}
            elif key in {
                "external_account_id",
                "acc_id",
                "account_number",
                "trd_acc_id",
                "trade_account_id",
            }:
                expected = {expected_external}
            elif key in {"broker", "broker_name", "broker_id", "provider"}:
                expected = {expected_broker}
            elif key in {"environment", "trading_environment", "trd_env", "trading_env"}:
                if str(value).upper().split(".")[-1] not in environment_aliases:
                    mismatches.add(f"{path}={value}")
                return
            else:
                expected = {expected_internal, expected_external}
            if str(value) not in expected:
                mismatches.add(f"{path}={value}")

        def walk(value: object, path: str = "metadata", *, account_context: bool = False) -> None:
            if isinstance(value, Mapping):
                try:
                    items = tuple(value.items())
                except Exception as exc:
                    mismatches.add(f"{path}.unreadable_mapping={type(exc).__name__}")
                    return
                for raw_key, raw_value in items:
                    key = normalize_provider_key(raw_key)
                    current = f"{path}.{key}"
                    if key in account_keys:
                        check_account(key, raw_value, current)
                    if account_context and key in {"id", "internal_id", "external_id"}:
                        check_account("account", raw_value, current)
                    if key in provider_container_keys and not empty_provider_value(raw_value):
                        mismatches.add(f"{current}.provider_payload")
                    # The Moomoo SIM deal endpoint is a documented bounded
                    # limitation.  Once an explicit verified baseline exists,
                    # this one diagnostic flag is no longer itself a blocker;
                    # every normalized order/fill fact is still validated
                    # below.  No other evidence-shaped metadata is exempted.
                    evidence_gap_marker = key == "fill_history_unsupported"
                    execution_key = (
                        not (allow_cumulative_order_evidence and evidence_gap_marker)
                        and (key in execution_keys or is_execution_evidence_key(key))
                    )
                    if key == "status" and str(raw_value).upper() in {"OK", "COMPLETE", "SUCCESS"}:
                        execution_key = False
                    if (
                        not (allow_cumulative_order_evidence and evidence_gap_marker)
                        and (execution_key or any(
                        token in key for token in ("fill", "deal", "execut", "order", "position", "trade")
                        ))
                    ):
                        mismatches.add(f"{current}.execution_evidence")
                    walk(
                        raw_value,
                        current,
                        account_context=account_context
                        or key in {"account", "account_alias", "account_identifier"},
                    )
                return
            if isinstance(value, (list, tuple)):
                for index, item in enumerate(value):
                    walk(item, f"{path}[{index}]", account_context=account_context)
                return
            if value is None or scalar(value):
                return
            mismatches.add(f"{path}.opaque_type={type(value).__name__}")

        walk(metadata)
        return sorted(mismatches)

    @staticmethod
    def execution_fact_fingerprints(facts: BrokerFactSnapshot) -> tuple[str, str]:
        """Return stable position/open-order fingerprints for a fresh fact set."""
        positions = sorted(
            (str(item.instrument_id), str(Decimal(str(item.signed_quantity))))
            for item in facts.positions
        )
        orders = sorted(
            (
                str(item.external_order_id),
                str(item.instrument_id),
                item.side.value,
                str(Decimal(str(item.quantity))),
                str(Decimal(str(item.filled_quantity))),
                item.status.value,
            )
            for item in facts.open_orders
        )
        encoded_positions = json.dumps(positions, separators=(",", ":"), sort_keys=True).encode("utf-8")
        encoded_orders = json.dumps(orders, separators=(",", ":"), sort_keys=True).encode("utf-8")
        return hashlib.sha256(encoded_positions).hexdigest(), hashlib.sha256(encoded_orders).hexdigest()

    @classmethod
    def _has_submission_fill_evidence(
        cls,
        result: BrokerSubmissionResult,
        *,
        expected_quantity: Decimal | None = None,
        expected_instrument_id: str | None = None,
    ) -> bool:
        if result.cumulative_filled_quantity is not None and result.cumulative_filled_quantity > 0:
            return True
        # A Moomoo place-order response may echo the newly created order with
        # ``dealt_qty=0`` and ``dealt_avg_price=0``.  That is an ACK, not a
        # fill, but only when the adapter has supplied the dedicated authority
        # marker and this core re-validates the complete normalized envelope.
        if cls._authoritative_submission_ack_no_fill(
            result,
            expected_quantity=expected_quantity,
            expected_instrument_id=expected_instrument_id,
        ):
            return False
        # An adapter-authenticated terminal zero-fill command response may
        # contain the expected order ID/status row.  Validate that exact raw
        # row before exempting those identity fields from the generic
        # lifecycle-evidence scanner.
        if (
            result.authority == ADAPTER_ORDER_SNAPSHOT_AUTHORITY
            and result.no_fill_asserted
            and result.status in {
                BrokerOrderStatus.CANCELLED,
                BrokerOrderStatus.REJECTED,
                BrokerOrderStatus.FAILED,
            }
        ):
            if cls._authoritative_command_no_fill(
                result,
                expected_quantity=expected_quantity,
                expected_instrument_id=expected_instrument_id,
                evidence_detector=cls._payload_has_submission_evidence,
            ):
                return False
            # An asserted adapter authority that fails the complete
            # response-envelope check is ambiguous even when its remaining
            # fields happen to look like zero.  Do not fall through to the
            # narrower positive-fill scanner and turn duplicate/unknown
            # response aliases into a clean rejection.
            return True
        return bool(cls._payload_has_positive_fill_evidence(result.raw_payload))

    @classmethod
    def _authoritative_submission_ack_no_fill(
        cls,
        result: BrokerSubmissionResult,
        *,
        expected_quantity: Decimal | None,
        expected_instrument_id: str | None,
    ) -> bool:
        """Accept only a fully validated zero-fill submission ACK.

        This is deliberately narrower than the terminal no-fill authority:
        it accepts only a working/submitting response with explicit, agreeing
        zero dealt quantity and zero dealt average price.  Positive,
        malformed, missing, conflicting, or nested provider fill evidence is
        rejected and therefore remains reconciliation evidence.
        """
        if (
            result.authority != ADAPTER_SUBMISSION_ACK_AUTHORITY
            or not result.no_fill_asserted
            or result.status
            not in {
                BrokerOrderStatus.WORKING,
                BrokerOrderStatus.SUBMITTING,
            }
            or result.cumulative_filled_quantity != 0
        ):
            return False
        if expected_quantity is not None and result.submitted_quantity != expected_quantity:
            return False
        if expected_instrument_id is not None and result.instrument_id != expected_instrument_id:
            return False
        try:
            payload = coerce_provider_payload(result.raw_payload)
        except ProviderPayloadError:
            return False
        if not isinstance(payload, Mapping):
            return False
        response_values = [
            value
            for key, value in payload.items()
            if normalize_provider_key(key) == "response"
        ]
        if len(response_values) != 1:
            return False
        row = response_values[0]
        if isinstance(row, (list, tuple)):
            if len(row) != 1:
                return False
            row = row[0]
        if not isinstance(row, Mapping):
            return False

        def values(aliases: set[str]) -> list[object]:
            return [value for key, value in row.items() if normalize_provider_key(key) in aliases]

        def exact_decimal(items: list[object], expected: Decimal) -> bool:
            if not items:
                return False
            parsed: list[Decimal] = []
            for item in items:
                try:
                    value = Decimal(str(item))
                except (InvalidOperation, TypeError, ValueError):
                    return False
                if not value.is_finite():
                    return False
                parsed.append(value)
            return all(value == expected for value in parsed)

        if not exact_decimal(
            values({
                "dealt_qty", "filled_qty", "filled_quantity",
                "cumulative_filled_quantity", "fill_qty",
            }),
            Decimal("0"),
        ):
            return False
        if not exact_decimal(
            values({"dealt_avg_price", "avg_fill_price", "fill_price"}),
            Decimal("0"),
        ):
            return False
        status_values = values({"order_status", "status"})
        if not status_values:
            return False
        status_tokens = {
            str(value or "").strip().upper().replace("-", "_").split(".")[-1]
            for value in status_values
        }
        if not status_tokens or not status_tokens <= {
            "SUBMITTED", "SUBMITTING", "WAITING_SUBMIT", "WAITING_VERIFY",
            "PENDING", "WORKING",
        }:
            return False
        if len(status_tokens) != 1:
            return False
        if expected_quantity is not None:
            if not exact_decimal(values({"qty", "quantity"}), expected_quantity):
                return False
        order_ids = values({"order_id", "orderid", "external_order_id", "broker_order_id", "id"})
        if not order_ids or any(str(value).strip() != str(result.external_order_id).strip() for value in order_ids):
            return False

        # Remove only the fields positively validated above.  Every remaining
        # provider value is still scanned, so nested/sibling fill or lifecycle
        # evidence cannot be hidden behind this ACK marker.
        validated_keys = {
            "dealt_qty", "filled_qty", "filled_quantity", "cumulative_filled_quantity", "fill_qty",
            "dealt_avg_price", "avg_fill_price", "fill_price", "order_status", "status",
            "qty", "quantity", "order_id", "orderid", "external_order_id", "broker_order_id", "id",
            "fill_outside_rth",
        }
        remaining = {
            key: value
            for key, value in row.items()
            if normalize_provider_key(key) not in validated_keys
        }
        return not cls._payload_has_submission_evidence(remaining) and not cls._payload_has_positive_fill_evidence(remaining)

    @classmethod
    def _has_cancel_evidence(
        cls,
        result: BrokerSubmissionResult,
        *,
        expected_quantity: Decimal | None = None,
        expected_instrument_id: str | None = None,
    ) -> bool:
        """Detect any provider fact that contradicts a clean cancel.

        Cancellation needs a slightly wider guard than submission fill
        parsing: an untrusted nested order row is lifecycle evidence even
        when it does not expose a fill quantity.  Adapter-authoritative
        terminal zero-fill rows are validated separately and may be exempted.
        """
        if result.cumulative_filled_quantity is not None and result.cumulative_filled_quantity > 0:
            return True
        if (
            result.authority == ADAPTER_ORDER_SNAPSHOT_AUTHORITY
            and result.no_fill_asserted
            and result.status in {
                BrokerOrderStatus.CANCELLED,
                BrokerOrderStatus.REJECTED,
                BrokerOrderStatus.FAILED,
            }
        ):
            if cls._authoritative_command_no_fill(
                result,
                expected_quantity=expected_quantity,
                expected_instrument_id=expected_instrument_id,
                evidence_detector=cls._payload_has_submission_evidence,
            ):
                return False
            # A failed authority validation is itself reconciliation
            # evidence; an unrecognized or multiply wrapped response cannot
            # prove a clean cancel.
            return True
        return bool(cls._payload_has_submission_evidence(result.raw_payload))

    def _record_submission_fill_evidence(
        self,
        *,
        intent: OrderIntent,
        leg: OrderLeg,
        account: Account,
        broker_order_id: str,
        result: BrokerSubmissionResult,
        status: BrokerOrderStatus | None = None,
    ) -> None:
        observed_status = status or result.status
        if observed_status is BrokerOrderStatus.PREPARED:
            observed_status = BrokerOrderStatus.UNKNOWN
        try:
            self.repository.record_submission(
                broker_order_id,
                status=observed_status,
                external_order_id=result.external_order_id,
                metadata={
                    **self._submission_metadata(result),
                    "submission_fill_evidence": True,
                    "fill_evidence_complete": False,
                },
                now=self._now(),
                submitted_at=result.submitted_at,
            )
        except (KeyError, TypeError, ValueError, sqlite3.Error) as exc:
            # Preserve the original response and turn even a malformed
            # durable write into an actionable blocker.
            self._require_reconciliation(
                intent.id,
                account,
                category="BROKER_SUBMISSION_FILL_EVIDENCE",
                entity_type="BROKER_ORDER",
                entity_key=broker_order_id,
                details={"message": str(exc), "raw_payload": self._safe_provider_payload(result.raw_payload)},
            )
        current_leg = next(
            item for item in self._required_intent(intent.id)["legs"] if item["id"] == leg.id
        )
        self._mark_leg_reconciliation(leg.id, str(current_leg["status"]))
        self._require_reconciliation(
            intent.id,
            account,
            category="BROKER_SUBMISSION_FILL_EVIDENCE",
            entity_type="BROKER_ORDER",
            entity_key=broker_order_id,
            details={
                "broker_status": result.status.value,
                "external_order_id": result.external_order_id,
                "reported_cumulative_fill": (
                    str(result.cumulative_filled_quantity)
                    if result.cumulative_filled_quantity is not None
                    else None
                ),
                "cumulative_fill_known": result.cumulative_filled_quantity is not None,
                "no_submit_asserted": result.no_submit_asserted,
                "no_fill_asserted": result.no_fill_asserted,
                "raw_payload": self._safe_provider_payload(result.raw_payload),
                "fill_evidence_complete": False,
            },
        )
        current_intent = self._required_intent(intent.id)
        self._record_recovery_action(
            intent=current_intent,
            account=account,
            action_key=f"SUBMISSION_FILL_EVIDENCE:{broker_order_id}",
            state="RECONCILIATION_REQUIRED",
            summary="Submission response carried positive or malformed fill/deal evidence without normalized durable fills; no allocation or remediation is allowed.",
            observed_positions=self._observe_positions(account)[0],
            remaining_quantities=self._remaining_quantities(current_intent),
            allowed_next_steps=("refresh_broker_facts", "refresh_broker_fills", "reconcile_broker_position", "operator_review"),
            metadata={
                "broker_order_id": broker_order_id,
                "broker_status": result.status.value,
                "reported_cumulative_fill": (
                    str(result.cumulative_filled_quantity)
                    if result.cumulative_filled_quantity is not None
                    else None
                ),
                "cumulative_fill_known": result.cumulative_filled_quantity is not None,
                "no_submit_asserted": result.no_submit_asserted,
                "no_fill_asserted": result.no_fill_asserted,
                "raw_payload": self._safe_provider_payload(result.raw_payload),
            },
        )

    @staticmethod
    def _safe_provider_payload(payload: object) -> dict[str, object]:
        """Keep raw provider evidence durable even when a value is unusual."""
        if isinstance(payload, Mapping):
            candidate = dict(payload)
        else:
            candidate = {"value": payload}
        try:
            json.dumps(candidate)
        except (TypeError, ValueError):
            return {"_unserializable_payload": repr(candidate)}
        return candidate

    @staticmethod
    def _submission_metadata(
        result: BrokerSubmissionResult,
        *,
        definite_no_submit: bool = False,
    ) -> dict:
        return {
            "accepted": result.accepted,
            "ambiguous": result.ambiguous,
            "provider_status": result.status.value,
            "error_code": result.error_code,
            "error": result.error_message,
            "reported_cumulative_fill": (
                str(result.cumulative_filled_quantity)
                if result.cumulative_filled_quantity is not None
                else None
            ),
            "cumulative_fill_known": result.cumulative_filled_quantity is not None,
            "no_submit_asserted": result.no_submit_asserted,
            "no_fill_asserted": result.no_fill_asserted,
            "authority": result.authority,
            "submitted_quantity": (
                str(result.submitted_quantity) if result.submitted_quantity is not None else None
            ),
            "instrument_id": result.instrument_id,
            "raw_payload": GenericOMS._safe_provider_payload(result.raw_payload),
            "definite_no_submit": definite_no_submit,
        }

    def _require_reconciliation(
        self,
        intent_id: str,
        account: Account,
        *,
        category: str,
        entity_type: str,
        entity_key: str,
        details: dict,
    ) -> None:
        self._require_reconciliation_for_account_id(
            intent_id,
            account.id,
            category=category,
            entity_type=entity_type,
            entity_key=entity_key,
            details=details,
        )

    def _require_reconciliation_for_account_id(
        self,
        intent_id: str,
        account_id: str,
        *,
        category: str,
        entity_type: str,
        entity_key: str,
        details: dict,
    ) -> None:
        current = self._required_intent(intent_id)["status"]
        if current != IntentStatus.RECONCILIATION_REQUIRED.value:
            self.repository.transition_intent(intent_id, IntentStatus.RECONCILIATION_REQUIRED, now=self._now())
        now = self._now()
        run_id = str(uuid.uuid4())
        self.repository.save_reconciliation_run(
            ReconciliationRun(
                id=run_id,
                account_id=account_id,
                started_at=now,
                completed_at=now,
                status=ReconciliationStatus.COMPLETED,
                metadata={"source": "oms"},
            )
        )
        issue_key = f"{category}:{entity_type}:{entity_key}"
        self.repository.upsert_reconciliation_issue(
            ReconciliationIssue(
                id=str(uuid.uuid4()),
                run_id=run_id,
                account_id=account_id,
                issue_key=issue_key,
                entity_type=entity_type,
                entity_key=entity_key,
                category=category,
                severity=IssueSeverity.CRITICAL,
                status=IssueStatus.OPEN,
                sticky=True,
                details=details,
                detected_at=now,
            )
        )

    def _cancel_working_attempts(self, intent: OrderIntent, account: Account) -> None:
        for leg in intent.legs:
            for attempt in self.repository.broker_orders_for_leg(leg.id):
                if attempt["status"] not in {
                    BrokerOrderStatus.WORKING.value,
                    BrokerOrderStatus.PARTIALLY_FILLED.value,
                } or not attempt["external_order_id"]:
                    continue
                try:
                    result = self.adapter.cancel_order(account, str(attempt["external_order_id"]))
                except Exception as exc:
                    self.repository.transition_broker_order(
                        str(attempt["id"]),
                        BrokerOrderStatus.UNKNOWN,
                        now=self._now(),
                    )
                    self._require_reconciliation(
                        intent.id,
                        account,
                        category="CANCEL_AMBIGUITY",
                        entity_type="BROKER_ORDER",
                        entity_key=str(attempt["id"]),
                        details={"message": str(exc)},
                    )
                    self._record_recovery_action(
                        intent=self._required_intent(intent.id),
                        account=account,
                        action_key=f"CANCEL_AMBIGUITY:{attempt['id']}",
                        state="RECONCILIATION_REQUIRED",
                        summary="Cancellation transport failed; broker order exposure is unknown and no retry or hedge is allowed.",
                        observed_positions=self._observe_positions(account)[0],
                        remaining_quantities=self._remaining_quantities(self._required_intent(intent.id)),
                        allowed_next_steps=("refresh_broker_facts", "reconcile_broker_order", "operator_review"),
                        metadata={"broker_order_id": str(attempt["id"]), "error": str(exc)},
                    )
                    continue

                cancel_fill_evidence = (
                    isinstance(result, BrokerSubmissionResult)
                    and self._has_cancel_evidence(
                        result,
                        expected_quantity=Decimal(str(attempt["submitted_quantity"])),
                        expected_instrument_id=str(leg.instrument_id),
                    )
                )
                clean_cancel = (
                    isinstance(result, BrokerSubmissionResult)
                    and result.accepted is True
                    and result.status is BrokerOrderStatus.CANCELLED
                    and result.external_order_id in {None, attempt["external_order_id"]}
                    and result.no_fill_asserted
                    and result.cumulative_filled_quantity is not None
                    and result.cumulative_filled_quantity == 0
                    and result.ambiguous is False
                    and not cancel_fill_evidence
                )
                if clean_cancel:
                    try:
                        self.repository.record_submission(
                            str(attempt["id"]),
                            status=BrokerOrderStatus.CANCELLED,
                            external_order_id=result.external_order_id or attempt["external_order_id"],
                            metadata={
                                **self._submission_metadata(result),
                                "cancel_requested": True,
                                "cancel_result": self._safe_provider_payload(result.raw_payload),
                                "terminal_zero_fill_proof": True,
                            },
                            now=self._now(),
                        )
                    except (ValueError, sqlite3.Error) as exc:
                        clean_cancel = False
                        cancel_error = str(exc)
                    else:
                        cancel_error = None
                else:
                    cancel_error = (
                        "cancel response was not an unambiguous CANCELLED result"
                        if isinstance(result, BrokerSubmissionResult)
                        else "adapter cancel_order() returned an invalid result"
                    )

                if clean_cancel:
                    current_intent = self._required_intent(intent.id)
                    current_leg = next(item for item in current_intent["legs"] if item["id"] == leg.id)
                    try:
                        cumulative = Decimal(str(current_leg["cumulative_filled_quantity"]))
                    except (InvalidOperation, TypeError, ValueError):
                        cumulative = Decimal("-1")
                    if cumulative == 0:
                        self._set_leg_terminal(leg.id, str(current_leg["status"]), LegStatus.CANCELLED)
                        self._resolve_recovery_actions_for_order(account, intent.id, str(attempt["id"]))
                        self._record_recovery_action(
                            intent=current_intent,
                            account=account,
                            action_key=f"CANCEL_RESULT:{attempt['id']}",
                            state="CANCELLED",
                            summary="Broker cancellation was durably confirmed with zero fills.",
                            observed_positions=self._observe_positions(account)[0],
                            remaining_quantities=self._remaining_quantities(self._required_intent(intent.id)),
                            allowed_next_steps=("operator_review",),
                            metadata={"broker_order_id": str(attempt["id"])},
                        )
                        self._resolve_recovery_action(
                            account.id,
                            intent.id,
                            f"CANCEL_RESULT:{attempt['id']}",
                            resolved_at=self._now(),
                        )
                    else:
                        self._mark_leg_reconciliation(leg.id, str(current_leg["status"]))
                        self._require_reconciliation(
                            intent.id,
                            account,
                            category="TERMINAL_PARTIAL_FILL",
                            entity_type="BROKER_ORDER",
                            entity_key=str(attempt["id"]),
                            details={"broker_status": BrokerOrderStatus.CANCELLED.value, "durable_filled_quantity": str(cumulative)},
                        )
                        self._record_recovery_action(
                            intent=current_intent,
                            account=account,
                            action_key=f"CANCEL_RESULT:{attempt['id']}",
                            state="RECONCILIATION_REQUIRED",
                            summary="Cancellation was confirmed after a positive fill; residual exposure requires reconciliation.",
                            observed_positions=self._observe_positions(account)[0],
                            remaining_quantities=self._remaining_quantities(self._required_intent(intent.id)),
                            allowed_next_steps=("refresh_broker_facts", "refresh_broker_fills", "reconcile_broker_position", "operator_review"),
                            metadata={"broker_order_id": str(attempt["id"]), "durable_filled_quantity": str(cumulative)},
                        )
                else:
                    if cancel_fill_evidence:
                        response_status = result.status
                        if response_status is BrokerOrderStatus.PREPARED:
                            response_status = BrokerOrderStatus.UNKNOWN
                        try:
                            self.repository.record_submission(
                                str(attempt["id"]),
                                status=response_status,
                                external_order_id=result.external_order_id or attempt["external_order_id"],
                                metadata={
                                    "cancel_requested": True,
                                    "cancel_fill_evidence": True,
                                    "reported_cumulative_fill": (
                                        str(result.cumulative_filled_quantity)
                                        if result.cumulative_filled_quantity is not None
                                        else None
                                    ),
                                    "cumulative_fill_known": result.cumulative_filled_quantity is not None,
                                    "no_submit_asserted": result.no_submit_asserted,
                                    "no_fill_asserted": result.no_fill_asserted,
                                    "raw_payload": self._safe_provider_payload(result.raw_payload),
                                },
                                now=self._now(),
                            )
                        except (KeyError, TypeError, ValueError, sqlite3.Error) as exc:
                            cancel_error = f"cancel fill evidence could not be durably recorded: {exc}"
                        current_intent = self._required_intent(intent.id)
                        current_leg = next(item for item in current_intent["legs"] if item["id"] == leg.id)
                        self._mark_leg_reconciliation(leg.id, str(current_leg["status"]))
                        self._require_reconciliation(
                            intent.id,
                            account,
                            category="CANCEL_FILL_EVIDENCE",
                            entity_type="BROKER_ORDER",
                            entity_key=str(attempt["id"]),
                            details={
                                "broker_status": result.status.value,
                                "reported_cumulative_fill": (
                                    str(result.cumulative_filled_quantity)
                                    if result.cumulative_filled_quantity is not None
                                    else None
                                ),
                                "cumulative_fill_known": result.cumulative_filled_quantity is not None,
                                "no_submit_asserted": result.no_submit_asserted,
                                "no_fill_asserted": result.no_fill_asserted,
                                "raw_payload": self._safe_provider_payload(result.raw_payload),
                                "error": cancel_error,
                            },
                        )
                        self._record_recovery_action(
                            intent=current_intent,
                            account=account,
                            action_key=f"CANCEL_FILL_EVIDENCE:{attempt['id']}",
                            state="RECONCILIATION_REQUIRED",
                            summary="Cancellation response carried positive or malformed fill/deal evidence; broker fills and residual exposure require reconciliation.",
                            observed_positions=self._observe_positions(account)[0],
                            remaining_quantities=self._remaining_quantities(self._required_intent(intent.id)),
                            allowed_next_steps=("refresh_broker_facts", "refresh_broker_fills", "reconcile_broker_position", "operator_review"),
                            metadata={
                                "broker_order_id": str(attempt["id"]),
                                "reported_cumulative_fill": (
                                    str(result.cumulative_filled_quantity)
                                    if result.cumulative_filled_quantity is not None
                                    else None
                                ),
                                "cumulative_fill_known": result.cumulative_filled_quantity is not None,
                                "no_submit_asserted": result.no_submit_asserted,
                                "no_fill_asserted": result.no_fill_asserted,
                                "raw_payload": self._safe_provider_payload(result.raw_payload),
                            },
                        )
                        continue
                    try:
                        if isinstance(result, BrokerSubmissionResult) and result.status is BrokerOrderStatus.UNKNOWN:
                            self.repository.transition_broker_order(
                                str(attempt["id"]),
                                BrokerOrderStatus.UNKNOWN,
                                now=self._now(),
                            )
                    except (KeyError, ValueError, sqlite3.Error):
                        pass
                    self._require_reconciliation(
                        intent.id,
                        account,
                        category="CANCEL_AMBIGUITY",
                        entity_type="BROKER_ORDER",
                        entity_key=str(attempt["id"]),
                        details={"message": cancel_error},
                    )
                    self._record_recovery_action(
                        intent=self._required_intent(intent.id),
                        account=account,
                        action_key=f"CANCEL_AMBIGUITY:{attempt['id']}",
                        state="RECONCILIATION_REQUIRED",
                        summary="Cancellation result was ambiguous or rejected; poll broker truth before any further action.",
                        observed_positions=self._observe_positions(account)[0],
                        remaining_quantities=self._remaining_quantities(self._required_intent(intent.id)),
                        allowed_next_steps=("refresh_broker_facts", "reconcile_broker_order", "operator_review"),
                        metadata={"broker_order_id": str(attempt["id"]), "error": cancel_error},
                    )
        self._finalize_intent_status(intent.id, account)

    def cancel_known_partial_attempt(
        self,
        intent_id: str,
        *,
        account: Account,
        external_order_id: str,
    ) -> dict[str, object]:
        """Cancel one exact, durably known partial broker attempt.

        This is deliberately narrower than the failure-policy cancellation
        sweep.  A fresh account-facts snapshot must identify exactly one open
        order belonging to this intent/leg and agree with durable cumulative
        fills before the adapter cancellation call is made.  The cancellation
        response is durably recorded, then ordinary restart recovery re-queries
        the broker so a late fill cannot be hidden by the command response.
        """
        self._startup_safety_audit()
        account = self._canonical_account_for_boundary(account, source="cancel_known_partial_attempt")
        intent = self._required_intent(intent_id)
        if str(intent.get("account_id")) != account.id:
            raise OMSExecutionError("partial cancellation account does not match persisted intent")
        external_order_id = str(external_order_id or "").strip()
        if not external_order_id:
            raise OMSExecutionError("partial cancellation requires an external broker order identity")
        matches: list[tuple[dict[str, object], dict[str, object]]] = []
        for leg in intent.get("legs", ()):
            attempts = self.repository.broker_orders_for_leg(str(leg["id"]))
            if len(attempts) > 1:
                raise OMSExecutionError(f"partial cancellation cannot operate on multiple attempts for leg {leg['id']}")
            if not attempts:
                continue
            attempt = attempts[0]
            if str(attempt.get("external_order_id") or "") == external_order_id:
                matches.append((leg, attempt))
        if len(matches) != 1:
            raise OMSExecutionError("partial cancellation external order is not uniquely owned by the intent")
        leg, attempt = matches[0]
        if str(attempt.get("account_id")) != account.id:
            raise OMSExecutionError("partial cancellation broker attempt belongs to another account")
        try:
            submitted = Decimal(str(attempt["submitted_quantity"]))
            durable_filled = self._durable_filled_quantity(str(attempt["id"]))
            local_cumulative = Decimal(str(leg.get("cumulative_filled_quantity", "0")))
        except (KeyError, InvalidOperation, TypeError, ValueError, sqlite3.Error) as exc:
            raise OMSExecutionError("partial cancellation durable quantities are invalid") from exc
        if submitted <= 0 or durable_filled <= 0 or durable_filled >= submitted or local_cumulative != durable_filled:
            raise OMSExecutionError("partial cancellation requires one exact positive partial fill")
        if str(attempt.get("status")) not in {
            BrokerOrderStatus.WORKING.value,
            BrokerOrderStatus.PARTIALLY_FILLED.value,
        }:
            raise OMSExecutionError("partial cancellation requires a working or partially-filled attempt")

        try:
            _facts, _positions, open_orders = self._strict_authoritative_account_facts(
                account,
                max_age_seconds=self.BROKER_FACT_MAX_AGE_SECONDS,
            )
        except OMSExecutionError:
            raise
        open_matches = [row for row in open_orders if row.external_order_id == external_order_id]
        if len(open_matches) != 1:
            raise OMSExecutionError("fresh broker facts do not identify exactly one open partial order")
        snapshot = open_matches[0]
        if (
            snapshot.account_id != account.id
            or snapshot.instrument_id != str(leg.get("instrument_id"))
            or snapshot.side.value != str(leg.get("side"))
            or snapshot.quantity != submitted
            or snapshot.filled_quantity != durable_filled
            or snapshot.status not in {BrokerOrderStatus.WORKING, BrokerOrderStatus.PARTIALLY_FILLED}
        ):
            raise OMSExecutionError("fresh broker partial order facts conflict with durable attempt evidence")
        validation = self._validate_poll_snapshot(
            account=account,
            attempt=attempt,
            leg=leg,
            snapshot=snapshot,
        )
        if not validation["valid"]:
            raise OMSExecutionError(
                "fresh broker partial order facts failed strict identity validation: "
                + ", ".join(str(value) for value in validation["mismatches"])
            )

        try:
            result = self.adapter.cancel_order(account, external_order_id)
        except Exception as exc:
            self.repository.transition_broker_order(str(attempt["id"]), BrokerOrderStatus.UNKNOWN, now=self._now())
            details = {"broker_order_id": str(attempt["id"]), "external_order_id": external_order_id, "error": str(exc)}
            self._require_reconciliation(
                intent_id,
                account,
                category="CANCEL_AMBIGUITY",
                entity_type="BROKER_ORDER",
                entity_key=str(attempt["id"]),
                details=details,
            )
            self._record_recovery_action(
                intent=self._required_intent(intent_id),
                account=account,
                action_key=f"CANCEL_AMBIGUITY:{attempt['id']}",
                state="RECONCILIATION_REQUIRED",
                summary="Partial-order cancellation transport failed; broker exposure is unknown and no retry or hedge is allowed.",
                observed_positions={"_status": "UNAVAILABLE"},
                remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                allowed_next_steps=("refresh_broker_facts", "reconcile_broker_order", "operator_review"),
                metadata=details,
            )
            raise OMSExecutionError("partial cancellation outcome is ambiguous") from exc
        if not isinstance(result, BrokerSubmissionResult):
            raise OMSExecutionError("adapter cancel_order() returned an invalid result")
        accepted_cancel = (
            result.accepted is True
            and result.status is BrokerOrderStatus.CANCELLED
            and result.external_order_id in {None, external_order_id}
            and result.ambiguous is False
        )
        if not accepted_cancel:
            self.repository.transition_broker_order(str(attempt["id"]), BrokerOrderStatus.UNKNOWN, now=self._now())
            details = {
                "broker_order_id": str(attempt["id"]),
                "external_order_id": external_order_id,
                "status": result.status.value,
                "accepted": result.accepted,
                "ambiguous": result.ambiguous,
                "raw_payload": self._safe_provider_payload(result.raw_payload),
            }
            self._require_reconciliation(
                intent_id,
                account,
                category="CANCEL_AMBIGUITY",
                entity_type="BROKER_ORDER",
                entity_key=str(attempt["id"]),
                details=details,
            )
            self._record_recovery_action(
                intent=self._required_intent(intent_id),
                account=account,
                action_key=f"CANCEL_AMBIGUITY:{attempt['id']}",
                state="RECONCILIATION_REQUIRED",
                summary="Broker cancellation was not an unambiguous accepted CANCELLED result; no retry or hedge is allowed.",
                observed_positions={"_status": "UNKNOWN"},
                remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                allowed_next_steps=("refresh_broker_facts", "reconcile_broker_order", "operator_review"),
                metadata=details,
            )
            raise OMSExecutionError("partial cancellation outcome is ambiguous")
        self.repository.record_submission(
            str(attempt["id"]),
            status=BrokerOrderStatus.CANCELLED,
            external_order_id=result.external_order_id or external_order_id,
            metadata={
                **self._submission_metadata(result),
                "cancel_requested": True,
                "known_partial_cancel": True,
                "terminal_zero_fill_proof": bool(
                    result.no_fill_asserted and result.cumulative_filled_quantity == 0
                ),
            },
            now=self._now(),
            submitted_at=self._parse_timestamp(attempt.get("submitted_at")),
        )
        recovered = self.recover_intent(intent_id, account=account)
        return {
            "intent_id": intent_id,
            "leg_id": str(leg["id"]),
            "broker_order_id": str(attempt["id"]),
            "external_order_id": external_order_id,
            "cancel_status": result.status.value,
            "recovered": recovered,
            "broker_contacted": True,
            "cancel_count": 1,
            "submission_count": 0,
        }

    @staticmethod
    def _parse_timestamp(value: object) -> datetime | None:
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc) if value.tzinfo and value.utcoffset() is not None else None
        if value is None:
            return None
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
        return parsed.astimezone(timezone.utc) if parsed.tzinfo and parsed.utcoffset() is not None else None

    def _temporal_order_mismatches(
        self,
        attempt: Mapping[str, object],
        observed_at: datetime | None,
    ) -> list[str]:
        """Reject broker timestamps that predate durable submit evidence."""
        if observed_at is None:
            return []
        raw_anchor = attempt.get("submitted_at") or attempt.get("updated_at")
        if raw_anchor in (None, ""):
            return []
        anchor = self._parse_timestamp(raw_anchor)
        if anchor is None:
            return ["durable_submit_timestamp_unparseable"]
        observed = observed_at.astimezone(timezone.utc)
        if observed < anchor - timedelta(seconds=self.BROKER_TIME_ORDER_TOLERANCE_SECONDS):
            return [
                "broker_fact_predates_durable_submit",
                f"observed:{observed.isoformat()}",
                f"durable_submit:{anchor.isoformat()}",
            ]
        return []

    def _temporal_fill_mismatches(
        self,
        attempt: Mapping[str, object],
        broker_fill: BrokerFill,
    ) -> list[str]:
        """Reject fill facts that predate the durable order timeline."""
        mismatches: list[str] = []
        for label, observed_at in (
            ("filled_at", broker_fill.filled_at),
            ("received_at", broker_fill.received_at),
        ):
            for mismatch in self._temporal_order_mismatches(attempt, observed_at):
                mismatches.append(f"{label}:{mismatch}")
        return mismatches

    def _record_fill_temporal_conflict(
        self,
        *,
        intent_id: str,
        account: Account,
        broker_fill: BrokerFill,
        mismatches: Sequence[str],
    ) -> None:
        """Persist an ordering contradiction without allocating the fill."""
        self._require_reconciliation(
            intent_id,
            account,
            category="BROKER_FILL_TIME_CONFLICT",
            entity_type="BROKER_FILL",
            entity_key=broker_fill.dedupe_key,
            details={
                "external_order_id": broker_fill.external_order_id,
                "mismatches": list(mismatches),
                "filled_at": broker_fill.filled_at.isoformat(),
                "received_at": broker_fill.received_at.isoformat(),
            },
        )
        self._record_recovery_action(
            intent=self._required_intent(intent_id),
            account=account,
            action_key=f"FILL_TIME_CONFLICT:{broker_fill.dedupe_key}",
            state="RECONCILIATION_REQUIRED",
            summary="Broker fill timestamps predate durable submit evidence; no allocation was accepted.",
            observed_positions=self._observe_positions(account)[0],
            remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
            allowed_next_steps=("refresh_broker_fills", "reconcile_broker_order", "operator_review"),
            metadata={"external_order_id": broker_fill.external_order_id, "mismatches": list(mismatches)},
        )

    def _attempt_is_recovery_relevant(
        self,
        intent: Mapping[str, object],
        attempt: Mapping[str, object],
        now: datetime,
    ) -> bool:
        terminal_orders = {
            BrokerOrderStatus.FILLED.value,
            BrokerOrderStatus.REJECTED.value,
            BrokerOrderStatus.CANCELLED.value,
            BrokerOrderStatus.FAILED.value,
        }
        if str(attempt.get("status")) not in terminal_orders:
            return False
        intent_statuses = {
            IntentStatus.SUBMITTING.value,
            IntentStatus.WORKING.value,
            IntentStatus.PARTIALLY_FILLED.value,
            IntentStatus.RECONCILIATION_REQUIRED.value,
            IntentStatus.FILLED.value,
            IntentStatus.COMPLETED.value,
            IntentStatus.REJECTED.value,
            IntentStatus.CANCELLED.value,
            IntentStatus.FAILED.value,
        }
        if str(intent.get("status")) not in intent_statuses:
            return False
        updated_at = self._parse_timestamp(attempt.get("updated_at"))
        if updated_at is None:
            return False
        return (now - updated_at).total_seconds() <= self.TERMINAL_RECOVERY_WINDOW_SECONDS

    @staticmethod
    def _policy_seconds(intent: Mapping[str, object], name: str, default: int) -> int:
        policy = intent.get("execution_policy") or {}
        try:
            value = int(policy.get(name, default))  # type: ignore[union-attr]
        except (AttributeError, TypeError, ValueError):
            return default
        return value if value > 0 else default

    def _observe_positions(self, account: Account) -> tuple[dict[str, str], str | None]:
        """Read optional position truth for operator output without guessing flatness."""
        context = self._recovery_fact_context
        if isinstance(context, Mapping) and context.get("account_id") == account.id:
            observed = dict(context["observed_positions"])
            self._last_broker_read_retryable = False
            self._last_observed_positions = observed
            return observed, None
        self._last_broker_read_retryable = False
        getter = getattr(self.adapter, "get_positions", None)
        if not callable(getter):
            observed = {"_status": "UNAVAILABLE", "_reason": "adapter does not expose position reads"}
            self._last_observed_positions = observed
            return observed, None
        try:
            positions = tuple(getter(account))
        except Exception as exc:
            self._last_broker_read_retryable = bool(getattr(exc, "retryable", False))
            observed = {"_status": "ERROR", "_error": str(exc)}
            self._last_observed_positions = observed
            return observed, str(exc)
        totals: dict[str, Decimal] = {}
        for position in positions:
            try:
                instrument_id = str(position.instrument_id)
                quantity = Decimal(str(position.signed_quantity))
                if not instrument_id or not quantity.is_finite():
                    raise ValueError("position instrument or quantity is invalid")
            except (AttributeError, InvalidOperation, TypeError, ValueError) as exc:
                observed = {"_status": "ERROR", "_error": str(exc)}
                self._last_observed_positions = observed
                return observed, str(exc)
            totals[instrument_id] = totals.get(instrument_id, Decimal("0")) + quantity
        observed = {instrument_id: str(quantity) for instrument_id, quantity in sorted(totals.items())}
        self._last_observed_positions = observed
        return observed, None

    @staticmethod
    def _remaining_quantities(intent: Mapping[str, object]) -> dict[str, str]:
        remaining: dict[str, str] = {}
        for leg in intent.get("legs", ()):
            try:
                requested = Decimal(str(leg["quantity"]))
                cumulative = Decimal(str(leg.get("cumulative_filled_quantity", "0")))
                value = max(Decimal("0"), requested - cumulative)
            except (KeyError, InvalidOperation, TypeError, ValueError):
                value = Decimal("0")
            remaining[str(leg["id"])] = str(value)
        return remaining

    def _record_recovery_action(
        self,
        *,
        intent: Mapping[str, object],
        account: Account,
        action_key: str,
        state: str,
        summary: str,
        observed_positions: Mapping[str, object],
        remaining_quantities: Mapping[str, object],
        stale: bool = False,
        timed_out: bool = False,
        allowed_next_steps: tuple[str, ...] = (
            "poll_again",
            "reconcile_broker_position",
            "operator_review",
        ),
        metadata: Mapping[str, object] | None = None,
    ) -> None:
        self._record_recovery_action_for_account_id(
            intent=intent,
            account_id=account.id,
            action_key=action_key,
            state=state,
            summary=summary,
            observed_positions=observed_positions,
            remaining_quantities=remaining_quantities,
            stale=stale,
            timed_out=timed_out,
            allowed_next_steps=allowed_next_steps,
            metadata=metadata,
        )

    def _record_recovery_action_for_account_id(
        self,
        *,
        intent: Mapping[str, object],
        account_id: str,
        action_key: str,
        state: str,
        summary: str,
        observed_positions: Mapping[str, object],
        remaining_quantities: Mapping[str, object],
        stale: bool = False,
        timed_out: bool = False,
        allowed_next_steps: tuple[str, ...] = (
            "poll_again",
            "reconcile_broker_position",
            "operator_review",
        ),
        metadata: Mapping[str, object] | None = None,
    ) -> None:
        action_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{account_id}:{intent['id']}:{action_key}"))
        self.repository.upsert_recovery_action(
            RecoveryAction(
                id=action_id,
                intent_id=str(intent["id"]),
                account_id=account_id,
                action_key=action_key,
                state=state,
                summary=summary,
                observed_positions=observed_positions,
                remaining_quantities=remaining_quantities,
                stale=stale,
                timed_out=timed_out,
                allowed_next_steps=allowed_next_steps,
                detected_at=self._now(),
                metadata=dict(metadata or {}),
            )
        )

    def _resolve_recovery_actions_for_order(
        self,
        account: Account,
        intent_id: str,
        broker_order_id: str,
        *,
        action_prefixes: tuple[str, ...] = (),
    ) -> None:
        for action in self.repository.recovery_actions_for_intent(intent_id, status=RecoveryActionStatus.OPEN):
            metadata = action.get("metadata") or {}
            if (
                metadata.get("broker_order_id") == broker_order_id
                and (not action_prefixes or str(action["action_key"]).startswith(action_prefixes))
            ):
                self._resolve_recovery_action(
                    account.id,
                    intent_id,
                    str(action["action_key"]),
                    resolved_at=self._now(),
                )

    def _mark_leg_reconciliation(self, leg_id: str, leg_status: str) -> None:
        """Move an observed broker event to recon without illegal jumps."""
        if leg_status in {LegStatus.FILLED.value, LegStatus.RECONCILIATION_REQUIRED.value}:
            return
        if leg_status == LegStatus.PLANNED.value:
            self.repository.transition_leg(leg_id, LegStatus.SUBMITTING, now=self._now())
        self.repository.transition_leg(leg_id, LegStatus.RECONCILIATION_REQUIRED, now=self._now())

    def _force_leg_reconciliation(self, leg_id: str, leg_status: str) -> None:
        """Force a durable safety block, including after local terminal state."""
        if leg_status == LegStatus.RECONCILIATION_REQUIRED.value:
            return
        if leg_status == LegStatus.PLANNED.value:
            self.repository.transition_leg(leg_id, LegStatus.SUBMITTING, now=self._now())
        self.repository.transition_leg(leg_id, LegStatus.RECONCILIATION_REQUIRED, now=self._now())

    def _quarantine_multiple_attempts(self, intent_id: str, account: Account) -> bool:
        """Quarantine every unsupported multi-attempt leg on this account.

        The invariant is account-wide: a clean-looking sibling intent must not
        permit a new submission while another managed leg has replacement
        history whose fill ownership the generic core cannot aggregate safely.
        """
        rows = self.repository.multi_attempt_legs_for_account(account.id)
        if not rows:
            return False
        proof_closed = self._closed_historical_intent_ids(account)
        observed_positions, _ = self._observe_positions(account)
        for row in rows:
            affected_intent_id = str(row["intent_id"])
            if affected_intent_id in proof_closed:
                # A verified retired/round-trip closure is lifecycle-frozen.
                # Do not recreate the unsupported-attempt quarantine for a
                # historical row that already passed exact proof; account
                # submission gates still inspect fresh broker facts and will
                # block any new/unknown exposure.
                continue
            affected_intent = self._required_intent(affected_intent_id)
            leg = next(
                (item for item in affected_intent["legs"] if str(item["id"]) == str(row["leg_id"])),
                None,
            )
            if leg is None:
                continue
            attempts = list(row.get("attempts") or ())
            self._force_leg_reconciliation(str(leg["id"]), str(leg["status"]))
            self._require_reconciliation(
                affected_intent_id,
                account,
                category="MULTIPLE_ATTEMPTS_UNSUPPORTED",
                entity_type="ORDER_LEG",
                entity_key=str(leg["id"]),
                details={
                    "leg_id": str(leg["id"]),
                    "attempt_ids": [str(item["id"]) for item in attempts],
                    "attempt_numbers": [int(item["attempt_number"]) for item in attempts],
                    "policy": "one_attempt_per_leg",
                },
            )
            self._record_recovery_action(
                intent=self._required_intent(affected_intent_id),
                account=account,
                action_key=f"MULTIPLE_ATTEMPTS:{leg['id']}",
                state="RECONCILIATION_REQUIRED",
                summary="Multiple durable broker attempts exist for one leg; aggregate fill ownership is quarantined until an operator reconciles them.",
                observed_positions=observed_positions,
                remaining_quantities=self._remaining_quantities(self._required_intent(affected_intent_id)),
                allowed_next_steps=("reconcile_broker_order", "reconcile_broker_fills", "operator_review"),
                metadata={"attempt_ids": [str(item["id"]) for item in attempts]},
            )
        # Returning true for a sibling row is intentional: account-wide open
        # issues/actions also make the caller's route unsafe to continue.
        return True

    def _quarantine_multiple_attempts_for_account(self, account: Account) -> bool:
        """Run the account-wide attempt invariant without a current intent."""
        return self._quarantine_multiple_attempts("", account)

    @classmethod
    def _fill_metadata_for_persistence(cls, broker_fill: BrokerFill) -> dict[str, object]:
        """Retain normalized source-account presence for replay validation."""
        metadata = dict(broker_fill.metadata)
        metadata.setdefault(cls._BROKER_FILL_ACCOUNT_ID_KEY, broker_fill.account_id)
        if broker_fill.instrument_id is not None:
            metadata.setdefault("_instrument_id", broker_fill.instrument_id)
        return metadata

    @staticmethod
    def _fill_evidence_fingerprint(fills: Sequence[BrokerFill]) -> str:
        """Return a stable fingerprint for the normalized fills carried by an event.

        Event dedupe is only safe when the evidence attached to a replay is the
        same evidence that was durably observed the first time.  Keep the
        fingerprint independent of object identity and JSON key ordering.
        """
        records: list[dict[str, object]] = []
        for item in fills:
            if not isinstance(item, BrokerFill):
                records.append({"invalid_type": type(item).__name__, "repr": repr(item)})
                continue
            records.append(
                {
                    "external_order_id": item.external_order_id,
                    "external_fill_id": item.external_fill_id,
                    "dedupe_key": item.dedupe_key,
                    "quantity": str(item.quantity),
                    "price": str(item.price),
                    "fee": str(item.fee) if item.fee is not None else None,
                    "fee_currency": item.fee_currency,
                    "filled_at": item.filled_at.isoformat(),
                    "account_id": item.account_id,
                    "evidence_reference": item.evidence_reference,
                    "instrument_id": item.instrument_id,
                    "metadata": dict(item.metadata),
                }
            )
        encoded = json.dumps(records, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _legacy_fill_evidence_fingerprint(fills: Sequence[BrokerFill]) -> str:
        """Fingerprint used before account/provenance fields were persisted."""
        records: list[dict[str, object]] = []
        for item in fills:
            if not isinstance(item, BrokerFill):
                records.append({"invalid_type": type(item).__name__, "repr": repr(item)})
                continue
            records.append(
                {
                    "external_order_id": item.external_order_id,
                    "external_fill_id": item.external_fill_id,
                    "dedupe_key": item.dedupe_key,
                    "quantity": str(item.quantity),
                    "price": str(item.price),
                    "fee": str(item.fee) if item.fee is not None else None,
                    "fee_currency": item.fee_currency,
                    "filled_at": item.filled_at.isoformat(),
                    "received_at": item.received_at.isoformat(),
                    "metadata": dict(item.metadata),
                }
            )
        encoded = json.dumps(records, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _event_evidence_fingerprint(event: BrokerOrderEvent, fills: Sequence[BrokerFill]) -> str:
        """Fingerprint the event facts and attached fills together.

        Dedupe identity alone is not evidence identity.  Persisting this
        fingerprint lets an exact replay remain a no-op even after local
        lifecycle state has advanced, while a changed status/metadata/fill
        payload becomes a durable contradiction.
        """
        fill_records: list[dict[str, object]] = []
        for item in fills:
            if not isinstance(item, BrokerFill):
                fill_records.append({"invalid_type": type(item).__name__, "repr": repr(item)})
                continue
            fill_records.append(
                {
                    "external_order_id": item.external_order_id,
                    "external_fill_id": item.external_fill_id,
                    "dedupe_key": item.dedupe_key,
                    "quantity": str(item.quantity),
                    "price": str(item.price),
                    "fee": str(item.fee) if item.fee is not None else None,
                    "fee_currency": item.fee_currency,
                    "filled_at": item.filled_at.isoformat(),
                    "account_id": item.account_id,
                    "evidence_reference": item.evidence_reference,
                    "instrument_id": item.instrument_id,
                    "metadata": dict(item.metadata),
                }
            )
        payload = {
            "event_id": event.id,
            "broker_order_id": event.broker_order_id,
            "dedupe_key": event.dedupe_key,
            "event_type": event.event_type,
            "event_at": event.event_at.isoformat(),
            "broker_status": event.broker_status.value if event.broker_status is not None else None,
            "external_event_id": event.external_event_id,
            "external_order_id": event.external_order_id,
            "client_order_id": event.client_order_id,
            "cumulative_filled_quantity": (
                str(event.cumulative_filled_quantity)
                if event.cumulative_filled_quantity is not None
                else None
            ),
            "account_id": event.account_id,
            "external_account_id": event.external_account_id,
            "no_fill_asserted": event.no_fill_asserted,
            "metadata": dict(event.metadata),
            "fills": fill_records,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _is_generated_fingerprint(value: object) -> bool:
        return (
            isinstance(value, str)
            and len(value) == 64
            and all(character in "0123456789abcdefABCDEF" for character in value)
        )

    @classmethod
    def _event_fingerprints_from_metadata(
        cls,
        metadata: Mapping[str, object],
    ) -> tuple[str | None, str | None]:
        """Read the protected envelope, with exact legacy-row compatibility."""
        for key, envelope in metadata.items():
            if not cls._is_event_envelope_key(key) or not isinstance(envelope, Mapping):
                continue
            fill = envelope.get("fill_fingerprint")
            event = envelope.get("event_fingerprint")
            if cls._is_generated_fingerprint(fill) and cls._is_generated_fingerprint(event):
                return str(fill), str(event)
        fill = metadata.get(cls._LEGACY_FILL_FINGERPRINT_KEY)
        event = metadata.get(cls._LEGACY_EVENT_FINGERPRINT_KEY)
        return (
            str(fill) if cls._is_generated_fingerprint(fill) else None,
            str(event) if cls._is_generated_fingerprint(event) else None,
        )

    @classmethod
    def _is_event_envelope_key(cls, key: object) -> bool:
        normalized = normalize_provider_key(key)
        prefix = cls._EVENT_EVIDENCE_ENVELOPE_KEY
        return normalized == prefix or normalized.startswith(f"{prefix}_")

    @classmethod
    def _event_user_metadata(cls, metadata: Mapping[str, object]) -> dict[str, object]:
        """Return provider metadata without hiding collision-shaped values.

        Dedicated fingerprint columns are authoritative for new rows.  For a
        pre-migration row, only the two exact historical top-level keys are
        treated as OMS-owned; envelope-shaped mappings are retained because a
        provider may legitimately use those names or nest fill evidence
        beneath them.
        """
        result: dict[str, object] = {}
        for key, value in metadata.items():
            normalized = normalize_provider_key(key)
            if normalized in {cls._LEGACY_FILL_FINGERPRINT_KEY, cls._LEGACY_EVENT_FINGERPRINT_KEY} and cls._is_generated_fingerprint(value):
                continue
            result[key] = value
        return result

    def _authoritative_event_fills(
        self,
        event: BrokerOrderEvent,
        account: Account,
        attempt: Mapping[str, object],
    ) -> tuple[tuple[BrokerFill, ...], str | None]:
        """Fetch event fills only through the adapter-authoritative read path."""
        getter = getattr(self.adapter, "get_fills", None)
        if not callable(getter):
            return (), "adapter does not expose get_fills(account, since=...)"
        try:
            raw = getter(account, since=None)
            rows = tuple(raw)
        except Exception as exc:
            return (), f"authoritative event fill lookup failed: {exc}"
        if any(not isinstance(item, BrokerFill) for item in rows):
            return (), "adapter get_fills() returned a non-normalized event fill"
        expected_external_order_id = str(
            event.external_order_id or attempt.get("external_order_id") or ""
        )
        return (
            tuple(item for item in rows if item.external_order_id == expected_external_order_id),
            None,
        )

    def _authoritative_event_snapshot(
        self,
        event: BrokerOrderEvent,
        account: Account,
        attempt: Mapping[str, object],
        leg: Mapping[str, object],
    ) -> tuple[BrokerOrderSnapshot | None, str | None]:
        """Read and validate the provider order fact behind a public event.

        Push/status arguments are advisory.  The returned snapshot is the
        only public-route source allowed to drive broker-order lifecycle; the
        caller's status, cumulative quantity, and no-fill flag are never
        trusted directly.
        """
        getter = getattr(self.adapter, "get_order", None)
        if not callable(getter):
            return None, "adapter does not expose get_order(account, external_order_id)"
        external_order_id = event.external_order_id or attempt.get("external_order_id")
        if not external_order_id:
            return None, "event has no external order identity for authoritative status lookup"
        try:
            snapshot = getter(account, str(external_order_id))
        except Exception as exc:
            return None, f"authoritative event status lookup failed: {exc}"
        if snapshot is None:
            return None, f"authoritative order {external_order_id!r} was not found"
        if not isinstance(snapshot, BrokerOrderSnapshot):
            return None, "adapter get_order() returned a non-normalized order snapshot"
        mismatches: list[str] = []
        if snapshot.status is BrokerOrderStatus.UNKNOWN:
            mismatches.append("unknown_broker_order_status")
        if snapshot.account_id != account.id:
            mismatches.append("account_id")
        mismatches.extend(
            f"account_alias:{item}"
            for item in self._account_alias_mismatches(
                account,
                account_id=snapshot.account_id,
                external_account_id=snapshot.external_account_id,
                metadata=snapshot.metadata,
            )
        )
        if snapshot.external_order_id != str(external_order_id):
            mismatches.append("external_order_id")
        if snapshot.instrument_id != str(leg["instrument_id"]):
            mismatches.append("instrument_id")
        if snapshot.side.value != str(leg["side"]):
            mismatches.append("side")
        try:
            submitted_quantity = Decimal(str(attempt["submitted_quantity"]))
            if snapshot.quantity != submitted_quantity:
                mismatches.append("quantity")
            mismatches.extend(
                self._broker_order_quantity_mismatches(
                    snapshot.status,
                    submitted_quantity,
                    snapshot.filled_quantity,
                )
            )
        except (KeyError, InvalidOperation, TypeError, ValueError):
            mismatches.append("submitted_quantity")
        if attempt.get("external_order_id") and snapshot.external_order_id != attempt["external_order_id"]:
            mismatches.append("durable_external_order_id")
        if snapshot.client_order_id and attempt.get("client_order_id"):
            if snapshot.client_order_id != attempt["client_order_id"]:
                mismatches.append("client_order_id")
        snapshot_time = snapshot.order_time or snapshot.captured_at
        mismatches.extend(
            f"temporal_order:{item}"
            for item in self._temporal_order_mismatches(attempt, snapshot_time)
        )
        if str(attempt.get("status")) in {
            BrokerOrderStatus.FILLED.value,
            BrokerOrderStatus.REJECTED.value,
            BrokerOrderStatus.CANCELLED.value,
            BrokerOrderStatus.FAILED.value,
        } and snapshot.status.value != str(attempt.get("status")):
            mismatches.append("local_terminal_status_conflict")
        try:
            coerce_provider_payload(snapshot.metadata)
        except ProviderPayloadError as exc:
            mismatches.append(f"metadata:{exc}")
        if (
            snapshot.status in {
                BrokerOrderStatus.REJECTED,
                BrokerOrderStatus.CANCELLED,
                BrokerOrderStatus.FAILED,
            }
            and snapshot.filled_quantity == 0
        ):
            try:
                event_metadata = coerce_provider_payload(event.metadata)
            except ProviderPayloadError as exc:
                mismatches.append(f"event_metadata:{exc}")
            else:
                if not isinstance(event_metadata, Mapping):
                    mismatches.append("event_metadata:not_mapping")
                elif any(
                    self._payload_has_submission_evidence(value)
                    for value in event_metadata.values()
                ):
                    # Public event metadata remains advisory.  Validate every
                    # sibling branch before the authenticated snapshot is
                    # re-enveloped; a caller cannot hide a deal claim beside
                    # a clean raw row or authoritative-snapshot wrapper.
                    mismatches.append("event_metadata:sibling_submission_evidence")
        if (
            snapshot.status in {
                BrokerOrderStatus.REJECTED,
                BrokerOrderStatus.CANCELLED,
                BrokerOrderStatus.FAILED,
            }
            and snapshot.filled_quantity == 0
            and not self._snapshot_no_fill_is_authoritative(
                snapshot,
                expected_quantity=Decimal(str(attempt["submitted_quantity"])),
                expected_instrument_id=str(leg["instrument_id"]),
            )
        ):
            mismatches.append("terminal_no_fill_evidence")
        if mismatches:
            return None, "authoritative order snapshot mismatch: " + ", ".join(mismatches)
        return snapshot, None

    def _record_terminal_fill_contradiction(
        self,
        *,
        intent_id: str,
        account: Account,
        leg: Mapping[str, object],
        broker_order_id: str,
        terminal_status: BrokerOrderStatus | str,
        source: str,
        cumulative_filled_quantity: Decimal | None,
        incoming_fill_count: int,
        observed_positions: Mapping[str, object],
    ) -> bool:
        """Quarantine a terminal no-fill fact contradicted by fill evidence."""
        status = (
            terminal_status.value
            if isinstance(terminal_status, BrokerOrderStatus)
            else str(terminal_status)
        )
        try:
            cumulative = Decimal("0") if cumulative_filled_quantity is None else Decimal(str(cumulative_filled_quantity))
        except (InvalidOperation, TypeError, ValueError):
            cumulative = Decimal("-1")
        if cumulative == 0 and incoming_fill_count == 0:
            return False
        self._force_leg_reconciliation(str(leg["id"]), str(leg["status"]))
        details = {
            "broker_order_id": broker_order_id,
            "terminal_status": status,
            "cumulative_filled_quantity": str(cumulative_filled_quantity)
            if cumulative_filled_quantity is not None
            else None,
            "incoming_fill_count": incoming_fill_count,
            "source": source,
        }
        self._require_reconciliation(
            intent_id,
            account,
            category="TERMINAL_FILL_CONTRADICTION",
            entity_type="BROKER_ORDER",
            entity_key=broker_order_id,
            details=details,
        )
        self._record_recovery_action(
            intent=self._required_intent(intent_id),
            account=account,
            action_key=f"TERMINAL_FILL_CONTRADICTION:{broker_order_id}:{source}",
            state="RECONCILIATION_REQUIRED",
            summary=(
                f"Broker order reported {status} terminally, but fill evidence was also observed; "
                "no allocation or terminal completion was inferred."
            ),
            observed_positions=observed_positions,
            remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
            allowed_next_steps=(
                "refresh_broker_facts",
                "refresh_broker_fills",
                "reconcile_broker_order",
                "reconcile_broker_position",
                "operator_review",
            ),
            metadata=details,
        )
        return True

    def _set_leg_terminal(self, leg_id: str, leg_status: str, target: LegStatus) -> None:
        if leg_status == target.value:
            return
        if leg_status == LegStatus.PLANNED.value and target is LegStatus.REJECTED:
            self.repository.transition_leg(leg_id, LegStatus.SUBMITTING, now=self._now())
        self.repository.transition_leg(leg_id, target, now=self._now())

    def _durable_filled_quantity(self, broker_order_id: str) -> Decimal:
        total = Decimal("0")
        seen_external_fill_ids: set[str] = set()
        for fill in self.repository.fills_for_broker_order(broker_order_id):
            external_fill_id = str(fill.get("external_fill_id") or "").strip()
            if external_fill_id:
                if external_fill_id in seen_external_fill_ids:
                    raise ValueError(
                        f"duplicate durable external fill identity for broker order {broker_order_id}"
                    )
                seen_external_fill_ids.add(external_fill_id)
            try:
                quantity = Decimal(str(fill["quantity"]))
            except (InvalidOperation, TypeError, ValueError) as exc:
                raise ValueError(f"invalid durable fill quantity for broker order {broker_order_id}") from exc
            if not quantity.is_finite() or quantity <= 0:
                raise ValueError(f"invalid durable fill quantity for broker order {broker_order_id}")
            total += quantity
        return total

    @staticmethod
    def _broker_order_quantity_mismatches(
        status: BrokerOrderStatus,
        submitted_quantity: Decimal,
        filled_quantity: Decimal,
    ) -> list[str]:
        """Check the normalized status/cumulative quantity contract.

        A broker fact is not self-consistent merely because its quantity is in
        range: PARTIALLY_FILLED must be strict partial, FILLED must be exact,
        and working/rejected/failed states cannot silently carry positive
        or complete fills.  CANCELLED may retain a strict partial fill.
        """
        if status is BrokerOrderStatus.UNKNOWN:
            return ["unknown_broker_order_status"]
        if submitted_quantity <= 0:
            return ["submitted_quantity_not_positive"]
        if filled_quantity < 0 or filled_quantity > submitted_quantity:
            return ["filled_quantity_range"]
        if status in {
            BrokerOrderStatus.PREPARED,
            BrokerOrderStatus.SUBMITTING,
            BrokerOrderStatus.WORKING,
        }:
            return [] if filled_quantity == 0 else [f"{status.value.lower()}_with_nonzero_fill"]
        if status is BrokerOrderStatus.PARTIALLY_FILLED:
            return [] if 0 < filled_quantity < submitted_quantity else [
                "partial_status_requires_strict_partial_fill"
            ]
        if status is BrokerOrderStatus.FILLED:
            return [] if filled_quantity == submitted_quantity else [
                "filled_status_requires_complete_fill"
            ]
        if status in {BrokerOrderStatus.REJECTED, BrokerOrderStatus.FAILED}:
            return [] if filled_quantity == 0 else [f"{status.value.lower()}_with_positive_fill"]
        if status is BrokerOrderStatus.CANCELLED:
            return [] if filled_quantity < submitted_quantity else [
                "cancelled_status_cannot_be_complete_fill"
            ]
        return ["unsupported_broker_order_status"]

    def _validate_poll_snapshot(
        self,
        *,
        account: Account,
        attempt: Mapping[str, object],
        leg: Mapping[str, object],
        snapshot: BrokerOrderSnapshot,
        require_working_zero_fill_evidence: bool = False,
    ) -> dict[str, object]:
        """Validate normalized broker facts before they can affect lifecycle state."""
        mismatches: list[str] = []
        if snapshot.status is BrokerOrderStatus.UNKNOWN:
            mismatches.append("unknown_broker_order_status")
        if snapshot.account_id != account.id or str(attempt.get("account_id")) != account.id:
            mismatches.append("account_id")
        mismatches.extend(
            f"account_alias:{item}"
            for item in self._account_alias_mismatches(
                account,
                account_id=snapshot.account_id,
                external_account_id=snapshot.external_account_id,
                metadata=snapshot.metadata,
            )
        )
        if snapshot.instrument_id != str(leg["instrument_id"]):
            mismatches.append("instrument_id")
        if snapshot.side.value != str(leg["side"]):
            mismatches.append("side")
        # When the provider does not expose an order timestamp, the broker
        # snapshot capture itself is the only temporal fact available.  It is
        # still safer than accepting a terminal fact that predates submission.
        snapshot_time = snapshot.order_time or snapshot.captured_at
        mismatches.extend(
            f"temporal_order:{item}"
            for item in self._temporal_order_mismatches(attempt, snapshot_time)
        )
        if (
            str(attempt.get("status")) in {
                BrokerOrderStatus.FILLED.value,
                BrokerOrderStatus.REJECTED.value,
                BrokerOrderStatus.CANCELLED.value,
                BrokerOrderStatus.FAILED.value,
            }
            and snapshot.status.value != str(attempt.get("status"))
        ):
            mismatches.append("local_terminal_status_conflict")
        try:
            submitted_quantity = Decimal(str(attempt["submitted_quantity"]))
            if snapshot.quantity != submitted_quantity:
                mismatches.append("quantity")
            if snapshot.filled_quantity < 0 or snapshot.filled_quantity > submitted_quantity:
                mismatches.append("filled_quantity_range")
            mismatches.extend(
                self._broker_order_quantity_mismatches(
                    snapshot.status,
                    submitted_quantity,
                    snapshot.filled_quantity,
                )
            )
            durable_quantity = self._durable_filled_quantity(str(attempt["id"]))
            if snapshot.filled_quantity != durable_quantity:
                mismatches.append("filled_quantity_not_supported_by_durable_fills")
            if (
                snapshot.status in {BrokerOrderStatus.REJECTED, BrokerOrderStatus.CANCELLED}
                and snapshot.filled_quantity == 0
                and not self._snapshot_no_fill_is_authoritative(
                    snapshot,
                    expected_quantity=submitted_quantity,
                    expected_instrument_id=str(leg["instrument_id"]),
                )
            ):
                mismatches.append("terminal_no_fill_evidence")
            if (
                require_working_zero_fill_evidence
                and snapshot.status in {
                    BrokerOrderStatus.PREPARED,
                    BrokerOrderStatus.SUBMITTING,
                    BrokerOrderStatus.WORKING,
                }
                and snapshot.filled_quantity == 0
                and not self._snapshot_no_fill_is_authoritative(
                    snapshot,
                    expected_quantity=submitted_quantity,
                    expected_instrument_id=str(leg["instrument_id"]),
                )
            ):
                mismatches.append("working_zero_fill_evidence")
        except (KeyError, InvalidOperation, TypeError, ValueError, sqlite3.Error) as exc:
            mismatches.append(f"quantity_evidence:{exc}")
            submitted_quantity = Decimal("0")
            durable_quantity = Decimal("0")
        if attempt.get("external_order_id") and snapshot.external_order_id != attempt["external_order_id"]:
            mismatches.append("external_order_id")
        if snapshot.client_order_id and attempt.get("client_order_id"):
            if snapshot.client_order_id != attempt["client_order_id"]:
                mismatches.append("client_order_id")
        return {
            "valid": not mismatches,
            "mismatches": mismatches,
            "submitted_quantity": str(submitted_quantity),
            "snapshot_filled_quantity": str(snapshot.filled_quantity),
            "durable_filled_quantity": str(durable_quantity),
            "snapshot_status": snapshot.status.value,
        }

    def _record_poll_snapshot_failure(
        self,
        *,
        intent: Mapping[str, object],
        account: Account,
        leg: Mapping[str, object],
        attempt: Mapping[str, object],
        observed_positions: Mapping[str, object],
        validation: Mapping[str, object],
    ) -> None:
        self._mark_leg_reconciliation(str(leg["id"]), str(leg["status"]))
        try:
            current_order = BrokerOrderStatus(str(attempt["status"]))
            if current_order not in {
                BrokerOrderStatus.UNKNOWN,
                BrokerOrderStatus.FILLED,
                BrokerOrderStatus.REJECTED,
                BrokerOrderStatus.CANCELLED,
            }:
                self.repository.transition_broker_order(
                    str(attempt["id"]),
                    BrokerOrderStatus.UNKNOWN,
                    now=self._now(),
                )
        except (KeyError, ValueError, sqlite3.Error):
            pass
        details = dict(validation)
        details["broker_order_id"] = str(attempt["id"])
        self._require_reconciliation(
            str(intent["id"]),
            account,
            category="BROKER_SNAPSHOT_MISMATCH",
            entity_type="BROKER_ORDER",
            entity_key=str(attempt["id"]),
            details=details,
        )
        self._record_recovery_action(
            intent=intent,
            account=account,
            action_key=f"SNAPSHOT_MISMATCH:{attempt['id']}",
            state="RECONCILIATION_REQUIRED",
            summary=(
                f"Broker snapshot for order {attempt['id']} failed identity or fill-evidence validation; "
                "do not submit, cancel, hedge, or infer exposure."
            ),
            observed_positions=observed_positions,
            remaining_quantities=self._remaining_quantities(self._required_intent(str(intent["id"]))),
            allowed_next_steps=("refresh_broker_facts", "refresh_broker_fills", "reconcile_broker_position", "operator_review"),
            metadata=details,
        )

    def recovery_status(self, intent_id: str, *, account: Account | None = None) -> dict:
        """Return durable recovery facts and operator actions without submitting."""
        self._startup_safety_audit()
        intent = self._required_intent(intent_id)
        account_id = str(intent["account_id"])
        canonical = self._canonical_account_for_intent(
            intent,
            supplied=account,
            source="recovery_status",
        )
        if canonical is not None:
            account = canonical
            self._validate_persisted_execution_policy(intent, account, source="recovery_status")
            self._ensure_terminal_history_safety(account)
            self._quarantine_multiple_attempts(intent_id, account)
            intent = self._required_intent(intent_id)
        expired_history = self.repository.expired_terminal_attempts(
            account_id,
            now=self._now(),
            terminal_window_seconds=self.TERMINAL_RECOVERY_WINDOW_SECONDS,
        )
        legs: list[dict[str, object]] = []
        for leg in intent["legs"]:
            attempts = self.repository.broker_orders_for_leg(str(leg["id"]))
            legs.append(
                {
                    "id": leg["id"],
                    "instrument_id": leg["instrument_id"],
                    "status": leg["status"],
                    "requested_quantity": leg["quantity"],
                    "cumulative_filled_quantity": leg["cumulative_filled_quantity"],
                    "remaining_quantity": self._remaining_quantities({"legs": (leg,)})[str(leg["id"])],
                    "attempts": [
                        {
                            "id": attempt["id"],
                            "external_order_id": attempt["external_order_id"],
                            "status": attempt["status"],
                            "submitted_at": attempt["submitted_at"],
                            "updated_at": attempt["updated_at"],
                        }
                        for attempt in attempts
                    ],
                }
            )
        account_open_actions = self.repository.open_recovery_actions(account_id)
        open_actions = [
            action
            for action in account_open_actions
            if action["intent_id"] == intent_id
        ]
        resolved_action_keys = {
            str(action["action_key"])
            for action in self.repository.recovery_actions_for_intent(intent_id)
            if action["status"] == RecoveryActionStatus.RESOLVED.value
        }
        unresolved_expired_history = [
            row
            for row in expired_history
            if f"TERMINAL_HISTORY_EXPIRED:{row['broker_order_id']}" not in resolved_action_keys
            and not (
                account is not None
                and self._expired_attempt_is_durable_no_submit(row, account)
            )
        ]
        fresh_authoritative_facts_valid = False
        fresh_authoritative_facts_error: str | None = None
        fresh_execution_evidence_ready = False
        fresh_execution_evidence_error: str | None = None
        fresh_broker_positions: dict[str, str] = {}
        durable_managed_exposure: dict[str, str] = {}
        fresh_exposure_agrees = False
        fresh_exposure_error: str | None = None
        if canonical is not None:
            try:
                facts, positions, _open_orders = self._strict_authoritative_account_facts(
                    canonical,
                    max_age_seconds=self.BROKER_FACT_MAX_AGE_SECONDS,
                    require_no_open_orders=True,
                )
                self._require_usable_execution_evidence(
                    canonical,
                    facts,
                    source="recovery status",
                )
                fresh_execution_evidence_ready = True
                self._validate_recovery_fact_fill_provenance(canonical, facts.fills)
                observed_exposure = {
                    instrument_id: Decimal(quantity)
                    for instrument_id, quantity in self._recovery_observed_positions(positions).items()
                }
                durable_exposure = self._strict_durable_managed_account_exposure(canonical)
                fresh_broker_positions = {
                    instrument_id: str(quantity)
                    for instrument_id, quantity in sorted(observed_exposure.items())
                }
                durable_managed_exposure = {
                    instrument_id: str(quantity)
                    for instrument_id, quantity in sorted(durable_exposure.items())
                }
                if observed_exposure != durable_exposure:
                    raise OMSExecutionError(
                        "fresh broker positions do not match durable managed exposure"
                    )
                fresh_exposure_agrees = True
            except Exception as exc:
                fresh_authoritative_facts_error = str(exc)
                fresh_execution_evidence_error = str(exc)
                fresh_exposure_error = str(exc)
            else:
                fresh_authoritative_facts_valid = True
        active_statuses = {
            IntentStatus.SUBMITTING.value,
            IntentStatus.WORKING.value,
            IntentStatus.PARTIALLY_FILLED.value,
            IntentStatus.RECONCILIATION_REQUIRED.value,
        }
        return {
            "intent_id": intent_id,
            "account_id": account_id,
            "intent_status": intent["status"],
            "legs": legs,
            "operator_actions": self.repository.recovery_actions_for_intent(intent_id),
            "open_reconciliation_issues": self.repository.open_reconciliation_issues(account_id),
            "fresh_authoritative_facts_valid": fresh_authoritative_facts_valid,
            "fresh_authoritative_facts_error": fresh_authoritative_facts_error,
            "fresh_execution_evidence_ready": fresh_execution_evidence_ready,
            "fresh_execution_evidence_error": fresh_execution_evidence_error,
            "fresh_broker_positions": fresh_broker_positions,
            "durable_managed_exposure": durable_managed_exposure,
            "fresh_exposure_agrees": fresh_exposure_agrees,
            "fresh_exposure_error": fresh_exposure_error,
            "safe_to_submit": not bool(
                self.repository.open_reconciliation_issues(account_id)
                or account_open_actions
                or canonical is None
                or not fresh_authoritative_facts_valid
                or not fresh_execution_evidence_ready
                or intent["status"] in active_statuses
                or unresolved_expired_history
            ),
        }

    def recover_pending_intents(self, *, account: Account) -> list[dict]:
        """Poll every durable active intent only from one fresh fact snapshot."""
        account = self._canonical_account_for_boundary(account, source="recover_pending_intents")
        recoverable_ids = self.repository.recoverable_intent_ids(
            account.id,
            now=self._now(),
            terminal_window_seconds=self.TERMINAL_RECOVERY_WINDOW_SECONDS,
        )
        if not recoverable_ids:
            return []
        # Proof-backed retired/round-trip closures are already durable
        # terminal evidence.  Do not require a fresh provider snapshot merely
        # to return those rows: a transient read failure must not create a new
        # account blocker or demote a closed lifecycle.  Active rows still
        # require the one strict account-fact context below.
        proof_closed = self._closed_historical_intent_ids(account)
        active_ids = [intent_id for intent_id in recoverable_ids if intent_id not in proof_closed]
        if not active_ids:
            return [
                intent
                for intent_id in recoverable_ids
                if (intent := self.repository.get_intent(intent_id)) is not None
            ]
        try:
            context = self._validated_recovery_fact_context(account)
        except Exception as exc:
            blocked_by_id: dict[str, dict] = {}
            for intent_id in active_ids:
                intent = self.repository.get_intent(intent_id)
                if intent is None:
                    continue
                self._record_recovery_fact_failure(intent, account, exc)
                blocked_by_id[intent_id] = self._required_intent(intent_id)
            return [
                row
                for intent_id in recoverable_ids
                if (row := self.repository.get_intent(intent_id)) is not None
                and (intent_id in proof_closed or intent_id in blocked_by_id)
            ]
        previous_context = self._recovery_fact_context
        self._recovery_fact_context = context
        try:
            return self._recover_pending_intents_with_facts(account=account)
        finally:
            self._recovery_fact_context = previous_context

    def _recover_pending_intents_with_facts(self, *, account: Account) -> list[dict]:
        """Internal pending recovery body with a validated context installed."""
        account = self._canonical_account_for_boundary(account, source="recover_pending_intents")
        self._ensure_terminal_history_safety(account)
        self._quarantine_multiple_attempts_for_account(account)
        proof_closed = self._closed_historical_intent_ids(account)
        recovered: list[dict] = []
        recoverable_ids = self.repository.recoverable_intent_ids(
            account.id,
            now=self._now(),
            terminal_window_seconds=self.TERMINAL_RECOVERY_WINDOW_SECONDS,
        )
        # Recover compensating EXIT lifecycles before their source ENTER
        # lifecycles.  A source intent with complete fills can otherwise be
        # temporarily re-quarantined solely because a sibling EXIT still has
        # a WAIT_FOR_BROKER action; once the EXIT is recovered, no second
        # broker cycle should be required to restore the source aggregate.
        # This ordering does not promote anything by itself: each intent still
        # applies the full broker-fact, fill, duplicate-attempt, and blocker
        # gates in recover_intent().
        ordered_ids = sorted(
            recoverable_ids,
            key=lambda intent_id: (
                0
                if str((self.repository.get_intent(intent_id) or {}).get("action"))
                in {IntentAction.EXIT.value, IntentAction.FLATTEN.value}
                else 1,
                str(intent_id),
            ),
        )
        for intent_id in ordered_ids:
            # A validated retired-baseline or round-trip closure is durable
            # lifecycle evidence.  Do not poll it again merely because its
            # terminal broker rows are recent; a transient read failure must
            # not demote a proven-closed intent back to reconciliation.  The
            # account-wide fresh-facts gate still protects every new submit.
            if intent_id in proof_closed:
                intent = self.repository.get_intent(intent_id)
                if intent is not None:
                    recovered.append(intent)
                continue
            recovered.append(self._recover_intent_with_facts(intent_id, account=account))
        return recovered

    # Short alias for callers that describe the operation as a polling cycle.
    def poll_and_recover(self, *, account: Account) -> list[dict]:
        return self.recover_pending_intents(account=account)

    def _ensure_terminal_history_safety(self, account: Account) -> None:
        """Persist blockers for terminal attempts beyond automatic polling scope."""
        expired = self.repository.expired_terminal_attempts(
            account.id,
            now=self._now(),
            terminal_window_seconds=self.TERMINAL_RECOVERY_WINDOW_SECONDS,
        )
        verified_retired_order_ids = self._verified_retired_baseline_order_ids(account)
        closed_historical_intents = self._closed_historical_intent_ids(account)
        for row in expired:
            if (
                str(row["broker_order_id"]) in verified_retired_order_ids
                or str(row["intent_id"]) in closed_historical_intents
            ):
                continue
            # A normalized provider rejection with no external order claim is
            # already durable no-submit evidence.  It must not be converted
            # into a generic aged-history blocker merely because the local
            # rejected row is old.  Keep this predicate deliberately narrow:
            # an external ID, any fill, ambiguous evidence, or a missing
            # no-submit assertion remains subject to the normal age guard.
            if self._expired_attempt_is_durable_no_submit(row, account):
                continue
            intent_id = str(row["intent_id"])
            action_key = f"TERMINAL_HISTORY_EXPIRED:{row['broker_order_id']}"
            actions = self.repository.recovery_actions_for_intent(intent_id)
            existing = next((item for item in actions if item["action_key"] == action_key), None)
            if existing is not None and existing["status"] == RecoveryActionStatus.RESOLVED.value:
                # Explicit operator reconciliation is durable. Do not silently
                # reopen it on every status or submission check.
                continue
            intent = self._required_intent(intent_id)
            self._require_reconciliation(
                intent_id,
                account,
                category="TERMINAL_HISTORY_EXPIRED",
                entity_type="BROKER_ORDER",
                entity_key=str(row["broker_order_id"]),
                details={
                    "broker_order_id": str(row["broker_order_id"]),
                    "broker_order_status": str(row["broker_order_status"]),
                    "updated_at": row["updated_at"],
                    "recovery_window_seconds": self.TERMINAL_RECOVERY_WINDOW_SECONDS,
                    "requires_explicit_history_reconciliation": True,
                },
            )
            self._record_recovery_action(
                intent=intent,
                account=account,
                action_key=action_key,
                state="RECONCILIATION_REQUIRED",
                summary="Terminal broker order history is older than the automatic restart-recovery window; verify account/order history before any new submission.",
                observed_positions={"_status": "TERMINAL_HISTORY_EXPIRED"},
                remaining_quantities=self._remaining_quantities(intent),
                allowed_next_steps=("reconcile_terminal_order_history", "refresh_broker_facts", "operator_review"),
                metadata={
                    "broker_order_id": str(row["broker_order_id"]),
                    "external_order_id": row["external_order_id"],
                    "updated_at": row["updated_at"],
                    "recovery_window_seconds": self.TERMINAL_RECOVERY_WINDOW_SECONDS,
                },
            )

    def _expired_attempt_is_durable_no_submit(
        self,
        row: Mapping[str, object],
        account: Account,
    ) -> bool:
        """Recognize only an owned, fill-free, definite no-submit row.

        This is used before a recovery poll, so it intentionally relies only
        on durable local provider assertions.  A later fresh open-order poll
        still runs through ``_restore_definite_rejection`` and its stricter
        client-order identity check before the intent can be terminalized.
        """
        broker_order_id = str(row.get("broker_order_id", "")).strip()
        intent_id = str(row.get("intent_id", "")).strip()
        if not broker_order_id or not intent_id:
            return False
        if str(row.get("account_id", "")) != account.id:
            return False
        if str(row.get("broker_order_status", "")) != BrokerOrderStatus.REJECTED.value:
            return False
        if row.get("external_order_id"):
            return False
        intent = self.repository.get_intent(intent_id)
        if intent is None or str(intent.get("account_id")) != account.id:
            return False
        for leg in intent.get("legs", ()):
            if not isinstance(leg, Mapping) or str(leg.get("status")) not in {
                LegStatus.REJECTED.value,
                LegStatus.RECONCILIATION_REQUIRED.value,
            }:
                continue
            attempts = self.repository.broker_orders_for_leg(str(leg.get("id")))
            attempt = next(
                (item for item in attempts if str(item.get("id")) == broker_order_id),
                None,
            )
            if attempt is None:
                continue
            if str(attempt.get("account_id", "")) != account.id:
                return False
            # A durable fill row wins over local no-submit metadata.  This
            # prevents an aged contradictory record from being skipped.
            if self.repository.fills_for_broker_order(broker_order_id):
                return False
            return self._is_durable_no_submit_rejection(attempt, dict(leg), ())
        return False
    def _verified_retired_baseline_books(self, account: Account) -> set[str]:
        """Return only retired books proven by the account's verified baseline."""
        baseline = self.repository.latest_execution_evidence_baseline(account.id)
        if baseline is None or not isinstance(baseline.metadata, Mapping):
            return set()
        book_id = str(baseline.metadata.get("legacy_book_id", "")).strip()
        if not book_id:
            return set()
        verified, _reason = self.repository.verified_retired_book_closure(
            account.id,
            book_id,
            baseline,
            allow_retired_baseline_blockers=True,
            capacity_only=True,
        )
        return {book_id} if verified else set()

    def _verified_retired_baseline_order_ids(self, account: Account) -> set[str]:
        """Return local broker-order IDs covered by a verified retired baseline."""
        books = self._verified_retired_baseline_books(account)
        order_ids: set[str] = set()
        for book_id in books:
            order_ids.update(
                str(row["id"])
                for row in self.repository.book_broker_orders(account.id, book_id=book_id)
                if str(row.get("id", "")).strip()
            )
        return order_ids

    def _closed_historical_intent_ids(self, account: Account) -> set[str]:
        """Return proof-backed historical intents that are capacity-neutral.

        A retired import is admitted only by its immutable baseline/source
        ledger proof.  A compensated partial is admitted only when its
        persisted closure envelope names the exact compensation intent group
        and every leg in that group remains terminal with attempt-scoped fill
        evidence.  No status or book label is sufficient by itself.
        """
        closed: set[str] = set()
        for book_id in self._verified_retired_baseline_books(account):
            closed.update(str(row["id"]) for row in self.repository.book_intents(account.id, book_id=book_id))

        for row in self.repository.book_intents(account.id):
            intent_id = str(row["id"])
            intent = self.repository.get_intent(intent_id)
            if intent is None or str(intent.get("account_id")) != account.id:
                continue
            metadata = intent.get("metadata")
            if not isinstance(metadata, Mapping):
                continue
            roundtrip = metadata.get("verified_roundtrip_closure")
            if isinstance(roundtrip, Mapping):
                group = roundtrip.get("intent_ids")
                group_ids = {str(value) for value in group} if isinstance(group, list) else set()
                if (
                    str(intent.get("status")) in {
                        IntentStatus.COMPLETED.value,
                        IntentStatus.RECONCILIATION_REQUIRED.value,
                    }
                    and group_ids
                    and intent_id in group_ids
                    and str(roundtrip.get("account_id", account.id)) == account.id
                ):
                    # The closure envelope remains authoritative only while
                    # every member still has the exact one-attempt durable
                    # fill proof that created it.  A later transient status
                    # demotion may be ignored for lifecycle polling, but a
                    # changed/missing fill claim must never be hidden.
                    valid_group = True
                    for candidate_id in group_ids:
                        candidate = self.repository.get_intent(candidate_id)
                        if candidate is None or str(candidate.get("account_id")) != account.id:
                            valid_group = False
                            break
                        candidate_metadata = candidate.get("metadata")
                        candidate_closure = (
                            candidate_metadata.get("verified_roundtrip_closure")
                            if isinstance(candidate_metadata, Mapping)
                            else None
                        )
                        if (
                            not isinstance(candidate_closure, Mapping)
                            or str(candidate_closure.get("account_id", account.id)) != account.id
                            or {str(value) for value in candidate_closure.get("intent_ids", ())}
                            != group_ids
                            or str(candidate.get("status"))
                            not in {
                                IntentStatus.COMPLETED.value,
                                IntentStatus.RECONCILIATION_REQUIRED.value,
                            }
                            or not self._all_legs_have_fill_evidence(candidate_id)
                        ):
                            valid_group = False
                            break
                    if valid_group:
                        closed.update(group_ids)
                continue
            closure = metadata.get("compensated_partial_closure")
            if not isinstance(closure, Mapping):
                continue
            if (
                str(intent.get("status")) not in {
                    IntentStatus.CANCELLED.value,
                    IntentStatus.RECONCILIATION_REQUIRED.value,
                }
                or str(closure.get("reason", ""))
                != "fresh_broker_and_historical_proof_of_compensated_partial_entry"
                or str(closure.get("account_id", account.id)) != account.id
                or not str(closure.get("verified_at", "")).strip()
            ):
                continue
            compensation_ids = closure.get("compensating_intent_ids")
            if not isinstance(compensation_ids, list) or not compensation_ids:
                continue
            candidate_ids = {intent_id, *(str(value) for value in compensation_ids)}
            valid = True
            for candidate_id in candidate_ids:
                candidate = self.repository.get_intent(candidate_id)
                if candidate is None or str(candidate.get("account_id")) != account.id:
                    valid = False
                    break
                if str(candidate.get("book_id") or "") != str(intent.get("book_id") or ""):
                    valid = False
                    break
                if str(candidate.get("status")) not in {
                    IntentStatus.CANCELLED.value,
                    IntentStatus.FILLED.value,
                    IntentStatus.COMPLETED.value,
                    IntentStatus.RECONCILIATION_REQUIRED.value,
                }:
                    valid = False
                    break
                for leg in candidate.get("legs", ()):
                    leg_status = str(leg.get("status"))
                    if leg_status == LegStatus.FILLED.value:
                        attempts = self.repository.broker_orders_for_leg(str(leg["id"]))
                        if len(attempts) != 1 or not self._attempt_has_complete_fill_evidence(attempts[0], leg):
                            valid = False
                            break
                    elif leg_status == LegStatus.CANCELLED.value:
                        try:
                            if Decimal(str(leg.get("cumulative_filled_quantity", "0"))) != 0:
                                valid = False
                                break
                        except (InvalidOperation, TypeError, ValueError):
                            valid = False
                            break
                    else:
                        valid = False
                        break
                if not valid:
                    break
            if valid:
                closed.update(candidate_ids)
        return closed

    def is_proof_backed_closed_broker_fill(
        self,
        *,
        account: Account,
        broker_fill: BrokerFill,
    ) -> bool:
        """Return whether one provider fill belongs to proven closed history.

        This is deliberately the same durable ownership predicate used by the
        account/capacity gate.  A provider row is historical-only only when it
        has one exact account claim, that claim belongs to a proof-backed
        closed intent/book, and its immutable fill identity/economics match
        the persisted attempt-scoped fill.  Unknown, foreign, mismatched, or
        incomplete rows return ``False`` and remain account blockers.
        """

        try:
            if not isinstance(broker_fill, BrokerFill):
                return False
            if self._account_alias_mismatches(
                account,
                account_id=broker_fill.account_id,
                metadata=broker_fill.metadata,
            ):
                return False
            claims = self.repository.broker_orders_for_external_order_id(
                broker_fill.external_order_id
            )
            if len(claims) != 1:
                return False
            claim = claims[0]
            if str(claim.get("account_id")) != account.id:
                return False
            intent_id = self.repository.intent_id_for_broker_order(str(claim.get("id")))
            if intent_id not in self._closed_historical_intent_ids(account):
                return False
            intent = self.repository.get_intent(str(intent_id))
            if intent is None or str(intent.get("account_id")) != account.id:
                return False
            leg = next(
                (
                    item
                    for item in intent.get("legs", ())
                    if str(item.get("id")) == str(claim.get("order_leg_id"))
                ),
                None,
            )
            if leg is None or str(leg.get("status")) != LegStatus.FILLED.value:
                return False
            attempts = self.repository.broker_orders_for_leg(str(leg.get("id")))
            if len(attempts) != 1 or str(attempts[0].get("id")) != str(claim.get("id")):
                return False
            if not self._attempt_has_complete_fill_evidence(claim, leg):
                return False
            expected_instrument = str(leg.get("instrument_id") or "").strip()
            if not expected_instrument or broker_fill.instrument_id != expected_instrument:
                return False
            self._validate_broker_fill_provenance(
                claim,
                broker_fill,
                expected_instrument_id=expected_instrument,
            )
            stored_fills = self.repository.fills_for_broker_order(str(claim.get("id")))
            if len(stored_fills) != 1:
                return False
            stored = stored_fills[0]
            stored_metadata = stored.get("metadata")
            stored_reference = (
                stored_metadata.get("_evidence_reference")
                if isinstance(stored_metadata, Mapping)
                else None
            ) or (
                stored_metadata.get("evidence_reference")
                if isinstance(stored_metadata, Mapping)
                else None
            )
            stored_fee = stored.get("fee")
            stored_fee_value = Decimal(str(stored_fee)) if stored_fee is not None else None
            # Legacy imports persist the provider's stable order reference
            # (``<account>:order:<external>``), while a fresh normalized
            # cumulative snapshot uses the attempt-scoped
            # ``<external>:<dedupe>`` reference.  Accept that one explicit
            # provider reference alias only after every stronger claim,
            # intent, leg, attempt, fill, account, instrument, and economic
            # equality check above has passed.
            legacy_order_reference = f"{account.id}:order:{broker_fill.external_order_id}"
            if (
                str(stored.get("dedupe_key")) != broker_fill.dedupe_key
                or stored.get("external_fill_id") != broker_fill.external_fill_id
                or Decimal(str(stored.get("quantity"))) != broker_fill.quantity
                or Decimal(str(stored.get("price"))) != broker_fill.price
                or stored_fee_value != broker_fill.fee
                or stored.get("fee_currency") != broker_fill.fee_currency
                or str(stored.get("filled_at")) != broker_fill.filled_at.isoformat()
                or stored_reference not in {
                    None,
                    broker_fill.evidence_reference,
                    broker_fill.dedupe_key,
                    f"{broker_fill.external_order_id}:{broker_fill.dedupe_key}",
                    legacy_order_reference,
                }
            ):
                return False
            return True
        except (KeyError, InvalidOperation, TypeError, ValueError, sqlite3.Error):
            return False

    @staticmethod
    def _dedupe_account_broker_order_facts(
        open_orders: Sequence[BrokerOrderSnapshot],
    ) -> tuple[tuple[BrokerOrderSnapshot, ...], list[dict[str, object]]]:
        """Group account facts by provider order ID before reconciliation.

        An exact duplicate row is harmless and may be queried twice by a
        broker endpoint.  Any difference in the normalized row—including
        status, quantity, instrument, fill/no-fill flags, timestamps, or
        provider metadata—is an account-fact contradiction and remains a
        durable blocker rather than being selected by first-row order.
        """
        grouped: dict[str, list[BrokerOrderSnapshot]] = {}
        for snapshot in open_orders:
            grouped.setdefault(str(snapshot.external_order_id).strip(), []).append(snapshot)
        deduped: list[BrokerOrderSnapshot] = []
        blockers: list[dict[str, object]] = []
        for external_order_id, rows in grouped.items():
            first = rows[0]
            if all(item == first for item in rows[1:]):
                deduped.append(first)
                continue
            blockers.append(
                {
                    "kind": "duplicate_broker_order_fact",
                    "external_order_id": external_order_id,
                    "row_count": len(rows),
                    "rows": [
                        {
                            "snapshot_id": item.id,
                            "account_id": item.account_id,
                            "instrument_id": item.instrument_id,
                            "status": item.status.value,
                            "quantity": str(item.quantity),
                            "filled_quantity": str(item.filled_quantity),
                            "no_fill_asserted": item.no_fill_asserted,
                            "captured_at": item.captured_at.isoformat(),
                        }
                        for item in rows
                    ],
                }
            )
            # Keep one row only for diagnostic validation; the blocker means
            # it cannot make the account gate return safe regardless.
            deduped.append(first)
        return tuple(deduped), blockers

    @staticmethod
    def _dedupe_account_position_facts(
        positions: Sequence[PositionSnapshot],
    ) -> tuple[tuple[PositionSnapshot, ...], list[dict[str, object]]]:
        """Reject contradictory duplicate position rows before netting."""

        grouped: dict[str, list[PositionSnapshot]] = {}
        for position in positions:
            grouped.setdefault(str(position.instrument_id), []).append(position)
        deduped: list[PositionSnapshot] = []
        blockers: list[dict[str, object]] = []
        for instrument_id, rows in grouped.items():
            first = rows[0]
            if all(item == first for item in rows[1:]):
                deduped.append(first)
                continue
            blockers.append(
                {
                    "kind": "duplicate_broker_position_fact",
                    "instrument_id": instrument_id,
                    "row_count": len(rows),
                    "rows": [
                        {
                            "snapshot_id": item.broker_snapshot_id,
                            "position_id": item.id,
                            "account_id": item.account_id,
                            "signed_quantity": str(item.signed_quantity),
                            "captured_at": item.captured_at.isoformat(),
                        }
                        for item in rows
                    ],
                }
            )
            deduped.append(first)
        return tuple(deduped), blockers

    @staticmethod
    def _recovery_observed_positions(
        positions: Sequence[PositionSnapshot],
    ) -> dict[str, str]:
        """Render already-validated position facts for recovery diagnostics."""
        totals: dict[str, Decimal] = {}
        for position in positions:
            instrument_id = str(position.instrument_id)
            quantity = Decimal(str(position.signed_quantity))
            totals[instrument_id] = totals.get(instrument_id, Decimal("0")) + quantity
        return {
            instrument_id: str(quantity)
            for instrument_id, quantity in sorted(totals.items())
            if quantity != 0
        }

    @staticmethod
    def _recovery_context_fill_quantity(
        context: Mapping[str, object],
        external_order_id: object,
    ) -> Decimal:
        """Return the cumulative fill quantity authenticated by a context."""
        total = Decimal("0")
        for broker_fill in context.get("fills", ()):
            if str(getattr(broker_fill, "external_order_id", "")) != str(external_order_id):
                continue
            total += Decimal(str(getattr(broker_fill, "quantity", "0")))
        return total

    def _validate_recovery_fill_provenance(
        self,
        account: Account,
        broker_fill: BrokerFill,
        *,
        attempt: Mapping[str, object],
        leg: Mapping[str, object],
    ) -> None:
        """Require a matched recovery fill to carry complete broker evidence."""
        if broker_fill.evidence_mode is ExecutionEvidenceMode.UNAVAILABLE:
            raise ValueError("authoritative recovery fill has unavailable execution evidence")
        if not str(broker_fill.evidence_reference or "").strip():
            raise ValueError("authoritative recovery fill has no evidence reference")
        expected_instrument_id = str(leg.get("instrument_id") or "").strip()
        if not expected_instrument_id:
            raise ValueError("matched recovery leg has no canonical instrument identity")
        if broker_fill.instrument_id != expected_instrument_id:
            raise ValueError("authoritative recovery fill instrument does not match durable broker order")
        self._validate_broker_fill_provenance(
            attempt,
            broker_fill,
            expected_instrument_id=expected_instrument_id,
        )

    def _validate_recovery_fact_fill_provenance(
        self,
        account: Account,
        broker_fills: Sequence[BrokerFill],
    ) -> None:
        """Authenticate current-snapshot fills before recovery writes state."""
        for broker_fill in broker_fills:
            claims = self.repository.broker_orders_for_external_order_id(
                broker_fill.external_order_id
            )
            if len(claims) != 1:
                if self.is_proof_backed_closed_broker_fill(
                    account=account,
                    broker_fill=broker_fill,
                ):
                    continue
                raise OMSExecutionError(
                    "authoritative recovery fill is not uniquely owned by one durable broker order"
                )
            attempt = claims[0]
            if str(attempt.get("account_id")) != account.id:
                raise OMSExecutionError(
                    "authoritative recovery fill belongs to a different account"
                )
            intent_id = self.repository.intent_id_for_broker_order(str(attempt.get("id")))
            if intent_id is None:
                raise OMSExecutionError(
                    "authoritative recovery fill has no durable owning intent"
                )
            if intent_id in self._closed_historical_intent_ids(account) and self.is_proof_backed_closed_broker_fill(
                account=account,
                broker_fill=broker_fill,
            ):
                continue
            owner = self.repository.get_intent(str(intent_id))
            if owner is None:
                raise OMSExecutionError(
                    "authoritative recovery fill has no durable owning intent"
                )
            leg = next(
                (
                    item
                    for item in owner.get("legs", ())
                    if str(item.get("id")) == str(attempt.get("order_leg_id"))
                ),
                None,
            )
            if not isinstance(leg, Mapping):
                raise OMSExecutionError(
                    "authoritative recovery fill has no durable owning leg"
                )
            try:
                self._validate_recovery_fill_provenance(
                    account,
                    broker_fill,
                    attempt=attempt,
                    leg=leg,
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise OMSExecutionError(str(exc)) from exc

    def _validated_recovery_fact_context(self, account: Account) -> dict[str, object]:
        """Validate the sole broker snapshot allowed to drive recovery.

        Recovery is allowed to ingest terminal order/fill evidence only after
        this account-scoped snapshot has passed the same completeness,
        identity, freshness, duplicate, and quantity checks used by order
        safety gates.  The context is then reused by the recovery helpers so
        a missing position reader or a cacheable fill reader cannot silently
        replace the validated account view.
        """
        facts, positions, open_orders = self._strict_authoritative_account_facts(
            account,
            max_age_seconds=self.BROKER_FACT_MAX_AGE_SECONDS,
        )
        # The strict fact boundary has already rejected conflicting identity
        # rows and collapses exact provider replays for every downstream
        # consumer.  Do not re-run that validation in recovery.
        fills = facts.fills
        # Recovery may persist a newly observed fill or advance a leg/intent
        # lifecycle only from the same evidence contract that authorizes an
        # order decision.  In particular, a cumulative snapshot without a
        # verified flat baseline is not enough merely because its rows look
        # internally consistent; reject before any recovery mutation.
        self._require_usable_execution_evidence(
            account,
            facts,
            source="recovery",
        )
        if fills:
            if facts.execution_evidence_mode is ExecutionEvidenceMode.UNAVAILABLE:
                raise OMSExecutionError(
                    "authoritative recovery facts contain fills but execution evidence is unavailable"
                )
            self._validate_recovery_fact_fill_provenance(
                account,
                fills,
            )
        return {
            "account_id": account.id,
            "facts": facts,
            "positions": positions,
            "open_orders": open_orders,
            "fills": fills,
            "observed_positions": self._recovery_observed_positions(positions),
        }

    def _execution_evidence_readiness(
        self,
        account: Account,
        facts: BrokerFactSnapshot,
    ) -> tuple[bool, str | None]:
        """Return whether one validated snapshot can support an order decision.

        Individual-deal evidence is self-authenticating after the strict fact
        validator has checked its identities.  Cumulative order snapshots have
        a narrower contract: the account must have a persisted, matching,
        verified-flat baseline before the snapshot can be used to authorize an
        order or to claim a clean recovery state.  Keep this decision shared by
        direct proof-gated OMS routes and status/reporting callers so the
        private compensating capability cannot bypass the same baseline rule.
        """
        if facts.execution_evidence_mode is ExecutionEvidenceMode.UNAVAILABLE:
            return False, "execution evidence is unavailable"
        if facts.execution_evidence_mode is ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS:
            baseline = self.repository.latest_execution_evidence_baseline(account.id)
            if not (
                baseline is not None
                and baseline.account_id == account.id
                and baseline.evidence_mode is ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS
                and baseline.verified_flat
                and baseline.status == "VERIFIED"
            ):
                return (
                    False,
                    "verified cumulative-order baseline requires matching account, "
                    "mode, VERIFIED status, and flat proof",
                )
        return True, None

    def _require_usable_execution_evidence(
        self,
        account: Account,
        facts: BrokerFactSnapshot,
        *,
        source: str,
    ) -> None:
        ready, reason = self._execution_evidence_readiness(account, facts)
        if not ready:
            raise OMSExecutionError(f"{source} requires usable execution evidence: {reason}")

    @staticmethod
    def _validate_authoritative_fill_identities(
        facts: BrokerFactSnapshot,
    ) -> tuple[BrokerFill, ...]:
        """Reject contradictory provider fill identities before any proof path.

        Exact replay rows are collapsed for recovery consumers.  A changed
        row sharing either the provider fill identity or its order/dedupe key
        is ambiguous and must block every safety boundary, including Stage 6
        preflight and proof-gated compensation/residual submission.
        """
        fills_by_identity: dict[tuple[str, str], BrokerFill] = {}
        fills_by_dedupe: dict[tuple[str, str], BrokerFill] = {}
        for broker_fill in facts.fills:
            identity = (
                str(broker_fill.external_order_id),
                str(broker_fill.external_fill_id or broker_fill.dedupe_key),
            )
            prior_identity = fills_by_identity.get(identity)
            if prior_identity is not None:
                if prior_identity != broker_fill:
                    raise OMSExecutionError(
                        "authoritative account facts contain contradictory duplicate fill identity"
                    )
                continue
            dedupe_identity = (str(broker_fill.external_order_id), str(broker_fill.dedupe_key))
            prior_dedupe = fills_by_dedupe.get(dedupe_identity)
            if prior_dedupe is not None and prior_dedupe != broker_fill:
                raise OMSExecutionError(
                    "authoritative account facts contain contradictory duplicate fill dedupe key"
                )
            fills_by_identity[identity] = broker_fill
            fills_by_dedupe[dedupe_identity] = broker_fill
        return tuple(fills_by_identity.values())

    def strict_authoritative_account_facts(
        self,
        account: Account,
        *,
        max_age_seconds: int | float | None = None,
        require_no_open_orders: bool = False,
    ) -> tuple[BrokerFactSnapshot, tuple[PositionSnapshot, ...], tuple[BrokerOrderSnapshot, ...]]:
        """Expose the canonical read-only account-fact safety boundary.

        Stage 6 orchestration and CLI recovery paths must use the same strict
        validator as order-bearing OMS paths.  This public wrapper does not
        add a new broker capability or mutate the ledger; it only prevents
        callers from bypassing account identity, alias, freshness, duplicate,
        quantity, and completeness checks by reading the adapter directly.
        """

        return self._strict_authoritative_account_facts(
            account,
            max_age_seconds=max_age_seconds,
            require_no_open_orders=require_no_open_orders,
        )

    def _strict_authoritative_account_facts(
        self,
        account: Account,
        *,
        max_age_seconds: int | float | None = None,
        require_no_open_orders: bool = False,
    ) -> tuple[BrokerFactSnapshot, tuple[PositionSnapshot, ...], tuple[BrokerOrderSnapshot, ...]]:
        """Read and validate one account-fact snapshot for a safety decision.

        All order-bearing proof paths must use this boundary.  In particular,
        an empty positions/open-orders collection is meaningful only after the
        adapter has returned a complete, account-scoped, fresh snapshot.  The
        duplicate and alias checks live here so individual proof routes cannot
        accidentally accept a weaker variant of the same broker facts.
        """
        getter = getattr(self.adapter, "get_authoritative_account_facts", None)
        if not callable(getter):
            raise OMSExecutionError(
                "adapter does not expose required get_authoritative_account_facts(account)"
            )
        try:
            facts = getter(account)
        except Exception as exc:
            raise OMSExecutionError(f"authoritative account facts unavailable: {exc}") from exc
        if not isinstance(facts, BrokerFactSnapshot):
            raise OMSExecutionError(
                "adapter get_authoritative_account_facts() must return BrokerFactSnapshot"
            )
        if facts.account_id != account.id:
            raise OMSExecutionError(
                "authoritative account facts belong to a different account "
                f"(got {facts.account_id!r}, expected {account.id!r})"
            )
        fact_aliases = self._account_alias_mismatches(
            account,
            account_id=facts.account_id,
            metadata=facts.metadata,
        )
        if fact_aliases:
            raise OMSExecutionError(
                "authoritative account fact identity mismatch: " + ", ".join(fact_aliases)
            )
        if not facts.complete:
            raise OMSExecutionError(
                "authoritative account facts are incomplete: "
                + (facts.error or "unspecified")
            )
        if facts.error not in (None, ""):
            raise OMSExecutionError(f"authoritative account facts report an error: {facts.error}")
        observed_at = self._parse_timestamp(facts.captured_at)
        if observed_at is None:
            raise OMSExecutionError("authoritative account facts have no valid captured_at")
        now = self._now()
        if observed_at > now + timedelta(seconds=self.BROKER_CLOCK_SKEW_TOLERANCE_SECONDS):
            raise OMSExecutionError("authoritative account facts are stale (captured_at is in the future)")
        if max_age_seconds is not None and (now - observed_at).total_seconds() > float(max_age_seconds):
            raise OMSExecutionError("authoritative account facts are stale")

        metadata_mismatches = self._broker_fact_metadata_blockers(
            account,
            facts.metadata,
            # The adapter's documented cumulative fallback reports this one
            # diagnostic marker.  The account-wide and Stage 6 gates still
            # require a verified baseline before accepting that mode.
            allow_cumulative_order_evidence=(
                facts.execution_evidence_mode is ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS
            ),
        )
        if metadata_mismatches:
            raise OMSExecutionError(
                "authoritative account fact metadata is unsafe: "
                + ", ".join(metadata_mismatches)
            )

        positions, duplicate_position_blockers = self._dedupe_account_position_facts(facts.positions)
        if duplicate_position_blockers:
            raise OMSExecutionError(
                "authoritative account facts contain contradictory duplicate rows (positions): "
                + json.dumps(duplicate_position_blockers, sort_keys=True, default=str)
            )
        for position in positions:
            aliases = self._account_alias_mismatches(
                account,
                account_id=getattr(position, "account_id", None),
                external_account_id=getattr(position, "external_account_id", None),
                metadata=getattr(position, "metadata", None),
            )
            if aliases:
                raise OMSExecutionError(
                    f"authoritative position {position.instrument_id} has account mismatch: {aliases}"
                )
            quantity = Decimal(str(position.signed_quantity))
            if not quantity.is_finite():
                raise OMSExecutionError(
                    f"authoritative position {position.instrument_id} has a non-finite quantity"
                )

        open_orders, duplicate_order_blockers = self._dedupe_account_broker_order_facts(facts.open_orders)
        if duplicate_order_blockers:
            raise OMSExecutionError(
                "authoritative account facts contain contradictory duplicate orders: "
                + json.dumps(duplicate_order_blockers, sort_keys=True, default=str)
            )
        for snapshot in open_orders:
            aliases = self._account_alias_mismatches(
                account,
                account_id=snapshot.account_id,
                external_account_id=snapshot.external_account_id,
                metadata=snapshot.metadata,
            )
            if aliases:
                raise OMSExecutionError(
                    f"authoritative order {snapshot.external_order_id} has account mismatch: {aliases}"
                )
            quantity_mismatches = self._broker_order_quantity_mismatches(
                snapshot.status,
                snapshot.quantity,
                snapshot.filled_quantity,
            )
            if quantity_mismatches:
                raise OMSExecutionError(
                    f"authoritative order {snapshot.external_order_id} has contradictory status/quantity facts: "
                    + ", ".join(quantity_mismatches)
                )
        if require_no_open_orders and open_orders:
            raise OMSExecutionError("broker account is not freshly flat: authoritative open orders are present")

        for broker_fill in facts.fills:
            aliases = self._account_alias_mismatches(
                account,
                account_id=broker_fill.account_id,
                metadata=broker_fill.metadata,
            )
            if aliases:
                raise OMSExecutionError(
                    f"authoritative fill {broker_fill.dedupe_key} has account mismatch: {aliases}"
                )
            if not broker_fill.quantity.is_finite() or broker_fill.quantity <= 0:
                raise OMSExecutionError(
                    f"authoritative fill {broker_fill.dedupe_key} has an invalid quantity"
                )
        validated_fills = self._validate_authoritative_fill_identities(facts)
        if validated_fills != facts.fills:
            facts = replace(facts, fills=validated_fills)
        return facts, positions, open_orders

    def strict_historical_order_facts(
        self,
        account: Account,
        *,
        requested_start: datetime,
        requested_end: datetime,
    ) -> BrokerHistoricalOrderFacts:
        """Expose the canonical bounded historical-facts safety boundary."""

        return self._strict_historical_order_facts(
            account,
            requested_start=requested_start,
            requested_end=requested_end,
        )

    def _strict_historical_order_facts(
        self,
        account: Account,
        *,
        requested_start: datetime,
        requested_end: datetime,
    ) -> BrokerHistoricalOrderFacts:
        """Read one complete, fresh, account-scoped bounded history window.

        Residual proof uses this boundary for attempted terminal-zero orders.
        A current account snapshot can be flat while a late fill remains only
        in the provider's historical order endpoint; treating that omission as
        no-fill would make the residual order unsafe.  Keep this validator
        independent from the fresh account-facts helper because historical
        coverage is an additional proof obligation, not a replacement for it.
        """
        start = self._parse_timestamp(requested_start)
        end = self._parse_timestamp(requested_end)
        if start is None or end is None or end <= start:
            raise OMSExecutionError("historical order proof window is invalid")
        getter = getattr(self.adapter, "get_historical_order_facts", None)
        if not callable(getter):
            raise OMSExecutionError("bounded historical order facts are required")
        try:
            history = getter(account, start, end)
        except Exception as exc:
            raise OMSExecutionError(f"historical order facts unavailable: {exc}") from exc
        if not isinstance(history, BrokerHistoricalOrderFacts):
            raise OMSExecutionError("historical order provider returned an invalid result")
        if history.account_id != account.id:
            raise OMSExecutionError("historical order facts belong to a different account")
        aliases = self._account_alias_mismatches(
            account,
            account_id=history.account_id,
            metadata=history.metadata,
        )
        if aliases:
            raise OMSExecutionError("historical order fact identity mismatch: " + ", ".join(aliases))
        if not history.complete or history.error:
            raise OMSExecutionError(
                f"bounded historical order facts are incomplete: {history.error or 'unspecified'}"
            )
        if (
            history.requested_start > start
            or history.requested_end < end
            or history.execution_evidence_mode is ExecutionEvidenceMode.UNAVAILABLE
            or "HISTORICAL_ORDER_SNAPSHOTS" not in history.execution_evidence_scope
        ):
            raise OMSExecutionError("historical order facts do not cover the complete proof window")
        captured_at = self._parse_timestamp(history.captured_at)
        now = self._now()
        if captured_at is None:
            raise OMSExecutionError("historical order facts have no valid captured_at")
        if captured_at > now + timedelta(seconds=self.BROKER_CLOCK_SKEW_TOLERANCE_SECONDS):
            raise OMSExecutionError("historical order facts are from the future")
        if (now - captured_at).total_seconds() > self.TERMINAL_RECOVERY_WINDOW_SECONDS:
            raise OMSExecutionError("historical order facts are stale")
        return history

    def _strict_roundtrip_broker_evidence(
        self,
        account: Account,
        *,
        expected_rows: Sequence[Mapping[str, object]],
        requested_start: datetime,
        requested_end: datetime,
    ) -> tuple[BrokerFactSnapshot, BrokerHistoricalOrderFacts]:
        """Validate the complete broker evidence set used by round-trip closure.

        The two verified round-trip resolvers must not each implement a weaker
        variant of the account/fill/history contract.  This boundary reuses the
        strict account and bounded-history readers, then authenticates every
        current and historical fill against the explicitly named durable rows.
        Unknown fills, duplicate evidence identities, stale history, and
        contradictory order/fill facts therefore fail closed before any local
        terminal promotion.
        """
        start = self._parse_timestamp(requested_start)
        end = self._parse_timestamp(requested_end)
        if start is None or end is None or end <= start:
            raise OMSExecutionError("round-trip broker evidence window is invalid")

        expected: dict[str, Mapping[str, object]] = {}
        for row in expected_rows:
            external_id = str(row.get("external_order_id") or "").strip()
            if not external_id or external_id in expected:
                raise OMSExecutionError("round-trip broker evidence has ambiguous durable order identities")
            leg = row.get("leg")
            attempt = row.get("attempt")
            if not isinstance(leg, Mapping) or not isinstance(attempt, Mapping):
                raise OMSExecutionError("round-trip broker evidence lacks durable leg/attempt context")
            expected[external_id] = row

        if not expected:
            raise OMSExecutionError("round-trip broker evidence has no expected orders")

        facts, positions, _open_orders = self._strict_authoritative_account_facts(
            account,
            max_age_seconds=self.TERMINAL_RECOVERY_WINDOW_SECONDS,
            require_no_open_orders=True,
        )
        if positions:
            raise OMSExecutionError("round-trip account is not flat")
        if facts.execution_evidence_mode is ExecutionEvidenceMode.UNAVAILABLE:
            raise OMSExecutionError("authoritative account execution evidence is unavailable")
        if not ({"CURRENT_DEALS", "CURRENT_ORDER_SNAPSHOTS"} & set(facts.execution_evidence_scope)):
            raise OMSExecutionError("authoritative account facts lack current execution evidence scope")

        def parse_decimal(value: object, label: str) -> Decimal:
            try:
                result = Decimal(str(value))
            except (InvalidOperation, TypeError, ValueError) as exc:
                raise OMSExecutionError(f"{label} is not numeric") from exc
            if not result.is_finite() or result <= 0:
                raise OMSExecutionError(f"{label} is invalid")
            return result

        def validate_fill_groups(
            items: Sequence[BrokerFill],
            *,
            label: str,
        ) -> dict[str, tuple[BrokerFill, ...]]:
            grouped: dict[str, list[BrokerFill]] = {}
            seen_identity: set[tuple[str, str]] = set()
            for item in items:
                if not isinstance(item, BrokerFill):
                    raise OMSExecutionError(f"{label} contains a non-normalized fill")
                external_id = str(item.external_order_id).strip()
                row = expected.get(external_id)
                if row is None:
                    raise OMSExecutionError(f"{label} contains unexpected fill {external_id}")
                identity = (external_id, str(item.external_fill_id or item.dedupe_key).strip())
                if not identity[1] or identity in seen_identity:
                    raise OMSExecutionError(f"{label} contains a duplicate fill identity")
                seen_identity.add(identity)
                if item.account_id not in (None, account.id):
                    raise OMSExecutionError(f"{label} contains a foreign account fill")
                aliases = self._account_alias_mismatches(
                    account,
                    account_id=item.account_id,
                    metadata=item.metadata,
                )
                if aliases:
                    raise OMSExecutionError(f"{label} contains foreign account aliases: {aliases}")
                if item.evidence_mode is ExecutionEvidenceMode.UNAVAILABLE or not item.evidence_reference:
                    raise OMSExecutionError(f"{label} contains fill without execution provenance")
                leg = row["leg"]
                attempt = row["attempt"]
                instrument_id = str(leg.get("instrument_id") or "")
                if item.instrument_id not in (None, instrument_id):
                    raise OMSExecutionError(f"{label} fill {external_id} instrument provenance mismatch")
                if item.quantity <= 0 or not item.quantity.is_finite():
                    raise OMSExecutionError(f"{label} fill {external_id} quantity is invalid")
                temporal_mismatches = self._temporal_fill_mismatches(attempt, item)
                if temporal_mismatches:
                    raise OMSExecutionError(
                        f"{label} fill {external_id} has invalid temporal provenance: {temporal_mismatches}"
                    )
                if item.filled_at < start - timedelta(seconds=self.BROKER_TIME_ORDER_TOLERANCE_SECONDS):
                    raise OMSExecutionError(f"{label} fill {external_id} predates the proof window")
                if item.filled_at > end + timedelta(seconds=self.BROKER_CLOCK_SKEW_TOLERANCE_SECONDS):
                    raise OMSExecutionError(f"{label} fill {external_id} is newer than the proof window")
                grouped.setdefault(external_id, []).append(item)

            if set(grouped) != set(expected):
                missing = sorted(set(expected) - set(grouped))
                extra = sorted(set(grouped) - set(expected))
                raise OMSExecutionError(
                    f"{label} does not exactly cover durable fills; missing={missing}, extra={extra}"
                )
            for external_id, row in expected.items():
                leg = row["leg"]
                expected_quantity = parse_decimal(row.get("quantity"), f"{label} durable quantity")
                expected_price = parse_decimal(row["fill"].get("price"), f"{label} durable price")
                total_quantity = sum((item.quantity for item in grouped[external_id]), Decimal("0"))
                total_notional = sum((item.quantity * item.price for item in grouped[external_id]), Decimal("0"))
                if total_quantity != expected_quantity or total_notional / total_quantity != expected_price:
                    raise OMSExecutionError(
                        f"{label} fill {external_id} quantity or economics conflict with durable evidence"
                    )
                instrument_id = str(leg.get("instrument_id") or "")
                if any(item.instrument_id not in (None, instrument_id) for item in grouped[external_id]):
                    raise OMSExecutionError(f"{label} fill {external_id} instrument mismatch")
            return {key: tuple(value) for key, value in grouped.items()}

        validate_fill_groups(facts.fills, label="current account facts")
        history = self._strict_historical_order_facts(
            account,
            requested_start=start,
            requested_end=end,
        )

        history_by_external: dict[str, BrokerOrderSnapshot] = {}
        for order in history.orders:
            external_id = str(order.external_order_id).strip()
            if external_id in history_by_external:
                raise OMSExecutionError("historical order facts contain duplicate order identities")
            aliases = self._account_alias_mismatches(
                account,
                account_id=order.account_id,
                external_account_id=order.external_account_id,
                metadata=order.metadata,
            )
            if aliases:
                raise OMSExecutionError(
                    f"historical order {external_id} has foreign account aliases: {aliases}"
                )
            mismatches = self._broker_order_quantity_mismatches(
                order.status,
                order.quantity,
                order.filled_quantity,
            )
            if mismatches:
                raise OMSExecutionError(
                    f"historical order {external_id} has contradictory status/quantity facts: {mismatches}"
                )
            history_by_external[external_id] = order

        if not set(expected).issubset(history_by_external):
            missing = sorted(set(expected) - set(history_by_external))
            raise OMSExecutionError(f"historical order facts do not contain every round-trip order: {missing}")
        for external_id, row in expected.items():
            order = history_by_external[external_id]
            leg = row["leg"]
            expected_quantity = parse_decimal(row.get("quantity"), "historical durable quantity")
            if (
                order.account_id != account.id
                or order.instrument_id != str(leg.get("instrument_id"))
                or order.side.value != str(leg.get("side"))
                or order.quantity != expected_quantity
                or order.filled_quantity != expected_quantity
                or order.status is not BrokerOrderStatus.FILLED
            ):
                raise OMSExecutionError(f"historical order {external_id} conflicts with durable fill")
        validate_fill_groups(history.fills, label="historical order facts")
        return facts, history

    def _strict_durable_managed_account_exposure(self, account: Account) -> dict[str, Decimal]:
        """Return the account-wide managed position implied by allocations.

        This is used only by proof-gated partial EXIT recovery.  Unlike a
        book-local net, it cannot silently treat another managed book as flat;
        unknown/unowned non-zero rows are rejected rather than netted away.
        """
        closed_historical = self._closed_historical_intent_ids(account)
        totals: dict[str, Decimal] = {}
        for row in self.repository.position_allocations(account.id):
            source_id = str(row.get("source_intent_id") or "").strip()
            if source_id in closed_historical:
                continue
            try:
                quantity = Decimal(str(row.get("signed_quantity", "0")))
            except (InvalidOperation, TypeError, ValueError) as exc:
                raise OMSExecutionError("durable account allocation quantity is invalid") from exc
            if not quantity.is_finite():
                raise OMSExecutionError("durable account allocation quantity is not finite")
            ownership = str(row.get("ownership_class") or "").upper()
            if quantity != 0 and ownership != OwnershipClass.MANAGED.value:
                raise OMSExecutionError("durable account exposure contains unknown or unowned allocation")
            if quantity == 0:
                continue
            instrument_id = str(row.get("instrument_id") or "").strip()
            if not instrument_id or not source_id or str(row.get("account_id")) != account.id:
                raise OMSExecutionError("durable account allocation lacks canonical ownership")
            totals[instrument_id] = totals.get(instrument_id, Decimal("0")) + quantity
        return {key: value for key, value in totals.items() if value != 0}

    def _account_wide_broker_fact_gate(self, intent_id: str, account: Account) -> bool:
        """Block a new submit when account-wide broker facts are unsafe.

        Stage 3 does not attribute or remediate unknown exposure.  It only
        permits submission when non-zero positions, open orders, and fills
        returned by the account-scoped adapter are either flat, claimed by a
        managed attempt, or exactly represented by durable fill evidence.
        """
        intent = self._required_intent(intent_id)
        try:
            # Account-wide facts have an explicit completeness, identity,
            # freshness, duplicate, and status/quantity contract.  Do not fall
            # back to individual readers or infer that missing methods/empty
            # collections mean a flat account.
            facts, positions, open_orders = self._strict_authoritative_account_facts(
                account,
                max_age_seconds=self.BROKER_FACT_MAX_AGE_SECONDS,
            )
            fills = facts.fills
            duplicate_position_blockers = []
            duplicate_order_blockers = []
        except Exception as exc:
            details = {
                "intent_id": intent_id,
                "account_id": account.id,
                "reason": "account-wide broker facts unavailable",
                "error": str(exc),
            }
            self._require_reconciliation(
                intent_id,
                account,
                category="BROKER_FACT_UNAVAILABLE",
                entity_type="ACCOUNT",
                entity_key=account.id,
                details=details,
            )
            self._record_recovery_action(
                intent=intent,
                account=account,
                action_key=f"BROKER_FACT_UNAVAILABLE:{account.id}:QUERY",
                state="RECONCILIATION_REQUIRED",
                summary="Account-wide broker facts are unavailable; no new submission is safe.",
                observed_positions={"_status": "UNAVAILABLE", "_error": str(exc)},
                remaining_quantities=self._remaining_quantities(intent),
                allowed_next_steps=("refresh_broker_facts", "reconcile_broker_position", "operator_review"),
                metadata=details,
            )
            return False

        blockers: list[dict[str, object]] = [
            *duplicate_order_blockers,
            *duplicate_position_blockers,
        ]
        if facts.error not in (None, ""):
            blockers.append(
                {
                    "kind": "broker_fact_error",
                    "error": str(facts.error),
                }
            )
        execution_evidence_ready, execution_evidence_reason = self._execution_evidence_readiness(
            account,
            facts,
        )
        if facts.execution_evidence_mode is ExecutionEvidenceMode.UNAVAILABLE:
            blockers.append(
                {
                    "kind": "execution_evidence_unavailable",
                    "reason": execution_evidence_reason,
                }
            )
        elif not execution_evidence_ready:
            blockers.append(
                {
                    "kind": "cumulative_execution_evidence_baseline_required",
                    "reason": execution_evidence_reason,
                }
            )
        allocations: dict[str, Decimal] = {}
        closed_historical_intents = self._closed_historical_intent_ids(account)
        try:
            for row in self.repository.position_allocations(account.id):
                if str(row.get("source_intent_id") or "") in closed_historical_intents:
                    continue
                quantity = Decimal(str(row.get("signed_quantity", "0")))
                if not quantity.is_finite():
                    raise ValueError("allocation quantity is not finite")
                instrument_id = str(row.get("instrument_id", ""))
                ownership = str(row.get("ownership_class", "")).strip().upper()
                if ownership == OwnershipClass.UNKNOWN.value:
                    # Unknown inventory is never owned net exposure.  It is a
                    # sticky safety fact that must be reconciled separately.
                    blockers.append(
                        {
                            "kind": "unknown_allocation",
                            "allocation_id": str(row.get("id", "")),
                            "instrument_id": instrument_id,
                            "signed_quantity": str(quantity),
                        }
                    )
                    continue
                if ownership != OwnershipClass.MANAGED.value:
                    if quantity != 0:
                        blockers.append(
                            {
                                "kind": "unowned_allocation",
                                "allocation_id": str(row.get("id", "")),
                                "instrument_id": instrument_id,
                                "ownership_class": ownership,
                                "signed_quantity": str(quantity),
                            }
                        )
                    continue
                allocations[instrument_id] = allocations.get(instrument_id, Decimal("0")) + quantity
        except (InvalidOperation, TypeError, ValueError, sqlite3.Error) as exc:
            blockers.append({"kind": "managed_allocation", "error": str(exc)})

        observed_by_instrument: dict[str, Decimal] = {}
        for position in positions:
            try:
                instrument_id = str(position.instrument_id)
                quantity = Decimal(str(position.signed_quantity))
                if not instrument_id or not quantity.is_finite():
                    raise ValueError("position instrument or quantity is invalid")
                position_account = getattr(position, "account_id", None)
                position_aliases = self._account_alias_mismatches(
                    account,
                    account_id=position_account,
                    metadata=getattr(position, "metadata", None),
                )
                if position_aliases:
                    blockers.append(
                        {
                            "kind": "position_account",
                            "instrument_id": instrument_id,
                            "mismatches": position_aliases,
                        }
                    )
                if position_account not in (None, account.id):
                    blockers.append(
                        {"kind": "position_account", "instrument_id": instrument_id, "account_id": position_account}
                    )
                observed_by_instrument[instrument_id] = observed_by_instrument.get(instrument_id, Decimal("0")) + quantity
                managed = allocations.get(instrument_id, Decimal("0"))
                if quantity != 0 and quantity != managed:
                    blockers.append(
                        {
                            "kind": "unknown_position",
                            "instrument_id": instrument_id,
                            "observed_quantity": str(quantity),
                            "managed_quantity": str(managed),
                        }
                    )
            except (AttributeError, InvalidOperation, TypeError, ValueError) as exc:
                blockers.append({"kind": "position", "error": str(exc)})

        # Reverse reconciliation is required even when the broker omitted a
        # locally managed instrument entirely.  A local allocation without an
        # exact signed broker position is not safe to treat as accounted for.
        for instrument_id, managed in allocations.items():
            if managed == 0:
                continue
            observed = observed_by_instrument.get(instrument_id, Decimal("0"))
            if observed != managed:
                blockers.append(
                    {
                        "kind": "managed_position_missing_or_mismatched",
                        "instrument_id": instrument_id,
                        "managed_quantity": str(managed),
                        "observed_quantity": str(observed),
                    }
                )

        for snapshot in open_orders:
            if snapshot.status is BrokerOrderStatus.UNKNOWN:
                blockers.append(
                    {
                        "kind": "unknown_broker_order_status",
                        "external_order_id": snapshot.external_order_id,
                    }
                )
                continue
            consistency = self._broker_order_quantity_mismatches(
                snapshot.status,
                snapshot.quantity,
                snapshot.filled_quantity,
            )
            if consistency:
                blockers.append(
                    {
                        "kind": "broker_order_status_quantity_conflict",
                        "external_order_id": snapshot.external_order_id,
                        "mismatches": consistency,
                    }
                )
            aliases = self._account_alias_mismatches(
                account,
                account_id=snapshot.account_id,
                external_account_id=snapshot.external_account_id,
                metadata=snapshot.metadata,
            )
            claims = self.repository.broker_orders_for_external_order_id(snapshot.external_order_id)
            if aliases or not claims or len(claims) != 1:
                blockers.append(
                    {
                        "kind": "unattributed_open_order",
                        "external_order_id": snapshot.external_order_id,
                        "mismatches": aliases,
                        "claim_count": len(claims),
                    }
                )
                continue
            if any(str(item.get("account_id")) != account.id for item in claims):
                blockers.append(
                    {"kind": "cross_account_open_order", "external_order_id": snapshot.external_order_id}
                )
                continue
            claim = claims[0]
            claim_intent_id = self.repository.intent_id_for_broker_order(str(claim.get("id")))
            claim_intent = self._required_intent(claim_intent_id) if claim_intent_id else None
            if claim_intent is not None and not self._book_claim_is_known(claim_intent, account):
                blockers.append(
                    {
                        "kind": "unknown_book_open_order",
                        "external_order_id": snapshot.external_order_id,
                        "intent_id": str(claim_intent.get("id")),
                        "book_id": claim_intent.get("book_id"),
                    }
                )
            claim_leg = (
                next((item for item in claim_intent["legs"] if item["id"] == claim["order_leg_id"]), None)
                if claim_intent is not None
                else None
            )
            if claim_leg is None:
                blockers.append({"kind": "open_order_claim_missing_leg", "external_order_id": snapshot.external_order_id})
                continue
            validation = self._validate_poll_snapshot(
                account=account,
                attempt=claim,
                leg=claim_leg,
                snapshot=snapshot,
                require_working_zero_fill_evidence=True,
            )
            if not validation["valid"]:
                blockers.append(
                    {
                        "kind": "open_order_identity_mismatch",
                        "external_order_id": snapshot.external_order_id,
                        "mismatches": validation["mismatches"],
                    }
                )

        recognized_closed_historical_fills: list[str] = []
        for broker_fill in fills:
            # Current account-fact queries can legitimately repeat fills from
            # retired/compensated rounds.  Keep those durable facts visible,
            # but do not classify them as new unmatched exposure when the
            # exact provider order/fill is covered by the same proof-backed
            # closed-history predicate used for capacity attribution.
            if self.is_proof_backed_closed_broker_fill(
                account=account,
                broker_fill=broker_fill,
            ):
                recognized_closed_historical_fills.append(broker_fill.external_order_id)
                continue
            aliases = self._account_alias_mismatches(
                account,
                account_id=broker_fill.account_id,
                metadata=broker_fill.metadata,
            )
            claims = self.repository.broker_orders_for_external_order_id(broker_fill.external_order_id)
            if aliases or not claims or len(claims) != 1 or any(
                str(item.get("account_id")) != account.id for item in claims
            ):
                blockers.append(
                    {
                        "kind": "unattributed_broker_fill",
                        "external_order_id": broker_fill.external_order_id,
                        "dedupe_key": broker_fill.dedupe_key,
                        "mismatches": aliases,
                        "claim_count": len(claims),
                    }
                )
                continue
            claim = claims[0]
            claim_intent_id = self.repository.intent_id_for_broker_order(str(claim.get("id")))
            if claim_intent_id is None and self.repository.book_mode_active(account.id):
                blockers.append(
                    {
                        "kind": "unknown_book_broker_fill",
                        "external_order_id": broker_fill.external_order_id,
                        "dedupe_key": broker_fill.dedupe_key,
                        "reason": "broker fill claim has no owning intent",
                    }
                )
            if claim_intent_id is not None:
                claim_intent = self._required_intent(claim_intent_id)
                if not self._book_claim_is_known(claim_intent, account):
                    blockers.append(
                        {
                            "kind": "unknown_book_broker_fill",
                            "external_order_id": broker_fill.external_order_id,
                            "dedupe_key": broker_fill.dedupe_key,
                            "intent_id": str(claim_intent.get("id")),
                            "book_id": claim_intent.get("book_id"),
                        }
                    )
                claim_leg = next(
                    (
                        item
                        for item in claim_intent["legs"]
                        if str(item["id"]) == str(claim.get("order_leg_id"))
                    ),
                    None,
                )
                if claim_leg is not None:
                    try:
                        self._validate_broker_fill_provenance(
                            claim,
                            broker_fill,
                            expected_instrument_id=str(claim_leg["instrument_id"]),
                        )
                    except (KeyError, TypeError, ValueError) as exc:
                        blockers.append(
                            {
                                "kind": "broker_fill_instrument_provenance",
                                "external_order_id": broker_fill.external_order_id,
                                "dedupe_key": broker_fill.dedupe_key,
                                "error": str(exc),
                            }
                        )
                        continue
            exact = False
            for claim in claims:
                for stored in self.repository.fills_for_broker_order(str(claim.get("id"))):
                    try:
                        stored_metadata = stored.get("metadata") or {}
                        stored_external_order_value = (
                            stored_metadata.get("_external_order_id")
                            if isinstance(stored_metadata, Mapping)
                            else None
                        )
                        stored_external_order_id = str(
                            stored_external_order_value or claim.get("external_order_id") or ""
                        )
                        stored_evidence_reference = (
                            stored_metadata.get("_evidence_reference")
                            if isinstance(stored_metadata, Mapping)
                            else None
                        )
                        stored_fee = stored.get("fee")
                        stored_fee_value = Decimal(str(stored_fee)) if stored_fee is not None else None
                        broker_fee_value = broker_fill.fee
                        if isinstance(stored_metadata, Mapping):
                            stored_metadata = {
                                key: value
                                for key, value in stored_metadata.items()
                                if key not in {
                                    "_external_order_id",
                                    "_evidence_reference",
                                    self._BROKER_FILL_ACCOUNT_ID_KEY,
                                        # The repository adds the canonical
                                        # internal instrument after the
                                        # adapter has produced its provider
                                        # evidence.  Instrument provenance is
                                        # validated above; this internal
                                        # bookkeeping key must not make an
                                        # otherwise identical fresh fill look
                                        # non-durable.
                                        "_instrument_id",
                                }
                            }
                        exact = (
                            str(stored.get("dedupe_key")) == broker_fill.dedupe_key
                            and str(stored.get("external_fill_id") or "")
                            == str(broker_fill.external_fill_id or "")
                            and Decimal(str(stored.get("quantity"))) == broker_fill.quantity
                            and Decimal(str(stored.get("price"))) == broker_fill.price
                            and stored_fee_value == broker_fee_value
                            and stored.get("fee_currency") == broker_fill.fee_currency
                            and str(stored.get("filled_at")) == broker_fill.filled_at.isoformat()
                            and stored_external_order_id == broker_fill.external_order_id
                            and stored_evidence_reference in {
                                None,
                                broker_fill.evidence_reference,
                                broker_fill.dedupe_key,
                                f"{broker_fill.external_order_id}:{broker_fill.dedupe_key}",
                            }
                            and dict(stored_metadata) == dict(broker_fill.metadata)
                        )
                    except (InvalidOperation, TypeError, ValueError):
                        exact = False
                    if exact:
                        break
                if exact:
                    break
            if not exact:
                blockers.append(
                    {
                        "kind": "broker_fill_not_durable",
                        "external_order_id": broker_fill.external_order_id,
                        "dedupe_key": broker_fill.dedupe_key,
                    }
                )

        if not blockers:
            return True
        details = {
            "intent_id": intent_id,
            "account_id": account.id,
            "blockers": blockers,
            "recognized_closed_historical_fill_order_ids": recognized_closed_historical_fills,
        }
        fingerprint = hashlib.sha256(
            json.dumps(details, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        self._require_reconciliation(
            intent_id,
            account,
            category="BROKER_FACT_GATE_BLOCKED",
            entity_type="ACCOUNT",
            entity_key=account.id,
            details=details,
        )
        self._record_recovery_action(
            intent=intent,
            account=account,
            action_key=f"BROKER_FACT_GATE:{account.id}:{fingerprint}",
            state="RECONCILIATION_REQUIRED",
            summary="Account-wide broker facts include unknown or unattributed exposure; no new submission is safe.",
            observed_positions={"_status": "BROKER_FACT_GATE_BLOCKED", "blockers": blockers},
            remaining_quantities=self._remaining_quantities(intent),
            allowed_next_steps=("refresh_broker_facts", "reconcile_broker_position", "reconcile_broker_order", "operator_review"),
            metadata=details,
        )
        return False

    def resolve_terminal_history_reconciliation(
        self,
        intent_id: str,
        *,
        account: Account,
        broker_order_ids: Sequence[str],
        verified: bool = False,
    ) -> dict:
        """Resolve an aged-history blocker only after explicit operator verification."""
        if not verified:
            raise ValueError("explicit terminal-history verification is required")
        intent = self._required_intent(intent_id)
        canonical = self._canonical_account_for_intent(
            intent,
            supplied=account,
            source="resolve_terminal_history",
        )
        if canonical is None:
            raise OMSExecutionError("cannot resolve terminal history without an enabled canonical account")
        account = canonical
        self._ensure_terminal_history_safety(account)
        self._quarantine_multiple_attempts(intent_id, account)
        expected = {
            str(row["broker_order_id"])
            for row in self.repository.expired_terminal_attempts(
                account.id,
                now=self._now(),
                terminal_window_seconds=self.TERMINAL_RECOVERY_WINDOW_SECONDS,
            )
            if str(row["intent_id"]) == intent_id
        }
        supplied = {str(value) for value in broker_order_ids}
        if not expected or supplied != expected:
            raise ValueError("verified broker order IDs do not exactly match the aged terminal attempts")
        for broker_order_id in expected:
            self._resolve_reconciliation_issue(
                account.id,
                f"TERMINAL_HISTORY_EXPIRED:BROKER_ORDER:{broker_order_id}",
                resolved_at=self._now(),
            )
            self._resolve_recovery_action(
                account.id,
                intent_id,
                f"TERMINAL_HISTORY_EXPIRED:{broker_order_id}",
                resolved_at=self._now(),
            )
        return self.recovery_status(intent_id, account=account)

    def resolve_verified_retired_baseline(self, *, account: Account) -> dict[str, object]:
        """Resolve only blockers proven to belong to a verified retired book.

        This is an explicit local reconciliation operation.  It never edits
        historical orders, fills, or allocations, and it refuses to resolve
        any issue/action that is not attributable to the exact baseline
        claims.  A later pilot run must use a new cycle if a previous intent
        was already persisted without any broker attempt.
        """
        account = self._canonical_account_for_boundary(account, source="resolve_verified_retired_baseline")
        baseline = self.repository.latest_execution_evidence_baseline(account.id)
        if baseline is None or not isinstance(baseline.metadata, Mapping):
            raise OMSExecutionError("verified retired baseline is unavailable")
        book_id = str(baseline.metadata.get("legacy_book_id", "")).strip()
        if not book_id:
            raise OMSExecutionError("verified retired baseline has no retired book identity")
        verified, reason = self.repository.verified_retired_book_closure(
            account.id,
            book_id,
            baseline,
            allow_retired_baseline_blockers=True,
        )
        if not verified:
            raise OMSExecutionError(f"retired baseline proof failed: {reason}")

        # Terminal-history quarantine changes only the local intent status;
        # it does not erase the imported, attempt-scoped fill evidence.  Keep
        # a proof-driven list before resolving any blocker so a later status
        # transition can never broaden to the current pilot or another book.
        retired_intents_to_complete = self._verified_retired_intents_to_complete(
            account=account,
            book_id=book_id,
            source_ledger_fingerprint=str(baseline.source_ledger_fingerprint),
        )

        allocation_ids = {
            str(row["id"])
            for row in self.repository.book_position_allocations(account.id, book_id=book_id)
        }
        order_rows = self.repository.book_broker_orders(account.id, book_id=book_id)
        order_ids = {str(row["id"]) for row in order_rows}
        intent_ids = {
            str(row["id"])
            for row in self.repository.book_intents(account.id, book_id=book_id)
        }

        def derived_issue_base(issue: Mapping[str, object]) -> bool:
            category = str(issue.get("category", ""))
            entity_key = str(issue.get("entity_key", ""))
            details = self._issue_details(issue)
            if category == "TERMINAL_HISTORY_EXPIRED":
                return entity_key in order_ids
            if category == "BOOK_OWNERSHIP_OR_CAPACITY":
                blockers = details.get("blockers")
                return bool(blockers) and all(
                    isinstance(item, Mapping)
                    and item.get("kind") == "unknown_book_allocation"
                    and str(item.get("book_id", "")) == book_id
                    and str(item.get("allocation_id", "")) in allocation_ids
                    for item in blockers
                )
            return False

        def derived_action_base(action: Mapping[str, object]) -> bool:
            key = str(action.get("action_key", ""))
            if key.startswith("TERMINAL_HISTORY_EXPIRED:"):
                return key.split(":", 1)[1] in order_ids and str(action.get("intent_id", "")) in intent_ids
            if key.startswith("BOOK_RISK_BLOCK:"):
                metadata = action.get("metadata")
                blockers = metadata.get("blockers") if isinstance(metadata, Mapping) else None
                return bool(blockers) and all(
                    isinstance(item, Mapping)
                    and item.get("kind") == "unknown_book_allocation"
                    and str(item.get("book_id", "")) == book_id
                    and str(item.get("allocation_id", "")) in allocation_ids
                    for item in blockers
                )
            return False

        issues = self.repository.open_reconciliation_issues(account.id)
        actions = self.repository.open_recovery_actions(account.id)
        direct_issue_links = {
            (str(issue.get("id", "")), str(issue.get("issue_key", "")))
            for issue in issues
            if derived_issue_base(issue)
        }
        direct_action_links = {
            (
                str(action.get("id", "")),
                str(action.get("intent_id", "")),
                str(action.get("action_key", "")),
            )
            for action in actions
            if derived_action_base(action)
        }

        def linked_account_wrapper(row: Mapping[str, object], details: object) -> bool:
            # Validate the wrapper's own durable identity before trusting any
            # copied causal metadata.  Causal links alone cannot turn a
            # malformed account issue/action into a covered retired blocker.
            if str(row.get("category", "")) == "ACCOUNT_RECONCILIATION_BLOCK":
                if (
                    str(row.get("entity_type", "")) != "ACCOUNT"
                    or str(row.get("entity_key", "")) != account.id
                    or str(row.get("issue_key", ""))
                    != f"ACCOUNT_RECONCILIATION_BLOCK:ACCOUNT:{account.id}"
                ):
                    return False
            else:
                if str(row.get("action_key", "")) != f"ACCOUNT_RECONCILIATION_BLOCK:ACCOUNT:{account.id}":
                    return False
            if not isinstance(details, Mapping):
                return False
            if str(details.get("causal_account_id", "")) != account.id:
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
            if str(row.get("account_id", "")) != account.id:
                return False
            if "action_key" in row:
                wrapper_intent_id = str(row.get("intent_id", ""))
                if (
                    not wrapper_intent_id
                    or wrapper_intent_id not in causal_intent_ids
                    or wrapper_intent_id not in intent_ids
                ):
                    return False
            normalized_issues = {
                (str(link.get("id", "")), str(link.get("issue_key", "")))
                for link in issue_links
                if isinstance(link, Mapping)
            }
            normalized_actions = {
                (
                    str(link.get("id", "")),
                    str(link.get("intent_id", "")),
                    str(link.get("action_key", "")),
                )
                for link in action_links
                if isinstance(link, Mapping)
            }
            if len(normalized_issues) != len(issue_links) or len(normalized_actions) != len(action_links):
                return False
            if not normalized_issues.issubset(direct_issue_links):
                return False
            if not normalized_actions.issubset(direct_action_links):
                return False
            if not all(intent_id in intent_ids for _, intent_id, _ in normalized_actions):
                return False
            if not all(intent_id in causal_intent_ids for _, intent_id, _ in normalized_actions):
                return False
            return bool(normalized_issues and normalized_actions)

        def derived_issue(issue: Mapping[str, object]) -> bool:
            if derived_issue_base(issue):
                return True
            if str(issue.get("category", "")) == "ACCOUNT_RECONCILIATION_BLOCK":
                return linked_account_wrapper(issue, self._issue_details(issue))
            return False

        def derived_action(action: Mapping[str, object]) -> bool:
            if derived_action_base(action):
                return True
            if str(action.get("action_key", "")).startswith("ACCOUNT_RECONCILIATION_BLOCK:"):
                return linked_account_wrapper(action, action.get("metadata"))
            return False

        if not all(derived_issue(issue) for issue in issues):
            raise OMSExecutionError("unrelated open reconciliation issue prevents retired-baseline resolution")
        if not all(derived_action(action) for action in actions):
            raise OMSExecutionError("unrelated open recovery action prevents retired-baseline resolution")

        resolved_issue_keys: list[str] = []
        for issue in issues:
            if derived_issue(issue):
                if self._resolve_reconciliation_issue(account.id, str(issue["issue_key"]), resolved_at=self._now()):
                    resolved_issue_keys.append(str(issue["issue_key"]))
        resolved_action_keys: list[str] = []
        for action in actions:
            if derived_action(action):
                if self._resolve_recovery_action(
                    account.id,
                    str(action["intent_id"]),
                    str(action["action_key"]),
                    resolved_at=self._now(),
                ):
                    resolved_action_keys.append(str(action["action_key"]))

        restored_intent_ids: list[str] = []
        for intent_id in retired_intents_to_complete:
            current = self.repository.get_intent(intent_id)
            if current is None:
                raise OMSExecutionError("verified retired intent disappeared during reconciliation")
            current_status = str(current.get("status", ""))
            if current_status == IntentStatus.RECONCILIATION_REQUIRED.value:
                # The proof above requires legacy-import/retired provenance,
                # one complete FILLED attempt per leg, and exact baseline
                # coverage.  COMPLETED is the safe terminal normalization
                # after an explicit operator reconciliation; no new order is
                # submitted and no current pilot intent is touched.
                self.repository.transition_intent(
                    intent_id,
                    IntentStatus.COMPLETED,
                    now=self._now(),
                )
                restored_intent_ids.append(intent_id)
            elif current_status in {
                IntentStatus.FILLED.value,
                IntentStatus.COMPLETED.value,
            }:
                continue
            else:
                raise OMSExecutionError(
                    f"verified retired intent {intent_id} changed to an unsafe status {current_status!r}"
                )

        event_id = f"retired-baseline-reconciliation:{baseline.id}"
        if not any(event.get("id") == event_id for event in self.repository.operational_events(account.id, limit=1000)):
            self.repository.record_operational_event(
                event_id=event_id,
                account_id=account.id,
                event_type="RETIRED_BASELINE_RECONCILIATION",
                mode=account.environment.value,
                outcome="RESOLVED",
                occurred_at=self._now(),
                summary="Resolved only aged-history and derivative ownership blockers covered by a verified flat retired baseline.",
                details={
                    "baseline_id": baseline.id,
                    "source_ledger_fingerprint": baseline.source_ledger_fingerprint,
                    "legacy_book_id": book_id,
                    "broker_order_ids": sorted(order_ids),
                    "resolved_issue_keys": resolved_issue_keys,
                    "resolved_action_keys": resolved_action_keys,
                    "restored_intent_ids": restored_intent_ids,
                },
            )
        return {
            "account_id": account.id,
            "baseline_id": baseline.id,
            "legacy_book_id": book_id,
            "broker_order_ids": tuple(sorted(order_ids)),
            "resolved_issue_keys": tuple(resolved_issue_keys),
            "resolved_action_keys": tuple(resolved_action_keys),
            "restored_intent_ids": tuple(restored_intent_ids),
            "retry_requires_new_cycle": True,
        }

    def resolve_unsubmitted_intent(
        self,
        intent_id: str,
        *,
        account: Account,
        expected_book_id: str | None = None,
    ) -> dict[str, object]:
        """Cancel one quarantined intent proven never to have been submitted.

        This is deliberately narrower than generic reconciliation resolution:
        the intent must have no broker attempts, fills, allocations, or open
        account blockers, and a fresh complete account-facts snapshot must be
        flat with no open orders or fills.  No other intent or blocker is
        altered, and a proof failure remains fail-closed.
        """
        self._startup_safety_audit()
        account = self._canonical_account_for_boundary(account, source="resolve_unsubmitted_intent")
        self._ensure_terminal_history_safety(account)
        self._quarantine_multiple_attempts_for_account(account)
        intent = self._required_intent(intent_id)
        if str(intent.get("account_id", "")) != account.id:
            raise OMSExecutionError("unsubmitted-intent proof account does not match persisted intent")
        if expected_book_id is not None and str(intent.get("book_id") or "") != str(expected_book_id):
            raise OMSExecutionError("unsubmitted-intent proof book does not match persisted intent")
        if str(intent.get("status", "")) != IntentStatus.RECONCILIATION_REQUIRED.value:
            raise OMSExecutionError("only a reconciliation-required intent can be recovered as never submitted")
        if not intent.get("legs"):
            raise OMSExecutionError("never-submitted intent has no persisted legs")

        for leg in intent["legs"]:
            leg_id = str(leg["id"])
            if str(leg.get("status", "")) not in {
                LegStatus.PLANNED.value,
                LegStatus.RECONCILIATION_REQUIRED.value,
            }:
                raise OMSExecutionError(f"never-submitted leg {leg_id} has already advanced")
            try:
                cumulative = Decimal(str(leg.get("cumulative_filled_quantity", "0")))
            except (InvalidOperation, TypeError, ValueError) as exc:
                raise OMSExecutionError(f"never-submitted leg {leg_id} has invalid cumulative fill") from exc
            if not cumulative.is_finite() or cumulative != 0:
                raise OMSExecutionError(f"never-submitted leg {leg_id} has non-zero cumulative fill")
            if self.repository.broker_orders_for_leg(leg_id):
                raise OMSExecutionError(f"never-submitted leg {leg_id} has a broker attempt")
            if self.repository.fills_for_leg(leg_id):
                raise OMSExecutionError(f"never-submitted leg {leg_id} has durable fill evidence")

        if any(
            str(row.get("source_intent_id") or "") == intent_id
            for row in self.repository.position_allocations(account.id)
        ):
            raise OMSExecutionError("never-submitted intent already owns a position allocation")
        if any(
            str(row.get("intent_id") or "") == intent_id
            for row in self.repository.book_broker_orders(account.id)
        ):
            raise OMSExecutionError("never-submitted intent already owns a broker-order claim")
        open_issues = self.repository.open_reconciliation_issues(account.id)
        open_actions = self.repository.open_recovery_actions(account.id)
        retired_book_ids = self._verified_retired_baseline_books(account)
        retired_allocation_ids: set[str] = set()
        if retired_book_ids:
            for retired_book_id in retired_book_ids:
                retired_allocation_ids.update(
                    str(row.get("id"))
                    for row in self.repository.book_position_allocations(
                        account.id,
                        book_id=retired_book_id,
                    )
                )

        def retired_capacity_issue(issue: Mapping[str, object]) -> bool:
            if str(issue.get("category", "")) != "BOOK_OWNERSHIP_OR_CAPACITY":
                return False
            details = self._issue_details(issue)
            blockers = details.get("blockers") if isinstance(details, Mapping) else None
            return bool(blockers) and all(
                isinstance(item, Mapping)
                and item.get("kind") == "unknown_book_allocation"
                and str(item.get("book_id", "")) in retired_book_ids
                and str(item.get("allocation_id", "")) in retired_allocation_ids
                for item in blockers
            )

        def retired_capacity_action(action: Mapping[str, object]) -> bool:
            if not str(action.get("action_key", "")).startswith("BOOK_RISK_BLOCK:"):
                return False
            metadata = action.get("metadata")
            blockers = metadata.get("blockers") if isinstance(metadata, Mapping) else None
            return bool(blockers) and all(
                isinstance(item, Mapping)
                and item.get("kind") == "unknown_book_allocation"
                and str(item.get("book_id", "")) in retired_book_ids
                and str(item.get("allocation_id", "")) in retired_allocation_ids
                for item in blockers
            )

        verified_roundtrip_external_ids: set[str] = set()
        for historical_row in self.repository.book_intents(account.id):
            historical_intent = self.repository.get_intent(str(historical_row.get("id", "")))
            if historical_intent is None:
                continue
            historical_metadata = historical_intent.get("metadata")
            roundtrip = (
                historical_metadata.get("verified_roundtrip_closure")
                if isinstance(historical_metadata, Mapping)
                else None
            )
            if not isinstance(roundtrip, Mapping) or str(roundtrip.get("account_id", account.id)) != account.id:
                continue
            for field in ("entry_external_order_ids", "exit_external_order_ids"):
                verified_roundtrip_external_ids.update(
                    str(value).strip()
                    for value in (roundtrip.get(field) or ())
                    if str(value).strip()
                )

        def baseline_wrapper(issue: Mapping[str, object]) -> bool:
            if (
                str(issue.get("category", "")) != "ACCOUNT_RECONCILIATION_BLOCK"
                or str(issue.get("entity_type", "")) != "ACCOUNT"
                or str(issue.get("entity_key", "")) != account.id
                or str(issue.get("issue_key", "")) != f"ACCOUNT_RECONCILIATION_BLOCK:ACCOUNT:{account.id}"
            ):
                return False
            details = self._issue_details(issue)
            baseline = self.repository.latest_execution_evidence_baseline(account.id)
            causal_intents = details.get("causal_intent_ids") if isinstance(details, Mapping) else None
            if (
                baseline is None
                or not isinstance(details, Mapping)
                or str(details.get("causal_account_id", "")) != account.id
                or str(details.get("causal_baseline_id", "")) != str(baseline.id)
                or str(details.get("causal_source_ledger_fingerprint", ""))
                != str(baseline.source_ledger_fingerprint)
                or not isinstance(causal_intents, list)
                or intent_id not in {str(value) for value in causal_intents}
            ):
                return False
            for causal_id in causal_intents:
                causal = self.repository.get_intent(str(causal_id))
                if causal is None or str(causal.get("account_id")) != account.id:
                    return False
                if any(
                    self.repository.broker_orders_for_leg(str(leg["id"]))
                    or self.repository.fills_for_leg(str(leg["id"]))
                    for leg in causal.get("legs", ())
                ):
                    return False
            return True

        def stale_roundtrip_fact_action(action: Mapping[str, object]) -> bool:
            if not str(action.get("action_key", "")).startswith("BROKER_FACT_GATE:"):
                return False
            if str(action.get("intent_id", "")) != intent_id:
                return False
            metadata = action.get("metadata")
            blockers = metadata.get("blockers") if isinstance(metadata, Mapping) else None
            external_ids = {
                str(item.get("external_order_id", "")).strip()
                for item in (blockers or ())
                if isinstance(item, Mapping)
            }
            return bool(external_ids) and external_ids.issubset(verified_roundtrip_external_ids)

        removable_issue_keys = {
            str(issue.get("issue_key"))
            for issue in open_issues
            if retired_capacity_issue(issue) or baseline_wrapper(issue)
        }
        removable_action_keys = {
            str(action.get("action_key"))
            for action in open_actions
            if retired_capacity_action(action) or stale_roundtrip_fact_action(action)
        }
        if any(str(issue.get("issue_key")) not in removable_issue_keys for issue in open_issues):
            raise OMSExecutionError(
                "pending account reconciliation or recovery blockers prevent never-submitted recovery"
            )
        if any(str(action.get("action_key")) not in removable_action_keys for action in open_actions):
            raise OMSExecutionError(
                "pending account reconciliation or recovery blockers prevent never-submitted recovery"
            )

        facts, normalized_positions, normalized_open_orders = self._strict_authoritative_account_facts(
            account,
            max_age_seconds=self.BROKER_FACT_MAX_AGE_SECONDS,
            require_no_open_orders=True,
        )
        if facts.execution_evidence_mode is ExecutionEvidenceMode.UNAVAILABLE:
            raise OMSExecutionError("execution evidence is unavailable for never-submitted recovery")
        if (
            facts.execution_evidence_mode is ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS
            and self.repository.latest_execution_evidence_baseline(account.id) is None
        ):
            raise OMSExecutionError("verified cumulative-order baseline is required for never-submitted recovery")
        nonzero_positions = [
            position
            for position in normalized_positions
            if position.signed_quantity != 0
        ]
        if nonzero_positions:
            raise OMSExecutionError("broker account has non-zero position exposure")

        # A cumulative-order provider's verified baseline is not, by itself,
        # proof that this intent was never submitted after the baseline was
        # captured.  Require a fresh, complete historical-order window from
        # the persisted intent creation through this proof operation.  This
        # remains fail-closed when the optional history capability is absent,
        # unsupported, malformed, or does not cover the requested interval.
        try:
            window_start = datetime.fromisoformat(str(intent["created_at"]).replace("Z", "+00:00"))
        except (KeyError, TypeError, ValueError) as exc:
            raise OMSExecutionError("never-submitted intent has an invalid creation timestamp") from exc
        if window_start.tzinfo is None or window_start.utcoffset() is None:
            window_start = window_start.replace(tzinfo=timezone.utc)
        window_start = window_start.astimezone(timezone.utc)
        window_end = self._now()
        if window_end <= window_start:
            window_end = window_start + timedelta(microseconds=1)
        historical = self._strict_historical_order_facts(
            account,
            requested_start=window_start,
            requested_end=window_end,
        )

        # A never-submitted intent has no durable broker/client identifier to
        # match.  Reject only history rows whose provider evidence explicitly
        # claims this intent, its leg, or its idempotency key; unrelated
        # account history remains valid evidence of a complete query window.
        markers = {
            intent_id,
            str(intent.get("idempotency_key") or ""),
            *(str(leg["id"]) for leg in intent["legs"]),
        }
        markers.discard("")

        def contains_marker(value: object) -> bool:
            if isinstance(value, Mapping):
                return any(contains_marker(key) or contains_marker(item) for key, item in value.items())
            if isinstance(value, (list, tuple, set, frozenset)):
                return any(contains_marker(item) for item in value)
            if isinstance(value, str):
                return any(
                    value == marker
                    or value.startswith(f"{marker}:")
                    or value.startswith(f"{marker}|")
                    for marker in markers
                )
            return False

        for order in historical.orders:
            if order.client_order_id in markers or contains_marker(order.metadata):
                raise OMSExecutionError(
                    "historical order facts contain an explicit claim for this intent"
                )
        for fill in historical.fills:
            if contains_marker(fill.metadata):
                raise OMSExecutionError(
                    "historical fill facts contain an explicit claim for this intent"
                )
        if facts.fills:
            # Current cumulative snapshots may include already-attributed
            # fills from other managed intents.  They do not invalidate a
            # never-submitted proof; any same-account fill that cannot be
            # matched to a durable broker-order claim remains a blocker.
            claimed_orders: dict[str, list[Mapping[str, object]]] = {}
            for order in self.repository.book_broker_orders(account.id):
                external_id = str(order.get("external_order_id") or "").strip()
                if external_id:
                    claimed_orders.setdefault(external_id, []).append(order)
            for fill in facts.fills:
                candidates = claimed_orders.get(str(fill.external_order_id), ())
                if not candidates or not any(
                    self.repository.fills_for_broker_order(str(order["id"]))
                    for order in candidates
                ):
                    raise OMSExecutionError(
                        "broker facts contain execution evidence without an exact durable claim"
                    )

        proof_time = self._now()
        for issue_key in sorted(removable_issue_keys):
            self._resolve_reconciliation_issue(account.id, issue_key, resolved_at=proof_time)
        for action in open_actions:
            if str(action.get("action_key")) in removable_action_keys:
                self._resolve_recovery_action(
                    account.id,
                    str(action.get("intent_id")),
                    str(action.get("action_key")),
                    resolved_at=proof_time,
                )
        changed = self.repository.cancel_unsubmitted_intent(intent_id, now=proof_time)
        self.repository.record_operational_event(
            event_id=f"unsubmitted-intent-recovery:{intent_id}",
            account_id=account.id,
            event_type="UNSUBMITTED_INTENT_RECOVERY",
            mode=account.environment.value,
            outcome="CANCELLED",
            occurred_at=proof_time,
            summary="Cancelled a reconciliation-quarantined intent after proving that no broker submission or exposure existed.",
            details={
                "intent_id": intent_id,
                "book_id": intent.get("book_id"),
                "leg_ids": [str(leg["id"]) for leg in intent["legs"]],
                "proof": {
                    "attempt_count": 0,
                    "fill_count": 0,
                    "allocation_count": 0,
                    "account_open_issue_count": 0,
                    "account_open_action_count": 0,
                    "broker_captured_at": facts.captured_at.isoformat(),
                    "broker_position_count": len(facts.positions),
                    "broker_open_order_count": len(facts.open_orders),
                    "broker_fill_count": len(facts.fills),
                    "execution_evidence_mode": facts.execution_evidence_mode.value,
                    "historical_window": {
                        "requested_start": historical.requested_start.isoformat(),
                        "requested_end": historical.requested_end.isoformat(),
                        "captured_at": historical.captured_at.isoformat(),
                        "order_count": len(historical.orders),
                        "fill_count": len(historical.fills),
                        "evidence_mode": historical.execution_evidence_mode.value,
                    },
                    "changed": changed,
                },
            },
        )
        return {
            "intent_id": intent_id,
            "account_id": account.id,
            "status": IntentStatus.CANCELLED.value,
            "changed": changed,
            "proof": "fresh_flat_account_and_no_local_submission_evidence",
        }

    def cancel_unsubmitted_legs_after_flattening(
        self,
        intent_id: str,
        *,
        account: Account,
        expected_book_id: str | None = None,
    ) -> dict[str, object]:
        """Quarantine only never-submitted legs after a verified flat flattening.

        This is intentionally not completion: filled legs and their durable
        claims remain unchanged, while only planned/quarantined legs with no
        broker attempt or fill are cancelled.  The intent itself stays
        ``RECONCILIATION_REQUIRED`` so the aborted multi-leg lifecycle remains
        auditable and cannot be mistaken for a completed entry.
        """
        self._startup_safety_audit()
        account = self._canonical_account_for_boundary(account, source="cancel_unsubmitted_legs_after_flattening")
        self._ensure_terminal_history_safety(account)
        self._quarantine_multiple_attempts_for_account(account)
        intent = self._required_intent(intent_id)
        if str(intent.get("account_id", "")) != account.id:
            raise OMSExecutionError("aborted-intent proof account does not match persisted intent")
        if expected_book_id is not None and str(intent.get("book_id") or "") != str(expected_book_id):
            raise OMSExecutionError("aborted-intent proof book does not match persisted intent")
        if str(intent.get("status", "")) != IntentStatus.RECONCILIATION_REQUIRED.value:
            raise OMSExecutionError("only a reconciliation-required intent can be safely aborted")
        if self.repository.open_reconciliation_issues(account.id) or self.repository.open_recovery_actions(account.id):
            raise OMSExecutionError("pending reconciliation or recovery blockers prevent aborted-intent cleanup")

        facts, normalized_positions, normalized_open_orders = self._strict_authoritative_account_facts(
            account,
            max_age_seconds=self.BROKER_FACT_MAX_AGE_SECONDS,
            require_no_open_orders=True,
        )
        if any(position.signed_quantity != 0 for position in normalized_positions):
            raise OMSExecutionError("broker account is not flat for aborted-intent cleanup")
        if facts.execution_evidence_mode is ExecutionEvidenceMode.UNAVAILABLE:
            raise OMSExecutionError("execution evidence is unavailable for aborted-intent cleanup")

        # Every submitted attempt must already be terminal and fully
        # accounted; only the exact planned/reconciled legs may be cancelled.
        cancelable: list[str] = []
        for leg in intent["legs"]:
            leg_id = str(leg["id"])
            status = str(leg.get("status", ""))
            attempts = self.repository.broker_orders_for_leg(leg_id)
            fills = self.repository.fills_for_leg(leg_id)
            cumulative = Decimal(str(leg.get("cumulative_filled_quantity", "0")))
            if status in {LegStatus.PLANNED.value, LegStatus.RECONCILIATION_REQUIRED.value}:
                if attempts or fills or cumulative != 0:
                    raise OMSExecutionError(f"unsubmitted leg {leg_id} has broker evidence")
                cancelable.append(leg_id)
                continue
            if status == LegStatus.FILLED.value:
                if len(attempts) != 1 or not self._attempt_has_complete_fill_evidence(attempts[0], leg):
                    raise OMSExecutionError(f"submitted leg {leg_id} lacks complete terminal fill proof")
                continue
            if status in {LegStatus.CANCELLED.value, LegStatus.REJECTED.value}:
                continue
            raise OMSExecutionError(f"aborted intent leg {leg_id} is not terminal or unsubmitted")
        if not cancelable:
            raise OMSExecutionError("aborted intent has no unsubmitted legs to cancel")

        for leg_id in cancelable:
            self.repository.transition_leg(leg_id, LegStatus.CANCELLED, now=self._now())
        remaining_exposure = self.repository.book_signed_exposure(account.id, str(intent.get("book_id") or ""))
        if any(value != 0 for value in remaining_exposure.values()):
            raise OMSExecutionError("local book remains non-flat after cancelling unsubmitted legs")
        self.repository.record_operational_event(
            event_id=f"aborted-intent-unsubmitted-legs:{intent_id}",
            account_id=account.id,
            event_type="ABORTED_INTENT_UNSUBMITTED_LEGS_CANCELLED",
            mode=account.environment.value,
            outcome="CANCELLED_UNSUBMITTED_LEGS",
            occurred_at=self._now(),
            summary="Cancelled only legs proven never submitted after terminal fills were flattened and the account was verified flat.",
            details={
                "intent_id": intent_id,
                "book_id": intent.get("book_id"),
                "cancelled_leg_ids": cancelable,
                "intent_status_preserved": IntentStatus.RECONCILIATION_REQUIRED.value,
                "broker_position_count": len(normalized_positions),
                "broker_open_order_count": len(normalized_open_orders),
                "broker_fill_count": len(facts.fills),
                "execution_evidence_mode": facts.execution_evidence_mode.value,
            },
        )
        return {
            "intent_id": intent_id,
            "status": IntentStatus.RECONCILIATION_REQUIRED.value,
            "cancelled_leg_ids": tuple(cancelable),
            "proof": "terminal_submitted_legs_flattened_and_unsubmitted_legs_cancelled",
        }

    def resolve_compensated_partial_intent(
        self,
        intent_id: str,
        *,
        account: Account,
    ) -> dict[str, object]:
        """Close one compensated partial ENTER/EXIT after fresh broker proof.

        This is an explicit recovery operation for a multi-leg entry where a
        submitted leg filled, a linked exit flattened that exact quantity,
        and another leg was either never submitted or has a provider-proven
        terminal zero-fill attempt.  It does not infer flatness from the local
        ledger or from a stale baseline: both a fresh complete
        account snapshot and a complete historical-order window covering the
        intent through ``now`` are mandatory.  Only the exact intent's
        terminal-history blockers are eligible for resolution; unrelated
        account blockers remain sticky.
        """
        self._startup_safety_audit()
        intent = self._required_intent(intent_id)
        canonical = self._canonical_account_for_intent(
            intent,
            supplied=account,
            source="resolve_compensated_partial_intent",
        )
        if canonical is None:
            raise OMSExecutionError("cannot resolve compensated intent without an enabled canonical account")
        account = canonical
        if not self._validate_persisted_execution_policy(
            intent, account, source="resolve_compensated_partial_intent"
        ):
            raise OMSExecutionError("cannot resolve compensated intent with an invalid persisted policy")
        if str(intent.get("status")) != IntentStatus.RECONCILIATION_REQUIRED.value:
            raise OMSExecutionError("only a reconciliation-required intent can be compensated")
        source_action = str(intent.get("action"))
        if source_action not in {IntentAction.ENTER.value, IntentAction.EXIT.value}:
            raise OMSExecutionError("compensated partial recovery supports only ENTER or EXIT intents")

        # A partial EXIT may be completed by the explicit residual route when
        # one sibling exited and another sibling was durably rejected/cancelled
        # with authenticated zero-fill evidence.  Discover only an exact
        # source-linked residual here; all of its legs are revalidated below.
        residual_candidate_ids: set[str] = set()
        if source_action == IntentAction.EXIT.value:
            for candidate_row in self.repository.book_intents(
                account.id, book_id=str(intent.get("book_id") or "")
            ):
                candidate_id = str(candidate_row.get("id") or "")
                if not candidate_id or candidate_id == intent_id:
                    continue
                candidate = self.repository.get_intent(candidate_id)
                metadata = candidate.get("metadata") if candidate is not None else None
                if (
                    candidate is not None
                    and str(candidate.get("account_id")) == account.id
                    and str(candidate.get("book_id") or "") == str(intent.get("book_id") or "")
                    and isinstance(metadata, Mapping)
                    and metadata.get("verified_residual_exit") is True
                    and str(metadata.get("source_intent_id")) == intent_id
                ):
                    residual_candidate_ids.add(candidate_id)

        # This scan may create the normal aged-terminal blocker.  The proof
        # below may resolve only the exact aged attempts it covers.
        self._ensure_terminal_history_safety(account)
        self._quarantine_multiple_attempts_for_account(account)

        def parse_decimal(value: object, label: str) -> Decimal:
            try:
                parsed = Decimal(str(value))
            except (InvalidOperation, TypeError, ValueError) as exc:
                raise OMSExecutionError(f"{label} is not numeric") from exc
            if not parsed.is_finite():
                raise OMSExecutionError(f"{label} is not finite")
            return parsed

        def parse_time(value: object, label: str) -> datetime:
            parsed = self._parse_timestamp(value)
            if parsed is None:
                raise OMSExecutionError(f"{label} is missing or invalid")
            return parsed

        def marker(value: object, needles: set[str]) -> bool:
            if isinstance(value, Mapping):
                return any(marker(key, needles) or marker(item, needles) for key, item in value.items())
            if isinstance(value, (list, tuple, set, frozenset)):
                return any(marker(item, needles) for item in value)
            if isinstance(value, str):
                return any(
                    value == needle
                    or value.startswith(f"{needle}:")
                    or value.startswith(f"{needle}|")
                    for needle in needles
                )
            return False

        def exact_source_entry(value: object, source_id: str, *, allow_canonical: bool = False) -> bool:
            if isinstance(value, Mapping):
                canonical_compensation = allow_canonical or (
                    value.get("verified_compensating_exit") is True
                    or value.get("verified_residual_exit") is True
                    or value.get("verified_partial_compensation") is True
                )
                for key, item in value.items():
                    key_text = normalize_provider_key(key)
                    if key_text in {"source_entry_intent", "source_entry_intent_id"} and str(item) == source_id:
                        return True
                    if canonical_compensation and key_text == "source_intent_id" and str(item) == source_id:
                        return True
                    if exact_source_entry(item, source_id, allow_canonical=canonical_compensation):
                        return True
            elif isinstance(value, (list, tuple, set, frozenset)):
                return any(
                    exact_source_entry(item, source_id, allow_canonical=allow_canonical)
                    for item in value
                )
            return False

        def validate_local_fill_leg(leg: Mapping[str, object]) -> tuple[dict[str, object], Decimal]:
            leg_id = str(leg["id"])
            attempts = self.repository.broker_orders_for_leg(leg_id)
            fills_by_attempt: list[dict[str, object]] = []
            requested = parse_decimal(leg.get("quantity"), f"leg {leg_id} quantity")
            if requested <= 0:
                raise OMSExecutionError(f"leg {leg_id} has invalid requested quantity")
            if len(attempts) != 1:
                raise OMSExecutionError(f"leg {leg_id} does not have exactly one broker attempt")
            attempt = attempts[0]
            if str(attempt.get("account_id")) != account.id:
                raise OMSExecutionError(f"leg {leg_id} broker attempt belongs to another account")
            if str(attempt.get("status")) != BrokerOrderStatus.FILLED.value:
                raise OMSExecutionError(f"leg {leg_id} broker attempt is not terminal FILLED")
            external_id = str(attempt.get("external_order_id") or "").strip()
            if not external_id:
                raise OMSExecutionError(f"leg {leg_id} has no external broker order identity")
            if parse_decimal(attempt.get("submitted_quantity"), f"leg {leg_id} submitted quantity") != requested:
                raise OMSExecutionError(f"leg {leg_id} submitted quantity does not match requested quantity")
            if str(leg.get("status")) != LegStatus.FILLED.value:
                raise OMSExecutionError(f"filled leg {leg_id} is not locally FILLED")
            if parse_decimal(leg.get("cumulative_filled_quantity"), f"leg {leg_id} cumulative quantity") != requested:
                raise OMSExecutionError(f"leg {leg_id} local cumulative quantity is incomplete")
            if not self._attempt_has_complete_fill_evidence(attempt, leg):
                raise OMSExecutionError(f"leg {leg_id} lacks durable attempt-scoped fill evidence")
            fills = self.repository.fills_for_broker_order(str(attempt["id"]))
            if not fills:
                raise OMSExecutionError(f"leg {leg_id} has no durable fills")
            total = Decimal("0")
            for fill in fills:
                quantity = parse_decimal(fill.get("quantity"), f"fill {fill.get('id')} quantity")
                if quantity <= 0:
                    raise OMSExecutionError(f"fill {fill.get('id')} has invalid quantity")
                total += quantity
                metadata = fill.get("metadata")
                if not isinstance(metadata, Mapping):
                    raise OMSExecutionError(f"fill {fill.get('id')} has no durable provenance")
                if str(metadata.get("_external_order_id", external_id)) != external_id:
                    raise OMSExecutionError(f"fill {fill.get('id')} has a foreign order identity")
                if str(metadata.get("_broker_fill_account_id", account.id)) != account.id:
                    raise OMSExecutionError(f"fill {fill.get('id')} has a foreign account identity")
            if total != requested:
                raise OMSExecutionError(f"leg {leg_id} durable fills do not equal requested quantity")
            return {"leg": leg, "attempt": attempt, "fills": fills, "verified_quantity": requested}, requested

        def validate_local_source_partial_leg(
            leg: Mapping[str, object],
        ) -> tuple[dict[str, object], Decimal]:
            """Validate one durably partial source leg without treating it as complete.

            A cancelled/failed leg with positive, exact durable fills is a
            legitimate residual source.  It still needs one immutable broker
            attempt, strict fill provenance, and a terminal provider state;
            active or ambiguous attempts never enter this path.
            """
            leg_id = str(leg["id"])
            attempts = self.repository.broker_orders_for_leg(leg_id)
            requested = parse_decimal(leg.get("quantity"), f"leg {leg_id} quantity")
            cumulative = parse_decimal(
                leg.get("cumulative_filled_quantity"), f"leg {leg_id} cumulative quantity"
            )
            if requested <= 0 or cumulative <= 0 or cumulative >= requested:
                raise OMSExecutionError(f"leg {leg_id} is not strictly partial")
            if len(attempts) != 1:
                raise OMSExecutionError(f"partial leg {leg_id} does not have exactly one broker attempt")
            if str(leg.get("status")) not in {
                LegStatus.PARTIALLY_FILLED.value,
                LegStatus.CANCELLED.value,
                LegStatus.RECONCILIATION_REQUIRED.value,
            }:
                raise OMSExecutionError(f"partial leg {leg_id} is not durably terminal/partial")
            attempt = attempts[0]
            if str(attempt.get("account_id")) != account.id:
                raise OMSExecutionError(f"partial leg {leg_id} broker attempt belongs to another account")
            if str(attempt.get("status")) not in {
                BrokerOrderStatus.PARTIALLY_FILLED.value,
                BrokerOrderStatus.CANCELLED.value,
            }:
                raise OMSExecutionError(f"partial leg {leg_id} broker attempt is active or unknown")
            external_id = str(attempt.get("external_order_id") or "").strip()
            if not external_id:
                raise OMSExecutionError(f"partial leg {leg_id} has no external broker order identity")
            submitted = parse_decimal(
                attempt.get("submitted_quantity"), f"partial leg {leg_id} submitted quantity"
            )
            if submitted != requested:
                raise OMSExecutionError(f"partial leg {leg_id} submitted quantity does not match requested quantity")
            fills = self.repository.fills_for_broker_order(str(attempt["id"]))
            if not fills:
                raise OMSExecutionError(f"partial leg {leg_id} has no durable fills")
            total = Decimal("0")
            identities: set[str] = set()
            submitted_at = parse_time(
                attempt.get("submitted_at") or attempt.get("updated_at"),
                f"partial attempt {external_id} submitted_at",
            )
            for fill in fills:
                quantity = parse_decimal(fill.get("quantity"), f"fill {fill.get('id')} quantity")
                price = parse_decimal(fill.get("price"), f"fill {fill.get('id')} price")
                if quantity <= 0 or price <= 0:
                    raise OMSExecutionError(f"fill {fill.get('id')} has invalid economics")
                identity = str(fill.get("external_fill_id") or fill.get("dedupe_key") or "").strip()
                if not identity or identity in identities:
                    raise OMSExecutionError(f"partial leg {leg_id} has duplicate/unknown fill identity")
                identities.add(identity)
                metadata = fill.get("metadata")
                if not isinstance(metadata, Mapping):
                    raise OMSExecutionError(f"fill {fill.get('id')} has no durable provenance")
                if str(fill.get("external_order_id") or external_id) != external_id:
                    raise OMSExecutionError(f"fill {fill.get('id')} has a foreign order identity")
                if str(fill.get("account_id") or account.id) != account.id:
                    raise OMSExecutionError(f"fill {fill.get('id')} has a foreign account identity")
                filled_at = parse_time(fill.get("filled_at"), f"fill {fill.get('id')} filled_at")
                received_at = parse_time(fill.get("received_at"), f"fill {fill.get('id')} received_at")
                if filled_at < submitted_at - timedelta(seconds=self.BROKER_TIME_ORDER_TOLERANCE_SECONDS):
                    raise OMSExecutionError(f"fill {fill.get('id')} predates durable submission")
                if received_at < filled_at - timedelta(seconds=self.BROKER_TIME_ORDER_TOLERANCE_SECONDS):
                    raise OMSExecutionError(f"fill {fill.get('id')} receipt predates fill")
                total += quantity
            if total != cumulative:
                raise OMSExecutionError(f"partial leg {leg_id} durable fills do not equal cumulative quantity")
            return {
                "leg": leg,
                "attempt": attempt,
                "fills": fills,
                "verified_quantity": cumulative,
                "partial_source": True,
            }, cumulative

        def validate_terminal_zero_leg(leg: Mapping[str, object]) -> dict[str, object]:
            """Validate one attempted sibling with an authenticated zero fill."""
            leg_id = str(leg["id"])
            attempts = self.repository.broker_orders_for_leg(leg_id)
            if len(attempts) != 1:
                raise OMSExecutionError(
                    f"zero-fill leg {leg_id} does not have exactly one broker attempt"
                )
            if self.repository.fills_for_leg(leg_id):
                raise OMSExecutionError(f"zero-fill leg {leg_id} has durable fills")
            try:
                cumulative = Decimal(str(leg.get("cumulative_filled_quantity", "0")))
            except (InvalidOperation, TypeError, ValueError) as exc:
                raise OMSExecutionError(f"zero-fill leg {leg_id} has malformed cumulative quantity") from exc
            if cumulative != 0:
                raise OMSExecutionError(f"zero-fill leg {leg_id} is not actually zero-filled")
            if str(leg.get("status")) not in {
                LegStatus.CANCELLED.value,
                LegStatus.REJECTED.value,
                LegStatus.FAILED.value,
                LegStatus.RECONCILIATION_REQUIRED.value,
            }:
                raise OMSExecutionError(f"zero-fill leg {leg_id} is not terminal")
            attempt = attempts[0]
            if not self._attempt_has_terminal_zero_fill_evidence(attempt, leg):
                raise OMSExecutionError(
                    f"zero-fill leg {leg_id} lacks strict terminal provider evidence"
                )
            return {"leg": leg, "attempt": attempt}

        submitted_legs: list[dict[str, object]] = []
        unsent_legs: list[str] = []
        terminal_zero_legs: dict[str, dict[str, object]] = {}
        entry_by_instrument: dict[str, Decimal] = {}
        expected_external_ids: dict[str, dict[str, object]] = {}
        for leg in intent["legs"]:
            status = str(leg.get("status"))
            attempts = self.repository.broker_orders_for_leg(str(leg["id"]))
            fills = self.repository.fills_for_leg(str(leg["id"]))
            cumulative = parse_decimal(leg.get("cumulative_filled_quantity"), f"leg {leg['id']} cumulative quantity")
            if not attempts:
                if fills or cumulative != 0 or status not in {
                    LegStatus.PLANNED.value,
                    LegStatus.RECONCILIATION_REQUIRED.value,
                    LegStatus.CANCELLED.value,
                }:
                    raise OMSExecutionError(f"leg {leg['id']} has ambiguous unsent evidence")
                unsent_legs.append(str(leg["id"]))
                continue
            if cumulative == 0:
                zero_proof = validate_terminal_zero_leg(leg)
                unsent_legs.append(str(leg["id"]))
                terminal_zero_legs[str(zero_proof["attempt"].get("external_order_id"))] = zero_proof
                continue
            requested = parse_decimal(leg.get("quantity"), f"leg {leg['id']} quantity")
            if cumulative < requested:
                proof, quantity = validate_local_source_partial_leg(leg)
                unsent_legs.append(str(leg["id"]))
            else:
                proof, quantity = validate_local_fill_leg(leg)
            submitted_legs.append(proof)
            external_id = str(proof["attempt"].get("external_order_id"))
            expected_external_ids[external_id] = proof
            instrument = str(leg.get("instrument_id"))
            signed = quantity if str(leg.get("side")) == Side.BUY.value else -quantity
            entry_by_instrument[instrument] = entry_by_instrument.get(instrument, Decimal("0")) + signed

        if not submitted_legs or not unsent_legs:
            raise OMSExecutionError("intent is not a partial ENTER/EXIT with both filled and unsent legs")

        facts, normalized_positions, normalized_open_orders = self._strict_authoritative_account_facts(
            account,
            max_age_seconds=self.BROKER_FACT_MAX_AGE_SECONDS,
            require_no_open_orders=True,
        )
        # ENTER compensation proves a flat account.  EXIT compensation restores
        # the account-wide managed allocation that existed before the partial
        # exit; it must not be mistaken for a flat account.  The shared fact
        # helper has already rejected incomplete, stale, contradictory, or
        # foreign-account rows.
        now = self._now()
        future_tolerance = timedelta(seconds=self.BROKER_CLOCK_SKEW_TOLERANCE_SECONDS)
        if source_action == IntentAction.ENTER.value:
            expected_account_positions: dict[str, Decimal] = {}
            if any(position.signed_quantity != 0 for position in normalized_positions):
                raise OMSExecutionError("broker account is not freshly flat")
        else:
            expected_account_positions = self._strict_durable_managed_account_exposure(account)
            observed_positions = {
                str(position.instrument_id): Decimal(str(position.signed_quantity))
                for position in normalized_positions
                if Decimal(str(position.signed_quantity)) != 0
            }
            if residual_candidate_ids:
                # The explicit residual path may have completed the remaining
                # EXIT exposure, so the only safe alternative to the original
                # durable allocation is an actually flat fresh account.  The
                # exact source/residual quantity proof is checked after both
                # lifecycles are validated below.
                if observed_positions:
                    raise OMSExecutionError(
                        "partial EXIT residual completion requires a freshly flat broker account"
                    )
            elif observed_positions != expected_account_positions:
                raise OMSExecutionError(
                    "broker account positions do not equal the durable managed EXIT compensation exposure"
                )
        if facts.execution_evidence_mode is ExecutionEvidenceMode.UNAVAILABLE:
            raise OMSExecutionError("execution evidence is unavailable")
        for broker_fill in facts.fills:
            claims = self.repository.broker_orders_for_external_order_id(broker_fill.external_order_id)
            if not claims:
                raise OMSExecutionError("fresh broker facts contain an unmatched fill")
            if broker_fill.account_id not in (None, account.id):
                raise OMSExecutionError("fresh broker facts contain a foreign-account fill")

        history_getter = getattr(self.adapter, "get_historical_order_facts", None)
        if not callable(history_getter):
            raise OMSExecutionError("bounded historical order facts are required")
        window_start = parse_time(intent.get("created_at"), "intent created_at")
        history_requested_end = self._now()
        try:
            historical = history_getter(account, window_start, history_requested_end)
        except Exception as exc:
            raise OMSExecutionError(f"historical order facts unavailable: {exc}") from exc
        if not isinstance(historical, BrokerHistoricalOrderFacts):
            raise OMSExecutionError("historical order facts are invalid")
        # Use a post-query observation for freshness, while retaining the
        # exact end timestamp requested from the provider for coverage.
        now = self._now()
        if (
            historical.account_id != account.id
            or not historical.complete
            or historical.error
            or historical.requested_start > window_start
            or historical.requested_end < history_requested_end
            or historical.requested_end > now + future_tolerance
            or historical.execution_evidence_mode is ExecutionEvidenceMode.UNAVAILABLE
            or "HISTORICAL_ORDER_SNAPSHOTS" not in historical.execution_evidence_scope
        ):
            raise OMSExecutionError("historical order facts do not cover the complete fresh proof window")
        history_captured = parse_time(historical.captured_at, "historical captured_at")
        if history_captured > now + future_tolerance or (now - history_captured).total_seconds() > 900:
            raise OMSExecutionError("historical order facts are stale")

        history_by_external: dict[str, list[BrokerOrderSnapshot]] = {}
        for snapshot in historical.orders:
            history_by_external.setdefault(str(snapshot.external_order_id), []).append(snapshot)
        history_fills_by_external: dict[str, list[BrokerFill]] = {}
        for broker_fill in historical.fills:
            history_fills_by_external.setdefault(str(broker_fill.external_order_id), []).append(broker_fill)
        for external_id, proof in expected_external_ids.items():
            rows = history_by_external.get(external_id, [])
            if len(rows) != 1:
                raise OMSExecutionError(f"historical order coverage is missing or contradictory for {external_id}")
            snapshot = rows[0]
            leg = proof["leg"]
            requested = parse_decimal(leg.get("quantity"), f"history {external_id} quantity")
            quantity = parse_decimal(
                proof.get("verified_quantity", requested), f"history {external_id} filled quantity"
            )
            partial_source = proof.get("partial_source") is True
            if (
                snapshot.account_id != account.id
                or snapshot.instrument_id != str(leg.get("instrument_id"))
                or snapshot.side.value != str(leg.get("side"))
                or snapshot.quantity != requested
                or snapshot.filled_quantity != quantity
                or snapshot.status
                not in (
                    {BrokerOrderStatus.PARTIALLY_FILLED, BrokerOrderStatus.CANCELLED}
                    if partial_source
                    else {BrokerOrderStatus.FILLED}
                )
            ):
                raise OMSExecutionError(f"historical order {external_id} conflicts with the durable leg")
            attempt_time = parse_time(proof["attempt"].get("submitted_at"), f"attempt {external_id} submitted_at")
            observed_time = snapshot.order_time or snapshot.captured_at
            if observed_time < attempt_time - timedelta(seconds=self.BROKER_TIME_ORDER_TOLERANCE_SECONDS):
                raise OMSExecutionError(f"historical order {external_id} predates durable submission")

        for external_id, proof in terminal_zero_legs.items():
            rows = history_by_external.get(external_id, [])
            if len(rows) != 1:
                raise OMSExecutionError(
                    f"historical order coverage is missing or contradictory for terminal zero-fill {external_id}"
                )
            snapshot = rows[0]
            leg = proof["leg"]
            attempt = proof["attempt"]
            submitted = parse_decimal(attempt.get("submitted_quantity"), f"history {external_id} quantity")
            if (
                snapshot.account_id != account.id
                or snapshot.instrument_id != str(leg.get("instrument_id"))
                or snapshot.side.value != str(leg.get("side"))
                or snapshot.quantity != submitted
                or snapshot.filled_quantity != 0
                or snapshot.status
                not in {
                    BrokerOrderStatus.CANCELLED,
                    BrokerOrderStatus.REJECTED,
                    BrokerOrderStatus.FAILED,
                }
            ):
                raise OMSExecutionError(
                    f"historical order {external_id} contradicts terminal zero-fill proof"
                )
            attempt_time = parse_time(attempt.get("submitted_at"), f"attempt {external_id} submitted_at")
            observed_time = snapshot.order_time or snapshot.captured_at
            if observed_time < attempt_time - timedelta(seconds=self.BROKER_TIME_ORDER_TOLERANCE_SECONDS):
                raise OMSExecutionError(f"historical order {external_id} predates durable submission")
            if history_fills_by_external.get(external_id):
                raise OMSExecutionError(
                    f"historical order facts contain a late fill for terminal zero-fill {external_id}"
                )

        # If the provider also exposes deal rows, bind every row to the same
        # expected attempt/account/instrument.  Cumulative-only history is
        # allowed to omit deals, but it may not contain contradictory ones.
        for external_id, proof in expected_external_ids.items():
            history_fills = history_fills_by_external.get(external_id, [])
            leg = proof["leg"]
            expected_quantity = parse_decimal(
                proof.get("verified_quantity", leg.get("quantity")), f"history {external_id} quantity"
            )
            total_history_quantity = Decimal("0")
            for broker_fill in history_fills:
                if broker_fill.account_id not in (None, account.id):
                    raise OMSExecutionError(f"historical fill {external_id} belongs to another account")
                if broker_fill.instrument_id not in (None, str(leg.get("instrument_id"))):
                    raise OMSExecutionError(f"historical fill {external_id} has a foreign instrument")
                if broker_fill.quantity <= 0 or not broker_fill.quantity.is_finite():
                    raise OMSExecutionError(f"historical fill {external_id} has invalid quantity")
                total_history_quantity += broker_fill.quantity
                attempt_time = parse_time(
                    proof["attempt"].get("submitted_at"), f"attempt {external_id} submitted_at"
                )
                if broker_fill.filled_at < attempt_time - timedelta(seconds=self.BROKER_TIME_ORDER_TOLERANCE_SECONDS):
                    raise OMSExecutionError(f"historical fill {external_id} predates durable submission")
            if history_fills and total_history_quantity != expected_quantity:
                raise OMSExecutionError(f"historical fills for {external_id} do not equal the durable quantity")
            if (
                historical.execution_evidence_mode is ExecutionEvidenceMode.INDIVIDUAL_DEALS
                and total_history_quantity != expected_quantity
            ):
                raise OMSExecutionError(f"individual-deal history lacks complete evidence for {external_id}")

        linked_exits: list[dict[str, object]] = []
        linked_exit_intents_to_complete: set[str] = set()
        for row in self.repository.book_intents(account.id):
            candidate_id = str(row.get("id"))
            if candidate_id == intent_id:
                continue
            candidate = self.repository.get_intent(candidate_id)
            if candidate is None or not exact_source_entry(candidate.get("metadata"), intent_id):
                continue
            if (
                str(candidate.get("account_id")) != account.id
                or str(candidate.get("book_id") or "") != str(intent.get("book_id") or "")
            ):
                raise OMSExecutionError(f"linked compensation {candidate_id} has an account or book mismatch")
            candidate_metadata = candidate.get("metadata")
            is_residual_completion = (
                source_action == IntentAction.EXIT.value
                and isinstance(candidate_metadata, Mapping)
                and candidate_metadata.get("verified_residual_exit") is True
                and str(candidate_metadata.get("source_intent_id")) == intent_id
            )
            allowed_compensation_actions = (
                {IntentAction.EXIT.value, IntentAction.FLATTEN.value}
                if source_action == IntentAction.ENTER.value
                else ({IntentAction.ENTER.value, IntentAction.EXIT.value} if is_residual_completion else {IntentAction.ENTER.value})
            )
            if str(candidate.get("action")) not in allowed_compensation_actions:
                raise OMSExecutionError(f"linked compensation {candidate_id} has an invalid action")
            positive_legs = []
            unsubmitted_or_definite_rejection = False
            for leg in candidate["legs"]:
                attempts = self.repository.broker_orders_for_leg(str(leg["id"]))
                if not attempts:
                    unsubmitted_or_definite_rejection = True
                    continue
                # A retry may leave behind a separately recorded, definite
                # no-submit rejection for the same compensation request.
                # It has no broker order claim, fill, or exposure and must not
                # mask a later, independently proven compensation.  Any
                # external order, fill, or ambiguous attempt remains a hard
                # blocker and follows the normal strict validation below.
                if (
                    len(attempts) == 1
                    and self._is_durable_no_submit_rejection(
                        attempts[0], leg, normalized_open_orders
                    )
                ):
                    unsubmitted_or_definite_rejection = True
                    continue
                proof, quantity = validate_local_fill_leg(leg)
                positive_legs.append((proof, quantity))
                external_id = str(proof["attempt"].get("external_order_id"))
                expected_external_ids[external_id] = proof
            if positive_legs:
                if unsubmitted_or_definite_rejection:
                    raise OMSExecutionError(
                        f"linked compensation {candidate_id} has an unsubmitted or rejected leg"
                    )
                candidate_status = str(candidate.get("status"))
                if candidate_status not in {
                    IntentStatus.FILLED.value,
                    IntentStatus.COMPLETED.value,
                    IntentStatus.RECONCILIATION_REQUIRED.value,
                }:
                    raise OMSExecutionError(f"linked compensation {candidate_id} is not terminal")
                if candidate_status == IntentStatus.RECONCILIATION_REQUIRED.value:
                    # A prior recovery pass may have left the exit intent in
                    # RECONCILIATION_REQUIRED even though every own leg is a
                    # complete, terminal fill.  It is eligible for promotion
                    # only after the same account/history/offset proof below;
                    # no status or label alone is trusted here.
                    if any(str(leg.get("status")) != LegStatus.FILLED.value for leg in candidate["legs"]):
                        raise OMSExecutionError(f"linked compensation {candidate_id} is not fully filled")
                    linked_exit_intents_to_complete.add(candidate_id)
                linked_exits.extend(positive_legs)

        if not linked_exits:
            raise OMSExecutionError("no linked compensating intent with durable fill evidence")

        # Re-check the complete historical window after discovering linked
        # compensation intents.  Entry claims are checked above, but exit
        # order IDs are only known after their exact source-entry metadata has
        # been validated.
        for external_id, proof in expected_external_ids.items():
            rows = history_by_external.get(external_id, [])
            if len(rows) != 1:
                raise OMSExecutionError(f"historical order coverage is missing or contradictory for {external_id}")
            snapshot = rows[0]
            leg = proof["leg"]
            requested = parse_decimal(leg.get("quantity"), f"history {external_id} quantity")
            quantity = parse_decimal(
                proof.get("verified_quantity", requested), f"history {external_id} filled quantity"
            )
            partial_source = proof.get("partial_source") is True
            if (
                snapshot.account_id != account.id
                or snapshot.instrument_id != str(leg.get("instrument_id"))
                or snapshot.side.value != str(leg.get("side"))
                or snapshot.quantity != requested
                or snapshot.filled_quantity != quantity
                or snapshot.status
                not in (
                    {BrokerOrderStatus.PARTIALLY_FILLED, BrokerOrderStatus.CANCELLED}
                    if partial_source
                    else {BrokerOrderStatus.FILLED}
                )
            ):
                raise OMSExecutionError(f"historical order {external_id} conflicts with the durable leg")
            attempt_time = parse_time(proof["attempt"].get("submitted_at"), f"attempt {external_id} submitted_at")
            observed_time = snapshot.order_time or snapshot.captured_at
            if observed_time < attempt_time - timedelta(seconds=self.BROKER_TIME_ORDER_TOLERANCE_SECONDS):
                raise OMSExecutionError(f"historical order {external_id} predates durable submission")
            history_fills = history_fills_by_external.get(external_id, [])
            if history_fills:
                history_quantity = sum((fill.quantity for fill in history_fills), Decimal("0"))
                if history_quantity != quantity:
                    raise OMSExecutionError(f"historical fills for {external_id} do not equal the durable quantity")
                for broker_fill in history_fills:
                    if broker_fill.account_id not in (None, account.id):
                        raise OMSExecutionError(f"historical fill {external_id} belongs to another account")
                    if broker_fill.instrument_id not in (None, str(leg.get("instrument_id"))):
                        raise OMSExecutionError(f"historical fill {external_id} has a foreign instrument")
        exit_by_instrument: dict[str, Decimal] = {}
        for proof, quantity in linked_exits:
            leg = proof["leg"]
            instrument = str(leg.get("instrument_id"))
            signed = quantity if str(leg.get("side")) == Side.BUY.value else -quantity
            exit_by_instrument[instrument] = exit_by_instrument.get(instrument, Decimal("0")) + signed
        residual_completion_ids = {
            str(proof["leg"].get("intent_id"))
            for proof, _quantity in linked_exits
            if str(proof["leg"].get("intent_id")) in residual_candidate_ids
        }
        if source_action == IntentAction.EXIT.value and residual_completion_ids:
            # A partial EXIT residual is a different proof shape from a
            # compensating ENTER: every positive source EXIT fill and every
            # authenticated zero-fill sibling must be paired with the exact
            # residual order, and the durable pre-EXIT allocation must then be
            # fully offset.  No labels or netting are trusted here.
            zero_by_instrument = {
                str(proof["leg"].get("instrument_id")): proof["leg"]
                for proof in terminal_zero_legs.values()
            }
            residual_by_instrument: dict[str, Decimal] = {}
            for proof, quantity in linked_exits:
                candidate_id = str(proof["leg"].get("intent_id"))
                if candidate_id not in residual_candidate_ids:
                    raise OMSExecutionError("partial EXIT residual contains an unrelated linked intent")
                instrument = str(proof["leg"].get("instrument_id"))
                source_zero_leg = zero_by_instrument.get(instrument)
                if source_zero_leg is None:
                    raise OMSExecutionError("partial EXIT residual does not match a terminal zero-fill sibling")
                if (
                    str(proof["leg"].get("side")) != str(source_zero_leg.get("side"))
                    or quantity != parse_decimal(source_zero_leg.get("quantity"), "terminal zero sibling quantity")
                ):
                    raise OMSExecutionError("partial EXIT residual quantity or side differs from zero-fill sibling")
                if instrument in residual_by_instrument:
                    raise OMSExecutionError("partial EXIT residual reuses a zero-fill sibling instrument")
                signed = quantity if str(proof["leg"].get("side")) == Side.BUY.value else -quantity
                residual_by_instrument[instrument] = signed
            if set(residual_by_instrument) != set(zero_by_instrument):
                raise OMSExecutionError("partial EXIT residual does not cover every zero-fill sibling")
            try:
                pre_source_basis = self.repository.book_signed_exposure(
                    account.id,
                    str(intent.get("book_id") or ""),
                    exclude_intent_ids={intent_id, *residual_candidate_ids},
                )
            except Exception as exc:
                raise OMSExecutionError("partial EXIT residual durable book basis is unavailable") from exc
            combined = dict(pre_source_basis)
            for instrument, quantity in entry_by_instrument.items():
                combined[instrument] = combined.get(instrument, Decimal("0")) + quantity
            for instrument, quantity in residual_by_instrument.items():
                combined[instrument] = combined.get(instrument, Decimal("0")) + quantity
            if any(quantity != 0 for quantity in combined.values()):
                raise OMSExecutionError("partial EXIT residual does not flatten the durable managed book")
        elif set(exit_by_instrument) != set(entry_by_instrument) or any(
            entry_by_instrument[instrument] + exit_by_instrument.get(instrument, Decimal("0")) != 0
            for instrument in entry_by_instrument
        ):
            raise OMSExecutionError("linked compensation does not exactly offset the filled source exposure")

        # Require the historical window to contain every submitted claim,
        # including the compensating exit, and reject explicit claims for this
        # entry that cannot be matched to one of those claims.
        all_markers = {intent_id, str(intent.get("idempotency_key") or "")}
        all_markers.update(str(leg["id"]) for leg in intent["legs"])
        for proof, _quantity in linked_exits:
            exit_intent = self._required_intent(str(proof["leg"].get("intent_id")))
            all_markers.add(str(exit_intent.get("id")))
            all_markers.add(str(exit_intent.get("idempotency_key") or ""))
        all_markers.discard("")
        proof_external_ids = set(expected_external_ids) | set(terminal_zero_legs)
        for snapshot in historical.orders:
            if marker(snapshot.client_order_id, all_markers) or marker(snapshot.metadata, all_markers):
                if str(snapshot.external_order_id) not in proof_external_ids:
                    raise OMSExecutionError("historical facts contain an unmatched claim for the recovered intents")

        book_id = str(intent.get("book_id") or "")
        local_book_totals: dict[str, Decimal] = {}
        for row in self.repository.book_position_allocations(account.id, book_id=book_id):
            instrument = str(row.get("instrument_id"))
            try:
                allocation_quantity = parse_decimal(
                    row.get("signed_quantity", "0"), f"allocation {row.get('id')} quantity"
                )
            except OMSExecutionError:
                raise
            if allocation_quantity != 0 and str(row.get("ownership_class") or "").upper() != OwnershipClass.MANAGED.value:
                raise OMSExecutionError("local book allocations contain unknown ownership")
            if instrument in entry_by_instrument:
                local_book_totals[instrument] = local_book_totals.get(instrument, Decimal("0")) + allocation_quantity
        if source_action == IntentAction.ENTER.value:
            if any(local_book_totals.get(instrument, Decimal("0")) != 0 for instrument in entry_by_instrument):
                raise OMSExecutionError("local book allocations are not flat after compensation")
        else:
            if not local_book_totals:
                raise OMSExecutionError("partial EXIT compensation lacks durable book allocation evidence")
            if any(
                expected_account_positions.get(instrument, Decimal("0"))
                != local_book_totals.get(instrument, Decimal("0"))
                for instrument in local_book_totals
            ):
                raise OMSExecutionError("partial EXIT compensation has cross-book or allocation exposure")

        proof_attempts = tuple(expected_external_ids.values()) + tuple(terminal_zero_legs.values())
        allowed_issue_keys = {
            f"TERMINAL_HISTORY_EXPIRED:BROKER_ORDER:{internal_id}"
            for proof in proof_attempts
            for internal_id in [str(proof["attempt"].get("id"))]
        }
        # A prior recovery poll may have emitted the paired terminal-evidence
        # issue/action when the aged order was absent from the current-order
        # endpoint.  The fresh, complete historical proof above binds every
        # expected external order, account, instrument, side, quantity, and
        # terminal fill, so these exact derivative rows are safe to resolve
        # together with the age quarantine.  Any row for another attempt still
        # fails the ownership check below and remains sticky.
        allowed_issue_keys.update(
            {
                f"BROKER_TERMINAL_EVIDENCE_MISSING:BROKER_ORDER:{internal_id}"
                for proof in proof_attempts
                for internal_id in [str(proof["attempt"].get("id"))]
            }
        )
        partial_source_attempts = {
            str(proof["attempt"].get("id")): proof
            for proof in expected_external_ids.values()
            if proof.get("partial_source") is True
        }

        def proven_partial_source_issue(issue: Mapping[str, object]) -> bool:
            """Allow only exact recovery rows caused by a proven partial source."""
            category = str(issue.get("category", ""))
            if category not in {"FILL_EVIDENCE_REQUIRED", "TERMINAL_FILL_CONTRADICTION"}:
                return False
            internal_id = str(issue.get("entity_key", "")).strip()
            proof = partial_source_attempts.get(internal_id)
            if proof is None:
                return False
            details = self._issue_details(issue)
            expected_quantity = parse_decimal(
                proof.get("verified_quantity"), f"partial source {internal_id} quantity"
            )
            try:
                observed_quantity = Decimal(
                    str(details.get("durable_filled_quantity", details.get("cumulative_filled_quantity")))
                )
            except (InvalidOperation, TypeError, ValueError):
                return False
            if observed_quantity != expected_quantity:
                return False
            if category == "FILL_EVIDENCE_REQUIRED":
                return str(details.get("broker_status", "")) in {
                    BrokerOrderStatus.PARTIALLY_FILLED.value,
                    BrokerOrderStatus.CANCELLED.value,
                }
            try:
                incoming_count = int(details.get("incoming_fill_count", 0) or 0)
            except (TypeError, ValueError):
                return False
            return (
                str(details.get("terminal_status", "")) == BrokerOrderStatus.CANCELLED.value
                and incoming_count > 0
            )
        retired_book_ids = self._verified_retired_baseline_books(account)
        retired_allocation_ids: set[str] = set()
        if retired_book_ids:
            for retired_book_id in retired_book_ids:
                retired_allocation_ids.update(
                    str(row.get("id"))
                    for row in self.repository.book_position_allocations(
                        account.id,
                        book_id=retired_book_id,
                    )
                )

        def retired_capacity_issue(issue: Mapping[str, object]) -> bool:
            if str(issue.get("category", "")) != "BOOK_OWNERSHIP_OR_CAPACITY":
                return False
            details = self._issue_details(issue)
            blockers = details.get("blockers") if isinstance(details, Mapping) else None
            return bool(blockers) and all(
                isinstance(item, Mapping)
                and item.get("kind") == "unknown_book_allocation"
                and str(item.get("book_id", "")) in retired_book_ids
                and str(item.get("allocation_id", "")) in retired_allocation_ids
                for item in blockers
            )

        def retired_capacity_action(action: Mapping[str, object]) -> bool:
            if not str(action.get("action_key", "")).startswith("BOOK_RISK_BLOCK:"):
                return False
            metadata = action.get("metadata")
            blockers = metadata.get("blockers") if isinstance(metadata, Mapping) else None
            return bool(blockers) and all(
                isinstance(item, Mapping)
                and item.get("kind") == "unknown_book_allocation"
                and str(item.get("book_id", "")) in retired_book_ids
                and str(item.get("allocation_id", "")) in retired_allocation_ids
                for item in blockers
            )

        def related_proven_compensated_intent(other_id: str) -> bool:
            other = self.repository.get_intent(other_id)
            if other is None or str(other.get("account_id")) != account.id:
                return False
            if str(other.get("status")) not in {
                IntentStatus.RECONCILIATION_REQUIRED.value,
                IntentStatus.CANCELLED.value,
            }:
                return False
            metadata = other.get("metadata")
            closure = metadata.get("compensated_partial_closure") if isinstance(metadata, Mapping) else None
            if not isinstance(closure, Mapping):
                return False
            if str(closure.get("reason", "")) not in {
                "fresh_broker_and_historical_proof_of_compensated_partial_entry",
                "fresh_broker_and_historical_proof_of_compensated_partial_exit",
            } or str(closure.get("account_id", account.id)) != account.id:
                return False
            external_ids = {
                str(value).strip()
                for field in (
                    "filled_entry_external_order_ids",
                    "filled_source_external_order_ids",
                    "compensating_external_order_ids",
                    "terminal_zero_external_order_ids",
                )
                for value in (closure.get(field) or ())
                if str(value).strip()
            }
            if not external_ids:
                return False
            if str(other.get("status")) == IntentStatus.CANCELLED.value:
                # A previously completed proof-gated closure is durable local
                # evidence.  The current call must not re-open its already
                # terminal sibling merely because this intent's fresh history
                # window starts later.
                return other_id in self._closed_historical_intent_ids(account)
            for external_id in external_ids:
                rows = history_by_external.get(external_id, [])
                if len(rows) != 1:
                    return False
                snapshot = rows[0]
                if snapshot.account_id != account.id or snapshot.status is not BrokerOrderStatus.FILLED:
                    return False
            return True

        def baseline_wrapper(row: Mapping[str, object]) -> bool:
            if str(row.get("category", "")) != "ACCOUNT_RECONCILIATION_BLOCK":
                return False
            if (
                str(row.get("entity_type", "")) != "ACCOUNT"
                or str(row.get("entity_key", "")) != account.id
                or str(row.get("issue_key", "")) != f"ACCOUNT_RECONCILIATION_BLOCK:ACCOUNT:{account.id}"
            ):
                return False
            details = self._issue_details(row)
            if not isinstance(details, Mapping):
                return False
            baseline = self.repository.latest_execution_evidence_baseline(account.id)
            if baseline is None:
                return False
            if (
                str(details.get("causal_account_id", "")) != account.id
                or str(details.get("causal_baseline_id", "")) != str(baseline.id)
                or str(details.get("causal_source_ledger_fingerprint", ""))
                != str(baseline.source_ledger_fingerprint)
            ):
                return False
            causal_intents = details.get("causal_intent_ids")
            if not isinstance(causal_intents, list) or not causal_intents:
                return False
            # A wrapper may be tolerated during proof of an unrelated
            # compensated historical intent only when every named causal
            # intent is still truly unsubmitted.  It cannot hide a live claim.
            for causal_id in causal_intents:
                causal = self.repository.get_intent(str(causal_id))
                if causal is None or str(causal.get("account_id")) != account.id:
                    return False
                if any(self.repository.broker_orders_for_leg(str(leg["id"])) for leg in causal.get("legs", ())):
                    return False
                if any(self.repository.fills_for_leg(str(leg["id"])) for leg in causal.get("legs", ())):
                    return False
            return True

        verified_roundtrip_external_ids: set[str] = set()
        for historical_row in self.repository.book_intents(account.id):
            historical_intent = self.repository.get_intent(str(historical_row.get("id", "")))
            if historical_intent is None:
                continue
            historical_metadata = historical_intent.get("metadata")
            roundtrip = (
                historical_metadata.get("verified_roundtrip_closure")
                if isinstance(historical_metadata, Mapping)
                else None
            )
            if not isinstance(roundtrip, Mapping) or str(roundtrip.get("account_id", account.id)) != account.id:
                continue
            for field in ("entry_external_order_ids", "exit_external_order_ids"):
                verified_roundtrip_external_ids.update(
                    str(value).strip()
                    for value in (roundtrip.get(field) or ())
                    if str(value).strip()
                )

        def stale_unsubmitted_history_action(action: Mapping[str, object]) -> bool:
            if not str(action.get("action_key", "")).startswith("BROKER_FACT_GATE:"):
                return False
            intent_row = self.repository.get_intent(str(action.get("intent_id", "")))
            if intent_row is None or str(intent_row.get("account_id")) != account.id:
                return False
            if any(
                self.repository.broker_orders_for_leg(str(leg["id"]))
                or self.repository.fills_for_leg(str(leg["id"]))
                for leg in intent_row.get("legs", ())
            ):
                return False
            metadata = action.get("metadata")
            blockers = metadata.get("blockers") if isinstance(metadata, Mapping) else None
            if not isinstance(blockers, list) or not blockers:
                return False
            external_ids = {
                str(item.get("external_order_id", "")).strip()
                for item in blockers
                if isinstance(item, Mapping)
            }
            return bool(external_ids) and external_ids.issubset(verified_roundtrip_external_ids)

        open_issues = self.repository.open_reconciliation_issues(account.id)
        issue_keys_to_resolve: set[str] = set()
        for issue in open_issues:
            issue_key = str(issue.get("issue_key"))
            category = str(issue.get("category", ""))
            if issue_key in allowed_issue_keys or proven_partial_source_issue(issue):
                issue_keys_to_resolve.add(issue_key)
                continue
            if category == "MIXED_TERMINAL_LEGS":
                other_id = str(issue.get("entity_key", ""))
                if other_id == intent_id or related_proven_compensated_intent(other_id):
                    if other_id == intent_id:
                        issue_keys_to_resolve.add(issue_key)
                    continue
            if retired_capacity_issue(issue) or baseline_wrapper(issue):
                continue
            raise OMSExecutionError("an unrelated reconciliation issue prevents compensated closure")
        if any(
            str(issue.get("issue_key")) not in issue_keys_to_resolve
            and not (retired_capacity_issue(issue) or baseline_wrapper(issue))
            and not (
                str(issue.get("category", "")) == "MIXED_TERMINAL_LEGS"
                and related_proven_compensated_intent(str(issue.get("entity_key", "")))
            )
            for issue in open_issues
        ):
            raise OMSExecutionError("an unrelated reconciliation issue prevents compensated closure")
        open_actions = self.repository.open_recovery_actions(account.id)
        allowed_action_keys = {
            f"TERMINAL_HISTORY_EXPIRED:{internal_id}"
            for proof in proof_attempts
            for internal_id in [str(proof["attempt"].get("id"))]
        }
        allowed_action_keys.update(
            {
                f"TERMINAL_ORDER_EVIDENCE:{internal_id}"
                for proof in proof_attempts
                for internal_id in [str(proof["attempt"].get("id"))]
            }
        )

        def proven_partial_source_action(action: Mapping[str, object]) -> bool:
            action_key = str(action.get("action_key", ""))
            action_intent = str(action.get("intent_id", ""))
            if action_intent != intent_id:
                return False
            if action_key.startswith("TERMINAL_PARTIAL:"):
                internal_id = action_key.split(":", 1)[1]
            elif action_key.startswith("TERMINAL_FILL_CONTRADICTION:"):
                internal_id = action_key.split(":", 2)[1]
            else:
                return False
            proof = partial_source_attempts.get(internal_id)
            if proof is None:
                return False
            metadata = action.get("metadata")
            if not isinstance(metadata, Mapping):
                return False
            expected_quantity = parse_decimal(
                proof.get("verified_quantity"), f"partial source {internal_id} quantity"
            )
            try:
                observed_quantity = Decimal(
                    str(metadata.get("durable_filled_quantity", metadata.get("cumulative_filled_quantity")))
                )
            except (InvalidOperation, TypeError, ValueError):
                return False
            if observed_quantity != expected_quantity:
                return False
            if action_key.startswith("TERMINAL_PARTIAL:"):
                return True
            try:
                incoming_count = int(metadata.get("incoming_fill_count", 0) or 0)
            except (TypeError, ValueError):
                return False
            return (
                str(metadata.get("terminal_status", "")) == BrokerOrderStatus.CANCELLED.value
                and incoming_count > 0
            )
        owned_intent_ids = {intent_id}
        for proof, _quantity in linked_exits:
            owned_intent_ids.add(str(proof["leg"].get("intent_id")))
        action_keys_to_resolve: set[str] = set()
        for action in open_actions:
            action_key = str(action.get("action_key"))
            action_intent = str(action.get("intent_id"))
            if action_intent in owned_intent_ids and action_key in allowed_action_keys:
                action_keys_to_resolve.add(action_key)
                continue
            if proven_partial_source_action(action):
                action_keys_to_resolve.add(action_key)
                continue
            if action_key == f"MIXED_TERMINAL_LEGS:{intent_id}":
                action_keys_to_resolve.add(action_key)
                continue
            if action_key.startswith("MIXED_TERMINAL_LEGS:") and related_proven_compensated_intent(action_intent):
                related = self.repository.get_intent(action_intent)
                if related is not None and str(related.get("status")) == IntentStatus.CANCELLED.value:
                    action_keys_to_resolve.add(action_key)
                continue
            if retired_capacity_action(action) or (
                action_key.startswith("ACCOUNT_RECONCILIATION_BLOCK:")
                and baseline_wrapper(action)
            ) or stale_unsubmitted_history_action(action):
                continue
            raise OMSExecutionError("an unrelated recovery action prevents compensated closure")

        for issue in open_issues:
            if str(issue.get("issue_key")) in issue_keys_to_resolve:
                self._resolve_reconciliation_issue(account.id, str(issue["issue_key"]), resolved_at=now)
        for action in open_actions:
            if str(action.get("action_key")) in action_keys_to_resolve:
                self._resolve_recovery_action(
                    account.id,
                    str(action["intent_id"]),
                    str(action["action_key"]),
                    resolved_at=now,
                )
        for candidate_id in sorted(linked_exit_intents_to_complete):
            candidate_external_ids = sorted(
                str(proof["attempt"].get("external_order_id"))
                for proof, _quantity in linked_exits
                if str(proof["leg"].get("intent_id")) == candidate_id
            )
            candidate_metadata = {
                "reason": "fresh_broker_and_historical_proof_of_exact_compensating_exit",
                "source_entry_intent_id": intent_id,
                "account_id": account.id,
                "book_id": str(intent.get("book_id") or ""),
                "external_order_ids": candidate_external_ids,
                "verified_at": now.isoformat(),
            }
            self.repository.mark_verified_compensating_intent(
                candidate_id,
                closure_metadata=candidate_metadata,
                now=now,
                _resolution_capability=self.repository._resolution_capability(),
            )
            event_id = f"verified-compensating-exit:{candidate_id}"
            if not any(event.get("id") == event_id for event in self.repository.operational_events(account.id, limit=1000)):
                self.repository.record_operational_event(
                    event_id=event_id,
                    account_id=account.id,
                    event_type="VERIFIED_COMPENSATING_EXIT_CLOSED",
                    mode=account.environment.value,
                    outcome=IntentStatus.COMPLETED.value,
                    occurred_at=now,
                    summary="Terminalized a linked exit intent after exact fresh flat-account and historical-order proof.",
                    details={"intent_id": candidate_id, "proof": candidate_metadata},
                )
        closure_reason = (
            "fresh_broker_and_historical_proof_of_compensated_partial_entry"
            if source_action == IntentAction.ENTER.value
            else "fresh_broker_and_historical_proof_of_compensated_partial_exit"
        )
        proof_metadata = {
            "reason": closure_reason,
            "source_action": source_action,
            "verified_at": now.isoformat(),
            "historical_window": {
                "requested_start": historical.requested_start.isoformat(),
                "requested_end": historical.requested_end.isoformat(),
                "captured_at": historical.captured_at.isoformat(),
                "order_count": len(historical.orders),
                "fill_count": len(historical.fills),
                "evidence_mode": historical.execution_evidence_mode.value,
            },
            "account_facts_captured_at": facts.captured_at.isoformat(),
            "filled_entry_external_order_ids": sorted(
                str(proof["attempt"].get("external_order_id")) for proof in submitted_legs
            ),
            "filled_source_external_order_ids": sorted(
                str(proof["attempt"].get("external_order_id")) for proof in submitted_legs
            ),
            "partial_source_leg_ids": sorted(
                str(proof["leg"].get("id"))
                for proof in submitted_legs
                if proof.get("partial_source") is True
            ),
            "partial_source_external_order_ids": sorted(
                str(proof["attempt"].get("external_order_id"))
                for proof in submitted_legs
                if proof.get("partial_source") is True
            ),
            "compensating_external_order_ids": sorted(
                str(proof["attempt"].get("external_order_id")) for proof, _quantity in linked_exits
            ),
            "terminal_zero_external_order_ids": sorted(terminal_zero_legs),
            "compensating_intent_ids": sorted(
                {
                    str(proof["leg"].get("intent_id"))
                    for proof, _quantity in linked_exits
                    if str(proof["leg"].get("intent_id", "")).strip()
                }
            ),
            "unsent_leg_ids": sorted(unsent_legs),
            "account_id": account.id,
            "book_id": str(intent.get("book_id") or ""),
        }
        self.repository.mark_compensated_partial_intent(
            intent_id,
            closure_metadata=proof_metadata,
            now=now,
            _resolution_capability=self.repository._resolution_capability(),
        )
        event_id = f"compensated-partial-intent:{intent_id}"
        if not any(event.get("id") == event_id for event in self.repository.operational_events(account.id, limit=1000)):
            self.repository.record_operational_event(
                event_id=event_id,
                account_id=account.id,
                event_type="COMPENSATED_PARTIAL_INTENT_CLOSED",
                mode=account.environment.value,
                outcome=IntentStatus.CANCELLED.value,
                occurred_at=now,
                summary="Closed a partial entry only after fresh flat-account and complete historical-order proof of exact compensation.",
                details={"intent_id": intent_id, "proof": proof_metadata},
            )
        return {
            "intent_id": intent_id,
            "account_id": account.id,
            "status": IntentStatus.CANCELLED.value,
            "proof": "fresh_flat_account_and_exact_compensating_exit",
            "filled_entry_external_order_ids": proof_metadata["filled_entry_external_order_ids"],
            "compensating_external_order_ids": proof_metadata["compensating_external_order_ids"],
            "terminal_zero_external_order_ids": proof_metadata["terminal_zero_external_order_ids"],
            "unsent_leg_ids": proof_metadata["unsent_leg_ids"],
        }

    def _verified_retired_intents_to_complete(
        self,
        *,
        account: Account,
        book_id: str,
        source_ledger_fingerprint: str,
    ) -> tuple[str, ...]:
        """Return only quarantined retired intents with complete fill proof.

        This is deliberately narrower than the retired-book closure: terminal
        rejected/cancelled outcomes remain untouched, while a quarantined
        imported intent is eligible only when its own provenance and every
        leg's single broker attempt prove a complete fill.  In particular,
        never-submitted intents in another book cannot enter this set.
        """
        candidates: list[str] = []
        for row in self.repository.book_intents(account.id, book_id=book_id):
            intent_id = str(row["id"])
            intent = self.repository.get_intent(intent_id)
            if intent is None:
                raise OMSExecutionError("verified retired intent is missing")
            metadata = intent.get("metadata")
            if (
                not isinstance(metadata, Mapping)
                or metadata.get("legacy_import") is not True
                or metadata.get("retired") is not True
                or str(metadata.get("source_ledger_fingerprint", "")) != source_ledger_fingerprint
                or str(intent.get("account_id", "")) != account.id
                or str(intent.get("book_id", "")) != book_id
            ):
                raise OMSExecutionError("retired intent provenance is not covered by the verified baseline")
            status = str(intent.get("status", ""))
            if status in {
                IntentStatus.FILLED.value,
                IntentStatus.COMPLETED.value,
                IntentStatus.REJECTED.value,
                IntentStatus.CANCELLED.value,
                IntentStatus.FAILED.value,
            }:
                continue
            if status != IntentStatus.RECONCILIATION_REQUIRED.value:
                raise OMSExecutionError(
                    f"retired intent {intent_id} is not terminal or quarantined: {status!r}"
                )
            if str(intent.get("action", "")) not in {
                IntentAction.ENTER.value,
                IntentAction.EXIT.value,
            }:
                raise OMSExecutionError("retired quarantined intent has an unsupported action")
            if not self._all_legs_have_fill_evidence(intent_id):
                raise OMSExecutionError(
                    f"retired quarantined intent {intent_id} lacks complete attempt-scoped fill evidence"
                )
            candidates.append(intent_id)
        return tuple(candidates)

    def _age_flags(
        self,
        intent: Mapping[str, object],
        attempt: Mapping[str, object],
        match: BrokerOrderSnapshot,
        now: datetime,
    ) -> tuple[bool, bool, dict[str, object]]:
        broker_order_time = self._parse_timestamp(match.order_time)
        anchor = (
            broker_order_time
            or self._parse_timestamp(attempt.get("submitted_at"))
            or self._parse_timestamp(attempt.get("updated_at"))
            or self._parse_timestamp(intent.get("created_at"))
        )
        timeout_seconds = self._policy_seconds(intent, "timeout_seconds", 300)
        stale_seconds = self._policy_seconds(intent, "stale_order_seconds", timeout_seconds)
        elapsed_seconds = max(0.0, (now - anchor).total_seconds()) if anchor is not None else None
        stale_anchor = (
            broker_order_time
            or self._parse_timestamp(attempt.get("updated_at"))
            or self._parse_timestamp(attempt.get("submitted_at"))
            or self._parse_timestamp(intent.get("created_at"))
        )
        stale_age_seconds = max(0.0, (now - stale_anchor).total_seconds()) if stale_anchor is not None else None
        timed_out = elapsed_seconds is not None and elapsed_seconds >= timeout_seconds
        stale = stale_age_seconds is not None and stale_age_seconds >= stale_seconds
        evidence = {
            "timeout_seconds": timeout_seconds,
            "stale_order_seconds": stale_seconds,
            "elapsed_seconds": elapsed_seconds,
            "stale_age_seconds": stale_age_seconds,
            "submitted_at": attempt.get("submitted_at"),
            "updated_at": attempt.get("updated_at"),
            "broker_order_time": match.order_time.isoformat() if match.order_time is not None else None,
            "stale_time_source": "broker_order_time" if broker_order_time is not None else "durable_attempt_time",
            "captured_at": match.captured_at.isoformat(),
        }
        return timed_out, stale, evidence

    def _finalize_intent_status(self, intent_id: str, account: Account) -> None:
        """Apply only conservative aggregate lifecycle transitions."""
        recovered = self._required_intent(intent_id)
        if (
            intent_id in self._closed_historical_intent_ids(account)
            and str(recovered.get("status")) in {
                IntentStatus.COMPLETED.value,
                IntentStatus.CANCELLED.value,
                IntentStatus.FILLED.value,
                IntentStatus.RECONCILIATION_REQUIRED.value,
            }
        ):
            # Proof-backed historical lifecycle is immutable under ordinary
            # polling/finalization.  Account-wide fresh-facts gates remain
            # blocking for new risk-bearing submissions.
            return
        statuses = {str(leg["status"]) for leg in recovered["legs"]}
        if not statuses:
            return
        open_issues = self.repository.open_reconciliation_issues(account.id)
        open_actions = self.repository.open_recovery_actions(account.id)
        terminal = {LegStatus.REJECTED.value, LegStatus.CANCELLED.value}
        if statuses & terminal:
            if statuses <= terminal:
                # A clean rejection/cancellation mix has no broker exposure;
                # choose the conservative aggregate terminal outcome below.
                # Clear only this exact intent's aggregate observation, then
                # re-evaluate all remaining blockers.
                self._resolve_reconciliation_issue(
                    account.id,
                    f"MIXED_TERMINAL_LEGS:INTENT:{intent_id}",
                    resolved_at=self._now(),
                )
                self._resolve_recovery_action(
                    account.id,
                    intent_id,
                    f"MIXED_TERMINAL_LEGS:{intent_id}",
                    resolved_at=self._now(),
                )
                open_issues = self.repository.open_reconciliation_issues(account.id)
                open_actions = self.repository.open_recovery_actions(account.id)
                if open_issues or open_actions:
                    if recovered["status"] != IntentStatus.RECONCILIATION_REQUIRED.value:
                        try:
                            self.repository.transition_intent(
                                intent_id,
                                IntentStatus.RECONCILIATION_REQUIRED,
                                now=self._now(),
                            )
                        except ValueError:
                            pass
                    return
                target = (
                    IntentStatus.REJECTED
                    if LegStatus.REJECTED.value in statuses
                    else IntentStatus.CANCELLED
                )
                if recovered["status"] != target.value:
                    try:
                        self.repository.transition_intent(intent_id, target, now=self._now())
                    except ValueError:
                        self._require_reconciliation(
                            intent_id,
                            account,
                            category="MIXED_TERMINAL_LEGS",
                            entity_type="INTENT",
                            entity_key=intent_id,
                            details={"leg_statuses": sorted(statuses)},
                        )
                return
            self._require_reconciliation(
                intent_id,
                account,
                category="MIXED_TERMINAL_LEGS",
                entity_type="INTENT",
                entity_key=intent_id,
                details={"leg_statuses": sorted(statuses)},
            )
            self._record_recovery_action(
                intent=recovered,
                account=account,
                action_key=f"MIXED_TERMINAL_LEGS:{intent_id}",
                state="RECONCILIATION_REQUIRED",
                summary="Terminal legs have mixed rejection/cancellation or residual exposure; operator reconciliation is required.",
                observed_positions=self._observe_positions(account)[0],
                remaining_quantities=self._remaining_quantities(recovered),
                allowed_next_steps=("refresh_broker_facts", "refresh_broker_fills", "reconcile_broker_position", "operator_review"),
                metadata={"leg_statuses": sorted(statuses)},
            )
            return
        if statuses & {LegStatus.RECONCILIATION_REQUIRED.value, LegStatus.PLANNED.value, LegStatus.SUBMITTING.value}:
            return
        # Hard account actions are blockers for lifecycle refresh.  A normal
        # WAIT/WORKING observation remains visible while an order is active,
        # but still blocks ``safe_to_submit`` and completion account-wide.
        # A terminal intent with complete, own fill evidence is not itself
        # re-opened merely because a sibling intent has an account-wide
        # blocker.  The blocker remains durable and continues to fail the
        # account submission gate; only evidence naming this intent (or one
        # of its own legs/attempts/fills) may demote its terminal lifecycle.
        # This matters for a verified partial EXIT: recovery must preserve
        # the completed entry while the EXIT/residual round-trip is being
        # reconciled, rather than rewriting historical ownership as if the
        # entry itself had become ambiguous.
        if recovered["status"] in {
            IntentStatus.COMPLETED.value,
        }:
            own_leg_ids = {str(leg.get("id")) for leg in recovered["legs"]}
            own_attempt_ids = {
                str(attempt.get("id"))
                for leg_id in own_leg_ids
                for attempt in self.repository.broker_orders_for_leg(leg_id)
            }
            own_external_order_ids = {
                str(attempt.get("external_order_id"))
                for leg_id in own_leg_ids
                for attempt in self.repository.broker_orders_for_leg(leg_id)
                if str(attempt.get("external_order_id") or "").strip()
            }
            own_fill_ids: set[str] = set()
            for attempt_id in own_attempt_ids:
                own_fill_ids.update(
                    str(fill.get("id"))
                    for fill in self.repository.fills_for_broker_order(attempt_id)
                )

            def issue_belongs_to_intent(issue: Mapping[str, object]) -> bool:
                details = self._issue_details(issue)
                if str(details.get("intent_id", "")) == intent_id:
                    return True
                entity_type = str(issue.get("entity_type", ""))
                entity_key = str(issue.get("entity_key", ""))
                if entity_type in {"INTENT", "ORDER_INTENT"} and entity_key == intent_id:
                    return True
                if entity_type in {"ORDER_LEG", "LEG"} and entity_key in own_leg_ids:
                    return True
                if entity_type in {"BROKER_ORDER", "ORDER"} and entity_key in own_attempt_ids:
                    return True
                if entity_type == "BROKER_FILL" and entity_key in own_fill_ids:
                    return True
                for key in (
                    "broker_order_id",
                    "order_leg_id",
                    "leg_id",
                    "fill_id",
                    "external_order_id",
                ):
                    value = str(details.get(key, ""))
                    if value and (
                        value in own_attempt_ids
                        or value in own_leg_ids
                        or value in own_fill_ids
                        or value in own_external_order_ids
                    ):
                        return True
                return False

            def action_belongs_to_intent(action: Mapping[str, object]) -> bool:
                if str(action.get("intent_id", "")) == intent_id:
                    return True
                metadata = action.get("metadata")
                if isinstance(metadata, Mapping) and str(metadata.get("intent_id", "")) == intent_id:
                    return True
                return False

            if not any(issue_belongs_to_intent(issue) for issue in open_issues) and not any(
                action_belongs_to_intent(action) for action in open_actions
            ):
                return
        blocking_actions = [
            action
            for action in open_actions
            if (
                str(action.get("state")) not in {"WORKING", "PARTIALLY_FILLED"}
                or str(action.get("intent_id")) != intent_id
            )
        ]
        if open_issues or blocking_actions:
            if recovered["status"] != IntentStatus.RECONCILIATION_REQUIRED.value:
                try:
                    self.repository.transition_intent(
                        intent_id,
                        IntentStatus.RECONCILIATION_REQUIRED,
                        now=self._now(),
                    )
                except ValueError:
                    pass
            return
        if statuses <= {LegStatus.WORKING.value, LegStatus.PARTIALLY_FILLED.value, LegStatus.FILLED.value}:
            if statuses == {LegStatus.FILLED.value}:
                # No open action may be silently carried across a terminal
                # FILLED promotion. Operators must explicitly resolve even a
                # stale wait/evidence action first.
                if open_actions:
                    if recovered["status"] != IntentStatus.RECONCILIATION_REQUIRED.value:
                        try:
                            self.repository.transition_intent(
                                intent_id,
                                IntentStatus.RECONCILIATION_REQUIRED,
                                now=self._now(),
                            )
                        except ValueError:
                            pass
                    return
                target = IntentStatus.FILLED
            elif LegStatus.FILLED.value in statuses or LegStatus.PARTIALLY_FILLED.value in statuses:
                target = IntentStatus.PARTIALLY_FILLED
            else:
                target = IntentStatus.WORKING
            if recovered["status"] != target.value:
                try:
                    self.repository.transition_intent(intent_id, target, now=self._now())
                except ValueError:
                    self._require_reconciliation(
                        intent_id,
                        account,
                        category="INTENT_STATE_CONFLICT",
                        entity_type="INTENT",
                        entity_key=intent_id,
                        details={"intent_status": recovered["status"], "target_status": target.value},
                    )

    def _record_account_identity_mismatch(
        self,
        *,
        intent: Mapping[str, object],
        supplied_account_id: str,
        observed_account_id: str | None = None,
        source: str,
    ) -> None:
        expected_account_id = str(intent["account_id"])
        details = {
            "intent_id": str(intent["id"]),
            "expected_account_id": expected_account_id,
            "supplied_account_id": supplied_account_id,
            "observed_account_id": observed_account_id,
            "source": source,
        }
        self._require_reconciliation_for_account_id(
            str(intent["id"]),
            expected_account_id,
            category="ACCOUNT_IDENTITY_MISMATCH",
            entity_type="ACCOUNT",
            entity_key=expected_account_id,
            details=details,
        )
        self._record_recovery_action_for_account_id(
            intent=intent,
            account_id=expected_account_id,
            action_key=f"ACCOUNT_IDENTITY_MISMATCH:{source}",
            state="RECONCILIATION_REQUIRED",
            summary="Supplied account identity does not match the durable intent/order account; no broker facts or allocations were accepted.",
            observed_positions={
                "_status": "ACCOUNT_IDENTITY_MISMATCH",
                "expected_account_id": expected_account_id,
                "supplied_account_id": supplied_account_id,
                "observed_account_id": observed_account_id,
            },
            remaining_quantities=self._remaining_quantities(intent),
            allowed_next_steps=("use_intent_account", "reconcile_broker_order", "operator_review"),
            metadata=details,
        )

    def _record_recovery_fact_failure(
        self,
        intent: Mapping[str, object],
        account: Account,
        error: Exception,
    ) -> None:
        """Durably block recovery without accepting any broker evidence."""
        intent_id = str(intent["id"])
        details = {
            "intent_id": intent_id,
            "account_id": account.id,
            "reason": "fresh authoritative recovery facts unavailable",
            "error": str(error),
        }
        self._require_reconciliation(
            intent_id,
            account,
            category="BROKER_FACT_UNAVAILABLE",
            entity_type="ACCOUNT",
            entity_key=account.id,
            details=details,
        )
        self._record_recovery_action(
            intent=intent,
            account=account,
            action_key=f"BROKER_FACT_UNAVAILABLE:{account.id}:RECOVERY",
            state="RECONCILIATION_REQUIRED",
            summary=(
                "Recovery requires a complete, account-scoped, fresh authoritative broker fact snapshot; "
                "no fills or lifecycle promotion were applied."
            ),
            observed_positions={"_status": "UNAVAILABLE", "_error": str(error)},
            remaining_quantities=self._remaining_quantities(intent),
            allowed_next_steps=("refresh_broker_facts", "reconcile_broker_position", "operator_review"),
            metadata=details,
        )

    def recover_intent(self, intent_id: str, *, account: Account) -> dict:
        """Recover only after one fresh strict account-fact snapshot."""
        self._startup_safety_audit()
        intent = self._required_intent(intent_id)
        if account.id != str(intent["account_id"]):
            return self._recover_intent_with_facts(intent_id, account=account)
        canonical = self._canonical_account_for_intent(
            intent,
            supplied=account,
            source="recover_intent",
        )
        if canonical is None:
            return self._required_intent(intent_id)
        account = canonical
        if not self._validate_persisted_execution_policy(intent, account, source="recover_intent"):
            return self._required_intent(intent_id)
        if intent_id in self._closed_historical_intent_ids(account):
            return self._required_intent(intent_id)
        try:
            context = self._validated_recovery_fact_context(account)
        except Exception as exc:
            self._record_recovery_fact_failure(intent, account, exc)
            return self._required_intent(intent_id)
        previous_context = self._recovery_fact_context
        self._recovery_fact_context = context
        try:
            return self._recover_intent_with_facts(intent_id, account=account)
        finally:
            self._recovery_fact_context = previous_context

    def _recover_intent_with_facts(self, intent_id: str, *, account: Account) -> dict:
        """Internal recovery body; caller must install a validated context."""
        self._startup_safety_audit()
        intent = self._required_intent(intent_id)
        if account.id != str(intent["account_id"]):
            self._record_account_identity_mismatch(
                intent=intent,
                supplied_account_id=account.id,
                source="recover_intent",
            )
            return self._required_intent(intent_id)
        canonical = self._canonical_account_for_intent(
            intent,
            supplied=account,
            source="recover_intent",
        )
        if canonical is None:
            return self._required_intent(intent_id)
        account = canonical
        if not self._validate_persisted_execution_policy(intent, account, source="recover_intent"):
            return self._required_intent(intent_id)
        if intent_id in self._closed_historical_intent_ids(account):
            # A proof-backed closed intent is not an active recovery target.
            # In particular, transient provider read failures or duplicated
            # historical snapshots must not demote its durable terminal state.
            return self._required_intent(intent_id)
        # A public restart-recovery call must materialize aged terminal and
        # account-wide unsupported-attempt blockers before it queries or
        # promotes any broker state.
        self._ensure_terminal_history_safety(account)
        self._quarantine_multiple_attempts(intent_id, account)
        now = self._now()
        context = self._recovery_fact_context
        if not isinstance(context, Mapping) or context.get("account_id") != account.id:
            error = OMSExecutionError("recovery is missing a validated account-fact context")
            self._record_recovery_fact_failure(intent, account, error)
            return self._required_intent(intent_id)
        observed_positions = dict(context["observed_positions"])
        open_orders = tuple(context["open_orders"])
        duplicate_order_blockers: list[dict[str, object]] = []

        if duplicate_order_blockers:
            details = {
                "intent_id": intent_id,
                "account_id": account.id,
                "blockers": duplicate_order_blockers,
            }
            fingerprint = hashlib.sha256(
                json.dumps(details, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")
            ).hexdigest()
            self._require_reconciliation(
                intent_id,
                account,
                category="BROKER_FACT_GATE_BLOCKED",
                entity_type="ACCOUNT",
                entity_key=account.id,
                details=details,
            )
            self._record_recovery_action(
                intent=self._required_intent(intent_id),
                account=account,
                action_key=f"BROKER_FACT_GATE:{account.id}:{fingerprint}",
                state="RECONCILIATION_REQUIRED",
                summary="Broker order facts contain contradictory duplicate rows; no lifecycle promotion or new submission is safe.",
                observed_positions=observed_positions,
                remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                allowed_next_steps=("refresh_broker_facts", "reconcile_broker_order", "operator_review"),
                metadata=details,
            )

        for leg in intent["legs"]:
            leg_attempts = self.repository.broker_orders_for_leg(str(leg["id"]))
            if len(leg_attempts) > 1:
                self._quarantine_multiple_attempts(intent_id, account)
                continue
            for attempt in leg_attempts:
                if str(attempt.get("account_id")) != account.id:
                    self._record_account_identity_mismatch(
                        intent=intent,
                        supplied_account_id=account.id,
                        observed_account_id=str(attempt.get("account_id")),
                        source=f"attempt:{attempt['id']}",
                    )
                    continue
                if self._enforce_unique_external_order_claim(
                    intent_id=intent_id,
                    account=account,
                    external_order_id=attempt.get("external_order_id"),
                    leg_id=str(leg["id"]),
                    source="poll_recovery",
                ):
                    continue
                terminal_attempt = attempt["status"] in {
                    BrokerOrderStatus.FILLED.value,
                    BrokerOrderStatus.REJECTED.value,
                    BrokerOrderStatus.CANCELLED.value,
                    BrokerOrderStatus.FAILED.value,
                }
                if attempt["status"] not in {
                    BrokerOrderStatus.PREPARED.value,
                    BrokerOrderStatus.SUBMITTING.value,
                    BrokerOrderStatus.UNKNOWN.value,
                    BrokerOrderStatus.WORKING.value,
                    BrokerOrderStatus.PARTIALLY_FILLED.value,
                } and not terminal_attempt and not self._attempt_is_recovery_relevant(intent, attempt, now):
                    continue
                if attempt["external_order_id"]:
                    matches = [item for item in open_orders if item.external_order_id == attempt["external_order_id"]]
                    if not matches:
                        try:
                            single = self.adapter.get_order(account, str(attempt["external_order_id"]))
                            if single is not None and not isinstance(single, BrokerOrderSnapshot):
                                raise TypeError("adapter get_order() must return BrokerOrderSnapshot or None")
                        except Exception as exc:
                            single = None
                            self._require_reconciliation(
                                intent_id,
                                account,
                                category="BROKER_QUERY_FAILED",
                                entity_type="BROKER_ORDER",
                                entity_key=str(attempt["id"]),
                                details={
                                    "message": str(exc),
                                    "retryable": bool(getattr(exc, "retryable", False)),
                                },
                            )
                            if bool(getattr(exc, "retryable", False)):
                                self._record_recovery_action(
                                    intent=self._required_intent(intent_id),
                                    account=account,
                                    action_key=f"POLL_ERROR:{account.id}:order-read",
                                    state="RECONCILIATION_REQUIRED",
                                    summary="Broker order polling was rate-limited after bounded retries; retry the recovery cycle later without resubmitting.",
                                    observed_positions=observed_positions,
                                    remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                                    allowed_next_steps=("refresh_broker_orders", "operator_review"),
                                    metadata={"error": str(exc), "retryable": True},
                                )
                                return self._required_intent(intent_id)
                        matches = [single] if single is not None else []
                else:
                    matches = [item for item in open_orders if item.client_order_id == attempt["client_order_id"]]
                if len(matches) != 1:
                    # A known no-submit rejection is already complete broker
                    # evidence. It may be revisited inside the bounded
                    # restart window, but must not become a false evidence
                    # blocker merely because no broker row exists.
                    if self._is_durable_no_submit_rejection(attempt, leg, open_orders):
                        continue
                    if terminal_attempt or self._attempt_is_recovery_relevant(intent, attempt, now):
                        self._mark_leg_reconciliation(str(leg["id"]), str(leg["status"]))
                        self._require_reconciliation(
                            intent_id,
                            account,
                            category="BROKER_TERMINAL_EVIDENCE_MISSING",
                            entity_type="BROKER_ORDER",
                            entity_key=str(attempt["id"]),
                            details={
                                "local_order_status": attempt["status"],
                                "match_count": len(matches),
                            },
                        )
                        self._record_recovery_action(
                            intent=intent,
                            account=account,
                            action_key=f"TERMINAL_ORDER_EVIDENCE:{attempt['id']}",
                            state="RECONCILIATION_REQUIRED",
                            summary="A recently terminal local order lacks unique broker evidence; operator reconciliation is required before submission.",
                            observed_positions=observed_positions,
                            remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                            allowed_next_steps=("refresh_broker_facts", "reconcile_broker_order", "operator_review"),
                            metadata={"broker_order_id": str(attempt["id"]), "match_count": len(matches)},
                        )
                        continue
                    if attempt["status"] != BrokerOrderStatus.UNKNOWN.value:
                        try:
                            self.repository.transition_broker_order(
                                str(attempt["id"]),
                                BrokerOrderStatus.UNKNOWN,
                                now=self._now(),
                            )
                        except (KeyError, ValueError, sqlite3.Error) as exc:
                            self._require_reconciliation(
                                intent_id,
                                account,
                                category="BROKER_STATE_CONFLICT",
                                entity_type="BROKER_ORDER",
                                entity_key=str(attempt["id"]),
                                details={"message": str(exc), "match_count": len(matches)},
                            )
                    if leg["status"] != LegStatus.RECONCILIATION_REQUIRED.value:
                        self.repository.transition_leg(
                            str(leg["id"]),
                            LegStatus.RECONCILIATION_REQUIRED,
                            now=self._now(),
                        )
                    self._record_recovery_action(
                        intent=intent,
                        account=account,
                        action_key=f"AMBIGUOUS_ORDER:{attempt['id']}",
                        state="RECONCILIATION_REQUIRED",
                        summary=(
                            f"Broker order evidence is ambiguous for leg {leg['id']} "
                            f"(matched {len(matches)} rows); do not submit, cancel, or hedge."
                        ),
                        observed_positions=observed_positions,
                        remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                        allowed_next_steps=("refresh_broker_facts", "reconcile_broker_position", "operator_review"),
                        metadata={"broker_order_id": str(attempt["id"]), "match_count": len(matches)},
                    )
                    self._require_reconciliation(
                        intent_id,
                        account,
                        category="AMBIGUOUS_ORDER_MATCH",
                        entity_type="BROKER_ORDER",
                        entity_key=str(attempt["id"]),
                        details={"match_count": len(matches)},
                    )
                    continue

                match = matches[0]
                # A provider order read can become terminal between the
                # initial account-fact snapshot and this per-order query.  Do
                # not let that raw snapshot create fill evidence: refresh the
                # strict account-fact context first, and continue only if the
                # refreshed facts authenticate the same cumulative quantity.
                if match.filled_quantity > 0 and (
                    self._recovery_context_fill_quantity(context, match.external_order_id)
                    != match.filled_quantity
                ):
                    try:
                        context = self._validated_recovery_fact_context(account)
                    except Exception as exc:
                        self._record_recovery_fact_failure(
                            self._required_intent(intent_id),
                            account,
                            exc,
                        )
                        return self._required_intent(intent_id)
                    self._recovery_fact_context = context
                    observed_positions = dict(context["observed_positions"])
                    open_orders = tuple(context["open_orders"])
                validation = self._validate_poll_snapshot(
                    account=account,
                    attempt=attempt,
                    leg=leg,
                    snapshot=match,
                )
                if not validation["valid"]:
                    self._record_poll_snapshot_failure(
                        intent=intent,
                        account=account,
                        leg=leg,
                        attempt=attempt,
                        observed_positions=observed_positions,
                        validation=validation,
                    )
                    continue
                target = match.status
                if target not in {
                    BrokerOrderStatus.WORKING,
                    BrokerOrderStatus.PARTIALLY_FILLED,
                    BrokerOrderStatus.FILLED,
                    BrokerOrderStatus.REJECTED,
                    BrokerOrderStatus.CANCELLED,
                }:
                    target = BrokerOrderStatus.UNKNOWN
                if attempt["status"] != target.value:
                    try:
                        self.repository.record_submission(
                            str(attempt["id"]),
                            status=target,
                            external_order_id=match.external_order_id,
                            metadata={
                                "recovered": True,
                                "snapshot_id": match.id,
                                "observed_filled_quantity": str(match.filled_quantity),
                            },
                            now=self._now(),
                            submitted_at=self._parse_timestamp(attempt.get("submitted_at"))
                            or self._parse_timestamp(attempt.get("updated_at")),
                        )
                    except (ValueError, sqlite3.Error) as exc:
                        self._mark_leg_reconciliation(str(leg["id"]), str(leg["status"]))
                        self._require_reconciliation(
                            intent_id,
                            account,
                            category="BROKER_STATE_CONFLICT",
                            entity_type="BROKER_ORDER",
                            entity_key=str(attempt["id"]),
                            details={"message": str(exc), "observed_status": target.value},
                        )
                        self._record_recovery_action(
                            intent=intent,
                            account=account,
                            action_key=f"STATE_CONFLICT:{attempt['id']}",
                            state="RECONCILIATION_REQUIRED",
                            summary="Broker order status conflicts with durable local state; operator reconciliation is required.",
                            observed_positions=observed_positions,
                            remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                            allowed_next_steps=("refresh_broker_facts", "reconcile_broker_order", "operator_review"),
                            metadata={"broker_order_id": str(attempt["id"]), "error": str(exc)},
                        )
                        continue

                current_leg = next(
                    item for item in self._required_intent(intent_id)["legs"] if item["id"] == leg["id"]
                )
                if target is BrokerOrderStatus.WORKING:
                    timed_out, stale, age = self._age_flags(intent, attempt, match, now)
                    if current_leg["status"] == LegStatus.PLANNED.value:
                        self.repository.transition_leg(str(leg["id"]), LegStatus.SUBMITTING, now=self._now())
                        self.repository.transition_leg(str(leg["id"]), LegStatus.WORKING, now=self._now())
                    elif current_leg["status"] == LegStatus.SUBMITTING.value:
                        self.repository.transition_leg(str(leg["id"]), LegStatus.WORKING, now=self._now())
                    if timed_out or stale:
                        if current_leg["status"] != LegStatus.RECONCILIATION_REQUIRED.value:
                            self.repository.transition_leg(
                                str(leg["id"]),
                                LegStatus.RECONCILIATION_REQUIRED,
                                now=self._now(),
                            )
                        state = "TIMED_OUT" if timed_out else "STALE"
                        category = "ORDER_TIMEOUT" if timed_out else "STALE_ORDER"
                        self._record_recovery_action(
                            intent=intent,
                            account=account,
                            action_key=f"{state}:{attempt['id']}",
                            state=state,
                            summary=(
                                f"Order {attempt['id']} remains WORKING after {state.lower()} detection; "
                                "wait for fresh broker truth and do not automatically retry, cancel, or hedge."
                            ),
                            observed_positions=observed_positions,
                            remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                            stale=stale,
                            timed_out=timed_out,
                            allowed_next_steps=("refresh_broker_facts", "reconcile_broker_position", "operator_review"),
                            metadata={"broker_order_id": str(attempt["id"]), **age},
                        )
                        self._require_reconciliation(
                            intent_id,
                            account,
                            category=category,
                            entity_type="BROKER_ORDER",
                            entity_key=str(attempt["id"]),
                            details={"broker_status": target.value, **age},
                        )
                    else:
                        self._record_recovery_action(
                            intent=intent,
                            account=account,
                            action_key=f"WAIT_FOR_BROKER:{attempt['id']}",
                            state="WORKING",
                            summary=(
                                f"Order {attempt['id']} is WORKING with "
                                f"{self._remaining_quantities(self._required_intent(intent_id)).get(str(leg['id']), 'unknown')} "
                                "remaining; poll again and do not submit a duplicate."
                            ),
                            observed_positions=observed_positions,
                            remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                            metadata={"broker_order_id": str(attempt["id"]), **age},
                        )
                elif target in {BrokerOrderStatus.PARTIALLY_FILLED, BrokerOrderStatus.FILLED}:
                    # Timeout/stale policy applies to working residuals.  A
                    # terminal FILLED snapshot whose cumulative quantity is
                    # exactly complete has no remaining broker exposure to
                    # age; reapplying the working-order clock on restart
                    # would quarantine an already authenticated fill and
                    # make a clean restart unsafe.  PARTIALLY_FILLED (and
                    # any terminal row that is not complete) remains subject
                    # to the normal fail-closed age checks below.
                    if target is BrokerOrderStatus.FILLED and match.filled_quantity == match.quantity:
                        timed_out, stale, age = False, False, {
                            "timeout_seconds": self._policy_seconds(intent, "timeout_seconds", 300),
                            "stale_order_seconds": self._policy_seconds(intent, "stale_order_seconds", 300),
                            "elapsed_seconds": None,
                            "stale_age_seconds": None,
                            "submitted_at": attempt.get("submitted_at"),
                            "updated_at": attempt.get("updated_at"),
                            "broker_order_time": (
                                match.order_time.isoformat() if match.order_time is not None else None
                            ),
                            "stale_time_source": "terminal_filled_no_residual",
                            "captured_at": match.captured_at.isoformat(),
                        }
                    else:
                        timed_out, stale, age = self._age_flags(intent, attempt, match, now)
                    if timed_out or stale:
                        self._mark_leg_reconciliation(str(leg["id"]), str(current_leg["status"]))
                        state = (
                            "PARTIAL_TIMED_OUT"
                            if target is BrokerOrderStatus.PARTIALLY_FILLED and timed_out
                            else "PARTIAL_STALE"
                            if target is BrokerOrderStatus.PARTIALLY_FILLED
                            else "TIMED_OUT"
                            if timed_out
                            else "STALE"
                        )
                        category = "ORDER_TIMEOUT" if timed_out else "STALE_ORDER"
                        self._record_recovery_action(
                            intent=intent,
                            account=account,
                            action_key=f"{state}:{attempt['id']}",
                            state=state,
                            summary=(
                                f"Order {attempt['id']} has a {target.value} residual after {state.lower()} detection; "
                                "wait for fresh broker truth and do not automatically retry, cancel, or hedge."
                            ),
                            observed_positions=observed_positions,
                            remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                            stale=stale,
                            timed_out=timed_out,
                            allowed_next_steps=("refresh_broker_facts", "refresh_broker_fills", "reconcile_broker_position", "operator_review"),
                            metadata={"broker_order_id": str(attempt["id"]), **age},
                        )
                        self._require_reconciliation(
                            intent_id,
                            account,
                            category=category,
                            entity_type="BROKER_ORDER",
                            entity_key=str(attempt["id"]),
                            details={"broker_status": target.value, **age},
                        )
                    elif target is BrokerOrderStatus.PARTIALLY_FILLED and match.filled_quantity > 0:
                        if current_leg["status"] not in {
                            LegStatus.PARTIALLY_FILLED.value,
                            LegStatus.FILLED.value,
                        }:
                            self._mark_leg_reconciliation(str(leg["id"]), str(current_leg["status"]))
                        self._record_recovery_action(
                            intent=intent,
                            account=account,
                            action_key=f"WAIT_FOR_BROKER:{attempt['id']}",
                            state="PARTIALLY_FILLED",
                            summary=(
                                f"Order {attempt['id']} is PARTIALLY_FILLED with "
                                f"{self._remaining_quantities(self._required_intent(intent_id)).get(str(leg['id']), 'unknown')} "
                                "remaining; poll again and do not submit a duplicate."
                            ),
                            observed_positions=observed_positions,
                            remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                            metadata={"broker_order_id": str(attempt["id"]), **age},
                        )
                    elif target is BrokerOrderStatus.FILLED and current_leg["status"] == LegStatus.FILLED.value:
                        self._resolve_recovery_actions_for_order(
                            account,
                            intent_id,
                            str(attempt["id"]),
                            action_prefixes=("WAIT_FOR_BROKER:", "FILL_EVIDENCE:"),
                        )
                        self._resolve_reconciliation_issue(
                            account.id,
                            f"FILL_EVIDENCE_REQUIRED:BROKER_ORDER:{attempt['id']}",
                            resolved_at=self._now(),
                        )
                    else:
                        self._mark_leg_reconciliation(str(leg["id"]), str(current_leg["status"]))
                        self._record_recovery_action(
                            intent=intent,
                            account=account,
                            action_key=f"FILL_EVIDENCE:{attempt['id']}",
                            state="PARTIALLY_FILLED" if target is BrokerOrderStatus.PARTIALLY_FILLED else "FILLED_PENDING_EVIDENCE",
                            summary=(
                                f"Broker reports {target.value} for order {attempt['id']}, but normalized fill evidence "
                                "is required before position ownership changes."
                            ),
                            observed_positions=observed_positions,
                            remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                            allowed_next_steps=("refresh_broker_fills", "reconcile_broker_position", "operator_review"),
                            metadata={
                                "broker_order_id": str(attempt["id"]),
                                "broker_status": target.value,
                                "observed_filled_quantity": str(match.filled_quantity),
                            },
                        )
                        self._require_reconciliation(
                            intent_id,
                            account,
                            category="FILL_EVIDENCE_REQUIRED",
                            entity_type="BROKER_ORDER",
                            entity_key=str(attempt["id"]),
                            details={"broker_status": target.value, "observed_filled_quantity": str(match.filled_quantity)},
                        )
                elif target in {BrokerOrderStatus.REJECTED, BrokerOrderStatus.CANCELLED}:
                    try:
                        cumulative = Decimal(str(current_leg["cumulative_filled_quantity"]))
                    except (InvalidOperation, TypeError, ValueError):
                        cumulative = Decimal("-1")
                    if cumulative > 0:
                        if current_leg["status"] != LegStatus.RECONCILIATION_REQUIRED.value:
                            self._mark_leg_reconciliation(str(leg["id"]), str(current_leg["status"]))
                        self._record_recovery_action(
                            intent=intent,
                            account=account,
                            action_key=f"TERMINAL_PARTIAL:{attempt['id']}",
                            state="RECONCILIATION_REQUIRED",
                            summary=(
                                f"Order {attempt['id']} is {target.value} with durable positive fills; residual exposure "
                                "requires reconciliation before any further action."
                            ),
                            observed_positions=observed_positions,
                            remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                            allowed_next_steps=("refresh_broker_facts", "refresh_broker_fills", "reconcile_broker_position", "operator_review"),
                            metadata={
                                "broker_order_id": str(attempt["id"]),
                                "broker_status": target.value,
                                "durable_filled_quantity": str(cumulative),
                            },
                        )
                        self._require_reconciliation(
                            intent_id,
                            account,
                            category="FILL_EVIDENCE_REQUIRED",
                            entity_type="BROKER_ORDER",
                            entity_key=str(attempt["id"]),
                            details={"broker_status": target.value, "durable_filled_quantity": str(cumulative)},
                        )
                    elif match.filled_quantity > 0:
                        self._mark_leg_reconciliation(str(leg["id"]), str(current_leg["status"]))
                        self._record_recovery_action(
                            intent=intent,
                            account=account,
                            action_key=f"TERMINAL_PARTIAL:{attempt['id']}",
                            state="RECONCILIATION_REQUIRED",
                            summary=(
                                f"Order {attempt['id']} is {target.value} after a positive fill; residual exposure "
                                "requires reconciliation before any further action."
                            ),
                            observed_positions=observed_positions,
                            remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                            allowed_next_steps=("refresh_broker_facts", "refresh_broker_fills", "reconcile_broker_position", "operator_review"),
                            metadata={"broker_order_id": str(attempt["id"]), "filled_quantity": str(match.filled_quantity)},
                        )
                        self._require_reconciliation(
                            intent_id,
                            account,
                            category="TERMINAL_PARTIAL_FILL",
                            entity_type="BROKER_ORDER",
                            entity_key=str(attempt["id"]),
                            details={"broker_status": target.value, "filled_quantity": str(match.filled_quantity)},
                        )
                    elif current_leg["status"] not in {
                        LegStatus.RECONCILIATION_REQUIRED.value,
                        LegStatus.FILLED.value,
                    }:
                        leg_target = LegStatus.REJECTED if target is BrokerOrderStatus.REJECTED else LegStatus.CANCELLED
                        self._set_leg_terminal(
                            str(leg["id"]),
                            str(current_leg["status"]),
                            leg_target,
                        )
                else:
                    if current_leg["status"] != LegStatus.RECONCILIATION_REQUIRED.value:
                        self._mark_leg_reconciliation(str(leg["id"]), str(current_leg["status"]))
                    self._record_recovery_action(
                        intent=intent,
                        account=account,
                        action_key=f"UNKNOWN_ORDER:{attempt['id']}",
                        state="RECONCILIATION_REQUIRED",
                        summary="Broker order state is UNKNOWN; no retry, cancel, hedge, or position inference is allowed.",
                        observed_positions=observed_positions,
                        remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                        allowed_next_steps=("refresh_broker_facts", "reconcile_broker_order", "operator_review"),
                        metadata={"broker_order_id": str(attempt["id"])},
                    )
                    self._require_reconciliation(
                        intent_id,
                        account,
                        category="UNKNOWN_BROKER_ORDER_STATE",
                        entity_type="BROKER_ORDER",
                        entity_key=str(attempt["id"]),
                        details={"broker_status": target.value},
                    )

        # A clean provider rejection is already complete broker evidence: no
        # external order ID, no fill quantity, and no matching open order.  Do
        # not ask a broker for account-wide deal history in that case.
        clean_rejection = self._restore_definite_rejection(intent_id, account, open_orders)
        if self._intent_needs_fill_recovery(intent_id, open_orders) and not clean_rejection:
            self._recover_fills(
                intent_id,
                account,
                broker_fills=tuple(context["fills"]),
            )
        self._resolve_snapshot_fill_evidence(account, intent_id)

        # A persisted definite no-submit rejection may intentionally leave
        # later, never-submitted legs PLANNED.  Do not reinterpret that safe
        # terminal outcome as a mixed terminal-leg exposure.
        if not clean_rejection:
            self._finalize_intent_status(intent_id, account)
        return self._required_intent(intent_id)

    def _restore_definite_rejection(
        self,
        intent_id: str,
        account: Account,
        open_orders: tuple[BrokerOrderSnapshot, ...],
    ) -> bool:
        """Restore a persisted clean no-submit rejection without broad cleanup."""
        intent = self._required_intent(intent_id)
        if intent["status"] not in {
            IntentStatus.REJECTED.value,
            IntentStatus.RECONCILIATION_REQUIRED.value,
        }:
            return False
        rejected_legs: list[dict] = []
        for leg in intent["legs"]:
            attempts = self.repository.broker_orders_for_leg(str(leg["id"]))
            if not attempts:
                if leg["status"] == LegStatus.PLANNED.value:
                    continue
                return False
            if leg["status"] not in {
                LegStatus.REJECTED.value,
                LegStatus.RECONCILIATION_REQUIRED.value,
            }:
                return False
            if not all(self._is_durable_no_submit_rejection(attempt, leg, open_orders) for attempt in attempts):
                return False
            if leg["status"] == LegStatus.RECONCILIATION_REQUIRED.value:
                self.repository.transition_leg(str(leg["id"]), LegStatus.REJECTED, now=self._now())
            rejected_legs.append(leg)

        if not rejected_legs:
            return False

        recovered = self._required_intent(intent_id)
        if any(
            leg["status"] not in {LegStatus.REJECTED.value, LegStatus.PLANNED.value}
            for leg in recovered["legs"]
        ):
            return False
        if any(
            leg["status"] == LegStatus.PLANNED.value
            and self.repository.broker_orders_for_leg(str(leg["id"]))
            for leg in recovered["legs"]
        ):
            return False

        for leg in rejected_legs:
            # Resolve only the exact leg issue that named this rejected leg;
            # unrelated account or order issues remain sticky.
            self._resolve_owned_reconciliation_issue(
                account.id,
                intent_id,
                f"INCOMPLETE_INTENT:ORDER_LEG:{leg['id']}",
                entity_type="ORDER_LEG",
                entity_key=str(leg["id"]),
            )

            # Older recovery runs could have created a terminal-history
            # blocker before this rejection was recognized as a definite
            # no-submit.  Resolve only the exact owned order/action pair now
            # that the strict no-submit predicate has passed; preserve the
            # durable issue row and leave unrelated blockers untouched.
            for attempt in self.repository.broker_orders_for_leg(str(leg["id"])):
                if not self._is_durable_no_submit_rejection(attempt, leg, open_orders):
                    continue
                broker_order_id = str(attempt["id"])
                self._resolve_owned_reconciliation_issue(
                    account.id,
                    intent_id,
                    f"TERMINAL_HISTORY_EXPIRED:BROKER_ORDER:{broker_order_id}",
                    entity_type="BROKER_ORDER",
                    entity_key=broker_order_id,
                )
                self._resolve_recovery_action(
                    account.id,
                    intent_id,
                    f"TERMINAL_HISTORY_EXPIRED:{broker_order_id}",
                    resolved_at=self._now(),
                )

        # Older generic smoke runs created this account/fills issue while
        # polling a known no-submit rejection.  The key is intentionally
        # narrow and must carry this intent's ownership evidence.
        self._resolve_owned_reconciliation_issue(
            account.id,
            intent_id,
            f"BROKER_QUERY_FAILED:ACCOUNT:{account.id}:fills",
            entity_type="ACCOUNT",
            entity_key=f"{account.id}:fills",
            require_intent_detail=True,
        )
        if recovered["status"] != IntentStatus.REJECTED.value:
            self.repository.transition_intent(intent_id, IntentStatus.REJECTED, now=self._now())
        return True

    @staticmethod
    def _issue_details(issue: Mapping[str, object]) -> dict[str, object]:
        details = issue.get("details_json", {})
        if isinstance(details, str):
            try:
                parsed = json.loads(details)
            except (TypeError, ValueError):
                return {}
            return parsed if isinstance(parsed, dict) else {}
        return details if isinstance(details, dict) else {}

    def _resolve_reconciliation_issue(
        self,
        account_id: str,
        issue_key: str,
        *,
        resolved_at: datetime | None = None,
    ) -> bool:
        """Resolve only through this OMS's validated private capability."""
        return self.repository.resolve_reconciliation_issue_by_key(
            account_id,
            issue_key,
            resolved_at=resolved_at,
            _resolution_capability=self.repository._resolution_capability(),
        )

    def _resolve_recovery_action(
        self,
        account_id: str,
        intent_id: str,
        action_key: str,
        *,
        resolved_at: datetime | None = None,
    ) -> bool:
        """Resolve an action only after the caller has validated its evidence."""
        return self.repository.resolve_recovery_action_by_key(
            account_id,
            intent_id,
            action_key,
            resolved_at=resolved_at,
            _resolution_capability=self.repository._resolution_capability(),
        )

    def _resolve_owned_reconciliation_issue(
        self,
        account_id: str,
        intent_id: str,
        issue_key: str,
        *,
        entity_type: str,
        entity_key: str,
        require_intent_detail: bool = False,
    ) -> bool:
        for issue in self.repository.open_reconciliation_issues(account_id):
            if issue.get("issue_key") != issue_key:
                continue
            if issue.get("entity_type") != entity_type or issue.get("entity_key") != entity_key:
                continue
            details = self._issue_details(issue)
            if require_intent_detail and details.get("intent_id") != intent_id:
                continue
            if details.get("intent_id") not in (None, intent_id):
                continue
            return self._resolve_reconciliation_issue(account_id, issue_key, resolved_at=self._now())
        return False

    @classmethod
    def _is_durable_no_submit_rejection(
        cls,
        attempt: dict,
        leg: dict,
        open_orders: tuple[BrokerOrderSnapshot, ...],
    ) -> bool:
        if attempt["status"] != BrokerOrderStatus.REJECTED.value:
            return False
        if attempt["external_order_id"]:
            return False
        try:
            if Decimal(str(leg["cumulative_filled_quantity"])) != 0:
                return False
        except (InvalidOperation, TypeError, ValueError):
            return False
        metadata = cls._attempt_metadata(attempt)
        if str(metadata.get("provider_status", "")).upper() != BrokerOrderStatus.REJECTED.value:
            return False
        if metadata.get("accepted") not in (None, False):
            return False
        if metadata.get("ambiguous") is True:
            return False
        if metadata.get("definite_no_submit") is not True:
            return False
        if metadata.get("no_submit_asserted") is not True:
            return False
        if metadata.get("no_fill_asserted") is not True:
            return False
        if metadata.get("cumulative_fill_known") is not True:
            return False
        try:
            if Decimal(str(metadata.get("reported_cumulative_fill"))) != 0:
                return False
        except (InvalidOperation, TypeError, ValueError):
            return False
        if cls._payload_has_submission_evidence(metadata.get("raw_payload", {})):
            return False
        if any(
            attempt["client_order_id"]
            and item.client_order_id == attempt["client_order_id"]
            for item in open_orders
        ):
            return False
        return True

    def _attempt_has_terminal_zero_fill_evidence(
        self,
        attempt: Mapping[str, object],
        leg: Mapping[str, object],
    ) -> bool:
        """Prove an attempted terminal leg never filled after a restart.

        A zero cumulative quantity is not evidence by itself.  For an
        attempted broker order the durable row must carry the adapter's
        authenticated terminal no-fill envelope that was validated before the
        cancel/rejection was persisted.  This is intentionally separate from
        ``_is_durable_no_submit_rejection`` because a provider may have created
        and then cancelled an order with an external identity.
        """
        status = str(attempt.get("status", ""))
        if status not in {
            BrokerOrderStatus.CANCELLED.value,
            BrokerOrderStatus.REJECTED.value,
            BrokerOrderStatus.FAILED.value,
        }:
            return False
        try:
            if Decimal(str(leg.get("cumulative_filled_quantity", "0"))) != 0:
                return False
            if self._durable_filled_quantity(str(attempt["id"])) != 0:
                return False
            submitted_quantity = Decimal(str(attempt["submitted_quantity"]))
            expected_quantity = Decimal(str(leg["quantity"]))
        except (KeyError, InvalidOperation, TypeError, ValueError, sqlite3.Error):
            return False
        if submitted_quantity <= 0 or submitted_quantity != expected_quantity:
            return False
        if not str(attempt.get("external_order_id") or "").strip():
            return False
        metadata = self._attempt_metadata(dict(attempt))
        if metadata.get("terminal_zero_fill_proof") is not True:
            return False
        if metadata.get("ambiguous") is True or metadata.get("no_fill_asserted") is not True:
            return False
        if metadata.get("cumulative_fill_known") is not True:
            return False
        try:
            if Decimal(str(metadata.get("reported_cumulative_fill"))) != 0:
                return False
        except (InvalidOperation, TypeError, ValueError):
            return False
        if metadata.get("authority") != ADAPTER_ORDER_SNAPSHOT_AUTHORITY:
            return False
        instrument_id = str(leg.get("instrument_id") or "").strip()
        if metadata.get("submitted_quantity") not in (None, ""):
            try:
                if Decimal(str(metadata["submitted_quantity"])) != expected_quantity:
                    return False
            except (InvalidOperation, TypeError, ValueError):
                return False
        if metadata.get("instrument_id") not in (None, instrument_id):
            return False
        raw_payload = metadata.get("raw_payload")
        if not isinstance(raw_payload, Mapping):
            return False
        try:
            result = BrokerSubmissionResult(
                broker_order_id=str(attempt["id"]),
                accepted=metadata.get("accepted") if isinstance(metadata.get("accepted"), bool) else None,
                status=BrokerOrderStatus(status),
                external_order_id=str(attempt["external_order_id"]),
                cumulative_filled_quantity=Decimal("0"),
                raw_payload=raw_payload,
                no_fill_asserted=True,
                authority=ADAPTER_ORDER_SNAPSHOT_AUTHORITY,
                submitted_quantity=expected_quantity,
                instrument_id=instrument_id,
            )
        except (TypeError, ValueError, InvalidOperation):
            return False
        return not self._has_cancel_evidence(
            result,
            expected_quantity=expected_quantity,
            expected_instrument_id=instrument_id,
        )

    @staticmethod
    def _attempt_metadata(attempt: dict) -> dict:
        value = attempt.get("metadata_json", {})
        if isinstance(value, dict):
            return value
        if isinstance(value, str) and value:
            try:
                parsed = json.loads(value)
            except (TypeError, ValueError):
                return {}
            return parsed if isinstance(parsed, dict) else {}
        return {}

    def _intent_needs_fill_recovery(
        self,
        intent_id: str,
        open_orders: tuple[BrokerOrderSnapshot, ...],
    ) -> bool:
        intent = self._required_intent(intent_id)
        for leg in intent["legs"]:
            status = leg["status"]
            if status not in {LegStatus.PLANNED.value, LegStatus.REJECTED.value}:
                return True
            if self.repository.fills_for_leg(str(leg["id"])):
                return True
            attempts = self.repository.broker_orders_for_leg(str(leg["id"]))
            for attempt in attempts:
                if attempt["external_order_id"] or attempt["status"] != BrokerOrderStatus.REJECTED.value:
                    return True
                if not self._is_durable_no_submit_rejection(attempt, leg, open_orders):
                    return True
        return False

    def _resolve_exact_sibling_fill_blockers(
        self,
        *,
        account: Account,
        broker_fill: BrokerFill,
        matched_attempt_id: str,
    ) -> None:
        """Close a sibling-feed blocker only after exact durable attribution.

        Account-scoped fill feeds are visible while each intent is recovered
        independently.  The first intent can therefore record a temporary
        sibling blocker before the owning intent has persisted its own fill.
        Once that exact order/fill is durable, clear only the matching
        sibling issue/action; foreign, duplicate, or changed evidence remains
        sticky.
        """
        try:
            claims = self.repository.broker_orders_for_external_order_id(
                broker_fill.external_order_id
            )
        except sqlite3.Error:
            return
        if len(claims) != 1 or str(claims[0].get("id")) != str(matched_attempt_id):
            return
        affected_intents: set[str] = set()
        for issue in self.repository.open_reconciliation_issues(account.id):
            if issue.get("category") != "SIBLING_INTENT_BROKER_FILL_UNMATCHED":
                continue
            details = self._issue_details(issue)
            if (
                str(details.get("external_order_id", "")) != broker_fill.external_order_id
                or str(issue.get("entity_key", "")) != broker_fill.dedupe_key
            ):
                continue
            owner = str(issue.get("intent_id", "")).strip()
            try:
                if self._resolve_reconciliation_issue(
                    account.id,
                    str(issue["issue_key"]),
                    resolved_at=self._now(),
                ):
                    if owner:
                        affected_intents.add(owner)
            except (KeyError, ValueError, sqlite3.Error):
                # The original blocker remains durable if its exact row
                # cannot be resolved; never turn recovery into a raw error.
                continue
        expected_action_key = f"SIBLING_INTENT_FILL:{broker_fill.dedupe_key}"
        for action in self.repository.open_recovery_actions(account.id):
            if str(action.get("action_key", "")) != expected_action_key:
                continue
            metadata = action.get("metadata")
            if not isinstance(metadata, Mapping):
                continue
            if str(metadata.get("external_order_id", "")) != broker_fill.external_order_id:
                continue
            owner = str(action.get("intent_id", "")).strip()
            try:
                if self._resolve_recovery_action(
                    account.id,
                    owner,
                    expected_action_key,
                    resolved_at=self._now(),
                ) and owner:
                    affected_intents.add(owner)
            except (KeyError, ValueError, sqlite3.Error):
                continue
        for owner in affected_intents:
            try:
                owner_intent = self.repository.get_intent(owner)
                if owner_intent is not None and str(owner_intent.get("account_id")) == account.id:
                    self._finalize_intent_status(owner, account)
            except (KeyError, ValueError, sqlite3.Error):
                # Any remaining lifecycle conflict stays durable and visible
                # to the next recovery/status call.
                continue

    def _recover_fills(
        self,
        intent_id: str,
        account: Account,
        *,
        broker_fills: Sequence[BrokerFill],
    ) -> None:
        """Apply broker deal facts to known attempts; never infer fills from status."""
        intent = self._required_intent(intent_id)
        if account.id != str(intent["account_id"]):
            self._record_account_identity_mismatch(
                intent=intent,
                supplied_account_id=account.id,
                source="recover_fills",
            )
            return
        canonical = self._canonical_account_for_intent(
            intent,
            supplied=account,
            source="recover_fills",
        )
        if canonical is None:
            return
        account = canonical
        # Fill history is another ingestion path.  Do not let it aggregate
        # evidence across replacement attempts when the Stage 3 policy only
        # supports one attempt per logical leg.
        if self._quarantine_multiple_attempts(intent_id, account):
            return
        if any(not isinstance(item, BrokerFill) for item in broker_fills):
            self._record_recovery_fact_failure(
                self._required_intent(intent_id),
                account,
                OMSExecutionError("validated account facts contained a non-normalized fill"),
            )
            return

        intent = self._required_intent(intent_id)
        attempts = [
            attempt
            for leg in intent["legs"]
            for attempt in self.repository.broker_orders_for_leg(str(leg["id"]))
        ]
        terminal_snapshot_facts: dict[str, Mapping[str, object]] = {}
        for issue in self.repository.open_reconciliation_issues(account.id):
            if issue.get("category") != "BROKER_SNAPSHOT_MISMATCH":
                continue
            details = self._issue_details(issue)
            status = str(details.get("snapshot_status", ""))
            if status not in {
                BrokerOrderStatus.REJECTED.value,
                BrokerOrderStatus.CANCELLED.value,
            }:
                continue
            try:
                snapshot_quantity = Decimal(str(details.get("snapshot_filled_quantity", "-1")))
            except (InvalidOperation, TypeError, ValueError):
                continue
            if snapshot_quantity == 0 and issue.get("entity_type") == "BROKER_ORDER":
                terminal_snapshot_facts[str(issue.get("entity_key"))] = details
        for broker_fill in broker_fills:
            # A normalized fill timestamp is broker evidence, not a query
            # timestamp.  Reject old facts before any ownership matching or
            # repository allocation can occur.
            candidate_attempt = next(
                (
                    item
                    for item in attempts
                    if item.get("external_order_id") == broker_fill.external_order_id
                ),
                None,
            )
            if self._enforce_unique_external_order_claim(
                intent_id=intent_id,
                account=account,
                external_order_id=broker_fill.external_order_id,
                leg_id=str(candidate_attempt["order_leg_id"]) if candidate_attempt is not None else None,
                source="fill_recovery",
            ):
                continue
            matching_attempts = [
                item for item in attempts if item.get("external_order_id") == broker_fill.external_order_id
            ]
            if len(matching_attempts) == 1:
                temporal_mismatches = self._temporal_fill_mismatches(matching_attempts[0], broker_fill)
                if temporal_mismatches:
                    self._record_fill_temporal_conflict(
                        intent_id=intent_id,
                        account=account,
                        broker_fill=broker_fill,
                        mismatches=temporal_mismatches,
                    )
                    continue
            fill_alias_mismatches = self._account_alias_mismatches(
                account,
                account_id=broker_fill.account_id,
                metadata=broker_fill.metadata,
            )
            if fill_alias_mismatches:
                self._record_account_identity_mismatch(
                    intent=self._required_intent(intent_id),
                    supplied_account_id=account.id,
                    observed_account_id=self._fill_account_id(broker_fill) or fill_alias_mismatches[0],
                    source=f"fill-alias:{broker_fill.dedupe_key}",
                )
                self._require_reconciliation(
                    intent_id,
                    account,
                    category="BROKER_FILL_ACCOUNT_ID_CONFLICT",
                    entity_type="BROKER_FILL",
                    entity_key=broker_fill.dedupe_key,
                    details={"mismatches": fill_alias_mismatches, "external_order_id": broker_fill.external_order_id},
                )
                self._record_recovery_action(
                    intent=self._required_intent(intent_id),
                    account=account,
                    action_key=f"FILL_ACCOUNT_ID_CONFLICT:{broker_fill.dedupe_key}",
                    state="RECONCILIATION_REQUIRED",
                    summary="Broker fill carried conflicting account aliases; no allocation was accepted.",
                    observed_positions=self._observe_positions(account)[0],
                    remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                    allowed_next_steps=("refresh_broker_fills", "reconcile_broker_order", "operator_review"),
                    metadata={"mismatches": fill_alias_mismatches, "external_order_id": broker_fill.external_order_id},
                )
                continue
            observed_fill_account_id = self._fill_account_id(broker_fill)
            if observed_fill_account_id is not None and observed_fill_account_id not in {
                account.id,
                account.external_account_id,
            }:
                self._record_account_identity_mismatch(
                    intent=intent,
                    supplied_account_id=account.id,
                    observed_account_id=observed_fill_account_id,
                    source=f"fill:{broker_fill.dedupe_key}",
                )
                continue
            try:
                foreign_claims = [
                    item
                    for item in self.repository.broker_orders_for_external_order_id(
                        broker_fill.external_order_id
                    )
                    if str(item.get("account_id")) != account.id
                ]
            except sqlite3.Error as exc:
                self._require_reconciliation(
                    intent_id,
                    account,
                    category="AMBIGUOUS_FILL_MATCH",
                    entity_type="BROKER_FILL",
                    entity_key=broker_fill.dedupe_key,
                    details={"message": str(exc), "external_order_id": broker_fill.external_order_id},
                )
                continue
            if foreign_claims:
                self._record_account_identity_mismatch(
                    intent=intent,
                    supplied_account_id=account.id,
                    observed_account_id=str(foreign_claims[0].get("account_id")),
                    source=f"event-fill-external-id:{broker_fill.external_order_id}:{broker_fill.dedupe_key}",
                )
                continue
            try:
                external_claims = self.repository.broker_orders_for_external_order_id(
                    broker_fill.external_order_id
                )
            except sqlite3.Error as exc:
                self._require_reconciliation(
                    intent_id,
                    account,
                    category="BROKER_FILL_RECORD_FAILED",
                    entity_type="BROKER_FILL",
                    entity_key=broker_fill.dedupe_key,
                    details={"external_order_id": broker_fill.external_order_id, "message": str(exc)},
                )
                continue
            foreign_claims = [
                item for item in external_claims if str(item.get("account_id")) != account.id
            ]
            if foreign_claims:
                self._record_account_identity_mismatch(
                    intent=intent,
                    supplied_account_id=account.id,
                    observed_account_id=str(foreign_claims[0].get("account_id")),
                    source=f"fill-external-id:{broker_fill.external_order_id}:{broker_fill.dedupe_key}",
                )
                continue
            matches = [
                attempt
                for attempt in attempts
                if attempt["external_order_id"] == broker_fill.external_order_id
            ]
            for matched_attempt in matches:
                if str(matched_attempt.get("account_id")) != account.id:
                    self._record_account_identity_mismatch(
                        intent=intent,
                        supplied_account_id=account.id,
                        observed_account_id=str(matched_attempt.get("account_id")),
                        source=f"fill-attempt:{matched_attempt['id']}:{broker_fill.dedupe_key}",
                    )
                    matches = []
                    break
            if not matches and observed_fill_account_id is not None and observed_fill_account_id not in {
                account.id,
                account.external_account_id,
            }:
                # The payload-level account mismatch above already recorded
                # the durable blocker; do not attempt external-ID matching.
                continue
            if not matches:
                # The account-scoped fill feed also repeats provider fills
                # from proof-backed retired/compensated rounds.  Use the same
                # durable ownership predicate as the account-fact gate before
                # classifying a row as new unmatched exposure.  This keeps
                # historical visibility while avoiding a false sibling/unknown
                # blocker for a closed, verified round.  Anything not covered
                # by that exact proof remains fail-closed below.
                if self.is_proof_backed_closed_broker_fill(
                    account=account,
                    broker_fill=broker_fill,
                ):
                    continue
                # Fill history is account-scoped.  An external order that
                # cannot be attributed to this intent is never silently
                # skipped.  A same-account sibling claim is especially
                # important: it may be a fill for another managed intent and
                # must block account-wide submission until attributed.
                sibling_claims = [
                    item
                    for item in external_claims
                    if str(item.get("account_id")) == account.id
                    and self.repository.intent_id_for_broker_order(str(item.get("id"))) not in {None, intent_id}
                ]
                # A broker fill feed is account-scoped and commonly repeats
                # already-recorded fills from a completed sibling intent.  A
                # durable exact dedupe on that claimed attempt is known
                # evidence, not an unmatched exposure.  A new/changed fill on
                # the sibling remains an account-wide blocker below.
                def sibling_fill_is_exact(item_fill: Mapping[str, object]) -> bool:
                    if str(item_fill.get("dedupe_key")) != broker_fill.dedupe_key:
                        return False
                    if item_fill.get("external_fill_id") != broker_fill.external_fill_id:
                        return False
                    try:
                        if Decimal(str(item_fill.get("quantity"))) != broker_fill.quantity:
                            return False
                        if Decimal(str(item_fill.get("price"))) != broker_fill.price:
                            return False
                        stored_fee = item_fill.get("fee")
                        if (Decimal(str(stored_fee)) if stored_fee is not None else None) != broker_fill.fee:
                            return False
                    except (InvalidOperation, TypeError, ValueError):
                        return False
                    if item_fill.get("fee_currency") != broker_fill.fee_currency:
                        return False
                    if str(item_fill.get("filled_at")) != broker_fill.filled_at.isoformat():
                        return False
                    stored_metadata = item_fill.get("metadata")
                    if isinstance(stored_metadata, Mapping):
                        stored_metadata = {
                            key: value
                            for key, value in stored_metadata.items()
                            if key not in {
                                "_external_order_id",
                                "_evidence_reference",
                                self._BROKER_FILL_ACCOUNT_ID_KEY,
                                "_instrument_id",
                            }
                        }
                        return dict(stored_metadata) == dict(broker_fill.metadata)
                    return not broker_fill.metadata

                already_durable_on_sibling = any(
                    any(
                        sibling_fill_is_exact(item_fill)
                        for item_fill in self.repository.fills_for_broker_order(str(item.get("id")))
                    )
                    for item in sibling_claims
                )
                if already_durable_on_sibling:
                    continue
                if sibling_claims:
                    category = "SIBLING_INTENT_BROKER_FILL_UNMATCHED"
                    action_key = f"SIBLING_INTENT_FILL:{broker_fill.dedupe_key}"
                    summary = "A same-account broker fill is claimed by a sibling managed intent but is not attributable to this recovery; operator reconciliation is required."
                else:
                    category = "UNKNOWN_BROKER_FILL"
                    action_key = f"UNKNOWN_BROKER_FILL:{broker_fill.dedupe_key}"
                    summary = "Account-scoped broker fill could not be attributed to a managed attempt; operator reconciliation is required."
                self._require_reconciliation(
                    intent_id,
                    account,
                    category=category,
                    entity_type="BROKER_FILL",
                    entity_key=broker_fill.dedupe_key,
                    details={
                        "external_order_id": broker_fill.external_order_id,
                        "reason": "same-account sibling claim" if sibling_claims else "no managed broker attempt matched the account-scoped fill",
                        "sibling_broker_order_ids": [str(item.get("id")) for item in sibling_claims],
                    },
                )
                self._record_recovery_action(
                    intent=self._required_intent(intent_id),
                    account=account,
                    action_key=action_key,
                    state="RECONCILIATION_REQUIRED",
                    summary=summary,
                    observed_positions=self._observe_positions(account)[0],
                    remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                    allowed_next_steps=("refresh_broker_orders", "refresh_broker_fills", "reconcile_broker_position", "operator_review"),
                    metadata={"external_order_id": broker_fill.external_order_id, "dedupe_key": broker_fill.dedupe_key},
                )
                continue
            if len(matches) != 1:
                self._require_reconciliation(
                    intent_id,
                    account,
                    category="AMBIGUOUS_FILL_MATCH",
                    entity_type="BROKER_FILL",
                    entity_key=broker_fill.dedupe_key,
                    details={
                        "external_order_id": broker_fill.external_order_id,
                        "match_count": len(matches),
                    },
                )
                continue
            attempt = matches[0]
            matched_leg = next(
                item for item in self._required_intent(intent_id)["legs"] if item["id"] == attempt["order_leg_id"]
            )
            terminal_snapshot = terminal_snapshot_facts.get(str(attempt["id"]))
            if terminal_snapshot is not None or str(attempt.get("status")) in {
                BrokerOrderStatus.REJECTED.value,
                BrokerOrderStatus.CANCELLED.value,
            } or str(matched_leg.get("status")) in {
                LegStatus.REJECTED.value,
                LegStatus.CANCELLED.value,
            }:
                # A locally clean terminal rejection/cancellation is not
                # allowed to become a filled allocation merely because a
                # later history query returns a deal.  Preserve the broker
                # fact and require explicit reconciliation first.
                self._record_terminal_fill_contradiction(
                    intent_id=intent_id,
                    account=account,
                    leg=matched_leg,
                    broker_order_id=str(attempt["id"]),
                    terminal_status=(
                        str(terminal_snapshot.get("snapshot_status"))
                        if terminal_snapshot is not None
                        else str(attempt.get("status"))
                    ),
                    source=f"history:{broker_fill.dedupe_key}",
                    cumulative_filled_quantity=Decimal(
                        str(matched_leg.get("cumulative_filled_quantity", "0"))
                    ),
                    incoming_fill_count=1,
                    observed_positions=self._observe_positions(account)[0],
                )
                continue
            try:
                self._validate_recovery_fill_provenance(
                    account,
                    broker_fill,
                    attempt=attempt,
                    leg=matched_leg,
                )
                self.repository.record_fill(
                    Fill(
                        id=str(
                            uuid.uuid5(
                                uuid.NAMESPACE_URL,
                                f"{account.id}:{broker_fill.external_order_id}:{broker_fill.dedupe_key}",
                            )
                        ),
                        broker_order_id=str(attempt["id"]),
                        order_leg_id=str(attempt["order_leg_id"]),
                        external_fill_id=broker_fill.external_fill_id,
                        dedupe_key=broker_fill.dedupe_key,
                        quantity=broker_fill.quantity,
                        price=broker_fill.price,
                        fee=broker_fill.fee,
                        fee_currency=broker_fill.fee_currency,
                        filled_at=broker_fill.filled_at,
                        received_at=broker_fill.received_at,
                        metadata=self._fill_metadata_for_persistence(broker_fill),
                        account_id=account.id,
                        external_order_id=broker_fill.external_order_id,
                        evidence_reference=broker_fill.evidence_reference,
                    ),
                    now=self._now(),
                    _validation_token=self.repository._fill_validation_capability(),
                )
            except (KeyError, ValueError, InvalidOperation, sqlite3.Error) as exc:
                self._require_reconciliation(
                    intent_id,
                    account,
                    category="BROKER_FILL_RECORD_FAILED",
                    entity_type="BROKER_FILL",
                    entity_key=broker_fill.dedupe_key,
                    details={
                        "external_order_id": broker_fill.external_order_id,
                        "message": str(exc),
                    },
                )
                self._record_recovery_action(
                    intent=intent,
                    account=account,
                    action_key=f"FILL_RECORD_FAILED:{broker_fill.dedupe_key}",
                    state="RECONCILIATION_REQUIRED",
                    summary="A broker fill could not be durably applied; exposure remains unresolved and no remediation is allowed.",
                    observed_positions=self._observe_positions(account)[0],
                    remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                    allowed_next_steps=("refresh_broker_fills", "reconcile_broker_position", "operator_review"),
                    metadata={
                        "external_order_id": broker_fill.external_order_id,
                        "broker_order_id": str(attempt["id"]),
                        "error": str(exc),
                    },
                )
            else:
                self._resolve_exact_sibling_fill_blockers(
                    account=account,
                    broker_fill=broker_fill,
                    matched_attempt_id=str(attempt["id"]),
                )

        # A broker status can arrive before its deal record. Once durable fill
        # evidence has made a leg fully filled, close only that exact evidence
        # gap; never resolve unrelated reconciliation findings.
        recovered = self._required_intent(intent_id)
        for leg in recovered["legs"]:
            if leg["status"] != LegStatus.FILLED.value:
                continue
            for attempt in self.repository.broker_orders_for_leg(str(leg["id"])):
                if not self._attempt_has_complete_fill_evidence(attempt, leg):
                    continue
                self._resolve_reconciliation_issue(
                    account.id,
                    f"FILL_EVIDENCE_REQUIRED:BROKER_ORDER:{attempt['id']}",
                )
                if leg["status"] == LegStatus.FILLED.value:
                    self._resolve_recovery_actions_for_order(
                        account,
                        intent_id,
                        str(attempt["id"]),
                        action_prefixes=("WAIT_FOR_BROKER:", "FILL_EVIDENCE:"),
                    )

        # Moomoo SIM can leave a durable account-wide deal-query error from a
        # prior recovery even though its order list now supplies complete,
        # terminal fill facts.  Resolve that one issue only when every leg of
        # this exact intent has durable positive fill evidence; partial,
        # ambiguous, or unfilled intents remain reconciliation-required.
        if self._all_legs_have_fill_evidence(intent_id):
            self._resolve_exact_fill_query_issue(account, intent_id)

    def _all_legs_have_fill_evidence(self, intent_id: str) -> bool:
        intent = self._required_intent(intent_id)
        if not intent["legs"]:
            return False
        for leg in intent["legs"]:
            if leg["status"] != LegStatus.FILLED.value:
                return False
            try:
                requested = Decimal(str(leg["quantity"]))
                cumulative = Decimal(str(leg["cumulative_filled_quantity"]))
            except (InvalidOperation, TypeError, ValueError):
                return False
            if requested <= 0 or cumulative != requested:
                return False
            attempts = self.repository.broker_orders_for_leg(str(leg["id"]))
            if len(attempts) != 1:
                return False
            if not self._attempt_has_complete_fill_evidence(attempts[0], leg):
                return False
            try:
                if self._leg_durable_filled_quantity(str(leg["id"])) != requested:
                    return False
            except (InvalidOperation, TypeError, ValueError, sqlite3.Error):
                return False
        return True

    def _leg_durable_filled_quantity(self, leg_id: str) -> Decimal:
        total = Decimal("0")
        for attempt in self.repository.broker_orders_for_leg(leg_id):
            total += self._durable_filled_quantity(str(attempt["id"]))
        return total

    def _attempt_has_complete_fill_evidence(
        self,
        attempt: Mapping[str, object],
        leg: Mapping[str, object],
    ) -> bool:
        if str(attempt.get("status")) != BrokerOrderStatus.FILLED.value:
            return False
        try:
            submitted = Decimal(str(attempt["submitted_quantity"]))
            requested = Decimal(str(leg["quantity"]))
            durable = self._durable_filled_quantity(str(attempt["id"]))
        except (KeyError, InvalidOperation, TypeError, ValueError, sqlite3.Error):
            return False
        return submitted > 0 and submitted == requested and durable == submitted

    def _leg_has_fill_evidence(self, leg_id: str, broker_order_id: str) -> bool:
        """Return true only for a fully allocated leg backed by this order."""
        try:
            with self.repository.transaction() as conn:
                row = conn.execute(
                    "SELECT quantity, cumulative_filled_quantity, status FROM core_order_legs WHERE id = ?",
                    (leg_id,),
                ).fetchone()
        except sqlite3.Error:
            return False
        if row is None or str(row["status"]) != LegStatus.FILLED.value:
            return False
        try:
            if Decimal(str(row["cumulative_filled_quantity"])) != Decimal(str(row["quantity"])):
                return False
        except (InvalidOperation, TypeError, ValueError):
            return False
        try:
            attempt = self.repository.get_broker_order(broker_order_id)
        except sqlite3.Error:
            return False
        if attempt is None or attempt["status"] != BrokerOrderStatus.FILLED.value:
            return False
        try:
            if not self._attempt_has_complete_fill_evidence(attempt, row):
                return False
            return self._leg_durable_filled_quantity(leg_id) == Decimal(str(row["quantity"]))
        except (InvalidOperation, TypeError, ValueError, sqlite3.Error):
            return False

    def _leg_has_partial_fill_evidence(self, leg_id: str, broker_order_id: str) -> bool:
        """Return true for a positive, durable, non-complete partial fill."""
        try:
            with self.repository.transaction() as conn:
                row = conn.execute(
                    """SELECT quantity, cumulative_filled_quantity, status
                       FROM core_order_legs WHERE id = ?""",
                    (leg_id,),
                ).fetchone()
        except sqlite3.Error:
            return False
        if row is None or str(row["status"]) not in {
            LegStatus.PARTIALLY_FILLED.value,
            LegStatus.RECONCILIATION_REQUIRED.value,
        }:
            return False
        try:
            requested = Decimal(str(row["quantity"]))
            cumulative = Decimal(str(row["cumulative_filled_quantity"]))
            durable = self._durable_filled_quantity(broker_order_id)
        except (InvalidOperation, TypeError, ValueError, sqlite3.Error):
            return False
        if requested <= 0 or cumulative <= 0 or cumulative >= requested or durable != cumulative:
            return False
        try:
            attempt = self.repository.get_broker_order(broker_order_id)
        except sqlite3.Error:
            return False
        if attempt is None or attempt["status"] != BrokerOrderStatus.PARTIALLY_FILLED.value:
            return False
        try:
            if len(self.repository.broker_orders_for_leg(leg_id)) != 1:
                return False
            return self._leg_durable_filled_quantity(leg_id) == cumulative
        except (InvalidOperation, TypeError, ValueError, sqlite3.Error):
            return False

    def _resolve_exact_fill_query_issue(self, account: Account, intent_id: str) -> None:
        issue_key = f"BROKER_QUERY_FAILED:ACCOUNT:{account.id}:fills"
        for issue in self.repository.open_reconciliation_issues(account.id):
            if issue.get("issue_key") != issue_key:
                continue
            details = issue.get("details_json", {})
            if isinstance(details, str):
                try:
                    details = json.loads(details)
                except (TypeError, ValueError):
                    details = {}
            if isinstance(details, dict) and details.get("intent_id") == intent_id:
                self._resolve_reconciliation_issue(account.id, issue_key)
                return

    def _resolve_snapshot_fill_evidence(self, account: Account, intent_id: str) -> None:
        """Resolve only fill-support mismatches after the exact fills arrive."""
        for action in self.repository.recovery_actions_for_intent(intent_id):
            if not str(action["action_key"]).startswith("SNAPSHOT_MISMATCH:"):
                continue
            metadata = action.get("metadata") or {}
            mismatches = metadata.get("mismatches") or []
            if set(mismatches) != {"filled_quantity_not_supported_by_durable_fills"}:
                continue
            try:
                snapshot_status = metadata.get("snapshot_status")
                snapshot_quantity_for_policy = Decimal(str(metadata.get("snapshot_filled_quantity", "0")))
            except (InvalidOperation, TypeError, ValueError):
                continue
            if snapshot_status in {
                BrokerOrderStatus.REJECTED.value,
                BrokerOrderStatus.CANCELLED.value,
            } and snapshot_quantity_for_policy > 0:
                # A terminal rejection/cancellation carrying a positive fill
                # remains an actionable terminal-partial observation even
                # after the deal facts arrive; it is not a clean evidence gap.
                continue
            broker_order_id = str(metadata.get("broker_order_id", ""))
            if not broker_order_id or self.repository.intent_id_for_broker_order(broker_order_id) != intent_id:
                continue
            try:
                snapshot_quantity = Decimal(str(metadata["snapshot_filled_quantity"]))
                durable_quantity = self._durable_filled_quantity(broker_order_id)
            except (InvalidOperation, TypeError, ValueError, KeyError, sqlite3.Error):
                continue
            if snapshot_quantity != durable_quantity:
                continue
            self._resolve_reconciliation_issue(
                account.id,
                f"BROKER_SNAPSHOT_MISMATCH:BROKER_ORDER:{broker_order_id}",
                resolved_at=self._now(),
            )
            self._resolve_recovery_action(
                account.id,
                intent_id,
                str(action["action_key"]),
                resolved_at=self._now(),
            )

    def _enforce_unique_external_order_claim(
        self,
        *,
        intent_id: str,
        account: Account,
        external_order_id: str | None,
        leg_id: str | None = None,
        source: str,
    ) -> bool:
        """Quarantine every duplicate provider-order claim before matching.

        Older databases can contain same-account duplicate claims even though
        the current schema prevents creating them.  The startup audit makes
        those rows visible, but every normalized fill/recovery route must also
        enforce the invariant immediately before it can record evidence or
        allocate quantity.  A current matching row is not an exemption: the
        sibling claim makes ownership ambiguous until an operator resolves it.
        """
        identifier = str(external_order_id or "").strip()
        if not identifier:
            return False
        try:
            claims = self.repository.broker_orders_for_external_order_id(identifier)
        except sqlite3.Error as exc:
            self._require_reconciliation(
                intent_id,
                account,
                category="BROKER_ORDER_CLAIM_QUERY_FAILED",
                entity_type="BROKER_ORDER",
                entity_key=identifier,
                details={"external_order_id": identifier, "source": source, "message": str(exc)},
            )
            return True
        if len(claims) <= 1:
            return False
        details = {
            "external_order_id": identifier,
            "source": source,
            "claim_count": len(claims),
            "claims": [
                {
                    "broker_order_id": str(item.get("id")),
                    "account_id": str(item.get("account_id")),
                    "order_leg_id": str(item.get("order_leg_id")),
                    "intent_id": self.repository.intent_id_for_broker_order(str(item.get("id"))),
                    "status": str(item.get("status")),
                }
                for item in claims
            ],
        }
        try:
            if leg_id is not None:
                current_leg = next(
                    item for item in self._required_intent(intent_id)["legs"] if str(item["id"]) == str(leg_id)
                )
                self._force_leg_reconciliation(str(leg_id), str(current_leg["status"]))
        except (KeyError, ValueError, sqlite3.Error):
            # The durable account issue/action below is the safety boundary;
            # an already-terminal leg may reject a second transition.
            pass
        self._require_reconciliation(
            intent_id,
            account,
            category="DUPLICATE_BROKER_ORDER_CLAIM",
            entity_type="BROKER_ORDER",
            entity_key=identifier,
            details=details,
        )
        try:
            current_intent = self._required_intent(intent_id)
            self._record_recovery_action(
                intent=current_intent,
                account=account,
                action_key=f"DUPLICATE_BROKER_ORDER_CLAIM:{identifier}",
                state="RECONCILIATION_REQUIRED",
                summary="Multiple durable attempts claim one provider order; no fill matching, recording, allocation, or submission is safe.",
                observed_positions=self._observe_positions(account)[0],
                remaining_quantities=self._remaining_quantities(current_intent),
                allowed_next_steps=("refresh_broker_orders", "reconcile_broker_order", "operator_review"),
                metadata=details,
            )
        except (KeyError, ValueError, sqlite3.Error):
            pass
        return True

    def _apply_normalized_fill(
        self,
        *,
        account: Account,
        intent_id: str,
        broker_order_id: str,
        broker_fill: BrokerFill,
    ) -> bool:
        attempt = self.repository.get_broker_order(broker_order_id)
        if attempt is None:
            raise KeyError(f"Unknown broker order: {broker_order_id}")
        if self._enforce_unique_external_order_claim(
            intent_id=intent_id,
            account=account,
            external_order_id=broker_fill.external_order_id,
            leg_id=str(attempt["order_leg_id"]),
            source="normalized_fill",
        ):
            raise ValueError("duplicate broker order claims require reconciliation")
        if len(self.repository.broker_orders_for_leg(str(attempt["order_leg_id"]))) > 1:
            # This private path is used by both push and polling recovery;
            # keep the invariant here as a final shared guard as well.
            account_for_intent = self.repository.get_account(account.id)
            if account_for_intent is not None:
                self._quarantine_multiple_attempts(intent_id, account_for_intent)
            raise ValueError("multiple broker attempts for one leg are unsupported")
        if str(attempt["account_id"]) != account.id:
            raise ValueError("broker fill account does not match account")
        temporal_mismatches = self._temporal_fill_mismatches(attempt, broker_fill)
        if temporal_mismatches:
            self._record_fill_temporal_conflict(
                intent_id=intent_id,
                account=account,
                broker_fill=broker_fill,
                mismatches=temporal_mismatches,
            )
            raise ValueError("broker fill timestamp predates durable submit evidence")
        expected_instrument_id = next(
            (
                str(item["instrument_id"])
                for item in self._required_intent(intent_id)["legs"]
                if str(item["id"]) == str(attempt["order_leg_id"])
            ),
            None,
        )
        self._validate_broker_fill_provenance(
            attempt,
            broker_fill,
            expected_instrument_id=expected_instrument_id,
        )
        fill_account_mismatches = self._account_alias_mismatches(
            account,
            account_id=broker_fill.account_id,
            metadata=broker_fill.metadata,
        )
        if fill_account_mismatches:
            raise ValueError(
                "broker fill account aliases do not match canonical account: "
                + ", ".join(fill_account_mismatches)
            )
        observed_account_id = self._fill_account_id(broker_fill)
        if observed_account_id is not None and observed_account_id not in {
            account.id,
            account.external_account_id,
        }:
            raise ValueError("broker fill account identity does not match account")
        if attempt["external_order_id"] != broker_fill.external_order_id:
            raise ValueError("broker fill external order does not match persisted broker order")
        foreign_claims = [
            item
            for item in self.repository.broker_orders_for_external_order_id(broker_fill.external_order_id)
            if str(item.get("account_id")) != account.id
        ]
        if foreign_claims:
            raise ValueError("broker fill external order ID is claimed by another account")
        fill = Fill(
            id=str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"{account.id}:{broker_fill.external_order_id}:{broker_fill.dedupe_key}",
                )
            ),
            broker_order_id=broker_order_id,
            order_leg_id=str(attempt["order_leg_id"]),
            external_fill_id=broker_fill.external_fill_id,
            dedupe_key=broker_fill.dedupe_key,
            quantity=broker_fill.quantity,
            price=broker_fill.price,
            fee=broker_fill.fee,
            fee_currency=broker_fill.fee_currency,
            filled_at=broker_fill.filled_at,
            received_at=broker_fill.received_at,
            metadata=self._fill_metadata_for_persistence(broker_fill),
            account_id=account.id,
            external_order_id=broker_fill.external_order_id,
            evidence_reference=broker_fill.evidence_reference,
        )
        return self.repository.record_fill(
            fill,
            now=self._now(),
            _validation_token=self.repository._fill_validation_capability(),
        )

    @staticmethod
    def _validate_broker_fill_provenance(
        attempt: Mapping[str, object],
        broker_fill: BrokerFill,
        *,
        expected_instrument_id: str | None = None,
    ) -> None:
        """Require normalized fill evidence to bind to one broker attempt."""
        persisted_external_order_id = str(attempt.get("external_order_id") or "").strip()
        if not persisted_external_order_id:
            raise ValueError("broker attempt has no persisted external order identity")
        if broker_fill.external_order_id != persisted_external_order_id:
            raise ValueError("broker fill external order does not match persisted broker order")
        expected_reference = f"{persisted_external_order_id}:{broker_fill.dedupe_key}"
        if broker_fill.evidence_reference not in {broker_fill.dedupe_key, expected_reference}:
            raise ValueError("broker fill evidence reference does not match persisted broker order")

        expected_instrument = str(expected_instrument_id or "").strip()
        if expected_instrument:
            if broker_fill.instrument_id is not None and broker_fill.instrument_id != expected_instrument:
                raise ValueError("broker fill instrument does not match persisted order leg")

        symbol_values: list[str] = []
        canonical_instrument_values: list[str] = []
        try:
            fill_metadata = coerce_provider_payload(broker_fill.metadata)
        except ProviderPayloadError as exc:
            raise ValueError(f"broker fill metadata is not safely inspectable: {exc}") from exc

        def walk(value: object) -> None:
            if isinstance(value, Mapping):
                for raw_key, raw_value in value.items():
                    key = normalize_provider_key(raw_key)
                    if key in {"external_order_id", "order_id", "orderid", "broker_order_id"}:
                        if raw_value not in (None, "") and str(raw_value) != persisted_external_order_id:
                            raise ValueError("broker fill metadata external order does not match persisted broker order")
                    elif key in {"evidence_reference", "_evidence_reference"}:
                        if raw_value not in (None, "") and str(raw_value) not in {
                            broker_fill.dedupe_key,
                            expected_reference,
                        }:
                            raise ValueError("broker fill metadata evidence reference does not match persisted broker order")
                    elif key in {"instrument_id", "internal_instrument_id", "oms_instrument_id"}:
                        if raw_value not in (None, ""):
                            canonical_instrument_values.append(str(raw_value).strip())
                    elif key in {"code", "symbol", "ticker", "external_symbol"}:
                        if raw_value not in (None, ""):
                            normalized_symbol = str(raw_value).strip().upper()
                            if not normalized_symbol:
                                raise ValueError("broker fill instrument symbol is empty")
                            symbol_values.append(normalized_symbol)
                    walk(raw_value)
            elif isinstance(value, (list, tuple)):
                for item in value:
                    walk(item)
            elif value is not None and not isinstance(value, (str, int, float, bool, Decimal, datetime)):
                raise ValueError(
                    f"opaque broker fill metadata type {type(value).__name__!r} cannot be validated"
                )

        walk(fill_metadata)
        if symbol_values and any(value != symbol_values[0] for value in symbol_values[1:]):
            raise ValueError("broker fill instrument symbol aliases conflict")
        if canonical_instrument_values and any(
            value != canonical_instrument_values[0] for value in canonical_instrument_values[1:]
        ):
            raise ValueError("broker fill canonical instrument aliases conflict")
        if expected_instrument:
            if canonical_instrument_values and any(value != expected_instrument for value in canonical_instrument_values):
                raise ValueError("broker fill metadata instrument does not match persisted order leg")
            if symbol_values and broker_fill.instrument_id is None:
                raise ValueError("broker fill symbol provenance lacks a canonical instrument identity")
        if broker_fill.instrument_id is not None and canonical_instrument_values and any(
            value != broker_fill.instrument_id for value in canonical_instrument_values
        ):
            raise ValueError("broker fill instrument metadata does not match normalized instrument identity")

    def _validate_exact_selected_fill_facts(
        self,
        *,
        account: Account,
        facts: BrokerFactSnapshot,
        expected_rows: Sequence[Mapping[str, object]],
        source: str,
    ) -> dict[str, list[BrokerFill]]:
        """Authenticate the complete fresh fill set for one proof claim.

        A proof route must not select one convenient fill from an account-wide
        snapshot and silently net away another row.  Every selected durable
        fill is matched by its full immutable identity/economics/timestamps
        and provenance, and every additional row must be an exact,
        proof-backed closed-history replay.  Active sibling, foreign,
        unattributed, changed, or otherwise ambiguous rows fail before an
        exit intent can be created.
        """
        expected_by_external: dict[str, list[Mapping[str, object]]] = {}
        expected_by_identity: dict[tuple[str, str], Mapping[str, object]] = {}
        for row in expected_rows:
            external_order_id = str(row.get("external_order_id") or "").strip()
            durable_fill = row.get("fill")
            attempt = row.get("attempt")
            if not external_order_id or not isinstance(durable_fill, Mapping) or not isinstance(attempt, Mapping):
                raise OMSExecutionError(f"{source} durable fill claim is incomplete")
            durable_identity = str(
                durable_fill.get("external_fill_id") or durable_fill.get("dedupe_key") or ""
            ).strip()
            if not durable_identity:
                raise OMSExecutionError(f"{source} durable fill claim has no immutable identity")
            identity = (external_order_id, durable_identity)
            if identity in expected_by_identity:
                raise OMSExecutionError(f"{source} durable fill claims contain a duplicate identity")
            expected_by_identity[identity] = row
            expected_by_external.setdefault(external_order_id, []).append(row)

        matched: dict[str, list[BrokerFill]] = {}
        matched_identities: set[tuple[str, str]] = set()
        for broker_fill in facts.fills:
            external_order_id = str(broker_fill.external_order_id).strip()
            candidates = expected_by_external.get(external_order_id)
            if not candidates:
                if self.is_proof_backed_closed_broker_fill(
                    account=account,
                    broker_fill=broker_fill,
                ):
                    continue
                raise OMSExecutionError(
                    f"{source} fresh broker facts contain an extra unattributed, sibling, or foreign fill "
                    f"{external_order_id}"
                )

            matching_row: Mapping[str, object] | None = None
            mismatch_reasons: list[str] = []
            for row in candidates:
                reason = self._selected_fill_fact_mismatch(
                    account=account,
                    broker_fill=broker_fill,
                    row=row,
                )
                if reason is None:
                    matching_row = row
                    break
                mismatch_reasons.append(reason)
            if matching_row is None:
                raise OMSExecutionError(
                    f"{source} fresh broker fill {external_order_id} conflicts with durable evidence: "
                    + "; ".join(sorted(set(mismatch_reasons)))
                )

            durable_fill = matching_row["fill"]
            durable_identity = str(
                durable_fill.get("external_fill_id") or durable_fill.get("dedupe_key") or ""
            )
            identity = (external_order_id, durable_identity)
            if identity in matched_identities:
                raise OMSExecutionError(
                    f"{source} fresh broker facts contain a duplicate selected fill identity {identity!r}"
                )
            matched_identities.add(identity)
            matched.setdefault(external_order_id, []).append(broker_fill)

        missing = sorted(set(expected_by_identity) - matched_identities)
        if missing:
            raise OMSExecutionError(
                f"{source} fresh broker facts lack complete selected fill evidence: {missing!r}"
            )
        return matched

    def _selected_fill_fact_mismatch(
        self,
        *,
        account: Account,
        broker_fill: BrokerFill,
        row: Mapping[str, object],
    ) -> str | None:
        """Return one durable-vs-provider mismatch for a selected fill."""
        instrument_id = str(row.get("instrument_id") or "").strip()
        external_order_id = str(row.get("external_order_id") or "").strip()
        attempt = row.get("attempt")
        durable_fill = row.get("fill")
        if not isinstance(attempt, Mapping) or not isinstance(durable_fill, Mapping):
            return "durable fill claim is incomplete"
        if broker_fill.external_order_id != external_order_id:
            return "external order identity differs"
        if broker_fill.external_fill_id != durable_fill.get("external_fill_id"):
            return "external fill identity differs"
        if broker_fill.dedupe_key != str(durable_fill.get("dedupe_key") or ""):
            return "dedupe identity differs"
        try:
            if broker_fill.quantity != Decimal(str(durable_fill.get("quantity"))):
                return "fill quantity differs"
            if broker_fill.price != Decimal(str(durable_fill.get("price"))):
                return "fill price differs"
            durable_fee = durable_fill.get("fee")
            durable_fee_value = Decimal(str(durable_fee)) if durable_fee is not None else None
            if broker_fill.fee != durable_fee_value:
                return "fill fee differs"
        except (InvalidOperation, TypeError, ValueError):
            return "fill economics are malformed"
        if broker_fill.fee_currency != durable_fill.get("fee_currency"):
            return "fill fee currency differs"
        durable_filled_at = self._parse_timestamp(durable_fill.get("filled_at"))
        if durable_filled_at is None or broker_fill.filled_at != durable_filled_at:
            return "fill timestamp differs"
        # ``received_at`` is the local observation time, not provider-side
        # execution identity.  In particular, Moomoo SIM's cumulative-order
        # fallback creates a new receipt timestamp each time the same filled
        # order is observed.  Keep the broker execution timestamp and every
        # other immutable/provenance check strict, but do not require two
        # observations to have identical local ingestion times.
        durable_mode = str(durable_fill.get("evidence_mode") or "").strip()
        if durable_mode and broker_fill.evidence_mode.value != durable_mode:
            return "execution evidence mode differs"
        stored_metadata = durable_fill.get("metadata")
        if not isinstance(stored_metadata, Mapping):
            return "durable fill provenance metadata is missing"
        stored_external = str(stored_metadata.get("_external_order_id") or "").strip()
        if stored_external and stored_external != external_order_id:
            return "durable external-order provenance differs"
        stored_instrument = str(stored_metadata.get("_instrument_id") or "").strip()
        if stored_instrument and stored_instrument != instrument_id:
            return "durable instrument provenance differs"
        stored_account = str(stored_metadata.get(self._BROKER_FILL_ACCOUNT_ID_KEY) or "").strip()
        if stored_account and stored_account != account.id:
            return "durable account provenance differs"
        stored_reference = str(
            stored_metadata.get("_evidence_reference")
            or stored_metadata.get("evidence_reference")
            or ""
        ).strip()
        if stored_reference and broker_fill.evidence_reference not in {
            stored_reference,
            broker_fill.dedupe_key,
            f"{external_order_id}:{broker_fill.dedupe_key}",
        }:
            return "execution evidence reference differs"
        if self._fill_account_id(broker_fill) != account.id:
            return "provider fill account provenance differs"
        if broker_fill.instrument_id != instrument_id:
            return "provider fill instrument provenance differs"
        if broker_fill.evidence_mode is ExecutionEvidenceMode.UNAVAILABLE or not broker_fill.evidence_reference:
            return "provider fill execution provenance is unavailable"
        try:
            self._validate_broker_fill_provenance(
                attempt,
                broker_fill,
                expected_instrument_id=instrument_id,
            )
        except (KeyError, TypeError, ValueError) as exc:
            return str(exc)
        return None

    @staticmethod
    def _fill_account_id(broker_fill: BrokerFill) -> str | None:
        if broker_fill.account_id not in (None, ""):
            return str(broker_fill.account_id)
        try:
            fill_metadata = coerce_provider_payload(broker_fill.metadata)
        except ProviderPayloadError:
            return "<opaque:provider_payload>"

        def walk(value: object) -> str | None:
            if isinstance(value, Mapping):
                for raw_key, raw_value in value.items():
                    key = normalize_provider_key(raw_key)
                    if key in {"account_id", "account", "external_account_id", "account_alias", "account_identifier"}:
                        if raw_value not in (None, ""):
                            return str(raw_value)
                    found = walk(raw_value)
                    if found is not None:
                        return found
            elif isinstance(value, (list, tuple)):
                for item in value:
                    found = walk(item)
                    if found is not None:
                        return found
            elif value is not None and not isinstance(value, (str, int, float, bool, Decimal, datetime)):
                return f"<opaque:{type(value).__name__}>"
            return None

        found = walk(fill_metadata)
        if found is not None:
            return found
        return None

    @staticmethod
    def _account_alias_mismatches(
        account: Account,
        *,
        account_id: str | None = None,
        external_account_id: str | None = None,
        metadata: Mapping[str, object] | None = None,
    ) -> list[str]:
        """Validate every account alias carried by a broker fact."""
        expected_internal = str(account.id)
        expected_external = str(account.external_account_id)
        expected_broker = str(account.broker)
        expected_environment = str(account.environment.value)
        environment_aliases = {expected_environment}
        if expected_environment == "SIM":
            environment_aliases.add("SIMULATE")
        elif expected_environment == "LIVE":
            environment_aliases.add("REAL")
        mismatches: set[str] = set()
        try:
            metadata = coerce_provider_payload(metadata or {})
        except ProviderPayloadError as exc:
            return [f"metadata.provider_payload={exc}"]

        def check(key: str, value: object, *, expected: set[str]) -> None:
            if value in (None, ""):
                return
            if str(value) not in expected:
                mismatches.add(f"{key}={value}")

        check("account_id", account_id, expected={expected_internal})
        check("external_account_id", external_account_id, expected={expected_external})

        def walk(value: object, path: str = "metadata") -> None:
            if isinstance(value, Mapping):
                for raw_key, raw_value in value.items():
                    key = normalize_provider_key(raw_key)
                    current_path = f"{path}.{key}"
                    if key in {"account_id", "internal_account_id", "oms_account_id"}:
                        check(current_path, raw_value, expected={expected_internal})
                    elif key in {
                        "external_account_id",
                        "acc_id",
                        "account_number",
                        "trd_acc_id",
                        "trade_account_id",
                    }:
                        check(current_path, raw_value, expected={expected_external})
                    elif key in {"account", "account_alias", "account_identifier"}:
                        check(current_path, raw_value, expected={expected_internal, expected_external})
                        walk(raw_value, current_path)
                    elif key in {"broker", "broker_name", "broker_id", "provider"}:
                        check(current_path, raw_value, expected={expected_broker})
                    elif key in {"environment", "trading_environment", "trd_env", "trading_env"}:
                        if raw_value not in (None, "") and str(raw_value).upper().split(".")[-1] not in {
                            value.upper() for value in environment_aliases
                        }:
                            mismatches.add(f"{current_path}={raw_value}")
                    else:
                        walk(raw_value, current_path)
            elif isinstance(value, (list, tuple)):
                for index, item in enumerate(value):
                    walk(item, f"{path}[{index}]")
            elif value is not None and not isinstance(value, (str, int, float, bool, Decimal, datetime)):
                mismatches.add(f"{path}.opaque_type={type(value).__name__}")

        walk(metadata or {})
        return sorted(mismatches)

    def _validate_event_fill_evidence(
        self,
        *,
        attempt: Mapping[str, object],
        event: BrokerOrderEvent,
        incoming_fills: Sequence[BrokerFill] = (),
    ) -> dict[str, object] | None:
        """Validate optional cumulative fill facts before applying event state."""
        if event.cumulative_filled_quantity is None:
            return None
        mismatches: list[str] = []
        try:
            submitted_quantity = Decimal(str(attempt["submitted_quantity"]))
            observed_quantity = Decimal(str(event.cumulative_filled_quantity))
            durable_quantity = self._durable_filled_quantity(str(attempt["id"]))
            existing_dedupe_keys = {
                str(item["dedupe_key"])
                for item in self.repository.fills_for_broker_order(str(attempt["id"]))
            }
            projected_quantity = durable_quantity
            projected_inputs = (
                incoming_fills
                if event.broker_status in {
                    BrokerOrderStatus.PARTIALLY_FILLED,
                    BrokerOrderStatus.FILLED,
                }
                else ()
            )
            for broker_fill in projected_inputs:
                if not isinstance(broker_fill, BrokerFill):
                    continue
                if broker_fill.external_order_id != attempt.get("external_order_id"):
                    continue
                if broker_fill.dedupe_key in existing_dedupe_keys:
                    continue
                projected_quantity += broker_fill.quantity
            if observed_quantity < 0 or observed_quantity > submitted_quantity:
                mismatches.append("filled_quantity_range")
            if observed_quantity != projected_quantity:
                mismatches.append("filled_quantity_not_supported_by_durable_fills")
            if event.broker_status is not None:
                mismatches.extend(
                    self._broker_order_quantity_mismatches(
                        event.broker_status,
                        submitted_quantity,
                        observed_quantity,
                    )
                )
        except (KeyError, InvalidOperation, TypeError, ValueError, sqlite3.Error) as exc:
            mismatches.append(f"quantity_evidence:{exc}")
            submitted_quantity = Decimal("0")
            observed_quantity = Decimal("0")
            durable_quantity = Decimal("0")
            projected_quantity = Decimal("0")
        return {
            "valid": not mismatches,
            "mismatches": mismatches,
            "broker_order_id": str(attempt["id"]),
            "event_id": event.id,
            "event_status": event.broker_status.value if event.broker_status is not None else None,
            "submitted_quantity": str(submitted_quantity),
            "event_filled_quantity": str(observed_quantity),
            "durable_filled_quantity": str(durable_quantity),
            "projected_filled_quantity": str(projected_quantity),
        }

    def _record_event_fill_mismatch(
        self,
        *,
        intent: Mapping[str, object],
        account: Account,
        attempt: Mapping[str, object],
        event: BrokerOrderEvent,
        observed_positions: Mapping[str, object],
        validation: Mapping[str, object],
    ) -> None:
        leg = next(item for item in self._required_intent(str(intent["id"]))["legs"] if item["id"] == attempt["order_leg_id"])
        self._mark_leg_reconciliation(str(leg["id"]), str(leg["status"]))
        try:
            current_order = BrokerOrderStatus(str(attempt["status"]))
            if current_order not in {
                BrokerOrderStatus.UNKNOWN,
                BrokerOrderStatus.FILLED,
                BrokerOrderStatus.REJECTED,
                BrokerOrderStatus.CANCELLED,
            }:
                self.repository.transition_broker_order(
                    str(attempt["id"]),
                    BrokerOrderStatus.UNKNOWN,
                    now=self._now(),
                )
        except (KeyError, ValueError, sqlite3.Error):
            pass
        details = dict(validation)
        details["dedupe_key"] = event.dedupe_key
        self._require_reconciliation(
            str(intent["id"]),
            account,
            category="BROKER_EVENT_FILL_MISMATCH",
            entity_type="BROKER_ORDER",
            entity_key=f"{attempt['id']}:{event.dedupe_key}",
            details=details,
        )
        self._record_recovery_action(
            intent=intent,
            account=account,
            action_key=f"EVENT_FILL_MISMATCH:{attempt['id']}:{event.dedupe_key}",
            state="RECONCILIATION_REQUIRED",
            summary=(
                f"Normalized broker event for order {attempt['id']} failed cumulative-fill validation; "
                "do not infer exposure or submit, cancel, or hedge."
            ),
            observed_positions=observed_positions,
            remaining_quantities=self._remaining_quantities(self._required_intent(str(intent["id"]))),
            allowed_next_steps=("refresh_broker_facts", "refresh_broker_fills", "reconcile_broker_position", "operator_review"),
            metadata=details,
        )

    def _resolve_event_fill_evidence(
        self,
        *,
        account: Account,
        intent_id: str,
        event: BrokerOrderEvent,
    ) -> None:
        """Resolve only an event fill mismatch once its exact fill arrives."""
        if event.cumulative_filled_quantity is None:
            return
        action_key = f"EVENT_FILL_MISMATCH:{event.broker_order_id}:{event.dedupe_key}"
        actions = [
            action
            for action in self.repository.recovery_actions_for_intent(intent_id)
            if action["action_key"] == action_key and action["status"] == RecoveryActionStatus.OPEN.value
        ]
        if not actions:
            return
        metadata = actions[0].get("metadata") or {}
        mismatches = set(metadata.get("mismatches") or ())
        if mismatches != {"filled_quantity_not_supported_by_durable_fills"}:
            return
        try:
            event_quantity = Decimal(str(metadata["event_filled_quantity"]))
            durable_quantity = self._durable_filled_quantity(event.broker_order_id)
        except (InvalidOperation, TypeError, ValueError, KeyError, sqlite3.Error):
            return
        if event_quantity != durable_quantity:
            return
        if (
            metadata.get("event_status") in {
                BrokerOrderStatus.REJECTED.value,
                BrokerOrderStatus.CANCELLED.value,
            }
            and event_quantity > 0
        ):
            return
        self._resolve_reconciliation_issue(
            account.id,
            f"BROKER_EVENT_FILL_MISMATCH:BROKER_ORDER:{event.broker_order_id}:{event.dedupe_key}",
            resolved_at=self._now(),
        )
        self._resolve_recovery_action(
            account.id,
            intent_id,
            action_key,
            resolved_at=self._now(),
        )

    def _ingest_validated_broker_order_event(
        self,
        event: BrokerOrderEvent,
        *,
        account: Account,
        fills: Sequence[BrokerFill],
    ) -> dict:
        """Private adapter bridge for already-authoritative normalized fills.

        A normal caller must use :meth:`ingest_broker_order_event`, which
        obtains fill facts from the adapter.  This narrow instance-private
        bridge exists for an adapter push integration that has already
        authenticated the normalized fills and cannot be reached with an
        arbitrary module-level token.
        """
        return self.ingest_broker_order_event(
            event,
            account=account,
            fills=fills,
            _validated_fills_capability=self.__broker_event_validation_capability,
        )

    def ingest_broker_order_event(
        self,
        event: BrokerOrderEvent,
        *,
        account: Account,
        fills: Sequence[BrokerFill] = (),
        _validated_fills_capability: object | None = None,
    ) -> dict:
        """Ingest normalized push-compatible facts without submitting anything.

        Order status alone never creates a position allocation.  A terminal
        filled/partial event without matching adapter-authoritative normalized
        fill facts remains reconciliation-required until polling or a later
        fill update supplies durable evidence.  Caller-supplied ``fills`` are
        ignored as untrusted unless this OMS-instance private adapter bridge
        supplied its capability; the public path performs its own fill query.
        """
        attempt = self.repository.get_broker_order(event.broker_order_id)
        if attempt is None:
            raise KeyError(f"Unknown broker order: {event.broker_order_id}")
        intent_id = self.repository.intent_id_for_broker_order(event.broker_order_id)
        if intent_id is None:
            raise KeyError(f"No intent for broker order: {event.broker_order_id}")
        intent = self._required_intent(intent_id)
        if (
            account.id != str(intent["account_id"])
            or str(attempt.get("account_id")) != str(intent["account_id"])
        ):
            self._record_account_identity_mismatch(
                intent=intent,
                supplied_account_id=account.id,
                observed_account_id=str(attempt.get("account_id")),
                source=f"event:{event.broker_order_id}",
            )
            # Use the persisted intent account to build the response.  The
            # supplied account is intentionally wrong, so passing it back to
            # recovery_status would raise a second raw identity exception
            # instead of returning the durable blocker/action.
            return self.recovery_status(intent_id)
        canonical = self._canonical_account_for_intent(
            intent,
            supplied=account,
            source=f"event:{event.broker_order_id}",
        )
        if canonical is None:
            return self.recovery_status(intent_id)
        account = canonical
        if not self._validate_persisted_execution_policy(intent, account, source=f"event:{event.broker_order_id}"):
            return self.recovery_status(intent_id)
        if self._enforce_unique_external_order_claim(
            intent_id=intent_id,
            account=account,
            external_order_id=event.external_order_id or attempt.get("external_order_id"),
            leg_id=str(attempt.get("order_leg_id")),
            source="broker_event",
        ):
            return self.recovery_status(intent_id, account=account)
        self._ensure_terminal_history_safety(account)
        self._quarantine_multiple_attempts(intent_id, account)
        event_alias_mismatches = self._account_alias_mismatches(
            account,
            account_id=event.account_id,
            external_account_id=event.external_account_id,
            metadata=event.metadata,
        )
        observed_positions, position_error = self._observe_positions(account)
        if position_error is not None:
            self._require_reconciliation(
                intent_id,
                account,
                category="BROKER_QUERY_FAILED",
                entity_type="ACCOUNT",
                entity_key=f"{account.id}:positions",
                details={"message": position_error},
            )
            self._record_recovery_action(
                intent=intent,
                account=account,
                action_key=f"POLL_ERROR:{account.id}:positions",
                state="RECONCILIATION_REQUIRED",
                summary="Broker position polling failed; observed exposure is unavailable and no remediation is allowed.",
                observed_positions=observed_positions,
                remaining_quantities=self._remaining_quantities(intent),
                allowed_next_steps=("refresh_broker_facts", "reconcile_broker_position", "operator_review"),
                metadata={"error": position_error},
            )

        event_leg = next(item for item in intent["legs"] if item["id"] == attempt["order_leg_id"])
        validated_fills = _validated_fills_capability is self.__broker_event_validation_capability
        if not validated_fills:
            # A public push event is only a notification.  Obtain the
            # authoritative order snapshot before allowing *any* status,
            # cumulative quantity, or no-fill assertion to influence state.
            authoritative_snapshot, status_query_error = self._authoritative_event_snapshot(
                event,
                account,
                attempt,
                event_leg,
            )
            if status_query_error is not None:
                details = {
                    "event_id": event.id,
                    "dedupe_key": event.dedupe_key,
                    "source": "public_event_status_argument",
                    "error": status_query_error,
                    "caller_status": event.broker_status.value if event.broker_status is not None else None,
                    "caller_cumulative_filled_quantity": (
                        str(event.cumulative_filled_quantity)
                        if event.cumulative_filled_quantity is not None
                        else None
                    ),
                    "caller_no_fill_asserted": event.no_fill_asserted,
                }
                self._require_reconciliation(
                    intent_id,
                    account,
                    category="BROKER_EVENT_STATUS_UNAVAILABLE",
                    entity_type="BROKER_ORDER",
                    entity_key=event.broker_order_id,
                    details=details,
                )
                self._record_recovery_action(
                    intent=intent,
                    account=account,
                    action_key=f"BROKER_EVENT_STATUS_QUERY:{event.broker_order_id}:{event.dedupe_key}",
                    state="RECONCILIATION_REQUIRED",
                    summary="Public broker-event status was not authenticated by an authoritative order snapshot; no caller status or fill quantity was applied.",
                    observed_positions=observed_positions,
                    remaining_quantities=self._remaining_quantities(intent),
                    allowed_next_steps=("refresh_broker_order", "refresh_broker_facts", "operator_review"),
                    metadata=details,
                )
                if event.cumulative_filled_quantity not in (None, Decimal("0")):
                    self._require_reconciliation(
                        intent_id,
                        account,
                        category="BROKER_EVENT_FILL_MISMATCH",
                        entity_type="BROKER_ORDER",
                        entity_key=event.broker_order_id,
                        details={**details, "reason": "caller cumulative fill was not authenticated"},
                    )
                    self._record_recovery_action(
                        intent=intent,
                        account=account,
                        action_key=f"EVENT_FILL_MISMATCH:{event.broker_order_id}:{event.dedupe_key}",
                        state="RECONCILIATION_REQUIRED",
                        summary="A public event supplied cumulative fill evidence without an authenticated broker order snapshot.",
                        observed_positions=observed_positions,
                        remaining_quantities=self._remaining_quantities(intent),
                        allowed_next_steps=("refresh_broker_order", "refresh_broker_fills", "operator_review"),
                        metadata={**details, "reason": "caller cumulative fill was not authenticated"},
                    )
                if event.broker_status in {
                    BrokerOrderStatus.REJECTED,
                    BrokerOrderStatus.CANCELLED,
                }:
                    self._require_reconciliation(
                        intent_id,
                        account,
                        category="TERMINAL_STATUS_WITHOUT_FILL_EVIDENCE",
                        entity_type="BROKER_ORDER",
                        entity_key=event.broker_order_id,
                        details={**details, "reason": "caller terminal status was not authenticated"},
                    )
                event = replace(
                    event,
                    broker_status=None,
                    cumulative_filled_quantity=None,
                    no_fill_asserted=False,
                    external_order_id=event.external_order_id or attempt.get("external_order_id"),
                )
            else:
                assert authoritative_snapshot is not None
                event = replace(
                    event,
                    broker_status=authoritative_snapshot.status,
                    cumulative_filled_quantity=authoritative_snapshot.filled_quantity,
                    no_fill_asserted=authoritative_snapshot.no_fill_asserted,
                    authority=authoritative_snapshot.authority,
                    external_order_id=authoritative_snapshot.external_order_id,
                    client_order_id=(
                        authoritative_snapshot.client_order_id or event.client_order_id
                    ),
                    metadata={
                        **dict(event.metadata),
                        "authoritative_snapshot": dict(authoritative_snapshot.metadata),
                    },
                )
        untrusted_fill_count = 0
        fill_query_error: str | None = None
        if validated_fills:
            try:
                fills = tuple(fills)
            except TypeError as exc:
                fills = ()
                fill_query_error = f"validated adapter event fills were not iterable: {exc}"
            if any(not isinstance(item, BrokerFill) for item in fills):
                fills = ()
                fill_query_error = "validated adapter event fills contained a non-normalized value"
        else:
            try:
                untrusted_fill_count = len(tuple(fills))
            except TypeError:
                untrusted_fill_count = 1
            # A caller-provided fill sequence is not evidence.  Do not feed it
            # into fingerprints or terminal decisions; use the adapter's
            # authoritative fill reader instead.
            fills, fill_query_error = self._authoritative_event_fills(event, account, attempt)

        if untrusted_fill_count:
            details = {
                "event_id": event.id,
                "dedupe_key": event.dedupe_key,
                "supplied_fill_count": untrusted_fill_count,
                "accepted_fill_count": 0,
                "source": "public_event_argument",
            }
            self._require_reconciliation(
                intent_id,
                account,
                category="UNTRUSTED_EVENT_FILL_EVIDENCE",
                entity_type="BROKER_ORDER",
                entity_key=event.broker_order_id,
                details=details,
            )
            self._record_recovery_action(
                intent=intent,
                account=account,
                action_key=f"UNTRUSTED_EVENT_FILLS:{event.broker_order_id}:{event.dedupe_key}",
                state="RECONCILIATION_REQUIRED",
                summary="Caller-supplied push fills were not accepted as broker evidence; use the adapter-authoritative fill path and reconcile this event.",
                observed_positions=observed_positions,
                remaining_quantities=self._remaining_quantities(intent),
                allowed_next_steps=("refresh_broker_fills", "reconcile_broker_order", "operator_review"),
                metadata=details,
            )
        if fill_query_error is not None:
            details = {
                "event_id": event.id,
                "dedupe_key": event.dedupe_key,
                "source": "adapter_authoritative_event_fill_lookup",
                "error": fill_query_error,
            }
            self._require_reconciliation(
                intent_id,
                account,
                category="BROKER_EVENT_FILL_UNAVAILABLE",
                entity_type="BROKER_ORDER",
                entity_key=event.broker_order_id,
                details=details,
            )
            self._record_recovery_action(
                intent=intent,
                account=account,
                action_key=f"BROKER_EVENT_FILL_QUERY:{event.broker_order_id}:{event.dedupe_key}",
                state="RECONCILIATION_REQUIRED",
                summary="The adapter-authoritative fill lookup failed or returned an invalid shape; no push fill was trusted.",
                observed_positions=observed_positions,
                remaining_quantities=self._remaining_quantities(intent),
                allowed_next_steps=("refresh_broker_fills", "reconcile_broker_order", "operator_review"),
                metadata=details,
            )
        if event_alias_mismatches:
            self._require_reconciliation(
                intent_id,
                account,
                category="BROKER_EVENT_ACCOUNT_ID_CONFLICT",
                entity_type="BROKER_ORDER",
                entity_key=event.broker_order_id,
                details={
                    "event_id": event.id,
                    "dedupe_key": event.dedupe_key,
                    "mismatches": event_alias_mismatches,
                },
            )
            self._record_recovery_action(
                intent=intent,
                account=account,
                action_key=f"EVENT_ACCOUNT_ID_CONFLICT:{event.broker_order_id}:{event.dedupe_key}",
                state="RECONCILIATION_REQUIRED",
                summary="A normalized broker event carried conflicting account aliases; no order state or fill evidence was accepted.",
                observed_positions=observed_positions,
                remaining_quantities=self._remaining_quantities(intent),
                allowed_next_steps=("refresh_broker_facts", "reconcile_broker_order", "operator_review"),
                metadata={"event_id": event.id, "mismatches": event_alias_mismatches},
            )
            return self.recovery_status(intent_id, account=account)
        temporal_mismatches = self._temporal_order_mismatches(attempt, event.event_at)
        temporal_mismatches.extend(
            f"received_{item}"
            for item in self._temporal_order_mismatches(attempt, event.received_at)
        )
        temporal_conflict = bool(temporal_mismatches)
        if temporal_conflict:
            self._require_reconciliation(
                intent_id,
                account,
                category="BROKER_EVENT_TIME_CONFLICT",
                entity_type="BROKER_ORDER",
                entity_key=event.broker_order_id,
                details={
                    "event_id": event.id,
                    "dedupe_key": event.dedupe_key,
                    "mismatches": temporal_mismatches,
                    "event_at": event.event_at.isoformat(),
                    "submitted_at": attempt.get("submitted_at"),
                    "updated_at": attempt.get("updated_at"),
                },
            )
            self._record_recovery_action(
                intent=intent,
                account=account,
                action_key=f"EVENT_TIME_CONFLICT:{event.broker_order_id}:{event.dedupe_key}",
                state="RECONCILIATION_REQUIRED",
                summary="Broker event timestamp predates durable submit evidence; no lifecycle or fill allocation is allowed.",
                observed_positions=observed_positions,
                remaining_quantities=self._remaining_quantities(intent),
                allowed_next_steps=("refresh_broker_facts", "reconcile_broker_order", "operator_review"),
                metadata={"event_id": event.id, "mismatches": temporal_mismatches},
            )
        event_leg = next(item for item in intent["legs"] if item["id"] == attempt["order_leg_id"])
        local_terminal_order = str(attempt["status"]) in {
            BrokerOrderStatus.FILLED.value,
            BrokerOrderStatus.REJECTED.value,
            BrokerOrderStatus.CANCELLED.value,
            BrokerOrderStatus.FAILED.value,
        }
        local_terminal_leg = str(event_leg["status"]) in {
            LegStatus.FILLED.value,
            LegStatus.REJECTED.value,
            LegStatus.CANCELLED.value,
            LegStatus.FAILED.value,
        }
        if (
            event.external_order_id is not None
            and attempt.get("external_order_id") is not None
            and event.external_order_id != attempt["external_order_id"]
        ):
            self._require_reconciliation(
                intent_id,
                account,
                category="BROKER_EVENT_EXTERNAL_ID_CONFLICT",
                entity_type="BROKER_ORDER",
                entity_key=event.broker_order_id,
                details={
                    "durable_external_order_id": attempt["external_order_id"],
                    "observed_external_order_id": event.external_order_id,
                    "event_id": event.id,
                },
            )
            self._record_recovery_action(
                intent=intent,
                account=account,
                action_key=f"EVENT_EXTERNAL_ID_CONFLICT:{event.broker_order_id}:{event.dedupe_key}",
                state="RECONCILIATION_REQUIRED",
                summary="A normalized broker event carried a conflicting external order ID; no state inference is allowed.",
                observed_positions=observed_positions,
                remaining_quantities=self._remaining_quantities(intent),
                allowed_next_steps=("refresh_broker_facts", "reconcile_broker_order", "operator_review"),
                metadata={"event_id": event.id, "error": "external order ID conflict"},
            )
            return self.recovery_status(intent_id, account=account)
        observed_event_client_order_id = event.client_order_id
        if observed_event_client_order_id is None:
            metadata_client_order_id = event.metadata.get("client_order_id")
            if metadata_client_order_id is not None:
                observed_event_client_order_id = str(metadata_client_order_id)
        if (
            observed_event_client_order_id is not None
            and attempt.get("client_order_id") is not None
            and observed_event_client_order_id != attempt["client_order_id"]
        ):
            self._require_reconciliation(
                intent_id,
                account,
                category="BROKER_EVENT_CLIENT_ID_CONFLICT",
                entity_type="BROKER_ORDER",
                entity_key=event.broker_order_id,
                details={
                    "durable_client_order_id": attempt["client_order_id"],
                    "observed_client_order_id": observed_event_client_order_id,
                    "event_id": event.id,
                },
            )
            self._record_recovery_action(
                intent=intent,
                account=account,
                action_key=f"EVENT_CLIENT_ID_CONFLICT:{event.broker_order_id}:{event.dedupe_key}",
                state="RECONCILIATION_REQUIRED",
                summary="A normalized broker event carried a conflicting client order ID; no state inference is allowed.",
                observed_positions=observed_positions,
                remaining_quantities=self._remaining_quantities(intent),
                allowed_next_steps=("refresh_broker_facts", "reconcile_broker_order", "operator_review"),
                metadata={"event_id": event.id, "error": "client order ID conflict"},
            )
            return self.recovery_status(intent_id, account=account)
        multiple_attempts = self._quarantine_multiple_attempts(intent_id, account)
        event_fill_identity_conflict = False
        for broker_fill in fills:
            if not isinstance(broker_fill, BrokerFill):
                event_fill_identity_conflict = True
                identity_mismatches = ["normalized_fill_type"]
            else:
                identity_mismatches: list[str] = []
                try:
                    self._validate_broker_fill_provenance(
                        attempt,
                        broker_fill,
                        expected_instrument_id=str(event_leg["instrument_id"]),
                    )
                except (KeyError, TypeError, ValueError) as exc:
                    identity_mismatches.append(f"provenance:{exc}")
                identity_mismatches.extend(
                    self._account_alias_mismatches(
                        account,
                        account_id=broker_fill.account_id,
                        metadata=broker_fill.metadata,
                    )
                )
                if broker_fill.account_id not in (None, account.id, account.external_account_id):
                    identity_mismatches.append(f"account_id={broker_fill.account_id}")
            if identity_mismatches:
                event_fill_identity_conflict = True
                self._require_reconciliation(
                    intent_id,
                    account,
                    category="BROKER_EVENT_FILL_IDENTITY_CONFLICT",
                    entity_type="BROKER_FILL",
                    entity_key=(broker_fill.dedupe_key if isinstance(broker_fill, BrokerFill) else event.broker_order_id),
                    details={
                        "event_id": event.id,
                        "dedupe_key": event.dedupe_key,
                        "mismatches": identity_mismatches,
                    },
                )
                self._record_recovery_action(
                    intent=self._required_intent(intent_id),
                    account=account,
                    action_key=(
                        f"EVENT_FILL_IDENTITY_CONFLICT:{event.broker_order_id}:"
                        f"{broker_fill.dedupe_key if isinstance(broker_fill, BrokerFill) else event.dedupe_key}"
                    ),
                    state="RECONCILIATION_REQUIRED",
                    summary="A replayed broker event carried fill account/provenance facts that did not match the authenticated attempt; no no-op or allocation is allowed.",
                    observed_positions=observed_positions,
                    remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                    allowed_next_steps=("refresh_broker_fills", "reconcile_broker_order", "operator_review"),
                    metadata={"event_id": event.id, "mismatches": identity_mismatches},
                )
        fill_fingerprint = self._fill_evidence_fingerprint(fills)
        event_fingerprint = self._event_evidence_fingerprint(event, fills)
        existing_event = self.repository.broker_order_event(event.broker_order_id, event.dedupe_key)
        if existing_event is not None:
            stored_metadata = existing_event.get("metadata") or {}
            # New rows keep the OMS fingerprints in dedicated columns.  The
            # metadata fallback is only for pre-migration rows and must never
            # cause provider data to be discarded.
            stored_fill_fingerprint = existing_event.get("oms_fill_fingerprint")
            stored_fingerprint = existing_event.get("oms_event_fingerprint")
            if not stored_fill_fingerprint or not stored_fingerprint:
                legacy_fill, legacy_event = self._event_fingerprints_from_metadata(stored_metadata)
                stored_fill_fingerprint = stored_fill_fingerprint or legacy_fill
                stored_fingerprint = stored_fingerprint or legacy_event
            legacy_fill_fingerprint = self._legacy_fill_evidence_fingerprint(fills)

            def legacy_field_matches(key: str, current: object, *, stored_key: str | None = None) -> bool:
                name = stored_key or key
                if name not in stored_metadata:
                    # A legacy row may predate an optional field.  Its absence
                    # is compatible only when the replay does not introduce a
                    # value that the old row could not have agreed to.
                    return current in (None, False)
                stored = stored_metadata.get(name)
                if isinstance(current, Decimal):
                    current = str(current)
                if isinstance(current, BrokerOrderStatus):
                    current = current.value
                return str(stored) == str(current)

            legacy_reserved_metadata = {
                "external_order_id",
                "client_order_id",
                "cumulative_filled_quantity",
                "account_id",
                "external_account_id",
                "no_fill_asserted",
            }
            legacy_stored_user_metadata = {
                key: value
                for key, value in stored_metadata.items()
                if key not in legacy_reserved_metadata
                and not (
                    normalize_provider_key(key)
                    in {self._LEGACY_FILL_FINGERPRINT_KEY, self._LEGACY_EVENT_FINGERPRINT_KEY}
                    and self._is_generated_fingerprint(value)
                )
            }
            # Compare the pre-Stage-3 user payload with the same historical
            # shape that was persisted.  The normalized event above carries
            # private fingerprints, which did not exist in legacy rows and
            # must not turn an otherwise exact replay into a false conflict.
            legacy_current_user_metadata = {
                key: value
                for key, value in self._event_user_metadata(event.metadata).items()
            }

            legacy_fill_compatible = (
                stored_fill_fingerprint in {fill_fingerprint, legacy_fill_fingerprint}
                or (stored_fill_fingerprint is None and not fills)
            )
            # A pre-provenance event row cannot silently absorb a newly
            # normalized instrument identity.  Exact legacy replay remains
            # compatible only when the incoming historical shape also lacked
            # that field.
            if any(
                isinstance(item, BrokerFill) and item.instrument_id is not None
                for item in fills
            ) and stored_fingerprint is None:
                legacy_fill_compatible = False
            legacy_exact = (
                stored_fingerprint is None
                and legacy_fill_compatible
                and existing_event.get("event_type") == event.event_type
                and existing_event.get("broker_status") == (
                    event.broker_status.value if event.broker_status is not None else None
                )
                and existing_event.get("external_event_id") == event.external_event_id
                and existing_event.get("event_at") == event.event_at.isoformat()
                # Legacy replay compatibility is intentionally strict about
                # the historical receive timestamp.  NULL is unknown, not a
                # wildcard that can silently accept new fill evidence.
                and existing_event.get("received_at") == event.received_at.isoformat()
                and legacy_field_matches("external_order_id", event.external_order_id)
                and legacy_field_matches("client_order_id", event.client_order_id)
                and legacy_field_matches(
                    "cumulative_filled_quantity",
                    event.cumulative_filled_quantity,
                )
                and legacy_field_matches("account_id", event.account_id)
                and legacy_field_matches("external_account_id", event.external_account_id)
                and legacy_field_matches("no_fill_asserted", event.no_fill_asserted)
                and legacy_stored_user_metadata == legacy_current_user_metadata
            )

            def stable_fill_evidence_matches() -> bool:
                """Match immutable fill facts while ignoring local receive time.

                Stage 3 rows written before the stable fingerprint contract
                may have included the adapter's per-query ``received_at``.
                Reconstructing that historical hash is neither necessary nor
                safe: compare the durable fill rows' immutable broker facts
                directly instead.  A new fill, changed quantity/price, or
                changed provider metadata therefore cannot take this path.
                """
                durable_rows = self.repository.fills_for_broker_order(event.broker_order_id)
                if len(durable_rows) != len(fills):
                    return False
                incoming_by_key: dict[str, BrokerFill] = {}
                for item in fills:
                    if not isinstance(item, BrokerFill) or item.dedupe_key in incoming_by_key:
                        return False
                    incoming_by_key[item.dedupe_key] = item
                for stored in durable_rows:
                    key = str(stored.get("dedupe_key") or "")
                    item = incoming_by_key.get(key)
                    if item is None:
                        return False
                    metadata = stored.get("metadata")
                    if not isinstance(metadata, Mapping):
                        return False
                    stored_external_order_id = str(
                        metadata.get("_external_order_id") or attempt.get("external_order_id") or ""
                    )
                    stored_evidence_reference = metadata.get("_evidence_reference")
                    missing_instrument = object()
                    stored_instrument = metadata.get("_instrument_id", missing_instrument)
                    if stored_instrument is missing_instrument:
                        if item.instrument_id is not None:
                            return False
                    elif stored_instrument != item.instrument_id:
                        return False
                    missing_source_account = object()
                    stored_source_account = metadata.get(
                        self._BROKER_FILL_ACCOUNT_ID_KEY,
                        missing_source_account,
                    )
                    if stored_source_account is missing_source_account:
                        # Legacy fill rows did not persist whether the
                        # normalized source supplied an account identity.
                        # They can only take the compatibility path when the
                        # replay likewise supplies no source account.
                        if item.account_id is not None:
                            return False
                    elif stored_source_account != item.account_id:
                        return False
                    stored_user_metadata = {
                        name: value
                        for name, value in metadata.items()
                        if name not in {
                            "_external_order_id",
                            "_evidence_reference",
                            "_instrument_id",
                            self._BROKER_FILL_ACCOUNT_ID_KEY,
                        }
                    }
                    try:
                        if (
                            str(stored.get("external_fill_id") or "")
                            != str(item.external_fill_id or "")
                            or Decimal(str(stored.get("quantity"))) != item.quantity
                            or Decimal(str(stored.get("price"))) != item.price
                            or (
                                Decimal(str(stored.get("fee"))) if stored.get("fee") is not None else None
                            )
                            != item.fee
                            or stored.get("fee_currency") != item.fee_currency
                            or str(stored.get("filled_at")) != item.filled_at.isoformat()
                            or stored_external_order_id != item.external_order_id
                            or stored_evidence_reference
                            not in {None, item.evidence_reference, item.dedupe_key,
                                    f"{item.external_order_id}:{item.dedupe_key}"}
                            or dict(stored_user_metadata) != dict(item.metadata)
                        ):
                            return False
                    except (InvalidOperation, TypeError, ValueError):
                        return False
                return True

            stable_replay = (
                existing_event.get("id") == event.id
                and existing_event.get("event_type") == event.event_type
                and existing_event.get("broker_status") == (
                    event.broker_status.value if event.broker_status is not None else None
                )
                and existing_event.get("external_event_id") == event.external_event_id
                and existing_event.get("event_at") == event.event_at.isoformat()
                and legacy_field_matches("external_order_id", event.external_order_id)
                and legacy_field_matches("client_order_id", event.client_order_id)
                and legacy_field_matches("cumulative_filled_quantity", event.cumulative_filled_quantity)
                and legacy_field_matches("account_id", event.account_id)
                and legacy_field_matches("external_account_id", event.external_account_id)
                and legacy_field_matches("no_fill_asserted", event.no_fill_asserted)
                and legacy_stored_user_metadata == legacy_current_user_metadata
                and stable_fill_evidence_matches()
            )
            if (
                not event_fill_identity_conflict
                and (stored_fingerprint == event_fingerprint or legacy_exact or stable_replay)
            ):
                # Exact replay is a true no-op.  In particular, do not run a
                # terminal-state transition check against a previously
                # WORKING event whose full-fill evidence already advanced the
                # durable order to FILLED.
                return self.recovery_status(intent_id, account=account)
        try:
            event_recorded = self.repository.record_broker_order_event(
                event,
                oms_fill_fingerprint=fill_fingerprint,
                oms_event_fingerprint=event_fingerprint,
            )
        except (KeyError, ValueError, sqlite3.Error) as exc:
            self._require_reconciliation(
                intent_id,
                account,
                category="BROKER_EVENT_RECORD_FAILED",
                entity_type="BROKER_ORDER",
                entity_key=event.broker_order_id,
                details={"event_id": event.id, "dedupe_key": event.dedupe_key, "message": str(exc)},
            )
            self._record_recovery_action(
                intent=intent,
                account=account,
                action_key=f"EVENT_RECORD_FAILED:{event.broker_order_id}:{event.dedupe_key}",
                state="RECONCILIATION_REQUIRED",
                summary="A normalized broker event conflicted with durable event history; no state inference or remediation is allowed.",
                observed_positions=observed_positions,
                remaining_quantities=self._remaining_quantities(intent),
                allowed_next_steps=("refresh_broker_facts", "reconcile_broker_order", "operator_review"),
                metadata={"event_id": event.id, "dedupe_key": event.dedupe_key, "error": str(exc)},
            )
            return self.recovery_status(intent_id, account=account)
        if temporal_conflict:
            return self.recovery_status(intent_id, account=account)
        terminal_fill_contradiction = False
        if event.broker_status in {BrokerOrderStatus.REJECTED, BrokerOrderStatus.CANCELLED}:
            terminal_fill_contradiction = self._record_terminal_fill_contradiction(
                intent_id=intent_id,
                account=account,
                leg=event_leg,
                broker_order_id=event.broker_order_id,
                terminal_status=event.broker_status,
                source=f"event:{event.dedupe_key}",
                cumulative_filled_quantity=event.cumulative_filled_quantity,
                incoming_fill_count=len(fills),
                observed_positions=observed_positions,
            )
        # Only previously unseen fill evidence is a late fill.  An exact
        # replay of the same event/fill tuple must remain idempotent after the
        # first call has already durably accepted its evidence.  A replay with
        # changed evidence is rejected by the fingerprint above before this
        # check can bypass terminal validation.
        durable_fill_keys = {
            str(item["dedupe_key"])
            for item in self.repository.fills_for_broker_order(event.broker_order_id)
        }
        new_incoming_fills = tuple(
            item
            for item in fills
            if not isinstance(item, BrokerFill) or item.dedupe_key not in durable_fill_keys
        )
        late_fill_after_terminal = bool(new_incoming_fills) and (local_terminal_order or local_terminal_leg)
        if late_fill_after_terminal:
            self._force_leg_reconciliation(str(event_leg["id"]), str(event_leg["status"]))
            self._require_reconciliation(
                intent_id,
                account,
                category="LATE_FILL_AFTER_TERMINAL",
                entity_type="BROKER_ORDER",
                entity_key=event.broker_order_id,
                details={
                    "event_id": event.id,
                    "dedupe_key": event.dedupe_key,
                    "local_order_status": str(attempt["status"]),
                    "local_leg_status": str(event_leg["status"]),
                    "fill_count": len(fills),
                },
            )
            self._record_recovery_action(
                intent=self._required_intent(intent_id),
                account=account,
                action_key=f"LATE_FILL_AFTER_TERMINAL:{event.broker_order_id}:{event.dedupe_key}",
                state="RECONCILIATION_REQUIRED",
                summary="A fill-only broker event arrived after local terminal state; evidence is retained but requires explicit reconciliation before finalization.",
                observed_positions=observed_positions,
                remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                allowed_next_steps=("refresh_broker_facts", "refresh_broker_fills", "reconcile_broker_position", "operator_review"),
                    metadata={"broker_order_id": event.broker_order_id, "event_id": event.id, "fill_count": len(fills)},
            )
        block_incoming_fills = multiple_attempts or late_fill_after_terminal or terminal_fill_contradiction
        event_fill_validation = self._validate_event_fill_evidence(
            attempt=attempt,
            event=event,
            incoming_fills=fills,
        )
        if event_fill_validation is not None and not event_fill_validation["valid"]:
            self._record_event_fill_mismatch(
                intent=intent,
                account=account,
                attempt=attempt,
                event=event,
                observed_positions=observed_positions,
                validation=event_fill_validation,
            )
        target = event.broker_status
        if event_fill_validation is not None and not event_fill_validation["valid"]:
            # The event is durably recorded, but its lifecycle status cannot
            # be trusted until cumulative fill facts are reconciled.  Any
            # supplied normalized fills are still processed below.
            target = None
        if target is not None:
            current = BrokerOrderStatus(str(attempt["status"]))
            state_conflict = False
            terminal = {
                BrokerOrderStatus.FILLED,
                BrokerOrderStatus.REJECTED,
                BrokerOrderStatus.CANCELLED,
                BrokerOrderStatus.FAILED,
            }
            if current in terminal and target is not current:
                state_conflict = True
                self._require_reconciliation(
                    intent_id,
                    account,
                    category="BROKER_STATE_CONFLICT",
                    entity_type="BROKER_ORDER",
                    entity_key=event.broker_order_id,
                    details={"durable_status": current.value, "observed_status": target.value},
                )
                self._record_recovery_action(
                    intent=intent,
                    account=account,
                    action_key=f"STATE_CONFLICT:{event.broker_order_id}",
                    state="RECONCILIATION_REQUIRED",
                    summary="A late broker update conflicts with a durable terminal order state; operator review is required.",
                    observed_positions=observed_positions,
                    remaining_quantities=self._remaining_quantities(intent),
                    allowed_next_steps=("refresh_broker_facts", "reconcile_broker_order", "operator_review"),
                    metadata={"broker_order_id": event.broker_order_id, "durable_status": current.value, "observed_status": target.value},
                )
            elif current is not target or event.external_order_id:
                if current is BrokerOrderStatus.PREPARED and target not in {
                    BrokerOrderStatus.SUBMITTING,
                    BrokerOrderStatus.CANCELLED,
                    BrokerOrderStatus.FAILED,
                }:
                    self.repository.transition_broker_order(
                        event.broker_order_id,
                        BrokerOrderStatus.SUBMITTING,
                        now=self._now(),
                    )
                try:
                    self.repository.record_submission(
                        event.broker_order_id,
                        status=target,
                        external_order_id=event.external_order_id,
                        metadata={
                            "event_id": event.id,
                            "event_type": event.event_type,
                            "event_metadata": dict(event.metadata),
                            "observed_cumulative_filled_quantity": (
                                str(event.cumulative_filled_quantity)
                                if event.cumulative_filled_quantity is not None
                                else None
                            ),
                        },
                        now=self._now(),
                        submitted_at=event.event_at,
                    )
                except (ValueError, sqlite3.Error) as exc:
                    self._require_reconciliation(
                        intent_id,
                        account,
                        category="BROKER_STATE_CONFLICT",
                        entity_type="BROKER_ORDER",
                        entity_key=event.broker_order_id,
                        details={"message": str(exc), "observed_status": target.value},
                    )
                    self._record_recovery_action(
                        intent=intent,
                        account=account,
                        action_key=f"EVENT_STATE_CONFLICT:{event.broker_order_id}:{event.dedupe_key}",
                        state="RECONCILIATION_REQUIRED",
                        summary="A normalized broker event could not be applied to the durable order state; operator review is required.",
                        observed_positions=observed_positions,
                        remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                        allowed_next_steps=("refresh_broker_facts", "reconcile_broker_order", "operator_review"),
                        metadata={"broker_order_id": event.broker_order_id, "event_id": event.id, "error": str(exc)},
                    )
                    return self.recovery_status(intent_id, account=account)

            if state_conflict:
                # Persist the late contradiction, but do not feed its status
                # through terminal lifecycle transitions.  Supplied fills
                # below may still be recorded as independent broker facts.
                target = None

        if target is not None:
            leg = next(item for item in self._required_intent(intent_id)["legs"] if item["id"] == attempt["order_leg_id"])
            if target is BrokerOrderStatus.WORKING:
                if leg["status"] == LegStatus.PLANNED.value:
                    self.repository.transition_leg(str(leg["id"]), LegStatus.SUBMITTING, now=self._now())
                    self.repository.transition_leg(str(leg["id"]), LegStatus.WORKING, now=self._now())
                elif leg["status"] == LegStatus.SUBMITTING.value:
                    self.repository.transition_leg(str(leg["id"]), LegStatus.WORKING, now=self._now())
                self._record_recovery_action(
                    intent=intent,
                    account=account,
                    action_key=f"WAIT_FOR_BROKER:{event.broker_order_id}",
                    state="WORKING",
                    summary="Normalized broker update reports WORKING; poll again and do not submit a duplicate.",
                    observed_positions=observed_positions,
                    remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                    metadata={"broker_order_id": event.broker_order_id, "event_id": event.id},
                )
            elif target in {BrokerOrderStatus.PARTIALLY_FILLED, BrokerOrderStatus.FILLED}:
                has_fill_evidence = (
                    self._leg_has_partial_fill_evidence(str(leg["id"]), event.broker_order_id)
                    if target is BrokerOrderStatus.PARTIALLY_FILLED
                    else self._leg_has_fill_evidence(str(leg["id"]), event.broker_order_id)
                )
                if not has_fill_evidence:
                    self._mark_leg_reconciliation(str(leg["id"]), str(leg["status"]))
                if has_fill_evidence:
                    self._resolve_recovery_actions_for_order(
                        account,
                        intent_id,
                        event.broker_order_id,
                        action_prefixes=("WAIT_FOR_BROKER:", "FILL_EVIDENCE:"),
                    )
                    self._resolve_reconciliation_issue(
                        account.id,
                        f"FILL_EVIDENCE_REQUIRED:BROKER_ORDER:{event.broker_order_id}",
                        resolved_at=self._now(),
                    )
                else:
                    self._record_recovery_action(
                        intent=intent,
                        account=account,
                        action_key=f"FILL_EVIDENCE:{event.broker_order_id}",
                        state="PARTIALLY_FILLED" if target is BrokerOrderStatus.PARTIALLY_FILLED else "FILLED_PENDING_EVIDENCE",
                        summary="Broker status was received without sufficient normalized fill evidence; position ownership is unchanged.",
                        observed_positions=observed_positions,
                        remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                        allowed_next_steps=("refresh_broker_fills", "reconcile_broker_position", "operator_review"),
                        metadata={"broker_order_id": event.broker_order_id, "event_id": event.id},
                    )
                    self._require_reconciliation(
                        intent_id,
                        account,
                        category="FILL_EVIDENCE_REQUIRED",
                        entity_type="BROKER_ORDER",
                        entity_key=event.broker_order_id,
                        details={"broker_status": target.value, "event_id": event.id},
                    )
            elif target in {BrokerOrderStatus.REJECTED, BrokerOrderStatus.CANCELLED}:
                try:
                    durable_quantity = self._durable_filled_quantity(event.broker_order_id)
                except ValueError:
                    durable_quantity = Decimal("-1")
                clean_terminal = (
                    event.cumulative_filled_quantity == 0
                    and durable_quantity == 0
                    and not self.repository.fills_for_broker_order(event.broker_order_id)
                    and self._event_no_fill_is_authoritative(
                        event,
                        expected_quantity=Decimal(str(attempt["submitted_quantity"])),
                        expected_instrument_id=str(leg["instrument_id"]),
                    )
                )
                if terminal_fill_contradiction:
                    # The broker terminal status is retained above, but the
                    # leg remains reconciliation-required because incoming or
                    # cumulative fill evidence contradicts a clean terminal
                    # outcome.  In particular, never allocate supplied fills.
                    self._force_leg_reconciliation(str(leg["id"]), str(leg["status"]))
                elif clean_terminal:
                    terminal_leg = (
                        LegStatus.REJECTED
                        if target is BrokerOrderStatus.REJECTED
                        else LegStatus.CANCELLED
                    )
                    self._set_leg_terminal(str(leg["id"]), str(leg["status"]), terminal_leg)
                    self._resolve_recovery_actions_for_order(account, intent_id, event.broker_order_id)
                    self._record_recovery_action(
                        intent=intent,
                        account=account,
                        action_key=f"TERMINAL_CLEAN:{event.broker_order_id}",
                        state=terminal_leg.value,
                        summary=f"Broker event durably reports {target.value} with zero fills; no broker exposure was inferred.",
                        observed_positions=observed_positions,
                        remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                        allowed_next_steps=("operator_review",),
                        metadata={"broker_order_id": event.broker_order_id, "event_id": event.id},
                    )
                    self._resolve_recovery_action(
                        account.id,
                        intent_id,
                        f"TERMINAL_CLEAN:{event.broker_order_id}",
                        resolved_at=self._now(),
                    )
                else:
                    self._mark_leg_reconciliation(str(leg["id"]), str(leg["status"]))
                    if event.cumulative_filled_quantity and event.cumulative_filled_quantity > 0:
                        category = "TERMINAL_PARTIAL_FILL"
                        action_key = f"TERMINAL_PARTIAL:{event.broker_order_id}"
                        state = "RECONCILIATION_REQUIRED"
                        summary = "Terminal broker status includes a positive fill; residual exposure requires reconciliation."
                    else:
                        category = "TERMINAL_STATUS_WITHOUT_FILL_EVIDENCE"
                        action_key = f"TERMINAL_WITHOUT_EVIDENCE:{event.broker_order_id}"
                        state = "RECONCILIATION_REQUIRED"
                        summary = "Terminal broker status lacks complete fill/no-fill evidence; operator reconciliation is required."
                    self._record_recovery_action(
                        intent=intent,
                        account=account,
                        action_key=action_key,
                        state=state,
                        summary=summary,
                        observed_positions=observed_positions,
                        remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                        allowed_next_steps=("refresh_broker_facts", "refresh_broker_fills", "reconcile_broker_position", "operator_review"),
                        metadata={"broker_order_id": event.broker_order_id, "event_id": event.id},
                    )
                    self._require_reconciliation(
                        intent_id,
                        account,
                        category=category,
                        entity_type="BROKER_ORDER",
                        entity_key=event.broker_order_id,
                        details={"broker_status": target.value, "event_id": event.id},
                    )
            elif target in {BrokerOrderStatus.UNKNOWN, BrokerOrderStatus.FAILED}:
                self._mark_leg_reconciliation(str(leg["id"]), str(leg["status"]))
                self._record_recovery_action(
                    intent=intent,
                    account=account,
                    action_key=f"UNKNOWN_ORDER:{event.broker_order_id}",
                    state="RECONCILIATION_REQUIRED",
                    summary="Normalized broker update is UNKNOWN/FAILED; no retry, cancel, hedge, or position inference is allowed.",
                    observed_positions=observed_positions,
                    remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                    allowed_next_steps=("refresh_broker_facts", "reconcile_broker_order", "operator_review"),
                    metadata={"broker_order_id": event.broker_order_id, "event_id": event.id, "broker_status": target.value},
                )
                self._require_reconciliation(
                    intent_id,
                    account,
                    category="UNKNOWN_BROKER_ORDER_STATE",
                    entity_type="BROKER_ORDER",
                    entity_key=event.broker_order_id,
                    details={"broker_status": target.value, "event_id": event.id},
                )

        if block_incoming_fills:
            fills = ()
        for broker_fill in fills:
            if not isinstance(broker_fill, BrokerFill):
                self._require_reconciliation(
                    intent_id,
                    account,
                    category="AMBIGUOUS_FILL_MATCH",
                    entity_type="BROKER_FILL",
                    entity_key=event.broker_order_id,
                    details={"message": "normalized event fill has an invalid type"},
                )
                continue
            fill_alias_mismatches = self._account_alias_mismatches(
                account,
                metadata=broker_fill.metadata,
            )
            if fill_alias_mismatches:
                self._record_account_identity_mismatch(
                    intent=intent,
                    supplied_account_id=account.id,
                    observed_account_id=self._fill_account_id(broker_fill) or fill_alias_mismatches[0],
                    source=f"event-fill-alias:{broker_fill.dedupe_key}",
                )
                self._require_reconciliation(
                    intent_id,
                    account,
                    category="BROKER_FILL_ACCOUNT_ID_CONFLICT",
                    entity_type="BROKER_FILL",
                    entity_key=broker_fill.dedupe_key,
                    details={"mismatches": fill_alias_mismatches, "external_order_id": broker_fill.external_order_id},
                )
                self._record_recovery_action(
                    intent=self._required_intent(intent_id),
                    account=account,
                    action_key=f"FILL_ACCOUNT_ID_CONFLICT:{broker_fill.dedupe_key}",
                    state="RECONCILIATION_REQUIRED",
                    summary="Broker event fill carried conflicting account aliases; no allocation was accepted.",
                    observed_positions=self._observe_positions(account)[0],
                    remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                    allowed_next_steps=("refresh_broker_fills", "reconcile_broker_order", "operator_review"),
                    metadata={"mismatches": fill_alias_mismatches, "external_order_id": broker_fill.external_order_id},
                )
                continue
            observed_fill_account_id = self._fill_account_id(broker_fill)
            if observed_fill_account_id is not None and observed_fill_account_id not in {
                account.id,
                account.external_account_id,
            }:
                self._record_account_identity_mismatch(
                    intent=intent,
                    supplied_account_id=account.id,
                    observed_account_id=observed_fill_account_id,
                    source=f"event-fill:{broker_fill.dedupe_key}",
                )
                continue
            try:
                self._apply_normalized_fill(
                    account=account,
                    intent_id=intent_id,
                    broker_order_id=event.broker_order_id,
                    broker_fill=broker_fill,
                )
            except (KeyError, ValueError, InvalidOperation, sqlite3.Error) as exc:
                self._require_reconciliation(
                    intent_id,
                    account,
                    category="AMBIGUOUS_FILL_MATCH",
                    entity_type="BROKER_FILL",
                    entity_key=broker_fill.dedupe_key,
                    details={"message": str(exc), "external_order_id": broker_fill.external_order_id},
                )
        recovered = self._required_intent(intent_id)
        self._resolve_event_fill_evidence(
            account=account,
            intent_id=intent_id,
            event=event,
        )
        if event.broker_status is BrokerOrderStatus.PARTIALLY_FILLED:
            current_leg = next(
                item
                for item in self._required_intent(intent_id)["legs"]
                if item["id"] == attempt["order_leg_id"]
            )
            if self._leg_has_partial_fill_evidence(
                str(current_leg["id"]),
                event.broker_order_id,
            ):
                self._resolve_recovery_actions_for_order(
                    account,
                    intent_id,
                    event.broker_order_id,
                    action_prefixes=("WAIT_FOR_BROKER:", "FILL_EVIDENCE:"),
                )
                self._resolve_reconciliation_issue(
                    account.id,
                    f"FILL_EVIDENCE_REQUIRED:BROKER_ORDER:{event.broker_order_id}",
                    resolved_at=self._now(),
                )
        if any(leg["status"] == LegStatus.FILLED.value for leg in recovered["legs"]):
            self._resolve_recovery_actions_for_order(
                account,
                intent_id,
                event.broker_order_id,
                action_prefixes=("WAIT_FOR_BROKER:", "FILL_EVIDENCE:"),
            )
            self._resolve_reconciliation_issue(
                account.id,
                f"FILL_EVIDENCE_REQUIRED:BROKER_ORDER:{event.broker_order_id}",
                resolved_at=self._now(),
            )
        if self._all_legs_have_fill_evidence(intent_id):
            self._resolve_exact_fill_query_issue(account, intent_id)
        self._finalize_intent_status(intent_id, account)
        return self.recovery_status(intent_id, account=account)

    def apply_fill(self, fill: Fill, *, now: datetime | None = None) -> bool:
        """Apply a caller-supplied fill only after full durable ownership checks.

        This public compatibility API has no separate ``Account`` argument,
        so it derives the account from the persisted broker attempt.  It must
        not become an escape hatch that attaches an arbitrary fill to a
        terminal order or aggregates replacement attempts.
        """
        intent_id = self.repository.intent_id_for_broker_order(fill.broker_order_id)
        if intent_id is None:
            raise OMSExecutionError(f"unknown broker order for fill: {fill.broker_order_id}")
        intent = self._required_intent(intent_id)
        attempt = self.repository.get_broker_order(fill.broker_order_id)
        if attempt is None:
            raise OMSExecutionError(f"unknown broker order for fill: {fill.broker_order_id}")
        if str(attempt.get("order_leg_id")) != fill.order_leg_id:
            raise OMSExecutionError("fill broker order does not belong to the supplied logical leg")
        if str(attempt.get("account_id")) != str(intent.get("account_id")):
            self._record_account_identity_mismatch(
                intent=intent,
                supplied_account_id=str(attempt.get("account_id")),
                observed_account_id=str(intent.get("account_id")),
                source=f"apply_fill:{fill.broker_order_id}",
            )
            raise OMSExecutionError("fill broker order account does not match intent account")
        account = self.repository.get_account(str(intent["account_id"]))
        if account is None:
            raise OMSExecutionError(f"unknown persisted account for intent: {intent_id}")
        canonical = self._canonical_account_for_intent(
            intent,
            supplied=None,
            source=f"apply_fill:{fill.broker_order_id}",
        )
        if canonical is None:
            raise OMSExecutionError("fill cannot be applied without an enabled canonical account")
        account = canonical
        if not self._validate_persisted_execution_policy(intent, account, source=f"apply_fill:{fill.broker_order_id}"):
            raise OMSExecutionError("fill cannot be applied with an invalid persisted execution policy")
        self._ensure_terminal_history_safety(account)
        alias_mismatches = self._account_alias_mismatches(account, metadata=fill.metadata)
        if alias_mismatches:
            self._record_account_identity_mismatch(
                intent=intent,
                supplied_account_id=account.id,
                observed_account_id=alias_mismatches[0],
                source=f"apply_fill:{fill.dedupe_key}",
            )
            raise OMSExecutionError("fill account aliases do not match intent account")
        if self._enforce_unique_external_order_claim(
            intent_id=intent_id,
            account=account,
            external_order_id=attempt.get("external_order_id") or fill.external_order_id,
            leg_id=fill.order_leg_id,
            source="public_apply_fill",
        ):
            raise OMSExecutionError("duplicate broker order claims require reconciliation")
        if self._quarantine_multiple_attempts(intent_id, account):
            raise OMSExecutionError("multiple broker attempts for one leg require reconciliation")

        # ``Fill`` is a public value object and can be fabricated by any
        # caller.  Even matching IDs are not proof that a provider emitted
        # the evidence.  Push/poll ingestion creates the trusted capability
        # only after validating a normalized BrokerFill; this compatibility
        # method is therefore deprecated and fail-closed for a fully shaped
        # direct fill rather than allocating from caller data.
        evidence_reference_is_bound = fill.evidence_reference in {
            fill.dedupe_key,
            f"{attempt.get('external_order_id')}:{fill.dedupe_key}",
        }
        if evidence_reference_is_bound:
            details = {
                "broker_order_id": fill.broker_order_id,
                "dedupe_key": fill.dedupe_key,
                "reason": "public direct fill path is not a validated broker evidence route",
            }
            self._require_reconciliation(
                intent_id,
                account,
                category="PUBLIC_APPLY_FILL_DISABLED",
                entity_type="BROKER_FILL",
                entity_key=fill.dedupe_key,
                details=details,
            )
            self._record_recovery_action(
                intent=self._required_intent(intent_id),
                account=account,
                action_key=f"PUBLIC_APPLY_FILL_DISABLED:{fill.broker_order_id}:{fill.dedupe_key}",
                state="RECONCILIATION_REQUIRED",
                summary="Direct caller-supplied fills are disabled; use validated broker poll/event ingestion.",
                observed_positions=self._observe_positions(account)[0],
                remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                allowed_next_steps=("refresh_broker_fills", "reconcile_broker_order", "operator_review"),
                metadata=details,
            )
            raise OMSExecutionError("public direct fill path is disabled; use broker evidence ingestion")

        # Exact replay of a durable fill remains idempotent, including after
        # the logical leg has become terminal.  A new fill on that state is a
        # contradiction and is never allowed to allocate silently.
        existing = next(
            (
                item
                for item in self.repository.fills_for_broker_order(fill.broker_order_id)
                if str(item.get("dedupe_key")) == fill.dedupe_key
            ),
            None,
        )
        if existing is not None:
            try:
                return self.repository.record_fill(
                    fill,
                    now=now or self._now(),
                    _validation_token=self.repository._fill_validation_capability(),
                )
            except (KeyError, ValueError, InvalidOperation, sqlite3.Error) as exc:
                self._require_reconciliation(
                    intent_id,
                    account,
                    category="PUBLIC_FILL_RECORD_FAILED",
                    entity_type="BROKER_FILL",
                    entity_key=fill.dedupe_key,
                    details={"broker_order_id": fill.broker_order_id, "message": str(exc)},
                )
                raise OMSExecutionError("durable fill replay conflicted") from exc

        leg = next((item for item in intent["legs"] if item["id"] == fill.order_leg_id), None)
        if leg is None:
            raise OMSExecutionError(f"unknown logical leg for fill: {fill.order_leg_id}")
        terminal_leg = str(leg.get("status")) in {
            LegStatus.FILLED.value,
            LegStatus.REJECTED.value,
            LegStatus.CANCELLED.value,
            LegStatus.FAILED.value,
        }
        terminal_intent = str(intent.get("status")) in {
            IntentStatus.FILLED.value,
            IntentStatus.COMPLETED.value,
            IntentStatus.REJECTED.value,
            IntentStatus.CANCELLED.value,
            IntentStatus.FAILED.value,
        }
        terminal_attempt = str(attempt.get("status")) in {
            BrokerOrderStatus.FILLED.value,
            BrokerOrderStatus.REJECTED.value,
            BrokerOrderStatus.CANCELLED.value,
            BrokerOrderStatus.FAILED.value,
        }
        if terminal_leg or terminal_intent or terminal_attempt:
            if str(attempt.get("status")) in {
                BrokerOrderStatus.REJECTED.value,
                BrokerOrderStatus.CANCELLED.value,
            } or str(leg.get("status")) in {
                LegStatus.REJECTED.value,
                LegStatus.CANCELLED.value,
            }:
                self._record_terminal_fill_contradiction(
                    intent_id=intent_id,
                    account=account,
                    leg=leg,
                    broker_order_id=fill.broker_order_id,
                    terminal_status=str(attempt.get("status")),
                    source=f"apply:{fill.dedupe_key}",
                    cumulative_filled_quantity=Decimal(str(leg.get("cumulative_filled_quantity", "0"))),
                    incoming_fill_count=1,
                    observed_positions=self._observe_positions(account)[0],
                )
            else:
                self._force_leg_reconciliation(str(leg["id"]), str(leg["status"]))
                self._require_reconciliation(
                    intent_id,
                    account,
                    category="LATE_FILL_AFTER_TERMINAL",
                    entity_type="BROKER_ORDER",
                    entity_key=fill.broker_order_id,
                    details={"source": "apply_fill", "dedupe_key": fill.dedupe_key},
                )
                self._record_recovery_action(
                    intent=self._required_intent(intent_id),
                    account=account,
                    action_key=f"LATE_FILL_AFTER_TERMINAL:{fill.broker_order_id}:apply:{fill.dedupe_key}",
                    state="RECONCILIATION_REQUIRED",
                    summary="A caller supplied a new fill after terminal order state; explicit reconciliation is required.",
                    observed_positions=self._observe_positions(account)[0],
                    remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                    allowed_next_steps=("refresh_broker_fills", "reconcile_broker_order", "operator_review"),
                    metadata={"broker_order_id": fill.broker_order_id, "dedupe_key": fill.dedupe_key},
                )
            raise OMSExecutionError("cannot apply a new fill to terminal order state")

        if (
            fill.account_id is None
            or fill.external_order_id is None
            or fill.evidence_reference is None
        ):
            details = {
                "broker_order_id": fill.broker_order_id,
                "dedupe_key": fill.dedupe_key,
                "missing_account_id": fill.account_id is None,
                "missing_external_order_id": fill.external_order_id is None,
                "missing_evidence_reference": fill.evidence_reference is None,
            }
            self._require_reconciliation(
                intent_id,
                account,
                category="PUBLIC_FILL_PROVENANCE_MISSING",
                entity_type="BROKER_FILL",
                entity_key=fill.dedupe_key,
                details=details,
            )
            self._record_recovery_action(
                intent=self._required_intent(intent_id),
                account=account,
                action_key=f"PUBLIC_FILL_PROVENANCE_MISSING:{fill.broker_order_id}:{fill.dedupe_key}",
                state="RECONCILIATION_REQUIRED",
                summary="A public fill must carry explicit normalized broker-order and evidence identity; no allocation was accepted.",
                observed_positions=self._observe_positions(account)[0],
                remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                allowed_next_steps=("refresh_broker_fills", "reconcile_broker_order", "operator_review"),
                metadata=details,
            )
            raise OMSExecutionError("public fill is missing exact broker evidence identity")

        try:
            return self.repository.record_fill(
                fill,
                now=now or self._now(),
                _validation_token=self.repository._fill_validation_capability(),
            )
        except (KeyError, ValueError, InvalidOperation, sqlite3.Error) as exc:
            self._require_reconciliation(
                intent_id,
                account,
                category="PUBLIC_FILL_RECORD_FAILED",
                entity_type="BROKER_FILL",
                entity_key=fill.dedupe_key,
                details={"broker_order_id": fill.broker_order_id, "message": str(exc)},
            )
            self._record_recovery_action(
                intent=self._required_intent(intent_id),
                account=account,
                action_key=f"PUBLIC_FILL_RECORD_FAILED:{fill.broker_order_id}:{fill.dedupe_key}",
                state="RECONCILIATION_REQUIRED",
                summary="A caller-supplied fill could not be durably validated or recorded; no remediation is allowed.",
                observed_positions=self._observe_positions(account)[0],
                remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                allowed_next_steps=("refresh_broker_fills", "reconcile_broker_order", "operator_review"),
                metadata={"broker_order_id": fill.broker_order_id, "error": str(exc)},
            )
            raise OMSExecutionError("durable fill recording failed") from exc

    def complete_intent(self, intent_id: str) -> dict:
        self._startup_safety_audit()
        intent = self._required_intent(intent_id)
        account = self._canonical_account_for_intent(
            intent,
            supplied=None,
            source="complete_intent",
        )
        if account is None:
            raise OMSExecutionError("cannot complete intent without an enabled canonical account")
        if not self._validate_persisted_execution_policy(intent, account, source="complete_intent"):
            raise OMSExecutionError("cannot complete intent with an invalid persisted execution policy")
        # Public completion must perform safety scans before inspecting or
        # promoting the local terminal status.  Aged terminal history or a
        # sibling multi-attempt leg is an account-wide blocker.
        self._ensure_terminal_history_safety(account)
        if self._quarantine_multiple_attempts(intent_id, account):
            raise OMSExecutionError("cannot complete intent with multiple broker attempts for a leg")
        intent = self._required_intent(intent_id)
        if intent["status"] != IntentStatus.FILLED.value:
            raise OMSExecutionError("only a fully filled intent can be completed")
        for leg in intent["legs"]:
            if leg["status"] != LegStatus.FILLED.value:
                raise OMSExecutionError("cannot complete intent with a non-filled leg")
            try:
                if Decimal(str(leg["cumulative_filled_quantity"])) != Decimal(str(leg["quantity"])):
                    raise OMSExecutionError("cannot complete intent with incomplete fill quantity")
            except (InvalidOperation, TypeError, ValueError) as exc:
                raise OMSExecutionError("cannot complete intent with invalid fill quantity") from exc
            attempts = self.repository.broker_orders_for_leg(str(leg["id"]))
            if len(attempts) != 1 or not self._attempt_has_complete_fill_evidence(attempts[0], leg):
                self._require_reconciliation(
                    intent_id,
                    account,
                    category="FILL_EVIDENCE_REQUIRED",
                    entity_type="ORDER_LEG",
                    entity_key=str(leg["id"]),
                    details={"attempt_count": len(attempts), "reason": "attempt-scoped evidence required"},
                )
                self._record_recovery_action(
                    intent=self._required_intent(intent_id),
                    account=account,
                    action_key=f"FILL_EVIDENCE_REQUIRED:{leg['id']}",
                    state="RECONCILIATION_REQUIRED",
                    summary="Completion requires complete durable fill evidence owned by the one supported broker attempt.",
                    observed_positions=self._observe_positions(account)[0],
                    remaining_quantities=self._remaining_quantities(self._required_intent(intent_id)),
                    allowed_next_steps=("refresh_broker_fills", "reconcile_broker_order", "operator_review"),
                    metadata={"leg_id": str(leg["id"])},
                )
                raise OMSExecutionError("cannot complete intent without attempt-scoped fill evidence")
        open_issues = self.repository.open_reconciliation_issues(str(intent["account_id"]))
        open_actions = self.repository.open_recovery_actions(str(intent["account_id"]))
        if open_issues or open_actions:
            raise OMSExecutionError(
                "cannot complete intent while reconciliation issues or recovery actions remain open"
            )
        self.repository.transition_intent(intent_id, IntentStatus.COMPLETED, now=self._now())
        return self._required_intent(intent_id)

    def _required_intent(self, intent_id: str) -> dict:
        intent = self.repository.get_intent(intent_id)
        if intent is None:
            raise KeyError(f"Unknown intent: {intent_id}")
        return intent
