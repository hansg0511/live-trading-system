"""Durable, broker-neutral validation for supervised Stage 6 SIM sessions.

This module is deliberately a validation boundary.  It does not submit,
cancel, replace, retry, or recover orders.  The existing Stage6PilotRunner
and GenericOMS remain the only execution path; this validator consumes an
operator/audit evidence document produced around that path.

Evidence is intentionally strict.  A missing, malformed, contradictory, or
legacy-only observation is retained as ``INVALID``/unqualified instead of
being guessed into a clean pass.  The repository stores the original
canonical evidence so later status reports are deterministic and auditable.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
import json
from decimal import Decimal, InvalidOperation
from typing import Any


class Stage6SessionOutcome(str, Enum):
    """Durable result classes for one supervised validation session."""

    CLEAN_PASS = "CLEAN_PASS"
    FAILED = "FAILED"
    INVALID = "INVALID"


class Stage6EvidenceClass(str, Enum):
    """Evidence provenance used to keep old reports out of completion counts."""

    DURABLE = "DURABLE"
    LEGACY_VERIFIED_EVIDENCE = "LEGACY_VERIFIED_EVIDENCE"


class Stage6ValidationError(ValueError):
    """Raised when a validation evidence document is not structurally safe."""


_TERMINAL_ORDER_STATUSES = {"FILLED", "COMPLETED"}
_RTH_STATES = {"RTH", "REGULAR", "MORNING", "AFTERNOON"}
_EXECUTION_PATHS = {
    "STAGE6PILOTRUNNER->GENERICOMS",
    "STAGE6_PILOT_RUNNER_GENERIC_OMS",
}


def _mapping(value: object, name: str) -> Mapping[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise Stage6ValidationError(f"{name} must be an object")
    return value


def _required_text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise Stage6ValidationError(f"{name} is required")
    return value.strip()


def _optional_text(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, name)


def _bool(value: object, name: str, *, default: bool | None = None) -> bool:
    if value is None and default is not None:
        return default
    if type(value) is not bool:
        raise Stage6ValidationError(f"{name} must be true or false")
    return bool(value)


def _nonnegative_int(value: object, name: str, *, default: int | None = None) -> int:
    if value is None and default is not None:
        return default
    if type(value) is not int or value < 0:
        raise Stage6ValidationError(f"{name} must be a non-negative integer")
    return value


def _timestamp(value: object, name: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise Stage6ValidationError(f"{name} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise Stage6ValidationError(f"{name} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise Stage6ValidationError(f"{name} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _canonical(value: Any) -> Any:
    """Return JSON primitives with deterministic mapping order."""

    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {
            str(key): _canonical(item)
            for key, item in sorted(value.items(), key=lambda mapping_item: str(mapping_item[0]))
        }
    if isinstance(value, (tuple, list)):
        return [_canonical(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted(_canonical(item) for item in value)
    return value


def canonical_evidence_json(value: Mapping[str, Any]) -> str:
    """Serialize evidence for hashing and immutable persistence."""

    if not isinstance(value, Mapping):
        raise Stage6ValidationError("evidence must be an object")
    return json.dumps(_canonical(value), sort_keys=True, separators=(",", ":"), default=str)


def _string_tuple(value: object, name: str, *, required: bool = False) -> tuple[str, ...]:
    if value is None:
        if required:
            raise Stage6ValidationError(f"{name} is required")
        return ()
    if type(value) is not list or any(not isinstance(item, str) or not item.strip() for item in value):
        raise Stage6ValidationError(f"{name} must be an array of non-empty strings")
    result = tuple(str(item).strip() for item in value)
    if required and not result:
        raise Stage6ValidationError(f"{name} must not be empty")
    return result


def _items(value: object, name: str, *, required: bool = False) -> tuple[Mapping[str, Any], ...]:
    if value is None:
        if required:
            raise Stage6ValidationError(f"{name} is required")
        return ()
    if type(value) is not list or any(not isinstance(item, Mapping) for item in value):
        raise Stage6ValidationError(f"{name} must be an array of objects")
    result = tuple(item for item in value if isinstance(item, Mapping))
    if required and not result:
        raise Stage6ValidationError(f"{name} must not be empty")
    return result


def _count_from(mapping: Mapping[str, Any], name: str, *keys: str, default: int | None = None) -> int:
    for key in keys:
        if key in mapping:
            return _nonnegative_int(mapping.get(key), f"{name}.{key}")
    return _nonnegative_int(None, name, default=default)


def _decimal(value: object, name: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise Stage6ValidationError(f"{name} must be numeric") from exc
    if not parsed.is_finite():
        raise Stage6ValidationError(f"{name} must be finite")
    return parsed


def _is_zero(value: object, name: str) -> bool:
    return _decimal(value, name) == 0


def _same_quantity(requested: object, filled: object, name: str) -> bool:
    return _decimal(requested, f"{name}.requested") == _decimal(filled, f"{name}.filled")


def _facts(
    mapping: Mapping[str, Any] | None,
    name: str,
    reasons: list[str],
    *,
    account_id: str | None = None,
) -> Mapping[str, Any] | None:
    if mapping is None:
        reasons.append(f"{name} missing")
        return None
    try:
        complete = _bool(mapping.get("complete"), f"{name}.complete")
    except Stage6ValidationError as exc:
        reasons.append(str(exc))
        return mapping
    if not complete:
        reasons.append(f"{name} is not complete")
    if account_id is not None and "account_id" in mapping and str(mapping.get("account_id")) != account_id:
        reasons.append(f"{name} belongs to the wrong account")
    captured = mapping.get("captured_at")
    if captured is None:
        reasons.append(f"{name}.captured_at missing")
    else:
        try:
            _timestamp(captured, f"{name}.captured_at")
        except Stage6ValidationError as exc:
            reasons.append(str(exc))
    if mapping.get("flat") is not True:
        reasons.append(f"{name} is not flat")
    try:
        open_order_count = _count_from(mapping, name, "open_order_count", "open_orders_count", default=None)
        if open_order_count != 0:
            reasons.append(f"{name} has {open_order_count} open order(s)")
    except Stage6ValidationError as exc:
        reasons.append(str(exc))
    open_orders = mapping.get("open_orders")
    if open_orders is not None:
        if type(open_orders) is not list:
            reasons.append(f"{name}.open_orders must be an array")
        elif open_orders:
            reasons.append(f"{name} contains open order rows")
    return mapping


def _validate_blocker_counts(mapping: Mapping[str, Any], name: str, reasons: list[str]) -> None:
    blockers = mapping.get("blockers")
    if blockers is not None:
        if not isinstance(blockers, Mapping):
            reasons.append(f"{name}.blockers must be an object")
        else:
            mapping = {**mapping, **blockers}
    for label, keys in (
        ("issues", ("issues", "open_issues", "open_issue_count")),
        ("actions", ("actions", "open_actions", "open_action_count")),
        ("unfinished intents", ("unfinished_intents", "unfinished", "unfinished_intent_count")),
    ):
        try:
            count = _count_from(mapping, name, *keys, default=None)
            if count != 0:
                reasons.append(f"{name} has {count} {label}")
        except Stage6ValidationError as exc:
            reasons.append(str(exc))


def _validate_rth(value: object, name: str, reasons: list[str]) -> None:
    if isinstance(value, bool):
        if not value:
            reasons.append(f"{name} is not regular US RTH")
        return
    if not isinstance(value, Mapping):
        reasons.append(f"{name} missing or malformed")
        return
    if value.get("observed") is not True and value.get("is_rth") is not True:
        reasons.append(f"{name}.observed must be true")
    state = value.get("market_state", value.get("session"))
    if state is not None and str(state).strip().upper() not in _RTH_STATES:
        reasons.append(f"{name} market state is not RTH: {state}")


def _validate_books(preflight: Mapping[str, Any], reasons: list[str]) -> set[str]:
    raw = preflight.get("books", preflight.get("book_allocations"))
    if isinstance(raw, Mapping):
        raw = list(raw.values())
    try:
        books = _items(raw, "preflight.books", required=True)
    except Stage6ValidationError as exc:
        reasons.append(str(exc))
        return set()
    if len(books) != 2:
        reasons.append("preflight.books must contain exactly two books")
    seen: set[str] = set()
    for index, book in enumerate(books):
        book_id = book.get("book_id", book.get("id"))
        if not isinstance(book_id, str) or not book_id.strip():
            reasons.append(f"preflight.books[{index}].book_id is required")
        elif str(book_id).strip() in seen:
            reasons.append(f"preflight.books contains duplicate book {book_id}")
        else:
            seen.add(str(book_id).strip())
        for field_name in ("allocation_valid", "mapping_valid"):
            if book.get(field_name) is not True:
                reasons.append(f"preflight.books[{index}].{field_name} must be true")
        if "exposure" in book:
            try:
                if not _is_zero(book["exposure"], f"preflight.books[{index}].exposure"):
                    reasons.append(f"preflight.books[{index}] is not flat")
            except Stage6ValidationError as exc:
                reasons.append(str(exc))
    return seen


def _order_key(order: Mapping[str, Any]) -> str | None:
    for key in ("external_order_id", "order_id", "broker_order_id"):
        value = order.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, int):
            return str(value)
    return None


def _validate_orders(
    section: Mapping[str, Any] | None,
    name: str,
    reasons: list[str],
    *,
    required: bool = True,
    allowed_intents: set[str] | None = None,
    account_id: str | None = None,
) -> tuple[Mapping[str, Any], ...]:
    if section is None:
        if required:
            reasons.append(f"{name} section missing")
        return ()
    try:
        expected = _items(section.get("expected_orders"), f"{name}.expected_orders", required=required)
        actual = _items(section.get("actual_orders"), f"{name}.actual_orders", required=required)
    except Stage6ValidationError as exc:
        reasons.append(str(exc))
        return ()
    if expected and len(actual) != len(expected):
        reasons.append(f"{name} actual order count does not match expected order count")
    expected_ids = [_order_key(item) for item in expected]
    actual_ids = [_order_key(item) for item in actual]
    if any(value is None for value in expected_ids):
        reasons.append(f"{name} expected orders lack durable broker identity")
    if len(set(value for value in expected_ids if value is not None)) != len(expected_ids):
        reasons.append(f"{name} expected orders contain duplicate broker identities")
    if any(value is None for value in actual_ids):
        reasons.append(f"{name} contains an order without durable broker identity")
    if len(set(value for value in actual_ids if value is not None)) != len(actual_ids):
        reasons.append(f"{name} contains duplicate broker order identities")
    if all(value is not None for value in expected_ids) and set(expected_ids) != set(actual_ids):
        reasons.append(f"{name} actual order identities do not match expected identities")
    for index, order in enumerate(actual):
        if order.get("attributable") is not True:
            reasons.append(f"{name}.actual_orders[{index}] is not attributable to this session")
        if str(order.get("status", "")).upper() not in _TERMINAL_ORDER_STATUSES:
            reasons.append(f"{name}.actual_orders[{index}] is not fully terminal")
        try:
            requested = order.get("quantity", order.get("submitted_quantity"))
            filled = order.get("filled_quantity", order.get("cumulative_filled_quantity"))
            if requested is None or filled is None or not _same_quantity(requested, filled, f"{name}.actual_orders[{index}]"):
                reasons.append(f"{name}.actual_orders[{index}] lacks complete cumulative fill evidence")
        except Stage6ValidationError as exc:
            reasons.append(str(exc))
        if not isinstance(order.get("intent_id"), str) or not str(order.get("intent_id")).strip():
            reasons.append(f"{name}.actual_orders[{index}] lacks intent attribution")
        elif allowed_intents is not None and str(order.get("intent_id")) not in allowed_intents:
            reasons.append(f"{name}.actual_orders[{index}] is attributed to an unexpected intent")
        if account_id is not None and "account_id" in order and str(order.get("account_id")) != account_id:
            reasons.append(f"{name}.actual_orders[{index}] is attributed to the wrong account")
    for field_name in ("unexpected_attempts", "duplicate_attempts"):
        if field_name not in section:
            reasons.append(f"{name}.{field_name} is required")
            continue
        try:
            if _nonnegative_int(section.get(field_name), f"{name}.{field_name}") != 0:
                reasons.append(f"{name}.{field_name} must be zero")
        except Stage6ValidationError as exc:
            reasons.append(str(exc))
    if section.get("fills_complete") is not True:
        reasons.append(f"{name}.fills_complete must be true")
    if section.get("orders_attributable") is not True:
        reasons.append(f"{name}.orders_attributable must be true")
    return actual


def _validate_recovery(
    recovery: Mapping[str, Any] | None,
    reasons: list[str],
    *,
    required_intent_ids: set[str] | None = None,
    required_order_ids: set[str] | None = None,
) -> None:
    if recovery is None:
        reasons.append("restart recovery evidence missing")
        return
    for field_name in (
        "performed",
        "fresh_process",
        "intents_preserved",
        "orders_preserved",
        "no_duplicate_attempts",
        "exposure_agrees",
    ):
        if recovery.get(field_name) is not True:
            reasons.append(f"recovery.{field_name} must be true")
    result = recovery.get("result", recovery.get("status"))
    if result is None:
        reasons.append("recovery result is missing")
    elif str(result).upper() not in {"PASS", "RECOVERED", "CLEAN"}:
        reasons.append(f"recovery result is not clean: {result}")
    process_id = recovery.get("process_id", recovery.get("fresh_process_id"))
    if not isinstance(process_id, str) or not process_id.strip():
        reasons.append("recovery fresh process identity missing")
    if recovery.get("captured_at") is None:
        reasons.append("recovery.captured_at missing")
    else:
        try:
            _timestamp(recovery.get("captured_at"), "recovery.captured_at")
        except Stage6ValidationError as exc:
            reasons.append(str(exc))
    try:
        preserved_intents = _string_tuple(
            recovery.get("preserved_intent_ids", recovery.get("intent_ids")),
            "recovery.preserved_intent_ids",
            required=True,
        )
        preserved_orders = _string_tuple(
            recovery.get("preserved_order_ids", recovery.get("order_ids")),
            "recovery.preserved_order_ids",
            required=True,
        )
        if len(set(preserved_intents)) != len(preserved_intents):
            reasons.append("recovery.preserved_intent_ids contains duplicates")
        if len(set(preserved_orders)) != len(preserved_orders):
            reasons.append("recovery.preserved_order_ids contains duplicates")
        if required_intent_ids is not None and not required_intent_ids.issubset(set(preserved_intents)):
            reasons.append("recovery did not preserve every entry intent identity")
        if required_order_ids is not None and not required_order_ids.issubset(set(preserved_orders)):
            reasons.append("recovery did not preserve every entry order identity")
    except Stage6ValidationError as exc:
        reasons.append(str(exc))


def _validate_final(
    final: Mapping[str, Any] | None,
    reasons: list[str],
    *,
    account_id: str | None = None,
    expected_book_ids: set[str] | None = None,
) -> None:
    if final is None:
        reasons.append("final evidence missing")
        return
    facts = final.get("fresh_facts", final.get("account_facts"))
    _facts(_mapping(facts, "final.fresh_facts"), "final.fresh_facts", reasons, account_id=account_id)
    for field_name in ("flat", "no_open_orders", "terminal_intents", "all_orders_attributable"):
        if final.get(field_name) is not True:
            reasons.append(f"final.{field_name} must be true")
    try:
        if _count_from(final, "final", "open_order_count", "open_orders_count", default=None) != 0:
            reasons.append("final has open orders")
    except Stage6ValidationError as exc:
        reasons.append(str(exc))
    _validate_blocker_counts(final, "final", reasons)
    exposures = final.get("book_exposure", final.get("book_exposures"))
    if not isinstance(exposures, Mapping) or len(exposures) != 2:
        reasons.append("final.book_exposure must contain both books")
    else:
        for book_id, value in exposures.items():
            try:
                if not _is_zero(value, f"final book {book_id} exposure"):
                    reasons.append(f"final book {book_id} is not flat")
            except Stage6ValidationError as exc:
                reasons.append(str(exc))
        if expected_book_ids is not None and {str(item) for item in exposures} != expected_book_ids:
            reasons.append("final.book_exposure book identities do not match preflight books")


def evaluate_stage6_session(evidence: Mapping[str, Any]) -> "Stage6SessionResult":
    """Validate an explicit Stage6 evidence document without side effects."""

    if not isinstance(evidence, Mapping):
        raise Stage6ValidationError("evidence must be an object")
    reasons: list[str] = []
    session_id = _required_text(evidence.get("session_id"), "session_id")
    account_id = _required_text(evidence.get("account_id"), "account_id")
    commit_sha = _required_text(evidence.get("commit_sha"), "commit_sha")
    compatibility = _required_text(
        evidence.get("execution_compatibility", evidence.get("compatibility_id")),
        "execution_compatibility",
    )
    if commit_sha.upper() in {"UNKNOWN", "UNSET", "N/A", "NA"}:
        reasons.append("commit_sha must identify the executed revision")
    if compatibility.upper() in {"UNKNOWN", "UNSET", "N/A", "NA"}:
        reasons.append("execution_compatibility must identify the executed semantics")
    environment = str(evidence.get("environment", evidence.get("account_environment", ""))).strip().upper()
    if environment != "SIM":
        reasons.append("environment must be SIM")
    execution_path = str(evidence.get("execution_path", "")).strip().upper().replace(" ", "")
    if execution_path not in _EXECUTION_PATHS:
        reasons.append("execution_path must be the existing Stage6PilotRunner -> GenericOMS path")
    if evidence.get("supervised") is not True:
        reasons.append("supervised must be true")
    if evidence.get("automatic") is True:
        reasons.append("automatic execution is not validation evidence")
    try:
        started_at = _timestamp(evidence.get("started_at"), "started_at")
    except Stage6ValidationError as exc:
        reasons.append(str(exc))
        started_at = datetime.fromtimestamp(0, tz=timezone.utc)
    try:
        completed_at = _timestamp(evidence.get("completed_at"), "completed_at")
    except Stage6ValidationError as exc:
        reasons.append(str(exc))
        completed_at = started_at
    if completed_at < started_at:
        reasons.append("completed_at precedes started_at")

    trading_date_raw = evidence.get("us_trading_date", evidence.get("trading_date"))
    try:
        trading_date = date.fromisoformat(_required_text(trading_date_raw, "us_trading_date"))
    except (Stage6ValidationError, ValueError) as exc:
        reasons.append(f"us_trading_date must be YYYY-MM-DD: {exc}")
        trading_date = date(1970, 1, 1)
    if trading_date.weekday() >= 5:
        reasons.append("us_trading_date must be a weekday US trading date")

    run_ids = _string_tuple(evidence.get("run_ids"), "run_ids", required=True)
    entry_intent_ids = _string_tuple(evidence.get("entry_intent_ids"), "entry_intent_ids", required=True)
    exit_intent_ids = _string_tuple(evidence.get("exit_intent_ids"), "exit_intent_ids", required=True)
    if len(entry_intent_ids) != 2:
        reasons.append("entry_intent_ids must contain exactly two intents")
    if len(exit_intent_ids) != 2:
        reasons.append("exit_intent_ids must contain exactly two intents")
    if len(set(entry_intent_ids)) != len(entry_intent_ids):
        reasons.append("entry_intent_ids contains duplicates")
    if len(set(exit_intent_ids)) != len(exit_intent_ids):
        reasons.append("exit_intent_ids contains duplicates")

    preflight = _mapping(evidence.get("preflight"), "preflight")
    expected_book_ids: set[str] | None = None
    if preflight is None:
        reasons.append("preflight section missing")
    else:
        identity = _mapping(preflight.get("account_identity"), "preflight.account_identity")
        if identity is None:
            reasons.append("preflight.account_identity missing")
        else:
            if not isinstance(identity.get("account_id"), str) or not identity.get("account_id", "").strip():
                reasons.append("preflight account identity account_id is required")
            elif str(identity.get("account_id")) != account_id:
                reasons.append("preflight account identity does not match account_id")
            if str(identity.get("environment", "")).upper() != "SIM":
                reasons.append("preflight account identity is not SIM")
        _facts(
            _mapping(preflight.get("fresh_facts", preflight.get("account_facts")), "preflight.fresh_facts"),
            "preflight.fresh_facts",
            reasons,
            account_id=account_id,
        )
        _validate_rth(preflight.get("rth", preflight.get("market_session")), "preflight.rth", reasons)
        expected_book_ids = _validate_books(preflight, reasons)
        _validate_blocker_counts(preflight, "preflight", reasons)
        if preflight.get("mappings_valid") is not True:
            reasons.append("preflight.mappings_valid must be true")
        if preflight.get("quantities_valid") is not True:
            reasons.append("preflight.quantities_valid must be true")
        if preflight.get("safety_gates_passed") is not True:
            reasons.append("preflight.safety_gates_passed must be true")

    entry = _mapping(evidence.get("entry"), "entry")
    if entry is None:
        reasons.append("entry section missing")
    else:
        expected_intents = _items(entry.get("expected_intents"), "entry.expected_intents", required=True)
        if len(expected_intents) != 2:
            reasons.append("entry.expected_intents must contain exactly two intents")
        expected_intent_values = {
            str(item.get("intent_id")).strip()
            for item in expected_intents
            if isinstance(item.get("intent_id"), str) and item.get("intent_id", "").strip()
        }
        if expected_intent_values != set(entry_intent_ids):
            reasons.append("entry.expected_intents do not match entry_intent_ids")
        actual_entry_orders = _validate_orders(
            entry,
            "entry",
            reasons,
            allowed_intents=set(entry_intent_ids),
            account_id=account_id,
        )

    recovery = _mapping(evidence.get("restart_recovery", evidence.get("recovery")), "recovery")
    if entry is not None:
        entry_order_ids = {
            order_id
            for order_id in (_order_key(order) for order in actual_entry_orders)
            if order_id is not None
        }
    else:
        entry_order_ids = set()
    _validate_recovery(
        recovery,
        reasons,
        required_intent_ids=set(entry_intent_ids),
        required_order_ids=entry_order_ids or None,
    )

    exit_section = _mapping(evidence.get("exit"), "exit")
    if exit_section is None:
        reasons.append("exit section missing")
    else:
        _validate_orders(
            exit_section,
            "exit",
            reasons,
            allowed_intents=set(exit_intent_ids),
            account_id=account_id,
        )
        if exit_section.get("current_exposure_inverse") is not True:
            reasons.append("exit.current_exposure_inverse must be true")
        if str(exit_section.get("submitted_via", "")).strip().upper().replace(" ", "") not in _EXECUTION_PATHS:
            reasons.append("exit.submitted_via must identify Stage6PilotRunner -> GenericOMS")
        partial = exit_section.get("delayed_partial_recovery", exit_section.get("partial_recovery"))
        if partial is not None:
            if not isinstance(partial, Mapping):
                reasons.append("exit.delayed_partial_recovery must be an object")
            elif partial.get("occurred") is True:
                for key in ("recovered", "no_duplicate_attempts", "no_blind_rescue"):
                    if partial.get(key) is not True:
                        reasons.append(f"exit.delayed_partial_recovery.{key} must be true")

    final = _mapping(evidence.get("final"), "final")
    _validate_final(final, reasons, account_id=account_id, expected_book_ids=expected_book_ids)

    manual_intervention = bool(evidence.get("manual_intervention", False))
    manual_flags = evidence.get("manual_flags", evidence.get("manual_actions", []))
    if isinstance(manual_flags, Mapping):
        manual_intervention = manual_intervention or any(value is True for value in manual_flags.values())
    elif isinstance(manual_flags, list):
        manual_intervention = manual_intervention or bool(manual_flags)
    elif manual_flags not in (None, False):
        reasons.append("manual_flags must be an object, array, or false")
    if manual_intervention:
        reasons.append("manual database change/direct broker rescue/operator intervention is not countable")

    evidence_class_raw = str(evidence.get("evidence_class", Stage6EvidenceClass.DURABLE.value)).strip().upper()
    if evidence_class_raw not in {
        Stage6EvidenceClass.DURABLE.value,
        Stage6EvidenceClass.LEGACY_VERIFIED_EVIDENCE.value,
    }:
        reasons.append(f"unsupported evidence_class: {evidence_class_raw}")
    legacy = bool(evidence.get("legacy", False)) or evidence_class_raw.startswith("LEGACY")
    if legacy:
        reasons.append("legacy evidence is retained but not countable as a clean supervised session")
        evidence_class = Stage6EvidenceClass.LEGACY_VERIFIED_EVIDENCE.value
    else:
        evidence_class = Stage6EvidenceClass.DURABLE.value

    explicit_reasons = evidence.get("failure_reasons", [])
    if explicit_reasons is not None:
        if type(explicit_reasons) is not list or any(not isinstance(item, str) or not item.strip() for item in explicit_reasons):
            reasons.append("failure_reasons must be an array of non-empty strings")
        else:
            reasons.extend(str(item).strip() for item in explicit_reasons)
    declared = str(evidence.get("declared_result", evidence.get("result", ""))).strip().upper()
    if declared == Stage6SessionOutcome.FAILED.value and not legacy:
        outcome = Stage6SessionOutcome.FAILED
    elif not reasons and not legacy:
        outcome = Stage6SessionOutcome.CLEAN_PASS
    else:
        outcome = Stage6SessionOutcome.INVALID

    audit_refs = _string_tuple(evidence.get("audit_refs"), "audit_refs")
    expected_entry = entry.get("expected_orders", ()) if entry is not None else ()
    actual_entry = entry.get("actual_orders", ()) if entry is not None else ()
    expected_exit = exit_section.get("expected_orders", ()) if exit_section is not None else ()
    actual_exit = exit_section.get("actual_orders", ()) if exit_section is not None else ()
    try:
        expected_entry_count = len(expected_entry) if type(expected_entry) is list else 0
        actual_entry_count = len(actual_entry) if type(actual_entry) is list else 0
        expected_exit_count = len(expected_exit) if type(expected_exit) is list else 0
        actual_exit_count = len(actual_exit) if type(actual_exit) is list else 0
    except TypeError:
        expected_entry_count = actual_entry_count = expected_exit_count = actual_exit_count = 0
    return Stage6SessionResult(
        session_id=session_id,
        us_trading_date=trading_date.isoformat(),
        started_at=started_at,
        completed_at=completed_at,
        commit_sha=commit_sha,
        execution_compatibility=compatibility,
        account_id=account_id,
        run_ids=run_ids,
        entry_intent_ids=entry_intent_ids,
        exit_intent_ids=exit_intent_ids,
        expected_entry_order_count=expected_entry_count,
        actual_entry_order_count=actual_entry_count,
        expected_exit_order_count=expected_exit_count,
        actual_exit_order_count=actual_exit_count,
        duplicate_attempt_count=sum(
            int(section.get("duplicate_attempts", 0))
            for section in (entry, exit_section)
            if section is not None and type(section.get("duplicate_attempts", 0)) is int
        ),
        preflight=dict(preflight or {}),
        restart_recovery=dict(recovery or {}),
        final=dict(final or {}),
        manual_intervention=manual_intervention,
        outcome=outcome,
        qualified=outcome is Stage6SessionOutcome.CLEAN_PASS,
        counted_for_completion=outcome is Stage6SessionOutcome.CLEAN_PASS,
        evidence_class=evidence_class,
        failure_reasons=tuple(dict.fromkeys(reasons)),
        audit_refs=audit_refs,
        evidence=dict(evidence),
    )


@dataclass(frozen=True, slots=True)
class Stage6SessionResult:
    """Immutable result retained for one supervised validation session."""

    session_id: str
    us_trading_date: str
    started_at: datetime
    completed_at: datetime
    commit_sha: str
    execution_compatibility: str
    account_id: str
    run_ids: tuple[str, ...]
    entry_intent_ids: tuple[str, ...]
    exit_intent_ids: tuple[str, ...]
    expected_entry_order_count: int
    actual_entry_order_count: int
    expected_exit_order_count: int
    actual_exit_order_count: int
    duplicate_attempt_count: int
    preflight: Mapping[str, Any]
    restart_recovery: Mapping[str, Any]
    final: Mapping[str, Any]
    manual_intervention: bool
    outcome: Stage6SessionOutcome
    qualified: bool
    counted_for_completion: bool
    evidence_class: str
    failure_reasons: tuple[str, ...] = ()
    audit_refs: tuple[str, ...] = ()
    evidence: Mapping[str, Any] = field(default_factory=dict)

    @property
    def result(self) -> str:
        return self.outcome.value

    @property
    def trading_date(self) -> str:
        return self.us_trading_date

    def as_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "us_trading_date": self.us_trading_date,
            "trading_date": self.us_trading_date,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat(),
            "commit_sha": self.commit_sha,
            "execution_compatibility": self.execution_compatibility,
            "account_id": self.account_id,
            "run_ids": list(self.run_ids),
            "entry_intent_ids": list(self.entry_intent_ids),
            "exit_intent_ids": list(self.exit_intent_ids),
            "expected_entry_order_count": self.expected_entry_order_count,
            "actual_entry_order_count": self.actual_entry_order_count,
            "expected_exit_order_count": self.expected_exit_order_count,
            "actual_exit_order_count": self.actual_exit_order_count,
            "duplicate_attempt_count": self.duplicate_attempt_count,
            "preflight": _canonical(self.preflight),
            "restart_recovery": _canonical(self.restart_recovery),
            "final": _canonical(self.final),
            "manual_intervention": self.manual_intervention,
            "result": self.result,
            "qualified": self.qualified,
            "counted_for_completion": self.counted_for_completion,
            "evidence_class": self.evidence_class,
            "failure_reasons": list(self.failure_reasons),
            "audit_refs": list(self.audit_refs),
            "evidence": _canonical(self.evidence),
        }


def stage6_completion_status(
    sessions: Sequence[Mapping[str, Any]],
    *,
    required_sessions: int = 3,
    execution_compatibility: str | None = None,
) -> dict[str, Any]:
    """Calculate completion from retained rows without mutating them."""

    if isinstance(required_sessions, bool) or required_sessions <= 0:
        raise ValueError("required_sessions must be positive")
    rows = [dict(row) for row in sessions]
    clean_rows = [
        row
        for row in rows
        if str(row.get("result", row.get("outcome", ""))).upper() == Stage6SessionOutcome.CLEAN_PASS.value
        and bool(row.get("qualified", False))
        and bool(row.get("counted_for_completion", False))
        and str(row.get("environment", "")).upper() == "SIM"
        and str(row.get("evidence_class", Stage6EvidenceClass.DURABLE.value)).upper()
        == Stage6EvidenceClass.DURABLE.value
        and str(row.get("execution_compatibility", "")).strip().upper()
        not in {"", "UNKNOWN", "UNSET", "N/A", "NA"}
    ]
    compatibilities = {str(row.get("execution_compatibility", "")).strip() for row in clean_rows}
    compatibilities.discard("")
    if execution_compatibility is not None:
        clean_rows = [
            row for row in clean_rows if str(row.get("execution_compatibility", "")) == execution_compatibility
        ]
        compatibilities = {str(row.get("execution_compatibility", "")) for row in clean_rows}
    dates: dict[str, Mapping[str, Any]] = {}
    duplicate_dates: list[str] = []
    for row in sorted(clean_rows, key=lambda item: (str(item.get("us_trading_date", item.get("trading_date", ""))), str(item.get("session_id", "")))):
        trading_date = str(row.get("us_trading_date", row.get("trading_date", "")))
        if not trading_date:
            duplicate_dates.append("<missing>")
            continue
        if trading_date in dates:
            duplicate_dates.append(trading_date)
            continue
        dates[trading_date] = row
    equivalent = len(compatibilities) <= 1
    count = len(dates) if equivalent else 0
    complete = equivalent and count >= required_sessions
    reasons: list[str] = []
    if not equivalent:
        reasons.append("clean sessions use incompatible execution identities")
    if count < required_sessions:
        reasons.append(f"requires {required_sessions} distinct US trading dates; only {count} qualified")
    if duplicate_dates:
        reasons.append("duplicate clean session date retained but not counted: " + ", ".join(sorted(set(duplicate_dates))))
    return {
        "status": "STAGE_6_COMPLETE" if complete else "STAGE_6_IN_PROGRESS",
        "complete": complete,
        "required_sessions": required_sessions,
        "qualified_clean_session_count": count,
        "qualified_clean_dates": sorted(dates),
        "execution_compatibilities": sorted(compatibilities),
        "duplicate_clean_dates": sorted(set(duplicate_dates)),
        "retained_session_count": len(rows),
        "reasons": reasons,
    }


__all__ = [
    "Stage6EvidenceClass",
    "Stage6SessionOutcome",
    "Stage6SessionResult",
    "Stage6ValidationError",
    "canonical_evidence_json",
    "evaluate_stage6_session",
    "stage6_completion_status",
]
