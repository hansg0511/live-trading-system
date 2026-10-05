"""Stage 7 local operational-readiness primitives.

This module is intentionally local and broker-neutral.  It does not create a
second ledger, generate signals, or submit orders.  It provides explicit mode
and configuration guards, a repository-backed operator view, one bounded
recovery/status tick, and a durable local audit envelope.  Actual SIM pilot
dispatch remains an explicit handoff to the Stage 6 runner.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
import json
from pathlib import Path
import uuid
from typing import Any, Protocol

from .domain import Account, Book, TradingEnvironment
from .oms import GenericOMS
from .repository import SQLiteTradingRepository


class OperationalSafetyError(ValueError):
    """Raised when an operational configuration would weaken a safety gate."""


class OperationalAuditError(RuntimeError):
    """A lifecycle/action result could not be durably recorded."""


class OperationalMode(str, Enum):
    """Modes deliberately exposed by the local operator layer.

    There is no REAL/LIVE member.  Production trading is structurally absent
    from this interface rather than being a mode that can be selected by
    configuration.
    """

    DRY_RUN = "DRY_RUN"
    SIM_ARMED = "SIM_ARMED"


class ServiceState(str, Enum):
    STOPPED = "STOPPED"
    RUNNING = "RUNNING"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _required_id(value: object, name: str) -> str:
    result = str(value).strip() if value is not None else ""
    if not result:
        raise OperationalSafetyError(f"{name} is required")
    return result


def _mapping(value: object, name: str) -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise OperationalSafetyError(f"{name} must be a mapping")
    return dict(value)


def _mode(value: object) -> OperationalMode:
    if isinstance(value, OperationalMode):
        return value
    raw = str(value or OperationalMode.DRY_RUN.value).strip().upper().replace("-", "_")
    if raw in {"REAL", "LIVE", "PRODUCTION", "REAL_SUBMIT", "LIVE_SUBMIT"}:
        raise OperationalSafetyError("REAL/LIVE operational mode is unavailable")
    try:
        return OperationalMode(raw)
    except ValueError as exc:
        raise OperationalSafetyError("mode must be DRY_RUN or SIM_ARMED") from exc


def _id_tuple(values: object, name: str) -> tuple[str, ...]:
    if values is None:
        return ()
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise OperationalSafetyError(f"{name} must be a sequence of IDs")
    result = tuple(_required_id(value, name) for value in values)
    if len(set(result)) != len(result):
        raise OperationalSafetyError(f"{name} must not contain duplicates")
    return result


def _safe_value(value: Any, *, key: str | None = None) -> Any:
    """Make status/audit data JSON-safe and redact common secret fields."""
    normalized_key = str(key or "").lower().replace("-", "_")
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
    if any(term in normalized_key for term in sensitive_terms):
        return "<redacted>"
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, Mapping):
        return {str(k): _safe_value(v, key=str(k)) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_safe_value(item) for item in value]
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return "<opaque>"


def _decode_field(row: Mapping[str, Any], key: str) -> Any:
    value = row.get(key)
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return {"_invalid_json": True, "raw": "<unparseable>"}
    return value


@dataclass(frozen=True, slots=True)
class OperationalConfig:
    """Validated local account/book/sleeve/pilot configuration.

    The account is supplied explicitly; no broker discovery is performed.
    ``SIM_ARMED`` is impossible without ``sim_arm=True`` and a SIM account.
    """

    account: Account
    book_ids: tuple[str, ...] = ()
    sleeve_ids: tuple[str, ...] = ()
    pilot_id: str | None = None
    mode: OperationalMode = OperationalMode.DRY_RUN
    sim_arm: bool = False
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.account, Account):
            raise OperationalSafetyError("account must be an Account")
        selected_mode = _mode(self.mode)
        object.__setattr__(self, "mode", selected_mode)
        object.__setattr__(self, "book_ids", _id_tuple(self.book_ids, "book_ids"))
        object.__setattr__(self, "sleeve_ids", _id_tuple(self.sleeve_ids, "sleeve_ids"))
        if self.pilot_id is not None:
            object.__setattr__(self, "pilot_id", _required_id(self.pilot_id, "pilot_id"))
        if not isinstance(self.sim_arm, bool):
            raise OperationalSafetyError("sim_arm must be a bool")
        if not isinstance(self.metadata, Mapping):
            raise OperationalSafetyError("metadata must be a mapping")
        object.__setattr__(self, "metadata", dict(self.metadata))
        if self.account.environment is not TradingEnvironment.SIM:
            raise OperationalSafetyError("only SIM accounts are available to Stage 7 operations")
        if not self.account.enabled:
            raise OperationalSafetyError("operational account must be enabled")
        if selected_mode is OperationalMode.SIM_ARMED and not self.sim_arm:
            raise OperationalSafetyError("SIM_ARMED requires explicit sim_arm=True")
        if selected_mode is OperationalMode.DRY_RUN and self.sim_arm:
            raise OperationalSafetyError("sim_arm cannot be set while mode is DRY_RUN")
        if self.pilot_id is not None:
            if not self.book_ids or not self.sleeve_ids:
                raise OperationalSafetyError("pilot configuration requires book_ids and sleeve_ids")
            if len(self.book_ids) != len(self.sleeve_ids):
                raise OperationalSafetyError("pilot books and sleeves must have equal cardinality")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "OperationalConfig":
        root = _mapping(value, "operational configuration")
        account_raw = _mapping(root.get("account"), "account")
        if not account_raw:
            raise OperationalSafetyError("account configuration is required")
        try:
            account = Account(
                id=account_raw.get("id", account_raw.get("account_id")),
                broker=account_raw.get("broker"),
                environment=account_raw.get("environment", "SIM"),
                external_account_id=account_raw.get(
                    "external_account_id",
                    account_raw.get("external_id"),
                ),
                base_currency=account_raw.get("base_currency", "USD"),
                enabled=account_raw.get("enabled", True),
                metadata=_mapping(account_raw.get("metadata"), "account.metadata"),
            )
        except (TypeError, ValueError) as exc:
            raise OperationalSafetyError(f"invalid account configuration: {exc}") from exc
        pilot_raw = _mapping(root.get("pilot"), "pilot")
        books = root.get("book_ids", root.get("books"))
        sleeves = root.get("sleeve_ids", root.get("sleeves"))
        if pilot_raw:
            books = pilot_raw.get("book_ids", pilot_raw.get("books", books))
            sleeves = pilot_raw.get("sleeve_ids", pilot_raw.get("sleeves", sleeves))
        mode = _mode(root.get("mode", OperationalMode.DRY_RUN.value))
        sim_arm = root.get("sim_arm", root.get("arm_sim", False))
        return cls(
            account=account,
            book_ids=_id_tuple(books, "book_ids"),
            sleeve_ids=_id_tuple(sleeves, "sleeve_ids"),
            pilot_id=pilot_raw.get("id", pilot_raw.get("pilot_id")) if pilot_raw else root.get("pilot_id"),
            mode=mode,
            sim_arm=sim_arm,
            metadata=_mapping(root.get("metadata"), "metadata"),
        )

    @classmethod
    def load(cls, path: str | Path) -> "OperationalConfig":
        config_path = Path(path)
        try:
            raw = json.loads(config_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise OperationalSafetyError(f"could not load JSON config: {config_path}") from exc
        return cls.from_mapping(raw)

    def validate_repository(self, repository: SQLiteTradingRepository) -> None:
        persisted = repository.get_account(self.account.id)
        if persisted is None:
            raise OperationalSafetyError(f"configured account is not persisted: {self.account.id}")
        if (
            persisted.broker != self.account.broker
            or persisted.environment is not self.account.environment
            or persisted.external_account_id != self.account.external_account_id
            or persisted.enabled is not self.account.enabled
        ):
            raise OperationalSafetyError("configured account does not match the canonical persisted identity")
        for book_id in self.book_ids:
            book = repository.get_book(book_id)
            if book is None or not bool(book.get("enabled")):
                raise OperationalSafetyError(f"configured book is missing or disabled: {book_id}")

    def as_dict(self) -> dict[str, Any]:
        return _safe_value(
            {
                "account": {
                    "id": self.account.id,
                    "broker": self.account.broker,
                    "environment": self.account.environment.value,
                    "external_account_id": self.account.external_account_id,
                    "base_currency": self.account.base_currency,
                    "enabled": self.account.enabled,
                },
                "book_ids": self.book_ids,
                "sleeve_ids": self.sleeve_ids,
                "pilot_id": self.pilot_id,
                "mode": self.mode.value,
                "sim_arm": self.sim_arm,
                "metadata": self.metadata,
            }
        )


@dataclass(frozen=True, slots=True)
class OperationalAuditEvent:
    """Durable, local, secret-redacted operational decision envelope."""

    account_id: str
    event_type: str
    mode: OperationalMode
    outcome: str
    summary: str
    details: Mapping[str, Any] = field(default_factory=dict)
    event_id: str = field(default_factory=lambda: f"stage7-event-{uuid.uuid4().hex}")
    occurred_at: datetime = field(default_factory=_utc_now)

    def __post_init__(self) -> None:
        object.__setattr__(self, "account_id", _required_id(self.account_id, "account_id"))
        object.__setattr__(self, "event_type", _required_id(self.event_type, "event_type"))
        object.__setattr__(self, "mode", _mode(self.mode))
        object.__setattr__(self, "outcome", _required_id(self.outcome, "outcome"))
        object.__setattr__(self, "summary", _required_id(self.summary, "summary"))
        if self.occurred_at.tzinfo is None or self.occurred_at.utcoffset() is None:
            raise OperationalSafetyError("operational event time must be timezone-aware")
        object.__setattr__(self, "occurred_at", self.occurred_at.astimezone(timezone.utc))
        object.__setattr__(self, "details", _safe_value(dict(self.details)))

    def persist(self, repository: SQLiteTradingRepository) -> str:
        return repository.record_operational_event(
            event_id=self.event_id,
            account_id=self.account_id,
            event_type=self.event_type,
            mode=self.mode.value,
            outcome=self.outcome,
            occurred_at=self.occurred_at,
            summary=self.summary,
            details=self.details,
        )


@dataclass(frozen=True, slots=True)
class OperationalAlert:
    key: str
    severity: str
    summary: str
    details: Mapping[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return _safe_value({
            "key": self.key,
            "severity": self.severity,
            "summary": self.summary,
            "details": self.details,
        })


class AlertSink(Protocol):
    """Local alert boundary; external notification providers are out of scope."""

    def emit(self, alert: OperationalAlert) -> None:
        ...


class CollectingAlertSink:
    """Small in-process alert sink useful for a local operator and tests."""

    def __init__(self) -> None:
        self.alerts: list[OperationalAlert] = []

    def emit(self, alert: OperationalAlert) -> None:
        self.alerts.append(alert)


class OperationalStatusReporter:
    """Render a single repository-backed operator/status snapshot."""

    def __init__(
        self,
        repository: SQLiteTradingRepository,
        *,
        config: OperationalConfig | None = None,
        oms: GenericOMS | None = None,
    ) -> None:
        self.repository = repository
        self.config = config
        # book_risk_status is a repository-only read.  The OMS object is not
        # used for broker calls here; a missing adapter is intentional.
        self.oms = oms

    def report(self, account: Account | None = None, *, lifecycle: ServiceState | str | None = None) -> dict[str, Any]:
        selected = account or (self.config.account if self.config else None)
        if selected is None:
            raise OperationalSafetyError("an account is required for an operational status report")
        canonical = self.repository.get_account(selected.id)
        if canonical is None:
            raise OperationalSafetyError(f"account is not persisted: {selected.id}")
        risk_status: Mapping[str, Any]
        try:
            if self.oms is not None:
                risk_status = self.oms.book_risk_status(account=canonical)
            else:
                # This is deliberately a fresh short-lived OMS with no adapter
                # calls; the method only reads durable book/position state.
                risk_status = GenericOMS(self.repository, object()).book_risk_status(account=canonical)
        except Exception as exc:
            risk_status = {"status": "STATUS_ERROR", "error": str(exc), "books": {}}

        configured_books = self.config.book_ids if self.config else ()
        all_book_ids = list(dict.fromkeys([*configured_books, *risk_status.get("books", {}).keys()]))
        books: list[dict[str, Any]] = []
        for book_id in all_book_ids:
            durable = self.repository.get_book(book_id) or {"id": book_id, "name": book_id, "enabled": False}
            risk = risk_status.get("books", {}).get(book_id, {"status": "NO_ACTIVE_ALLOCATION", "exposure": "0"})
            books.append({
                "id": book_id,
                "name": durable.get("name", book_id),
                "enabled": bool(durable.get("enabled")),
                "allocation": risk,
            })

        intent_rows = self.repository.book_intents(canonical.id)
        intents = [_safe_value({**row, "metadata": _decode_field(row, "metadata_json")}) for row in intent_rows]
        unfinished_intents = [
            {
                "intent_id": str(row["id"]),
                "book_id": row.get("book_id"),
                "action": row.get("action"),
                "status": str(row.get("status")),
                "reason": "intent remains reconciliation-required and is not a completed lifecycle",
            }
            for row in intent_rows
            if str(row.get("status")) == "RECONCILIATION_REQUIRED"
        ]
        order_rows = self.repository.book_broker_orders(canonical.id)
        orders = [_safe_value({**row, "metadata": _decode_field(row, "metadata_json")}) for row in order_rows]
        fills: list[dict[str, Any]] = []
        for row in order_rows:
            order_id = str(row["id"])
            fills.append({
                "broker_order_id": order_id,
                "fills": _safe_value(self.repository.fills_for_broker_order(order_id)),
            })

        issues = []
        for row in self.repository.open_reconciliation_issues(canonical.id):
            issues.append(_safe_value({**row, "details": _decode_field(row, "details_json")}))
        actions = _safe_value(self.repository.open_recovery_actions(canonical.id))
        pilot_runs: dict[str, dict[str, Any]] = {}
        for row in intent_rows:
            intent = self.repository.get_intent(str(row["id"]))
            metadata = intent.get("metadata", {}) if intent else {}
            if not isinstance(metadata, Mapping):
                continue
            for key, marker in metadata.items():
                if not isinstance(marker, Mapping) or "run_id" not in marker:
                    continue
                if "pilot" not in str(key).lower():
                    continue
                run_id = str(marker["run_id"])
                pilot_runs.setdefault(run_id, {"run_id": run_id, "source": key, "intent_ids": []})
                pilot_runs[run_id]["intent_ids"].append(str(row["id"]))
                pilot_runs[run_id].setdefault("modes", set()).add(str(marker.get("mode", "UNKNOWN")))
        for run in pilot_runs.values():
            run["modes"] = sorted(run.get("modes", set()))

        events = _safe_value(self.repository.operational_events(canonical.id, limit=100))
        alerts: list[dict[str, Any]] = []
        if risk_status.get("status") == "STATUS_ERROR":
            alerts.append(OperationalAlert(
                key="BOOK_RISK_STATUS_ERROR",
                severity="CRITICAL",
                summary="Book risk status could not be read; do not start risk-bearing work.",
                details={"error": risk_status.get("error", "unknown book-risk error")},
            ).as_dict())
        for issue in issues:
            alerts.append(OperationalAlert(
                key=f"reconciliation:{issue.get('issue_key', issue.get('id', 'unknown'))}",
                severity=str(issue.get("severity", "ERROR")),
                summary="Open reconciliation blocker requires operator review.",
                details={"issue_id": issue.get("id"), "category": issue.get("category")},
            ).as_dict())
        for action in actions:
            alerts.append(OperationalAlert(
                key=f"recovery:{action.get('action_key', action.get('id', 'unknown'))}",
                severity="ERROR",
                summary="Open recovery action requires an allowed operator next step.",
                details={"action_id": action.get("id"), "allowed_next_steps": action.get("allowed_next_steps", [])},
            ).as_dict())
        for unfinished in unfinished_intents:
            alerts.append(OperationalAlert(
                key=f"unfinished-intent:{unfinished['intent_id']}",
                severity="CRITICAL",
                summary="A persisted intent is still reconciliation-required; do not treat zero open issue rows as lifecycle completion.",
                details=unfinished,
            ).as_dict())
        unknown = risk_status.get("unknown_allocations", [])
        if unknown:
            alerts.append(OperationalAlert(
                key="unknown-allocations",
                severity="CRITICAL",
                summary="Unknown or external allocation blocks new risk-bearing work.",
                details={"count": len(unknown)},
            ).as_dict())

        mode = self.config.mode.value if self.config else OperationalMode.DRY_RUN.value
        submit_gate = "DRY_RUN_ONLY" if mode == OperationalMode.DRY_RUN.value else "SIM_ARMED_REQUIRES_FRESH_FACTS"
        return _safe_value({
            "schema": "stage7.operational_status.v1",
            "generated_at": _utc_now(),
            "lifecycle": (lifecycle.value if isinstance(lifecycle, ServiceState) else lifecycle) or ServiceState.STOPPED.value,
            "mode": mode,
            "account": {
                "id": canonical.id,
                "broker": canonical.broker,
                "environment": canonical.environment.value,
                "external_account_id": canonical.external_account_id,
                "base_currency": canonical.base_currency,
                "enabled": canonical.enabled,
            },
            "books": books,
            "book_risk": risk_status,
            "intents": intents,
            "orders": orders,
            "fills": fills,
            "recovery_actions": actions,
            "reconciliation_blockers": issues,
            "unfinished_intent_blockers": unfinished_intents,
            "pilot_runs": list(pilot_runs.values()),
            "operational_events": events,
            "alerts": alerts,
            "submit_gate": submit_gate,
        })


class OperationalService:
    """Minimal bounded lifecycle primitive for a future local scheduler."""

    def __init__(
        self,
        repository: SQLiteTradingRepository,
        config: OperationalConfig,
        *,
        oms: GenericOMS | None = None,
        alert_sink: AlertSink | None = None,
        clock: Any | None = None,
    ) -> None:
        self.repository = repository
        self.config = config
        self.config.validate_repository(repository)
        self.oms = oms
        self.alert_sink = alert_sink or CollectingAlertSink()
        self.clock = clock or _utc_now
        self._state = ServiceState.STOPPED
        self.reporter = OperationalStatusReporter(repository, config=config, oms=oms)

    @property
    def state(self) -> ServiceState:
        return self._state

    def _audit(
        self,
        event_type: str,
        outcome: str,
        summary: str,
        details: Mapping[str, Any] | None = None,
    ) -> str:
        try:
            return OperationalAuditEvent(
                account_id=self.config.account.id,
                event_type=event_type,
                mode=self.config.mode,
                outcome=outcome,
                summary=summary,
                details=details or {},
                occurred_at=self.clock(),
            ).persist(self.repository)
        except Exception as exc:
            raise OperationalAuditError(
                f"could not durably record operational event {event_type}/{outcome}"
            ) from exc

    @staticmethod
    def _correlation(prefix: str) -> str:
        return f"stage7-{prefix}-{uuid.uuid4().hex}"

    def _emit_report_alerts(self, report: Mapping[str, Any]) -> None:
        for raw in report.get("alerts", ()):
            if isinstance(raw, Mapping):
                self.alert_sink.emit(OperationalAlert(
                    key=str(raw.get("key", "operational-alert")),
                    severity=str(raw.get("severity", "ERROR")),
                    summary=str(raw.get("summary", "Operational blocker")),
                    details=_mapping(raw.get("details"), "alert.details"),
                ))

    def start(self) -> dict[str, Any]:
        if self._state is ServiceState.RUNNING:
            self._audit(
                "service.start",
                "NOOP",
                "Operational service was already running.",
                {"correlation_id": self._correlation("start")},
            )
        else:
            correlation_id = self._correlation("start")
            # The durable intent is written before mutating in-memory
            # lifecycle state.  If this write fails, the service remains
            # stopped and no transition is claimed.
            self._audit(
                "service.start",
                "INTENT",
                "Operational service start was requested.",
                {"correlation_id": correlation_id, "mode": self.config.mode.value},
            )
            self._state = ServiceState.RUNNING
            try:
                self._audit(
                    "service.start",
                    "STARTED",
                    "Operational service started in an explicit local mode.",
                    {"correlation_id": correlation_id, "mode": self.config.mode.value},
                )
            except OperationalAuditError as exc:
                raise OperationalAuditError(
                    "service is running, but its terminal start audit could not be recorded"
                ) from exc
        return self.status()

    def stop(self) -> dict[str, Any]:
        if self._state is ServiceState.STOPPED:
            self._audit(
                "service.stop",
                "NOOP",
                "Operational service was already stopped.",
                {"correlation_id": self._correlation("stop")},
            )
        else:
            correlation_id = self._correlation("stop")
            self._audit(
                "service.stop",
                "INTENT",
                "Operational service stop was requested.",
                {"correlation_id": correlation_id},
            )
            self._state = ServiceState.STOPPED
            try:
                self._audit(
                    "service.stop",
                    "STOPPED",
                    "Operational service stopped; no order action was taken.",
                    {"correlation_id": correlation_id},
                )
            except OperationalAuditError as exc:
                raise OperationalAuditError(
                    "service is stopped, but its terminal stop audit could not be recorded"
                ) from exc
        return self.status()

    def status(self) -> dict[str, Any]:
        return self.reporter.report(self.config.account, lifecycle=self._state)

    def once(self) -> dict[str, Any]:
        """Run one bounded status/recovery tick; never generate or submit orders."""
        if self._state is not ServiceState.RUNNING:
            self._audit(
                "service.tick",
                "BLOCKED",
                "Service tick requested while stopped; no broker action was taken.",
                {"correlation_id": self._correlation("tick")},
            )
            result = self.status()
            result["tick"] = {"outcome": "BLOCKED", "reason": "service is stopped"}
            return result

        correlation_id = self._correlation("tick")
        # This event is the durable action boundary.  No core/broker recovery
        # call is made until it succeeds.
        self._audit(
            "service.tick",
            "INTENT",
            "One bounded operational tick was requested.",
            {"correlation_id": correlation_id, "mode": self.config.mode.value},
        )
        tick: dict[str, Any] = {"outcome": "STATUS_ONLY", "correlation_id": correlation_id}
        if self.config.mode is OperationalMode.DRY_RUN:
            tick["reason"] = "dry-run mode never contacts a broker"
        elif self.oms is None:
            tick = {
                "outcome": "BLOCKED",
                "reason": "SIM recovery requires an explicitly wired GenericOMS",
                "correlation_id": correlation_id,
            }
            self.alert_sink.emit(OperationalAlert("sim-recovery-unavailable", "CRITICAL", tick["reason"], tick))
        else:
            try:
                recovered = self.oms.poll_and_recover(account=self.config.account)
                tick = {"outcome": "RECOVERY_COMPLETE", "recovered": recovered, "correlation_id": correlation_id}
            except Exception as exc:  # keep scheduler/service alive and surface a local blocker
                tick = {
                    "outcome": "BLOCKED",
                    "reason": "recovery tick failed",
                    "error": str(exc),
                    "correlation_id": correlation_id,
                }
                self.alert_sink.emit(OperationalAlert("recovery-tick-failed", "CRITICAL", tick["reason"], tick))
        try:
            self._audit(
                "service.tick",
                str(tick["outcome"]),
                "Operational tick terminal result recorded.",
                {"correlation_id": correlation_id, "result": tick},
            )
        except OperationalAuditError as exc:
            tick = {
                "outcome": "AUDIT_FAILURE",
                "reason": "tick action completed or failed but its terminal audit could not be recorded",
                "correlation_id": correlation_id,
                "error": str(exc),
            }
            self.alert_sink.emit(OperationalAlert("operational-audit-failed", "CRITICAL", tick["reason"], tick))
        result = self.status()
        result["tick"] = _safe_value(tick)
        self._emit_report_alerts(result)
        return result

    def run(self, *, ticks: int = 1) -> list[dict[str, Any]]:
        """Run a finite number of ticks; a scheduler owns the outer loop."""
        if isinstance(ticks, bool) or int(ticks) <= 0:
            raise ValueError("ticks must be a positive integer")
        if self._state is ServiceState.STOPPED:
            self.start()
        return [self.once() for _ in range(int(ticks))]

    def handoff_to_pilot(self, runner: Any, spec: Any, *, confirm_sim: bool = False) -> Any:
        """Explicit future Stage 6 handoff; this is the sole dispatch escape hatch."""
        if not confirm_sim or self.config.mode is not OperationalMode.SIM_ARMED or not self.config.sim_arm:
            raise OperationalSafetyError("pilot handoff requires explicit SIM_ARMED mode and confirm_sim=True")
        if runner is None or not callable(getattr(runner, "run", None)):
            raise OperationalSafetyError("pilot handoff requires a Stage 6 runner")
        if getattr(spec, "account", None) != self.config.account:
            raise OperationalSafetyError("pilot handoff account does not match operational configuration")
        correlation_id = self._correlation("pilot")
        self._audit(
            "pilot.handoff",
            "INTENT",
            "Explicit Stage 6 SIM pilot handoff requested; runner owns all dispatch guards.",
            {"correlation_id": correlation_id},
        )
        try:
            # The strategy-owned runner converts this stable wire value to
            # its own enum.  Keeping the generic core string-only preserves
            # the architecture boundary and avoids importing any strategy.
            result = runner.run(spec, mode="SIM_SUBMIT")
        except Exception as exc:
            try:
                self._audit(
                    "pilot.handoff",
                    "FAILED",
                    "Explicit Stage 6 handoff failed closed.",
                    {"correlation_id": correlation_id, "error": str(exc)},
                )
            except OperationalAuditError as audit_exc:
                raise OperationalAuditError(
                    "pilot handoff failed and its terminal audit could not be recorded"
                ) from audit_exc
            raise
        try:
            self._audit(
                "pilot.handoff",
                "RETURNED",
                "Explicit Stage 6 SIM pilot handoff returned.",
                {"correlation_id": correlation_id, "run_id": getattr(result, "run_id", None)},
            )
        except OperationalAuditError as exc:
            raise OperationalAuditError(
                "pilot handoff returned, but its terminal audit could not be recorded"
            ) from exc
        return result


__all__ = [
    "AlertSink",
    "CollectingAlertSink",
    "OperationalAlert",
    "OperationalAuditEvent",
    "OperationalAuditError",
    "OperationalConfig",
    "OperationalMode",
    "OperationalSafetyError",
    "OperationalService",
    "OperationalStatusReporter",
    "ServiceState",
]
