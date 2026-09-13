from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
import json
import logging
import math
import os
from pathlib import Path
import time
from typing import Any, Iterator

from src.core.models import Order, OrderSide, OrderStatus, OrderType
from src.db.positions_db import (
    DB_PATH,
    create_pair_operation,
    find_active_operation,
    get_active_operations,
    get_all_open_positions,
    get_all_orders,
    get_conn,
    get_open_reconciliation_issues,
    get_operation,
    get_operation_by_idempotency_key,
    get_operations_pending_completion,
    get_pending_orders,
    init_db,
    mark_leg_failure,
    mark_leg_submitting,
    open_position_from_operation,
    close_position_from_operation,
    record_aggregate_fill,
    record_broker_submission,
    record_fill_event,
    resolve_reconciliation_issue_by_key,
    set_system_state,
    transition_leg,
    update_order_status,
    upsert_reconciliation_issue,
    utc_now,
)
from .config import ExecutionConfig
from .reconciliation import ReconciliationIssue, ReconciliationResult, compare_broker_and_local_state
from .risk import RiskLimits, RiskSnapshot, evaluate_entry_risk


class ExecutionSafetyError(RuntimeError):
    """Raised when an operation cannot proceed without risking ambiguity."""


def _get(row: Any, *keys: str, default: Any = None) -> Any:
    if isinstance(row, dict):
        for key in keys:
            if key in row and row[key] is not None:
                return row[key]
        return default
    for key in keys:
        try:
            value = row[key]
            if value is not None:
                return value
        except Exception:
            pass
        if hasattr(row, key):
            value = getattr(row, key)
            if value is not None:
                return value
    return default


def _float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
        return number if math.isfinite(number) else default
    except (TypeError, ValueError):
        return default


def _broker_status(value: Any) -> str:
    text = getattr(value, "name", value)
    text = str(text).upper().split(".")[-1]
    if text in {"FILLED_ALL", "FILLED", "FULLY_FILLED"}:
        return "filled"
    if text in {"FILLED_PART", "PARTIALLY_FILLED", "PARTIAL_FILLED", "CANCELLED_PART", "FILL_CANCELLED"}:
        return "partially_filled"
    if text in {"CANCELLED_ALL", "CANCELLED", "DELETED", "DISABLED"}:
        return "cancelled"
    if text in {"REJECTED", "FAILED", "SUBMIT_FAILED", "TIMEOUT"}:
        return "rejected"
    return "submitted"


def _enum_name(value: Any) -> str:
    value = getattr(value, "name", value)
    return str(value).upper().split(".")[-1]


def _symbol(ticker: str) -> str:
    text = str(ticker)
    return text if "." in text else f"US.{text}"


def _utc_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        result = value
    elif hasattr(value, "to_pydatetime"):
        result = value.to_pydatetime()
    else:
        try:
            result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    if result.tzinfo is None:
        return result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


class _ProcessLock:
    """Small OS-level lock; an existing lock fails closed rather than guessed."""

    def __init__(self, path: Path):
        self.path = path
        self._held = False

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(str(self.path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise ExecutionSafetyError(
                f"Another execution runner appears active ({self.path}); remove the lock only after verifying it is stale."
            ) from exc
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json.dumps({"pid": os.getpid(), "created_at": utc_now()}))
        self._held = True
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._held:
            try:
                self.path.unlink()
            except FileNotFoundError:
                pass
            self._held = False


@dataclass
class _Admission:
    snapshot: RiskSnapshot
    decision: Any


class ExecutionEngine:
    """Durable two-leg state machine around a broker adapter."""

    def __init__(
        self,
        broker: Any,
        *,
        config: ExecutionConfig | None = None,
        db_path: str | Path | None = None,
        logger: logging.Logger | None = None,
    ):
        self.broker = broker
        self.config = config or ExecutionConfig.from_env()
        if db_path is not None:
            self.config = self.config.with_overrides(state_db_path=str(db_path))
        self.config.validate()
        self.db_path = self.config.db_path
        init_db(self.db_path, trd_env=self.config.trd_env)
        self.logger = logger or logging.getLogger("live_trading.execution")
        self._run_lock = _ProcessLock(self.db_path.with_suffix(self.db_path.suffix + ".runner.lock"))
        self._last_reconciliation: ReconciliationResult | None = None

    @contextmanager
    def run_lock(self) -> Iterator[_ProcessLock]:
        with self._run_lock:
            yield self._run_lock

    def _log(self, message: str, **fields: Any) -> None:
        context = " ".join(f"{key}={value}" for key, value in fields.items() if value is not None)
        self.logger.info("%s%s", message, f" {context}" if context else "")

    def _record_issue(self, issue: ReconciliationIssue) -> None:
        pass_issue_keys = getattr(self, "_reconciliation_pass_issue_keys", None)
        if pass_issue_keys is not None:
            pass_issue_keys.add((issue.category, issue.entity_key))
        self._log(
            "Reconciliation issue recorded",
            category=issue.category,
            entity_key=issue.entity_key,
            severity=issue.severity,
            environment=self.config.trd_env,
        )
        upsert_reconciliation_issue(
            category=issue.category,
            entity_key=issue.entity_key,
            details=issue.details,
            severity=issue.severity,
            db_path=self.db_path,
        )

    def _broker_orders(self) -> tuple[list[dict], list[dict]]:
        recent = list(self.broker.get_recent_orders()) if hasattr(self.broker, "get_recent_orders") else []
        open_orders = list(self.broker.get_open_orders()) if hasattr(self.broker, "get_open_orders") else []
        by_id: dict[str, dict] = {}
        for row in recent + open_orders:
            broker_id = _get(row, "order_id", "id", "broker_order_id")
            if broker_id is not None:
                by_id[str(broker_id)] = row
        return list(by_id.values()), open_orders

    @staticmethod
    def _match_broker_order(operation: dict, leg: dict, broker_orders: list[dict]) -> list[dict]:
        operation_id = str(operation["operation_id"])
        leg_name = str(leg["leg"])
        exact = []
        for row in broker_orders:
            remark = str(_get(row, "remark", "remark_str", "client_order_id", default=""))
            if operation_id in remark and leg_name in remark:
                exact.append(row)
        if exact:
            return exact
        symbol = str(leg["symbol"])
        side = str(leg["side"]).upper()
        requested = _float(leg["requested_quantity"])
        candidates = []
        for row in broker_orders:
            row_symbol = _symbol(str(_get(row, "code", "symbol", default="")))
            row_side = str(_get(row, "trd_side", "side", default="")).upper()
            row_qty = _float(_get(row, "qty", "quantity", "order_qty", default=0.0))
            if row_symbol == symbol and (not row_side or side in row_side) and abs(row_qty - requested) < 1e-6:
                candidates.append(row)
        return candidates

    def _apply_broker_fills(
        self,
        leg: dict,
        broker_order_id: str,
        *,
        pair: str | None = None,
        environment: str | None = None,
        account_id: str | None = None,
    ) -> int:
        if not hasattr(self.broker, "get_order_fills"):
            return 0
        applied = 0
        fills = self.broker.get_order_fills(str(broker_order_id)) or []
        for fill in fills:
            quantity = _float(_get(fill, "qty", "quantity", "dealt_qty"), 0.0)
            price = _float(_get(fill, "price", "fill_price", "dealt_avg_price"), 0.0)
            if quantity <= 0 or price <= 0:
                continue
            result = record_fill_event(
                operation_id=str(leg["operation_id"]),
                leg=str(leg["leg"]),
                broker_order_id=str(broker_order_id),
                broker_fill_id=str(_get(fill, "deal_id", "fill_id", default="")) or None,
                fill_price=price,
                fill_quantity=quantity,
                fill_time=str(_get(fill, "create_time", "updated_time", default=utc_now())),
                broker_order_status=str(_get(fill, "order_status", default="")) or None,
                raw_payload=fill,
                db_path=self.db_path,
            )
            if not result.get("duplicate"):
                applied += 1
                self._log(
                    "Fill applied",
                    operation_id=leg["operation_id"],
                    pair=pair or leg.get("pair"),
                    leg=leg["leg"],
                    local_order_id=leg.get("local_order_id"),
                    broker_order_id=broker_order_id,
                    environment=environment,
                    account_id=account_id,
                    filled=f"{result.get('cumulative_filled_quantity', 0):g}/{leg.get('requested_quantity', 0):g}",
                )
        # A Moomoo order query may expose only aggregate dealt_qty. Apply only
        # the unseen delta, so order and deal pushes cannot double-count.
        if not fills:
            for row in getattr(self.broker, "get_recent_orders", lambda: [])() or []:
                if str(_get(row, "order_id", "id", default="")) != str(broker_order_id):
                    continue
                quantity = _float(_get(row, "dealt_qty", "filled_qty"), 0.0)
                price = _float(_get(row, "dealt_avg_price", "avg_fill_price"), 0.0)
                if quantity > 0 and price > 0:
                    result = record_aggregate_fill(
                        broker_order_id=str(broker_order_id),
                        cumulative_quantity=quantity,
                        average_price=price,
                        fill_time=str(_get(row, "updated_time", "create_time", default=utc_now())),
                        broker_order_status=str(_get(row, "order_status", "status", default="")) or None,
                        db_path=self.db_path,
                    )
                    if not result.get("duplicate"):
                        applied += 1
                        self._log(
                            "Aggregate fill applied",
                            operation_id=leg["operation_id"],
                            pair=pair or leg.get("pair"),
                            leg=leg["leg"],
                            local_order_id=leg.get("local_order_id"),
                            broker_order_id=broker_order_id,
                            environment=environment,
                            account_id=account_id,
                            filled=f"{result.get('cumulative_filled_quantity', 0):g}/{leg.get('requested_quantity', 0):g}",
                        )
                break
        return applied

    def _recover_and_update_operation(self, operation: dict, broker_orders: list[dict]) -> int:
        applied = 0
        for leg in operation.get("legs", []):
            status = str(leg["status"])
            if status in {"filled", "cancelled", "rejected", "failed"} and leg.get("broker_order_id"):
                broker_id = str(leg["broker_order_id"])
            else:
                broker_id = str(leg.get("broker_order_id") or "")
            if not broker_id:
                # A ``created`` leg is a durable intent that has not reached
                # the broker yet (leg 2 normally remains here while leg 1 is
                # submitted). It is not a missing broker order and must not
                # poison readiness for the safe restart-resume path. A
                # ``submitting``/``requires_reconciliation`` leg, by
                # contrast, means a broker call may have happened and must be
                # correlated or manually resolved.
                if status == "created":
                    continue
                matches = self._match_broker_order(operation, leg, broker_orders)
                if len(matches) == 1:
                    row = matches[0]
                    broker_id_value = _get(row, "order_id", "id", "broker_order_id")
                    if broker_id_value is not None:
                        broker_id = str(broker_id_value)
                        record_broker_submission(
                            str(operation["operation_id"]),
                            str(leg["leg"]),
                            broker_id,
                            submitted_at=str(_get(row, "create_time", "updated_time", default=utc_now())),
                            broker_order_status=str(_get(row, "order_status", "status", default="SUBMITTED")),
                            db_path=self.db_path,
                        )
                        self._log("Recovered broker order", operation_id=operation["operation_id"], leg=leg["leg"], broker_order_id=broker_id)
                elif len(matches) > 1:
                    self._record_issue(
                        ReconciliationIssue(
                            "ambiguous_broker_order_match",
                            f"{operation['operation_id']}:{leg['leg']}",
                            {"candidate_count": len(matches), "symbol": leg["symbol"]},
                        )
                    )
                    continue
                else:
                    self._record_issue(
                        ReconciliationIssue(
                            "operation_leg_missing_broker_order",
                            f"{operation['operation_id']}:{leg['leg']}",
                            {"status": status, "symbol": leg["symbol"]},
                        )
                    )
                    try:
                        transition_leg(str(operation["operation_id"]), str(leg["leg"]), "requires_reconciliation", db_path=self.db_path)
                    except ValueError:
                        pass
                    continue
            try:
                applied += self._apply_broker_fills(
                    leg,
                    broker_id,
                    pair=str(operation["pair"]),
                    environment=str(operation["environment"]),
                    account_id=operation.get("account_id"),
                )
                if hasattr(self.broker, "get_order_status"):
                    current = _broker_status(self.broker.get_order_status(broker_id))
                    if current == "filled":
                        # If the broker reported full status but no deal rows,
                        # do not mark local state filled without quantity evidence.
                        latest = get_operation(str(operation["operation_id"]), self.db_path)
                        latest_leg = next(item for item in latest["legs"] if item["leg"] == leg["leg"])
                        if float(latest_leg["cumulative_filled_quantity"] or 0.0) + 1e-9 >= float(latest_leg["requested_quantity"]):
                            transition_leg(str(operation["operation_id"]), str(leg["leg"]), "filled", db_path=self.db_path)
                        else:
                            transition_leg(str(operation["operation_id"]), str(leg["leg"]), "partially_filled", db_path=self.db_path)
                    elif current in {"cancelled", "rejected"}:
                        update_order_status(
                            int(leg["local_order_id"]), current, self.db_path,
                            broker_order_status=current.upper(),
                        )
                    elif current == "partially_filled":
                        transition_leg(str(operation["operation_id"]), str(leg["leg"]), "partially_filled", db_path=self.db_path)
            except Exception as exc:
                self._record_issue(
                    ReconciliationIssue(
                        "broker_order_reconciliation_failed",
                        broker_id,
                        {"operation_id": operation["operation_id"], "leg": leg["leg"], "error": str(exc)[:500]},
                    )
                )
        return applied

    def startup_reconcile(self) -> ReconciliationResult:
        """Recover missed callbacks and compare broker state with local state."""
        result = ReconciliationResult()
        self._reconciliation_pass_issue_keys: set[tuple[str, str]] = set()
        try:
            broker_positions = list(self.broker.get_positions())
            broker_orders, _ = self._broker_orders()
        except Exception as exc:
            issue = ReconciliationIssue("broker_state_query_failed", "broker", {"error": str(exc)[:500]})
            self._record_issue(issue)
            result.issues.append(issue)
            set_system_state("reconciliation_ready", "false", self.db_path)
            self._last_reconciliation = result
            self._reconciliation_pass_issue_keys = set()
            return result

        operations = get_active_operations(self.db_path)
        known_operation_ids = {str(operation["operation_id"]) for operation in operations}
        operations.extend(
            operation
            for operation in get_operations_pending_completion(self.db_path)
            if str(operation["operation_id"]) not in known_operation_ids
        )
        for operation in operations:
            if operation is None:
                continue
            result.applied_fill_count += self._recover_and_update_operation(operation, broker_orders)
            refreshed = get_operation(str(operation["operation_id"]), self.db_path)
            if refreshed is None:
                continue
            status = str(refreshed["status"])
            if status == "partially_filled":
                issue = ReconciliationIssue(
                    "partial_pair_operation",
                    str(operation["operation_id"]),
                    {"pair": operation["pair"], "operation_type": operation["operation_type"]},
                )
                self._record_issue(issue)
                result.issues.append(issue)
            try:
                if status in {"leg2_submitted", "partially_filled", "open", "closed"}:
                    if operation["operation_type"] == "entry":
                        if all(leg["status"] == "filled" for leg in refreshed["legs"]):
                            open_position_from_operation(str(operation["operation_id"]), entry_date=date.today().isoformat(), db_path=self.db_path)
                    elif operation["operation_type"] == "exit":
                        if all(leg["status"] == "filled" for leg in refreshed["legs"]):
                            close_position_from_operation(str(operation["operation_id"]), exit_date=date.today().isoformat(), db_path=self.db_path)
            except (ValueError, KeyError, TypeError) as exc:
                self._record_issue(
                    ReconciliationIssue(
                        "operation_completion_pending",
                        str(operation["operation_id"]),
                        {"status": status, "error": str(exc)[:500]},
                        severity="medium",
                    )
                )

        comparison = compare_broker_and_local_state(
            local_positions=get_all_open_positions(self.db_path),
            broker_positions=broker_positions,
            local_orders=get_all_orders(self.db_path),
            broker_orders=broker_orders,
        )
        result.issues.extend(comparison.issues)
        for issue in comparison.issues:
            self._record_issue(issue)

        # Resolve only issues absent from this complete snapshot. A query
        # failure never clears an old block.
        current_keys = {(issue.category, issue.entity_key) for issue in result.issues}
        current_keys.update(self._reconciliation_pass_issue_keys)
        for old in get_open_reconciliation_issues(self.db_path):
            key = (str(old["category"]), str(old["entity_key"]))
            if key not in current_keys and key[0] != "broker_state_query_failed":
                resolve_reconciliation_issue_by_key(key[0], key[1], self.db_path)
        # Persist the aggregate readiness after considering any old issues that
        # remain open (for example, a manually-created mismatch).
        open_issues = get_open_reconciliation_issues(self.db_path)
        result.issues.extend(
            ReconciliationIssue(row["category"], row["entity_key"], {"persisted": True}, row["severity"])
            for row in open_issues
            if (row["category"], row["entity_key"]) not in current_keys
        )
        set_system_state("reconciliation_ready", "true" if not open_issues else "false", self.db_path)
        self._last_reconciliation = result
        self._reconciliation_pass_issue_keys = set()
        self._log("Startup reconciliation complete", issues=len(open_issues), fills=result.applied_fill_count)
        return result

    def reconcile(self, timeout: int = 10, interval: int = 2) -> ReconciliationResult:
        deadline = time.time() + max(0, timeout)
        latest = self.startup_reconcile()
        while time.time() < deadline:
            active = get_active_operations(self.db_path)
            if not active or all(str(op["status"]) not in {"leg1_submitted", "leg2_submitted", "partially_filled", "requires_reconciliation"} for op in active):
                break
            time.sleep(max(0.1, interval))
            latest = self.startup_reconcile()
        return latest

    def _validate_execution_conditions(self, legs: list[dict], signal_timestamp: Any = None) -> None:
        for leg in legs:
            price = _float(leg.get("intended_price"), 0.0)
            quantity = _float(leg.get("requested_quantity"), 0.0)
            if price <= 0 or quantity <= 0:
                raise ExecutionSafetyError("execution sizing contains a non-positive or invalid price/quantity")
        if self.config.is_real:
            timestamp = _utc_datetime(signal_timestamp)
            if timestamp is None:
                raise ExecutionSafetyError("REAL execution requires a signal/price timestamp")
            age = (datetime.now(timezone.utc) - timestamp).total_seconds()
            if age < -60 or age > self.config.data_max_age_seconds:
                raise ExecutionSafetyError(f"signal/price data is stale ({age:.0f}s old)")
        symbols = [str(leg["symbol"]) for leg in legs]
        if self.config.require_symbol_validation and hasattr(self.broker, "validate_symbols"):
            try:
                valid = bool(self.broker.validate_symbols(symbols))
            except Exception as exc:
                if self.config.is_real:
                    raise ExecutionSafetyError(f"symbol validation failed: {exc}") from exc
                valid = True
            if not valid and self.config.is_real:
                raise ExecutionSafetyError("one or more symbols are not valid/tradable")
        if self.config.require_market_state and hasattr(self.broker, "get_market_state"):
            try:
                state = self.broker.get_market_state(symbols)
                rows = state.get("rows", []) if isinstance(state, dict) else []
                states = {_enum_name(_get(row, "market_state", "state", default="")) for row in rows}
                if self.config.is_real and len(rows) < len(symbols):
                    raise ExecutionSafetyError("market/session state did not cover every symbol")
                if self.config.is_real and (not rows or not states or "" in states):
                    raise ExecutionSafetyError("market/session state was unavailable")
                if self.config.is_real and rows and states and states.issubset({"CLOSED", "休市", "NONE"}):
                    raise ExecutionSafetyError("market/session is closed")
            except ExecutionSafetyError:
                raise
            except Exception as exc:
                if self.config.is_real:
                    raise ExecutionSafetyError(f"market/session check failed: {exc}") from exc

    def _admit_entry(self, legs: list[dict], signal_timestamp: Any = None) -> _Admission:
        reconciliation = self.startup_reconcile()
        if self.config.is_real and not reconciliation.ready:
            raise ExecutionSafetyError("REAL entry blocked until reconciliation is clean")
        self._validate_execution_conditions(legs, signal_timestamp)
        try:
            account = self.broker.get_account_balance()
            if self.config.is_real:
                expected = self.config.expected_real_account_id
                if expected is None or account.account_id is None or str(account.account_id) != str(expected):
                    raise PermissionError("broker account identity does not match EXPECTED_REAL_ACCOUNT_ID")
            positions = self.broker.get_positions()
            snapshot = RiskSnapshot.from_account(
                account,
                positions=positions,
                open_pairs=len(get_all_open_positions(self.db_path)),
                pending_operations=sum(
                    1
                    for operation in get_active_operations(self.db_path)
                    if str(operation["status"]) != "open"
                ),
                pending_orders=len(get_pending_orders(db_path=self.db_path)),
                unresolved_reconciliation=len(get_open_reconciliation_issues(self.db_path)),
                risk_data_available=True,
            )
        except PermissionError as exc:
            raise ExecutionSafetyError(str(exc)) from exc
        except Exception as exc:
            snapshot = RiskSnapshot(
                equity=None,
                buying_power=None,
                gross_exposure=None,
                pending_orders=len(get_pending_orders(db_path=self.db_path)),
                unresolved_reconciliation=len(get_open_reconciliation_issues(self.db_path)),
                risk_data_available=False,
                warnings=[str(exc)[:500]],
            )
        limits = RiskLimits(
            max_account_utilization=self.config.max_account_utilization,
            max_margin_utilization=self.config.max_margin_utilization,
            max_gross_exposure=self.config.max_gross_exposure,
            max_pair_exposure=self.config.max_pair_exposure,
            max_open_pairs=self.config.max_open_pairs,
            max_pending_operations=self.config.max_pending_operations,
            estimated_margin_rate=self.config.estimated_margin_rate,
        )
        decision = evaluate_entry_risk(snapshot, legs, limits, environment=self.config.trd_env)
        self._log(
            "Pre-trade risk decision",
            approved=decision.approved,
            reason=decision.reason,
            warnings="|".join(decision.warnings) if decision.warnings else None,
            environment=self.config.trd_env,
        )
        if not decision.approved:
            raise ExecutionSafetyError(decision.reason)
        return _Admission(snapshot, decision)

    def _submit_operation(self, operation_id: str) -> dict:
        operation = get_operation(operation_id, self.db_path)
        if operation is None:
            raise ExecutionSafetyError(f"Missing operation {operation_id}")
        for leg in sorted(operation["legs"], key=lambda row: row["leg"]):
            if str(leg["status"]) not in {"created", "requires_reconciliation"}:
                continue
            # Never submit leg 2 if leg 1 failed or was not durably accepted.
            refreshed = get_operation(operation_id, self.db_path)
            leg1 = next(item for item in refreshed["legs"] if item["leg"] == "ticker1")
            if leg["leg"] == "ticker2" and str(leg1["status"]) in {"failed", "rejected", "cancelled", "requires_reconciliation"}:
                break
            if leg["leg"] == "ticker2" and not leg1.get("broker_order_id"):
                break
            broker_order_id: str | None = None
            try:
                mark_leg_submitting(operation_id, str(leg["leg"]), self.db_path)
                order = Order(
                    symbol=str(leg["symbol"]),
                    quantity=float(leg["requested_quantity"]),
                    side=OrderSide.BUY if str(leg["side"]) == "BUY" else OrderSide.SELL,
                    order_type=OrderType.MARKET,
                    price=float(leg["intended_price"]),
                    remark=f"lts:{operation_id}:{leg['leg']}",
                )
                broker_order_id, submitted_at = self.broker.place_order(order)
                record_broker_submission(
                    operation_id,
                    str(leg["leg"]),
                    str(broker_order_id),
                    submitted_at=str(submitted_at or utc_now()),
                    db_path=self.db_path,
                )
                self._log(
                    "Leg submitted",
                    operation_id=operation_id,
                    pair=operation["pair"],
                    leg=leg["leg"],
                    local_order_id=leg["local_order_id"],
                    broker_order_id=broker_order_id,
                    environment=operation["environment"],
                    account_id=operation.get("account_id"),
                )
            except Exception as exc:
                self._log(
                    "Leg submission failed",
                    operation_id=operation_id,
                    pair=operation["pair"],
                    leg=leg["leg"],
                    local_order_id=leg.get("local_order_id"),
                    broker_order_id=broker_order_id,
                    environment=operation["environment"],
                    account_id=operation.get("account_id"),
                    error=str(exc)[:500],
                )
                if broker_order_id is None:
                    # A transport/response failure is ambiguous: the broker
                    # may have accepted the order even though no ID reached
                    # this process. Keep the intent recoverable so startup
                    # reconciliation can correlate the durable remark, and
                    # never classify it as a clean failed/no-exposure leg.
                    transition_leg(
                        operation_id,
                        str(leg["leg"]),
                        "requires_reconciliation",
                        error=str(exc),
                        db_path=self.db_path,
                    )
                else:
                    mark_leg_failure(
                        operation_id,
                        str(leg["leg"]),
                        str(exc),
                        broker_order_id=broker_order_id,
                        db_path=self.db_path,
                    )
                refreshed = get_operation(operation_id, self.db_path)
                if leg["leg"] == "ticker2" and refreshed and refreshed["legs"][0].get("broker_order_id"):
                    issue = ReconciliationIssue(
                        "one_leg_submission_failure",
                        operation_id,
                        {"pair": operation["pair"], "failed_leg": leg["leg"], "error": str(exc)[:500]},
                    )
                    self._record_issue(issue)
                    first = refreshed["legs"][0]
                    try:
                        if first.get("broker_order_id") and str(first["status"]) != "filled":
                            self.broker.cancel_order(str(first["broker_order_id"]))
                            self._log("Requested conservative cancellation of first leg", operation_id=operation_id, broker_order_id=first["broker_order_id"])
                    except Exception as cancel_exc:
                        self._record_issue(
                            ReconciliationIssue(
                                "one_leg_cancel_failed",
                                operation_id,
                                {"broker_order_id": first.get("broker_order_id"), "error": str(cancel_exc)[:500]},
                            )
                        )
                break
        return get_operation(operation_id, self.db_path)

    def execute_entry(
        self,
        entry_signal: dict[str, Any],
        plan: dict[str, Any],
        *,
        signal_timestamp: Any = None,
        operation_date: str | None = None,
        strategy_id: str = "stat_arb",
    ) -> dict:
        pair = str(entry_signal["pair"])
        op_date = operation_date or date.today().isoformat()
        strategy_id = str(strategy_id).strip() or "stat_arb"
        idempotency_key = f"{strategy_id}|{pair}|entry|{op_date}"
        existing = get_operation_by_idempotency_key(idempotency_key, self.db_path)
        if existing is not None:
            self._log("Duplicate entry invocation suppressed", operation_id=existing["operation_id"], pair=pair, status=existing["status"])
            # A process can stop after leg 1 is durably accepted but before
            # the leg 2 call.  Reconcile first, then resume only when the
            # broker/local snapshot is clean and the persisted state proves
            # that resuming leg 2 is safe.  Unresolved operations remain
            # manual-reconciliation-only and are never blindly retried.
            if str(existing.get("status")) in {"created", "leg1_submitted", "partially_filled", "requires_reconciliation"}:
                try:
                    with self.run_lock():
                        reconciliation = self.startup_reconcile()
                        refreshed = get_operation(existing["operation_id"], self.db_path)
                        if reconciliation.ready and refreshed is not None:
                            existing = self._submit_operation(existing["operation_id"])
                except Exception as exc:
                    self._record_issue(
                        ReconciliationIssue(
                            "duplicate_entry_resume_failed",
                            str(existing["operation_id"]),
                            {"pair": pair, "error": str(exc)[:500]},
                        )
                    )
                    existing = get_operation(existing["operation_id"], self.db_path) or existing
            return existing
        open_positions = get_all_open_positions(self.db_path)
        if any(str(position.get("pair")) == pair for position in open_positions):
            raise ExecutionSafetyError(f"Entry blocked because {pair} already has an open local position")
        active = find_active_operation(pair, "entry", self.db_path)
        if active is not None:
            self._log(
                "Active entry invocation suppressed",
                operation_id=active["operation_id"],
                pair=pair,
                status=active["status"],
            )
            return active
        legs = [
            {
                "leg": "ticker1",
                "symbol": _symbol(entry_signal["ticker1"]),
                "side": plan["ticker1_side"],
                "intended_price": entry_signal["latest_price_s1"],
                "intended_quantity_raw": plan["ticker1_intended_qty"],
                "requested_quantity": plan["ticker1_qty"],
            },
            {
                "leg": "ticker2",
                "symbol": _symbol(entry_signal["ticker2"]),
                "side": plan["ticker2_side"],
                "intended_price": entry_signal["latest_price_s2"],
                "intended_quantity_raw": plan["ticker2_intended_qty"],
                "requested_quantity": plan["ticker2_qty"],
            },
        ]
        pending_for_pair = [
            order
            for order in get_pending_orders(db_path=self.db_path)
            if str(order.get("strategy_identifier")) == pair
            and str(order.get("leg_type", "")).lower() == "entry"
        ]
        if pending_for_pair:
            raise ExecutionSafetyError(
                f"Entry blocked because pending local order intent already exists for {pair}"
            )
        self._admit_entry(legs, signal_timestamp)
        # Reconciliation may have completed a prior operation while the
        # admission query was running. Re-check before creating/submitting a
        # second intent.
        reconciled_position = next(
            (position for position in get_all_open_positions(self.db_path) if str(position.get("pair")) == pair),
            None,
        )
        if reconciled_position is not None:
            raise ExecutionSafetyError(f"Entry blocked because {pair} became open during reconciliation")
        reconciled_active = find_active_operation(pair, "entry", self.db_path)
        if reconciled_active is not None:
            return reconciled_active
        metadata = {
            "entry_zscore": entry_signal["entry_zscore"],
            "entry_hedge_ratio": entry_signal["entry_hedge_ratio"],
            "entry_alpha": entry_signal["entry_alpha"],
            "entry_residual_mean": entry_signal["entry_residual_mean"],
            "entry_residual_std": entry_signal["entry_residual_std"],
            "signal_timestamp": str(signal_timestamp) if signal_timestamp is not None else None,
        }
        operation_id = create_pair_operation(
            strategy_id=strategy_id,
            strategy_identifier=pair,
            pair=pair,
            operation_type="entry",
            ticker1=_symbol(entry_signal["ticker1"]),
            ticker2=_symbol(entry_signal["ticker2"]),
            legs=legs,
            metadata=metadata,
            environment=self.config.trd_env,
            account_id=self.config.account_id or self.config.expected_real_account_id,
            idempotency_key=idempotency_key,
            db_path=self.db_path,
        )
        return self._submit_operation(operation_id)

    def execute_exit(
        self,
        exit_signal: dict[str, Any],
        *,
        signal_timestamp: Any = None,
        operation_date: str | None = None,
        strategy_id: str = "stat_arb",
    ) -> dict:
        pair = str(exit_signal["pair"])
        position = exit_signal.get("open_pos")
        if position is None:
            raise ExecutionSafetyError(f"Cannot exit {pair}: open position is missing")
        op_date = operation_date or date.today().isoformat()
        strategy_id = str(strategy_id).strip() or "stat_arb"
        idempotency_key = f"{strategy_id}|{pair}|exit|{op_date}"
        existing = get_operation_by_idempotency_key(idempotency_key, self.db_path)
        if existing is not None:
            self._log("Duplicate exit invocation suppressed", operation_id=existing["operation_id"], pair=pair, status=existing["status"])
            return existing
        active = find_active_operation(pair, "exit", self.db_path)
        if active is not None:
            self._log(
                "Active exit invocation suppressed",
                operation_id=active["operation_id"],
                pair=pair,
                status=active["status"],
            )
            return active
        reconciliation = self.startup_reconcile()
        if self.config.is_real and not reconciliation.ready:
            raise ExecutionSafetyError("REAL exit blocked until reconciliation is clean")
        refreshed_position = next(
            (item for item in get_all_open_positions(self.db_path) if str(item.get("pair")) == pair),
            None,
        )
        if refreshed_position is None:
            raise ExecutionSafetyError(f"Exit blocked because {pair} is no longer open after reconciliation")
        position = refreshed_position
        legs = [
            {
                "leg": "ticker1",
                "symbol": _symbol(exit_signal["ticker1"]),
                "side": "BUY" if position["entry_side1"] == "SELL" else "SELL",
                "intended_price": exit_signal["latest_price_s1"],
                "intended_quantity_raw": position["executed_size1"],
                "requested_quantity": position["executed_size1"],
            },
            {
                "leg": "ticker2",
                "symbol": _symbol(exit_signal["ticker2"]),
                "side": "BUY" if position["entry_side2"] == "SELL" else "SELL",
                "intended_price": exit_signal["latest_price_s2"],
                "intended_quantity_raw": position["executed_size2"],
                "requested_quantity": position["executed_size2"],
            },
        ]
        self._validate_execution_conditions(legs, signal_timestamp)
        if self.config.is_real:
            try:
                account = self.broker.get_account_balance()
            except Exception as exc:
                raise ExecutionSafetyError(f"REAL exit blocked because account state query failed: {exc}") from exc
            expected = self.config.expected_real_account_id
            if expected is None or account.account_id is None or str(account.account_id) != str(expected):
                raise ExecutionSafetyError("REAL exit blocked because broker account identity could not be verified")
        metadata = {
            "position_id": position["id"],
            "exit_reason": exit_signal.get("exit_reason", "unknown"),
            "exit_zscore": exit_signal.get("exit_zscore"),
            "signal_timestamp": str(signal_timestamp) if signal_timestamp is not None else None,
        }
        operation_id = create_pair_operation(
            strategy_id=strategy_id,
            strategy_identifier=pair,
            pair=pair,
            operation_type="exit",
            ticker1=_symbol(exit_signal["ticker1"]),
            ticker2=_symbol(exit_signal["ticker2"]),
            legs=legs,
            metadata=metadata,
            environment=self.config.trd_env,
            account_id=self.config.account_id or self.config.expected_real_account_id,
            idempotency_key=idempotency_key,
            position_id=int(position["id"]),
            db_path=self.db_path,
        )
        return self._submit_operation(operation_id)
