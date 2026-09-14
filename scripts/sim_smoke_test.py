"""Supervised, strategy-independent Moomoo SIM two-leg smoke test.

The command deliberately has no strategy or market-data selection logic.  It
only exercises the existing durable ``MooMooAdapter`` + ``ExecutionEngine``
path for one explicitly supplied US pair.

Examples (PowerShell):

    python scripts/sim_smoke_test.py preflight `
        --state-db data/sim-smoke.db --symbol1 US.AAPL --symbol2 US.MSFT

    python scripts/sim_smoke_test.py enter `
        --state-db data/sim-smoke.db --acc-id 5077333 `
        --symbol1 US.AAPL --symbol2 US.MSFT --submit

    python scripts/sim_smoke_test.py status `
        --state-db data/sim-smoke.db --symbol1 US.AAPL --symbol2 US.MSFT

    python scripts/sim_smoke_test.py exit `
        --state-db data/sim-smoke.db --acc-id 5077333 `
        --symbol1 US.AAPL --symbol2 US.MSFT --submit

``preflight`` and ``status`` never submit orders.  ``enter`` and ``exit``
refuse to run without ``--submit`` and all stages refuse ``REAL``.
"""

from __future__ import annotations

import argparse
import ast
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timezone
import json
import math
import os
from pathlib import Path
import sys
from typing import Any, Callable, Iterable

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python 3.8 fallback is not used here
    ZoneInfo = None  # type: ignore[assignment]


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.brokers.moomoo.adapter import MooMooAdapter
from src.core.models import AccountBalance, Position
from src.db.positions_db import (
    DB_PATH,
    get_all_open_positions,
    get_all_orders,
    get_operation,
    get_system_state,
    get_open_position,
    init_db,
    resolve_db_path,
    set_system_state,
)
from src.execution import ExecutionConfig, ExecutionEngine, ExecutionSafetyError


SMOKE_STRATEGY_ID = "sim_smoke"
DEFAULT_GROSS_CAP = 1_000.0
HARD_MAX_GROSS_CAP = 5_000.0
HARD_MAX_SNAPSHOT_AGE_SECONDS = 900
REGULAR_STATES = {"MORNING", "AFTERNOON", "REGULAR", "OPEN"}
ET_NAME = "America/New_York"
# Temporary legacy-smoke exception for the terminal SIM orders manually
# created while proving the app/OpenD extended-hours path. This is deliberately
# not a general reconciliation policy: it applies only to this SIM account and
# only if every field below still matches the broker response. Any unknown
# order, non-terminal status, open order, or non-zero external position remains
# a blocking condition.
SMOKE_ALLOWLIST_ACCOUNT_ID = 5_077_333
SMOKE_ALLOWED_TERMINAL_EXTERNAL_ORDERS = {
    "3405094": {"symbol": "US.AAPL", "side": "SELL", "quantity": 3.0, "status": "FILLED_ALL"},
    "3404610": {"symbol": "US.AAPL", "side": "SELL", "quantity": 3.0, "status": "CANCELLED_ALL"},
    "3404570": {"symbol": "US.EOG", "side": "SELL", "quantity": 46.0, "status": "FILLED_ALL"},
    "3404609": {"symbol": "US.SLB", "side": "BUY", "quantity": 218.0, "status": "FILLED_ALL"},
}
# ``OpenSecTradeContext.get_acc_list`` exposes ``trdmarket_auth`` as the
# protobuf integer values in current moomoo-api releases (US=2, JP=15,
# MY=111, ...), even though callers generally expect market names.
_TRD_MARKET_NUMBER_TO_NAME = {
    "1": "HK",
    "2": "US",
    "3": "CN",
    "4": "HKCC",
    "5": "FUTURES",
    "6": "SG",
    "8": "AU",
    "15": "JP",
    "111": "MY",
    "112": "CA",
}


class SmokeTestError(RuntimeError):
    """Invalid configuration or unavailable broker capability."""


class SmokeTestBlocked(SmokeTestError):
    """A safety gate intentionally prevented the requested stage."""

    def __init__(self, message: str, result: dict[str, Any] | None = None):
        super().__init__(message)
        self.result = result or {}


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


def _enum_name(value: Any) -> str:
    value = getattr(value, "name", value)
    return str(value).upper().split(".")[-1]


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def _records(data: Any) -> list[dict[str, Any]]:
    if data is None:
        return []
    if isinstance(data, list):
        return [dict(item) if isinstance(item, dict) else item for item in data]
    if hasattr(data, "to_dict"):
        try:
            return data.to_dict("records")
        except Exception:
            return []
    return []


def _normalise_auth(value: Any) -> set[str]:
    if value is None:
        return set()
    def _market_name(item: Any) -> str:
        normalized = _enum_name(item)
        return _TRD_MARKET_NUMBER_TO_NAME.get(normalized, normalized)

    if isinstance(value, (list, tuple, set)):
        return {_market_name(item) for item in value}
    text = str(value).strip()
    try:
        parsed = ast.literal_eval(text)
        if isinstance(parsed, (list, tuple, set)):
            return {_market_name(item) for item in parsed}
    except (SyntaxError, ValueError):
        pass
    return {
        _TRD_MARKET_NUMBER_TO_NAME.get(part.strip().upper().split(".")[-1], part.strip().upper().split(".")[-1])
        for part in text.replace(";", ",").split(",")
        if part.strip()
    }


def _normalise_symbol(value: Any) -> str:
    text = str(value or "").strip().upper()
    if "." not in text:
        return text
    prefix, ticker = text.split(".", 1)
    return f"{prefix}.{ticker}"


def _as_output(value: Any) -> Any:
    """Convert SDK/numpy/dataclass values into JSON-safe output."""
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if hasattr(value, "item"):
        try:
            return _as_output(value.item())
        except Exception:
            pass
    if hasattr(value, "name"):
        return value.name
    if isinstance(value, dict):
        return {str(key): _as_output(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_as_output(item) for item in value]
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    return str(value)


def _safe_account_firm(value: Any) -> str | None:
    text = _enum_name(value)
    return None if text in {"", "NONE", "N/A", "NAN"} else text


@dataclass(frozen=True)
class SimAccount:
    acc_id: int
    acc_type: str
    sim_acc_type: str
    acc_role: str
    trd_env: str
    security_firm: str | None
    trdmarket_auth: tuple[str, ...]
    acc_status: str
    competition_acc_name: str | None = None

    @classmethod
    def from_row(cls, row: Any) -> "SimAccount":
        raw_id = _get(row, "acc_id", "account_id")
        try:
            acc_id = int(raw_id)
        except (TypeError, ValueError) as exc:
            raise SmokeTestError(f"Broker returned an invalid SIM account ID: {raw_id!r}") from exc
        return cls(
            acc_id=acc_id,
            acc_type=_enum_name(_get(row, "acc_type", default="")),
            sim_acc_type=_enum_name(_get(row, "sim_acc_type", default="")),
            acc_role=_enum_name(_get(row, "acc_role", "role", default="")),
            trd_env=_enum_name(_get(row, "trd_env", "trading_env", default="")),
            security_firm=_safe_account_firm(_get(row, "security_firm", default="")),
            trdmarket_auth=tuple(sorted(_normalise_auth(_get(row, "trdmarket_auth", "market_auth")))),
            acc_status=_enum_name(_get(row, "acc_status", "status", default="")),
            competition_acc_name=str(_get(row, "competition_acc_name", default="") or "") or None,
        )

    def as_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["trdmarket_auth"] = list(self.trdmarket_auth)
        return result


class _SmokeTerminalOrderAllowlistAdapter:
    """Expose a narrowly filtered broker-order view to the legacy smoke engine.

    The underlying Moomoo account remains authoritative and all order actions
    still go directly to it. Only four known, terminal external orders are
    omitted from the engine's recent-order reconciliation input. This lets the
    frozen smoke harness test its own submission path without treating our
    already-flat, manually-created test activity as an unresolved mismatch.
    """

    def __init__(self, broker: Any, account_id: int):
        self._broker = broker
        self._allowlist_enabled = int(account_id) == SMOKE_ALLOWLIST_ACCOUNT_ID
        self._permitted_terminal_orders: list[dict[str, Any]] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self._broker, name)

    @staticmethod
    def _order_audit_row(row: Any, order_id: str, rule: dict[str, Any]) -> dict[str, Any]:
        return {
            "order_id": order_id,
            "symbol": _normalise_symbol(_get(row, "code", "symbol", default="")),
            "side": _enum_name(_get(row, "trd_side", "side", default="")),
            "quantity": _safe_float(_get(row, "qty", "quantity")),
            "status": _enum_name(_get(row, "order_status", "status", default="")),
            "allowlist_rule": dict(rule),
        }

    def _matches_known_terminal_order(self, row: Any) -> tuple[str, dict[str, Any]] | None:
        if not self._allowlist_enabled:
            return None
        order_id = str(_get(row, "order_id", "id", "broker_order_id", default=""))
        rule = SMOKE_ALLOWED_TERMINAL_EXTERNAL_ORDERS.get(order_id)
        if rule is None:
            return None
        symbol = _normalise_symbol(_get(row, "code", "symbol", default=""))
        side = _enum_name(_get(row, "trd_side", "side", default=""))
        quantity = _safe_float(_get(row, "qty", "quantity"))
        status = _enum_name(_get(row, "order_status", "status", default=""))
        if (
            symbol == rule["symbol"]
            and side == rule["side"]
            and quantity is not None
            and math.isclose(quantity, rule["quantity"], rel_tol=0.0, abs_tol=1e-9)
            and status == rule["status"]
        ):
            return order_id, rule
        return None

    def get_recent_orders(self) -> list[dict[str, Any]]:
        rows = list(self._broker.get_recent_orders())
        permitted: list[dict[str, Any]] = []
        filtered: list[dict[str, Any]] = []
        for row in rows:
            match = self._matches_known_terminal_order(row)
            if match is None:
                filtered.append(row)
                continue
            order_id, rule = match
            permitted.append(self._order_audit_row(row, order_id, rule))
        self._permitted_terminal_orders = permitted
        return filtered

    def permitted_terminal_orders(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self._permitted_terminal_orders]


def _is_eligible_sim_account(row: Any, market: str = "US") -> bool:
    account = SimAccount.from_row(row)
    if account.trd_env != "SIMULATE" or account.acc_status != "ACTIVE":
        return False
    if account.acc_role == "MASTER":
        return False
    if market.upper() not in set(account.trdmarket_auth):
        return False
    # A US STOCK_AND_OPTION paper account is returned as MARGIN by some SDK
    # versions.  Competition US accounts are also margin accounts.
    return account.acc_type in {"MARGIN", "STOCK_AND_OPTION"} or (
        account.sim_acc_type == "COMPETITION" and account.acc_type == "MARGIN"
    )


def eligible_sim_accounts(rows: Iterable[Any], market: str = "US") -> list[SimAccount]:
    """Return unique, non-MASTER SIM margin accounts authorized for market."""
    selected: dict[int, SimAccount] = {}
    for row in rows:
        try:
            if not _is_eligible_sim_account(row, market):
                continue
            account = SimAccount.from_row(row)
        except SmokeTestError:
            continue
        # Account lists queried through several SecurityFirm contexts can
        # duplicate the same account.  One account ID is one selection.
        selected.setdefault(account.acc_id, account)
    return list(selected.values())


def select_sim_account(rows: Iterable[Any], acc_id: int | None = None, market: str = "US") -> SimAccount:
    accounts = eligible_sim_accounts(rows, market)
    if acc_id is not None:
        matches = [account for account in accounts if account.acc_id == int(acc_id)]
        if not matches:
            raise SmokeTestError(
                f"acc_id={acc_id} is not an eligible US SIM margin account "
                "(requires active, non-MASTER US authorization)."
            )
        return matches[0]
    if len(accounts) != 1:
        if not accounts:
            raise SmokeTestError(
                "No eligible US SIM margin account was discovered; "
                "a STOCK_AND_OPTION/MARGIN account with US authorization is required."
            )
        choices = ", ".join(str(account.acc_id) for account in accounts)
        raise SmokeTestError(
            f"Multiple eligible US SIM accounts found ({choices}); rerun preflight with --acc-id."
        )
    return accounts[0]


def _import_moomoo():
    try:
        import moomoo as moo  # type: ignore
    except ImportError as exc:
        raise SmokeTestError(
            "The moomoo SDK is unavailable to this interpreter. Use the installed "
            "moomoo environment (with APPDATA redirected if needed)."
        ) from exc
    return moo


def discover_sim_accounts(
    host: str = "127.0.0.1",
    port: int = 11111,
    *,
    security_firm: str | None = None,
) -> list[dict[str, Any]]:
    """Discover account rows without querying balances or placing orders.

    The first ``NONE`` context normally returns all account rows.  If it does
    not, the known security-firm contexts are tried as a fallback, mirroring
    the repository's account helper.  Only the account-list endpoint is used.
    """
    moo = _import_moomoo()
    market_none = getattr(moo.TrdMarket, "NONE", None)
    if market_none is None:
        raise SmokeTestError("The installed moomoo SDK does not expose TrdMarket.NONE")

    requested = security_firm.strip().upper() if security_firm else None
    firm_values: list[Any] = [None]
    if requested:
        firm = getattr(getattr(moo, "SecurityFirm", object()), requested, requested)
        firm_values = [firm]
    else:
        # Always query NONE first, then every known firm.  Different OpenD
        # versions expose different account subsets per context, so omitting
        # NONE or stopping after the first eligible row can hide an account
        # and incorrectly auto-select one.
        firm_values.extend(
            getattr(moo.SecurityFirm, name, None)
            for name in ("FUTUSECURITIES", "FUTUINC", "FUTUSG", "FUTUAU", "FUTUCA", "FUTUJP", "FUTUMY")
        )
        firm_values = [None] + [value for value in firm_values[1:] if value is not None]

    rows_by_id: dict[int, dict[str, Any]] = {}
    errors: list[str] = []
    for index, firm in enumerate(firm_values):
        context = None
        try:
            kwargs: dict[str, Any] = {
                "host": host,
                "port": int(port),
                "is_encrypt": False,
                "filter_trdmarket": market_none,
            }
            if firm is not None:
                kwargs["security_firm"] = firm
            try:
                context = moo.OpenSecTradeContext(**kwargs)
            except TypeError:
                # Older SDKs may not accept security_firm on this constructor.
                kwargs.pop("security_firm", None)
                context = moo.OpenSecTradeContext(**kwargs)
            ret, data = context.get_acc_list()
            if ret != 0:
                errors.append(f"context {index}: get_acc_list ret={ret}: {data}")
                continue
            for row in _records(data):
                raw_id = _get(row, "acc_id", "account_id")
                try:
                    numeric_id = int(raw_id)
                except (TypeError, ValueError):
                    continue
                # Prefer rows with explicit US authorization/type details when
                # duplicate contexts return slightly different representations.
                previous = rows_by_id.get(numeric_id)
                quality = (
                    "US" in _normalise_auth(_get(row, "trdmarket_auth", "market_auth")),
                    _enum_name(_get(row, "acc_type", default="")) in {"MARGIN", "STOCK_AND_OPTION"},
                    _safe_account_firm(_get(row, "security_firm", default="")) is not None,
                )
                previous_quality = (
                    "US" in _normalise_auth(_get(previous, "trdmarket_auth", "market_auth")) if previous else False,
                    _enum_name(_get(previous, "acc_type", default="")) in {"MARGIN", "STOCK_AND_OPTION"} if previous else False,
                    _safe_account_firm(_get(previous, "security_firm", default="")) is not None if previous else False,
                )
                if previous is None or quality > previous_quality:
                    rows_by_id[numeric_id] = dict(row) if isinstance(row, dict) else row
            # NONE can still return a non-US subset on some OpenD versions.
            # Query every known firm when no firm was requested so that a
            # second eligible SIM account is not hidden by an early match.
            # ``select_sim_account`` must see the complete set before it can
            # safely require an explicit account ID.
        except Exception as exc:
            errors.append(f"context {index}: {exc}")
        finally:
            if context is not None:
                try:
                    context.close()
                except Exception:
                    pass
    if not rows_by_id:
        detail = f" Details: {'; '.join(errors[:3])}" if errors else ""
        raise SmokeTestError(f"OpenD account discovery returned no account rows.{detail}")
    return list(rows_by_id.values())


def _validate_sim_environment() -> None:
    requested = str(os.getenv("FUTU_TRD_ENV", "SIMULATE")).upper().strip()
    if requested == "REAL":
        raise SmokeTestError("SIM smoke test refuses REAL; unset FUTU_TRD_ENV=REAL or set it to SIMULATE.")
    if requested not in {"", "SIMULATE"}:
        raise SmokeTestError(f"Unsupported FUTU_TRD_ENV={requested!r}; only SIMULATE is allowed.")


def _validate_symbol(value: Any, label: str) -> str:
    symbol = _normalise_symbol(value)
    if not symbol.startswith("US.") or len(symbol) <= 3 or any(char.isspace() for char in symbol):
        raise SmokeTestError(f"{label} must be an explicit US symbol such as US.AAPL; got {value!r}")
    if "." not in symbol or symbol.count(".") != 1:
        raise SmokeTestError(f"{label} has an invalid US symbol format: {value!r}")
    return symbol


def _positive_int(value: Any, label: str) -> int:
    number = _safe_float(value)
    if number is None or number <= 0 or not number.is_integer():
        raise SmokeTestError(f"{label} must be a positive integer share quantity")
    return int(number)


def _validate_gross_cap(value: Any) -> float:
    cap = _safe_float(value)
    if cap is None or cap <= 0 or cap > HARD_MAX_GROSS_CAP:
        raise SmokeTestError(f"gross cap must be in (0, {HARD_MAX_GROSS_CAP:g}] for SIM smoke tests")
    return cap


def _et_now(now_fn: Callable[[], datetime] | None = None) -> datetime:
    now = now_fn() if now_fn else datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    if ZoneInfo is None:
        return now
    return now.astimezone(ZoneInfo(ET_NAME))


def _operation_date(now_fn: Callable[[], datetime] | None = None) -> str:
    return _et_now(now_fn).date().isoformat()


def _regular_session_blockers(
    states: Iterable[str],
    *,
    now_fn: Callable[[], datetime] | None = None,
) -> list[str]:
    now = _et_now(now_fn)
    blockers: list[str] = []
    if now.weekday() >= 5:
        blockers.append(f"US regular session is closed on weekends (America/New_York={now.isoformat()})")
    elif not (time(9, 30) <= now.time() < time(16, 0)):
        blockers.append(
            f"outside US regular hours 09:30-16:00 America/New_York (now={now.isoformat()})"
        )
    observed = {str(state).upper() for state in states if str(state).strip()}
    if not observed:
        blockers.append("OpenD returned no market-state rows")
    elif not observed.issubset(REGULAR_STATES):
        blockers.append(
            "OpenD market state is not regular US session: " + ", ".join(sorted(observed))
        )
    return blockers


def _position_qty(position: Any) -> float:
    return abs(_safe_float(_get(position, "quantity", "qty", "position"), 0.0) or 0.0)


def _position_symbol(position: Any) -> str:
    return _normalise_symbol(_get(position, "symbol", "code", default=""))


def _position_output(position: Any) -> dict[str, Any]:
    if isinstance(position, Position):
        return _as_output(asdict(position))
    return _as_output(dict(position)) if isinstance(position, dict) else _as_output(position)


def _balance_output(balance: Any) -> dict[str, Any]:
    if isinstance(balance, AccountBalance):
        return _as_output(asdict(balance))
    if isinstance(balance, dict):
        return _as_output(balance)
    return _as_output(balance)


def _state_rows(state: Any) -> list[dict[str, Any]]:
    if isinstance(state, dict):
        return _records(state.get("rows", state.get("data", [])))
    return _records(state)


def _snapshot_rows(snapshot: Any) -> list[dict[str, Any]]:
    if isinstance(snapshot, dict):
        return _records(snapshot.get("rows", snapshot.get("data", [])))
    return _records(snapshot)


def _snapshot_datetime(value: Any) -> datetime | None:
    """Parse Moomoo snapshot timestamps, treating naive values as US/Eastern."""
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            parsed = datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    elif hasattr(value, "to_pydatetime"):
        try:
            parsed = value.to_pydatetime()
        except Exception:
            return None
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            parsed = None
            for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
                try:
                    parsed = datetime.strptime(text, fmt)
                    break
                except ValueError:
                    continue
            if parsed is None:
                return None
    if parsed.tzinfo is None:
        if ZoneInfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        else:
            parsed = parsed.replace(tzinfo=ZoneInfo(ET_NAME))
    return parsed.astimezone(timezone.utc)


def _snapshot_freshness_blockers(
    rows: Iterable[Any],
    symbols: list[str],
    *,
    max_age_seconds: int = HARD_MAX_SNAPSHOT_AGE_SECONDS,
    now_fn: Callable[[], datetime] | None = None,
) -> list[str]:
    """Require a parseable, recent timestamp for every reference snapshot."""
    rows = list(rows)
    now = _et_now(now_fn).astimezone(timezone.utc)
    age_limit = min(int(max_age_seconds), HARD_MAX_SNAPSHOT_AGE_SECONDS)
    blockers: list[str] = []
    for symbol in symbols:
        row = next(
            (
                item
                for item in rows
                if _normalise_symbol(_get(item, "code", "symbol", default="")) == symbol
            ),
            None,
        )
        raw_timestamp = _get(row, "update_time", "timestamp", "time", default=None)
        timestamp = _snapshot_datetime(raw_timestamp)
        if timestamp is None:
            blockers.append(f"OpenD snapshot for {symbol} has no parseable update_time")
            continue
        age = (now - timestamp).total_seconds()
        if age < -60:
            blockers.append(f"OpenD snapshot for {symbol} is from the future ({age:.0f}s age)")
        elif age > age_limit:
            blockers.append(
                f"OpenD snapshot for {symbol} is stale ({age:.0f}s old; max {age_limit}s)"
            )
    return blockers


def _extract_prices(rows: Iterable[Any], symbols: list[str]) -> tuple[dict[str, float], list[str]]:
    by_symbol: dict[str, float] = {}
    blockers: list[str] = []
    for row in rows:
        symbol = _normalise_symbol(_get(row, "code", "symbol", default=""))
        price = _safe_float(_get(row, "last_price", "price", "current_price"))
        if symbol in symbols and price is not None and price > 0:
            by_symbol[symbol] = price
    for symbol in symbols:
        if symbol not in by_symbol:
            blockers.append(f"no positive OpenD snapshot last_price for {symbol}")
    return by_symbol, blockers


def _read_saved_selection(db_path: Path) -> dict[str, Any] | None:
    raw = get_system_state("sim_smoke_selection", db_path)
    if not raw:
        return None
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _persist_selection(db_path: Path, account: SimAccount, *, symbols: list[str], pair: str) -> None:
    set_system_state(
        "sim_smoke_selection",
        {
            "account": account.as_dict(),
            "symbols": symbols,
            "pair": pair,
            "environment": "SIMULATE",
        },
        db_path,
    )


def _config_for(account: SimAccount, args: argparse.Namespace, db_path: Path) -> ExecutionConfig:
    base = ExecutionConfig.from_env()
    gross_cap = _validate_gross_cap(getattr(args, "gross_cap", DEFAULT_GROSS_CAP))
    config = base.with_overrides(
        trd_env="SIMULATE",
        market="US",
        account_id=account.acc_id,
        security_firm=getattr(args, "security_firm", None) or account.security_firm,
        state_db_path=str(db_path),
        # The smoke operation is one pair and must remain below the explicit
        # command-line cap even if the broader engine defaults are larger.
        max_gross_exposure=min(base.max_gross_exposure, gross_cap),
        max_pair_exposure=min(base.max_pair_exposure, gross_cap),
        max_open_pairs=1,
        max_pending_operations=2,
    )
    config.validate()
    return config


def _prepare(
    args: argparse.Namespace,
    *,
    discoverer: Callable[..., list[dict[str, Any]]] = discover_sim_accounts,
    adapter_factory: Callable[..., Any] = MooMooAdapter,
) -> dict[str, Any]:
    _validate_sim_environment()
    stage = str(getattr(args, "stage", "")).lower()
    if stage not in {"preflight", "enter", "status", "exit"}:
        raise SmokeTestError(f"unknown smoke-test stage: {stage}")
    if stage in {"enter", "exit"} and not bool(getattr(args, "submit", False)):
        raise SmokeTestError(f"{stage} requires explicit --submit; no order was attempted")
    if stage in {"preflight", "status"} and bool(getattr(args, "submit", False)):
        raise SmokeTestError(f"--submit is only valid for enter/exit, not {stage}")

    state_db_arg = getattr(args, "state_db", None)
    if not state_db_arg:
        raise SmokeTestError("--state-db is required and must point to an isolated smoke-test database")
    db_path = resolve_db_path(state_db_arg)
    if db_path.resolve() == resolve_db_path(DB_PATH).resolve():
        raise SmokeTestError("The smoke harness refuses the normal trading database; pass an isolated --state-db")
    init_db(db_path, trd_env="SIMULATE")

    symbol1 = _validate_symbol(getattr(args, "symbol1", None), "--symbol1")
    symbol2 = _validate_symbol(getattr(args, "symbol2", None), "--symbol2")
    if symbol1 == symbol2:
        raise SmokeTestError("--symbol1 and --symbol2 must be different symbols")
    symbols = [symbol1, symbol2]
    pair = str(getattr(args, "pair", None) or f"{symbol1}-{symbol2}").strip()
    if not pair:
        raise SmokeTestError("pair label cannot be empty")
    quantity1 = _positive_int(getattr(args, "quantity1", 1), "--quantity1")
    quantity2 = _positive_int(getattr(args, "quantity2", 1), "--quantity2")
    gross_cap = _validate_gross_cap(getattr(args, "gross_cap", DEFAULT_GROSS_CAP))

    explicit_acc_id = getattr(args, "acc_id", None)
    if stage in {"enter", "exit"} and explicit_acc_id is None:
        raise SmokeTestError(f"{stage} requires the explicit --acc-id selected by preflight")
    saved = _read_saved_selection(db_path)
    discovery_kwargs = {
        "host": getattr(args, "host", "127.0.0.1"),
        "port": int(getattr(args, "port", 11111)),
        "security_firm": getattr(args, "security_firm", None),
    }
    rows = discoverer(**discovery_kwargs)
    selected_id = explicit_acc_id
    if selected_id is None and stage == "status" and saved:
        try:
            selected_id = int(saved.get("account", {}).get("acc_id"))
        except (TypeError, ValueError):
            selected_id = None
    account = select_sim_account(rows, selected_id, market="US")
    if stage in {"enter", "exit"} and not saved:
        raise SmokeTestError(
            f"{stage} requires a completed preflight selection in this isolated DB; "
            "run preflight first."
        )
    if saved:
        saved_id = saved.get("account", {}).get("acc_id")
        if saved_id is None or int(saved_id) != account.acc_id:
            raise SmokeTestError(
                f"--acc-id {account.acc_id} differs from preflight-selected SIM account {saved_id}; "
                "use the same account or start a fresh isolated DB."
            )
        if str(saved.get("environment", "")).upper() != "SIMULATE":
            raise SmokeTestError("the isolated DB was not selected for SIMULATE; start a fresh smoke DB")
        saved_symbols = [_normalise_symbol(value) for value in saved.get("symbols", [])]
        if saved_symbols != symbols or str(saved.get("pair", "")) != pair:
            raise SmokeTestError(
                "stage symbols/pair differ from the completed preflight; "
                "use the same pair or start a fresh isolated DB."
            )
    config = _config_for(account, args, db_path)
    adapter = adapter_factory(
        host=getattr(args, "host", "127.0.0.1"),
        port=int(getattr(args, "port", 11111)),
        market="US",
        trd_env="SIMULATE",
        acc_id=account.acc_id,
        security_firm=config.security_firm,
        db_path=str(db_path),
    )
    try:
        connected = adapter.connect()
    except Exception:
        try:
            adapter.disconnect()
        except Exception:
            pass
        raise
    if connected is False:
        try:
            adapter.disconnect()
        except Exception:
            pass
        raise SmokeTestError("MooMooAdapter.connect() returned false")
    # Keep the legacy exception at the smoke-harness boundary. Neither the
    # shared Moomoo adapter nor general reconciliation logic receives it.
    smoke_adapter = _SmokeTerminalOrderAllowlistAdapter(adapter, account.acc_id)
    engine = ExecutionEngine(smoke_adapter, config=config, db_path=db_path)
    return {
        "stage": stage,
        "db_path": db_path,
        "account": account,
        "adapter": smoke_adapter,
        "engine": engine,
        "config": config,
        "symbols": symbols,
        "pair": pair,
        "quantity1": quantity1,
        "quantity2": quantity2,
        "gross_cap": gross_cap,
    }


def _base_result(ctx: dict[str, Any]) -> dict[str, Any]:
    account: SimAccount = ctx["account"]
    return {
        "stage": ctx["stage"],
        "environment": "SIMULATE",
        "account": account.as_dict(),
        "pair": ctx["pair"],
        "symbols": list(ctx["symbols"]),
        "state_db": str(ctx["db_path"]),
        "orders_submitted": False,
        "blockers": [],
        "warnings": [],
    }


def _add_permitted_terminal_order_audit(result: dict[str, Any], adapter: Any) -> None:
    if isinstance(adapter, _SmokeTerminalOrderAllowlistAdapter):
        result["permitted_terminal_external_orders"] = _as_output(adapter.permitted_terminal_orders())


def _run_preflight(
    ctx: dict[str, Any],
    args: argparse.Namespace,
    *,
    require_regular: bool,
    now_fn: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    adapter = ctx["adapter"]
    engine: ExecutionEngine = ctx["engine"]
    result = _base_result(ctx)
    blockers: list[str] = []

    try:
        balance = adapter.get_account_balance()
        result["balance"] = _balance_output(balance)
        equity = _safe_float(_get(balance, "equity", "total_assets"))
        buying_power = _safe_float(_get(balance, "buying_power", "power", "available_power"))
        if equity is None or equity <= 0:
            blockers.append("account equity is unavailable or non-positive")
        if buying_power is None or buying_power <= 0:
            blockers.append("account buying power is unavailable or non-positive")
    except Exception as exc:
        balance = None
        blockers.append(f"account query failed: {exc}")

    try:
        broker_positions = list(adapter.get_positions())
        result["broker_positions"] = [_position_output(position) for position in broker_positions]
    except Exception as exc:
        broker_positions = []
        blockers.append(f"position query failed: {exc}")

    try:
        open_orders = list(adapter.get_open_orders())
        result["open_orders"] = _as_output(open_orders)
        if open_orders and ctx["stage"] in {"preflight", "enter"}:
            blockers.append(f"account has {len(open_orders)} open broker order(s); smoke entry will not add another")
    except Exception as exc:
        open_orders = []
        blockers.append(f"open-order query failed: {exc}")

    try:
        symbols_valid = bool(adapter.validate_symbols(ctx["symbols"]))
        result["symbols_valid"] = symbols_valid
        if not symbols_valid:
            blockers.append("OpenD symbol validation did not cover both explicit US symbols")
    except Exception as exc:
        result["symbols_valid"] = False
        blockers.append(f"symbol validation failed: {exc}")

    try:
        market_state = adapter.get_market_state(ctx["symbols"])
        state_rows = _state_rows(market_state)
        result["market_state"] = _as_output(state_rows)
        seen_codes = {_normalise_symbol(_get(row, "code", "symbol", default="")) for row in state_rows}
        missing = [symbol for symbol in ctx["symbols"] if symbol not in seen_codes]
        if missing:
            blockers.append("OpenD market-state response omitted: " + ", ".join(missing))
        states = {_enum_name(_get(row, "market_state", "state", default="")) for row in state_rows}
        result["market_states"] = sorted(state for state in states if state)
        if require_regular:
            blockers.extend(_regular_session_blockers(states, now_fn=now_fn))
    except Exception as exc:
        result["market_state"] = []
        blockers.append(f"market-state query failed: {exc}")

    try:
        snapshot = adapter.get_market_snapshot(ctx["symbols"])
        snapshot_rows = _snapshot_rows(snapshot)
        prices, price_blockers = _extract_prices(snapshot_rows, ctx["symbols"])
        result["reference_prices"] = {
            symbol: {
                "last_price": prices[symbol],
                "source": "MooMoo/OpenD market snapshot",
                "update_time": _as_output(_get(next((row for row in snapshot_rows if _normalise_symbol(_get(row, "code", "symbol", default="")) == symbol), {}), "update_time", default=None)),
            }
            for symbol in prices
        }
        blockers.extend(price_blockers)
        blockers.extend(
            _snapshot_freshness_blockers(
                snapshot_rows,
                ctx["symbols"],
                max_age_seconds=ctx["config"].data_max_age_seconds,
                now_fn=now_fn,
            )
        )
    except Exception as exc:
        prices = {}
        result["reference_prices"] = {}
        blockers.append(f"reference-price snapshot failed: {exc}")

    proposed_gross = sum(
        prices.get(symbol, 0.0) * quantity
        for symbol, quantity in zip(ctx["symbols"], (ctx["quantity1"], ctx["quantity2"]))
    )
    result["proposed_gross_exposure"] = proposed_gross
    result["gross_cap"] = ctx["gross_cap"]
    if proposed_gross <= 0 or proposed_gross > ctx["gross_cap"]:
        blockers.append(
            f"proposed gross exposure ${proposed_gross:,.2f} exceeds the explicit SIM cap ${ctx['gross_cap']:,.2f}"
        )

    # A new smoke entry must start with a flat, clean account.  Existing
    # positions/orders are never cancelled or closed by this harness.
    if ctx["stage"] in {"preflight", "enter"}:
        nonzero_positions = [position for position in broker_positions if _position_qty(position) > 0]
        if nonzero_positions:
            description = ", ".join(
                f"{_position_symbol(position)} x {_position_qty(position):g}"
                for position in nonzero_positions
            )
            blockers.append(f"SIM account is not flat; existing positions detected ({description})")
        local_open = get_all_open_positions(ctx["db_path"])
        if local_open:
            blockers.append(f"isolated smoke DB already has {len(local_open)} open local position(s)")

    # Reconciliation is mandatory before every operation, even in SIMULATE.
    try:
        with engine.run_lock():
            reconciliation = engine.startup_reconcile()
    except Exception as exc:
        reconciliation = None
        blockers.append(f"startup reconciliation failed: {exc}")
    if reconciliation is not None:
        result["reconciliation_ready"] = reconciliation.ready
        result["reconciliation_issues"] = _as_output([asdict(issue) for issue in reconciliation.issues])
        if not reconciliation.ready:
            blockers.append(f"startup reconciliation has {len(reconciliation.issues)} unresolved issue(s)")
    _add_permitted_terminal_order_audit(result, adapter)

    if ctx["stage"] == "exit":
        local_position = get_open_position(ctx["pair"], ctx["db_path"])
        result["local_position"] = _as_output(local_position)
        if local_position is None:
            blockers.append(f"no open local smoke position exists for {ctx['pair']}")

    # Exercise the same centralized entry risk admission used immediately
    # before submission.  It is a read-only gate and never creates an order.
    if ctx["stage"] in {"preflight", "enter"} and len(prices) == 2 and balance is not None:
        legs = [
            {
                "symbol": ctx["symbols"][0],
                "side": str(getattr(args, "side1", "BUY")).upper(),
                "intended_price": prices[ctx["symbols"][0]],
                "requested_quantity": ctx["quantity1"],
            },
            {
                "symbol": ctx["symbols"][1],
                "side": str(getattr(args, "side2", "SELL")).upper(),
                "intended_price": prices[ctx["symbols"][1]],
                "requested_quantity": ctx["quantity2"],
            },
        ]
        try:
            engine._admit_entry(legs, signal_timestamp=datetime.now(timezone.utc))
            result["risk_admission"] = "approved"
        except Exception as exc:
            result["risk_admission"] = "blocked"
            blockers.append(f"centralized risk admission blocked entry: {exc}")

    result["ready_for_submit"] = not blockers
    result["blockers"] = list(dict.fromkeys(blockers))
    # A selection is a completed preflight marker only after every gate has
    # passed.  Merely opening the status command must never arm a later
    # mutating command.
    if result["ready_for_submit"]:
        _persist_selection(
            ctx["db_path"],
            ctx["account"],
            symbols=ctx["symbols"],
            pair=ctx["pair"],
        )
    set_system_state("sim_smoke_last_preflight", _as_output(result), ctx["db_path"])
    return result


def _entry_signal_and_plan(ctx: dict[str, Any], args: argparse.Namespace, prices: dict[str, float]) -> tuple[dict, dict]:
    zscore = _safe_float(getattr(args, "entry_zscore", 2.5), 2.5) or 2.5
    signal = {
        "pair": ctx["pair"],
        "ticker1": ctx["symbols"][0],
        "ticker2": ctx["symbols"][1],
        "entry_zscore": zscore,
        "entry_hedge_ratio": 1.0,
        "entry_alpha": 0.0,
        "entry_residual_mean": 0.0,
        "entry_residual_std": 1.0,
        "latest_price_s1": prices[ctx["symbols"][0]],
        "latest_price_s2": prices[ctx["symbols"][1]],
    }
    plan = {
        "ticker1_side": str(getattr(args, "side1", "BUY")).upper(),
        "ticker2_side": str(getattr(args, "side2", "SELL")).upper(),
        "ticker1_qty": ctx["quantity1"],
        "ticker2_qty": ctx["quantity2"],
        "ticker1_intended_qty": float(ctx["quantity1"]),
        "ticker2_intended_qty": float(ctx["quantity2"]),
    }
    return signal, plan


def _run_entry(
    ctx: dict[str, Any],
    args: argparse.Namespace,
    preflight: dict[str, Any],
    *,
    now_fn: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    if not preflight.get("ready_for_submit"):
        raise SmokeTestBlocked("entry blocked by preflight; no paper orders were submitted", preflight)
    prices = {symbol: float(info["last_price"]) for symbol, info in preflight["reference_prices"].items()}
    signal, plan = _entry_signal_and_plan(ctx, args, prices)
    engine: ExecutionEngine = ctx["engine"]
    operation_date = _operation_date(now_fn)
    with engine.run_lock():
        result = engine.execute_entry(
            signal,
            plan,
            signal_timestamp=datetime.now(timezone.utc),
            operation_date=operation_date,
            strategy_id=SMOKE_STRATEGY_ID,
        )
        final = engine.reconcile(timeout=int(getattr(args, "timeout", 15)))
    operation = get_operation(result["operation_id"], ctx["db_path"]) or result
    output = dict(preflight)
    output.update(
        {
            "stage": "enter",
            "orders_submitted": True,
            "operation": _as_output(operation),
            "reconciliation_ready": final.ready,
            "reconciliation_issues": _as_output([asdict(issue) for issue in final.issues]),
            "entry_fully_filled": str(operation.get("status")) == "open"
            and all(str(leg.get("status")) == "filled" for leg in operation.get("legs", [])),
        }
    )
    output["ready_for_submit"] = False
    output["blockers"] = [] if output["entry_fully_filled"] else [
        "entry did not reach two-leg fully-filled/open state within the timeout; inspect status before any exit"
    ]
    return output


def _run_exit(
    ctx: dict[str, Any],
    args: argparse.Namespace,
    preflight: dict[str, Any],
    *,
    now_fn: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    if not preflight.get("ready_for_submit"):
        raise SmokeTestBlocked("exit blocked by preflight; no paper orders were submitted", preflight)
    position = get_open_position(ctx["pair"], ctx["db_path"])
    if position is None:
        raise SmokeTestBlocked("exit requires an open smoke position", preflight)
    prices = {symbol: float(info["last_price"]) for symbol, info in preflight["reference_prices"].items()}
    signal = {
        "pair": ctx["pair"],
        "ticker1": ctx["symbols"][0],
        "ticker2": ctx["symbols"][1],
        "exit_reason": "smoke_test_exit",
        "exit_zscore": 0.0,
        "latest_price_s1": prices[ctx["symbols"][0]],
        "latest_price_s2": prices[ctx["symbols"][1]],
        "open_pos": position,
    }
    engine: ExecutionEngine = ctx["engine"]
    with engine.run_lock():
        result = engine.execute_exit(
            signal,
            signal_timestamp=datetime.now(timezone.utc),
            operation_date=_operation_date(now_fn),
            strategy_id=SMOKE_STRATEGY_ID,
        )
        final = engine.reconcile(timeout=int(getattr(args, "timeout", 15)))
    operation = get_operation(result["operation_id"], ctx["db_path"]) or result
    remaining = get_open_position(ctx["pair"], ctx["db_path"])
    output = dict(preflight)
    output.update(
        {
            "stage": "exit",
            "orders_submitted": True,
            "operation": _as_output(operation),
            "reconciliation_ready": final.ready,
            "reconciliation_issues": _as_output([asdict(issue) for issue in final.issues]),
            "pair_flat_locally": remaining is None,
            "local_position": _as_output(remaining),
        }
    )
    output["ready_for_submit"] = False
    output["blockers"] = [] if remaining is None and str(operation.get("status")) == "closed" else [
        "exit did not reach a closed/two-leg-flat state within the timeout; inspect status before retrying"
    ]
    return output


def _run_status(ctx: dict[str, Any]) -> dict[str, Any]:
    adapter = ctx["adapter"]
    engine: ExecutionEngine = ctx["engine"]
    output = _base_result(ctx)
    with engine.run_lock():
        reconciliation = engine.startup_reconcile()
    blockers = []
    if not reconciliation.ready:
        blockers.append(
            f"startup reconciliation has {len(reconciliation.issues)} unresolved issue(s); "
            "inspect the broker/local mismatch before any mutating stage"
        )
    output.update(
        {
            "reconciliation_ready": reconciliation.ready,
            "reconciliation_issues": _as_output([asdict(issue) for issue in reconciliation.issues]),
            "broker_positions": [_position_output(item) for item in adapter.get_positions()],
            "open_orders": _as_output(list(adapter.get_open_orders())),
            "recent_orders": _as_output(list(adapter.get_recent_orders())),
            "permitted_terminal_external_orders": _as_output(adapter.permitted_terminal_orders()),
            "local_open_positions": _as_output(get_all_open_positions(ctx["db_path"])),
            "local_orders": _as_output(get_all_orders(ctx["db_path"])),
            "ready_for_submit": False,
            "blockers": blockers,
        }
    )
    return output


def run_stage(
    args: argparse.Namespace,
    *,
    discoverer: Callable[..., list[dict[str, Any]]] = discover_sim_accounts,
    adapter_factory: Callable[..., Any] = MooMooAdapter,
    now_fn: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    """Run one stage; dependency arguments make safety gates unit-testable."""
    ctx = _prepare(args, discoverer=discoverer, adapter_factory=adapter_factory)
    try:
        if ctx["stage"] == "status":
            return _run_status(ctx)
        preflight = _run_preflight(ctx, args, require_regular=True, now_fn=now_fn)
        if ctx["stage"] == "preflight":
            return preflight
        if ctx["stage"] == "enter":
            return _run_entry(ctx, args, preflight, now_fn=now_fn)
        return _run_exit(ctx, args, preflight, now_fn=now_fn)
    finally:
        try:
            ctx["adapter"].disconnect()
        except Exception:
            pass


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Supervised SIM-only two-leg Moomoo smoke test")
    parser.add_argument("stage", choices=("preflight", "enter", "status", "exit"))
    parser.add_argument("--state-db", required=True, help="isolated SQLite path; data/trading.db is refused")
    parser.add_argument("--symbol1", required=True, help="explicit US symbol, e.g. US.AAPL")
    parser.add_argument("--symbol2", required=True, help="explicit US symbol, e.g. US.MSFT")
    parser.add_argument("--pair", help="optional durable pair label")
    parser.add_argument("--acc-id", type=int, help="explicit eligible SIM account; required for enter/exit")
    parser.add_argument("--security-firm", help="optional broker firm override discovered during preflight")
    parser.add_argument("--quantity1", type=float, default=1, help="positive integer shares for symbol1 (default 1)")
    parser.add_argument("--quantity2", type=float, default=1, help="positive integer shares for symbol2 (default 1)")
    parser.add_argument("--side1", choices=("BUY", "SELL"), default="BUY")
    parser.add_argument("--side2", choices=("BUY", "SELL"), default="SELL")
    parser.add_argument("--entry-zscore", type=float, default=2.5, help="metadata only; no strategy signal is generated")
    parser.add_argument("--gross-cap", type=float, default=DEFAULT_GROSS_CAP, help="hard SIM gross cap, max 5000")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=11111)
    parser.add_argument("--timeout", type=int, default=15, help="seconds to poll fills after each order")
    parser.add_argument("--submit", action="store_true", help="required by enter/exit; explicitly permits paper orders")
    parser.add_argument("--json", action="store_true", help="print machine-readable JSON")
    return parser


def _print_result(result: dict[str, Any], as_json: bool) -> None:
    safe = _as_output(result)
    if as_json:
        print(json.dumps(safe, ensure_ascii=False, sort_keys=True))
        return
    print(f"Stage: {safe.get('stage')}  Environment: {safe.get('environment')}  Account: {safe.get('account', {}).get('acc_id')}")
    print(f"Pair: {safe.get('pair')}  State DB: {safe.get('state_db')}")
    if safe.get("market_states"):
        print(f"Market state: {', '.join(safe['market_states'])}")
    if safe.get("reference_prices"):
        print("Reference prices: " + ", ".join(f"{key}={value['last_price']}" for key, value in safe["reference_prices"].items()))
    if "proposed_gross_exposure" in safe:
        print(f"Proposed gross: ${safe['proposed_gross_exposure']:,.2f} / cap ${safe['gross_cap']:,.2f}")
    print(f"Reconciliation ready: {safe.get('reconciliation_ready', 'not run')}")
    print(f"Orders submitted: {safe.get('orders_submitted', False)}")
    if safe.get("operation"):
        operation = safe["operation"]
        print(f"Operation: {operation.get('operation_id')} status={operation.get('status')}")
        for leg in operation.get("legs", []):
            print(
                f"  {leg.get('leg')}: {leg.get('symbol')} {leg.get('side')} "
                f"status={leg.get('status')} broker_order_id={leg.get('broker_order_id')} "
                f"filled={leg.get('cumulative_filled_quantity')}"
            )
    blockers = safe.get("blockers") or []
    if blockers:
        print("BLOCKED:")
        for blocker in blockers:
            print(f"  - {blocker}")
    else:
        print("Ready for the next requested stage.")


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        _validate_gross_cap(args.gross_cap)
        _positive_int(args.quantity1, "--quantity1")
        _positive_int(args.quantity2, "--quantity2")
        result = run_stage(args)
        _print_result(result, args.json)
        return 0 if not result.get("blockers") else 2
    except SmokeTestBlocked as exc:
        result = dict(exc.result)
        result.setdefault("stage", getattr(args, "stage", "unknown"))
        result.setdefault("environment", "SIMULATE")
        result.setdefault("orders_submitted", False)
        result.setdefault("blockers", [str(exc)])
        _print_result(result, args.json)
        return 2
    except (SmokeTestError, ExecutionSafetyError, ValueError, PermissionError) as exc:
        print(f"BLOCKED: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # pragma: no cover - final CLI safety net
        print(f"FATAL: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
