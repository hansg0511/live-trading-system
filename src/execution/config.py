from __future__ import annotations

from dataclasses import dataclass, replace
import math
import os
from pathlib import Path
from typing import Any

from src.db.positions_db import DB_PATH, resolve_db_path


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value in (None, ""):
        return default
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be numeric") from exc


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value in (None, ""):
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc


@dataclass(frozen=True)
class ExecutionConfig:
    """Runtime controls for the execution engine.

    Defaults are intentionally conservative and non-live.  Strategy thresholds
    and sizing are not part of this configuration.
    """

    trd_env: str = "SIMULATE"
    market: str = "US"
    account_id: int | None = None
    expected_real_account_id: int | None = None
    enable_live_trading: bool = False
    security_firm: str | None = None
    state_db_path: str = DB_PATH
    max_account_utilization: float = 0.25
    max_margin_utilization: float = 0.50
    max_gross_exposure: float = 50_000.0
    max_pair_exposure: float = 10_000.0
    max_open_pairs: int = 5
    max_pending_operations: int = 5
    estimated_margin_rate: float = 1.0
    data_max_age_seconds: int = 900
    require_market_state: bool = True
    require_symbol_validation: bool = True

    @classmethod
    def from_env(cls) -> "ExecutionConfig":
        expected = os.getenv("EXPECTED_REAL_ACCOUNT_ID") or os.getenv("FUTU_EXPECTED_ACCOUNT_ID")
        account = os.getenv("FUTU_ACC_ID")
        return cls(
            trd_env=os.getenv("FUTU_TRD_ENV", "SIMULATE").upper(),
            market=os.getenv("FUTU_DEFAULT_MARKET", "US").upper(),
            account_id=int(account) if account else None,
            expected_real_account_id=int(expected) if expected else None,
            enable_live_trading=_env_bool("ENABLE_LIVE_TRADING", False),
            security_firm=os.getenv("FUTU_SECURITY_FIRM") or None,
            state_db_path=os.getenv("TRADING_STATE_DB") or DB_PATH,
            max_account_utilization=_env_float("MAX_ACCOUNT_UTILIZATION", 0.25),
            max_margin_utilization=_env_float("MAX_MARGIN_UTILIZATION", 0.50),
            max_gross_exposure=_env_float("MAX_GROSS_EXPOSURE", 50_000.0),
            max_pair_exposure=_env_float("MAX_PAIR_EXPOSURE", 10_000.0),
            max_open_pairs=_env_int("MAX_OPEN_PAIRS", 5),
            max_pending_operations=_env_int("MAX_PENDING_OPERATIONS", 5),
            estimated_margin_rate=_env_float("ESTIMATED_MARGIN_RATE", 1.0),
            data_max_age_seconds=_env_int("DATA_MAX_AGE_SECONDS", 900),
            require_market_state=_env_bool("REQUIRE_MARKET_STATE", True),
            require_symbol_validation=_env_bool("REQUIRE_SYMBOL_VALIDATION", True),
        )

    @property
    def is_real(self) -> bool:
        return self.trd_env.upper() == "REAL"

    @property
    def db_path(self) -> Path:
        return resolve_db_path(self.state_db_path)

    def with_overrides(self, **kwargs: Any) -> "ExecutionConfig":
        return replace(self, **kwargs)

    def validate(self) -> None:
        env = self.trd_env.upper()
        if env not in {"SIMULATE", "REAL"}:
            raise ValueError("FUTU_TRD_ENV/trd_env must be SIMULATE or REAL")
        if not math.isfinite(self.max_account_utilization) or self.max_account_utilization <= 0 or self.max_account_utilization > 1:
            raise ValueError("max_account_utilization must be in (0, 1]")
        if not math.isfinite(self.max_margin_utilization) or self.max_margin_utilization <= 0 or self.max_margin_utilization > 1:
            raise ValueError("max_margin_utilization must be in (0, 1]")
        if (
            not math.isfinite(self.max_gross_exposure)
            or not math.isfinite(self.max_pair_exposure)
            or self.max_gross_exposure <= 0
            or self.max_pair_exposure <= 0
        ):
            raise ValueError("gross and pair exposure caps must be positive")
        if self.max_open_pairs < 1 or self.max_pending_operations < 1:
            raise ValueError("position and pending-operation caps must be positive")
        if self.data_max_age_seconds <= 0:
            raise ValueError("data_max_age_seconds must be positive")
        if not math.isfinite(self.estimated_margin_rate) or self.estimated_margin_rate <= 0:
            raise ValueError("estimated_margin_rate must be finite and positive")
        if self.account_id is not None and self.account_id <= 0:
            raise ValueError("account_id must be positive")
        if self.is_real:
            if not self.enable_live_trading:
                raise PermissionError("REAL trading requires ENABLE_LIVE_TRADING=true")
            if self.expected_real_account_id is None:
                raise PermissionError("REAL trading requires EXPECTED_REAL_ACCOUNT_ID")
            if self.expected_real_account_id <= 0:
                raise PermissionError("EXPECTED_REAL_ACCOUNT_ID must be positive")
            if self.account_id is not None and self.account_id != self.expected_real_account_id:
                raise PermissionError("account_id must exactly match EXPECTED_REAL_ACCOUNT_ID")

    def startup_summary(self) -> str:
        self.validate()
        account = self.account_id or self.expected_real_account_id or "unselected"
        armed = self.is_real and self.enable_live_trading
        return (
            f"Execution environment={self.trd_env.upper()} account_id={account} "
            f"live_armed={'YES' if armed else 'NO'} "
            f"gross_cap={self.max_gross_exposure:.2f} "
            f"pair_cap={self.max_pair_exposure:.2f} "
            f"account_util_cap={self.max_account_utilization:.1%} "
            f"margin_util_cap={self.max_margin_utilization:.1%}"
        )
