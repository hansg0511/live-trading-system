"""Pure response-normalization helpers shared by Moomoo adapters.

This module deliberately has no dependency on either execution engine or the
Moomoo SDK.  It keeps provider-shape handling reusable as the repository moves
from the legacy pair engine to the broker-neutral execution path.
"""

from __future__ import annotations

import ast
import math
from typing import Any

import pandas as pd


TRD_MARKET_NUMBER_TO_NAME = {
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


def normalise_env(value: Any) -> str:
    if value is None:
        return ""
    name = getattr(value, "name", None)
    value = name or value
    return str(value).upper().split(".")[-1]


def as_records(data: Any) -> list[dict[str, Any]]:
    if data is None:
        return []
    if isinstance(data, pd.DataFrame):
        return data.to_dict("records")
    if isinstance(data, list):
        return [dict(item) if isinstance(item, dict) else item for item in data]
    if hasattr(data, "to_dict"):
        try:
            return data.to_dict("records")
        except Exception:
            return []
    return []


def get_value(row: Any, *keys: str, default: Any = None) -> Any:
    if row is None:
        return default
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


def safe_float(value: Any, default: float | None = 0.0) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def parse_market_auth(value: Any) -> set[str]:
    def market_name(item: Any) -> str:
        normalized = normalise_env(item)
        return TRD_MARKET_NUMBER_TO_NAME.get(normalized, normalized)

    if value is None:
        return set()
    if isinstance(value, (list, tuple, set)):
        return {market_name(item) for item in value}
    text = str(value).strip()
    try:
        parsed = ast.literal_eval(text)
        if isinstance(parsed, (list, tuple, set)):
            return {market_name(item) for item in parsed}
    except (SyntaxError, ValueError):
        pass
    return {
        TRD_MARKET_NUMBER_TO_NAME.get(part.strip().upper().split(".")[-1], part.strip().upper().split(".")[-1])
        for part in text.replace(";", ",").split(",")
        if part.strip()
    }


def status_name(value: Any) -> str:
    return normalise_env(value)


__all__ = [
    "TRD_MARKET_NUMBER_TO_NAME",
    "as_records",
    "get_value",
    "normalise_env",
    "parse_market_auth",
    "safe_float",
    "status_name",
]
