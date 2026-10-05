"""Pure response-normalization helpers shared by Moomoo adapters.

This module deliberately has no dependency on either execution engine or the
Moomoo SDK.  It keeps provider-shape handling reusable as the repository moves
from the legacy pair engine to the broker-neutral execution path.
"""

from __future__ import annotations

import ast
from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
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


class MoomooResponseShapeError(ValueError):
    """The SDK returned a shape that cannot be treated as tabular records.

    An empty list is a valid, positively recognized empty result.  Malformed,
    unsupported, or otherwise unrecognized values are deliberately distinct so
    account-wide safety gates cannot mistake them for an empty account.
    """


def normalise_env(value: Any) -> str:
    if value is None:
        return ""
    name = getattr(value, "name", None)
    value = name or value
    return str(value).upper().split(".")[-1]


def canonicalize_payload(value: Any, *, _path: str = "response", _depth: int = 0) -> Any:
    """Convert recognized SDK/table values to recursively inspectable data.

    The generic OMS deliberately only reasons over mappings, sequences, and
    scalar values.  Returning a pandas/DataFrame or SDK-specific row object in
    a raw payload would make its evidence walkers silently skip provider facts.
    Recognized tables are converted to primitive row structures; opaque values
    are rejected instead of being treated as empty or harmless metadata.
    """
    if _depth > 50:
        raise MoomooResponseShapeError(f"Moomoo response nesting exceeds the safe limit at {_path}")
    if value is None or isinstance(value, (str, int, float, bool, Decimal)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Enum):
        return canonicalize_payload(value.value, _path=_path, _depth=_depth + 1)
    if isinstance(value, pd.DataFrame):
        try:
            records = value.to_dict("records")
        except Exception as exc:
            raise MoomooResponseShapeError(f"Moomoo table at {_path} could not be converted") from exc
        return canonicalize_payload(records, _path=_path, _depth=_depth + 1)
    if isinstance(value, Mapping):
        return {
            str(key): canonicalize_payload(item, _path=f"{_path}.{key}", _depth=_depth + 1)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [
            canonicalize_payload(item, _path=f"{_path}[{index}]", _depth=_depth + 1)
            for index, item in enumerate(value)
        ]
    # pandas/numpy scalar values expose a safe scalar conversion.  We still
    # recurse through the result so custom objects cannot pass through.
    item_method = getattr(value, "item", None)
    if callable(item_method):
        try:
            item = item_method()
        except Exception as exc:
            raise MoomooResponseShapeError(f"Moomoo scalar at {_path} could not be converted") from exc
        if item is not value:
            return canonicalize_payload(item, _path=_path, _depth=_depth + 1)
    table_method = getattr(value, "to_dict", None)
    if callable(table_method):
        try:
            try:
                converted = table_method("records")
            except TypeError:
                converted = table_method()
        except Exception as exc:
            raise MoomooResponseShapeError(f"Moomoo table at {_path} could not be converted") from exc
        return canonicalize_payload(converted, _path=_path, _depth=_depth + 1)
    raise MoomooResponseShapeError(
        f"Moomoo response at {_path} contains opaque type {type(value).__name__!r}"
    )


def as_records(data: Any) -> list[dict[str, Any]]:
    if data is None:
        raise MoomooResponseShapeError("Moomoo response was null; an explicit empty table is required")
    canonical = canonicalize_payload(data)
    if isinstance(canonical, Mapping):
        records: list[Any] = [canonical]
    elif isinstance(canonical, list):
        records = canonical
    else:
        raise MoomooResponseShapeError("Moomoo tabular response did not convert to a record list")
    normalized: list[dict[str, Any]] = []
    for index, item in enumerate(records):
        if not isinstance(item, Mapping):
            raise MoomooResponseShapeError(
                f"Moomoo response row {index} has unsupported type {type(item).__name__!r}"
            )
        normalized.append(dict(item))
    return normalized


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
    "MoomooResponseShapeError",
    "as_records",
    "canonicalize_payload",
    "get_value",
    "normalise_env",
    "parse_market_auth",
    "safe_float",
    "status_name",
]
