"""Fail-closed normalization for provider-supplied diagnostic payloads.

The generic core accepts mappings and primitive values from broker adapters,
but it must not treat a serialized object or an opaque SDK value as harmless
diagnostic text.  This module keeps the boundary rule shared by OMS,
repository, and adapter-facing validation code.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
import json
import re
from typing import Any


class ProviderPayloadError(ValueError):
    """A provider value could not be safely reduced to inspectable data."""


EXECUTION_EVIDENCE_KEYS = frozenset(
    {
        # Provider deal/fill identities exposed by different SDK versions.
        "trade_id",
        "tradeid",
        "execution_id",
        "executionid",
        "exec_id",
        "execid",
        "deal_id",
        "dealid",
        "fill_id",
        "fillid",
        "external_fill_id",
        "externalfillid",
        # Some response shapes expose a stable reference rather than an ID.
        "trade_ref",
        "traderef",
        "trade_reference",
        "tradereference",
        "execution_ref",
        "executionref",
        "execution_reference",
        "executionreference",
        "exec_ref",
        "execref",
        "deal_ref",
        "dealref",
        "deal_reference",
        "dealreference",
    }
)


def normalize_provider_key(value: object) -> str:
    """Normalize provider aliases such as ``RAW`` and ``authoritative-snapshot``."""
    # Provider SDKs mix snake/kebab/case styles and frequently use initialism
    # suffixes (``accountId``, ``brokerOrderID``).  Lower-casing first loses
    # the word boundaries, which makes those aliases invisible to the
    # fail-closed validators that consume this helper.  Split acronym-to-word
    # and lower-to-upper transitions before normalizing punctuation.
    text = str(value).strip()
    text = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", text)
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", text)
    text = re.sub(r"[^A-Za-z0-9]+", "_", text).lower()
    return re.sub(r"_+", "_", text).strip("_")


def is_execution_evidence_key(value: object) -> bool:
    """Return whether a provider key names a material execution identity.

    Callers must still check that the corresponding value is populated.  The
    vocabulary is centralized here so generic OMS and broker adapters cannot
    disagree about whether a nonempty ``tradeId``/``executionId`` is evidence
    against a clean zero-fill or no-submit assertion.
    """
    return normalize_provider_key(value) in EXECUTION_EVIDENCE_KEYS


def coerce_provider_payload(value: Any, *, path: str = "provider", depth: int = 0) -> Any:
    """Return recursively inspectable provider data or raise fail-closed.

    Strings remain ordinary scalar diagnostics unless they visibly begin an
    object/array serialization.  Such strings are parsed as JSON and walked
    recursively; malformed object-looking strings are ambiguity, never proof
    that a response carried no order, fill, account, or instrument facts.
    """
    if depth > 50:
        raise ProviderPayloadError(f"provider payload nesting exceeds the safe limit at {path}")
    if value is None or isinstance(value, (str, int, float, bool, Decimal)):
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.startswith(("{", "[")):
                try:
                    parsed = json.loads(stripped)
                except (json.JSONDecodeError, TypeError, ValueError) as exc:
                    raise ProviderPayloadError(
                        f"serialized provider object at {path} is malformed"
                    ) from exc
                if not isinstance(parsed, (Mapping, list)):
                    raise ProviderPayloadError(
                        f"serialized provider object at {path} is not an object or array"
                    )
                return coerce_provider_payload(parsed, path=path, depth=depth + 1)
        return value
    if isinstance(value, (datetime, date)):
        return value
    if isinstance(value, Enum):
        return coerce_provider_payload(value.value, path=path, depth=depth + 1)
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            result[str(key)] = coerce_provider_payload(
                item,
                path=f"{path}.{key}",
                depth=depth + 1,
            )
        return result
    if isinstance(value, (list, tuple)):
        return [
            coerce_provider_payload(item, path=f"{path}[{index}]", depth=depth + 1)
            for index, item in enumerate(value)
        ]
    raise ProviderPayloadError(
        f"provider payload at {path} contains opaque type {type(value).__name__!r}"
    )


__all__ = [
    "EXECUTION_EVIDENCE_KEYS",
    "ProviderPayloadError",
    "coerce_provider_payload",
    "is_execution_evidence_key",
    "normalize_provider_key",
]
