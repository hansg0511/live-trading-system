"""Declarative, SIM-only Stage 6 pilot configuration.

The configuration boundary is intentionally strict.  It turns a JSON file
into the existing Stage 5/6 domain objects, persists only the declared
account/strategy/books needed by the generic repository, and supplies the
Moomoo resolver to the existing adapter.  It never creates an order directly.

The checked-in template contains placeholders and must fail validation.  A
real pilot therefore cannot be armed accidentally by copying the template.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
import hashlib
import json
from pathlib import Path
from typing import Any

from src.brokers.moomoo import StaticMoomooInstrumentResolver
from src.trading_core.domain import (
    Account,
    AssetClass,
    Book,
    ExecutionPolicy,
    Instrument,
    InstrumentMapping,
    IntentAction,
    MappingPurpose,
    Strategy,
    TradingEnvironment,
)
from src.trading_core.oms import GenericOMS
from src.trading_core.repository import SQLiteTradingRepository

from .stage5_sleeves import (
    Clean40AllocationUpdate,
    NormalizedPairTarget,
    PairSleeve,
    SleeveAllocationTarget,
)
from .stage6_pilot import Stage6PilotRunner, Stage6PilotSpec


class Stage6ConfigError(ValueError):
    """A configuration is incomplete, contradictory, or unsafe to arm."""


def _mapping(value: object, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise Stage6ConfigError(f"{name} must be an object")
    return value


def _sequence(value: object, name: str, *, length: int | None = None) -> tuple[Mapping[str, Any], ...]:
    if type(value) is not list:
        raise Stage6ConfigError(f"{name} must be an array")
    result: list[Mapping[str, Any]] = []
    for index, item in enumerate(value):
        result.append(_mapping(item, f"{name}[{index}]"))
    if length is not None and len(result) != length:
        raise Stage6ConfigError(f"{name} must contain exactly {length} item(s)")
    return tuple(result)


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise Stage6ConfigError(f"{name} is required")
    result = value.strip()
    if "REPLACE_WITH" in result.upper() or "TODO" in result.upper():
        raise Stage6ConfigError(f"{name} still contains a placeholder")
    return result


def _reject_placeholders(value: object, path: str = "configuration") -> None:
    """Reject unfinished values even when they are nested in strategy metadata."""

    if isinstance(value, str) and ("REPLACE_WITH" in value.upper() or "TODO" in value.upper()):
        raise Stage6ConfigError(f"{path} still contains a placeholder")
    if isinstance(value, Mapping):
        for key, item in value.items():
            _reject_placeholders(item, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _reject_placeholders(item, f"{path}[{index}]")


def _optional_text(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _text(value, name)


def _decimal(value: object, name: str) -> Decimal:
    if isinstance(value, bool):
        raise Stage6ConfigError(f"{name} must be a finite decimal")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise Stage6ConfigError(f"{name} must be a finite decimal") from exc
    if not result.is_finite():
        raise Stage6ConfigError(f"{name} must be finite")
    return result


def _json_numeric(value: object, name: str) -> Decimal:
    """Accept only JSON numeric scalars, never numeric-looking strings."""

    if type(value) not in {int, float}:
        raise Stage6ConfigError(f"{name} must be a JSON number, not a string or object")
    return _decimal(value, name)


def _json_numeric_pair(value: object, name: str) -> tuple[Decimal, Decimal]:
    if type(value) is not list or len(value) != 2:
        raise Stage6ConfigError(f"{name} must be an array of exactly two JSON numbers")
    return (_json_numeric(value[0], f"{name}[0]"), _json_numeric(value[1], f"{name}[1]"))


def _json_string_pair(value: object, name: str) -> tuple[str, str]:
    if type(value) is not list or len(value) != 2 or any(type(item) is not str for item in value):
        raise Stage6ConfigError(f"{name} must be an array of exactly two strings")
    return (_text(value[0], f"{name}[0]"), _text(value[1], f"{name}[1]"))


def _json_positive_int(value: object, name: str) -> int:
    if type(value) is not int or value <= 0:
        raise Stage6ConfigError(f"{name} must be a positive JSON integer")
    return value


def _canonical(value: object) -> object:
    """Convert the executable config into deterministic JSON primitives."""

    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, Path):
        return str(value.resolve(strict=False))
    if isinstance(value, Mapping):
        return {str(key): _canonical(item) for key, item in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (list, tuple, set, frozenset)):
        values = [_canonical(item) for item in value]
        return sorted(values) if isinstance(value, (set, frozenset)) else values
    return value


def _decimal_text(value: object) -> str | None:
    if value is None:
        return None
    try:
        return format(Decimal(str(value)), "f")
    except (InvalidOperation, TypeError, ValueError):
        return str(value)


def _instrument_matches(existing: Mapping[str, Any], desired: Instrument) -> bool:
    expected = {
        "id": desired.id,
        "asset_class": desired.asset_class.value,
        "symbol": desired.symbol,
        "venue": desired.venue,
        "currency": desired.currency,
        "multiplier": _decimal_text(desired.multiplier),
        "tick_size": _decimal_text(desired.tick_size),
        "lot_size": _decimal_text(desired.lot_size),
        "expiry": desired.expiry.isoformat() if desired.expiry else None,
        "strike": _decimal_text(desired.strike),
        "option_right": desired.option_right,
        "metadata": dict(desired.metadata),
    }
    for key, value in expected.items():
        actual = existing.get(key)
        if key in {"multiplier", "tick_size", "lot_size", "strike"}:
            if _decimal_text(actual) != value:
                return False
        elif actual != value:
            return False
    return True


def _mapping_matches(existing: Mapping[str, Any], desired: InstrumentMapping) -> bool:
    return all(
        (
            existing.get("id") == desired.id,
            existing.get("instrument_id") == desired.instrument_id,
            existing.get("provider") == desired.provider,
            existing.get("purpose") == desired.purpose.value,
            existing.get("external_symbol") == desired.external_symbol,
            existing.get("external_id") == desired.external_id,
            existing.get("metadata") == dict(desired.metadata),
        )
    )


def _timestamp(value: object, name: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise Stage6ConfigError(f"{name} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise Stage6ConfigError(f"{name} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise Stage6ConfigError(f"{name} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _bool(value: object, name: str, *, default: bool | None = None) -> bool:
    if value is None and default is not None:
        return default
    if not isinstance(value, bool):
        raise Stage6ConfigError(f"{name} must be true or false")
    return value


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool):
        raise Stage6ConfigError(f"{name} must be a positive integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise Stage6ConfigError(f"{name} must be a positive integer") from exc
    if result <= 0 or str(value).strip() != str(result):
        raise Stage6ConfigError(f"{name} must be a positive integer")
    return result


def _load_json(path: str | Path) -> Mapping[str, Any]:
    path = Path(path)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Stage6ConfigError(f"could not read JSON configuration {path}: {exc}") from exc
    return _mapping(value, "configuration")


@dataclass(frozen=True, slots=True)
class Stage6ExecutionConfig:
    """Validated execution/session boundary for the first RTH pilot."""

    policy: ExecutionPolicy
    rth_handoff_policy: str
    market: str
    security_firm: str

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "Stage6ExecutionConfig":
        policy_raw = dict(_mapping(raw.get("execution_policy"), "execution_policy"))
        handoff = _text(policy_raw.pop("rth_handoff_policy", None), "execution_policy.rth_handoff_policy").upper()
        if handoff != "RTH_ONLY":
            raise Stage6ConfigError(
                "execution_policy.rth_handoff_policy must be RTH_ONLY; no extended/overnight handoff is armed by this CLI"
            )
        market = _text(raw.get("market"), "market").upper()
        security_firm = _text(raw.get("security_firm"), "security_firm")
        try:
            policy = ExecutionPolicy(**policy_raw)
        except (TypeError, ValueError) as exc:
            raise Stage6ConfigError(f"invalid execution_policy: {exc}") from exc
        if policy.execution_session.value != "REGULAR" or policy.allow_extended_hours:
            raise Stage6ConfigError("Stage 6 SIM handoff requires REGULAR execution with allow_extended_hours=false")
        return cls(
            policy=policy,
            rth_handoff_policy=handoff,
            market=market,
            security_firm=security_firm,
        )


@dataclass(frozen=True, slots=True)
class Stage6PilotConfig:
    """Fully materialized declarative inputs for one two-sleeve pilot."""

    state_db: Path
    account: Account
    strategy: Strategy
    books: tuple[Book, Book]
    sleeves: tuple[PairSleeve, PairSleeve]
    instruments: tuple[Instrument, ...]
    mappings: tuple[InstrumentMapping, ...]
    allocation_update: Clean40AllocationUpdate
    targets: tuple[NormalizedPairTarget, NormalizedPairTarget]
    execution: Stage6ExecutionConfig
    require_flat_entry: bool = True
    source_path: Path | None = None

    @classmethod
    def load(cls, path: str | Path) -> "Stage6PilotConfig":
        source_path = Path(path)
        raw = _load_json(source_path)
        _reject_placeholders(raw)
        version = raw.get("schema_version")
        if version != 1:
            raise Stage6ConfigError("schema_version must be 1")
        configured_mode = str(raw.get("mode", "DRY_RUN")).strip().upper()
        if configured_mode not in {"DRY_RUN", "SIM_SUBMIT"}:
            raise Stage6ConfigError("configuration mode must be DRY_RUN or SIM_SUBMIT; REAL/LIVE is unavailable")
        state_db = Path(_text(raw.get("state_db"), "state_db"))

        account_raw = _mapping(raw.get("account"), "account")
        try:
            account_environment = TradingEnvironment(account_raw.get("environment"))
        except (TypeError, ValueError) as exc:
            if str(account_raw.get("environment", "")).strip().upper() in {"REAL", "LIVE"}:
                raise Stage6ConfigError(
                    "Stage 6 configuration accepts SIM accounts only; REAL/LIVE is unavailable"
                ) from exc
            raise Stage6ConfigError("account.environment must be SIM") from exc
        try:
            account = Account(
                id=_text(account_raw.get("id"), "account.id"),
                broker=_text(account_raw.get("broker"), "account.broker"),
                environment=account_environment,
                external_account_id=_text(account_raw.get("external_account_id"), "account.external_account_id"),
                base_currency=_text(account_raw.get("base_currency"), "account.base_currency"),
                enabled=_bool(account_raw.get("enabled"), "account.enabled"),
                metadata=_mapping(account_raw.get("metadata", {}), "account.metadata"),
                created_at=_timestamp(account_raw.get("created_at"), "account.created_at"),
                updated_at=_timestamp(account_raw.get("updated_at"), "account.updated_at"),
            )
        except (TypeError, ValueError) as exc:
            raise Stage6ConfigError(f"invalid account: {exc}") from exc
        if account.environment is not TradingEnvironment.SIM:
            raise Stage6ConfigError("Stage 6 configuration accepts SIM accounts only; REAL/LIVE is unavailable")
        try:
            external_account_number = int(account.external_account_id)
        except (TypeError, ValueError) as exc:
            raise Stage6ConfigError("account.external_account_id must be a positive SIM account number") from exc
        if external_account_number <= 0 or str(external_account_number) != account.external_account_id:
            raise Stage6ConfigError("account.external_account_id must be a positive SIM account number")
        if account.broker.lower() != "moomoo":
            raise Stage6ConfigError("Stage 6 configuration requires broker=moomoo")

        strategy_raw = _mapping(raw.get("strategy"), "strategy")
        try:
            strategy = Strategy(
                id=_text(strategy_raw.get("id"), "strategy.id"),
                name=_text(strategy_raw.get("name"), "strategy.name"),
                strategy_type=_text(strategy_raw.get("strategy_type"), "strategy.strategy_type"),
                version=_text(strategy_raw.get("version", "1"), "strategy.version"),
                enabled=_bool(strategy_raw.get("enabled"), "strategy.enabled"),
                config=_mapping(strategy_raw.get("config", {}), "strategy.config"),
                metadata=_mapping(strategy_raw.get("metadata", {}), "strategy.metadata"),
                created_at=_timestamp(strategy_raw.get("created_at"), "strategy.created_at"),
                updated_at=_timestamp(strategy_raw.get("updated_at"), "strategy.updated_at"),
            )
        except (TypeError, ValueError) as exc:
            raise Stage6ConfigError(f"invalid strategy: {exc}") from exc

        books_raw = _sequence(raw.get("books"), "books", length=2)
        try:
            books = tuple(
                Book(
                    id=_text(item.get("id"), f"books[{index}].id"),
                    name=_text(item.get("name"), f"books[{index}].name"),
                    enabled=_bool(item.get("enabled"), f"books[{index}].enabled"),
                    metadata=_mapping(item.get("metadata", {}), f"books[{index}].metadata"),
                    created_at=_timestamp(item.get("created_at"), f"books[{index}].created_at"),
                    updated_at=_timestamp(item.get("updated_at"), f"books[{index}].updated_at"),
                )
                for index, item in enumerate(books_raw)
            )
        except (TypeError, ValueError) as exc:
            raise Stage6ConfigError(f"invalid books: {exc}") from exc
        if len({book.id for book in books}) != 2:
            raise Stage6ConfigError("books must have distinct IDs")

        execution = Stage6ExecutionConfig.from_mapping(raw)
        sleeves_raw = _sequence(raw.get("sleeves"), "sleeves", length=2)
        instruments: list[Instrument] = []
        mappings: list[InstrumentMapping] = []
        sleeves: list[PairSleeve] = []
        seen_instruments: set[str] = set()
        seen_symbols: set[str] = set()
        for sleeve_index, item in enumerate(sleeves_raw):
            pair_raw = _sequence(item.get("pair"), f"sleeves[{sleeve_index}].pair", length=2)
            pair_ids: list[str] = []
            pair_symbols: list[str] = []
            configuration = _mapping(item.get("configuration", {}), f"sleeves[{sleeve_index}].configuration")
            for leg_index, leg in enumerate(pair_raw):
                instrument_id = _text(leg.get("instrument_id"), f"sleeves[{sleeve_index}].pair[{leg_index}].instrument_id")
                internal_symbol = _text(leg.get("symbol"), f"sleeves[{sleeve_index}].pair[{leg_index}].symbol")
                broker_symbol = _text(
                    leg.get("moomoo_symbol"),
                    f"sleeves[{sleeve_index}].pair[{leg_index}].moomoo_symbol",
                ).upper()
                if "." not in broker_symbol or broker_symbol.split(".", 1)[0] != execution.market:
                    raise Stage6ConfigError(
                        f"sleeves[{sleeve_index}].pair[{leg_index}].moomoo_symbol must use {execution.market}.CODE format"
                    )
                if instrument_id in seen_instruments:
                    raise Stage6ConfigError(f"instrument ID is reused across sleeves: {instrument_id}")
                if broker_symbol in seen_symbols:
                    raise Stage6ConfigError(f"Moomoo symbol is reused across mappings: {broker_symbol}")
                seen_instruments.add(instrument_id)
                seen_symbols.add(broker_symbol)
                pair_ids.append(instrument_id)
                pair_symbols.append(internal_symbol)
                try:
                    instruments.append(
                        Instrument(
                            id=instrument_id,
                            asset_class=leg.get("asset_class", AssetClass.EQUITY.value),
                            symbol=internal_symbol,
                            venue=_text(leg.get("venue", execution.market), f"sleeves[{sleeve_index}].pair[{leg_index}].venue"),
                            currency=_text(leg.get("currency", account.base_currency), f"sleeves[{sleeve_index}].pair[{leg_index}].currency"),
                            metadata=_mapping(leg.get("metadata", {}), f"sleeves[{sleeve_index}].pair[{leg_index}].metadata"),
                            created_at=account.created_at,
                            updated_at=account.updated_at,
                        )
                    )
                    mappings.append(
                        InstrumentMapping(
                            id=f"stage6-moomoo-{instrument_id}",
                            instrument_id=instrument_id,
                            provider="moomoo",
                            purpose=MappingPurpose.BROKER,
                            external_symbol=broker_symbol,
                            external_id=leg.get("external_id"),
                            metadata={
                                "source": "stage6-config",
                                "market": execution.market,
                                "security_firm": execution.security_firm,
                            },
                            created_at=account.created_at,
                            updated_at=account.updated_at,
                        )
                    )
                except (TypeError, ValueError) as exc:
                    raise Stage6ConfigError(f"invalid instrument mapping in sleeve {sleeve_index}: {exc}") from exc
            sleeve_id = _text(item.get("sleeve_id"), f"sleeves[{sleeve_index}].sleeve_id")
            strategy_id = _text(item.get("strategy_id"), f"sleeves[{sleeve_index}].strategy_id")
            account_id = _text(item.get("account_id"), f"sleeves[{sleeve_index}].account_id")
            book_id = _text(item.get("book_id"), f"sleeves[{sleeve_index}].book_id")
            if strategy_id != strategy.id or account_id != account.id:
                raise Stage6ConfigError(f"sleeve {sleeve_id} is not bound to the configured strategy/account")
            if book_id not in {book.id for book in books}:
                raise Stage6ConfigError(f"sleeve {sleeve_id} is not bound to one of the two configured books")
            try:
                sleeves.append(
                    PairSleeve(
                        sleeve_id=sleeve_id,
                        strategy_id=strategy_id,
                        account_id=account_id,
                        book_id=book_id,
                        name=_text(item.get("name"), f"sleeves[{sleeve_index}].name"),
                        instrument_ids=tuple(pair_ids),
                        symbols=tuple(pair_symbols),
                        configuration=configuration,
                        version=_text(item.get("version", "1"), f"sleeves[{sleeve_index}].version"),
                        enabled=_bool(item.get("enabled"), f"sleeves[{sleeve_index}].enabled"),
                    )
                )
            except (TypeError, ValueError) as exc:
                raise Stage6ConfigError(f"invalid sleeve {sleeve_index}: {exc}") from exc
        sleeves_tuple = tuple(sleeves)
        if len({item.sleeve_id for item in sleeves_tuple}) != 2 or len({item.book_id for item in sleeves_tuple}) != 2:
            raise Stage6ConfigError("sleeves must have distinct sleeve and book identities")

        allocation_raw = _mapping(raw.get("allocation_update"), "allocation_update")
        allocation_targets: list[SleeveAllocationTarget] = []
        for index, item in enumerate(_sequence(allocation_raw.get("targets"), "allocation_update.targets", length=2)):
            try:
                target_weight = (
                    _json_numeric(item["target_weight"], f"allocation_update.targets[{index}].target_weight")
                    if "target_weight" in item
                    else None
                )
                capacity = (
                    _json_numeric(item["capacity"], f"allocation_update.targets[{index}].capacity")
                    if "capacity" in item
                    else None
                )
                allocation_targets.append(
                    SleeveAllocationTarget(
                        sleeve_id=_text(item.get("sleeve_id"), f"allocation_update.targets[{index}].sleeve_id"),
                        book_id=_text(item.get("book_id"), f"allocation_update.targets[{index}].book_id"),
                        target_weight=target_weight,
                        capacity=capacity,
                        capacity_unit=str(item.get("capacity_unit", "FRACTION")),
                    )
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise Stage6ConfigError(f"invalid allocation target {index}: {exc}") from exc
        try:
            allocation = Clean40AllocationUpdate(
                account_id=_text(allocation_raw.get("account_id"), "allocation_update.account_id"),
                version=_json_positive_int(allocation_raw.get("version"), "allocation_update.version"),
                targets=tuple(allocation_targets),
                effective_at=_timestamp(allocation_raw.get("effective_at"), "allocation_update.effective_at"),
                provenance=_text(allocation_raw.get("provenance"), "allocation_update.provenance"),
            )
        except (TypeError, ValueError) as exc:
            raise Stage6ConfigError(f"invalid allocation_update: {exc}") from exc
        if allocation.account_id != account.id:
            raise Stage6ConfigError("allocation_update.account_id does not match account.id")

        targets_raw = _sequence(raw.get("targets"), "targets", length=2)
        targets: list[NormalizedPairTarget] = []
        for index, item in enumerate(targets_raw):
            try:
                quantities = _json_numeric_pair(
                    item.get("signed_quantities"),
                    f"targets[{index}].signed_quantities",
                )
                target_instruments = _json_string_pair(
                    item.get("instrument_ids"),
                    f"targets[{index}].instrument_ids",
                )
                targets.append(
                    NormalizedPairTarget(
                        sleeve_id=_text(item.get("sleeve_id"), f"targets[{index}].sleeve_id"),
                        cycle_id=_text(item.get("cycle_id"), f"targets[{index}].cycle_id"),
                        signal_id=_text(item.get("signal_id"), f"targets[{index}].signal_id"),
                        instrument_ids=target_instruments,
                        signed_quantities=quantities,
                        action=IntentAction(item.get("action")),
                        evaluated_at=_timestamp(item.get("evaluated_at"), f"targets[{index}].evaluated_at"),
                        provenance=_mapping(item.get("provenance", {}), f"targets[{index}].provenance"),
                    )
                )
            except (TypeError, ValueError) as exc:
                raise Stage6ConfigError(f"invalid target {index}: {exc}") from exc
        require_flat_entry = _bool(raw.get("require_flat_entry"), "require_flat_entry", default=True)
        try:
            execution = Stage6ExecutionConfig.from_mapping(raw)
            result = cls(
                state_db=state_db,
                account=account,
                strategy=strategy,
                books=(books[0], books[1]),
                sleeves=(sleeves_tuple[0], sleeves_tuple[1]),
                instruments=tuple(instruments),
                mappings=tuple(mappings),
                allocation_update=allocation,
                targets=(targets[0], targets[1]),
                execution=execution,
                require_flat_entry=require_flat_entry,
                source_path=source_path,
            )
            result.spec()
            return result
        except (IndexError, TypeError, ValueError) as exc:
            raise Stage6ConfigError(f"invalid Stage 6 pilot configuration: {exc}") from exc

    def spec(self) -> Stage6PilotSpec:
        return Stage6PilotSpec(
            account=self.account,
            sleeves=self.sleeves,
            allocation_update=self.allocation_update,
            targets=self.targets,
            execution_policy=self.execution.policy,
            require_flat_entry=self.require_flat_entry,
            run_id=self.canonical_run_id(),
        )

    def canonical_executable_config(self) -> dict[str, Any]:
        """Return every input that can alter routing, persistence, or orders."""

        policy = self.execution.policy
        return {
            "state_db": str(self.state_db.resolve(strict=False)),
            "account": {
                "id": self.account.id,
                "external_account_id": self.account.external_account_id,
                "broker": self.account.broker,
                "environment": self.account.environment,
                "base_currency": self.account.base_currency,
                "enabled": self.account.enabled,
                "metadata": self.account.metadata,
            },
            "market": self.execution.market,
            "security_firm": self.execution.security_firm,
            "strategy": {
                "id": self.strategy.id,
                "name": self.strategy.name,
                "strategy_type": self.strategy.strategy_type,
                "version": self.strategy.version,
                "enabled": self.strategy.enabled,
                "config": self.strategy.config,
                "metadata": self.strategy.metadata,
            },
            "books": [
                {
                    "id": book.id,
                    "name": book.name,
                    "enabled": book.enabled,
                    "metadata": book.metadata,
                }
                for book in sorted(self.books, key=lambda item: item.id)
            ],
            "sleeves": [
                {
                    "sleeve_id": sleeve.sleeve_id,
                    "strategy_id": sleeve.strategy_id,
                    "account_id": sleeve.account_id,
                    "book_id": sleeve.book_id,
                    "name": sleeve.name,
                    "version": sleeve.version,
                    "enabled": sleeve.enabled,
                    "instrument_ids": sleeve.instrument_ids,
                    "symbols": sleeve.symbols,
                    "configuration": sleeve.configuration,
                }
                for sleeve in sorted(self.sleeves, key=lambda item: item.sleeve_id)
            ],
            "instruments": [
                {
                    "id": instrument.id,
                    "asset_class": instrument.asset_class,
                    "symbol": instrument.symbol,
                    "venue": instrument.venue,
                    "currency": instrument.currency,
                    "multiplier": instrument.multiplier,
                    "tick_size": instrument.tick_size,
                    "lot_size": instrument.lot_size,
                    "expiry": instrument.expiry,
                    "strike": instrument.strike,
                    "option_right": instrument.option_right,
                    "metadata": instrument.metadata,
                }
                for instrument in sorted(self.instruments, key=lambda item: item.id)
            ],
            "mappings": [
                {
                    "id": mapping.id,
                    "instrument_id": mapping.instrument_id,
                    "provider": mapping.provider,
                    "purpose": mapping.purpose,
                    "external_symbol": mapping.external_symbol,
                    "external_id": mapping.external_id,
                    "metadata": mapping.metadata,
                }
                for mapping in sorted(self.mappings, key=lambda item: item.id)
            ],
            "allocation_update": {
                "account_id": self.allocation_update.account_id,
                "version": self.allocation_update.version,
                "effective_at": self.allocation_update.effective_at,
                "provenance": self.allocation_update.provenance,
                "targets": [
                    {
                        "sleeve_id": target.sleeve_id,
                        "book_id": target.book_id,
                        "target_weight": target.target_weight,
                        "capacity": target.capacity,
                        "capacity_unit": target.capacity_unit,
                    }
                    for target in sorted(self.allocation_update.targets, key=lambda item: item.sleeve_id)
                ],
            },
            "targets": [
                {
                    "sleeve_id": target.sleeve_id,
                    "cycle_id": target.cycle_id,
                    "signal_id": target.signal_id,
                    "instrument_ids": target.instrument_ids,
                    "signed_quantities": target.signed_quantities,
                    "action": target.action,
                    "evaluated_at": target.evaluated_at,
                    "provenance": target.provenance,
                }
                for target in sorted(self.targets, key=lambda item: item.sleeve_id)
            ],
            "execution_policy": {
                "legging_policy": policy.legging_policy,
                "partial_fill_policy": policy.partial_fill_policy,
                "failure_policy": policy.failure_policy,
                "max_attempts": policy.max_attempts,
                "timeout_seconds": policy.timeout_seconds,
                "require_native_atomicity": policy.require_native_atomicity,
                "allow_extended_hours": policy.allow_extended_hours,
                "required_capabilities": sorted(policy.required_capabilities),
                "metadata": policy.metadata,
                "execution_session": policy.execution_session,
                "stale_order_seconds": policy.stale_order_seconds,
                "rth_handoff_policy": self.execution.rth_handoff_policy,
            },
            "require_flat_entry": self.require_flat_entry,
        }

    def canonical_run_id(self) -> str:
        encoded = json.dumps(
            _canonical(self.canonical_executable_config()),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return "stage6-run-" + hashlib.sha256(encoded).hexdigest()[:24]

    def resolver(self) -> StaticMoomooInstrumentResolver:
        return StaticMoomooInstrumentResolver(
            {item.instrument_id: item.external_symbol for item in self.mappings}
        )

    def confirmation_phrase(self) -> str:
        return f"ARM STAGE6 SIM {self.spec().run_id}"

    def ensure_repository(self, repository: SQLiteTradingRepository) -> None:
        """Persist and compare every declaration used by the executable run."""

        repository.initialize()
        existing = self.validate_repository(repository)
        existing_account = existing["account"]
        existing_strategy = existing["strategy"]
        existing_books = existing["books"]
        existing_instruments = existing["instruments"]
        existing_mappings = existing["mappings"]

        if existing_account is None:
            repository.save_account(self.account)
        if existing_strategy is None:
            repository.save_strategy(self.strategy)
        for book in self.books:
            if existing_books[book.id] is None:
                repository.save_book(book)
        for instrument in self.instruments:
            if existing_instruments[instrument.id] is None:
                repository.save_instrument(instrument)
        for mapping in self.mappings:
            if existing_mappings[mapping.id] is None:
                repository.save_instrument_mapping(mapping)

    def validate_repository(self, repository: SQLiteTradingRepository) -> dict[str, Any]:
        """Compare declarations with an existing repository without writes."""

        configured_db = self.state_db.resolve(strict=False)
        repository_db = repository.db_path.resolve(strict=False)
        if configured_db != repository_db:
            raise Stage6ConfigError(
                f"repository DB path {repository_db} does not match configured state_db {configured_db}"
            )
        existing_account = repository.get_account(self.account.id)
        if existing_account is not None and existing_account != self.account:
            raise Stage6ConfigError("persisted account identity does not match the config")

        existing_strategy = repository.get_strategy(self.strategy.id)
        if existing_strategy is not None and existing_strategy != self.strategy:
            raise Stage6ConfigError("persisted strategy identity does not match the config")

        existing_books: dict[str, dict[str, Any] | None] = {}
        for book in self.books:
            existing = repository.get_book(book.id)
            existing_books[book.id] = existing
            if existing is not None and (
                str(existing.get("id")) != book.id
                or str(existing.get("name")) != book.name
                or bool(existing.get("enabled")) != book.enabled
                or existing.get("metadata") != dict(book.metadata)
            ):
                raise Stage6ConfigError(f"persisted book identity does not match the config: {book.id}")

        existing_instruments: dict[str, dict[str, Any] | None] = {}
        for instrument in self.instruments:
            existing = repository.get_instrument(instrument.id)
            existing_instruments[instrument.id] = existing
            if existing is not None and not _instrument_matches(existing, instrument):
                raise Stage6ConfigError(f"persisted instrument identity does not match the config: {instrument.id}")

        existing_mappings: dict[str, dict[str, Any] | None] = {}
        for mapping in self.mappings:
            existing = repository.get_instrument_mapping(mapping.id)
            if existing is None:
                existing = repository.find_instrument_mapping(
                    instrument_id=mapping.instrument_id,
                    provider=mapping.provider,
                    purpose=mapping.purpose.value,
                )
            existing_mappings[mapping.id] = existing
            if existing is not None and not _mapping_matches(existing, mapping):
                raise Stage6ConfigError(
                    f"persisted Moomoo instrument mapping does not match the config: {mapping.instrument_id}"
                )
        return {
            "account": existing_account,
            "strategy": existing_strategy,
            "books": existing_books,
            "instruments": existing_instruments,
            "mappings": existing_mappings,
        }

    def build_runner(
        self,
        repository: SQLiteTradingRepository,
        *,
        adapter: Any | None = None,
        clock: Any | None = None,
    ) -> Stage6PilotRunner:
        """Build the existing generic OMS runner; no direct order path exists."""

        if adapter is None:
            adapter = _DryRunAdapter()
        return Stage6PilotRunner(
            repository,
            GenericOMS(repository, adapter, clock=clock),
            clock=clock,
            dispatch_wait_seconds=self.execution.policy.timeout_seconds,
        )

    def build_moomoo_adapter(self) -> Any:
        """Create the SIM-only adapter; caller must explicitly connect it."""

        from src.brokers.moomoo import MooMooGenericAdapter

        return MooMooGenericAdapter(
            instrument_resolver=self.resolver(),
            market=self.execution.market,
            external_account_id=self.account.external_account_id,
            environment=self.account.environment,
            security_firm=self.execution.security_firm,
        )


class _DryRunAdapter:
    """Sentinel adapter proving a dry-run cannot contact a broker."""

    def __getattr__(self, name: str) -> Any:
        raise RuntimeError(f"dry-run attempted broker adapter call: {name}")


def load_stage6_config(path: str | Path) -> Stage6PilotConfig:
    return Stage6PilotConfig.load(path)


__all__ = [
    "Stage6ConfigError",
    "Stage6ExecutionConfig",
    "Stage6PilotConfig",
    "load_stage6_config",
]
