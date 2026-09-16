"""Supervised generic-OMS Moomoo SIM smoke harness.

This is deliberately separate from ``sim_smoke_test.py``.  It proves the
broker-neutral OMS path with an isolated generic-core database; it never uses
the legacy pair engine or its tables.

Read-only preflight/status commands may be run freely. ``enter`` and ``exit``
require ``--submit`` and always use Moomoo SIM.  They never cancel, close, or
attribute positions/orders that this harness did not create.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import sys
import time
from typing import Any, Callable, Mapping


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.brokers.moomoo.generic_adapter import MooMooGenericAdapter, StaticMoomooInstrumentResolver
from src.trading_core.domain import (
    Account,
    AssetClass,
    ExecutionPolicy,
    ExecutionSession,
    Instrument,
    InstrumentMapping,
    IntentAction,
    IntentStatus,
    LegStatus,
    MappingPurpose,
    OrderIntent,
    OrderLeg,
    QuantityUnit,
    RiskDecisionRecord,
    Side,
    Strategy,
    TradingEnvironment,
)
from src.trading_core.oms import GenericOMS
from src.trading_core.repository import SQLiteTradingRepository


SMOKE_STRATEGY_ID = "generic-sim-smoke"
MAX_GROSS_CAP = Decimal("1000")


class GenericSmokeError(RuntimeError):
    pass


class GenericSmokeBlocked(GenericSmokeError):
    pass


def _symbol(value: str) -> str:
    result = str(value).strip().upper()
    if not result.startswith("US.") or len(result) <= 3:
        raise ValueError("symbols must be US-qualified, for example US.AAPL")
    return result


def _quantity(value: Any, field: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} must be a positive whole number") from exc
    if not result.is_finite() or result <= 0 or result != result.to_integral_value():
        raise ValueError(f"{field} must be a positive whole number")
    return result


def _price(value: Any, field: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} must be a positive price") from exc
    if not result.is_finite() or result <= 0:
        raise ValueError(f"{field} must be a positive price")
    return result


def _execution_session(args: argparse.Namespace) -> ExecutionSession:
    """Resolve the explicit session while supporting the legacy toggle."""

    legacy_extended = bool(getattr(args, "allow_extended_hours", False))
    requested = getattr(args, "execution_session", None)
    if requested is None:
        requested = getattr(args, "session", None)
    if requested is None:
        return ExecutionSession.EXTENDED if legacy_extended else ExecutionSession.REGULAR
    try:
        session = ExecutionSession(requested)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid execution session: {requested!r}") from exc
    if legacy_extended:
        if session is ExecutionSession.OVERNIGHT:
            raise GenericSmokeBlocked(
                "--allow-extended-hours is a legacy alias for --session EXTENDED and cannot be combined with OVERNIGHT"
            )
        session = ExecutionSession.EXTENDED
    return session


def _instrument_id(symbol: str) -> str:
    return f"generic-sim-smoke:{symbol.lower().replace('.', '-') }"


def _account_id(external_account_id: str | int) -> str:
    return f"moomoo:sim:{external_account_id}"


def _identity_decimal(value: Decimal | None) -> str | None:
    """Canonicalize numeric fields before hashing a smoke intent identity."""

    return None if value is None else format(Decimal(value), "f")


def _intent_identity_payload(
    kind: str,
    symbols: tuple[str, str],
    quantities: tuple[Decimal, Decimal],
    sides: tuple[Side, Side],
    *,
    prices: tuple[Decimal | None, Decimal | None],
    execution_session: ExecutionSession | str | None,
    allow_extended_hours: bool = False,
) -> dict[str, Any]:
    """Return the stable, material payload used for smoke intent identity.

    The isolated database may outlive an individual supervised smoke attempt.
    Keep the identity deterministic, but include every request field that can
    change the broker operation so a later materially different request does
    not collide with an older intent.
    """

    session = execution_session or ExecutionSession.REGULAR
    if not isinstance(session, ExecutionSession):
        session = ExecutionSession(session)
    if allow_extended_hours:
        if session is ExecutionSession.REGULAR:
            session = ExecutionSession.EXTENDED
        elif session is not ExecutionSession.EXTENDED:
            raise ValueError("allow_extended_hours cannot be combined with a non-extended execution_session")
    effective_extended = session is ExecutionSession.EXTENDED
    order_type = "LIMIT" if all(price is not None for price in prices) else "MARKET"
    return {
        "identity_version": 2,
        "strategy_id": SMOKE_STRATEGY_ID,
        "kind": kind,
        "action": "ENTER" if kind == "entry" else "EXIT",
        "symbols": list(symbols),
        "legs": [
            {
                "sequence": index,
                "symbol": symbols[index],
                "side": sides[index].value,
                "quantity": _identity_decimal(quantities[index]),
                "quantity_unit": "UNITS",
                "order_type": order_type,
                "limit_price": _identity_decimal(prices[index]),
            }
            for index in range(len(symbols))
        ],
        "execution_policy": {
            "allow_extended_hours": effective_extended,
            "execution_session": session.value,
            "failure_policy": "HOLD_AND_RECONCILE",
            "legging_policy": "SEQUENTIAL",
            "partial_fill_policy": "WAIT",
            "require_native_atomicity": False,
            "required_capabilities": [],
        },
    }


def _intent_identity_digest(
    kind: str,
    symbols: tuple[str, str],
    quantities: tuple[Decimal, Decimal],
    sides: tuple[Side, Side],
    *,
    prices: tuple[Decimal | None, Decimal | None],
    execution_session: ExecutionSession | str | None,
    allow_extended_hours: bool = False,
) -> str:
    payload = _intent_identity_payload(
        kind,
        symbols,
        quantities,
        sides,
        prices=prices,
        execution_session=execution_session,
        allow_extended_hours=allow_extended_hours,
    )
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _intent_key(
    kind: str,
    symbols: tuple[str, str],
    quantities: tuple[Decimal, Decimal],
    sides: tuple[Side, Side],
    *,
    prices: tuple[Decimal | None, Decimal | None],
    execution_session: ExecutionSession | str | None,
    allow_extended_hours: bool = False,
) -> str:
    digest = _intent_identity_digest(
        kind,
        symbols,
        quantities,
        sides,
        prices=prices,
        execution_session=execution_session,
        allow_extended_hours=allow_extended_hours,
    )
    return f"{SMOKE_STRATEGY_ID}|v2|{kind}|{symbols[0]}|{symbols[1]}|{digest}"


def _intent_id(
    kind: str,
    symbols: tuple[str, str],
    quantities: tuple[Decimal, Decimal],
    sides: tuple[Side, Side],
    *,
    prices: tuple[Decimal | None, Decimal | None],
    execution_session: ExecutionSession | str | None,
    allow_extended_hours: bool = False,
) -> str:
    digest = _intent_identity_digest(
        kind,
        symbols,
        quantities,
        sides,
        prices=prices,
        execution_session=execution_session,
        allow_extended_hours=allow_extended_hours,
    )
    return f"{SMOKE_STRATEGY_ID}-{kind}-{symbols[0][3:].lower()}-{symbols[1][3:].lower()}-{digest[:16]}"


def _intent_key_prefix(kind: str, symbols: tuple[str, str]) -> str:
    return f"{SMOKE_STRATEGY_ID}|v2|{kind}|{symbols[0]}|{symbols[1]}|"


def _find_entry_intent(
    repository: SQLiteTradingRepository,
    account: Account,
    symbols: tuple[str, str],
    quantities: tuple[Decimal, Decimal],
    sides: tuple[Side, Side],
    execution_session: ExecutionSession,
) -> dict[str, Any] | None:
    """Find the latest matching entry while allowing exit prices to differ.

    Entry and exit prices are intentionally different in extended/overnight
    smoke workflows.  The durable identity includes those prices, so an exit
    cannot reconstruct the entry key from its own fresh prices.  Match the
    cycle-defining fields and choose the newest v2 entry; the exact exit key
    still retains full payload idempotency.
    """

    prefix = _intent_key_prefix("entry", symbols) + "%"
    with repository.transaction() as conn:
        rows = conn.execute(
            """SELECT id FROM core_order_intents
               WHERE account_id = ? AND action = 'ENTER' AND idempotency_key LIKE ?
               ORDER BY created_at DESC""",
            (account.id, prefix),
        ).fetchall()

    expected_extended = execution_session is ExecutionSession.EXTENDED
    for row in rows:
        intent = repository.get_intent(str(row["id"]))
        if intent is None:
            continue
        policy = intent.get("execution_policy") or {}
        if str(policy.get("execution_session", "")).upper() != execution_session.value:
            continue
        if bool(policy.get("allow_extended_hours", False)) != expected_extended:
            continue
        legs = intent.get("legs") or []
        if len(legs) != len(symbols):
            continue
        matches = True
        for index, leg in enumerate(legs):
            try:
                quantity = Decimal(str(leg.get("quantity")))
            except (InvalidOperation, ValueError):
                matches = False
                break
            if (
                str(leg.get("instrument_id")) != _instrument_id(symbols[index])
                or str(leg.get("side", "")).upper() != sides[index].value
                or quantity != quantities[index]
            ):
                matches = False
                break
        if matches:
            return intent
    return None


def _as_json(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _as_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_as_json(item) for item in value]
    if hasattr(value, "value"):
        return value.value
    return value


def _build_account(external_account_id: str | int) -> Account:
    now = datetime.now(timezone.utc)
    return Account(
        id=_account_id(external_account_id),
        broker="moomoo",
        environment=TradingEnvironment.SIM,
        external_account_id=str(external_account_id),
        base_currency="USD",
        metadata={"purpose": "generic supervised SIM smoke"},
        created_at=now,
        updated_at=now,
    )


def _ensure_setup(repository: SQLiteTradingRepository, account: Account, symbols: tuple[str, str]) -> None:
    """Create fixed smoke configuration once; never overwrite an existing DB."""
    with repository.transaction() as conn:
        existing = conn.execute("SELECT external_account_id FROM core_accounts WHERE id = ?", (account.id,)).fetchone()
    if existing is not None:
        if str(existing["external_account_id"]) != account.external_account_id:
            raise GenericSmokeBlocked("isolated smoke database belongs to a different broker account")
        return

    now = datetime.now(timezone.utc)
    repository.save_account(account)
    repository.save_strategy(
        Strategy(
            id=SMOKE_STRATEGY_ID,
            name="Generic supervised SIM smoke",
            strategy_type="infrastructure_smoke",
            config={},
            created_at=now,
            updated_at=now,
        )
    )
    for symbol in symbols:
        instrument_id = _instrument_id(symbol)
        repository.save_instrument(
            Instrument(
                id=instrument_id,
                asset_class=AssetClass.EQUITY,
                symbol=symbol[3:],
                venue="US",
                currency="USD",
                created_at=now,
                updated_at=now,
            )
        )
        repository.save_instrument_mapping(
            InstrumentMapping(
                id=f"{instrument_id}:moomoo",
                instrument_id=instrument_id,
                provider="moomoo",
                purpose=MappingPurpose.BROKER,
                external_symbol=symbol,
                created_at=now,
                updated_at=now,
            )
        )


def _intent(
    kind: str,
    account: Account,
    symbols: tuple[str, str],
    quantities: tuple[Decimal, Decimal],
    sides: tuple[Side, Side],
    *,
    prices: tuple[Decimal | None, Decimal | None],
    allow_extended_hours: bool = False,
    execution_session: ExecutionSession | str | None = None,
) -> OrderIntent:
    now = datetime.now(timezone.utc)
    resolved_session = execution_session or ExecutionSession.REGULAR
    intent_id = _intent_id(
        kind,
        symbols,
        quantities,
        sides,
        prices=prices,
        execution_session=resolved_session,
        allow_extended_hours=allow_extended_hours,
    )
    idempotency_key = _intent_key(
        kind,
        symbols,
        quantities,
        sides,
        prices=prices,
        execution_session=resolved_session,
        allow_extended_hours=allow_extended_hours,
    )
    order_type = "LIMIT" if all(price is not None for price in prices) else "MARKET"
    return OrderIntent(
        id=intent_id,
        idempotency_key=idempotency_key,
        strategy_id=SMOKE_STRATEGY_ID,
        account_id=account.id,
        action=IntentAction.ENTER if kind == "entry" else IntentAction.EXIT,
        execution_policy=ExecutionPolicy(
            allow_extended_hours=allow_extended_hours,
            execution_session=execution_session or ExecutionSession.REGULAR,
        ),
        legs=tuple(
            OrderLeg(
                id=f"{intent_id}:leg:{index}",
                intent_id=intent_id,
                sequence=index,
                instrument_id=_instrument_id(symbol),
                side=sides[index],
                quantity=quantities[index],
                quantity_unit=QuantityUnit.UNITS,
                order_type=order_type,
                limit_price=prices[index],
                status=LegStatus.PLANNED,
                metadata={"smoke_symbol": symbol, "smoke_kind": kind},
                created_at=now,
                updated_at=now,
            )
            for index, symbol in enumerate(symbols)
        ),
        metadata={"supervised_smoke": True, "symbols": list(symbols)},
        created_at=now,
        updated_at=now,
    )


def _risk_decision(intent: OrderIntent, prices: tuple[Decimal | None, Decimal | None]) -> RiskDecisionRecord:
    gross = sum((leg.quantity * price for leg, price in zip(intent.legs, prices) if price is not None), Decimal("0"))
    if gross > MAX_GROSS_CAP:
        raise GenericSmokeBlocked(f"proposed limit-order gross ${gross} exceeds the hard SIM cap ${MAX_GROSS_CAP}")
    return RiskDecisionRecord(
        id=f"risk:{intent.id}",
        intent_id=intent.id,
        approved=True,
        reason="supervised generic SIM smoke preflight approved",
        checks={"gross_cap": str(MAX_GROSS_CAP), "priced_gross": str(gross), "supervised": True},
        evaluated_at=datetime.now(timezone.utc),
    )


def _entry_is_exit_ready(entry: Mapping[str, Any] | None) -> bool:
    """Require durable full fills before an entry can be used for an exit.

    ``COMPLETED`` is the normal post-submit acknowledgement, while ``FILLED``
    is also valid after a restart recovered broker fills before the original
    process could record that acknowledgement.  Either status is safe only
    when every leg is terminally filled for its full requested quantity.
    Reconciliation-required, partial, working, and unknown states never pass.
    """

    if entry is None or str(entry.get("status", "")).upper() not in {
        IntentStatus.FILLED.value,
        IntentStatus.COMPLETED.value,
    }:
        return False
    legs = entry.get("legs") or ()
    if not legs:
        return False
    for leg in legs:
        status = str(leg.get("status", "")).upper()
        if status != LegStatus.FILLED.value:
            return False
        try:
            quantity = Decimal(str(leg.get("quantity")))
            cumulative = Decimal(str(leg.get("cumulative_filled_quantity")))
        except (InvalidOperation, TypeError, ValueError):
            return False
        if not quantity.is_finite() or quantity <= 0 or cumulative != quantity:
            return False
    return True


def _preflight(
    adapter: Any,
    repository: SQLiteTradingRepository,
    account: Account,
    *,
    stage: str,
    symbols: tuple[str, str],
    entry_intent: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    capabilities = adapter.get_capabilities(account)
    balance = adapter.get_balances(account)
    positions = tuple(adapter.get_positions(account))
    open_orders = tuple(adapter.get_open_orders(account))
    blockers: list[str] = []
    if not capabilities.supports_submit or not capabilities.supports_fill_read:
        blockers.append("adapter lacks required submit/fill capabilities")
    if balance.buying_power is None or balance.buying_power <= 0:
        blockers.append("SIM buying power is unavailable or non-positive")
    nonzero = [position for position in positions if position.signed_quantity != 0]
    if stage in {"preflight", "enter"} and nonzero:
        blockers.append("broker account is not flat; the generic smoke test will not touch existing positions")
    if stage == "exit":
        entry = entry_intent
        if not _entry_is_exit_ready(entry):
            blockers.append("exit requires a fully filled and reconciled generic smoke entry")
        else:
            expected = {
                str(leg["instrument_id"]): (Decimal(leg["quantity"]) if leg["side"] == "BUY" else -Decimal(leg["quantity"]))
                for leg in entry["legs"]
            }
            actual = {position.instrument_id: position.signed_quantity for position in nonzero}
            if actual != expected:
                blockers.append("broker positions do not exactly match the fully filled generic smoke entry")
    if open_orders:
        blockers.append("broker account has open orders; the generic smoke test will not add more")
    allocations = repository.position_allocations(account.id)
    if stage in {"preflight", "enter"} and any(Decimal(item["signed_quantity"]) != 0 for item in allocations):
        blockers.append("isolated generic smoke database has non-flat managed allocations")
    if repository.open_reconciliation_issues(account.id):
        blockers.append("isolated generic smoke database has open reconciliation issues")
    return {
        "environment": "SIM",
        "account_id": account.id,
        "external_account_id": account.external_account_id,
        "capabilities": asdict(capabilities),
        "balance": asdict(balance),
        "broker_positions": [asdict(item) for item in positions],
        "open_orders": [asdict(item) for item in open_orders],
        "ready_for_submit": not blockers,
        "blockers": blockers,
    }


def _wait_for_fill(oms: GenericOMS, intent_id: str, account: Account, timeout_seconds: int, sleep_fn: Callable[[float], None] = time.sleep) -> dict:
    deadline = time.monotonic() + timeout_seconds
    while True:
        state = oms.recover_intent(intent_id, account=account)
        if state["status"] == "FILLED":
            return oms.complete_intent(intent_id)
        if state["status"] == IntentStatus.REJECTED.value:
            raise GenericSmokeBlocked(f"{intent_id} was rejected; no broker order or fill was recorded")
        if time.monotonic() >= deadline:
            raise GenericSmokeBlocked(f"{intent_id} did not become fully filled within {timeout_seconds} seconds")
        sleep_fn(1)


def run_stage(
    args: argparse.Namespace,
    *,
    adapter_factory: Callable[..., Any] = MooMooGenericAdapter,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    stage = str(args.stage).lower()
    if stage not in {"preflight", "enter", "status", "exit"}:
        raise ValueError("stage must be preflight, enter, status, or exit")
    if stage in {"enter", "exit"} and not args.submit:
        raise GenericSmokeBlocked(f"{stage} requires --submit; no broker order was attempted")
    if stage in {"preflight", "status"} and args.submit:
        raise GenericSmokeBlocked("--submit is valid only for enter or exit")
    if not args.state_db:
        raise ValueError("--state-db is required and must be an isolated generic smoke database")
    state_db = Path(args.state_db).resolve()
    if state_db == (PROJECT_ROOT / "data" / "trading.db").resolve():
        raise GenericSmokeBlocked("the generic smoke harness refuses data/trading.db")
    symbols = (_symbol(args.symbol1), _symbol(args.symbol2))
    if symbols[0] == symbols[1]:
        raise ValueError("the two smoke symbols must differ")
    quantities = (_quantity(args.quantity1, "quantity1"), _quantity(args.quantity2, "quantity2"))
    execution_session = _execution_session(args)
    entry_sides = (Side(str(args.side1).upper()), Side(str(args.side2).upper()))
    prices = (
        _price(args.limit_price1, "limit_price1") if args.limit_price1 is not None else None,
        _price(args.limit_price2, "limit_price2") if args.limit_price2 is not None else None,
    )
    if (prices[0] is None) != (prices[1] is None):
        raise ValueError("provide both limit prices or neither")
    if execution_session is not ExecutionSession.REGULAR and not all(prices):
        raise GenericSmokeBlocked("non-regular-session smoke orders must provide both limit prices")
    exit_sides = tuple(Side.SELL if side is Side.BUY else Side.BUY for side in entry_sides)
    exit_intent_key = _intent_key(
        "exit",
        symbols,
        quantities,
        exit_sides,
        prices=prices,
        execution_session=execution_session,
    )

    resolver = StaticMoomooInstrumentResolver({_instrument_id(symbol): symbol for symbol in symbols})
    adapter = adapter_factory(
        instrument_resolver=resolver,
        external_account_id=str(args.acc_id),
        market="US",
        environment=TradingEnvironment.SIM,
        host=args.host,
        port=args.port,
        security_firm=args.security_firm,
    )
    try:
        adapter.connect()
        account = _build_account(adapter.selected_external_account_id or args.acc_id)
        repository = SQLiteTradingRepository(state_db)
        repository.initialize()
        _ensure_setup(repository, account, symbols)
        oms = GenericOMS(repository, adapter)
        entry_intent = _find_entry_intent(
            repository,
            account,
            symbols,
            quantities,
            entry_sides,
            execution_session,
        )
        if stage == "status":
            # Status is broker-read-only but may repair the local durable
            # state through the normal OMS recovery path.
            current_exit_intent = repository.get_intent_by_idempotency_key(account.id, exit_intent_key)
            for existing in (entry_intent, current_exit_intent):
                if existing is not None and existing["status"] in {
                    IntentStatus.SUBMITTING.value,
                    IntentStatus.WORKING.value,
                    IntentStatus.PARTIALLY_FILLED.value,
                    IntentStatus.RECONCILIATION_REQUIRED.value,
                }:
                    oms.recover_intent(str(existing["id"]), account=account)
            entry_intent = _find_entry_intent(
                repository,
                account,
                symbols,
                quantities,
                entry_sides,
                execution_session,
            )
        preflight = _preflight(
            adapter,
            repository,
            account,
            stage=stage,
            symbols=symbols,
            entry_intent=entry_intent,
        )
        result: dict[str, Any] = {"stage": stage, "state_db": str(state_db), "preflight": preflight, "orders_submitted": False}
        if stage in {"preflight", "status"}:
            result["entry_intent"] = entry_intent
            result["exit_intent"] = repository.get_intent_by_idempotency_key(account.id, exit_intent_key)
            return _as_json(result)
        if not preflight["ready_for_submit"]:
            raise GenericSmokeBlocked("preflight blocked mutation; no broker order was attempted")

        kind = "entry" if stage == "enter" else "exit"
        if stage == "exit":
            entry = entry_intent
            if not _entry_is_exit_ready(entry):
                raise GenericSmokeBlocked("exit requires a fully filled and reconciled generic smoke entry")
            sides = exit_sides
        else:
            if repository.get_intent_by_idempotency_key(account.id, exit_intent_key) is not None:
                raise GenericSmokeBlocked("this isolated database already completed a smoke cycle; use a new --state-db")
            sides = entry_sides
        intent = _intent(
            kind,
            account,
            symbols,
            quantities,
            sides,
            prices=prices,
            execution_session=execution_session,
        )
        submitted = oms.submit_intent(intent, account=account, risk_decision=_risk_decision(intent, prices))
        result["orders_submitted"] = True
        result["submitted_intent"] = submitted
        result["completed_intent"] = _wait_for_fill(oms, intent.id, account, args.timeout, sleep_fn=sleep_fn)
        final_positions = tuple(adapter.get_positions(account))
        final_open_orders = tuple(adapter.get_open_orders(account))
        result["broker_positions"] = [asdict(item) for item in final_positions]
        result["open_orders"] = [asdict(item) for item in final_open_orders]
        result["allocations"] = repository.position_allocations(account.id)
        result["open_reconciliation_issues"] = repository.open_reconciliation_issues(account.id)
        if result["open_reconciliation_issues"] or final_open_orders:
            raise GenericSmokeBlocked("post-submit reconciliation is not clean; no further smoke action is safe")
        if stage == "exit" and any(item.signed_quantity != 0 for item in final_positions):
            raise GenericSmokeBlocked("exit filled locally but broker positions are not flat")
        return _as_json(result)
    finally:
        try:
            adapter.disconnect()
        except Exception:
            pass


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=("preflight", "enter", "status", "exit"))
    parser.add_argument("--state-db", required=True)
    parser.add_argument("--acc-id", required=True, type=int)
    parser.add_argument("--symbol1", default="US.AAPL")
    parser.add_argument("--symbol2", default="US.MSFT")
    parser.add_argument("--quantity1", default=1)
    parser.add_argument("--quantity2", default=1)
    parser.add_argument("--side1", choices=("BUY", "SELL"), default="BUY")
    parser.add_argument("--side2", choices=("BUY", "SELL"), default="SELL")
    parser.add_argument("--limit-price1")
    parser.add_argument("--limit-price2")
    parser.add_argument(
        "--session",
        "--execution-session",
        dest="session",
        choices=("REGULAR", "EXTENDED", "OVERNIGHT"),
        default="REGULAR",
        help="execution session (default: REGULAR); non-regular sessions require both limit prices",
    )
    parser.add_argument(
        "--allow-extended-hours",
        action="store_true",
        help="legacy alias for --session EXTENDED",
    )
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--submit", action="store_true")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=11111)
    parser.add_argument("--security-firm")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        result = run_stage(parse_args(argv))
    except (GenericSmokeError, ValueError) as exc:
        print(json.dumps({"ready": False, "error": str(exc)}, indent=2))
        return 2
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
