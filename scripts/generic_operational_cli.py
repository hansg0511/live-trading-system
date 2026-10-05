"""Local Stage 7 operational status/lifecycle CLI.

This command is intentionally ledger/read/status oriented.  It defaults to
dry-run, never discovers or submits broker orders, and has no REAL mode.  A
SIM-armed ``once`` tick only invokes the explicitly wired GenericOMS recovery
path; this CLI does not wire a broker adapter itself.
"""

from __future__ import annotations

import argparse
from dataclasses import replace
import json
from pathlib import Path
import sys
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.trading_core.operations import (  # noqa: E402
    OperationalConfig,
    OperationalMode,
    OperationalSafetyError,
    OperationalService,
)
from src.trading_core.repository import SQLiteTradingRepository  # noqa: E402


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generic Stage 7 local operational status/lifecycle")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("status", "start", "once", "stop"):
        command = subparsers.add_parser(name)
        command.add_argument("--state-db", default="data/generic-sim-smoke.db")
        command.add_argument("--account-id")
        command.add_argument("--config", type=Path)
        command.add_argument("--mode", choices=("DRY_RUN", "SIM_ARMED"))
        command.add_argument("--sim-arm", action="store_true")
        command.add_argument("--json", action="store_true", dest="as_json")
    return parser


def _config(repository: SQLiteTradingRepository, args: argparse.Namespace) -> OperationalConfig:
    if args.config is not None:
        config = OperationalConfig.load(args.config)
        if args.account_id and args.account_id != config.account.id:
            raise OperationalSafetyError("--account-id does not match the configured account")
    else:
        if not args.account_id:
            raise OperationalSafetyError("--account-id or --config is required")
        account = repository.get_account(args.account_id)
        if account is None:
            raise OperationalSafetyError(f"account is not persisted: {args.account_id}")
        config = OperationalConfig(account=account)
    if args.mode is not None or args.sim_arm:
        selected_mode = OperationalMode(args.mode or config.mode.value)
        config = replace(config, mode=selected_mode, sim_arm=bool(args.sim_arm or config.sim_arm))
    return config


def _emit(value: Any, *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(value, sort_keys=True, indent=2, default=str))
    else:
        account = value.get("account", {}) if isinstance(value, dict) else {}
        print(f"account={account.get('id', '<unknown>')} mode={value.get('mode')} lifecycle={value.get('lifecycle')}")
        print(f"submit_gate={value.get('submit_gate')}")
        print(f"books={len(value.get('books', []))} intents={len(value.get('intents', []))} orders={len(value.get('orders', []))} fills={sum(len(item.get('fills', [])) for item in value.get('fills', []))}")
        print(f"reconciliation_blockers={len(value.get('reconciliation_blockers', []))} unfinished_intents={len(value.get('unfinished_intent_blockers', []))} recovery_actions={len(value.get('recovery_actions', []))} alerts={len(value.get('alerts', []))}")
        if value.get("tick"):
            print(f"tick={value['tick']}")


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    repository = SQLiteTradingRepository(args.state_db)
    repository.initialize()
    try:
        config = _config(repository, args)
        service = OperationalService(repository, config)
        if args.command == "status":
            result = service.status()
        elif args.command == "start":
            result = service.start()
        elif args.command == "stop":
            result = service.stop()
        else:
            service.start()
            result = service.once()
        _emit(result, as_json=args.as_json)
        return 0
    except (OperationalSafetyError, ValueError, OSError) as exc:
        error = {"status": "BLOCKED", "error": str(exc)}
        if args.as_json:
            print(json.dumps(error, sort_keys=True))
        else:
            print(f"BLOCKED: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
