"""Declarative Stage 6 pilot CLI.

Commands are intentionally asymmetric:

* ``validate`` only parses and validates the JSON configuration.
* ``dry-run`` prepares the local generic repository and prints the full
  two-intent report without contacting a broker.
* ``sim-submit`` is the only broker-capable command and requires both
  ``--arm-sim`` and the exact confirmation phrase containing the deterministic
  run correlation.  It still submits only through ``Stage6PilotRunner`` and
  ``GenericOMS``; there is no direct order path here.

REAL/LIVE and non-RTH handoffs are rejected by the configuration boundary.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.strategies.stat_arb.stage6_config import (  # noqa: E402
    Stage6ConfigError,
    Stage6PilotConfig,
    load_stage6_config,
)
from src.strategies.stat_arb.stage6_pilot import Stage6RunMode  # noqa: E402
from src.trading_core.repository import SQLiteTradingRepository  # noqa: E402


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generic Stage 6 combined-book SIM pilot")
    subparsers = parser.add_subparsers(dest="command", required=True)
    validate = subparsers.add_parser("validate", help="validate config only; no DB or broker access")
    dry_run = subparsers.add_parser("dry-run", help="prepare repository and print a no-submit report")
    baseline = subparsers.add_parser(
        "baseline",
        help="explicitly verify fresh flat SIM facts and import bounded legacy order evidence (no submit)",
    )
    sim_submit = subparsers.add_parser("sim-submit", help="explicitly arm one SIM pilot through GenericOMS")
    for command in (validate, dry_run, baseline, sim_submit):
        command.add_argument("--config", type=Path, required=True)
        command.add_argument("--json", action="store_true", dest="as_json")
    baseline.add_argument("--legacy-db", type=Path, required=True)
    baseline.add_argument("--legacy-label", default="legacy-smoke")
    baseline.add_argument(
        "--verify-flat",
        action="store_true",
        help="explicitly confirm that this command may create the audited flat baseline",
    )
    sim_submit.add_argument("--arm-sim", action="store_true", help="required explicit SIM arm")
    sim_submit.add_argument(
        "--confirm",
        help="must exactly equal: ARM STAGE6 SIM <deterministic-run-id>",
    )
    return parser


def _summary(config: Stage6PilotConfig) -> dict[str, Any]:
    spec = config.spec()
    return {
        "valid": True,
        "state_db": str(config.state_db),
        "account_id": config.account.id,
        "external_account_id": config.account.external_account_id,
        "environment": config.account.environment.value,
        "sleeves": [item.sleeve_id for item in config.sleeves],
        "books": [item.book_id for item in config.sleeves],
        "instrument_ids": [item.id for item in config.instruments],
        "moomoo_symbols": [item.external_symbol for item in config.mappings],
        "run_id": spec.run_id,
        "required_confirmation": config.confirmation_phrase(),
        "rth_handoff_policy": config.execution.rth_handoff_policy,
        "mode": "DRY_RUN",
    }


def _emit(value: Any, *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(value, sort_keys=True, indent=2, default=str))
        return
    if isinstance(value, dict) and "started_at" in value and "stop_reasons" in value:
        print(
            f"run_id={value.get('run_id')} mode={value.get('mode')} "
            f"config_preflight={'PASS' if value.get('preflight_passed') else 'STOP'} "
            f"broker_preflight={'PASS' if value.get('broker_preflight_passed') else 'NOT_RUN/STOP'}"
        )
        for plan in value.get("intent_plans", ()):
            print(
                f"intent={plan.get('intent_id')} sleeve={plan.get('sleeve_id')} "
                f"action={plan.get('action')}"
            )
        for reason in value.get("stop_reasons", ()):
            print(f"STOP: {reason}")
        if value.get("required_confirmation"):
            print(f"required_confirmation={value['required_confirmation']}")
        return
    if isinstance(value, dict) and "required_confirmation" in value and "run_id" in value:
        print(f"valid={value.get('valid', True)} run_id={value['run_id']}")
        print(f"account={value.get('account_id')} external_account={value.get('external_account_id')} environment={value.get('environment')}")
        print(f"sleeves={','.join(value.get('sleeves', []))} books={','.join(value.get('books', []))}")
        print(f"required_confirmation={value.get('required_confirmation')}")
        return
    print(json.dumps(value, sort_keys=True, indent=2, default=str))


def _run(args: argparse.Namespace) -> tuple[int, Any]:
    config = load_stage6_config(args.config)
    if args.command == "validate":
        return 0, _summary(config)

    if args.command == "sim-submit":
        if not args.arm_sim:
            raise Stage6ConfigError("sim-submit requires --arm-sim")
        expected = config.confirmation_phrase()
        if args.confirm != expected:
            raise Stage6ConfigError(
                f"exact SIM confirmation required: {expected!r}; no broker connection was attempted"
            )
    if args.command == "baseline" and not args.verify_flat:
        raise Stage6ConfigError(
            "baseline requires --verify-flat; it performs a fresh read-only SIM account-facts check"
        )

    repository = SQLiteTradingRepository(config.state_db)
    config.ensure_repository(repository)
    if args.command == "dry-run":
        report = config.build_runner(repository).run(config.spec(), mode=Stage6RunMode.DRY_RUN)
        payload = report.as_dict()
        payload["required_confirmation"] = config.confirmation_phrase()
        payload["config"] = _summary(config)
        return (0 if report.preflight_passed and not report.stop_reasons else 2), payload

    adapter = config.build_moomoo_adapter()
    connected = False
    try:
        adapter.connect()
        connected = True
        if args.command == "baseline":
            result = config.build_runner(repository, adapter=adapter).establish_verified_legacy_baseline(
                config.spec(),
                str(args.legacy_db),
                legacy_label=args.legacy_label,
            )
            result["config"] = _summary(config)
            return 0, result
        report = config.build_runner(repository, adapter=adapter).run(
            config.spec(),
            mode=Stage6RunMode.SIM_SUBMIT,
        )
    finally:
        if connected:
            adapter.disconnect()
    payload = report.as_dict()
    payload["config"] = _summary(config)
    return (0 if report.preflight_passed and not report.stop_reasons else 2), payload


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        code, payload = _run(args)
        _emit(payload, as_json=args.as_json)
        return code
    except (Stage6ConfigError, ValueError, OSError, PermissionError, RuntimeError) as exc:
        payload = {"valid": False, "status": "BLOCKED", "error": str(exc)}
        _emit(payload, as_json=args.as_json)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
