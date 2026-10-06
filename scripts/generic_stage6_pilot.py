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
from collections.abc import Mapping
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any
import uuid

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
from src.trading_core.stage6_validation import (  # noqa: E402
    Stage6SessionOutcome,
    Stage6ValidationError,
    evaluate_stage6_session,
)


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
    validation_commands = {}
    for name, help_text in (
        ("session-preflight", "record one no-submit Stage 6 preflight evidence observation"),
        ("session-recover", "record an existing fresh-process recovery observation; never submits"),
        ("session-finalize", "derive and immutably persist one Stage 6 validation result"),
        ("session-status", "show retained observations and one immutable session result"),
        ("stage-status", "show retained Stage 6 history and the three-date completion gate"),
    ):
        command = subparsers.add_parser(name, help=help_text)
        command.add_argument("--config", type=Path, required=True)
        command.add_argument("--json", action="store_true", dest="as_json")
        command.add_argument("--session-id")
        command.add_argument("--evidence", type=Path, help="JSON evidence document captured around the existing runner")
        command.add_argument("--commit-sha")
        command.add_argument("--execution-compatibility")
        command.add_argument("--trading-date", help="US trading date (YYYY-MM-DD) when absent from evidence")
        command.add_argument("--process-id")
        validation_commands[name] = command
    validation_commands["session-recover"].add_argument(
        "--fresh-process", action="store_true", help="assert that the evidence came from a separate process"
    )
    validation_commands["stage-status"].add_argument("--required-sessions", type=int, default=3)
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


def _read_evidence(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Stage6ConfigError(f"could not read Stage 6 evidence {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise Stage6ConfigError("Stage 6 evidence must be a JSON object")
    return value


def _validation_defaults(
    config: Stage6PilotConfig,
    args: argparse.Namespace,
    evidence: dict[str, Any] | None,
) -> dict[str, Any]:
    """Fill only session identity metadata; never fill acceptance facts."""

    value = dict(evidence or {})
    run_id = config.spec().run_id
    now = datetime.now(timezone.utc).isoformat()
    value.setdefault("session_id", args.session_id or f"stage6-validation-{run_id}")
    value.setdefault("account_id", config.account.id)
    value.setdefault("environment", config.account.environment.value)
    value.setdefault("execution_path", "Stage6PilotRunner->GenericOMS")
    value.setdefault("execution_compatibility", args.execution_compatibility or "UNKNOWN")
    value.setdefault("commit_sha", args.commit_sha or "UNKNOWN")
    value.setdefault("us_trading_date", args.trading_date or value.get("trading_date"))
    value.setdefault("started_at", now)
    value.setdefault("completed_at", value.get("started_at", now))
    value.setdefault("captured_at", value.get("completed_at", now))
    value.setdefault("run_ids", [run_id])
    value.setdefault("supervised", True)
    return value


def _observation_payload(value: Mapping[str, Any], phase: str) -> dict[str, Any]:
    payload = dict(value)
    payload["phase"] = phase
    return payload


def _record_validation_observation(
    repository: SQLiteTradingRepository,
    *,
    config: Stage6PilotConfig,
    args: argparse.Namespace,
    phase: str,
    evidence: dict[str, Any] | None,
) -> dict[str, Any]:
    if evidence is None:
        if phase != "PREFLIGHT":
            raise Stage6ConfigError(
                f"{phase.lower()} evidence must be captured by the existing Stage6PilotRunner path; "
                "the validation command will not invent a recovery/fill observation"
            )
        dry_report = config.build_runner(repository).run(config.spec(), mode=Stage6RunMode.DRY_RUN)
        evidence = {
            "session_id": args.session_id or f"stage6-validation-{config.spec().run_id}",
            "account_id": config.account.id,
            "environment": config.account.environment.value,
            "execution_path": "Stage6PilotRunner->GenericOMS",
            "execution_mode": "DRY_RUN",
            "supervised": True,
            "commit_sha": args.commit_sha or "UNKNOWN",
            "execution_compatibility": args.execution_compatibility or "UNKNOWN",
            "us_trading_date": args.trading_date,
            "run_ids": [config.spec().run_id],
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "dry_run_report": dry_report.as_dict(),
            "preflight": {
                "source": "dry-run",
                "fresh_facts": {"complete": False, "flat": False, "open_order_count": 0},
            },
        }
    value = _validation_defaults(config, args, evidence)
    phase_evidence = value.get("preflight" if phase == "PREFLIGHT" else "restart_recovery")
    captured_at = value.get("captured_at")
    if phase_evidence is not None and isinstance(phase_evidence, Mapping):
        captured_at = phase_evidence.get("captured_at", captured_at)
    try:
        parsed = datetime.fromisoformat(str(captured_at).replace("Z", "+00:00"))
    except ValueError as exc:
        raise Stage6ConfigError("validation observation captured_at must be ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise Stage6ConfigError("validation observation captured_at must include a timezone")
    session_id = str(value["session_id"])
    observation_id = f"{session_id}:{phase.lower()}:{uuid.uuid4().hex}"
    process_id = args.process_id
    if phase == "RECOVERY" and args.fresh_process:
        value["restart_recovery"] = {
            **dict(value.get("restart_recovery") or {}),
            "fresh_process": True,
            "process_id": process_id or value.get("process_id") or "operator-provided",
        }
        process_id = str(value["restart_recovery"]["process_id"])
    repository.record_stage6_validation_observation(
        observation_id=observation_id,
        session_id=session_id,
        account_id=config.account.id,
        phase=phase,
        captured_at=parsed,
        evidence=_observation_payload(value, phase),
        process_id=process_id,
        fresh_process=bool(
            phase == "RECOVERY"
            and isinstance(value.get("restart_recovery"), Mapping)
            and value["restart_recovery"].get("fresh_process") is True
        ),
    )
    return {
        "observation_id": observation_id,
        "session_id": session_id,
        "account_id": config.account.id,
        "phase": phase,
        "captured_at": parsed.isoformat(),
        "broker_contacted": False,
        "orders_submitted": 0,
        "evidence": value,
    }


def _merge_validation_observations(
    repository: SQLiteTradingRepository,
    session_id: str,
) -> dict[str, Any]:
    observations = repository.stage6_validation_observations(session_id)
    merged: dict[str, Any] = {"session_id": session_id}
    for row in observations:
        value = row.get("evidence")
        if not isinstance(value, Mapping):
            continue
        for key in (
            "account_id",
            "environment",
            "execution_path",
            "execution_compatibility",
            "commit_sha",
            "us_trading_date",
            "started_at",
            "completed_at",
            "run_ids",
            "entry_intent_ids",
            "exit_intent_ids",
            "supervised",
            "manual_intervention",
            "manual_flags",
            "audit_refs",
        ):
            if key in value:
                merged[key] = value[key]
        phase = str(row.get("phase", "")).upper()
        if phase == "PREFLIGHT" and isinstance(value.get("preflight"), Mapping):
            merged["preflight"] = value["preflight"]
        elif phase == "RECOVERY" and isinstance(value.get("restart_recovery"), Mapping):
            merged["restart_recovery"] = value["restart_recovery"]
        elif phase == "FINAL" and isinstance(value.get("final"), Mapping):
            merged["final"] = value["final"]
        for key in ("entry", "exit"):
            if isinstance(value.get(key), Mapping):
                merged[key] = value[key]
    return merged


def _run_validation_command(args: argparse.Namespace, config: Stage6PilotConfig) -> tuple[int, Any]:
    repository = SQLiteTradingRepository(config.state_db)
    config.ensure_repository(repository)
    if args.command == "session-preflight":
        return 0, _record_validation_observation(
            repository,
            config=config,
            args=args,
            phase="PREFLIGHT",
            evidence=_read_evidence(args.evidence),
        )
    if args.command == "session-recover":
        return 0, _record_validation_observation(
            repository,
            config=config,
            args=args,
            phase="RECOVERY",
            evidence=_read_evidence(args.evidence),
        )
    if args.command == "session-status":
        session_id = args.session_id
        if not session_id:
            raise Stage6ConfigError("session-status requires --session-id")
        return 0, {
            "session": repository.get_stage6_validation_session(session_id),
            "observations": repository.stage6_validation_observations(session_id),
            "broker_contacted": False,
            "orders_submitted": 0,
        }
    if args.command == "session-finalize":
        evidence = _read_evidence(args.evidence)
        if evidence is None:
            session_id = args.session_id
            if not session_id:
                raise Stage6ConfigError("session-finalize requires --session-id or --evidence")
            evidence = _merge_validation_observations(repository, session_id)
        evidence = _validation_defaults(config, args, evidence)
        try:
            result = evaluate_stage6_session(evidence)
        except Stage6ValidationError as exc:
            raise Stage6ConfigError(str(exc)) from exc
        repository.save_stage6_validation_session(result)
        payload = result.as_dict()
        payload["broker_contacted"] = False
        payload["orders_submitted"] = 0
        return (0 if result.outcome is Stage6SessionOutcome.CLEAN_PASS else 2), payload
    if args.command == "stage-status":
        if args.required_sessions <= 0:
            raise Stage6ConfigError("--required-sessions must be positive")
        return 0, repository.stage6_validation_status(
            config.account.id,
            execution_compatibility=args.execution_compatibility,
            required_sessions=args.required_sessions,
        )
    raise Stage6ConfigError(f"unsupported Stage 6 validation command: {args.command}")


def _run(args: argparse.Namespace) -> tuple[int, Any]:
    config = load_stage6_config(args.config)
    if args.command == "validate":
        return 0, _summary(config)

    if args.command in {
        "session-preflight",
        "session-recover",
        "session-status",
        "session-finalize",
        "stage-status",
    }:
        return _run_validation_command(args, config)

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
