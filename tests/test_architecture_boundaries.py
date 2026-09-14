"""Static architecture guards for the provider-neutral Stage 1–3 boundary.

These tests intentionally inspect source shape and Git blobs only.  They do
not import broker SDKs, open a broker connection, or assert prose in the
architecture document.
"""

from __future__ import annotations

import ast
import hashlib
from pathlib import Path
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
GENERIC_ROOT = ROOT / "src" / "trading_core"

# These are import roots, not an attempt to enumerate every broker in the
# world.  The local package boundaries catch repository adapters/providers;
# the common SDK roots catch accidental direct provider coupling.
FORBIDDEN_GENERIC_IMPORT_PREFIXES = (
    "src.strategies",
    "src.brokers",
    "src.data",
    "strategies",
    "moomoo",
    "futu",
    "yfinance",
    "alpaca",
    "ib_insync",
    "ibkr",
    "ccxt",
    "binance",
    "longbridge",
    "tigeropen",
)

# These are strategy/pair terms, not generic trading vocabulary.  The scan is
# intentionally narrow so generic concepts such as position, allocation,
# order, leg, and instrument remain available.
PAIR_SPECIFIC_TOKENS = (
    "pair",
    "ticker1",
    "ticker2",
    "stat_arb",
    "hedge_ratio",
    "zscore",
    "cointegration",
)

CURRENT_LEGACY_SMOKE_BLOBS = {
    "scripts/sim_smoke_test.py": "2fb7744b391e2e0621a1c2ef76742d3174a3af9d",
    "tests/test_sim_smoke.py": "66ed2827b4ecc27b5447461495113a755d50e0e1",
}


def _source_files(root: Path) -> list[Path]:
    return sorted(
        path
        for path in root.rglob("*.py")
        if "__pycache__" not in path.parts
    )


def _module_name(node: ast.AST) -> str:
    if isinstance(node, ast.Import):
        return ",".join(alias.name for alias in node.names)
    if isinstance(node, ast.ImportFrom):
        prefix = "." * node.level
        return prefix + (node.module or "")
    return ""


def _imports(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            modules.append(_module_name(node))
    return modules


def _is_forbidden_import(module: str) -> bool:
    normalized = module.lstrip(".")
    return any(
        normalized == prefix or normalized.startswith(prefix + ".")
        for prefix in FORBIDDEN_GENERIC_IMPORT_PREFIXES
    )


def _git_root() -> Path | None:
    """Find the repository even when tests are launched from another cwd."""
    for start in (Path(__file__).resolve(), ROOT):
        try:
            result = subprocess.run(
                ["git", "-C", str(start), "rev-parse", "--show-toplevel"],
                check=True,
                capture_output=True,
                text=True,
            )
        except (OSError, subprocess.CalledProcessError):
            continue
        value = result.stdout.strip()
        if value:
            return Path(value).resolve()
    return None


def _git_blob(root: Path, relative_path: str) -> str:
    """Return the Git blob ID after normalizing Windows line endings.

    The working tree may contain CRLF while the committed blob contains LF,
    depending on ``core.autocrlf``.  Normalize only the CRLF line terminators
    and ask Git to hash the resulting bytes without applying another filter so
    this guard behaves the same on every checkout configuration.
    """
    content = (root / relative_path).read_bytes().replace(b"\r\n", b"\n")
    result = subprocess.run(
        ["git", "-C", str(root), "hash-object", "--no-filters", "--stdin"],
        check=True,
        capture_output=True,
        input=content,
    )
    return result.stdout.decode("ascii").strip()


def _translator_sources() -> list[Path]:
    sources: list[Path] = []
    for path in _source_files(ROOT / "src"):
        if GENERIC_ROOT in path.parents:
            continue
        lowered_parts = {part.lower() for part in path.parts}
        if "translator" in path.stem.lower() or lowered_parts & {"translation", "translators"}:
            sources.append(path)
    return sources


def test_generic_core_has_no_strategy_or_provider_imports():
    assert GENERIC_ROOT.is_dir(), f"missing generic core directory: {GENERIC_ROOT}"
    violations: list[str] = []
    for path in _source_files(GENERIC_ROOT):
        for module in _imports(path):
            if _is_forbidden_import(module):
                violations.append(f"{path.relative_to(ROOT)} imports {module}")
            normalized = module.lstrip(".").lower()
            if "translator" in normalized or "translation" in normalized:
                violations.append(f"{path.relative_to(ROOT)} imports {module} (reverse translator edge)")
    assert not violations, "provider/strategy imports crossed into trading_core:\n" + "\n".join(violations)


def test_generic_core_has_no_pair_specific_domain_tokens():
    assert GENERIC_ROOT.is_dir(), f"missing generic core directory: {GENERIC_ROOT}"
    violations: list[str] = []
    for path in _source_files(GENERIC_ROOT):
        text = path.read_text(encoding="utf-8").lower()
        for token in PAIR_SPECIFIC_TOKENS:
            if token in text:
                violations.append(f"{path.relative_to(ROOT)} contains {token!r}")
    assert not violations, "pair-specific vocabulary crossed into trading_core:\n" + "\n".join(violations)


def test_translators_depend_on_core_when_present():
    sources = _translator_sources()
    if not sources:
        pytest.skip("no strategy translator module exists yet")

    violations: list[str] = []
    for path in sources:
        modules = _imports(path)
        imports_core = any(
            module.lstrip(".") == "src.trading_core"
            or module.lstrip(".").startswith("src.trading_core.")
            or module.lstrip(".") == "trading_core"
            or module.lstrip(".").startswith("trading_core.")
            for module in modules
        )
        if not imports_core:
            violations.append(f"{path.relative_to(ROOT)} does not import trading_core")
    assert not violations, "translator/core dependency direction is invalid:\n" + "\n".join(violations)


@pytest.mark.parametrize("relative_path,expected_blob", CURRENT_LEGACY_SMOKE_BLOBS.items())
def test_legacy_smoke_boundary_tracks_current_smoke_passed_blob(relative_path: str, expected_blob: str):
    """The working legacy smoke artifacts remain an explicit revision."""
    git_root = _git_root()
    if git_root is None:
        pytest.skip("Git repository is not available; cannot verify audit blob")

    target = git_root / relative_path
    assert target.is_file(), f"legacy smoke artifact is missing: {relative_path}"
    try:
        actual_blob = _git_blob(git_root, relative_path)
    except (OSError, subprocess.CalledProcessError) as exc:
        pytest.skip(f"Git blob lookup unavailable: {exc}")
    assert actual_blob == expected_blob, (
        f"{relative_path} changed from the current smoke-passed revision "
        f"({expected_blob}); update the documented smoke boundary if intentional"
    )
