"""Execution-safety primitives for durable two-leg trading."""

from .config import ExecutionConfig
from .engine import ExecutionEngine, ExecutionSafetyError
from .risk import RiskDecision, RiskLimits, RiskSnapshot, evaluate_entry_risk

__all__ = [
    "ExecutionConfig",
    "ExecutionEngine",
    "ExecutionSafetyError",
    "RiskDecision",
    "RiskLimits",
    "RiskSnapshot",
    "evaluate_entry_risk",
]
