from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json

from scripts.generic_stage6_pilot import main
from src.strategies.stat_arb.stage6_config import Stage6PilotConfig
from src.trading_core.domain import BrokerOrderSnapshot, BrokerOrderStatus, ExecutionEvidenceMode, Side
from src.trading_core.ports import BrokerFill, BrokerHistoricalOrderFacts

from tests.test_generic_stage6_config_cli import _config_dict


class HistoricalOnlyAdapter:
    """Fake adapter proving the export uses only bounded historical reads."""

    def __init__(self, account_id: str, now: datetime) -> None:
        self.account_id = account_id
        self.now = now
        self.connect_calls = 0
        self.disconnect_calls = 0
        self.history_calls: list[tuple[datetime, datetime]] = []
        self.submit_calls = 0
        self.cancel_calls = 0
        self.replace_calls = 0
        self.recovery_calls = 0

    def connect(self) -> None:
        self.connect_calls += 1

    def disconnect(self) -> None:
        self.disconnect_calls += 1

    def get_historical_order_facts(self, account, start, end):  # type: ignore[no-untyped-def]
        self.history_calls.append((start, end))
        orders = []
        fills = []
        for external_id, instrument_id, side, price, filled_at, code in (
            ("3449827", "pilot-a-1", Side.BUY, "336.57", "2026-10-07T17:13:03+00:00", "US.AAPL"),
            ("3449828", "pilot-a-2", Side.SELL, "527.58", "2026-10-07T17:13:00+00:00", "US.MSFT"),
        ):
            order_time = datetime.fromisoformat(filled_at)
            raw = {
                "order_id": external_id,
                "code": code,
                "trd_side": side.value,
                "qty": "1",
                "dealt_qty": "1",
                "dealt_avg_price": price,
                "order_status": "FILLED_ALL",
                "updated_time": filled_at,
            }
            orders.append(
                BrokerOrderSnapshot(
                    id=f"history:order:{external_id}",
                    broker_snapshot_id="history-snapshot",
                    account_id=account.id,
                    external_account_id=account.external_account_id,
                    instrument_id=instrument_id,
                    external_order_id=external_id,
                    side=side,
                    quantity=Decimal("1"),
                    filled_quantity=Decimal("1"),
                    status=BrokerOrderStatus.FILLED,
                    captured_at=self.now,
                    order_time=order_time,
                    authority="ADAPTER_ORDER_SNAPSHOT",
                    metadata={"external_symbol": code, "raw": raw},
                )
            )
            evidence_reference = f"{external_id}:moomoo-order-fill:{external_id}"
            fills.append(
                BrokerFill(
                    external_order_id=external_id,
                    dedupe_key=f"moomoo-order-fill:{external_id}",
                    quantity=Decimal("1"),
                    price=Decimal(price),
                    filled_at=order_time,
                    received_at=self.now,
                    account_id=account.id,
                    instrument_id=instrument_id,
                    evidence_reference=evidence_reference,
                    evidence_mode=ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS,
                    metadata={
                        "source": "history_order_list_query",
                        "synthetic": True,
                        "evidence_mode": "CUMULATIVE_ORDER_SNAPSHOTS",
                        "evidence_reference": evidence_reference,
                        "evidence_scope": "HISTORICAL_ORDER_SNAPSHOTS",
                        "raw": raw,
                    },
                )
            )
        return BrokerHistoricalOrderFacts(
            account_id=account.id,
            requested_start=start,
            requested_end=end,
            captured_at=self.now,
            complete=True,
            orders=tuple(orders),
            fills=tuple(fills),
            metadata={
                "source": "history_order_list_query",
                "requested_start": start.isoformat(),
                "requested_end": end.isoformat(),
            },
            execution_evidence_mode=ExecutionEvidenceMode.CUMULATIVE_ORDER_SNAPSHOTS,
            execution_evidence_scope=frozenset({"HISTORICAL_ORDER_SNAPSHOTS"}),
        )

    def submit_order(self, *_args, **_kwargs):  # type: ignore[no-untyped-def]
        self.submit_calls += 1
        raise AssertionError("historical-preflight must never submit")

    def cancel_order(self, *_args, **_kwargs):  # type: ignore[no-untyped-def]
        self.cancel_calls += 1
        raise AssertionError("historical-preflight must never cancel")

    def replace_order(self, *_args, **_kwargs):  # type: ignore[no-untyped-def]
        self.replace_calls += 1
        raise AssertionError("historical-preflight must never replace")


def test_historical_preflight_exports_exact_facts_without_repository_or_order_mutation(tmp_path, monkeypatch, capsys):
    values = _config_dict(tmp_path)
    config_path = tmp_path / "stage6-pilot.json"
    config_path.write_text(json.dumps(values), encoding="utf-8")
    config = Stage6PilotConfig.load(config_path)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    adapter = HistoricalOnlyAdapter(config.account.id, now)
    monkeypatch.setattr(Stage6PilotConfig, "build_moomoo_adapter", lambda _self: adapter)

    assert main(
        [
            "historical-preflight",
            "--config",
            str(config_path),
            "--start",
            "2026-10-07T17:00:00+00:00",
            "--end",
            "2026-10-07T18:00:00+00:00",
            "--json",
        ]
    ) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "HISTORICAL_PREFLIGHT"
    assert payload["historical_preflight_passed"] is True
    facts = payload["historical_facts"]
    assert facts["complete"] is True
    assert facts["execution_evidence_mode"] == "CUMULATIVE_ORDER_SNAPSHOTS"
    assert facts["execution_evidence_scope"] == ["HISTORICAL_ORDER_SNAPSHOTS"]
    assert [row["external_order_id"] for row in facts["orders"]] == ["3449827", "3449828"]
    assert [row["external_order_id"] for row in facts["fills"]] == ["3449827", "3449828"]
    assert payload["mutations"] == {
        "order_intents": 0,
        "order_legs": 0,
        "broker_orders": 0,
        "fills": 0,
        "submission_calls": 0,
        "cancel_calls": 0,
        "replace_calls": 0,
        "recovery_calls": 0,
        "persistence_writes": 0,
    }
    assert not config.state_db.exists()
    assert (adapter.connect_calls, adapter.disconnect_calls) == (1, 1)
    assert len(adapter.history_calls) == 1
    assert adapter.submit_calls == adapter.cancel_calls == adapter.replace_calls == adapter.recovery_calls == 0


def test_historical_preflight_requires_timezone_aware_bounds(tmp_path, monkeypatch):
    values = _config_dict(tmp_path)
    config_path = tmp_path / "stage6-pilot.json"
    config_path.write_text(json.dumps(values), encoding="utf-8")
    adapter = HistoricalOnlyAdapter("pilot-account", datetime.now(timezone.utc))
    monkeypatch.setattr(Stage6PilotConfig, "build_moomoo_adapter", lambda _self: adapter)

    assert main(
        [
            "historical-preflight",
            "--config",
            str(config_path),
            "--start",
            "2026-10-07T17:00:00",
            "--end",
            "2026-10-07T18:00:00+00:00",
            "--json",
        ]
    ) == 2
    assert adapter.connect_calls == 0

