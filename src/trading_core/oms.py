"""Fake-testable broker-neutral order-management coordinator."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
import uuid

from .domain import (
    Account,
    BrokerOrderStatus,
    FailurePolicy,
    Fill,
    IntentStatus,
    IssueSeverity,
    IssueStatus,
    LegStatus,
    OrderIntent,
    OrderLeg,
    ReconciliationIssue,
    ReconciliationRun,
    ReconciliationStatus,
    RiskDecisionRecord,
)
from .ports import BrokerAdapter, BrokerFill, BrokerSubmissionResult, BrokerSubmitRequest
from .repository import SQLiteTradingRepository


class OMSExecutionError(RuntimeError):
    pass


class GenericOMS:
    """Coordinates durable logical intents without importing an adapter SDK."""

    def __init__(self, repository: SQLiteTradingRepository, adapter: BrokerAdapter):
        self.repository = repository
        self.adapter = adapter

    def submit_intent(
        self,
        intent: OrderIntent,
        *,
        account: Account,
        risk_decision: RiskDecisionRecord,
    ) -> dict:
        if intent.account_id != account.id:
            raise ValueError("intent and account identity must match")
        if risk_decision.intent_id != intent.id:
            raise ValueError("risk decision and intent identity must match")

        intent_id, created = self.repository.create_intent(intent)
        existing = self._required_intent(intent_id)
        resumable = {
            IntentStatus.CREATED.value,
            IntentStatus.RISK_APPROVED.value,
            IntentStatus.SUBMITTING.value,
        }
        if not created and existing["status"] not in resumable:
            return existing

        blockers = self.repository.open_reconciliation_issues(account.id)
        if blockers:
            blocked_decision = replace(
                risk_decision,
                id=f"{risk_decision.id}:reconciliation-block",
                approved=False,
                reason=f"blocked by {len(blockers)} open reconciliation issue(s)",
            )
            self.repository.save_risk_decision(blocked_decision)
            if existing["status"] == IntentStatus.CREATED.value:
                self.repository.transition_intent(intent_id, IntentStatus.REJECTED)
            else:
                self._require_reconciliation(
                    intent_id,
                    account,
                    category="ACCOUNT_RECONCILIATION_BLOCK",
                    entity_type="ACCOUNT",
                    entity_key=account.id,
                    details={"open_issue_count": len(blockers)},
                )
            return self._required_intent(intent_id)

        if existing["status"] == IntentStatus.CREATED.value:
            self.repository.save_risk_decision(risk_decision)
            if not risk_decision.approved:
                self.repository.transition_intent(intent_id, IntentStatus.REJECTED)
                return self._required_intent(intent_id)
            self.repository.transition_intent(intent_id, IntentStatus.RISK_APPROVED)
            existing = self._required_intent(intent_id)
        if existing["status"] == IntentStatus.RISK_APPROVED.value:
            self.repository.transition_intent(intent_id, IntentStatus.SUBMITTING)

        accepted_any = False
        for leg in intent.legs:
            stored = {item["id"]: item for item in self._required_intent(intent_id)["legs"]}[leg.id]
            if stored["status"] in {
                LegStatus.WORKING.value,
                LegStatus.PARTIALLY_FILLED.value,
                LegStatus.FILLED.value,
            }:
                accepted_any = True
                continue
            if stored["status"] in {
                LegStatus.REJECTED.value,
                LegStatus.FAILED.value,
                LegStatus.CANCELLED.value,
                LegStatus.RECONCILIATION_REQUIRED.value,
            }:
                self._require_reconciliation(
                    intent_id,
                    account,
                    category="INCOMPLETE_INTENT",
                    entity_type="ORDER_LEG",
                    entity_key=leg.id,
                    details={"status": stored["status"]},
                )
                return self._required_intent(intent_id)

            outcome = self._submit_leg(intent, leg, account, stored_status=stored["status"])
            if outcome == "accepted":
                accepted_any = True
                continue
            if outcome == "ambiguous" or accepted_any:
                self._require_reconciliation(
                    intent_id,
                    account,
                    category="SUBMISSION_AMBIGUITY" if outcome == "ambiguous" else "INCOMPLETE_INTENT",
                    entity_type="ORDER_LEG",
                    entity_key=leg.id,
                    details={"outcome": outcome},
                )
            else:
                self.repository.transition_intent(intent_id, IntentStatus.REJECTED)
            if intent.execution_policy.failure_policy is FailurePolicy.CANCEL_WORKING_LEGS:
                self._cancel_working_attempts(intent, account)
            return self._required_intent(intent_id)

        current = self._required_intent(intent_id)["status"]
        if current == IntentStatus.SUBMITTING.value:
            self.repository.transition_intent(intent_id, IntentStatus.WORKING)
        return self._required_intent(intent_id)

    def _submit_leg(
        self,
        intent: OrderIntent,
        leg: OrderLeg,
        account: Account,
        *,
        stored_status: str,
    ) -> str:
        attempts = self.repository.broker_orders_for_leg(leg.id)
        if attempts:
            last = attempts[-1]
            if last["status"] != BrokerOrderStatus.PREPARED.value:
                if stored_status != LegStatus.RECONCILIATION_REQUIRED.value:
                    self.repository.transition_leg(leg.id, LegStatus.RECONCILIATION_REQUIRED)
                return "ambiguous"
            broker_order_id = str(last["id"])
            attempt_number = int(last["attempt_number"])
            client_order_id = str(last["client_order_id"])
        else:
            if stored_status == LegStatus.PLANNED.value:
                self.repository.transition_leg(leg.id, LegStatus.SUBMITTING)
            elif stored_status != LegStatus.SUBMITTING.value:
                raise OMSExecutionError(f"cannot prepare attempt from leg status {stored_status}")
            broker_order_id = str(uuid.uuid4())
            attempt_number, client_order_id = self.repository.create_broker_order(
                broker_order_id=broker_order_id,
                order_leg_id=leg.id,
                account_id=account.id,
                broker=account.broker,
                attempt_number=None,
                client_order_id=None,
                submitted_quantity=leg.quantity,
            )

        self.repository.transition_broker_order(broker_order_id, BrokerOrderStatus.SUBMITTING)
        request = BrokerSubmitRequest(
            broker_order_id=broker_order_id,
            account_id=account.id,
            broker=account.broker,
            order_leg=leg,
            client_order_id=client_order_id,
            attempt_number=attempt_number,
        )
        try:
            result = self.adapter.submit_order(account, request)
        except Exception as exc:
            self.repository.record_submission(
                broker_order_id,
                status=BrokerOrderStatus.UNKNOWN,
                external_order_id=None,
                metadata={"exception": type(exc).__name__, "message": str(exc)},
            )
            self.repository.transition_leg(leg.id, LegStatus.RECONCILIATION_REQUIRED)
            return "ambiguous"

        if result.broker_order_id != broker_order_id or self._is_ambiguous(result):
            self.repository.record_submission(
                broker_order_id,
                status=BrokerOrderStatus.UNKNOWN,
                external_order_id=result.external_order_id,
                metadata={"provider_status": result.status.value, "error": result.error_message},
            )
            self.repository.transition_leg(leg.id, LegStatus.RECONCILIATION_REQUIRED)
            return "ambiguous"

        if result.accepted is False or result.status is BrokerOrderStatus.REJECTED:
            self.repository.record_submission(
                broker_order_id,
                status=BrokerOrderStatus.REJECTED,
                external_order_id=result.external_order_id,
                metadata={"provider_status": result.status.value, "error": result.error_message},
            )
            self.repository.transition_leg(leg.id, LegStatus.REJECTED)
            return "rejected"

        accepted_status = result.status
        if accepted_status not in {
            BrokerOrderStatus.WORKING,
            BrokerOrderStatus.PARTIALLY_FILLED,
            BrokerOrderStatus.FILLED,
        }:
            accepted_status = BrokerOrderStatus.WORKING
        self.repository.record_submission(
            broker_order_id,
            status=accepted_status,
            external_order_id=result.external_order_id,
            metadata={
                "provider_status": result.status.value,
                "reported_cumulative_fill": str(result.cumulative_filled_quantity),
                "raw_payload": dict(result.raw_payload),
            },
        )
        if accepted_status is BrokerOrderStatus.WORKING:
            self.repository.transition_leg(leg.id, LegStatus.WORKING)
            return "accepted"

        # Quantity without fill price/deal evidence cannot safely update the
        # owned position ledger. Preserve the broker status and reconcile.
        self.repository.transition_leg(leg.id, LegStatus.RECONCILIATION_REQUIRED)
        return "ambiguous"

    @staticmethod
    def _is_ambiguous(result: BrokerSubmissionResult) -> bool:
        return bool(result.ambiguous or result.accepted is None or (result.accepted and not result.external_order_id))

    def _require_reconciliation(
        self,
        intent_id: str,
        account: Account,
        *,
        category: str,
        entity_type: str,
        entity_key: str,
        details: dict,
    ) -> None:
        current = self._required_intent(intent_id)["status"]
        if current != IntentStatus.RECONCILIATION_REQUIRED.value:
            self.repository.transition_intent(intent_id, IntentStatus.RECONCILIATION_REQUIRED)
        now = datetime.now(timezone.utc)
        run_id = str(uuid.uuid4())
        self.repository.save_reconciliation_run(
            ReconciliationRun(
                id=run_id,
                account_id=account.id,
                started_at=now,
                completed_at=now,
                status=ReconciliationStatus.COMPLETED,
                metadata={"source": "oms"},
            )
        )
        issue_key = f"{category}:{entity_type}:{entity_key}"
        self.repository.upsert_reconciliation_issue(
            ReconciliationIssue(
                id=str(uuid.uuid4()),
                run_id=run_id,
                account_id=account.id,
                issue_key=issue_key,
                entity_type=entity_type,
                entity_key=entity_key,
                category=category,
                severity=IssueSeverity.CRITICAL,
                status=IssueStatus.OPEN,
                sticky=True,
                details=details,
                detected_at=now,
            )
        )

    def _cancel_working_attempts(self, intent: OrderIntent, account: Account) -> None:
        for leg in intent.legs:
            for attempt in self.repository.broker_orders_for_leg(leg.id):
                if attempt["status"] not in {
                    BrokerOrderStatus.WORKING.value,
                    BrokerOrderStatus.PARTIALLY_FILLED.value,
                } or not attempt["external_order_id"]:
                    continue
                try:
                    self.adapter.cancel_order(account, str(attempt["external_order_id"]))
                except Exception as exc:
                    self.repository.transition_broker_order(str(attempt["id"]), BrokerOrderStatus.UNKNOWN)
                    self._require_reconciliation(
                        intent.id,
                        account,
                        category="CANCEL_AMBIGUITY",
                        entity_type="BROKER_ORDER",
                        entity_key=str(attempt["id"]),
                        details={"message": str(exc)},
                    )

    def recover_intent(self, intent_id: str, *, account: Account) -> dict:
        """Reconstruct uncertain attempts from broker facts without resubmission."""
        intent = self._required_intent(intent_id)
        try:
            open_orders = tuple(self.adapter.get_open_orders(account))
        except Exception as exc:
            self._require_reconciliation(
                intent_id,
                account,
                category="BROKER_QUERY_FAILED",
                entity_type="ACCOUNT",
                entity_key=account.id,
                details={"message": str(exc)},
            )
            return self._required_intent(intent_id)

        for leg in intent["legs"]:
            for attempt in self.repository.broker_orders_for_leg(str(leg["id"])):
                if attempt["status"] not in {
                    BrokerOrderStatus.PREPARED.value,
                    BrokerOrderStatus.SUBMITTING.value,
                    BrokerOrderStatus.UNKNOWN.value,
                    BrokerOrderStatus.WORKING.value,
                    BrokerOrderStatus.PARTIALLY_FILLED.value,
                }:
                    continue
                if attempt["external_order_id"]:
                    matches = [item for item in open_orders if item.external_order_id == attempt["external_order_id"]]
                    if not matches:
                        try:
                            single = self.adapter.get_order(account, str(attempt["external_order_id"]))
                        except Exception as exc:
                            single = None
                            self._require_reconciliation(
                                intent_id,
                                account,
                                category="BROKER_QUERY_FAILED",
                                entity_type="BROKER_ORDER",
                                entity_key=str(attempt["id"]),
                                details={"message": str(exc)},
                            )
                        matches = [single] if single is not None else []
                else:
                    matches = [item for item in open_orders if item.client_order_id == attempt["client_order_id"]]
                if len(matches) != 1:
                    if attempt["status"] != BrokerOrderStatus.UNKNOWN.value:
                        self.repository.transition_broker_order(str(attempt["id"]), BrokerOrderStatus.UNKNOWN)
                    if leg["status"] != LegStatus.RECONCILIATION_REQUIRED.value:
                        self.repository.transition_leg(str(leg["id"]), LegStatus.RECONCILIATION_REQUIRED)
                    self._require_reconciliation(
                        intent_id,
                        account,
                        category="AMBIGUOUS_ORDER_MATCH",
                        entity_type="BROKER_ORDER",
                        entity_key=str(attempt["id"]),
                        details={"match_count": len(matches)},
                    )
                    continue
                match = matches[0]
                target = match.status
                if target not in {
                    BrokerOrderStatus.WORKING,
                    BrokerOrderStatus.PARTIALLY_FILLED,
                    BrokerOrderStatus.FILLED,
                    BrokerOrderStatus.REJECTED,
                    BrokerOrderStatus.CANCELLED,
                }:
                    target = BrokerOrderStatus.UNKNOWN
                if attempt["status"] != target.value:
                    self.repository.record_submission(
                        str(attempt["id"]),
                        status=target,
                        external_order_id=match.external_order_id,
                        metadata={"recovered": True, "snapshot_id": match.id},
                    )
                if target is BrokerOrderStatus.WORKING and leg["status"] in {
                    LegStatus.SUBMITTING.value,
                    LegStatus.RECONCILIATION_REQUIRED.value,
                }:
                    self.repository.transition_leg(str(leg["id"]), LegStatus.WORKING)
                elif target in {BrokerOrderStatus.PARTIALLY_FILLED, BrokerOrderStatus.FILLED}:
                    if leg["status"] != LegStatus.RECONCILIATION_REQUIRED.value:
                        self.repository.transition_leg(str(leg["id"]), LegStatus.RECONCILIATION_REQUIRED)
                    self._require_reconciliation(
                        intent_id,
                        account,
                        category="FILL_EVIDENCE_REQUIRED",
                        entity_type="BROKER_ORDER",
                        entity_key=str(attempt["id"]),
                        details={"broker_status": target.value},
                    )

        self._recover_fills(intent_id, account)

        recovered = self._required_intent(intent_id)
        leg_statuses = {leg["status"] for leg in recovered["legs"]}
        if leg_statuses and leg_statuses <= {LegStatus.WORKING.value, LegStatus.FILLED.value}:
            target = IntentStatus.FILLED if leg_statuses == {LegStatus.FILLED.value} else IntentStatus.WORKING
            if recovered["status"] != target.value:
                self.repository.transition_intent(intent_id, target)
        return self._required_intent(intent_id)

    def _recover_fills(self, intent_id: str, account: Account) -> None:
        """Apply broker deal facts to known attempts; never infer fills from status."""
        try:
            broker_fills = tuple(self.adapter.get_fills(account, since=None))
        except Exception as exc:
            self._require_reconciliation(
                intent_id,
                account,
                category="BROKER_QUERY_FAILED",
                entity_type="ACCOUNT",
                entity_key=f"{account.id}:fills",
                details={"message": str(exc)},
            )
            return

        intent = self._required_intent(intent_id)
        attempts = [
            attempt
            for leg in intent["legs"]
            for attempt in self.repository.broker_orders_for_leg(str(leg["id"]))
        ]
        for broker_fill in broker_fills:
            if not isinstance(broker_fill, BrokerFill):
                raise TypeError("adapter get_fills() must return BrokerFill values")
            matches = [
                attempt
                for attempt in attempts
                if attempt["external_order_id"] == broker_fill.external_order_id
            ]
            if len(matches) != 1:
                self._require_reconciliation(
                    intent_id,
                    account,
                    category="AMBIGUOUS_FILL_MATCH",
                    entity_type="BROKER_FILL",
                    entity_key=broker_fill.dedupe_key,
                    details={
                        "external_order_id": broker_fill.external_order_id,
                        "match_count": len(matches),
                    },
                )
                continue
            attempt = matches[0]
            self.repository.record_fill(
                Fill(
                    id=str(
                        uuid.uuid5(
                            uuid.NAMESPACE_URL,
                            f"{account.id}:{broker_fill.external_order_id}:{broker_fill.dedupe_key}",
                        )
                    ),
                    broker_order_id=str(attempt["id"]),
                    order_leg_id=str(attempt["order_leg_id"]),
                    external_fill_id=broker_fill.external_fill_id,
                    dedupe_key=broker_fill.dedupe_key,
                    quantity=broker_fill.quantity,
                    price=broker_fill.price,
                    fee=broker_fill.fee,
                    fee_currency=broker_fill.fee_currency,
                    filled_at=broker_fill.filled_at,
                    received_at=broker_fill.received_at,
                    metadata=broker_fill.metadata,
                )
            )

    def apply_fill(self, fill) -> bool:
        return self.repository.record_fill(fill)

    def complete_intent(self, intent_id: str) -> dict:
        intent = self._required_intent(intent_id)
        if intent["status"] != IntentStatus.FILLED.value:
            raise OMSExecutionError("only a fully filled intent can be completed")
        self.repository.transition_intent(intent_id, IntentStatus.COMPLETED)
        return self._required_intent(intent_id)

    def _required_intent(self, intent_id: str) -> dict:
        intent = self.repository.get_intent(intent_id)
        if intent is None:
            raise KeyError(f"Unknown intent: {intent_id}")
        return intent
