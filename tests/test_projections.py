"""
The Ledger — Phase 3: Projection Tests

Tests cover:
  1. ApplicationSummary — happy path, idempotency, state transitions
  2. AgentPerformanceLedger — running averages, counts
  3. ComplianceAuditView — temporal queries, snapshots, rebuild
  4. ProjectionDaemon — routing, checkpoints, fault tolerance, lag
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from in_memory_store import InMemoryEventStore
from models.events import (
    ApplicationApproved,
    ApplicationDeclined,
    ApplicationSubmitted,
    BaseEvent,
    ComplianceCheckCompleted,
    ComplianceCheckInitiated,
    ComplianceRuleFailed,
    ComplianceRuleNoted,
    ComplianceRulePassed,
    CreditAnalysisCompleted,
    DecisionGenerated,
    FraudScreeningCompleted,
    HumanReviewCompleted,
)
from projections.agent_performance import AgentPerformanceLedgerProjection
from projections.application_summary import ApplicationSummaryProjection
from projections.base import InMemoryProjectionStore
from projections.compliance_audit import ComplianceAuditViewProjection
from projections.daemon import ProjectionDaemon


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def store():
    return InMemoryEventStore()


@pytest.fixture
def proj_store():
    return InMemoryProjectionStore()


@pytest.fixture
def app_summary(proj_store):
    return ApplicationSummaryProjection(store=proj_store)


@pytest.fixture
def agent_perf(proj_store):
    return AgentPerformanceLedgerProjection(store=proj_store)


@pytest.fixture
def compliance_view():
    # Give compliance its own store so temporal queries don't collide
    return ComplianceAuditViewProjection(store=InMemoryProjectionStore())


# =============================================================================
# Helpers
# =============================================================================


async def _seed_loan(store: InMemoryEventStore, app_id: str = "LOAN-001"):
    """Submit a loan application and return the stream version."""
    return await store.append(
        stream_id=f"loan-{app_id}",
        events=[
            ApplicationSubmitted(
                payload={
                    "application_id": app_id,
                    "applicant_id": "COMP-001",
                    "requested_amount_usd": 100000.0,
                    "loan_purpose": "expansion",
                    "submission_channel": "api",
                    "submitted_at": datetime.now(timezone.utc).isoformat(),
                }
            )
        ],
        expected_version=-1,
    )


async def _add_credit_analysis(
    store: InMemoryEventStore,
    app_id: str = "LOAN-001",
    confidence: float = 0.85,
    risk_tier: str = "MEDIUM",
    version: int = 1,
):
    """Append CreditAnalysisCompleted to loan stream."""
    return await store.append(
        stream_id=f"loan-{app_id}",
        events=[
            CreditAnalysisCompleted(
                payload={
                    "application_id": app_id,
                    "agent_id": "credit-agent-001",
                    "session_id": str(uuid.uuid4()),
                    "model_version": "v2.3",
                    "confidence_score": confidence,
                    "risk_tier": risk_tier,
                    "recommended_limit_usd": 80000.0,
                    "analysis_duration_ms": 1500,
                    "input_data_hash": "abc123",
                }
            )
        ],
        expected_version=version,
    )


async def _add_fraud_screening(
    store: InMemoryEventStore,
    app_id: str = "LOAN-001",
    fraud_score: float = 0.15,
    version: int = 2,
):
    """Append FraudScreeningCompleted to loan stream."""
    return await store.append(
        stream_id=f"loan-{app_id}",
        events=[
            FraudScreeningCompleted(
                payload={
                    "application_id": app_id,
                    "agent_id": "fraud-agent-001",
                    "session_id": str(uuid.uuid4()),
                    "fraud_score": fraud_score,
                    "anomaly_flags": [],
                    "screening_model_version": "v1.0",
                    "input_data_hash": "def456",
                }
            )
        ],
        expected_version=version,
    )


async def _add_compliance_events(
    store: InMemoryEventStore,
    app_id: str = "LOAN-001",
    overall_verdict: str = "CLEAR",
    has_hard_block: bool = False,
):
    """Append full compliance lifecycle to compliance stream."""
    stream_id = f"compliance-{app_id}"
    v = await store.append(
        stream_id=stream_id,
        events=[
            ComplianceCheckInitiated(
                payload={
                    "application_id": app_id,
                    "regulation_set_version": "2026-Q1",
                    "checks_required": ["REG-001", "REG-002", "REG-003"],
                }
            )
        ],
        expected_version=-1,
    )
    v = await store.append(
        stream_id=stream_id,
        events=[
            ComplianceRulePassed(
                payload={
                    "application_id": app_id,
                    "rule_id": "REG-001",
                    "rule_version": "v1.0",
                    "evaluation_timestamp": datetime.now(timezone.utc).isoformat(),
                    "evidence_hash": "hash001",
                }
            )
        ],
        expected_version=v,
    )
    if has_hard_block:
        v = await store.append(
            stream_id=stream_id,
            events=[
                ComplianceRuleFailed(
                    payload={
                        "application_id": app_id,
                        "rule_id": "REG-003",
                        "rule_version": "v1.0",
                        "failure_reason": "Montana jurisdiction excluded",
                        "remediation_required": False,
                        "is_hard_block": True,
                    }
                )
            ],
            expected_version=v,
        )
    else:
        v = await store.append(
            stream_id=stream_id,
            events=[
                ComplianceRulePassed(
                    payload={
                        "application_id": app_id,
                        "rule_id": "REG-002",
                        "rule_version": "v1.0",
                        "evaluation_timestamp": datetime.now(timezone.utc).isoformat(),
                        "evidence_hash": "hash002",
                    }
                )
            ],
            expected_version=v,
        )
    v = await store.append(
        stream_id=stream_id,
        events=[
            ComplianceCheckCompleted(
                payload={
                    "application_id": app_id,
                    "overall_verdict": overall_verdict,
                    "checks_completed": 3 if not has_hard_block else 2,
                }
            )
        ],
        expected_version=v,
    )
    return v


# =============================================================================
# 1. ApplicationSummary Tests
# =============================================================================


class TestApplicationSummary:
    """Tests for the ApplicationSummary projection."""

    async def test_happy_path_full_lifecycle(self, store, app_summary):
        """Feed full lifecycle → verify all columns."""
        app_id = "LOAN-HAPPY"
        await _seed_loan(store, app_id)
        await _add_credit_analysis(store, app_id, confidence=0.85, version=1)
        await _add_fraud_screening(store, app_id, fraud_score=0.15, version=2)

        # Process all events through daemon
        daemon = ProjectionDaemon(store, [app_summary])
        await daemon.run_once()

        summary = app_summary.get_summary(app_id)
        assert summary is not None
        assert summary["application_id"] == app_id
        assert summary["state"] == "ANALYSIS_COMPLETE"
        assert summary["applicant_id"] == "COMP-001"
        assert summary["requested_amount_usd"] == 100000.0
        assert summary["risk_tier"] == "MEDIUM"
        assert summary["fraud_score"] == 0.15

    async def test_submitted_state(self, store, app_summary):
        """ApplicationSubmitted → state should be SUBMITTED."""
        await _seed_loan(store, "LOAN-SUB")
        daemon = ProjectionDaemon(store, [app_summary])
        await daemon.run_once()

        summary = app_summary.get_summary("LOAN-SUB")
        assert summary["state"] == "SUBMITTED"

    async def test_approved_state(self, store, app_summary):
        """Full path to FINAL_APPROVED."""
        app_id = "LOAN-APPROVED"
        await _seed_loan(store, app_id)
        await store.append(
            stream_id=f"loan-{app_id}",
            events=[
                ApplicationApproved(
                    payload={
                        "application_id": app_id,
                        "approved_amount_usd": 75000.0,
                        "interest_rate": 5.5,
                        "conditions": ["Monthly reporting"],
                        "approved_by": "auto",
                        "effective_date": "2026-01-15",
                    }
                )
            ],
            expected_version=1,
        )

        daemon = ProjectionDaemon(store, [app_summary])
        await daemon.run_once()

        summary = app_summary.get_summary(app_id)
        assert summary["state"] == "FINAL_APPROVED"
        assert summary["approved_amount_usd"] == 75000.0

    async def test_declined_state(self, store, app_summary):
        """Path to FINAL_DECLINED."""
        app_id = "LOAN-DECLINED"
        await _seed_loan(store, app_id)
        await store.append(
            stream_id=f"loan-{app_id}",
            events=[
                ApplicationDeclined(
                    payload={
                        "application_id": app_id,
                        "decline_reasons": ["High risk"],
                        "declined_by": "auto",
                        "adverse_action_notice_required": True,
                    }
                )
            ],
            expected_version=1,
        )

        daemon = ProjectionDaemon(store, [app_summary])
        await daemon.run_once()

        summary = app_summary.get_summary(app_id)
        assert summary["state"] == "FINAL_DECLINED"

    async def test_idempotent_processing(self, store, app_summary):
        """Process same events twice → no duplicates, same state."""
        await _seed_loan(store, "LOAN-IDEM")

        daemon = ProjectionDaemon(store, [app_summary])
        await daemon.run_once()
        state1 = app_summary.get_summary("LOAN-IDEM")

        # Reset checkpoint and process again
        app_summary._checkpoint = 0
        await daemon.run_once()
        state2 = app_summary.get_summary("LOAN-IDEM")

        assert state1["state"] == state2["state"]
        assert state1["applicant_id"] == state2["applicant_id"]
        assert app_summary.count() == 1  # Still one row

    async def test_decision_generated_states(self, store, app_summary):
        """DecisionGenerated with APPROVE → APPROVED_PENDING_HUMAN."""
        app_id = "LOAN-DEC"
        await _seed_loan(store, app_id)
        await store.append(
            stream_id=f"loan-{app_id}",
            events=[
                DecisionGenerated(
                    payload={
                        "application_id": app_id,
                        "orchestrator_agent_id": "orch-001",
                        "recommendation": "APPROVE",
                        "confidence_score": 0.9,
                        "contributing_agent_sessions": [],
                        "decision_basis_summary": "Strong financials",
                        "model_versions": {"credit": "v2.3"},
                    }
                )
            ],
            expected_version=1,
        )

        daemon = ProjectionDaemon(store, [app_summary])
        await daemon.run_once()

        summary = app_summary.get_summary(app_id)
        assert summary["state"] == "APPROVED_PENDING_HUMAN"
        assert summary["decision"] == "APPROVE"

    async def test_rebuild_from_scratch(self, store, app_summary):
        """Rebuild produces identical state to incremental."""
        await _seed_loan(store, "LOAN-REB")
        await _add_credit_analysis(store, "LOAN-REB", version=1)

        daemon = ProjectionDaemon(store, [app_summary])
        await daemon.run_once()
        state_before = app_summary.get_summary("LOAN-REB")

        await app_summary.rebuild_from_scratch()
        assert app_summary.count() == 0

        await daemon.run_once()
        state_after = app_summary.get_summary("LOAN-REB")

        assert state_before["state"] == state_after["state"]
        assert state_before["risk_tier"] == state_after["risk_tier"]


# =============================================================================
# 2. AgentPerformanceLedger Tests
# =============================================================================


class TestAgentPerformanceLedger:
    """Tests for the AgentPerformanceLedger projection."""

    async def test_credit_analysis_metrics(self, store, agent_perf):
        """Three credit analyses → correct avg confidence and count."""
        for i, conf in enumerate([0.80, 0.90, 0.70]):
            app_id = f"LOAN-PERF-{i}"
            await _seed_loan(store, app_id)
            await _add_credit_analysis(
                store, app_id, confidence=conf, version=1
            )

        daemon = ProjectionDaemon(store, [agent_perf])
        await daemon.run_once()

        perf = agent_perf.get_performance("credit-agent-001", "v2.3")
        assert perf is not None
        assert perf["analyses_completed"] == 3
        assert abs(perf["avg_confidence_score"] - 0.80) < 0.01  # (0.8+0.9+0.7)/3

    async def test_fraud_screening_metrics(self, store, agent_perf):
        """Fraud screening updates analyses_completed."""
        await _seed_loan(store, "LOAN-FR")
        await _add_fraud_screening(store, "LOAN-FR", version=1)

        daemon = ProjectionDaemon(store, [agent_perf])
        await daemon.run_once()

        perf = agent_perf.get_performance("fraud-agent-001", "v1.0")
        assert perf is not None
        assert perf["analyses_completed"] == 1

    async def test_decision_categorisation(self, store, agent_perf):
        """DecisionGenerated categorises by recommendation."""
        for i, rec in enumerate(["APPROVE", "DECLINE", "REFER"]):
            app_id = f"LOAN-CAT-{i}"
            await _seed_loan(store, app_id)
            await store.append(
                stream_id=f"loan-{app_id}",
                events=[
                    DecisionGenerated(
                        payload={
                            "application_id": app_id,
                            "orchestrator_agent_id": "orch-001",
                            "recommendation": rec,
                            "confidence_score": 0.8,
                            "contributing_agent_sessions": [],
                            "decision_basis_summary": "test",
                            "model_versions": {"credit": "v2.3"},
                        }
                    )
                ],
                expected_version=1,
            )

        daemon = ProjectionDaemon(store, [agent_perf])
        await daemon.run_once()

        perf = agent_perf.get_performance("orch-001", "v2.3")
        assert perf is not None
        assert perf["decisions_generated"] == 3
        assert perf["approve_count"] == 1
        assert perf["decline_count"] == 1
        assert perf["refer_count"] == 1

    async def test_human_override_tracking(self, store, agent_perf):
        """HumanReviewCompleted with override=True increments override count."""
        await _seed_loan(store, "LOAN-OVERRIDE")
        await store.append(
            stream_id="loan-LOAN-OVERRIDE",
            events=[
                HumanReviewCompleted(
                    payload={
                        "application_id": "LOAN-OVERRIDE",
                        "reviewer_id": "LO-Sarah",
                        "override": True,
                        "final_decision": "APPROVE",
                        "override_reason": "Longstanding customer",
                    }
                )
            ],
            expected_version=1,
        )

        daemon = ProjectionDaemon(store, [agent_perf])
        await daemon.run_once()

        perf = agent_perf.get_performance("human_review", "manual")
        assert perf is not None
        assert perf["human_override_count"] == 1


# =============================================================================
# 3. ComplianceAuditView Tests
# =============================================================================


class TestComplianceAuditView:
    """Tests for the ComplianceAuditView projection with temporal queries."""

    async def test_current_compliance_clear(self, store, compliance_view):
        """Full compliance lifecycle → overall_verdict CLEAR."""
        app_id = "LOAN-COMP-CLEAR"
        await _add_compliance_events(store, app_id, overall_verdict="CLEAR")

        daemon = ProjectionDaemon(store, [compliance_view])
        await daemon.run_once()

        result = compliance_view.get_current_compliance(app_id)
        assert result is not None
        assert result["overall_verdict"] == "CLEAR"
        assert result["has_hard_block"] is False
        assert len(result["rules"]) == 2  # REG-001, REG-002

    async def test_hard_block_compliance(self, store, compliance_view):
        """Compliance with hard block → has_hard_block True."""
        app_id = "LOAN-COMP-BLOCK"
        await _add_compliance_events(
            store, app_id, overall_verdict="BLOCKED", has_hard_block=True
        )

        daemon = ProjectionDaemon(store, [compliance_view])
        await daemon.run_once()

        result = compliance_view.get_current_compliance(app_id)
        assert result is not None
        assert result["overall_verdict"] == "BLOCKED"
        assert result["has_hard_block"] is True

    async def test_snapshot_created_on_completed(self, store, compliance_view):
        """ComplianceCheckCompleted → snapshot row created."""
        app_id = "LOAN-SNAP"
        await _add_compliance_events(store, app_id, overall_verdict="CLEAR")

        daemon = ProjectionDaemon(store, [compliance_view])
        await daemon.run_once()

        # Check snapshots exist
        snapshots = compliance_view._store.get_all("compliance_snapshots")
        assert len(snapshots) >= 1
        snap = snapshots[0]
        assert snap["application_id"] == app_id
        assert snap["overall_verdict"] == "CLEAR"

    async def test_temporal_query_returns_point_in_time(
        self, store, compliance_view
    ):
        """
        Compliance events at T1, more events at T2.
        get_compliance_at(T1) returns only T1 state.
        """
        app_id = "LOAN-TEMPORAL"
        # Create compliance events (they get timestamps)
        await _add_compliance_events(store, app_id, overall_verdict="CLEAR")

        daemon = ProjectionDaemon(store, [compliance_view])
        await daemon.run_once()

        # Query at a future time → should still see the data
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        result = compliance_view.get_compliance_at(app_id, future)
        assert result is not None
        assert result["overall_verdict"] == "CLEAR"

        # Query at epoch (before anything) → should return None
        past = datetime(2020, 1, 1, tzinfo=timezone.utc)
        result_past = compliance_view.get_compliance_at(app_id, past)
        assert result_past is None

    async def test_rebuild_produces_identical_state(
        self, store, compliance_view
    ):
        """Rebuild from scratch produces identical state to incremental."""
        app_id = "LOAN-REBUILD"
        await _add_compliance_events(store, app_id, overall_verdict="CLEAR")

        daemon = ProjectionDaemon(store, [compliance_view])
        await daemon.run_once()
        state_before = compliance_view.get_current_compliance(app_id)

        # Rebuild
        await compliance_view.rebuild_from_scratch()
        assert compliance_view.get_current_compliance(app_id) is None

        # Replay
        await daemon.run_once()
        state_after = compliance_view.get_current_compliance(app_id)

        assert state_before["overall_verdict"] == state_after["overall_verdict"]
        assert state_before["has_hard_block"] == state_after["has_hard_block"]
        assert len(state_before["rules"]) == len(state_after["rules"])

    async def test_idempotent_insert(self, store, compliance_view):
        """Processing same events twice → no duplicate rows."""
        app_id = "LOAN-IDEM-COMP"
        await _add_compliance_events(store, app_id, overall_verdict="CLEAR")

        daemon = ProjectionDaemon(store, [compliance_view])
        await daemon.run_once()
        count1 = compliance_view.get_current_compliance(app_id)["total_events"]

        # Reset checkpoint and reprocess
        compliance_view._checkpoint = 0
        await daemon.run_once()
        count2 = compliance_view.get_current_compliance(app_id)["total_events"]

        assert count1 == count2  # No duplicates


# =============================================================================
# 4. ProjectionDaemon Tests
# =============================================================================


class TestProjectionDaemon:
    """Tests for the ProjectionDaemon routing, checkpointing, and fault tolerance."""

    async def test_routes_to_correct_projections(self, store):
        """An event only reaches projections that subscribe to it."""
        app_summary = ApplicationSummaryProjection()
        compliance_view = ComplianceAuditViewProjection()

        await _seed_loan(store, "LOAN-ROUTE")
        daemon = ProjectionDaemon(store, [app_summary, compliance_view])
        await daemon.run_once()

        # ApplicationSubmitted should reach app_summary, not compliance
        assert app_summary.get_summary("LOAN-ROUTE") is not None
        assert compliance_view.get_current_compliance("LOAN-ROUTE") is None

    async def test_skips_already_processed_events(self, store):
        """Events below checkpoint are skipped."""
        app_summary = ApplicationSummaryProjection()
        await _seed_loan(store, "LOAN-SKIP")

        daemon = ProjectionDaemon(store, [app_summary])
        await daemon.run_once()
        assert app_summary.get_summary("LOAN-SKIP") is not None

        # Add another event
        await store.append(
            stream_id="loan-LOAN-SKIP",
            events=[
                ApplicationApproved(
                    payload={
                        "application_id": "LOAN-SKIP",
                        "approved_amount_usd": 50000.0,
                        "interest_rate": 5.0,
                        "conditions": [],
                        "approved_by": "auto",
                        "effective_date": "2026-01-01",
                    }
                )
            ],
            expected_version=1,
        )

        await daemon.run_once()
        summary = app_summary.get_summary("LOAN-SKIP")
        assert summary["state"] == "FINAL_APPROVED"

    async def test_fault_tolerance_skips_bad_event(self, store):
        """Projection throws → daemon retries then skips, continues."""

        class FailingProjection(ApplicationSummaryProjection):
            def __init__(self):
                super().__init__()
                self._fail_count = 0

            async def handle(self, event):
                self._fail_count += 1
                if self._fail_count <= 4:
                    raise ValueError("Simulated failure")
                await super().handle(event)

        failing = FailingProjection()
        await _seed_loan(store, "LOAN-FAIL")

        # Daemon with max_retries=3 → should skip after 3 retries
        daemon = ProjectionDaemon(store, [failing], max_retries=3)

        # Process multiple times to exhaust retries
        for _ in range(4):
            await daemon.run_once()

        # The event was skipped — checkpoint advanced past it
        checkpoint = await failing.get_checkpoint()
        assert checkpoint > 0

    async def test_lag_metric(self, store):
        """Feed events, check lag reports non-zero during processing."""
        app_summary = ApplicationSummaryProjection()

        # Add events without processing
        await _seed_loan(store, "LOAN-LAG-1")
        await _seed_loan(store, "LOAN-LAG-2")
        await _seed_loan(store, "LOAN-LAG-3")

        daemon = ProjectionDaemon(store, [app_summary])

        # Before processing: lag should be 0 (daemon hasn't seen events yet)
        lags = await daemon.get_all_lags()
        assert lags["application_summary"]["lag_events"] == 0

        # Process
        await daemon.run_once()

        # After processing: lag should be 0 (caught up)
        lags = await daemon.get_all_lags()
        assert lags["application_summary"]["lag_events"] == 0

    async def test_multiple_projections_independent_checkpoints(self, store):
        """Each projection has its own checkpoint."""
        app_summary = ApplicationSummaryProjection()
        agent_perf = AgentPerformanceLedgerProjection()

        await _seed_loan(store, "LOAN-MULTI")
        await _add_credit_analysis(store, "LOAN-MULTI", version=1)

        daemon = ProjectionDaemon(store, [app_summary, agent_perf])
        await daemon.run_once()

        cp1 = await app_summary.get_checkpoint()
        cp2 = await agent_perf.get_checkpoint()

        # Both should have processed up to the same point
        assert cp1 > 0
        assert cp2 > 0
        assert cp1 == cp2  # Both should be at the latest event

    async def test_projection_lag_under_load(self, store):
        """Feed 50 events rapidly → assert processing completes."""
        app_summary = ApplicationSummaryProjection()

        # Create 50 loan applications
        for i in range(50):
            await _seed_loan(store, f"LOAN-LOAD-{i}")

        daemon = ProjectionDaemon(store, [app_summary])
        processed = await daemon.run_once()

        assert processed == 50
        assert app_summary.count() == 50

        lags = await daemon.get_all_lags()
        assert lags["application_summary"]["lag_events"] == 0
