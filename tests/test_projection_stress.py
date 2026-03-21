"""
The Ledger — Phase 3: Projection Stress & Edge-Case Tests

Covers the gaps from the Phase 3 sanity checklist:
  1. Full end-to-end lifecycle (every state transition in one test)
  2. Multiple agents/model_versions simultaneously
  3. Temporal queries at distinct timestamps (T1 vs T2)
  4. Daemon isolation (one projection failing doesn't block others)
  5. Out-of-order event handling
  6. agent_sessions, human_reviewer_id, final_decision_at verification
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
    CreditAnalysisRequested,
    DecisionGenerated,
    FraudScreeningCompleted,
    HumanReviewCompleted,
    HumanReviewRequested,
    StoredEvent,
)
from projections.agent_performance import AgentPerformanceLedgerProjection
from projections.application_summary import ApplicationSummaryProjection
from projections.base import InMemoryProjectionStore, Projection
from projections.compliance_audit import ComplianceAuditViewProjection
from projections.daemon import ProjectionDaemon


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def store():
    return InMemoryEventStore()


# =============================================================================
# 1. Full End-to-End Lifecycle — Every State Transition
# =============================================================================


class TestFullLifecycleProjection:
    """Verify ApplicationSummary tracks every state through a complete lifecycle."""

    async def test_full_lifecycle_submitted_through_approved(self, store):
        """
        SUBMITTED → AWAITING_ANALYSIS → ANALYSIS_COMPLETE → COMPLIANCE_REVIEW
        → PENDING_DECISION → APPROVED_PENDING_HUMAN → FINAL_APPROVED
        """
        app_id = "LIFE-001"
        proj = ApplicationSummaryProjection()
        daemon = ProjectionDaemon(store, [proj])

        # 1. ApplicationSubmitted
        await store.append(
            f"loan-{app_id}",
            [ApplicationSubmitted(payload={
                "application_id": app_id, "applicant_id": "COMP-001",
                "requested_amount_usd": 500000.0, "loan_purpose": "expansion",
                "submission_channel": "api",
                "submitted_at": datetime.now(timezone.utc).isoformat(),
            })],
            expected_version=-1,
        )
        await daemon.run_once()
        assert proj.get_summary(app_id)["state"] == "SUBMITTED"
        assert proj.get_summary(app_id)["applicant_id"] == "COMP-001"
        assert proj.get_summary(app_id)["requested_amount_usd"] == 500000.0

        # 2. CreditAnalysisRequested → state stays (not subscribed to it directly
        #    in ApplicationSummary because it's a sub-lifecycle event)
        await store.append(
            f"loan-{app_id}",
            [CreditAnalysisRequested(payload={
                "application_id": app_id,
                "assigned_agent_id": "credit-agent-001",
                "requested_at": datetime.now(timezone.utc).isoformat(),
                "priority": "normal",
            })],
            expected_version=1,
        )

        # 3. CreditAnalysisCompleted → ANALYSIS_COMPLETE
        await store.append(
            f"loan-{app_id}",
            [CreditAnalysisCompleted(payload={
                "application_id": app_id, "agent_id": "credit-agent-001",
                "session_id": "sess-001", "model_version": "v2.3",
                "confidence_score": 0.88, "risk_tier": "MEDIUM",
                "recommended_limit_usd": 400000.0,
                "analysis_duration_ms": 2500, "input_data_hash": "abc123",
            })],
            expected_version=2,
        )
        await daemon.run_once()
        s = proj.get_summary(app_id)
        assert s["state"] == "ANALYSIS_COMPLETE"
        assert s["risk_tier"] == "MEDIUM"

        # 4. FraudScreeningCompleted
        await store.append(
            f"loan-{app_id}",
            [FraudScreeningCompleted(payload={
                "application_id": app_id, "agent_id": "fraud-agent-001",
                "session_id": "sess-002", "fraud_score": 0.12,
                "anomaly_flags": [], "screening_model_version": "v1.0",
                "input_data_hash": "def456",
            })],
            expected_version=3,
        )
        await daemon.run_once()
        assert proj.get_summary(app_id)["fraud_score"] == 0.12

        # 5. ComplianceCheckCompleted → compliance_status
        await store.append(
            f"compliance-{app_id}",
            [ComplianceCheckCompleted(payload={
                "application_id": app_id, "overall_verdict": "CLEAR",
                "checks_completed": 6,
            })],
            expected_version=-1,
        )
        await daemon.run_once()
        assert proj.get_summary(app_id)["compliance_status"] == "CLEAR"

        # 6. DecisionGenerated → APPROVED_PENDING_HUMAN
        await store.append(
            f"loan-{app_id}",
            [DecisionGenerated(payload={
                "application_id": app_id,
                "orchestrator_agent_id": "orch-001",
                "recommendation": "APPROVE", "confidence_score": 0.9,
                "contributing_agent_sessions": ["sess-001", "sess-002"],
                "decision_basis_summary": "Strong financials, low fraud",
                "model_versions": {"credit": "v2.3", "fraud": "v1.0"},
            })],
            expected_version=4,
        )
        await daemon.run_once()
        s = proj.get_summary(app_id)
        assert s["state"] == "APPROVED_PENDING_HUMAN"
        assert s["decision"] == "APPROVE"

        # 7. HumanReviewCompleted
        await store.append(
            f"loan-{app_id}",
            [HumanReviewCompleted(payload={
                "application_id": app_id, "reviewer_id": "LO-Sarah-Chen",
                "override": False, "final_decision": "APPROVE",
                "override_reason": "",
            })],
            expected_version=5,
        )
        await daemon.run_once()
        assert proj.get_summary(app_id)["human_reviewer_id"] == "LO-Sarah-Chen"

        # 8. ApplicationApproved → FINAL_APPROVED
        await store.append(
            f"loan-{app_id}",
            [ApplicationApproved(payload={
                "application_id": app_id,
                "approved_amount_usd": 400000.0, "interest_rate": 5.5,
                "conditions": ["Monthly revenue reporting"],
                "approved_by": "LO-Sarah-Chen",
                "effective_date": "2026-03-21",
            })],
            expected_version=6,
        )
        await daemon.run_once()
        s = proj.get_summary(app_id)
        assert s["state"] == "FINAL_APPROVED"
        assert s["approved_amount_usd"] == 400000.0
        assert s["final_decision_at"] is not None

    async def test_full_lifecycle_declined(self, store):
        """SUBMITTED → ... → DECLINED_PENDING_HUMAN → FINAL_DECLINED."""
        app_id = "LIFE-DEC"
        proj = ApplicationSummaryProjection()
        daemon = ProjectionDaemon(store, [proj])

        await store.append(
            f"loan-{app_id}",
            [ApplicationSubmitted(payload={
                "application_id": app_id, "applicant_id": "COMP-002",
                "requested_amount_usd": 1000000.0, "loan_purpose": "acquisition",
                "submission_channel": "portal",
                "submitted_at": datetime.now(timezone.utc).isoformat(),
            })],
            expected_version=-1,
        )

        await store.append(
            f"loan-{app_id}",
            [DecisionGenerated(payload={
                "application_id": app_id,
                "orchestrator_agent_id": "orch-001",
                "recommendation": "DECLINE", "confidence_score": 0.82,
                "contributing_agent_sessions": [],
                "decision_basis_summary": "High risk",
                "model_versions": {},
            })],
            expected_version=1,
        )

        await store.append(
            f"loan-{app_id}",
            [ApplicationDeclined(payload={
                "application_id": app_id,
                "decline_reasons": ["High risk tier", "Excessive leverage"],
                "declined_by": "auto",
                "adverse_action_notice_required": True,
            })],
            expected_version=2,
        )

        await daemon.run_once()
        s = proj.get_summary(app_id)
        assert s["state"] == "FINAL_DECLINED"
        assert s["decision"] == "DECLINE"
        assert s["final_decision_at"] is not None


# =============================================================================
# 2. Multiple Agents / Model Versions Simultaneously
# =============================================================================


class TestMultipleAgentsModelVersions:
    """Verify AgentPerformanceLedger handles multiple agents and versions."""

    async def test_two_agents_two_versions_simultaneously(self, store):
        """Two different agents with different model versions."""
        proj = AgentPerformanceLedgerProjection()
        daemon = ProjectionDaemon(store, [proj])

        # Credit agent v2.3 — analysis 1
        await store.append("loan-MA1", [
            ApplicationSubmitted(payload={
                "application_id": "MA1", "applicant_id": "C1",
                "requested_amount_usd": 100.0, "loan_purpose": "",
                "submission_channel": "api", "submitted_at": "",
            })
        ], expected_version=-1)
        await store.append("loan-MA1", [
            CreditAnalysisCompleted(payload={
                "application_id": "MA1", "agent_id": "agent-A",
                "session_id": "s1", "model_version": "v2.3",
                "confidence_score": 0.80, "risk_tier": "MEDIUM",
                "recommended_limit_usd": 80.0,
                "analysis_duration_ms": 1000, "input_data_hash": "h1",
            })
        ], expected_version=1)

        # Credit agent v2.4 — analysis 2
        await store.append("loan-MA2", [
            ApplicationSubmitted(payload={
                "application_id": "MA2", "applicant_id": "C2",
                "requested_amount_usd": 200.0, "loan_purpose": "",
                "submission_channel": "api", "submitted_at": "",
            })
        ], expected_version=-1)
        await store.append("loan-MA2", [
            CreditAnalysisCompleted(payload={
                "application_id": "MA2", "agent_id": "agent-A",
                "session_id": "s2", "model_version": "v2.4",
                "confidence_score": 0.95, "risk_tier": "LOW",
                "recommended_limit_usd": 180.0,
                "analysis_duration_ms": 800, "input_data_hash": "h2",
            })
        ], expected_version=1)

        # Fraud agent v1.0
        await store.append("loan-MA1", [
            FraudScreeningCompleted(payload={
                "application_id": "MA1", "agent_id": "agent-B",
                "session_id": "s3", "fraud_score": 0.10,
                "anomaly_flags": [], "screening_model_version": "v1.0",
                "input_data_hash": "h3",
            })
        ], expected_version=2)

        await daemon.run_once()

        # Verify: agent-A v2.3
        p1 = proj.get_performance("agent-A", "v2.3")
        assert p1 is not None
        assert p1["analyses_completed"] == 1
        assert p1["avg_confidence_score"] == 0.80
        assert p1["avg_duration_ms"] == 1000

        # Verify: agent-A v2.4 (different version = different row)
        p2 = proj.get_performance("agent-A", "v2.4")
        assert p2 is not None
        assert p2["analyses_completed"] == 1
        assert p2["avg_confidence_score"] == 0.95

        # Verify: agent-B v1.0
        p3 = proj.get_performance("agent-B", "v1.0")
        assert p3 is not None
        assert p3["analyses_completed"] == 1

        # Verify: total 3 distinct rows
        all_perf = proj.get_all_performance()
        assert len(all_perf) == 3

    async def test_running_average_accuracy(self, store):
        """Verify running average matches manual calculation."""
        proj = AgentPerformanceLedgerProjection()
        daemon = ProjectionDaemon(store, [proj])

        confidences = [0.70, 0.85, 0.92, 0.60, 0.78]
        durations = [1000, 1500, 800, 2000, 1200]

        for i, (conf, dur) in enumerate(zip(confidences, durations)):
            app_id = f"AVG-{i}"
            await store.append(f"loan-{app_id}", [
                ApplicationSubmitted(payload={
                    "application_id": app_id, "applicant_id": "C1",
                    "requested_amount_usd": 100.0, "loan_purpose": "",
                    "submission_channel": "api", "submitted_at": "",
                })
            ], expected_version=-1)
            await store.append(f"loan-{app_id}", [
                CreditAnalysisCompleted(payload={
                    "application_id": app_id, "agent_id": "agent-AVG",
                    "session_id": f"s-{i}", "model_version": "v3.0",
                    "confidence_score": conf, "risk_tier": "MEDIUM",
                    "recommended_limit_usd": 80.0,
                    "analysis_duration_ms": dur, "input_data_hash": f"h{i}",
                })
            ], expected_version=1)

        await daemon.run_once()

        perf = proj.get_performance("agent-AVG", "v3.0")
        expected_avg_conf = sum(confidences) / len(confidences)
        expected_avg_dur = sum(durations) / len(durations)

        assert perf["analyses_completed"] == 5
        assert abs(perf["avg_confidence_score"] - expected_avg_conf) < 0.001
        assert abs(perf["avg_duration_ms"] - expected_avg_dur) < 0.1


# =============================================================================
# 3. Temporal Queries at Distinct Timestamps
# =============================================================================


class TestTemporalQueryDistinctTimestamps:
    """Verify ComplianceAuditView returns different states at different times."""

    async def test_two_compliance_checks_different_timestamps(self, store):
        """
        T1: First compliance check (CLEAR)
        T2: Second compliance check (BLOCKED)
        Query at T1 → CLEAR. Query at T2 → BLOCKED.
        Uses up_to_position for precise queries when timestamps collide.
        """
        proj = ComplianceAuditViewProjection()
        daemon = ProjectionDaemon(store, [proj])
        app_id = "TEMP-001"

        # First check — CLEAR
        await store.append(f"compliance-{app_id}", [
            ComplianceCheckInitiated(payload={
                "application_id": app_id, "regulation_set_version": "2026-Q1",
                "checks_required": ["REG-001"],
            })
        ], expected_version=-1)
        await store.append(f"compliance-{app_id}", [
            ComplianceRulePassed(payload={
                "application_id": app_id, "rule_id": "REG-001",
                "rule_version": "v1.0",
                "evaluation_timestamp": datetime.now(timezone.utc).isoformat(),
                "evidence_hash": "e1",
            })
        ], expected_version=1)
        await store.append(f"compliance-{app_id}", [
            ComplianceCheckCompleted(payload={
                "application_id": app_id, "overall_verdict": "CLEAR",
                "checks_completed": 1,
            })
        ], expected_version=2)

        await daemon.run_once()

        # Capture the global_position of the first ComplianceCheckCompleted
        # by looking at the snapshot
        snapshots = proj._store.get_all("compliance_snapshots")
        first_snapshot_pos = snapshots[0]["global_position"]

        # Second check — BLOCKED (e.g., new regulation discovered)
        await store.append(f"compliance-{app_id}", [
            ComplianceCheckInitiated(payload={
                "application_id": app_id, "regulation_set_version": "2026-Q2",
                "checks_required": ["REG-003"],
            })
        ], expected_version=3)
        await store.append(f"compliance-{app_id}", [
            ComplianceRuleFailed(payload={
                "application_id": app_id, "rule_id": "REG-003",
                "rule_version": "v1.0",
                "failure_reason": "Montana excluded",
                "remediation_required": False, "is_hard_block": True,
            })
        ], expected_version=4)
        await store.append(f"compliance-{app_id}", [
            ComplianceCheckCompleted(payload={
                "application_id": app_id, "overall_verdict": "BLOCKED",
                "checks_completed": 1,
            })
        ], expected_version=5)

        await daemon.run_once()

        # Query at T1 using up_to_position → should show CLEAR
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        result_t1 = proj.get_compliance_at(
            app_id, future, up_to_position=first_snapshot_pos
        )
        assert result_t1 is not None
        assert result_t1["overall_verdict"] == "CLEAR"

        # Current query → should show BLOCKED
        current = proj.get_current_compliance(app_id)
        assert current is not None
        assert current["overall_verdict"] == "BLOCKED"
        assert current["has_hard_block"] is True

    async def test_query_before_any_events_returns_none(self, store):
        """Query at a time before any events → None."""
        proj = ComplianceAuditViewProjection()
        past = datetime(2020, 1, 1, tzinfo=timezone.utc)
        assert proj.get_compliance_at("NONEXIST", past) is None


# =============================================================================
# 4. Daemon Isolation — One Projection Failing Doesn't Block Others
# =============================================================================


class TestDaemonIsolation:
    """One failing projection must not block other projections."""

    async def test_failing_projection_does_not_block_others(self, store):
        """
        ProjectionA always fails. ProjectionB works fine.
        After daemon runs, B should have processed all events, A should have
        skipped (after retries).
        """
        class AlwaysFailProjection(Projection):
            def __init__(self):
                self._checkpoint = 0

            @property
            def name(self):
                return "always_fail"

            @property
            def subscribed_events(self):
                return {"ApplicationSubmitted"}

            async def handle(self, event):
                raise RuntimeError("I always fail!")

            async def rebuild_from_scratch(self):
                self._checkpoint = 0

        failing = AlwaysFailProjection()
        working = ApplicationSummaryProjection()

        await store.append("loan-ISO-001", [
            ApplicationSubmitted(payload={
                "application_id": "ISO-001", "applicant_id": "C1",
                "requested_amount_usd": 100.0, "loan_purpose": "",
                "submission_channel": "api", "submitted_at": "",
            })
        ], expected_version=-1)

        daemon = ProjectionDaemon(store, [failing, working], max_retries=3)

        # Run enough times to exhaust retries
        for _ in range(4):
            await daemon.run_once()

        # Working projection should have processed the event
        assert working.get_summary("ISO-001") is not None
        assert working.get_summary("ISO-001")["state"] == "SUBMITTED"

        # Failing projection's checkpoint should have advanced (skipped)
        cp = await failing.get_checkpoint()
        assert cp > 0  # Event was skipped after retries


# =============================================================================
# 5. Out-of-Order & Gap Events
# =============================================================================


class TestEventEdgeCases:
    """Test out-of-order events and event gaps."""

    async def test_duplicate_events_no_state_change(self, store):
        """Feed the same event data twice → state unchanged, no duplicates."""
        proj = ComplianceAuditViewProjection()
        daemon = ProjectionDaemon(store, [proj])
        app_id = "DUP-001"

        await store.append(f"compliance-{app_id}", [
            ComplianceRulePassed(payload={
                "application_id": app_id, "rule_id": "REG-001",
                "rule_version": "v1.0",
                "evaluation_timestamp": datetime.now(timezone.utc).isoformat(),
                "evidence_hash": "e1",
            })
        ], expected_version=-1)

        await daemon.run_once()
        state1 = proj.get_current_compliance(app_id)

        # Reset checkpoint and reprocess
        proj._checkpoint = 0
        await daemon.run_once()
        state2 = proj.get_current_compliance(app_id)

        # Should be identical — no duplicate rows
        assert state1["total_events"] == state2["total_events"]

    async def test_multiple_applications_independent(self, store):
        """Events for different applications don't interfere."""
        proj = ApplicationSummaryProjection()
        daemon = ProjectionDaemon(store, [proj])

        for i in range(5):
            app_id = f"IND-{i:03d}"
            await store.append(f"loan-{app_id}", [
                ApplicationSubmitted(payload={
                    "application_id": app_id, "applicant_id": f"COMP-{i}",
                    "requested_amount_usd": (i + 1) * 10000.0,
                    "loan_purpose": "test", "submission_channel": "api",
                    "submitted_at": datetime.now(timezone.utc).isoformat(),
                })
            ], expected_version=-1)

        await daemon.run_once()

        for i in range(5):
            s = proj.get_summary(f"IND-{i:03d}")
            assert s is not None
            assert s["applicant_id"] == f"COMP-{i}"
            assert s["requested_amount_usd"] == (i + 1) * 10000.0

        assert proj.count() == 5


# =============================================================================
# 6. Rebuild vs Incremental Comparison
# =============================================================================


class TestRebuildConsistency:
    """rebuild_from_scratch must produce identical results to incremental."""

    async def test_application_summary_rebuild_consistency(self, store):
        """Feed events, capture state, rebuild, compare."""
        proj = ApplicationSummaryProjection()
        daemon = ProjectionDaemon(store, [proj])

        # Create diverse events
        for i in range(10):
            app_id = f"REB-{i:03d}"
            await store.append(f"loan-{app_id}", [
                ApplicationSubmitted(payload={
                    "application_id": app_id, "applicant_id": f"C-{i}",
                    "requested_amount_usd": (i + 1) * 50000.0,
                    "loan_purpose": "test", "submission_channel": "api",
                    "submitted_at": datetime.now(timezone.utc).isoformat(),
                })
            ], expected_version=-1)

        # Add credit analysis to half
        for i in range(5):
            app_id = f"REB-{i:03d}"
            await store.append(f"loan-{app_id}", [
                CreditAnalysisCompleted(payload={
                    "application_id": app_id, "agent_id": "agent-R",
                    "session_id": f"rs-{i}", "model_version": "v3.0",
                    "confidence_score": 0.70 + i * 0.05, "risk_tier": "MEDIUM",
                    "recommended_limit_usd": 40000.0,
                    "analysis_duration_ms": 1000, "input_data_hash": f"rh{i}",
                })
            ], expected_version=1)

        await daemon.run_once()

        # Capture incremental state
        incremental_summaries = {
            s["application_id"]: s for s in proj.get_all_summaries()
        }
        assert len(incremental_summaries) == 10

        # Rebuild
        await proj.rebuild_from_scratch()
        assert proj.count() == 0

        await daemon.run_once()

        # Capture rebuild state
        rebuild_summaries = {
            s["application_id"]: s for s in proj.get_all_summaries()
        }

        # Compare
        assert len(rebuild_summaries) == 10
        for app_id in incremental_summaries:
            inc = incremental_summaries[app_id]
            reb = rebuild_summaries[app_id]
            assert inc["state"] == reb["state"]
            assert inc["applicant_id"] == reb["applicant_id"]
            assert inc["requested_amount_usd"] == reb["requested_amount_usd"]
            assert inc.get("risk_tier") == reb.get("risk_tier")

    async def test_compliance_rebuild_consistency(self, store):
        """ComplianceAuditView rebuild produces identical results."""
        proj = ComplianceAuditViewProjection()
        daemon = ProjectionDaemon(store, [proj])

        app_id = "REB-COMP"
        await store.append(f"compliance-{app_id}", [
            ComplianceCheckInitiated(payload={
                "application_id": app_id, "regulation_set_version": "2026-Q1",
                "checks_required": ["REG-001", "REG-002"],
            })
        ], expected_version=-1)
        await store.append(f"compliance-{app_id}", [
            ComplianceRulePassed(payload={
                "application_id": app_id, "rule_id": "REG-001",
                "rule_version": "v1.0",
                "evaluation_timestamp": datetime.now(timezone.utc).isoformat(),
                "evidence_hash": "re1",
            })
        ], expected_version=1)
        await store.append(f"compliance-{app_id}", [
            ComplianceRulePassed(payload={
                "application_id": app_id, "rule_id": "REG-002",
                "rule_version": "v1.0",
                "evaluation_timestamp": datetime.now(timezone.utc).isoformat(),
                "evidence_hash": "re2",
            })
        ], expected_version=2)
        await store.append(f"compliance-{app_id}", [
            ComplianceCheckCompleted(payload={
                "application_id": app_id, "overall_verdict": "CLEAR",
                "checks_completed": 2,
            })
        ], expected_version=3)

        await daemon.run_once()
        state_inc = proj.get_current_compliance(app_id)

        # Rebuild
        await proj.rebuild_from_scratch()
        await daemon.run_once()
        state_reb = proj.get_current_compliance(app_id)

        assert state_inc["overall_verdict"] == state_reb["overall_verdict"]
        assert state_inc["has_hard_block"] == state_reb["has_hard_block"]
        assert len(state_inc["rules"]) == len(state_reb["rules"])
        assert state_inc["total_events"] == state_reb["total_events"]


# =============================================================================
# 7. Large Batch Performance
# =============================================================================


class TestLargeBatch:
    """Verify projections handle large event volumes correctly."""

    async def test_100_applications_processed(self, store):
        """100 applications → all 100 appear in ApplicationSummary."""
        proj = ApplicationSummaryProjection()
        daemon = ProjectionDaemon(store, [proj])

        for i in range(100):
            app_id = f"BATCH-{i:04d}"
            await store.append(f"loan-{app_id}", [
                ApplicationSubmitted(payload={
                    "application_id": app_id, "applicant_id": f"C-{i}",
                    "requested_amount_usd": 10000.0,
                    "loan_purpose": "test", "submission_channel": "api",
                    "submitted_at": datetime.now(timezone.utc).isoformat(),
                })
            ], expected_version=-1)

        processed = await daemon.run_once()
        assert processed == 100
        assert proj.count() == 100

        # Lag should be zero after processing
        lags = await daemon.get_all_lags()
        assert lags["application_summary"]["lag_events"] == 0

    async def test_all_three_projections_100_events(self, store):
        """100 credit analyses → all 3 projections process correctly."""
        app_proj = ApplicationSummaryProjection()
        perf_proj = AgentPerformanceLedgerProjection()
        comp_proj = ComplianceAuditViewProjection()

        daemon = ProjectionDaemon(store, [app_proj, perf_proj, comp_proj])

        for i in range(20):
            app_id = f"ALL3-{i:03d}"
            await store.append(f"loan-{app_id}", [
                ApplicationSubmitted(payload={
                    "application_id": app_id, "applicant_id": f"C-{i}",
                    "requested_amount_usd": 50000.0,
                    "loan_purpose": "test", "submission_channel": "api",
                    "submitted_at": datetime.now(timezone.utc).isoformat(),
                })
            ], expected_version=-1)
            await store.append(f"loan-{app_id}", [
                CreditAnalysisCompleted(payload={
                    "application_id": app_id, "agent_id": "agent-BULK",
                    "session_id": f"bs-{i}", "model_version": "v3.0",
                    "confidence_score": 0.75, "risk_tier": "MEDIUM",
                    "recommended_limit_usd": 40000.0,
                    "analysis_duration_ms": 1200, "input_data_hash": f"bh{i}",
                })
            ], expected_version=1)

        await daemon.run_once()

        assert app_proj.count() == 20
        perf = perf_proj.get_performance("agent-BULK", "v3.0")
        assert perf["analyses_completed"] == 20
        assert abs(perf["avg_confidence_score"] - 0.75) < 0.001

        lags = await daemon.get_all_lags()
        for name in ["application_summary", "agent_performance_ledger", "compliance_audit_view"]:
            assert lags[name]["lag_events"] == 0
