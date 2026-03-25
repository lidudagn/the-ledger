-- =============================================================================
-- The Ledger — Event Store Schema
-- Phase 1: Core tables for event sourcing with optimistic concurrency control
-- =============================================================================

-- Events table: Append-only event log. The source of truth.
-- Contract C8: This table is append-only by convention.
--              UPDATE/DELETE permissions MUST be revoked at DB role level in production.
CREATE TABLE IF NOT EXISTS events (
    event_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    stream_id        TEXT NOT NULL,
    stream_position  BIGINT NOT NULL,
    global_position  BIGINT GENERATED ALWAYS AS IDENTITY,
    event_type       TEXT NOT NULL,
    event_version    SMALLINT NOT NULL DEFAULT 1,
    payload          JSONB NOT NULL,
    metadata         JSONB NOT NULL DEFAULT '{}'::jsonb,
    recorded_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    -- OCC enforcement at storage layer
    CONSTRAINT uq_stream_position UNIQUE (stream_id, stream_position),

    -- Prevent negative positions (defensive)
    CONSTRAINT chk_stream_position_positive CHECK (stream_position >= 0)
);

-- Contract C3: global_position provides total ordering but NOT contiguous sequencing.
--              Transaction rollbacks create gaps. Consumers must use > not BETWEEN.
CREATE INDEX IF NOT EXISTS idx_events_stream_id ON events (stream_id, stream_position);
CREATE INDEX IF NOT EXISTS idx_events_global_pos ON events (global_position);
CREATE INDEX IF NOT EXISTS idx_events_type ON events (event_type);
CREATE INDEX IF NOT EXISTS idx_events_recorded ON events (recorded_at);


-- Event streams table: Stream metadata + current_version for fast version checks.
-- Avoids scanning events table for version lookups.
CREATE TABLE IF NOT EXISTS event_streams (
    stream_id        TEXT PRIMARY KEY,
    aggregate_type   TEXT NOT NULL,
    current_version  BIGINT NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    archived_at      TIMESTAMPTZ,
    metadata         JSONB NOT NULL DEFAULT '{}'::jsonb
);


-- Projection checkpoints: Tracks last-processed global_position per projection.
-- Used by the async projection daemon (Phase 3).
CREATE TABLE IF NOT EXISTS projection_checkpoints (
    projection_name  TEXT PRIMARY KEY,
    last_position    BIGINT NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- Outbox table: Written atomically with events in the same transaction.
-- Contract C5: Provides at-least-once delivery. event_id is the dedup key.
--              All consumers MUST be idempotent.
CREATE TABLE IF NOT EXISTS outbox (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id         UUID NOT NULL REFERENCES events(event_id),
    destination      TEXT NOT NULL,
    payload          JSONB NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at     TIMESTAMPTZ,
    attempts         SMALLINT NOT NULL DEFAULT 0
);


-- =============================================================================
-- Phase 3: Projection Tables (CQRS Read Models)
-- =============================================================================

-- Projection 1: ApplicationSummary — One row per loan application, current state.
CREATE TABLE IF NOT EXISTS application_summary (
    application_id       TEXT PRIMARY KEY,
    state                TEXT NOT NULL DEFAULT 'UNKNOWN',
    applicant_id         TEXT,
    requested_amount_usd NUMERIC,
    approved_amount_usd  NUMERIC,
    risk_tier            TEXT,
    fraud_score          NUMERIC,
    compliance_status    TEXT,
    decision             TEXT,
    agent_sessions       JSONB NOT NULL DEFAULT '[]'::jsonb,
    last_event_type      TEXT,
    last_event_at        TIMESTAMPTZ,
    submitted_at         TIMESTAMPTZ,
    approved_at          TIMESTAMPTZ,
    declined_at          TIMESTAMPTZ,
    human_reviewer_id    TEXT,
    final_decision_at    TIMESTAMPTZ,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Projection 2: AgentPerformanceLedger — Metrics per agent + model_version.
CREATE TABLE IF NOT EXISTS agent_performance (
    agent_id             TEXT NOT NULL,
    model_version        TEXT NOT NULL,
    analyses_completed   INTEGER NOT NULL DEFAULT 0,
    decisions_generated  INTEGER NOT NULL DEFAULT 0,
    avg_confidence_score NUMERIC,
    avg_duration_ms      NUMERIC,
    approve_count        INTEGER NOT NULL DEFAULT 0,
    decline_count        INTEGER NOT NULL DEFAULT 0,
    refer_count          INTEGER NOT NULL DEFAULT 0,
    human_override_count INTEGER NOT NULL DEFAULT 0,
    first_seen_at        TIMESTAMPTZ,
    last_seen_at         TIMESTAMPTZ,
    PRIMARY KEY (agent_id, model_version)
);

-- Projection 3: ComplianceAuditView — Append-only compliance event log.
-- Stores every compliance event for temporal query support.
CREATE TABLE IF NOT EXISTS compliance_audit_events (
    id                   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    application_id       TEXT NOT NULL,
    event_type           TEXT NOT NULL,
    rule_id              TEXT,
    rule_version         TEXT,
    verdict              TEXT,
    failure_reason       TEXT,
    is_hard_block        BOOLEAN DEFAULT FALSE,
    regulation_version   TEXT,
    evidence_hash        TEXT,
    global_position      BIGINT NOT NULL UNIQUE,
    recorded_at          TIMESTAMPTZ NOT NULL,
    payload              JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_compliance_audit_app
    ON compliance_audit_events (application_id, recorded_at);
CREATE INDEX IF NOT EXISTS idx_compliance_audit_global
    ON compliance_audit_events (global_position);

-- Compliance snapshots for temporal queries.
-- Created on each ComplianceCheckCompleted event.
CREATE TABLE IF NOT EXISTS compliance_snapshots (
    application_id       TEXT NOT NULL,
    snapshot_at          TIMESTAMPTZ NOT NULL,
    global_position      BIGINT NOT NULL,
    overall_verdict      TEXT NOT NULL,
    rules_evaluated      JSONB NOT NULL DEFAULT '[]'::jsonb,
    has_hard_block       BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (application_id, snapshot_at)
);
