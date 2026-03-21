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

    -- Prevent zero/negative positions (defensive)
    CONSTRAINT chk_stream_position_positive CHECK (stream_position > 0)
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
