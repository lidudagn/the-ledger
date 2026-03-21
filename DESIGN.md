# DESIGN.md — Architectural Tradeoffs & Analysis

This document outlines the core architectural decisions, tradeoffs, and quantitative analyses for The Ledger event sourcing system.

---

---

## 1. Event Store Schema Column Justifications

Per Phase 1 requirements, standard enterprise event sourcing strictly forbids unnecessary columns. Every column in our schema exists for a specific mathematical or operational reason:

### `events` Table
*   `event_id (UUID)`: Idempotency key for identical-payload detection and Outbox reference.
*   `stream_id (TEXT)`: Defines the aggregate boundary (e.g., `loan-123`). Essential for loading an aggregate's state.
*   `stream_position (BIGINT)`: Enforces Optimistic Concurrency Control (OCC) via the `UNIQUE(stream_id, stream_position)` constraint.
*   `global_position (BIGINT)`: The absolute ordering of all events across the entire system. Required for catch-up subscriptions, Replay, Projection lag calculation, and Temporal Queries.
*   `event_type (TEXT)`: Used to route events to the correct handlers/upcasters without parsing the JSON body.
*   `event_version (SMALLINT)`: Crucial for Upcasting. Tells the `UpcasterRegistry` which structural version of the payload is stored.
*   `payload (JSONB)`: The actual fact data. `JSONB` allows schema-less storage while maintaining indexability if needed.
*   `metadata (JSONB)`: Out-of-band tracing data (correlation ID, causation ID, origin agent). Separated from payload so business logic doesn't couple to tracing logic.
*   `recorded_at (TIMESTAMPTZ)`: Wall-clock time of the commit. Used for temporal inferences in legacy upcasters and regulatory audits.

### `event_streams` Table
*   `stream_id (TEXT)`: Primary Key, linking to `events.stream_id`.
*   `aggregate_type (TEXT)`: Allows us to quickly discover all streams of a certain type without scanning the massive `events` table (e.g., "find all LoanApplications").
*   `current_version (BIGINT)`: Caching the max `stream_position`. Prevents needing `MAX(stream_position)` aggregations on every append.
*   `created_at (TIMESTAMPTZ)`: Stream lifecycle tracking.
*   `archived_at (TIMESTAMPTZ)`: Cold-storage routing. Allows the system to freeze streams from further appends (enforced by code).
*   `metadata (JSONB)`: Stream-level tags (e.g., `tenant_id` for multi-tenant isolation).

### `projection_checkpoints` & `outbox` Tables
*   `projection_name` / `last_position` / `updated_at`: The exact pointer required for a daemon to resume after a crash with at-least-once delivery guarantees.
*   `outbox.destination`, `attempts`, `published_at`: Standard transactional outbox columns for asynchronous message bus publishing.

---

## 2. Aggregate Boundary Justification

**Decision:** `ComplianceRecord` is modelled as a separate aggregate from `LoanApplication`.

**Alternative Considered:** Modelling compliance checks as events directly within the `LoanApplication` stream (e.g., `loan-{id}`).

**Why it was rejected (The Coupling Trace):**
If compliance events shared the loan stream, the aggregate boundary would become too large. In our scenario, four specialized AI agents collaborate per application, often concurrently. 

*   **Failure Mode under concurrency:** The ComplianceAgent might be running its rules concurrently while the CreditAnalysisAgent evaluates the financials. Both agents fetch the current state at `version=5`. The Credit agent appends `CreditAnalysisCompleted` at `expected_version=5` (success, stream becomes `version=6`). A millisecond later, the Compliance agent tries to append `ComplianceRulePassed` at `expected_version=5`. It receives an `OptimisticConcurrencyError` and must retry.
*   **The resulting coupling:** The compliance process is now logically coupled to the credit analysis process simply because they share a storage stream, even though their business invariants are entirely independent (credit limits do not invalidate regulatory checks). 
*   **Resolution:** By splitting `ComplianceRecord` into its own aggregate (`compliance-{id}`), the ComplianceAgent and CreditAnalysisAgent write to separate streams. They can operate in perfectly parallel isolation with zero false-contention OCC errors. The requirement that an application cannot be approved without compliance clearance is enforced as a cross-aggregate read during the `handle_approve_application` command.

### Stream Routing Tradeoff (Phase 2 Reality vs. Purity)

In Phase 2, we deliberately chose to have agents like `CreditAnalysis` append their `CreditAnalysisCompleted` events directly to the `loan-{id}` stream, rather than their own independent streams. This represents a deliberate tradeoff:

*   **Option A (Pure Isolation):** Agents write exclusively to agent-specific streams. The loan aggregate listens for these via integration/bridge events. This is theoretically pure but introduces significant orchestration complexity and eventual consistency delays within the core state machine itself.
*   **Option B (Coupled Writing):** Agents write directly to the `loan-{id}` stream to drive the state machine synchronously.

**Decision:** We chose Option B for Phase 2 simplicity. We acknowledge this reduces aggregate purity—the loan stream effectively "owns" the result of the agent's work. It creates tighter coupling but drastically simplifies testing and synchronous command validation.

---

## 2. Projection Strategy & SLOs

### Inline vs. Async
*   **ApplicationSummary (Async):** We chose Async. While an inline projection would provide read-your-own-writes consistency, pushing the update to an Async Daemon improves write throughput for the event store. At 1,000 applications/hour, optimizing command latency is critical.
*   **SLO Commitment:** For `ApplicationSummary` and `AgentPerformanceLedger`, the SLO is **p99 < 500ms lag**.
*   **ComplianceAuditView (Async):** The regulatory read model must process complex temporal state. It operates with a **p99 < 2000ms lag** SLO. Regulators prioritize completeness and temporal accuracy over sub-second real-time updates.

### Temporal Query Snapshot Strategy
To support `get_compliance_at(timestamp)`, we use an **Event-Count / Lifecycle Boundary Snapshot Strategy**.
*   **Trigger:** Snapshots are generated synchronously by the projection handler whenever it processes a `ComplianceCheckCompleted` event. 
*   **Justification (The Cost of Replay):** Without snapshots, answering `get_compliance_at(timestamp)` requires replaying the *entire* compliance event history from position 0 up to the requested timestamp. Under load, this worst-case replay cost scales linearly ($O(N)$) and becomes an unacceptable CPU bottleneck for regulatory audits. Taking a snapshot at every single `RulePassed` event would mitigate this but create unnecessary write amplification. Taking snapshots at lifecycle boundaries is the optimum tradeoff: it bounds the worst-case replay cost (max $K$ events since last boundary) while keeping storage overhead low.
*   **Invalidation logic:** Snapshots are never invalidated during normal operation because the past is immutable. However, if an upcaster is deployed that alters the structural interpretation of historical events, the projection's `rebuild_from_scratch()` method is invoked, which truncates the `compliance_snapshots` table and rebuilds them deterministically from position 0.

### Eventual Consistency & Read Tolerance

The system operates under strict **eventual consistency**. Read models (projections) are guaranteed to lag behind the write model (event store). 
*   **The Contract:** UI clients and API consumers must be designed to tolerate stale reads within the defined SLO bounds. 
*   **Critical Flows:** Operations requiring strong consistency (e.g., verifying an application hasn't already been approved before disbursing funds) must rely on synchronous write-side command handlers evaluating the aggregate state, *never* on projection state.

### Distributed Daemon Analysis (Marten Async Parallel)
If our system scaled to require a distributed, multi-node projection daemon architecture (like Marten 7.0), our current single-process Python `asyncio` loop would fail because multiple workers would process the exact same events simultaneously.
*   **Coordination Primitive:** To achieve distributed projection execution, we would implement **Leased Checkpoints** using PostgreSQL advisory locks (`pg_try_advisory_lock()`). Each projection instance would attempt to acquire a lock on a specific `projection_name` row in the `projection_checkpoints` table.
*   **Failure Mode Guarded:** This guards against the "split-brain daemon" failure mode. If node A crashes mid-batch, its lock times out or is released. Node B acquires the lock, reads the last successfully committed `last_position` from the database, and resumes processing natively, guaranteeing at-least-once delivery without duplicate concurrent processing.

---

## 4. Concurrency Analysis & Budgets

**Scenario:** Peak load of 100 concurrent applications, each processed by 4 AI agents simultaneously.

*   **The Collision Vector:** Without explicit orchestration, all 4 agents might finish their tasks and attempt to write `DecisionGenerated`, `CreditAnalysisCompleted`, `FraudScreeningCompleted`, and `ComplianceCheckCompleted` to the system simultaneously. Due to our aggregate boundaries, only events targeting the identical stream (`loan-{id}`) will collide.
*   **Expected `OptimisticConcurrencyError` Rate (Worst-Case Analysis):** While organic, uncoordinated collisions are rare due to LLM latency variance, orchestrated workflows create dangerous burst contention. When the `DecisionOrchestrator` generates its final recommendation, it triggers synchronous `HumanReviewRequested` commands. Simultaneously, background compliance checks might finalize. This creates hotspot contention on the `loan-{id}` stream. In these worst-case burst scenarios, we expect up to **15-20 OCEs per minute** across the 100 applications, tightly clustered around lifecycle transitions.
*   **Retry Strategy:** We employ a **Jittered Exponential Backoff**. 
    *   Attempt 1: Immediate reload and retry.
    *   Attempt 2: Reload, wait 50ms + random(0-50ms), retry.
    *   Attempt 3: Reload, wait 150ms + random(0-50ms), retry.
*   **Retry Budget:** A maximum of **3 retries** are permitted. If the 4th attempt fails, the command is rejected with an `OptimisticConcurrencyError` bubble-up. A persistent failure after 3 jittered retries indicates a systemic hotspot (e.g., a runaway script thrashing the stream) rather than normal organic concurrency.

---

## 4. Upcasting Inference Decisions

When evolving `CreditAnalysisCompleted` from v1 to v2, we must introduce three new fields: `model_version`, `confidence_score`, and `regulatory_basis`. Since historical v1 events lack this data, the upcaster must infer it.

| Field | Inference Strategy | Downstream Consequence of Error | Over Null Decision |
| :--- | :--- | :--- | :--- |
| `model_version` | **Inferred** via temporal mapping (e.g., checking the `recorded_at` timestamp against a known schedule of model deployments). | Tracing an agent session might attribute a decision to v2.2 instead of v2.1. | Selected Inference. A `null` model version breaks the Gas Town pattern and AgentPerformanceLedger. We rely on strict deployment logs to make the temporal inference highly accurate (error rate < 1%). |
| `confidence_score` | **Null**. | The `AgentPerformanceLedger` will ignore this event for average confidence calculations (N-1 denominator). | Selected Null. Fabricating an LLM's confidence score retroactively is mathematically indefensible and legally risky in a regulated environment. Providing a false 0.8 confidences damages the integrity of the audit ledger far more than a null. |
| `regulatory_basis` | **Inferred** via the active regulation set at the `recorded_at` date. | A compliance rule might be audited against a slightly delayed regulation rollout window. | Selected Inference. Historical regulations are a matter of public public record; temporal inference here is deterministic and 100% accurate given a correct lookup table. |

---

## 5. EventStoreDB Comparison

While we implemented The Ledger using PostgreSQL natively (ideal for Python/FastAPI integration without external dependencies), the enterprise standard for this throughput profile is EventStoreDB (ESDB).

**Mapping Concepts:**
*   **Postgres `event_streams.stream_id`** → ESDB **Stream ID**.
*   **Postgres `events.global_position`** → ESDB **$all stream position**. ESDB natively maintains a global, monotonically increasing log of all events across all streams.
*   **Postgres `load_all()`** → ESDB **Read `$all`**.
*   **Postgres `ProjectionDaemon`** → ESDB **Catch-up Subscriptions** or **Persistent Subscriptions**, which natively manage checkpoints and guarantee at-least-once delivery to workers without requiring custom polling loops.

**What ESDB Provides That We Had to Build:**
Our PostgreSQL implementation relies on polling `SELECT ... WHERE global_position > X` in the background daemon. This introduces latency (our 100ms poll interval) and CPU overhead. EventStoreDB uses a native push model via gRPC; when an event is appended, ESDB immediately pushes it to connected subscribers. Furthermore, ESDB handles log truncation, scavenging, and distributed cluster consensus seamlessly, whereas scaling our PostgreSQL events table to billions of rows would require complex table partitioning strategies (e.g., pg_partman) that we currently lack.

---

## 6. What I Would Do Differently

If granted another full day to refine the architecture, the single most significant decision I would reconsider is **the physical implementation of the `Outbox` pattern and how it couples to the `ProjectionDaemon`.**

Currently, our `ProjectionDaemon` acts effectively as a polling consumer on the `events` table itself via `global_position`. This is a classic "pull" based Catch-Up Subscription. While robust, it doesn't utilize the `outbox` table we designed in Phase 1. 

**The Flaw:** By relying on polling the main `events` table for projections, we've tightly coupled our read models to the core storage mechanism. If we ever shard the `events` table across multiple Postgres nodes, the global `global_position` counter becomes incredibly difficult to serialize perfectly.

**The Refinement:** I would refactor the system to truly leverage the Transactional Outbox. 
1. The `EventStore.append()` method writes to `events` and identical payloads to the `outbox` table.
2. We implement a standalone `OutboxRelay` process utilizing PostgreSQL `LISTEN/NOTIFY`.
3. The Relay reads the outbox and publishes the events onto an actual message bus (e.g., Redis Streams or Kafka).
4. The `ProjectionDaemon` subscribes to Redis Streams. 

This shifts the architecture from a polling monolith to a true reactive, push-based distributed system. It offloads read-pressure from the primary database, provides **at-least-once delivery** guarantees (paired with **effectively-once** processing via our idempotent projection consumers), and perfectly positions the system for the Polyglot Bridge integration required in later enterprise phases.
