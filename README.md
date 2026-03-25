# The Ledger

Agentic Event Store & Enterprise Audit Infrastructure.
Building the immutable memory and governance backbone for multi-agent AI systems at production scale.

## Prerequisites

- Python ≥ 3.11
- PostgreSQL ≥ 15 (for real DB tests)
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

## Installation

```bash
# Clone and install
git clone <repo-url> && cd the-ledger

# Using uv (recommended)
uv pip install -e ".[dev]"

# Or using pip
pip install -e ".[dev]"
```

## Database Setup

1. **Create the database:**
   ```bash
   createdb the_ledger
   ```

2. **Set the connection URL:**
   ```bash
   export DATABASE_URL="postgresql://localhost/the_ledger"
   # Or copy .env.example → .env and edit
   cp .env.example .env
   ```

3. **Run migrations:**
   ```bash
   psql $DATABASE_URL -f src/schema.sql
   ```

## Running Tests

### Unit Tests (in-memory store — no DB required)

```bash
pytest tests/test_models.py tests/test_concurrency.py tests/test_aggregates.py -v
```

### Real Database Tests (requires PostgreSQL)

```bash
DATABASE_URL="postgresql://localhost/the_ledger" pytest tests/test_event_store.py -v
```

### Full Test Suite

```bash
pytest tests/ -v
```

## Project Structure

```
src/
├── schema.sql                  # PostgreSQL schema (events, event_streams, outbox, projections)
├── event_store.py              # EventStore async class (append, load, OCC)
├── in_memory_store.py          # In-memory EventStore for unit testing
├── upcasters.py                # Registered upcaster functions (v1→v2 transforms)
├── models/
│   └── events.py               # Pydantic models: BaseEvent, StoredEvent, all domain events, errors
├── aggregates/
│   ├── loan_application.py     # LoanApplicationAggregate — state machine + business rules
│   ├── agent_session.py        # AgentSessionAggregate — Gas Town pattern enforcement
│   ├── compliance_record.py    # ComplianceRecordAggregate — separate compliance boundary
│   └── audit_ledger.py         # AuditLedgerAggregate — hash chain contiguity
├── commands/
│   └── handlers.py             # Command handlers: load → validate → build → append
├── projections/                # CQRS read models (Phase 3)
│   ├── base.py                 # Projection ABC + InMemoryProjectionStore + PgProjectionStore
│   ├── daemon.py               # ProjectionDaemon — fault-tolerant, SLO-aware async poller
│   ├── application_summary.py  # ApplicationSummaryProjection (15 event types, UPSERT)
│   ├── agent_performance.py    # AgentPerformanceLedgerProjection (running averages)
│   └── compliance_audit.py     # ComplianceAuditViewProjection (temporal queries + snapshots)
├── upcasting/
│   └── registry.py             # UpcasterRegistry — chain/ceiling guards, pure transforms
├── integrity/
│   ├── audit_chain.py          # SHA-256 cryptographic audit chain + tamper detection
│   └── gas_town.py             # Agent crash recovery — context reconstruction
├── what_if/
│   └── projector.py            # Counterfactual what-if engine (13-step causal replay)
├── regulatory/
│   └── package.py              # Regulatory examination package generator (8 sections)
├── mcp_server/
│   ├── server.py               # FastMCP server with lifespan, daemon, SLO config
│   ├── tools.py                # 11 command tools (idempotency, rate limiting, structured errors)
│   └── resources.py            # 6 query resources (projections, audit trail, health)
└── agents/                     # AI agent implementations
tests/
├── test_concurrency.py         # Double-decision OCC test (Phase 1 gate)
├── test_aggregates.py          # Aggregate state machine + business rules (Phase 2)
├── test_models.py              # Pydantic model validation
├── test_event_store.py         # Real PostgreSQL integration tests
├── test_projections.py         # Projection routing + state transitions (Phase 3)
├── test_projection_stress.py   # Projection SLO stress tests
├── test_upcasting.py           # Upcaster transforms + immutability (Phase 4)
├── test_registry.py            # UpcasterRegistry chain/ceiling guard tests
├── test_audit_chain.py         # SHA-256 hash chain verification
├── test_gas_town.py            # Agent crash recovery + NEEDS_RECONCILIATION
├── test_mcp_lifecycle.py       # Full MCP tool lifecycle (Phase 5)
├── test_what_if.py             # Counterfactual replay + causal filtering (Phase 6)
├── test_regulatory_package.py  # Regulatory package generation + integrity
├── test_narratives.py          # Narrative sentence generation
├── test_stress.py              # System-level stress tests
├── test_incremental.py         # Incremental processing tests
├── test_credit_analysis_agent.py
├── test_fraud_agent.py
├── test_compliance_agent.py
├── test_decision_agent.py
└── test_document_processing_agent.py
```

## Key Concepts

- **Optimistic Concurrency Control**: Every `append()` requires `expected_version`. Conflicts raise `OptimisticConcurrencyError`.
- **Gas Town Pattern**: Agent sessions must have `AgentContextLoaded` before any decision events.
- **CQRS**: Commands append events via handlers; queries read from projections.
