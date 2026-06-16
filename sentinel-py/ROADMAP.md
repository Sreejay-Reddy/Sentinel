# Roadmap

Sentinel is a PostgreSQL-backed execution coordination primitive for correctness-sensitive distributed work.

The focus is to keep the core small, explicit, and reliable rather than continuously expanding the surface area.

## Principles

- PostgreSQL first
- Explicit over automatic
- Correctness over convenience
- Small core API
- Framework agnostic

---

## Completed

### Core Runtime

- [x] Lease acquisition with conditional upsert
- [x] Ownership validation via fencing tokens
- [x] Heartbeat management on OS threads
- [x] Hard TTL as absolute execution deadline
- [x] Cached result replay on repeat calls
- [x] Explicit reconciliation (`reconcile`, `force_complete`, `reset`)
- [x] Execution uncertainty surfaced on `fn()` failure
- [x] `expire_lease` to collapse uncertainty window immediately on failure

### APIs

- [x] `Sentinel` — synchronous client
- [x] `AsyncSentinel` — async client with full execution parity
- [x] `init_db` / `async_init_db` — schema initialization

### Integrations

- [x] Django — `DjangoSentinel` with borrowed connection and optional migrations

### Tooling

- [x] pytest suite covering core, once, helper, and reconciliation (sync and async)
- [x] GitHub Actions CI against Postgres service container
- [x] PyPI publishing as `sentinel-coordination`

---

## Near Term (0.5.0)

- [ ] `sentinel_events` append-only event log written in the same transaction as state transitions
- [ ] FastAPI integration after async core stabilizes
- [ ] Expanded test coverage for edge cases and contention scenarios
- [ ] Improved examples and integration guides

---

## Under Consideration

These are ideas being explored and are not guaranteed.

- [ ] Correlate — separate OSS library that reads `sentinel_events` for cross-service execution observability
- [ ] Batched adaptive heartbeats — bucket-level batch `UPDATE` with adaptive interval
- [ ] Additional framework adapters (Flask, Starlette)
- [ ] Embeddable reconciliation dashboard

---

## Non Goals

Sentinel is intentionally not:

- A queue
- A workflow engine
- A scheduler
- A retry framework
- A distributed lock library
- A framework-specific tool