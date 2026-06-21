# Changelog

All notable changes to this project will be documented in this file.
The format loosely follows Keep a Changelog.

---

## 0.4.2 — 2026-06-21

### Added
- **Batched adaptive heartbeats** — single `UPDATE ... WHERE key = ANY(%s) RETURNING key` per cycle across all registered keys. One round trip to Postgres regardless of how many tasks are in flight.
- **Lazy heartbeat thread initialization** — thread spawns only on first task registration with `hard_ttl_ms`. Thread exits when bucket is empty and respawns on next registration. No idle threads in processes where heartbeats are unused.
- **Opt-in heartbeats** — tasks are only registered for heartbeating if `hard_ttl_ms` is provided. Tasks without `hard_ttl_ms` are never added to the heartbeat bucket.
- **Adaptive beat intervals** — interval tightens as execution approaches `hard_ttl_ms`. Early window beats every `ttl/3`, mid-window every `ttl/5`, final stretch every `ttl/10`.
- **Per-key two-strike failure detection** — if a key misses two consecutive heartbeats it is deregistered and marked uncertain.
- **Connection-level two-strike retry** — two consecutive connection failures clear the entire bucket and reconnect. All affected tasks become uncertain.
- **`ttl_ms` column on `sentinel_leases`** — stored at acquire time, read by batch heartbeat directly from the row. Enables true single-statement batching without passing TTL values from the Python side.

### Changed
- `HeartbeatManager` reduced from multi-bucket multi-thread design to single bucket single thread.
- Heartbeat no longer requires `owner_id`, `fencing_token`, or `fn` — manager is fully self-contained.

---

## 0.4.1 — 2026-06-20

### Fixed
- Lease now collapses immediately when `fn()` raises an exception. Previously the lease stayed alive until `hard_ttl_ms` expired, causing callers in that window to see `execution_alive=True` instead of `uncertain=True`.

### Added
- `sentinel.inspect(key)` — inspect the current state of any lease, returns an `InspectResult` with status, liveness, expiry times, and execution result.
- `sen` CLI tool — `sen inspect <key>` reads `DATABASE_URL` from environment or `.env` and prints lease state directly to the terminal.
- `sentinel.reconcile` — standalone `Reconcile` class exposed as an attribute on `Sentinel`. Replaces the previous pattern of reconciliation methods on `OnceResult`.

### Changed
- `reconcile()`, `force_complete()`, and `reset()` no longer require `owner_id` and `fencing_token`. Keys are resolved internally; WHERE clause guards handle safety.
- Reconciliation is no longer exposed on `OnceResult`. Use `sentinel.reconcile` instead.

---

## [0.4.0] - 2026

### Added

- AsyncSentinel
- AsyncOnce
- Async execution coordination support
- Async reconciliation support
- Async helper utilities
- Async database initialization

### Improved

- Sync and async execution parity
- CI coverage
- Package metadata

---

## [0.3.1] - 2026

### Improved

- Documentation updates
- Packaging improvements
- Public repository improvements

---

## [0.3.0] - 2026

### Added

- Explicit reconciliation APIs
- Execution uncertainty handling
- Improved execution lifecycle management

---

## [0.2.0] - 2026

### Added

- Heartbeat management
- Cached result replay
- Ownership validation
- Fencing token support

---

## [0.1.0] - 2026

### Initial Release

Introduced Sentinel.

Features included:

- PostgreSQL-backed execution coordination
- Lease acquisition
- Single execution semantics
- Canonical completion
- Django integration