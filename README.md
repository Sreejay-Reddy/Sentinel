# Sentinel

Not all work is safe to retry.

Payments, webhooks, startup jobs, long-running operations and other correctness-sensitive operations often need stronger guarantees than "just run it again."

Sentinel is a PostgreSQL-backed execution coordination primitive that provides execution ownership, cached result replay, heartbeat-backed liveness, fencing tokens, and explicit handling of uncertain execution outcomes.

Sentinel's primary interface is `once()`, which coordinates execution across competing workers and replays completed results to subsequent callers.

---

## Installation

```bash
pip install sentinel-coordination
```

![Sentinel CLI](assets/demo.gif)

Requires Python 3.9+ and a PostgreSQL database.

---

## Database Setup

```python
from sentinel import init_db

conn = get_conn()
init_db(conn)
conn.close()
```

This creates the coordination tables Sentinel needs. Safe to run multiple times.

---

## Getting Started

```python
import psycopg
from sentinel import Sentinel

def get_conn():
    return psycopg.connect("postgresql://postgres:postgres@localhost/testdb")

sentinel = Sentinel(
    get_conn=get_conn,
    default_ttl_ms=3000
)
```

---

## CLI

Sentinel ships with `sen`, a command-line tool for inspecting lease state directly from your terminal.

![Sentinel CLI](assets/demo.gif)

### Inspect a lease

```bash
sen inspect <key>
```

`sen` reads `DATABASE_URL` from your environment or a `.env` file automatically.

```bash
export DATABASE_URL=postgresql://user:password@localhost/mydb
sen inspect <key>
```

---

## The Once API

`sentinel.once()` is the primary interface. Given a key and a function, it guarantees that function runs **at most once per key** across any number of competing workers and returns the cached result to anyone else who asks.

```python
def process_payment(amount, customer_id):
    charge_card(
        amount=amount,
        customer_id=customer_id
    )

    return {
        "ok": True,
        "payment_id": "pay_123"
    }

result = sentinel.once(
    key="payment-order-789",
    fn=process_payment,
    kwargs={
        "amount": 99_00,
        "customer_id": "cus_abc"
    },
    ttl_ms=3000,
    hard_ttl_ms=30000
)
```

### Reading the result

```python
result = sentinel.once(...)

if result.execution_alive:
    # Another worker is actively executing.

elif result.uncertain:
    # Execution truth could not be established.
    # Use reconciliation tooling if needed.
    # Reconciallition tooling documentation is in Docs/philosophy.md 

else:
    # If execution_alive and uncertain are both False,
    # response contains either a newly completed result
    # or a cached result from a previous execution.
    return result.response
```

---

## Async

If you're working in an async context, use `AsyncSentinel`:

```python
import psycopg
from sentinel import AsyncSentinel

async def get_conn():
    return await psycopg.AsyncConnection.connect("postgresql://...")

sentinel = AsyncSentinel(
    get_conn=get_conn,
    default_ttl_ms=3000
)

result = await sentinel.once(
    key="payment-order-789",
    fn=process_payment,
    kwargs={"amount": 99_00, "customer_id": "cus_abc"},
    ttl_ms=3000,
    hard_ttl_ms=30000
)
```

`AsyncSentinel` accepts async functions as `fn`. The heartbeat runs on OS threads and does not interfere with the event loop.

For async schema setup:

```python
from sentinel import async_init_db

await async_init_db(conn)
```

---

## Django

Install the Django optional dependency:

```bash
pip install sentinel-coordination[django]
```

Then use DjangoSentinel directly:

```python
from sentinel.integrations.django import DjangoSentinel

sentinel = DjangoSentinel()
```

DjangoSentinel uses Django's configured database connection and respects Django's connection lifecycle.

To use Django migrations instead of `init_db`, add `sentinel.integrations` to `INSTALLED_APPS` and run:

```bash
python manage.py migrate sentinel.integrations
```

---

## TTL and Hard TTL

```python
sentinel.once(
    key="...",
    fn=fn,
    ttl_ms=3000,       # Heartbeat interval and lease window
    hard_ttl_ms=30000  # Absolute maximum lifetime of this execution
)
```

`ttl_ms` controls how often the heartbeat needs to renew the lease. `hard_ttl_ms` is the ceiling, no matter how healthy the heartbeat, execution cannot extend past this point.

For short work, they can be equal. For long-running jobs, use a short `ttl_ms` to detect dead workers quickly and a large `hard_ttl_ms` to give live workers room to finish.

If you omit `hard_ttl_ms`, it defaults to `ttl_ms` meaning heartbeat extension won't meaningfully extend the lease. This is intentional: explicit is better than surprising behavior for long-running work.

---

## Namespaces

If you're running multiple systems against the same database, namespaces keep your coordination keys isolated.

```python
sentinel = Sentinel(
    get_conn=get_conn,
    namespace="payments"
)
```

---

## Tradeoffs

Sentinel makes specific choices that won't suit everyone.

**PostgreSQL only.** The coordination layer runs on PostgreSQL. If you need Redis-backed coordination or want to avoid adding DB load for execution state, Sentinel isn't the right fit today. Redis support is on the roadmap.

**Explicit over automatic.** Uncertain states are surfaced, not resolved for you. This is a feature for correctness-sensitive systems and friction for everything else.

**No built-in retries.** Sentinel coordinates execution. It doesn't implement retry logic, backoff, or dead-letter queues. You bring those or compose them yourself.

**Not a queue.** Sentinel doesn't dispatch work or schedule tasks. It coordinates execution of work you've already routed to a worker.

---

## Known Failure Boundaries

If a worker enters the executing state and disappears before completion, Sentinel will not automatically replay the work.

At that point Sentinel cannot safely determine whether the side effect completed, partially completed, or never completed.

Instead, Sentinel surfaces the outcome as uncertain and requires explicit reconciliation.

Sentinel chooses correctness over automatic replay.

---

## Project Status

The core execution semantics are stable as of 0.4.0. Reconciliation tooling and observability APIs will continue to evolve.

---

## Roadmap

- Redis cache for better throughput 
- Append-only execution event log (`sentinel_events`)
- FastAPI integration
- Correlate — cross-service execution observability
- Stronger reconciliation tooling
- Metrics and observability hooks
- Framework integrations
- Additional language support

---

## License

MIT