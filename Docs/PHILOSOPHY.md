# Sentinel

Distributed execution is hard to get right. Workers crash mid-flight. Retries overlap. Processes freeze while holding a lock. Side effects partially succeed and leave you guessing.

Most tools respond to this by pretending it isn't a problem. They retry silently, hide uncertainty, and hope the work was idempotent. Sentinel doesn't. It gives you a coordination layer built around an honest model of what can go wrong, and explicit tools for handling it when it does.

At its core, Sentinel is a PostgreSQL-backed execution primitive that guarantees **one active execution generation at a time**, rejects stale workers with fencing tokens, and surfaces uncertain outcomes instead of burying them.

---

## Philosophy

The dominant pattern in distributed task execution is optimistic: assume work is safe to retry, hide failures behind automatic replays, and let the application figure out the mess when duplicates show up downstream.

That works until it doesn't. And when it doesn't, you're debugging a payment that charged twice, an invoice that sent three times, or a downstream system in an inconsistent state you can't easily reconstruct.

Sentinel starts from a different assumption: **some work is not safe to replay, and your coordination layer should know the difference.**

When a worker crashes mid-execution, Sentinel doesn't guess. It marks the execution state as uncertain and hands that back to you. You decide whether to reset and retry, force-complete, or escalate. That's not a limitation, that's the correct behavior for correctness-sensitive systems.

A few things Sentinel will never do:

- Silently replay work it can't verify completed
- Pretend uncertainty doesn't exist to give you a cleaner API
- Guarantee something it can't actually guarantee

If that trade-off doesn't fit your use case, if your work is truly idempotent and automatic retries are fine, Sentinel may be more ceremony than you need. It's worth being honest about that.

---

## Execution Authority vs Execution Truth

Most coordination systems focus on authority:

> Who is allowed to perform work right now?

Sentinel does too. Leases, heartbeats, and fencing tokens exist to answer that question.

At any point in time, Sentinel can determine which execution generation is authoritative. If a lease expires and a new worker acquires authority, the previous generation becomes stale and loses the ability to make canonical state transitions.

But authority and truth are not the same thing.

A second question exists:

> Did the side effect actually happen?

This is the harder problem.

Consider a payment workflow:

```text
Worker A acquires authority
        ↓
Worker A charges customer
        ↓
Worker A crashes before completion is recorded
```

Sentinel can determine that Worker A no longer holds authority.

What Sentinel cannot determine is whether the payment was successfully charged.

The side effect may have:

* completed successfully,
* partially completed,
* or never completed at all.

That information exists outside of Sentinel.

This distinction creates an important boundary:

```text
Execution Authority
    ↓
Who is allowed to act?

Execution Truth
    ↓
What actually happened?
```

Authority is a coordination problem.
Truth is an observation problem.

When authority is lost but execution truth cannot be established, Sentinel surfaces uncertainty rather than pretending to know the answer.

This is why Sentinel does not automatically replay expired executions that have already crossed the execution boundary.

The system knows authority was lost.

The system does not know whether the side effect occurred.

Automatically retrying in that situation risks turning uncertainty into duplication.

Instead, Sentinel preserves the execution state and requires explicit reconciliation (Read the Reconciliation section below for more information).

This is a deliberate design choice:

Sentinel prioritizes correctness of execution authority over automatic progress, and explicit uncertainty over incorrect certainty.

---

## What Sentinel Is Good At

- Payment processing and financial operations
- Webhook ingestion and deduplication
- Distributed task ownership across competing workers
- Long-running jobs where you need heartbeat-backed liveness
- Workflows where the cost of a duplicate is higher than the cost of a manual reconciliation

## What Sentinel Is Not

- A general-purpose task queue (use Celery, Dramatiq, or similar)
- A distributed transaction system
- A guarantee against duplicate side effects in downstream services
- A replacement for idempotency keys at the API layer

Sentinel coordinates *execution*. What happens inside that execution, whether your database write is transactional, whether your API call is idempotent is still your responsibility.

---

## Where Sentinel Fits

Sentinel lives at the boundary between work arriving and work executing, after your queue or stream delivers an event, and before your code touches the outside world.

```text
Kafka / SQS / Flink
        ↓
  event delivered
  to your worker
        ↓
     Sentinel         ← coordination happens here
        ↓
  side effect runs
  (charge card, send email, write DB, call API)
```

Kafka can guarantee exactly-once delivery to your consumer. It cannot guarantee exactly-once execution of what your consumer does next. Sentinel closes that gap.

If your worker crashes after Kafka commits the offset but before the payment goes through, Kafka considers the job done. Sentinel is what catches it.

---

## Why Not Just Use...

**Temporal**

Temporal is a full workflow engine. It manages retries, timelines, activity state, and long-running saga orchestration. It's powerful and the right tool for complex multi-step workflows.

Sentinel is not that. It's a single primitive, coordinate execution of one unit of work, surface the outcome honestly. No workflow DSL, no activity workers, no server to operate. If you're already running Temporal, Sentinel is probably redundant. If you just need to ensure a payment handler doesn't double-execute, Temporal is a lot of infrastructure for a narrow problem.

**Kafka**

Kafka is a durable distributed log. It solves delivery and ordering. It does not solve execution. Sentinel is what you reach for after Kafka has done its job, when the message is in your worker and you need to guarantee what happens next.

**etcd / ZooKeeper**

Both are distributed coordination systems built for infrastructure concerns, leader election, cluster membership, service discovery. They're designed to be run as part of your platform, not called from application code. Using etcd for execution coordination means building the lease model, fencing tokens, and execution state tracking yourself on top of a general-purpose primitive. Sentinel is that layer already built, opinionated, and pointed at application-level execution rather than infrastructure coordination.

**Redis (SETNX / Redlock)**

Redis-based locking is common and fast. It also has well-documented failure modes, Redlock in particular has been the subject of serious distributed systems criticism around clock skew and network partition behavior. More importantly, Redis locks give you mutual exclusion but not execution state. You still have to model claimed vs executing vs completed yourself, and you still have to handle the uncertain outcome when a lock expires mid-execution. Sentinel does all of that. Redis support is on the roadmap as a backend option, but the coordination semantics will remain the same.

**The honest version:** Sentinel is an opinionated, lightweight primitive that makes one specific bet, that explicit uncertainty handling is worth more than automatic retries for correctness-sensitive work. If that bet fits your problem, it's significantly less infrastructure than the alternatives. If it doesn't, use something else.

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

---

### Reading the result

```python
result = sentinel.once(...)

if result.execution_alive:
    # Another worker is actively executing.

elif result.uncertain:
    # Execution truth could not be established.
    # Use reconciliation tooling if needed.

else:
    # If execution_alive and uncertain are both False,
    # response contains either a newly completed result
    # or a cached result from a previous execution.
    return result.response
```

---

### Why Uncertainty matters

This is the state most systems hide from you. It surfaces when a worker claimed execution, entered the side-effect zone, and then disappeared, crashed, froze, timed out. The work may have completed. It may have half-completed. Sentinel doesn't know, and it won't pretend otherwise.

What you do with that is up to you. That's the point.

---

## Execution States

Every execution tracked by Sentinel moves through four states:

| State | Meaning |
|---|---|
| `claimed` | Work has been claimed. Execution hasn't started. Safe to reset and retry. |
| `executing` | Execution has started. Side effects may be in flight. Replay is potentially unsafe. |
| `completed` | Execution finished. Result is cached and reusable. |
| `reconciling` | Execution entered recovery mode. Automatic progress is blocked until reconciliation resolves execution truth. |

The `claimed` → `executing` transition is the important one. Before that boundary, a reset is safe. After it, you're in uncertain territory and Sentinel will tell you so.

---

## Reconciliation

When execution ends up in an uncertain state, Sentinel gives you explicit tools to resolve it rather than forcing a guess.

```python
result = sentinel.once(...)

if result.execution_alive:
    # Another worker is currently executing.

elif result.uncertain:
    # Execution truth could not be established.
    # Reconciliation tools are exposed on this result.

    result.reconcile(...) # Changes state, required before using force_complete and reset
    result.reset(...) # Resets the state, so work can be claimed again for retries
    result.force_complete(...) # Marks the work as completed, response object should also be passed 

else:
    return result.response
```

Example:
```python
result = sentinel.once(...)

if result.uncertain:
    result.reconcile()

    # Verify downstream system.
    payment_exists = check_payment()

    if payment_exists:
        result.force_complete(
            response={"ok": True}
        )
    else:
        result.reset()
```

The typical reconciliation pattern:

1. Detect `result.uncertain` on a result
2. Use `reconcile` to start reconciliation
3. Check your downstream system (did the payment go through?)
4. If yes: `force_complete` with the known result
5. If no or unknown: `reset` and let it retry

This is more work than a silent retry. It's also the only approach that doesn't risk charging a customer twice.

---


## Fencing Tokens

Every lease acquisition generates a monotonically increasing fencing token. Sentinel uses this to reject stale workers, if a worker pauses (GC, network partition, slow disk) and comes back after its lease has expired and been re-acquired by someone else, its operations will be rejected.

This protects against a class of bugs that are easy to miss: the worker that thinks it still holds the lease but doesn't.

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

**Python only.** No Go client, no multi-language support yet. If your workers are polyglot, you'll need a different solution or a coordination service layer in front of Sentinel. Go client currently on the roadmap.

**No built-in retries.** Sentinel coordinates execution. It doesn't implement retry logic, backoff, or dead-letter queues. You bring those or compose them yourself.

**Not a queue.** Sentinel doesn't dispatch work or schedule tasks. It coordinates execution of work you've already routed to a worker.

---

## Known Failure Boundaries

Sentinel intentionally prevents automatic re-execution once work has crossed the execution boundary.

If a worker enters the `executing` state and then crashes, freezes, loses heartbeat authority, or disappears before canonical completion occurs, Sentinel will not automatically restart the work, even after the lease expires.

This is intentional.

At that point, Sentinel cannot safely determine whether the side effect:
- fully completed,
- partially completed,
- or never completed at all.

Instead of risking duplicate execution, Sentinel preserves the execution state and requires explicit reconciliation.

This creates an important tradeoff:

- Sentinel prevents overlapping or duplicate authoritative execution
- But uncertain execution outcomes may require reconciliation logic before progress can continue

This is why expired `executing` states surface as reconciliation-required rather than automatically resetting back to `claimed`.

Sentinel chooses correctness of execution authority over automatic replay.
