# Sentinel

A lightweight coordination primitive for backend systems.

---

## Features

* DB-backed leases
* Fencing tokens (prevents stale worker writes)
* Simple API (`acquire`, `heartbeat`, `release`, `lease`)

---

## Example

```python
import sentinel

with sentinel.lease(conn, "order_123", ttl_ms=5000, get_conn=get_conn):
    process_order()
```

---

## API

### acquire

```python
result = sentinel.acquire(conn, key, ttl_ms=5000)
```

Returns:

* `acquired` (bool)
* `owner_id` (str)
* `expires_at` (timestamp)
* `fencing_token` (int)

---

### heartbeat

```python
result = sentinel.heartbeat(conn, key, owner_id, ttl_ms=5000)
```

Returns:

* `extended` (bool)

---

### release

```python
result = sentinel.release(conn, key, owner_id)
```

Returns:

* `released` (bool)

---

### lease (recommended)

```python
with sentinel.lease(conn, key, ttl_ms=5000, get_conn=get_conn):
    do_work()
```

Handles:

* acquire
* automatic heartbeat
* release on exit

---

## Fencing Tokens (Important)

Each acquire returns a **monotonically increasing fencing token**.

To use fencing tokens safely:

1. Store `fencing_token` in your application data
2. Include a condition in all writes

Example:

```sql
UPDATE orders
SET status = 'paid', fencing_token = %s
WHERE id = %s AND fencing_token <= %s;
```

This ensures:

* stale workers cannot overwrite newer ones
* race conditions are safe

---

## Use Cases

* Background workers
* Job queues
* Payment processing
* Idempotent APIs

---

## Status

⚠️ Beta (v0.1.0)

---

## Limitations

* No retries yet
* No async support
* TTL tuning required
* External side effects must be handled separately

---

## Philosophy

Sentinel does not eliminate race conditions.
It makes them **safe**.
