# Sentinel

A lightweight coordination primitive for backend systems.

## Features
- DB-backed leases
- Fencing tokens (prevents stale worker writes)
- Simple API (acquire, lease)

## Example

```python
with sentinel.lease(conn, "order_123", get_conn=get_conn):
    process_order()
```

To use fencing tokens safely:

1. Store fencing_token in your application data
2. Include fencing_token condition in all writes

Example:

```bash
UPDATE orders
SET status = 'paid', fencing_token = %s
WHERE id = %s AND fencing_token <= %s;
```