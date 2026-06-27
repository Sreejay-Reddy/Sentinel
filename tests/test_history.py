import time
import pytest
import psycopg
from sentinel.core import acquire, start_execution, complete
from sentinel.events import history, SentinelEvent

DSN = "postgresql://sentinel_test:sentinel_test@localhost/sentinel_test"

def test_sync_history_happy_path(conn):
    key = "sync:history:happy"
    r = acquire(conn, key, ttl_ms=5000, hard_ttl_ms=10000)
    start_execution(conn, key, owner_id=r.owner_id, fencing_token=r.fencing_token)
    complete(conn, key, owner_id=r.owner_id, fencing_token=r.fencing_token, execution_result={"ok": True})

    events = history(conn, key)
    event_types = [e.event for e in events]

    assert event_types == [
        SentinelEvent.ACQUIRED.value,
        SentinelEvent.EXECUTING.value,
        SentinelEvent.COMPLETED.value,
    ]
    assert all(e.fencing_token == r.fencing_token for e in events)

def test_sync_history_reconciliation_path(conn):
    from sentinel.reconciliation import Reconcile

    key = "sync:history:reconcile"
    r = acquire(conn, key, ttl_ms=100, hard_ttl_ms=200)
    start_execution(conn, key, owner_id=r.owner_id, fencing_token=r.fencing_token)
    time.sleep(0.25)

    reconciler = Reconcile(lambda: psycopg.connect(DSN))
    reconciler.reconcile(key)
    reconciler.reset(key)

    events = history(conn, key)
    event_types = [e.event for e in events]

    assert SentinelEvent.ACQUIRED.value in event_types
    assert SentinelEvent.EXECUTING.value in event_types
    assert SentinelEvent.RECONCILING.value in event_types
    assert SentinelEvent.RESET.value in event_types

    reconciling_token = next(e.fencing_token for e in events if e.event == SentinelEvent.RECONCILING.value)
    assert reconciling_token != r.fencing_token