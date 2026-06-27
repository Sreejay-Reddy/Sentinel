import pytest
import pytest_asyncio
import asyncio
import psycopg
from sentinel.async_core import acquire, start_execution, complete
from sentinel.events import history, SentinelEvent

DSN = "postgresql://sentinel_test:sentinel_test@localhost/sentinel_test"

# ─── HISTORY ────────────────────────────────────────────────────────────────

from sentinel.events import history, SentinelEvent

@pytest.mark.asyncio
async def test_async_history_happy_path(aconn, conn):
    key = "async:history:happy"
    r = await acquire(aconn, key, ttl_ms=5000, hard_ttl_ms=10000)
    await start_execution(aconn, key, owner_id=r.owner_id, fencing_token=r.fencing_token)
    await complete(aconn, key, owner_id=r.owner_id, fencing_token=r.fencing_token, execution_result={"ok": True})

    events = history(conn, key)
    event_types = [e.event for e in events]

    assert event_types == [
        SentinelEvent.ACQUIRED.value,
        SentinelEvent.EXECUTING.value,
        SentinelEvent.COMPLETED.value,
    ]
    assert all(e.fencing_token == r.fencing_token for e in events)

@pytest.mark.asyncio
async def test_async_history_reconciliation_path(aconn, conn):
    from sentinel.async_reconcilliation import AsyncReconcile

    key = "async:history:reconcile"
    r = await acquire(aconn, key, ttl_ms=100, hard_ttl_ms=200)
    await start_execution(aconn, key, owner_id=r.owner_id, fencing_token=r.fencing_token)
    await asyncio.sleep(0.25)

    async def get_conn():
        return await psycopg.AsyncConnection.connect(DSN)

    reconciler = AsyncReconcile(get_conn)
    await reconciler.reconcile(key)
    await reconciler.reset(key)

    events = history(conn, key)
    event_types = [e.event for e in events]

    assert SentinelEvent.ACQUIRED.value in event_types
    assert SentinelEvent.EXECUTING.value in event_types
    assert SentinelEvent.RECONCILING.value in event_types
    assert SentinelEvent.RESET.value in event_types

    reconciling_token = next(e.fencing_token for e in events if e.event == SentinelEvent.RECONCILING.value)
    assert reconciling_token != r.fencing_token