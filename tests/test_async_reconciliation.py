import time
import pytest
import pytest_asyncio
import psycopg
from sentinel.async_core import acquire, start_execution
from sentinel.async_reconcilliation import AsyncReconcile

DSN = "postgresql://sentinel_test:sentinel_test@localhost/sentinel_test"

async def get_async_conn():
    return await psycopg.AsyncConnection.connect(DSN)

@pytest_asyncio.fixture
async def areconcile():
    return AsyncReconcile(get_conn=get_async_conn)

# ─── RECONCILE ──────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_async_reconcile_marks_reconciling(aconn, areconcile):
    r = await acquire(aconn, "async:rec:reconcile:basic", ttl_ms=100, hard_ttl_ms=10000)
    await start_execution(aconn, "async:rec:reconcile:basic", owner_id=r.owner_id, fencing_token=r.fencing_token)
    time.sleep(0.2)
    result = await areconcile.reconcile("async:rec:reconcile:basic")
    assert result.success is True

@pytest.mark.asyncio
async def test_async_reconcile_fails_if_lease_still_alive(aconn, areconcile):
    r = await acquire(aconn, "async:rec:reconcile:alive", ttl_ms=5000, hard_ttl_ms=10000)
    await start_execution(aconn, "async:rec:reconcile:alive", owner_id=r.owner_id, fencing_token=r.fencing_token)
    result = await areconcile.reconcile("async:rec:reconcile:alive")
    assert result.success is False

# ─── FORCE COMPLETE ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_async_force_complete_from_reconciling(aconn, areconcile):
    r = await acquire(aconn, "async:rec:force:basic", ttl_ms=100, hard_ttl_ms=10000)
    await start_execution(aconn, "async:rec:force:basic", owner_id=r.owner_id, fencing_token=r.fencing_token)
    time.sleep(0.2)
    await areconcile.reconcile("async:rec:force:basic")
    result = await areconcile.force_complete(
        "async:rec:force:basic",
        execution_result='{"value": 1}'
    )
    assert result.success is True

@pytest.mark.asyncio
async def test_async_force_complete_fails_if_not_reconciling(aconn, areconcile):
    r = await acquire(aconn, "async:rec:force:bad_status", ttl_ms=5000, hard_ttl_ms=10000)
    await start_execution(aconn, "async:rec:force:bad_status", owner_id=r.owner_id, fencing_token=r.fencing_token)
    result = await areconcile.force_complete(
        "async:rec:force:bad_status",
        execution_result='{"value": 1}'
    )
    assert result.success is False

# ─── RESET ──────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_async_reset_allows_rerun(aconn, areconcile):
    r = await acquire(aconn, "async:rec:reset:basic", ttl_ms=100, hard_ttl_ms=10000)
    await start_execution(aconn, "async:rec:reset:basic", owner_id=r.owner_id, fencing_token=r.fencing_token)
    time.sleep(0.2)
    await areconcile.reconcile("async:rec:reset:basic")
    result = await areconcile.reset("async:rec:reset:basic")
    assert result.success is True

@pytest.mark.asyncio
async def test_async_reset_fails_if_not_reconciling(aconn, areconcile):
    r = await acquire(aconn, "async:rec:reset:bad_status", ttl_ms=5000, hard_ttl_ms=10000)
    await start_execution(aconn, "async:rec:reset:bad_status", owner_id=r.owner_id, fencing_token=r.fencing_token)
    result = await areconcile.reset("async:rec:reset:bad_status")
    assert result.success is False