import time
import pytest
import pytest_asyncio
import asyncio
import psycopg
from sentinel.async_core import (
    acquire,
    start_execution,
    heartbeat,
    complete,
    expire_lease,
    release,
    inspect,
)


# ─── ACQUIRE ────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_async_acquire_fresh_key(aconn):
    r = await acquire(aconn, "async:core:acquire:fresh", ttl_ms=5000, hard_ttl_ms=10000)
    assert r.acquired is True
    assert r.owner_id is not None
    assert r.fencing_token is not None
    assert r.status == "claimed"
    assert r.lease_alive is True

@pytest.mark.asyncio
async def test_async_acquire_same_key_twice_fails(aconn):
    await acquire(aconn, "async:core:acquire:double", ttl_ms=5000, hard_ttl_ms=10000)
    r2 = await acquire(aconn, "async:core:acquire:double", ttl_ms=5000, hard_ttl_ms=10000)
    assert r2.acquired is False

@pytest.mark.asyncio
async def test_async_acquire_after_lease_expiry_succeeds(aconn):
    r1 = await acquire(aconn, "async:core:acquire:expiry", ttl_ms=100, hard_ttl_ms=200)
    assert r1.acquired is True
    time.sleep(0.25)
    r2 = await acquire(aconn, "async:core:acquire:expiry", ttl_ms=5000, hard_ttl_ms=10000)
    assert r2.acquired is True
    assert r2.fencing_token != r1.fencing_token

@pytest.mark.asyncio
async def test_async_acquire_executing_key_not_acquirable(aconn):
    r1 = await acquire(aconn, "async:core:acquire:executing", ttl_ms=5000, hard_ttl_ms=10000)
    await start_execution(aconn, "async:core:acquire:executing", owner_id=r1.owner_id, fencing_token=r1.fencing_token)
    r2 = await acquire(aconn, "async:core:acquire:executing", ttl_ms=5000, hard_ttl_ms=10000)
    assert r2.acquired is False
    assert r2.status == "executing"

@pytest.mark.asyncio
async def test_async_acquire_fencing_tokens_are_unique(aconn):
    r1 = await acquire(aconn, "async:core:acquire:tokens", ttl_ms=100, hard_ttl_ms=200)
    time.sleep(0.25)
    r2 = await acquire(aconn, "async:core:acquire:tokens", ttl_ms=5000, hard_ttl_ms=10000)
    assert r1.fencing_token != r2.fencing_token

# ─── START EXECUTION ────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_async_start_execution_transitions_to_executing(aconn):
    r = await acquire(aconn, "async:core:start:basic", ttl_ms=5000, hard_ttl_ms=10000)
    result = await start_execution(aconn, "async:core:start:basic", owner_id=r.owner_id, fencing_token=r.fencing_token)
    assert result.success is True
    assert result.status == "executing"

@pytest.mark.asyncio
async def test_async_start_execution_wrong_token_fails(aconn):
    r = await acquire(aconn, "async:core:start:bad_token", ttl_ms=5000, hard_ttl_ms=10000)
    result = await start_execution(aconn, "async:core:start:bad_token", owner_id=r.owner_id, fencing_token=r.fencing_token + 999)
    assert result.success is False

# ─── HEARTBEAT ──────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_async_heartbeat_extends_lease(aconn):
    r = await acquire(aconn, "async:core:hb:extends", ttl_ms=500, hard_ttl_ms=10000)
    await start_execution(aconn, "async:core:hb:extends", owner_id=r.owner_id, fencing_token=r.fencing_token)
    time.sleep(0.3)
    hb = await heartbeat(aconn, "async:core:hb:extends", r.owner_id, r.fencing_token, ttl_ms=5000)
    assert hb.success is True

@pytest.mark.asyncio
async def test_async_heartbeat_fails_wrong_token(aconn):
    r = await acquire(aconn, "async:core:hb:bad_token", ttl_ms=5000, hard_ttl_ms=10000)
    await start_execution(aconn, "async:core:hb:bad_token", owner_id=r.owner_id, fencing_token=r.fencing_token)
    hb = await heartbeat(aconn, "async:core:hb:bad_token", r.owner_id, r.fencing_token + 999, ttl_ms=5000)
    assert hb.success is False

@pytest.mark.asyncio
async def test_async_heartbeat_fails_after_hard_ttl(aconn):
    r = await acquire(aconn, "async:core:hb:hard_ttl", ttl_ms=100, hard_ttl_ms=200)
    await start_execution(aconn, "async:core:hb:hard_ttl", owner_id=r.owner_id, fencing_token=r.fencing_token)
    time.sleep(0.3)
    hb = await heartbeat(aconn, "async:core:hb:hard_ttl", r.owner_id, r.fencing_token, ttl_ms=5000)
    assert hb.success is False

@pytest.mark.asyncio
async def test_async_heartbeat_fails_on_claimed_status(aconn):
    r = await acquire(aconn, "async:core:hb:claimed", ttl_ms=5000, hard_ttl_ms=10000)
    hb = await heartbeat(aconn, "async:core:hb:claimed", r.owner_id, r.fencing_token, ttl_ms=5000)
    assert hb.success is False

# ─── COMPLETE ───────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_async_complete_transitions_to_completed(aconn):
    r = await acquire(aconn, "async:core:complete:basic", ttl_ms=5000, hard_ttl_ms=10000)
    await start_execution(aconn, "async:core:complete:basic", owner_id=r.owner_id, fencing_token=r.fencing_token)
    result = await complete(aconn, "async:core:complete:basic", owner_id=r.owner_id, fencing_token=r.fencing_token, execution_result={"ok": True})
    assert result.success is True

@pytest.mark.asyncio
async def test_async_complete_wrong_token_fails(aconn):
    r = await acquire(aconn, "async:core:complete:bad_token", ttl_ms=5000, hard_ttl_ms=10000)
    await start_execution(aconn, "async:core:complete:bad_token", owner_id=r.owner_id, fencing_token=r.fencing_token)
    result = await complete(aconn, "async:core:complete:bad_token", owner_id=r.owner_id, fencing_token=r.fencing_token + 999)
    assert result.success is False

# ─── EXPIRE LEASE ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_async_expire_lease_kills_lease(aconn):
    r = await acquire(aconn, "async:core:expire:basic", ttl_ms=5000, hard_ttl_ms=10000)
    await start_execution(aconn, "async:core:expire:basic", owner_id=r.owner_id, fencing_token=r.fencing_token)
    result = await expire_lease(aconn, "async:core:expire:basic", owner_id=r.owner_id, fencing_token=r.fencing_token)
    assert result.success is True

@pytest.mark.asyncio
async def test_async_expire_lease_stops_heartbeat(aconn):
    r = await acquire(aconn, "async:core:expire:heartbeat", ttl_ms=5000, hard_ttl_ms=10000)
    await start_execution(aconn, "async:core:expire:heartbeat", owner_id=r.owner_id, fencing_token=r.fencing_token)
    await expire_lease(aconn, "async:core:expire:heartbeat", owner_id=r.owner_id, fencing_token=r.fencing_token)
    hb = await heartbeat(aconn, "async:core:expire:heartbeat", r.owner_id, r.fencing_token, ttl_ms=5000)
    assert hb.success is False

# ─── RELEASE ────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_async_release_allows_reacquire(aconn):
    r = await acquire(aconn, "async:core:release:basic", ttl_ms=5000, hard_ttl_ms=10000)
    await release(aconn, "async:core:release:basic", owner_id=r.owner_id, fencing_token=r.fencing_token)
    r2 = await acquire(aconn, "async:core:release:basic", ttl_ms=5000, hard_ttl_ms=10000)
    assert r2.acquired is True


# ─── INSPECT ────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_async_inspect_missing_key(aconn):
    result = await inspect(
        aconn,
        "async:inspect:missing",
    )

    assert result is None


@pytest.mark.asyncio
async def test_async_inspect_executing(aconn):
    r = await acquire(
        aconn,
        "async:inspect:executing",
        ttl_ms=5000,
        hard_ttl_ms=10000,
    )

    await start_execution(
        aconn,
        "async:inspect:executing",
        owner_id=r.owner_id,
        fencing_token=r.fencing_token,
    )

    result = await inspect(
        aconn,
        "async:inspect:executing",
    )

    assert result.key == "async:inspect:executing"
    assert result.status == "executing"
    assert result.lease_alive is True


@pytest.mark.asyncio
async def test_async_inspect_completed(aconn):
    r = await acquire(
        aconn,
        "async:inspect:completed",
        ttl_ms=5000,
        hard_ttl_ms=10000,
    )

    await start_execution(
        aconn,
        "async:inspect:completed",
        owner_id=r.owner_id,
        fencing_token=r.fencing_token,
    )

    await complete(
        aconn,
        "async:inspect:completed",
        execution_result={"ok": True},
        owner_id=r.owner_id,
        fencing_token=r.fencing_token,
    )

    result = await inspect(
        aconn,
        "async:inspect:completed",
    )

    assert result.status == "completed"
    assert result.execution_result == {"ok": True}


@pytest.mark.asyncio
async def test_async_inspect_expired_execution(aconn):
    r = await acquire(
        aconn,
        "async:inspect:expired",
        ttl_ms=100,
        hard_ttl_ms=10000,
    )

    await start_execution(
        aconn,
        "async:inspect:expired",
        owner_id=r.owner_id,
        fencing_token=r.fencing_token,
    )

    await asyncio.sleep(0.2)

    result = await inspect(
        aconn,
        "async:inspect:expired",
    )

    assert result.lease_alive is False