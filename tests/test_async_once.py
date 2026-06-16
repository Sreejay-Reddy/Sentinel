import time
import asyncio
import threading
import pytest
import pytest_asyncio
import psycopg
from sentinel import AsyncSentinel
from sentinel.async_reconcilliation import AsyncReconcile

DSN = "postgresql://sentinel_test:sentinel_test@localhost/sentinel_test"

async def get_async_conn():
    return await psycopg.AsyncConnection.connect(DSN)

@pytest_asyncio.fixture
async def asentinel():
    return AsyncSentinel(get_conn=get_async_conn)

# ─── BASIC EXECUTION ────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_async_once_basic_execution(asentinel):
    async def fn():
        return {"ok": True}
    result = await asentinel.once("async:once:basic", fn=fn, ttl_ms=5000, hard_ttl_ms=10000)
    assert result.success is True
    assert result.response == {"ok": True}
    assert result.cached is False
    assert result.uncertain is False
    assert result.execution_alive is None

@pytest.mark.asyncio
async def test_async_once_kwargs_passed_correctly(asentinel):
    async def multiply(x, y):
        return {"product": x * y}
    result = await asentinel.once("async:once:kwargs", fn=multiply, kwargs={"x": 6, "y": 7}, ttl_ms=5000, hard_ttl_ms=10000)
    assert result.response == {"product": 42}

@pytest.mark.asyncio
async def test_async_once_none_response_handled(asentinel):
    async def fn():
        return None
    result = await asentinel.once("async:once:none", fn=fn, ttl_ms=5000, hard_ttl_ms=10000)
    assert result.success is True
    assert result.uncertain is False
    assert result.cached is False

@pytest.mark.asyncio
async def test_async_once_large_payload(asentinel):
    async def fn():
        return {"data": "x" * 10000, "items": list(range(100))}
    result = await asentinel.once("async:once:large", fn=fn, ttl_ms=5000, hard_ttl_ms=10000)
    assert result.response["data"] == "x" * 10000
    assert len(result.response["items"]) == 100

# ─── CACHED REPLAY ──────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_async_once_cached_on_second_call(asentinel):
    call_count = 0
    async def fn():
        nonlocal call_count
        call_count += 1
        return {"count": call_count}
    r1 = await asentinel.once("async:once:cached", fn=fn, ttl_ms=5000, hard_ttl_ms=10000)
    assert r1.cached is False
    assert call_count == 1
    r2 = await asentinel.once("async:once:cached", fn=fn, ttl_ms=5000, hard_ttl_ms=10000)
    assert r2.cached is True
    assert r2.response == {"count": 1}
    assert call_count == 1

@pytest.mark.asyncio
async def test_async_once_cached_none_replayed(asentinel):
    async def fn():
        return None
    await asentinel.once("async:once:cached_none", fn=fn, ttl_ms=5000, hard_ttl_ms=10000)
    async def should_not_run():
        return {"should": "not run"}
    r2 = await asentinel.once("async:once:cached_none", fn=should_not_run, ttl_ms=5000, hard_ttl_ms=10000)
    assert r2.cached is True

# ─── FAILURE AND UNCERTAINTY ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_async_once_fn_raises_returns_uncertain(asentinel):
    async def boom():
        raise ValueError("intentional")
    result = await asentinel.once("async:once:raises", fn=boom, ttl_ms=5000, hard_ttl_ms=10000)
    assert result.success is False
    assert result.uncertain is True
    assert result.execution_alive is False

@pytest.mark.asyncio
async def test_async_once_fn_raises_exception_surfaced(asentinel):
    async def boom():
        raise ValueError("intentional")
    result = await asentinel.once("async:once:exception", fn=boom, ttl_ms=5000, hard_ttl_ms=10000)
    assert result.exception is not None
    assert isinstance(result.exception, ValueError)

@pytest.mark.asyncio
async def test_async_once_second_caller_after_failure_sees_uncertain():
    async def boom():
        raise ValueError("intentional")
    s1 = AsyncSentinel(get_conn=get_async_conn)
    s2 = AsyncSentinel(get_conn=get_async_conn)
    await s1.once("async:once:post_failure", fn=boom, ttl_ms=5000, hard_ttl_ms=30000)
    r2 = await s2.once("async:once:post_failure", fn=boom, ttl_ms=5000, hard_ttl_ms=30000)
    assert r2.uncertain is True
    assert r2.execution_alive is False

# ─── RECONCILE VIA ONCE ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_async_once_uncertain_exposes_reconcile(asentinel):
    async def boom():
        raise ValueError("intentional")
    result = await asentinel.once("async:once:reconcile_exposed", fn=boom, ttl_ms=5000, hard_ttl_ms=10000)
    assert result.uncertain is True
    assert result.reconcile is not None
    assert isinstance(result.reconcile, AsyncReconcile)

@pytest.mark.asyncio
async def test_async_once_execution_alive_no_reconcile():
    ready = asyncio.Event()
    results = {}

    async def slow_fn():
        ready.set()
        await asyncio.sleep(0.5)
        return {"done": True}

    async def run_first():
        s = AsyncSentinel(get_conn=get_async_conn)
        results["first"] = await s.once("async:once:alive", fn=slow_fn, ttl_ms=5000, hard_ttl_ms=10000)

    async def run_second():
        await ready.wait()
        await asyncio.sleep(0.05)
        s = AsyncSentinel(get_conn=get_async_conn)
        results["second"] = await s.once("async:once:alive", fn=slow_fn, ttl_ms=5000, hard_ttl_ms=10000)

    await asyncio.gather(run_first(), run_second())
    assert results["second"].execution_alive is True
    assert results["second"].reconcile is None

# ─── NAMESPACE ──────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_async_once_namespace_isolates_keys():
    s1 = AsyncSentinel(get_conn=get_async_conn, namespace="ns1")
    s2 = AsyncSentinel(get_conn=get_async_conn, namespace="ns2")
    async def fn1(): return {"src": "ns1"}
    async def fn2(): return {"src": "ns2"}
    await s1.once("shared", fn=fn1, ttl_ms=5000, hard_ttl_ms=10000)
    r2 = await s2.once("shared", fn=fn2, ttl_ms=5000, hard_ttl_ms=10000)
    assert r2.response == {"src": "ns2"}
    assert r2.cached is False

@pytest.mark.asyncio
async def test_async_once_namespace_key_prefixing():
    s = AsyncSentinel(get_conn=get_async_conn, namespace="myapp")
    assert s._key("payment-123") == "myapp:payment-123"

@pytest.mark.asyncio
async def test_async_once_no_namespace_key_unchanged():
    s = AsyncSentinel(get_conn=get_async_conn)
    assert s._key("payment-123") == "payment-123"

# ─── CONTENTION ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_async_once_concurrent_only_one_executes():
    call_count = 0

    async def slow_fn():
        nonlocal call_count
        await asyncio.sleep(0.2)
        call_count += 1
        return {"count": call_count}

    results = await asyncio.gather(*[
        AsyncSentinel(get_conn=get_async_conn).once(
            "async:once:concurrent", fn=slow_fn, ttl_ms=5000, hard_ttl_ms=10000
        )
        for _ in range(5)
    ])

    assert call_count == 1
    successes = [r for r in results if r.success]
    assert len(successes) >= 1