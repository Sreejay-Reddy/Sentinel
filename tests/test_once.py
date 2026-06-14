import time
import threading
import pytest
from sentinel.sentinel import Sentinel
from sentinel.reconciliation import Reconcile


@pytest.fixture
def sentinel(get_conn_fixture):
    return Sentinel(get_conn=get_conn_fixture)


# ─── BASIC EXECUTION ────────────────────────────────────────────────────────

def test_once_basic_execution(sentinel):
    result = sentinel.once("once:basic", fn=lambda: {"ok": True}, ttl_ms=5000, hard_ttl_ms=10000)
    assert result.success is True
    assert result.response == {"ok": True}
    assert result.cached is False
    assert result.uncertain is False
    assert result.execution_alive is None


def test_once_kwargs_passed_correctly(sentinel):
    def multiply(x, y):
        return {"product": x * y}

    result = sentinel.once("once:kwargs", fn=multiply, kwargs={"x": 6, "y": 7}, ttl_ms=5000, hard_ttl_ms=10000)
    assert result.response == {"product": 42}


def test_once_none_response_handled(sentinel):
    result = sentinel.once("once:none", fn=lambda: None, ttl_ms=5000, hard_ttl_ms=10000)
    assert result.success is True
    assert result.uncertain is False
    assert result.cached is False


def test_once_large_payload(sentinel):
    big = {"data": "x" * 10000, "items": list(range(100))}
    result = sentinel.once("once:large", fn=lambda: big, ttl_ms=5000, hard_ttl_ms=10000)
    assert result.response["data"] == "x" * 10000
    assert len(result.response["items"]) == 100


# ─── CACHED REPLAY ──────────────────────────────────────────────────────────

def test_once_cached_on_second_call(sentinel):
    call_count = 0

    def fn():
        nonlocal call_count
        call_count += 1
        return {"count": call_count}

    r1 = sentinel.once("once:cached", fn=fn, ttl_ms=5000, hard_ttl_ms=10000)
    assert r1.cached is False
    assert call_count == 1

    r2 = sentinel.once("once:cached", fn=fn, ttl_ms=5000, hard_ttl_ms=10000)
    assert r2.cached is True
    assert r2.response == {"count": 1}
    assert call_count == 1


def test_once_cached_none_replayed(sentinel):
    sentinel.once("once:cached_none", fn=lambda: None, ttl_ms=5000, hard_ttl_ms=10000)
    r2 = sentinel.once("once:cached_none", fn=lambda: {"should": "not run"}, ttl_ms=5000, hard_ttl_ms=10000)
    assert r2.cached is True


# ─── FAILURE AND UNCERTAINTY ────────────────────────────────────────────────

def test_once_fn_raises_returns_uncertain(sentinel):
    def boom():
        raise ValueError("intentional")

    result = sentinel.once("once:raises", fn=boom, ttl_ms=5000, hard_ttl_ms=10000)
    assert result.success is False
    assert result.uncertain is True
    assert result.execution_alive is False


def test_once_fn_raises_exception_surfaced(sentinel):
    def boom():
        raise ValueError("intentional")

    result = sentinel.once("once:exception", fn=boom, ttl_ms=5000, hard_ttl_ms=10000)
    assert result.exception is not None
    assert isinstance(result.exception, ValueError)


def test_once_second_caller_after_failure_sees_uncertain(get_conn_fixture):
    def boom():
        raise ValueError("intentional")

    s1 = Sentinel(get_conn=get_conn_fixture)
    s2 = Sentinel(get_conn=get_conn_fixture)

    s1.once("once:post_failure", fn=boom, ttl_ms=5000, hard_ttl_ms=30000)
    r2 = s2.once("once:post_failure", fn=boom, ttl_ms=5000, hard_ttl_ms=30000)

    assert r2.uncertain is True
    assert r2.execution_alive is False


# ─── RECONCILE EXPOSED ──────────────────────────────────────────────────────

def test_once_uncertain_exposes_reconcile(sentinel):
    def boom():
        raise ValueError("intentional")

    result = sentinel.once("once:reconcile_exposed", fn=boom, ttl_ms=5000, hard_ttl_ms=10000)
    assert result.uncertain is True
    assert result.reconcile is not None
    assert isinstance(result.reconcile, Reconcile)


def test_once_execution_alive_no_reconcile(get_conn_fixture):
    barrier = threading.Barrier(2)
    results = []

    def slow_fn():
        barrier.wait()
        time.sleep(0.5)
        return {"done": True}

    def run_first():
        s = Sentinel(get_conn=get_conn_fixture)
        results.append(("first", s.once("once:alive", fn=slow_fn, ttl_ms=5000, hard_ttl_ms=10000)))

    def run_second():
        barrier.wait()
        time.sleep(0.05)
        s = Sentinel(get_conn=get_conn_fixture)
        results.append(("second", s.once("once:alive", fn=slow_fn, ttl_ms=5000, hard_ttl_ms=10000)))

    t1 = threading.Thread(target=run_first)
    t2 = threading.Thread(target=run_second)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    second_result = next(r for label, r in results if label == "second")
    assert second_result.execution_alive is True
    assert second_result.reconcile is None


# ─── NAMESPACE ──────────────────────────────────────────────────────────────

def test_once_namespace_isolates_keys(get_conn_fixture):
    s1 = Sentinel(get_conn=get_conn_fixture, namespace="ns1")
    s2 = Sentinel(get_conn=get_conn_fixture, namespace="ns2")

    s1.once("shared", fn=lambda: {"src": "ns1"}, ttl_ms=5000, hard_ttl_ms=10000)
    r2 = s2.once("shared", fn=lambda: {"src": "ns2"}, ttl_ms=5000, hard_ttl_ms=10000)

    assert r2.response == {"src": "ns2"}
    assert r2.cached is False


def test_once_namespace_key_prefixing(get_conn_fixture):
    s = Sentinel(get_conn=get_conn_fixture, namespace="myapp")
    assert s._key("payment-123") == "myapp:payment-123"


def test_once_no_namespace_key_unchanged(get_conn_fixture):
    s = Sentinel(get_conn=get_conn_fixture)
    assert s._key("payment-123") == "payment-123"


# ─── CONTENTION ─────────────────────────────────────────────────────────────

def test_once_concurrent_only_one_executes(get_conn_fixture):
    call_count = 0
    lock = threading.Lock()

    def slow_fn():
        nonlocal call_count
        time.sleep(0.2)
        with lock:
            call_count += 1
        return {"count": call_count}

    results = []
    errors = []

    def run():
        try:
            s = Sentinel(get_conn=get_conn_fixture)
            results.append(s.once("once:concurrent", fn=slow_fn, ttl_ms=5000, hard_ttl_ms=10000))
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=run) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"Threads raised: {errors}"
    assert call_count == 1
    successes = [r for r in results if r.success]
    assert len(successes) >= 1
