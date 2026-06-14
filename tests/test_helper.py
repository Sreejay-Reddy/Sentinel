import time
import pytest
from sentinel.core import acquire, start_execution, complete
from sentinel.helper import validate_and_extend, fetch_cached_response


# ─── VALIDATE AND EXTEND ────────────────────────────────────────────────────

def test_validate_and_extend_succeeds(conn):
    r = acquire(conn, "helper:validate:basic", ttl_ms=5000, hard_ttl_ms=10000)
    result = validate_and_extend(
        conn, "helper:validate:basic",
        owner_id=r.owner_id,
        fencing_token=r.fencing_token,
        ttl_ms=5000
    )
    assert result.success is True
    assert result.status == "claimed"


def test_validate_and_extend_fails_wrong_token(conn):
    r = acquire(conn, "helper:validate:bad_token", ttl_ms=5000, hard_ttl_ms=10000)
    result = validate_and_extend(
        conn, "helper:validate:bad_token",
        owner_id=r.owner_id,
        fencing_token=r.fencing_token + 999,
        ttl_ms=5000
    )
    assert result.success is False


def test_validate_and_extend_fails_on_executing_status(conn):
    r = acquire(conn, "helper:validate:executing", ttl_ms=5000, hard_ttl_ms=10000)
    start_execution(conn, "helper:validate:executing", owner_id=r.owner_id, fencing_token=r.fencing_token)
    result = validate_and_extend(
        conn, "helper:validate:executing",
        owner_id=r.owner_id,
        fencing_token=r.fencing_token,
        ttl_ms=5000
    )
    assert result.success is False


def test_validate_and_extend_fails_expired_lease(conn):
    r = acquire(conn, "helper:validate:expired", ttl_ms=100, hard_ttl_ms=10000)
    time.sleep(0.2)
    result = validate_and_extend(
        conn, "helper:validate:expired",
        owner_id=r.owner_id,
        fencing_token=r.fencing_token,
        ttl_ms=5000
    )
    assert result.success is False


# ─── FETCH CACHED RESPONSE ──────────────────────────────────────────────────

def test_fetch_cached_response_returns_result(conn):
    r = acquire(conn, "helper:cache:basic", ttl_ms=5000, hard_ttl_ms=10000)
    start_execution(conn, "helper:cache:basic", owner_id=r.owner_id, fencing_token=r.fencing_token)
    complete(conn, "helper:cache:basic", owner_id=r.owner_id, fencing_token=r.fencing_token, execution_result={"val": 1})
    cached = fetch_cached_response(conn, "helper:cache:basic")
    assert cached is not None
    assert cached["response"] == {"val": 1}
    assert cached["status"] == "completed"


def test_fetch_cached_response_returns_none_if_not_completed(conn):
    acquire(conn, "helper:cache:not_completed", ttl_ms=5000, hard_ttl_ms=10000)
    cached = fetch_cached_response(conn, "helper:cache:not_completed")
    assert cached is None


def test_fetch_cached_response_returns_none_for_unknown_key(conn):
    cached = fetch_cached_response(conn, "helper:cache:nonexistent")
    assert cached is None
