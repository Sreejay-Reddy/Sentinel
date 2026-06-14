import time
import pytest
from sentinel.core import (
    acquire,
    start_execution,
    heartbeat,
    complete,
    expire_lease,
    release,
)


# ─── ACQUIRE ────────────────────────────────────────────────────────────────

def test_acquire_fresh_key(conn):
    r = acquire(conn, "core:acquire:fresh", ttl_ms=5000, hard_ttl_ms=10000)
    assert r.acquired is True
    assert r.owner_id is not None
    assert r.fencing_token is not None
    assert r.status == "claimed"
    assert r.lease_alive is True


def test_acquire_same_key_twice_fails(conn):
    acquire(conn, "core:acquire:double", ttl_ms=5000, hard_ttl_ms=10000)
    r2 = acquire(conn, "core:acquire:double", ttl_ms=5000, hard_ttl_ms=10000)
    assert r2.acquired is False


def test_acquire_after_lease_expiry_succeeds(conn):
    r1 = acquire(conn, "core:acquire:expiry", ttl_ms=100, hard_ttl_ms=200)
    assert r1.acquired is True
    time.sleep(0.25)
    r2 = acquire(conn, "core:acquire:expiry", ttl_ms=5000, hard_ttl_ms=10000)
    assert r2.acquired is True
    assert r2.fencing_token != r1.fencing_token


def test_acquire_executing_key_not_acquirable(conn):
    r1 = acquire(conn, "core:acquire:executing", ttl_ms=5000, hard_ttl_ms=10000)
    start_execution(conn, "core:acquire:executing", owner_id=r1.owner_id, fencing_token=r1.fencing_token)
    r2 = acquire(conn, "core:acquire:executing", ttl_ms=5000, hard_ttl_ms=10000)
    assert r2.acquired is False
    assert r2.status == "executing"


def test_acquire_completed_key_not_acquirable(conn):
    r1 = acquire(conn, "core:acquire:completed", ttl_ms=5000, hard_ttl_ms=10000)
    start_execution(conn, "core:acquire:completed", owner_id=r1.owner_id, fencing_token=r1.fencing_token)
    complete(conn, "core:acquire:completed", owner_id=r1.owner_id, fencing_token=r1.fencing_token, execution_result={"ok": True})
    r2 = acquire(conn, "core:acquire:completed", ttl_ms=5000, hard_ttl_ms=10000)
    assert r2.acquired is False
    assert r2.status == "completed"


def test_acquire_fencing_tokens_are_unique(conn):
    r1 = acquire(conn, "core:acquire:tokens", ttl_ms=100, hard_ttl_ms=200)
    time.sleep(0.25)
    r2 = acquire(conn, "core:acquire:tokens", ttl_ms=5000, hard_ttl_ms=10000)
    assert r1.fencing_token != r2.fencing_token


def test_acquire_returns_status_on_failure(conn):
    acquire(conn, "core:acquire:status", ttl_ms=5000, hard_ttl_ms=10000)
    r2 = acquire(conn, "core:acquire:status", ttl_ms=5000, hard_ttl_ms=10000)
    assert r2.status == "claimed"


# ─── START EXECUTION ────────────────────────────────────────────────────────

def test_start_execution_transitions_to_executing(conn):
    r = acquire(conn, "core:start:basic", ttl_ms=5000, hard_ttl_ms=10000)
    result = start_execution(conn, "core:start:basic", owner_id=r.owner_id, fencing_token=r.fencing_token)
    assert result.success is True
    assert result.status == "executing"


def test_start_execution_wrong_token_fails(conn):
    r = acquire(conn, "core:start:bad_token", ttl_ms=5000, hard_ttl_ms=10000)
    result = start_execution(conn, "core:start:bad_token", owner_id=r.owner_id, fencing_token=r.fencing_token + 999)
    assert result.success is False


def test_start_execution_wrong_status_fails(conn):
    r = acquire(conn, "core:start:bad_status", ttl_ms=5000, hard_ttl_ms=10000)
    start_execution(conn, "core:start:bad_status", owner_id=r.owner_id, fencing_token=r.fencing_token)
    result = start_execution(conn, "core:start:bad_status", owner_id=r.owner_id, fencing_token=r.fencing_token)
    assert result.success is False


# ─── HEARTBEAT ──────────────────────────────────────────────────────────────

def test_heartbeat_extends_lease(conn):
    r = acquire(conn, "core:hb:extends", ttl_ms=500, hard_ttl_ms=10000)
    start_execution(conn, "core:hb:extends", owner_id=r.owner_id, fencing_token=r.fencing_token)
    time.sleep(0.3)
    hb = heartbeat(conn, "core:hb:extends", r.owner_id, r.fencing_token, ttl_ms=5000)
    assert hb.success is True
    r2 = acquire(conn, "core:hb:extends", ttl_ms=500, hard_ttl_ms=10000)
    assert r2.acquired is False


def test_heartbeat_fails_wrong_token(conn):
    r = acquire(conn, "core:hb:bad_token", ttl_ms=5000, hard_ttl_ms=10000)
    start_execution(conn, "core:hb:bad_token", owner_id=r.owner_id, fencing_token=r.fencing_token)
    hb = heartbeat(conn, "core:hb:bad_token", r.owner_id, r.fencing_token + 999, ttl_ms=5000)
    assert hb.success is False


def test_heartbeat_fails_after_hard_ttl(conn):
    r = acquire(conn, "core:hb:hard_ttl", ttl_ms=100, hard_ttl_ms=200)
    start_execution(conn, "core:hb:hard_ttl", owner_id=r.owner_id, fencing_token=r.fencing_token)
    time.sleep(0.3)
    hb = heartbeat(conn, "core:hb:hard_ttl", r.owner_id, r.fencing_token, ttl_ms=5000)
    assert hb.success is False


def test_heartbeat_fails_on_claimed_status(conn):
    r = acquire(conn, "core:hb:claimed", ttl_ms=5000, hard_ttl_ms=10000)
    hb = heartbeat(conn, "core:hb:claimed", r.owner_id, r.fencing_token, ttl_ms=5000)
    assert hb.success is False


# ─── COMPLETE ───────────────────────────────────────────────────────────────

def test_complete_transitions_to_completed(conn):
    r = acquire(conn, "core:complete:basic", ttl_ms=5000, hard_ttl_ms=10000)
    start_execution(conn, "core:complete:basic", owner_id=r.owner_id, fencing_token=r.fencing_token)
    result = complete(conn, "core:complete:basic", owner_id=r.owner_id, fencing_token=r.fencing_token, execution_result={"ok": True})
    assert result.success is True


def test_complete_stores_result(conn):
    from sentinel.helper import fetch_cached_response
    r = acquire(conn, "core:complete:stores", ttl_ms=5000, hard_ttl_ms=10000)
    start_execution(conn, "core:complete:stores", owner_id=r.owner_id, fencing_token=r.fencing_token)
    complete(conn, "core:complete:stores", owner_id=r.owner_id, fencing_token=r.fencing_token, execution_result={"value": 42})
    cached = fetch_cached_response(conn, "core:complete:stores")
    assert cached is not None
    assert cached["response"] == {"value": 42}


def test_complete_wrong_token_fails(conn):
    r = acquire(conn, "core:complete:bad_token", ttl_ms=5000, hard_ttl_ms=10000)
    start_execution(conn, "core:complete:bad_token", owner_id=r.owner_id, fencing_token=r.fencing_token)
    result = complete(conn, "core:complete:bad_token", owner_id=r.owner_id, fencing_token=r.fencing_token + 999)
    assert result.success is False


def test_complete_stores_none_result(conn):
    from sentinel.helper import fetch_cached_response
    r = acquire(conn, "core:complete:none", ttl_ms=5000, hard_ttl_ms=10000)
    start_execution(conn, "core:complete:none", owner_id=r.owner_id, fencing_token=r.fencing_token)
    complete(conn, "core:complete:none", owner_id=r.owner_id, fencing_token=r.fencing_token, execution_result=None)
    cached = fetch_cached_response(conn, "core:complete:none")
    assert cached is not None
    assert cached["response"] is None


# ─── EXPIRE LEASE ───────────────────────────────────────────────────────────

def test_expire_lease_kills_lease(conn):
    r = acquire(conn, "core:expire:basic", ttl_ms=5000, hard_ttl_ms=10000)
    start_execution(conn, "core:expire:basic", owner_id=r.owner_id, fencing_token=r.fencing_token)
    result = expire_lease(conn, "core:expire:basic", owner_id=r.owner_id, fencing_token=r.fencing_token)
    assert result.success is True


def test_expire_lease_stops_heartbeat(conn):
    r = acquire(conn, "core:expire:heartbeat", ttl_ms=5000, hard_ttl_ms=10000)
    start_execution(conn, "core:expire:heartbeat", owner_id=r.owner_id, fencing_token=r.fencing_token)
    expire_lease(conn, "core:expire:heartbeat", owner_id=r.owner_id, fencing_token=r.fencing_token)
    hb = heartbeat(conn, "core:expire:heartbeat", r.owner_id, r.fencing_token, ttl_ms=5000)
    assert hb.success is False


def test_expire_lease_fails_on_claimed_status(conn):
    r = acquire(conn, "core:expire:claimed", ttl_ms=5000, hard_ttl_ms=10000)
    result = expire_lease(conn, "core:expire:claimed", owner_id=r.owner_id, fencing_token=r.fencing_token)
    assert result.success is False


# ─── RELEASE ────────────────────────────────────────────────────────────────

def test_release_allows_reacquire(conn):
    r = acquire(conn, "core:release:basic", ttl_ms=5000, hard_ttl_ms=10000)
    release(conn, "core:release:basic", owner_id=r.owner_id, fencing_token=r.fencing_token)
    r2 = acquire(conn, "core:release:basic", ttl_ms=5000, hard_ttl_ms=10000)
    assert r2.acquired is True


def test_release_wrong_token_fails(conn):
    r = acquire(conn, "core:release:bad_token", ttl_ms=5000, hard_ttl_ms=10000)
    result = release(conn, "core:release:bad_token", owner_id=r.owner_id, fencing_token=r.fencing_token + 999)
    assert result.success is False
    r2 = acquire(conn, "core:release:bad_token", ttl_ms=5000, hard_ttl_ms=10000)
    assert r2.acquired is False
