import time
import pytest
from sentinel.core import acquire, start_execution, complete
from sentinel.reconciliation import Reconcile


@pytest.fixture
def reconcile(get_conn_fixture):
    return Reconcile(get_conn_fixture)


# ─── RECONCILE ──────────────────────────────────────────────────────────────

def test_reconcile_marks_reconciling(conn, reconcile):
    r = acquire(conn, "rec:reconcile:basic", ttl_ms=100, hard_ttl_ms=10000)
    start_execution(conn, "rec:reconcile:basic", owner_id=r.owner_id, fencing_token=r.fencing_token)
    time.sleep(0.2)
    result = reconcile.reconcile("rec:reconcile:basic")
    assert result.success is True


def test_reconcile_fails_if_lease_still_alive(conn, reconcile):
    r = acquire(conn, "rec:reconcile:alive", ttl_ms=5000, hard_ttl_ms=10000)
    start_execution(conn, "rec:reconcile:alive", owner_id=r.owner_id, fencing_token=r.fencing_token)
    result = reconcile.reconcile("rec:reconcile:alive")
    assert result.success is False


# ─── FORCE COMPLETE ─────────────────────────────────────────────────────────

def test_force_complete_from_reconciling(conn, reconcile):
    r = acquire(conn, "rec:force:basic", ttl_ms=100, hard_ttl_ms=10000)
    start_execution(conn, "rec:force:basic", owner_id=r.owner_id, fencing_token=r.fencing_token)
    time.sleep(0.2)
    reconcile.reconcile("rec:force:basic")
    result = reconcile.force_complete(
        "rec:force:basic",
        execution_result='{"value": 1}'
    )
    assert result.success is True


def test_force_complete_fails_if_not_reconciling(conn, reconcile):
    r = acquire(conn, "rec:force:bad_status", ttl_ms=5000, hard_ttl_ms=10000)
    start_execution(conn, "rec:force:bad_status", owner_id=r.owner_id, fencing_token=r.fencing_token)
    result = reconcile.force_complete(
        "rec:force:bad_status",
        execution_result='{"value": 1}'
    )
    assert result.success is False


# ─── RESET ──────────────────────────────────────────────────────────────────

def test_reset_allows_rerun(conn, reconcile):
    r = acquire(conn, "rec:reset:basic", ttl_ms=100, hard_ttl_ms=10000)
    start_execution(conn, "rec:reset:basic", owner_id=r.owner_id, fencing_token=r.fencing_token)
    time.sleep(0.2)
    reconcile.reconcile("rec:reset:basic")
    result = reconcile.reset("rec:reset:basic")
    assert result.success is True


def test_reset_fails_if_not_reconciling(conn, reconcile):
    r = acquire(conn, "rec:reset:bad_status", ttl_ms=5000, hard_ttl_ms=10000)
    start_execution(conn, "rec:reset:bad_status", owner_id=r.owner_id, fencing_token=r.fencing_token)
    result = reconcile.reset("rec:reset:bad_status")
    assert result.success is False
