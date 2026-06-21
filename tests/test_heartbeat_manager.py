import time
import threading
import pytest
from sentinel import Sentinel
from sentinel.heartbeat import HeartbeatManager


def make_sentinel(get_conn_fixture):
    return Sentinel(get_conn=get_conn_fixture, default_ttl_ms=3000)


# --- Single task heartbeat ---

def test_heartbeat_extends_lease(get_conn_fixture):
    sentinel = make_sentinel(get_conn_fixture)
    lease_times = []

    def slow_fn():
        time.sleep(6)
        return {"ok": True}

    def poll():
        conn = get_conn_fixture()
        for _ in range(3):
            time.sleep(2)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT lease_expires_at FROM sentinel_leases WHERE key = %s",
                    ("hb-test-single",)
                )
                row = cur.fetchone()
                if row:
                    lease_times.append(row[0])
        conn.close()

    poller = threading.Thread(target=poll)
    poller.start()

    result = sentinel.once(
        key="hb-test-single",
        fn=slow_fn,
        ttl_ms=3000,
        hard_ttl_ms=30000
    )

    poller.join()

    assert result.success
    assert result.status == "completed"
    assert len(lease_times) >= 2
    assert lease_times[-1] > lease_times[0], "lease_expires_at did not advance — heartbeat not firing"


# --- Batched heartbeat across multiple tasks ---

def test_batched_heartbeat_multiple_tasks(get_conn_fixture):
    sentinel = make_sentinel(get_conn_fixture)
    results = {}
    errors = {}
    lease_snapshots = {i: [] for i in range(4)}

    def slow_fn(task_id):
        time.sleep(6)
        return {"ok": True, "task": task_id}

    def run_task(task_id):
        try:
            result = sentinel.once(
                key=f"hb-test-{task_id}",
                fn=slow_fn,
                ttl_ms=3000,
                hard_ttl_ms=30000,
                kwargs={"task_id": task_id}
            )
            results[task_id] = result
        except Exception as e:
            errors[task_id] = e

    threads = [threading.Thread(target=run_task, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()

    conn = get_conn_fixture()
    for _ in range(3):
        time.sleep(2)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT key, lease_expires_at FROM sentinel_leases
                WHERE key LIKE 'hb-test-%'
                ORDER BY key
            """)
            for row in cur.fetchall():
                task_id = int(row[0].split("-")[-1])
                if task_id in lease_snapshots:
                    lease_snapshots[task_id].append(row[1])
    conn.close()

    for t in threads:
        t.join()

    assert not errors, f"Tasks failed: {errors}"

    for task_id, result in results.items():
        assert result.success, f"Task {task_id} failed"
        assert result.status == "completed"

    for task_id, times in lease_snapshots.items():
        if len(times) >= 2:
            assert times[-1] > times[0], f"Heartbeat not firing for hb-test-{task_id}"


# --- No heartbeat when hard_ttl_ms not provided ---

def test_no_heartbeat_without_hard_ttl(get_conn_fixture):
    from sentinel.heartbeat_config import get_manager
    sentinel = make_sentinel(get_conn_fixture)

    def fast_fn():
        return {"ok": True}

    sentinel.once(
        key="hb-test-no-hard-ttl",
        fn=fast_fn,
        ttl_ms=3000
    )

    manager = get_manager()
    assert len(manager.bucket) == 0, "Heartbeat bucket should be empty when hard_ttl_ms not provided"


# --- Lazy thread initialization ---

def test_lazy_thread_init(get_conn_fixture):
    manager = HeartbeatManager(get_conn=get_conn_fixture)
    assert manager._thread is None, "Thread should not spawn before first registration"

    task = manager.register(key="hb-test-lazy", ttl_ms=3000, hard_ttl_ms=10000)
    time.sleep(0.1)
    assert manager._thread is not None and manager._thread.is_alive(), "Thread should spawn after registration"

    manager.deregister(task)
    time.sleep(0.2)
    assert not manager._thread.is_alive(), "Thread should exit when bucket is empty"


# --- Heartbeat timing gap ---

def test_heartbeat_fires_after_execution_starts(get_conn_fixture, caplog):
    import logging
    sentinel = make_sentinel(get_conn_fixture)

    def slow_fn(task_id):
        time.sleep(6)
        return {"ok": True}

    results = {}
    threads = [
        threading.Thread(
            target=lambda i=i: results.update({
                i: sentinel.once(
                    key=f"hb-gap-test-{i}",
                    fn=slow_fn,
                    ttl_ms=3000,
                    hard_ttl_ms=30000,
                    kwargs={"task_id": i}
                )
            })
        )
        for i in range(5)
    ]

    with caplog.at_level(logging.WARNING, logger="sentinel"):
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    two_strike_failures = [
        r.message for r in caplog.records
        if "Heartbeat failed twice" in r.message
    ]

    assert not two_strike_failures, f"Visibility gap caused two-strike failures: {two_strike_failures}"

    for i, result in results.items():
        assert result.success, f"Task {i} failed"