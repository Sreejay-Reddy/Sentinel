import threading
import time
from .logging import logger


class HeartbeatTask:
    def __init__(self, key, ttl_ms, hard_ttl_ms):
        self.key = key
        self.ttl_ms = ttl_ms
        self.hard_ttl_ms = hard_ttl_ms
        self.registered_at = time.time()
        self.next_heartbeat_at = time.time() + self._interval()

    def _interval(self):
        now = time.time()
        elapsed = now - self.registered_at
        hard_ttl_s = self.hard_ttl_ms / 1000.0
        progress = min(elapsed / hard_ttl_s, 1.0)

        if progress < 0.5:
            return (self.ttl_ms / 1000.0) / 3
        elif progress < 0.8:
            return (self.ttl_ms / 1000.0) / 5
        else:
            return (self.ttl_ms / 1000.0) / 10

    def update_next(self):
        self.next_heartbeat_at = time.time() + self._interval()


class HeartbeatManager:
    def __init__(self, get_conn, owns_connection=True):
        self.get_conn = get_conn
        self.owns_connection = owns_connection
        self.bucket = set()
        self.lock = threading.Lock()
        self._thread = None
        self._failure_counts = {}

    def register(self, key, ttl_ms, hard_ttl_ms):
        task = HeartbeatTask(key, ttl_ms, hard_ttl_ms)

        with self.lock:
            self.bucket.add(task)
            self._ensure_thread()

        return task

    def deregister(self, task):
        with self.lock:
            self.bucket.discard(task)
            self._failure_counts.pop(task.key, None)
    
    def stop(self):
        with self.lock:
            self.bucket.clear()
            self._failure_counts.clear()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1)
        self._thread = None

    def _ensure_thread(self):
        if self._thread is None or not self._thread.is_alive():
            self._thread = threading.Thread(target=self._worker, daemon=True)
            self._thread.start()

    def _get_keys(self, tasks):
        return [t.key for t in tasks]

    def _worker(self):
        conn = self.get_conn()
        consecutive_conn_failures = 0

        try:
            while True:
                with self.lock:
                    tasks = list(self.bucket)

                if not tasks:
                    break

                now = time.time()
                due = [t for t in tasks if t.next_heartbeat_at <= now]

                if due:
                    keys = self._get_keys(due)

                    try:
                        from .core import batch_heartbeat
                        updated_keys = batch_heartbeat(conn, keys)
                        consecutive_conn_failures = 0

                        updated_set = set(updated_keys)

                        for task in due:
                            if task.key in updated_set:
                                task.update_next()
                                self._failure_counts.pop(task.key, None)
                            else:
                                count = self._failure_counts.get(task.key, 0) + 1
                                self._failure_counts[task.key] = count

                                if count >= 2:
                                    logger.warning(
                                        f"Heartbeat failed twice for key {task.key}, deregistering"
                                    )
                                    self.deregister(task)

                    except Exception:
                        logger.exception("Batch heartbeat connection failure")
                        consecutive_conn_failures += 1

                        if consecutive_conn_failures >= 2:
                            logger.warning(
                                "Two consecutive connection failures, marking all tasks uncertain"
                            )
                            with self.lock:
                                self.bucket.clear()
                                self._failure_counts.clear()

                            try:
                                if self.owns_connection:
                                    conn.close()
                            except Exception:
                                pass

                            conn = self.get_conn()
                            consecutive_conn_failures = 0
                            break

                time.sleep(0.05)

        finally:
            try:
                if self.owns_connection:
                    conn.close()
            except Exception:
                logger.exception("Could not close db connection")

    def debug_dump(self):
        with self.lock:
            print(f"Bucket: {len(self.bucket)} tasks")
            for task in self.bucket:
                print(f"  {task.key} — next beat in {task.next_heartbeat_at - time.time():.2f}s")