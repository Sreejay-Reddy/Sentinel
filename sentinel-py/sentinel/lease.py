import threading
import time
from .core import acquire, heartbeat, release, start_execution

class Lease:
    def __init__(self, conn, key, ttl_ms, get_conn, auto_release):
        self.conn = conn
        self.key = key
        self._token = None
        self.ttl_ms = ttl_ms
        self.get_conn = get_conn

        self.owner_id = None
        self._running = False
        self._thread = None
        self._error = None

        self._auto_release = auto_release

    def __enter__(self):
        result = acquire(self.conn, self.key, ttl_ms=self.ttl_ms)

        if not result.acquired:
            raise Exception("Could not acquire lease")

        self.owner_id = result.owner_id
        self._token = result.fencing_token

        started = start_execution(
            self.conn,
            self.key,
            owner_id=self.owner_id,
            fencing_token=self._token
        )

        if not started.success:
            raise Exception("Lost lease before execution started")

        self._running = True

        self._thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True
        )
        self._thread.start()

        return self

    def _heartbeat_loop(self):
        conn = self.get_conn()
        interval = self.ttl_ms / 2000

        while self._running:
            time.sleep(interval)

            result = heartbeat(
                conn,
                self.key,
                owner_id=self.owner_id,
                fencing_token=self._token,
                ttl_ms=self.ttl_ms
            )

            if not result.success:
                print("Lost lease")
                self._running = False
                self._error = Exception("Lease lost during execution")
                break

        conn.close()

    def assert_valid(self):
        if not self._running:
            raise self._error or Exception("Lease no longer valid")

    @property
    def is_valid(self):
        return self._running

    def release(self):
        if self._running:
            self._running = False

        if self._thread:
            self._thread.join()

        release(
            self.conn,
            self.key,
            owner_id=self.owner_id,
            fencing_token=self._token
        )

    def run(self, fn):
        self.assert_valid()
        result = fn(self)
        self.assert_valid()
        return result

    def __exit__(self, exc_type, exc, tb):
        self._running = False

        if self._thread:
            self._thread.join()

        if self._auto_release:
            release(
                self.conn,
                self.key,
                owner_id=self.owner_id,
                fencing_token=self._token
            )

def lease(conn, key, ttl_ms=5000, get_conn=None, auto_release=True):
    if get_conn is None:
        raise Exception("lease requires get_conn")

    return Lease(conn, key, ttl_ms, get_conn, auto_release)