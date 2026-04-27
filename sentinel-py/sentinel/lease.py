import threading
import time
from .core import acquire, heartbeat, release

class Lease:
    def __init__(self, conn, key, ttl_ms, get_conn):
        self.conn = conn
        self.key = key
        self.ttl_ms = ttl_ms
        self.get_conn = get_conn

        self.owner_id = None
        self._running = False
        self._thread = None

    def __enter__(self):
        result = acquire(self.conn, self.key, ttl_ms=self.ttl_ms)

        if not result.acquired:
            raise Exception("Could not acquire lease")

        self.owner_id = result.owner_id
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

            result = heartbeat(conn, self.key, self.owner_id, self.ttl_ms)

            if not result.extended:
                print("Lost lease")
                self._running = False
                break

        conn.close()

    def __exit__(self, exc_type, exc, tb):
        self._running = False

        if self._thread:
            self._thread.join()

        release(self.conn, self.key, self.owner_id)


def lease(conn, key, ttl_ms=5000, get_conn=None):
    if get_conn is None:
        raise Exception("lease requires get_conn")

    return Lease(conn, key, ttl_ms, get_conn)