# Purely experimental and early implementation currently not exposed in client
from sentinel.core import acquire, heartbeat, release
from sentinel.heartbeat_config import get_manager


class Lease:
    def __init__(self, conn, key, ttl_ms, hard_ttl_ms, get_conn):
        self.conn = conn
        self.get_conn = get_conn 
        self._owns_conn = conn is None 
        self.key = key
        self.ttl_ms = ttl_ms
        self.hard_ttl_ms = hard_ttl_ms

        self.owner_id = None
        self.token = None
        self._task = None
        self.acquired = False

    def __enter__(self):

        if self.conn is None:
            self.conn = self.get_conn()
            
        result = acquire(self.conn, self.key, ttl_ms=self.ttl_ms, hard_ttl_ms=self.hard_ttl_ms)

        if not result.acquired:
            return None
        
        self.owner_id = result.owner_id
        self.token = result.fencing_token
        self.acquired = True

        manager = get_manager()

        self._task = manager.register(
            key=self.key,
            fn=heartbeat,
            args=(self.key, self.owner_id, self.token, self.ttl_ms),
            ttl_ms=self.ttl_ms
        )

        return self

    def __exit__(self, exc_type, exc, tb):
        if not self.acquired:
            return

        manager = get_manager()

        try:
            if self._task:
                manager.deregister(self._task)
                self._task = None

            release(self.conn, self.key, owner_id=self.owner_id, fencing_token=self.token)
            self.acquired = False
            
        finally:
            if self._owns_conn and self.conn:
                self.conn.close()

def lease(conn, key, ttl_ms, hard_ttl_ms, get_conn=None):
    if get_conn is None:
        raise Exception("lease requires get_conn")
    
    return Lease(conn, key, ttl_ms, hard_ttl_ms, get_conn)