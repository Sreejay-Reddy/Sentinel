from .lease import Lease
from .once import Once
from .reconciliation import Reconcile
from .heartbeat_config import get_manager

class Sentinel:
    def __init__(self, get_conn, default_ttl_ms=3000, namespace=None):
        self.get_conn = get_conn
        self.manager = get_manager(get_conn)
        self.default_ttl_ms = default_ttl_ms
        self.namespace = namespace

        self.reconcile = Reconcile(
            get_conn=get_conn,
            namespace=namespace
        )

    def _conn(self):
        return self.get_conn()

    def _ttl(self, ttl_ms):
        return ttl_ms if ttl_ms is not None else self.default_ttl_ms
    
    def _hard_ttl(self, ttl_ms, hard_ttl_ms):
        if hard_ttl_ms is None or hard_ttl_ms < ttl_ms:
            return ttl_ms
        return hard_ttl_ms

    def _key(self, key):
        return f"{self.namespace}:{key}" if self.namespace else key
    
    def lease(self, key, ttl_ms=None, hard_ttl_ms=None):
        key = self._key(key)
        ttl = self._ttl(ttl_ms)
        hard_ttl = self._hard_ttl(ttl,hard_ttl_ms)

        return Lease(None, key, ttl, hard_ttl, self.get_conn)
    
    def once(self, key, fn, ttl_ms=None, hard_ttl_ms=None):
        key = self._key(key)

        ttl = self._ttl(ttl_ms)
        hard_ttl = self._hard_ttl(ttl, hard_ttl_ms)

        once = Once(
            get_conn=self.get_conn,
            key=key,
            fn=fn,
            ttl_ms=ttl,
            hard_ttl_ms=hard_ttl
        )

        return once.run()
        