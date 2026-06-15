from .lease import Lease
from .async_once import AsyncOnce
from .heartbeat_config import get_manager

class AsyncSentinel:
    def __init__(self, get_conn = None, default_ttl_ms=3000, namespace=None, owns_connection=True):
        self.default_ttl_ms = default_ttl_ms
        self.namespace = namespace
        self.integration = None
        self.owns_connection = owns_connection 

        if get_conn:
            self.get_conn = get_conn

        else:
            raise ValueError(
                "No database connection provider found."
            )

        self.manager = get_manager(
            self._conn,
            owns_connection=self.owns_connection
        )

    async def _conn(self):
        return await self.get_conn()

    def _ttl(self, ttl_ms):
        return ttl_ms if ttl_ms is not None else self.default_ttl_ms
    
    def _hard_ttl(self, ttl_ms, hard_ttl_ms):
        if hard_ttl_ms is None or hard_ttl_ms < ttl_ms:
            return ttl_ms
        return hard_ttl_ms

    def _key(self, key):
        return f"{self.namespace}:{key}" if self.namespace else key
        
    # def lease(self, key, ttl_ms=None, hard_ttl_ms=None):
    #     key = self._key(key)
    #     ttl = self._ttl(ttl_ms)
    #     hard_ttl = self._hard_ttl(ttl,hard_ttl_ms)

    #     return Lease(None, key, ttl, hard_ttl, self._conn)
    
    async def once(self, key, fn, ttl_ms=None, hard_ttl_ms=None, kwargs=None):
        key = self._key(key)

        ttl = self._ttl(ttl_ms)
        hard_ttl = self._hard_ttl(ttl, hard_ttl_ms)

        execution = AsyncOnce(
            get_conn=self._conn,
            key=key,
            fn=fn,
            ttl_ms=ttl,
            hard_ttl_ms=hard_ttl,
            kwargs=kwargs,
            owns_connection=self.owns_connection
        )

        return await execution.run()
        