from .async_core import (
    acquire,
    heartbeat,
    start_execution,
    complete,
    expire_lease
)

from .async_helper import (
    validate_and_extend,
    fetch_cached_response
)

from .heartbeat_config import get_manager
from .logging import logger
from .async_reconcilliation import AsyncReconcile
from .result import OnceResult


class AsyncOnce:
    def __init__(
        self,
        get_conn,
        key,
        fn,
        ttl_ms,
        hard_ttl_ms,
        kwargs=None,
        owns_connection=True,
    ):
        self.get_conn = get_conn
        self.key = key
        self.fn = fn
        self.ttl_ms = ttl_ms
        self.hard_ttl_ms = hard_ttl_ms
        self.kwargs = kwargs or {}
        self.owns_connection = owns_connection

        self.reconcile = AsyncReconcile(get_conn)
        self._task = None

    async def run(self):
        conn = await self.get_conn()
        manager = None

        try:
            acquired = await acquire(
                conn,
                self.key,
                ttl_ms=self.ttl_ms,
                hard_ttl_ms=self.hard_ttl_ms
            )

            # Acquire failed
            if not acquired.acquired:

                # Completed result already exists
                if acquired.status == "completed":
                    cached = await fetch_cached_response(
                        conn,
                        self.key
                    )

                    return OnceResult(
                        success=True,
                        status="completed",
                        uncertain=False,
                        response=cached["response"]
                        if cached is not None else None,
                        cached=True
                    )

                # Executing state requires reconciliation
                if acquired.status == "executing" and not acquired.lease_alive:
                    return OnceResult(
                        success=False,
                        status="executing",
                        uncertain=True,
                        execution_alive=False,
                        reconcile=self.reconcile
                    )
                
                if acquired.status == "executing" and acquired.lease_alive:
                    return OnceResult(
                        success=False,
                        status="executing",
                        execution_alive=True,
                        uncertain=False
                    )

                return OnceResult(
                    success=False,
                    status=acquired.status
                )

            # Tighten authority before execution
            validated = await validate_and_extend(
                conn,
                self.key,
                owner_id=acquired.owner_id,
                fencing_token=acquired.fencing_token,
                ttl_ms=self.ttl_ms
            )

            if not validated.success:
                return OnceResult(
                    success=False,
                    status=validated.status
                )

            # Enter execution boundary
            started = await start_execution(
                conn,
                self.key,
                owner_id=acquired.owner_id,
                fencing_token=acquired.fencing_token
            )

            if not started.success:
                return OnceResult(
                    success=False,
                    status=started.status
                )
            
            manager = get_manager()

            self._task = manager.register(
                key=self.key,
                fn=heartbeat,
                args=(
                    self.key,
                    acquired.owner_id,
                    acquired.fencing_token,
                    self.ttl_ms
                ),
                ttl_ms=self.ttl_ms
            )

            # Execute user function
            try:
                response = await self.fn(**self.kwargs)

            except Exception as e:
                logger.exception(
                    "Execution terminated with an exception after execution started. "
                    "Side effects may have partially completed. "
                    "Manual reconciliation may be required."
                )

                try:
                    await expire_lease(
                        conn,
                        self.key,
                        owner_id=acquired.owner_id,
                        fencing_token=acquired.fencing_token
                    )
                except Exception:
                    logger.exception("Could not expire lease after fn() failure")

                return OnceResult(
                    success=False,
                    status="executing",
                    execution_alive=False,
                    uncertain=True,
                    exception=e,
                    reconcile=self.reconcile
                )

            # Finalize canonical completion
            completed = await complete(
                conn,
                self.key,
                owner_id=acquired.owner_id,
                fencing_token=acquired.fencing_token,
                execution_result=response
            )

            if not completed.success:
                logger.warning(
                    "Execution completed but could not be canonically finalized. "
                    "Execution outcome may require reconciliation."
                )

                return OnceResult(
                    success=False,
                    status=completed.status,
                    uncertain=True,
                    reconcile=self.reconcile
                )

            return OnceResult(
                success=True,
                status="completed",
                response=response,
                uncertain=False,
                cached=False
            )

        finally:
            try:
                if self.owns_connection:
                    await conn.close()
            except Exception:
                logger.exception("Could not close db connection")
            if self._task and manager:
                manager.deregister(self._task)
                self._task = None
