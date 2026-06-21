from .core import (
    acquire,
    start_execution,
    complete,
    expire_lease
)

from .helper import (
    validate_and_extend,
    fetch_cached_response
)

from .heartbeat_config import get_manager
from .logging import logger
from .result import OnceResult


class Once:
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

        self._task = None

    def run(self):
        conn = self.get_conn()
        manager = None

        try:
            acquired = acquire(
                conn,
                self.key,
                ttl_ms=self.ttl_ms,
                hard_ttl_ms=self.hard_ttl_ms
            )

            # Acquire failed
            if not acquired.acquired:

                # Completed result already exists
                if acquired.status == "completed":
                    cached = fetch_cached_response(
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
                        execution_alive=False
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
            validated = validate_and_extend(
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
            started = start_execution(
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
            
            if self.hard_ttl_ms:
                manager = get_manager()

                self._task = manager.register(
                    key=self.key,
                    ttl_ms=self.ttl_ms,
                    hard_ttl_ms=self.hard_ttl_ms
                )

            # Execute user function
            try:
                response = self.fn(**self.kwargs)

            except Exception as e:
                logger.exception(
                    "Execution terminated with an exception after execution started. "
                    "Side effects may have partially completed. "
                    "Manual reconciliation may be required."
                )

                try:
                    expire_lease(
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
                    exception=e
                )

            # Finalize canonical completion
            completed = complete(
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
                    uncertain=True
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
                    conn.close()
            except Exception:
                logger.exception("Could not close db connection")
            if self._task and manager:
                manager.deregister(self._task)
                self._task = None
