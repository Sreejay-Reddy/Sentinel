from .core import (
    acquire,
    heartbeat,
    complete
)

from .helper import (
    validate_and_extend,
    fetch_cached_response
)

from .heartbeat_config import get_manager
from .reconciliation import Reconcile
from .result import OnceResult


class Once:
    def __init__(
        self,
        get_conn,
        key,
        fn,
        ttl_ms,
        hard_ttl_ms
    ):
        self.get_conn = get_conn
        self.key = key
        self.fn = fn
        self.ttl_ms = ttl_ms
        self.hard_ttl_ms = hard_ttl_ms

        self.reconcile = Reconcile(get_conn)
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
                        response=cached["response"]
                        if cached is not None else None,
                        cached=True
                    )

                # Executing state requires reconciliation
                if acquired.status == "executing":
                    return OnceResult(
                        success=False,
                        status="executing",
                        reconcile=self.reconcile
                    )

                return OnceResult(
                    success=False,
                    status=acquired.status
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
                    status=validated.status,
                    reconcile=self.reconcile
                )

            # Enter execution boundary
            started = self.reconcile.start_execution(
                self.key,
                owner_id=acquired.owner_id,
                fencing_token=acquired.fencing_token
            )

            if not started.success:
                return OnceResult(
                    success=False,
                    status=started.status,
                    reconcile=self.reconcile
                )

            # Execute user function
            try:
                response = self.fn()

            except Exception as e:
                return OnceResult(
                    success=False,
                    status="executing",
                    exception=e,
                    reconcile=self.reconcile
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
                return OnceResult(
                    success=False,
                    status=completed.status,
                    reconcile=self.reconcile
                )

            return OnceResult(
                success=True,
                status="completed",
                response=response,
                cached=False
            )

        finally:
            conn.close()
            if self._task and manager:
                manager.deregister(self._task)
                self._task = None
