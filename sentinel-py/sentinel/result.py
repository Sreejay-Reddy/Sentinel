# Response structure of acquire
class AcquireResult:
    def __init__(self, acquired, owner_id=None, expires_at=None, fencing_token=None, status=None, lease_alive=None):
        self.acquired = acquired
        self.owner_id = owner_id
        self.expires_at = expires_at
        self.fencing_token = fencing_token
        self.status = status
        self.lease_alive = lease_alive

class OperationResult:
    def __init__(self, success: bool, status=None):
        self.success = success
        self.status = status

class OnceResult:
    def __init__(
        self,
        success=False,
        status=None,
        response=None,
        cached=False,
        exception=None,
        reconcile=None,
        execution_alive=None
    ):
        self.success = success
        self.status = status
        self.response = response
        self.cached = cached
        self.exception = exception
        self.reconcile = reconcile
        self.execution_alive = execution_alive