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

class InspectResult:
    def __init__(
        self, key, owner_id, fencing_token, status, lease_alive, 
        lease_expires_at, lease_updated_at, hard_expires_at, execution_result=None
    ):
        self.key = key
        self.owner_id = owner_id
        self.fencing_token = fencing_token
        self.status = status
        self.lease_alive = lease_alive
        self.lease_expires_at = lease_expires_at
        self.lease_updated_at = lease_updated_at
        self.hard_expires_at = hard_expires_at
        self.execution_result = execution_result
        

class OnceResult:
    def __init__(
        self,
        success=False,
        status=None,
        response=None,
        cached=False,
        exception=None,
        execution_alive=None,
        uncertain=False
    ):
        self.success = success
        self.status = status
        self.response = response
        self.cached = cached
        self.exception = exception
        self.execution_alive = execution_alive
        self.uncertain = uncertain