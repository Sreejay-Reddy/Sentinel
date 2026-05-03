# Response structure of acquire
class AcquireResult:
    def __init__(self, acquired, owner_id=None, expires_at=None, fencing_token=None):
        self.acquired = acquired
        self.owner_id = owner_id
        self.expires_at = expires_at
        self.fencing_token = fencing_token

class OperationResult:
    def __init__(self, success: bool):
        self.success = success