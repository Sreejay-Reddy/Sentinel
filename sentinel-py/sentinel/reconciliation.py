from sentinel.result import OperationResult


class Reconcile:
    def __init__(self, get_conn, namespace=None):
        self.get_conn = get_conn
        self.namespace = namespace

    def _key(self, key):
        return f"{self.namespace}:{key}" if self.namespace else key

    def start_execution(self, key, *, owner_id, fencing_token):
        conn = self.get_conn()

        try:
            with conn.cursor() as cur:
                cur.execute("""
                UPDATE sentinel_leases
                SET
                    status = 'executing',
                    lease_updated_at = NOW()
                WHERE key = %s
                  AND owner_id = %s
                  AND fencing_token = %s
                  AND status = 'claimed'
                RETURNING 1;
                """, (key, owner_id, fencing_token))

                success = cur.fetchone() is not None

            conn.commit()

            return OperationResult(success)

        finally:
            conn.close()

    def force_complete(
        self,
        key,
        *,
        owner_id,
        fencing_token,
        execution_result
    ):
        conn = self.get_conn()

        try:
            with conn.cursor() as cur:
                cur.execute("""
                UPDATE sentinel_leases
                SET
                    status = 'completed',
                    execution_result = %s,
                    lease_updated_at = NOW()
                WHERE key = %s
                  AND owner_id = %s
                  AND fencing_token = %s
                  AND status = 'executing'
                RETURNING 1;
                """, (
                    execution_result,
                    key,
                    owner_id,
                    fencing_token
                ))

                success = cur.fetchone() is not None

            conn.commit()

            return OperationResult(success)

        finally:
            conn.close()

    def reset_to_claimed(
        self,
        key,
        *,
        owner_id,
        fencing_token
    ):
        conn = self.get_conn()

        try:
            with conn.cursor() as cur:
                cur.execute("""
                UPDATE sentinel_leases
                SET
                    status = 'claimed',
                    lease_updated_at = NOW()
                WHERE key = %s
                  AND owner_id = %s
                  AND fencing_token = %s
                  AND status = 'executing'
                RETURNING 1;
                """, (
                    key,
                    owner_id,
                    fencing_token
                ))

                success = cur.fetchone() is not None

            conn.commit()

            return OperationResult(success)

        finally:
            conn.close()