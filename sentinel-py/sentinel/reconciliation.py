from sentinel.result import OperationResult


class Reconcile:
    def __init__(self, get_conn, namespace=None):
        self.get_conn = get_conn
        self.namespace = namespace

    def reconcile(self, key):
        conn = self.get_conn()

        try:
            with conn.cursor() as cur:
                cur.execute("""
                UPDATE sentinel_leases
                SET
                    status = 'reconciling',
                    lease_updated_at = NOW()
                WHERE key = %s
                  AND status = 'executing'
                  AND lease_expires_at < NOW()
                RETURNING 1;
                """, (key,))

                success = cur.fetchone() is not None

            conn.commit()

            return OperationResult(success)

        finally:
            conn.close()

    def force_complete(self, key, execution_result):
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
                  AND status = 'reconciling'
                RETURNING 1;
                """, (execution_result, key))

                success = cur.fetchone() is not None

            conn.commit()

            return OperationResult(success)

        finally:
            conn.close()

    def reset(self, key):
        conn = self.get_conn()

        try:
            with conn.cursor() as cur:
                cur.execute("""
                UPDATE sentinel_leases
                SET
                    status = 'claimed',
                    lease_updated_at = NOW()
                WHERE key = %s
                  AND status = 'reconciling'
                RETURNING 1;
                """, (key,))

                success = cur.fetchone() is not None

            conn.commit()

            return OperationResult(success)

        finally:
            conn.close()