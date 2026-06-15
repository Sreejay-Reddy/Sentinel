from sentinel.result import OperationResult


class AsyncReconcile:
    def __init__(self, get_conn, namespace=None):
        self.get_conn = get_conn
        self.namespace = namespace

    async def reconcile(self, key, *, owner_id, fencing_token):
        conn = await self.get_conn()

        try:
            async with conn.cursor() as cur:
                await cur.execute("""
                UPDATE sentinel_leases
                SET
                    status = 'reconciling',
                    lease_updated_at = NOW()
                WHERE key = %s
                  AND owner_id = %s
                  AND fencing_token = %s
                  AND status = 'executing'
                  AND lease_expires_at < NOW()
                RETURNING 1;
                """, (key, owner_id, fencing_token))

                success = await cur.fetchone() is not None

            await conn.commit()

            return OperationResult(success)

        finally:
            await conn.close()

    async def force_complete(
        self,
        key,
        *,
        owner_id,
        fencing_token,
        execution_result
    ):
        conn = await self.get_conn()

        try:
            async with conn.cursor() as cur:
                await cur.execute("""
                UPDATE sentinel_leases
                SET
                    status = 'completed',
                    execution_result = %s,
                    lease_updated_at = NOW()
                WHERE key = %s
                  AND owner_id = %s
                  AND fencing_token = %s
                  AND status = 'reconciling'
                RETURNING 1;
                """, (
                    execution_result,
                    key,
                    owner_id,
                    fencing_token
                ))

                success = await cur.fetchone() is not None

            await conn.commit()

            return OperationResult(success)

        finally:
            await conn.close()

    async def reset(
        self,
        key,
        *,
        owner_id,
        fencing_token
    ):
        conn = await self.get_conn()

        try:
            async with conn.cursor() as cur:
                await cur.execute("""
                UPDATE sentinel_leases
                SET
                    status = 'claimed',
                    lease_updated_at = NOW()
                WHERE key = %s
                  AND owner_id = %s
                  AND fencing_token = %s
                  AND status = 'reconciling'
                RETURNING 1;
                """, (
                    key,
                    owner_id,
                    fencing_token
                ))

                success = await cur.fetchone() is not None

            await conn.commit()

            return OperationResult(success)

        finally:
            await conn.close()