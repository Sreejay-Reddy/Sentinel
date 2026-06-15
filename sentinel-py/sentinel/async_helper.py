from .result import OperationResult
from .utils import row_to_dict

async def validate_and_extend(
    conn,
    key,
    *,
    owner_id,
    fencing_token,
    ttl_ms
):
    row = None

    async with conn.cursor() as cur:
        await cur.execute("""
        UPDATE sentinel_leases
        SET
            lease_expires_at = NOW() + (%s * INTERVAL '1 millisecond'),
            lease_updated_at = NOW()
        WHERE key = %s
          AND owner_id = %s
          AND fencing_token = %s
          AND lease_expires_at > NOW()
          AND hard_expires_at > NOW()
          AND status = 'claimed'
        RETURNING status;
        """, (
            ttl_ms,
            key,
            owner_id,
            fencing_token
        ))

        result = await cur.fetchone()
        success = result is not None

        if result is not None:
            row = row_to_dict(cur, result)

    await conn.commit()

    if row is None:
        return OperationResult(success)

    return OperationResult(success, status=row["status"])

async def fetch_cached_response(conn, key):
    row = None

    async with conn.cursor() as cur:
        await cur.execute("""
        SELECT
            execution_result,
            status
        FROM sentinel_leases
        WHERE key = %s
          AND status = 'completed'
        """, (key,))

        result = await cur.fetchone()

        if result is not None:
            row = row_to_dict(cur, result)

    if row is None:
        return None

    return {
        "response": row["execution_result"],
        "status": row["status"]
    }