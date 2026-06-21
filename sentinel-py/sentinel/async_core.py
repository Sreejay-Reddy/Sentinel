import json
from .utils import get_owner_id, row_to_dict
from .result import AcquireResult, OperationResult, InspectResult

async def acquire(conn, key, *, owner_id=None, ttl_ms=10000, hard_ttl_ms = None):

    owner_id = owner_id or get_owner_id()
    ttl_ms = ttl_ms if ttl_ms and ttl_ms > 0 else 10000
    hard_ttl_ms = hard_ttl_ms if hard_ttl_ms and hard_ttl_ms > ttl_ms else ttl_ms

    row = None 

    async with conn.cursor() as cur:
        await cur.execute("""
        INSERT INTO sentinel_leases (
            key,
            owner_id,
            lease_expires_at,
            lease_updated_at,
            hard_expires_at,
            fencing_token
        )
        VALUES (
            %s,
            %s,
            NOW() + (%s * INTERVAL '1 millisecond'),
            NOW(),
            NOW() + (%s * INTERVAL '1 millisecond'),
            nextval('sentinel_token_seq')
        )
        ON CONFLICT (key)
        DO UPDATE
        SET
            owner_id = EXCLUDED.owner_id,
            lease_expires_at = EXCLUDED.lease_expires_at,
            lease_updated_at = NOW(),
            hard_expires_at = EXCLUDED.hard_expires_at,
            fencing_token = nextval('sentinel_token_seq')
        WHERE sentinel_leases.lease_expires_at < NOW() AND sentinel_leases.status = 'claimed'
        RETURNING owner_id, lease_expires_at, fencing_token, status, lease_expires_at > NOW() AS lease_alive;
        """, (key, owner_id, ttl_ms, hard_ttl_ms))

        result = await cur.fetchone()

        if result is not None:
            row = row_to_dict(cur, result)

    await conn.commit()

    if row is not None and row["fencing_token"] is None:
        raise Exception("Invariant violation: fencing_token is None")

    if row is not None:
        return AcquireResult(
            acquired=True,
            owner_id=row["owner_id"],
            expires_at=row["lease_expires_at"],
            lease_alive=row["lease_alive"],
            fencing_token=row["fencing_token"],
            status=row["status"]
        )
    
    async with conn.cursor() as cur:
        await cur.execute("""
        SELECT owner_id, lease_expires_at, fencing_token, status, lease_expires_at > NOW() AS lease_alive
        FROM sentinel_leases
        WHERE key = %s
        """, (key,))

        result = await cur.fetchone()

        if result is not None:
            row = row_to_dict(cur, result)
    
    await conn.commit()

    if row is not None:
        return AcquireResult(acquired=False,
            owner_id=row["owner_id"],
            expires_at=row["lease_expires_at"],
            lease_alive=row["lease_alive"],
            fencing_token=row["fencing_token"],
            status=row["status"])

async def start_execution(conn, key, *, owner_id, fencing_token):

    row = None
    
    async with conn.cursor() as cur:
        await cur.execute("""
        UPDATE sentinel_leases
        SET status = 'executing',
            lease_updated_at = NOW()
        WHERE key = %s
          AND owner_id = %s
          AND fencing_token = %s
          AND status = 'claimed'
        RETURNING status;
        """, (key, owner_id, fencing_token))

        result = await cur.fetchone()
        success = result is not None
        if result is not None:
            row = row_to_dict(cur, result)
    
    await conn.commit()
    if row is None:
        return OperationResult(success)

    return OperationResult(success, status=row["status"])

async def release(conn, key, *, owner_id, fencing_token):
    async with conn.cursor() as cur:
        await cur.execute("""
        DELETE FROM sentinel_leases
        WHERE key = %s AND owner_id = %s AND fencing_token = %s
        RETURNING 1;
        """, (key, owner_id, fencing_token))

        success = await cur.fetchone() is not None

    await conn.commit()
    return OperationResult(success)

async def complete(conn, key, *, owner_id, fencing_token, execution_result=None):

    serialized_result = (
        json.dumps(execution_result)
        if execution_result is not None
        else None
    )

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
        RETURNING 1;
        """, (serialized_result, key, owner_id, fencing_token))

        success = await cur.fetchone() is not None

    await conn.commit()
    return OperationResult(success)

# Unused since updates are batched and internally sync inside heartbeat manager [0.4.2 Changelog]
async def heartbeat(conn, key, owner_id, fencing_token, ttl_ms=5000):
    async with conn.cursor() as cur:
        await cur.execute("""
        UPDATE sentinel_leases
        SET lease_expires_at = NOW() + (%s * INTERVAL '1 millisecond')
        WHERE key = %s 
            AND owner_id = %s 
            AND fencing_token = %s 
            AND hard_expires_at > NOW()
            AND status = 'executing'
        RETURNING 1;
        """, (ttl_ms, key, owner_id, fencing_token))

        success = await cur.fetchone() is not None

    await conn.commit()
    return OperationResult(success)

async def expire_lease(conn, key, *, owner_id, fencing_token):
    async with conn.cursor() as cur:
        await cur.execute("""
        UPDATE sentinel_leases
        SET hard_expires_at = NOW(),
            lease_expires_at = NOW()
        WHERE key = %s
            AND owner_id = %s
            AND fencing_token = %s
            AND status = 'executing'
        RETURNING 1;
        """, (key, owner_id, fencing_token))
        success = await cur.fetchone() is not None
    await conn.commit()
    return OperationResult(success)

async def inspect(conn, key):
    async with conn.cursor() as cur:
        await cur.execute("""
        SELECT owner_id, lease_expires_at, fencing_token, status, 
        lease_expires_at > NOW() AS lease_alive, lease_updated_at,
        hard_expires_at, execution_result
        FROM sentinel_leases
        WHERE key = %s 
        """, (key,))

        result = await cur.fetchone()

        if result is None:
            return None

        row = row_to_dict(cur, result)

    return InspectResult(
        key = key,
        owner_id = row["owner_id"],
        fencing_token = row["fencing_token"],
        status = row["status"],
        lease_alive = row["lease_alive"],
        lease_expires_at = row["lease_expires_at"],
        lease_updated_at = row["lease_updated_at"],
        hard_expires_at = row["hard_expires_at"],
        execution_result = row["execution_result"])
