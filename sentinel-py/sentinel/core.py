from .utils import get_owner_id, row_to_dict
from .result import AcquireResult, OperationResult

def acquire(conn, key, *, owner_id=None, ttl_ms=10000):

    owner_id = owner_id or get_owner_id()
    ttl_ms = ttl_ms if ttl_ms and ttl_ms > 0 else 10000

    row = None 

    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO sentinel_leases (
            key,
            owner_id,
            lease_expires_at,
            fencing_token
        )
        VALUES (
            %s,
            %s,
            NOW() + (%s * INTERVAL '1 millisecond'),
            nextval('sentinel_token_seq')
        )
        ON CONFLICT (key)
        DO UPDATE
        SET
            owner_id = EXCLUDED.owner_id,
            lease_expires_at = EXCLUDED.lease_expires_at,
            fencing_token = nextval('sentinel_token_seq')
        WHERE sentinel_leases.lease_expires_at < NOW()
        RETURNING owner_id, lease_expires_at, fencing_token;
        """, (key, owner_id, ttl_ms))

        result = cur.fetchone()

        if result is not None:
            row = row_to_dict(cur, result)

    conn.commit()

    if row is not None and row["fencing_token"] is None:
        raise Exception("Invariant violation: fencing_token is None")

    if row is not None:
        return AcquireResult(
            acquired=True,
            owner_id=row["owner_id"],
            expires_at=row["lease_expires_at"],
            fencing_token=row["fencing_token"]
        )

    return AcquireResult(acquired=False)

def start_execution(conn, key, *, owner_id, fencing_token):
    with conn.cursor() as cur:
        cur.execute("""
        UPDATE sentinel_leases
        SET status = 'executing',
            lease_updated_at = NOW()
        WHERE key = %s
          AND owner_id = %s
          AND fencing_token = %s
        RETURNING 1;
        """, (key, owner_id, fencing_token))

        success = cur.fetchone() is not None
    
    conn.commit()
    return OperationResult(success)

def release(conn, key, *, owner_id, fencing_token):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM sentinel_leases
        WHERE key = %s AND owner_id = %s AND fencing_token = %s
        RETURNING 1;
        """, (key, owner_id, fencing_token))

        success = cur.fetchone() is not None

    conn.commit()
    return OperationResult(success)

def complete(conn, key, *, owner_id, fencing_token, response=None, response_code=None):
    with conn.cursor() as cur:
        cur.execute("""
        UPDATE sentinel_leases
        SET
            status = 'completed',
            response = %s,
            response_code = %s,
            lease_updated_at = NOW()
        WHERE key = %s
          AND owner_id = %s
          AND fencing_token = %s
        RETURNING 1;
        """, (response, response_code, key, owner_id, fencing_token))

        success = cur.fetchone() is not None

    conn.commit()
    return OperationResult(success)

def heartbeat(conn, key, *, owner_id, fencing_token, ttl_ms=5000):
    with conn.cursor() as cur:
        cur.execute("""
        UPDATE sentinel_leases
        SET lease_expires_at = NOW() + (%s * INTERVAL '1 millisecond')
        WHERE key = %s AND owner_id = %s AND fencing_token = %s
        RETURNING 1;
        """, (ttl_ms, key, owner_id, fencing_token))

        success = cur.fetchone() is not None

    conn.commit()
    return OperationResult(success)


