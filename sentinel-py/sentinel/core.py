from .utils import get_owner_id, row_to_dict
from .result import AcquireResult, ReleaseResult, HeartBeatResult

def acquire(conn, key, owner_id=None, ttl_ms=10000):

    owner_id = owner_id or get_owner_id()
    ttl_ms = ttl_ms if ttl_ms and ttl_ms > 0 else 10000

    row = None 

    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO sentinel_leases (key, owner_id, lease_expires_at, fencing_token)
        VALUES (%s, %s, NOW() + (%s * INTERVAL '1 millisecond'), 1)
        ON CONFLICT (key)
        DO UPDATE
        SET owner_id = EXCLUDED.owner_id,
            lease_expires_at = EXCLUDED.lease_expires_at,
            fencing_token = sentinel_leases.fencing_token + 1
        WHERE sentinel_leases.lease_expires_at < NOW()
        RETURNING owner_id, lease_expires_at, fencing_token;
        """, (key, owner_id, ttl_ms))

        result = cur.fetchone()

        if result:
            row = row_to_dict(cur, result)

    conn.commit()

    if row:
        return AcquireResult(
            acquired=True,
            owner_id=row["owner_id"],
            expires_at=row["lease_expires_at"],
            fencing_token=row["fencing_token"]
        )

    return AcquireResult(acquired=False)

def release(conn, key, owner_id):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM sentinel_leases
        WHERE key = %s AND owner_id = %s
        RETURNING key;
        """, (key, owner_id))

        success = cur.fetchone() is not None

    conn.commit()
    return ReleaseResult(success)

def heartbeat(conn, key, owner_id, ttl_ms=5000):
    with conn.cursor() as cur:
        cur.execute("""
        UPDATE sentinel_leases
        SET lease_expires_at = NOW() + (%s * INTERVAL '1 millisecond')
        WHERE key = %s AND owner_id = %s
        RETURNING owner_id;
        """, (ttl_ms, key, owner_id))

        success = cur.fetchone() is not None

    conn.commit()

    return HeartBeatResult(success)

