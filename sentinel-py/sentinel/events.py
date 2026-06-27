from enum import Enum
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Any

class SentinelEvent(Enum):
    ACQUIRED    = "acquired"
    REJECTED    = "rejected"
    EXECUTING   = "executing"
    COMPLETED   = "completed"
    EXPIRED     = "expired"
    RECONCILING = "reconciling"
    RELEASED = "released"
    RESET = "reset"

def write_event(cur, key, event, *, owner_id=None, fencing_token=None, metadata=None):
    cur.execute("""
        INSERT INTO sentinel_events (key, event, owner_id, fencing_token, metadata)
        VALUES (%s, %s, %s, %s, %s)
    """, (key, event.value, owner_id, fencing_token, metadata))

async def async_write_event(cur, key, event, *, owner_id=None, fencing_token=None, metadata=None):
    await cur.execute("""
        INSERT INTO sentinel_events (key, event, owner_id, fencing_token, metadata)
        VALUES (%s, %s, %s, %s, %s)
    """, (key, event.value, owner_id, fencing_token, metadata))

@dataclass
class EventRecord:
    id: int
    key: str
    event: str
    owner_id: Optional[str]
    fencing_token: Optional[int]
    metadata: Optional[Any]
    occurred_at: datetime

    def __str__(self):
        parts = [
            self.occurred_at.strftime('%Y-%m-%d %H:%M:%S'),
            f"{self.event:<12}",
            f"token={self.fencing_token}",
            f"owner={self.owner_id}",
        ]
        if self.metadata:
            parts.append(f"meta={self.metadata}")
        return "  ".join(parts)


def history(conn, key, *, limit=50):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, key, event, owner_id, fencing_token, metadata, occurred_at
            FROM sentinel_events
            WHERE key = %s
            ORDER BY occurred_at ASC, id ASC
            LIMIT %s
        """, (key, limit))

        rows = cur.fetchall()

    return [
        EventRecord(
            id=row[0],
            key=row[1],
            event=row[2],
            owner_id=row[3],
            fencing_token=row[4],
            metadata=row[5],
            occurred_at=row[6],
        )
        for row in rows
    ]