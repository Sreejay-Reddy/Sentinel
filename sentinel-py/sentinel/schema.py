SCHEMA_SQL = """
CREATE SEQUENCE IF NOT EXISTS sentinel_token_seq;

CREATE TABLE IF NOT EXISTS sentinel_leases (
    key TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    lease_expires_at TIMESTAMPTZ NOT NULL,
    lease_updated_at TIMESTAMPTZ,
    hard_expires_at TIMESTAMPTZ,
    execution_result JSONB,
    status TEXT NOT NULL DEFAULT 'claimed' CHECK (status IN ('claimed','executing','completed','reconciling')),
    fencing_token BIGINT NOT NULL DEFAULT 1,
    ttl_ms INTEGER NOT NULL DEFAULT 3000
);

CREATE INDEX IF NOT EXISTS idx_sentinel_expiry
    ON sentinel_leases (lease_expires_at);

CREATE TABLE IF NOT EXISTS sentinel_events (
    id BIGSERIAL PRIMARY KEY,
    key TEXT NOT NULL,
    event TEXT NOT NULL CHECK(event IN('acquired','rejected','executing','completed','expired','reconciling','released','reset')),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    fencing_token BIGINT,
    owner_id TEXT,
    metadata JSONB
);

CREATE INDEX IF NOT EXISTS idx_sentinel_events
    ON sentinel_events (key, occurred_at, fencing_token);
"""