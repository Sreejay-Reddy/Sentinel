SCHEMA_SQL = """
CREATE SEQUENCE IF NOT EXISTS sentinel_token_seq;

CREATE TABLE IF NOT EXISTS sentinel_leases (
    key TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    lease_expires_at TIMESTAMP NOT NULL,
    lease_updated_at TIMESTAMP,
    response JSONB,
    response_code INTEGER,
    status TEXT NOT NULL DEFAULT 'claimed' CHECK (status IN ('claimed','executing','completed')),
    fencing_token BIGINT NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_sentinel_expiry
    ON sentinel_leases (lease_expires_at);
"""