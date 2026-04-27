SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS sentinel_leases (
    key TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    lease_expires_at TIMESTAMP NOT NULL,
    fencing_token BIGSERIAL
);
"""