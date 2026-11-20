"""SQLite persistence layer.

A single database file holds all tenants.  Writers use BEGIN IMMEDIATE so a
transaction holds the write lock for its whole lifetime; readers use plain
queries.  WAL mode plus a generous busy timeout lets concurrent processes
serialize deterministically instead of failing with SQLITE_BUSY.
"""
from __future__ import annotations

import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS tenants (
    tenant_id TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS identities (
    tenant      TEXT NOT NULL,
    identity_id TEXT NOT NULL,
    generation  INTEGER NOT NULL,          -- current generation; bumped on retire, never lowered
    actions     TEXT NOT NULL,             -- JSON array of authorized actions
    phase       TEXT NOT NULL,             -- ACTIVE | PREPARED | RETIRED
    PRIMARY KEY (tenant, identity_id)
);

CREATE TABLE IF NOT EXISTS credentials (
    tenant        TEXT NOT NULL,
    credential_id TEXT NOT NULL,
    identity_id   TEXT NOT NULL,
    generation    INTEGER NOT NULL,        -- generation the credential was issued for
    valid_from    INTEGER NOT NULL,        -- inclusive
    valid_until   INTEGER NOT NULL,        -- exclusive: exactly-at-expiry means ineligible
    revoked       INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant, credential_id)
);

CREATE TABLE IF NOT EXISTS services (
    tenant       TEXT NOT NULL,
    service_id   TEXT NOT NULL,
    requirements TEXT NOT NULL,            -- JSON object {action: min_identity_count}
    PRIMARY KEY (tenant, service_id)
);

CREATE TABLE IF NOT EXISTS bindings (
    tenant        TEXT NOT NULL,
    service_id    TEXT NOT NULL,
    credential_id TEXT NOT NULL,
    PRIMARY KEY (tenant, service_id, credential_id)
);

CREATE TABLE IF NOT EXISTS plans (
    tenant      TEXT PRIMARY KEY,          -- at most one open retirement plan per tenant
    plan_id     TEXT NOT NULL,
    candidates  TEXT NOT NULL,             -- JSON array of identity ids fixed at prepare time
    deadline    INTEGER NOT NULL,          -- old credentials survive until this tick (exclusive)
    created_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS requests (
    tenant      TEXT NOT NULL,
    request_id  TEXT NOT NULL,             -- unique within the tenant
    op          TEXT NOT NULL,
    params_hash TEXT NOT NULL,             -- sha256 of canonical params (without request_id)
    receipt     TEXT NOT NULL,             -- JSON receipt of the first execution
    PRIMARY KEY (tenant, request_id)
);
"""


class Store:
    """Owns the SQLite connection and the schema."""

    def __init__(self, path: str):
        self.path = path
        # isolation_level=None: we issue explicit BEGIN IMMEDIATE / COMMIT.
        self.conn = sqlite3.connect(path, timeout=60.0, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        if path != ":memory:":
            self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=60000")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(SCHEMA)

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "Store":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
