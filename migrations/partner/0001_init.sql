CREATE SCHEMA IF NOT EXISTS partner;

CREATE TABLE IF NOT EXISTS partner.restaurant_integrations (
    id TEXT PRIMARY KEY,
    restaurant_id TEXT NOT NULL UNIQUE,
    external_store_id TEXT NOT NULL UNIQUE,
    callback_url TEXT NOT NULL,
    shared_secret_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS partner.partner_order_submissions (
    id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL UNIQUE,
    restaurant_id TEXT NOT NULL,
    external_store_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('pending', 'submitted', 'accepted', 'rejected', 'failed')
    ),
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS partner.partner_callbacks (
    id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    external_store_id TEXT NOT NULL,
    callback_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    payload_fingerprint TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (external_store_id, id)
);

CREATE TABLE IF NOT EXISTS partner.inbox_messages (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    event_version INTEGER NOT NULL CHECK (event_version > 0),
    aggregate_id TEXT NOT NULL,
    payload_fingerprint TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS partner.outbox_messages (
    id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    event_version INTEGER NOT NULL CHECK (event_version > 0),
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    correlation_id TEXT NOT NULL DEFAULT '',
    causation_id TEXT NOT NULL DEFAULT '',
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    publish_attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_partner_submissions_status
    ON partner.partner_order_submissions (status, created_at);

CREATE INDEX IF NOT EXISTS idx_partner_outbox_unpublished
    ON partner.outbox_messages (occurred_at)
    WHERE published_at IS NULL;
