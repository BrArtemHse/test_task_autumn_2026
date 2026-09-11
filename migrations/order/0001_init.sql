CREATE SCHEMA IF NOT EXISTS orders;

CREATE TABLE IF NOT EXISTS orders.orders (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    restaurant_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN (
            'pending_restaurant_confirmation',
            'accepted',
            'rejected',
            'preparing',
            'ready',
            'completed',
            'cancelled'
        )
    ),
    total_cents BIGINT NOT NULL CHECK (total_cents >= 0),
    version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders.order_items (
    id BIGSERIAL PRIMARY KEY,
    order_id TEXT NOT NULL REFERENCES orders.orders(id),
    menu_item_id TEXT NOT NULL,
    name TEXT NOT NULL,
    unit_price_cents BIGINT NOT NULL CHECK (unit_price_cents > 0),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    modifier_item_ids JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(modifier_item_ids) = 'array'),
    line_total_cents BIGINT NOT NULL CHECK (line_total_cents > 0)
);

CREATE TABLE IF NOT EXISTS orders.idempotency_keys (
    id BIGSERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_fingerprint TEXT NOT NULL,
    order_id TEXT NOT NULL REFERENCES orders.orders(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS orders.inbox_messages (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    event_version INTEGER NOT NULL CHECK (event_version > 0),
    aggregate_id TEXT NOT NULL,
    payload_fingerprint TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders.outbox_messages (
    id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    event_version INTEGER NOT NULL CHECK (event_version > 0),
    producer TEXT NOT NULL DEFAULT 'order-service',
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

CREATE INDEX IF NOT EXISTS idx_orders_user_id
    ON orders.orders (user_id);

CREATE INDEX IF NOT EXISTS idx_orders_outbox_unpublished
    ON orders.outbox_messages (occurred_at)
    WHERE published_at IS NULL;
