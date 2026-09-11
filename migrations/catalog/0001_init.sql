CREATE SCHEMA IF NOT EXISTS catalog;

CREATE TABLE IF NOT EXISTS catalog.restaurants (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    cuisine TEXT NOT NULL,
    is_open BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS catalog.menu_categories (
    id TEXT PRIMARY KEY,
    restaurant_id TEXT NOT NULL REFERENCES catalog.restaurants(id),
    name TEXT NOT NULL,
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS catalog.menu_items (
    id TEXT PRIMARY KEY,
    restaurant_id TEXT NOT NULL REFERENCES catalog.restaurants(id),
    category_id TEXT NOT NULL REFERENCES catalog.menu_categories(id),
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    price_cents BIGINT NOT NULL CHECK (price_cents > 0),
    available BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS catalog.menu_item_modifiers (
    id TEXT PRIMARY KEY,
    menu_item_id TEXT NOT NULL REFERENCES catalog.menu_items(id),
    name TEXT NOT NULL,
    price_delta_cents BIGINT NOT NULL DEFAULT 0,
    available BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS catalog.menu_stop_list (
    id BIGSERIAL PRIMARY KEY,
    restaurant_id TEXT NOT NULL REFERENCES catalog.restaurants(id),
    menu_item_id TEXT NOT NULL REFERENCES catalog.menu_items(id),
    reason TEXT NOT NULL DEFAULT '',
    starts_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ends_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (restaurant_id, menu_item_id)
);

CREATE TABLE IF NOT EXISTS catalog.outbox_messages (
    id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    event_version INTEGER NOT NULL CHECK (event_version > 0),
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    publish_attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_catalog_menu_items_restaurant_id
    ON catalog.menu_items (restaurant_id);

CREATE INDEX IF NOT EXISTS idx_catalog_outbox_unpublished
    ON catalog.outbox_messages (occurred_at)
    WHERE published_at IS NULL;
