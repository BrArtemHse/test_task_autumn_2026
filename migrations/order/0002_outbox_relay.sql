ALTER TABLE orders.outbox_messages
    ADD COLUMN IF NOT EXISTS locked_by TEXT,
    ADD COLUMN IF NOT EXISTS locked_until TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_orders_outbox_claimable
    ON orders.outbox_messages (next_attempt_at, occurred_at, id)
    WHERE published_at IS NULL;
