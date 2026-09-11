ALTER TABLE partner.outbox_messages
    ADD COLUMN IF NOT EXISTS producer TEXT;

UPDATE partner.outbox_messages
SET producer = 'partner-service'
WHERE producer IS NULL;

ALTER TABLE partner.outbox_messages
    ALTER COLUMN producer SET NOT NULL,
    ADD COLUMN IF NOT EXISTS locked_by TEXT,
    ADD COLUMN IF NOT EXISTS locked_until TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_partner_outbox_due_unpublished
    ON partner.outbox_messages (occurred_at, id)
    WHERE published_at IS NULL;

UPDATE partner.restaurant_integrations
SET shared_secret_hash = '9cb7b6237cbae2a4f1bc49a89e7cae5e2595efabfc91aae5887f144c8bb0b053',
    updated_at = now()
WHERE id = 'integration-sample-pizza'
  AND shared_secret_hash = 'development-only:not-used-in-consumer-slice';
