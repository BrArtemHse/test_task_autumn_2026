ALTER TABLE partner.partner_order_submissions
    ADD COLUMN IF NOT EXISTS destination_url TEXT,
    ADD COLUMN IF NOT EXISTS payload JSONB,
    ADD COLUMN IF NOT EXISTS correlation_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS source_event_id TEXT;

UPDATE partner.partner_order_submissions AS submission
SET destination_url = integration.callback_url
FROM partner.restaurant_integrations AS integration
WHERE submission.restaurant_id = integration.restaurant_id
  AND submission.destination_url IS NULL;

UPDATE partner.partner_order_submissions
SET destination_url = ''
WHERE destination_url IS NULL;

UPDATE partner.partner_order_submissions
SET payload = '{}'::jsonb
WHERE payload IS NULL;

UPDATE partner.partner_order_submissions
SET source_event_id = 'legacy:' || id
WHERE source_event_id IS NULL;

ALTER TABLE partner.partner_order_submissions
    ALTER COLUMN destination_url SET NOT NULL,
    ALTER COLUMN payload SET NOT NULL,
    ALTER COLUMN source_event_id SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_partner_submissions_source_event
    ON partner.partner_order_submissions (source_event_id);
