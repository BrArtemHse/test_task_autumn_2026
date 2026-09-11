ALTER TABLE partner.partner_order_submissions
    ADD COLUMN IF NOT EXISTS locked_by TEXT,
    ADD COLUMN IF NOT EXISTS locked_until TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS submitted_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_partner_submissions_claimable
    ON partner.partner_order_submissions (next_attempt_at, created_at, id)
    WHERE status = 'pending';
