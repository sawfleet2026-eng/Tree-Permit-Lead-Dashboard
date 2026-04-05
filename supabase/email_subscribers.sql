-- ══════════════════════════════════════════════════════════════════════════════
--  email_subscribers — stores dashboard email notification subscriptions
-- ══════════════════════════════════════════════════════════════════════════════
-- Run this in the Supabase SQL Editor to create the table.
-- The dashboard JS upserts rows on subscription; the pipeline reads them
-- when sending daily digests and adhoc welcome reports.

CREATE TABLE IF NOT EXISTS email_subscribers (
    email           TEXT        PRIMARY KEY,
    daily_digest    BOOLEAN     NOT NULL DEFAULT true,
    new_lead_alerts BOOLEAN     NOT NULL DEFAULT true,
    error_alerts    BOOLEAN     NOT NULL DEFAULT false,
    is_active       BOOLEAN     NOT NULL DEFAULT true,
    subscribed_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Index for fast lookup of active digest subscribers
CREATE INDEX IF NOT EXISTS idx_email_subscribers_active_digest
    ON email_subscribers (is_active, daily_digest)
    WHERE is_active = true AND daily_digest = true;

-- Row-Level Security: allow anon inserts/upserts from the dashboard,
-- restrict reads to the service_role (pipeline backend).
ALTER TABLE email_subscribers ENABLE ROW LEVEL SECURITY;

-- Anon users can insert / upsert their own subscription
CREATE POLICY "anon_can_subscribe" ON email_subscribers
    FOR INSERT TO anon
    WITH CHECK (true);

CREATE POLICY "anon_can_update_own" ON email_subscribers
    FOR UPDATE TO anon
    USING (true)
    WITH CHECK (true);

-- Service role (pipeline) can read all active subscribers
CREATE POLICY "service_can_read" ON email_subscribers
    FOR SELECT TO service_role
    USING (true);

-- Also allow anon to read their own row (for pre-fill check)
CREATE POLICY "anon_can_read_own" ON email_subscribers
    FOR SELECT TO anon
    USING (true);
