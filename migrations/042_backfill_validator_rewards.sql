-- Migration 042: Backfill validator_rewards from finalize_block_events
--
-- The validator_rewards table is empty because the trigger wasn't attached
-- (block_results_raw table didn't exist when trigger was created).
-- This migration backfills from the already-parsed finalize_block_events table.

BEGIN;

-- ============================================================================
-- Backfill rewards from finalize_block_events
-- ============================================================================

-- Aggregate rewards and commission per (height, validator) before inserting
-- to avoid "ON CONFLICT DO UPDATE cannot affect row a second time" error
INSERT INTO api.validator_rewards (height, validator_address, rewards, commission)
SELECT
  f.height,
  f.attributes->>'validator' as validator_address,
  SUM(CASE WHEN f.event_type = 'rewards' THEN
    COALESCE(
      NULLIF(regexp_replace(f.attributes->>'amount', '[^0-9.]', '', 'g'), '')::NUMERIC,
      0
    ) / 1e18
  ELSE 0 END) as rewards,
  SUM(CASE WHEN f.event_type = 'commission' THEN
    COALESCE(
      NULLIF(regexp_replace(f.attributes->>'amount', '[^0-9.]', '', 'g'), '')::NUMERIC,
      0
    ) / 1e18
  ELSE 0 END) as commission
FROM api.finalize_block_events f
WHERE f.event_type IN ('rewards', 'commission')
  AND f.attributes->>'validator' IS NOT NULL
  AND f.attributes->>'validator' != ''
GROUP BY f.height, f.attributes->>'validator'
ON CONFLICT (height, validator_address)
DO UPDATE SET
  rewards = api.validator_rewards.rewards + EXCLUDED.rewards,
  commission = api.validator_rewards.commission + EXCLUDED.commission;

-- ============================================================================
-- Refresh materialized views that depend on validator_rewards (if they exist)
-- ============================================================================

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname = 'api' AND matviewname = 'mv_daily_rewards') THEN
    REFRESH MATERIALIZED VIEW api.mv_daily_rewards;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname = 'api' AND matviewname = 'mv_validator_leaderboard') THEN
    REFRESH MATERIALIZED VIEW api.mv_validator_leaderboard;
  END IF;
END $$;

COMMIT;
