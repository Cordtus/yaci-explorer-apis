BEGIN;

-- Migration 065: Replace mv_daily_rewards with trigger-incremented table
--
-- Problem: REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_daily_rewards takes
-- ~5 minutes because it joins 13.4M validator_rewards rows against blocks_raw
-- with JSONB timestamp extraction on every row.
--
-- Fix: Create rt_daily_rewards table, seed it from the current MV, and add a
-- trigger on validator_rewards INSERT that increments daily totals. Same
-- pattern as rt_hourly_rewards (migration 055/061).

-- ============================================================================
-- 1. Create the incremental table
-- ============================================================================

CREATE TABLE IF NOT EXISTS api.rt_daily_rewards (
  date DATE PRIMARY KEY,
  total_rewards NUMERIC NOT NULL DEFAULT 0,
  total_commission NUMERIC NOT NULL DEFAULT 0
);

-- ============================================================================
-- 2. Seed from existing mv_daily_rewards (one-time copy)
-- ============================================================================

INSERT INTO api.rt_daily_rewards (date, total_rewards, total_commission)
SELECT date, total_rewards, total_commission
FROM api.mv_daily_rewards
ON CONFLICT (date) DO UPDATE SET
  total_rewards = EXCLUDED.total_rewards,
  total_commission = EXCLUDED.total_commission;

-- ============================================================================
-- 3. Trigger: increment daily totals on validator_rewards INSERT
-- ============================================================================

CREATE OR REPLACE FUNCTION api.trg_rt_daily_rewards()
RETURNS TRIGGER AS $$
DECLARE
  block_time TIMESTAMPTZ;
  reward_date DATE;
BEGIN
  SELECT (b.data->'block'->'header'->>'time')::TIMESTAMPTZ INTO block_time
  FROM api.blocks_raw b
  WHERE b.id = NEW.height;

  IF block_time IS NULL THEN
    block_time := NOW();
  END IF;

  reward_date := block_time::DATE;

  INSERT INTO api.rt_daily_rewards (date, total_rewards, total_commission)
  VALUES (reward_date, COALESCE(NEW.rewards, 0), COALESCE(NEW.commission, 0))
  ON CONFLICT (date) DO UPDATE SET
    total_rewards = api.rt_daily_rewards.total_rewards + COALESCE(NEW.rewards, 0),
    total_commission = api.rt_daily_rewards.total_commission + COALESCE(NEW.commission, 0);

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_rt_daily_rewards ON api.validator_rewards;
CREATE TRIGGER trg_rt_daily_rewards
  AFTER INSERT ON api.validator_rewards
  FOR EACH ROW
  EXECUTE FUNCTION api.trg_rt_daily_rewards();

-- ============================================================================
-- 4. Backward-compatible view so PostgREST clients still query mv_daily_rewards
-- ============================================================================

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname = 'api' AND matviewname = 'mv_daily_rewards') THEN
    DROP MATERIALIZED VIEW api.mv_daily_rewards CASCADE;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_views WHERE schemaname = 'api' AND viewname = 'mv_daily_rewards') THEN
    DROP VIEW api.mv_daily_rewards CASCADE;
  END IF;
END $$;

CREATE OR REPLACE VIEW api.mv_daily_rewards AS
SELECT
  date,
  total_rewards,
  total_commission,
  0::BIGINT AS validators_earning
FROM api.rt_daily_rewards;

GRANT SELECT ON api.rt_daily_rewards TO web_anon;
GRANT SELECT ON api.mv_daily_rewards TO web_anon;

-- ============================================================================
-- 5. Remove mv_daily_rewards from the MV refresh list in refresh_analytics_views
--    (validator-refresh.ts also needs updating, but it handles missing MVs gracefully)
-- ============================================================================

CREATE OR REPLACE FUNCTION api.refresh_analytics_views()
RETURNS void
LANGUAGE sql
AS $$
  REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_validator_delegator_counts;
  REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_validator_leaderboard;
$$;

COMMIT;
