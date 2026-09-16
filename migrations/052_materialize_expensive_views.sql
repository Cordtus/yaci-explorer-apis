-- Migration 052: Materialize expensive views and optimize cache layer
--
-- Problem: Several high-traffic endpoints recompute expensive aggregations on
-- every request. chain_stats does a 4-way UNION with DISTINCT + unnest across
-- multiple tables. get_network_overview runs 10+ independent subqueries with
-- JSONB timestamp extraction. get_hourly_rewards joins and groups over JSONB
-- timestamps without index support.
--
-- Solution:
-- 1. Materialize chain_stats as mv_chain_stats (refresh every 15 min)
-- 2. Materialize network overview as mv_network_overview (refresh every 15 min)
-- 3. Materialize hourly rewards as mv_hourly_rewards (refresh every 15 min)
-- 4. Replace the expensive per-row signing stats trigger with incremental counters
-- 5. Rewrite functions to read from materialized views

BEGIN;

-- ============================================================================
-- 1. Materialized View: chain_stats
--    Replaces the regular view that scans 4 tables with UNION DISTINCT
-- ============================================================================

DROP VIEW IF EXISTS api.chain_stats CASCADE;

DROP MATERIALIZED VIEW IF EXISTS api.mv_chain_stats CASCADE;

CREATE MATERIALIZED VIEW api.mv_chain_stats AS
SELECT
  (SELECT MAX(id) FROM api.blocks_raw) AS latest_block,
  (SELECT COUNT(*) FROM api.transactions_main) AS total_transactions,
  (
    SELECT COUNT(*) FROM (
      SELECT DISTINCT sender AS addr
      FROM api.messages_main
      WHERE sender IS NOT NULL
      UNION
      SELECT DISTINCT "from" AS addr
      FROM api.evm_transactions
      UNION
      SELECT DISTINCT "to" AS addr
      FROM api.evm_transactions
      WHERE "to" IS NOT NULL
      UNION
      SELECT DISTINCT unnest(mentions) AS addr
      FROM api.messages_main
      WHERE mentions IS NOT NULL
    ) all_addresses
  ) AS unique_addresses,
  (SELECT COUNT(*) FROM api.evm_transactions) AS evm_transactions,
  (SELECT COUNT(*)::INTEGER FROM api.validators WHERE status = 'BOND_STATUS_BONDED' AND NOT jailed) AS active_validators;

-- Unique index required for REFRESH CONCURRENTLY (single-row MV uses a constant)
CREATE UNIQUE INDEX mv_chain_stats_singleton_idx ON api.mv_chain_stats ((1));

-- Backward-compatible view wrapper so existing PostgREST queries still work
CREATE OR REPLACE VIEW api.chain_stats AS
SELECT * FROM api.mv_chain_stats;

GRANT SELECT ON api.chain_stats TO web_anon;
GRANT SELECT ON api.mv_chain_stats TO web_anon;

-- ============================================================================
-- 2. Materialized View: network_overview
--    Replaces the function that runs 10+ subqueries per call
-- ============================================================================

DROP MATERIALIZED VIEW IF EXISTS api.mv_network_overview CASCADE;

CREATE MATERIALIZED VIEW api.mv_network_overview AS
SELECT
  (SELECT COUNT(*)::INTEGER FROM api.validators) AS total_validators,
  (SELECT COUNT(*)::INTEGER FROM api.validators WHERE status = 'BOND_STATUS_BONDED' AND NOT jailed) AS active_validators,
  (SELECT COUNT(*)::INTEGER FROM api.validators WHERE jailed = TRUE) AS jailed_validators,
  (SELECT COALESCE(SUM(tokens), 0) FROM api.validators WHERE status = 'BOND_STATUS_BONDED') AS total_bonded_tokens,
  (
    SELECT COALESCE(SUM(rewards), 0)
    FROM api.validator_rewards vr
    JOIN api.blocks_raw b ON b.id = vr.height
    WHERE (b.data->'block'->'header'->>'time')::timestamptz > NOW() - INTERVAL '24 hours'
  ) AS total_rewards_24h,
  (
    SELECT COALESCE(SUM(commission), 0)
    FROM api.validator_rewards vr
    JOIN api.blocks_raw b ON b.id = vr.height
    WHERE (b.data->'block'->'header'->>'time')::timestamptz > NOW() - INTERVAL '24 hours'
  ) AS total_commission_24h,
  (
    SELECT COALESCE(AVG(
      EXTRACT(EPOCH FROM (
        (b1.data->'block'->'header'->>'time')::timestamptz -
        (b2.data->'block'->'header'->>'time')::timestamptz
      ))
    ), 6)
    FROM api.blocks_raw b1
    JOIN api.blocks_raw b2 ON b2.id = b1.id - 1
    WHERE b1.id > (SELECT MAX(id) - 100 FROM api.blocks_raw)
  ) AS avg_block_time,
  (SELECT COUNT(*) FROM api.transactions_main) AS total_transactions,
  (SELECT COUNT(DISTINCT sender) FROM api.messages_main WHERE sender IS NOT NULL) AS unique_addresses,
  (SELECT COUNT(*)::INTEGER FROM api.validators WHERE status = 'BOND_STATUS_BONDED') AS max_validators;

CREATE UNIQUE INDEX mv_network_overview_singleton_idx ON api.mv_network_overview ((1));

GRANT SELECT ON api.mv_network_overview TO web_anon;

-- Replace the function to read from the materialized view
DROP FUNCTION IF EXISTS api.get_network_overview();

CREATE OR REPLACE FUNCTION api.get_network_overview()
RETURNS TABLE (
  total_validators INTEGER,
  active_validators INTEGER,
  jailed_validators INTEGER,
  total_bonded_tokens NUMERIC,
  total_rewards_24h NUMERIC,
  total_commission_24h NUMERIC,
  avg_block_time NUMERIC,
  total_transactions BIGINT,
  unique_addresses BIGINT,
  max_validators INTEGER
) AS $$
BEGIN
  RETURN QUERY SELECT
    mv.total_validators,
    mv.active_validators,
    mv.jailed_validators,
    mv.total_bonded_tokens,
    mv.total_rewards_24h,
    mv.total_commission_24h,
    mv.avg_block_time,
    mv.total_transactions,
    mv.unique_addresses,
    mv.max_validators
  FROM api.mv_network_overview mv;
END;
$$ LANGUAGE plpgsql STABLE;

GRANT EXECUTE ON FUNCTION api.get_network_overview() TO web_anon;

-- ============================================================================
-- 3. Materialized View: hourly_rewards
--    Pre-computes the hourly rewards aggregation (last 48h for chart data)
-- ============================================================================

DROP MATERIALIZED VIEW IF EXISTS api.mv_hourly_rewards CASCADE;

CREATE MATERIALIZED VIEW api.mv_hourly_rewards AS
SELECT
  date_trunc('hour', (b.data->'block'->'header'->>'time')::timestamptz) AS hour,
  COALESCE(SUM(vr.rewards), 0) AS rewards,
  COALESCE(SUM(vr.commission), 0) AS commission
FROM api.validator_rewards vr
JOIN api.blocks_raw b ON b.id = vr.height
WHERE (b.data->'block'->'header'->>'time')::timestamptz > NOW() - INTERVAL '48 hours'
GROUP BY date_trunc('hour', (b.data->'block'->'header'->>'time')::timestamptz);

CREATE UNIQUE INDEX mv_hourly_rewards_hour_idx ON api.mv_hourly_rewards (hour);

GRANT SELECT ON api.mv_hourly_rewards TO web_anon;

-- Replace the function to read from the materialized view
CREATE OR REPLACE FUNCTION api.get_hourly_rewards(
  _hours INTEGER DEFAULT 24
)
RETURNS TABLE (
  hour TIMESTAMPTZ,
  rewards NUMERIC,
  commission NUMERIC
) AS $$
BEGIN
  RETURN QUERY
  SELECT mv.hour, mv.rewards, mv.commission
  FROM api.mv_hourly_rewards mv
  WHERE mv.hour > NOW() - (_hours || ' hours')::INTERVAL
  ORDER BY mv.hour DESC;
END;
$$ LANGUAGE plpgsql STABLE;

GRANT EXECUTE ON FUNCTION api.get_hourly_rewards(INTEGER) TO web_anon;

-- ============================================================================
-- 4. Optimize signing stats trigger: incremental counters
--    Instead of scanning 10K rows per insert, just increment/decrement.
--    The trigger fires ~100 times per block (once per validator), so this
--    replaces ~1M row scans per block with simple arithmetic.
-- ============================================================================

CREATE OR REPLACE FUNCTION api.trg_update_validator_signing_stats()
RETURNS TRIGGER AS $$
DECLARE
  op_addr TEXT;
  oldest_was_signed BOOLEAN;
  new_signed BIGINT;
  new_missed BIGINT;
BEGIN
  -- Resolve consensus address to operator address
  SELECT vca.operator_address INTO op_addr
  FROM api.validator_consensus_addresses vca
  WHERE vca.consensus_address = NEW.consensus_address
    OR vca.hex_address = UPPER(NEW.consensus_address)
  LIMIT 1;

  IF op_addr IS NULL THEN
    RETURN NEW;
  END IF;

  -- Check if we need to evict the oldest entry from the 10K window.
  -- Look up the row exactly 10,000 blocks ago for this validator.
  SELECT signed INTO oldest_was_signed
  FROM api.validator_block_signatures
  WHERE consensus_address = NEW.consensus_address
    AND height = NEW.height - 10000;

  -- Compute new counters using local variables for clarity
  SELECT
    GREATEST(0, COALESCE(v.blocks_signed, 0)
      + (CASE WHEN NEW.signed THEN 1 ELSE 0 END)
      - (CASE WHEN oldest_was_signed IS TRUE THEN 1 ELSE 0 END)),
    GREATEST(0, COALESCE(v.blocks_missed, 0)
      + (CASE WHEN NOT NEW.signed THEN 1 ELSE 0 END)
      - (CASE WHEN oldest_was_signed IS FALSE THEN 1 ELSE 0 END))
  INTO new_signed, new_missed
  FROM api.validators v
  WHERE v.operator_address = op_addr;

  UPDATE api.validators
  SET
    blocks_signed = new_signed,
    blocks_missed = new_missed,
    signing_percentage = CASE
      WHEN new_signed + new_missed > 0
      THEN ROUND(new_signed::NUMERIC / (new_signed + new_missed)::NUMERIC * 100, 2)
      ELSE NULL
    END,
    last_signed_height = CASE WHEN NEW.signed THEN NEW.height ELSE last_signed_height END,
    updated_at = NOW()
  WHERE operator_address = op_addr;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 5. Add missing MV refreshes to the existing MVs not in validator-refresh.ts
--    (This is handled in the script update, but ensure initial population here)
-- ============================================================================

-- Ensure validator_stats view still exists with all columns (including inactive_validators from 045)
DROP VIEW IF EXISTS api.validator_stats CASCADE;

CREATE VIEW api.validator_stats AS
SELECT
  (SELECT COUNT(*) FROM api.validators) AS total_validators,
  (SELECT COUNT(*) FROM api.validators WHERE status = 'BOND_STATUS_BONDED' AND NOT jailed) AS active_validators,
  (SELECT COUNT(*) FROM api.validators
   WHERE status IN ('BOND_STATUS_UNBONDED', 'BOND_STATUS_UNBONDING') AND NOT jailed) AS inactive_validators,
  (SELECT COUNT(*) FROM api.validators WHERE jailed = TRUE) AS jailed_validators,
  (SELECT COALESCE(SUM(tokens), 0) FROM api.validators WHERE status = 'BOND_STATUS_BONDED') AS total_bonded_tokens;

GRANT SELECT ON api.validator_stats TO web_anon;

COMMIT;
