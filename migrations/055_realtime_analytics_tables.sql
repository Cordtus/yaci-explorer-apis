-- Migration 055: Replace materialized views with trigger-updated tables
--
-- Problem: Analytics MVs refresh every 15 minutes, making data stale.
-- Solution: Single-row/counter tables maintained by INSERT triggers give
-- near-real-time data with negligible per-row overhead.
--
-- Replaces:
--   mv_chain_stats        -> rt_chain_stats (trigger on blocks_raw, transactions_main, evm_transactions)
--   mv_daily_tx_stats     -> rt_daily_tx_stats (trigger on transactions_main)
--   mv_hourly_tx_stats    -> rt_hourly_tx_stats (trigger on transactions_main)
--   mv_message_type_stats -> rt_message_type_stats (trigger on messages_main)
--   mv_hourly_rewards     -> rt_hourly_rewards (trigger on validator_rewards)
--
-- Each table has backward-compatible views so PostgREST queries still work.
-- The old MVs are kept for rollback safety but removed from the refresh loop.

BEGIN;

-- ============================================================================
-- 2A. rt_chain_stats (replaces mv_chain_stats)
-- ============================================================================

CREATE TABLE IF NOT EXISTS api.rt_chain_stats (
  id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  latest_block BIGINT NOT NULL DEFAULT 0,
  total_transactions BIGINT NOT NULL DEFAULT 0,
  unique_addresses BIGINT NOT NULL DEFAULT 0,
  evm_transactions BIGINT NOT NULL DEFAULT 0,
  active_validators INTEGER NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed from current MV values (if MV exists)
INSERT INTO api.rt_chain_stats (id, latest_block, total_transactions, unique_addresses, evm_transactions, active_validators)
SELECT
  1,
  COALESCE(latest_block, 0),
  COALESCE(total_transactions, 0),
  COALESCE(unique_addresses, 0),
  COALESCE(evm_transactions, 0),
  COALESCE(active_validators, 0)
FROM api.mv_chain_stats
ON CONFLICT (id) DO UPDATE SET
  latest_block = EXCLUDED.latest_block,
  total_transactions = EXCLUDED.total_transactions,
  unique_addresses = EXCLUDED.unique_addresses,
  evm_transactions = EXCLUDED.evm_transactions,
  active_validators = EXCLUDED.active_validators,
  updated_at = NOW();

-- Trigger: blocks_raw INSERT -> update latest_block
CREATE OR REPLACE FUNCTION api.trg_rt_chain_stats_block()
RETURNS TRIGGER AS $$
BEGIN
  UPDATE api.rt_chain_stats SET latest_block = NEW.id, updated_at = NOW() WHERE id = 1;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_rt_chain_stats_block ON api.blocks_raw;
CREATE TRIGGER trg_rt_chain_stats_block
  AFTER INSERT ON api.blocks_raw
  FOR EACH ROW
  EXECUTE FUNCTION api.trg_rt_chain_stats_block();

-- Trigger: transactions_main INSERT -> increment total_transactions
CREATE OR REPLACE FUNCTION api.trg_rt_chain_stats_tx()
RETURNS TRIGGER AS $$
BEGIN
  UPDATE api.rt_chain_stats SET total_transactions = total_transactions + 1, updated_at = NOW() WHERE id = 1;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_rt_chain_stats_tx ON api.transactions_main;
CREATE TRIGGER trg_rt_chain_stats_tx
  AFTER INSERT ON api.transactions_main
  FOR EACH ROW
  EXECUTE FUNCTION api.trg_rt_chain_stats_tx();

-- Trigger: evm_transactions INSERT -> increment evm_transactions
CREATE OR REPLACE FUNCTION api.trg_rt_chain_stats_evm()
RETURNS TRIGGER AS $$
BEGIN
  UPDATE api.rt_chain_stats SET evm_transactions = evm_transactions + 1, updated_at = NOW() WHERE id = 1;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_rt_chain_stats_evm ON api.evm_transactions;
CREATE TRIGGER trg_rt_chain_stats_evm
  AFTER INSERT ON api.evm_transactions
  FOR EACH ROW
  EXECUTE FUNCTION api.trg_rt_chain_stats_evm();

-- Trigger: validators UPDATE (status/jailed change) -> recount active
CREATE OR REPLACE FUNCTION api.trg_rt_chain_stats_validators()
RETURNS TRIGGER AS $$
BEGIN
  -- Only recount if status or jailed actually changed
  IF OLD.status IS DISTINCT FROM NEW.status OR OLD.jailed IS DISTINCT FROM NEW.jailed THEN
    UPDATE api.rt_chain_stats SET
      active_validators = (SELECT COUNT(*)::INTEGER FROM api.validators WHERE status = 'BOND_STATUS_BONDED' AND NOT jailed),
      updated_at = NOW()
    WHERE id = 1;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_rt_chain_stats_validators ON api.validators;
CREATE TRIGGER trg_rt_chain_stats_validators
  AFTER UPDATE ON api.validators
  FOR EACH ROW
  EXECUTE FUNCTION api.trg_rt_chain_stats_validators();

-- Replace chain_stats view to read from rt table
DROP VIEW IF EXISTS api.chain_stats CASCADE;
CREATE VIEW api.chain_stats AS
SELECT
  latest_block,
  total_transactions,
  unique_addresses,
  evm_transactions,
  active_validators
FROM api.rt_chain_stats
WHERE id = 1;

GRANT SELECT ON api.rt_chain_stats TO web_anon;
GRANT SELECT ON api.chain_stats TO web_anon;

-- ============================================================================
-- 2B. rt_daily_tx_stats + rt_hourly_tx_stats
-- ============================================================================

-- Daily transaction stats
CREATE TABLE IF NOT EXISTS api.rt_daily_tx_stats (
  date DATE PRIMARY KEY,
  total_txs BIGINT NOT NULL DEFAULT 0,
  successful_txs BIGINT NOT NULL DEFAULT 0,
  failed_txs BIGINT NOT NULL DEFAULT 0,
  unique_senders BIGINT NOT NULL DEFAULT 0
);

-- Seed from existing MV
INSERT INTO api.rt_daily_tx_stats (date, total_txs, successful_txs, failed_txs, unique_senders)
SELECT date, total_txs, successful_txs, failed_txs, unique_senders
FROM api.mv_daily_tx_stats
ON CONFLICT (date) DO UPDATE SET
  total_txs = EXCLUDED.total_txs,
  successful_txs = EXCLUDED.successful_txs,
  failed_txs = EXCLUDED.failed_txs,
  unique_senders = EXCLUDED.unique_senders;

-- Hourly transaction stats (last 7 days)
CREATE TABLE IF NOT EXISTS api.rt_hourly_tx_stats (
  hour TIMESTAMPTZ PRIMARY KEY,
  tx_count BIGINT NOT NULL DEFAULT 0
);

-- Seed from existing MV
INSERT INTO api.rt_hourly_tx_stats (hour, tx_count)
SELECT hour, tx_count
FROM api.mv_hourly_tx_stats
ON CONFLICT (hour) DO UPDATE SET tx_count = EXCLUDED.tx_count;

-- Trigger: transactions_main INSERT -> increment daily + hourly
CREATE OR REPLACE FUNCTION api.trg_rt_tx_stats()
RETURNS TRIGGER AS $$
DECLARE
  tx_date DATE;
  tx_hour TIMESTAMPTZ;
  is_error BOOLEAN;
BEGIN
  -- Skip transactions with no timestamp (ingest errors)
  IF NEW.timestamp IS NULL THEN
    RETURN NEW;
  END IF;

  tx_date := NEW.timestamp::DATE;
  tx_hour := date_trunc('hour', NEW.timestamp);
  is_error := NEW.error IS NOT NULL;

  -- Daily stats
  INSERT INTO api.rt_daily_tx_stats (date, total_txs, successful_txs, failed_txs)
  VALUES (
    tx_date,
    1,
    CASE WHEN NOT is_error THEN 1 ELSE 0 END,
    CASE WHEN is_error THEN 1 ELSE 0 END
  )
  ON CONFLICT (date) DO UPDATE SET
    total_txs = api.rt_daily_tx_stats.total_txs + 1,
    successful_txs = api.rt_daily_tx_stats.successful_txs + CASE WHEN NOT is_error THEN 1 ELSE 0 END,
    failed_txs = api.rt_daily_tx_stats.failed_txs + CASE WHEN is_error THEN 1 ELSE 0 END;

  -- Hourly stats
  INSERT INTO api.rt_hourly_tx_stats (hour, tx_count)
  VALUES (tx_hour, 1)
  ON CONFLICT (hour) DO UPDATE SET tx_count = api.rt_hourly_tx_stats.tx_count + 1;

  -- Prune hourly entries older than 7 days (keep table bounded)
  DELETE FROM api.rt_hourly_tx_stats WHERE hour < NOW() - INTERVAL '7 days';

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_rt_tx_stats ON api.transactions_main;
CREATE TRIGGER trg_rt_tx_stats
  AFTER INSERT ON api.transactions_main
  FOR EACH ROW
  EXECUTE FUNCTION api.trg_rt_tx_stats();

-- Backward-compatible views
CREATE OR REPLACE VIEW api.daily_tx_stats AS
SELECT date, total_txs, successful_txs, failed_txs, unique_senders
FROM api.rt_daily_tx_stats
ORDER BY date DESC;

CREATE OR REPLACE VIEW api.hourly_tx_stats AS
SELECT hour, tx_count
FROM api.rt_hourly_tx_stats
ORDER BY hour DESC;

GRANT SELECT ON api.rt_daily_tx_stats TO web_anon;
GRANT SELECT ON api.rt_hourly_tx_stats TO web_anon;
GRANT SELECT ON api.daily_tx_stats TO web_anon;
GRANT SELECT ON api.hourly_tx_stats TO web_anon;

-- ============================================================================
-- 2C. rt_message_type_stats (replaces mv_message_type_stats)
-- ============================================================================

CREATE TABLE IF NOT EXISTS api.rt_message_type_stats (
  message_type TEXT PRIMARY KEY,
  count BIGINT NOT NULL DEFAULT 0
);

-- Seed from existing MV
INSERT INTO api.rt_message_type_stats (message_type, count)
SELECT message_type, count
FROM api.mv_message_type_stats
ON CONFLICT (message_type) DO UPDATE SET count = EXCLUDED.count;

-- Trigger: messages_main INSERT -> increment type counter
CREATE OR REPLACE FUNCTION api.trg_rt_message_type_stats()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO api.rt_message_type_stats (message_type, count)
  VALUES (NEW.type, 1)
  ON CONFLICT (message_type) DO UPDATE SET count = api.rt_message_type_stats.count + 1;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_rt_message_type_stats ON api.messages_main;
CREATE TRIGGER trg_rt_message_type_stats
  AFTER INSERT ON api.messages_main
  FOR EACH ROW
  EXECUTE FUNCTION api.trg_rt_message_type_stats();

-- Backward-compatible view with computed percentage
DROP VIEW IF EXISTS api.message_type_stats;
CREATE VIEW api.message_type_stats AS
SELECT
  message_type,
  count,
  ROUND((count::NUMERIC / NULLIF(SUM(count) OVER (), 0) * 100)::NUMERIC, 2) AS percentage
FROM api.rt_message_type_stats
ORDER BY count DESC;

GRANT SELECT ON api.rt_message_type_stats TO web_anon;
GRANT SELECT ON api.message_type_stats TO web_anon;

-- ============================================================================
-- 2D. rt_hourly_rewards (replaces mv_hourly_rewards)
-- ============================================================================

CREATE TABLE IF NOT EXISTS api.rt_hourly_rewards (
  hour TIMESTAMPTZ PRIMARY KEY,
  rewards NUMERIC NOT NULL DEFAULT 0,
  commission NUMERIC NOT NULL DEFAULT 0
);

-- Seed from existing MV
INSERT INTO api.rt_hourly_rewards (hour, rewards, commission)
SELECT hour, rewards, commission
FROM api.mv_hourly_rewards
ON CONFLICT (hour) DO UPDATE SET
  rewards = EXCLUDED.rewards,
  commission = EXCLUDED.commission;

-- Trigger: validator_rewards INSERT -> increment hourly bucket
CREATE OR REPLACE FUNCTION api.trg_rt_hourly_rewards()
RETURNS TRIGGER AS $$
DECLARE
  block_time TIMESTAMPTZ;
  reward_hour TIMESTAMPTZ;
BEGIN
  -- Look up block time from blocks_raw (should be in buffer during same transaction)
  SELECT (b.data->'block'->'header'->>'time')::TIMESTAMPTZ INTO block_time
  FROM api.blocks_raw b
  WHERE b.id = NEW.height;

  IF block_time IS NULL THEN
    -- Fallback to NOW() if block not found (shouldn't happen)
    block_time := NOW();
  END IF;

  reward_hour := date_trunc('hour', block_time);

  INSERT INTO api.rt_hourly_rewards (hour, rewards, commission)
  VALUES (reward_hour, COALESCE(NEW.rewards, 0), COALESCE(NEW.commission, 0))
  ON CONFLICT (hour) DO UPDATE SET
    rewards = api.rt_hourly_rewards.rewards + COALESCE(NEW.rewards, 0),
    commission = api.rt_hourly_rewards.commission + COALESCE(NEW.commission, 0);

  -- Prune entries older than 48 hours
  DELETE FROM api.rt_hourly_rewards WHERE hour < NOW() - INTERVAL '48 hours';

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_rt_hourly_rewards ON api.validator_rewards;
CREATE TRIGGER trg_rt_hourly_rewards
  AFTER INSERT ON api.validator_rewards
  FOR EACH ROW
  EXECUTE FUNCTION api.trg_rt_hourly_rewards();

-- Replace the function to read from real-time table
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
  SELECT rt.hour, rt.rewards, rt.commission
  FROM api.rt_hourly_rewards rt
  WHERE rt.hour > NOW() - (_hours || ' hours')::INTERVAL
  ORDER BY rt.hour DESC;
END;
$$ LANGUAGE plpgsql STABLE;

GRANT SELECT ON api.rt_hourly_rewards TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_hourly_rewards(INTEGER) TO web_anon;

-- ============================================================================
-- 2E. Update get_network_overview() to read from live tables
-- ============================================================================

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
    (SELECT COUNT(*)::INTEGER FROM api.validators) AS total_validators,
    (SELECT COUNT(*)::INTEGER FROM api.validators WHERE status = 'BOND_STATUS_BONDED' AND NOT jailed) AS active_validators,
    (SELECT COUNT(*)::INTEGER FROM api.validators WHERE jailed = TRUE) AS jailed_validators,
    (SELECT COALESCE(SUM(tokens), 0) FROM api.validators WHERE status = 'BOND_STATUS_BONDED') AS total_bonded_tokens,
    -- Sum last 24 hours from real-time hourly rewards
    (SELECT COALESCE(SUM(r.rewards), 0) FROM api.rt_hourly_rewards r WHERE r.hour > NOW() - INTERVAL '24 hours') AS total_rewards_24h,
    (SELECT COALESCE(SUM(r.commission), 0) FROM api.rt_hourly_rewards r WHERE r.hour > NOW() - INTERVAL '24 hours') AS total_commission_24h,
    -- Avg block time from last 100 blocks (lightweight query)
    (SELECT COALESCE(AVG(
      EXTRACT(EPOCH FROM (
        (b1.data->'block'->'header'->>'time')::timestamptz -
        (b2.data->'block'->'header'->>'time')::timestamptz
      ))
    ), 6)
    FROM api.blocks_raw b1
    JOIN api.blocks_raw b2 ON b2.id = b1.id - 1
    WHERE b1.id > (SELECT MAX(id) - 100 FROM api.blocks_raw)) AS avg_block_time,
    -- Read from rt_chain_stats
    (SELECT cs.total_transactions FROM api.rt_chain_stats cs WHERE cs.id = 1) AS total_transactions,
    (SELECT cs.unique_addresses FROM api.rt_chain_stats cs WHERE cs.id = 1) AS unique_addresses,
    (SELECT COUNT(*)::INTEGER FROM api.validators WHERE status = 'BOND_STATUS_BONDED') AS max_validators;
END;
$$ LANGUAGE plpgsql STABLE;

GRANT EXECUTE ON FUNCTION api.get_network_overview() TO web_anon;

COMMIT;
