BEGIN;

-- Migration 061: Fix trigger deadlocks under concurrent block inserts
--
-- Root cause: The indexer writes blocks concurrently (5 goroutines). Multiple
-- triggers fire per INSERT, and the combination of lock acquisitions across
-- concurrent transactions creates deadlock cycles.
--
-- Primary deadlock vector: trg_rt_chain_stats_block and trg_rt_chain_stats_tx
-- both UPDATE a single shared row (rt_chain_stats WHERE id=1). Every concurrent
-- block insert contends for this row lock, and combined with locks from other
-- triggers (extract_signatures, extract_finalize_events, validators UPDATE),
-- forms deadlock cycles.
--
-- Secondary vectors: DELETE prune operations in trg_rt_tx_stats and
-- trg_rt_hourly_rewards can conflict with concurrent inserts into the same tables.
--
-- Fix strategy:
-- 1. Drop hot-row triggers (rt_chain_stats) - replace with periodic refresh
-- 2. Remove DELETE prune operations from remaining triggers
-- 3. Add periodic refresh function for chain stats
-- 4. Convert extract_block_signatures to set-based INSERT

--------------------------------------------------------------------------------
-- 1. Drop hot-row triggers that cause the primary deadlock
--------------------------------------------------------------------------------

DROP TRIGGER IF EXISTS trg_rt_chain_stats_block ON api.blocks_raw;
DROP TRIGGER IF EXISTS trg_rt_chain_stats_tx ON api.transactions_main;

--------------------------------------------------------------------------------
-- 2. Replace trg_rt_tx_stats to remove the DELETE prune operation
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION api.trg_rt_tx_stats()
RETURNS TRIGGER AS $$
DECLARE
  tx_date DATE;
  tx_hour TIMESTAMPTZ;
  is_error BOOLEAN;
BEGIN
  IF NEW.timestamp IS NULL THEN
    RETURN NEW;
  END IF;

  tx_date := NEW.timestamp::DATE;
  tx_hour := date_trunc('hour', NEW.timestamp);
  is_error := NEW.error IS NOT NULL;

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

  INSERT INTO api.rt_hourly_tx_stats (hour, tx_count)
  VALUES (tx_hour, 1)
  ON CONFLICT (hour) DO UPDATE SET tx_count = api.rt_hourly_tx_stats.tx_count + 1;

  -- Prune moved to periodic refresh (was causing deadlocks)
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- 3. Replace trg_rt_hourly_rewards to remove the DELETE prune operation
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION api.trg_rt_hourly_rewards()
RETURNS TRIGGER AS $$
DECLARE
  block_time TIMESTAMPTZ;
  reward_hour TIMESTAMPTZ;
BEGIN
  SELECT (b.data->'block'->'header'->>'time')::TIMESTAMPTZ INTO block_time
  FROM api.blocks_raw b
  WHERE b.id = NEW.height;

  IF block_time IS NULL THEN
    block_time := NOW();
  END IF;

  reward_hour := date_trunc('hour', block_time);

  INSERT INTO api.rt_hourly_rewards (hour, rewards, commission)
  VALUES (reward_hour, COALESCE(NEW.rewards, 0), COALESCE(NEW.commission, 0))
  ON CONFLICT (hour) DO UPDATE SET
    rewards = api.rt_hourly_rewards.rewards + COALESCE(NEW.rewards, 0),
    commission = api.rt_hourly_rewards.commission + COALESCE(NEW.commission, 0);

  -- Prune moved to periodic refresh (was causing deadlocks)
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- 4. Convert extract_block_signatures to set-based INSERT
--    Eliminates row-at-a-time lock acquisition pattern
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION api.extract_block_signatures(
  _height BIGINT,
  _block_data JSONB
) RETURNS INTEGER AS $$
DECLARE
  block_time TIMESTAMPTZ;
  extracted_count INTEGER;
BEGIN
  block_time := (_block_data->'block'->'header'->>'time')::TIMESTAMPTZ;

  WITH sigs AS (
    SELECT
      row_number() OVER () - 1 AS sig_idx,
      UPPER(COALESCE(
        s->>'validatorAddress',
        s->>'validator_address',
        ''
      )) AS validator_addr,
      COALESCE(
        (s->>'blockIdFlag')::INTEGER,
        (s->>'block_id_flag')::INTEGER,
        1
      ) AS flag
    FROM jsonb_array_elements(
      COALESCE(
        _block_data->'block'->'lastCommit'->'signatures',
        _block_data->'block'->'last_commit'->'signatures',
        '[]'::JSONB
      )
    ) AS s
  )
  INSERT INTO api.validator_block_signatures (
    height, validator_index, consensus_address, signed, block_id_flag, block_time
  )
  SELECT
    _height,
    sig_idx,
    validator_addr,
    (flag = 2),
    flag,
    block_time
  FROM sigs
  WHERE validator_addr != ''
  ORDER BY sig_idx
  ON CONFLICT (height, validator_index) DO UPDATE SET
    consensus_address = EXCLUDED.consensus_address,
    signed = EXCLUDED.signed,
    block_id_flag = EXCLUDED.block_id_flag,
    block_time = EXCLUDED.block_time;

  GET DIAGNOSTICS extracted_count = ROW_COUNT;
  RETURN extracted_count;
END;
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- 5. Convert extract_finalize_block_events to set-based INSERT for events,
--    and use advisory lock for validators UPDATE to prevent deadlocks
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION api.extract_finalize_block_events()
RETURNS TRIGGER AS $$
DECLARE
  events JSONB;
BEGIN
  events := COALESCE(
    NEW.data->'finalizeBlockEvents',
    NEW.data->'finalize_block_events',
    '[]'::JSONB
  );

  IF jsonb_array_length(events) = 0 THEN
    RETURN NEW;
  END IF;

  -- Set-based INSERT for all events at once
  WITH event_data AS (
    SELECT
      row_number() OVER () - 1 AS event_idx,
      e->>'type' AS event_type,
      (
        SELECT jsonb_object_agg(
          COALESCE(a->>'key', ''),
          COALESCE(a->>'value', '')
        )
        FROM jsonb_array_elements(COALESCE(e->'attributes', '[]'::JSONB)) a
      ) AS attrs
    FROM jsonb_array_elements(events) AS e
  )
  INSERT INTO api.finalize_block_events (height, event_index, event_type, attributes)
  SELECT NEW.height, event_idx, event_type, attrs
  FROM event_data
  ORDER BY event_idx
  ON CONFLICT (height, event_index) DO UPDATE SET
    event_type = EXCLUDED.event_type,
    attributes = EXCLUDED.attributes;

  -- Handle jailing events: set-based INSERT
  WITH event_data AS (
    SELECT
      e->>'type' AS event_type,
      (
        SELECT jsonb_object_agg(
          COALESCE(a->>'key', ''),
          COALESCE(a->>'value', '')
        )
        FROM jsonb_array_elements(COALESCE(e->'attributes', '[]'::JSONB)) a
      ) AS attrs
    FROM jsonb_array_elements(events) AS e
    WHERE e->>'type' IN ('slash', 'liveness', 'jail')
  )
  INSERT INTO api.jailing_events (
    validator_address, height, prev_block_flag, current_block_flag
  )
  SELECT
    COALESCE(attrs->>'validator', attrs->>'address', ''),
    NEW.height,
    'FINALIZE_BLOCK_EVENT',
    event_type
  FROM event_data
  ON CONFLICT (validator_address, height) DO NOTHING;

  -- Update validator jailed status with advisory lock to prevent deadlocks
  -- across concurrent transactions updating the same validator rows
  PERFORM pg_advisory_xact_lock(hashtext('validator_jail_update'));

  WITH jail_addrs AS (
    SELECT DISTINCT COALESCE(
      (
        SELECT jsonb_object_agg(
          COALESCE(a->>'key', ''),
          COALESCE(a->>'value', '')
        )
        FROM jsonb_array_elements(COALESCE(e->'attributes', '[]'::JSONB)) a
      )->>'validator',
      (
        SELECT jsonb_object_agg(
          COALESCE(a->>'key', ''),
          COALESCE(a->>'value', '')
        )
        FROM jsonb_array_elements(COALESCE(e->'attributes', '[]'::JSONB)) a
      )->>'address',
      ''
    ) AS addr
    FROM jsonb_array_elements(events) AS e
    WHERE e->>'type' IN ('slash', 'liveness', 'jail')
  )
  UPDATE api.validators SET
    jailed = TRUE,
    updated_at = NOW()
  WHERE consensus_address IN (SELECT addr FROM jail_addrs WHERE addr != '')
     OR operator_address IN (
       SELECT vca.operator_address
       FROM api.validator_consensus_addresses vca
       WHERE vca.consensus_address IN (SELECT addr FROM jail_addrs WHERE addr != '')
     );

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- 6. Create periodic refresh function for chain stats and pruning
--    Called by validator-refresh service or cron, not by triggers
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION api.refresh_rt_chain_stats()
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  -- Refresh chain stats from source tables
  UPDATE api.rt_chain_stats SET
    latest_block = COALESCE((SELECT MAX(id) FROM api.blocks_raw), 0),
    total_transactions = COALESCE((SELECT count(*) FROM api.transactions_main), 0),
    updated_at = NOW()
  WHERE id = 1;

  -- Prune old hourly tx stats (>7 days)
  DELETE FROM api.rt_hourly_tx_stats WHERE hour < NOW() - INTERVAL '7 days';

  -- Prune old hourly rewards (>48 hours)
  DELETE FROM api.rt_hourly_rewards WHERE hour < NOW() - INTERVAL '48 hours';
END;
$$;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'postgres') THEN
    EXECUTE 'GRANT EXECUTE ON FUNCTION api.refresh_rt_chain_stats() TO postgres';
  END IF;
END
$$;

COMMIT;
