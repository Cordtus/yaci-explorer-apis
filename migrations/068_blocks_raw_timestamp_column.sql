BEGIN;

-- Migration 068: Block timestamp column on blocks_raw
--
-- Extracts (data->'block'->'header'->>'time')::timestamptz into a proper column
-- so queries can use a B-tree index instead of JSONB extraction per row.
--
-- Uses a regular column + trigger + batched backfill instead of GENERATED STORED
-- to avoid a full table rewrite (blocks_raw is ~4 GB, disk is tight).
--
-- Affected functions:
--   get_blocks_paginated  -- date filtering in WHERE clause
--   trg_rt_hourly_rewards -- block_time lookup per validator_rewards INSERT
--   trg_rt_daily_rewards  -- same

-- ============================================================================
-- 1. Helper function (reusable by other migrations and triggers)
-- ============================================================================

CREATE OR REPLACE FUNCTION api.parse_block_time(data jsonb)
RETURNS timestamptz
LANGUAGE sql IMMUTABLE STRICT
AS $$
  SELECT (data->'block'->'header'->>'time')::timestamptz;
$$;

-- ============================================================================
-- 2. Add plain column (no table rewrite, just catalog update)
-- ============================================================================

ALTER TABLE api.blocks_raw
  ADD COLUMN IF NOT EXISTS block_time TIMESTAMPTZ;

-- ============================================================================
-- 3. Trigger: populate block_time on INSERT (before index creation so new
--    rows arriving during backfill also get populated)
-- ============================================================================

CREATE OR REPLACE FUNCTION api.trg_set_block_time()
RETURNS TRIGGER AS $$
BEGIN
  NEW.block_time := api.parse_block_time(NEW.data);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_set_block_time ON api.blocks_raw;
CREATE TRIGGER trg_set_block_time
  BEFORE INSERT ON api.blocks_raw
  FOR EACH ROW
  EXECUTE FUNCTION api.trg_set_block_time();

-- ============================================================================
-- 4. Backfill existing rows in batches of 10,000 to avoid long locks
-- ============================================================================

DO $$
DECLARE
  batch_size INT := 10000;
  updated INT;
  total INT := 0;
BEGIN
  LOOP
    UPDATE api.blocks_raw
    SET block_time = api.parse_block_time(data)
    WHERE id IN (
      SELECT id FROM api.blocks_raw
      WHERE block_time IS NULL
      LIMIT batch_size
    );
    GET DIAGNOSTICS updated = ROW_COUNT;
    total := total + updated;
    EXIT WHEN updated = 0;
    RAISE NOTICE 'Backfilled % rows (% total)', updated, total;
    PERFORM pg_sleep(0.1);
  END LOOP;
  RAISE NOTICE 'block_time backfill complete: % rows', total;
END;
$$;

-- ============================================================================
-- 5. Index (after backfill so it's built once, not incrementally)
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_blocks_raw_block_time
  ON api.blocks_raw(block_time DESC);

-- ============================================================================
-- 6. Update get_blocks_paginated to use the new column
-- ============================================================================

CREATE OR REPLACE FUNCTION api.get_blocks_paginated(
  _limit int DEFAULT 20,
  _offset int DEFAULT 0,
  _min_tx_count int DEFAULT NULL,
  _from_date timestamp DEFAULT NULL,
  _to_date timestamp DEFAULT NULL
)
RETURNS jsonb
LANGUAGE sql STABLE
AS $$
  WITH filtered_blocks AS (
    SELECT b.id, b.data, b.tx_count
    FROM api.blocks_raw b
    WHERE
      (_min_tx_count IS NULL OR b.tx_count >= _min_tx_count)
      AND (_from_date IS NULL OR b.block_time >= _from_date)
      AND (_to_date IS NULL OR b.block_time <= _to_date)
    ORDER BY b.id DESC
  ),
  total AS (
    SELECT COUNT(*) AS count FROM filtered_blocks
  ),
  paginated AS (
    SELECT * FROM filtered_blocks
    LIMIT _limit OFFSET _offset
  )
  SELECT jsonb_build_object(
    'data', COALESCE(jsonb_agg(
      jsonb_build_object(
        'id', p.id,
        'data', p.data,
        'tx_count', COALESCE(p.tx_count, 0)
      ) ORDER BY p.id DESC
    ), '[]'::jsonb),
    'pagination', jsonb_build_object(
      'total', (SELECT count FROM total),
      'limit', _limit,
      'offset', _offset,
      'has_next', _offset + _limit < (SELECT count FROM total),
      'has_prev', _offset > 0
    )
  )
  FROM paginated p;
$$;

-- ============================================================================
-- 7. Update trg_rt_hourly_rewards to use block_time column
-- ============================================================================

CREATE OR REPLACE FUNCTION api.trg_rt_hourly_rewards()
RETURNS TRIGGER AS $$
DECLARE
  block_time TIMESTAMPTZ;
  reward_hour TIMESTAMPTZ;
BEGIN
  SELECT b.block_time INTO block_time
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

-- ============================================================================
-- 8. Update trg_rt_daily_rewards to use block_time column
-- ============================================================================

CREATE OR REPLACE FUNCTION api.trg_rt_daily_rewards()
RETURNS TRIGGER AS $$
DECLARE
  block_time TIMESTAMPTZ;
  reward_date DATE;
BEGIN
  SELECT b.block_time INTO block_time
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

COMMIT;
