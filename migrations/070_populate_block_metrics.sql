BEGIN;

-- Migration 070: Populate block_metrics and add trigger
--
-- block_metrics (created in migration 039) is empty. get_validator_rewards_history
-- LEFT JOINs it for block_time, returning NULL timestamps for all rows.
--
-- Fix: backfill from blocks_raw using the generated block_time column (migration 068),
-- then add a trigger to keep it populated going forward.

-- ============================================================================
-- 1. Backfill block_metrics from blocks_raw
-- ============================================================================

INSERT INTO api.block_metrics (height, block_time, tx_count)
SELECT b.id, b.block_time, COALESCE(b.tx_count, 0)
FROM api.blocks_raw b
ON CONFLICT (height) DO NOTHING;

-- ============================================================================
-- 2. Trigger: populate block_metrics on blocks_raw INSERT
-- ============================================================================

CREATE OR REPLACE FUNCTION api.trg_populate_block_metrics()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO api.block_metrics (height, block_time, tx_count)
  VALUES (NEW.id, NEW.block_time, COALESCE(NEW.tx_count, 0))
  ON CONFLICT (height) DO UPDATE SET
    block_time = EXCLUDED.block_time,
    tx_count = EXCLUDED.tx_count;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_populate_block_metrics ON api.blocks_raw;
CREATE TRIGGER trg_populate_block_metrics
  AFTER INSERT ON api.blocks_raw
  FOR EACH ROW
  EXECUTE FUNCTION api.trg_populate_block_metrics();

COMMIT;
