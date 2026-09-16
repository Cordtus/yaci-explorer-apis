-- Migration 041: Create block_results_raw table
--
-- The block_results_raw table stores block results data from the yaci indexer
-- when run with --enable-block-results flag. This table is required for:
-- - Extracting finalize_block_events (jailing, slashing)
-- - Extracting validator rewards and commission data
--
-- Note: The yaci indexer will INSERT into this table, but we need to CREATE it first.

BEGIN;

-- ============================================================================
-- Table: block_results_raw - Raw block results from yaci indexer
-- ============================================================================

CREATE TABLE IF NOT EXISTS api.block_results_raw (
  id BIGINT PRIMARY KEY,           -- Block height (matches blocks_raw.id)
  height BIGINT NOT NULL,          -- Same as id, for compatibility
  data JSONB NOT NULL DEFAULT '{}', -- Raw block results JSON
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index for efficient time-based queries on finalize_block_events
CREATE INDEX IF NOT EXISTS idx_block_results_raw_height ON api.block_results_raw(height DESC);

-- Grant permissions
GRANT SELECT ON api.block_results_raw TO web_anon;
GRANT INSERT, UPDATE ON api.block_results_raw TO web_anon;

-- ============================================================================
-- Recreate triggers that depend on block_results_raw
-- ============================================================================

-- Drop and recreate the finalize_block_events trigger
DROP TRIGGER IF EXISTS trigger_extract_finalize_events ON api.block_results_raw;
CREATE TRIGGER trigger_extract_finalize_events
  AFTER INSERT ON api.block_results_raw
  FOR EACH ROW
  EXECUTE FUNCTION api.extract_finalize_block_events();

-- Drop and recreate the rewards extraction trigger
DROP TRIGGER IF EXISTS trigger_extract_rewards ON api.block_results_raw;
CREATE TRIGGER trigger_extract_rewards
  AFTER INSERT ON api.block_results_raw
  FOR EACH ROW
  EXECUTE FUNCTION api.extract_rewards_from_events();

COMMIT;
