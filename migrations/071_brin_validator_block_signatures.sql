BEGIN;

-- Migration 071: Replace B-tree with BRIN on validator_block_signatures(height)
--
-- validator_block_signatures has 23.4M rows. height is monotonically increasing
-- and only used for range filtering (WHERE height > window_start). All queries
-- that need ordering also filter by consensus_address and use the composite
-- indexes (idx_vbs_consensus_height, idx_vbs_missed).
--
-- The standalone idx_vbs_height B-tree (~200+ MB) can be replaced with a BRIN
-- index (~100 KB) that serves the same range predicates at a fraction of the
-- storage and write amplification cost.
--
-- BRIN works by storing min/max values per range of physical pages. Since rows
-- are inserted in height order, physical correlation is near-perfect.

DROP INDEX IF EXISTS api.idx_vbs_height;

CREATE INDEX IF NOT EXISTS idx_vbs_height_brin
  ON api.validator_block_signatures USING brin(height)
  WITH (pages_per_range = 32);

COMMIT;
