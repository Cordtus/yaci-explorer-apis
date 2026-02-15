BEGIN;

-- Migration 073: Trim blocks_raw.data to reduce storage
--
-- blocks_raw stores the full GetBlockWithTxs gRPC response (~2KB/block).
-- Most of that is validator commit signatures and redundant header hashes
-- that are never queried after the INSERT triggers extract what they need.
--
-- This migration creates functions and triggers to:
--   1. Strip unnecessary header fields on ingest (BEFORE INSERT)
--      keeping lastCommit.signatures for downstream AFTER INSERT triggers
--   2. Remove lastCommit after signature extraction triggers finish (AFTER INSERT)
--
-- Historical data is NOT trimmed here (too expensive for a deploy migration).
-- Run `npx tsx scripts/trim-blocks-raw.ts` to backfill in production.
--
-- Expected savings: ~75% reduction in blocks_raw size
--   7.7M blocks * ~1.6KB saved per block = ~12GB reclaimed
--
-- Fields preserved in stored data:
--   block.header: time, height, chainId, proposerAddress
--   block.data    -- raw txs array (tx count)
--   blockId       -- block hash (frontend display)
--
-- Fields removed:
--   block.header: appHash, dataHash, validatorsHash, nextValidatorsHash,
--     consensusHash, lastCommitHash, lastResultsHash, evidenceHash,
--     lastBlockId, version
--   block.evidence  -- never read
--   block.lastCommit -- stripped AFTER signature extraction triggers run
--   pagination       -- never read

-- ============================================================================
-- 1. Function: strip block data to essential fields only (full trim)
--    Used by the backfill script for historical data.
-- ============================================================================

CREATE OR REPLACE FUNCTION api.trim_block_data(data jsonb)
RETURNS jsonb
LANGUAGE sql IMMUTABLE STRICT
AS $$
  SELECT jsonb_build_object(
    'block', jsonb_build_object(
      'header', jsonb_build_object(
        'height', data->'block'->'header'->'height',
        'time', data->'block'->'header'->'time',
        'chainId', COALESCE(
          data->'block'->'header'->'chainId',
          data->'block'->'header'->'chain_id'
        ),
        'proposerAddress', COALESCE(
          data->'block'->'header'->'proposerAddress',
          data->'block'->'header'->'proposer_address'
        )
      ),
      'data', data->'block'->'data'
    ),
    'blockId', COALESCE(data->'blockId', data->'block_id')
  );
$$;

-- ============================================================================
-- 2. Function: strip unnecessary header fields but keep lastCommit
--    Used by BEFORE INSERT so AFTER INSERT triggers still see signatures.
-- ============================================================================

CREATE OR REPLACE FUNCTION api.prune_block_header(data jsonb)
RETURNS jsonb
LANGUAGE sql IMMUTABLE STRICT
AS $$
  SELECT data
    -- Remove header hashes that are never queried
    #- '{block,header,appHash}'
    #- '{block,header,app_hash}'
    #- '{block,header,dataHash}'
    #- '{block,header,data_hash}'
    #- '{block,header,validatorsHash}'
    #- '{block,header,validators_hash}'
    #- '{block,header,nextValidatorsHash}'
    #- '{block,header,next_validators_hash}'
    #- '{block,header,consensusHash}'
    #- '{block,header,consensus_hash}'
    #- '{block,header,lastCommitHash}'
    #- '{block,header,last_commit_hash}'
    #- '{block,header,lastResultsHash}'
    #- '{block,header,last_results_hash}'
    #- '{block,header,evidenceHash}'
    #- '{block,header,evidence_hash}'
    #- '{block,header,lastBlockId}'
    #- '{block,header,last_block_id}'
    #- '{block,header,version}'
    -- Remove evidence (never read)
    #- '{block,evidence}'
    -- Remove pagination wrapper (never read)
    #- '{pagination}'
    ;
$$;

-- ============================================================================
-- 3. BEFORE INSERT trigger: strip header hashes before storage
--    Keeps lastCommit so AFTER INSERT signature triggers can read it.
--    Named 'trg_a_*' to fire first among BEFORE INSERT triggers.
-- ============================================================================

CREATE OR REPLACE FUNCTION api.trg_prune_block_on_insert()
RETURNS TRIGGER AS $$
BEGIN
  NEW.data := api.prune_block_header(NEW.data);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_a_prune_block_data ON api.blocks_raw;
CREATE TRIGGER trg_a_prune_block_data
  BEFORE INSERT ON api.blocks_raw
  FOR EACH ROW
  EXECUTE FUNCTION api.trg_prune_block_on_insert();

-- ============================================================================
-- 4. AFTER INSERT trigger: strip lastCommit after signature triggers finish
--
--    Trigger ordering on api.blocks_raw (alphabetical within timing):
--      BEFORE INSERT:
--        trg_a_prune_block_data    -- this: strip header hashes
--        trg_set_block_time        -- extract header.time -> block_time column
--      AFTER INSERT:
--        trg_populate_block_metrics -- reads block_time column (not JSONB)
--        trigger_detect_jailing     -- reads lastCommit.signatures
--        trigger_extract_block_signatures -- reads lastCommit.signatures
--        trigger_zzz_trim_commit    -- this: removes lastCommit (fires LAST)
-- ============================================================================

CREATE OR REPLACE FUNCTION api.trg_trim_block_after_insert()
RETURNS TRIGGER AS $$
BEGIN
  UPDATE api.blocks_raw
  SET data = data
    #- '{block,lastCommit}'
    #- '{block,last_commit}'
  WHERE id = NEW.id;

  RETURN NULL;  -- AFTER trigger return value is ignored
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_zzz_trim_commit ON api.blocks_raw;
CREATE TRIGGER trigger_zzz_trim_commit
  AFTER INSERT ON api.blocks_raw
  FOR EACH ROW
  EXECUTE FUNCTION api.trg_trim_block_after_insert();

COMMIT;
