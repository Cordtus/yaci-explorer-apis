-- Migration 040: Parse block signatures from blocks_raw
--
-- Extracts validator signatures from block.last_commit.signatures to track:
-- - Which validators signed each block
-- - Which validators missed signing
-- - Calculate signing metrics (uptime, missed blocks)
--
-- Block signature structure (Cosmos SDK / CometBFT):
-- block.last_commit.signatures[] = {
--   block_id_flag: 1 (ABSENT), 2 (COMMIT/signed), 3 (NIL)
--   validator_address: hex-encoded consensus address
--   timestamp: when signed
--   signature: the actual signature
-- }

BEGIN;

-- ============================================================================
-- Table: validator_block_signatures - Track per-block signing status
-- ============================================================================

CREATE TABLE IF NOT EXISTS api.validator_block_signatures (
  id SERIAL PRIMARY KEY,
  height BIGINT NOT NULL,
  validator_index INTEGER NOT NULL,
  consensus_address TEXT NOT NULL,
  signed BOOLEAN NOT NULL,
  block_id_flag INTEGER NOT NULL,
  block_time TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT NOW(),

  UNIQUE(height, validator_index)
);

-- Indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_vbs_height ON api.validator_block_signatures(height DESC);
CREATE INDEX IF NOT EXISTS idx_vbs_consensus_addr ON api.validator_block_signatures(consensus_address);
CREATE INDEX IF NOT EXISTS idx_vbs_consensus_height ON api.validator_block_signatures(consensus_address, height DESC);
CREATE INDEX IF NOT EXISTS idx_vbs_missed ON api.validator_block_signatures(consensus_address, height DESC) WHERE NOT signed;

-- ============================================================================
-- Function: Extract signatures from a single block
-- ============================================================================

CREATE OR REPLACE FUNCTION api.extract_block_signatures(
  _height BIGINT,
  _block_data JSONB
) RETURNS INTEGER AS $$
DECLARE
  signatures JSONB;
  sig JSONB;
  sig_idx INTEGER;
  block_time TIMESTAMPTZ;
  validator_addr TEXT;
  flag INTEGER;
  is_signed BOOLEAN;
  extracted_count INTEGER := 0;
BEGIN
  -- Get block time
  block_time := (_block_data->'block'->'header'->>'time')::TIMESTAMPTZ;

  -- Get signatures array (handle both camelCase and snake_case)
  signatures := COALESCE(
    _block_data->'block'->'lastCommit'->'signatures',
    _block_data->'block'->'last_commit'->'signatures',
    '[]'::JSONB
  );

  IF jsonb_array_length(signatures) = 0 THEN
    RETURN 0;
  END IF;

  sig_idx := 0;
  FOR sig IN SELECT * FROM jsonb_array_elements(signatures)
  LOOP
    -- Get validator address (hex format from consensus)
    validator_addr := UPPER(COALESCE(
      sig->>'validatorAddress',
      sig->>'validator_address',
      ''
    ));

    -- Get block_id_flag: 1=absent, 2=commit(signed), 3=nil
    flag := COALESCE(
      (sig->>'blockIdFlag')::INTEGER,
      (sig->>'block_id_flag')::INTEGER,
      1
    );

    -- Validator signed if flag = 2 (BLOCK_ID_FLAG_COMMIT)
    is_signed := (flag = 2);

    -- Skip empty validator addresses (can happen for absent validators)
    IF validator_addr != '' THEN
      INSERT INTO api.validator_block_signatures (
        height, validator_index, consensus_address, signed, block_id_flag, block_time
      ) VALUES (
        _height, sig_idx, validator_addr, is_signed, flag, block_time
      )
      ON CONFLICT (height, validator_index) DO UPDATE SET
        consensus_address = EXCLUDED.consensus_address,
        signed = EXCLUDED.signed,
        block_id_flag = EXCLUDED.block_id_flag,
        block_time = EXCLUDED.block_time;

      extracted_count := extracted_count + 1;
    END IF;

    sig_idx := sig_idx + 1;
  END LOOP;

  RETURN extracted_count;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Trigger: Extract signatures on new blocks
-- ============================================================================

CREATE OR REPLACE FUNCTION api.trigger_extract_signatures()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM api.extract_block_signatures(NEW.id, NEW.data);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_extract_block_signatures ON api.blocks_raw;

CREATE TRIGGER trigger_extract_block_signatures
  AFTER INSERT ON api.blocks_raw
  FOR EACH ROW
  EXECUTE FUNCTION api.trigger_extract_signatures();

-- ============================================================================
-- Function: Get validator signing stats
-- ============================================================================

CREATE OR REPLACE FUNCTION api.get_validator_signing_stats(
  _consensus_address TEXT,
  _window_size INTEGER DEFAULT 10000
) RETURNS TABLE (
  total_blocks INTEGER,
  blocks_signed INTEGER,
  blocks_missed INTEGER,
  signing_percentage NUMERIC,
  recent_missed_count INTEGER,
  first_signed_height BIGINT,
  last_signed_height BIGINT
) AS $$
DECLARE
  max_height BIGINT;
BEGIN
  -- Get current max height
  SELECT MAX(id) INTO max_height FROM api.blocks_raw;

  RETURN QUERY
  SELECT
    COUNT(*)::INTEGER as total_blocks,
    COUNT(*) FILTER (WHERE vbs.signed)::INTEGER as blocks_signed,
    COUNT(*) FILTER (WHERE NOT vbs.signed)::INTEGER as blocks_missed,
    CASE
      WHEN COUNT(*) > 0 THEN
        ROUND((COUNT(*) FILTER (WHERE vbs.signed)::NUMERIC / COUNT(*)::NUMERIC) * 100, 2)
      ELSE 100
    END as signing_percentage,
    -- Recent missed in last 1000 blocks
    (SELECT COUNT(*)::INTEGER
     FROM api.validator_block_signatures
     WHERE consensus_address = UPPER(_consensus_address)
       AND NOT signed
       AND height > max_height - 1000) as recent_missed_count,
    MIN(vbs.height) FILTER (WHERE vbs.signed) as first_signed_height,
    MAX(vbs.height) FILTER (WHERE vbs.signed) as last_signed_height
  FROM api.validator_block_signatures vbs
  WHERE vbs.consensus_address = UPPER(_consensus_address)
    AND vbs.height > max_height - _window_size;
END;
$$ LANGUAGE plpgsql STABLE;

-- ============================================================================
-- Function: Get signing stats for all validators (for validators list)
-- ============================================================================

CREATE OR REPLACE FUNCTION api.get_all_validators_signing_stats(
  _window_size INTEGER DEFAULT 10000
) RETURNS TABLE (
  consensus_address TEXT,
  total_blocks INTEGER,
  blocks_signed INTEGER,
  blocks_missed INTEGER,
  signing_percentage NUMERIC
) AS $$
DECLARE
  max_height BIGINT;
BEGIN
  SELECT MAX(id) INTO max_height FROM api.blocks_raw;

  RETURN QUERY
  SELECT
    vbs.consensus_address,
    COUNT(*)::INTEGER as total_blocks,
    COUNT(*) FILTER (WHERE vbs.signed)::INTEGER as blocks_signed,
    COUNT(*) FILTER (WHERE NOT vbs.signed)::INTEGER as blocks_missed,
    CASE
      WHEN COUNT(*) > 0 THEN
        ROUND((COUNT(*) FILTER (WHERE vbs.signed)::NUMERIC / COUNT(*)::NUMERIC) * 100, 2)
      ELSE 100
    END as signing_percentage
  FROM api.validator_block_signatures vbs
  WHERE vbs.height > max_height - _window_size
  GROUP BY vbs.consensus_address;
END;
$$ LANGUAGE plpgsql STABLE;

-- ============================================================================
-- Function: Get validator performance (updated to use real signing data)
-- ============================================================================

CREATE OR REPLACE FUNCTION api.get_validator_performance(
  _operator_address TEXT
)
RETURNS TABLE (
  uptime_percentage NUMERIC,
  blocks_signed INTEGER,
  blocks_missed INTEGER,
  total_jailing_events INTEGER,
  last_jailed_height BIGINT,
  rewards_rank INTEGER,
  delegation_rank INTEGER
) AS $$
DECLARE
  consensus_addr TEXT;
  signing_stats RECORD;
BEGIN
  -- Get consensus address for this operator
  SELECT consensus_address INTO consensus_addr
  FROM api.validator_consensus_addresses
  WHERE operator_address = _operator_address
  LIMIT 1;

  -- Get signing stats from block signatures
  IF consensus_addr IS NOT NULL THEN
    SELECT * INTO signing_stats
    FROM api.get_validator_signing_stats(consensus_addr, 10000);
  END IF;

  RETURN QUERY
  SELECT
    -- Uptime from actual signing data
    COALESCE(signing_stats.signing_percentage, 100) as uptime_percentage,

    -- Blocks signed from actual data
    COALESCE(signing_stats.blocks_signed, 0) as blocks_signed,

    -- Blocks missed from actual data
    COALESCE(signing_stats.blocks_missed, 0) as blocks_missed,

    -- Total jailing events
    (SELECT COUNT(*)::INTEGER
     FROM api.jailing_events
     WHERE operator_address = _operator_address) as total_jailing_events,

    -- Last jailed height
    (SELECT MAX(height)
     FROM api.jailing_events
     WHERE operator_address = _operator_address) as last_jailed_height,

    -- Rewards rank
    (SELECT rank::INTEGER
     FROM (
       SELECT operator_address,
              RANK() OVER (ORDER BY lifetime_rewards DESC NULLS LAST) as rank
       FROM api.mv_validator_leaderboard
     ) ranked
     WHERE operator_address = _operator_address) as rewards_rank,

    -- Delegation rank
    (SELECT rank::INTEGER
     FROM (
       SELECT operator_address,
              RANK() OVER (ORDER BY delegator_count DESC NULLS LAST) as rank
       FROM api.mv_validator_leaderboard
     ) ranked
     WHERE operator_address = _operator_address) as delegation_rank;
END;
$$ LANGUAGE plpgsql STABLE;

-- ============================================================================
-- Materialized View: Validator signing summary (refreshed periodically)
-- ============================================================================

DROP MATERIALIZED VIEW IF EXISTS api.mv_validator_signing_stats CASCADE;

CREATE MATERIALIZED VIEW api.mv_validator_signing_stats AS
SELECT
  vbs.consensus_address,
  vca.operator_address,
  COUNT(*) as total_blocks,
  COUNT(*) FILTER (WHERE vbs.signed) as blocks_signed,
  COUNT(*) FILTER (WHERE NOT vbs.signed) as blocks_missed,
  CASE
    WHEN COUNT(*) > 0 THEN
      ROUND((COUNT(*) FILTER (WHERE vbs.signed)::NUMERIC / COUNT(*)::NUMERIC) * 100, 2)
    ELSE 100
  END as signing_percentage,
  MAX(vbs.height) as last_height
FROM api.validator_block_signatures vbs
LEFT JOIN api.validator_consensus_addresses vca ON vca.consensus_address = vbs.consensus_address
WHERE vbs.height > (SELECT MAX(id) - 10000 FROM api.blocks_raw)
GROUP BY vbs.consensus_address, vca.operator_address;

CREATE UNIQUE INDEX IF NOT EXISTS mv_validator_signing_stats_consensus_idx
ON api.mv_validator_signing_stats(consensus_address);

CREATE INDEX IF NOT EXISTS mv_validator_signing_stats_operator_idx
ON api.mv_validator_signing_stats(operator_address);

-- ============================================================================
-- Function: Backfill signatures from existing blocks
-- ============================================================================

CREATE OR REPLACE FUNCTION api.backfill_block_signatures(
  _start_height BIGINT DEFAULT NULL,
  _batch_size INTEGER DEFAULT 1000
) RETURNS TABLE(blocks_processed INTEGER, signatures_extracted INTEGER) AS $$
DECLARE
  rec RECORD;
  total_blocks INTEGER := 0;
  total_sigs INTEGER := 0;
  extracted INTEGER;
  actual_start BIGINT;
BEGIN
  -- Determine start height
  IF _start_height IS NULL THEN
    -- Start from where we left off, or block 1
    SELECT COALESCE(MAX(height), 0) + 1 INTO actual_start FROM api.validator_block_signatures;
  ELSE
    actual_start := _start_height;
  END IF;

  -- Process blocks in batches
  FOR rec IN
    SELECT id, data
    FROM api.blocks_raw
    WHERE id >= actual_start
    ORDER BY id
    LIMIT _batch_size
  LOOP
    extracted := api.extract_block_signatures(rec.id, rec.data);
    total_blocks := total_blocks + 1;
    total_sigs := total_sigs + extracted;
  END LOOP;

  RETURN QUERY SELECT total_blocks, total_sigs;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Function: Get validators list with signing stats (for main page)
-- ============================================================================

CREATE OR REPLACE FUNCTION api.get_validators_with_signing_stats(
  _limit INTEGER DEFAULT 100,
  _offset INTEGER DEFAULT 0
) RETURNS TABLE (
  operator_address TEXT,
  moniker TEXT,
  status TEXT,
  jailed BOOLEAN,
  tokens NUMERIC,
  voting_power_pct NUMERIC,
  commission_rate NUMERIC,
  signing_percentage NUMERIC,
  blocks_missed INTEGER
) AS $$
BEGIN
  RETURN QUERY
  SELECT
    v.operator_address,
    v.moniker,
    v.status,
    v.jailed,
    v.tokens,
    v.voting_power_pct,
    v.commission_rate,
    COALESCE(ss.signing_percentage, 100) as signing_percentage,
    COALESCE(ss.blocks_missed::INTEGER, 0) as blocks_missed
  FROM api.validators v
  LEFT JOIN api.mv_validator_signing_stats ss ON ss.operator_address = v.operator_address
  WHERE v.status = 'BOND_STATUS_BONDED'
  ORDER BY v.tokens DESC NULLS LAST
  LIMIT _limit
  OFFSET _offset;
END;
$$ LANGUAGE plpgsql STABLE;

-- ============================================================================
-- Grants
-- ============================================================================

GRANT SELECT ON api.validator_block_signatures TO web_anon;
GRANT SELECT ON api.mv_validator_signing_stats TO web_anon;
GRANT EXECUTE ON FUNCTION api.extract_block_signatures(BIGINT, JSONB) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_validator_signing_stats(TEXT, INTEGER) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_all_validators_signing_stats(INTEGER) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_validator_performance(TEXT) TO web_anon;
GRANT EXECUTE ON FUNCTION api.backfill_block_signatures(BIGINT, INTEGER) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_validators_with_signing_stats(INTEGER, INTEGER) TO web_anon;

COMMIT;
