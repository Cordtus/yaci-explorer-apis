-- Migration 032: Index jailing events from block signatures
--
-- Jailing events don't appear as transactions - they happen during begin_block.
-- We detect jailing by comparing validator signatures between consecutive blocks:
-- - A validator going from COMMIT/NIL to ABSENT suggests they may have been jailed
-- - We cross-reference with the validators table to confirm jailed status
--
-- This migration:
-- 1. Creates jailing_events table to store detected jailing events
-- 2. Creates a function to detect jailing from block signature changes
-- 3. Backfills from existing block data

BEGIN;

-- ============================================================================
-- Table: jailing_events - stores detected jailing events
-- ============================================================================

CREATE TABLE IF NOT EXISTS api.jailing_events (
  id SERIAL PRIMARY KEY,
  validator_address TEXT NOT NULL,         -- Consensus address (base64 encoded in blocks)
  operator_address TEXT,                   -- Operator address (if we can map it)
  height BIGINT NOT NULL,                  -- Block height where jailing was detected
  detected_at TIMESTAMPTZ DEFAULT NOW(),
  prev_block_flag TEXT,                    -- Previous block signature flag
  current_block_flag TEXT,                 -- Current block signature flag (should be ABSENT)

  UNIQUE(validator_address, height)
);

CREATE INDEX IF NOT EXISTS idx_jailing_events_height ON api.jailing_events(height);
CREATE INDEX IF NOT EXISTS idx_jailing_events_validator ON api.jailing_events(validator_address);
CREATE INDEX IF NOT EXISTS idx_jailing_events_operator ON api.jailing_events(operator_address);

-- ============================================================================
-- Table: validator_consensus_addresses - maps consensus to operator addresses
-- ============================================================================

CREATE TABLE IF NOT EXISTS api.validator_consensus_addresses (
  consensus_address TEXT PRIMARY KEY,      -- Base64 consensus address from block signatures
  operator_address TEXT REFERENCES api.validators(operator_address),
  first_seen_height BIGINT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_val_consensus_operator ON api.validator_consensus_addresses(operator_address);

-- ============================================================================
-- Function: Detect and record jailing events from block data
-- ============================================================================

CREATE OR REPLACE FUNCTION api.detect_jailing_from_block()
RETURNS TRIGGER AS $$
DECLARE
  current_height BIGINT;
  prev_block RECORD;
  current_sigs JSONB;
  prev_sigs JSONB;
  sig JSONB;
  prev_sig JSONB;
  val_addr TEXT;
  current_flag TEXT;
  prev_flag TEXT;
BEGIN
  -- Get current block height and signatures
  current_height := (NEW.data->'block'->'header'->>'height')::BIGINT;

  -- Get signatures from current block
  current_sigs := COALESCE(
    NEW.data->'block'->'last_commit'->'signatures',
    NEW.data->'block'->'lastCommit'->'signatures',
    '[]'::JSONB
  );

  -- Get previous block
  SELECT data INTO prev_block
  FROM api.blocks_raw
  WHERE id = current_height - 1;

  IF prev_block IS NULL THEN
    RETURN NEW;
  END IF;

  -- Get signatures from previous block
  prev_sigs := COALESCE(
    prev_block.data->'block'->'last_commit'->'signatures',
    prev_block.data->'block'->'lastCommit'->'signatures',
    '[]'::JSONB
  );

  -- Compare signatures to detect jailing
  -- Build a map of previous block signatures for lookup
  FOR sig IN SELECT * FROM jsonb_array_elements(current_sigs)
  LOOP
    val_addr := COALESCE(sig->>'validatorAddress', sig->>'validator_address');
    current_flag := COALESCE(sig->>'blockIdFlag', sig->>'block_id_flag');

    -- Skip if no validator address or not ABSENT
    IF val_addr IS NULL OR val_addr = '' THEN
      CONTINUE;
    END IF;

    -- Only look for validators that are now ABSENT
    IF current_flag != 'BLOCK_ID_FLAG_ABSENT' THEN
      -- Record consensus address mapping if we see a valid signature
      IF current_flag IN ('BLOCK_ID_FLAG_COMMIT', 'BLOCK_ID_FLAG_NIL') THEN
        INSERT INTO api.validator_consensus_addresses (consensus_address, first_seen_height)
        VALUES (val_addr, current_height)
        ON CONFLICT (consensus_address) DO NOTHING;
      END IF;
      CONTINUE;
    END IF;

    -- Check what this validator's status was in the previous block
    prev_flag := NULL;
    FOR prev_sig IN SELECT * FROM jsonb_array_elements(prev_sigs)
    LOOP
      IF COALESCE(prev_sig->>'validatorAddress', prev_sig->>'validator_address') = val_addr THEN
        prev_flag := COALESCE(prev_sig->>'blockIdFlag', prev_sig->>'block_id_flag');
        EXIT;
      END IF;
    END LOOP;

    -- If validator was actively signing before but is now ABSENT, record potential jailing
    -- The jailing happened at height-1 (the block they failed to sign)
    IF prev_flag IN ('BLOCK_ID_FLAG_COMMIT', 'BLOCK_ID_FLAG_NIL') THEN
      INSERT INTO api.jailing_events (
        validator_address, height, prev_block_flag, current_block_flag
      ) VALUES (
        val_addr, current_height - 1, prev_flag, current_flag
      )
      ON CONFLICT (validator_address, height) DO NOTHING;
    END IF;
  END LOOP;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Function: Backfill jailing events from historical blocks
-- Can be called manually: SELECT api.backfill_jailing_events();
-- ============================================================================

-- Drop the old function first (can't change return type with CREATE OR REPLACE)
DROP FUNCTION IF EXISTS api.backfill_jailing_events();

CREATE OR REPLACE FUNCTION api.backfill_jailing_events()
RETURNS TABLE(events_found INT, addresses_mapped INT) AS $$
DECLARE
  block_rec RECORD;
  prev_block_rec RECORD;
  current_sigs JSONB;
  prev_sigs JSONB;
  sig JSONB;
  prev_sig JSONB;
  val_addr TEXT;
  current_flag TEXT;
  prev_flag TEXT;
  events_count INT := 0;
  addr_count INT := 0;
BEGIN
  -- Iterate through all blocks starting from height 2
  FOR block_rec IN
    SELECT id, data FROM api.blocks_raw WHERE id > 1 ORDER BY id
  LOOP
    -- Get current block signatures
    current_sigs := COALESCE(
      block_rec.data->'block'->'last_commit'->'signatures',
      block_rec.data->'block'->'lastCommit'->'signatures',
      '[]'::JSONB
    );

    -- Get previous block
    SELECT id, data INTO prev_block_rec
    FROM api.blocks_raw
    WHERE id = block_rec.id - 1;

    IF prev_block_rec IS NULL THEN
      CONTINUE;
    END IF;

    -- Get previous block signatures
    prev_sigs := COALESCE(
      prev_block_rec.data->'block'->'last_commit'->'signatures',
      prev_block_rec.data->'block'->'lastCommit'->'signatures',
      '[]'::JSONB
    );

    -- Compare signatures
    FOR sig IN SELECT * FROM jsonb_array_elements(current_sigs)
    LOOP
      val_addr := COALESCE(sig->>'validatorAddress', sig->>'validator_address');
      current_flag := COALESCE(sig->>'blockIdFlag', sig->>'block_id_flag');

      IF val_addr IS NULL OR val_addr = '' THEN
        CONTINUE;
      END IF;

      -- Map consensus addresses
      IF current_flag IN ('BLOCK_ID_FLAG_COMMIT', 'BLOCK_ID_FLAG_NIL') THEN
        INSERT INTO api.validator_consensus_addresses (consensus_address, first_seen_height)
        VALUES (val_addr, block_rec.id)
        ON CONFLICT (consensus_address) DO NOTHING;
        GET DIAGNOSTICS addr_count = ROW_COUNT;
      END IF;

      -- Look for jailing (ABSENT now, was signing before)
      IF current_flag != 'BLOCK_ID_FLAG_ABSENT' THEN
        CONTINUE;
      END IF;

      prev_flag := NULL;
      FOR prev_sig IN SELECT * FROM jsonb_array_elements(prev_sigs)
      LOOP
        IF COALESCE(prev_sig->>'validatorAddress', prev_sig->>'validator_address') = val_addr THEN
          prev_flag := COALESCE(prev_sig->>'blockIdFlag', prev_sig->>'block_id_flag');
          EXIT;
        END IF;
      END LOOP;

      IF prev_flag IN ('BLOCK_ID_FLAG_COMMIT', 'BLOCK_ID_FLAG_NIL') THEN
        INSERT INTO api.jailing_events (
          validator_address, height, prev_block_flag, current_block_flag
        ) VALUES (
          val_addr, block_rec.id - 1, prev_flag, 'BLOCK_ID_FLAG_ABSENT'
        )
        ON CONFLICT (validator_address, height) DO NOTHING;
        GET DIAGNOSTICS events_count = ROW_COUNT;
      END IF;
    END LOOP;
  END LOOP;

  RETURN QUERY SELECT events_count, addr_count;
END;
$$ LANGUAGE plpgsql;

GRANT EXECUTE ON FUNCTION api.backfill_jailing_events() TO web_anon;

-- ============================================================================
-- Create trigger on blocks_raw
-- ============================================================================

DROP TRIGGER IF EXISTS trigger_detect_jailing ON api.blocks_raw;

CREATE TRIGGER trigger_detect_jailing
  AFTER INSERT ON api.blocks_raw
  FOR EACH ROW
  EXECUTE FUNCTION api.detect_jailing_from_block();

-- ============================================================================
-- Grants
-- ============================================================================

GRANT SELECT ON api.jailing_events TO web_anon;
GRANT SELECT ON api.validator_consensus_addresses TO web_anon;

COMMIT;
