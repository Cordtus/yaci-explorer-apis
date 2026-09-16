-- Migration 050: Fix block signature address format
-- CometBFT returns validator addresses as base64 in block JSON.
-- The extract_block_signatures function was UPPER()ing these base64 strings,
-- corrupting them. This fix decodes base64 to uppercase hex for consistent
-- storage matching validator_consensus_addresses table.

BEGIN;

-- ============================================================================
-- 1. Fix extract_block_signatures to decode base64 addresses to hex
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
  raw_addr TEXT;
  validator_addr TEXT;
  raw_flag TEXT;
  flag_val TEXT;
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
    -- Get raw validator address (base64 from CometBFT or hex)
    raw_addr := COALESCE(
      sig->>'validatorAddress',
      sig->>'validator_address',
      ''
    );

    -- Convert base64 to uppercase hex for consistent storage
    -- CometBFT returns base64, but we store hex to match validator_consensus_addresses
    IF raw_addr != '' AND raw_addr ~ '[+/=a-z]' THEN
      -- Contains base64 characters, decode to hex
      BEGIN
        validator_addr := UPPER(encode(decode(raw_addr, 'base64'), 'hex'));
      EXCEPTION WHEN OTHERS THEN
        validator_addr := UPPER(raw_addr);
      END;
    ELSIF raw_addr != '' THEN
      -- Already hex or some other format
      validator_addr := UPPER(raw_addr);
    ELSE
      validator_addr := '';
    END IF;

    -- Get block_id_flag as text, handling both integer and string enum formats
    raw_flag := COALESCE(sig->>'blockIdFlag', sig->>'block_id_flag', '1');

    -- Normalize to string enum format
    CASE raw_flag
      WHEN '1' THEN flag_val := 'BLOCK_ID_FLAG_ABSENT';
      WHEN '2' THEN flag_val := 'BLOCK_ID_FLAG_COMMIT';
      WHEN '3' THEN flag_val := 'BLOCK_ID_FLAG_NIL';
      ELSE flag_val := raw_flag;
    END CASE;

    -- Validator signed if flag is COMMIT
    is_signed := (flag_val = 'BLOCK_ID_FLAG_COMMIT');

    -- Skip empty validator addresses (can happen for absent validators)
    IF validator_addr != '' THEN
      INSERT INTO api.validator_block_signatures (
        height, validator_index, consensus_address, signed, block_id_flag, block_time
      ) VALUES (
        _height, sig_idx, validator_addr, is_signed, flag_val, block_time
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
-- 2. After applying this migration, re-run the backfill:
--    SELECT api.backfill_block_signatures(1, <max_block>);
--    This will overwrite corrupted base64 addresses with correct hex.
-- ============================================================================

COMMIT;
