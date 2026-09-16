BEGIN;

-- Migration 063: Handle base64-encoded validator addresses in block signatures
--
-- The indexer now stores validatorAddress as base64 (e.g. "cpj1wY+FoSXU7Ucbu+pHNJ+qahQ=")
-- instead of uppercase hex (e.g. "7298F5C18F85A125D4ED471BBBEA47349FAA6A14").
-- The validators table uses hex consensus_address, so we need to normalize at extraction time.

-- Helper: detect if a string is base64-encoded (contains +, /, or = which don't appear in hex)
-- and convert to uppercase hex. If already hex, just uppercase it.
CREATE OR REPLACE FUNCTION api.normalize_consensus_address(addr TEXT)
RETURNS TEXT LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE
    WHEN addr IS NULL OR addr = '' THEN ''
    WHEN addr ~ '[+/=]' THEN UPPER(encode(decode(addr, 'base64'), 'hex'))
    ELSE UPPER(addr)
  END;
$$;

-- Rebuild extract_block_signatures with base64 normalization
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
      api.normalize_consensus_address(COALESCE(
        s->>'validatorAddress',
        s->>'validator_address',
        ''
      )) AS validator_addr,
      CASE
        -- Handle string enum format
        WHEN COALESCE(s->>'blockIdFlag', s->>'block_id_flag', '') = 'BLOCK_ID_FLAG_ABSENT' THEN 1
        WHEN COALESCE(s->>'blockIdFlag', s->>'block_id_flag', '') = 'BLOCK_ID_FLAG_COMMIT' THEN 2
        WHEN COALESCE(s->>'blockIdFlag', s->>'block_id_flag', '') = 'BLOCK_ID_FLAG_NIL' THEN 3
        -- Handle integer format
        WHEN COALESCE(s->>'blockIdFlag', s->>'block_id_flag', '') ~ '^\d+$'
          THEN COALESCE(s->>'blockIdFlag', s->>'block_id_flag', '1')::INTEGER
        ELSE 1
      END AS flag
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

-- Also fix detect_jailing_from_block to normalize base64 addresses
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
  is_absent BOOLEAN;
  was_active BOOLEAN;
BEGIN
  current_height := (NEW.data->'block'->'header'->>'height')::BIGINT;

  current_sigs := COALESCE(
    NEW.data->'block'->'last_commit'->'signatures',
    NEW.data->'block'->'lastCommit'->'signatures',
    '[]'::JSONB
  );

  SELECT data INTO prev_block
  FROM api.blocks_raw
  WHERE id = current_height - 1;

  IF prev_block IS NULL THEN
    RETURN NEW;
  END IF;

  prev_sigs := COALESCE(
    prev_block.data->'block'->'last_commit'->'signatures',
    prev_block.data->'block'->'lastCommit'->'signatures',
    '[]'::JSONB
  );

  FOR sig IN SELECT * FROM jsonb_array_elements(current_sigs)
  LOOP
    val_addr := api.normalize_consensus_address(
      COALESCE(sig->>'validatorAddress', sig->>'validator_address')
    );
    current_flag := COALESCE(sig->>'blockIdFlag', sig->>'block_id_flag');

    IF val_addr IS NULL OR val_addr = '' THEN
      CONTINUE;
    END IF;

    -- Handle both string enum and integer formats for ABSENT check
    is_absent := (current_flag = 'BLOCK_ID_FLAG_ABSENT' OR current_flag = '1');

    IF NOT is_absent THEN
      -- Record consensus address mapping for active validators
      was_active := (current_flag IN ('BLOCK_ID_FLAG_COMMIT', 'BLOCK_ID_FLAG_NIL', '2', '3'));
      IF was_active THEN
        INSERT INTO api.validator_consensus_addresses (consensus_address, first_seen_height)
        VALUES (val_addr, current_height)
        ON CONFLICT (consensus_address) DO NOTHING;
      END IF;
      CONTINUE;
    END IF;

    -- Validator is now absent, check previous block status
    prev_flag := NULL;
    FOR prev_sig IN SELECT * FROM jsonb_array_elements(prev_sigs)
    LOOP
      IF api.normalize_consensus_address(
        COALESCE(prev_sig->>'validatorAddress', prev_sig->>'validator_address')
      ) = val_addr THEN
        prev_flag := COALESCE(prev_sig->>'blockIdFlag', prev_sig->>'block_id_flag');
        EXIT;
      END IF;
    END LOOP;

    -- Was actively signing before (handle both string enum and integer formats)
    was_active := (prev_flag IN ('BLOCK_ID_FLAG_COMMIT', 'BLOCK_ID_FLAG_NIL', '2', '3'));

    IF was_active THEN
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

-- Grant execute on the helper function
GRANT EXECUTE ON FUNCTION api.normalize_consensus_address(TEXT) TO web_anon;

-- Fix existing base64 addresses in validator_block_signatures
-- Convert all base64-encoded consensus_addresses to hex
UPDATE api.validator_block_signatures
SET consensus_address = UPPER(encode(decode(consensus_address, 'base64'), 'hex'))
WHERE consensus_address ~ '[+/=]';

COMMIT;
