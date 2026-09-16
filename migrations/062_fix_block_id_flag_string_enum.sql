BEGIN;

-- Migration 062: Fix extract_block_signatures to handle string enum blockIdFlag
--
-- Some blocks encode blockIdFlag as a string enum (e.g. "BLOCK_ID_FLAG_COMMIT")
-- instead of an integer (2). The set-based rewrite in migration 061 didn't
-- account for this, causing SQLSTATE 22P02 errors starting at block ~250,333.

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

-- Also fix detect_jailing_from_block which compares against string enum values
-- The original function already used string comparisons, so it should be fine.
-- But let's also fix the detect_jailing trigger to handle both formats consistently.

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
    val_addr := COALESCE(sig->>'validatorAddress', sig->>'validator_address');
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
      IF COALESCE(prev_sig->>'validatorAddress', prev_sig->>'validator_address') = val_addr THEN
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

COMMIT;
