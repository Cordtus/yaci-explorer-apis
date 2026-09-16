-- Migration 051: Use 10,000 block window for signing stats
-- Changes signing_percentage, blocks_signed, blocks_missed on validators table
-- to reflect performance over the last 10,000 blocks rather than all-time cumulative.

BEGIN;

-- ============================================================================
-- 1. Replace the per-row trigger with a 10K window recalculation
-- ============================================================================

CREATE OR REPLACE FUNCTION api.trg_update_validator_signing_stats()
RETURNS TRIGGER AS $$
DECLARE
  op_addr TEXT;
  window_start BIGINT;
  v_signed BIGINT;
  v_missed BIGINT;
  v_pct NUMERIC;
BEGIN
  -- Resolve consensus address to operator address
  SELECT vca.operator_address INTO op_addr
  FROM api.validator_consensus_addresses vca
  WHERE vca.consensus_address = NEW.consensus_address
    OR vca.hex_address = UPPER(NEW.consensus_address)
  LIMIT 1;

  IF op_addr IS NULL THEN
    RETURN NEW;
  END IF;

  -- Calculate window start (last 10,000 blocks)
  window_start := NEW.height - 10000;

  -- Count signed/missed in the 10K window
  SELECT
    COUNT(*) FILTER (WHERE signed),
    COUNT(*) FILTER (WHERE NOT signed)
  INTO v_signed, v_missed
  FROM api.validator_block_signatures
  WHERE consensus_address = NEW.consensus_address
    AND height > window_start;

  -- Calculate percentage
  IF v_signed + v_missed > 0 THEN
    v_pct := ROUND(v_signed::NUMERIC / (v_signed + v_missed)::NUMERIC * 100, 2);
  ELSE
    v_pct := NULL;
  END IF;

  UPDATE api.validators
  SET
    blocks_signed = v_signed,
    blocks_missed = v_missed,
    signing_percentage = v_pct,
    last_signed_height = CASE WHEN NEW.signed THEN NEW.height ELSE last_signed_height END,
    updated_at = NOW()
  WHERE operator_address = op_addr;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 2. Recalculate all validators using 10K window from current tip
-- ============================================================================

DO $$
DECLARE
  v_rec RECORD;
  v_signed BIGINT;
  v_missed BIGINT;
  v_pct NUMERIC;
  v_last_height BIGINT;
  window_start BIGINT;
  updated_count INT := 0;
BEGIN
  SELECT MAX(id) - 10000 INTO window_start FROM api.blocks_raw;

  FOR v_rec IN
    SELECT DISTINCT vca.operator_address, vca.consensus_address
    FROM api.validator_consensus_addresses vca
  LOOP
    BEGIN
      SELECT
        COUNT(*) FILTER (WHERE signed),
        COUNT(*) FILTER (WHERE NOT signed),
        MAX(height) FILTER (WHERE signed)
      INTO v_signed, v_missed, v_last_height
      FROM api.validator_block_signatures
      WHERE consensus_address = UPPER(v_rec.consensus_address)
        AND height > window_start;

      v_pct := CASE
        WHEN v_signed + v_missed > 0 THEN
          ROUND(v_signed::NUMERIC / (v_signed + v_missed)::NUMERIC * 100, 2)
        ELSE NULL
      END;

      UPDATE api.validators SET
        blocks_signed = v_signed,
        blocks_missed = v_missed,
        signing_percentage = v_pct,
        last_signed_height = COALESCE(v_last_height, last_signed_height),
        updated_at = NOW()
      WHERE operator_address = v_rec.operator_address;

      updated_count := updated_count + 1;
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'Error for %: %', v_rec.operator_address, SQLERRM;
    END;
  END LOOP;

  RAISE NOTICE 'Recalculated 10K window signing stats for % validators', updated_count;
END;
$$;

COMMIT;
