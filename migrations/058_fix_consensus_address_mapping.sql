-- Migration 058: Fix consensus address mapping globally
--
-- Problems:
--   1. Genesis validators have no MsgCreateValidator in transactions_main,
--      so the backfill from migration 044 never maps their consensus addresses.
--   2. get_validator_performance 500s when signing_stats RECORD is uninitialized.
--   3. Base64 entries in validator_consensus_addresses have operator_address=NULL
--      even when a matching hex entry (from MsgCreateValidator) exists.
--   4. validators.consensus_address is NULL for affected validators.
--
-- Solution:
--   1. Fix get_validator_performance for defensive NULL handling.
--   2. Re-run MsgCreateValidator backfill with hex entry creation.
--   3. Cross-reference base64 <-> hex entries to propagate operator_address.
--   4. Register known genesis validators from node pubkey data.
--   5. Sync validators.consensus_address from mapping table.
--   6. Improve triggers for forward-looking correctness.

BEGIN;

-- ============================================================================
-- 1. Fix get_validator_performance - handle uninitialized RECORD
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
  has_signing_stats BOOLEAN := FALSE;
BEGIN
  -- Get consensus address for this operator (prefer hex entries)
  SELECT COALESCE(vca.hex_address, vca.consensus_address) INTO consensus_addr
  FROM api.validator_consensus_addresses vca
  WHERE vca.operator_address = _operator_address
  ORDER BY (vca.hex_address IS NOT NULL) DESC
  LIMIT 1;

  -- Get signing stats from block signatures
  IF consensus_addr IS NOT NULL THEN
    SELECT * INTO signing_stats
    FROM api.get_validator_signing_stats(consensus_addr, 10000);
    has_signing_stats := FOUND;
  END IF;

  RETURN QUERY
  SELECT
    CASE WHEN has_signing_stats
      THEN COALESCE(signing_stats.signing_percentage, 100)
      ELSE 100
    END as uptime_percentage,

    CASE WHEN has_signing_stats
      THEN COALESCE(signing_stats.blocks_signed, 0)
      ELSE 0
    END as blocks_signed,

    CASE WHEN has_signing_stats
      THEN COALESCE(signing_stats.blocks_missed, 0)
      ELSE 0
    END as blocks_missed,

    (SELECT COUNT(*)::INTEGER
     FROM api.jailing_events
     WHERE operator_address = _operator_address) as total_jailing_events,

    (SELECT MAX(height)
     FROM api.jailing_events
     WHERE operator_address = _operator_address) as last_jailed_height,

    (SELECT rank::INTEGER
     FROM (
       SELECT operator_address,
              RANK() OVER (ORDER BY lifetime_rewards DESC NULLS LAST) as rank
       FROM api.mv_validator_leaderboard
     ) ranked
     WHERE operator_address = _operator_address) as rewards_rank,

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
-- 2. Helper: Register a validator's consensus address from its Ed25519 pubkey
-- ============================================================================

CREATE OR REPLACE FUNCTION api.register_validator_consensus_address(
  _operator_address TEXT,
  _pubkey_base64 TEXT
) RETURNS TEXT AS $$
DECLARE
  hex_addr TEXT;
BEGIN
  -- Compute hex consensus address from pubkey
  hex_addr := api.compute_consensus_address(_pubkey_base64);

  -- Insert/update hex entry in mapping table
  INSERT INTO api.validator_consensus_addresses (
    consensus_address, operator_address, hex_address, first_seen_height
  ) VALUES (
    hex_addr, _operator_address, hex_addr, 1
  )
  ON CONFLICT (consensus_address) DO UPDATE
  SET operator_address = _operator_address,
      hex_address = hex_addr;

  -- Update validators table
  UPDATE api.validators
  SET consensus_address = hex_addr
  WHERE operator_address = _operator_address
    AND (consensus_address IS NULL OR consensus_address = '');

  -- Also update any base64 entries that have matching hex_address
  UPDATE api.validator_consensus_addresses
  SET operator_address = _operator_address
  WHERE hex_address = hex_addr
    AND operator_address IS NULL;

  RETURN hex_addr;
END;
$$ LANGUAGE plpgsql;

GRANT EXECUTE ON FUNCTION api.register_validator_consensus_address(TEXT, TEXT) TO web_anon;

-- ============================================================================
-- 3. Re-run MsgCreateValidator backfill with hex entry creation
-- ============================================================================

-- Improved backfill that creates hex entries and updates validators table
DO $$
DECLARE
  msg RECORD;
  raw_data JSONB;
  pubkey_data JSONB;
  pubkey_base64 TEXT;
  hex_addr TEXT;
  valoper_addr TEXT;
  processed_count INTEGER := 0;
  updated_count INTEGER := 0;
BEGIN
  FOR msg IN
    SELECT m.id, m.type, mr.data
    FROM api.messages_main m
    JOIN api.messages_raw mr ON mr.id = m.id
    WHERE m.type LIKE '%MsgCreateValidator'
  LOOP
    processed_count := processed_count + 1;
    raw_data := msg.data;

    -- Extract pubkey
    pubkey_data := COALESCE(raw_data->'pubkey', raw_data->'pub_key');
    IF pubkey_data IS NULL THEN
      CONTINUE;
    END IF;

    pubkey_base64 := pubkey_data->>'key';
    IF pubkey_base64 IS NULL OR pubkey_base64 = '' THEN
      CONTINUE;
    END IF;

    -- Get validator operator address
    valoper_addr := COALESCE(raw_data->>'validatorAddress', raw_data->>'validator_address');
    IF valoper_addr IS NULL OR valoper_addr = '' THEN
      CONTINUE;
    END IF;

    -- Compute hex consensus address
    hex_addr := api.compute_consensus_address(pubkey_base64);

    -- Insert/update hex entry in mapping table
    INSERT INTO api.validator_consensus_addresses (
      consensus_address, operator_address, hex_address, first_seen_height
    ) VALUES (
      hex_addr, valoper_addr, hex_addr, 1
    )
    ON CONFLICT (consensus_address) DO UPDATE
    SET operator_address = valoper_addr,
        hex_address = hex_addr;

    -- Update validators table
    UPDATE api.validators
    SET consensus_address = hex_addr
    WHERE operator_address = valoper_addr
      AND (consensus_address IS NULL OR consensus_address = '');

    IF FOUND THEN
      updated_count := updated_count + 1;
    END IF;

    -- Also update any base64 entries with matching hex_address
    UPDATE api.validator_consensus_addresses
    SET operator_address = valoper_addr
    WHERE hex_address = hex_addr
      AND operator_address IS NULL;
  END LOOP;

  RAISE NOTICE 'MsgCreateValidator backfill: processed=%, updated=%', processed_count, updated_count;
END $$;

-- ============================================================================
-- 4. Register known genesis validators (no MsgCreateValidator in DB)
-- ============================================================================

-- republic-validator (val0): pubkey from node status
-- Only runs if the validator exists in the database (skips in CI dry-run)
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM api.validators WHERE operator_address = 'raivaloper1a6xl9lpxyv5e4gujfx7qslef9496mhwtpk9lh6') THEN
    PERFORM api.register_validator_consensus_address(
      'raivaloper1a6xl9lpxyv5e4gujfx7qslef9496mhwtpk9lh6',
      'fiHxSVPIdcfrAmNaTwRrU09DbK+II97I1m1QnVEneCw='
    );
  END IF;
END $$;

-- ============================================================================
-- 5. Cross-reference: propagate operator_address between entries
-- ============================================================================

-- For base64 entries with hex_address but no operator_address,
-- check if a hex entry exists with operator_address set
UPDATE api.validator_consensus_addresses base64_entry
SET operator_address = hex_entry.operator_address
FROM api.validator_consensus_addresses hex_entry
WHERE base64_entry.hex_address = hex_entry.consensus_address
  AND base64_entry.operator_address IS NULL
  AND hex_entry.operator_address IS NOT NULL;

-- For hex entries with no operator_address, check if a base64 entry
-- with matching hex_address has operator_address set
UPDATE api.validator_consensus_addresses hex_entry
SET operator_address = base64_entry.operator_address
FROM api.validator_consensus_addresses base64_entry
WHERE base64_entry.hex_address = hex_entry.consensus_address
  AND hex_entry.operator_address IS NULL
  AND base64_entry.operator_address IS NOT NULL;

-- ============================================================================
-- 6. Sync validators.consensus_address from mapping table
-- ============================================================================

UPDATE api.validators v
SET consensus_address = vca.hex_addr
FROM (
  SELECT operator_address, COALESCE(hex_address, consensus_address) AS hex_addr
  FROM api.validator_consensus_addresses
  WHERE operator_address IS NOT NULL
    AND (hex_address IS NOT NULL OR consensus_address ~ '^[0-9A-F]+$')
) vca
WHERE v.operator_address = vca.operator_address
  AND (v.consensus_address IS NULL OR v.consensus_address = '');

-- ============================================================================
-- 7. Improve extract_validator_consensus_pubkey trigger for forward-looking fix
--    After mapping a hex address to an operator, propagate to base64 entries
-- ============================================================================

CREATE OR REPLACE FUNCTION api.extract_validator_consensus_pubkey()
RETURNS TRIGGER AS $$
DECLARE
  msg_type TEXT;
  raw_data JSONB;
  pubkey_data JSONB;
  pubkey_base64 TEXT;
  consensus_addr TEXT;
  valoper_addr TEXT;
BEGIN
  msg_type := NEW.type;
  IF msg_type NOT LIKE '%MsgCreateValidator' THEN
    RETURN NEW;
  END IF;

  SELECT data INTO raw_data
  FROM api.messages_raw
  WHERE id = NEW.id;

  IF raw_data IS NULL THEN
    RETURN NEW;
  END IF;

  -- Extract pubkey (handles both camelCase and snake_case)
  pubkey_data := COALESCE(raw_data->'pubkey', raw_data->'pub_key');
  IF pubkey_data IS NULL THEN
    RETURN NEW;
  END IF;

  pubkey_base64 := pubkey_data->>'key';
  IF pubkey_base64 IS NULL OR pubkey_base64 = '' THEN
    RETURN NEW;
  END IF;

  -- Compute consensus address (hex)
  consensus_addr := api.compute_consensus_address(pubkey_base64);

  -- Get validator operator address
  valoper_addr := COALESCE(raw_data->>'validatorAddress', raw_data->>'validator_address');
  IF valoper_addr IS NULL OR valoper_addr = '' THEN
    RETURN NEW;
  END IF;

  -- Insert/update hex entry in mapping table
  INSERT INTO api.validator_consensus_addresses (
    consensus_address, operator_address, hex_address, first_seen_height
  ) VALUES (
    consensus_addr, valoper_addr, consensus_addr, 1
  )
  ON CONFLICT (consensus_address) DO UPDATE
  SET operator_address = valoper_addr,
      hex_address = consensus_addr;

  -- Update validators table
  UPDATE api.validators
  SET consensus_address = consensus_addr
  WHERE operator_address = valoper_addr
    AND (consensus_address IS NULL OR consensus_address = '');

  -- Propagate to any base64 entries with matching hex_address
  UPDATE api.validator_consensus_addresses
  SET operator_address = valoper_addr
  WHERE hex_address = consensus_addr
    AND operator_address IS NULL;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 8. Refresh materialized views that use consensus address data
-- ============================================================================

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname = 'api' AND matviewname = 'mv_validator_signing_stats') THEN
    REFRESH MATERIALIZED VIEW api.mv_validator_signing_stats;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname = 'api' AND matviewname = 'mv_validator_leaderboard') THEN
    REFRESH MATERIALIZED VIEW api.mv_validator_leaderboard;
  END IF;
END $$;

COMMIT;
