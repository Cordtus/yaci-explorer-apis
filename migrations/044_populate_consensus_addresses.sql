-- Migration 044: Populate validator consensus addresses
--
-- The consensus_address field in the validators table is typically NULL because
-- the consensus address derives from a different key (Ed25519 tendermint pubkey)
-- than the operator address (secp256k1 account pubkey).
--
-- This migration:
-- 1. Updates the MsgCreateValidator trigger to extract and store the consensus pubkey
-- 2. Creates a function to compute consensus address from Ed25519 pubkey
-- 3. Updates the validator_consensus_addresses mapping when validators are created
-- 4. Creates a view to help join validators with their consensus addresses

BEGIN;

-- ============================================================================
-- Function: Extract consensus address from MsgCreateValidator pubkey
-- The pubkey in MsgCreateValidator is an Any type with the Ed25519 key bytes
-- ============================================================================

-- Note: The pubkey is typically stored as:
-- {
--   "@type": "/cosmos.crypto.ed25519.PubKey",
--   "key": "<base64 encoded 32-byte Ed25519 pubkey>"
-- }
--
-- The consensus address is: base64(sha256(pubkey_bytes)[:20])
-- But PostgreSQL doesn't have native sha256 without pgcrypto.

-- First, ensure pgcrypto extension is available for sha256
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Function to compute consensus address from Ed25519 pubkey bytes
CREATE OR REPLACE FUNCTION api.compute_consensus_address(_pubkey_base64 TEXT)
RETURNS TEXT AS $$
DECLARE
  pubkey_bytes BYTEA;
  hash_bytes BYTEA;
  address_bytes BYTEA;
BEGIN
  -- Decode base64 pubkey
  pubkey_bytes := decode(_pubkey_base64, 'base64');

  -- SHA256 hash of pubkey bytes
  hash_bytes := digest(pubkey_bytes, 'sha256');

  -- Take first 20 bytes
  address_bytes := substring(hash_bytes from 1 for 20);

  -- Return as uppercase hex (matching CometBFT format)
  RETURN upper(encode(address_bytes, 'hex'));
END;
$$ LANGUAGE plpgsql IMMUTABLE;

GRANT EXECUTE ON FUNCTION api.compute_consensus_address(TEXT) TO web_anon;

-- ============================================================================
-- Trigger function to extract consensus pubkey from MsgCreateValidator
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
  tx_height BIGINT;
BEGIN
  -- Check if this is a MsgCreateValidator message
  msg_type := NEW.type;
  IF msg_type NOT LIKE '%MsgCreateValidator' THEN
    RETURN NEW;
  END IF;

  -- Get the raw message data
  SELECT data INTO raw_data
  FROM api.messages_raw
  WHERE id = NEW.id;

  IF raw_data IS NULL THEN
    RETURN NEW;
  END IF;

  -- Get the transaction height
  SELECT height INTO tx_height
  FROM api.transactions_main
  WHERE id = NEW.id;

  -- Extract pubkey (handles both camelCase and snake_case)
  pubkey_data := COALESCE(raw_data->'pubkey', raw_data->'pub_key');
  IF pubkey_data IS NULL THEN
    RETURN NEW;
  END IF;

  -- Get the base64 key value
  pubkey_base64 := pubkey_data->>'key';
  IF pubkey_base64 IS NULL OR pubkey_base64 = '' THEN
    RETURN NEW;
  END IF;

  -- Compute consensus address
  consensus_addr := api.compute_consensus_address(pubkey_base64);

  -- Get validator operator address
  valoper_addr := COALESCE(raw_data->>'validatorAddress', raw_data->>'validator_address');
  IF valoper_addr IS NULL OR valoper_addr = '' THEN
    RETURN NEW;
  END IF;

  -- Update the validators table with consensus address
  UPDATE api.validators
  SET consensus_address = consensus_addr
  WHERE operator_address = valoper_addr
    AND (consensus_address IS NULL OR consensus_address = '');

  -- Also update the validator_consensus_addresses mapping
  INSERT INTO api.validator_consensus_addresses (
    consensus_address,
    operator_address,
    first_seen_height
  ) VALUES (
    consensus_addr,
    valoper_addr,
    tx_height
  )
  ON CONFLICT (consensus_address) DO UPDATE
  SET operator_address = EXCLUDED.operator_address
  WHERE api.validator_consensus_addresses.operator_address IS NULL;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger on messages_main for new MsgCreateValidator messages
DROP TRIGGER IF EXISTS trigger_extract_validator_consensus_pubkey ON api.messages_main;
CREATE TRIGGER trigger_extract_validator_consensus_pubkey
  AFTER INSERT ON api.messages_main
  FOR EACH ROW
  EXECUTE FUNCTION api.extract_validator_consensus_pubkey();

-- ============================================================================
-- Backfill function to process existing MsgCreateValidator messages
-- ============================================================================

CREATE OR REPLACE FUNCTION api.backfill_validator_consensus_addresses()
RETURNS TABLE(processed INTEGER, updated INTEGER) AS $$
DECLARE
  msg RECORD;
  raw_data JSONB;
  pubkey_data JSONB;
  pubkey_base64 TEXT;
  consensus_addr TEXT;
  valoper_addr TEXT;
  processed_count INTEGER := 0;
  updated_count INTEGER := 0;
BEGIN
  FOR msg IN
    SELECT m.id, m.type, t.height, mr.data
    FROM api.messages_main m
    JOIN api.messages_raw mr ON mr.id = m.id
    JOIN api.transactions_main t ON t.id = m.id
    WHERE m.type LIKE '%MsgCreateValidator'
    ORDER BY t.height
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

    -- Compute consensus address
    consensus_addr := api.compute_consensus_address(pubkey_base64);

    -- Get validator operator address
    valoper_addr := COALESCE(raw_data->>'validatorAddress', raw_data->>'validator_address');
    IF valoper_addr IS NULL OR valoper_addr = '' THEN
      CONTINUE;
    END IF;

    -- Update validators table
    UPDATE api.validators
    SET consensus_address = consensus_addr
    WHERE operator_address = valoper_addr
      AND (consensus_address IS NULL OR consensus_address = '');

    IF FOUND THEN
      updated_count := updated_count + 1;
    END IF;

    -- Update mapping table
    INSERT INTO api.validator_consensus_addresses (
      consensus_address,
      operator_address,
      first_seen_height
    ) VALUES (
      consensus_addr,
      valoper_addr,
      msg.height
    )
    ON CONFLICT (consensus_address) DO UPDATE
    SET operator_address = EXCLUDED.operator_address
    WHERE api.validator_consensus_addresses.operator_address IS NULL;
  END LOOP;

  RETURN QUERY SELECT processed_count, updated_count;
END;
$$ LANGUAGE plpgsql;

GRANT EXECUTE ON FUNCTION api.backfill_validator_consensus_addresses() TO web_anon;

-- ============================================================================
-- View: validators_with_consensus - joins validators with consensus addresses
-- ============================================================================

DROP VIEW IF EXISTS api.validators_with_consensus CASCADE;

CREATE OR REPLACE VIEW api.validators_with_consensus AS
SELECT
  v.*,
  COALESCE(v.consensus_address, vca.consensus_address) AS resolved_consensus_address
FROM api.validators v
LEFT JOIN api.validator_consensus_addresses vca
  ON vca.operator_address = v.operator_address;

GRANT SELECT ON api.validators_with_consensus TO web_anon;

-- ============================================================================
-- Run backfill immediately
-- ============================================================================

DO $$
DECLARE
  result RECORD;
BEGIN
  SELECT * INTO result FROM api.backfill_validator_consensus_addresses();
  RAISE NOTICE 'Backfill complete: processed=%, updated=%', result.processed, result.updated;
END $$;

COMMIT;
