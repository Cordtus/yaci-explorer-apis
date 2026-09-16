-- =============================================================================
-- Fix Address Extraction
-- - Add delegatorAddress and EVM from field extraction to update_message_main
-- - Update chain_stats to include EVM addresses and mentions
-- - Backfill all existing messages
-- =============================================================================

BEGIN;

-- =============================================================================
-- HELPER FUNCTION: Convert base64 EVM address to hex format
-- =============================================================================

CREATE OR REPLACE FUNCTION base64_to_hex_address(b64 TEXT)
RETURNS TEXT
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  raw_bytes BYTEA;
BEGIN
  IF b64 IS NULL OR b64 = '' THEN
    RETURN NULL;
  END IF;

  BEGIN
    raw_bytes := decode(b64, 'base64');
    -- EVM addresses are 20 bytes
    IF length(raw_bytes) = 20 THEN
      RETURN '0x' || encode(raw_bytes, 'hex');
    ELSE
      RETURN NULL;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
  END;
END;
$$;

-- =============================================================================
-- HELPER FUNCTION: Convert valoper address to delegator address
-- Changes prefix from xxxvaloper1... to xxx1...
-- =============================================================================

CREATE OR REPLACE FUNCTION valoper_to_delegator(valoper_addr TEXT)
RETURNS TEXT
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  prefix_end INT;
  prefix TEXT;
BEGIN
  IF valoper_addr IS NULL OR valoper_addr = '' THEN
    RETURN NULL;
  END IF;

  -- Find 'valoper1' and extract prefix before it
  prefix_end := position('valoper1' in valoper_addr);
  IF prefix_end = 0 THEN
    RETURN NULL;
  END IF;

  prefix := substring(valoper_addr from 1 for prefix_end - 1);
  -- Return prefix + '1' + rest after 'valoper1'
  RETURN prefix || substring(valoper_addr from prefix_end + 7);
END;
$$;

-- =============================================================================
-- UPDATE: extract_addresses to also capture EVM hex addresses
-- =============================================================================

CREATE OR REPLACE FUNCTION extract_addresses(msg JSONB)
RETURNS TEXT[]
LANGUAGE SQL STABLE
AS $$
WITH
  -- Extract Bech32 addresses (Cosmos standard)
  bech32_addresses AS (
    SELECT unnest(
      regexp_matches(
        msg::text,
        E'(?<=[\\"\'\\\\s]|^)([a-z0-9]{2,83}1[qpzry9x8gf2tvdw0s3jn54khce6mua7l]{38,})(?=[\\"\'\\\\s]|$)',
        'g'
      )
    ) AS addr
  ),
  -- Extract EVM hex addresses (0x followed by 40 hex chars)
  evm_addresses AS (
    SELECT unnest(
      regexp_matches(
        msg::text,
        E'(0x[a-fA-F0-9]{40})(?=[\\"\'\\\\s,}\\]]|$)',
        'gi'
      )
    ) AS addr
  ),
  all_addresses AS (
    SELECT addr FROM bech32_addresses
    UNION
    SELECT addr FROM evm_addresses
  )
SELECT array_agg(DISTINCT addr)
FROM all_addresses
WHERE addr IS NOT NULL;
$$;

-- =============================================================================
-- UPDATE: update_message_main with improved sender extraction
-- =============================================================================

CREATE OR REPLACE FUNCTION update_message_main()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
  sender TEXT;
  mentions TEXT[];
  metadata JSONB;
  decoded_bytes BYTEA;
  decoded_text TEXT;
  decoded_json JSONB;
  new_addresses TEXT[];
  evm_from_hex TEXT;
BEGIN
  -- Try to extract EVM from address first (for MsgEthereumTx)
  IF NEW.data->>'@type' = '/cosmos.evm.vm.v1.MsgEthereumTx' THEN
    evm_from_hex := base64_to_hex_address(NEW.data->>'from');
  END IF;

  sender := COALESCE(
    -- EVM from address (converted to hex)
    evm_from_hex,
    -- Standard Cosmos sender fields
    NULLIF(NEW.data->>'sender', ''),
    NULLIF(NEW.data->>'fromAddress', ''),
    -- Delegator address for staking messages
    NULLIF(NEW.data->>'delegatorAddress', ''),
    -- Derive from validator address if no delegator (MsgCreateValidator)
    valoper_to_delegator(NEW.data->>'validatorAddress'),
    -- Other common sender fields
    NULLIF(NEW.data->>'admin', ''),
    NULLIF(NEW.data->>'voter', ''),
    NULLIF(NEW.data->>'depositor', ''),
    NULLIF(NEW.data->>'address', ''),
    NULLIF(NEW.data->>'executor', ''),
    NULLIF(NEW.data->>'authority', ''),
    NULLIF(NEW.data->>'granter', ''),
    NULLIF(NEW.data->>'grantee', ''),
    NULLIF(NEW.data->>'signer', ''),
    -- Group proposal proposers
    (
      SELECT jsonb_array_elements_text(NEW.data->'proposers')
      LIMIT 1
    ),
    -- Multi-send inputs
    (
      CASE
        WHEN jsonb_typeof(NEW.data->'inputs') = 'array'
             AND jsonb_array_length(NEW.data->'inputs') > 0
        THEN NEW.data->'inputs'->0->>'address'
        ELSE NULL
      END
    )
  );

  mentions := extract_addresses(NEW.data);
  metadata := extract_metadata(NEW.data);

  -- Extract decoded data from IBC packet
  IF NEW.data->>'@type' = '/ibc.core.channel.v1.MsgRecvPacket' THEN
    IF metadata->'packet' ? 'data' THEN
      BEGIN
        decoded_bytes := decode(metadata->'packet'->>'data', 'base64');
        decoded_text := convert_from(decoded_bytes, 'UTF8');
        decoded_json := decoded_text::jsonb;
        metadata := metadata || jsonb_build_object('decodedData', decoded_json);
        IF decoded_json ? 'sender' THEN
          sender := decoded_json->>'sender';
        END IF;
        new_addresses := extract_addresses(decoded_json);
        SELECT array_agg(DISTINCT addr) INTO mentions
        FROM unnest(mentions || new_addresses) AS addr;
      EXCEPTION WHEN OTHERS THEN
        UPDATE api.transactions_main
        SET error = 'Error decoding base64 packet data'
        WHERE id = NEW.id;
      END;
    END IF;
  END IF;

  INSERT INTO api.messages_main (id, message_index, type, sender, mentions, metadata)
  VALUES (
           NEW.id,
           NEW.message_index,
           NEW.data->>'@type',
           sender,
           mentions,
           metadata
         )
  ON CONFLICT (id, message_index) DO UPDATE
  SET type = EXCLUDED.type,
      sender = EXCLUDED.sender,
      mentions = EXCLUDED.mentions,
      metadata = EXCLUDED.metadata;

  RETURN NEW;
END;
$$;

-- =============================================================================
-- UPDATE: chain_stats view to include EVM addresses and mentions
-- =============================================================================

DROP VIEW IF EXISTS api.chain_stats CASCADE;

CREATE OR REPLACE VIEW api.chain_stats AS
SELECT
  (SELECT MAX(id) FROM api.blocks_raw) AS latest_block,
  (SELECT COUNT(*) FROM api.transactions_main) AS total_transactions,
  (
    SELECT COUNT(*) FROM (
      -- Message senders
      SELECT DISTINCT sender AS addr
      FROM api.messages_main
      WHERE sender IS NOT NULL
      UNION
      -- EVM from addresses
      SELECT DISTINCT "from" AS addr
      FROM api.evm_transactions
      UNION
      -- EVM to addresses
      SELECT DISTINCT "to" AS addr
      FROM api.evm_transactions
      WHERE "to" IS NOT NULL
      UNION
      -- Mentioned addresses (flatten array)
      SELECT DISTINCT unnest(mentions) AS addr
      FROM api.messages_main
      WHERE mentions IS NOT NULL
    ) all_addresses
  ) AS unique_addresses,
  (SELECT COUNT(*) FROM api.evm_transactions) AS evm_transactions,
  (SELECT COUNT(*) FROM api.validators WHERE status = 'BOND_STATUS_BONDED') AS active_validators;

-- =============================================================================
-- BACKFILL: Reprocess all existing messages to extract senders properly
-- =============================================================================

-- Temporarily disable trigger to avoid double-processing
ALTER TABLE api.messages_raw DISABLE TRIGGER new_message_update;

-- Update messages_main by re-running extraction logic on existing data
UPDATE api.messages_main m
SET
  sender = COALESCE(
    -- EVM from address (converted to hex)
    CASE
      WHEN r.data->>'@type' = '/cosmos.evm.vm.v1.MsgEthereumTx'
      THEN base64_to_hex_address(r.data->>'from')
      ELSE NULL
    END,
    -- Standard Cosmos sender fields
    NULLIF(r.data->>'sender', ''),
    NULLIF(r.data->>'fromAddress', ''),
    NULLIF(r.data->>'delegatorAddress', ''),
    valoper_to_delegator(r.data->>'validatorAddress'),
    NULLIF(r.data->>'admin', ''),
    NULLIF(r.data->>'voter', ''),
    NULLIF(r.data->>'depositor', ''),
    NULLIF(r.data->>'address', ''),
    NULLIF(r.data->>'executor', ''),
    NULLIF(r.data->>'authority', ''),
    NULLIF(r.data->>'granter', ''),
    NULLIF(r.data->>'grantee', ''),
    NULLIF(r.data->>'signer', ''),
    (
      SELECT jsonb_array_elements_text(r.data->'proposers')
      LIMIT 1
    ),
    (
      CASE
        WHEN jsonb_typeof(r.data->'inputs') = 'array'
             AND jsonb_array_length(r.data->'inputs') > 0
        THEN r.data->'inputs'->0->>'address'
        ELSE NULL
      END
    )
  ),
  mentions = extract_addresses(r.data)
FROM api.messages_raw r
WHERE m.id = r.id AND m.message_index = r.message_index;

-- Re-enable trigger
ALTER TABLE api.messages_raw ENABLE TRIGGER new_message_update;

COMMIT;
