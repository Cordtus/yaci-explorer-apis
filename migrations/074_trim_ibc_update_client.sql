BEGIN;

-- Migration 074: Trim IBC MsgUpdateClient proof data from raw tables
--
-- MsgUpdateClient messages contain the full counterparty chain header,
-- validator set, and trusted validator set (~80KB per message, ~245KB per tx).
-- This data is only needed for IBC light client verification on-chain and
-- is never queried by the explorer after ingestion.
--
-- On Juno: ~12.7% of transactions are MsgUpdateClient, contributing
-- an estimated ~100+ GB of stored proof data.
--
-- This migration:
--   1. Updates extract_metadata to strip clientMessage from messages_main.metadata
--   2. Adds a function to strip clientMessage from transaction message arrays
--   3. Adds AFTER INSERT triggers on transactions_raw and messages_raw to trim
--      after downstream triggers have extracted what they need
--   4. Does NOT backfill historical data (use scripts/trim-ibc-update-client.ts)
--
-- Fields removed from MsgUpdateClient:
--   clientMessage.signedHeader     (~30KB) -- counterparty block header + sigs
--   clientMessage.validatorSet     (~24KB) -- counterparty validator set
--   clientMessage.trustedValidators (~24KB) -- trusted validator set
--
-- Fields preserved:
--   @type, clientId, signer         -- message identity
--   clientMessage.@type             -- header type identifier
--   clientMessage.trustedHeight     -- trusted height reference

-- ============================================================================
-- 1. Update extract_metadata to also strip clientMessage
--    This prevents the 80KB blob from being stored in messages_main.metadata
-- ============================================================================

CREATE OR REPLACE FUNCTION extract_metadata(msg JSONB)
RETURNS JSONB
LANGUAGE SQL STABLE
AS $$
  WITH keys_to_remove AS (
    SELECT ARRAY[
      '@type', 'sender', 'executor', 'admin', 'voter',
      'messages', 'proposalId', 'proposers', 'authority', 'fromAddress'
    ]::text[] AS keys
  ),
  base AS (
    SELECT msg - (SELECT keys FROM keys_to_remove) AS metadata
  )
  SELECT
    CASE
      -- For MsgUpdateClient: replace clientMessage with a trimmed stub
      WHEN msg->>'@type' = '/ibc.core.client.v1.MsgUpdateClient'
           AND (SELECT metadata FROM base) ? 'clientMessage'
      THEN (SELECT metadata FROM base) || jsonb_build_object(
        'clientMessage', jsonb_build_object(
          '@type', msg->'clientMessage'->>'@type',
          'trustedHeight', msg->'clientMessage'->'trustedHeight'
        )
      )
      ELSE (SELECT metadata FROM base)
    END
$$;

-- ============================================================================
-- 2. Function: strip IBC proof data from a transaction's message array
--    Replaces clientMessage contents with a minimal stub in-place.
-- ============================================================================

CREATE OR REPLACE FUNCTION api.trim_ibc_update_client_tx(data jsonb)
RETURNS jsonb
LANGUAGE sql IMMUTABLE STRICT
AS $$
  WITH messages AS (
    SELECT ordinality, msg,
      CASE
        WHEN msg->>'@type' = '/ibc.core.client.v1.MsgUpdateClient'
             AND msg ? 'clientMessage'
        THEN jsonb_build_object(
          '@type', msg->>'@type',
          'clientId', msg->'clientId',
          'signer', msg->'signer',
          'clientMessage', jsonb_build_object(
            '@type', msg->'clientMessage'->>'@type',
            'trustedHeight', msg->'clientMessage'->'trustedHeight'
          )
        )
        ELSE msg
      END AS trimmed_msg
    FROM jsonb_array_elements(data->'tx'->'body'->'messages')
         WITH ORDINALITY AS t(msg, ordinality)
  )
  SELECT jsonb_set(
    data,
    '{tx,body,messages}',
    COALESCE(
      (SELECT jsonb_agg(trimmed_msg ORDER BY ordinality) FROM messages),
      '[]'::jsonb
    )
  );
$$;

-- ============================================================================
-- 3. Function: strip clientMessage from a single messages_raw row
-- ============================================================================

CREATE OR REPLACE FUNCTION api.trim_ibc_update_client_msg(data jsonb)
RETURNS jsonb
LANGUAGE sql IMMUTABLE STRICT
AS $$
  SELECT
    CASE
      WHEN data->>'@type' = '/ibc.core.client.v1.MsgUpdateClient'
           AND data ? 'clientMessage'
      THEN jsonb_build_object(
        '@type', data->>'@type',
        'clientId', data->'clientId',
        'signer', data->'signer',
        'clientMessage', jsonb_build_object(
          '@type', data->'clientMessage'->>'@type',
          'trustedHeight', data->'clientMessage'->'trustedHeight'
        )
      )
      ELSE data
    END;
$$;

-- ============================================================================
-- 4. AFTER INSERT trigger on transactions_raw: trim MsgUpdateClient
--    Named 'zzz_*' to fire after new_transaction_update and
--    new_transaction_events_raw which extract messages and events.
--
--    Only fires the UPDATE when the tx actually contains MsgUpdateClient,
--    so the overhead for normal transactions is a single text check.
-- ============================================================================

CREATE OR REPLACE FUNCTION api.trg_trim_ibc_tx_after_insert()
RETURNS TRIGGER AS $$
BEGIN
  -- Quick check: only process if tx contains MsgUpdateClient
  IF NEW.data::text LIKE '%MsgUpdateClient%' THEN
    UPDATE api.transactions_raw
    SET data = api.trim_ibc_update_client_tx(data)
    WHERE id = NEW.id;
  END IF;

  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS zzz_trim_ibc_update_client ON api.transactions_raw;
CREATE TRIGGER zzz_trim_ibc_update_client
  AFTER INSERT ON api.transactions_raw
  FOR EACH ROW
  EXECUTE FUNCTION api.trg_trim_ibc_tx_after_insert();

-- ============================================================================
-- 5. AFTER INSERT trigger on messages_raw: trim MsgUpdateClient
--    Named 'zzz_*' to fire after new_message_update which extracts
--    sender/type/metadata into messages_main.
-- ============================================================================

CREATE OR REPLACE FUNCTION api.trg_trim_ibc_msg_after_insert()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.data->>'@type' = '/ibc.core.client.v1.MsgUpdateClient'
     AND NEW.data ? 'clientMessage' THEN
    UPDATE api.messages_raw
    SET data = api.trim_ibc_update_client_msg(data)
    WHERE id = NEW.id AND message_index = NEW.message_index;
  END IF;

  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS zzz_trim_ibc_update_client ON api.messages_raw;
CREATE TRIGGER zzz_trim_ibc_update_client
  AFTER INSERT ON api.messages_raw
  FOR EACH ROW
  EXECUTE FUNCTION api.trg_trim_ibc_msg_after_insert();

COMMIT;
