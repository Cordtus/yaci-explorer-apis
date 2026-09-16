-- Migration 049: Handle error-metadata transactions gracefully
-- When the indexer fails to fetch a transaction via RPC (e.g., tx pruned),
-- it stores minimal error metadata: {"error": "...", "hash": "...", "reason": "..."}
-- The triggers must skip these records since they lack txResponse/tx fields.

BEGIN;

-- ============================================================================
-- 1. Fix update_transaction_main: skip error-metadata transactions
--    Note: trigger references public schema, so we update both
-- ============================================================================

CREATE OR REPLACE FUNCTION public.update_transaction_main()
RETURNS TRIGGER AS $$
DECLARE
  error_text TEXT;
  proposal_ids TEXT[];
BEGIN
  -- Skip error-metadata transactions (no txResponse means fetch failed)
  IF NEW.data->'txResponse' IS NULL THEN
    RETURN NEW;
  END IF;

  error_text := NEW.data->'txResponse'->>'rawLog';

  IF error_text IS NULL THEN
    error_text := extract_proposal_failure_logs(NEW.data);
  END IF;

  proposal_ids := extract_proposal_ids(NEW.data->'txResponse'->'events');

  INSERT INTO api.transactions_main (id, fee, memo, error, height, timestamp, proposal_ids)
  VALUES (
            NEW.id,
            NEW.data->'tx'->'authInfo'->'fee',
            NEW.data->'tx'->'body'->>'memo',
            error_text,
            (NEW.data->'txResponse'->>'height')::BIGINT,
            (NEW.data->'txResponse'->>'timestamp')::TIMESTAMPTZ,
            proposal_ids
         )
  ON CONFLICT (id) DO UPDATE
  SET fee = EXCLUDED.fee,
      memo = EXCLUDED.memo,
      error = EXCLUDED.error,
      height = EXCLUDED.height,
      timestamp = EXCLUDED.timestamp,
      proposal_ids = EXCLUDED.proposal_ids;

  -- Insert top level messages
  INSERT INTO api.messages_raw (id, message_index, data)
  SELECT
    NEW.id,
    message_index - 1,
    message
  FROM jsonb_array_elements(NEW.data->'tx'->'body'->'messages') WITH ORDINALITY AS message(message, message_index)
  ON CONFLICT (id, message_index) DO UPDATE
  SET data = EXCLUDED.data;

  -- Insert nested messages (e.g., within proposals)
  INSERT INTO api.messages_raw (id, message_index, data)
  SELECT
    NEW.id,
    10000 + ((top_level.msg_index - 1) * 1000) + sub_level.sub_index,
    sub_level.sub_msg
  FROM jsonb_array_elements(NEW.data->'tx'->'body'->'messages')
       WITH ORDINALITY AS top_level(msg, msg_index)
       CROSS JOIN LATERAL (
         SELECT sub_msg, sub_index
         FROM jsonb_array_elements(top_level.msg->'messages')
              WITH ORDINALITY AS inner_msg(sub_msg, sub_index)
       ) AS sub_level
  WHERE top_level.msg->>'@type' = '/cosmos.group.v1.MsgSubmitProposal'
    AND top_level.msg->'messages' IS NOT NULL
  ON CONFLICT (id, message_index) DO UPDATE
  SET data = EXCLUDED.data;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 2. Fix update_events_raw: skip error-metadata transactions
-- ============================================================================

CREATE OR REPLACE FUNCTION api.update_events_raw()
RETURNS TRIGGER AS $$
DECLARE
  ev jsonb;
  ev_ord int;
BEGIN
  -- Skip error-metadata transactions (no txResponse means fetch failed)
  IF NEW.data->'txResponse' IS NULL THEN
    RETURN NEW;
  END IF;

  DELETE FROM api.events_raw WHERE id = NEW.id;

  FOR ev, ev_ord IN
    SELECT e, (ord::int - 1)
    FROM jsonb_array_elements(NEW.data->'txResponse'->'events') WITH ORDINALITY AS t(e, ord)
  LOOP
    INSERT INTO api.events_raw (id, event_index, data)
    VALUES (NEW.id, ev_ord, ev);
  END LOOP;

  RETURN NEW;
END
$$ LANGUAGE plpgsql;

COMMIT;
