-- =============================================================================
-- YACI Trigger Infrastructure
-- Adds trigger functions and triggers to parse _raw tables into _main tables
-- Based on Yaci's 001_schema.up.sql
-- =============================================================================

BEGIN;

-- =============================================================================
-- MISSING TABLES
-- =============================================================================

-- events_raw table (was missing from 001)
CREATE TABLE IF NOT EXISTS api.events_raw (
  id TEXT NOT NULL,
  event_index BIGINT NOT NULL,
  data JSONB NOT NULL,
  PRIMARY KEY (id, event_index),
  FOREIGN KEY (id) REFERENCES api.transactions_raw(id) ON DELETE CASCADE
);

-- =============================================================================
-- HELPER FUNCTIONS
-- =============================================================================

-- Extract Bech32-like addresses from JSONB
CREATE OR REPLACE FUNCTION extract_addresses(msg JSONB)
RETURNS TEXT[]
LANGUAGE SQL STABLE
AS $$
WITH addresses AS (
  SELECT unnest(
    regexp_matches(
      msg::text,
      E'(?<=[\\"\'\\\\s]|^)([a-z0-9]{2,83}1[qpzry9x8gf2tvdw0s3jn54khce6mua7l]{38,})(?=[\\"\'\\\\s]|$)',
      'g'
    )
  ) AS addr
)
SELECT array_agg(DISTINCT addr)
FROM addresses;
$$;

-- Filter metadata from message
CREATE OR REPLACE FUNCTION extract_metadata(msg JSONB)
RETURNS JSONB
LANGUAGE SQL STABLE
AS $$
  WITH keys_to_remove AS (
      SELECT ARRAY['@type', 'sender', 'executor', 'admin', 'voter', 'messages', 'proposalId', 'proposers', 'authority', 'fromAddress']::text[] AS keys
  )
  SELECT msg - (SELECT keys FROM keys_to_remove)
$$;

-- Extract proposal failure logs
CREATE OR REPLACE FUNCTION extract_proposal_failure_logs(json_data JSONB)
RETURNS TEXT
LANGUAGE sql
AS $$
WITH
  events AS (
    SELECT jsonb_array_elements(json_data->'txResponse'->'events') AS event
  ),
  typed_attributes AS (
    SELECT
      event->>'type' AS event_type,
      jsonb_array_elements(event->'attributes') AS attribute
    FROM events
  )
  SELECT
    TRIM(BOTH '"' FROM typed_attributes.attribute->>'value') AS logs
  FROM typed_attributes
  WHERE
    typed_attributes.event_type = 'cosmos.group.v1.EventExec'
    AND typed_attributes.attribute->>'key' = 'logs'
    AND EXISTS (
      SELECT 1
      FROM typed_attributes t2
      WHERE t2.event_type = typed_attributes.event_type
        AND t2.attribute->>'key' = 'result'
        AND t2.attribute->>'value' = '"PROPOSAL_EXECUTOR_RESULT_FAILURE"'
    )
  LIMIT 1;
$$;

-- Extract proposal IDs from events
CREATE OR REPLACE FUNCTION extract_proposal_ids(events JSONB)
RETURNS TEXT[]
LANGUAGE plpgsql
AS $$
DECLARE
  proposal_ids TEXT[];
BEGIN
   SELECT
     ARRAY_AGG(DISTINCT TRIM(BOTH '"' FROM attr->>'value'))
   INTO proposal_ids
   FROM jsonb_array_elements(events) AS ev(event)
   CROSS JOIN LATERAL jsonb_array_elements(ev.event->'attributes') AS attr
   WHERE attr->>'key' = 'proposal_id';

  RETURN proposal_ids;
END;
$$;

-- Extract msg_index from event
CREATE OR REPLACE FUNCTION api.extract_event_msg_index(ev jsonb)
RETURNS bigint
LANGUAGE sql
STABLE
AS $$
  SELECT NULLIF(a->>'value','')::bigint
  FROM jsonb_array_elements(ev->'attributes') a
  WHERE a->>'key' = 'msg_index'
  LIMIT 1
$$;

-- =============================================================================
-- TRIGGER FUNCTIONS
-- =============================================================================

-- Parse raw transaction into transactions_main
CREATE OR REPLACE FUNCTION update_transaction_main()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
  error_text TEXT;
  proposal_ids TEXT[];
BEGIN
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
$$;

-- Parse raw message into messages_main
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
BEGIN
  sender := COALESCE(
    NULLIF(NEW.data->>'sender', ''),
    NULLIF(NEW.data->>'fromAddress', ''),
    NULLIF(NEW.data->>'admin', ''),
    NULLIF(NEW.data->>'voter', ''),
    NULLIF(NEW.data->>'address', ''),
    NULLIF(NEW.data->>'executor', ''),
    NULLIF(NEW.data->>'authority', ''),
    NULLIF(New.data->>'granter', ''),
    (
      SELECT jsonb_array_elements_text(NEW.data->'proposers')
      LIMIT 1
    ),
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

-- Insert raw events from transaction
CREATE OR REPLACE FUNCTION api.update_events_raw()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
  ev jsonb;
  ev_ord int;
BEGIN
  DELETE FROM api.events_raw WHERE id = NEW.id;

  FOR ev, ev_ord IN
    SELECT e, (ord::int - 1)
    FROM jsonb_array_elements(NEW.data->'txResponse'->'events') WITH ORDINALITY AS t(e, ord)
  LOOP
    INSERT INTO api.events_raw (id, event_index, data)
    VALUES (NEW.id, ev_ord, ev);
  END LOOP;

  RETURN NEW;
END $$;

-- Parse raw event into events_main
CREATE OR REPLACE FUNCTION api.update_event_main()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
  a jsonb;
  a_ord int;
  msg_idx bigint;
  ev_type text;
BEGIN
  msg_idx := api.extract_event_msg_index(NEW.data);
  ev_type := NEW.data->>'type';

  DELETE FROM api.events_main
  WHERE id = NEW.id AND event_index = NEW.event_index;

  FOR a, a_ord IN
    SELECT attr, (ord::int - 1)
    FROM jsonb_array_elements(NEW.data->'attributes') WITH ORDINALITY AS t(attr, ord)
  LOOP
    INSERT INTO api.events_main (
      id, event_index, attr_index, event_type, attr_key, attr_value, msg_index
    ) VALUES (
      NEW.id,
      NEW.event_index,
      a_ord,
      ev_type,
      a->>'key',
      a->>'value',
      msg_idx
    );
  END LOOP;

  RETURN NEW;
END $$;

-- =============================================================================
-- TRIGGERS
-- =============================================================================

-- Drop existing triggers if they exist
DROP TRIGGER IF EXISTS new_transaction_update ON api.transactions_raw;
DROP TRIGGER IF EXISTS new_message_update ON api.messages_raw;
DROP TRIGGER IF EXISTS new_transaction_events_raw ON api.transactions_raw;
DROP TRIGGER IF EXISTS new_event_update ON api.events_raw;

-- Create triggers
CREATE TRIGGER new_transaction_update
AFTER INSERT OR UPDATE
ON api.transactions_raw
FOR EACH ROW
EXECUTE FUNCTION update_transaction_main();

CREATE TRIGGER new_message_update
AFTER INSERT OR UPDATE
ON api.messages_raw
FOR EACH ROW
EXECUTE FUNCTION update_message_main();

CREATE TRIGGER new_transaction_events_raw
AFTER INSERT OR UPDATE OF data
ON api.transactions_raw
FOR EACH ROW
EXECUTE FUNCTION api.update_events_raw();

CREATE TRIGGER new_event_update
AFTER INSERT OR UPDATE OF data
ON api.events_raw
FOR EACH ROW
EXECUTE FUNCTION api.update_event_main();

COMMIT;
