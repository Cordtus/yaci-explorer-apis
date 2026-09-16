-- Fix function overloading conflict for get_transactions_paginated
-- Migration 014 added new parameters but didn't drop the old 5-parameter version
-- PostgREST cannot resolve which function to use when both exist

BEGIN;

-- Drop the old 5-parameter version that conflicts with the new 9-parameter version
DROP FUNCTION IF EXISTS api.get_transactions_paginated(int, int, text, bigint, text);

-- Ensure the new version exists with all parameters
CREATE OR REPLACE FUNCTION api.get_transactions_paginated(
  _limit int DEFAULT 20,
  _offset int DEFAULT 0,
  _status text DEFAULT NULL,
  _block_height bigint DEFAULT NULL,
  _block_height_min bigint DEFAULT NULL,
  _block_height_max bigint DEFAULT NULL,
  _message_type text DEFAULT NULL,
  _timestamp_min timestamptz DEFAULT NULL,
  _timestamp_max timestamptz DEFAULT NULL
)
RETURNS jsonb
LANGUAGE sql STABLE
AS $$
  WITH filtered_txs AS (
    SELECT DISTINCT t.id
    FROM api.transactions_main t
    LEFT JOIN api.messages_main m ON t.id = m.id
    WHERE (_status IS NULL OR
           (_status = 'success' AND t.error IS NULL) OR
           (_status = 'failed' AND t.error IS NOT NULL))
      AND (_block_height IS NULL OR t.height = _block_height)
      AND (_block_height_min IS NULL OR t.height >= _block_height_min)
      AND (_block_height_max IS NULL OR t.height <= _block_height_max)
      AND (_message_type IS NULL OR m.type = _message_type)
      AND (_timestamp_min IS NULL OR t.timestamp >= _timestamp_min)
      AND (_timestamp_max IS NULL OR t.timestamp <= _timestamp_max)
  ),
  paginated AS (
    SELECT t.*
    FROM api.transactions_main t
    JOIN filtered_txs f ON t.id = f.id
    ORDER BY t.height DESC, t.id
    LIMIT _limit OFFSET _offset
  ),
  total AS (
    SELECT COUNT(*) AS count FROM filtered_txs
  ),
  tx_messages AS (
    SELECT
      m.id,
      jsonb_agg(
        jsonb_build_object(
          'id', m.id,
          'message_index', m.message_index,
          'type', m.type,
          'sender', m.sender,
          'mentions', m.mentions,
          'metadata', m.metadata
        ) ORDER BY m.message_index
      ) AS messages
    FROM api.messages_main m
    WHERE m.id IN (SELECT id FROM paginated)
    GROUP BY m.id
  ),
  tx_events AS (
    SELECT
      e.id,
      jsonb_agg(
        jsonb_build_object(
          'id', e.id,
          'event_index', e.event_index,
          'attr_index', e.attr_index,
          'event_type', e.event_type,
          'attr_key', e.attr_key,
          'attr_value', e.attr_value,
          'msg_index', e.msg_index
        ) ORDER BY e.event_index, e.attr_index
      ) AS events
    FROM api.events_main e
    WHERE e.id IN (SELECT id FROM paginated)
    GROUP BY e.id
  )
  SELECT jsonb_build_object(
    'data', COALESCE(jsonb_agg(
      jsonb_build_object(
        'id', p.id,
        'height', p.height,
        'timestamp', p.timestamp,
        'fee', p.fee,
        'memo', p.memo,
        'error', p.error,
        'proposal_ids', p.proposal_ids,
        'messages', COALESCE(m.messages, '[]'::jsonb),
        'events', COALESCE(e.events, '[]'::jsonb),
        'ingest_error', NULL
      ) ORDER BY p.height DESC
    ), '[]'::jsonb),
    'pagination', jsonb_build_object(
      'total', (SELECT count FROM total),
      'limit', _limit,
      'offset', _offset,
      'has_next', _offset + _limit < (SELECT count FROM total),
      'has_prev', _offset > 0
    )
  )
  FROM paginated p
  LEFT JOIN tx_messages m ON p.id = m.id
  LEFT JOIN tx_events e ON p.id = e.id;
$$;

GRANT EXECUTE ON FUNCTION api.get_transactions_paginated(int, int, text, bigint, bigint, bigint, text, timestamptz, timestamptz) TO web_anon;

COMMIT;
