-- Support looking up transactions by both cosmos (bech32) and EVM (hex) address forms
-- When a wallet has both address formats, queries should match on either one

BEGIN;

-- Update get_transactions_by_address to accept an optional alternate address
CREATE OR REPLACE FUNCTION api.get_transactions_by_address(
  _address text,
  _limit int DEFAULT 50,
  _offset int DEFAULT 0,
  _alt_address text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE sql STABLE
AS $$
  WITH addr_txs AS (
    SELECT DISTINCT m.id
    FROM api.messages_main m
    WHERE m.sender = _address
       OR _address = ANY(m.mentions)
       OR (_alt_address IS NOT NULL AND (
            m.sender = _alt_address
            OR _alt_address = ANY(m.mentions)
          ))
  ),
  paginated AS (
    SELECT t.*
    FROM api.transactions_main t
    JOIN addr_txs a ON t.id = a.id
    ORDER BY t.height DESC
    LIMIT _limit OFFSET _offset
  ),
  total AS (
    SELECT COUNT(*) AS count FROM addr_txs
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

-- Update get_address_stats to accept an optional alternate address
CREATE OR REPLACE FUNCTION api.get_address_stats(
  _address text,
  _alt_address text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE sql STABLE
AS $$
  WITH tx_ids AS (
    SELECT DISTINCT m.id
    FROM api.messages_main m
    WHERE m.sender = _address
       OR _address = ANY(m.mentions)
       OR (_alt_address IS NOT NULL AND (
            m.sender = _alt_address
            OR _alt_address = ANY(m.mentions)
          ))
  ),
  aggregated AS (
    SELECT
      COUNT(DISTINCT t.id) AS transaction_count,
      MIN(t.timestamp) AS first_seen,
      MAX(t.timestamp) AS last_seen
    FROM api.transactions_main t
    JOIN tx_ids ON t.id = tx_ids.id
  )
  SELECT jsonb_build_object(
    'address', _address,
    'transaction_count', transaction_count,
    'first_seen', first_seen,
    'last_seen', last_seen
  )
  FROM aggregated;
$$;

COMMIT;
