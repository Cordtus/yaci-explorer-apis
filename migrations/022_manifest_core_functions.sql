-- =============================================================================
-- Manifest Indexer Core RPC Functions
-- Migration 022: Add required RPC functions for yaci-explorer frontend
-- Safe to run on existing database with populated data
-- =============================================================================

BEGIN;

-- =============================================================================
-- UNIVERSAL SEARCH
-- =============================================================================

CREATE OR REPLACE FUNCTION api.universal_search(_query text)
RETURNS jsonb
LANGUAGE plpgsql STABLE
AS $$
DECLARE
  results jsonb := '[]'::jsonb;
  trimmed text := trim(_query);
  block_result jsonb;
  tx_result jsonb;
BEGIN
  -- Check for block height (numeric)
  IF trimmed ~ '^\d+$' THEN
    SELECT jsonb_build_object(
      'type', 'block',
      'value', jsonb_build_object('id', id),
      'score', 100
    ) INTO block_result
    FROM api.blocks_raw
    WHERE id = trimmed::bigint;

    IF block_result IS NOT NULL THEN
      results := results || block_result;
    END IF;
  END IF;

  -- Check for Cosmos tx hash (64 hex, no 0x)
  IF trimmed ~ '^[a-fA-F0-9]{64}$' THEN
    SELECT jsonb_build_object(
      'type', 'transaction',
      'value', jsonb_build_object('id', id),
      'score', 100
    ) INTO tx_result
    FROM api.transactions_main
    WHERE id = upper(trimmed) OR id = lower(trimmed);

    IF tx_result IS NOT NULL THEN
      results := results || tx_result;
    END IF;
  END IF;

  -- Check for Cosmos address (bech32)
  IF trimmed ~ '^[a-z]+1[a-z0-9]{38,}$' THEN
    results := results || jsonb_build_object(
      'type', 'address',
      'value', jsonb_build_object('address', trimmed),
      'score', 90
    );
  END IF;

  RETURN results;
END;
$$;

-- =============================================================================
-- TRANSACTION DETAIL
-- =============================================================================

CREATE OR REPLACE FUNCTION api.get_transaction_detail(_hash text)
RETURNS jsonb
LANGUAGE plpgsql STABLE
AS $$
DECLARE
  result jsonb;
  normalized_hash text;
BEGIN
  -- Normalize hash (try both cases)
  SELECT id INTO normalized_hash
  FROM api.transactions_main
  WHERE id = upper(_hash) OR id = lower(_hash)
  LIMIT 1;

  IF normalized_hash IS NULL THEN
    RETURN NULL;
  END IF;

  SELECT jsonb_build_object(
    'id', t.id,
    'fee', t.fee,
    'memo', t.memo,
    'error', t.error,
    'height', t.height,
    'timestamp', t.timestamp,
    'proposal_ids', t.proposal_ids,
    'messages', COALESCE(msg.messages, '[]'::jsonb),
    'events', COALESCE(evt.events, '[]'::jsonb),
    'raw_data', r.data
  ) INTO result
  FROM api.transactions_main t
  LEFT JOIN api.transactions_raw r ON t.id = r.id
  LEFT JOIN LATERAL (
    SELECT jsonb_agg(
      jsonb_build_object(
        'id', m.id,
        'message_index', m.message_index,
        'type', m.type,
        'sender', m.sender,
        'mentions', m.mentions,
        'metadata', m.metadata,
        'data', mr.data
      ) ORDER BY m.message_index
    ) AS messages
    FROM api.messages_main m
    LEFT JOIN api.messages_raw mr ON m.id = mr.id AND m.message_index = mr.message_index
    WHERE m.id = normalized_hash
  ) msg ON TRUE
  LEFT JOIN LATERAL (
    SELECT jsonb_agg(
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
    WHERE e.id = normalized_hash
  ) evt ON TRUE
  WHERE t.id = normalized_hash;

  RETURN result;
END;
$$;

-- =============================================================================
-- PAGINATED TRANSACTIONS
-- =============================================================================

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
        'events', COALESCE(e.events, '[]'::jsonb)
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

-- =============================================================================
-- TRANSACTIONS BY ADDRESS
-- =============================================================================

CREATE OR REPLACE FUNCTION api.get_transactions_by_address(
  _address text,
  _limit int DEFAULT 50,
  _offset int DEFAULT 0
)
RETURNS jsonb
LANGUAGE sql STABLE
AS $$
  WITH addr_txs AS (
    SELECT DISTINCT m.id
    FROM api.messages_main m
    WHERE m.sender = _address OR _address = ANY(m.mentions)
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
        'events', COALESCE(e.events, '[]'::jsonb)
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

-- =============================================================================
-- ADDRESS STATS
-- =============================================================================

CREATE OR REPLACE FUNCTION api.get_address_stats(_address text)
RETURNS jsonb
LANGUAGE sql STABLE
AS $$
  WITH tx_ids AS (
    SELECT DISTINCT m.id
    FROM api.messages_main m
    WHERE m.sender = _address OR _address = ANY(m.mentions)
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

-- =============================================================================
-- PAGINATED BLOCKS
-- =============================================================================

CREATE OR REPLACE FUNCTION api.get_blocks_paginated(
  _limit int DEFAULT 20,
  _offset int DEFAULT 0,
  _min_tx_count int DEFAULT NULL,
  _from_date timestamp DEFAULT NULL,
  _to_date timestamp DEFAULT NULL
)
RETURNS jsonb
LANGUAGE sql STABLE
AS $$
  WITH filtered_blocks AS (
    SELECT b.id, b.data, b.tx_count
    FROM api.blocks_raw b
    WHERE
      (_min_tx_count IS NULL OR b.tx_count >= _min_tx_count)
      AND (_from_date IS NULL OR (b.data->'block'->'header'->>'time')::timestamp >= _from_date)
      AND (_to_date IS NULL OR (b.data->'block'->'header'->>'time')::timestamp <= _to_date)
    ORDER BY b.id DESC
  ),
  total AS (
    SELECT COUNT(*) AS count FROM filtered_blocks
  ),
  paginated AS (
    SELECT * FROM filtered_blocks
    LIMIT _limit OFFSET _offset
  )
  SELECT jsonb_build_object(
    'data', COALESCE(jsonb_agg(
      jsonb_build_object(
        'id', p.id,
        'data', p.data,
        'tx_count', COALESCE(p.tx_count, 0)
      ) ORDER BY p.id DESC
    ), '[]'::jsonb),
    'pagination', jsonb_build_object(
      'total', (SELECT count FROM total),
      'limit', _limit,
      'offset', _offset,
      'has_next', _offset + _limit < (SELECT count FROM total),
      'has_prev', _offset > 0
    )
  )
  FROM paginated p;
$$;

-- =============================================================================
-- BLOCK TIME ANALYSIS
-- =============================================================================

CREATE OR REPLACE FUNCTION api.get_block_time_analysis(_limit int DEFAULT 100)
RETURNS jsonb
LANGUAGE sql STABLE
AS $$
  WITH block_times AS (
    SELECT
      id,
      (data->'block'->'header'->>'time')::timestamp AS block_time,
      LAG((data->'block'->'header'->>'time')::timestamp) OVER (ORDER BY id) AS prev_time
    FROM api.blocks_raw
    ORDER BY id DESC
    LIMIT _limit
  ),
  diffs AS (
    SELECT EXTRACT(EPOCH FROM (block_time - prev_time)) AS diff_seconds
    FROM block_times
    WHERE prev_time IS NOT NULL
  )
  SELECT jsonb_build_object(
    'avg', ROUND(AVG(diff_seconds)::numeric, 2),
    'min', ROUND(MIN(diff_seconds)::numeric, 2),
    'max', ROUND(MAX(diff_seconds)::numeric, 2)
  )
  FROM diffs;
$$;

-- =============================================================================
-- GRANT PERMISSIONS
-- =============================================================================

GRANT EXECUTE ON FUNCTION api.universal_search(text) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_transaction_detail(text) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_transactions_paginated(int, int, text, bigint, bigint, bigint, text, timestamptz, timestamptz) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_transactions_by_address(text, int, int) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_address_stats(text) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_blocks_paginated(int, int, int, timestamp, timestamp) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_block_time_analysis(int) TO web_anon;

COMMIT;
