-- Migration: 001_core_functions.sql
-- Description: Core SQL functions for optimized data access
-- Phase: 1 - Core Functions
--
-- This migration creates the primary RPC functions that eliminate N+1 query patterns
-- and provide single-round-trip data access for all primary use cases.

-- =============================================================================
-- Function: api.get_transactions_by_address
-- Purpose: Replace N+1 pattern for address transaction queries
-- Returns paginated transactions with messages and events in a single round-trip
-- =============================================================================
CREATE OR REPLACE FUNCTION api.get_transactions_by_address(
  _address text,
  _limit int DEFAULT 50,
  _offset int DEFAULT 0
) RETURNS jsonb
LANGUAGE sql STABLE
AS $$
  WITH
  -- Find all unique tx IDs for this address (using indexes)
  addr_tx_ids AS (
    SELECT DISTINCT m.id
    FROM api.messages_main m
    WHERE m.sender = _address
       OR _address = ANY(m.mentions)
  ),
  -- Get total count for pagination metadata
  total_count AS (
    SELECT COUNT(*)::int AS cnt FROM addr_tx_ids
  ),
  -- Paginate at database level
  paginated_txs AS (
    SELECT t.*
    FROM api.transactions_main t
    JOIN addr_tx_ids a ON t.id = a.id
    ORDER BY t.height DESC
    LIMIT _limit OFFSET _offset
  ),
  -- Aggregate messages for paginated txs
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
    WHERE m.id IN (SELECT id FROM paginated_txs)
    GROUP BY m.id
  ),
  -- Aggregate events for paginated txs
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
    WHERE e.id IN (SELECT id FROM paginated_txs)
    GROUP BY e.id
  )
  -- Assemble final response
  SELECT jsonb_build_object(
    'data', COALESCE((
      SELECT jsonb_agg(
        jsonb_build_object(
          'id', t.id,
          'fee', t.fee,
          'memo', t.memo,
          'error', t.error,
          'height', t.height,
          'timestamp', t.timestamp,
          'proposal_ids', t.proposal_ids,
          'messages', COALESCE(m.messages, '[]'::jsonb),
          'events', COALESCE(e.events, '[]'::jsonb),
          'ingest_error', NULL
        ) ORDER BY t.height DESC
      )
      FROM paginated_txs t
      LEFT JOIN tx_messages m ON t.id = m.id
      LEFT JOIN tx_events e ON t.id = e.id
    ), '[]'::jsonb),
    'pagination', jsonb_build_object(
      'total', (SELECT cnt FROM total_count),
      'limit', _limit,
      'offset', _offset,
      'has_next', _offset + _limit < (SELECT cnt FROM total_count),
      'has_prev', _offset > 0
    )
  );
$$;

GRANT EXECUTE ON FUNCTION api.get_transactions_by_address(text, int, int) TO web_anon;

-- =============================================================================
-- Function: api.get_address_stats
-- Purpose: Get aggregated statistics for an address
-- Returns transaction count, first/last seen, sent/received counts
-- =============================================================================
CREATE OR REPLACE FUNCTION api.get_address_stats(_address text)
RETURNS jsonb
LANGUAGE sql STABLE
AS $$
  WITH addr_activity AS (
    SELECT
      m.id,
      m.sender,
      t.timestamp
    FROM api.messages_main m
    JOIN api.transactions_main t ON m.id = t.id
    WHERE m.sender = _address
       OR _address = ANY(m.mentions)
  ),
  aggregated AS (
    SELECT
      COUNT(DISTINCT id) AS transaction_count,
      MIN(timestamp) AS first_seen,
      MAX(timestamp) AS last_seen,
      COUNT(*) FILTER (WHERE sender = _address) AS total_sent,
      COUNT(*) FILTER (WHERE sender IS DISTINCT FROM _address) AS total_received
    FROM addr_activity
  )
  SELECT jsonb_build_object(
    'address', _address,
    'transaction_count', transaction_count,
    'first_seen', first_seen,
    'last_seen', last_seen,
    'total_sent', total_sent,
    'total_received', total_received
  )
  FROM aggregated;
$$;

GRANT EXECUTE ON FUNCTION api.get_address_stats(text) TO web_anon;

-- =============================================================================
-- Function: api.get_transaction_detail
-- Purpose: Get full transaction detail including messages, events, and EVM data
-- Returns complete transaction data in a single round-trip
-- =============================================================================
CREATE OR REPLACE FUNCTION api.get_transaction_detail(_hash text)
RETURNS jsonb
LANGUAGE sql STABLE
AS $$
  WITH
  tx_main AS (
    SELECT * FROM api.transactions_main WHERE id = _hash
  ),
  tx_raw AS (
    SELECT * FROM api.transactions_raw WHERE id = _hash
  ),
  tx_messages AS (
    SELECT jsonb_agg(
      jsonb_build_object(
        'id', m.id,
        'message_index', m.message_index,
        'type', m.type,
        'sender', m.sender,
        'mentions', m.mentions,
        'metadata', m.metadata,
        'data', r.data
      ) ORDER BY m.message_index
    ) AS messages
    FROM api.messages_main m
    LEFT JOIN api.messages_raw r ON m.id = r.id AND m.message_index = r.message_index
    WHERE m.id = _hash
  ),
  tx_events AS (
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
    WHERE e.id = _hash
  ),
  evm_data AS (
    SELECT jsonb_build_object(
      'ethereum_tx_hash', MAX(CASE WHEN e.attr_key = 'ethereumTxHash' THEN e.attr_value END),
      'recipient', MAX(CASE WHEN e.attr_key = 'recipient' THEN e.attr_value END),
      'gas_used', MAX(CASE WHEN e.attr_key = 'txGasUsed' THEN NULLIF(e.attr_value, '')::bigint END),
      'tx_type', MAX(CASE WHEN e.attr_key = 'txType' THEN NULLIF(e.attr_value, '')::int END)
    ) AS evm
    FROM api.events_main e
    WHERE e.id = _hash AND e.event_type = 'ethereum_tx'
  )
  SELECT jsonb_build_object(
    'id', t.id,
    'fee', t.fee,
    'memo', t.memo,
    'error', t.error,
    'height', t.height,
    'timestamp', t.timestamp,
    'proposal_ids', t.proposal_ids,
    'messages', COALESCE(m.messages, '[]'::jsonb),
    'events', COALESCE(e.events, '[]'::jsonb),
    'evm_data', CASE WHEN (ev.evm->>'ethereum_tx_hash') IS NOT NULL THEN ev.evm ELSE NULL END,
    'raw_data', r.data,
    'ingest_error', CASE
      WHEN t.id IS NULL AND r.data ? 'error' THEN
        jsonb_build_object(
          'message', r.data->>'error',
          'reason', r.data->>'reason',
          'hash', _hash
        )
      ELSE NULL
    END
  )
  FROM tx_raw r
  LEFT JOIN tx_main t ON TRUE
  LEFT JOIN tx_messages m ON TRUE
  LEFT JOIN tx_events e ON TRUE
  LEFT JOIN evm_data ev ON TRUE;
$$;

GRANT EXECUTE ON FUNCTION api.get_transaction_detail(text) TO web_anon;

-- =============================================================================
-- Function: api.get_transactions_paginated
-- Purpose: Get filtered and paginated transaction list
-- Supports filtering by status, block height, and message type
-- =============================================================================
CREATE OR REPLACE FUNCTION api.get_transactions_paginated(
  _limit int DEFAULT 20,
  _offset int DEFAULT 0,
  _status text DEFAULT NULL,
  _block_height int DEFAULT NULL,
  _message_type text DEFAULT NULL
) RETURNS jsonb
LANGUAGE sql STABLE
AS $$
  WITH
  type_filtered_txs AS (
    SELECT DISTINCT m.id
    FROM api.messages_main m
    WHERE _message_type IS NULL OR m.type = _message_type
  ),
  filtered_txs AS (
    SELECT t.*
    FROM api.transactions_main t
    WHERE
      (_message_type IS NULL OR t.id IN (SELECT id FROM type_filtered_txs))
      AND (_status IS NULL
           OR (_status = 'success' AND t.error IS NULL)
           OR (_status = 'failed' AND t.error IS NOT NULL))
      AND (_block_height IS NULL OR t.height = _block_height)
    ORDER BY t.height DESC
    LIMIT _limit OFFSET _offset
  ),
  total_count AS (
    SELECT COUNT(*)::int AS cnt
    FROM api.transactions_main t
    WHERE
      (_message_type IS NULL OR t.id IN (SELECT id FROM type_filtered_txs))
      AND (_status IS NULL
           OR (_status = 'success' AND t.error IS NULL)
           OR (_status = 'failed' AND t.error IS NOT NULL))
      AND (_block_height IS NULL OR t.height = _block_height)
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
    WHERE m.id IN (SELECT id FROM filtered_txs)
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
    WHERE e.id IN (SELECT id FROM filtered_txs)
    GROUP BY e.id
  )
  SELECT jsonb_build_object(
    'data', COALESCE((
      SELECT jsonb_agg(
        jsonb_build_object(
          'id', t.id,
          'fee', t.fee,
          'memo', t.memo,
          'error', t.error,
          'height', t.height,
          'timestamp', t.timestamp,
          'proposal_ids', t.proposal_ids,
          'messages', COALESCE(m.messages, '[]'::jsonb),
          'events', COALESCE(e.events, '[]'::jsonb),
          'ingest_error', NULL
        ) ORDER BY t.height DESC
      )
      FROM filtered_txs t
      LEFT JOIN tx_messages m ON t.id = m.id
      LEFT JOIN tx_events e ON t.id = e.id
    ), '[]'::jsonb),
    'pagination', jsonb_build_object(
      'total', (SELECT cnt FROM total_count),
      'limit', _limit,
      'offset', _offset,
      'has_next', _offset + _limit < (SELECT cnt FROM total_count),
      'has_prev', _offset > 0
    )
  );
$$;

GRANT EXECUTE ON FUNCTION api.get_transactions_paginated(int, int, text, int, text) TO web_anon;

-- =============================================================================
-- Function: api.universal_search
-- Purpose: Cross-entity search supporting blocks, transactions, and addresses
-- Detects query type and returns appropriate results with scores
-- =============================================================================
CREATE OR REPLACE FUNCTION api.universal_search(_query text)
RETURNS jsonb
LANGUAGE plpgsql STABLE
AS $$
DECLARE
  results jsonb := '[]'::jsonb;
  trimmed text := trim(_query);
  block_height int;
  block_result jsonb;
  tx_result jsonb;
  evm_tx_result jsonb;
  address_result jsonb;
BEGIN
  -- Check for block height (numeric)
  IF trimmed ~ '^\d+$' THEN
    block_height := trimmed::int;
    SELECT jsonb_build_object(
      'type', 'block',
      'value', jsonb_build_object('id', id, 'data', data),
      'score', 100
    ) INTO block_result
    FROM api.blocks_raw
    WHERE id = block_height;

    IF block_result IS NOT NULL THEN
      results := results || block_result;
    END IF;
  END IF;

  -- Check for EVM tx hash (0x + 64 hex)
  IF trimmed ~ '^0x[a-fA-F0-9]{64}$' THEN
    SELECT jsonb_build_object(
      'type', 'evm_transaction',
      'value', jsonb_build_object(
        'tx_id', tx_id,
        'ethereum_tx_hash', ethereum_tx_hash,
        'height', height,
        'timestamp', timestamp
      ),
      'score', 100
    ) INTO evm_tx_result
    FROM api.evm_tx_map
    WHERE ethereum_tx_hash = trimmed;

    IF evm_tx_result IS NOT NULL THEN
      results := results || evm_tx_result;
    END IF;
  END IF;

  -- Check for Cosmos tx hash (64 hex, no 0x)
  IF trimmed ~ '^[a-fA-F0-9]{64}$' THEN
    SELECT jsonb_build_object(
      'type', 'transaction',
      'value', api.get_transaction_detail(trimmed),
      'score', 100
    ) INTO tx_result
    FROM api.transactions_main
    WHERE id = upper(trimmed);

    IF tx_result IS NOT NULL THEN
      results := results || tx_result;
    END IF;
  END IF;

  -- Check for Bech32 address
  IF trimmed ~ '^[a-z]+1[a-z0-9]{38,}$' THEN
    SELECT jsonb_build_object(
      'type', 'address',
      'value', api.get_address_stats(trimmed),
      'score', 90
    ) INTO address_result;

    IF address_result IS NOT NULL AND
       (address_result->'value'->>'transaction_count')::int > 0 THEN
      results := results || address_result;
    END IF;
  END IF;

  -- Check for EVM address (0x + 40 hex)
  IF trimmed ~ '^0x[a-fA-F0-9]{40}$' THEN
    SELECT jsonb_build_object(
      'type', 'evm_address',
      'value', jsonb_build_object(
        'address', address,
        'tx_count', tx_count,
        'first_seen', first_seen,
        'last_seen', last_seen
      ),
      'score', 95
    ) INTO address_result
    FROM api.evm_address_activity
    WHERE address = trimmed;

    IF address_result IS NOT NULL THEN
      results := results || address_result;
    END IF;
  END IF;

  RETURN results;
END;
$$;

GRANT EXECUTE ON FUNCTION api.universal_search(text) TO web_anon;
