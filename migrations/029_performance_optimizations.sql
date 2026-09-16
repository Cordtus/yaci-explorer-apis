-- Migration 029: Performance optimizations
-- - Add composite indexes for common query patterns
-- - Fix N+1 delegator count subquery in get_validators_paginated / get_validator_detail
-- - Add index on delegation_events for efficient aggregation

BEGIN;

-- ============================================================================
-- Step 1: Composite indexes for frequent access patterns
-- ============================================================================

-- delegation_events: validator + event_type composite for delegator count aggregation
-- Used heavily by get_validators_paginated and get_validator_detail
CREATE INDEX IF NOT EXISTS idx_delegation_events_validator_type
  ON api.delegation_events (validator_address, event_type);

-- transactions_main: height + timestamp composite for paginated queries
CREATE INDEX IF NOT EXISTS idx_tx_main_height_timestamp
  ON api.transactions_main (height DESC, timestamp DESC);

-- messages_main: id + sender composite for address-based transaction lookups
CREATE INDEX IF NOT EXISTS idx_msg_main_id_sender
  ON api.messages_main (id, sender);

-- validators: status + tokens composite for filtered sorted queries
CREATE INDEX IF NOT EXISTS idx_validators_status_tokens
  ON api.validators (status, tokens DESC NULLS LAST);

-- ============================================================================
-- Step 2: Fix N+1 delegator count in get_validators_paginated
-- Replaces per-row correlated subquery with pre-aggregated CTE
-- ============================================================================

CREATE OR REPLACE FUNCTION api.get_validators_paginated(
  _limit INT DEFAULT 20,
  _offset INT DEFAULT 0,
  _sort_by TEXT DEFAULT 'tokens',
  _sort_dir TEXT DEFAULT 'desc',
  _status TEXT DEFAULT NULL,
  _search TEXT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql STABLE
AS $$
DECLARE
  result JSONB;
BEGIN
  WITH total_bonded AS (
    SELECT COALESCE(SUM(tokens), 0) AS total
    FROM api.validators
    WHERE status = 'BOND_STATUS_BONDED' AND tokens IS NOT NULL
  ),
  delegator_counts AS (
    SELECT
      validator_address,
      COUNT(DISTINCT delegator_address) AS delegator_count
    FROM api.delegation_events
    WHERE event_type IN ('DELEGATE', 'CREATE_VALIDATOR')
    GROUP BY validator_address
  ),
  filtered AS (
    SELECT
      v.*,
      ipfs.ipfs_peer_id,
      CASE
        WHEN tb.total > 0 AND v.tokens IS NOT NULL
        THEN ROUND((v.tokens / tb.total) * 100, 4)
        ELSE 0
      END AS voting_power_pct,
      COALESCE(dc.delegator_count, 0) AS delegator_count
    FROM api.validators v
    CROSS JOIN total_bonded tb
    LEFT JOIN api.validator_ipfs_addresses ipfs
      ON ipfs.validator_address = v.operator_address
    LEFT JOIN delegator_counts dc
      ON dc.validator_address = v.operator_address
    WHERE (_status IS NULL OR v.status = _status)
    AND (_search IS NULL OR v.moniker ILIKE '%' || _search || '%' OR v.operator_address ILIKE '%' || _search || '%')
  ),
  total AS (
    SELECT COUNT(*) AS cnt FROM filtered
  ),
  sorted AS (
    SELECT * FROM filtered
    ORDER BY
      CASE WHEN _sort_by = 'tokens' AND _sort_dir = 'desc' THEN tokens END DESC NULLS LAST,
      CASE WHEN _sort_by = 'tokens' AND _sort_dir = 'asc' THEN tokens END ASC NULLS LAST,
      CASE WHEN _sort_by = 'moniker' AND _sort_dir = 'desc' THEN moniker END DESC NULLS LAST,
      CASE WHEN _sort_by = 'moniker' AND _sort_dir = 'asc' THEN moniker END ASC NULLS LAST,
      CASE WHEN _sort_by = 'commission' AND _sort_dir = 'desc' THEN commission_rate END DESC NULLS LAST,
      CASE WHEN _sort_by = 'commission' AND _sort_dir = 'asc' THEN commission_rate END ASC NULLS LAST,
      CASE WHEN _sort_by = 'status' AND _sort_dir = 'desc' THEN status END DESC NULLS LAST,
      CASE WHEN _sort_by = 'status' AND _sort_dir = 'asc' THEN status END ASC NULLS LAST,
      CASE WHEN _sort_by = 'delegators' AND _sort_dir = 'desc' THEN delegator_count END DESC,
      CASE WHEN _sort_by = 'delegators' AND _sort_dir = 'asc' THEN delegator_count END ASC,
      tokens DESC NULLS LAST
    LIMIT _limit OFFSET _offset
  )
  SELECT jsonb_build_object(
    'data', COALESCE(jsonb_agg(to_jsonb(s)), '[]'::jsonb),
    'pagination', jsonb_build_object(
      'total', (SELECT cnt FROM total),
      'limit', _limit,
      'offset', _offset,
      'has_next', _offset + _limit < (SELECT cnt FROM total),
      'has_prev', _offset > 0
    )
  )
  INTO result
  FROM sorted s;

  RETURN result;
END;
$$;

-- ============================================================================
-- Step 3: Fix N+1 delegator count in get_validator_detail
-- ============================================================================

CREATE OR REPLACE FUNCTION api.get_validator_detail(_operator_address TEXT)
RETURNS JSONB
LANGUAGE sql STABLE
AS $$
  WITH total_bonded AS (
    SELECT COALESCE(SUM(tokens), 0) AS total
    FROM api.validators
    WHERE status = 'BOND_STATUS_BONDED' AND tokens IS NOT NULL
  ),
  delegator_count AS (
    SELECT COUNT(DISTINCT delegator_address) AS cnt
    FROM api.delegation_events
    WHERE validator_address = _operator_address
    AND event_type IN ('DELEGATE', 'CREATE_VALIDATOR')
  ),
  validator AS (
    SELECT
      v.*,
      ipfs.ipfs_peer_id,
      ipfs.ipfs_multiaddrs,
      CASE
        WHEN tb.total > 0 AND v.tokens IS NOT NULL
        THEN ROUND((v.tokens / tb.total) * 100, 4)
        ELSE 0
      END AS voting_power_pct,
      dc.cnt AS delegator_count
    FROM api.validators v
    CROSS JOIN total_bonded tb
    CROSS JOIN delegator_count dc
    LEFT JOIN api.validator_ipfs_addresses ipfs
      ON ipfs.validator_address = v.operator_address
    WHERE v.operator_address = _operator_address
  )
  SELECT to_jsonb(validator) FROM validator;
$$;

-- ============================================================================
-- Grants
-- ============================================================================

GRANT EXECUTE ON FUNCTION api.get_validators_paginated(INT, INT, TEXT, TEXT, TEXT, TEXT) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_validator_detail(TEXT) TO web_anon;

COMMIT;
