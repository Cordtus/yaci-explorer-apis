-- Migration 038: Materialized views for expensive aggregations
-- MEDIUM priority improvements from database audit:
-- 1. Materialized view for validator delegator counts (expensive COUNT DISTINCT)
-- 2. Materialized view for fee revenue by denom
-- 3. Add composite index for messages_main type+sender lookups

BEGIN;

-- ============================================================================
-- 1. Materialized view for validator delegator counts
-- Replaces expensive COUNT(DISTINCT delegator_address) in every validator query
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS api.mv_validator_delegator_counts AS
SELECT
  validator_address,
  COUNT(DISTINCT delegator_address) AS delegator_count,
  MAX(timestamp) AS last_delegation_at
FROM api.delegation_events
WHERE event_type IN ('DELEGATE', 'CREATE_VALIDATOR')
GROUP BY validator_address;

CREATE UNIQUE INDEX IF NOT EXISTS mv_validator_delegator_counts_addr_idx
  ON api.mv_validator_delegator_counts(validator_address);

-- ============================================================================
-- 2. Materialized view for fee revenue by denom
-- Useful for analytics dashboards showing fee collection over time
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS api.mv_fee_revenue_daily AS
SELECT
  date_trunc('day', timestamp)::date AS date,
  f->>'denom' AS fee_denom,
  SUM(NULLIF(f->>'amount', '')::NUMERIC) AS total_fees,
  COUNT(*) AS tx_count
FROM api.transactions_main,
  LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(fee) = 'array' THEN fee ELSE '[]'::jsonb END) AS f
WHERE fee IS NOT NULL AND jsonb_typeof(fee) = 'array'
GROUP BY date_trunc('day', timestamp)::date, f->>'denom';

CREATE UNIQUE INDEX IF NOT EXISTS mv_fee_revenue_daily_date_denom_idx
  ON api.mv_fee_revenue_daily(date, fee_denom);

-- ============================================================================
-- 3. Composite index for messages_main type+sender lookups
-- Covers query patterns filtering by message type and sender together
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_msg_main_type_sender
  ON api.messages_main (type, sender);

-- ============================================================================
-- 4. Composite index for finalize_block_events
-- Better coverage for event queries filtering by type and height range
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_finalize_events_height_type
  ON api.finalize_block_events (height DESC, event_type);

-- ============================================================================
-- 5. Update get_validators_paginated to use materialized delegator counts
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
    LEFT JOIN api.mv_validator_delegator_counts dc
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
-- 6. Update get_validator_detail to use materialized delegator counts
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
      COALESCE(dc.delegator_count, 0) AS delegator_count
    FROM api.validators v
    CROSS JOIN total_bonded tb
    LEFT JOIN api.mv_validator_delegator_counts dc
      ON dc.validator_address = v.operator_address
    LEFT JOIN api.validator_ipfs_addresses ipfs
      ON ipfs.validator_address = v.operator_address
    WHERE v.operator_address = _operator_address
  )
  SELECT to_jsonb(validator) FROM validator;
$$;

-- ============================================================================
-- 7. Update refresh_analytics_views to include new materialized views
-- ============================================================================

CREATE OR REPLACE FUNCTION api.refresh_analytics_views()
RETURNS void
LANGUAGE sql
AS $$
  REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_daily_tx_stats;
  REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_hourly_tx_stats;
  REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_message_type_stats;
  REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_validator_delegator_counts;
  REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_fee_revenue_daily;
$$;

-- ============================================================================
-- Grants
-- ============================================================================

GRANT SELECT ON api.mv_validator_delegator_counts TO web_anon;
GRANT SELECT ON api.mv_fee_revenue_daily TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_validators_paginated(INT, INT, TEXT, TEXT, TEXT, TEXT) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_validator_detail(TEXT) TO web_anon;
GRANT EXECUTE ON FUNCTION api.refresh_analytics_views() TO web_anon;

COMMIT;
