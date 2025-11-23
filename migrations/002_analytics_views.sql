-- Migration: 002_analytics_views.sql
-- Phase 2: Analytics views and functions for pre-aggregated statistics
-- Eliminates client-side computation of analytics data

-- =============================================================================
-- 1. Chain Statistics View
-- =============================================================================

CREATE OR REPLACE VIEW api.chain_stats AS
WITH
latest_blocks AS (
  SELECT id, data
  FROM api.blocks_raw
  ORDER BY id DESC
  LIMIT 100
),
block_times AS (
  SELECT
    id,
    (data->'block'->'header'->>'time')::timestamptz AS block_time,
    LAG((data->'block'->'header'->>'time')::timestamptz) OVER (ORDER BY id) AS prev_time
  FROM latest_blocks
),
block_time_stats AS (
  SELECT
    AVG(EXTRACT(EPOCH FROM (block_time - prev_time))) AS avg_block_time,
    MIN(EXTRACT(EPOCH FROM (block_time - prev_time))) AS min_block_time,
    MAX(EXTRACT(EPOCH FROM (block_time - prev_time))) AS max_block_time
  FROM block_times
  WHERE prev_time IS NOT NULL
    AND EXTRACT(EPOCH FROM (block_time - prev_time)) > 0
    AND EXTRACT(EPOCH FROM (block_time - prev_time)) < 100
),
latest_block AS (
  SELECT
    id,
    jsonb_array_length(
      COALESCE(
        data->'block'->'last_commit'->'signatures',
        data->'block'->'lastCommit'->'signatures',
        '[]'::jsonb
      )
    ) AS validator_count
  FROM api.blocks_raw
  ORDER BY id DESC
  LIMIT 1
)
SELECT
  (SELECT id FROM latest_block) AS latest_block,
  (SELECT COUNT(*) FROM api.transactions_main) AS total_transactions,
  (SELECT COUNT(DISTINCT m.sender) FROM api.messages_main m WHERE m.sender IS NOT NULL) AS unique_addresses,
  COALESCE((SELECT avg_block_time FROM block_time_stats), 0) AS avg_block_time,
  COALESCE((SELECT min_block_time FROM block_time_stats), 0) AS min_block_time,
  COALESCE((SELECT max_block_time FROM block_time_stats), 0) AS max_block_time,
  (SELECT validator_count FROM latest_block) AS active_validators;

GRANT SELECT ON api.chain_stats TO web_anon;

-- =============================================================================
-- 2. Daily Transaction Volume View (90 days)
-- =============================================================================

CREATE OR REPLACE VIEW api.tx_volume_daily AS
SELECT
  DATE(timestamp) AS date,
  COUNT(*) AS count
FROM api.transactions_main
WHERE timestamp >= NOW() - INTERVAL '90 days'
GROUP BY DATE(timestamp)
ORDER BY date DESC;

GRANT SELECT ON api.tx_volume_daily TO web_anon;

-- =============================================================================
-- 3. Hourly Transaction Volume View (7 days)
-- =============================================================================

CREATE OR REPLACE VIEW api.tx_volume_hourly AS
SELECT
  DATE_TRUNC('hour', timestamp) AS hour,
  COUNT(*) AS count
FROM api.transactions_main
WHERE timestamp >= NOW() - INTERVAL '7 days'
GROUP BY DATE_TRUNC('hour', timestamp)
ORDER BY hour DESC;

GRANT SELECT ON api.tx_volume_hourly TO web_anon;

-- =============================================================================
-- 4. Message Type Statistics View
-- =============================================================================

CREATE OR REPLACE VIEW api.message_type_stats AS
SELECT
  COALESCE(type, 'Unknown') AS type,
  COUNT(*) AS count
FROM api.messages_main
GROUP BY type
ORDER BY count DESC;

GRANT SELECT ON api.message_type_stats TO web_anon;

-- =============================================================================
-- 5. Gas Usage Distribution View
-- =============================================================================

CREATE OR REPLACE VIEW api.gas_usage_distribution AS
SELECT
  range_label AS range,
  COUNT(*) AS count
FROM (
  SELECT
    CASE
      WHEN (fee->>'gasLimit')::bigint < 100000 THEN '0-100k'
      WHEN (fee->>'gasLimit')::bigint < 250000 THEN '100k-250k'
      WHEN (fee->>'gasLimit')::bigint < 500000 THEN '250k-500k'
      WHEN (fee->>'gasLimit')::bigint < 1000000 THEN '500k-1M'
      ELSE '1M+'
    END AS range_label
  FROM api.transactions_main
  WHERE fee->>'gasLimit' IS NOT NULL
) AS binned
GROUP BY range_label
ORDER BY
  CASE range_label
    WHEN '0-100k' THEN 1
    WHEN '100k-250k' THEN 2
    WHEN '250k-500k' THEN 3
    WHEN '500k-1M' THEN 4
    WHEN '1M+' THEN 5
  END;

GRANT SELECT ON api.gas_usage_distribution TO web_anon;

-- =============================================================================
-- 6. Transaction Success Rate View
-- =============================================================================

CREATE OR REPLACE VIEW api.tx_success_rate AS
SELECT
  COUNT(*) AS total,
  COUNT(*) FILTER (WHERE error IS NULL) AS successful,
  COUNT(*) FILTER (WHERE error IS NOT NULL) AS failed,
  ROUND(
    100.0 * COUNT(*) FILTER (WHERE error IS NULL) / NULLIF(COUNT(*), 0),
    2
  ) AS success_rate_percent
FROM api.transactions_main;

GRANT SELECT ON api.tx_success_rate TO web_anon;

-- =============================================================================
-- 7. Fee Revenue View
-- =============================================================================

CREATE OR REPLACE VIEW api.fee_revenue AS
SELECT
  coin->>'denom' AS denom,
  SUM((coin->>'amount')::numeric) AS total_amount
FROM api.transactions_main,
     jsonb_array_elements(fee->'amount') AS coin
WHERE fee->'amount' IS NOT NULL
GROUP BY coin->>'denom'
ORDER BY total_amount DESC;

GRANT SELECT ON api.fee_revenue TO web_anon;

-- =============================================================================
-- 8. Block Time Analysis Function
-- =============================================================================

CREATE OR REPLACE FUNCTION api.get_block_time_analysis(_limit int DEFAULT 100)
RETURNS jsonb
LANGUAGE sql STABLE
AS $$
  WITH
  recent_blocks AS (
    SELECT
      id,
      (data->'block'->'header'->>'time')::timestamptz AS block_time
    FROM api.blocks_raw
    ORDER BY id DESC
    LIMIT _limit
  ),
  block_intervals AS (
    SELECT
      EXTRACT(EPOCH FROM (block_time - LAG(block_time) OVER (ORDER BY id))) AS interval_seconds
    FROM recent_blocks
  ),
  stats AS (
    SELECT
      AVG(interval_seconds) AS avg,
      MIN(interval_seconds) AS min,
      MAX(interval_seconds) AS max
    FROM block_intervals
    WHERE interval_seconds > 0 AND interval_seconds < 100
  )
  SELECT jsonb_build_object(
    'avg', COALESCE(avg, 0),
    'min', COALESCE(min, 0),
    'max', COALESCE(max, 0)
  )
  FROM stats;
$$;

GRANT EXECUTE ON FUNCTION api.get_block_time_analysis(int) TO web_anon;

-- =============================================================================
-- 9. Active Addresses Daily Function
-- =============================================================================

CREATE OR REPLACE FUNCTION api.get_active_addresses_daily(_days int DEFAULT 30)
RETURNS TABLE (date date, count bigint)
LANGUAGE sql STABLE
AS $$
  SELECT
    DATE(t.timestamp) AS date,
    COUNT(DISTINCT m.sender) AS count
  FROM api.messages_main m
  JOIN api.transactions_main t ON m.id = t.id
  WHERE t.timestamp >= NOW() - (_days || ' days')::interval
    AND m.sender IS NOT NULL
  GROUP BY DATE(t.timestamp)
  ORDER BY date DESC;
$$;

GRANT EXECUTE ON FUNCTION api.get_active_addresses_daily(int) TO web_anon;
