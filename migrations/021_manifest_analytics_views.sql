-- =============================================================================
-- Manifest Indexer Analytics Views
-- Migration 021: Create analytics views
-- Safe to run on existing database with populated data
-- =============================================================================

BEGIN;

-- Chain statistics view (no EVM for Manifest)
CREATE OR REPLACE VIEW api.chain_stats AS
SELECT
  (SELECT MAX(id) FROM api.blocks_raw) AS latest_block,
  (SELECT COUNT(*) FROM api.transactions_main) AS total_transactions,
  (SELECT COUNT(DISTINCT sender) FROM api.messages_main WHERE sender IS NOT NULL) AS unique_addresses,
  0::bigint AS evm_transactions,
  0::bigint AS active_validators;

-- Transaction volume daily
CREATE OR REPLACE VIEW api.tx_volume_daily AS
SELECT
  DATE(timestamp) AS date,
  COUNT(*) AS count
FROM api.transactions_main
WHERE timestamp IS NOT NULL
GROUP BY DATE(timestamp)
ORDER BY date DESC;

-- Transaction volume hourly
CREATE OR REPLACE VIEW api.tx_volume_hourly AS
SELECT
  DATE_TRUNC('hour', timestamp) AS hour,
  COUNT(*) AS count
FROM api.transactions_main
WHERE timestamp IS NOT NULL
GROUP BY DATE_TRUNC('hour', timestamp)
ORDER BY hour DESC;

-- Message type distribution
CREATE OR REPLACE VIEW api.message_type_stats AS
SELECT
  type,
  COUNT(*) AS count
FROM api.messages_main
WHERE type IS NOT NULL
GROUP BY type
ORDER BY count DESC;

-- Transaction success rate
CREATE OR REPLACE VIEW api.tx_success_rate AS
SELECT
  COUNT(*) AS total,
  COUNT(*) FILTER (WHERE error IS NULL) AS successful,
  COUNT(*) FILTER (WHERE error IS NOT NULL) AS failed,
  ROUND(100.0 * COUNT(*) FILTER (WHERE error IS NULL) / NULLIF(COUNT(*), 0), 2) AS success_rate_percent
FROM api.transactions_main;

-- Fee revenue by denomination
CREATE OR REPLACE VIEW api.fee_revenue AS
SELECT
  fee_item->>'denom' AS denom,
  SUM((fee_item->>'amount')::numeric) AS total_amount
FROM api.transactions_main,
  jsonb_array_elements(fee->'amount') AS fee_item
WHERE fee IS NOT NULL AND fee->'amount' IS NOT NULL
GROUP BY fee_item->>'denom';

-- Gas usage distribution
CREATE OR REPLACE VIEW api.gas_usage_distribution AS
SELECT
  CASE
    WHEN (fee->>'gasLimit')::bigint < 100000 THEN '0-100k'
    WHEN (fee->>'gasLimit')::bigint < 250000 THEN '100k-250k'
    WHEN (fee->>'gasLimit')::bigint < 500000 THEN '250k-500k'
    WHEN (fee->>'gasLimit')::bigint < 1000000 THEN '500k-1M'
    ELSE '1M+'
  END AS gas_range,
  COUNT(*) AS count
FROM api.transactions_main
WHERE fee->>'gasLimit' IS NOT NULL
GROUP BY 1
ORDER BY MIN((fee->>'gasLimit')::bigint);

-- Grant access to views
GRANT SELECT ON api.chain_stats TO web_anon;
GRANT SELECT ON api.tx_volume_daily TO web_anon;
GRANT SELECT ON api.tx_volume_hourly TO web_anon;
GRANT SELECT ON api.message_type_stats TO web_anon;
GRANT SELECT ON api.tx_success_rate TO web_anon;
GRANT SELECT ON api.fee_revenue TO web_anon;
GRANT SELECT ON api.gas_usage_distribution TO web_anon;

COMMIT;
