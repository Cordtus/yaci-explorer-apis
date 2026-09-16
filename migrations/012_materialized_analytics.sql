BEGIN;

-- Daily transaction statistics
DROP MATERIALIZED VIEW IF EXISTS api.mv_daily_tx_stats CASCADE;

CREATE MATERIALIZED VIEW api.mv_daily_tx_stats AS
WITH daily_txs AS (
	SELECT
		date_trunc('day', timestamp)::date AS date,
		COUNT(*)::bigint AS total_txs,
		COUNT(*) FILTER (WHERE error IS NULL)::bigint AS successful_txs,
		COUNT(*) FILTER (WHERE error IS NOT NULL)::bigint AS failed_txs
	FROM api.transactions_main
	GROUP BY date_trunc('day', timestamp)::date
),
daily_senders AS (
	SELECT
		date_trunc('day', t.timestamp)::date AS date,
		COUNT(DISTINCT m.sender)::bigint AS unique_senders
	FROM api.transactions_main t
	JOIN api.messages_main m ON m.id = t.id
	GROUP BY date_trunc('day', t.timestamp)::date
)
SELECT
	dt.date,
	dt.total_txs,
	dt.successful_txs,
	dt.failed_txs,
	COALESCE(ds.unique_senders, 0) AS unique_senders
FROM daily_txs dt
LEFT JOIN daily_senders ds ON ds.date = dt.date;

CREATE UNIQUE INDEX mv_daily_tx_stats_date_idx ON api.mv_daily_tx_stats(date);

-- Hourly transaction statistics for last 7 days
DROP MATERIALIZED VIEW IF EXISTS api.mv_hourly_tx_stats CASCADE;

CREATE MATERIALIZED VIEW api.mv_hourly_tx_stats AS
SELECT
	date_trunc('hour', timestamp) AS hour,
	COUNT(*)::bigint AS tx_count
FROM api.transactions_main
WHERE timestamp >= NOW() - INTERVAL '7 days'
GROUP BY date_trunc('hour', timestamp);

CREATE UNIQUE INDEX mv_hourly_tx_stats_hour_idx ON api.mv_hourly_tx_stats(hour);

-- Message type distribution
DROP MATERIALIZED VIEW IF EXISTS api.mv_message_type_stats CASCADE;

CREATE MATERIALIZED VIEW api.mv_message_type_stats AS
WITH totals AS (
	SELECT COUNT(*)::numeric AS total
	FROM api.messages_main
),
type_counts AS (
	SELECT
		type AS message_type,
		COUNT(*)::bigint AS count
	FROM api.messages_main
	GROUP BY type
)
SELECT
	tc.message_type,
	tc.count,
	ROUND((tc.count::numeric / t.total * 100)::numeric, 2) AS percentage
FROM type_counts tc
CROSS JOIN totals t;

CREATE UNIQUE INDEX mv_message_type_stats_type_idx ON api.mv_message_type_stats(message_type);

-- Function to refresh all analytics views concurrently
CREATE OR REPLACE FUNCTION api.refresh_analytics_views()
RETURNS void
LANGUAGE sql
AS $$
	REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_daily_tx_stats;
	REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_hourly_tx_stats;
	REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_message_type_stats;
$$;

-- Grant permissions
GRANT SELECT ON api.mv_daily_tx_stats TO web_anon;
GRANT SELECT ON api.mv_hourly_tx_stats TO web_anon;
GRANT SELECT ON api.mv_message_type_stats TO web_anon;
GRANT EXECUTE ON FUNCTION api.refresh_analytics_views() TO web_anon;

COMMIT;
