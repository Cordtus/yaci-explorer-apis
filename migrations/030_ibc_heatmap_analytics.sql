-- =============================================================================
-- IBC Heat Map Analytics & Volume Timeseries Fix
-- Migration 030: Enhanced IBC analytics with heat maps and timeframe support
-- =============================================================================

BEGIN;

-- =============================================================================
-- FIX: get_ibc_volume_timeseries response structure
-- Frontend expects { timeseries, summary } not { hours, data, channels }
-- =============================================================================

CREATE OR REPLACE FUNCTION api.get_ibc_volume_timeseries(
  _hours integer DEFAULT 24,
  _channel text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
  WITH ibc_messages AS (
    SELECT
      m.*,
      t.timestamp,
      t.error,
      api.extract_ibc_transfer_details(m) as details,
      CASE
        WHEN m.type = '/ibc.applications.transfer.v1.MsgTransfer' THEN 'outgoing'
        WHEN m.type = '/ibc.core.channel.v1.MsgRecvPacket' THEN 'incoming'
        ELSE 'other'
      END as direction
    FROM api.messages_main m
    JOIN api.transactions_main t ON m.id = t.id
    WHERE m.type IN (
      '/ibc.applications.transfer.v1.MsgTransfer',
      '/ibc.core.channel.v1.MsgRecvPacket'
    )
    AND t.timestamp >= NOW() - (_hours || ' hours')::interval
    AND (t.error IS NULL OR t.error = '')
  ),
  filtered AS (
    SELECT
      timestamp,
      direction,
      details->>'source_channel' AS channel
    FROM ibc_messages
    WHERE _channel IS NULL OR details->>'source_channel' = _channel
  ),
  hourly_volume AS (
    SELECT
      date_trunc('hour', timestamp) AS hour_bucket,
      direction,
      COUNT(*) AS transfer_count
    FROM filtered
    WHERE timestamp IS NOT NULL
    GROUP BY date_trunc('hour', timestamp), direction
  ),
  time_series AS (
    SELECT generate_series(
      date_trunc('hour', NOW() - (_hours || ' hours')::interval),
      date_trunc('hour', NOW()),
      '1 hour'::interval
    ) AS hour_bucket
  ),
  timeseries_data AS (
    SELECT
      ts.hour_bucket AS hour,
      COALESCE(SUM(hv.transfer_count) FILTER (WHERE hv.direction = 'outgoing'), 0)::int AS outgoing_count,
      COALESCE(SUM(hv.transfer_count) FILTER (WHERE hv.direction = 'incoming'), 0)::int AS incoming_count,
      COALESCE(SUM(hv.transfer_count), 0)::int AS total_count
    FROM time_series ts
    LEFT JOIN hourly_volume hv ON hv.hour_bucket = ts.hour_bucket
    GROUP BY ts.hour_bucket
    ORDER BY ts.hour_bucket DESC
  ),
  summary_data AS (
    SELECT
      COALESCE(SUM(transfer_count) FILTER (WHERE direction = 'outgoing'), 0)::int AS total_outgoing,
      COALESCE(SUM(transfer_count) FILTER (WHERE direction = 'incoming'), 0)::int AS total_incoming,
      COALESCE(SUM(transfer_count), 0)::int AS total_transfers
    FROM hourly_volume
  ),
  peak_data AS (
    SELECT
      hour_bucket AS peak_hour,
      SUM(transfer_count)::int AS peak_count
    FROM hourly_volume
    GROUP BY hour_bucket
    ORDER BY SUM(transfer_count) DESC
    LIMIT 1
  )
  SELECT jsonb_build_object(
    'timeseries', COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'hour', td.hour,
        'outgoing_count', td.outgoing_count,
        'incoming_count', td.incoming_count,
        'total_count', td.total_count
      ) ORDER BY td.hour DESC)
      FROM timeseries_data td
    ), '[]'::jsonb),
    'summary', jsonb_build_object(
      'total_outgoing', (SELECT total_outgoing FROM summary_data),
      'total_incoming', (SELECT total_incoming FROM summary_data),
      'total_transfers', (SELECT total_transfers FROM summary_data),
      'peak_hour', (SELECT peak_hour FROM peak_data),
      'peak_count', COALESCE((SELECT peak_count FROM peak_data), 0)
    )
  );
$$;

-- =============================================================================
-- NEW: IBC Heat Map Analytics Function
-- Provides comprehensive data for heat map visualizations
-- =============================================================================

CREATE OR REPLACE FUNCTION api.get_ibc_heatmap_data(
  _timeframe text DEFAULT '7d',
  _metric text DEFAULT 'count',
  _direction text DEFAULT NULL,
  _channel text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
  WITH params AS (
    SELECT
      CASE _timeframe
        WHEN '24h' THEN interval '24 hours'
        WHEN '48h' THEN interval '48 hours'
        WHEN '7d' THEN interval '7 days'
        WHEN '30d' THEN interval '30 days'
        ELSE interval '7 days'
      END AS lookback,
      CASE _timeframe
        WHEN '24h' THEN 'hour'
        WHEN '48h' THEN 'hour'
        WHEN '7d' THEN 'hour'
        WHEN '30d' THEN 'day'
        ELSE 'hour'
      END AS granularity
  ),
  ibc_data AS (
    SELECT
      m.*,
      t.timestamp,
      t.error,
      api.extract_ibc_transfer_details(m) as details,
      CASE
        WHEN m.type = '/ibc.applications.transfer.v1.MsgTransfer' THEN 'outgoing'
        WHEN m.type = '/ibc.core.channel.v1.MsgRecvPacket' THEN 'incoming'
      END as direction
    FROM api.messages_main m
    JOIN api.transactions_main t ON m.id = t.id
    CROSS JOIN params p
    WHERE m.type IN (
      '/ibc.applications.transfer.v1.MsgTransfer',
      '/ibc.core.channel.v1.MsgRecvPacket'
    )
    AND t.timestamp >= NOW() - p.lookback
    AND (t.error IS NULL OR t.error = '')
    AND (_direction IS NULL OR
      (_direction = 'outgoing' AND m.type = '/ibc.applications.transfer.v1.MsgTransfer') OR
      (_direction = 'incoming' AND m.type = '/ibc.core.channel.v1.MsgRecvPacket')
    )
  ),
  filtered AS (
    SELECT
      timestamp,
      direction,
      details->>'source_channel' AS channel,
      COALESCE((details->>'token_amount')::numeric, 0) AS amount
    FROM ibc_data
    WHERE _channel IS NULL OR details->>'source_channel' = _channel
  ),
  hourly_data AS (
    SELECT
      date_trunc('hour', timestamp) AS bucket,
      EXTRACT(DOW FROM timestamp)::int AS day_of_week,
      EXTRACT(HOUR FROM timestamp)::int AS hour_of_day,
      direction,
      COUNT(*) AS transfer_count,
      SUM(amount) AS total_volume
    FROM filtered
    WHERE (SELECT granularity FROM params) = 'hour'
    GROUP BY 1, 2, 3, 4
  ),
  daily_data AS (
    SELECT
      date_trunc('day', timestamp) AS bucket,
      EXTRACT(DOW FROM timestamp)::int AS day_of_week,
      NULL::int AS hour_of_day,
      direction,
      COUNT(*) AS transfer_count,
      SUM(amount) AS total_volume
    FROM filtered
    WHERE (SELECT granularity FROM params) = 'day'
    GROUP BY 1, 2, 4
  ),
  combined AS (
    SELECT * FROM hourly_data
    UNION ALL
    SELECT * FROM daily_data
  ),
  timeseries AS (
    SELECT
      bucket,
      COALESCE(SUM(transfer_count) FILTER (WHERE direction = 'outgoing'), 0) AS outgoing_count,
      COALESCE(SUM(transfer_count) FILTER (WHERE direction = 'incoming'), 0) AS incoming_count,
      COALESCE(SUM(total_volume) FILTER (WHERE direction = 'outgoing'), 0) AS outgoing_volume,
      COALESCE(SUM(total_volume) FILTER (WHERE direction = 'incoming'), 0) AS incoming_volume
    FROM combined
    GROUP BY bucket
    ORDER BY bucket
  ),
  heatmap_matrix AS (
    SELECT
      day_of_week,
      hour_of_day,
      COALESCE(SUM(transfer_count), 0) AS count,
      COALESCE(SUM(total_volume), 0) AS volume
    FROM combined
    WHERE hour_of_day IS NOT NULL
    GROUP BY day_of_week, hour_of_day
  ),
  summary AS (
    SELECT
      COALESCE(SUM(transfer_count) FILTER (WHERE direction = 'outgoing'), 0) AS total_outgoing_count,
      COALESCE(SUM(transfer_count) FILTER (WHERE direction = 'incoming'), 0) AS total_incoming_count,
      COALESCE(SUM(total_volume) FILTER (WHERE direction = 'outgoing'), 0) AS total_outgoing_volume,
      COALESCE(SUM(total_volume) FILTER (WHERE direction = 'incoming'), 0) AS total_incoming_volume,
      COALESCE(MAX(transfer_count), 0) AS peak_count,
      COALESCE(MAX(total_volume), 0) AS peak_volume
    FROM combined
  ),
  channel_breakdown AS (
    SELECT
      details->>'source_channel' AS channel,
      COUNT(*) AS transfer_count,
      SUM(COALESCE((details->>'token_amount')::numeric, 0)) AS total_volume
    FROM ibc_data
    WHERE details->>'source_channel' IS NOT NULL
    GROUP BY 1
    ORDER BY transfer_count DESC
    LIMIT 10
  )
  SELECT jsonb_build_object(
    'timeframe', _timeframe,
    'metric', _metric,
    'direction_filter', _direction,
    'channel_filter', _channel,
    'granularity', (SELECT granularity FROM params),
    'timeseries', COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'timestamp', bucket,
        'outgoing_count', outgoing_count,
        'incoming_count', incoming_count,
        'outgoing_volume', outgoing_volume,
        'incoming_volume', incoming_volume,
        'total_count', outgoing_count + incoming_count,
        'total_volume', outgoing_volume + incoming_volume
      ) ORDER BY bucket)
      FROM timeseries
    ), '[]'::jsonb),
    'heatmap', COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'day', day_of_week,
        'hour', hour_of_day,
        'count', count,
        'volume', volume
      ))
      FROM heatmap_matrix
    ), '[]'::jsonb),
    'summary', (
      SELECT jsonb_build_object(
        'total_outgoing_count', total_outgoing_count,
        'total_incoming_count', total_incoming_count,
        'total_outgoing_volume', total_outgoing_volume,
        'total_incoming_volume', total_incoming_volume,
        'total_transfers', total_outgoing_count + total_incoming_count,
        'total_volume', total_outgoing_volume + total_incoming_volume,
        'peak_hourly_count', peak_count,
        'peak_hourly_volume', peak_volume
      )
      FROM summary
    ),
    'top_channels', COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'channel', channel,
        'count', transfer_count,
        'volume', total_volume
      ))
      FROM channel_breakdown
    ), '[]'::jsonb)
  );
$$;

-- =============================================================================
-- GRANT PERMISSIONS
-- =============================================================================

GRANT EXECUTE ON FUNCTION api.get_ibc_volume_timeseries(integer, text) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_ibc_heatmap_data(text, text, text, text) TO web_anon;

COMMIT;
