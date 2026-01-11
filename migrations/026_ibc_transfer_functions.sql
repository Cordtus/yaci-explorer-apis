-- =============================================================================
-- IBC Transfer Functions
-- Migration 026: IBC transfer statistics and query functions
-- Safe to run on existing database with populated data
-- =============================================================================

BEGIN;

-- =============================================================================
-- IBC TRANSFER STATISTICS
-- =============================================================================

-- Get aggregate IBC statistics
CREATE OR REPLACE FUNCTION api.get_ibc_stats()
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
  WITH transfer_stats AS (
    SELECT
      COUNT(*) FILTER (WHERE type = '/ibc.applications.transfer.v1.MsgTransfer') as outgoing_transfers,
      COUNT(*) FILTER (WHERE type = '/ibc.core.channel.v1.MsgRecvPacket') as incoming_transfers,
      COUNT(*) FILTER (WHERE type = '/ibc.core.channel.v1.MsgAcknowledgement') as completed_transfers,
      COUNT(*) FILTER (WHERE type = '/ibc.core.channel.v1.MsgTimeout') as timed_out_transfers,
      COUNT(*) FILTER (WHERE type = '/ibc.core.client.v1.MsgUpdateClient') as relayer_updates
    FROM api.messages_main
    WHERE type LIKE '/ibc%'
  ),
  channel_stats AS (
    SELECT
      COUNT(*) as total_channels,
      COUNT(*) FILTER (WHERE state = 'STATE_OPEN') as open_channels,
      COUNT(*) FILTER (WHERE client_status = 'Active') as active_channels,
      COUNT(DISTINCT counterparty_chain_id) as connected_chains
    FROM api.ibc_connections
  ),
  denom_stats AS (
    SELECT COUNT(*) as total_denoms
    FROM api.ibc_denom_traces
  )
  SELECT jsonb_build_object(
    'outgoing_transfers', ts.outgoing_transfers,
    'incoming_transfers', ts.incoming_transfers,
    'completed_transfers', ts.completed_transfers,
    'timed_out_transfers', ts.timed_out_transfers,
    'relayer_updates', ts.relayer_updates,
    'total_channels', cs.total_channels,
    'open_channels', cs.open_channels,
    'active_channels', cs.active_channels,
    'connected_chains', cs.connected_chains,
    'total_denoms', ds.total_denoms
  )
  FROM transfer_stats ts, channel_stats cs, denom_stats ds;
$$;

-- =============================================================================
-- IBC TRANSFER QUERIES
-- =============================================================================

-- Get paginated IBC transfers with optional direction filter
CREATE OR REPLACE FUNCTION api.get_ibc_transfers(
  _limit integer DEFAULT 20,
  _offset integer DEFAULT 0,
  _direction text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
  WITH transfers AS (
    SELECT
      m.id as tx_hash,
      t.height,
      t.timestamp,
      m.type,
      m.sender,
      CASE
        WHEN m.type = '/ibc.applications.transfer.v1.MsgTransfer' THEN 'outgoing'
        WHEN m.type = '/ibc.core.channel.v1.MsgRecvPacket' THEN 'incoming'
        ELSE 'other'
      END as direction,
      m.metadata->>'sourceChannel' as source_channel,
      m.metadata->'token'->>'denom' as token_denom,
      m.metadata->'token'->>'amount' as token_amount,
      m.metadata->>'receiver' as receiver,
      t.error
    FROM api.messages_main m
    JOIN api.transactions_main t ON m.id = t.id
    WHERE m.type IN (
      '/ibc.applications.transfer.v1.MsgTransfer',
      '/ibc.core.channel.v1.MsgRecvPacket'
    )
    AND (_direction IS NULL OR
      (_direction = 'outgoing' AND m.type = '/ibc.applications.transfer.v1.MsgTransfer') OR
      (_direction = 'incoming' AND m.type = '/ibc.core.channel.v1.MsgRecvPacket')
    )
    ORDER BY t.height DESC, m.message_index
    LIMIT _limit OFFSET _offset
  ),
  total AS (
    SELECT COUNT(*) as count
    FROM api.messages_main
    WHERE type IN (
      '/ibc.applications.transfer.v1.MsgTransfer',
      '/ibc.core.channel.v1.MsgRecvPacket'
    )
    AND (_direction IS NULL OR
      (_direction = 'outgoing' AND type = '/ibc.applications.transfer.v1.MsgTransfer') OR
      (_direction = 'incoming' AND type = '/ibc.core.channel.v1.MsgRecvPacket')
    )
  )
  SELECT jsonb_build_object(
    'data', COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'tx_hash', tr.tx_hash,
        'height', tr.height,
        'timestamp', tr.timestamp,
        'direction', tr.direction,
        'sender', tr.sender,
        'receiver', tr.receiver,
        'source_channel', tr.source_channel,
        'token_denom', tr.token_denom,
        'token_amount', tr.token_amount,
        'resolved_denom', (
          SELECT jsonb_build_object(
            'symbol', COALESCE(d.symbol, d.base_denom),
            'decimals', COALESCE(d.decimals, 6),
            'base_denom', d.base_denom
          )
          FROM api.ibc_denom_traces d
          WHERE d.ibc_denom = tr.token_denom
        ),
        'counterparty_chain', (
          SELECT c.counterparty_chain_id
          FROM api.ibc_connections c
          WHERE c.channel_id = tr.source_channel AND c.port_id = 'transfer'
        ),
        'success', tr.error IS NULL OR tr.error = ''
      ) ORDER BY tr.height DESC)
      FROM transfers tr
    ), '[]'::jsonb),
    'pagination', jsonb_build_object(
      'total', (SELECT count FROM total),
      'limit', _limit,
      'offset', _offset
    )
  );
$$;

-- Get IBC transfers by address
CREATE OR REPLACE FUNCTION api.get_ibc_transfers_by_address(
  _address text,
  _limit integer DEFAULT 20,
  _offset integer DEFAULT 0
)
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
  WITH transfers AS (
    SELECT
      m.id as tx_hash,
      t.height,
      t.timestamp,
      m.type,
      m.sender,
      CASE
        WHEN m.type = '/ibc.applications.transfer.v1.MsgTransfer' THEN 'outgoing'
        WHEN m.type = '/ibc.core.channel.v1.MsgRecvPacket' THEN 'incoming'
        ELSE 'other'
      END as direction,
      m.metadata->>'sourceChannel' as source_channel,
      m.metadata->'token'->>'denom' as token_denom,
      m.metadata->'token'->>'amount' as token_amount,
      m.metadata->>'receiver' as receiver,
      t.error
    FROM api.messages_main m
    JOIN api.transactions_main t ON m.id = t.id
    WHERE m.type IN (
      '/ibc.applications.transfer.v1.MsgTransfer',
      '/ibc.core.channel.v1.MsgRecvPacket'
    )
    AND (m.sender = _address OR m.metadata->>'receiver' = _address OR _address = ANY(m.mentions))
    ORDER BY t.height DESC, m.message_index
    LIMIT _limit OFFSET _offset
  ),
  total AS (
    SELECT COUNT(*) as count
    FROM api.messages_main m
    WHERE type IN (
      '/ibc.applications.transfer.v1.MsgTransfer',
      '/ibc.core.channel.v1.MsgRecvPacket'
    )
    AND (m.sender = _address OR m.metadata->>'receiver' = _address OR _address = ANY(m.mentions))
  )
  SELECT jsonb_build_object(
    'data', COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'tx_hash', tr.tx_hash,
        'height', tr.height,
        'timestamp', tr.timestamp,
        'direction', tr.direction,
        'sender', tr.sender,
        'receiver', tr.receiver,
        'source_channel', tr.source_channel,
        'token_denom', tr.token_denom,
        'token_amount', tr.token_amount,
        'resolved_denom', (
          SELECT jsonb_build_object(
            'symbol', COALESCE(d.symbol, d.base_denom),
            'decimals', COALESCE(d.decimals, 6),
            'base_denom', d.base_denom
          )
          FROM api.ibc_denom_traces d
          WHERE d.ibc_denom = tr.token_denom
        ),
        'counterparty_chain', (
          SELECT c.counterparty_chain_id
          FROM api.ibc_connections c
          WHERE c.channel_id = tr.source_channel AND c.port_id = 'transfer'
        ),
        'success', tr.error IS NULL OR tr.error = ''
      ) ORDER BY tr.height DESC)
      FROM transfers tr
    ), '[]'::jsonb),
    'pagination', jsonb_build_object(
      'total', (SELECT count FROM total),
      'limit', _limit,
      'offset', _offset
    )
  );
$$;

-- =============================================================================
-- IBC CHANNEL ACTIVITY
-- =============================================================================

-- Get channel activity metrics
CREATE OR REPLACE FUNCTION api.get_ibc_channel_activity()
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
  WITH channel_activity AS (
    SELECT
      m.metadata->>'sourceChannel' as channel_id,
      COUNT(*) as transfer_count,
      COUNT(*) FILTER (WHERE t.error IS NULL OR t.error = '') as successful_transfers
    FROM api.messages_main m
    JOIN api.transactions_main t ON m.id = t.id
    WHERE m.type = '/ibc.applications.transfer.v1.MsgTransfer'
    AND m.metadata->>'sourceChannel' IS NOT NULL
    GROUP BY m.metadata->>'sourceChannel'
  )
  SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'channel_id', ca.channel_id,
    'transfer_count', ca.transfer_count,
    'successful_transfers', ca.successful_transfers,
    'counterparty_chain_id', c.counterparty_chain_id,
    'state', c.state,
    'client_status', c.client_status
  ) ORDER BY ca.transfer_count DESC), '[]'::jsonb)
  FROM channel_activity ca
  LEFT JOIN api.ibc_connections c ON ca.channel_id = c.channel_id AND c.port_id = 'transfer';
$$;

-- =============================================================================
-- IBC VOLUME TIMESERIES
-- =============================================================================

-- Get IBC volume over time
CREATE OR REPLACE FUNCTION api.get_ibc_volume_timeseries(
  _hours integer DEFAULT 24,
  _channel text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
WITH ibc_transfers AS (
  SELECT
    date_trunc('hour', (t.data->'txResponse'->>'timestamp')::timestamptz) AS hour_bucket,
    m.data->>'source_channel' AS channel,
    COALESCE(
      (m.data->'token'->>'amount')::numeric,
      (regexp_match(m.data->>'token', '"amount":"?([0-9]+)"?'))[1]::numeric,
      0
    ) AS amount,
    CASE
      WHEN m.data->>'@type' LIKE '%MsgTransfer' THEN 'outgoing'
      WHEN m.data->>'@type' LIKE '%MsgRecvPacket' THEN 'incoming'
      ELSE 'other'
    END AS direction
  FROM api.messages_raw m
  JOIN api.transactions_raw t ON m.id = t.id
  WHERE m.data->>'@type' IN (
    '/ibc.applications.transfer.v1.MsgTransfer',
    '/ibc.core.channel.v1.MsgRecvPacket'
  )
  AND (t.data->'txResponse'->>'timestamp')::timestamptz >= NOW() - (_hours || ' hours')::interval
  AND (_channel IS NULL OR m.data->>'source_channel' = _channel)
),
hourly_volume AS (
  SELECT
    hour_bucket,
    channel,
    direction,
    COUNT(*) AS transfer_count,
    SUM(amount) AS total_amount
  FROM ibc_transfers
  WHERE hour_bucket IS NOT NULL
  GROUP BY hour_bucket, channel, direction
),
time_series AS (
  SELECT generate_series(
    date_trunc('hour', NOW() - (_hours || ' hours')::interval),
    date_trunc('hour', NOW()),
    '1 hour'::interval
  ) AS hour_bucket
)
SELECT jsonb_build_object(
  'hours', _hours,
  'channel_filter', _channel,
  'data', COALESCE((
    SELECT jsonb_agg(jsonb_build_object(
      'hour', ts.hour_bucket,
      'outgoing_count', COALESCE((
        SELECT SUM(transfer_count) FROM hourly_volume hv
        WHERE hv.hour_bucket = ts.hour_bucket AND hv.direction = 'outgoing'
      ), 0),
      'incoming_count', COALESCE((
        SELECT SUM(transfer_count) FROM hourly_volume hv
        WHERE hv.hour_bucket = ts.hour_bucket AND hv.direction = 'incoming'
      ), 0),
      'outgoing_volume', COALESCE((
        SELECT SUM(total_amount) FROM hourly_volume hv
        WHERE hv.hour_bucket = ts.hour_bucket AND hv.direction = 'outgoing'
      ), 0),
      'incoming_volume', COALESCE((
        SELECT SUM(total_amount) FROM hourly_volume hv
        WHERE hv.hour_bucket = ts.hour_bucket AND hv.direction = 'incoming'
      ), 0)
    ) ORDER BY ts.hour_bucket)
    FROM time_series ts
  ), '[]'::jsonb),
  'channels', COALESCE((
    SELECT jsonb_agg(DISTINCT channel) FROM hourly_volume WHERE channel IS NOT NULL
  ), '[]'::jsonb)
);
$$;

-- =============================================================================
-- GRANT PERMISSIONS
-- =============================================================================

GRANT EXECUTE ON FUNCTION api.get_ibc_stats() TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_ibc_transfers(int, int, text) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_ibc_transfers_by_address(text, int, int) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_ibc_channel_activity() TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_ibc_volume_timeseries(int, text) TO web_anon;

COMMIT;
