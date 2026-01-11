-- =============================================================================
-- Migration 028: IBC Transfer Functions V2
-- Improves IBC transfer queries with better denom resolution and pagination
-- Replaces functions from migration 026
-- =============================================================================

BEGIN;

-- =============================================================================
-- IBC TRANSFER STATISTICS (unchanged but re-created for consistency)
-- =============================================================================

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
      COUNT(*) FILTER (WHERE state = 'STATE_OPEN' AND client_status = 'Active') as active_channels,
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
-- HELPER: Extract transfer details from message
-- Handles both MsgTransfer and MsgRecvPacket message types
-- =============================================================================

CREATE OR REPLACE FUNCTION api.extract_ibc_transfer_details(_message api.messages_main)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  _result jsonb;
  _packet_data jsonb;
  _token_denom text;
  _token_amount text;
  _sender text;
  _receiver text;
  _source_channel text;
BEGIN
  IF _message.type = '/ibc.applications.transfer.v1.MsgTransfer' THEN
    -- Outgoing transfer: straightforward extraction
    _sender := _message.sender;
    _receiver := _message.metadata->>'receiver';
    _source_channel := COALESCE(
      _message.metadata->>'sourceChannel',
      _message.metadata->>'source_channel'
    );
    _token_denom := COALESCE(
      _message.metadata->'token'->>'denom',
      _message.metadata->>'denom'
    );
    _token_amount := COALESCE(
      _message.metadata->'token'->>'amount',
      _message.metadata->>'amount'
    );

  ELSIF _message.type = '/ibc.core.channel.v1.MsgRecvPacket' THEN
    -- Incoming transfer: need to parse packet data
    -- Packet data can be in multiple locations depending on indexer version
    _packet_data := COALESCE(
      _message.metadata->'packet'->'data',
      _message.metadata->'packetData',
      _message.metadata->'packet_data'
    );

    -- If packet data is a string, it might be base64 or JSON encoded
    IF jsonb_typeof(_packet_data) = 'string' THEN
      BEGIN
        _packet_data := (_packet_data #>> '{}')::jsonb;
      EXCEPTION WHEN OTHERS THEN
        _packet_data := NULL;
      END;
    END IF;

    _sender := COALESCE(
      _packet_data->>'sender',
      _message.metadata->>'sender'
    );
    _receiver := COALESCE(
      _packet_data->>'receiver',
      _message.metadata->>'receiver'
    );
    _source_channel := COALESCE(
      _message.metadata->'packet'->>'sourceChannel',
      _message.metadata->'packet'->>'source_channel',
      _message.metadata->>'sourceChannel'
    );
    _token_denom := COALESCE(
      _packet_data->>'denom',
      _message.metadata->>'denom'
    );
    _token_amount := COALESCE(
      _packet_data->>'amount',
      _message.metadata->>'amount'
    );
  END IF;

  RETURN jsonb_build_object(
    'sender', _sender,
    'receiver', _receiver,
    'source_channel', _source_channel,
    'token_denom', _token_denom,
    'token_amount', _token_amount
  );
END;
$$;

-- =============================================================================
-- GET IBC TRANSFERS (paginated with direction filter)
-- =============================================================================

CREATE OR REPLACE FUNCTION api.get_ibc_transfers(
  _limit integer DEFAULT 20,
  _offset integer DEFAULT 0,
  _direction text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
  WITH filtered_messages AS (
    SELECT
      m.*,
      t.height,
      t.timestamp,
      t.error,
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
    AND (_direction IS NULL OR
      (_direction = 'outgoing' AND m.type = '/ibc.applications.transfer.v1.MsgTransfer') OR
      (_direction = 'incoming' AND m.type = '/ibc.core.channel.v1.MsgRecvPacket')
    )
  ),
  total AS (
    SELECT COUNT(*)::int as count FROM filtered_messages
  ),
  paginated AS (
    SELECT * FROM filtered_messages
    ORDER BY height DESC, message_index
    LIMIT _limit OFFSET _offset
  ),
  transfers AS (
    SELECT
      p.id as tx_hash,
      p.height,
      p.timestamp,
      p.direction,
      p.error,
      api.extract_ibc_transfer_details(p) as details
    FROM paginated p
  )
  SELECT jsonb_build_object(
    'data', COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'tx_hash', tr.tx_hash,
        'height', tr.height,
        'timestamp', tr.timestamp,
        'direction', tr.direction,
        'sender', tr.details->>'sender',
        'receiver', tr.details->>'receiver',
        'source_channel', tr.details->>'source_channel',
        'token_denom', tr.details->>'token_denom',
        'token_amount', tr.details->>'token_amount',
        'resolved_denom', api.resolve_denom(tr.details->>'token_denom'),
        'counterparty_chain', (
          SELECT c.counterparty_chain_id
          FROM api.ibc_connections c
          WHERE c.channel_id = tr.details->>'source_channel' AND c.port_id = 'transfer'
        ),
        'success', tr.error IS NULL OR tr.error = ''
      ) ORDER BY tr.height DESC)
      FROM transfers tr
    ), '[]'::jsonb),
    'pagination', jsonb_build_object(
      'total', (SELECT count FROM total),
      'limit', _limit,
      'offset', _offset,
      'has_next', _offset + _limit < (SELECT count FROM total),
      'has_prev', _offset > 0
    )
  );
$$;

-- =============================================================================
-- GET IBC TRANSFERS BY ADDRESS
-- =============================================================================

CREATE OR REPLACE FUNCTION api.get_ibc_transfers_by_address(
  _address text,
  _limit integer DEFAULT 10,
  _offset integer DEFAULT 0
)
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
  WITH filtered_messages AS (
    SELECT
      m.*,
      t.height,
      t.timestamp,
      t.error,
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
    AND (
      m.sender = _address
      OR m.metadata->>'receiver' = _address
      OR _address = ANY(m.mentions)
      -- Also check packet data for incoming transfers
      OR m.metadata->'packet'->'data'->>'receiver' = _address
      OR m.metadata->'packetData'->>'receiver' = _address
    )
  ),
  total AS (
    SELECT COUNT(*)::int as count FROM filtered_messages
  ),
  paginated AS (
    SELECT * FROM filtered_messages
    ORDER BY height DESC, message_index
    LIMIT _limit OFFSET _offset
  ),
  transfers AS (
    SELECT
      p.id as tx_hash,
      p.height,
      p.timestamp,
      p.direction,
      p.error,
      api.extract_ibc_transfer_details(p) as details
    FROM paginated p
  )
  SELECT jsonb_build_object(
    'data', COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'tx_hash', tr.tx_hash,
        'height', tr.height,
        'timestamp', tr.timestamp,
        'direction', tr.direction,
        'sender', tr.details->>'sender',
        'receiver', tr.details->>'receiver',
        'source_channel', tr.details->>'source_channel',
        'token_denom', tr.details->>'token_denom',
        'token_amount', tr.details->>'token_amount',
        'resolved_denom', api.resolve_denom(tr.details->>'token_denom'),
        'counterparty_chain', (
          SELECT c.counterparty_chain_id
          FROM api.ibc_connections c
          WHERE c.channel_id = tr.details->>'source_channel' AND c.port_id = 'transfer'
        ),
        'success', tr.error IS NULL OR tr.error = ''
      ) ORDER BY tr.height DESC)
      FROM transfers tr
    ), '[]'::jsonb),
    'pagination', jsonb_build_object(
      'total', (SELECT count FROM total),
      'limit', _limit,
      'offset', _offset,
      'has_next', _offset + _limit < (SELECT count FROM total),
      'has_prev', _offset > 0
    )
  );
$$;

-- =============================================================================
-- GET IBC CHANNEL ACTIVITY
-- =============================================================================

CREATE OR REPLACE FUNCTION api.get_ibc_channel_activity()
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
  WITH channel_activity AS (
    SELECT
      COALESCE(
        m.metadata->>'sourceChannel',
        m.metadata->>'source_channel'
      ) as channel_id,
      COUNT(*) as transfer_count,
      COUNT(*) FILTER (WHERE t.error IS NULL OR t.error = '') as successful_transfers
    FROM api.messages_main m
    JOIN api.transactions_main t ON m.id = t.id
    WHERE m.type = '/ibc.applications.transfer.v1.MsgTransfer'
    AND (m.metadata->>'sourceChannel' IS NOT NULL OR m.metadata->>'source_channel' IS NOT NULL)
    GROUP BY COALESCE(m.metadata->>'sourceChannel', m.metadata->>'source_channel')
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
-- GET IBC VOLUME TIMESERIES (improved)
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
      date_trunc('hour', timestamp) AS hour_bucket,
      direction,
      details->>'source_channel' AS channel,
      COALESCE((details->>'token_amount')::numeric, 0) AS amount
    FROM ibc_messages
    WHERE _channel IS NULL OR details->>'source_channel' = _channel
  ),
  hourly_volume AS (
    SELECT
      hour_bucket,
      direction,
      COUNT(*) AS transfer_count,
      SUM(amount) AS total_amount
    FROM filtered
    WHERE hour_bucket IS NOT NULL
    GROUP BY hour_bucket, direction
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
      SELECT jsonb_agg(DISTINCT channel)
      FROM filtered
      WHERE channel IS NOT NULL
    ), '[]'::jsonb)
  );
$$;

-- =============================================================================
-- GRANT PERMISSIONS
-- =============================================================================

GRANT EXECUTE ON FUNCTION api.extract_ibc_transfer_details(api.messages_main) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_ibc_stats() TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_ibc_transfers(int, int, text) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_ibc_transfers_by_address(text, int, int) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_ibc_channel_activity() TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_ibc_volume_timeseries(int, text) TO web_anon;

COMMIT;
