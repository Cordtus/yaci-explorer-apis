-- =============================================================================
-- Manifest Indexer IBC Functions
-- Migration 023: IBC tables and RPC functions
-- Safe to run on existing database with populated data
-- =============================================================================

BEGIN;

-- =============================================================================
-- IBC TABLES (create if not exist)
-- =============================================================================

-- IBC connections (comprehensive info from gRPC queries)
CREATE TABLE IF NOT EXISTS api.ibc_connections (
  channel_id TEXT NOT NULL,
  port_id TEXT NOT NULL,
  connection_id TEXT,
  client_id TEXT,
  counterparty_chain_id TEXT,
  counterparty_channel_id TEXT,
  counterparty_port_id TEXT,
  counterparty_client_id TEXT,
  counterparty_connection_id TEXT,
  state TEXT,
  ordering TEXT,
  version TEXT,
  client_status TEXT,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  PRIMARY KEY (channel_id, port_id)
);

CREATE INDEX IF NOT EXISTS idx_ibc_connections_chain ON api.ibc_connections(counterparty_chain_id);
CREATE INDEX IF NOT EXISTS idx_ibc_connections_state ON api.ibc_connections(state);

-- IBC denom traces (resolved from ibc/HASH to base denom)
CREATE TABLE IF NOT EXISTS api.ibc_denom_traces (
  ibc_denom TEXT PRIMARY KEY,
  base_denom TEXT NOT NULL,
  path TEXT NOT NULL,
  source_channel TEXT,
  source_chain_id TEXT,
  symbol TEXT,
  decimals INT DEFAULT 6,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ibc_denom_base ON api.ibc_denom_traces(base_denom);
CREATE INDEX IF NOT EXISTS idx_ibc_denom_channel ON api.ibc_denom_traces(source_channel);

-- Legacy IBC channels table (for backward compatibility)
CREATE TABLE IF NOT EXISTS api.ibc_channels (
  channel_id TEXT NOT NULL,
  port_id TEXT NOT NULL,
  counterparty_channel_id TEXT,
  counterparty_port_id TEXT,
  connection_id TEXT,
  state TEXT,
  ordering TEXT,
  version TEXT,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  PRIMARY KEY (channel_id, port_id)
);

-- =============================================================================
-- IBC RPC FUNCTIONS
-- =============================================================================

-- Get IBC connections with full route info
CREATE OR REPLACE FUNCTION api.get_ibc_connections(
  _limit INT DEFAULT 50,
  _offset INT DEFAULT 0,
  _chain_id TEXT DEFAULT NULL,
  _state TEXT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE SQL STABLE
AS $$
  WITH filtered AS (
    SELECT *
    FROM api.ibc_connections
    WHERE (_chain_id IS NULL OR counterparty_chain_id = _chain_id)
      AND (_state IS NULL OR state = _state)
    ORDER BY counterparty_chain_id, channel_id
    LIMIT _limit OFFSET _offset
  ),
  total AS (
    SELECT COUNT(*) AS count
    FROM api.ibc_connections
    WHERE (_chain_id IS NULL OR counterparty_chain_id = _chain_id)
      AND (_state IS NULL OR state = _state)
  )
  SELECT jsonb_build_object(
    'data', COALESCE(jsonb_agg(
      jsonb_build_object(
        'channel_id', f.channel_id,
        'port_id', f.port_id,
        'connection_id', f.connection_id,
        'client_id', f.client_id,
        'counterparty_chain_id', f.counterparty_chain_id,
        'counterparty_channel_id', f.counterparty_channel_id,
        'counterparty_port_id', f.counterparty_port_id,
        'counterparty_client_id', f.counterparty_client_id,
        'counterparty_connection_id', f.counterparty_connection_id,
        'state', f.state,
        'ordering', f.ordering,
        'client_status', f.client_status,
        'is_active', f.state = 'STATE_OPEN' AND f.client_status = 'Active',
        'updated_at', f.updated_at
      )
      ORDER BY f.counterparty_chain_id, f.channel_id
    ), '[]'::jsonb),
    'pagination', jsonb_build_object(
      'total', (SELECT count FROM total),
      'limit', _limit,
      'offset', _offset,
      'has_next', _offset + _limit < (SELECT count FROM total),
      'has_prev', _offset > 0
    )
  )
  FROM filtered f;
$$;

-- Get IBC connection by channel
CREATE OR REPLACE FUNCTION api.get_ibc_connection(_channel_id TEXT, _port_id TEXT DEFAULT 'transfer')
RETURNS JSONB
LANGUAGE SQL STABLE
AS $$
  SELECT jsonb_build_object(
    'channel_id', channel_id,
    'port_id', port_id,
    'connection_id', connection_id,
    'client_id', client_id,
    'counterparty_chain_id', counterparty_chain_id,
    'counterparty_channel_id', counterparty_channel_id,
    'counterparty_port_id', counterparty_port_id,
    'counterparty_client_id', counterparty_client_id,
    'counterparty_connection_id', counterparty_connection_id,
    'state', state,
    'ordering', ordering,
    'client_status', client_status,
    'is_active', state = 'STATE_OPEN' AND client_status = 'Active',
    'updated_at', updated_at
  )
  FROM api.ibc_connections
  WHERE channel_id = _channel_id AND port_id = _port_id;
$$;

-- Get IBC denom traces
CREATE OR REPLACE FUNCTION api.get_ibc_denom_traces(
  _limit INT DEFAULT 50,
  _offset INT DEFAULT 0,
  _base_denom TEXT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE SQL STABLE
AS $$
  WITH filtered AS (
    SELECT t.*, c.counterparty_chain_id
    FROM api.ibc_denom_traces t
    LEFT JOIN api.ibc_connections c ON t.source_channel = c.channel_id AND c.port_id = 'transfer'
    WHERE (_base_denom IS NULL OR t.base_denom ILIKE '%' || _base_denom || '%')
    ORDER BY t.base_denom, t.ibc_denom
    LIMIT _limit OFFSET _offset
  ),
  total AS (
    SELECT COUNT(*) AS count
    FROM api.ibc_denom_traces
    WHERE (_base_denom IS NULL OR base_denom ILIKE '%' || _base_denom || '%')
  )
  SELECT jsonb_build_object(
    'data', COALESCE(jsonb_agg(
      jsonb_build_object(
        'ibc_denom', f.ibc_denom,
        'base_denom', f.base_denom,
        'path', f.path,
        'source_channel', f.source_channel,
        'source_chain_id', COALESCE(f.source_chain_id, f.counterparty_chain_id),
        'symbol', f.symbol,
        'decimals', f.decimals,
        'updated_at', f.updated_at
      )
      ORDER BY f.base_denom, f.ibc_denom
    ), '[]'::jsonb),
    'pagination', jsonb_build_object(
      'total', (SELECT count FROM total),
      'limit', _limit,
      'offset', _offset,
      'has_next', _offset + _limit < (SELECT count FROM total),
      'has_prev', _offset > 0
    )
  )
  FROM filtered f;
$$;

-- Resolve IBC denom to base denom and source info
CREATE OR REPLACE FUNCTION api.resolve_ibc_denom(_ibc_denom TEXT)
RETURNS JSONB
LANGUAGE SQL STABLE
AS $$
  SELECT jsonb_build_object(
    'ibc_denom', t.ibc_denom,
    'base_denom', t.base_denom,
    'path', t.path,
    'source_channel', t.source_channel,
    'source_chain_id', COALESCE(t.source_chain_id, c.counterparty_chain_id),
    'symbol', t.symbol,
    'decimals', t.decimals,
    'route', jsonb_build_object(
      'channel_id', c.channel_id,
      'connection_id', c.connection_id,
      'client_id', c.client_id,
      'counterparty_channel_id', c.counterparty_channel_id,
      'counterparty_connection_id', c.counterparty_connection_id,
      'counterparty_client_id', c.counterparty_client_id
    )
  )
  FROM api.ibc_denom_traces t
  LEFT JOIN api.ibc_connections c ON t.source_channel = c.channel_id AND c.port_id = 'transfer'
  WHERE t.ibc_denom = _ibc_denom;
$$;

-- Get unique counterparty chains
CREATE OR REPLACE FUNCTION api.get_ibc_chains()
RETURNS JSONB
LANGUAGE SQL STABLE
AS $$
  SELECT COALESCE(jsonb_agg(
    jsonb_build_object(
      'chain_id', counterparty_chain_id,
      'channel_count', channel_count,
      'open_channels', open_channels,
      'active_channels', active_channels
    )
    ORDER BY counterparty_chain_id
  ), '[]'::jsonb)
  FROM (
    SELECT
      counterparty_chain_id,
      COUNT(*) AS channel_count,
      COUNT(*) FILTER (WHERE state = 'STATE_OPEN') AS open_channels,
      COUNT(*) FILTER (WHERE state = 'STATE_OPEN' AND client_status = 'Active') AS active_channels
    FROM api.ibc_connections
    WHERE counterparty_chain_id IS NOT NULL
    GROUP BY counterparty_chain_id
  ) chains;
$$;

-- =============================================================================
-- GRANT PERMISSIONS
-- =============================================================================

GRANT SELECT ON api.ibc_connections TO web_anon;
GRANT SELECT ON api.ibc_denom_traces TO web_anon;
GRANT SELECT ON api.ibc_channels TO web_anon;

GRANT EXECUTE ON FUNCTION api.get_ibc_connections(int, int, text, text) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_ibc_connection(text, text) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_ibc_denom_traces(int, int, text) TO web_anon;
GRANT EXECUTE ON FUNCTION api.resolve_ibc_denom(text) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_ibc_chains() TO web_anon;

COMMIT;
