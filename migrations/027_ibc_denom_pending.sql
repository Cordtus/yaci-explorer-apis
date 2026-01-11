-- =============================================================================
-- Migration 027: IBC Denom Pending Queue
-- Creates the pending queue table and trigger for on-demand IBC denom resolution
-- Used by chain-params-daemon.ts to resolve unknown IBC denoms
-- =============================================================================

BEGIN;

-- =============================================================================
-- IBC DENOM PENDING TABLE
-- Queue for IBC denoms that need resolution via gRPC
-- =============================================================================

CREATE TABLE IF NOT EXISTS api.ibc_denom_pending (
  ibc_denom TEXT PRIMARY KEY,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  last_attempt TIMESTAMP WITH TIME ZONE,
  attempts INT DEFAULT 0,
  error TEXT
);

CREATE INDEX IF NOT EXISTS idx_ibc_denom_pending_attempts
  ON api.ibc_denom_pending(attempts, last_attempt);

-- =============================================================================
-- NOTIFY FUNCTION
-- Sends notification when new IBC denom is queued for resolution
-- =============================================================================

CREATE OR REPLACE FUNCTION api.notify_ibc_denom_pending()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  PERFORM pg_notify('ibc_denom_pending', NEW.ibc_denom);
  RETURN NEW;
END;
$$;

-- Trigger to notify on new pending denoms
DROP TRIGGER IF EXISTS trg_ibc_denom_pending_notify ON api.ibc_denom_pending;
CREATE TRIGGER trg_ibc_denom_pending_notify
  AFTER INSERT ON api.ibc_denom_pending
  FOR EACH ROW
  EXECUTE FUNCTION api.notify_ibc_denom_pending();

-- =============================================================================
-- QUEUE UNKNOWN IBC DENOM FUNCTION
-- Called to queue an IBC denom for resolution if not already known
-- =============================================================================

CREATE OR REPLACE FUNCTION api.queue_unknown_ibc_denom(_ibc_denom TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
BEGIN
  -- Only queue if it looks like an IBC denom and isn't already resolved
  IF _ibc_denom IS NULL OR NOT _ibc_denom LIKE 'ibc/%' THEN
    RETURN FALSE;
  END IF;

  -- Check if already in denom_traces (resolved)
  IF EXISTS (SELECT 1 FROM api.ibc_denom_traces WHERE ibc_denom = _ibc_denom) THEN
    RETURN FALSE;
  END IF;

  -- Check if already in denom_metadata (resolved)
  IF EXISTS (SELECT 1 FROM api.denom_metadata WHERE denom = _ibc_denom AND ibc_source_denom IS NOT NULL) THEN
    RETURN FALSE;
  END IF;

  -- Insert into pending queue (ignore if already queued)
  INSERT INTO api.ibc_denom_pending (ibc_denom)
  VALUES (_ibc_denom)
  ON CONFLICT (ibc_denom) DO NOTHING;

  RETURN TRUE;
END;
$$;

-- =============================================================================
-- TRIGGER TO AUTO-QUEUE UNKNOWN IBC DENOMS FROM TRANSFERS
-- Extracts IBC denoms from MsgTransfer messages and queues for resolution
-- =============================================================================

CREATE OR REPLACE FUNCTION api.extract_and_queue_ibc_denoms()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  _denom TEXT;
  _token_denom TEXT;
BEGIN
  -- Only process IBC transfer messages
  IF NEW.type NOT IN (
    '/ibc.applications.transfer.v1.MsgTransfer',
    '/ibc.core.channel.v1.MsgRecvPacket'
  ) THEN
    RETURN NEW;
  END IF;

  -- Extract denom from metadata
  _token_denom := NEW.metadata->'token'->>'denom';

  -- For MsgRecvPacket, try to extract from packet data
  IF _token_denom IS NULL AND NEW.type = '/ibc.core.channel.v1.MsgRecvPacket' THEN
    -- Packet data might be in different locations
    _token_denom := NEW.metadata->'packet'->'data'->>'denom';
    IF _token_denom IS NULL THEN
      _token_denom := NEW.metadata->'packetData'->>'denom';
    END IF;
  END IF;

  -- Queue if it's an IBC denom
  IF _token_denom IS NOT NULL AND _token_denom LIKE 'ibc/%' THEN
    PERFORM api.queue_unknown_ibc_denom(_token_denom);
  END IF;

  RETURN NEW;
END;
$$;

-- Trigger on messages_main to auto-queue IBC denoms
DROP TRIGGER IF EXISTS trg_queue_ibc_denoms ON api.messages_main;
CREATE TRIGGER trg_queue_ibc_denoms
  AFTER INSERT ON api.messages_main
  FOR EACH ROW
  EXECUTE FUNCTION api.extract_and_queue_ibc_denoms();

-- =============================================================================
-- HELPER: RESOLVE DENOM (returns symbol/decimals for any denom)
-- Works for native, IBC, and EVM denoms
-- =============================================================================

CREATE OR REPLACE FUNCTION api.resolve_denom(_denom TEXT)
RETURNS JSONB
LANGUAGE sql
STABLE
AS $$
  SELECT COALESCE(
    -- First try denom_metadata (most comprehensive)
    (SELECT jsonb_build_object(
      'denom', denom,
      'symbol', symbol,
      'decimals', decimals,
      'is_native', is_native,
      'source_chain', ibc_source_chain,
      'source_denom', ibc_source_denom
    ) FROM api.denom_metadata WHERE denom = _denom),
    -- Then try ibc_denom_traces
    (SELECT jsonb_build_object(
      'denom', ibc_denom,
      'symbol', COALESCE(symbol, base_denom),
      'decimals', COALESCE(decimals, 6),
      'is_native', false,
      'source_chain', source_chain_id,
      'source_denom', base_denom
    ) FROM api.ibc_denom_traces WHERE ibc_denom = _denom),
    -- Fallback: parse common patterns
    CASE
      WHEN _denom LIKE 'u%' THEN jsonb_build_object(
        'denom', _denom,
        'symbol', UPPER(SUBSTRING(_denom FROM 2)),
        'decimals', 6,
        'is_native', true,
        'source_chain', NULL,
        'source_denom', NULL
      )
      WHEN _denom LIKE 'a%' THEN jsonb_build_object(
        'denom', _denom,
        'symbol', UPPER(SUBSTRING(_denom FROM 2)),
        'decimals', 18,
        'is_native', true,
        'source_chain', NULL,
        'source_denom', NULL
      )
      WHEN _denom LIKE 'ibc/%' THEN jsonb_build_object(
        'denom', _denom,
        'symbol', 'IBC/' || SUBSTRING(_denom FROM 5 FOR 8) || '...',
        'decimals', 6,
        'is_native', false,
        'source_chain', NULL,
        'source_denom', NULL
      )
      ELSE jsonb_build_object(
        'denom', _denom,
        'symbol', UPPER(_denom),
        'decimals', 6,
        'is_native', NULL,
        'source_chain', NULL,
        'source_denom', NULL
      )
    END
  );
$$;

-- =============================================================================
-- GRANT PERMISSIONS
-- =============================================================================

GRANT SELECT ON api.ibc_denom_pending TO web_anon;
GRANT EXECUTE ON FUNCTION api.queue_unknown_ibc_denom(text) TO web_anon;
GRANT EXECUTE ON FUNCTION api.resolve_denom(text) TO web_anon;

COMMIT;
