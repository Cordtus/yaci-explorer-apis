-- Migration 022: Republic Reputation and Slashing Plus module support
-- Tables, triggers, and functions for validator IPFS addresses and slashing records

BEGIN;

-- ============================================================================
-- Tables
-- ============================================================================

CREATE TABLE IF NOT EXISTS api.validator_ipfs_addresses (
  validator_address TEXT PRIMARY KEY,
  ipfs_multiaddrs TEXT[],
  ipfs_peer_id TEXT,
  tx_hash TEXT NOT NULL,
  height BIGINT,
  timestamp TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS api.slashing_records (
  id SERIAL PRIMARY KEY,
  slashing_id BIGINT UNIQUE,
  validator_address TEXT NOT NULL,
  submitter TEXT NOT NULL,
  condition TEXT NOT NULL CHECK (condition IN (
    'COMPUTE_MISCONDUCT',
    'REPUTATION_DEGRADATION',
    'DELEGATED_COLLUSION'
  )),
  evidence_type TEXT,
  evidence_data JSONB,
  tx_hash TEXT NOT NULL,
  height BIGINT,
  timestamp TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================================
-- Indexes
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_slashing_validator ON api.slashing_records(validator_address);
CREATE INDEX IF NOT EXISTS idx_slashing_condition ON api.slashing_records(condition);
CREATE INDEX IF NOT EXISTS idx_slashing_time ON api.slashing_records(timestamp DESC);

-- ============================================================================
-- Trigger: detect reputation messages (MsgSetIPFSAddress)
-- ============================================================================

CREATE OR REPLACE FUNCTION api.detect_reputation_messages()
RETURNS TRIGGER AS $$
DECLARE
  msg_record RECORD;
  raw_data JSONB;
BEGIN
  FOR msg_record IN
    SELECT m.id, m.message_index, m.type, m.metadata, m.sender
    FROM api.messages_main m
    WHERE m.id = NEW.id
    AND m.type LIKE '%republic.reputation%'
  LOOP
    raw_data := NULL;
    SELECT data INTO raw_data
    FROM api.messages_raw
    WHERE id = msg_record.id AND message_index = msg_record.message_index;

    -- MsgSetIPFSAddress
    IF msg_record.type LIKE '%MsgSetIPFSAddress' AND raw_data IS NOT NULL THEN
      INSERT INTO api.validator_ipfs_addresses (
        validator_address, ipfs_multiaddrs, ipfs_peer_id,
        tx_hash, height, timestamp
      ) VALUES (
        COALESCE(raw_data->>'validatorAddress', msg_record.sender),
        CASE
          WHEN raw_data ? 'ipfsMultiaddrs' AND jsonb_typeof(raw_data->'ipfsMultiaddrs') = 'array'
          THEN ARRAY(SELECT jsonb_array_elements_text(raw_data->'ipfsMultiaddrs'))
          ELSE NULL
        END,
        raw_data->>'ipfsPeerId',
        NEW.id, NEW.height, NEW.timestamp
      )
      ON CONFLICT (validator_address) DO UPDATE SET
        ipfs_multiaddrs = EXCLUDED.ipfs_multiaddrs,
        ipfs_peer_id = COALESCE(EXCLUDED.ipfs_peer_id, api.validator_ipfs_addresses.ipfs_peer_id),
        tx_hash = EXCLUDED.tx_hash,
        height = EXCLUDED.height,
        timestamp = EXCLUDED.timestamp,
        updated_at = NOW();
    END IF;
  END LOOP;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Trigger: detect slashing messages
-- ============================================================================

CREATE OR REPLACE FUNCTION api.detect_slashing_messages()
RETURNS TRIGGER AS $$
DECLARE
  msg_record RECORD;
  raw_data JSONB;
  slash_condition TEXT;
  slash_id BIGINT;
BEGIN
  FOR msg_record IN
    SELECT m.id, m.message_index, m.type, m.metadata, m.sender
    FROM api.messages_main m
    WHERE m.id = NEW.id
    AND m.type LIKE '%republic.slashingplus%'
  LOOP
    raw_data := NULL;
    SELECT data INTO raw_data
    FROM api.messages_raw
    WHERE id = msg_record.id AND message_index = msg_record.message_index;

    IF raw_data IS NULL THEN
      CONTINUE;
    END IF;

    -- Determine condition from message type
    slash_condition := CASE
      WHEN msg_record.type LIKE '%ComputeMisconduct%' THEN 'COMPUTE_MISCONDUCT'
      WHEN msg_record.type LIKE '%ReputationDegradation%' THEN 'REPUTATION_DEGRADATION'
      WHEN msg_record.type LIKE '%DelegatedCollusion%' THEN 'DELEGATED_COLLUSION'
      ELSE NULL
    END;

    IF slash_condition IS NULL THEN
      CONTINUE;
    END IF;

    -- Extract slashing_id from events if available
    slash_id := NULL;
    SELECT (e.attr_value)::BIGINT INTO slash_id
    FROM api.events_main e
    WHERE e.id = NEW.id
    AND e.attr_key = 'slashing_id'
    LIMIT 1;

    INSERT INTO api.slashing_records (
      slashing_id, validator_address, submitter, condition,
      evidence_type, evidence_data,
      tx_hash, height, timestamp
    ) VALUES (
      slash_id,
      COALESCE(raw_data->>'validatorAddress', raw_data->'evidence'->>'validatorAddress', ''),
      COALESCE(raw_data->>'submitter', msg_record.sender),
      slash_condition,
      raw_data->'evidence'->>'@type',
      raw_data->'evidence',
      NEW.id, NEW.height, NEW.timestamp
    )
    ON CONFLICT (slashing_id) DO NOTHING;
  END LOOP;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create triggers
DROP TRIGGER IF EXISTS trigger_detect_reputation ON api.transactions_main;
CREATE TRIGGER trigger_detect_reputation
  AFTER INSERT ON api.transactions_main
  FOR EACH ROW
  EXECUTE FUNCTION api.detect_reputation_messages();

DROP TRIGGER IF EXISTS trigger_detect_slashing ON api.transactions_main;
CREATE TRIGGER trigger_detect_slashing
  AFTER INSERT ON api.transactions_main
  FOR EACH ROW
  EXECUTE FUNCTION api.detect_slashing_messages();

-- ============================================================================
-- SQL Functions (API endpoints)
-- ============================================================================

CREATE OR REPLACE FUNCTION api.get_slashing_records(
  _limit INT DEFAULT 20,
  _offset INT DEFAULT 0,
  _validator TEXT DEFAULT NULL,
  _condition TEXT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE sql STABLE
AS $$
  WITH filtered AS (
    SELECT *
    FROM api.slashing_records
    WHERE (_validator IS NULL OR validator_address = _validator)
    AND (_condition IS NULL OR condition = _condition)
  ),
  total AS (
    SELECT COUNT(*) AS cnt FROM filtered
  ),
  page AS (
    SELECT * FROM filtered
    ORDER BY timestamp DESC NULLS LAST
    LIMIT _limit OFFSET _offset
  )
  SELECT jsonb_build_object(
    'data', COALESCE(jsonb_agg(to_jsonb(p)), '[]'::jsonb),
    'pagination', jsonb_build_object(
      'total', (SELECT cnt FROM total),
      'limit', _limit,
      'offset', _offset,
      'has_next', _offset + _limit < (SELECT cnt FROM total),
      'has_prev', _offset > 0
    )
  )
  FROM page p;
$$;

-- ============================================================================
-- Grants
-- ============================================================================

GRANT SELECT ON api.validator_ipfs_addresses TO web_anon;
GRANT SELECT ON api.slashing_records TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_slashing_records(INT, INT, TEXT, TEXT) TO web_anon;

COMMIT;
