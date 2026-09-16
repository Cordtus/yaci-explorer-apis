-- Migration 024: Validator delegation events support
-- Table, trigger, RPC functions, view, and grants for staking delegation tracking

BEGIN;

-- ============================================================================
-- Table: delegation_events
-- ============================================================================

CREATE TABLE IF NOT EXISTS api.delegation_events (
  id SERIAL PRIMARY KEY,
  event_type TEXT NOT NULL CHECK (event_type IN (
    'DELEGATE', 'UNDELEGATE', 'REDELEGATE', 'CREATE_VALIDATOR', 'EDIT_VALIDATOR'
  )),
  delegator_address TEXT,
  validator_address TEXT NOT NULL,
  src_validator_address TEXT,
  amount TEXT,
  denom TEXT,
  tx_hash TEXT NOT NULL,
  height BIGINT,
  timestamp TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================================
-- Indexes
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_delegation_events_validator ON api.delegation_events(validator_address);
CREATE INDEX IF NOT EXISTS idx_delegation_events_delegator ON api.delegation_events(delegator_address);
CREATE INDEX IF NOT EXISTS idx_delegation_events_type ON api.delegation_events(event_type);
CREATE INDEX IF NOT EXISTS idx_delegation_events_timestamp ON api.delegation_events(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_delegation_events_tx_hash ON api.delegation_events(tx_hash);

-- ============================================================================
-- Trigger function: detect staking messages
-- ============================================================================

CREATE OR REPLACE FUNCTION api.detect_staking_messages()
RETURNS TRIGGER AS $$
DECLARE
  msg_record RECORD;
  raw_data JSONB;
BEGIN
  FOR msg_record IN
    SELECT m.id, m.message_index, m.type, m.metadata, m.sender
    FROM api.messages_main m
    WHERE m.id = NEW.id
    AND (
      m.type LIKE '%MsgDelegate'
      OR m.type LIKE '%MsgUndelegate'
      OR m.type LIKE '%MsgBeginRedelegate'
      OR m.type LIKE '%MsgCreateValidator'
      OR m.type LIKE '%MsgEditValidator'
    )
  LOOP
    raw_data := NULL;
    SELECT data INTO raw_data
    FROM api.messages_raw
    WHERE id = msg_record.id AND message_index = msg_record.message_index;

    -- MsgDelegate
    IF msg_record.type LIKE '%MsgDelegate' AND msg_record.type NOT LIKE '%MsgBeginRedelegate' THEN
      INSERT INTO api.delegation_events (
        event_type, delegator_address, validator_address,
        amount, denom, tx_hash, height, timestamp
      ) VALUES (
        'DELEGATE',
        COALESCE(raw_data->>'delegatorAddress', msg_record.sender),
        COALESCE(raw_data->>'validatorAddress', ''),
        raw_data->'amount'->>'amount',
        raw_data->'amount'->>'denom',
        NEW.id, NEW.height, NEW.timestamp
      );

    -- MsgUndelegate
    ELSIF msg_record.type LIKE '%MsgUndelegate' THEN
      INSERT INTO api.delegation_events (
        event_type, delegator_address, validator_address,
        amount, denom, tx_hash, height, timestamp
      ) VALUES (
        'UNDELEGATE',
        COALESCE(raw_data->>'delegatorAddress', msg_record.sender),
        COALESCE(raw_data->>'validatorAddress', ''),
        raw_data->'amount'->>'amount',
        raw_data->'amount'->>'denom',
        NEW.id, NEW.height, NEW.timestamp
      );

    -- MsgBeginRedelegate
    ELSIF msg_record.type LIKE '%MsgBeginRedelegate' THEN
      INSERT INTO api.delegation_events (
        event_type, delegator_address, validator_address,
        src_validator_address, amount, denom,
        tx_hash, height, timestamp
      ) VALUES (
        'REDELEGATE',
        COALESCE(raw_data->>'delegatorAddress', msg_record.sender),
        COALESCE(raw_data->>'validatorDstAddress', ''),
        COALESCE(raw_data->>'validatorSrcAddress', ''),
        raw_data->'amount'->>'amount',
        raw_data->'amount'->>'denom',
        NEW.id, NEW.height, NEW.timestamp
      );

    -- MsgCreateValidator
    ELSIF msg_record.type LIKE '%MsgCreateValidator' THEN
      INSERT INTO api.delegation_events (
        event_type, delegator_address, validator_address,
        amount, denom, tx_hash, height, timestamp
      ) VALUES (
        'CREATE_VALIDATOR',
        COALESCE(raw_data->>'delegatorAddress', msg_record.sender),
        COALESCE(raw_data->>'validatorAddress', ''),
        raw_data->'value'->>'amount',
        raw_data->'value'->>'denom',
        NEW.id, NEW.height, NEW.timestamp
      );

      -- Upsert into validators table
      INSERT INTO api.validators (
        operator_address, moniker, identity, website, details,
        commission_rate, commission_max_rate, commission_max_change_rate,
        min_self_delegation, tokens, status,
        creation_height, first_seen_tx
      ) VALUES (
        COALESCE(raw_data->>'validatorAddress', ''),
        raw_data->'description'->>'moniker',
        raw_data->'description'->>'identity',
        raw_data->'description'->>'website',
        raw_data->'description'->>'details',
        NULLIF(raw_data->'commission'->'commissionRates'->>'rate', '')::NUMERIC,
        NULLIF(raw_data->'commission'->'commissionRates'->>'maxRate', '')::NUMERIC,
        NULLIF(raw_data->'commission'->'commissionRates'->>'maxChangeRate', '')::NUMERIC,
        NULLIF(raw_data->>'minSelfDelegation', '')::NUMERIC,
        NULLIF(raw_data->'value'->>'amount', '')::NUMERIC,
        'BOND_STATUS_BONDED',
        NEW.height,
        NEW.id
      )
      ON CONFLICT (operator_address) DO UPDATE SET
        moniker = COALESCE(EXCLUDED.moniker, api.validators.moniker),
        identity = COALESCE(EXCLUDED.identity, api.validators.identity),
        website = COALESCE(EXCLUDED.website, api.validators.website),
        details = COALESCE(EXCLUDED.details, api.validators.details),
        commission_rate = COALESCE(EXCLUDED.commission_rate, api.validators.commission_rate),
        commission_max_rate = COALESCE(EXCLUDED.commission_max_rate, api.validators.commission_max_rate),
        commission_max_change_rate = COALESCE(EXCLUDED.commission_max_change_rate, api.validators.commission_max_change_rate),
        min_self_delegation = COALESCE(EXCLUDED.min_self_delegation, api.validators.min_self_delegation),
        creation_height = COALESCE(api.validators.creation_height, EXCLUDED.creation_height),
        first_seen_tx = COALESCE(api.validators.first_seen_tx, EXCLUDED.first_seen_tx),
        updated_at = NOW();

    -- MsgEditValidator
    ELSIF msg_record.type LIKE '%MsgEditValidator' THEN
      INSERT INTO api.delegation_events (
        event_type, delegator_address, validator_address,
        tx_hash, height, timestamp
      ) VALUES (
        'EDIT_VALIDATOR',
        msg_record.sender,
        COALESCE(raw_data->>'validatorAddress', ''),
        NEW.id, NEW.height, NEW.timestamp
      );

      -- Update validators table
      UPDATE api.validators SET
        moniker = COALESCE(
          NULLIF(raw_data->'description'->>'moniker', '[do-not-modify]'),
          moniker
        ),
        identity = COALESCE(
          NULLIF(raw_data->'description'->>'identity', '[do-not-modify]'),
          identity
        ),
        website = COALESCE(
          NULLIF(raw_data->'description'->>'website', '[do-not-modify]'),
          website
        ),
        details = COALESCE(
          NULLIF(raw_data->'description'->>'details', '[do-not-modify]'),
          details
        ),
        commission_rate = COALESCE(
          NULLIF(raw_data->>'commissionRate', '')::NUMERIC,
          commission_rate
        ),
        min_self_delegation = COALESCE(
          NULLIF(raw_data->>'minSelfDelegation', '')::NUMERIC,
          min_self_delegation
        ),
        updated_at = NOW()
      WHERE operator_address = COALESCE(raw_data->>'validatorAddress', '');
    END IF;
  END LOOP;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger
DROP TRIGGER IF EXISTS trigger_detect_staking ON api.transactions_main;
CREATE TRIGGER trigger_detect_staking
  AFTER INSERT ON api.transactions_main
  FOR EACH ROW
  EXECUTE FUNCTION api.detect_staking_messages();

-- ============================================================================
-- SQL Functions (API endpoints)
-- ============================================================================

-- Paginated validators list with computed fields
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
      CASE
        WHEN tb.total > 0 AND v.tokens IS NOT NULL
        THEN ROUND((v.tokens / tb.total) * 100, 4)
        ELSE 0
      END AS voting_power_pct,
      (SELECT COUNT(DISTINCT delegator_address)
       FROM api.delegation_events de
       WHERE de.validator_address = v.operator_address
       AND de.event_type IN ('DELEGATE', 'CREATE_VALIDATOR')
      ) AS delegator_count
    FROM api.validators v
    CROSS JOIN total_bonded tb
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

-- Single validator detail
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
      CASE
        WHEN tb.total > 0 AND v.tokens IS NOT NULL
        THEN ROUND((v.tokens / tb.total) * 100, 4)
        ELSE 0
      END AS voting_power_pct,
      (SELECT COUNT(DISTINCT delegator_address)
       FROM api.delegation_events de
       WHERE de.validator_address = v.operator_address
       AND de.event_type IN ('DELEGATE', 'CREATE_VALIDATOR')
      ) AS delegator_count
    FROM api.validators v
    CROSS JOIN total_bonded tb
    WHERE v.operator_address = _operator_address
  )
  SELECT to_jsonb(validator) FROM validator;
$$;

-- Paginated delegation events for a validator
CREATE OR REPLACE FUNCTION api.get_delegation_events(
  _validator_address TEXT,
  _limit INT DEFAULT 20,
  _offset INT DEFAULT 0,
  _event_type TEXT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE sql STABLE
AS $$
  WITH filtered AS (
    SELECT *
    FROM api.delegation_events
    WHERE validator_address = _validator_address
    AND (_event_type IS NULL OR event_type = _event_type)
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
-- View: validator_stats
-- ============================================================================

DROP VIEW IF EXISTS api.validator_stats CASCADE;

CREATE OR REPLACE VIEW api.validator_stats AS
SELECT
  (SELECT COUNT(*) FROM api.validators) AS total_validators,
  (SELECT COUNT(*) FROM api.validators WHERE status = 'BOND_STATUS_BONDED') AS active_validators,
  (SELECT COUNT(*) FROM api.validators WHERE jailed = TRUE) AS jailed_validators,
  (SELECT COALESCE(SUM(tokens), 0) FROM api.validators WHERE status = 'BOND_STATUS_BONDED') AS total_bonded_tokens;

-- ============================================================================
-- Grants
-- ============================================================================

GRANT SELECT ON api.delegation_events TO web_anon;
GRANT SELECT ON api.validator_stats TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_validators_paginated(INT, INT, TEXT, TEXT, TEXT, TEXT) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_validator_detail(TEXT) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_delegation_events(TEXT, INT, INT, TEXT) TO web_anon;

COMMIT;
