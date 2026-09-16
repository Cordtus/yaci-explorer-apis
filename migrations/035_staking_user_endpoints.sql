-- Migration 031: Staking user endpoints
-- Functions for querying delegator-specific staking data

BEGIN;

-- ============================================================================
-- Function: Get delegation history for a specific delegator
-- ============================================================================

CREATE OR REPLACE FUNCTION api.get_delegator_history(
  _delegator_address TEXT,
  _limit INT DEFAULT 50,
  _offset INT DEFAULT 0,
  _event_type TEXT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE sql STABLE
AS $$
  WITH filtered AS (
    SELECT
      de.*,
      v.moniker as validator_moniker
    FROM api.delegation_events de
    LEFT JOIN api.validators v ON de.validator_address = v.operator_address
    WHERE de.delegator_address = _delegator_address
    AND (_event_type IS NULL OR de.event_type = _event_type)
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
-- Function: Get aggregated delegations for a delegator
-- Returns current delegation status per validator (sum of delegate - undelegate)
-- ============================================================================

CREATE OR REPLACE FUNCTION api.get_delegator_delegations(
  _delegator_address TEXT
)
RETURNS JSONB
LANGUAGE sql STABLE
AS $$
  WITH delegation_totals AS (
    SELECT
      de.validator_address,
      v.moniker as validator_moniker,
      v.commission_rate,
      v.status as validator_status,
      v.jailed as validator_jailed,
      de.denom,
      SUM(
        CASE
          WHEN de.event_type IN ('DELEGATE', 'CREATE_VALIDATOR') THEN COALESCE(de.amount::NUMERIC, 0)
          WHEN de.event_type = 'UNDELEGATE' THEN -COALESCE(de.amount::NUMERIC, 0)
          WHEN de.event_type = 'REDELEGATE' THEN
            CASE
              WHEN de.validator_address = de.src_validator_address THEN -COALESCE(de.amount::NUMERIC, 0)
              ELSE COALESCE(de.amount::NUMERIC, 0)
            END
          ELSE 0
        END
      ) as total_delegated
    FROM api.delegation_events de
    LEFT JOIN api.validators v ON de.validator_address = v.operator_address
    WHERE de.delegator_address = _delegator_address
    GROUP BY de.validator_address, v.moniker, v.commission_rate, v.status, v.jailed, de.denom
    HAVING SUM(
      CASE
        WHEN de.event_type IN ('DELEGATE', 'CREATE_VALIDATOR') THEN COALESCE(de.amount::NUMERIC, 0)
        WHEN de.event_type = 'UNDELEGATE' THEN -COALESCE(de.amount::NUMERIC, 0)
        WHEN de.event_type = 'REDELEGATE' THEN
          CASE
            WHEN de.validator_address = de.src_validator_address THEN -COALESCE(de.amount::NUMERIC, 0)
            ELSE COALESCE(de.amount::NUMERIC, 0)
          END
        ELSE 0
      END
    ) > 0
  )
  SELECT jsonb_build_object(
    'delegations', COALESCE(jsonb_agg(to_jsonb(dt)), '[]'::jsonb),
    'total_staked', COALESCE((SELECT SUM(total_delegated) FROM delegation_totals), 0)::TEXT,
    'validator_count', (SELECT COUNT(*) FROM delegation_totals)
  )
  FROM delegation_totals dt;
$$;

-- ============================================================================
-- Function: Get delegation summary stats for a delegator
-- ============================================================================

CREATE OR REPLACE FUNCTION api.get_delegator_stats(
  _delegator_address TEXT
)
RETURNS JSONB
LANGUAGE sql STABLE
AS $$
  SELECT jsonb_build_object(
    'total_delegations', (
      SELECT COUNT(*) FROM api.delegation_events
      WHERE delegator_address = _delegator_address AND event_type = 'DELEGATE'
    ),
    'total_undelegations', (
      SELECT COUNT(*) FROM api.delegation_events
      WHERE delegator_address = _delegator_address AND event_type = 'UNDELEGATE'
    ),
    'total_redelegations', (
      SELECT COUNT(*) FROM api.delegation_events
      WHERE delegator_address = _delegator_address AND event_type = 'REDELEGATE'
    ),
    'first_delegation', (
      SELECT MIN(timestamp) FROM api.delegation_events
      WHERE delegator_address = _delegator_address
    ),
    'last_activity', (
      SELECT MAX(timestamp) FROM api.delegation_events
      WHERE delegator_address = _delegator_address
    ),
    'unique_validators', (
      SELECT COUNT(DISTINCT validator_address) FROM api.delegation_events
      WHERE delegator_address = _delegator_address AND event_type IN ('DELEGATE', 'CREATE_VALIDATOR')
    )
  );
$$;

-- ============================================================================
-- Function: Get delegation events by delegator for a specific validator
-- ============================================================================

CREATE OR REPLACE FUNCTION api.get_delegator_validator_history(
  _delegator_address TEXT,
  _validator_address TEXT,
  _limit INT DEFAULT 50,
  _offset INT DEFAULT 0
)
RETURNS JSONB
LANGUAGE sql STABLE
AS $$
  WITH filtered AS (
    SELECT *
    FROM api.delegation_events
    WHERE delegator_address = _delegator_address
    AND (validator_address = _validator_address OR src_validator_address = _validator_address)
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
-- Index for delegator lookups (if not exists)
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_delegation_events_delegator_validator
  ON api.delegation_events(delegator_address, validator_address);

CREATE INDEX IF NOT EXISTS idx_delegation_events_delegator_timestamp
  ON api.delegation_events(delegator_address, timestamp DESC);

-- ============================================================================
-- Grants
-- ============================================================================

GRANT EXECUTE ON FUNCTION api.get_delegator_history(TEXT, INT, INT, TEXT) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_delegator_delegations(TEXT) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_delegator_stats(TEXT) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_delegator_validator_history(TEXT, TEXT, INT, INT) TO web_anon;

COMMIT;
