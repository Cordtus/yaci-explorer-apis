-- Migration 046: Fix validator display issues
-- 1. Add hex_address column to validator_consensus_addresses + populate bech32 entries
-- 2. Fix get_validators_paginated to resolve consensus_address from mapping table
-- 3. Fix get_recent_validator_events: exact address match + richer data
-- 4. Fix get_network_overview to return max_validators for meaningful health metric
-- 5. Fix get_validator_events_summary: exact address match

BEGIN;

-- ============================================================================
-- 1. Add hex_address column and populate bech32 entries in mapping table
--    CometBFT finalize_block_events use bech32 (raivalcons1...),
--    block_signatures use base64, validators table uses uppercase hex.
--    We store all formats so JOINs work via exact match.
-- ============================================================================

ALTER TABLE api.validator_consensus_addresses
  ADD COLUMN IF NOT EXISTS hex_address TEXT;

-- Populate hex from base64 entries
UPDATE api.validator_consensus_addresses
SET hex_address = UPPER(encode(decode(consensus_address, 'base64'), 'hex'))
WHERE hex_address IS NULL
AND consensus_address ~ '^[A-Za-z0-9+/=]+$';

-- Create index on hex_address for fast lookups
CREATE INDEX IF NOT EXISTS idx_vca_hex_address
  ON api.validator_consensus_addresses(hex_address);

-- ============================================================================
-- 2. Fix get_validators_paginated: LEFT JOIN validator_consensus_addresses
--    so uptime lookup works even when validators.consensus_address is NULL
-- ============================================================================

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
      COALESCE(v.consensus_address, vca.consensus_address) AS resolved_consensus_address,
      ipfs.ipfs_peer_id,
      CASE
        WHEN tb.total > 0 AND v.tokens IS NOT NULL
        THEN ROUND((v.tokens / tb.total) * 100, 4)
        ELSE 0
      END AS voting_power_pct,
      COALESCE(dc.delegator_count, 0) AS delegator_count
    FROM api.validators v
    CROSS JOIN total_bonded tb
    LEFT JOIN LATERAL (
      SELECT vca_inner.consensus_address
      FROM api.validator_consensus_addresses vca_inner
      WHERE vca_inner.operator_address = v.operator_address
      LIMIT 1
    ) vca ON true
    LEFT JOIN api.validator_ipfs_addresses ipfs
      ON ipfs.validator_address = v.operator_address
    LEFT JOIN api.mv_validator_delegator_counts dc
      ON dc.validator_address = v.operator_address
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
    'data', COALESCE(jsonb_agg(
      to_jsonb(s) - 'consensus_address' || jsonb_build_object('consensus_address', s.resolved_consensus_address)
    ), '[]'::jsonb),
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

-- ============================================================================
-- 3. Fix get_recent_validator_events: exact address match + richer return data
--    Mapping table now contains bech32, base64, and hex entries for each validator.
--    DROP required because return type changes (added block_time, attributes)
-- ============================================================================

DROP FUNCTION IF EXISTS api.get_recent_validator_events(TEXT[], INTEGER, INTEGER);

CREATE OR REPLACE FUNCTION api.get_recent_validator_events(
  _event_types TEXT[] DEFAULT ARRAY['slash', 'liveness', 'jail'],
  _limit INTEGER DEFAULT 50,
  _offset INTEGER DEFAULT 0
)
RETURNS TABLE (
  height BIGINT,
  event_type TEXT,
  validator_address TEXT,
  operator_address TEXT,
  moniker TEXT,
  reason TEXT,
  power TEXT,
  created_at TIMESTAMPTZ,
  block_time TIMESTAMPTZ,
  attributes JSONB
) AS $$
BEGIN
  RETURN QUERY
  SELECT
    f.height,
    f.event_type,
    COALESCE(f.attributes->>'address', f.attributes->>'validator', '') as validator_address,
    v.operator_address,
    v.moniker,
    COALESCE(f.attributes->>'reason', '') as reason,
    COALESCE(f.attributes->>'power', '') as power,
    f.created_at,
    (b.data->'block'->'header'->>'time')::timestamptz as block_time,
    f.attributes
  FROM api.finalize_block_events f
  LEFT JOIN api.validator_consensus_addresses vca
    ON vca.consensus_address = COALESCE(f.attributes->>'address', f.attributes->>'validator', '')
  LEFT JOIN api.validators v ON v.operator_address = vca.operator_address
  LEFT JOIN api.blocks_raw b ON b.id = f.height
  WHERE f.event_type = ANY(_event_types)
  ORDER BY f.height DESC
  LIMIT _limit
  OFFSET _offset;
END;
$$ LANGUAGE plpgsql STABLE;

-- ============================================================================
-- 3. Fix get_network_overview: add max_validators for meaningful health metric
--    DROP required because return type changes (added max_validators column)
-- ============================================================================

DROP FUNCTION IF EXISTS api.get_network_overview();

CREATE OR REPLACE FUNCTION api.get_network_overview()
RETURNS TABLE (
  total_validators INTEGER,
  active_validators INTEGER,
  jailed_validators INTEGER,
  total_bonded_tokens NUMERIC,
  total_rewards_24h NUMERIC,
  total_commission_24h NUMERIC,
  avg_block_time NUMERIC,
  total_transactions BIGINT,
  unique_addresses BIGINT,
  max_validators INTEGER
) AS $$
BEGIN
  RETURN QUERY
  SELECT
    (SELECT COUNT(*)::INTEGER FROM api.validators) as total_validators,
    (SELECT COUNT(*)::INTEGER FROM api.validators WHERE status = 'BOND_STATUS_BONDED' AND NOT jailed) as active_validators,
    (SELECT COUNT(*)::INTEGER FROM api.validators WHERE jailed = TRUE) as jailed_validators,
    (SELECT COALESCE(SUM(tokens), 0) FROM api.validators WHERE status = 'BOND_STATUS_BONDED') as total_bonded_tokens,
    (
      SELECT COALESCE(SUM(rewards), 0)
      FROM api.validator_rewards vr
      JOIN api.blocks_raw b ON b.id = vr.height
      WHERE (b.data->'block'->'header'->>'time')::timestamptz > NOW() - INTERVAL '24 hours'
    ) as total_rewards_24h,
    (
      SELECT COALESCE(SUM(commission), 0)
      FROM api.validator_rewards vr
      JOIN api.blocks_raw b ON b.id = vr.height
      WHERE (b.data->'block'->'header'->>'time')::timestamptz > NOW() - INTERVAL '24 hours'
    ) as total_commission_24h,
    (
      SELECT COALESCE(AVG(
        EXTRACT(EPOCH FROM (
          (b1.data->'block'->'header'->>'time')::timestamptz -
          (b2.data->'block'->'header'->>'time')::timestamptz
        ))
      ), 6)
      FROM api.blocks_raw b1
      JOIN api.blocks_raw b2 ON b2.id = b1.id - 1
      WHERE b1.id > (SELECT MAX(id) - 100 FROM api.blocks_raw)
    ) as avg_block_time,
    (SELECT COUNT(*) FROM api.transactions_main) as total_transactions,
    (SELECT COUNT(DISTINCT sender) FROM api.messages_main WHERE sender IS NOT NULL) as unique_addresses,
    -- max_validators: count of bonded validators (the target active set size)
    (SELECT COUNT(*)::INTEGER FROM api.validators WHERE status = 'BOND_STATUS_BONDED') as max_validators;
END;
$$ LANGUAGE plpgsql STABLE;

-- ============================================================================
-- 5. Fix get_validator_events_summary: exact address match
-- ============================================================================

CREATE OR REPLACE FUNCTION api.get_validator_events_summary(
  _limit INTEGER DEFAULT 20
)
RETURNS TABLE (
  height BIGINT,
  event_type TEXT,
  validator_moniker TEXT,
  operator_address TEXT,
  details JSONB,
  block_time TIMESTAMPTZ
) AS $$
BEGIN
  RETURN QUERY
  SELECT
    f.height,
    f.event_type,
    v.moniker as validator_moniker,
    v.operator_address,
    f.attributes as details,
    (b.data->'block'->'header'->>'time')::timestamptz as block_time
  FROM api.finalize_block_events f
  LEFT JOIN api.validator_consensus_addresses vca
    ON vca.consensus_address = COALESCE(f.attributes->>'address', f.attributes->>'validator', '')
  LEFT JOIN api.validators v ON v.operator_address = vca.operator_address
  LEFT JOIN api.blocks_raw b ON b.id = f.height
  WHERE f.event_type IN ('slash', 'liveness', 'jail', 'rewards', 'commission')
  ORDER BY f.height DESC
  LIMIT _limit;
END;
$$ LANGUAGE plpgsql STABLE;

-- ============================================================================
-- Grants
-- ============================================================================

GRANT EXECUTE ON FUNCTION api.get_validators_paginated(INT, INT, TEXT, TEXT, TEXT, TEXT) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_recent_validator_events(TEXT[], INTEGER, INTEGER) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_network_overview() TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_validator_events_summary(INTEGER) TO web_anon;

COMMIT;
