BEGIN;

-- Migration 064: Fix voting power percentage calculation
--
-- Bug: get_validators_paginated and get_validator_detail calculate
-- voting_power_pct for ALL validators (including unbonded) against only the
-- bonded total. This causes the sum of all voting powers to exceed 100%,
-- and gives unbonded validators a non-zero (meaningless) percentage.
--
-- Fix: Only calculate voting_power_pct for BONDED validators. Unbonded,
-- unbonding, and jailed validators get 0%.
--
-- Also adds missing 'voting_power' sort option to get_validators_paginated.

-- ============================================================================
-- 1. Fix get_validators_paginated
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
        WHEN v.status = 'BOND_STATUS_BONDED' AND tb.total > 0 AND v.tokens IS NOT NULL
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
      CASE WHEN _sort_by = 'uptime' AND _sort_dir = 'desc' THEN signing_percentage END DESC NULLS LAST,
      CASE WHEN _sort_by = 'uptime' AND _sort_dir = 'asc' THEN signing_percentage END ASC NULLS LAST,
      CASE WHEN _sort_by = 'voting_power' AND _sort_dir = 'desc' THEN voting_power_pct END DESC NULLS LAST,
      CASE WHEN _sort_by = 'voting_power' AND _sort_dir = 'asc' THEN voting_power_pct END ASC NULLS LAST,
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

GRANT EXECUTE ON FUNCTION api.get_validators_paginated(INT, INT, TEXT, TEXT, TEXT, TEXT) TO web_anon;

-- ============================================================================
-- 2. Fix get_validator_detail
-- ============================================================================

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
      ipfs.ipfs_peer_id,
      ipfs.ipfs_multiaddrs,
      CASE
        WHEN v.status = 'BOND_STATUS_BONDED' AND tb.total > 0 AND v.tokens IS NOT NULL
        THEN ROUND((v.tokens / tb.total) * 100, 4)
        ELSE 0
      END AS voting_power_pct,
      COALESCE(dc.delegator_count, 0) AS delegator_count
    FROM api.validators v
    CROSS JOIN total_bonded tb
    LEFT JOIN api.mv_validator_delegator_counts dc
      ON dc.validator_address = v.operator_address
    LEFT JOIN api.validator_ipfs_addresses ipfs
      ON ipfs.validator_address = v.operator_address
    WHERE v.operator_address = _operator_address
  )
  SELECT to_jsonb(validator) FROM validator;
$$;

GRANT EXECUTE ON FUNCTION api.get_validator_detail(TEXT) TO web_anon;

COMMIT;
