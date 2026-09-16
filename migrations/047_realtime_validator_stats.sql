-- Migration 047: Real-time validator stats via triggers
-- Adds signing/liveness columns to validators table and creates triggers
-- on validator_block_signatures and finalize_block_events so the validators
-- table is always up-to-date without expensive join queries.

BEGIN;

-- ============================================================================
-- 1. Add real-time stats columns to validators table
-- ============================================================================

ALTER TABLE api.validators
  ADD COLUMN IF NOT EXISTS signing_percentage NUMERIC DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS blocks_signed INTEGER DEFAULT 0,
  ADD COLUMN IF NOT EXISTS blocks_missed INTEGER DEFAULT 0,
  ADD COLUMN IF NOT EXISTS last_signed_height BIGINT,
  ADD COLUMN IF NOT EXISTS missed_blocks_counter INTEGER DEFAULT 0,
  ADD COLUMN IF NOT EXISTS last_jailed_height BIGINT,
  ADD COLUMN IF NOT EXISTS last_jailed_at TIMESTAMPTZ;

-- ============================================================================
-- 2. Trigger: update validator signing stats on block signature insert
--    Runs per-row on INSERT to validator_block_signatures.
--    Resolves consensus_address -> operator_address via mapping table,
--    then increments counters and recalculates percentage.
-- ============================================================================

CREATE OR REPLACE FUNCTION api.trg_update_validator_signing_stats()
RETURNS TRIGGER AS $$
DECLARE
  op_addr TEXT;
BEGIN
  -- Resolve consensus address to operator address
  SELECT vca.operator_address INTO op_addr
  FROM api.validator_consensus_addresses vca
  WHERE vca.consensus_address = NEW.consensus_address
    OR vca.hex_address = UPPER(NEW.consensus_address)
  LIMIT 1;

  IF op_addr IS NULL THEN
    RETURN NEW;
  END IF;

  UPDATE api.validators
  SET
    blocks_signed = CASE WHEN NEW.signed THEN COALESCE(blocks_signed, 0) + 1 ELSE blocks_signed END,
    blocks_missed = CASE WHEN NOT NEW.signed THEN COALESCE(blocks_missed, 0) + 1 ELSE blocks_missed END,
    signing_percentage = ROUND(
      (CASE WHEN NEW.signed THEN COALESCE(blocks_signed, 0) + 1 ELSE COALESCE(blocks_signed, 0) END)::NUMERIC
      / GREATEST(COALESCE(blocks_signed, 0) + COALESCE(blocks_missed, 0) + 1, 1)::NUMERIC
      * 100, 2
    ),
    last_signed_height = CASE WHEN NEW.signed THEN NEW.height ELSE last_signed_height END,
    updated_at = NOW()
  WHERE operator_address = op_addr;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_vbs_update_validator_stats ON api.validator_block_signatures;
CREATE TRIGGER trg_vbs_update_validator_stats
  AFTER INSERT ON api.validator_block_signatures
  FOR EACH ROW
  EXECUTE FUNCTION api.trg_update_validator_signing_stats();

-- ============================================================================
-- 3. Trigger: update validator liveness on finalize_block_events insert
--    Handles slash, liveness, and jail events to update jailing metadata.
-- ============================================================================

CREATE OR REPLACE FUNCTION api.trg_update_validator_liveness()
RETURNS TRIGGER AS $$
DECLARE
  addr TEXT;
  op_addr TEXT;
BEGIN
  IF NEW.event_type NOT IN ('slash', 'liveness', 'jail') THEN
    RETURN NEW;
  END IF;

  addr := COALESCE(NEW.attributes->>'address', NEW.attributes->>'validator', '');
  IF addr = '' THEN
    RETURN NEW;
  END IF;

  -- Resolve to operator address (exact match - table has bech32, base64, hex entries)
  SELECT vca.operator_address INTO op_addr
  FROM api.validator_consensus_addresses vca
  WHERE vca.consensus_address = addr
  LIMIT 1;

  IF op_addr IS NULL THEN
    RETURN NEW;
  END IF;

  UPDATE api.validators
  SET
    missed_blocks_counter = COALESCE(
      NULLIF(NEW.attributes->>'missed_blocks', '')::INTEGER,
      NULLIF(NEW.attributes->>'missed_blocks_counter', '')::INTEGER,
      missed_blocks_counter
    ),
    last_jailed_height = CASE
      WHEN NEW.event_type = 'jail' THEN NEW.height
      ELSE last_jailed_height
    END,
    last_jailed_at = CASE
      WHEN NEW.event_type = 'jail' THEN NOW()
      ELSE last_jailed_at
    END,
    updated_at = NOW()
  WHERE operator_address = op_addr;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_fbe_update_validator_liveness ON api.finalize_block_events;
CREATE TRIGGER trg_fbe_update_validator_liveness
  AFTER INSERT ON api.finalize_block_events
  FOR EACH ROW
  EXECUTE FUNCTION api.trg_update_validator_liveness();

-- ============================================================================
-- 4. Backfill: populate signing stats from existing block signatures
--    Uses a single aggregate query to compute totals per validator.
-- ============================================================================

WITH signing_agg AS (
  SELECT
    vca.operator_address,
    COUNT(*) FILTER (WHERE vbs.signed)::INTEGER AS total_signed,
    COUNT(*) FILTER (WHERE NOT vbs.signed)::INTEGER AS total_missed,
    MAX(CASE WHEN vbs.signed THEN vbs.height END) AS max_signed_height
  FROM api.validator_block_signatures vbs
  JOIN api.validator_consensus_addresses vca
    ON vca.consensus_address = vbs.consensus_address
      OR vca.hex_address = UPPER(vbs.consensus_address)
  GROUP BY vca.operator_address
)
UPDATE api.validators v
SET
  blocks_signed = sa.total_signed,
  blocks_missed = sa.total_missed,
  signing_percentage = CASE
    WHEN sa.total_signed + sa.total_missed > 0
    THEN ROUND(sa.total_signed::NUMERIC / (sa.total_signed + sa.total_missed)::NUMERIC * 100, 2)
    ELSE 100
  END,
  last_signed_height = sa.max_signed_height
FROM signing_agg sa
WHERE v.operator_address = sa.operator_address;

-- ============================================================================
-- 5. Backfill: populate liveness stats from existing finalize_block_events
-- ============================================================================

WITH liveness_agg AS (
  SELECT
    vca.operator_address,
    MAX(CASE WHEN f.event_type = 'jail' THEN f.height END) AS max_jail_height,
    MAX(CASE WHEN f.event_type = 'jail' THEN f.created_at END) AS max_jail_at,
    GREATEST(
      MAX(NULLIF(f.attributes->>'missed_blocks', '')::INTEGER),
      MAX(NULLIF(f.attributes->>'missed_blocks_counter', '')::INTEGER)
    ) AS max_missed_counter
  FROM api.finalize_block_events f
  JOIN api.validator_consensus_addresses vca
    ON vca.consensus_address = COALESCE(f.attributes->>'address', f.attributes->>'validator', '')
  WHERE f.event_type IN ('slash', 'liveness', 'jail')
  GROUP BY vca.operator_address
)
UPDATE api.validators v
SET
  last_jailed_height = la.max_jail_height,
  last_jailed_at = la.max_jail_at,
  missed_blocks_counter = COALESCE(la.max_missed_counter, 0)
FROM liveness_agg la
WHERE v.operator_address = la.operator_address;

-- ============================================================================
-- 6. Update get_validators_paginated to expose new columns
--    The new signing columns come through v.* automatically, but we need to
--    ensure resolved_consensus_address override and sorting by uptime works.
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
      CASE WHEN _sort_by = 'uptime' AND _sort_dir = 'desc' THEN signing_percentage END DESC NULLS LAST,
      CASE WHEN _sort_by = 'uptime' AND _sort_dir = 'asc' THEN signing_percentage END ASC NULLS LAST,
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
-- Grants
-- ============================================================================

GRANT EXECUTE ON FUNCTION api.get_validators_paginated(INT, INT, TEXT, TEXT, TEXT, TEXT) TO web_anon;

COMMIT;
