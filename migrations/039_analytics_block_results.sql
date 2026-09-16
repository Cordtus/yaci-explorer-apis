-- Migration 039: Enhanced Analytics from Block Results
--
-- Adds comprehensive analytics views and functions for:
-- - Validator rewards and commission tracking
-- - Network-wide metrics aggregation
-- - Time-series data for charts
-- - Validator leaderboard and performance metrics

BEGIN;

-- ============================================================================
-- Table: validator_rewards - Track rewards and commission per block
-- ============================================================================

CREATE TABLE IF NOT EXISTS api.validator_rewards (
  id SERIAL PRIMARY KEY,
  height BIGINT NOT NULL,
  validator_address TEXT NOT NULL,
  rewards NUMERIC(78, 18) DEFAULT 0,
  commission NUMERIC(78, 18) DEFAULT 0,
  created_at TIMESTAMPTZ DEFAULT NOW(),

  UNIQUE(height, validator_address)
);

CREATE INDEX IF NOT EXISTS idx_validator_rewards_height ON api.validator_rewards(height DESC);
CREATE INDEX IF NOT EXISTS idx_validator_rewards_validator ON api.validator_rewards(validator_address);
CREATE INDEX IF NOT EXISTS idx_validator_rewards_validator_height ON api.validator_rewards(validator_address, height DESC);

-- ============================================================================
-- Table: block_metrics - Per-block network metrics
-- ============================================================================

CREATE TABLE IF NOT EXISTS api.block_metrics (
  height BIGINT PRIMARY KEY,
  block_time TIMESTAMPTZ,
  tx_count INTEGER DEFAULT 0,
  gas_used BIGINT DEFAULT 0,
  total_rewards NUMERIC(78, 18) DEFAULT 0,
  total_commission NUMERIC(78, 18) DEFAULT 0,
  validator_count INTEGER DEFAULT 0,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_block_metrics_time ON api.block_metrics(block_time DESC);

-- ============================================================================
-- Function: Extract rewards from finalize_block_events
-- ============================================================================

CREATE OR REPLACE FUNCTION api.extract_rewards_from_events()
RETURNS TRIGGER AS $$
DECLARE
  events JSONB;
  event_item JSONB;
  event_type TEXT;
  attrs JSONB;
  attr_item JSONB;
  validator_addr TEXT;
  reward_amount NUMERIC;
  commission_amount NUMERIC;
BEGIN
  -- Get finalize_block_events array
  events := COALESCE(
    NEW.data->'finalizeBlockEvents',
    NEW.data->'finalize_block_events',
    '[]'::JSONB
  );

  IF jsonb_array_length(events) = 0 THEN
    RETURN NEW;
  END IF;

  FOR event_item IN SELECT * FROM jsonb_array_elements(events)
  LOOP
    event_type := event_item->>'type';

    -- Build attributes as key-value object
    attrs := '{}';
    FOR attr_item IN SELECT * FROM jsonb_array_elements(COALESCE(event_item->'attributes', '[]'::JSONB))
    LOOP
      attrs := attrs || jsonb_build_object(
        COALESCE(attr_item->>'key', ''),
        COALESCE(attr_item->>'value', '')
      );
    END LOOP;

    -- Handle rewards events
    IF event_type = 'rewards' THEN
      validator_addr := COALESCE(attrs->>'validator', '');
      reward_amount := COALESCE(
        NULLIF(regexp_replace(attrs->>'amount', '[^0-9.]', '', 'g'), '')::NUMERIC,
        0
      ) / 1e18;

      IF validator_addr != '' THEN
        INSERT INTO api.validator_rewards (height, validator_address, rewards)
        VALUES (NEW.height, validator_addr, reward_amount)
        ON CONFLICT (height, validator_address)
        DO UPDATE SET rewards = EXCLUDED.rewards + api.validator_rewards.rewards;
      END IF;
    END IF;

    -- Handle commission events
    IF event_type = 'commission' THEN
      validator_addr := COALESCE(attrs->>'validator', '');
      commission_amount := COALESCE(
        NULLIF(regexp_replace(attrs->>'amount', '[^0-9.]', '', 'g'), '')::NUMERIC,
        0
      ) / 1e18;

      IF validator_addr != '' THEN
        INSERT INTO api.validator_rewards (height, validator_address, commission)
        VALUES (NEW.height, validator_addr, commission_amount)
        ON CONFLICT (height, validator_address)
        DO UPDATE SET commission = EXCLUDED.commission + api.validator_rewards.commission;
      END IF;
    END IF;
  END LOOP;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for rewards extraction (only if table exists)
-- Note: block_results_raw is created by migration 041; trigger will be recreated there
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'api' AND table_name = 'block_results_raw') THEN
    DROP TRIGGER IF EXISTS trigger_extract_rewards ON api.block_results_raw;
    CREATE TRIGGER trigger_extract_rewards
      AFTER INSERT ON api.block_results_raw
      FOR EACH ROW
      EXECUTE FUNCTION api.extract_rewards_from_events();
  ELSE
    RAISE NOTICE 'Skipping trigger creation: api.block_results_raw does not exist yet (will be created in migration 041)';
  END IF;
END $$;

-- ============================================================================
-- Function: Get validator rewards history
-- ============================================================================

CREATE OR REPLACE FUNCTION api.get_validator_rewards_history(
  _operator_address TEXT,
  _limit INTEGER DEFAULT 100,
  _offset INTEGER DEFAULT 0
)
RETURNS TABLE (
  height BIGINT,
  rewards NUMERIC,
  commission NUMERIC,
  block_time TIMESTAMPTZ
) AS $$
BEGIN
  RETURN QUERY
  SELECT
    vr.height,
    vr.rewards,
    vr.commission,
    b.block_time
  FROM api.validator_rewards vr
  LEFT JOIN api.block_metrics b ON b.height = vr.height
  WHERE vr.validator_address IN (
    SELECT consensus_address
    FROM api.validator_consensus_addresses
    WHERE operator_address = _operator_address
    UNION
    SELECT _operator_address
  )
  ORDER BY vr.height DESC
  LIMIT _limit
  OFFSET _offset;
END;
$$ LANGUAGE plpgsql STABLE;

-- ============================================================================
-- Function: Get validator total rewards
-- ============================================================================

CREATE OR REPLACE FUNCTION api.get_validator_total_rewards(
  _operator_address TEXT
)
RETURNS TABLE (
  total_rewards NUMERIC,
  total_commission NUMERIC,
  blocks_with_rewards INTEGER
) AS $$
BEGIN
  RETURN QUERY
  SELECT
    COALESCE(SUM(vr.rewards), 0) as total_rewards,
    COALESCE(SUM(vr.commission), 0) as total_commission,
    COUNT(DISTINCT vr.height)::INTEGER as blocks_with_rewards
  FROM api.validator_rewards vr
  WHERE vr.validator_address IN (
    SELECT consensus_address
    FROM api.validator_consensus_addresses
    WHERE operator_address = _operator_address
    UNION
    SELECT _operator_address
  );
END;
$$ LANGUAGE plpgsql STABLE;

-- ============================================================================
-- Materialized View: Daily rewards aggregation
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS api.mv_daily_rewards AS
SELECT
  date_trunc('day', (b.data->'block'->'header'->>'time')::timestamptz)::date AS date,
  SUM(vr.rewards) AS total_rewards,
  SUM(vr.commission) AS total_commission,
  COUNT(DISTINCT vr.validator_address) AS validators_earning
FROM api.validator_rewards vr
JOIN api.blocks_raw b ON b.id = vr.height
WHERE vr.rewards > 0 OR vr.commission > 0
GROUP BY date_trunc('day', (b.data->'block'->'header'->>'time')::timestamptz)::date;

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname = 'api' AND matviewname = 'mv_daily_rewards') THEN
    EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS mv_daily_rewards_date_idx ON api.mv_daily_rewards(date)';
  END IF;
END $$;

-- ============================================================================
-- Materialized View: Validator leaderboard
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS api.mv_validator_leaderboard AS
SELECT
  v.operator_address,
  v.moniker,
  v.tokens,
  v.commission_rate,
  v.jailed,
  COALESCE(d.delegator_count, 0) AS delegator_count,
  COALESCE(r.total_rewards, 0) AS lifetime_rewards,
  COALESCE(r.total_commission, 0) AS lifetime_commission,
  COALESCE(j.jail_count, 0) AS jail_count,
  COALESCE(j.last_jailed_height, 0) AS last_jailed_height
FROM api.validators v
LEFT JOIN api.mv_validator_delegator_counts d ON d.validator_address = v.operator_address
LEFT JOIN (
  SELECT
    vca.operator_address,
    SUM(vr.rewards) AS total_rewards,
    SUM(vr.commission) AS total_commission
  FROM api.validator_rewards vr
  JOIN api.validator_consensus_addresses vca ON vca.consensus_address = vr.validator_address
  GROUP BY vca.operator_address
) r ON r.operator_address = v.operator_address
LEFT JOIN (
  SELECT
    j.operator_address,
    COUNT(*) AS jail_count,
    MAX(j.height) AS last_jailed_height
  FROM api.jailing_events j
  GROUP BY j.operator_address
) j ON j.operator_address = v.operator_address
WHERE v.status = 'BOND_STATUS_BONDED';

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname = 'api' AND matviewname = 'mv_validator_leaderboard') THEN
    EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS mv_validator_leaderboard_operator_idx ON api.mv_validator_leaderboard(operator_address)';
  END IF;
END $$;

-- ============================================================================
-- Function: Get network overview stats
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
  unique_addresses BIGINT
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
    (SELECT COUNT(DISTINCT sender) FROM api.messages_main WHERE sender IS NOT NULL) as unique_addresses;
END;
$$ LANGUAGE plpgsql STABLE;

-- ============================================================================
-- Function: Get hourly rewards chart data
-- ============================================================================

DROP FUNCTION IF EXISTS api.get_hourly_rewards(INTEGER);

CREATE OR REPLACE FUNCTION api.get_hourly_rewards(
  _hours INTEGER DEFAULT 24
)
RETURNS TABLE (
  hour TIMESTAMPTZ,
  rewards NUMERIC,
  commission NUMERIC
) AS $$
BEGIN
  RETURN QUERY
  SELECT
    date_trunc('hour', (b.data->'block'->'header'->>'time')::timestamptz) AS hour,
    COALESCE(SUM(vr.rewards), 0) AS rewards,
    COALESCE(SUM(vr.commission), 0) AS commission
  FROM api.validator_rewards vr
  JOIN api.blocks_raw b ON b.id = vr.height
  WHERE (b.data->'block'->'header'->>'time')::timestamptz > NOW() - (_hours || ' hours')::INTERVAL
  GROUP BY date_trunc('hour', (b.data->'block'->'header'->>'time')::timestamptz)
  ORDER BY hour DESC;
END;
$$ LANGUAGE plpgsql STABLE;

-- ============================================================================
-- Function: Get validator performance metrics
-- ============================================================================

CREATE OR REPLACE FUNCTION api.get_validator_performance(
  _operator_address TEXT
)
RETURNS TABLE (
  uptime_percentage NUMERIC,
  blocks_signed INTEGER,
  blocks_missed INTEGER,
  total_jailing_events INTEGER,
  last_jailed_height BIGINT,
  rewards_rank INTEGER,
  delegation_rank INTEGER
) AS $$
DECLARE
  consensus_addr TEXT;
  total_blocks INTEGER;
BEGIN
  -- Get consensus address
  SELECT consensus_address INTO consensus_addr
  FROM api.validator_consensus_addresses
  WHERE operator_address = _operator_address
  LIMIT 1;

  -- Get total blocks in recent window
  SELECT COUNT(*)::INTEGER INTO total_blocks
  FROM api.blocks_raw
  WHERE id > (SELECT MAX(id) - 1000 FROM api.blocks_raw);

  RETURN QUERY
  SELECT
    -- Uptime based on jailing events
    CASE
      WHEN total_blocks > 0 THEN
        GREATEST(0, 100 - (COALESCE(
          (SELECT COUNT(*) FROM api.jailing_events j
           WHERE j.operator_address = _operator_address
             AND j.height > (SELECT MAX(id) - 1000 FROM api.blocks_raw))::NUMERIC
          * 100 / total_blocks, 0)))
      ELSE 100
    END as uptime_percentage,

    -- Blocks signed (estimated from rewards)
    (SELECT COUNT(DISTINCT height)::INTEGER
     FROM api.validator_rewards
     WHERE validator_address = consensus_addr OR validator_address = _operator_address) as blocks_signed,

    -- Blocks missed (from jailing events)
    (SELECT COUNT(*)::INTEGER
     FROM api.jailing_events
     WHERE operator_address = _operator_address) as blocks_missed,

    -- Total jailing events
    (SELECT COUNT(*)::INTEGER
     FROM api.jailing_events
     WHERE operator_address = _operator_address) as total_jailing_events,

    -- Last jailed height
    (SELECT MAX(height)
     FROM api.jailing_events
     WHERE operator_address = _operator_address) as last_jailed_height,

    -- Rewards rank
    (SELECT rank::INTEGER
     FROM (
       SELECT operator_address,
              RANK() OVER (ORDER BY lifetime_rewards DESC) as rank
       FROM api.mv_validator_leaderboard
     ) ranked
     WHERE operator_address = _operator_address) as rewards_rank,

    -- Delegation rank
    (SELECT rank::INTEGER
     FROM (
       SELECT operator_address,
              RANK() OVER (ORDER BY delegator_count DESC) as rank
       FROM api.mv_validator_leaderboard
     ) ranked
     WHERE operator_address = _operator_address) as delegation_rank;
END;
$$ LANGUAGE plpgsql STABLE;

-- ============================================================================
-- Function: Get recent validator events for analytics
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
    ON vca.consensus_address = COALESCE(f.attributes->>'validator', f.attributes->>'address', '')
  LEFT JOIN api.validators v ON v.operator_address = vca.operator_address
  LEFT JOIN api.blocks_raw b ON b.id = f.height
  WHERE f.event_type IN ('slash', 'liveness', 'jail', 'rewards', 'commission')
  ORDER BY f.height DESC
  LIMIT _limit;
END;
$$ LANGUAGE plpgsql STABLE;

-- ============================================================================
-- Function: Refresh analytics views
-- ============================================================================

CREATE OR REPLACE FUNCTION api.refresh_analytics_views()
RETURNS void
LANGUAGE sql
AS $$
  REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_daily_tx_stats;
  REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_hourly_tx_stats;
  REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_message_type_stats;
  REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_validator_delegator_counts;
  REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_daily_rewards;
  REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_validator_leaderboard;
$$;

-- ============================================================================
-- Grants
-- ============================================================================

GRANT SELECT ON api.validator_rewards TO web_anon;
GRANT SELECT ON api.block_metrics TO web_anon;
GRANT SELECT ON api.mv_daily_rewards TO web_anon;
GRANT SELECT ON api.mv_validator_leaderboard TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_validator_rewards_history(TEXT, INTEGER, INTEGER) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_validator_total_rewards(TEXT) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_network_overview() TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_hourly_rewards(INTEGER) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_validator_performance(TEXT) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_validator_events_summary(INTEGER) TO web_anon;
GRANT EXECUTE ON FUNCTION api.refresh_analytics_views() TO web_anon;

COMMIT;
