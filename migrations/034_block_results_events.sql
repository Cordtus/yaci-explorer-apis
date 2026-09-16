-- Migration 034: Extract events from block_results_raw
--
-- This migration adds support for extracting finalize_block_events from the new
-- block_results_raw table populated by yaci with --enable-block-results flag.
--
-- finalize_block_events contain consensus-level events that are more reliable
-- than inferring jailing from block signature changes:
-- - "slash" events with validator address, power, and reason
-- - "liveness" events for jailing due to downtime
-- - Validator power updates
--
-- This is the preferred source for jailing/slashing data when available.

BEGIN;

-- ============================================================================
-- Table: finalize_block_events - stores parsed finalize_block_events
-- ============================================================================

CREATE TABLE IF NOT EXISTS api.finalize_block_events (
  id SERIAL PRIMARY KEY,
  height BIGINT NOT NULL,
  event_index INTEGER NOT NULL,
  event_type TEXT NOT NULL,
  attributes JSONB NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ DEFAULT NOW(),

  UNIQUE(height, event_index)
);

CREATE INDEX IF NOT EXISTS idx_finalize_events_height ON api.finalize_block_events(height);
CREATE INDEX IF NOT EXISTS idx_finalize_events_type ON api.finalize_block_events(event_type);
CREATE INDEX IF NOT EXISTS idx_finalize_events_type_height ON api.finalize_block_events(event_type, height DESC);

-- Index for validator-related events
CREATE INDEX IF NOT EXISTS idx_finalize_events_validator ON api.finalize_block_events((attributes->>'validator'))
WHERE attributes->>'validator' IS NOT NULL;

-- ============================================================================
-- Function: Extract finalize_block_events from block_results_raw
-- ============================================================================

CREATE OR REPLACE FUNCTION api.extract_finalize_block_events()
RETURNS TRIGGER AS $$
DECLARE
  events JSONB;
  event_item JSONB;
  event_idx INTEGER;
  event_type TEXT;
  attrs JSONB;
  attr_item JSONB;
BEGIN
  -- Get finalize_block_events array (handle both camelCase and snake_case)
  events := COALESCE(
    NEW.data->'finalizeBlockEvents',
    NEW.data->'finalize_block_events',
    '[]'::JSONB
  );

  -- Skip if no events
  IF jsonb_array_length(events) = 0 THEN
    RETURN NEW;
  END IF;

  event_idx := 0;
  FOR event_item IN SELECT * FROM jsonb_array_elements(events)
  LOOP
    -- Extract event type
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

    -- Insert event
    INSERT INTO api.finalize_block_events (height, event_index, event_type, attributes)
    VALUES (NEW.height, event_idx, event_type, attrs)
    ON CONFLICT (height, event_index) DO UPDATE SET
      event_type = EXCLUDED.event_type,
      attributes = EXCLUDED.attributes;

    -- Handle specific event types
    IF event_type IN ('slash', 'liveness', 'jail') THEN
      -- Record jailing event
      INSERT INTO api.jailing_events (
        validator_address,
        height,
        prev_block_flag,
        current_block_flag
      ) VALUES (
        COALESCE(attrs->>'validator', attrs->>'address', ''),
        NEW.height,
        'FINALIZE_BLOCK_EVENT',
        event_type
      )
      ON CONFLICT (validator_address, height) DO NOTHING;

      -- Update validator jailed status if we have a matching validator
      UPDATE api.validators SET
        jailed = TRUE,
        updated_at = NOW()
      WHERE consensus_address = COALESCE(attrs->>'validator', attrs->>'address', '')
         OR operator_address IN (
           SELECT operator_address
           FROM api.validator_consensus_addresses
           WHERE consensus_address = COALESCE(attrs->>'validator', attrs->>'address', '')
         );
    END IF;

    event_idx := event_idx + 1;
  END LOOP;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Trigger on block_results_raw (if table exists)
-- Note: block_results_raw is created by migration 041; this trigger will be
-- recreated there. We skip here if table doesn't exist yet.
-- ============================================================================

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'api' AND table_name = 'block_results_raw') THEN
    DROP TRIGGER IF EXISTS trigger_extract_finalize_events ON api.block_results_raw;
    CREATE TRIGGER trigger_extract_finalize_events
      AFTER INSERT ON api.block_results_raw
      FOR EACH ROW
      EXECUTE FUNCTION api.extract_finalize_block_events();
  ELSE
    RAISE NOTICE 'Skipping trigger creation: api.block_results_raw does not exist yet (will be created in migration 041)';
  END IF;
END $$;

-- ============================================================================
-- Function: Get jailing events for a validator
-- ============================================================================

CREATE OR REPLACE FUNCTION api.get_validator_jailing_events(
  _operator_address TEXT,
  _limit INTEGER DEFAULT 50,
  _offset INTEGER DEFAULT 0
)
RETURNS TABLE (
  height BIGINT,
  event_type TEXT,
  reason TEXT,
  power TEXT,
  detected_at TIMESTAMPTZ
) AS $$
BEGIN
  RETURN QUERY
  SELECT
    j.height,
    COALESCE(j.current_block_flag, 'unknown') as event_type,
    COALESCE(f.attributes->>'reason', '') as reason,
    COALESCE(f.attributes->>'power', '') as power,
    j.detected_at
  FROM api.jailing_events j
  LEFT JOIN api.finalize_block_events f
    ON f.height = j.height
    AND f.event_type IN ('slash', 'liveness', 'jail')
    AND (f.attributes->>'validator' = j.validator_address OR f.attributes->>'address' = j.validator_address)
  WHERE j.operator_address = _operator_address
     OR j.validator_address IN (
       SELECT consensus_address
       FROM api.validator_consensus_addresses
       WHERE operator_address = _operator_address
     )
  ORDER BY j.height DESC
  LIMIT _limit
  OFFSET _offset;
END;
$$ LANGUAGE plpgsql STABLE;

-- ============================================================================
-- Function: Get recent slashing/jailing events across all validators
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
  created_at TIMESTAMPTZ
) AS $$
BEGIN
  RETURN QUERY
  SELECT
    f.height,
    f.event_type,
    COALESCE(f.attributes->>'validator', f.attributes->>'address', '') as validator_address,
    v.operator_address,
    v.moniker,
    COALESCE(f.attributes->>'reason', '') as reason,
    COALESCE(f.attributes->>'power', '') as power,
    f.created_at
  FROM api.finalize_block_events f
  LEFT JOIN api.validator_consensus_addresses vca
    ON vca.consensus_address = COALESCE(f.attributes->>'validator', f.attributes->>'address', '')
  LEFT JOIN api.validators v ON v.operator_address = vca.operator_address
  WHERE f.event_type = ANY(_event_types)
  ORDER BY f.height DESC
  LIMIT _limit
  OFFSET _offset;
END;
$$ LANGUAGE plpgsql STABLE;

-- ============================================================================
-- Function: Backfill finalize_block_events from existing block_results_raw
-- ============================================================================

CREATE OR REPLACE FUNCTION api.backfill_finalize_block_events()
RETURNS TABLE(events_processed INTEGER, jailing_events_created INTEGER) AS $$
DECLARE
  rec RECORD;
  events JSONB;
  event_item JSONB;
  event_idx INTEGER;
  event_type TEXT;
  attrs JSONB;
  attr_item JSONB;
  total_events INTEGER := 0;
  total_jailing INTEGER := 0;
BEGIN
  FOR rec IN SELECT height, data FROM api.block_results_raw ORDER BY height
  LOOP
    events := COALESCE(
      rec.data->'finalizeBlockEvents',
      rec.data->'finalize_block_events',
      '[]'::JSONB
    );

    IF jsonb_array_length(events) = 0 THEN
      CONTINUE;
    END IF;

    event_idx := 0;
    FOR event_item IN SELECT * FROM jsonb_array_elements(events)
    LOOP
      event_type := event_item->>'type';

      attrs := '{}';
      FOR attr_item IN SELECT * FROM jsonb_array_elements(COALESCE(event_item->'attributes', '[]'::JSONB))
      LOOP
        attrs := attrs || jsonb_build_object(
          COALESCE(attr_item->>'key', ''),
          COALESCE(attr_item->>'value', '')
        );
      END LOOP;

      INSERT INTO api.finalize_block_events (height, event_index, event_type, attributes)
      VALUES (rec.height, event_idx, event_type, attrs)
      ON CONFLICT (height, event_index) DO NOTHING;

      total_events := total_events + 1;

      IF event_type IN ('slash', 'liveness', 'jail') THEN
        INSERT INTO api.jailing_events (
          validator_address,
          height,
          prev_block_flag,
          current_block_flag
        ) VALUES (
          COALESCE(attrs->>'validator', attrs->>'address', ''),
          rec.height,
          'FINALIZE_BLOCK_EVENT',
          event_type
        )
        ON CONFLICT (validator_address, height) DO NOTHING;

        total_jailing := total_jailing + 1;
      END IF;

      event_idx := event_idx + 1;
    END LOOP;
  END LOOP;

  RETURN QUERY SELECT total_events, total_jailing;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Grants
-- ============================================================================

GRANT SELECT ON api.finalize_block_events TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_validator_jailing_events(TEXT, INTEGER, INTEGER) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_recent_validator_events(TEXT[], INTEGER, INTEGER) TO web_anon;
GRANT EXECUTE ON FUNCTION api.backfill_finalize_block_events() TO web_anon;

COMMIT;
