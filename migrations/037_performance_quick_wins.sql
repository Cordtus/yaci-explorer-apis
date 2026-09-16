-- Migration 037: Performance quick wins
-- HIGH priority improvements from database audit:
-- 1. Add composite index on events_main for efficient event lookups
-- 2. Convert delegation_events.amount from TEXT to NUMERIC
-- 3. Optimize get_delegator_stats() to use single scan with FILTER clauses

BEGIN;

-- ============================================================================
-- 1. Composite index for events_main
-- Covers common query patterns: lookup by tx_id + event_type + attr_key
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_events_main_id_type_key
  ON api.events_main (id, event_type, attr_key);

-- ============================================================================
-- 2. Convert delegation_events.amount from TEXT to NUMERIC
-- Benefits:
-- - Eliminates casting overhead in every aggregation query
-- - Reduces storage (NUMERIC is more compact for numbers than TEXT)
-- - Enables proper numeric comparisons without cast
-- ============================================================================

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'api' AND table_name = 'delegation_events'
      AND column_name = 'amount' AND data_type <> 'numeric'
  ) THEN
    ALTER TABLE api.delegation_events ADD COLUMN IF NOT EXISTS amount_numeric NUMERIC;
    UPDATE api.delegation_events
      SET amount_numeric = NULLIF(amount::text, '')::NUMERIC
      WHERE amount_numeric IS NULL AND amount IS NOT NULL AND amount::text <> '';
    ALTER TABLE api.delegation_events DROP COLUMN IF EXISTS amount;
    ALTER TABLE api.delegation_events RENAME COLUMN amount_numeric TO amount;
  END IF;
END $$;

-- ============================================================================
-- 3. Optimize get_delegator_stats() to use single table scan
-- Before: 6 separate scans (one for each statistic)
-- After: 1 scan with FILTER clauses
-- ============================================================================

CREATE OR REPLACE FUNCTION api.get_delegator_stats(
  _delegator_address TEXT
)
RETURNS JSONB
LANGUAGE sql STABLE
AS $$
  SELECT jsonb_build_object(
    'total_delegations', COUNT(*) FILTER (WHERE event_type = 'DELEGATE'),
    'total_undelegations', COUNT(*) FILTER (WHERE event_type = 'UNDELEGATE'),
    'total_redelegations', COUNT(*) FILTER (WHERE event_type = 'REDELEGATE'),
    'first_delegation', MIN(timestamp),
    'last_activity', MAX(timestamp),
    'unique_validators', COUNT(DISTINCT validator_address) FILTER (
      WHERE event_type IN ('DELEGATE', 'CREATE_VALIDATOR')
    )
  )
  FROM api.delegation_events
  WHERE delegator_address = _delegator_address;
$$;

-- ============================================================================
-- 4. Update get_delegator_delegations() to use NUMERIC amount directly
-- No more amount::NUMERIC casts needed
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
          WHEN de.event_type IN ('DELEGATE', 'CREATE_VALIDATOR') THEN COALESCE(de.amount, 0)
          WHEN de.event_type = 'UNDELEGATE' THEN -COALESCE(de.amount, 0)
          WHEN de.event_type = 'REDELEGATE' THEN
            CASE
              WHEN de.validator_address = de.src_validator_address THEN -COALESCE(de.amount, 0)
              ELSE COALESCE(de.amount, 0)
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
        WHEN de.event_type IN ('DELEGATE', 'CREATE_VALIDATOR') THEN COALESCE(de.amount, 0)
        WHEN de.event_type = 'UNDELEGATE' THEN -COALESCE(de.amount, 0)
        WHEN de.event_type = 'REDELEGATE' THEN
          CASE
            WHEN de.validator_address = de.src_validator_address THEN -COALESCE(de.amount, 0)
            ELSE COALESCE(de.amount, 0)
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
-- 5. Update detect_staking_messages trigger to use NUMERIC
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
        NULLIF(raw_data->'amount'->>'amount', '')::NUMERIC,
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
        NULLIF(raw_data->'amount'->>'amount', '')::NUMERIC,
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
        NULLIF(raw_data->'amount'->>'amount', '')::NUMERIC,
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
        NULLIF(raw_data->'value'->>'amount', '')::NUMERIC,
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

-- ============================================================================
-- Grants
-- ============================================================================

GRANT EXECUTE ON FUNCTION api.get_delegator_stats(TEXT) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_delegator_delegations(TEXT) TO web_anon;

COMMIT;
