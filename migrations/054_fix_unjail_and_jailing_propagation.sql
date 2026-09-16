-- Migration 054: Fix MsgUnjail handling + jailing event propagation
--
-- Bug fixes:
-- 1. MsgUnjail was never processed because:
--    a) Migration 031 created the trigger WHEN clause without MsgUnjail
--    b) Migration 053 rewrote detect_staking_from_message() without MsgUnjail branch
--    Result: unjailing could only happen via the 6-hour full sync
--
-- 2. detect_jailing_from_block() (migration 032) inserts into jailing_events
--    but never updates validators.jailed. Migration 053 added jailing propagation
--    only to extract_finalize_block_events(), not to the block-signature path.
--    This adds a trigger on jailing_events to propagate to validators.
--
-- Changes:
-- A. Add MsgUnjail branch to detect_staking_from_message()
-- B. Recreate trigger with MsgUnjail in WHEN clause
-- C. Add trigger on jailing_events INSERT to propagate to validators table

BEGIN;

-- ============================================================================
-- 0. Update delegation_events CHECK constraint to allow UNJAIL
-- ============================================================================

ALTER TABLE api.delegation_events
  DROP CONSTRAINT IF EXISTS delegation_events_event_type_check;

ALTER TABLE api.delegation_events
  ADD CONSTRAINT delegation_events_event_type_check
  CHECK (event_type IN (
    'DELEGATE', 'UNDELEGATE', 'REDELEGATE', 'CREATE_VALIDATOR', 'EDIT_VALIDATOR', 'UNJAIL'
  ));

-- ============================================================================
-- A. Redefine detect_staking_from_message() with MsgUnjail support
-- ============================================================================

CREATE OR REPLACE FUNCTION api.detect_staking_from_message()
RETURNS TRIGGER AS $$
DECLARE
  raw_data JSONB;
  tx_record RECORD;
  val_addr TEXT;
  del_addr TEXT;
  dst_addr TEXT;
  src_addr TEXT;
BEGIN
  -- Only process staking message types
  IF NEW.type NOT LIKE '%MsgDelegate'
     AND NEW.type NOT LIKE '%MsgUndelegate'
     AND NEW.type NOT LIKE '%MsgBeginRedelegate'
     AND NEW.type NOT LIKE '%MsgCreateValidator'
     AND NEW.type NOT LIKE '%MsgEditValidator'
     AND NEW.type NOT LIKE '%MsgUnjail' THEN
    RETURN NEW;
  END IF;

  -- Get transaction context (height, timestamp)
  SELECT height, timestamp INTO tx_record
  FROM api.transactions_main
  WHERE id = NEW.id;

  -- If transaction not found (shouldn't happen), skip
  IF tx_record IS NULL THEN
    RETURN NEW;
  END IF;

  -- Get raw message data
  SELECT data INTO raw_data
  FROM api.messages_raw
  WHERE id = NEW.id AND message_index = NEW.message_index;

  -- Extract addresses with fallbacks for both camelCase and snake_case
  val_addr := COALESCE(
    raw_data->>'validatorAddress',
    raw_data->>'validator_address',
    NEW.metadata->>'validatorAddress',
    NEW.metadata->>'validator_address',
    ''
  );
  del_addr := COALESCE(
    raw_data->>'delegatorAddress',
    raw_data->>'delegator_address',
    NEW.sender,
    ''
  );

  -- Skip if no validator address found
  IF val_addr = '' THEN
    RETURN NEW;
  END IF;

  -- MsgDelegate (but not MsgBeginRedelegate)
  IF NEW.type LIKE '%MsgDelegate' AND NEW.type NOT LIKE '%MsgBeginRedelegate' THEN
    INSERT INTO api.delegation_events (
      event_type, delegator_address, validator_address,
      amount, denom, tx_hash, height, timestamp
    ) VALUES (
      'DELEGATE', del_addr, val_addr,
      NULLIF(COALESCE(raw_data->'amount'->>'amount', raw_data->'coin'->>'amount'), '')::NUMERIC,
      COALESCE(raw_data->'amount'->>'denom', raw_data->'coin'->>'denom'),
      NEW.id, tx_record.height, tx_record.timestamp
    )
    ON CONFLICT DO NOTHING;

    PERFORM pg_notify('validator_refresh', val_addr);

  -- MsgUndelegate
  ELSIF NEW.type LIKE '%MsgUndelegate' THEN
    INSERT INTO api.delegation_events (
      event_type, delegator_address, validator_address,
      amount, denom, tx_hash, height, timestamp
    ) VALUES (
      'UNDELEGATE', del_addr, val_addr,
      NULLIF(COALESCE(raw_data->'amount'->>'amount', raw_data->'coin'->>'amount'), '')::NUMERIC,
      COALESCE(raw_data->'amount'->>'denom', raw_data->'coin'->>'denom'),
      NEW.id, tx_record.height, tx_record.timestamp
    )
    ON CONFLICT DO NOTHING;

    PERFORM pg_notify('validator_refresh', val_addr);

  -- MsgBeginRedelegate
  ELSIF NEW.type LIKE '%MsgBeginRedelegate' THEN
    dst_addr := COALESCE(raw_data->>'validatorDstAddress', raw_data->>'validator_dst_address', '');
    src_addr := COALESCE(raw_data->>'validatorSrcAddress', raw_data->>'validator_src_address', '');

    INSERT INTO api.delegation_events (
      event_type, delegator_address, validator_address,
      src_validator_address, amount, denom,
      tx_hash, height, timestamp
    ) VALUES (
      'REDELEGATE', del_addr,
      dst_addr, src_addr,
      NULLIF(COALESCE(raw_data->'amount'->>'amount', raw_data->'coin'->>'amount'), '')::NUMERIC,
      COALESCE(raw_data->'amount'->>'denom', raw_data->'coin'->>'denom'),
      NEW.id, tx_record.height, tx_record.timestamp
    )
    ON CONFLICT DO NOTHING;

    -- Notify both source and destination validators
    IF dst_addr <> '' THEN
      PERFORM pg_notify('validator_refresh', dst_addr);
    END IF;
    IF src_addr <> '' THEN
      PERFORM pg_notify('validator_refresh', src_addr);
    END IF;

  -- MsgCreateValidator
  ELSIF NEW.type LIKE '%MsgCreateValidator' THEN
    INSERT INTO api.delegation_events (
      event_type, delegator_address, validator_address,
      amount, denom, tx_hash, height, timestamp
    ) VALUES (
      'CREATE_VALIDATOR', del_addr, val_addr,
      NULLIF(COALESCE(
        raw_data->'value'->>'amount',
        raw_data->'selfDelegation'->>'amount',
        raw_data->'self_delegation'->>'amount'
      ), '')::NUMERIC,
      COALESCE(
        raw_data->'value'->>'denom',
        raw_data->'selfDelegation'->>'denom',
        raw_data->'self_delegation'->>'denom'
      ),
      NEW.id, tx_record.height, tx_record.timestamp
    )
    ON CONFLICT DO NOTHING;

    -- Upsert validator record
    INSERT INTO api.validators (
      operator_address, moniker, identity, website, details,
      commission_rate, commission_max_rate, commission_max_change_rate,
      min_self_delegation, tokens, status, creation_height, first_seen_tx
    ) VALUES (
      val_addr,
      COALESCE(raw_data->'description'->>'moniker', raw_data->>'moniker'),
      COALESCE(raw_data->'description'->>'identity', raw_data->>'identity'),
      COALESCE(raw_data->'description'->>'website', raw_data->>'website'),
      COALESCE(raw_data->'description'->>'details', raw_data->>'details'),
      NULLIF(COALESCE(
        raw_data->'commission'->'commissionRates'->>'rate',
        raw_data->'commission'->'commission_rates'->>'rate'
      ), '')::NUMERIC,
      NULLIF(COALESCE(
        raw_data->'commission'->'commissionRates'->>'maxRate',
        raw_data->'commission'->'commission_rates'->>'max_rate'
      ), '')::NUMERIC,
      NULLIF(COALESCE(
        raw_data->'commission'->'commissionRates'->>'maxChangeRate',
        raw_data->'commission'->'commission_rates'->>'max_change_rate'
      ), '')::NUMERIC,
      NULLIF(COALESCE(raw_data->>'minSelfDelegation', raw_data->>'min_self_delegation'), '')::NUMERIC,
      NULLIF(COALESCE(
        raw_data->'value'->>'amount',
        raw_data->'selfDelegation'->>'amount'
      ), '')::NUMERIC,
      'BOND_STATUS_BONDED',
      tx_record.height,
      NEW.id
    )
    ON CONFLICT (operator_address) DO UPDATE SET
      moniker = COALESCE(EXCLUDED.moniker, api.validators.moniker),
      identity = COALESCE(EXCLUDED.identity, api.validators.identity),
      website = COALESCE(EXCLUDED.website, api.validators.website),
      details = COALESCE(EXCLUDED.details, api.validators.details),
      creation_height = COALESCE(api.validators.creation_height, EXCLUDED.creation_height),
      first_seen_tx = COALESCE(api.validators.first_seen_tx, EXCLUDED.first_seen_tx),
      updated_at = NOW();

    PERFORM pg_notify('validator_refresh', val_addr);

  -- MsgEditValidator
  ELSIF NEW.type LIKE '%MsgEditValidator' THEN
    INSERT INTO api.delegation_events (
      event_type, delegator_address, validator_address,
      tx_hash, height, timestamp
    ) VALUES (
      'EDIT_VALIDATOR', NEW.sender, val_addr,
      NEW.id, tx_record.height, tx_record.timestamp
    )
    ON CONFLICT DO NOTHING;

    -- Update validator record
    UPDATE api.validators SET
      moniker = COALESCE(
        NULLIF(COALESCE(raw_data->'description'->>'moniker', raw_data->>'moniker'), '[do-not-modify]'),
        moniker
      ),
      identity = COALESCE(
        NULLIF(COALESCE(raw_data->'description'->>'identity', raw_data->>'identity'), '[do-not-modify]'),
        identity
      ),
      website = COALESCE(
        NULLIF(COALESCE(raw_data->'description'->>'website', raw_data->>'website'), '[do-not-modify]'),
        website
      ),
      details = COALESCE(
        NULLIF(COALESCE(raw_data->'description'->>'details', raw_data->>'details'), '[do-not-modify]'),
        details
      ),
      commission_rate = COALESCE(
        NULLIF(raw_data->>'commissionRate', '')::NUMERIC,
        commission_rate
      ),
      updated_at = NOW()
    WHERE operator_address = val_addr;

    PERFORM pg_notify('validator_refresh', val_addr);

  -- MsgUnjail
  ELSIF NEW.type LIKE '%MsgUnjail' THEN
    INSERT INTO api.delegation_events (
      event_type, delegator_address, validator_address,
      tx_hash, height, timestamp
    ) VALUES (
      'UNJAIL', del_addr, val_addr,
      NEW.id, tx_record.height, tx_record.timestamp
    )
    ON CONFLICT DO NOTHING;

    -- Immediately clear jailed flag
    UPDATE api.validators SET
      jailed = FALSE,
      updated_at = NOW()
    WHERE operator_address = val_addr;

    PERFORM pg_notify('validator_refresh', val_addr);
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- B. Recreate trigger with MsgUnjail in WHEN clause
-- ============================================================================

DROP TRIGGER IF EXISTS trigger_detect_staking_from_message ON api.messages_main;

CREATE TRIGGER trigger_detect_staking_from_message
  AFTER INSERT ON api.messages_main
  FOR EACH ROW
  WHEN (
    NEW.type LIKE '%MsgDelegate'
    OR NEW.type LIKE '%MsgUndelegate'
    OR NEW.type LIKE '%MsgBeginRedelegate'
    OR NEW.type LIKE '%MsgCreateValidator'
    OR NEW.type LIKE '%MsgEditValidator'
    OR NEW.type LIKE '%MsgUnjail'
  )
  EXECUTE FUNCTION api.detect_staking_from_message();

-- ============================================================================
-- C. Trigger on jailing_events to propagate jailed status to validators
--
-- detect_jailing_from_block() (migration 032) inserts into jailing_events
-- but does NOT update validators.jailed. This trigger closes that gap.
-- ============================================================================

CREATE OR REPLACE FUNCTION api.propagate_jailing_event()
RETURNS TRIGGER AS $$
DECLARE
  resolved_operator TEXT;
BEGIN
  -- Resolve consensus address to operator address
  SELECT vca.operator_address INTO resolved_operator
  FROM api.validator_consensus_addresses vca
  WHERE vca.consensus_address = NEW.validator_address
  LIMIT 1;

  IF resolved_operator IS NOT NULL AND resolved_operator <> '' THEN
    UPDATE api.validators SET
      jailed = TRUE,
      updated_at = NOW()
    WHERE operator_address = resolved_operator;

    -- Trigger daemon to fetch fresh chain state
    PERFORM pg_notify('validator_refresh', resolved_operator);
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_propagate_jailing_event ON api.jailing_events;

CREATE TRIGGER trigger_propagate_jailing_event
  AFTER INSERT ON api.jailing_events
  FOR EACH ROW
  EXECUTE FUNCTION api.propagate_jailing_event();

COMMIT;
