-- Migration 056: Fix delegation_events amount NUMERIC cast
--
-- Hotfix: migration 054 was applied with text->numeric cast missing on
-- delegation_events.amount inserts. The column was changed from TEXT to
-- NUMERIC in migration 037 but the trigger was never updated.
--
-- This caused the yaci indexer to crash-loop on block 212928:
--   ERROR: column "amount" is of type numeric but expression is of type text (SQLSTATE 42804)
--
-- Also updates the CHECK constraint to include 'UNJAIL' event type.

BEGIN;

-- Update CHECK constraint to allow UNJAIL
ALTER TABLE api.delegation_events
  DROP CONSTRAINT IF EXISTS delegation_events_event_type_check;

ALTER TABLE api.delegation_events
  ADD CONSTRAINT delegation_events_event_type_check
  CHECK (event_type IN (
    'DELEGATE', 'UNDELEGATE', 'REDELEGATE', 'CREATE_VALIDATOR', 'EDIT_VALIDATOR', 'UNJAIL'
  ));

-- Redefine detect_staking_from_message() with NUMERIC casts on amount
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

COMMIT;
