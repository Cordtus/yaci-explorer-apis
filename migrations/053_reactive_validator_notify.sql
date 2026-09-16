-- Migration 053: Reactive validator refresh via pg_notify
--
-- Adds pg_notify('validator_refresh', operator_address) calls to:
-- 1. detect_staking_from_message() - fires on staking messages (DELEGATE, etc.)
-- 2. extract_finalize_block_events() - fires on slash/jail events
--
-- Also adds api.request_validator_refresh() RPC for on-demand refresh
-- (mirrors the pattern from migration 027 request_evm_decode).

BEGIN;

-- ============================================================================
-- Redefine detect_staking_from_message() with pg_notify calls
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
     AND NEW.type NOT LIKE '%MsgEditValidator' THEN
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
      COALESCE(raw_data->'amount'->>'amount', raw_data->'coin'->>'amount'),
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
      COALESCE(raw_data->'amount'->>'amount', raw_data->'coin'->>'amount'),
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
      COALESCE(raw_data->'amount'->>'amount', raw_data->'coin'->>'amount'),
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
      COALESCE(
        raw_data->'value'->>'amount',
        raw_data->'selfDelegation'->>'amount',
        raw_data->'self_delegation'->>'amount'
      ),
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
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Redefine extract_finalize_block_events() with pg_notify for jailing
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
  resolved_operator TEXT;
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

      -- Notify validator_refresh with operator_address (resolve from consensus address)
      SELECT vca.operator_address INTO resolved_operator
      FROM api.validator_consensus_addresses vca
      WHERE vca.consensus_address = COALESCE(attrs->>'validator', attrs->>'address', '')
      LIMIT 1;

      IF resolved_operator IS NOT NULL AND resolved_operator <> '' THEN
        PERFORM pg_notify('validator_refresh', resolved_operator);
      END IF;
    END IF;

    event_idx := event_idx + 1;
  END LOOP;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- RPC: request_validator_refresh (on-demand, mirrors request_evm_decode)
-- ============================================================================

CREATE OR REPLACE FUNCTION api.request_validator_refresh(_operator_address text)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
BEGIN
  -- Validate that the address looks like a valoper address
  IF _operator_address IS NULL OR _operator_address = '' THEN
    RETURN jsonb_build_object('success', false, 'message', 'operator_address required');
  END IF;

  -- Check if this validator exists in our table
  IF EXISTS (SELECT 1 FROM api.validators WHERE operator_address = _operator_address) THEN
    PERFORM pg_notify('validator_refresh', _operator_address);
    RETURN jsonb_build_object('success', true);
  END IF;

  -- Unknown validator - still notify in case it's new and not yet in table
  PERFORM pg_notify('validator_refresh', _operator_address);
  RETURN jsonb_build_object('success', true, 'message', 'validator not in table, refresh requested');
END;
$$;

GRANT EXECUTE ON FUNCTION api.request_validator_refresh(text) TO web_anon;

COMMIT;
