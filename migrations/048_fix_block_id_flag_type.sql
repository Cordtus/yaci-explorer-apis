-- Migration 048: Fix data type issues for CometBFT compatibility
-- 1. block_id_flag: CometBFT returns string enum instead of integer
-- 2. delegation amount: explicit TEXT->NUMERIC cast to handle edge cases

BEGIN;

-- ============================================================================
-- 1. Change block_id_flag column from INTEGER to TEXT
-- ============================================================================

ALTER TABLE api.validator_block_signatures
  ALTER COLUMN block_id_flag TYPE TEXT USING block_id_flag::TEXT;

-- ============================================================================
-- 2. Fix extract_block_signatures to handle string enum values
--    Maps: BLOCK_ID_FLAG_ABSENT=1, BLOCK_ID_FLAG_COMMIT=2, BLOCK_ID_FLAG_NIL=3
-- ============================================================================

CREATE OR REPLACE FUNCTION api.extract_block_signatures(
  _height BIGINT,
  _block_data JSONB
) RETURNS INTEGER AS $$
DECLARE
  signatures JSONB;
  sig JSONB;
  sig_idx INTEGER;
  block_time TIMESTAMPTZ;
  validator_addr TEXT;
  raw_flag TEXT;
  flag_val TEXT;
  is_signed BOOLEAN;
  extracted_count INTEGER := 0;
BEGIN
  -- Get block time
  block_time := (_block_data->'block'->'header'->>'time')::TIMESTAMPTZ;

  -- Get signatures array (handle both camelCase and snake_case)
  signatures := COALESCE(
    _block_data->'block'->'lastCommit'->'signatures',
    _block_data->'block'->'last_commit'->'signatures',
    '[]'::JSONB
  );

  IF jsonb_array_length(signatures) = 0 THEN
    RETURN 0;
  END IF;

  sig_idx := 0;
  FOR sig IN SELECT * FROM jsonb_array_elements(signatures)
  LOOP
    -- Get validator address (hex format from consensus)
    validator_addr := UPPER(COALESCE(
      sig->>'validatorAddress',
      sig->>'validator_address',
      ''
    ));

    -- Get block_id_flag as text, handling both integer and string enum formats
    raw_flag := COALESCE(sig->>'blockIdFlag', sig->>'block_id_flag', '1');

    -- Normalize to string enum format
    CASE raw_flag
      WHEN '1' THEN flag_val := 'BLOCK_ID_FLAG_ABSENT';
      WHEN '2' THEN flag_val := 'BLOCK_ID_FLAG_COMMIT';
      WHEN '3' THEN flag_val := 'BLOCK_ID_FLAG_NIL';
      ELSE flag_val := raw_flag;  -- Already a string enum
    END CASE;

    -- Validator signed if flag is COMMIT
    is_signed := (flag_val = 'BLOCK_ID_FLAG_COMMIT');

    -- Skip empty validator addresses (can happen for absent validators)
    IF validator_addr != '' THEN
      INSERT INTO api.validator_block_signatures (
        height, validator_index, consensus_address, signed, block_id_flag, block_time
      ) VALUES (
        _height, sig_idx, validator_addr, is_signed, flag_val, block_time
      )
      ON CONFLICT (height, validator_index) DO UPDATE SET
        consensus_address = EXCLUDED.consensus_address,
        signed = EXCLUDED.signed,
        block_id_flag = EXCLUDED.block_id_flag,
        block_time = EXCLUDED.block_time;

      extracted_count := extracted_count + 1;
    END IF;

    sig_idx := sig_idx + 1;
  END LOOP;

  RETURN extracted_count;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 3. Fix detect_staking_from_message: explicit TEXT->NUMERIC cast for amount
--    JSON ->> returns TEXT; implicit cast to NUMERIC fails on empty strings
-- ============================================================================

CREATE OR REPLACE FUNCTION api.detect_staking_from_message()
RETURNS TRIGGER AS $$
DECLARE
  raw_data JSONB;
  tx_record RECORD;
  val_addr TEXT;
  del_addr TEXT;
BEGIN
  -- Only process staking message types (including unjail)
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
    raw_data->>'validatorAddr',
    raw_data->>'validator_addr',
    NEW.metadata->>'validatorAddress',
    NEW.metadata->>'validator_address',
    NEW.metadata->>'validatorAddr',
    NEW.metadata->>'validator_addr',
    ''
  );
  del_addr := COALESCE(
    raw_data->>'delegatorAddress',
    raw_data->>'delegator_address',
    NEW.sender,
    ''
  );

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

  -- MsgBeginRedelegate
  ELSIF NEW.type LIKE '%MsgBeginRedelegate' THEN
    INSERT INTO api.delegation_events (
      event_type, delegator_address, validator_address,
      src_validator_address, amount, denom,
      tx_hash, height, timestamp
    ) VALUES (
      'REDELEGATE', del_addr,
      COALESCE(raw_data->>'validatorDstAddress', raw_data->>'validator_dst_address', ''),
      COALESCE(raw_data->>'validatorSrcAddress', raw_data->>'validator_src_address', ''),
      NULLIF(COALESCE(raw_data->'amount'->>'amount', raw_data->'coin'->>'amount'), '')::NUMERIC,
      COALESCE(raw_data->'amount'->>'denom', raw_data->'coin'->>'denom'),
      NEW.id, tx_record.height, tx_record.timestamp
    )
    ON CONFLICT DO NOTHING;

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

  -- MsgUnjail
  ELSIF NEW.type LIKE '%MsgUnjail' THEN
    INSERT INTO api.delegation_events (
      event_type, delegator_address, validator_address,
      tx_hash, height, timestamp
    ) VALUES (
      'UNJAIL', NULL, val_addr,
      NEW.id, tx_record.height, tx_record.timestamp
    )
    ON CONFLICT DO NOTHING;

    UPDATE api.validators SET
      jailed = FALSE,
      updated_at = NOW()
    WHERE operator_address = val_addr;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMIT;
