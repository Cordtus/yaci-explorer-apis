-- Migration 031: Fix staking trigger timing issue
--
-- PROBLEM: The detect_staking_messages trigger fires on transactions_main INSERT,
-- but at that point messages_main hasn't been populated yet (race condition).
--
-- SOLUTION: Move the trigger to fire on messages_main INSERT instead, where the
-- message data is guaranteed to exist.

BEGIN;

-- ============================================================================
-- Drop the broken trigger from transactions_main
-- ============================================================================

DROP TRIGGER IF EXISTS trigger_detect_staking ON api.transactions_main;

-- ============================================================================
-- New function: detect staking from messages_main context
-- ============================================================================

CREATE OR REPLACE FUNCTION api.detect_staking_from_message()
RETURNS TRIGGER AS $$
DECLARE
  raw_data JSONB;
  tx_record RECORD;
  val_addr TEXT;
  del_addr TEXT;
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
      COALESCE(raw_data->'amount'->>'amount', raw_data->'coin'->>'amount'),
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
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Create trigger on messages_main (fires when message is actually available)
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
  )
  EXECUTE FUNCTION api.detect_staking_from_message();

-- ============================================================================
-- Comprehensive backfill of all historical staking events
-- ============================================================================

-- Add unique index to prevent duplicates during backfill
-- (Using CREATE INDEX instead of UNIQUE constraint to allow COALESCE expression)
DROP INDEX IF EXISTS api.idx_delegation_events_unique;

CREATE UNIQUE INDEX idx_delegation_events_unique
  ON api.delegation_events (tx_hash, event_type, validator_address, COALESCE(delegator_address, ''));

-- Clear existing (potentially incomplete) delegation events for re-backfill
TRUNCATE api.delegation_events;

-- Backfill CREATE_VALIDATOR events
INSERT INTO api.delegation_events (
  event_type, delegator_address, validator_address,
  amount, denom, tx_hash, height, timestamp
)
SELECT
  'CREATE_VALIDATOR' AS event_type,
  COALESCE(
    mr.data->>'delegatorAddress',
    mr.data->>'delegator_address',
    m.sender
  ) AS delegator_address,
  COALESCE(
    mr.data->>'validatorAddress',
    mr.data->>'validator_address',
    m.metadata->>'validatorAddress',
    m.metadata->>'validator_address'
  ) AS validator_address,
  NULLIF(COALESCE(
    mr.data->'value'->>'amount',
    mr.data->'selfDelegation'->>'amount',
    mr.data->'self_delegation'->>'amount'
  ), '')::numeric AS amount,
  COALESCE(
    mr.data->'value'->>'denom',
    mr.data->'selfDelegation'->>'denom',
    mr.data->'self_delegation'->>'denom'
  ) AS denom,
  t.id AS tx_hash,
  t.height,
  t.timestamp
FROM api.messages_main m
JOIN api.transactions_main t ON m.id = t.id
LEFT JOIN api.messages_raw mr ON m.id = mr.id AND m.message_index = mr.message_index
WHERE m.type LIKE '%MsgCreateValidator'
AND COALESCE(
  mr.data->>'validatorAddress',
  mr.data->>'validator_address',
  m.metadata->>'validatorAddress',
  m.metadata->>'validator_address',
  ''
) <> ''
ON CONFLICT DO NOTHING;

-- Backfill DELEGATE events
INSERT INTO api.delegation_events (
  event_type, delegator_address, validator_address,
  amount, denom, tx_hash, height, timestamp
)
SELECT
  'DELEGATE' AS event_type,
  COALESCE(mr.data->>'delegatorAddress', mr.data->>'delegator_address', m.sender) AS delegator_address,
  COALESCE(
    mr.data->>'validatorAddress',
    mr.data->>'validator_address',
    m.metadata->>'validatorAddress',
    m.metadata->>'validator_address'
  ) AS validator_address,
  NULLIF(COALESCE(mr.data->'amount'->>'amount', mr.data->'coin'->>'amount'), '')::numeric AS amount,
  COALESCE(mr.data->'amount'->>'denom', mr.data->'coin'->>'denom') AS denom,
  t.id AS tx_hash,
  t.height,
  t.timestamp
FROM api.messages_main m
JOIN api.transactions_main t ON m.id = t.id
LEFT JOIN api.messages_raw mr ON m.id = mr.id AND m.message_index = mr.message_index
WHERE m.type LIKE '%MsgDelegate'
AND m.type NOT LIKE '%MsgBeginRedelegate'
AND COALESCE(
  mr.data->>'validatorAddress',
  mr.data->>'validator_address',
  m.metadata->>'validatorAddress',
  m.metadata->>'validator_address',
  ''
) <> ''
ON CONFLICT DO NOTHING;

-- Backfill UNDELEGATE events
INSERT INTO api.delegation_events (
  event_type, delegator_address, validator_address,
  amount, denom, tx_hash, height, timestamp
)
SELECT
  'UNDELEGATE' AS event_type,
  COALESCE(mr.data->>'delegatorAddress', mr.data->>'delegator_address', m.sender) AS delegator_address,
  COALESCE(
    mr.data->>'validatorAddress',
    mr.data->>'validator_address',
    m.metadata->>'validatorAddress',
    m.metadata->>'validator_address'
  ) AS validator_address,
  NULLIF(COALESCE(mr.data->'amount'->>'amount', mr.data->'coin'->>'amount'), '')::numeric AS amount,
  COALESCE(mr.data->'amount'->>'denom', mr.data->'coin'->>'denom') AS denom,
  t.id AS tx_hash,
  t.height,
  t.timestamp
FROM api.messages_main m
JOIN api.transactions_main t ON m.id = t.id
LEFT JOIN api.messages_raw mr ON m.id = mr.id AND m.message_index = mr.message_index
WHERE m.type LIKE '%MsgUndelegate'
AND COALESCE(
  mr.data->>'validatorAddress',
  mr.data->>'validator_address',
  m.metadata->>'validatorAddress',
  m.metadata->>'validator_address',
  ''
) <> ''
ON CONFLICT DO NOTHING;

-- Backfill REDELEGATE events
INSERT INTO api.delegation_events (
  event_type, delegator_address, validator_address,
  src_validator_address, amount, denom,
  tx_hash, height, timestamp
)
SELECT
  'REDELEGATE' AS event_type,
  COALESCE(mr.data->>'delegatorAddress', mr.data->>'delegator_address', m.sender) AS delegator_address,
  COALESCE(mr.data->>'validatorDstAddress', mr.data->>'validator_dst_address', '') AS validator_address,
  COALESCE(mr.data->>'validatorSrcAddress', mr.data->>'validator_src_address', '') AS src_validator_address,
  NULLIF(COALESCE(mr.data->'amount'->>'amount', mr.data->'coin'->>'amount'), '')::numeric AS amount,
  COALESCE(mr.data->'amount'->>'denom', mr.data->'coin'->>'denom') AS denom,
  t.id AS tx_hash,
  t.height,
  t.timestamp
FROM api.messages_main m
JOIN api.transactions_main t ON m.id = t.id
LEFT JOIN api.messages_raw mr ON m.id = mr.id AND m.message_index = mr.message_index
WHERE m.type LIKE '%MsgBeginRedelegate'
ON CONFLICT DO NOTHING;

-- Backfill EDIT_VALIDATOR events
INSERT INTO api.delegation_events (
  event_type, delegator_address, validator_address,
  tx_hash, height, timestamp
)
SELECT
  'EDIT_VALIDATOR' AS event_type,
  m.sender AS delegator_address,
  COALESCE(
    mr.data->>'validatorAddress',
    mr.data->>'validator_address',
    m.metadata->>'validatorAddress',
    m.metadata->>'validator_address'
  ) AS validator_address,
  t.id AS tx_hash,
  t.height,
  t.timestamp
FROM api.messages_main m
JOIN api.transactions_main t ON m.id = t.id
LEFT JOIN api.messages_raw mr ON m.id = mr.id AND m.message_index = mr.message_index
WHERE m.type LIKE '%MsgEditValidator'
AND COALESCE(
  mr.data->>'validatorAddress',
  mr.data->>'validator_address',
  m.metadata->>'validatorAddress',
  m.metadata->>'validator_address',
  ''
) <> ''
ON CONFLICT DO NOTHING;

-- ============================================================================
-- Backfill validators table from MsgCreateValidator messages
-- ============================================================================

INSERT INTO api.validators (
  operator_address, moniker, identity, website, details,
  commission_rate, commission_max_rate, commission_max_change_rate,
  min_self_delegation, tokens, status, creation_height, first_seen_tx
)
SELECT DISTINCT ON (val_addr)
  val_addr AS operator_address,
  COALESCE(mr.data->'description'->>'moniker', mr.data->>'moniker') AS moniker,
  COALESCE(mr.data->'description'->>'identity', mr.data->>'identity') AS identity,
  COALESCE(mr.data->'description'->>'website', mr.data->>'website') AS website,
  COALESCE(mr.data->'description'->>'details', mr.data->>'details') AS details,
  NULLIF(COALESCE(
    mr.data->'commission'->'commissionRates'->>'rate',
    mr.data->'commission'->'commission_rates'->>'rate'
  ), '')::NUMERIC AS commission_rate,
  NULLIF(COALESCE(
    mr.data->'commission'->'commissionRates'->>'maxRate',
    mr.data->'commission'->'commission_rates'->>'max_rate'
  ), '')::NUMERIC AS commission_max_rate,
  NULLIF(COALESCE(
    mr.data->'commission'->'commissionRates'->>'maxChangeRate',
    mr.data->'commission'->'commission_rates'->>'max_change_rate'
  ), '')::NUMERIC AS commission_max_change_rate,
  NULLIF(COALESCE(mr.data->>'minSelfDelegation', mr.data->>'min_self_delegation'), '')::NUMERIC AS min_self_delegation,
  NULLIF(COALESCE(
    mr.data->'value'->>'amount',
    mr.data->'selfDelegation'->>'amount'
  ), '')::NUMERIC AS tokens,
  'BOND_STATUS_BONDED' AS status,
  t.height AS creation_height,
  t.id AS first_seen_tx
FROM api.messages_main m
JOIN api.transactions_main t ON m.id = t.id
LEFT JOIN api.messages_raw mr ON m.id = mr.id AND m.message_index = mr.message_index
CROSS JOIN LATERAL (
  SELECT COALESCE(
    mr.data->>'validatorAddress',
    mr.data->>'validator_address',
    m.metadata->>'validatorAddress',
    m.metadata->>'validator_address'
  ) AS val_addr
) AS v
WHERE m.type LIKE '%MsgCreateValidator'
AND v.val_addr IS NOT NULL
AND v.val_addr <> ''
ORDER BY val_addr, t.height ASC
ON CONFLICT (operator_address) DO UPDATE SET
  moniker = COALESCE(EXCLUDED.moniker, api.validators.moniker),
  identity = COALESCE(EXCLUDED.identity, api.validators.identity),
  website = COALESCE(EXCLUDED.website, api.validators.website),
  details = COALESCE(EXCLUDED.details, api.validators.details),
  commission_rate = COALESCE(EXCLUDED.commission_rate, api.validators.commission_rate),
  commission_max_rate = COALESCE(EXCLUDED.commission_max_rate, api.validators.commission_max_rate),
  commission_max_change_rate = COALESCE(EXCLUDED.commission_max_change_rate, api.validators.commission_max_change_rate),
  creation_height = COALESCE(api.validators.creation_height, EXCLUDED.creation_height),
  first_seen_tx = COALESCE(api.validators.first_seen_tx, EXCLUDED.first_seen_tx),
  updated_at = NOW();

-- ============================================================================
-- Update validator creation_height from earliest delegation event
-- ============================================================================

UPDATE api.validators v SET
  creation_height = COALESCE(v.creation_height, de.min_height),
  first_seen_tx = COALESCE(v.first_seen_tx, de.first_tx),
  updated_at = NOW()
FROM (
  SELECT
    validator_address,
    MIN(height) AS min_height,
    (
      SELECT tx_hash
      FROM api.delegation_events de2
      WHERE de2.validator_address = de.validator_address
      ORDER BY de2.height NULLS LAST, de2.id
      LIMIT 1
    ) AS first_tx
  FROM api.delegation_events de
  GROUP BY validator_address
) de
WHERE v.operator_address = de.validator_address
AND (v.creation_height IS NULL OR v.first_seen_tx IS NULL);

COMMIT;
