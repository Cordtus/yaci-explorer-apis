-- Migration 030: One-time validator creation backfill
-- Finds the earliest transaction mentioning each validator to populate creation_height

BEGIN;

-- ============================================================================
-- Backfill creation_height from earliest transaction mentioning validator
-- ============================================================================

UPDATE api.validators v SET
  creation_height = subq.min_height,
  first_seen_tx = subq.first_tx,
  updated_at = NOW()
FROM (
  SELECT
    v2.operator_address,
    MIN(t.height) as min_height,
    (
      SELECT t2.id
      FROM api.transactions_main t2
      JOIN api.messages_main m2 ON t2.id = m2.id
      WHERE m2.sender = v2.operator_address
         OR v2.operator_address = ANY(m2.mentions)
      ORDER BY t2.height
      LIMIT 1
    ) as first_tx
  FROM api.validators v2
  JOIN api.messages_main m ON v2.operator_address = m.sender
                           OR v2.operator_address = ANY(m.mentions)
  JOIN api.transactions_main t ON m.id = t.id
  WHERE v2.creation_height IS NULL
  GROUP BY v2.operator_address
) subq
WHERE v.operator_address = subq.operator_address;

-- ============================================================================
-- Backfill CREATE_VALIDATOR events from MsgCreateValidator messages
-- ============================================================================

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
    ''
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
AND COALESCE(mr.data->>'validatorAddress', mr.data->>'validator_address', '') <> ''
AND NOT EXISTS (
  SELECT 1 FROM api.delegation_events de
  WHERE de.tx_hash = t.id
  AND de.event_type = 'CREATE_VALIDATOR'
);

-- ============================================================================
-- Backfill DELEGATE events
-- ============================================================================

INSERT INTO api.delegation_events (
  event_type, delegator_address, validator_address,
  amount, denom, tx_hash, height, timestamp
)
SELECT
  'DELEGATE' AS event_type,
  COALESCE(mr.data->>'delegatorAddress', mr.data->>'delegator_address', m.sender) AS delegator_address,
  COALESCE(mr.data->>'validatorAddress', mr.data->>'validator_address', '') AS validator_address,
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
AND COALESCE(mr.data->>'validatorAddress', mr.data->>'validator_address', '') <> ''
AND NOT EXISTS (
  SELECT 1 FROM api.delegation_events de
  WHERE de.tx_hash = t.id AND de.event_type = 'DELEGATE'
  AND de.validator_address = COALESCE(mr.data->>'validatorAddress', mr.data->>'validator_address', '')
);

-- ============================================================================
-- Backfill UNDELEGATE events
-- ============================================================================

INSERT INTO api.delegation_events (
  event_type, delegator_address, validator_address,
  amount, denom, tx_hash, height, timestamp
)
SELECT
  'UNDELEGATE' AS event_type,
  COALESCE(mr.data->>'delegatorAddress', mr.data->>'delegator_address', m.sender) AS delegator_address,
  COALESCE(mr.data->>'validatorAddress', mr.data->>'validator_address', '') AS validator_address,
  NULLIF(COALESCE(mr.data->'amount'->>'amount', mr.data->'coin'->>'amount'), '')::numeric AS amount,
  COALESCE(mr.data->'amount'->>'denom', mr.data->'coin'->>'denom') AS denom,
  t.id AS tx_hash,
  t.height,
  t.timestamp
FROM api.messages_main m
JOIN api.transactions_main t ON m.id = t.id
LEFT JOIN api.messages_raw mr ON m.id = mr.id AND m.message_index = mr.message_index
WHERE m.type LIKE '%MsgUndelegate'
AND COALESCE(mr.data->>'validatorAddress', mr.data->>'validator_address', '') <> ''
AND NOT EXISTS (
  SELECT 1 FROM api.delegation_events de
  WHERE de.tx_hash = t.id AND de.event_type = 'UNDELEGATE'
  AND de.validator_address = COALESCE(mr.data->>'validatorAddress', mr.data->>'validator_address', '')
);

-- ============================================================================
-- Backfill REDELEGATE events
-- ============================================================================

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
AND NOT EXISTS (
  SELECT 1 FROM api.delegation_events de
  WHERE de.tx_hash = t.id AND de.event_type = 'REDELEGATE'
);

-- ============================================================================
-- Update trigger to handle both camelCase and snake_case
-- ============================================================================

CREATE OR REPLACE FUNCTION api.detect_staking_messages()
RETURNS TRIGGER AS $$
DECLARE
  msg_record RECORD;
  raw_data JSONB;
  val_addr TEXT;
  del_addr TEXT;
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

    val_addr := COALESCE(raw_data->>'validatorAddress', raw_data->>'validator_address', '');
    del_addr := COALESCE(raw_data->>'delegatorAddress', raw_data->>'delegator_address', msg_record.sender);

    -- MsgDelegate
    IF msg_record.type LIKE '%MsgDelegate' AND msg_record.type NOT LIKE '%MsgBeginRedelegate' THEN
      INSERT INTO api.delegation_events (
        event_type, delegator_address, validator_address,
        amount, denom, tx_hash, height, timestamp
      ) VALUES (
        'DELEGATE', del_addr, val_addr,
        COALESCE(raw_data->'amount'->>'amount', raw_data->'coin'->>'amount'),
        COALESCE(raw_data->'amount'->>'denom', raw_data->'coin'->>'denom'),
        NEW.id, NEW.height, NEW.timestamp
      );

    -- MsgUndelegate
    ELSIF msg_record.type LIKE '%MsgUndelegate' THEN
      INSERT INTO api.delegation_events (
        event_type, delegator_address, validator_address,
        amount, denom, tx_hash, height, timestamp
      ) VALUES (
        'UNDELEGATE', del_addr, val_addr,
        COALESCE(raw_data->'amount'->>'amount', raw_data->'coin'->>'amount'),
        COALESCE(raw_data->'amount'->>'denom', raw_data->'coin'->>'denom'),
        NEW.id, NEW.height, NEW.timestamp
      );

    -- MsgBeginRedelegate
    ELSIF msg_record.type LIKE '%MsgBeginRedelegate' THEN
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
        NEW.id, NEW.height, NEW.timestamp
      );

    -- MsgCreateValidator
    ELSIF msg_record.type LIKE '%MsgCreateValidator' THEN
      INSERT INTO api.delegation_events (
        event_type, delegator_address, validator_address,
        amount, denom, tx_hash, height, timestamp
      ) VALUES (
        'CREATE_VALIDATOR', del_addr, val_addr,
        COALESCE(raw_data->'value'->>'amount', raw_data->'selfDelegation'->>'amount', raw_data->'self_delegation'->>'amount'),
        COALESCE(raw_data->'value'->>'denom', raw_data->'selfDelegation'->>'denom', raw_data->'self_delegation'->>'denom'),
        NEW.id, NEW.height, NEW.timestamp
      );

      -- Upsert validator with creation_height
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
        NULLIF(COALESCE(raw_data->'commission'->'commissionRates'->>'rate', raw_data->'commission'->'commission_rates'->>'rate'), '')::NUMERIC,
        NULLIF(COALESCE(raw_data->'commission'->'commissionRates'->>'maxRate', raw_data->'commission'->'commission_rates'->>'max_rate'), '')::NUMERIC,
        NULLIF(COALESCE(raw_data->'commission'->'commissionRates'->>'maxChangeRate', raw_data->'commission'->'commission_rates'->>'max_change_rate'), '')::NUMERIC,
        NULLIF(COALESCE(raw_data->>'minSelfDelegation', raw_data->>'min_self_delegation'), '')::NUMERIC,
        NULLIF(COALESCE(raw_data->'value'->>'amount', raw_data->'selfDelegation'->>'amount'), '')::NUMERIC,
        'BOND_STATUS_BONDED',
        NEW.height,
        NEW.id
      )
      ON CONFLICT (operator_address) DO UPDATE SET
        moniker = COALESCE(EXCLUDED.moniker, api.validators.moniker),
        creation_height = COALESCE(api.validators.creation_height, EXCLUDED.creation_height),
        first_seen_tx = COALESCE(api.validators.first_seen_tx, EXCLUDED.first_seen_tx),
        updated_at = NOW();

    -- MsgEditValidator
    ELSIF msg_record.type LIKE '%MsgEditValidator' THEN
      INSERT INTO api.delegation_events (
        event_type, delegator_address, validator_address,
        tx_hash, height, timestamp
      ) VALUES (
        'EDIT_VALIDATOR', msg_record.sender, val_addr,
        NEW.id, NEW.height, NEW.timestamp
      );

      UPDATE api.validators SET
        moniker = COALESCE(NULLIF(COALESCE(raw_data->'description'->>'moniker', raw_data->>'moniker'), '[do-not-modify]'), moniker),
        identity = COALESCE(NULLIF(COALESCE(raw_data->'description'->>'identity', raw_data->>'identity'), '[do-not-modify]'), identity),
        website = COALESCE(NULLIF(COALESCE(raw_data->'description'->>'website', raw_data->>'website'), '[do-not-modify]'), website),
        details = COALESCE(NULLIF(COALESCE(raw_data->'description'->>'details', raw_data->>'details'), '[do-not-modify]'), details),
        updated_at = NOW()
      WHERE operator_address = val_addr;
    END IF;
  END LOOP;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Re-create trigger
DROP TRIGGER IF EXISTS trigger_detect_staking ON api.transactions_main;
CREATE TRIGGER trigger_detect_staking
  AFTER INSERT ON api.transactions_main
  FOR EACH ROW
  EXECUTE FUNCTION api.detect_staking_messages();

COMMIT;
