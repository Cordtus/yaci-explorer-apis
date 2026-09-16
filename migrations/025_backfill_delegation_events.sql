-- Migration 025: Backfill delegation events from existing indexed data
-- Handles data that was indexed before the staking trigger was active

BEGIN;

-- ============================================================================
-- Backfill DELEGATE events from MsgDelegate
-- ============================================================================

INSERT INTO api.delegation_events (
  event_type, delegator_address, validator_address,
  amount, denom, tx_hash, height, timestamp
)
SELECT
  'DELEGATE' AS event_type,
  COALESCE(mr.data->>'delegatorAddress', m.sender) AS delegator_address,
  COALESCE(mr.data->>'validatorAddress', '') AS validator_address,
  NULLIF(mr.data->'amount'->>'amount', '')::numeric AS amount,
  mr.data->'amount'->>'denom' AS denom,
  t.id AS tx_hash,
  t.height,
  t.timestamp
FROM api.messages_main m
JOIN api.transactions_main t ON m.id = t.id
LEFT JOIN api.messages_raw mr ON m.id = mr.id AND m.message_index = mr.message_index
WHERE m.type LIKE '%MsgDelegate'
AND m.type NOT LIKE '%MsgBeginRedelegate'
AND NOT EXISTS (
  SELECT 1 FROM api.delegation_events de
  WHERE de.tx_hash = t.id AND de.event_type = 'DELEGATE'
  AND de.validator_address = COALESCE(mr.data->>'validatorAddress', '')
);

-- ============================================================================
-- Backfill UNDELEGATE events from MsgUndelegate
-- ============================================================================

INSERT INTO api.delegation_events (
  event_type, delegator_address, validator_address,
  amount, denom, tx_hash, height, timestamp
)
SELECT
  'UNDELEGATE' AS event_type,
  COALESCE(mr.data->>'delegatorAddress', m.sender) AS delegator_address,
  COALESCE(mr.data->>'validatorAddress', '') AS validator_address,
  NULLIF(mr.data->'amount'->>'amount', '')::numeric AS amount,
  mr.data->'amount'->>'denom' AS denom,
  t.id AS tx_hash,
  t.height,
  t.timestamp
FROM api.messages_main m
JOIN api.transactions_main t ON m.id = t.id
LEFT JOIN api.messages_raw mr ON m.id = mr.id AND m.message_index = mr.message_index
WHERE m.type LIKE '%MsgUndelegate'
AND NOT EXISTS (
  SELECT 1 FROM api.delegation_events de
  WHERE de.tx_hash = t.id AND de.event_type = 'UNDELEGATE'
  AND de.validator_address = COALESCE(mr.data->>'validatorAddress', '')
);

-- ============================================================================
-- Backfill REDELEGATE events from MsgBeginRedelegate
-- ============================================================================

INSERT INTO api.delegation_events (
  event_type, delegator_address, validator_address,
  src_validator_address, amount, denom,
  tx_hash, height, timestamp
)
SELECT
  'REDELEGATE' AS event_type,
  COALESCE(mr.data->>'delegatorAddress', m.sender) AS delegator_address,
  COALESCE(mr.data->>'validatorDstAddress', '') AS validator_address,
  COALESCE(mr.data->>'validatorSrcAddress', '') AS src_validator_address,
  NULLIF(mr.data->'amount'->>'amount', '')::numeric AS amount,
  mr.data->'amount'->>'denom' AS denom,
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
  AND de.validator_address = COALESCE(mr.data->>'validatorDstAddress', '')
);

-- ============================================================================
-- Backfill CREATE_VALIDATOR events from MsgCreateValidator
-- ============================================================================

INSERT INTO api.delegation_events (
  event_type, delegator_address, validator_address,
  amount, denom, tx_hash, height, timestamp
)
SELECT
  'CREATE_VALIDATOR' AS event_type,
  COALESCE(mr.data->>'delegatorAddress', m.sender) AS delegator_address,
  COALESCE(mr.data->>'validatorAddress', '') AS validator_address,
  NULLIF(mr.data->'value'->>'amount', '')::numeric AS amount,
  mr.data->'value'->>'denom' AS denom,
  t.id AS tx_hash,
  t.height,
  t.timestamp
FROM api.messages_main m
JOIN api.transactions_main t ON m.id = t.id
LEFT JOIN api.messages_raw mr ON m.id = mr.id AND m.message_index = mr.message_index
WHERE m.type LIKE '%MsgCreateValidator'
AND NOT EXISTS (
  SELECT 1 FROM api.delegation_events de
  WHERE de.tx_hash = t.id AND de.event_type = 'CREATE_VALIDATOR'
  AND de.validator_address = COALESCE(mr.data->>'validatorAddress', '')
);

-- Also upsert validator records from MsgCreateValidator
INSERT INTO api.validators (
  operator_address, moniker, identity, website, details,
  commission_rate, commission_max_rate, commission_max_change_rate,
  min_self_delegation, tokens, status,
  creation_height, first_seen_tx
)
SELECT DISTINCT ON (COALESCE(mr.data->>'validatorAddress', ''))
  COALESCE(mr.data->>'validatorAddress', '') AS operator_address,
  mr.data->'description'->>'moniker' AS moniker,
  mr.data->'description'->>'identity' AS identity,
  mr.data->'description'->>'website' AS website,
  mr.data->'description'->>'details' AS details,
  NULLIF(mr.data->'commission'->'commissionRates'->>'rate', '')::NUMERIC AS commission_rate,
  NULLIF(mr.data->'commission'->'commissionRates'->>'maxRate', '')::NUMERIC AS commission_max_rate,
  NULLIF(mr.data->'commission'->'commissionRates'->>'maxChangeRate', '')::NUMERIC AS commission_max_change_rate,
  NULLIF(mr.data->>'minSelfDelegation', '')::NUMERIC AS min_self_delegation,
  NULLIF(mr.data->'value'->>'amount', '')::NUMERIC AS tokens,
  'BOND_STATUS_BONDED' AS status,
  t.height AS creation_height,
  t.id AS first_seen_tx
FROM api.messages_main m
JOIN api.transactions_main t ON m.id = t.id
LEFT JOIN api.messages_raw mr ON m.id = mr.id AND m.message_index = mr.message_index
WHERE m.type LIKE '%MsgCreateValidator'
AND mr.data->>'validatorAddress' IS NOT NULL
ORDER BY COALESCE(mr.data->>'validatorAddress', ''), t.height ASC
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

-- Apply edits from MsgEditValidator (take the latest per validator)
UPDATE api.validators v SET
  moniker = COALESCE(
    NULLIF(mr.data->'description'->>'moniker', '[do-not-modify]'),
    v.moniker
  ),
  identity = COALESCE(
    NULLIF(mr.data->'description'->>'identity', '[do-not-modify]'),
    v.identity
  ),
  website = COALESCE(
    NULLIF(mr.data->'description'->>'website', '[do-not-modify]'),
    v.website
  ),
  details = COALESCE(
    NULLIF(mr.data->'description'->>'details', '[do-not-modify]'),
    v.details
  ),
  commission_rate = COALESCE(
    NULLIF(mr.data->>'commissionRate', '')::NUMERIC,
    v.commission_rate
  ),
  updated_at = NOW()
FROM (
  SELECT DISTINCT ON (COALESCE(mr2.data->>'validatorAddress', ''))
    mr2.data, t2.height
  FROM api.messages_main m2
  JOIN api.transactions_main t2 ON m2.id = t2.id
  LEFT JOIN api.messages_raw mr2 ON m2.id = mr2.id AND m2.message_index = mr2.message_index
  WHERE m2.type LIKE '%MsgEditValidator'
  AND mr2.data->>'validatorAddress' IS NOT NULL
  ORDER BY COALESCE(mr2.data->>'validatorAddress', ''), t2.height DESC
) mr
WHERE v.operator_address = COALESCE(mr.data->>'validatorAddress', '');

-- Backfill EDIT_VALIDATOR events
INSERT INTO api.delegation_events (
  event_type, delegator_address, validator_address,
  tx_hash, height, timestamp
)
SELECT
  'EDIT_VALIDATOR' AS event_type,
  m.sender AS delegator_address,
  COALESCE(mr.data->>'validatorAddress', '') AS validator_address,
  t.id AS tx_hash,
  t.height,
  t.timestamp
FROM api.messages_main m
JOIN api.transactions_main t ON m.id = t.id
LEFT JOIN api.messages_raw mr ON m.id = mr.id AND m.message_index = mr.message_index
WHERE m.type LIKE '%MsgEditValidator'
AND NOT EXISTS (
  SELECT 1 FROM api.delegation_events de
  WHERE de.tx_hash = t.id AND de.event_type = 'EDIT_VALIDATOR'
  AND de.validator_address = COALESCE(mr.data->>'validatorAddress', '')
);

-- ============================================================================
-- Re-ensure trigger exists
-- ============================================================================

DROP TRIGGER IF EXISTS trigger_detect_staking ON api.transactions_main;
CREATE TRIGGER trigger_detect_staking
  AFTER INSERT ON api.transactions_main
  FOR EACH ROW
  EXECUTE FUNCTION api.detect_staking_messages();

COMMIT;
