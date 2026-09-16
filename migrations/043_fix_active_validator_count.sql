-- Migration 043: Fix active validator count to exclude jailed validators
--
-- The validator_stats and chain_stats views incorrectly count jailed validators
-- as "active" because they only check status = 'BOND_STATUS_BONDED' without
-- excluding jailed = TRUE. This causes counts like "110 active validators"
-- when the chain max is 100 (because 10 are jailed).

BEGIN;

-- ============================================================================
-- Fix validator_stats view
-- ============================================================================

DROP VIEW IF EXISTS api.validator_stats CASCADE;

CREATE OR REPLACE VIEW api.validator_stats AS
SELECT
  (SELECT COUNT(*) FROM api.validators) AS total_validators,
  (SELECT COUNT(*) FROM api.validators WHERE status = 'BOND_STATUS_BONDED' AND NOT jailed) AS active_validators,
  (SELECT COUNT(*) FROM api.validators WHERE jailed = TRUE) AS jailed_validators,
  (SELECT COALESCE(SUM(tokens), 0) FROM api.validators WHERE status = 'BOND_STATUS_BONDED') AS total_bonded_tokens;

-- ============================================================================
-- Fix chain_stats view
-- ============================================================================

DROP VIEW IF EXISTS api.chain_stats CASCADE;

CREATE OR REPLACE VIEW api.chain_stats AS
SELECT
  (SELECT MAX(id) FROM api.blocks_raw) AS latest_block,
  (SELECT COUNT(*) FROM api.transactions_main) AS total_transactions,
  (
    SELECT COUNT(*) FROM (
      -- Message senders
      SELECT DISTINCT sender AS addr
      FROM api.messages_main
      WHERE sender IS NOT NULL
      UNION
      -- EVM from addresses
      SELECT DISTINCT "from" AS addr
      FROM api.evm_transactions
      UNION
      -- EVM to addresses
      SELECT DISTINCT "to" AS addr
      FROM api.evm_transactions
      WHERE "to" IS NOT NULL
      UNION
      -- Mentioned addresses (flatten array)
      SELECT DISTINCT unnest(mentions) AS addr
      FROM api.messages_main
      WHERE mentions IS NOT NULL
    ) all_addresses
  ) AS unique_addresses,
  (SELECT COUNT(*) FROM api.evm_transactions) AS evm_transactions,
  (SELECT COUNT(*) FROM api.validators WHERE status = 'BOND_STATUS_BONDED' AND NOT jailed) AS active_validators;

COMMIT;
