BEGIN;

-- Migration 069: Index on validator_rewards(validator_address)
--
-- get_validator_total_rewards queries WHERE validator_address IN (...) but the
-- only existing index is (height, validator_address). Without height in the
-- WHERE clause, Postgres falls back to sequential scans on 13.3M rows.

CREATE INDEX IF NOT EXISTS idx_validator_rewards_address
  ON api.validator_rewards(validator_address);

COMMIT;
