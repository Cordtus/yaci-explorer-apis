-- Migration 045: Fix validator stats to show Active, Inactive, and Jailed separately
--
-- Categories:
-- - Active: BONDED AND NOT jailed (in active validator set)
-- - Inactive: (UNBONDED OR UNBONDING) AND NOT jailed (voluntarily left)
-- - Jailed: jailed = true (slashed, always results in unbonding)

BEGIN;

-- ============================================================================
-- Fix validator_stats view
-- Must DROP first because we're adding a new column (inactive_validators)
-- ============================================================================

DROP VIEW IF EXISTS api.validator_stats;

CREATE VIEW api.validator_stats AS
SELECT
  (SELECT COUNT(*) FROM api.validators) AS total_validators,
  (SELECT COUNT(*) FROM api.validators
   WHERE status = 'BOND_STATUS_BONDED' AND NOT jailed) AS active_validators,
  (SELECT COUNT(*) FROM api.validators
   WHERE status IN ('BOND_STATUS_UNBONDED', 'BOND_STATUS_UNBONDING') AND NOT jailed) AS inactive_validators,
  (SELECT COUNT(*) FROM api.validators
   WHERE jailed = TRUE) AS jailed_validators,
  (SELECT COALESCE(SUM(tokens), 0) FROM api.validators
   WHERE status = 'BOND_STATUS_BONDED') AS total_bonded_tokens;

GRANT SELECT ON api.validator_stats TO web_anon;

COMMIT;
