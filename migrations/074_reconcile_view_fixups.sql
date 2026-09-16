-- =============================================================================
-- Reconcile fixup: re-expand views that use `validators.*`
--
-- 044_populate_consensus_addresses.sql defines validators_with_consensus with
-- `v.*` before 047/051 add columns to api.validators, so on a fresh install the
-- view is frozen with the older column set until a second migration pass.
-- Recreating it here (after all column additions) makes a fresh install match
-- the steady state.
-- =============================================================================

BEGIN;

DROP VIEW IF EXISTS api.validators_with_consensus CASCADE;

CREATE OR REPLACE VIEW api.validators_with_consensus AS
SELECT
  v.*,
  COALESCE(v.consensus_address, vca.consensus_address) AS resolved_consensus_address
FROM api.validators v
LEFT JOIN api.validator_consensus_addresses vca
  ON vca.operator_address = v.operator_address;

GRANT SELECT ON api.validators_with_consensus TO web_anon;

COMMIT;
