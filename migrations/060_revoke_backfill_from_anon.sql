-- Revoke anonymous access to admin/backfill RPC functions.
-- These are operational tools that should only be called by authenticated
-- roles (e.g., postgres), not by the public-facing PostgREST anonymous role.
--
-- On-demand user-facing functions (request_evm_decode, request_validator_refresh,
-- maybe_priority_decode, compute_proposal_tally, get_* queries) remain accessible.

BEGIN;

-- backfill_block_signatures: bulk reprocesses block signatures (CPU-intensive)
REVOKE EXECUTE ON FUNCTION api.backfill_block_signatures(BIGINT, INTEGER) FROM web_anon;
REVOKE EXECUTE ON FUNCTION api.backfill_block_signatures(BIGINT, INTEGER) FROM PUBLIC;

-- backfill_jailing_events: bulk reprocesses jailing events
REVOKE EXECUTE ON FUNCTION api.backfill_jailing_events() FROM web_anon;
REVOKE EXECUTE ON FUNCTION api.backfill_jailing_events() FROM PUBLIC;

-- backfill_finalize_block_events: bulk reprocesses finalize block events
REVOKE EXECUTE ON FUNCTION api.backfill_finalize_block_events() FROM web_anon;
REVOKE EXECUTE ON FUNCTION api.backfill_finalize_block_events() FROM PUBLIC;

-- backfill_validator_consensus_addresses: bulk reprocesses consensus address mappings
REVOKE EXECUTE ON FUNCTION api.backfill_validator_consensus_addresses() FROM web_anon;
REVOKE EXECUTE ON FUNCTION api.backfill_validator_consensus_addresses() FROM PUBLIC;

-- refresh_analytics_views: triggers expensive materialized view refreshes
REVOKE EXECUTE ON FUNCTION api.refresh_analytics_views() FROM web_anon;
REVOKE EXECUTE ON FUNCTION api.refresh_analytics_views() FROM PUBLIC;

COMMIT;
