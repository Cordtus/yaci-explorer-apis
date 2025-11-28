-- Migration 019: Fix governance vote tracking and security improvements
--
-- Fixes:
-- 1. track_governance_vote() was nulling out other vote counts when updating
-- 2. Add missing index on transactions_main.error for status filtering
-- 3. Revoke refresh_analytics_views() from web_anon (DoS prevention)

BEGIN;

-- =============================================================================
-- FIX 1: Governance vote tracking - preserve existing vote counts
-- =============================================================================

CREATE OR REPLACE FUNCTION api.track_governance_vote()
RETURNS TRIGGER AS $$
DECLARE
  msg_record RECORD;
  prop_id BIGINT;
  vote_option TEXT;
BEGIN
  FOR msg_record IN
    SELECT m.id, m.metadata, m.sender
    FROM api.messages_main m
    WHERE m.id = NEW.id
    AND m.type LIKE '%MsgVote%'
  LOOP
    prop_id := (msg_record.metadata->>'proposalId')::BIGINT;
    vote_option := msg_record.metadata->>'option';

    IF prop_id IS NOT NULL AND vote_option IS NOT NULL THEN
      -- Update vote tallies, preserving existing counts for other options
      UPDATE api.governance_proposals
      SET
        yes_count = CASE
          WHEN vote_option = 'VOTE_OPTION_YES'
          THEN (COALESCE(yes_count::bigint, 0) + 1)::TEXT
          ELSE yes_count
        END,
        no_count = CASE
          WHEN vote_option = 'VOTE_OPTION_NO'
          THEN (COALESCE(no_count::bigint, 0) + 1)::TEXT
          ELSE no_count
        END,
        abstain_count = CASE
          WHEN vote_option = 'VOTE_OPTION_ABSTAIN'
          THEN (COALESCE(abstain_count::bigint, 0) + 1)::TEXT
          ELSE abstain_count
        END,
        no_with_veto_count = CASE
          WHEN vote_option = 'VOTE_OPTION_NO_WITH_VETO'
          THEN (COALESCE(no_with_veto_count::bigint, 0) + 1)::TEXT
          ELSE no_with_veto_count
        END,
        status = 'PROPOSAL_STATUS_VOTING_PERIOD',
        last_updated = NOW()
      WHERE proposal_id = prop_id;
    END IF;
  END LOOP;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- FIX 2: Add missing index for status filtering performance
-- =============================================================================

-- Partial index for failed transactions (typically fewer, faster lookups)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tx_error_not_null
ON api.transactions_main(id)
WHERE error IS NOT NULL;

-- =============================================================================
-- FIX 3: Revoke refresh_analytics_views from web_anon (security)
-- =============================================================================

-- Revoke public access to prevent DoS via expensive refresh operations
REVOKE EXECUTE ON FUNCTION api.refresh_analytics_views() FROM web_anon;

-- Create a dedicated role for analytics refresh (to be used by cron/admin)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analytics_admin') THEN
    CREATE ROLE analytics_admin NOLOGIN;
  END IF;
END
$$;

GRANT EXECUTE ON FUNCTION api.refresh_analytics_views() TO analytics_admin;

-- Note: To refresh analytics, connect as postgres or grant analytics_admin to a user:
-- GRANT analytics_admin TO your_admin_user;

COMMIT;
