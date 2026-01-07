-- =============================================================================
-- Manifest Indexer Governance Functions
-- Migration 024: Governance tables and RPC functions
-- Safe to run on existing database with populated data
-- =============================================================================

BEGIN;

-- =============================================================================
-- GOVERNANCE TABLES (create if not exist)
-- =============================================================================

-- Governance proposals table (populated by trigger from indexed data)
CREATE TABLE IF NOT EXISTS api.governance_proposals (
  proposal_id BIGINT PRIMARY KEY,
  submit_tx_hash TEXT NOT NULL,
  submit_height BIGINT NOT NULL,
  submit_time TIMESTAMPTZ NOT NULL,
  proposer TEXT,
  title TEXT,
  summary TEXT,
  metadata TEXT,
  proposal_type TEXT,
  status TEXT NOT NULL DEFAULT 'PROPOSAL_STATUS_DEPOSIT_PERIOD',
  deposit_end_time TIMESTAMPTZ,
  voting_start_time TIMESTAMPTZ,
  voting_end_time TIMESTAMPTZ,
  yes_count TEXT,
  no_count TEXT,
  abstain_count TEXT,
  no_with_veto_count TEXT,
  last_updated TIMESTAMPTZ DEFAULT NOW(),
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_proposals_status ON api.governance_proposals(status);
CREATE INDEX IF NOT EXISTS idx_proposals_voting_end ON api.governance_proposals(voting_end_time);
CREATE INDEX IF NOT EXISTS idx_proposals_submit_time ON api.governance_proposals(submit_time DESC);

-- Governance snapshots for historical tally tracking
CREATE TABLE IF NOT EXISTS api.governance_snapshots (
  id SERIAL PRIMARY KEY,
  proposal_id BIGINT REFERENCES api.governance_proposals(proposal_id),
  status TEXT NOT NULL,
  yes_count TEXT NOT NULL,
  no_count TEXT NOT NULL,
  abstain_count TEXT NOT NULL,
  no_with_veto_count TEXT NOT NULL,
  total_voting_power TEXT,
  snapshot_time TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(proposal_id, snapshot_time)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_proposal ON api.governance_snapshots(proposal_id, snapshot_time DESC);

-- =============================================================================
-- GOVERNANCE RPC FUNCTIONS
-- =============================================================================

-- Get governance proposals with pagination
CREATE OR REPLACE FUNCTION api.get_governance_proposals(
  _limit INT DEFAULT 20,
  _offset INT DEFAULT 0,
  _status TEXT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE SQL STABLE
AS $$
  WITH filtered AS (
    SELECT p.*
    FROM api.governance_proposals p
    WHERE (_status IS NULL OR p.status = _status)
    ORDER BY p.proposal_id DESC
    LIMIT _limit OFFSET _offset
  ),
  total AS (
    SELECT COUNT(*) AS count
    FROM api.governance_proposals
    WHERE (_status IS NULL OR status = _status)
  ),
  with_snapshots AS (
    SELECT
      f.*,
      s.snapshot_time AS last_snapshot_time
    FROM filtered f
    LEFT JOIN LATERAL (
      SELECT snapshot_time
      FROM api.governance_snapshots
      WHERE proposal_id = f.proposal_id
      ORDER BY snapshot_time DESC
      LIMIT 1
    ) s ON TRUE
  )
  SELECT jsonb_build_object(
    'data', COALESCE(jsonb_agg(
      jsonb_build_object(
        'proposal_id', ws.proposal_id,
        'title', ws.title,
        'summary', ws.summary,
        'status', ws.status,
        'submit_time', ws.submit_time,
        'deposit_end_time', ws.deposit_end_time,
        'voting_start_time', ws.voting_start_time,
        'voting_end_time', ws.voting_end_time,
        'proposer', ws.proposer,
        'tally', jsonb_build_object(
          'yes', ws.yes_count,
          'no', ws.no_count,
          'abstain', ws.abstain_count,
          'no_with_veto', ws.no_with_veto_count
        ),
        'last_updated', ws.last_updated,
        'last_snapshot_time', ws.last_snapshot_time
      ) ORDER BY ws.proposal_id DESC
    ), '[]'::jsonb),
    'pagination', jsonb_build_object(
      'total', (SELECT count FROM total),
      'limit', _limit,
      'offset', _offset,
      'has_next', _offset + _limit < (SELECT count FROM total),
      'has_prev', _offset > 0
    )
  )
  FROM with_snapshots ws;
$$;

-- Compute proposal tally from indexed votes
CREATE OR REPLACE FUNCTION api.compute_proposal_tally(_proposal_id bigint)
RETURNS jsonb
LANGUAGE sql STABLE
AS $$
  SELECT jsonb_build_object(
    'yes', COUNT(*) FILTER (WHERE m.metadata->>'option' = 'VOTE_OPTION_YES'),
    'no', COUNT(*) FILTER (WHERE m.metadata->>'option' = 'VOTE_OPTION_NO'),
    'abstain', COUNT(*) FILTER (WHERE m.metadata->>'option' = 'VOTE_OPTION_ABSTAIN'),
    'no_with_veto', COUNT(*) FILTER (WHERE m.metadata->>'option' = 'VOTE_OPTION_NO_WITH_VETO')
  )
  FROM api.messages_main m
  WHERE m.type LIKE '%MsgVote%'
  AND (m.metadata->>'proposalId')::bigint = _proposal_id;
$$;

-- Active governance proposals view
CREATE OR REPLACE VIEW api.governance_active_proposals AS
SELECT
  proposal_id,
  status,
  voting_end_time,
  deposit_end_time
FROM api.governance_proposals
WHERE status IN (
  'PROPOSAL_STATUS_DEPOSIT_PERIOD',
  'PROPOSAL_STATUS_VOTING_PERIOD'
)
ORDER BY proposal_id DESC;

-- =============================================================================
-- CHAIN PARAMS TABLE AND FUNCTION
-- =============================================================================

-- Chain parameters table
CREATE TABLE IF NOT EXISTS api.chain_params (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Get chain parameters as JSON object
CREATE OR REPLACE FUNCTION api.get_chain_params()
RETURNS JSONB
LANGUAGE SQL STABLE
AS $$
  SELECT COALESCE(jsonb_object_agg(key, value), '{}'::jsonb)
  FROM api.chain_params;
$$;

-- =============================================================================
-- GRANT PERMISSIONS
-- =============================================================================

GRANT SELECT ON api.governance_proposals TO web_anon;
GRANT SELECT ON api.governance_snapshots TO web_anon;
GRANT SELECT ON api.governance_active_proposals TO web_anon;
GRANT SELECT ON api.chain_params TO web_anon;

GRANT EXECUTE ON FUNCTION api.get_governance_proposals(int, int, text) TO web_anon;
GRANT EXECUTE ON FUNCTION api.compute_proposal_tally(bigint) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_chain_params() TO web_anon;

COMMIT;
