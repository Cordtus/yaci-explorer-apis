-- Add governance proposal tracking with polling-based updates
-- Proposals detected from messages, enriched via chain API polling

BEGIN;

-- Core proposals table
CREATE TABLE IF NOT EXISTS api.governance_proposals (
  proposal_id BIGINT PRIMARY KEY,

  -- From initial message detection
  submit_tx_hash TEXT NOT NULL,
  submit_height BIGINT NOT NULL,
  submit_time TIMESTAMPTZ NOT NULL,
  proposer TEXT,

  -- From chain API enrichment
  title TEXT,
  summary TEXT,
  metadata TEXT,
  proposal_type TEXT,

  -- Status and timing
  status TEXT NOT NULL DEFAULT 'PROPOSAL_STATUS_DEPOSIT_PERIOD',
  deposit_end_time TIMESTAMPTZ,
  voting_start_time TIMESTAMPTZ,
  voting_end_time TIMESTAMPTZ,

  -- Current tally (updated by polling)
  yes_count TEXT,
  no_count TEXT,
  abstain_count TEXT,
  no_with_veto_count TEXT,

  -- Metadata
  last_updated TIMESTAMPTZ DEFAULT NOW(),
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Vote tally snapshots from polling
CREATE TABLE IF NOT EXISTS api.governance_snapshots (
  id SERIAL PRIMARY KEY,
  proposal_id BIGINT REFERENCES api.governance_proposals(proposal_id),

  -- Snapshot data
  status TEXT NOT NULL,
  yes_count TEXT NOT NULL,
  no_count TEXT NOT NULL,
  abstain_count TEXT NOT NULL,
  no_with_veto_count TEXT NOT NULL,

  -- Total voting power at snapshot time
  total_voting_power TEXT,

  -- Timing
  snapshot_time TIMESTAMPTZ DEFAULT NOW(),

  -- Constraints
  UNIQUE(proposal_id, snapshot_time)
);

-- Indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_proposals_status ON api.governance_proposals(status);
CREATE INDEX IF NOT EXISTS idx_proposals_voting_end ON api.governance_proposals(voting_end_time);
CREATE INDEX IF NOT EXISTS idx_proposals_submit_time ON api.governance_proposals(submit_time DESC);
CREATE INDEX IF NOT EXISTS idx_snapshots_proposal ON api.governance_snapshots(proposal_id, snapshot_time DESC);

-- Trigger function to detect proposal submissions
CREATE OR REPLACE FUNCTION api.detect_proposal_submission()
RETURNS TRIGGER AS $$
DECLARE
  msg_data JSONB;
  prop_id BIGINT;
BEGIN
  -- Check each message in the transaction
  FOR msg_data IN
    SELECT m.metadata
    FROM api.messages_main m
    WHERE m.id = NEW.id
    AND m.type LIKE '%MsgSubmitProposal%'
  LOOP
    -- Try to extract proposal_id from metadata
    -- Different message types have different structures
    prop_id := NULL;

    -- Try cosmos.gov.v1.MsgSubmitProposal
    IF msg_data ? 'proposalId' THEN
      prop_id := (msg_data->>'proposalId')::BIGINT;
    END IF;

    -- Try cosmos.group.v1.MsgSubmitProposal (uses events)
    IF prop_id IS NULL AND NEW.id = ANY(
      SELECT DISTINCT e.id
      FROM api.events_main e
      WHERE e.id = NEW.id
      AND e.event_type = 'submit_proposal'
      AND e.attr_key = 'proposal_id'
    ) THEN
      SELECT (e.attr_value)::BIGINT INTO prop_id
      FROM api.events_main e
      WHERE e.id = NEW.id
      AND e.event_type = 'submit_proposal'
      AND e.attr_key = 'proposal_id'
      LIMIT 1;
    END IF;

    -- If we found a proposal ID, insert it
    IF prop_id IS NOT NULL THEN
      INSERT INTO api.governance_proposals (
        proposal_id,
        submit_tx_hash,
        submit_height,
        submit_time,
        proposer
      ) VALUES (
        prop_id,
        NEW.id,
        NEW.height,
        NEW.timestamp,
        msg_data->>'proposer'
      )
      ON CONFLICT (proposal_id) DO NOTHING;
    END IF;
  END LOOP;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Attach trigger to transactions_main
DROP TRIGGER IF EXISTS trigger_detect_proposals ON api.transactions_main;
CREATE TRIGGER trigger_detect_proposals
  AFTER INSERT ON api.transactions_main
  FOR EACH ROW
  EXECUTE FUNCTION api.detect_proposal_submission();

-- View for active proposals (need polling)
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

-- Function to get proposals with latest snapshot
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

GRANT EXECUTE ON FUNCTION api.get_governance_proposals(INT, INT, TEXT) TO web_anon;
GRANT SELECT ON api.governance_proposals TO web_anon;
GRANT SELECT ON api.governance_snapshots TO web_anon;
GRANT SELECT ON api.governance_active_proposals TO web_anon;

COMMIT;
