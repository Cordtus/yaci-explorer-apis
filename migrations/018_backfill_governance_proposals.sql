-- Backfill governance_proposals from existing indexed data
-- This handles proposals that were indexed before triggers were active

BEGIN;

-- Backfill proposals from MsgSubmitProposal transactions
INSERT INTO api.governance_proposals (
  proposal_id,
  submit_tx_hash,
  submit_height,
  submit_time,
  proposer,
  title,
  summary,
  metadata,
  status
)
SELECT DISTINCT ON (e.attr_value::bigint)
  e.attr_value::bigint AS proposal_id,
  t.id AS submit_tx_hash,
  t.height AS submit_height,
  t.timestamp AS submit_time,
  m.metadata->>'proposer' AS proposer,
  mr.data->>'title' AS title,
  mr.data->>'summary' AS summary,
  mr.data->>'metadata' AS metadata,
  CASE
    WHEN EXISTS (
      SELECT 1 FROM api.events_main ev
      WHERE ev.id = t.id
      AND ev.event_type = 'submit_proposal'
      AND ev.attr_key = 'voting_period_start'
    ) THEN 'PROPOSAL_STATUS_VOTING_PERIOD'
    ELSE 'PROPOSAL_STATUS_DEPOSIT_PERIOD'
  END AS status
FROM api.events_main e
JOIN api.transactions_main t ON e.id = t.id
JOIN api.messages_main m ON t.id = m.id AND m.type LIKE '%MsgSubmitProposal%'
LEFT JOIN api.messages_raw mr ON m.id = mr.id AND m.message_index = mr.message_index
WHERE e.event_type = 'submit_proposal'
  AND e.attr_key = 'proposal_id'
  AND e.attr_value IS NOT NULL
ORDER BY e.attr_value::bigint, t.height
ON CONFLICT (proposal_id) DO UPDATE SET
  title = COALESCE(EXCLUDED.title, api.governance_proposals.title),
  summary = COALESCE(EXCLUDED.summary, api.governance_proposals.summary),
  metadata = COALESCE(EXCLUDED.metadata, api.governance_proposals.metadata),
  proposer = COALESCE(EXCLUDED.proposer, api.governance_proposals.proposer),
  last_updated = NOW();

-- Also ensure the trigger exists and is active
DROP TRIGGER IF EXISTS trigger_detect_proposals ON api.transactions_main;
CREATE TRIGGER trigger_detect_proposals
  AFTER INSERT ON api.transactions_main
  FOR EACH ROW
  EXECUTE FUNCTION api.detect_proposal_submission();

COMMIT;
