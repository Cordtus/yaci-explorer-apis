-- Migration 013: Refactor governance to use indexed data instead of REST polling
-- All governance data comes from indexed transactions - no external API needed

BEGIN;

-- Update detect_proposal_submission to extract more data from the indexed message
CREATE OR REPLACE FUNCTION api.detect_proposal_submission()
RETURNS TRIGGER AS $$
DECLARE
  msg_record RECORD;
  raw_data JSONB;
  prop_id BIGINT;
  prop_title TEXT;
  prop_summary TEXT;
  prop_metadata TEXT;
BEGIN
  -- Check each message in the transaction for governance proposals
  FOR msg_record IN
    SELECT m.id, m.message_index, m.type, m.metadata, m.sender
    FROM api.messages_main m
    WHERE m.id = NEW.id
    AND m.type LIKE '%MsgSubmitProposal%'
  LOOP
    prop_id := NULL;
    prop_title := NULL;
    prop_summary := NULL;
    prop_metadata := NULL;

    -- Get raw message data for full content
    SELECT data INTO raw_data
    FROM api.messages_raw
    WHERE id = msg_record.id AND message_index = msg_record.message_index;

    -- Extract proposal_id from metadata or events
    IF msg_record.metadata ? 'proposalId' THEN
      prop_id := (msg_record.metadata->>'proposalId')::BIGINT;
    END IF;

    -- Fallback: get from submit_proposal event
    IF prop_id IS NULL THEN
      SELECT (e.attr_value)::BIGINT INTO prop_id
      FROM api.events_main e
      WHERE e.id = NEW.id
      AND e.event_type = 'submit_proposal'
      AND e.attr_key = 'proposal_id'
      LIMIT 1;
    END IF;

    -- Extract title and summary from raw message data
    IF raw_data IS NOT NULL THEN
      prop_title := raw_data->>'title';
      prop_summary := raw_data->>'summary';
      prop_metadata := raw_data->>'metadata';
    END IF;

    IF prop_id IS NOT NULL THEN
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
      ) VALUES (
        prop_id,
        NEW.id,
        NEW.height,
        NEW.timestamp,
        msg_record.sender,
        prop_title,
        prop_summary,
        prop_metadata,
        'PROPOSAL_STATUS_DEPOSIT_PERIOD'
      )
      ON CONFLICT (proposal_id) DO UPDATE SET
        title = COALESCE(EXCLUDED.title, api.governance_proposals.title),
        summary = COALESCE(EXCLUDED.summary, api.governance_proposals.summary),
        metadata = COALESCE(EXCLUDED.metadata, api.governance_proposals.metadata),
        last_updated = NOW();
    END IF;
  END LOOP;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Track votes from indexed MsgVote transactions
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
      -- Update vote tallies based on vote option
      UPDATE api.governance_proposals
      SET
        yes_count = CASE WHEN vote_option = 'VOTE_OPTION_YES'
          THEN COALESCE(yes_count::bigint, 0) + 1 END::TEXT,
        no_count = CASE WHEN vote_option = 'VOTE_OPTION_NO'
          THEN COALESCE(no_count::bigint, 0) + 1 END::TEXT,
        abstain_count = CASE WHEN vote_option = 'VOTE_OPTION_ABSTAIN'
          THEN COALESCE(abstain_count::bigint, 0) + 1 END::TEXT,
        no_with_veto_count = CASE WHEN vote_option = 'VOTE_OPTION_NO_WITH_VETO'
          THEN COALESCE(no_with_veto_count::bigint, 0) + 1 END::TEXT,
        status = 'PROPOSAL_STATUS_VOTING_PERIOD',
        last_updated = NOW()
      WHERE proposal_id = prop_id;
    END IF;
  END LOOP;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for vote tracking
DROP TRIGGER IF EXISTS trigger_track_votes ON api.transactions_main;
CREATE TRIGGER trigger_track_votes
  AFTER INSERT ON api.transactions_main
  FOR EACH ROW
  EXECUTE FUNCTION api.track_governance_vote();

-- Function to compute vote tallies from indexed votes (for accuracy)
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

GRANT EXECUTE ON FUNCTION api.compute_proposal_tally(bigint) TO web_anon;

COMMIT;
