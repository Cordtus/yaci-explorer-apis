-- Migration 010: Add automatic priority EVM decode on transaction detail view
-- When viewing tx detail for undecoded EVM tx, automatically bump to priority queue

BEGIN;

-- Helper function to check and trigger priority decode (internal use)
CREATE OR REPLACE FUNCTION api.maybe_priority_decode(_tx_id text)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  -- Check if this is a pending EVM tx that needs decoding
  IF EXISTS (SELECT 1 FROM api.evm_pending_decode WHERE tx_id = _tx_id) THEN
    PERFORM pg_notify('evm_decode_priority', _tx_id);
  END IF;
END;
$$;

-- Update get_transaction_detail to auto-trigger priority decode for pending EVM txs
CREATE OR REPLACE FUNCTION api.get_transaction_detail(_hash text)
RETURNS jsonb
LANGUAGE plpgsql STABLE
AS $$
DECLARE
  result jsonb;
BEGIN
  -- Auto-trigger priority decode if this is a pending EVM tx
  PERFORM api.maybe_priority_decode(_hash);

  -- Return transaction detail
  SELECT jsonb_build_object(
    'id', t.id,
    'fee', t.fee,
    'memo', t.memo,
    'error', t.error,
    'height', t.height,
    'timestamp', t.timestamp,
    'proposal_ids', t.proposal_ids,
    'messages', COALESCE(msg.messages, '[]'::jsonb),
    'events', COALESCE(evt.events, '[]'::jsonb),
    'evm_data', evm.evm,
    'evm_logs', COALESCE(logs.logs, '[]'::jsonb),
    'raw_data', r.data
  ) INTO result
  FROM api.transactions_main t
  LEFT JOIN api.transactions_raw r ON t.id = r.id
  LEFT JOIN LATERAL (
    SELECT jsonb_agg(
      jsonb_build_object(
        'id', m.id,
        'message_index', m.message_index,
        'type', m.type,
        'sender', m.sender,
        'mentions', m.mentions,
        'metadata', m.metadata,
        'data', mr.data
      ) ORDER BY m.message_index
    ) AS messages
    FROM api.messages_main m
    LEFT JOIN api.messages_raw mr ON m.id = mr.id AND m.message_index = mr.message_index
    WHERE m.id = _hash
  ) msg ON TRUE
  LEFT JOIN LATERAL (
    SELECT jsonb_agg(
      jsonb_build_object(
        'id', e.id,
        'event_index', e.event_index,
        'attr_index', e.attr_index,
        'event_type', e.event_type,
        'attr_key', e.attr_key,
        'attr_value', e.attr_value,
        'msg_index', e.msg_index
      ) ORDER BY e.event_index, e.attr_index
    ) AS events
    FROM api.events_main e
    WHERE e.id = _hash
  ) evt ON TRUE
  LEFT JOIN LATERAL (
    SELECT jsonb_build_object(
      'hash', ev.hash,
      'from', ev."from",
      'to', ev."to",
      'nonce', ev.nonce,
      'gasLimit', ev.gas_limit::text,
      'gasPrice', ev.gas_price::text,
      'maxFeePerGas', ev.max_fee_per_gas::text,
      'maxPriorityFeePerGas', ev.max_priority_fee_per_gas::text,
      'value', ev.value::text,
      'data', ev.data,
      'type', ev.type,
      'chainId', ev.chain_id::text,
      'gasUsed', ev.gas_used,
      'status', ev.status,
      'functionName', ev.function_name,
      'functionSignature', ev.function_signature
    ) AS evm
    FROM api.evm_transactions ev
    WHERE ev.tx_id = _hash
  ) evm ON TRUE
  LEFT JOIN LATERAL (
    SELECT jsonb_agg(
      jsonb_build_object(
        'logIndex', l.log_index,
        'address', l.address,
        'topics', l.topics,
        'data', l.data
      ) ORDER BY l.log_index
    ) AS logs
    FROM api.evm_logs l
    WHERE l.tx_id = _hash
  ) logs ON TRUE
  WHERE t.id = _hash;

  RETURN result;
END;
$$;

COMMIT;
