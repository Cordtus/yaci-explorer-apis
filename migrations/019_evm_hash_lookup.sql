-- Migration: Support EVM tx hash lookup in get_transaction_detail
-- Allows direct navigation to /tx/0x... URLs with EVM transaction hashes

BEGIN;

-- Get transaction detail with EVM data
-- Accepts either Cosmos tx hash or EVM tx hash (0x-prefixed)
CREATE OR REPLACE FUNCTION api.get_transaction_detail(_hash text)
RETURNS jsonb
LANGUAGE sql STABLE
AS $$
  WITH
  resolved AS (
    -- Resolve EVM hash to Cosmos tx_id if needed, otherwise use input directly
    SELECT COALESCE(ev.tx_id, _hash) AS hash
    FROM (SELECT _hash AS input) i
    LEFT JOIN api.evm_transactions ev ON ev.hash = lower(_hash)
  ),
  tx_main AS (
    SELECT * FROM api.transactions_main WHERE id = (SELECT hash FROM resolved)
  ),
  tx_raw AS (
    SELECT * FROM api.transactions_raw WHERE id = (SELECT hash FROM resolved)
  ),
  tx_messages AS (
    SELECT jsonb_agg(
      jsonb_build_object(
        'id', m.id,
        'message_index', m.message_index,
        'type', m.type,
        'sender', m.sender,
        'mentions', m.mentions,
        'metadata', m.metadata,
        'data', r.data
      ) ORDER BY m.message_index
    ) AS messages
    FROM api.messages_main m
    LEFT JOIN api.messages_raw r ON m.id = r.id AND m.message_index = r.message_index
    WHERE m.id = (SELECT hash FROM resolved)
  ),
  tx_events AS (
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
    WHERE e.id = (SELECT hash FROM resolved)
  ),
  evm_data AS (
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
    WHERE ev.tx_id = (SELECT hash FROM resolved)
  ),
  evm_logs_data AS (
    SELECT jsonb_agg(
      jsonb_build_object(
        'logIndex', l.log_index,
        'address', l.address,
        'topics', l.topics,
        'data', l.data
      ) ORDER BY l.log_index
    ) AS logs
    FROM api.evm_logs l
    WHERE l.tx_id = (SELECT hash FROM resolved)
  )
  SELECT jsonb_build_object(
    'id', t.id,
    'fee', t.fee,
    'memo', t.memo,
    'error', t.error,
    'height', t.height,
    'timestamp', t.timestamp,
    'proposal_ids', t.proposal_ids,
    'messages', COALESCE(m.messages, '[]'::jsonb),
    'events', COALESCE(e.events, '[]'::jsonb),
    'evm_data', ev.evm,
    'evm_logs', COALESCE(el.logs, '[]'::jsonb),
    'raw_data', r.data
  )
  FROM tx_raw r
  LEFT JOIN tx_main t ON TRUE
  LEFT JOIN tx_messages m ON TRUE
  LEFT JOIN tx_events e ON TRUE
  LEFT JOIN evm_data ev ON TRUE
  LEFT JOIN evm_logs_data el ON TRUE;
$$;

COMMIT;
