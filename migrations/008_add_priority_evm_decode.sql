-- Migration 008: Add priority EVM decode RPC function
-- Creates PostgREST endpoint for on-demand EVM transaction decoding

-- Create function to request priority EVM decoding
-- This sends a PostgreSQL notification that the worker listens for
CREATE OR REPLACE FUNCTION api.request_evm_decode(_tx_id text)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
  v_pending_count int;
  v_decoded_count int;
BEGIN
  -- Check if transaction exists in pending queue
  SELECT COUNT(*) INTO v_pending_count
  FROM api.evm_pending_decode
  WHERE tx_id = _tx_id;

  -- Check if already decoded
  SELECT COUNT(*) INTO v_decoded_count
  FROM api.evm_transactions
  WHERE tx_id = _tx_id;

  IF v_decoded_count > 0 THEN
    -- Already decoded
    RETURN jsonb_build_object(
      'success', true,
      'message', 'Transaction already decoded',
      'status', 'already_decoded'
    );
  END IF;

  IF v_pending_count = 0 THEN
    -- Not in pending queue and not decoded
    RETURN jsonb_build_object(
      'success', false,
      'message', 'Transaction not found in pending queue or decoded transactions',
      'status', 'not_found'
    );
  END IF;

  -- Send notification to trigger priority decode
  PERFORM pg_notify('evm_decode_priority', _tx_id);

  RETURN jsonb_build_object(
    'success', true,
    'message', 'Priority decode requested',
    'status', 'requested',
    'tx_id', _tx_id
  );
END;
$$;

GRANT EXECUTE ON FUNCTION api.request_evm_decode(text) TO web_anon;

COMMENT ON FUNCTION api.request_evm_decode IS
'Request priority decoding of an EVM transaction. Sends notification to worker for immediate processing.';
