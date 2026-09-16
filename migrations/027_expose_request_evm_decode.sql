-- Expose request_evm_decode as a public RPC endpoint
-- The internal maybe_priority_decode() already exists (migration 010)
-- but was never granted to web_anon as a callable function.

BEGIN;

CREATE OR REPLACE FUNCTION api.request_evm_decode(_tx_hash text)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
BEGIN
  -- Check if this tx exists and is pending EVM decode
  IF EXISTS (SELECT 1 FROM api.evm_pending_decode WHERE tx_id = _tx_hash) THEN
    PERFORM pg_notify('evm_decode_priority', _tx_hash);
    RETURN jsonb_build_object('success', true);
  END IF;

  -- Already decoded or not an EVM tx
  RETURN jsonb_build_object('success', false);
END;
$$;

GRANT EXECUTE ON FUNCTION api.request_evm_decode(text) TO web_anon;

COMMIT;
