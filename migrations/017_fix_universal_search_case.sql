-- Fix universal_search to use lowercase for tx hash lookup
-- The transactions_main table stores hashes in lowercase

BEGIN;

CREATE OR REPLACE FUNCTION api.universal_search(_query text)
RETURNS jsonb
LANGUAGE plpgsql STABLE
AS $$
DECLARE
  results jsonb := '[]'::jsonb;
  trimmed text := trim(_query);
  block_result jsonb;
  tx_result jsonb;
  evm_tx_result jsonb;
  addr_result jsonb;
BEGIN
  -- Check for block height (numeric)
  IF trimmed ~ '^\d+$' THEN
    SELECT jsonb_build_object(
      'type', 'block',
      'value', jsonb_build_object('id', id),
      'score', 100
    ) INTO block_result
    FROM api.blocks_raw
    WHERE id = trimmed::bigint;

    IF block_result IS NOT NULL THEN
      results := results || block_result;
    END IF;
  END IF;

  -- Check for EVM hash (0x prefix, 64 hex chars)
  IF trimmed ~* '^0x[a-f0-9]{64}$' THEN
    SELECT jsonb_build_object(
      'type', 'evm_transaction',
      'value', jsonb_build_object('tx_id', tx_id, 'hash', hash),
      'score', 100
    ) INTO evm_tx_result
    FROM api.evm_transactions
    WHERE hash = lower(trimmed);

    IF evm_tx_result IS NOT NULL THEN
      results := results || evm_tx_result;
    END IF;
  END IF;

  -- Check for Cosmos tx hash (64 hex, no 0x)
  -- Use lower() since transactions_main stores hashes in lowercase
  IF trimmed ~ '^[a-fA-F0-9]{64}$' THEN
    SELECT jsonb_build_object(
      'type', 'transaction',
      'value', jsonb_build_object('id', id),
      'score', 100
    ) INTO tx_result
    FROM api.transactions_main
    WHERE id = lower(trimmed);

    IF tx_result IS NOT NULL THEN
      results := results || tx_result;
    END IF;
  END IF;

  -- Check for EVM address (0x prefix, 40 hex chars)
  IF trimmed ~* '^0x[a-f0-9]{40}$' THEN
    results := results || jsonb_build_object(
      'type', 'evm_address',
      'value', jsonb_build_object('address', lower(trimmed)),
      'score', 90
    );
  END IF;

  -- Check for Cosmos address (bech32)
  IF trimmed ~ '^[a-z]+1[a-z0-9]{38,}$' THEN
    results := results || jsonb_build_object(
      'type', 'address',
      'value', jsonb_build_object('address', trimmed),
      'score', 90
    );
  END IF;

  RETURN results;
END;
$$;

COMMIT;
