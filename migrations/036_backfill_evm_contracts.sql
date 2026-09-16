-- Migration: Backfill EVM contracts from existing contract deployment transactions
-- This identifies transactions where 'to' is null (contract deployments) and computes
-- the deployed contract address from sender address and nonce.
BEGIN;

-- Backfill evm_contracts from existing contract deployments
-- Contract address = keccak256(rlp([sender, nonce]))[12:32]
-- We use a PL/pgSQL function to compute this since it requires RLP encoding

-- First, create a helper function to compute contract address
-- Note: This is an approximation - for CREATE opcode, address = keccak256(rlp([sender, nonce]))[12:]
-- Since PostgreSQL doesn't have native keccak256, we'll compute it in the worker instead
-- This migration just sets up the structure

-- For now, we'll create a view that identifies missing contracts
CREATE OR REPLACE VIEW api.evm_missing_contracts AS
SELECT
    e.tx_id,
    e."from" as creator,
    e.nonce,
    e.data as bytecode,
    t.height as creation_height
FROM api.evm_transactions e
JOIN api.transactions_main t ON e.tx_id = t.id
WHERE e."to" IS NULL
  AND e.status = 1
  AND NOT EXISTS (
    SELECT 1 FROM api.evm_contracts c WHERE c.creation_tx = e.tx_id
  );

GRANT SELECT ON api.evm_missing_contracts TO web_anon;

-- Create a view for missing token metadata
CREATE OR REPLACE VIEW api.evm_tokens_missing_metadata AS
SELECT
    t.address,
    t.type,
    t.first_seen_tx,
    t.first_seen_height
FROM api.evm_tokens t
WHERE t.name IS NULL
   OR t.symbol IS NULL
   OR t.decimals IS NULL;

GRANT SELECT ON api.evm_tokens_missing_metadata TO web_anon;

-- Add an index to help with contract lookups
CREATE INDEX IF NOT EXISTS idx_evm_transactions_to_null
ON api.evm_transactions ("from", nonce)
WHERE "to" IS NULL;

-- Add index for contract lookups by creation_tx
CREATE INDEX IF NOT EXISTS idx_evm_contracts_creation_tx
ON api.evm_contracts (creation_tx);

COMMIT;
