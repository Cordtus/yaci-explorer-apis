-- Fix proposal_ids column type to match Yaci's expectations
-- Yaci triggers return TEXT[], not BIGINT[]

BEGIN;

ALTER TABLE api.transactions_main
  ALTER COLUMN proposal_ids TYPE TEXT[] USING proposal_ids::TEXT[];

COMMIT;
