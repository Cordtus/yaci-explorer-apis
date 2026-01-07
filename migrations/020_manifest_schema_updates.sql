-- =============================================================================
-- Manifest Indexer Schema Updates
-- Migration 020: Add missing column and indexes
-- Safe to run on existing database with populated data
-- =============================================================================

BEGIN;

-- Add tx_count column to blocks_raw if not exists
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'api' AND table_name = 'blocks_raw' AND column_name = 'tx_count'
  ) THEN
    ALTER TABLE api.blocks_raw ADD COLUMN tx_count INT DEFAULT 0;
  END IF;
END $$;

-- Core indexes for query performance
CREATE INDEX IF NOT EXISTS idx_tx_height ON api.transactions_main(height DESC);
CREATE INDEX IF NOT EXISTS idx_tx_timestamp ON api.transactions_main(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_tx_error_not_null ON api.transactions_main(id) WHERE error IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_msg_type ON api.messages_main(type);
CREATE INDEX IF NOT EXISTS idx_msg_sender ON api.messages_main(sender);
CREATE INDEX IF NOT EXISTS idx_msg_mentions ON api.messages_main USING GIN(mentions);
CREATE INDEX IF NOT EXISTS idx_messages_id ON api.messages_main(id);
CREATE INDEX IF NOT EXISTS idx_event_type ON api.events_main(event_type);
CREATE INDEX IF NOT EXISTS idx_events_id ON api.events_main(id);
CREATE INDEX IF NOT EXISTS idx_transactions_height ON api.transactions_main(height);
CREATE INDEX IF NOT EXISTS idx_blocks_tx_count ON api.blocks_raw(tx_count) WHERE tx_count > 0;

-- Update tx_count for existing blocks (one-time backfill)
UPDATE api.blocks_raw b
SET tx_count = (
  SELECT COUNT(*)
  FROM api.transactions_main t
  WHERE t.height = b.id
)
WHERE b.tx_count = 0 OR b.tx_count IS NULL;

COMMIT;
