-- Migration 008: Add critical performance indexes and tx_count column
-- Addresses issues documented in PERFORMANCE_REVIEW.md

BEGIN;

-- Critical indexes for get_transactions_by_address
CREATE INDEX IF NOT EXISTS idx_messages_sender ON api.messages_main(sender);
CREATE INDEX IF NOT EXISTS idx_messages_mentions_gin ON api.messages_main USING GIN(mentions);

-- Indexes for joining messages/events in paginated queries
CREATE INDEX IF NOT EXISTS idx_messages_id ON api.messages_main(id);
CREATE INDEX IF NOT EXISTS idx_events_id ON api.events_main(id);

-- Index for transaction height lookups
CREATE INDEX IF NOT EXISTS idx_transactions_height ON api.transactions_main(height);

-- Index for timestamp filtering
CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON api.transactions_main(timestamp DESC);

-- Add tx_count column to blocks_raw for efficient block listing
ALTER TABLE api.blocks_raw ADD COLUMN IF NOT EXISTS tx_count INT DEFAULT 0;

-- Backfill existing tx_count values
UPDATE api.blocks_raw b
SET tx_count = (
  SELECT COUNT(*) FROM api.transactions_main t WHERE t.height = b.id
)
WHERE tx_count = 0 OR tx_count IS NULL;

-- Trigger function to maintain tx_count on new transactions
CREATE OR REPLACE FUNCTION api.update_block_tx_count()
RETURNS TRIGGER AS $$
BEGIN
  UPDATE api.blocks_raw
  SET tx_count = tx_count + 1
  WHERE id = NEW.height;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Attach trigger to transactions_main
DROP TRIGGER IF EXISTS trigger_update_block_tx_count ON api.transactions_main;
CREATE TRIGGER trigger_update_block_tx_count
  AFTER INSERT ON api.transactions_main
  FOR EACH ROW
  EXECUTE FUNCTION api.update_block_tx_count();

-- Index on tx_count for filtering blocks by transaction count
CREATE INDEX IF NOT EXISTS idx_blocks_tx_count ON api.blocks_raw(tx_count) WHERE tx_count > 0;

COMMIT;
