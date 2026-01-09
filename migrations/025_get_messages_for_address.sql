-- =============================================================================
-- Migration 025: Add get_messages_for_address function
-- Required by yaci metrics collectors (locked_tokens, etc.)
-- =============================================================================

BEGIN;

-- Drop existing function if it exists (for idempotency)
DROP FUNCTION IF EXISTS api.get_messages_for_address(text);

-- Create a composite type for the return value that includes transaction error
CREATE TYPE api.message_with_error AS (
  id TEXT,
  message_index INT,
  type TEXT,
  sender TEXT,
  mentions TEXT[],
  metadata JSONB,
  error TEXT
);

-- Function to get all messages for an address (used by yaci metrics collectors)
-- Returns messages where the address is sender or in the mentions array
-- Includes the transaction error field for filtering successful transactions
CREATE OR REPLACE FUNCTION api.get_messages_for_address(_address text)
RETURNS SETOF api.message_with_error
LANGUAGE sql STABLE
AS $$
  SELECT
    m.id,
    m.message_index,
    m.type,
    m.sender,
    m.mentions,
    m.metadata,
    t.error
  FROM api.messages_main m
  JOIN api.transactions_main t ON m.id = t.id
  WHERE m.sender = _address
     OR _address = ANY(m.mentions)
     OR m.metadata->>'toAddress' = _address
  ORDER BY t.height DESC, m.message_index;
$$;

-- Grant execute permission to web_anon role
GRANT EXECUTE ON FUNCTION api.get_messages_for_address(text) TO web_anon;

COMMIT;
