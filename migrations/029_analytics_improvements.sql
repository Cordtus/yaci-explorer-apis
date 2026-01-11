-- =============================================================================
-- Analytics Improvements
-- Migration 029: Add daily active addresses view
-- =============================================================================

BEGIN;

-- Daily active addresses (unique senders per day)
CREATE OR REPLACE VIEW api.daily_active_addresses AS
SELECT
  DATE(timestamp) AS date,
  COUNT(DISTINCT sender) AS active_addresses
FROM api.messages_main
WHERE timestamp IS NOT NULL AND sender IS NOT NULL
GROUP BY DATE(timestamp)
ORDER BY date DESC;

-- Grant access
GRANT SELECT ON api.daily_active_addresses TO web_anon;

COMMIT;
