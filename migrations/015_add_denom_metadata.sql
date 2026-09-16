-- Add denom_metadata table for currency display names
-- Frontend uses this to map internal denoms (arai) to display symbols (RAI)

BEGIN;

CREATE TABLE IF NOT EXISTS api.denom_metadata (
  denom text PRIMARY KEY,
  symbol text NOT NULL,
  decimals int NOT NULL DEFAULT 18,
  ibc_hash text,
  description text
);

-- Seed with native denomination
INSERT INTO api.denom_metadata (denom, symbol, decimals, description)
VALUES ('arai', 'RAI', 18, 'Native token of Republic chain')
ON CONFLICT (denom) DO NOTHING;

GRANT SELECT ON api.denom_metadata TO web_anon;

COMMIT;
