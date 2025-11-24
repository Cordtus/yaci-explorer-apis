-- Migration: 004_denom_metadata.sql
-- Description: Table for storing denomination metadata

-- =============================================================================
-- Denom Metadata Table
-- Stores resolved IBC and native denomination metadata
-- Populated when new tokens are first seen/identified
-- =============================================================================

CREATE TABLE IF NOT EXISTS api.denom_metadata (
  denom TEXT PRIMARY KEY,
  symbol TEXT NOT NULL,
  ibc_hash TEXT,
  decimals INT DEFAULT 6,
  name TEXT,
  created_at TIMESTAMP DEFAULT NOW()
);

GRANT SELECT ON api.denom_metadata TO web_anon;
