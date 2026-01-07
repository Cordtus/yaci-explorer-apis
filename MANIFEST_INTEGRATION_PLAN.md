# Manifest Indexer Integration Plan

## Overview

Integrate yaci-explorer-apis with the existing Manifest indexer at `https://indexer.manifest.network`. The indexer has the core data tables but is missing the RPC functions and analytics views that yaci-explorer frontend requires.

## Current State Analysis

### Manifest Indexer Has (Working)
- `blocks_raw` (id, data)
- `transactions_raw` (id, data)
- `transactions_main` (id, fee, memo, error, height, timestamp, proposal_ids)
- `messages_raw` (id, message_index, data)
- `messages_main` (id, message_index, type, sender, mentions, metadata)
- `events_raw` (id, event_index, data)
- `events_main` (id, event_index, attr_index, event_type, attr_key, attr_value, msg_index)
- 2 RPC functions: `extract_event_msg_index`, `get_messages_for_address`

### Frontend Requires (Missing from Manifest Indexer)

#### Critical RPC Functions
| Function | Purpose | Priority |
|----------|---------|----------|
| `get_transaction_detail` | Full tx with messages, events | P0 |
| `get_transactions_paginated` | Filtered tx listing | P0 |
| `get_transactions_by_address` | Address tx history | P0 |
| `get_address_stats` | Address activity stats | P0 |
| `get_blocks_paginated` | Block listing with filters | P0 |
| `universal_search` | Cross-entity search | P0 |
| `get_block_time_analysis` | Block timing stats | P1 |
| `get_governance_proposals` | Governance listing | P1 |
| `get_chain_params` | Chain parameters | P2 |
| `get_ibc_connections` | IBC data | P2 |
| `get_ibc_connection` | Single IBC connection | P2 |
| `get_ibc_denom_traces` | IBC denom traces | P2 |
| `resolve_ibc_denom` | IBC denom resolution | P2 |
| `get_ibc_chains` | IBC chain summary | P2 |

#### Analytics Views
| View | Purpose | Priority |
|------|---------|----------|
| `chain_stats` | Overall chain stats | P0 |
| `tx_volume_daily` | Daily tx counts | P1 |
| `tx_volume_hourly` | Hourly tx counts | P1 |
| `message_type_stats` | Message distribution | P1 |
| `tx_success_rate` | Success/failure rate | P2 |
| `fee_revenue` | Fee totals by denom | P2 |

#### Schema Additions
| Change | Purpose |
|--------|---------|
| `blocks_raw.tx_count` column | Block tx count denormalization |
| Indexes on core tables | Query performance |

### Not Needed for Manifest
- EVM tables (`evm_transactions`, `evm_logs`, etc.) - Manifest has no EVM
- EVM RPC functions (`request_evm_decode`, etc.)
- EVM views (`evm_tx_map`, `evm_pending_decode`)

## Implementation Plan

### Phase 1: Core Schema Updates (Migration 020)
Add missing column and indexes that don't conflict with existing data.

```sql
-- Add tx_count to blocks_raw if not exists
ALTER TABLE api.blocks_raw ADD COLUMN IF NOT EXISTS tx_count INT DEFAULT 0;

-- Add missing indexes
CREATE INDEX IF NOT EXISTS idx_tx_height ON api.transactions_main(height DESC);
CREATE INDEX IF NOT EXISTS idx_tx_timestamp ON api.transactions_main(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_msg_sender ON api.messages_main(sender);
CREATE INDEX IF NOT EXISTS idx_msg_mentions ON api.messages_main USING GIN(mentions);
CREATE INDEX IF NOT EXISTS idx_blocks_tx_count ON api.blocks_raw(tx_count) WHERE tx_count > 0;
```

### Phase 2: Analytics Views (Migration 021)
Create analytics views that work with existing schema.

```sql
-- chain_stats view
CREATE OR REPLACE VIEW api.chain_stats AS
SELECT
  (SELECT MAX(id) FROM api.blocks_raw) as latest_block,
  (SELECT COUNT(*) FROM api.transactions_main) as total_transactions,
  (SELECT COUNT(DISTINCT sender) FROM api.messages_main WHERE sender IS NOT NULL) as unique_addresses,
  0 as evm_transactions,  -- No EVM on Manifest
  0 as active_validators;

-- tx_volume_daily
CREATE OR REPLACE VIEW api.tx_volume_daily AS
SELECT
  DATE(timestamp) as date,
  COUNT(*) as count
FROM api.transactions_main
WHERE timestamp IS NOT NULL
GROUP BY DATE(timestamp)
ORDER BY date DESC;

-- tx_volume_hourly
CREATE OR REPLACE VIEW api.tx_volume_hourly AS
SELECT
  DATE_TRUNC('hour', timestamp) as hour,
  COUNT(*) as count
FROM api.transactions_main
WHERE timestamp >= NOW() - INTERVAL '7 days'
GROUP BY DATE_TRUNC('hour', timestamp)
ORDER BY hour DESC;

-- message_type_stats
CREATE OR REPLACE VIEW api.message_type_stats AS
SELECT
  type,
  COUNT(*) as count
FROM api.messages_main
GROUP BY type
ORDER BY count DESC;

-- tx_success_rate
CREATE OR REPLACE VIEW api.tx_success_rate AS
SELECT
  COUNT(*) as total,
  COUNT(*) FILTER (WHERE error IS NULL) as successful,
  COUNT(*) FILTER (WHERE error IS NOT NULL) as failed,
  ROUND(100.0 * COUNT(*) FILTER (WHERE error IS NULL) / NULLIF(COUNT(*), 0), 2) as success_rate_percent
FROM api.transactions_main;
```

### Phase 3: Core RPC Functions (Migration 022)
Add the critical RPC functions.

Key functions to port from existing migrations:
- `get_transaction_detail` (from 001_complete_schema.sql)
- `get_transactions_paginated` (from 004, 014, 016)
- `get_transactions_by_address` (from 005)
- `get_address_stats` (from 001)
- `get_blocks_paginated` (from 006, 009)
- `universal_search` (from 001, 017)
- `get_block_time_analysis` (from 001)

### Phase 4: Governance Functions (Migration 023)
Add governance-related functions if tables exist.

### Phase 5: IBC Functions (Migration 024)
Add IBC-related functions if tables exist.

## Migration Strategy

### Option A: Additive Migrations (Recommended)
Create new migrations (020+) that only ADD functionality without modifying existing objects. This is safe to run against a production database.

### Option B: Full Schema Replacement
Replace all migrations with a single idempotent schema file. More risky but cleaner.

## Testing Plan

1. Create a test database with Manifest indexer schema
2. Apply migrations sequentially
3. Verify each RPC function returns expected data
4. Test frontend against the updated API

## Deployment

Once migrations are tested:
1. Get database access to Manifest indexer
2. Run migrations via `yarn migrate`
3. PostgREST will automatically expose new functions

## Files Created

```
migrations/
  020_manifest_schema_updates.sql      # tx_count column + indexes + backfill
  021_manifest_analytics_views.sql     # Analytics views (chain_stats, tx_volume, etc.)
  022_manifest_core_functions.sql      # Core RPC functions (search, tx detail, pagination)
  023_manifest_ibc_functions.sql       # IBC tables and RPC functions
  024_manifest_governance_functions.sql # Governance tables and RPC functions
```

## Implementation Status

| Migration | Status | Description |
|-----------|--------|-------------|
| 020 | Complete | Schema updates (tx_count, indexes, backfill) |
| 021 | Complete | Analytics views |
| 022 | Complete | Core RPC functions |
| 023 | Complete | IBC tables and functions |
| 024 | Complete | Governance tables and functions |

## Notes

- All functions use `CREATE OR REPLACE` for idempotency
- All DDL uses `IF NOT EXISTS` for safe re-runs
- All permissions grant SELECT/EXECUTE to `web_anon` role
- EVM-related code excluded (Manifest has no EVM)
- Migrations are additive only - safe for production databases
