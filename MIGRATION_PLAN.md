# YACI EXPLORER MIDDLEWARE MIGRATION
## Master Implementation Plan

**Status**: PLANNING PHASE - Not yet implemented
**Last Updated**: 2024-11-23

---

## SYSTEM COMPONENTS

| Component | Repository | Purpose | Local Path |
|-----------|------------|---------|------------|
| **Indexer** | https://github.com/Cordtus/yaci | Data ingestion from chain gRPC to PostgreSQL (DO NOT MODIFY) | `~/repos/yaci` |
| **Middleware/API** | https://github.com/Cordtus/yaci-explorer-apis | SQL functions, views, caching, client package (PRIMARY FOCUS) | `~/repos/yaci-postgrest` -> rename to `yaci-explorer-apis` |
| **Frontend** | https://github.com/Cordtus/yaci-explorer | React UI consuming middleware API (CONSUMER) | `~/repos/yaci-explorer` |

### Component Responsibilities

**Indexer (DO NOT MODIFY)**
- External dependency - minimize maintenance overhead
- Connects to chain gRPC endpoint
- Writes raw blockchain data to PostgreSQL
- Creates triggers that parse raw data into main tables
- Provides base schema: blocks_raw, transactions_main, messages_main, events_main

**Middleware/API (PRIMARY FOCUS)**
- SQL functions for optimized queries (eliminate N+1 patterns)
- Pre-aggregated analytics views
- TypeScript client package for frontend consumption
- Caching layer (server-side)
- PostgREST deployment configuration

**Frontend (CONSUMER)**
- Pure presentation layer
- Imports client from middleware package
- Uses TanStack Query for client-side caching
- No business logic or data transformation

---

## IMPORTANT NOTES

This plan is a living document that must be:
1. **Expanded** - Each phase requires detailed sub-planning before implementation
2. **Verified** - No assumptions about completeness or correctness
3. **Tested** - Every task must be tested before marked complete
4. **Iterative** - Adjust based on discoveries during implementation

---

## EXECUTIVE SUMMARY

### Ultimate Goal
Transform the current three-component system (yaci-indexer -> PostgREST -> yaci-explorer) into a robust, scalable architecture where the **middleware layer** (yaci-explorer-apis) serves as the intelligent data access layer, handling all caching, aggregation, and business logic--freeing the frontend to be a pure presentation layer and keeping the indexer as a pure data ingestion engine.

### Key Principles
1. **Indexer remains untouched** - External dependency, minimize maintenance overhead
2. **Middleware owns all intelligence** - SQL functions, caching, aggregation, complex queries
3. **Frontend is thin** - Pure presentation, no business logic, no data transformation
4. **Database-level optimization** - Single round-trip queries via RPC functions
5. **Future-proofing** - Architecture supports eventual GraphQL layer, EVM enhancements

### Current Pain Points Being Solved
- N+1 query patterns causing 4+ round trips per address page
- Client-side pagination (fetching ALL data, slicing in JS)
- Dual redundant caching (client + TanStack Query)
- Browser-based analytics aggregation (fetching 10K+ rows)
- EVM decoding in browser (500KB+ ethers.js bundle)
- No singleton pattern (15+ independent API client instances)

---

## PHASE BREAKDOWN

### PHASE 0: Repository Setup & Renaming
### PHASE 1: SQL Migrations - Core Functions
### PHASE 2: SQL Migrations - Analytics & Views
### PHASE 3: Middleware Client Package
### PHASE 4: Caching Layer Implementation
### PHASE 5: Frontend Migration
### PHASE 6: Testing & Validation
### PHASE 7: Deployment & Cutover

---

## PHASE 0: REPOSITORY SETUP & RENAMING

### Objective
Establish the yaci-explorer-apis repository with proper structure, documentation, and CI/CD pipelines.

### Sub-Tasks

#### 0.1 Repository Renaming
- **Location**: `~/repos/yaci-postgrest` -> `~/repos/yaci-explorer-apis`
- **Remote**: https://github.com/Cordtus/yaci-explorer-apis
- **Actions**:
  - Rename local directory
  - Update git remote if needed
  - Update fly.toml app name
  - Update any CI/CD references

#### 0.2 Directory Structure Creation
```
yaci-explorer-apis/
├── .github/
│   └── workflows/
│       ├── deploy.yml           # Fly.io PostgREST deployment
│       ├── migrate.yml          # SQL migration runner
│       └── test.yml             # Integration tests
├── docker/
│   ├── Dockerfile               # PostgREST container (existing)
│   └── docker-compose.dev.yml   # Local development stack
├── migrations/
│   ├── 001_address_functions.sql
│   ├── 002_analytics_views.sql
│   ├── 003_search_functions.sql
│   ├── 004_caching_tables.sql
│   └── migrate.sh               # Migration runner
├── packages/
│   └── client/
│       ├── src/
│       │   ├── client.ts        # Main API client
│       │   ├── types.ts         # Type definitions
│       │   ├── cache.ts         # Cache utilities
│       │   └── index.ts         # Exports
│       ├── package.json
│       ├── tsconfig.json
│       └── README.md
├── scripts/
│   ├── setup-dev.sh             # Development environment setup
│   └── seed-test-data.sh        # Test data seeding
├── fly.toml                     # Updated app config
├── package.json                 # Workspace root
├── tsconfig.json                # Root TypeScript config
└── README.md                    # Comprehensive documentation
```

#### 0.3 Documentation Creation
- **README.md**: Architecture overview, setup instructions, API reference
- **MIGRATION.md**: Step-by-step migration guide from current system
- **CLAUDE.md**: AI assistant context for the middleware repo

#### 0.4 CI/CD Pipeline Setup
- **deploy.yml**: Deploy PostgREST to Fly.io on push to main
- **migrate.yml**: Run SQL migrations against production DB
- **test.yml**: Run integration tests against test database

### Dependencies
- None (this is the foundation)

### Success Criteria
- [ ] Repository renamed and accessible
- [ ] Directory structure created
- [ ] CI/CD pipelines defined (even if not yet functional)
- [ ] Basic documentation in place

### Estimated Effort
- 2-3 hours

---

## PHASE 1: SQL MIGRATIONS - CORE FUNCTIONS

### Objective
Create PostgreSQL functions that eliminate N+1 query patterns and provide single-round-trip data access for all primary use cases.

### Sub-Tasks

#### 1.1 Address Functions (`001_address_functions.sql`)

**Function: `api.get_transactions_by_address()`**

**Purpose**: Replace the current N+1 pattern that:
1. Fetches ALL messages for an address
2. Extracts unique tx IDs in JavaScript
3. Slices for pagination in JavaScript
4. Makes separate queries for transactions, messages, events

**Current Code Being Replaced** (from `client.ts:565-645`):
```typescript
async getTransactionsByAddress(address, limit, offset) {
  // Query 1: Get ALL messages
  const { data: addressMessages } = await this.query('messages_main', {...})

  // JavaScript: Extract and dedupe
  const allTxIds = [...new Set(addressMessages.map(msg => msg.id))]

  // JavaScript: Client-side pagination!
  const paginatedTxIds = allTxIds.slice(offset, offset + limit)

  // Query 2, 3, 4: Batch fetch tx, messages, events
  const [txResult, msgResult, eventResult] = await Promise.all([...])

  // JavaScript: Assemble results
  // ...
}
```

**New SQL Function**:
```sql
CREATE OR REPLACE FUNCTION api.get_transactions_by_address(
  _address text,
  _limit int DEFAULT 50,
  _offset int DEFAULT 0
) RETURNS jsonb
LANGUAGE sql STABLE
AS $$
  -- Single query with CTEs for efficiency
  WITH
  -- Step 1: Find all unique tx IDs for this address (using indexes)
  addr_tx_ids AS (
    SELECT DISTINCT m.id
    FROM api.messages_main m
    WHERE m.sender = _address
       OR _address = ANY(m.mentions)
  ),
  -- Step 2: Get total count for pagination metadata
  total_count AS (
    SELECT COUNT(*)::int AS cnt FROM addr_tx_ids
  ),
  -- Step 3: Paginate at database level
  paginated_txs AS (
    SELECT t.*
    FROM api.transactions_main t
    JOIN addr_tx_ids a ON t.id = a.id
    ORDER BY t.height DESC
    LIMIT _limit OFFSET _offset
  ),
  -- Step 4: Aggregate messages for paginated txs
  tx_messages AS (
    SELECT
      m.id,
      jsonb_agg(
        jsonb_build_object(
          'id', m.id,
          'message_index', m.message_index,
          'type', m.type,
          'sender', m.sender,
          'mentions', m.mentions,
          'metadata', m.metadata
        ) ORDER BY m.message_index
      ) AS messages
    FROM api.messages_main m
    WHERE m.id IN (SELECT id FROM paginated_txs)
    GROUP BY m.id
  ),
  -- Step 5: Aggregate events for paginated txs
  tx_events AS (
    SELECT
      e.id,
      jsonb_agg(
        jsonb_build_object(
          'id', e.id,
          'event_index', e.event_index,
          'attr_index', e.attr_index,
          'event_type', e.event_type,
          'attr_key', e.attr_key,
          'attr_value', e.attr_value,
          'msg_index', e.msg_index
        ) ORDER BY e.event_index, e.attr_index
      ) AS events
    FROM api.events_main e
    WHERE e.id IN (SELECT id FROM paginated_txs)
    GROUP BY e.id
  )
  -- Step 6: Assemble final response
  SELECT jsonb_build_object(
    'data', COALESCE((
      SELECT jsonb_agg(
        jsonb_build_object(
          'id', t.id,
          'fee', t.fee,
          'memo', t.memo,
          'error', t.error,
          'height', t.height,
          'timestamp', t.timestamp,
          'proposal_ids', t.proposal_ids,
          'messages', COALESCE(m.messages, '[]'::jsonb),
          'events', COALESCE(e.events, '[]'::jsonb),
          'ingest_error', NULL
        ) ORDER BY t.height DESC
      )
      FROM paginated_txs t
      LEFT JOIN tx_messages m ON t.id = m.id
      LEFT JOIN tx_events e ON t.id = e.id
    ), '[]'::jsonb),
    'pagination', jsonb_build_object(
      'total', (SELECT cnt FROM total_count),
      'limit', _limit,
      'offset', _offset,
      'has_next', _offset + _limit < (SELECT cnt FROM total_count),
      'has_prev', _offset > 0
    )
  );
$$;

GRANT EXECUTE ON FUNCTION api.get_transactions_by_address(text, int, int) TO web_anon;
```

**Performance Improvement**:
- Before: 4+ round trips, fetches ALL messages (could be 10K+), client-side processing
- After: 1 round trip, database-level pagination, index-optimized

---

**Function: `api.get_address_stats()`**

**Purpose**: Replace the current multi-query pattern for address statistics.

**New SQL Function**:
```sql
CREATE OR REPLACE FUNCTION api.get_address_stats(_address text)
RETURNS jsonb
LANGUAGE sql STABLE
AS $$
  WITH addr_activity AS (
    SELECT
      m.id,
      m.sender,
      t.timestamp
    FROM api.messages_main m
    JOIN api.transactions_main t ON m.id = t.id
    WHERE m.sender = _address
       OR _address = ANY(m.mentions)
  ),
  aggregated AS (
    SELECT
      COUNT(DISTINCT id) AS transaction_count,
      MIN(timestamp) AS first_seen,
      MAX(timestamp) AS last_seen,
      COUNT(*) FILTER (WHERE sender = _address) AS total_sent,
      COUNT(*) FILTER (WHERE sender IS DISTINCT FROM _address) AS total_received
    FROM addr_activity
  )
  SELECT jsonb_build_object(
    'address', _address,
    'transaction_count', transaction_count,
    'first_seen', first_seen,
    'last_seen', last_seen,
    'total_sent', total_sent,
    'total_received', total_received
  )
  FROM aggregated;
$$;

GRANT EXECUTE ON FUNCTION api.get_address_stats(text) TO web_anon;
```

---

**Function: `api.get_transaction_detail()`**

**Purpose**: Single query for full transaction detail including messages, events, and EVM data.

```sql
CREATE OR REPLACE FUNCTION api.get_transaction_detail(_hash text)
RETURNS jsonb
LANGUAGE sql STABLE
AS $$
  WITH
  tx_main AS (
    SELECT * FROM api.transactions_main WHERE id = _hash
  ),
  tx_raw AS (
    SELECT * FROM api.transactions_raw WHERE id = _hash
  ),
  tx_messages AS (
    SELECT jsonb_agg(
      jsonb_build_object(
        'id', m.id,
        'message_index', m.message_index,
        'type', m.type,
        'sender', m.sender,
        'mentions', m.mentions,
        'metadata', m.metadata,
        'data', r.data
      ) ORDER BY m.message_index
    ) AS messages
    FROM api.messages_main m
    LEFT JOIN api.messages_raw r ON m.id = r.id AND m.message_index = r.message_index
    WHERE m.id = _hash
  ),
  tx_events AS (
    SELECT jsonb_agg(
      jsonb_build_object(
        'id', e.id,
        'event_index', e.event_index,
        'attr_index', e.attr_index,
        'event_type', e.event_type,
        'attr_key', e.attr_key,
        'attr_value', e.attr_value,
        'msg_index', e.msg_index
      ) ORDER BY e.event_index, e.attr_index
    ) AS events
    FROM api.events_main e
    WHERE e.id = _hash
  ),
  evm_data AS (
    SELECT jsonb_build_object(
      'ethereum_tx_hash', MAX(CASE WHEN e.attr_key = 'ethereumTxHash' THEN e.attr_value END),
      'recipient', MAX(CASE WHEN e.attr_key = 'recipient' THEN e.attr_value END),
      'gas_used', MAX(CASE WHEN e.attr_key = 'txGasUsed' THEN NULLIF(e.attr_value, '')::bigint END),
      'tx_type', MAX(CASE WHEN e.attr_key = 'txType' THEN NULLIF(e.attr_value, '')::int END)
    ) AS evm
    FROM api.events_main e
    WHERE e.id = _hash AND e.event_type = 'ethereum_tx'
  )
  SELECT jsonb_build_object(
    'id', t.id,
    'fee', t.fee,
    'memo', t.memo,
    'error', t.error,
    'height', t.height,
    'timestamp', t.timestamp,
    'proposal_ids', t.proposal_ids,
    'messages', COALESCE(m.messages, '[]'::jsonb),
    'events', COALESCE(e.events, '[]'::jsonb),
    'evm_data', CASE WHEN (ev.evm->>'ethereum_tx_hash') IS NOT NULL THEN ev.evm ELSE NULL END,
    'raw_data', r.data,
    'ingest_error', CASE
      WHEN t.id IS NULL AND r.data ? 'error' THEN
        jsonb_build_object(
          'message', r.data->>'error',
          'reason', r.data->>'reason',
          'hash', _hash
        )
      ELSE NULL
    END
  )
  FROM tx_raw r
  LEFT JOIN tx_main t ON TRUE
  LEFT JOIN tx_messages m ON TRUE
  LEFT JOIN tx_events e ON TRUE
  LEFT JOIN evm_data ev ON TRUE;
$$;

GRANT EXECUTE ON FUNCTION api.get_transaction_detail(text) TO web_anon;
```

---

#### 1.2 Transactions Functions

**Function: `api.get_transactions_paginated()`**

```sql
CREATE OR REPLACE FUNCTION api.get_transactions_paginated(
  _limit int DEFAULT 20,
  _offset int DEFAULT 0,
  _status text DEFAULT NULL,
  _block_height int DEFAULT NULL,
  _message_type text DEFAULT NULL
) RETURNS jsonb
LANGUAGE sql STABLE
AS $$
  WITH
  type_filtered_txs AS (
    SELECT DISTINCT m.id
    FROM api.messages_main m
    WHERE _message_type IS NULL OR m.type = _message_type
  ),
  filtered_txs AS (
    SELECT t.*
    FROM api.transactions_main t
    WHERE
      (_message_type IS NULL OR t.id IN (SELECT id FROM type_filtered_txs))
      AND (_status IS NULL
           OR (_status = 'success' AND t.error IS NULL)
           OR (_status = 'failed' AND t.error IS NOT NULL))
      AND (_block_height IS NULL OR t.height = _block_height)
    ORDER BY t.height DESC
    LIMIT _limit OFFSET _offset
  ),
  total_count AS (
    SELECT COUNT(*)::int AS cnt
    FROM api.transactions_main t
    WHERE
      (_message_type IS NULL OR t.id IN (SELECT id FROM type_filtered_txs))
      AND (_status IS NULL
           OR (_status = 'success' AND t.error IS NULL)
           OR (_status = 'failed' AND t.error IS NOT NULL))
      AND (_block_height IS NULL OR t.height = _block_height)
  ),
  tx_messages AS (
    SELECT
      m.id,
      jsonb_agg(
        jsonb_build_object(
          'id', m.id,
          'message_index', m.message_index,
          'type', m.type,
          'sender', m.sender,
          'mentions', m.mentions,
          'metadata', m.metadata
        ) ORDER BY m.message_index
      ) AS messages
    FROM api.messages_main m
    WHERE m.id IN (SELECT id FROM filtered_txs)
    GROUP BY m.id
  ),
  tx_events AS (
    SELECT
      e.id,
      jsonb_agg(
        jsonb_build_object(
          'id', e.id,
          'event_index', e.event_index,
          'attr_index', e.attr_index,
          'event_type', e.event_type,
          'attr_key', e.attr_key,
          'attr_value', e.attr_value,
          'msg_index', e.msg_index
        ) ORDER BY e.event_index, e.attr_index
      ) AS events
    FROM api.events_main e
    WHERE e.id IN (SELECT id FROM filtered_txs)
    GROUP BY e.id
  )
  SELECT jsonb_build_object(
    'data', COALESCE((
      SELECT jsonb_agg(
        jsonb_build_object(
          'id', t.id,
          'fee', t.fee,
          'memo', t.memo,
          'error', t.error,
          'height', t.height,
          'timestamp', t.timestamp,
          'proposal_ids', t.proposal_ids,
          'messages', COALESCE(m.messages, '[]'::jsonb),
          'events', COALESCE(e.events, '[]'::jsonb),
          'ingest_error', NULL
        ) ORDER BY t.height DESC
      )
      FROM filtered_txs t
      LEFT JOIN tx_messages m ON t.id = m.id
      LEFT JOIN tx_events e ON t.id = e.id
    ), '[]'::jsonb),
    'pagination', jsonb_build_object(
      'total', (SELECT cnt FROM total_count),
      'limit', _limit,
      'offset', _offset,
      'has_next', _offset + _limit < (SELECT cnt FROM total_count),
      'has_prev', _offset > 0
    )
  );
$$;

GRANT EXECUTE ON FUNCTION api.get_transactions_paginated(int, int, text, int, text) TO web_anon;
```

---

#### 1.3 Search Functions (`003_search_functions.sql`)

**Function: `api.universal_search()`**

```sql
CREATE OR REPLACE FUNCTION api.universal_search(_query text)
RETURNS jsonb
LANGUAGE plpgsql STABLE
AS $$
DECLARE
  results jsonb := '[]'::jsonb;
  trimmed text := trim(_query);
  block_height int;
  block_result jsonb;
  tx_result jsonb;
  evm_tx_result jsonb;
  address_result jsonb;
BEGIN
  -- Check for block height (numeric)
  IF trimmed ~ '^\d+$' THEN
    block_height := trimmed::int;
    SELECT jsonb_build_object(
      'type', 'block',
      'value', jsonb_build_object('id', id, 'data', data),
      'score', 100
    ) INTO block_result
    FROM api.blocks_raw
    WHERE id = block_height;

    IF block_result IS NOT NULL THEN
      results := results || block_result;
    END IF;
  END IF;

  -- Check for EVM tx hash (0x + 64 hex)
  IF trimmed ~ '^0x[a-fA-F0-9]{64}$' THEN
    SELECT jsonb_build_object(
      'type', 'evm_transaction',
      'value', jsonb_build_object(
        'tx_id', tx_id,
        'ethereum_tx_hash', ethereum_tx_hash,
        'height', height,
        'timestamp', timestamp
      ),
      'score', 100
    ) INTO evm_tx_result
    FROM api.evm_tx_map
    WHERE ethereum_tx_hash = trimmed;

    IF evm_tx_result IS NOT NULL THEN
      results := results || evm_tx_result;
    END IF;
  END IF;

  -- Check for Cosmos tx hash (64 hex, no 0x)
  IF trimmed ~ '^[a-fA-F0-9]{64}$' THEN
    SELECT jsonb_build_object(
      'type', 'transaction',
      'value', api.get_transaction_detail(trimmed),
      'score', 100
    ) INTO tx_result
    FROM api.transactions_main
    WHERE id = upper(trimmed);

    IF tx_result IS NOT NULL THEN
      results := results || tx_result;
    END IF;
  END IF;

  -- Check for Bech32 address
  IF trimmed ~ '^[a-z]+1[a-z0-9]{38,}$' THEN
    SELECT jsonb_build_object(
      'type', 'address',
      'value', api.get_address_stats(trimmed),
      'score', 90
    ) INTO address_result;

    IF address_result IS NOT NULL AND
       (address_result->'value'->>'transaction_count')::int > 0 THEN
      results := results || address_result;
    END IF;
  END IF;

  -- Check for EVM address (0x + 40 hex)
  IF trimmed ~ '^0x[a-fA-F0-9]{40}$' THEN
    SELECT jsonb_build_object(
      'type', 'evm_address',
      'value', jsonb_build_object(
        'address', address,
        'tx_count', tx_count,
        'first_seen', first_seen,
        'last_seen', last_seen
      ),
      'score', 95
    ) INTO address_result
    FROM api.evm_address_activity
    WHERE address = trimmed;

    IF address_result IS NOT NULL THEN
      results := results || address_result;
    END IF;
  END IF;

  RETURN results;
END;
$$;

GRANT EXECUTE ON FUNCTION api.universal_search(text) TO web_anon;
```

### Dependencies
- Phase 0 completed (repository structure in place)

### Success Criteria
- [ ] All functions created and granted to web_anon
- [ ] Functions tested with sample data
- [ ] Performance validated (single round-trip)
- [ ] Return types match frontend expectations

### Estimated Effort
- 8-12 hours

---

## PHASE 2: SQL MIGRATIONS - ANALYTICS & VIEWS

### Objective
Create pre-aggregated views and functions for analytics, eliminating client-side computation of statistics.

### Sub-Tasks

#### 2.1 Chain Statistics

**View: `api.chain_stats`**

```sql
CREATE OR REPLACE VIEW api.chain_stats AS
WITH
latest_blocks AS (
  SELECT id, data
  FROM api.blocks_raw
  ORDER BY id DESC
  LIMIT 100
),
block_times AS (
  SELECT
    id,
    (data->'block'->'header'->>'time')::timestamptz AS block_time,
    LAG((data->'block'->'header'->>'time')::timestamptz) OVER (ORDER BY id) AS prev_time
  FROM latest_blocks
),
block_time_stats AS (
  SELECT
    AVG(EXTRACT(EPOCH FROM (block_time - prev_time))) AS avg_block_time,
    MIN(EXTRACT(EPOCH FROM (block_time - prev_time))) AS min_block_time,
    MAX(EXTRACT(EPOCH FROM (block_time - prev_time))) AS max_block_time
  FROM block_times
  WHERE prev_time IS NOT NULL
    AND EXTRACT(EPOCH FROM (block_time - prev_time)) > 0
    AND EXTRACT(EPOCH FROM (block_time - prev_time)) < 100
),
latest_block AS (
  SELECT
    id,
    jsonb_array_length(
      COALESCE(
        data->'block'->'last_commit'->'signatures',
        data->'block'->'lastCommit'->'signatures',
        '[]'::jsonb
      )
    ) AS validator_count
  FROM api.blocks_raw
  ORDER BY id DESC
  LIMIT 1
)
SELECT
  (SELECT id FROM latest_block) AS latest_block,
  (SELECT COUNT(*) FROM api.transactions_main) AS total_transactions,
  (SELECT COUNT(DISTINCT m.sender) FROM api.messages_main m WHERE m.sender IS NOT NULL) AS unique_addresses,
  COALESCE((SELECT avg_block_time FROM block_time_stats), 0) AS avg_block_time,
  COALESCE((SELECT min_block_time FROM block_time_stats), 0) AS min_block_time,
  COALESCE((SELECT max_block_time FROM block_time_stats), 0) AS max_block_time,
  (SELECT validator_count FROM latest_block) AS active_validators;

GRANT SELECT ON api.chain_stats TO web_anon;
```

#### 2.2 Transaction Volume Views

```sql
CREATE OR REPLACE VIEW api.tx_volume_daily AS
SELECT
  DATE(timestamp) AS date,
  COUNT(*) AS count
FROM api.transactions_main
WHERE timestamp >= NOW() - INTERVAL '90 days'
GROUP BY DATE(timestamp)
ORDER BY date DESC;

CREATE OR REPLACE VIEW api.tx_volume_hourly AS
SELECT
  DATE_TRUNC('hour', timestamp) AS hour,
  COUNT(*) AS count
FROM api.transactions_main
WHERE timestamp >= NOW() - INTERVAL '7 days'
GROUP BY DATE_TRUNC('hour', timestamp)
ORDER BY hour DESC;

GRANT SELECT ON api.tx_volume_daily TO web_anon;
GRANT SELECT ON api.tx_volume_hourly TO web_anon;
```

#### 2.3 Message Type Statistics

```sql
CREATE OR REPLACE VIEW api.message_type_stats AS
SELECT
  COALESCE(type, 'Unknown') AS type,
  COUNT(*) AS count
FROM api.messages_main
GROUP BY type
ORDER BY count DESC;

GRANT SELECT ON api.message_type_stats TO web_anon;
```

#### 2.4 Gas Usage Distribution

```sql
CREATE OR REPLACE VIEW api.gas_usage_distribution AS
SELECT
  range_label AS range,
  COUNT(*) AS count
FROM (
  SELECT
    CASE
      WHEN (fee->>'gasLimit')::bigint < 100000 THEN '0-100k'
      WHEN (fee->>'gasLimit')::bigint < 250000 THEN '100k-250k'
      WHEN (fee->>'gasLimit')::bigint < 500000 THEN '250k-500k'
      WHEN (fee->>'gasLimit')::bigint < 1000000 THEN '500k-1M'
      ELSE '1M+'
    END AS range_label
  FROM api.transactions_main
  WHERE fee->>'gasLimit' IS NOT NULL
) AS binned
GROUP BY range_label
ORDER BY
  CASE range_label
    WHEN '0-100k' THEN 1
    WHEN '100k-250k' THEN 2
    WHEN '250k-500k' THEN 3
    WHEN '500k-1M' THEN 4
    WHEN '1M+' THEN 5
  END;

GRANT SELECT ON api.gas_usage_distribution TO web_anon;
```

#### 2.5 Transaction Success Rate

```sql
CREATE OR REPLACE VIEW api.tx_success_rate AS
SELECT
  COUNT(*) AS total,
  COUNT(*) FILTER (WHERE error IS NULL) AS successful,
  COUNT(*) FILTER (WHERE error IS NOT NULL) AS failed,
  ROUND(
    100.0 * COUNT(*) FILTER (WHERE error IS NULL) / NULLIF(COUNT(*), 0),
    2
  ) AS success_rate_percent
FROM api.transactions_main;

GRANT SELECT ON api.tx_success_rate TO web_anon;
```

#### 2.6 Fee Revenue

```sql
CREATE OR REPLACE VIEW api.fee_revenue AS
SELECT
  coin->>'denom' AS denom,
  SUM((coin->>'amount')::numeric) AS total_amount
FROM api.transactions_main,
     jsonb_array_elements(fee->'amount') AS coin
WHERE fee->'amount' IS NOT NULL
GROUP BY coin->>'denom'
ORDER BY total_amount DESC;

GRANT SELECT ON api.fee_revenue TO web_anon;
```

#### 2.7 Block Time Analysis Function

```sql
CREATE OR REPLACE FUNCTION api.get_block_time_analysis(_limit int DEFAULT 100)
RETURNS jsonb
LANGUAGE sql STABLE
AS $$
  WITH
  recent_blocks AS (
    SELECT
      id,
      (data->'block'->'header'->>'time')::timestamptz AS block_time
    FROM api.blocks_raw
    ORDER BY id DESC
    LIMIT _limit
  ),
  block_intervals AS (
    SELECT
      EXTRACT(EPOCH FROM (block_time - LAG(block_time) OVER (ORDER BY id))) AS interval_seconds
    FROM recent_blocks
  ),
  stats AS (
    SELECT
      AVG(interval_seconds) AS avg,
      MIN(interval_seconds) AS min,
      MAX(interval_seconds) AS max
    FROM block_intervals
    WHERE interval_seconds > 0 AND interval_seconds < 100
  )
  SELECT jsonb_build_object(
    'avg', COALESCE(avg, 0),
    'min', COALESCE(min, 0),
    'max', COALESCE(max, 0)
  )
  FROM stats;
$$;

GRANT EXECUTE ON FUNCTION api.get_block_time_analysis(int) TO web_anon;
```

#### 2.8 Active Addresses Over Time

```sql
CREATE OR REPLACE FUNCTION api.get_active_addresses_daily(_days int DEFAULT 30)
RETURNS TABLE (date date, count bigint)
LANGUAGE sql STABLE
AS $$
  SELECT
    DATE(t.timestamp) AS date,
    COUNT(DISTINCT m.sender) AS count
  FROM api.messages_main m
  JOIN api.transactions_main t ON m.id = t.id
  WHERE t.timestamp >= NOW() - (_days || ' days')::interval
    AND m.sender IS NOT NULL
  GROUP BY DATE(t.timestamp)
  ORDER BY date DESC;
$$;

GRANT EXECUTE ON FUNCTION api.get_active_addresses_daily(int) TO web_anon;
```

### Dependencies
- Phase 1 completed (core functions in place)

### Success Criteria
- [ ] All views created and accessible
- [ ] Pre-aggregation eliminates client-side computation
- [ ] Query performance < 100ms for all analytics views
- [ ] Data matches current frontend calculations

### Estimated Effort
- 6-8 hours

---

## PHASE 3: MIDDLEWARE CLIENT PACKAGE

### Objective
Create the TypeScript client package that the frontend will import directly from the middleware repo.

### Sub-Tasks

#### 3.1 Package Structure

```
packages/client/
├── src/
│   ├── client.ts       # Main YaciClient class
│   ├── types.ts        # All type definitions
│   ├── cache.ts        # Cache utilities (if needed)
│   └── index.ts        # Public exports
├── package.json
├── tsconfig.json
└── README.md
```

#### 3.2 Client Implementation

Key characteristics:
- No internal caching (rely on TanStack Query)
- No client-side aggregation (database handles it)
- Thin RPC wrappers for SQL functions
- No EVM decoding (no ethers.js dependency)

#### 3.3 Package Configuration

```json
{
  "name": "@yaci/client",
  "version": "1.0.0",
  "private": true,
  "type": "module",
  "main": "./dist/index.js",
  "types": "./dist/index.d.ts"
}
```

### Dependencies
- Phases 1 & 2 completed (SQL functions/views exist)

### Success Criteria
- [ ] Package builds without errors
- [ ] All types properly exported
- [ ] No external dependencies
- [ ] API matches frontend usage patterns

### Estimated Effort
- 6-8 hours

---

## PHASE 4: CACHING LAYER IMPLEMENTATION

### Objective
Configure caching strategy - primarily TanStack Query in frontend, with optional database-level caching for expensive queries.

### Sub-Tasks

#### 4.1 TanStack Query Configuration

Document recommended settings for frontend.

#### 4.2 Identify Hot Paths

Document which endpoints might need materialized views later.

#### 4.3 Optional Materialized Views

Create if performance requires.

### Dependencies
- Phase 3 completed

### Success Criteria
- [ ] Caching strategy documented
- [ ] Frontend TanStack Query configured
- [ ] Performance acceptable

### Estimated Effort
- 2-3 hours

---

## PHASE 5: FRONTEND MIGRATION

### Objective
Update yaci-explorer to use the new middleware client package.

### Sub-Tasks

#### 5.1 Update Package Dependencies

- Remove `packages/database-client`
- Remove `ethers` dependency
- Add local reference to middleware client

#### 5.2 Create Singleton API Instance

Create `src/lib/api.ts` with single instance.

#### 5.3 Update All Imports

Update 15+ files that currently create new YaciAPIClient instances.

#### 5.4 Update Method Calls

Adjust for any signature changes.

#### 5.5 Remove EVM Decoding Code

Adjust components that used ethers.js.

### Dependencies
- Phases 1-4 completed
- Middleware client package built

### Success Criteria
- [ ] All imports updated
- [ ] No references to old package
- [ ] Application builds without errors
- [ ] All pages render correctly
- [ ] ethers.js removed from bundle

### Estimated Effort
- 8-12 hours

---

## PHASE 6: TESTING & VALIDATION

### Objective
Verify the entire system works correctly after migration.

### Sub-Tasks

- Unit tests for SQL functions
- Integration tests
- Frontend smoke tests
- Performance validation
- Edge case testing

### Dependencies
- Phase 5 completed

### Success Criteria
- [ ] All SQL functions pass tests
- [ ] Integration tests pass
- [ ] All pages render correctly
- [ ] Performance improved or maintained
- [ ] No regressions

### Estimated Effort
- 6-10 hours

---

## PHASE 7: DEPLOYMENT & CUTOVER

### Objective
Deploy the new architecture to production.

### Sub-Tasks

- Database migration
- Deploy middleware
- Deploy frontend
- Monitoring
- Rollback plan

### Dependencies
- All previous phases completed and tested

### Success Criteria
- [ ] Migrations applied successfully
- [ ] Middleware deployed and healthy
- [ ] Frontend deployed and functional
- [ ] No errors in monitoring
- [ ] Performance meets expectations

### Estimated Effort
- 4-6 hours

---

## OPTIMIZED EXECUTION SEQUENCE

After comprehensive review for conflicts, redundancies, and efficiency:

### Key Optimizations Applied

1. **Parallel SQL migrations** - Phases 1 & 2 execute simultaneously
2. **Merged caching** - Phase 4 absorbed into Phase 3 documentation
3. **Distributed testing** - Each phase validates before completion
4. **Parallel frontend updates** - Group by feature area

### Execution Stages

| Stage | Tasks | Parallelism | Est. Effort |
|-------|-------|-------------|-------------|
| 1 | Phase 0: Repository setup | Serial | 2-3h |
| 2 | Phases 1+2: SQL migrations | **Parallel** | 8-10h |
| 3 | Phase 3+4: Client + caching | Serial | 6-8h |
| 4 | Phase 5: Frontend migration | Parallel by area | 8-12h |
| 5 | Phase 6: E2E validation | Serial | 4-6h |
| 6 | Phase 7: Production deploy | Serial | 3-4h |

**Total Estimated Effort: 31-43 hours**

### Frontend Migration Groups (Phase 5 Parallelism)

- **Address components**: `$address.tsx`, `AddressLink.tsx`, `AddressPage.tsx`, `AddressTransactions.tsx`
- **Transaction components**: `$tx.tsx`, `TransactionLink.tsx`, `TransactionsPage.tsx`, `TransactionDetails.tsx`, `TransactionRow.tsx`
- **Block components**: `$block.tsx`, `blocks._index.tsx`, `BlockLink.tsx`
- **Analytics/Dashboard**: `_index.tsx`, `AnalyticsDashboard.tsx`, `ChainStats.tsx`
- **Search/Layout**: `Search.tsx`, `Layout.tsx`, utilities

---

## LEGACY TIMELINE (Original)

| Phase | Description | Effort | Dependencies |
|-------|-------------|--------|--------------|
| 0 | Repository Setup | 2-3h | None |
| 1 | SQL Migrations - Core | 8-12h | Phase 0 |
| 2 | SQL Migrations - Analytics | 6-8h | Phase 1 |
| 3 | Middleware Client Package | 6-8h | Phases 1-2 |
| 4 | Caching Layer | 2-3h | Phase 3 |
| 5 | Frontend Migration | 8-12h | Phases 1-4 |
| 6 | Testing & Validation | 6-10h | Phase 5 |
| 7 | Deployment & Cutover | 4-6h | Phase 6 |

**Original Estimated Effort: 42-62 hours**

---

## RISK MITIGATION

| Risk | Mitigation |
|------|------------|
| SQL function performance | Test with production data volume before deployment |
| Breaking API changes | Maintain same response shapes as current client |
| Missing edge cases | Comprehensive testing phase |
| Deployment failures | Rollback plan in place |
| Data inconsistency | Run parallel validation before cutover |

---

## FUTURE ENHANCEMENTS

After this migration is complete, the architecture supports:

1. **GraphQL Layer** - Add Hasura or custom GraphQL server
2. **Enhanced EVM Support** - Contract verification, token tracking
3. **Cosmos Module Coverage** - Staking, governance, IBC views
4. **Real-time Updates** - PostgreSQL LISTEN/NOTIFY or WebSocket
5. **Multi-chain Support** - Schema namespacing per chain
