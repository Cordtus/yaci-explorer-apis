# Database Function Performance Review

## Critical Performance Issues

### 1. get_blocks_paginated - MAJOR ISSUE

**Problem**: The `block_tx_counts` CTE scans ALL transactions on every request:
```sql
WITH block_tx_counts AS (
  SELECT t.height AS block_id, COUNT(*) AS tx_count
  FROM api.transactions_main t
  GROUP BY t.height
)
```

**Impact**:
- Scans entire transactions_main table (currently 391 rows, but will grow to millions)
- Groups all data even though we only need ~20 blocks worth of counts
- O(n) query that becomes O(n) expensive as data grows

**Solution**: Materialize transaction counts in the blocks table or use a materialized view

### 2. get_transactions_paginated - MODERATE ISSUE

**Problem**: Two subqueries join messages and events for paginated results
```sql
tx_messages AS (
  SELECT m.id, jsonb_agg(...) FROM api.messages_main m
  WHERE m.id IN (SELECT id FROM paginated)
  GROUP BY m.id
)
```

**Impact**:
- Not terrible since it only processes paginated results
- But could be slow without proper indexes on messages_main.id and events_main.id

**Solution**: Add indexes (see recommendations below)

### 3. get_transactions_by_address - MODERATE ISSUE

**Problem**: Same as get_transactions_paginated, plus initial address lookup
```sql
WITH addr_txs AS (
  SELECT DISTINCT m.id FROM api.messages_main m
  WHERE m.sender = _address OR _address = ANY(m.mentions)
)
```

**Impact**:
- Array containment check (`ANY(mentions)`) can be slow without GIN index
- Needs index on sender column

**Solution**: Add GIN index on mentions array, btree index on sender

## Recommended Indexes

### Critical (Implement Immediately)

```sql
-- For get_transactions_by_address performance
CREATE INDEX idx_messages_sender ON api.messages_main(sender);
CREATE INDEX idx_messages_mentions_gin ON api.messages_main USING GIN(mentions);

-- For joining messages/events in paginated queries
CREATE INDEX idx_messages_id ON api.messages_main(id);
CREATE INDEX idx_events_id ON api.events_main(id);

-- For transaction height lookups
CREATE INDEX idx_transactions_height ON api.transactions_main(height);
```

### High Priority (Implement Soon)

```sql
-- For filtering transactions by timestamp
CREATE INDEX idx_transactions_timestamp ON api.transactions_main(timestamp);

-- For filtering blocks by timestamp (JSONB path)
CREATE INDEX idx_blocks_time ON api.blocks_raw
  USING BTREE ((data->'block'->'header'->>'time')::timestamp);
```

## Materialized View for Block Transaction Counts

The most critical fix is to avoid counting transactions on every request.

**Option 1: Add tx_count column to blocks_raw** (Preferred)
```sql
ALTER TABLE api.blocks_raw ADD COLUMN tx_count INT DEFAULT 0;

-- Update trigger to maintain count
CREATE OR REPLACE FUNCTION update_block_tx_count() RETURNS TRIGGER AS $$
BEGIN
  UPDATE api.blocks_raw
  SET tx_count = (
    SELECT COUNT(*) FROM api.transactions_main WHERE height = NEW.height
  )
  WHERE id = NEW.height;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER maintain_block_tx_count
AFTER INSERT ON api.transactions_main
FOR EACH ROW EXECUTE FUNCTION update_block_tx_count();
```

**Option 2: Materialized View** (Alternative)
```sql
CREATE MATERIALIZED VIEW api.block_stats AS
SELECT
  b.id,
  b.data,
  COALESCE(COUNT(t.id), 0) AS tx_count
FROM api.blocks_raw b
LEFT JOIN api.transactions_main t ON b.id = t.height
GROUP BY b.id, b.data;

CREATE UNIQUE INDEX ON api.block_stats(id);

-- Refresh periodically or on trigger
REFRESH MATERIALIZED VIEW CONCURRENTLY api.block_stats;
```

## Function Optimization: get_blocks_paginated v2

```sql
-- Assumes tx_count column added to blocks_raw
CREATE OR REPLACE FUNCTION api.get_blocks_paginated(
  _limit int DEFAULT 20,
  _offset int DEFAULT 0,
  _min_tx_count int DEFAULT NULL,
  _from_date timestamp DEFAULT NULL,
  _to_date timestamp DEFAULT NULL
)
RETURNS jsonb
LANGUAGE sql STABLE
AS $$
  WITH filtered_blocks AS (
    SELECT b.id, b.data, b.tx_count
    FROM api.blocks_raw b
    WHERE
      (_min_tx_count IS NULL OR b.tx_count >= _min_tx_count)
      AND (_from_date IS NULL OR (b.data->'block'->'header'->>'time')::timestamp >= _from_date)
      AND (_to_date IS NULL OR (b.data->'block'->'header'->>'time')::timestamp <= _to_date)
    ORDER BY b.id DESC
  ),
  total AS (
    SELECT COUNT(*) AS count FROM filtered_blocks
  ),
  paginated AS (
    SELECT * FROM filtered_blocks
    LIMIT _limit OFFSET _offset
  )
  SELECT jsonb_build_object(
    'data', COALESCE(jsonb_agg(
      jsonb_build_object(
        'id', p.id,
        'data', p.data,
        'tx_count', p.tx_count
      ) ORDER BY p.id DESC
    ), '[]'::jsonb),
    'pagination', jsonb_build_object(
      'total', (SELECT count FROM total),
      'limit', _limit,
      'offset', _offset,
      'has_next', _offset + _limit < (SELECT count FROM total),
      'has_prev', _offset > 0
    )
  )
  FROM paginated p;
$$;
```

## Performance Monitoring

### Query to check slow queries
```sql
SELECT query, mean_exec_time, calls
FROM pg_stat_statements
WHERE query LIKE '%api.get_%'
ORDER BY mean_exec_time DESC;
```

### Check index usage
```sql
SELECT schemaname, tablename, indexname, idx_scan, idx_tup_read
FROM pg_stat_user_indexes
WHERE schemaname = 'api'
ORDER BY idx_scan;
```

## Action Items

1. **Immediate**: Add critical indexes for messages and events
2. **High Priority**: Add tx_count column to blocks_raw with trigger
3. **Medium Priority**: Add timestamp indexes
4. **Optional**: Set up pg_stat_statements for ongoing monitoring

## Estimated Impact

With current data (391 transactions, ~100k blocks):
- Current: get_blocks_paginated scans 391 rows + 100k blocks
- Optimized: Only scans filtered blocks (typically 20-100 rows)

At scale (1M transactions, 1M blocks):
- Current: Would scan 1M+ rows per request (UNACCEPTABLE)
- Optimized: Still scans ~20-100 rows per request (ACCEPTABLE)

**Conclusion**: The tx_count materialization is CRITICAL before this goes to production with significant data.
