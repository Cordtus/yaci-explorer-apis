# YACI Explorer APIs - TODO

## High Priority

### Governance System
- [ ] Apply migration 007 (governance tables)
- [ ] Test proposal detection trigger with test transaction
- [ ] Verify governance poller works with chain API
- [ ] Build frontend governance page
- [ ] Add proposal detail page with vote history chart
- [ ] Test full workflow: submit proposal -> detect -> poll -> display

### Performance Optimizations
- [ ] Add tx_count column to blocks_raw table with trigger maintenance
- [ ] Create critical indexes for messages_main (sender, mentions GIN, id)
- [ ] Create critical indexes for events_main (id)
- [ ] Add index on transactions_main(height)
- [ ] Optimize get_blocks_paginated to use materialized tx_count instead of scanning all transactions
- [ ] Add timestamp indexes for date filtering

See `PERFORMANCE_REVIEW.md` for detailed analysis and solutions.

**Impact**: Current get_blocks_paginated scans ALL transactions on every request. Will become unacceptable as data grows.

## Medium Priority

- [ ] Set up pg_stat_statements for query performance monitoring
- [ ] Add database connection pooling configuration documentation
- [ ] Consider materialized views for analytics queries

## Low Priority

- [ ] Add more analytics views (hourly stats, validator stats, etc.)
- [ ] Document backup and recovery procedures
