# YACI Explorer APIs - TODO

## Completed

### Governance System
- [x] Apply migration 007 (governance tables)
- [x] Create governance poller worker (workers/governance-poller.ts) - DEPRECATED
- [x] Refactor governance to use database triggers instead of REST polling (migration 013)
- [x] Remove gov_worker process from fly.toml (no external API dependencies)
- [x] Add governance endpoints to TypeScript client
- [x] Add GovernanceProposal and ProposalSnapshot types
- [x] Build frontend governance page (yaci-explorer repo)
- [x] Add proposal detail page with vote history chart (yaci-explorer repo)

### Performance Optimizations
- [x] Add tx_count column to blocks_raw table with trigger (migration 008)
- [x] Create critical indexes for messages_main, events_main, transactions_main (migration 008)
- [x] Optimize get_blocks_paginated to use tx_count column (migration 009)
- [x] Add timestamp indexes for date filtering (migration 008)
- [x] Auto-priority EVM decode on transaction detail view (migration 010)
- [x] Set up pg_stat_statements for query performance monitoring (migration 011)
- [x] Create materialized views for analytics queries (migration 012)

### Infrastructure
- [x] Fix ESM compatibility issues (import.meta.url pattern)
- [x] Update CI/CD to make deploy depend on build passing
- [x] Refactor decode-evm-single.ts to use NOTIFY/LISTEN instead of HTTP

### Documentation
- [x] Add database connection pooling configuration documentation
- [x] Document backup and recovery procedures

## Architecture Notes

- All governance data extracted from indexed MsgSubmitProposal and MsgVote transactions
- Database triggers automatically populate governance tables
- No external REST API dependencies - everything from indexed gRPC data
- Materialized views refresh via api.refresh_analytics_views() function
- Three-process Fly.io deployment: app (PostgREST), worker (EVM daemon), priority_decoder (NOTIFY/LISTEN)
