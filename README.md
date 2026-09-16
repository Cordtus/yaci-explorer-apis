# YACI Explorer APIs

Middleware layer for the YACI Explorer blockchain explorer. Provides optimized database access, background workers, and a TypeScript client for the frontend.

## Architecture

```
Blockchain gRPC -> [yaci indexer] -> PostgreSQL raw tables
                                         |
                                    [triggers] -> parsed tables
                                         |
                                    [workers] -> EVM decoded tables
                                         |
                                    PostgREST -> [this package's client] -> Frontend
```

### System Components

| Component | Repository | Purpose |
|-----------|------------|---------|
| **Indexer** | [yaci](https://github.com/Cordtus/yaci) | Go service: gRPC -> PostgreSQL raw tables |
| **Middleware** (this repo) | yaci-explorer-apis | SQL functions, views, triggers, EVM workers, TypeScript client |
| **Frontend** | [yaci-explorer](https://github.com/Cordtus/yaci-explorer) | React UI consuming PostgREST via the client |

## What This Package Provides

- **SQL functions** for single-round-trip queries (no N+1 patterns)
- **Database triggers** for parsing raw indexed data into structured tables
- **Materialized views** for pre-aggregated analytics with zero-downtime refresh
- **EVM decode workers** for decoding Ethereum transactions, logs, and tokens
- **Chain params daemon** for IBC denom resolution and chain parameter tracking
- **Reactive updates** via pg_notify for real-time validator state changes
- **TypeScript client** (`@yaci/client`) for typed frontend access

## Directory Structure

```
migrations/          SQL schema, functions, views, triggers (001-053)
packages/client/     TypeScript client - thin RPC wrappers, zero deps
scripts/             Migration runner, EVM decode daemons, utilities
docker/              Dockerfile for multi-process deployment
docs/                API reference documentation
.github/workflows/   CI/CD (build validates, deploy deploys)
```

## Commands

```bash
bun install                     # Install dependencies
bun run build                   # Build client package (@yaci/client)
bun run typecheck               # Type check client (tsc --noEmit)
bun run migrate                 # Run SQL migrations
bun run migrate:dry             # Dry run migrations (list files only)
bun run decode:evm              # Run EVM decode daemon
bun run decode:priority         # Run priority EVM decode listener
bun run chain-params            # Run chain params daemon
```

## SQL Functions

### Core Queries
- `get_transaction_detail()` - Full transaction with messages, events, EVM data
- `get_transactions_paginated()` - Filtered transaction listing
- `get_transactions_by_address()` - Address transaction history
- `get_blocks_paginated()` - Paginated block listing
- `universal_search()` - Cross-entity search (blocks, txs, addresses)
- `get_address_stats()` - Address activity statistics

### Validators & Staking
- `get_validators_paginated()` - Validator list with filtering/sorting
- `get_validator_detail()` - Full validator info with consensus address
- `get_validators_with_signing_stats()` - Validators with uptime data
- `get_validator_performance()` - Uptime, jailing events, rankings
- `get_validator_signing_stats()` - Signing stats in configurable block window
- `get_validator_total_rewards()` - Lifetime reward totals
- `get_validator_rewards_history()` - Per-block reward history
- `get_delegation_events()` - Delegation history for a validator
- `get_delegator_history()` - Delegation history for a delegator
- `get_delegator_delegations()` - Current delegations by validator

### Governance
- `get_governance_proposals()` - Paginated proposals with status filter
- `compute_proposal_tally()` - Recalculate vote tallies

### Analytics
- `get_network_overview()` - Comprehensive network statistics
- `get_hourly_rewards()` - Hourly rewards aggregation
- `refresh_analytics_views()` - Refresh all materialized views (CONCURRENTLY)

See [docs/API.md](./docs/API.md) for complete API reference.

## Client Package

Located in `packages/client/`. Zero external dependencies, thin RPC wrappers with full TypeScript types.

```typescript
import { createClient } from '@yaci/client'

const client = createClient('https://api.example.com')

const tx = await client.getTransaction(hash)
const validators = await client.getValidatorLeaderboard()
const overview = await client.getNetworkOverview()
```

## Deployment

Deployed to Fly.io with three processes:

| Process | Purpose | Memory |
|---------|---------|--------|
| `app` | PostgREST API server (port 3000) | 256MB |
| `worker` | EVM decode daemon (batch processing) | 512MB |
| `priority_decoder` | Priority EVM decode (NOTIFY/LISTEN) | 512MB |

```bash
fly deploy
fly secrets set PGRST_DB_URI="postgresql://..."
fly secrets set DATABASE_URL="postgresql://..."
```

## Database Schema

**Raw tables** (indexer): `blocks_raw`, `transactions_raw`
**Intermediate** (triggers): `messages_raw`, `events_raw`, `messages_main`, `events_main`, `transactions_main`
**Validators**: `validators`, `validator_block_signatures`, `validator_rewards`, `finalize_block_events`, `block_metrics`
**EVM** (workers): `evm_transactions`, `evm_logs`, `evm_tokens`, `evm_token_transfers`, `evm_contracts`
**Governance** (triggers): `governance_proposals`, `governance_snapshots`
**Other**: `ibc_channels`, `denom_metadata`, `delegation_events`

## Environment Variables

| Variable | Used By | Description |
|----------|---------|-------------|
| `DATABASE_URL` | Workers | PostgreSQL connection for background processes |
| `PGRST_DB_URI` | PostgREST | PostgreSQL connection for API server |
| `PGRST_DB_ANON_ROLE` | PostgREST | Anonymous role (`web_anon`) |
| `PGRST_DB_SCHEMAS` | PostgREST | Exposed schema (`api`) |
| `POLL_INTERVAL_MS` | Workers | Polling interval (default: 5000) |
| `BATCH_SIZE` | Workers | EVM decode batch size (default: 100) |

## Related Documentation

- [API Reference](./docs/API.md) - Complete endpoint documentation
- [Operations Guide](./OPERATIONS.md) - Deployment, backup, and troubleshooting
