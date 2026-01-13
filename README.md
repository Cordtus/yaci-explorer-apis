# YACI Explorer APIs

Middleware layer for YACI Explorer providing optimized database access via PostgREST RPC functions.

## Architecture

```
Blockchain -> YACI Indexer -> PostgreSQL -> PostgREST -> This Package -> Frontend
```

This package provides:
- SQL functions for optimized single-round-trip queries
- Pre-aggregated analytics views and materialized views
- Background workers for EVM transaction decoding
- TypeScript client for frontend consumption
- Database triggers for governance and IBC tracking

## Components

### SQL Migrations (`/migrations`)

Database functions and views that PostgREST exposes as RPC endpoints.

**Core Functions:**
- `get_transactions_by_address()` - Paginated address transactions
- `get_address_stats()` - Address activity statistics
- `get_transaction_detail()` - Full transaction with messages, events, EVM data
- `get_transactions_paginated()` - Filtered transaction listing
- `get_blocks_paginated()` - Paginated block listing
- `get_block_time_analysis()` - Block time statistics
- `universal_search()` - Cross-entity search

**Governance Functions:**
- `get_governance_proposals()` - Paginated governance proposals
- `compute_proposal_tally()` - Calculate proposal vote tallies

**IBC Functions:**
- `get_ibc_stats()` - IBC transfer and channel statistics
- `get_ibc_transfers()` - Paginated IBC transfers with direction filter
- `get_ibc_transfers_by_address()` - IBC transfers for specific address
- `get_ibc_connections()` - IBC channels with filters
- `get_ibc_connection()` - Single channel details
- `get_ibc_denom_traces()` - IBC denom trace information
- `resolve_ibc_denom()` - Resolve IBC denom to full trace
- `resolve_denom()` - Resolve any denom (native or IBC)
- `get_ibc_chains()` - List connected chains
- `get_ibc_channel_activity()` - Transfer stats by channel
- `get_ibc_volume_timeseries()` - Hourly volume data
- `get_ibc_heatmap_data()` - IBC activity heatmap

**Analytics Views:**
- `chain_stats` - Overall chain statistics
- `tx_volume_daily` - Daily transaction counts
- `tx_volume_hourly` - Hourly transaction counts
- `daily_active_addresses` - Unique active addresses per day
- `message_type_stats` - Message type distribution
- `tx_success_rate` - Success/failure rates
- `fee_revenue` - Fee totals by denomination
- `gas_usage_distribution` - Gas usage percentiles

**Materialized Views** (refreshed via `api.refresh_analytics_views()`):
- `mv_daily_tx_stats` - Daily stats with unique senders
- `mv_hourly_tx_stats` - Hourly stats for last 7 days
- `mv_message_type_stats` - Message type percentages

### Client Package (`/packages/client`)

TypeScript client that wraps PostgREST RPC calls:

```typescript
import { createClient } from '@yaci/client'

const client = createClient('https://api.example.com')

// Address data
const txs = await client.getTransactionsByAddress(address, 50, 0)
const stats = await client.getAddressStats(address)

// Transaction data
const tx = await client.getTransaction(hash)
const txList = await client.getTransactions(20, 0, { status: 'success' })

// Block data
const block = await client.getBlock(height)
const blocks = await client.getBlocks(20, 0)

// Search
const results = await client.search('cosmos1...')

// Analytics
const chainStats = await client.getChainStats()
const dailyVolume = await client.getTxVolumeDaily()
const activeAddresses = await client.getDailyActiveAddresses(30)
const messageTypes = await client.getMessageTypeStats()
const successRate = await client.getTxSuccessRate()

// Governance
const proposals = await client.getGovernanceProposals(20, 0, 'VOTING')
const snapshots = await client.getProposalSnapshots(proposalId)

// IBC
const ibcStats = await client.getIbcStats()
const transfers = await client.getIbcTransfers(20, 0, 'outgoing')
const addressTransfers = await client.getIbcTransfersByAddress(address)
const connections = await client.getIbcConnections(50, 0, chainId)
const connection = await client.getIbcConnection(channelId)
const denomTraces = await client.getIbcDenomTraces(50, 0, baseDenom)
const resolved = await client.resolveIbcDenom('ibc/ABC123...')
const denomInfo = await client.resolveDenom('umfx')
const chains = await client.getIbcChains()
const channelActivity = await client.getIbcChannelActivity()
const volumeTimeseries = await client.getIbcVolumeTimeseries(24, channelId)
```

**Key characteristics:**
- No internal caching (use TanStack Query)
- No client-side aggregation (database handles it)
- No EVM decoding dependencies
- Thin RPC wrappers only

## Development

### Prerequisites

- Bun (latest)
- PostgreSQL 15+ with YACI schema
- PostgREST 12+

### Setup

```bash
bun install
bun run build
```

### Running Migrations

```bash
export DATABASE_URL="postgresql://user:pass@host:5432/db"
bun run migrate

# Dry run
bun run migrate:dry
```

### Running Workers

```bash
# EVM decode daemon (batch processing)
bun run decode:evm

# Priority EVM decode (NOTIFY/LISTEN)
bun run decode:priority

# Chain params daemon (IBC/denom resolution)
bun run chain-params
```

## Deployment

Deployed to Fly.io with three processes:
- `app` - PostgREST API server (port 3000)
- `worker` - EVM decode daemon (continuous batch processing)
- `priority_decoder` - Priority EVM decode via NOTIFY/LISTEN

```bash
fly deploy
```

Configuration in `fly.toml`. Required secrets:
- `PGRST_DB_URI` - PostgREST connection string
- `DATABASE_URL` - Worker connection string

## Frontend Integration

The frontend (yaci-explorer) imports the client directly:

```typescript
import { createClient } from '../../yaci-explorer-apis/packages/client'

const apiClient = createClient(import.meta.env.VITE_POSTGREST_URL)
```

### TanStack Query Configuration

Recommended settings for frontend:

```typescript
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 10000,      // 10s
      gcTime: 5 * 60 * 1000, // 5min
      retry: 1
    }
  }
})
```

## Related

- [YACI Indexer](https://github.com/Cordtus/yaci) - Data ingestion
- [YACI Explorer](https://github.com/Cordtus/yaci-explorer) - Frontend
- [OPERATIONS.md](./OPERATIONS.md) - Deployment, backup, and troubleshooting
