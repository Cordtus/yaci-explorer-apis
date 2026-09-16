# API Reference

All endpoints are accessible via PostgREST at `{BASE_URL}/rpc/{function_name}` (RPC functions) or `{BASE_URL}/{view_name}` (views/tables).

## RPC Functions

### Transactions

#### `get_transaction_detail(_hash text)`
Full transaction including messages, events, and EVM data.

Response: `id`, `fee`, `memo`, `error`, `height`, `timestamp`, `messages[]`, `events[]`, `evm_data`, `evm_logs[]`, `raw_data`, `ingest_error`

#### `get_transactions_paginated(_limit, _offset, _status, _block_height, _message_type)`
Filtered transaction list with pagination.

| Param | Type | Description |
|-------|------|-------------|
| `_limit` | int | Max results (default 20) |
| `_offset` | int | Skip count |
| `_status` | text | `'success'` or `'failed'` |
| `_block_height` | int | Filter by block |
| `_message_type` | text | Filter by message type |

#### `get_transactions_by_address(_address, _limit, _offset)`
All transactions involving an address (as sender or mention in events).

#### `get_address_stats(_address text)`
Returns: `address`, `transaction_count`, `first_seen`, `last_seen`, `total_sent`, `total_received`

### Blocks

#### `get_blocks_paginated(_limit, _offset)`
Paginated block list ordered by height descending.

#### `get_block_time_analysis(_limit int)`
Block time statistics (avg, min, max) for recent blocks. Default limit 100.

### Search

#### `universal_search(_query text)`
Searches across blocks, transactions, addresses (Cosmos and EVM formats). Returns array of:
```json
{ "type": "block|transaction|evm_transaction|address|evm_address", "value": {...}, "score": 1 }
```

### Validators

#### `get_validators_paginated(_limit, _offset, _status, _jailed, _search, _sort_by)`
Paginated validator list with filtering and sorting.

| Param | Type | Description |
|-------|------|-------------|
| `_limit` | int | Max results (default 100) |
| `_offset` | int | Skip count |
| `_status` | text | `'BOND_STATUS_BONDED'`, `'BOND_STATUS_UNBONDING'`, `'BOND_STATUS_UNBONDED'` |
| `_jailed` | bool | Filter by jailed status |
| `_search` | text | Search by moniker |
| `_sort_by` | text | Sort field (default: `'tokens'`) |

#### `get_validator_detail(_operator_address text)`
Full validator detail including tokens, commission, description, consensus address, delegation count.

#### `get_validators_with_signing_stats(_limit, _offset)`
Validators list joined with signing statistics (signed/missed counts, uptime percentage).

#### `get_all_validators_signing_stats(_window_size int)`
Signing stats for all validators within a given block window.

#### `get_validator_signing_stats(_consensus_address text, _window_size int)`
Signing stats for a single validator. Default window 10,000 blocks.

#### `get_validator_performance(_operator_address text)`
Performance metrics: uptime percentage, blocks signed/missed, jailing events, rewards rank, delegation rank.

#### `get_validator_total_rewards(_operator_address text)`
Lifetime reward totals: `total_rewards`, `total_commission`, `blocks_with_rewards`.

#### `get_validator_rewards_history(_operator_address, _limit, _offset)`
Per-block rewards and commission history.

#### `get_validator_jailing_events(_operator_address text, _limit int)`
Jailing events from finalize_block_events for a specific validator.

#### `get_recent_validator_events(_limit int, _event_types text[])`
Recent validator events (slashing, jailing, unjailing, etc.) across all validators.

#### `get_validator_events_summary(_limit int)`
Summary of recent validator events with moniker resolution.

#### `get_hourly_rewards(_hours int)`
Hourly aggregated rewards and commission. Default 24 hours.

#### `request_validator_refresh(_operator_address text)`
Triggers a pg_notify event for reactive validator data refresh.

### Staking / Delegations

#### `get_delegation_events(_validator_address, _limit, _offset)`
Delegation events for a specific validator.

#### `get_delegator_history(_delegator_address, _limit, _offset, _event_type)`
Delegation history for a delegator with optional event type filter (`DELEGATE`, `UNDELEGATE`, `REDELEGATE`, `CREATE_VALIDATOR`).

#### `get_delegator_delegations(_delegator_address text)`
Current delegations aggregated by validator. Returns: `delegations[]`, `total_staked`, `validator_count`.

#### `get_delegator_stats(_delegator_address text)`
Delegation statistics: total delegations/undelegations/redelegations, first/last activity, unique validators.

#### `get_delegator_validator_history(_delegator_address, _validator_address, _limit, _offset)`
Delegation history between a specific delegator and validator.

### Governance

#### `get_governance_proposals(_limit, _offset, _status)`
Paginated proposals with optional status filter (`DEPOSIT_PERIOD`, `VOTING_PERIOD`, `PASSED`, `REJECTED`).

Response per proposal: `proposal_id`, `title`, `summary`, `status`, `submit_time`, `voting_start_time`, `voting_end_time`, `proposer`, `tally`, `last_updated`

#### `compute_proposal_tally(_proposal_id bigint)`
Recalculates vote tallies from indexed vote messages.

### Network Analytics

#### `get_network_overview()`
Comprehensive network statistics:
```json
{
  "total_validators": 100,
  "active_validators": 50,
  "jailed_validators": 2,
  "total_bonded_tokens": "1000000",
  "total_rewards_24h": "5000",
  "total_commission_24h": "500",
  "avg_block_time": 6.5,
  "total_transactions": 100000,
  "unique_addresses": 5000
}
```

#### `refresh_analytics_views()`
Refreshes all materialized views (uses CONCURRENTLY for zero-downtime refresh).

### EVM

#### `request_evm_decode(_tx_hash text)`
Request priority EVM decode for a transaction. Triggers NOTIFY on `evm_decode_priority` channel.

### Republic (Chain-Specific)

#### `get_compute_jobs(_limit, _offset)`
Paginated compute job listings.

#### `get_compute_job(_job_id bigint)`
Single compute job detail.

#### `get_compute_benchmarks(_limit, _offset)`
Benchmark results for compute validation.

#### `get_slashing_records(_limit, _offset)`
Slashing records from reputation module.

## Views and Tables (Direct Query)

Access via `{BASE_URL}/{view_name}?select=*&order=...&limit=...`

### Analytics Views

| View | Description |
|------|-------------|
| `chain_stats` | Latest block, total txs, unique addresses, avg block time, active validators |
| `tx_volume_daily` | Daily transaction counts |
| `tx_volume_hourly` | Hourly transaction counts |
| `message_type_stats` | Message type distribution (type, count) |
| `tx_success_rate` | Total, successful, failed, success_rate_percent |
| `fee_revenue` | Fee totals by denomination |
| `gas_usage_distribution` | Gas usage percentiles (p50, p90, p99, avg, max) |

### Materialized Views

| View | Refresh | Description |
|------|---------|-------------|
| `mv_chain_stats` | On demand | Cached version of chain_stats |
| `mv_network_overview` | On demand | Cached network overview |
| `mv_daily_tx_stats` | On demand | Daily stats with unique senders |
| `mv_hourly_tx_stats` | On demand | Hourly stats for last 7 days |
| `mv_message_type_stats` | On demand | Message type percentages |
| `mv_hourly_rewards` | On demand | Hourly rewards aggregation |
| `mv_validator_signing_stats` | Trigger-based | Per-validator signing stats in 10K block window |
| `mv_validator_leaderboard` | On demand | Validator ranking by tokens, rewards, commission |
| `mv_daily_rewards` | On demand | Daily rewards and commission aggregation |

All materialized views have unique indexes for `REFRESH CONCURRENTLY` support.

### Validator Views

| View | Description |
|------|-------------|
| `validator_stats` | Aggregate validator counts by status |
| `validators_with_consensus` | Validators joined with computed consensus addresses |

### Governance Views

| View | Description |
|------|-------------|
| `governance_active_proposals` | Proposals in deposit/voting period |
| `governance_snapshots` | Historical tally snapshots (table, queryable) |

### EVM Views

| View | Description |
|------|-------------|
| `evm_tx_map` | Maps EVM tx hash to Cosmos tx ID |
| `evm_pending_decode` | Transactions awaiting EVM decode |
| `evm_missing_contracts` | Contracts without metadata |
| `evm_tokens_missing_metadata` | Tokens without name/symbol |

### EVM Tables (Direct Query)

| Table | Description |
|-------|-------------|
| `evm_contracts` | Deployed contracts: address, creator, creation_tx, bytecode_hash, name, is_verified |
| `evm_tokens` | Token registry: address, name, symbol, decimals, total_supply, type |
| `evm_token_transfers` | Token transfer history: from, to, token_address, amount, tx_hash |
| `evm_transactions` | Decoded EVM transactions: hash, from, to, value, gas, input data |
| `evm_logs` | Decoded event logs: address, topics, data |

### Other Tables (Direct Query)

| Table | Description |
|-------|-------------|
| `validators` | Validator set: operator_address, moniker, tokens, commission, status, jailed |
| `blocks_raw` | Raw block data from indexer |
| `denom_metadata` | Token denomination metadata (symbol, decimals, description) |
| `ibc_channels` | IBC channel information |
| `validator_block_signatures` | Per-block validator signing records |
| `validator_rewards` | Per-block validator rewards and commission |
| `finalize_block_events` | Consensus events from block results (slash, jail, liveness, rewards) |
| `block_metrics` | Per-block metrics (tx count, gas used, event counts) |

## Pagination

All paginated RPC responses follow this structure:
```json
{
  "data": [...],
  "pagination": {
    "total": 1000,
    "limit": 20,
    "offset": 0,
    "has_next": true,
    "has_prev": false
  }
}
```

For direct table queries, use PostgREST syntax: `?limit=20&offset=0&order=id.desc`

## Error Handling

PostgREST returns HTTP errors:
- `404` - Function/view not found
- `400` - Invalid parameters
- `500` - Database error (check logs)

Client throws `Error` with message format: `RPC {function} failed: {status} {statusText}`

## TypeScript Client

```typescript
import { createClient } from '@yaci/client'

const client = createClient('https://api.example.com')

// Transactions
const tx = await client.getTransaction(hash)
const txs = await client.getTransactions(20, 0, { status: 'success' })
const addrTxs = await client.getTransactionsByAddress(addr, 50, 0)
const stats = await client.getAddressStats(addr)

// Blocks
const block = await client.getBlock(height)
const blocks = await client.getBlocks(20, 0)

// Search
const results = await client.search('cosmos1...')

// Validators
const overview = await client.getNetworkOverview()
const perf = await client.getValidatorPerformance(opAddr)
const rewards = await client.getValidatorTotalRewards(opAddr)
const leaderboard = await client.getValidatorLeaderboard()

// Staking
const delegations = await client.getDelegatorDelegations(delAddr)
const history = await client.getDelegatorHistory(delAddr, 50, 0)

// Governance
const proposals = await client.getGovernanceProposals(20, 0, 'VOTING')

// Analytics
const chainStats = await client.getChainStats()
const dailyVol = await client.getTxVolumeDaily()
const msgTypes = await client.getMessageTypeStats()
```

See `packages/client/src/types.ts` for complete type definitions.
