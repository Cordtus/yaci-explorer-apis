# API Reference

All endpoints are accessible via PostgREST at `{BASE_URL}/rpc/{function_name}` or directly at `{BASE_URL}/{view_name}`.

## RPC Functions

### Transactions

#### `get_transaction_detail(_hash text)`
Returns full transaction including messages, events, and EVM data.

```sql
SELECT api.get_transaction_detail('ABC123...');
```

Response includes: `id`, `fee`, `memo`, `error`, `height`, `timestamp`, `messages[]`, `events[]`, `evm_data`, `evm_logs[]`, `raw_data`

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
All transactions involving an address (as sender or mention).

#### `get_address_stats(_address text)`
Returns: `address`, `transaction_count`, `first_seen`, `last_seen`

### Blocks

#### `get_blocks_paginated(_limit, _offset)`
Paginated block list ordered by height descending.

#### `get_block_time_analysis(_limit int)`
Block time statistics for recent blocks.

### Search

#### `universal_search(_query text)`
Searches across blocks, transactions, addresses. Returns array of:
```json
{ "type": "block|transaction|evm_transaction|address|evm_address", "value": {...}, "score": 1 }
```

### Governance

#### `get_governance_proposals(_limit, _offset, _status)`
Paginated proposals with optional status filter (`DEPOSIT_PERIOD`, `VOTING_PERIOD`, `PASSED`, `REJECTED`).

Response includes: `proposal_id`, `title`, `summary`, `status`, `submit_time`, `voting_start_time`, `voting_end_time`, `proposer`, `tally`, `last_updated`

#### `compute_proposal_tally(_proposal_id bigint)`
Recalculates vote tallies from indexed vote messages.

### IBC

#### `get_ibc_stats()`
Returns:
```json
{
  "outgoing_transfers": 100,
  "incoming_transfers": 50,
  "completed_transfers": 140,
  "timed_out_transfers": 10,
  "total_channels": 5,
  "open_channels": 4,
  "active_channels": 3,
  "connected_chains": 3,
  "total_denoms": 15
}
```

#### `get_ibc_transfers(_limit, _offset, _direction)`
| Param | Type | Description |
|-------|------|-------------|
| `_direction` | text | `'outgoing'`, `'incoming'`, or null for all |

Response per transfer: `tx_hash`, `height`, `timestamp`, `direction`, `sender`, `receiver`, `source_channel`, `token_denom`, `token_amount`, `resolved_denom`, `counterparty_chain`, `success`

#### `get_ibc_transfers_by_address(_address, _limit, _offset)`
IBC transfers for a specific address.

#### `get_ibc_connections(_limit, _offset, _chain_id, _state)`
List IBC channels with optional filters.

#### `get_ibc_connection(_channel_id, _port_id)`
Single channel details including counterparty info and client status.

#### `get_ibc_denom_traces(_limit, _offset, _base_denom)`
IBC denom trace information.

#### `resolve_ibc_denom(_ibc_denom text)`
Resolve `ibc/HASH` to full trace with routing info.

#### `resolve_denom(_denom text)`
Resolve any denom (native or IBC) to: `symbol`, `decimals`, `is_native`, `source_chain`, `source_denom`

#### `get_ibc_chains()`
List connected chains with channel counts.

#### `get_ibc_channel_activity()`
Transfer statistics per channel.

#### `get_ibc_volume_timeseries(_hours, _channel)`
Hourly volume data for charts.

#### `get_ibc_heatmap_data(_hours)`
Aggregated IBC activity for heatmap visualization.

### Utilities

#### `refresh_analytics_views()`
Refreshes all materialized views. Call periodically (e.g., hourly).

#### `get_chain_params()`
Returns chain parameters from indexed data.

## Views (Direct Query)

Access via `{BASE_URL}/{view_name}?select=*&order=...&limit=...`

### Analytics Views

| View | Columns | Description |
|------|---------|-------------|
| `chain_stats` | `latest_block`, `total_transactions`, `unique_addresses`, `evm_transactions`, `active_validators` | Overall chain metrics |
| `tx_volume_daily` | `date`, `count` | Daily tx counts |
| `tx_volume_hourly` | `hour`, `count` | Hourly tx counts |
| `daily_active_addresses` | `date`, `active_addresses` | Unique senders per day |
| `message_type_stats` | `type`, `count` | Message type distribution |
| `tx_success_rate` | `total`, `successful`, `failed`, `success_rate_percent` | Success metrics |
| `fee_revenue` | `denom`, `total_amount` | Fees by denomination |
| `gas_usage_distribution` | `p50`, `p90`, `p99`, `avg`, `max` | Gas percentiles |

### Governance Views

| View | Description |
|------|-------------|
| `governance_active_proposals` | Proposals in deposit/voting period |
| `governance_snapshots` | Historical tally snapshots |

### EVM Views

| View | Description |
|------|-------------|
| `evm_tx_map` | Maps EVM hash to Cosmos tx ID |
| `evm_pending_decode` | Transactions awaiting EVM decode |

## TypeScript Client

```typescript
import { createClient } from '@yaci/client'

const client = createClient('https://api.example.com')

// All methods return typed responses
const tx = await client.getTransaction(hash)           // TransactionDetail
const txs = await client.getTransactionsByAddress(addr, 50, 0)  // PaginatedResponse<Transaction>
const stats = await client.getChainStats()             // ChainStats
const ibcStats = await client.getIbcStats()            // IbcStats
```

See `packages/client/src/types.ts` for complete type definitions.

## Pagination

All paginated responses follow this structure:
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

## Error Handling

PostgREST returns HTTP errors:
- `404` - Function/view not found
- `400` - Invalid parameters
- `500` - Database error (check logs)

Client throws `Error` with message format: `RPC {function} failed: {status} {statusText}`
