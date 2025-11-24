# Clean Reset & Deployment Guide

This guide covers resetting the devnet and deploying the complete EVM-enabled stack.

## Overview

We've consolidated the schema into a single migration and enhanced the decode worker to extract EVM logs from protobuf-encoded transaction responses (no Yaci modifications needed).

## What's New

### Middleware
- **Single migration** (`001_complete_schema.sql`) - All tables, views, functions
- **Complete EVM domain**: transactions, logs, tokens, token_transfers, contracts
- **Enhanced decode worker** - Extracts logs from `MsgEthereumTxResponse`, detects ERC-20 transfers
- **4byte.directory integration** - Function signature lookups with caching

### Frontend
- **Standard EVM field names** - `hash`, `from`, `to`, `gasLimit`, etc.
- **EVMLogsCard component** - Displays logs with topics/data
- **EVM toggle** - Shows decoded tx + logs in EVM view

---

## Step 1: Reset Database

### Option A: Fresh Start (Recommended)

```bash
cd ~/repos/yaci-explorer-apis

# Backup if needed
fly postgres connect -a yaci-postgrest-db
\copy (SELECT * FROM api.transactions_main) TO 'backup.csv' CSV HEADER;
\q

# Drop and recreate database
fly postgres connect -a yaci-postgrest-db
DROP SCHEMA api CASCADE;
\q
```

### Option B: Keep Yaci Running, Clear Middleware Tables

```bash
fly postgres connect -a yaci-postgrest-db
DROP TABLE IF EXISTS api.evm_transactions CASCADE;
DROP TABLE IF EXISTS api.evm_logs CASCADE;
DROP TABLE IF EXISTS api.evm_tokens CASCADE;
DROP TABLE IF EXISTS api.evm_token_transfers CASCADE;
\q
```

---

## Step 2: Reset Yaci Indexer (Optional)

If you want to reindex from genesis:

```bash
cd ~/repos/yaci
docker compose down -v
docker compose up -d
```

Wait for indexing to catch up (check logs):
```bash
docker compose logs -f --tail=100
```

---

## Step 3: Deploy Middleware

### Install Dependencies

```bash
cd ~/repos/yaci-explorer-apis
yarn install
```

### Run Migrations

```bash
# Get your DATABASE_URL
fly postgres connect -a yaci-postgrest-db -c "SELECT current_database();"

# Set env var
export DATABASE_URL="postgres://user:pass@host:5432/dbname"

# Dry run first
./scripts/migrate.sh --dry-run

# Apply
./scripts/migrate.sh
```

Expected output:
```
Creating schema api...
Creating tables...
Creating indexes...
Creating views...
Creating functions...
Granting permissions...
Migration complete!
```

### Verify Schema

```bash
fly postgres connect -a yaci-postgrest-db

-- Check tables
\dt api.*

-- Should see:
-- blocks_raw, transactions_raw, transactions_main
-- messages_raw, messages_main, events_main
-- evm_transactions, evm_logs, evm_tokens, evm_token_transfers, evm_contracts
-- validators, proposals, proposal_votes, ibc_channels, denom_metadata

-- Check views
\dv api.*

-- Should see:
-- chain_stats, tx_volume_daily, tx_volume_hourly
-- message_type_stats, gas_usage_distribution, tx_success_rate
-- fee_revenue, evm_tx_map, evm_pending_decode

-- Check functions
\df api.*

-- Should see:
-- universal_search, get_transaction_detail
-- get_transactions_paginated, get_transactions_by_address
-- get_address_stats, get_block_time_analysis
```

---

## Step 4: Run Decode Worker

### Test Run

```bash
cd ~/repos/yaci-explorer-apis
yarn decode:evm
```

Expected output:
```
EVM Transaction Decoder Worker
==============================
Database: postgres://***@***:5432/yaci
Loading proto definitions...
Connected to database
Processing 10 pending EVM transactions...
Decoded tx 488a71f3... -> 0x8c5e4c11... (3 logs)
Decoded tx 7b2fa... -> 0x4a1bc... (1 logs)
...
Total decoded: 10 transactions
```

### Verify Decoding

```bash
fly postgres connect -a yaci-postgrest-db

-- Check decoded transactions
SELECT COUNT(*) FROM api.evm_transactions;

-- Check logs
SELECT COUNT(*) FROM api.evm_logs;

-- Check detected tokens
SELECT * FROM api.evm_tokens;

-- Check token transfers
SELECT * FROM api.evm_token_transfers LIMIT 5;
```

### Set Up Continuous Decoding

Create systemd service or cron:

```bash
# Cron (every 5 minutes)
crontab -e
*/5 * * * * cd ~/repos/yaci-explorer-apis && DATABASE_URL="..." yarn decode:evm >> /var/log/evm-decode.log 2>&1
```

Or systemd timer (better for long-running):

```ini
# /etc/systemd/system/evm-decode.service
[Unit]
Description=EVM Transaction Decoder
After=network.target

[Service]
Type=oneshot
WorkingDirectory=/home/user/repos/yaci-explorer-apis
Environment="DATABASE_URL=postgres://..."
ExecStart=/usr/bin/yarn decode:evm
User=user

# /etc/systemd/system/evm-decode.timer
[Unit]
Description=Run EVM decoder every 5 minutes

[Timer]
OnBootSec=1min
OnUnitActiveSec=5min

[Install]
WantedBy=timers.target
```

Enable:
```bash
sudo systemctl enable --now evm-decode.timer
sudo systemctl status evm-decode.timer
```

---

## Step 5: Deploy Frontend

### Build & Deploy

```bash
cd ~/repos/yaci-explorer
yarn build
fly deploy
```

---

## Step 6: Testing

### Test 1: API Endpoints

```bash
# Get an EVM transaction hash from Yaci
curl "https://yaci-postgrest.fly.dev/events_main?event_type=eq.ethereum_tx&limit=1" | jq -r '.[0].id'

# Test transaction detail endpoint
curl "https://yaci-postgrest.fly.dev/rpc/get_transaction_detail?_hash=<TX_HASH>" | jq '.evm_data'

# Should return:
# {
#   "hash": "0x...",
#   "from": "0x...",
#   "to": "0x...",
#   "gasLimit": "...",
#   "gasPrice": "...",
#   ...
# }

# Check logs
curl "https://yaci-postgrest.fly.dev/rpc/get_transaction_detail?_hash=<TX_HASH>" | jq '.evm_logs'

# Should return array of logs with topics
```

### Test 2: Search

```bash
# Search by EVM hash (0x prefix)
curl "https://yaci-postgrest.fly.dev/rpc/universal_search?_query=0x8c5e4c1197a3176aaf2dcad45d4bf87d4a3afa03b10bd35a9f7079e3aabc18ac" | jq

# Should return:
# [
#   {
#     "type": "evm_transaction",
#     "value": { "tx_id": "...", "hash": "0x..." },
#     "score": 100
#   }
# ]
```

### Test 3: Frontend (Manual or Playwright)

Visit: https://yaci-explorer.fly.dev

**Test Cases:**

1. **Search for EVM tx hash**
   - Enter `0x...` in search
   - Should navigate to tx detail with `?evm=true`

2. **EVM Toggle on TX Detail**
   - Navigate to EVM transaction
   - Toggle should be visible
   - Click "EVM View"
   - Should show:
     - EVMTransactionCard with all fields populated
     - EVMLogsCard with logs (if any)

3. **EVM Log Display**
   - Expand log entry
   - Should show:
     - Contract address
     - Topics array
     - Data hex
     - Copy buttons working

4. **Token Transfers**
   - Find ERC-20 Transfer log
   - Topics[0] should be `0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef`
   - Should see "Transfer(address,address,uint256)" label

5. **Function Decoding**
   - EVM tx with input data
   - Should show function name/signature from 4byte.directory

---

## Troubleshooting

### No EVM data showing

```bash
# Check if decode worker ran
SELECT COUNT(*) FROM api.evm_transactions;

# If 0, run manually
cd ~/repos/yaci-explorer-apis
DATABASE_URL="..." yarn decode:evm
```

### Logs not appearing

```bash
# Check if logs exist in raw data
SELECT data->'txResponse'->>'data' FROM api.transactions_raw WHERE id = '<TX_HASH>';

# Should return hex string starting with protobuf data
# If null, Yaci may not be capturing it

# Check decode worker proto parsing
yarn decode:evm 2>&1 | grep -i error
```

### Frontend shows "EVM data not available"

This means `evm_data.hash` is missing. Check API response:
```bash
curl "https://yaci-postgrest.fly.dev/rpc/get_transaction_detail?_hash=<TX_HASH>" | jq '.evm_data.hash'
```

Should return `"0x..."` not `null`.

### Proto decode errors

```bash
# Verify proto file exists
ls ~/repos/yaci-explorer-apis/proto/evm.proto

# Test protobufjs can load it
cd ~/repos/yaci-explorer-apis
npx tsx -e "import('protobufjs').then(p => p.load('proto/evm.proto')).then(console.log)"
```

---

## Next Steps

1. **Cosmos domain watcher** - Detect validator/proposal events, query gRPC, populate domain tables
2. **Token metadata enrichment** - Query ERC-20 name/symbol/decimals via eth_call
3. **Contract verification** - Upload ABI/source to `evm_contracts`
4. **Account profiles** - Unified view of Cosmos + EVM activity

---

## Performance Notes

- Decode worker processes in batches of 100
- 4byte.directory responses are cached in memory
- Each transaction with 10 logs takes ~500ms to process
- Postgres indexes on `evm_logs(address)` and `evm_logs(topics[1])` for fast queries
