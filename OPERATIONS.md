# YACI Explorer Stack Operations Guide

This document provides comprehensive coverage of the YACI Explorer three-component blockchain indexing and exploration system.

## System Architecture Overview

### Component Purpose and Interaction

The YACI Explorer stack consists of three specialized components working in concert to provide blockchain data indexing, processing, and visualization:

**1. Yaci Indexer** (github.com/dyphira-git/yaci)
- Go-based blockchain data extraction engine
- Connects directly to Cosmos SDK chain gRPC endpoints
- Continuously polls for new blocks and transactions
- Stores raw blockchain data as JSON in PostgreSQL
- Handles resume logic via `SELECT id FROM api.blocks_raw ORDER BY id DESC LIMIT 1`
- Configurable concurrency and retry mechanisms
- DO NOT MODIFY - external dependency maintained separately

**2. Middleware Layer** (github.com/dyphira-git/yaci-explorer-apis)
- Three-process architecture on Fly.io:
  - `app`: PostgREST API server (port 3000)
  - `worker`: EVM decode daemon (batch processing)
  - `priority_decoder`: Priority EVM decode via NOTIFY/LISTEN
- PostgREST automatically generates REST API from PostgreSQL schema
- SQL functions provide complex business logic (pagination, filtering, aggregation)
- Materialized views for analytics data (daily/hourly tx stats, message types)
- Database triggers parse raw JSON into structured relational tables
- Database triggers extract governance data from indexed MsgSubmitProposal/MsgVote
- No external API dependencies - all data from indexed gRPC transactions
- TypeScript client package for type-safe API consumption
- All business logic lives here - never in frontend or indexer

**3. Frontend Application** (github.com/dyphira-git/yaci-explorer)
- React Router 7 client-only application (SSR disabled)
- TanStack Query manages server state with intelligent caching
- Radix UI components with Tailwind CSS styling
- Consumes middleware via typed API client
- Supports both Cosmos-native and EVM transaction visualization
- IBC denomination resolution with local caching

### Data Flow Pipeline

```
Cosmos SDK Chain (gRPC)
    |
    v
Yaci Indexer
    |
    v (INSERT operations)
PostgreSQL api.transactions_raw
    |
    v (Database triggers fire automatically)
PostgreSQL api.transactions_main/messages_main/events_main
    |                                          |
    +---> Governance triggers                  |
    |     (MsgSubmitProposal/MsgVote)         |
    |         |                                |
    |         v                                |
    |     api.governance_proposals             |
    |                                          |
    +---> PostgREST API (HTTP REST) ---------->+---> Frontend (User Interface)
    |
    +---> EVM Worker (batch daemon)
    |         |
    |         v
    |     api.evm_transactions/evm_logs/evm_token_transfers
    |
    +---> Priority Decoder (NOTIFY/LISTEN)
              |
              v
          On-demand EVM decode for tx detail view
```

**Detailed Flow:**

1. Yaci polls chain gRPC endpoint at configured intervals
2. Extracts block and transaction data with full nested message support
3. Inserts raw JSON into transactions_raw and blocks_raw tables
4. PostgreSQL triggers immediately fire on INSERT:
   - update_transaction_main: Parses transaction metadata
   - update_events_raw: Extracts events array
   - update_message_main: Parses messages with address extraction
   - update_event_main: Normalizes event attributes
5. Governance triggers fire on MsgSubmitProposal/MsgVote:
   - detect_proposal_submission: Extracts proposal data from indexed messages
   - track_governance_vote: Updates vote tallies from indexed vote messages
   - No external REST API polling required
6. PostgREST exposes structured data via auto-generated REST endpoints
7. EVM worker daemon polls evm_pending_decode view every 5 seconds:
   - Identifies MsgEthereumTx messages not yet decoded
   - RLP decodes transaction data
   - Protobuf decodes response data
   - Lookups function signatures from 4byte.directory
   - Stores in evm_transactions, evm_logs, evm_token_transfers
8. Priority decoder listens for NOTIFY on evm_decode_priority channel:
   - Triggered when user views transaction detail (get_transaction_detail RPC)
   - Immediately decodes requested transaction for instant display
9. Frontend queries PostgREST endpoints with TanStack Query
10. TanStack Query provides automatic caching (10s stale time, 5min garbage collection)

## Configuration Reference

### Environment Variables by Component

#### Yaci Indexer Configuration

```bash
# Required
YACI_GRPC_ENDPOINT=rpc.example.com:9090
YACI_POSTGRES_DSN=postgres://yaci_writer:password@host:5432/postgres

# Optional
YACI_START=0                    # Override start height (default: resume from MAX(id), 0 means auto)
YACI_MAX_CONCURRENCY=100       # Maximum block retrieval concurrency (default: 100, Docker default: 5)
YACI_BLOCK_TIME=2              # Polling interval in seconds (default: 2)
YACI_MAX_RETRIES=3             # Connection retry attempts (default: 3)
YACI_INSECURE=false            # Disable TLS (default: false)
YACI_LOGLEVEL=info             # Log level (default: info)
YACI_MAX_RECV_MSG_SIZE=4194304 # Max gRPC message size in bytes (default: 4MB)
```

#### Middleware Configuration

**PostgREST Process:**
```bash
PGRST_DB_URI=postgres://authenticator:password@host:5432/postgres
PGRST_DB_ANON_ROLE=web_anon
PGRST_DB_SCHEMAS=api
PGRST_SERVER_PORT=3000
```

**EVM Worker Process:**
```bash
DATABASE_URL=postgres://postgres:password@host:5432/postgres
POLL_INTERVAL_MS=5000          # Worker polling frequency (default: 5000ms)
BATCH_SIZE=100                 # Transactions per batch (default: 100)
```

#### Frontend Configuration

```bash
VITE_POSTGREST_URL=https://yaci-explorer-apis.fly.dev
VITE_CHAIN_REST_ENDPOINT=https://rest.example.com  # For IBC denom resolution
```

### Database Connection Pooling

**PostgREST Connection Pooling**

PostgREST manages its own internal connection pool to PostgreSQL. Key settings:

```bash
PGRST_DB_POOL=10              # Max connections in pool (default: 10)
PGRST_DB_POOL_TIMEOUT=10      # Connection acquisition timeout in seconds (default: 10s)
PGRST_DB_POOL_ACQUISITION_TIMEOUT=10  # Alternative name for timeout setting
```

For Fly.io shared-cpu-1x instances, start with conservative settings:
- PGRST_DB_POOL=5 (leaves headroom for indexer and worker)
- PGRST_DB_POOL_TIMEOUT=10 (fail fast on contention)

**PgBouncer (Optional)**

Consider adding PgBouncer when:
- Connection churn is high (many short-lived queries)
- Multiple middleware instances are scaled horizontally
- Connection limit errors appear in logs

Basic PgBouncer configuration:
```ini
[databases]
postgres = host=republic-yaci-pg.flycast port=5432 dbname=postgres

[pgbouncer]
pool_mode = transaction
max_client_conn = 100
default_pool_size = 20
```

**Pool modes:**
- `transaction`: Connection returned after each transaction (recommended)
- `session`: Connection held for entire client session (required for prepared statements)

**Fly.io PostgreSQL Connection Limits**

Shared CPU instances have limited connections:
- shared-cpu-1x: ~25 max connections
- shared-cpu-2x: ~50 max connections
- dedicated-cpu-1x: ~100 max connections

Calculate your connection budget:
```
Total = (PostgREST pool * instances) + indexer + worker + admin
Example: (5 * 2) + 3 + 1 + 2 = 17 connections
```

Check current connection usage:
```sql
SELECT count(*) FROM pg_stat_activity WHERE datname = 'postgres';
```

Recommended settings for shared-cpu-1x (25 connection limit):
```bash
# Middleware (2 instances)
PGRST_DB_POOL=5              # 10 connections total

# Indexer
# Uses pgxpool connection pool (managed internally by pgx driver)

# Worker
# Uses single pg.Pool connection

# Total: ~14 connections (11 remaining for admin/monitoring)
```

**Monitoring Connection Usage**

Track connections by application:
```sql
SELECT application_name, count(*)
FROM pg_stat_activity
WHERE datname = 'postgres'
GROUP BY application_name;
```

Monitor connection states:
```sql
SELECT state, count(*)
FROM pg_stat_activity
GROUP BY state;
```

Check for idle connections consuming pool:
```sql
SELECT
  pid,
  usename,
  application_name,
  state,
  state_change,
  NOW() - state_change as idle_duration
FROM pg_stat_activity
WHERE state = 'idle'
  AND datname = 'postgres'
ORDER BY state_change;
```

Terminate long-idle connections if needed:
```sql
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE state = 'idle'
  AND state_change < NOW() - INTERVAL '10 minutes'
  AND datname = 'postgres';
```

### Database Schema Architecture

**Raw Storage Tables (written by Yaci):**
- `api.blocks_raw`: Block data with full header, transactions, last_commit
- `api.transactions_raw`: Transaction data with tx body, auth info, response
- `api.messages_raw`: Flattened messages (populated by trigger)
- `api.events_raw`: Flattened events (populated by trigger)

**Parsed Tables (populated by triggers):**
- `api.transactions_main`: Height, timestamp, fee, memo, error, proposal_ids
- `api.messages_main`: Type, sender, mentions array, metadata
- `api.events_main`: Normalized key-value event attributes

**EVM Tables (populated by worker):**
- `api.evm_transactions`: Decoded EVM tx fields (from, to, value, data, gas)
- `api.evm_logs`: Event logs with topics and data
- `api.evm_token_transfers`: Parsed ERC-20/721 transfers
- `api.evm_tokens`: Token metadata cache
- `api.evm_contracts`: Contract metadata, ABI, and source code storage

**Analytics Views and Materialized Views:**
- `api.mv_daily_tx_stats`: Daily transaction counts, success/fail, unique senders (materialized)
- `api.mv_hourly_tx_stats`: Hourly transaction counts for last 7 days (materialized)
- `api.mv_message_type_stats`: Message type distribution with percentages (materialized)
- `api.tx_volume_daily/hourly`: Transaction counts aggregated by time
- `api.message_type_stats`: Message type distribution
- `api.evm_pending_decode`: View of EVM transactions awaiting decode
- `api.query_stats`: pg_stat_statements wrapper for performance monitoring

**Governance Tables:**
- `api.governance_proposals`: Proposals detected from indexed MsgSubmitProposal
- `api.governance_snapshots`: Historical proposal state snapshots

**Refresh Materialized Views:**
```sql
SELECT api.refresh_analytics_views();
```

### Security Model

**PostgreSQL Roles:**

1. **postgres** (superuser)
   - Full database access
   - Used only for migrations and maintenance
   - Never exposed to applications

2. **yaci_writer** (indexer role)
   - INSERT permission on raw tables only
   - No access to parsed tables or views
   - Used by Yaci indexer

3. **authenticator** (PostgREST connection)
   - LOGIN permission
   - Can switch to web_anon role
   - Used by PostgREST for connection pooling

4. **web_anon** (public API role)
   - SELECT on all main tables and views
   - EXECUTE on API functions only
   - No INSERT, UPDATE, DELETE, TRUNCATE permissions
   - Enforces read-only API access

**Permission Grants:**
```sql
GRANT SELECT ON api.transactions_main TO web_anon;
GRANT SELECT ON api.messages_main TO web_anon;
GRANT SELECT ON api.events_main TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_messages_for_address(TEXT) TO web_anon;
```

## Deployment Procedures

### Initial Deployment from Scratch

**Prerequisites:**
- Fly.io CLI installed and authenticated
- GitHub repository access
- Access to target blockchain gRPC endpoint

**Step 1: Deploy PostgreSQL Database**

```bash
# Create managed PostgreSQL instance
fly postgres create republic-yaci-pg \
  --region sjc \
  --vm-size shared-cpu-1x \
  --volume-size 10

# Save connection string output
# postgres://postgres:PASSWORD@republic-yaci-pg.flycast:5432

# Verify connectivity
fly postgres connect -a republic-yaci-pg
```

**Step 2: Apply Database Migrations**

```bash
cd ~/repos/yaci-explorer-apis

# Apply all migrations in order
cat migrations/001_complete_schema.sql | fly postgres connect -a republic-yaci-pg
cat migrations/002_add_yaci_triggers.sql | fly postgres connect -a republic-yaci-pg
cat migrations/003_fix_proposal_ids_type.sql | fly postgres connect -a republic-yaci-pg

# Verify schema
fly postgres connect -a republic-yaci-pg -c "\dt api.*"
fly postgres connect -a republic-yaci-pg -c "SELECT COUNT(*) FROM pg_trigger WHERE tgrelid = 'api.transactions_raw'::regclass;"
```

**Step 3: Deploy Middleware**

```bash
cd ~/repos/yaci-explorer-apis

# Create app (if not exists)
fly apps create yaci-explorer-apis --org personal

# Set secrets
fly secrets set \
  DATABASE_URL="postgres://postgres:PASSWORD@republic-yaci-pg.flycast:5432/postgres?sslmode=disable" \
  -a yaci-explorer-apis

fly secrets set \
  PGRST_DB_URI="postgres://authenticator:PASSWORD@republic-yaci-pg.flycast:5432/postgres" \
  -a yaci-explorer-apis

# Deploy with fly.toml configuration
fly deploy -a yaci-explorer-apis

# Verify all three processes started
fly status -a yaci-explorer-apis
# Should show: 1 app process + 1 worker process + 1 priority_decoder process

# Test API
curl https://yaci-explorer-apis.fly.dev/
```

**Step 4: Deploy Indexer**

```bash
cd ~/repos/yaci

# Create app (if not exists)
fly apps create republic-yaci-indexer --org personal

# Set secrets
fly secrets set \
  YACI_POSTGRES_DSN="postgres://yaci_writer:PASSWORD@republic-yaci-pg.flycast:5432/postgres" \
  YACI_GRPC_ENDPOINT="rpc.example.com:9090" \
  -a republic-yaci-indexer

# Deploy
fly deploy -a republic-yaci-indexer

# Monitor logs for progress
fly logs -a republic-yaci-indexer
```

**Step 5: Deploy Frontend**

```bash
cd ~/repos/yaci-explorer

# Ensure .env or build-time vars are set
echo "VITE_POSTGREST_URL=https://yaci-explorer-apis.fly.dev" > .env.production

# Create app (if not exists)
fly apps create yaci-explorer --org personal

# Deploy
fly deploy -a yaci-explorer

# Verify
curl https://yaci-explorer.fly.dev
```

### Updating Deployed Components

**Middleware Updates:**
```bash
cd ~/repos/yaci-explorer-apis
git pull origin main
fly deploy -a yaci-explorer-apis
```

**Frontend Updates:**
```bash
cd ~/repos/yaci-explorer
git pull origin main
fly deploy -a yaci-explorer
```

**Database Migrations:**
```bash
# Always test in development first
cat migrations/004_new_migration.sql | fly postgres connect -a republic-yaci-pg
```

**Indexer Updates (use with caution):**
```bash
cd ~/repos/yaci
git pull origin main
fly deploy -a republic-yaci-indexer
```

### Rollback Procedures

**Middleware Rollback:**
```bash
# List recent releases
fly releases -a yaci-explorer-apis

# Rollback to previous version
fly releases rollback <version> -a yaci-explorer-apis
```

**Database Rollback:**
```bash
# Restore from backup
fly postgres connect -a republic-yaci-pg < backup.sql

# Or use Fly backup restore
fly postgres backup restore <backup-id> -a republic-yaci-pg
```

## Operations Manual

### Starting and Stopping Components

**Check Current Status:**
```bash
fly status -a republic-yaci-indexer
fly status -a yaci-explorer-apis
fly status -a yaci-explorer
```

**Stop Indexer (for maintenance):**
```bash
# List machines
fly machines list -a republic-yaci-indexer

# Stop specific machine
fly machine stop <machine-id> -a republic-yaci-indexer
```

**Start Indexer:**
```bash
fly machine start <machine-id> -a republic-yaci-indexer
```

**Restart Middleware:**
```bash
# Restart all machines
fly machine restart <machine-id> -a yaci-explorer-apis

# Or redeploy (cleaner)
cd ~/repos/yaci-explorer-apis
fly deploy -a yaci-explorer-apis
```

**Scale Middleware:**
```bash
# Scale app processes
fly scale count 2 --process-group=app -a yaci-explorer-apis

# Scale worker processes
fly scale count 2 --process-group=worker -a yaci-explorer-apis
```

### Monitoring and Observability

**Key Performance Indicators:**

1. **Indexer Progress**
   - Current indexed block vs chain tip
   - Blocks per second ingestion rate
   - Transaction throughput

2. **Database Health**
   - Row counts in main tables
   - Trigger execution success rate
   - Query response times

3. **Worker Performance**
   - EVM transactions decoded per minute
   - Pending decode queue size
   - Error rate

4. **API Health**
   - HTTP response times
   - Error rate (4xx/5xx)
   - Request volume

**Monitoring Commands:**

```bash
# Check indexer progress
fly postgres connect -a republic-yaci-pg -c "
SELECT
  MAX(id) as latest_block,
  COUNT(*) as total_blocks,
  MAX(data->'block'->'header'->>'time') as latest_block_time
FROM api.blocks_raw;
"

# Check parsed data status
fly postgres connect -a republic-yaci-pg -c "
SELECT
  (SELECT COUNT(*) FROM api.transactions_raw) as raw_txs,
  (SELECT COUNT(*) FROM api.transactions_main) as parsed_txs,
  (SELECT COUNT(*) FROM api.messages_main) as messages,
  (SELECT COUNT(*) FROM api.events_main) as events;
"

# Check EVM decode status
fly postgres connect -a republic-yaci-pg -c "
SELECT
  (SELECT COUNT(*) FROM api.evm_pending_decode) as pending,
  (SELECT COUNT(*) FROM api.evm_transactions) as decoded;
"

# Check governance proposals
fly postgres connect -a republic-yaci-pg -c "
SELECT proposal_id, title, status, last_updated
FROM api.governance_proposals
ORDER BY proposal_id DESC
LIMIT 10;
"

# Check worker logs
fly logs -a yaci-explorer-apis --instance=<worker-id>

# Check API health
curl -i https://yaci-explorer-apis.fly.dev/

# Monitor API logs
fly logs -a yaci-explorer-apis --instance=<app-id>

# Check database connections
fly postgres connect -a republic-yaci-pg -c "
SELECT
  datname,
  usename,
  application_name,
  state,
  query_start
FROM pg_stat_activity
WHERE datname = 'postgres';
"
```

**Setting Up Alerts:**

```bash
# Monitor via fly-log-shipper to external service
fly extensions list
fly extensions create fly-log-shipper

# Or use built-in metrics
fly dashboard -a yaci-explorer-apis
```

### Maintenance Tasks

**Daily:**
- Monitor indexer block height (should track chain tip within seconds)
- Check API health endpoint response
- Review error logs for anomalies

**Weekly:**
```bash
# Vacuum and analyze database
fly postgres connect -a republic-yaci-pg -c "VACUUM ANALYZE;"

# Check table sizes
fly postgres connect -a republic-yaci-pg -c "
SELECT
  schemaname,
  tablename,
  pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size,
  pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) AS table_size,
  pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename)) AS indexes_size
FROM pg_tables
WHERE schemaname = 'api'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
"

# Check slow queries
fly postgres connect -a republic-yaci-pg -c "
SELECT
  query,
  calls,
  total_exec_time,
  mean_exec_time
FROM pg_stat_statements
ORDER BY mean_exec_time DESC
LIMIT 10;
"
```

**Monthly:**
```bash
# Backup database
fly postgres backup create -a republic-yaci-pg

# Review backup retention
fly postgres backup list -a republic-yaci-pg

# Check index health
fly postgres connect -a republic-yaci-pg -c "
SELECT
  schemaname,
  tablename,
  indexname,
  idx_scan,
  idx_tup_read,
  idx_tup_fetch
FROM pg_stat_user_indexes
WHERE schemaname = 'api'
ORDER BY idx_scan;
"
```

### Backup and Recovery Procedures

**Overview:**
Fly.io provides automated daily backups for managed PostgreSQL instances. For blockchain data specifically, re-indexing from the chain is always an option, making traditional backups less critical than for other data types. However, backups significantly reduce recovery time and preserve computed analytics.

#### Fly.io Managed Backups

Fly.io automatically creates daily snapshots of your PostgreSQL database. These backups are stored redundantly and can be restored with simple commands.

**List Available Backups:**
```bash
# View all backups with timestamps and sizes
fly postgres backup list -a republic-yaci-pg

# Output shows:
# ID    CREATED AT           SIZE
# 12345 2024-01-15 03:00:00  2.5 GB
# 12344 2024-01-14 03:00:00  2.4 GB
```

**Restore from Backup:**
```bash
# Restore specific backup to existing database
fly postgres backup restore 12345 -a republic-yaci-pg

# WARNING: This will stop the database and restore the backup
# All connected applications will be disconnected temporarily
```

**Retention Policies:**
- Daily backups retained for 7 days (default Fly.io policy)
- For custom retention, consider paid Fly.io plans
- Backups are stored in same region as database

**Before Restoring:**
1. Stop the indexer to prevent write conflicts
2. Notify users of planned downtime
3. Document current database state (latest block height)
4. Verify backup timestamp matches intended recovery point

#### Manual Backup Procedures

Manual backups provide additional control and allow local storage for compliance or disaster recovery scenarios.

**Create Manual Snapshot:**
```bash
# Trigger on-demand backup
fly postgres backup create -a republic-yaci-pg

# Use for:
# - Before major migrations
# - Before schema changes
# - Before bulk data operations
# - Weekly/monthly archival
```

**Export to Local File:**
```bash
# Step 1: Create proxy tunnel to database
fly proxy 15433:5432 -a republic-yaci-pg &
PROXY_PID=$!

# Step 2: Export using pg_dump
pg_dump "postgres://postgres:PASSWORD@localhost:15433/postgres" \
  --format=custom \
  --compress=9 \
  --file=backup_$(date +%Y%m%d_%H%M%S).dump

# Step 3: Stop proxy
kill $PROXY_PID

# Format options:
# --format=custom: Binary format, best for pg_restore
# --format=plain: SQL text, human-readable
# --compress=9: Maximum compression
```

**Export Specific Schema Only:**
```bash
# Backup only api schema (production data)
pg_dump "postgres://postgres:PASSWORD@localhost:15433/postgres" \
  --schema=api \
  --format=custom \
  --file=api_schema_backup.dump

# Export as SQL for version control
pg_dump "postgres://postgres:PASSWORD@localhost:15433/postgres" \
  --schema-only \
  --schema=api \
  --file=api_schema_$(date +%Y%m%d).sql
```

**Export Specific Tables:**
```bash
# Backup only governance data (lightweight)
pg_dump "postgres://postgres:PASSWORD@localhost:15433/postgres" \
  --table=api.governance_proposals \
  --table=api.governance_snapshots \
  --format=custom \
  --file=governance_backup.dump

# Backup raw tables (largest data)
pg_dump "postgres://postgres:PASSWORD@localhost:15433/postgres" \
  --table=api.blocks_raw \
  --table=api.transactions_raw \
  --format=custom \
  --file=raw_data_backup.dump
```

#### Point-in-Time Recovery (PITR)

PITR allows recovery to any specific timestamp by replaying Write-Ahead Log (WAL) files. This is advanced functionality requiring continuous WAL archiving.

**When PITR Matters:**
- Production mainnet deployments with financial data
- Compliance requirements for data auditability
- Scenarios where re-indexing would take days/weeks
- When exact transaction ordering is critical

**When PITR Doesn't Matter:**
- Development/testnet environments that reset frequently
- Networks where re-indexing from chain takes <24 hours
- Non-financial data where approximation is acceptable

**Enable WAL Archiving (if needed):**
```bash
# Note: Fly.io managed Postgres may have limited PITR support
# Check current WAL settings
fly postgres connect -a republic-yaci-pg -c "SHOW wal_level;"
fly postgres connect -a republic-yaci-pg -c "SHOW archive_mode;"

# For self-managed instances, enable archiving:
# Edit postgresql.conf:
# wal_level = replica
# archive_mode = on
# archive_command = 'test ! -f /archive/%f && cp %p /archive/%f'

# Fly.io users: Contact support for advanced PITR configuration
```

#### Disaster Recovery Scenarios

**Scenario A: Database Corruption**

*Symptom:* PostgreSQL won't start, or reports corruption errors in logs

*Recovery Steps:*
```bash
# Step 1: Stop all connected services
fly machine stop <indexer-id> -a republic-yaci-indexer
fly machine stop <middleware-id> -a yaci-explorer-apis

# Step 2: Identify last good backup
fly postgres backup list -a republic-yaci-pg

# Step 3: Restore from backup
fly postgres backup restore <backup-id> -a republic-yaci-pg

# Step 4: Verify database integrity
fly postgres connect -a republic-yaci-pg -c "
SELECT COUNT(*) FROM api.blocks_raw;
SELECT MAX(id) as latest_block FROM api.blocks_raw;
"

# Step 5: Restart services
fly machine start <indexer-id> -a republic-yaci-indexer
fly machine start <middleware-id> -a yaci-explorer-apis

# Step 6: Monitor indexer catching up
fly logs -a republic-yaci-indexer
```

*Expected Downtime:* 15-30 minutes depending on backup size

**Scenario B: Accidental Data Deletion**

*Symptom:* Critical data was deleted (e.g., `DELETE FROM api.transactions_main WHERE ...`)

*Recovery Steps:*
```bash
# Option 1: Restore specific table from backup
fly proxy 15433:5432 -a republic-yaci-pg &

# Restore only affected table
pg_restore --dbname="postgres://postgres:PASSWORD@localhost:15433/postgres" \
  --table=api.transactions_main \
  --clean \
  backup.dump

# Option 2: Restore to temporary database, copy data over
createdb temp_restore
pg_restore --dbname=temp_restore backup.dump

# Copy missing data
psql -c "INSERT INTO api.transactions_main
         SELECT * FROM temp_restore.api.transactions_main
         WHERE id NOT IN (SELECT id FROM api.transactions_main);"
```

*Expected Downtime:* 5-15 minutes (no service interruption)

**Scenario C: Complete Infrastructure Failure**

*Symptom:* Fly.io region outage, complete data loss, need to rebuild from scratch

*Recovery Steps:*
```bash
# Step 1: Create new PostgreSQL instance in different region
fly postgres create republic-yaci-pg-recovery \
  --region ord \
  --vm-size shared-cpu-1x \
  --volume-size 10

# Step 2: Restore from local backup if available
fly proxy 15433:5432 -a republic-yaci-pg-recovery &
pg_restore --dbname="postgres://postgres:PASSWORD@localhost:15433/postgres" \
  --jobs=4 \
  backup.dump

# Step 3: Apply any migrations since backup
cat migrations/*.sql | fly postgres connect -a republic-yaci-pg-recovery

# Step 4: Update middleware secrets
fly secrets set \
  DATABASE_URL="postgres://postgres:NEWPASS@republic-yaci-pg-recovery.flycast:5432/postgres" \
  PGRST_DB_URI="postgres://authenticator:NEWPASS@republic-yaci-pg-recovery.flycast:5432/postgres" \
  -a yaci-explorer-apis

# Step 5: Update indexer secret
fly secrets set \
  YACI_POSTGRES_DSN="postgres://yaci_writer:NEWPASS@republic-yaci-pg-recovery.flycast:5432/postgres" \
  -a republic-yaci-indexer

# Step 6: Restart all services
fly machine restart <indexer-id> -a republic-yaci-indexer
fly machine restart <middleware-id> -a yaci-explorer-apis

# Step 7: If no backup available, proceed to re-indexing (see below)
```

*Expected Downtime:* 1-2 hours with backup, 12-48 hours without (re-indexing)

#### Recovery Testing

Regular recovery testing validates backup integrity and documents procedures.

**Monthly Recovery Test:**
```bash
# Step 1: Create test database
fly postgres create republic-yaci-pg-test \
  --region sjc \
  --vm-size shared-cpu-1x \
  --volume-size 5

# Step 2: Restore latest backup to test instance
fly postgres backup list -a republic-yaci-pg
fly proxy 15433:5432 -a republic-yaci-pg-test &

pg_restore --dbname="postgres://postgres:PASSWORD@localhost:15433/postgres" \
  --jobs=4 \
  production_backup.dump

# Step 3: Run validation queries
fly postgres connect -a republic-yaci-pg-test -c "
SELECT
  (SELECT COUNT(*) FROM api.blocks_raw) as blocks,
  (SELECT COUNT(*) FROM api.transactions_main) as transactions,
  (SELECT MAX(id) FROM api.blocks_raw) as latest_block;
"

# Step 4: Test API connectivity
export PGRST_DB_URI="postgres://authenticator:PASSWORD@republic-yaci-pg-test.flycast:5432/postgres"
postgrest &
curl http://localhost:3000/transactions_main?limit=10

# Step 5: Document results
echo "Recovery Test $(date): SUCCESS - Restored ${BLOCK_COUNT} blocks in ${DURATION}s" >> recovery_tests.log

# Step 6: Clean up test resources
fly postgres destroy republic-yaci-pg-test
```

**Test Frequency Recommendations:**
- Monthly: Full database restore to test instance
- Quarterly: Complete disaster recovery drill
- After major schema changes: Immediate backup and test restore

#### Data Re-indexing as Alternative

For blockchain data, re-indexing from the chain source is always possible and often preferable to backup restoration.

**When to Re-index vs Restore:**

*Restore from Backup When:*
- Recovery time is critical (minutes vs hours)
- Backup is recent (within last 24 hours)
- Need to preserve computed analytics and materialized views
- Database includes non-blockchain data (governance snapshots, etc.)

*Re-index from Chain When:*
- Backup is stale (>7 days old)
- Schema changed significantly since backup
- Want to validate data integrity from source
- Backup file is corrupted or missing
- Testing indexer improvements/bug fixes

**Re-indexing Steps:**

```bash
# Step 1: Stop indexer
fly machine stop <indexer-id> -a republic-yaci-indexer

# Step 2: Truncate all data tables
fly postgres connect -a republic-yaci-pg -c "
BEGIN;

-- Raw data tables
TRUNCATE api.blocks_raw CASCADE;
TRUNCATE api.transactions_raw CASCADE;

-- Parsed tables (CASCADE will handle these, but explicit for clarity)
TRUNCATE api.transactions_main CASCADE;
TRUNCATE api.messages_main CASCADE;
TRUNCATE api.events_main CASCADE;

-- EVM tables
TRUNCATE api.evm_transactions CASCADE;
TRUNCATE api.evm_logs CASCADE;
TRUNCATE api.evm_token_transfers CASCADE;
TRUNCATE api.evm_tokens CASCADE;

-- Governance tables
TRUNCATE api.governance_proposals CASCADE;
TRUNCATE api.governance_snapshots CASCADE;

-- Refresh materialized views (will be empty)
REFRESH MATERIALIZED VIEW api.mv_daily_tx_stats;
REFRESH MATERIALIZED VIEW api.mv_hourly_tx_stats;
REFRESH MATERIALIZED VIEW api.mv_message_type_stats;

COMMIT;
"

# Step 3: Verify clean state
fly postgres connect -a republic-yaci-pg -c "
SELECT
  (SELECT COUNT(*) FROM api.blocks_raw) as blocks,
  (SELECT COUNT(*) FROM api.transactions_raw) as txs,
  (SELECT COUNT(*) FROM api.governance_proposals) as proposals;
"
# Should return 0, 0, 0

# Step 4: Optionally override start height
# Default: Indexer resumes from MAX(id), which will be 0
# Override to start from specific block:
fly secrets set YACI_START=1000000 -a republic-yaci-indexer

# Step 5: Start indexer
fly machine start <indexer-id> -a republic-yaci-indexer

# Step 6: Monitor progress
fly logs -a republic-yaci-indexer -f

# Step 7: Check progress periodically
watch -n 30 'fly postgres connect -a republic-yaci-pg -c "
SELECT
  MAX(id) as current_block,
  COUNT(*) as total_blocks,
  MAX(data->\"block\"->\"header\"->>\"time\") as latest_time
FROM api.blocks_raw;
"'
```

**Re-indexing Performance Estimates:**

Network conditions vary, but typical rates:
- Light network (100 tx/block): 5,000 blocks/hour
- Medium network (500 tx/block): 2,000 blocks/hour
- Heavy network (2,000 tx/block): 500 blocks/hour

Example re-indexing times:
- 100,000 blocks (light): 20 hours
- 1,000,000 blocks (light): 200 hours (8 days)
- Recent 10,000 blocks (any): 2-3 hours

**Optimization for Faster Re-indexing:**
```bash
# Increase indexer concurrency
fly secrets set YACI_MAX_CONCURRENCY=10 -a republic-yaci-indexer

# Reduce block time for faster polling
fly secrets set YACI_BLOCK_TIME=1 -a republic-yaci-indexer

# Temporarily disable triggers during bulk re-index
fly postgres connect -a republic-yaci-pg -c "
ALTER TABLE api.transactions_raw DISABLE TRIGGER ALL;
ALTER TABLE api.blocks_raw DISABLE TRIGGER ALL;
"

# After re-indexing completes, re-enable and backfill
fly postgres connect -a republic-yaci-pg -c "
ALTER TABLE api.transactions_raw ENABLE TRIGGER ALL;
ALTER TABLE api.blocks_raw ENABLE TRIGGER ALL;
"

# Run backfill script
cd ~/repos/yaci-explorer-apis
fly proxy 15433:5432 -a republic-yaci-pg &
export DATABASE_URL="postgres://postgres:PASSWORD@localhost:15433/postgres"
npx tsx scripts/backfill-triggers.ts
```

**Hybrid Approach:**
For best of both worlds, restore recent backup then re-index from that point forward:
```bash
# Restore backup from 7 days ago (instant)
fly postgres backup restore <backup-id> -a republic-yaci-pg

# Check latest block in backup
fly postgres connect -a republic-yaci-pg -c "SELECT MAX(id) FROM api.blocks_raw;"
# Result: 1,500,000

# Start indexer (automatically resumes from 1,500,000)
fly machine start <indexer-id> -a republic-yaci-indexer

# Only needs to index 7 days of blocks instead of full history
```

### Handling Genesis Resets (Devnet)

When a development network resets from block 1:

```bash
# Step 1: Stop indexer immediately
fly machine stop <indexer-id> -a republic-yaci-indexer

# Step 2: Truncate all data tables
fly postgres connect -a republic-yaci-pg -c "
BEGIN;
TRUNCATE api.blocks_raw CASCADE;
TRUNCATE api.transactions_raw CASCADE;
TRUNCATE api.evm_transactions CASCADE;
TRUNCATE api.evm_logs CASCADE;
TRUNCATE api.evm_token_transfers CASCADE;
TRUNCATE api.evm_tokens CASCADE;
COMMIT;
"

# Step 3: Verify clean state
fly postgres connect -a republic-yaci-pg -c "
SELECT
  (SELECT COUNT(*) FROM api.blocks_raw) as blocks,
  (SELECT COUNT(*) FROM api.transactions_raw) as txs;
"

# Step 4: Start indexer (will automatically resume from block 1)
fly machine start <indexer-id> -a republic-yaci-indexer

# Step 5: Monitor restart
fly logs -a republic-yaci-indexer
```

### Trigger Backfill Process

If database triggers are added after data already exists:

```bash
cd ~/repos/yaci-explorer-apis

# Start proxy to database
fly proxy 15433:5432 -a republic-yaci-pg &

# Run backfill script
export DATABASE_URL="postgres://postgres:PASSWORD@localhost:15433/postgres?sslmode=disable"
npx tsx scripts/backfill-triggers.ts

# Monitor progress (shows batch processing)
# Script will output:
# Total transactions in transactions_raw: N
# Already parsed in transactions_main: M
# Remaining to parse: N-M
# Processing batch starting at offset 0...
# ...
# Backfill complete! Processed N transactions.
```

## Troubleshooting Guide

### Empty Transactions Page

**Symptom:** Frontend shows no transactions despite indexer running

**Diagnosis:**
```bash
# Check if indexer is writing data
fly postgres connect -a republic-yaci-pg -c "SELECT COUNT(*) FROM api.transactions_raw;"

# Check if triggers are parsing data
fly postgres connect -a republic-yaci-pg -c "SELECT COUNT(*) FROM api.transactions_main;"

# If raw has data but main is empty, check triggers exist
fly postgres connect -a republic-yaci-pg -c "
SELECT
  tgname as trigger_name,
  tgrelid::regclass as table_name,
  proname as function_name
FROM pg_trigger
JOIN pg_proc ON pg_trigger.tgfoid = pg_proc.oid
WHERE tgrelid::regclass::text LIKE 'api.%'
ORDER BY tgrelid::regclass, tgname;
"
```

**Resolution:**
```bash
# If triggers missing, apply migration
cat migrations/002_add_yaci_triggers.sql | fly postgres connect -a republic-yaci-pg

# Backfill existing data
npx tsx scripts/backfill-triggers.ts
```

### Indexer Stalled at Block Height

**Symptom:** Indexer logs show same block height repeatedly

**Diagnosis:**
```bash
# Check last indexed block
fly postgres connect -a republic-yaci-pg -c "SELECT MAX(id) FROM api.blocks_raw;"

# Check indexer logs for errors
fly logs -a republic-yaci-indexer | tail -100

# Test gRPC connectivity from indexer
fly ssh console -a republic-yaci-indexer
# Inside container:
grpcurl -plaintext YOUR_GRPC_ENDPOINT:9090 list
```

**Common Causes:**
1. Network connectivity to gRPC endpoint lost
2. Database connection pool exhausted
3. Disk space full on database
4. Chain gRPC endpoint rate limiting

**Resolution:**
```bash
# Restart indexer
fly machine restart <id> -a republic-yaci-indexer

# If database issue, check disk space
fly postgres connect -a republic-yaci-pg -c "SELECT pg_database_size('postgres');"

# Check connection pool
fly postgres connect -a republic-yaci-pg -c "SELECT count(*) FROM pg_stat_activity;"
```

### EVM Transactions Not Decoding

**Symptom:** EVM transactions visible but not decoded

**Diagnosis:**
```bash
# Check worker process status
fly status -a yaci-explorer-apis
# Look for worker process with state=started

# Check pending queue
fly postgres connect -a republic-yaci-pg -c "SELECT COUNT(*) FROM api.evm_pending_decode;"

# Check worker logs
fly logs -a yaci-explorer-apis | grep -i worker

# Check if EVM tables exist
fly postgres connect -a republic-yaci-pg -c "\dt api.evm_*"
```

**Common Causes:**
1. Worker process crashed
2. DATABASE_URL secret not set
3. Network connectivity to 4byte.directory
4. Malformed RLP data

**Resolution:**
```bash
# Restart worker process
fly machine restart <worker-id> -a yaci-explorer-apis

# Verify DATABASE_URL secret
fly secrets list -a yaci-explorer-apis

# Set if missing
fly secrets set DATABASE_URL="postgres://..." -a yaci-explorer-apis

# Check worker logs for specific errors
fly logs -a yaci-explorer-apis -i <worker-id>
```

### API Returning 500 Errors

**Symptom:** PostgREST returns internal server errors

**Diagnosis:**
```bash
# Check PostgREST logs
fly logs -a yaci-explorer-apis -i <app-id>

# Test database connectivity
fly postgres connect -a republic-yaci-pg -c "SELECT 1;"

# Check role permissions
fly postgres connect -a republic-yaci-pg -c "
SELECT
  grantee,
  table_schema,
  table_name,
  privilege_type
FROM information_schema.table_privileges
WHERE grantee = 'web_anon' AND table_schema = 'api';
"
```

**Common Causes:**
1. Database connection string incorrect
2. web_anon role missing permissions
3. SQL function errors
4. Database schema out of sync

**Resolution:**
```bash
# Verify PGRST_DB_URI secret
fly secrets list -a yaci-explorer-apis

# Re-grant permissions
fly postgres connect -a republic-yaci-pg -c "
GRANT SELECT ON ALL TABLES IN SCHEMA api TO web_anon;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA api TO web_anon;
"

# Restart PostgREST
fly machine restart <app-id> -a yaci-explorer-apis
```

### High Database CPU Usage

**Symptom:** Database performance degraded, queries slow

**Diagnosis:**
```bash
# Check active queries
fly postgres connect -a republic-yaci-pg -c "
SELECT
  pid,
  usename,
  application_name,
  state,
  query,
  query_start
FROM pg_stat_activity
WHERE state != 'idle'
ORDER BY query_start;
"

# Check slow queries
fly postgres connect -a republic-yaci-pg -c "
SELECT
  query,
  calls,
  total_exec_time,
  mean_exec_time,
  max_exec_time
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 20;
"

# Check table bloat
fly postgres connect -a republic-yaci-pg -c "
SELECT
  schemaname,
  tablename,
  pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename))
FROM pg_tables
WHERE schemaname = 'api'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
"
```

**Resolution:**
```bash
# Vacuum tables
fly postgres connect -a republic-yaci-pg -c "VACUUM FULL ANALYZE api.transactions_raw;"

# Add missing indexes
fly postgres connect -a republic-yaci-pg -c "
CREATE INDEX CONCURRENTLY idx_tx_timestamp ON api.transactions_main(timestamp DESC);
"

# Scale database if needed
fly postgres update --vm-size shared-cpu-2x -a republic-yaci-pg
```

## Development Workflow

### Local Development Setup

```bash
# Clone all three repositories
git clone https://github.com/dyphira-git/yaci.git
git clone https://github.com/dyphira-git/yaci-explorer-apis.git
git clone https://github.com/dyphira-git/yaci-explorer.git

# Start local PostgreSQL
docker run -d \
  --name yaci-postgres \
  -e POSTGRES_PASSWORD=foobar \
  -p 5432:5432 \
  postgres:16

# Apply migrations
cat yaci-explorer-apis/migrations/*.sql | psql postgres://postgres:foobar@localhost/postgres

# Run indexer locally
cd yaci
go run main.go extract postgres testnet.example.com:9090 \
  -p "postgres://postgres:foobar@localhost/postgres" --live

# Run middleware locally
cd yaci-explorer-apis
export PGRST_DB_URI="postgres://postgres:foobar@localhost/postgres"
export PGRST_DB_ANON_ROLE="web_anon"
postgrest &
export DATABASE_URL="postgres://postgres:foobar@localhost/postgres"
npx tsx scripts/decode-evm-daemon.ts &

# Run frontend locally
cd yaci-explorer
echo "VITE_POSTGREST_URL=http://localhost:3000" > .env.local
yarn dev
```

### Testing Changes

**Middleware Changes:**
```bash
cd yaci-explorer-apis

# Type check
yarn typecheck

# Test migration
cat migrations/00X_test.sql | psql $DATABASE_URL

# Test worker locally
export DATABASE_URL="..."
npx tsx scripts/decode-evm-daemon.ts
```

**Frontend Changes:**
```bash
cd yaci-explorer

# Type check
yarn typecheck

# Build test
yarn build

# Run tests (if implemented)
yarn test
```

### Creating New Migrations

```bash
cd yaci-explorer-apis

# Create new migration file
cat > migrations/004_new_feature.sql << 'EOF'
BEGIN;

-- Add new columns, tables, functions, etc.

COMMIT;
EOF

# Test locally first
cat migrations/004_new_feature.sql | psql $LOCAL_DATABASE_URL

# Create down migration
cat > migrations/004_new_feature.down.sql << 'EOF'
BEGIN;

-- Reverse all changes

COMMIT;
EOF

# Apply to production when ready
cat migrations/004_new_feature.sql | fly postgres connect -a republic-yaci-pg
```

## CI/CD Pipeline

### GitHub Actions Workflows

**Build Workflow** (.github/workflows/build.yml)
- Triggers: Pull requests to main, pushes to main
- Steps:
  1. Checkout code
  2. Setup Node.js 20 with yarn cache
  3. Install dependencies
  4. Type check with TypeScript
  5. Validate migration files (BEGIN/COMMIT checks)

**Deploy Workflow** (.github/workflows/deploy.yml)
- Triggers: After successful build workflow on main, manual workflow_dispatch
- Steps:
  1. Checkout code
  2. Setup flyctl
  3. Deploy to Fly.io with FLY_API_TOKEN secret

### Branch Strategy

- **main**: Production deployments (auto-deploy on merge)
- **middleware-migration**: Active development branch
- **feature/*****: Feature branches (PR to main)

### Required Secrets

GitHub repository secrets:
- `FLY_API_TOKEN`: Fly.io API token for deployments

### Manual Deployment

```bash
# Deploy without CI/CD
cd yaci-explorer-apis
fly deploy --remote-only -a yaci-explorer-apis

cd yaci-explorer
fly deploy --remote-only -a yaci-explorer
```

## Advanced Topics

### Custom SQL Functions

Example function for complex queries:

```sql
CREATE OR REPLACE FUNCTION api.get_address_summary(_address TEXT)
RETURNS JSONB
LANGUAGE SQL STABLE
AS $$
  WITH tx_stats AS (
    SELECT
      COUNT(DISTINCT t.id) as total_txs,
      SUM((t.fee->'amount'->0->>'amount')::BIGINT) as total_fees,
      MIN(t.timestamp) as first_seen,
      MAX(t.timestamp) as last_seen
    FROM api.transactions_main t
    JOIN api.messages_main m ON t.id = m.id
    WHERE m.sender = _address OR m.mentions @> ARRAY[_address]
  )
  SELECT jsonb_build_object(
    'address', _address,
    'total_transactions', COALESCE(total_txs, 0),
    'total_fees_paid', COALESCE(total_fees, 0),
    'first_seen', first_seen,
    'last_seen', last_seen
  )
  FROM tx_stats;
$$;

GRANT EXECUTE ON FUNCTION api.get_address_summary(TEXT) TO web_anon;
```

### Performance Optimization

**Indexing Strategy:**
```sql
-- Add covering indexes for common queries
CREATE INDEX CONCURRENTLY idx_msg_sender_height
ON api.messages_main(sender, height DESC)
WHERE message_index < 10000;

-- Partial indexes for EVM transactions
CREATE INDEX CONCURRENTLY idx_evm_tx_hash
ON api.evm_transactions(hash)
WHERE status = 1;

-- GIN indexes for array searches
CREATE INDEX CONCURRENTLY idx_msg_mentions_gin
ON api.messages_main USING GIN(mentions);
```

**Query Optimization:**
```sql
-- Use CTEs for complex queries
WITH recent_blocks AS (
  SELECT id, data
  FROM api.blocks_raw
  WHERE id > (SELECT MAX(id) - 100 FROM api.blocks_raw)
)
SELECT ...;

-- Avoid N+1 queries
SELECT
  t.*,
  (SELECT jsonb_agg(m.*) FROM api.messages_main m WHERE m.id = t.id) as messages
FROM api.transactions_main t;
```

### Scaling Considerations

**Vertical Scaling:**
```bash
# Scale database
fly postgres update --vm-size dedicated-cpu-1x -a republic-yaci-pg

# Scale middleware
fly scale vm shared-cpu-2x -a yaci-explorer-apis
```

**Horizontal Scaling:**
```bash
# Add more API instances
fly scale count 3 --process-group=app -a yaci-explorer-apis

# Add read replicas (requires Fly Postgres HA)
fly postgres attach --app yaci-explorer-apis republic-yaci-pg
```

**Partitioning Strategy (future):**
```sql
-- Time-based partitioning for large tables
CREATE TABLE api.transactions_main_2024_01 PARTITION OF api.transactions_main
FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
```

## Appendix

### Useful SQL Queries

```sql
-- Find high-value transactions
SELECT id, height, (fee->'amount'->0->>'amount')::BIGINT as fee_amount
FROM api.transactions_main
ORDER BY fee_amount DESC NULLS LAST
LIMIT 100;

-- Message type distribution
SELECT type, COUNT(*) as count
FROM api.messages_main
WHERE message_index < 10000
GROUP BY type
ORDER BY count DESC;

-- Recent EVM transactions
SELECT
  et.hash,
  et."from",
  et."to",
  et.value::TEXT,
  t.timestamp
FROM api.evm_transactions et
JOIN api.transactions_main t ON et.tx_id = t.id
ORDER BY t.timestamp DESC
LIMIT 50;

-- Token transfer summary
SELECT
  token_address,
  COUNT(*) as transfer_count,
  COUNT(DISTINCT from_address) as unique_senders
FROM api.evm_token_transfers
GROUP BY token_address
ORDER BY transfer_count DESC;
```

### Common Fly.io Commands

```bash
# SSH into running machine
fly ssh console -a yaci-explorer-apis

# Execute command in container
fly ssh console -a yaci-explorer-apis -C "ls -la"

# Copy files from container
fly ssh sftp get /path/in/container /local/path -a yaci-explorer-apis

# View machine metrics
fly dashboard -a yaci-explorer-apis

# List all apps
fly apps list

# View app configuration
fly config show -a yaci-explorer-apis
```

### Environment-Specific Configurations

**Development:**
- Local PostgreSQL
- Local PostgREST
- Vite dev server with hot reload
- Mock data for testing

**Staging:**
- Fly.io staging apps (yaci-explorer-staging)
- Separate database (republic-yaci-pg-staging)
- Connected to testnet

**Production:**
- Fly.io production apps
- Production database with backups
- Connected to mainnet
- Auto-scaling enabled

This guide provides comprehensive coverage for operating the YACI Explorer stack. For component-specific details, refer to individual repository documentation. For issues not covered here, check GitHub issues or contact the development team.
