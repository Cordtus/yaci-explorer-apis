# Scripts

Run with Bun (`bun run scripts/<name>.ts`). All database scripts need
`DATABASE_URL`; `package.json` has shortcuts for the common ones.

| Script | Purpose |
|--------|---------|
| `decode-evm-daemon.ts` | EVM decode daemon (continuous polling) |
| `decode-evm-single.ts` | Priority decoder, listens for `NOTIFY` from `api.request_evm_decode()` |
| `decode-evm.ts` | One-shot EVM decode of the pending queue |
| `chain-params-daemon.ts` | IBC denom traces and chain parameter resolution |
| `chain-query-service.ts` | HTTP gRPC proxy: balances, staking, slashing, auth, tx broadcast |
| `api-gateway.ts` | Routes `/chain/*` to the chain query service, everything else to PostgREST |
| `validator-refresh.ts` | Event-driven validator refresh via `pg_notify` |
| `backfill-evm-contracts.ts` | Backfill `evm_tokens` / `evm_token_transfers` from decoded logs |
| `backfill-evm-logs.ts` | Re-extract EVM logs from `transactions_raw` |
| `test-evm-hash-lookup.ts` | Verify `get_transaction_detail` resolves Cosmos and EVM hashes |
| `migrate.sh` | Apply `migrations/*.sql` in order (`bun run migrate`) |
| `deploy.sh` | LXD/systemd deploy entrypoint (see README) |

Environment variables are listed in the README. EVM metadata (token name,
symbol, decimals) is only fetched when `EVM_RPC_URL` is set.
