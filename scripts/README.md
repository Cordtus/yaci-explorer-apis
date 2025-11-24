# Scripts

## decode-evm.ts

EVM transaction decoder worker that extracts transaction data and logs from Yaci-indexed EVM transactions.

### What It Does

1. **Finds pending EVM transactions** - Queries `MsgEthereumTx` transactions that haven't been decoded
2. **Decodes RLP transaction bytes** - Extracts from/to/value/nonce/gas/etc using ethers.js
3. **Extracts logs from protobuf** - Decodes `MsgEthereumTxResponse` for log topics/data
4. **Looks up function signatures** - Queries 4byte.directory for method names
5. **Detects token transfers** - Identifies ERC-20 Transfer events and populates token tables

### Requirements

- Node.js 18+
- PostgreSQL connection to Yaci database
- Internet access for 4byte.directory API

### Usage

```bash
# One-time decode
DATABASE_URL="postgres://user:pass@host:5432/yaci" yarn decode:evm

# With explicit config
DATABASE_URL="postgres://..." yarn decode:evm
```

### Continuous Operation

**Cron (simple)**:
```bash
*/5 * * * * cd /path/to/yaci-explorer-apis && DATABASE_URL="..." yarn decode:evm
```

**Systemd (recommended)**:
```ini
# /etc/systemd/system/evm-decode.timer
[Timer]
OnBootSec=1min
OnUnitActiveSec=5min
```

### Data Flow

```
Yaci Indexer
  ↓ (stores raw EVM bytes + tx response)
messages_raw.data.raw (base64 RLP)
transactions_raw.data.txResponse.data (protobuf)
  ↓
decode-evm.ts
  ├─ Decode RLP → evm_transactions
  ├─ Decode protobuf → evm_logs
  ├─ Parse Transfer logs → evm_tokens, evm_token_transfers
  └─ Lookup 4byte.directory → function_name, function_signature
  ↓
PostgREST API
  ↓
Frontend
```

### What Gets Decoded

#### evm_transactions
- Standard EVM fields (hash, from, to, nonce, gas, value, data, type)
- Gas usage and status
- Function name/signature (from 4byte.directory)

#### evm_logs
- Contract address
- Topics array (event signature + indexed params)
- Data (non-indexed params)

#### evm_tokens (auto-detected)
- ERC-20 tokens (from Transfer events)
- First seen height/tx

#### evm_token_transfers
- Token address, from, to, value
- Parsed from `Transfer(address,address,uint256)` logs

### Performance

- Processes 100 transactions per batch
- ~500ms per transaction with 10 logs
- 4byte.directory requests cached in memory
- Uses database transactions for atomicity

### Logs

```
EVM Transaction Decoder Worker
==============================
Database: postgres://***@***:5432/yaci
Loading proto definitions...
Connected to database
Processing 10 pending EVM transactions...
Decoded tx 488a71f3eb96... -> 0x8c5e4c1197a3... (3 logs)
Decoded tx 7b2fa551a265... -> 0x4a1bc6d8e... (1 logs)

Total decoded: 10 transactions
```

### Error Handling

- Failed transactions are skipped, not blocking
- Protobuf decode errors logged but don't stop batch
- 4byte.directory failures cached as null (won't retry)
- Database errors roll back per-transaction

### Extending

To add support for more token standards:

1. Add event signature constants (e.g., ERC-721 Transfer)
2. Create parsing function (similar to `parseTransferLog`)
3. Handle in log processing loop
4. Insert into appropriate tables

Example:
```typescript
const ERC721_TRANSFER = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'

function parseERC721Transfer(log: DecodedLog) {
  if (log.topics.length === 4) { // ERC-721 has tokenId as topic[3]
    return {
      from: '0x' + log.topics[1].slice(-40),
      to: '0x' + log.topics[2].slice(-40),
      tokenId: BigInt(log.topics[3]).toString()
    }
  }
}
```
