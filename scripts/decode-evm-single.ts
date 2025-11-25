/**
 * Priority EVM Transaction Decoder
 * Listens for PostgreSQL NOTIFY to decode EVM transactions on-demand
 * Triggered via api.request_evm_decode() RPC function
 */

import pg from 'pg'
import { Transaction, hexlify, keccak256, getAddress } from 'ethers'

const DATABASE_URL = process.env.DATABASE_URL

interface DecodedTx {
  tx_id: string
  hash: string
  from: string
  to: string | null
  nonce: number
  gas_limit: bigint
  gas_price: bigint
  max_fee_per_gas: bigint | null
  max_priority_fee_per_gas: bigint | null
  value: bigint
  data: string
  type: number
  chain_id: bigint | null
  gas_used: number | null
  status: number
  function_name: string | null
  function_signature: string | null
}

interface DecodedLog {
  tx_id: string
  log_index: number
  address: string
  topics: string[]
  data: string
}

const pool = new pg.Pool({
  connectionString: DATABASE_URL,
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
  // Enable TCP keepalive to prevent proxy timeouts
  keepAlive: true,
  keepAliveInitialDelayMillis: 10000,
})

function decodeTransaction(rawBase64: string, txId: string, gasUsed: number | null): DecodedTx | null {
  try {
    const bytes = Uint8Array.from(atob(rawBase64), c => c.charCodeAt(0))
    const hexData = hexlify(bytes)
    const tx = Transaction.from(hexData)
    const hash = keccak256(hexData)

    let functionSignature: string | null = null
    let functionName: string | null = null

    if (tx.data && tx.data.length >= 10) {
      functionSignature = tx.data.slice(0, 10)
      const knownSignatures: Record<string, string> = {
        '0xa9059cbb': 'transfer(address,uint256)',
        '0x23b872dd': 'transferFrom(address,address,uint256)',
        '0x095ea7b3': 'approve(address,uint256)',
        '0x42842e0e': 'safeTransferFrom(address,address,uint256)',
      }
      if (knownSignatures[functionSignature]) {
        functionName = knownSignatures[functionSignature]
      }
    }

    return {
      tx_id: txId,
      hash,
      from: tx.from ? getAddress(tx.from) : '',
      to: tx.to ? getAddress(tx.to) : null,
      nonce: tx.nonce,
      gas_limit: tx.gasLimit,
      gas_price: tx.gasPrice || BigInt(0),
      max_fee_per_gas: tx.maxFeePerGas,
      max_priority_fee_per_gas: tx.maxPriorityFeePerGas,
      value: tx.value,
      data: tx.data,
      type: tx.type || 0,
      chain_id: tx.chainId,
      gas_used: gasUsed,
      status: 1,
      function_name: functionName,
      function_signature: functionSignature,
    }
  } catch (err) {
    console.error(`Failed to decode transaction ${txId}:`, err)
    return null
  }
}

async function decodeSingleTransaction(txId: string): Promise<{
  success: boolean
  message: string
  data?: any
}> {
  const client = await pool.connect()

  try {
    const pending = await client.query(
      `SELECT tx_id, raw_bytes, gas_used
       FROM api.evm_pending_decode
       WHERE tx_id = $1
       LIMIT 1`,
      [txId]
    )

    if (pending.rows.length === 0) {
      const existing = await client.query(
        `SELECT hash FROM api.evm_transactions WHERE tx_id = $1`,
        [txId]
      )

      if (existing.rows.length > 0) {
        return {
          success: true,
          message: 'Transaction already decoded',
          data: existing.rows[0],
        }
      }

      return {
        success: false,
        message: 'Transaction not found in pending queue or decoded transactions',
      }
    }

    const row = pending.rows[0]
    const decoded = decodeTransaction(row.raw_bytes, row.tx_id, row.gas_used)

    if (!decoded) {
      return {
        success: false,
        message: 'Failed to decode transaction',
      }
    }

    await client.query('BEGIN')

    await client.query(
      `INSERT INTO api.evm_transactions
       (tx_id, hash, "from", "to", nonce, gas_limit, gas_price, max_fee_per_gas,
        max_priority_fee_per_gas, value, data, type, chain_id, gas_used, status,
        function_name, function_signature)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
       ON CONFLICT (tx_id) DO NOTHING`,
      [
        decoded.tx_id,
        decoded.hash,
        decoded.from,
        decoded.to,
        decoded.nonce,
        decoded.gas_limit.toString(),
        decoded.gas_price.toString(),
        decoded.max_fee_per_gas?.toString(),
        decoded.max_priority_fee_per_gas?.toString(),
        decoded.value.toString(),
        decoded.data,
        decoded.type,
        decoded.chain_id?.toString(),
        decoded.gas_used,
        decoded.status,
        decoded.function_name,
        decoded.function_signature,
      ]
    )

    await client.query(`DELETE FROM api.evm_pending_decode WHERE tx_id = $1`, [txId])

    await client.query('COMMIT')

    console.log(`✓ Decoded priority transaction: ${txId}`)

    return {
      success: true,
      message: 'Transaction decoded successfully',
      data: decoded,
    }
  } catch (err) {
    await client.query('ROLLBACK')
    console.error(`Error decoding transaction ${txId}:`, err)
    return {
      success: false,
      message: err instanceof Error ? err.message : 'Unknown error',
    }
  } finally {
    client.release()
  }
}

let listenerClient: pg.PoolClient | null = null
let reconnectTimeout: NodeJS.Timeout | null = null

async function startPriorityListener() {
  console.log('[Priority EVM Decoder] Starting...')

  async function connect() {
    try {
      listenerClient = await pool.connect()
      console.log('[Priority EVM Decoder] Listening for evm_decode_priority notifications')

      await listenerClient.query('LISTEN evm_decode_priority')

      listenerClient.on('notification', async (msg) => {
        if (msg.channel === 'evm_decode_priority') {
          const txId = msg.payload
          if (!txId) return

          console.log(`[Priority EVM Decoder] Received request for ${txId}`)

          try {
            const result = await decodeSingleTransaction(txId)
            if (result.success) {
              console.log(`[Priority EVM Decoder] Decoded ${txId}`)
            } else {
              console.log(`[Priority EVM Decoder] Failed ${txId}: ${result.message}`)
            }
          } catch (err) {
            console.error(`[Priority EVM Decoder] Error processing ${txId}:`, err)
          }
        }
      })

      listenerClient.on('error', (err) => {
        console.error('[Priority EVM Decoder] Connection error:', err.message)
        scheduleReconnect()
      })

      listenerClient.on('end', () => {
        console.log('[Priority EVM Decoder] Connection ended')
        scheduleReconnect()
      })

      console.log('[Priority EVM Decoder] Ready')
    } catch (err) {
      console.error('[Priority EVM Decoder] Failed to connect:', err)
      scheduleReconnect()
    }
  }

  function scheduleReconnect() {
    if (reconnectTimeout) return
    if (shuttingDown) return

    listenerClient = null
    console.log('[Priority EVM Decoder] Reconnecting in 5 seconds...')
    reconnectTimeout = setTimeout(() => {
      reconnectTimeout = null
      connect()
    }, 5000)
  }

  await connect()
}

// Start listener
if (!DATABASE_URL) {
  console.error('[Priority EVM Decoder] DATABASE_URL required')
  process.exit(1)
}

startPriorityListener().catch((err) => {
  console.error('[Priority EVM Decoder] Fatal error:', err)
  process.exit(1)
})

let shuttingDown = false

async function shutdown() {
  if (shuttingDown) return
  shuttingDown = true
  console.log('\n[Priority EVM Decoder] Shutting down...')
  if (reconnectTimeout) {
    clearTimeout(reconnectTimeout)
  }
  if (listenerClient) {
    listenerClient.release()
  }
  await pool.end()
  process.exit(0)
}

process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)

export { decodeSingleTransaction }
