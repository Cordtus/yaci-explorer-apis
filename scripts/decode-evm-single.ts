/**
 * On-demand EVM Transaction Decoder
 * Provides HTTP endpoint for immediate EVM transaction decoding
 */

import pg from 'pg'
import { Transaction, hexlify, keccak256, getAddress } from 'ethers'
import { createServer } from 'http'

const DATABASE_URL = process.env.DATABASE_URL
const PORT = parseInt(process.env.EVM_DECODE_PORT || '3001')

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

const server = createServer(async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*')
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type')

  if (req.method === 'OPTIONS') {
    res.writeHead(200)
    res.end()
    return
  }

  if (req.method === 'POST' && req.url === '/decode') {
    let body = ''
    req.on('data', chunk => {
      body += chunk.toString()
    })
    req.on('end', async () => {
      try {
        const { txId } = JSON.parse(body)
        if (!txId) {
          res.writeHead(400, { 'Content-Type': 'application/json' })
          res.end(JSON.stringify({ success: false, message: 'txId required' }))
          return
        }

        const result = await decodeSingleTransaction(txId)
        res.writeHead(result.success ? 200 : 400, { 'Content-Type': 'application/json' })
        res.end(JSON.stringify(result))
      } catch (err) {
        res.writeHead(500, { 'Content-Type': 'application/json' })
        res.end(
          JSON.stringify({
            success: false,
            message: err instanceof Error ? err.message : 'Server error',
          })
        )
      }
    })
  } else if (req.method === 'GET' && req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' })
    res.end(JSON.stringify({ status: 'ok' }))
  } else {
    res.writeHead(404, { 'Content-Type': 'application/json' })
    res.end(JSON.stringify({ success: false, message: 'Not found' }))
  }
})

if (require.main === module) {
  if (!DATABASE_URL) {
    console.error('DATABASE_URL environment variable is required')
    process.exit(1)
  }

  server.listen(PORT, () => {
    console.log(`[EVM Decode API] Listening on port ${PORT}`)
    console.log(`[EVM Decode API] POST /decode with {"txId": "..."} to decode`)
    console.log(`[EVM Decode API] GET /health for health check`)
  })

  process.on('SIGINT', () => {
    console.log('\n[EVM Decode API] Shutting down...')
    server.close(() => {
      pool.end()
      process.exit(0)
    })
  })

  process.on('SIGTERM', () => {
    console.log('\n[EVM Decode API] Shutting down...')
    server.close(() => {
      pool.end()
      process.exit(0)
    })
  })
}

export { decodeSingleTransaction }
