#!/usr/bin/env npx tsx
/**
 * EVM Transaction Decoder Worker
 * Decodes raw EVM transaction bytes and logs, stores in domain tables
 *
 * Usage: npx tsx scripts/decode-evm.ts
 *
 * Environment variables:
 *   DATABASE_URL - PostgreSQL connection string
 */

import { Transaction, keccak256, getAddress, hexlify } from 'ethers'
import pg from 'pg'
import * as protobuf from 'protobufjs'
import { fileURLToPath } from 'url'
import { dirname, join } from 'path'

const { Pool } = pg

// Config
const DATABASE_URL = process.env.DATABASE_URL || 'postgres://postgres:foobar@localhost:5432/yaci'
const BATCH_SIZE = 100
const FOURBYTE_API = 'https://www.4byte.directory/api/v1/signatures'

// Paths
const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)
const PROTO_PATH = join(__dirname, '..', 'proto', 'evm.proto')

// Function signature cache
const sigCache = new Map<string, { name: string; signature: string } | null>()

// Known ERC-20 event signatures
const TRANSFER_TOPIC = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
const APPROVAL_TOPIC = '0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925'

interface PendingTx {
	tx_id: string
	height: number
	timestamp: string
	raw_bytes: string
	tx_response_data: string | null
	gas_used: number | null
}

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

/**
 * Decode base64-encoded RLP EVM transaction
 */
function decodeTransaction(rawBase64: string, gasUsed: number | null): Omit<DecodedTx, 'tx_id'> | null {
	try {
		const bytes = Uint8Array.from(atob(rawBase64), c => c.charCodeAt(0))
		const hexData = hexlify(bytes)
		const tx = Transaction.from(hexData)
		const hash = keccak256(hexData)

		return {
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
			function_name: null,
			function_signature: null,
		}
	} catch (err) {
		console.error('Failed to decode transaction:', err)
		return null
	}
}

/**
 * Decode MsgEthereumTxResponse from hex data to extract logs
 */
async function decodeTxResponse(
	hexData: string,
	root: protobuf.Root
): Promise<{ logs: DecodedLog[]; gasUsed: number; vmError: string | null } | null> {
	try {
		// Remove 0x prefix if present
		const hex = hexData.startsWith('0x') ? hexData.slice(2) : hexData
		const bytes = Buffer.from(hex, 'hex')

		// First decode TxMsgData
		const TxMsgData = root.lookupType('cosmos.evm.vm.v1.TxMsgData')
		const txMsgData = TxMsgData.decode(bytes) as any

		// Get the first msg_response (should be MsgEthereumTxResponse)
		if (!txMsgData.msgResponses || txMsgData.msgResponses.length === 0) {
			return null
		}

		const msgResponse = txMsgData.msgResponses[0]
		if (!msgResponse.typeUrl?.includes('MsgEthereumTxResponse')) {
			return null
		}

		// Decode the actual response
		const MsgEthereumTxResponse = root.lookupType('cosmos.evm.vm.v1.MsgEthereumTxResponse')
		const response = MsgEthereumTxResponse.decode(msgResponse.value) as any

		// Extract logs
		const logs: DecodedLog[] = (response.logs || []).map((log: any, index: number) => ({
			tx_id: '', // Will be set by caller
			log_index: log.index?.toNumber?.() ?? index,
			address: log.address?.toLowerCase() || '',
			topics: log.topics || [],
			data: log.data ? '0x' + Buffer.from(log.data).toString('hex') : '0x',
		}))

		return {
			logs,
			gasUsed: response.gasUsed?.toNumber?.() ?? 0,
			vmError: response.vmError || null,
		}
	} catch (err) {
		console.error('Failed to decode tx response:', err)
		return null
	}
}

/**
 * Get function selector from input data
 */
function getSelector(data: string): string | null {
	if (!data || data === '0x' || data.length < 10) return null
	return data.slice(0, 10).toLowerCase()
}

/**
 * Lookup function signature from 4byte.directory
 */
async function lookupSignature(selector: string): Promise<{ name: string; signature: string } | null> {
	if (sigCache.has(selector)) {
		return sigCache.get(selector) || null
	}

	try {
		const res = await fetch(`${FOURBYTE_API}/?hex_signature=${selector}`)
		if (!res.ok) {
			sigCache.set(selector, null)
			return null
		}

		const data = await res.json()
		if (!data.results || data.results.length === 0) {
			sigCache.set(selector, null)
			return null
		}

		const sig = {
			name: data.results[0].text_signature.split('(')[0],
			signature: data.results[0].text_signature,
		}
		sigCache.set(selector, sig)
		return sig
	} catch {
		sigCache.set(selector, null)
		return null
	}
}

/**
 * Detect if a log is an ERC-20 Transfer and extract details
 */
function parseTransferLog(log: DecodedLog): {
	tokenAddress: string
	from: string
	to: string
	value: string
} | null {
	if (log.topics.length < 3 || log.topics[0].toLowerCase() !== TRANSFER_TOPIC) {
		return null
	}

	try {
		// topics[1] = from (padded address)
		// topics[2] = to (padded address)
		// data = value
		const from = '0x' + log.topics[1].slice(-40)
		const to = '0x' + log.topics[2].slice(-40)
		const value = log.data === '0x' ? '0' : BigInt(log.data).toString()

		return {
			tokenAddress: log.address,
			from: from.toLowerCase(),
			to: to.toLowerCase(),
			value,
		}
	} catch {
		return null
	}
}

/**
 * Process pending EVM transactions
 */
async function processPendingTransactions(pool: pg.Pool, protoRoot: protobuf.Root): Promise<number> {
	// Get pending transactions with tx response data
	const pending = await pool.query<PendingTx>(`
		SELECT
			t.id AS tx_id,
			t.height,
			t.timestamp,
			m.data->>'raw' AS raw_bytes,
			tr.data->'txResponse'->>'data' AS tx_response_data,
			(
				SELECT e.attr_value::bigint
				FROM api.events_main e
				WHERE e.id = t.id
					AND e.event_type = 'ethereum_tx'
					AND e.attr_key = 'txGasUsed'
				LIMIT 1
			) AS gas_used
		FROM api.transactions_main t
		JOIN api.messages_main mm ON t.id = mm.id
		JOIN api.messages_raw m ON mm.id = m.id AND mm.message_index = m.message_index
		JOIN api.transactions_raw tr ON t.id = tr.id
		WHERE mm.type LIKE '%MsgEthereumTx%'
			AND NOT EXISTS (SELECT 1 FROM api.evm_transactions ev WHERE ev.tx_id = t.id)
		ORDER BY t.height DESC
		LIMIT $1
	`, [BATCH_SIZE])

	if (pending.rows.length === 0) {
		return 0
	}

	console.log(`Processing ${pending.rows.length} pending EVM transactions...`)

	let decoded = 0
	for (const row of pending.rows) {
		if (!row.raw_bytes) {
			console.warn(`No raw bytes for tx ${row.tx_id}`)
			continue
		}

		// Decode transaction
		const tx = decodeTransaction(row.raw_bytes, row.gas_used)
		if (!tx) {
			console.warn(`Failed to decode tx ${row.tx_id}`)
			continue
		}

		// Decode logs from tx response
		let logs: DecodedLog[] = []
		if (row.tx_response_data) {
			const responseData = await decodeTxResponse(row.tx_response_data, protoRoot)
			if (responseData) {
				logs = responseData.logs.map(log => ({ ...log, tx_id: row.tx_id }))
				if (responseData.gasUsed) {
					tx.gas_used = responseData.gasUsed
				}
				if (responseData.vmError) {
					tx.status = 0
				}
			}
		}

		// Lookup function signature
		const selector = getSelector(tx.data)
		if (selector) {
			const sig = await lookupSignature(selector)
			if (sig) {
				tx.function_name = sig.name
				tx.function_signature = sig.signature
			}
		}

		// Insert transaction
		const client = await pool.connect()
		try {
			await client.query('BEGIN')

			// Insert evm_transaction
			await client.query(`
				INSERT INTO api.evm_transactions (
					tx_id, hash, "from", "to", nonce, gas_limit, gas_price,
					max_fee_per_gas, max_priority_fee_per_gas, value, data,
					type, chain_id, gas_used, status, function_name, function_signature
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
				ON CONFLICT (tx_id) DO NOTHING
			`, [
				row.tx_id,
				tx.hash,
				tx.from,
				tx.to,
				tx.nonce,
				tx.gas_limit.toString(),
				tx.gas_price.toString(),
				tx.max_fee_per_gas?.toString() || null,
				tx.max_priority_fee_per_gas?.toString() || null,
				tx.value.toString(),
				tx.data,
				tx.type,
				tx.chain_id?.toString() || null,
				tx.gas_used,
				tx.status,
				tx.function_name,
				tx.function_signature,
			])

			// Insert logs
			for (const log of logs) {
				await client.query(`
					INSERT INTO api.evm_logs (tx_id, log_index, address, topics, data)
					VALUES ($1, $2, $3, $4, $5)
					ON CONFLICT (tx_id, log_index) DO NOTHING
				`, [
					log.tx_id,
					log.log_index,
					log.address,
					log.topics,
					log.data,
				])

				// Check for ERC-20 transfer and create token/transfer records
				const transfer = parseTransferLog(log)
				if (transfer) {
					// Ensure token exists
					await client.query(`
						INSERT INTO api.evm_tokens (address, type, first_seen_tx, first_seen_height)
						VALUES ($1, 'ERC20', $2, $3)
						ON CONFLICT (address) DO NOTHING
					`, [transfer.tokenAddress, row.tx_id, row.height])

					// Insert transfer
					await client.query(`
						INSERT INTO api.evm_token_transfers (tx_id, log_index, token_address, from_address, to_address, value)
						VALUES ($1, $2, $3, $4, $5, $6)
						ON CONFLICT (tx_id, log_index) DO NOTHING
					`, [
						log.tx_id,
						log.log_index,
						transfer.tokenAddress,
						transfer.from,
						transfer.to,
						transfer.value,
					])
				}
			}

			await client.query('COMMIT')
			decoded++
			console.log(`Decoded tx ${row.tx_id} -> ${tx.hash} (${logs.length} logs)`)
		} catch (err) {
			await client.query('ROLLBACK')
			console.error(`Failed to insert tx ${row.tx_id}:`, err)
		} finally {
			client.release()
		}
	}

	return decoded
}

/**
 * Main entry point
 */
async function main() {
	console.log('EVM Transaction Decoder Worker')
	console.log('==============================')
	console.log(`Database: ${DATABASE_URL.replace(/:[^:@]+@/, ':***@')}`)

	// Load proto definitions
	console.log('Loading proto definitions...')
	const protoRoot = await protobuf.load(PROTO_PATH)

	const pool = new Pool({ connectionString: DATABASE_URL })

	try {
		// Test connection
		await pool.query('SELECT 1')
		console.log('Connected to database')

		// Process in batches
		let totalDecoded = 0
		let batchDecoded: number

		do {
			batchDecoded = await processPendingTransactions(pool, protoRoot)
			totalDecoded += batchDecoded

			if (batchDecoded > 0 && batchDecoded === BATCH_SIZE) {
				// Small delay between batches
				await new Promise(r => setTimeout(r, 1000))
			}
		} while (batchDecoded === BATCH_SIZE)

		console.log(`\nTotal decoded: ${totalDecoded} transactions`)
	} finally {
		await pool.end()
	}
}

main().catch(err => {
	console.error('Fatal error:', err)
	process.exit(1)
})
