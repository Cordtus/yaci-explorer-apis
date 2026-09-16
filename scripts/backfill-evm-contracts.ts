#!/usr/bin/env npx tsx
/**
 * Backfill EVM contracts from existing contract deployment transactions
 * Also backfills tokens from Transfer events in evm_logs
 */

import pg from 'pg'
import { getCreateAddress, keccak256, JsonRpcProvider, Contract } from 'ethers'

const DATABASE_URL = process.env.DATABASE_URL || 'postgres://postgres:foobar@localhost:5432/yaci'
const EVM_RPC_URL = process.env.EVM_RPC_URL || ''

const ERC20_ABI = [
	'function name() view returns (string)',
	'function symbol() view returns (string)',
	'function decimals() view returns (uint8)',
]

let evmProvider: JsonRpcProvider | null = null
function getEvmProvider(): JsonRpcProvider | null {
	if (!EVM_RPC_URL) return null
	if (!evmProvider) {
		evmProvider = new JsonRpcProvider(EVM_RPC_URL)
	}
	return evmProvider
}

async function fetchTokenMetadata(tokenAddress: string): Promise<{ name: string | null; symbol: string | null; decimals: number | null }> {
	const provider = getEvmProvider()
	if (!provider) {
		return { name: null, symbol: null, decimals: null }
	}

	try {
		const contract = new Contract(tokenAddress, ERC20_ABI, provider)
		const [name, symbol, decimals] = await Promise.allSettled([
			contract.name(),
			contract.symbol(),
			contract.decimals(),
		])

		return {
			name: name.status === 'fulfilled' ? name.value : null,
			symbol: symbol.status === 'fulfilled' ? symbol.value : null,
			decimals: decimals.status === 'fulfilled' ? Number(decimals.value) : null,
		}
	} catch (err) {
		return { name: null, symbol: null, decimals: null }
	}
}

async function backfillContracts(pool: pg.Pool): Promise<number> {
	console.log('\n=== Backfilling Contracts ===')

	// Find contract deployments without corresponding evm_contracts entries
	const result = await pool.query(`
		SELECT
			e.tx_id,
			e."from" as creator,
			e.nonce,
			e.data as bytecode,
			t.height as creation_height
		FROM api.evm_transactions e
		JOIN api.transactions_main t ON e.tx_id = t.id
		WHERE e."to" IS NULL
		  AND e.status = 1
		  AND NOT EXISTS (
			SELECT 1 FROM api.evm_contracts c WHERE c.creation_tx = e.tx_id
		  )
	`)

	console.log(`Found ${result.rows.length} missing contracts`)

	let count = 0
	for (const row of result.rows) {
		const contractAddress = getCreateAddress({ from: row.creator, nonce: row.nonce })
		const bytecodeHash = row.bytecode ? keccak256(row.bytecode) : null

		await pool.query(`
			INSERT INTO api.evm_contracts (address, creator, creation_tx, creation_height, bytecode_hash)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (address) DO NOTHING
		`, [contractAddress.toLowerCase(), row.creator.toLowerCase(), row.tx_id, row.creation_height, bytecodeHash])

		count++
		console.log(`  Contract ${count}/${result.rows.length}: ${contractAddress}`)
	}

	return count
}

async function backfillTokens(pool: pg.Pool): Promise<number> {
	console.log('\n=== Backfilling Tokens from Transfer Events ===')

	const TRANSFER_TOPIC = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'

	// Find unique token addresses from Transfer events
	const result = await pool.query(`
		SELECT DISTINCT l.address as token_address, MIN(l.tx_id) as first_tx
		FROM api.evm_logs l
		WHERE l.topics[1] = $1
		  AND array_length(l.topics, 1) >= 3
		  AND NOT EXISTS (
			SELECT 1 FROM api.evm_tokens t WHERE t.address = l.address
		  )
		GROUP BY l.address
	`, [TRANSFER_TOPIC])

	console.log(`Found ${result.rows.length} tokens with Transfer events but no evm_tokens entry`)

	let count = 0
	for (const row of result.rows) {
		// Get height for first_seen
		const heightResult = await pool.query(
			'SELECT height FROM api.transactions_main WHERE id = $1',
			[row.first_tx]
		)
		const height = heightResult.rows[0]?.height || null

		// Try to fetch metadata
		const metadata = await fetchTokenMetadata(row.token_address)

		await pool.query(`
			INSERT INTO api.evm_tokens (address, type, name, symbol, decimals, first_seen_tx, first_seen_height)
			VALUES ($1, 'ERC20', $2, $3, $4, $5, $6)
			ON CONFLICT (address) DO UPDATE SET
				name = COALESCE(api.evm_tokens.name, EXCLUDED.name),
				symbol = COALESCE(api.evm_tokens.symbol, EXCLUDED.symbol),
				decimals = COALESCE(api.evm_tokens.decimals, EXCLUDED.decimals)
		`, [row.token_address, metadata.name, metadata.symbol, metadata.decimals, row.first_tx, height])

		count++
		const label = metadata.symbol || metadata.name || row.token_address
		console.log(`  Token ${count}/${result.rows.length}: ${label}`)
	}

	return count
}

async function backfillTokenTransfers(pool: pg.Pool): Promise<number> {
	console.log('\n=== Backfilling Token Transfers ===')

	const TRANSFER_TOPIC = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'

	// Find Transfer events without corresponding evm_token_transfers entries
	const result = await pool.query(`
		SELECT l.tx_id, l.log_index, l.address as token_address, l.topics, l.data
		FROM api.evm_logs l
		WHERE l.topics[1] = $1
		  AND array_length(l.topics, 1) >= 3
		  AND NOT EXISTS (
			SELECT 1 FROM api.evm_token_transfers t
			WHERE t.tx_id = l.tx_id AND t.log_index = l.log_index
		  )
		LIMIT 10000
	`, [TRANSFER_TOPIC])

	console.log(`Found ${result.rows.length} Transfer events without token_transfers entries`)

	let count = 0
	for (const row of result.rows) {
		const fromAddr = '0x' + row.topics[1].slice(-40)
		const toAddr = '0x' + row.topics[2].slice(-40)
		const value = row.data || '0x0'

		await pool.query(`
			INSERT INTO api.evm_token_transfers (tx_id, log_index, token_address, from_address, to_address, value)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (tx_id, log_index) DO NOTHING
		`, [row.tx_id, row.log_index, row.token_address, fromAddr.toLowerCase(), toAddr.toLowerCase(), value])

		count++
	}

	console.log(`  Inserted ${count} token transfers`)
	return count
}

async function main() {
	console.log('EVM Contracts & Tokens Backfill')
	console.log('================================')
	console.log(`Database: ${DATABASE_URL.replace(/:[^:@]+@/, ':***@')}`)
	console.log(`EVM RPC: ${EVM_RPC_URL || '(not configured - token metadata will not be fetched)'}`)

	const pool = new pg.Pool({ connectionString: DATABASE_URL })

	try {
		const contracts = await backfillContracts(pool)
		const tokens = await backfillTokens(pool)
		const transfers = await backfillTokenTransfers(pool)

		console.log('\n=== Summary ===')
		console.log(`Contracts backfilled: ${contracts}`)
		console.log(`Tokens backfilled: ${tokens}`)
		console.log(`Transfers backfilled: ${transfers}`)
	} finally {
		await pool.end()
	}
}

main().catch(err => {
	console.error('Fatal error:', err)
	process.exit(1)
})
