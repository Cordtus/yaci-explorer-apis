/**
 * Test script for EVM hash lookup functionality
 * Tests that get_transaction_detail can resolve both Cosmos and EVM tx hashes
 *
 * Usage: DATABASE_URL=... npx tsx scripts/test-evm-hash-lookup.ts
 */

import pg from "pg"

const { Pool } = pg

async function main() {
	const pool = new Pool({ connectionString: process.env.DATABASE_URL })

	console.log("Testing EVM hash lookup in get_transaction_detail...\n")

	try {
		// Find an EVM transaction to test with
		const evmTx = await pool.query(`
			SELECT tx_id, hash FROM api.evm_transactions LIMIT 1
		`)

		if (evmTx.rows.length === 0) {
			console.log("No EVM transactions found in database - skipping EVM hash test")
			console.log("Test SKIPPED (no test data)")
			return
		}

		const { tx_id: cosmosTxId, hash: evmHash } = evmTx.rows[0]
		console.log(`Found EVM transaction:`)
		console.log(`  Cosmos tx_id: ${cosmosTxId}`)
		console.log(`  EVM hash: ${evmHash}\n`)

		// Test 1: Lookup by Cosmos tx hash
		console.log("Test 1: Lookup by Cosmos tx hash...")
		const cosmosResult = await pool.query(
			`SELECT api.get_transaction_detail($1) AS result`,
			[cosmosTxId]
		)

		if (!cosmosResult.rows[0]?.result) {
			console.log("FAILED: No result for Cosmos tx hash")
			process.exit(1)
		}
		console.log("  Result ID:", cosmosResult.rows[0].result.id)
		console.log("  PASSED\n")

		// Test 2: Lookup by EVM hash (0x-prefixed)
		console.log("Test 2: Lookup by EVM hash...")
		const evmResult = await pool.query(
			`SELECT api.get_transaction_detail($1) AS result`,
			[evmHash]
		)

		if (!evmResult.rows[0]?.result) {
			console.log("FAILED: No result for EVM hash")
			process.exit(1)
		}
		console.log("  Result ID:", evmResult.rows[0].result.id)
		console.log("  PASSED\n")

		// Test 3: Both should return the same transaction
		console.log("Test 3: Both lookups return same transaction...")
		const cosmosId = cosmosResult.rows[0].result.id
		const evmId = evmResult.rows[0].result.id

		if (cosmosId !== evmId) {
			console.log(`FAILED: IDs don't match (${cosmosId} vs ${evmId})`)
			process.exit(1)
		}
		console.log("  PASSED\n")

		// Test 4: EVM data is present
		console.log("Test 4: EVM data is present in result...")
		if (!evmResult.rows[0].result.evm_data) {
			console.log("FAILED: evm_data is missing")
			process.exit(1)
		}
		console.log("  evm_data.hash:", evmResult.rows[0].result.evm_data.hash)
		console.log("  PASSED\n")

		// Test 5: Non-existent hash returns null
		console.log("Test 5: Non-existent hash returns null...")
		const nullResult = await pool.query(
			`SELECT api.get_transaction_detail($1) AS result`,
			["0x0000000000000000000000000000000000000000000000000000000000000000"]
		)

		if (nullResult.rows[0]?.result?.id) {
			console.log("FAILED: Expected null result for non-existent hash")
			process.exit(1)
		}
		console.log("  PASSED\n")

		console.log("All tests PASSED")

	} catch (err) {
		console.error("Test error:", err)
		process.exit(1)
	} finally {
		await pool.end()
	}
}

main()
