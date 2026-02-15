#!/usr/bin/env npx tsx
/**
 * Trim historical blocks_raw.data to reclaim storage
 *
 * Strips redundant header hashes, evidence, pagination, and lastCommit
 * from blocks_raw.data, keeping only the fields used by triggers and the
 * frontend (height, time, chainId, proposerAddress, block.data, blockId).
 *
 * Runs in batches with configurable size and delay to avoid overwhelming
 * the database. Progress is printed every batch. Safe to interrupt and
 * restart -- only processes rows that still have the old format.
 *
 * Prerequisites:
 *   - Migration 073 must be applied (creates api.trim_block_data function)
 *   - Signature extraction triggers must have already processed all blocks
 *     (validator_block_signatures table should be populated)
 *
 * Usage:
 *   DATABASE_URL=postgres://... npx tsx scripts/trim-blocks-raw.ts
 *
 * Environment:
 *   DATABASE_URL or PGRST_DB_URI  -- PostgreSQL connection string
 *   BATCH_SIZE                    -- rows per batch (default: 5000)
 *   DELAY_MS                     -- milliseconds between batches (default: 200)
 *
 * @example
 *   # Trim with defaults (5000 rows/batch, 200ms delay)
 *   DATABASE_URL=postgres://yaci:pass@localhost/yaci npx tsx scripts/trim-blocks-raw.ts
 *
 *   # Aggressive trim (larger batches, less delay)
 *   BATCH_SIZE=20000 DELAY_MS=50 DATABASE_URL=... npx tsx scripts/trim-blocks-raw.ts
 */

import pg from 'pg'

const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '5000', 10)
const DELAY_MS = parseInt(process.env.DELAY_MS || '200', 10)

async function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms))
}

async function main() {
	const dbUri = process.env.DATABASE_URL || process.env.PGRST_DB_URI
	if (!dbUri) {
		console.error('Set DATABASE_URL or PGRST_DB_URI')
		process.exit(1)
	}

	const client = new pg.Client({ connectionString: dbUri })
	await client.connect()

	// Verify the trim function exists
	const { rows: fnCheck } = await client.query(`
		SELECT 1 FROM pg_proc p
		JOIN pg_namespace n ON p.pronamespace = n.oid
		WHERE n.nspname = 'api' AND p.proname = 'trim_block_data'
	`)
	if (fnCheck.length === 0) {
		console.error('api.trim_block_data function not found. Run migration 073 first.')
		await client.end()
		process.exit(1)
	}

	// Get total count of untrimmed rows
	const { rows: countRows } = await client.query(`
		SELECT COUNT(*) AS remaining
		FROM api.blocks_raw
		WHERE data ? 'pagination'
		   OR data #> '{block,header}' ? 'appHash'
		   OR data #> '{block,header}' ? 'app_hash'
		   OR data #> '{block,lastCommit}' IS NOT NULL
		   OR data #> '{block,last_commit}' IS NOT NULL
	`)
	const totalRemaining = parseInt(countRows[0].remaining, 10)

	if (totalRemaining === 0) {
		console.log('All blocks already trimmed.')
		await client.end()
		return
	}

	console.log(`Trimming ${totalRemaining} blocks (batch size: ${BATCH_SIZE}, delay: ${DELAY_MS}ms)`)

	let totalUpdated = 0
	const startTime = Date.now()

	while (true) {
		const { rowCount } = await client.query(`
			UPDATE api.blocks_raw
			SET data = api.trim_block_data(data)
			WHERE id IN (
				SELECT id FROM api.blocks_raw
				WHERE data ? 'pagination'
				   OR data #> '{block,header}' ? 'appHash'
				   OR data #> '{block,header}' ? 'app_hash'
				   OR data ? 'block' AND (
				     data->'block' ? 'lastCommit'
				     OR data->'block' ? 'last_commit'
				   )
				ORDER BY id
				LIMIT $1
			)
		`, [BATCH_SIZE])

		if (!rowCount || rowCount === 0) break

		totalUpdated += rowCount
		const elapsed = ((Date.now() - startTime) / 1000).toFixed(1)
		const pct = ((totalUpdated / totalRemaining) * 100).toFixed(1)
		const rate = (totalUpdated / parseFloat(elapsed)).toFixed(0)
		console.log(
			`  ${totalUpdated}/${totalRemaining} (${pct}%) -- ${rate} rows/s -- ${elapsed}s elapsed`
		)

		if (DELAY_MS > 0) await sleep(DELAY_MS)
	}

	const elapsed = ((Date.now() - startTime) / 1000).toFixed(1)
	console.log(`\nDone: ${totalUpdated} rows trimmed in ${elapsed}s`)
	console.log('Run VACUUM (VERBOSE) api.blocks_raw; to reclaim disk space.')

	await client.end()
}

main().catch((err) => {
	console.error('Fatal:', err)
	process.exit(1)
})
