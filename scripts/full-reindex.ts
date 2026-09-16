#!/usr/bin/env npx tsx
/**
 * Full Reindex Script
 *
 * Re-processes all raw data through the current schema triggers.
 * This effectively "re-indexes" the entire chain with an upsert approach.
 *
 * What it does:
 * 1. Optionally truncates all derived tables (--clean flag)
 * 2. Re-fires triggers on transactions_raw to populate:
 *    - transactions_main
 *    - messages_raw -> messages_main
 *    - events_raw -> events_main
 *    - delegation_events (via staking trigger)
 *    - validators (via staking trigger)
 *    - governance tables (via governance trigger)
 * 3. Queues EVM transactions for decode worker
 * 4. Refreshes materialized views
 *
 * Usage:
 *   DATABASE_URL="postgres://..." npx tsx scripts/full-reindex.ts
 *   DATABASE_URL="postgres://..." npx tsx scripts/full-reindex.ts --clean     # Truncate first
 *   DATABASE_URL="postgres://..." npx tsx scripts/full-reindex.ts --batch=500 # Custom batch size
 *   DATABASE_URL="postgres://..." npx tsx scripts/full-reindex.ts --start=0   # Start from specific offset
 */

import pg from "pg"
const { Pool } = pg

const DATABASE_URL = process.env.DATABASE_URL
if (!DATABASE_URL) {
	console.error("ERROR: DATABASE_URL environment variable is required")
	process.exit(1)
}

// Parse CLI args
const args = process.argv.slice(2)
const CLEAN_MODE = args.includes("--clean")
const BATCH_SIZE = parseInt(
	args.find((a) => a.startsWith("--batch="))?.split("=")[1] || "200",
	10
)
const START_OFFSET = parseInt(
	args.find((a) => a.startsWith("--start="))?.split("=")[1] || "0",
	10
)

interface Stats {
	transactions_raw: number
	transactions_main: number
	messages_main: number
	events_main: number
	delegation_events: number
	validators: number
}

async function getStats(pool: pg.Pool): Promise<Stats> {
	const result = await pool.query(`
		SELECT
			(SELECT COUNT(*) FROM api.transactions_raw) as transactions_raw,
			(SELECT COUNT(*) FROM api.transactions_main) as transactions_main,
			(SELECT COUNT(*) FROM api.messages_main) as messages_main,
			(SELECT COUNT(*) FROM api.events_main) as events_main,
			(SELECT COUNT(*) FROM api.delegation_events) as delegation_events,
			(SELECT COUNT(*) FROM api.validators) as validators
	`)
	return result.rows[0]
}

async function truncateDerivedTables(pool: pg.Pool): Promise<void> {
	console.log("\n[CLEAN] Truncating derived tables...")

	const tables = [
		// Order matters due to foreign keys
		"api.events_main",
		"api.messages_main",
		"api.transactions_main",
		"api.events_raw",
		"api.messages_raw",
		"api.delegation_events",
		// Don't truncate validators - we want to preserve chain-queried data
		// "api.validators",
	]

	const client = await pool.connect()
	try {
		await client.query("BEGIN")
		for (const table of tables) {
			console.log(`  Truncating ${table}...`)
			await client.query(`TRUNCATE ${table} CASCADE`)
		}
		await client.query("COMMIT")
		console.log("[CLEAN] Done truncating tables\n")
	} catch (err) {
		await client.query("ROLLBACK")
		throw err
	} finally {
		client.release()
	}
}

async function reindexBatch(
	pool: pg.Pool,
	offset: number,
	batchSize: number
): Promise<number> {
	const client = await pool.connect()

	try {
		// UPDATE triggers fire on any column change
		// SET data = data is a no-op but fires AFTER UPDATE triggers
		const result = await client.query(
			`UPDATE api.transactions_raw
			 SET data = data
			 WHERE id IN (
				 SELECT id FROM api.transactions_raw
				 ORDER BY id
				 LIMIT $1 OFFSET $2
			 )`,
			[batchSize, offset]
		)

		return result.rowCount || 0
	} finally {
		client.release()
	}
}

async function refreshMaterializedViews(pool: pg.Pool): Promise<void> {
	console.log("\n[MV] Refreshing materialized views...")

	const views = [
		"api.mv_daily_tx_stats",
		"api.mv_hourly_tx_stats",
		"api.mv_message_type_stats",
	]

	const client = await pool.connect()
	try {
		for (const view of views) {
			try {
				console.log(`  Refreshing ${view}...`)
				await client.query(`REFRESH MATERIALIZED VIEW CONCURRENTLY ${view}`)
			} catch (err: any) {
				// View might not exist or not be concurrent-safe
				console.log(`  Warning: ${view} - ${err.message}`)
			}
		}
		console.log("[MV] Done refreshing views\n")
	} finally {
		client.release()
	}
}

async function updateValidatorStats(pool: pg.Pool): Promise<void> {
	console.log("\n[VALIDATORS] Updating validator creation heights...")

	const client = await pool.connect()
	try {
		// Update validators with creation_height from earliest delegation event
		const result = await client.query(`
			UPDATE api.validators v SET
				creation_height = COALESCE(v.creation_height, de.min_height),
				first_seen_tx = COALESCE(v.first_seen_tx, de.first_tx),
				updated_at = NOW()
			FROM (
				SELECT
					validator_address,
					MIN(height) AS min_height,
					(
						SELECT tx_hash
						FROM api.delegation_events de2
						WHERE de2.validator_address = de.validator_address
						ORDER BY de2.height NULLS LAST, de2.id
						LIMIT 1
					) AS first_tx
				FROM api.delegation_events de
				GROUP BY validator_address
			) de
			WHERE v.operator_address = de.validator_address
			AND (v.creation_height IS NULL OR v.first_seen_tx IS NULL)
		`)
		console.log(`  Updated ${result.rowCount} validators with creation info`)
	} finally {
		client.release()
	}
}

function formatDuration(ms: number): string {
	const seconds = Math.floor(ms / 1000)
	const minutes = Math.floor(seconds / 60)
	const hours = Math.floor(minutes / 60)

	if (hours > 0) {
		return `${hours}h ${minutes % 60}m ${seconds % 60}s`
	} else if (minutes > 0) {
		return `${minutes}m ${seconds % 60}s`
	} else {
		return `${seconds}s`
	}
}

async function main() {
	const pool = new Pool({ connectionString: DATABASE_URL })
	const startTime = Date.now()

	console.log("=".repeat(60))
	console.log("FULL REINDEX - Re-process all raw data through current schema")
	console.log("=".repeat(60))
	console.log(`Clean mode: ${CLEAN_MODE ? "YES (truncate first)" : "NO (upsert)"}`)
	console.log(`Batch size: ${BATCH_SIZE}`)
	console.log(`Start offset: ${START_OFFSET}`)
	console.log()

	try {
		// Get initial stats
		console.log("[STATS] Initial state:")
		const initialStats = await getStats(pool)
		console.log(`  transactions_raw:   ${initialStats.transactions_raw.toLocaleString()}`)
		console.log(`  transactions_main:  ${initialStats.transactions_main.toLocaleString()}`)
		console.log(`  messages_main:      ${initialStats.messages_main.toLocaleString()}`)
		console.log(`  events_main:        ${initialStats.events_main.toLocaleString()}`)
		console.log(`  delegation_events:  ${initialStats.delegation_events.toLocaleString()}`)
		console.log(`  validators:         ${initialStats.validators.toLocaleString()}`)

		// Optionally truncate
		if (CLEAN_MODE) {
			await truncateDerivedTables(pool)
		}

		// Main reindex loop
		const totalTxs = initialStats.transactions_raw
		let offset = START_OFFSET
		let processedTotal = 0
		let lastProgressTime = Date.now()

		console.log("\n[REINDEX] Starting reindex...")
		console.log(`  Total transactions to process: ${(totalTxs - START_OFFSET).toLocaleString()}`)
		console.log()

		while (offset < totalTxs) {
			const processed = await reindexBatch(pool, offset, BATCH_SIZE)

			if (processed === 0) {
				break
			}

			processedTotal += processed
			offset += processed

			// Progress update every 5 seconds or every 1000 transactions
			const now = Date.now()
			if (now - lastProgressTime > 5000 || processedTotal % 1000 === 0) {
				const percent = Math.round((offset / totalTxs) * 100)
				const elapsed = now - startTime
				const rate = processedTotal / (elapsed / 1000)
				const remaining = totalTxs - offset
				const eta = remaining / rate

				process.stdout.write(
					`\r  Progress: ${offset.toLocaleString()}/${totalTxs.toLocaleString()} (${percent}%) | ` +
						`${rate.toFixed(0)} tx/s | ETA: ${formatDuration(eta * 1000)}     `
				)
				lastProgressTime = now
			}

			// Small delay to avoid overwhelming the database
			await new Promise((resolve) => setTimeout(resolve, 50))
		}

		console.log("\n")

		// Update validator stats
		await updateValidatorStats(pool)

		// Refresh materialized views
		await refreshMaterializedViews(pool)

		// Final stats
		console.log("[STATS] Final state:")
		const finalStats = await getStats(pool)
		console.log(`  transactions_main:  ${finalStats.transactions_main.toLocaleString()} (${CLEAN_MODE ? "new" : `+${finalStats.transactions_main - initialStats.transactions_main}`})`)
		console.log(`  messages_main:      ${finalStats.messages_main.toLocaleString()} (${CLEAN_MODE ? "new" : `+${finalStats.messages_main - initialStats.messages_main}`})`)
		console.log(`  events_main:        ${finalStats.events_main.toLocaleString()} (${CLEAN_MODE ? "new" : `+${finalStats.events_main - initialStats.events_main}`})`)
		console.log(`  delegation_events:  ${finalStats.delegation_events.toLocaleString()} (${CLEAN_MODE ? "new" : `+${finalStats.delegation_events - initialStats.delegation_events}`})`)
		console.log(`  validators:         ${finalStats.validators.toLocaleString()} (${CLEAN_MODE ? "preserved" : `+${finalStats.validators - initialStats.validators}`})`)

		const totalTime = Date.now() - startTime
		console.log()
		console.log("=".repeat(60))
		console.log(`REINDEX COMPLETE in ${formatDuration(totalTime)}`)
		console.log(`Processed ${processedTotal.toLocaleString()} transactions`)
		console.log("=".repeat(60))
	} finally {
		await pool.end()
	}
}

main().catch((err) => {
	console.error("\nFatal error:", err)
	process.exit(1)
})
