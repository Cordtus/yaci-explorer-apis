#!/usr/bin/env npx tsx
/**
 * Trim historical IBC MsgUpdateClient proof data to reclaim storage
 *
 * Strips clientMessage.signedHeader, clientMessage.validatorSet, and
 * clientMessage.trustedValidators from MsgUpdateClient messages in both
 * transactions_raw and messages_raw. These contain full counterparty chain
 * headers and validator sets (~80KB per message) used only for on-chain
 * light client verification -- never queried by the explorer.
 *
 * Runs in batches with configurable size and delay to avoid overwhelming
 * the database. Progress is printed every batch. Safe to interrupt and
 * restart -- only processes rows that still have untrimmed clientMessage data.
 *
 * Prerequisites:
 *   - Migration 074 must be applied (creates trim functions and triggers)
 *
 * Usage:
 *   DATABASE_URL=postgres://... npx tsx scripts/trim-ibc-update-client.ts
 *
 * Environment:
 *   DATABASE_URL or PGRST_DB_URI  -- PostgreSQL connection string
 *   BATCH_SIZE                    -- rows per batch (default: 1000)
 *   DELAY_MS                     -- milliseconds between batches (default: 500)
 *
 * @example
 *   # Trim with defaults (1000 rows/batch, 500ms delay)
 *   DATABASE_URL=postgres://yaci:pass@localhost/yaci npx tsx scripts/trim-ibc-update-client.ts
 *
 *   # Aggressive trim (larger batches, less delay)
 *   BATCH_SIZE=5000 DELAY_MS=100 DATABASE_URL=... npx tsx scripts/trim-ibc-update-client.ts
 */

import pg from 'pg'

const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '1000', 10)
const DELAY_MS = parseInt(process.env.DELAY_MS || '500', 10)

async function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms))
}

async function trimTable(
	client: pg.Client,
	table: 'transactions_raw' | 'messages_raw',
	fnName: string,
	countQuery: string,
	updateQuery: string
): Promise<number> {
	const { rows: countRows } = await client.query(countQuery)
	const totalRemaining = parseInt(countRows[0].remaining, 10)

	if (totalRemaining === 0) {
		console.log(`  ${table}: all rows already trimmed.`)
		return 0
	}

	console.log(`  ${table}: ${totalRemaining} rows to trim (batch: ${BATCH_SIZE}, delay: ${DELAY_MS}ms)`)

	let totalUpdated = 0
	const startTime = Date.now()

	while (true) {
		const { rowCount } = await client.query(updateQuery, [BATCH_SIZE])

		if (!rowCount || rowCount === 0) break

		totalUpdated += rowCount
		const elapsed = ((Date.now() - startTime) / 1000).toFixed(1)
		const pct = ((totalUpdated / totalRemaining) * 100).toFixed(1)
		const rate = (totalUpdated / parseFloat(elapsed)).toFixed(0)
		console.log(
			`    ${totalUpdated}/${totalRemaining} (${pct}%) -- ${rate} rows/s -- ${elapsed}s elapsed`
		)

		if (DELAY_MS > 0) await sleep(DELAY_MS)
	}

	const elapsed = ((Date.now() - startTime) / 1000).toFixed(1)
	console.log(`  ${table}: ${totalUpdated} rows trimmed in ${elapsed}s`)
	return totalUpdated
}

async function main() {
	const dbUri = process.env.DATABASE_URL || process.env.PGRST_DB_URI
	if (!dbUri) {
		console.error('Set DATABASE_URL or PGRST_DB_URI')
		process.exit(1)
	}

	const client = new pg.Client({ connectionString: dbUri })
	await client.connect()

	// Verify the trim functions exist
	const { rows: fnCheck } = await client.query(`
		SELECT p.proname FROM pg_proc p
		JOIN pg_namespace n ON p.pronamespace = n.oid
		WHERE n.nspname = 'api'
		  AND p.proname IN ('trim_ibc_update_client_tx', 'trim_ibc_update_client_msg')
	`)
	const foundFns = fnCheck.map((r: { proname: string }) => r.proname)
	if (!foundFns.includes('trim_ibc_update_client_tx') || !foundFns.includes('trim_ibc_update_client_msg')) {
		console.error('Trim functions not found. Run migration 074 first.')
		console.error(`  Found: ${foundFns.join(', ') || '(none)'}`)
		await client.end()
		process.exit(1)
	}

	console.log('Trimming IBC MsgUpdateClient proof data...\n')

	// Phase 1: Trim transactions_raw
	// Detect untrimmed rows: tx data contains MsgUpdateClient with a clientMessage
	// that has signedHeader (the largest field, always present in untrimmed data)
	const txTrimmed = await trimTable(
		client,
		'transactions_raw',
		'trim_ibc_update_client_tx',
		`
		SELECT COUNT(*) AS remaining
		FROM api.transactions_raw
		WHERE data::text LIKE '%MsgUpdateClient%'
		  AND data::text LIKE '%signedHeader%'
		`,
		`
		UPDATE api.transactions_raw
		SET data = api.trim_ibc_update_client_tx(data)
		WHERE id IN (
			SELECT id FROM api.transactions_raw
			WHERE data::text LIKE '%MsgUpdateClient%'
			  AND data::text LIKE '%signedHeader%'
			ORDER BY id
			LIMIT $1
		)
		`
	)

	// Phase 2: Trim messages_raw
	const msgTrimmed = await trimTable(
		client,
		'messages_raw',
		'trim_ibc_update_client_msg',
		`
		SELECT COUNT(*) AS remaining
		FROM api.messages_raw
		WHERE data->>'@type' = '/ibc.core.client.v1.MsgUpdateClient'
		  AND data->'clientMessage' ? 'signedHeader'
		`,
		`
		UPDATE api.messages_raw
		SET data = api.trim_ibc_update_client_msg(data)
		WHERE (id, message_index) IN (
			SELECT id, message_index FROM api.messages_raw
			WHERE data->>'@type' = '/ibc.core.client.v1.MsgUpdateClient'
			  AND data->'clientMessage' ? 'signedHeader'
			ORDER BY id, message_index
			LIMIT $1
		)
		`
	)

	console.log(`\nDone: ${txTrimmed} transactions + ${msgTrimmed} messages trimmed.`)
	if (txTrimmed > 0 || msgTrimmed > 0) {
		console.log('Run VACUUM (VERBOSE) api.transactions_raw, api.messages_raw; to reclaim disk space.')
	}

	await client.end()
}

main().catch((err) => {
	console.error('Fatal:', err)
	process.exit(1)
})
