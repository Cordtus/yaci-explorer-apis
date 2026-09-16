#!/usr/bin/env npx tsx
/**
 * Diagnostic script for staking/delegation data
 *
 * Checks:
 * 1. Are staking messages present in messages_main?
 * 2. Is messages_raw populated with the data?
 * 3. What are the JSON field names in the raw data?
 * 4. Are delegation_events being populated?
 * 5. Are validators being populated?
 *
 * Usage:
 *   DATABASE_URL="postgres://..." npx tsx scripts/diagnose-staking-data.ts
 */

import pg from "pg"
const { Pool } = pg

const DATABASE_URL =
	process.env.DATABASE_URL ||
	"postgres://postgres:foobar@localhost:5432/postgres"

async function main() {
	const pool = new Pool({ connectionString: DATABASE_URL })

	console.log("=== Staking Data Diagnostic Report ===\n")

	try {
		// 1. Check staking messages in messages_main
		console.log("1. Staking messages in messages_main:")
		const msgTypes = await pool.query(`
			SELECT type, COUNT(*) as count
			FROM api.messages_main
			WHERE type LIKE '%MsgDelegate'
			   OR type LIKE '%MsgUndelegate'
			   OR type LIKE '%MsgBeginRedelegate'
			   OR type LIKE '%MsgCreateValidator'
			   OR type LIKE '%MsgEditValidator'
			GROUP BY type
			ORDER BY count DESC
		`)
		if (msgTypes.rows.length === 0) {
			console.log("   [WARN] No staking messages found in messages_main!")
		} else {
			for (const row of msgTypes.rows) {
				console.log(`   ${row.type}: ${row.count}`)
			}
		}

		// 2. Check if messages_raw has data for staking messages
		console.log("\n2. Messages_raw data availability:")
		const rawCheck = await pool.query(`
			SELECT
				m.type,
				COUNT(*) as total_messages,
				COUNT(mr.data) as with_raw_data,
				COUNT(*) - COUNT(mr.data) as missing_raw
			FROM api.messages_main m
			LEFT JOIN api.messages_raw mr ON m.id = mr.id AND m.message_index = mr.message_index
			WHERE m.type LIKE '%MsgDelegate'
			   OR m.type LIKE '%MsgUndelegate'
			   OR m.type LIKE '%MsgBeginRedelegate'
			   OR m.type LIKE '%MsgCreateValidator'
			   OR m.type LIKE '%MsgEditValidator'
			GROUP BY m.type
			ORDER BY m.type
		`)
		for (const row of rawCheck.rows) {
			const status = row.missing_raw > 0 ? "[WARN]" : "[OK]"
			console.log(
				`   ${status} ${row.type}: ${row.with_raw_data}/${row.total_messages} have raw data`
			)
		}

		// 3. Sample the JSON structure
		console.log("\n3. Sample raw JSON structure for MsgCreateValidator:")
		const sampleCreate = await pool.query(`
			SELECT mr.data
			FROM api.messages_raw mr
			JOIN api.messages_main m ON mr.id = m.id AND mr.message_index = m.message_index
			WHERE m.type LIKE '%MsgCreateValidator'
			LIMIT 1
		`)
		if (sampleCreate.rows.length > 0) {
			const data = sampleCreate.rows[0].data
			console.log("   Keys:", Object.keys(data).join(", "))
			if (data.description)
				console.log("   description keys:", Object.keys(data.description).join(", "))
			if (data.commission)
				console.log("   commission keys:", Object.keys(data.commission).join(", "))
			console.log(
				"   validatorAddress:",
				data.validatorAddress || data.validator_address || "[NOT FOUND]"
			)
		} else {
			console.log("   [WARN] No MsgCreateValidator samples found")
		}

		console.log("\n4. Sample raw JSON structure for MsgDelegate:")
		const sampleDelegate = await pool.query(`
			SELECT mr.data
			FROM api.messages_raw mr
			JOIN api.messages_main m ON mr.id = m.id AND mr.message_index = m.message_index
			WHERE m.type LIKE '%MsgDelegate'
			AND m.type NOT LIKE '%MsgBeginRedelegate'
			LIMIT 1
		`)
		if (sampleDelegate.rows.length > 0) {
			const data = sampleDelegate.rows[0].data
			console.log("   Keys:", Object.keys(data).join(", "))
			console.log(
				"   validatorAddress:",
				data.validatorAddress || data.validator_address || "[NOT FOUND]"
			)
			console.log(
				"   delegatorAddress:",
				data.delegatorAddress || data.delegator_address || "[NOT FOUND]"
			)
			if (data.amount) console.log("   amount:", JSON.stringify(data.amount))
		} else {
			console.log("   [WARN] No MsgDelegate samples found")
		}

		// 4. Check delegation_events
		console.log("\n5. Delegation events table:")
		const delegationEvents = await pool.query(`
			SELECT event_type, COUNT(*) as count
			FROM api.delegation_events
			GROUP BY event_type
			ORDER BY count DESC
		`)
		if (delegationEvents.rows.length === 0) {
			console.log("   [WARN] No delegation events found!")
		} else {
			for (const row of delegationEvents.rows) {
				console.log(`   ${row.event_type}: ${row.count}`)
			}
		}

		// 5. Check validators table
		console.log("\n6. Validators table:")
		const validators = await pool.query(`
			SELECT
				COUNT(*) as total,
				COUNT(*) FILTER (WHERE creation_height IS NOT NULL) as with_creation_height,
				COUNT(*) FILTER (WHERE first_seen_tx IS NOT NULL) as with_first_seen_tx,
				COUNT(*) FILTER (WHERE tokens IS NOT NULL) as with_tokens,
				COUNT(*) FILTER (WHERE status = 'BOND_STATUS_BONDED') as bonded
			FROM api.validators
		`)
		const v = validators.rows[0]
		console.log(`   Total validators: ${v.total}`)
		console.log(`   With creation_height: ${v.with_creation_height}`)
		console.log(`   With first_seen_tx: ${v.with_first_seen_tx}`)
		console.log(`   With tokens: ${v.with_tokens}`)
		console.log(`   Bonded: ${v.bonded}`)

		// 6. Check trigger exists
		console.log("\n7. Triggers on relevant tables:")
		const triggers = await pool.query(`
			SELECT
				tgname as trigger_name,
				tgrelid::regclass as table_name,
				pg_get_functiondef(tgfoid) IS NOT NULL as function_exists
			FROM pg_trigger
			WHERE tgrelid IN (
				'api.transactions_main'::regclass,
				'api.messages_main'::regclass
			)
			AND tgname LIKE '%staking%'
			ORDER BY table_name, trigger_name
		`)
		if (triggers.rows.length === 0) {
			console.log("   [WARN] No staking triggers found!")
		} else {
			for (const row of triggers.rows) {
				console.log(`   ${row.table_name}: ${row.trigger_name}`)
			}
		}

		// 7. Check for mismatch between expected and actual events
		console.log("\n8. Message vs Event counts:")
		const mismatch = await pool.query(`
			WITH msg_counts AS (
				SELECT
					CASE
						WHEN type LIKE '%MsgCreateValidator' THEN 'CREATE_VALIDATOR'
						WHEN type LIKE '%MsgEditValidator' THEN 'EDIT_VALIDATOR'
						WHEN type LIKE '%MsgBeginRedelegate' THEN 'REDELEGATE'
						WHEN type LIKE '%MsgUndelegate' THEN 'UNDELEGATE'
						WHEN type LIKE '%MsgDelegate' THEN 'DELEGATE'
					END as event_type,
					COUNT(*) as msg_count
				FROM api.messages_main
				WHERE type LIKE '%MsgDelegate'
				   OR type LIKE '%MsgUndelegate'
				   OR type LIKE '%MsgBeginRedelegate'
				   OR type LIKE '%MsgCreateValidator'
				   OR type LIKE '%MsgEditValidator'
				GROUP BY 1
			),
			event_counts AS (
				SELECT event_type, COUNT(*) as event_count
				FROM api.delegation_events
				GROUP BY event_type
			)
			SELECT
				COALESCE(m.event_type, e.event_type) as event_type,
				COALESCE(m.msg_count, 0) as messages,
				COALESCE(e.event_count, 0) as events,
				COALESCE(m.msg_count, 0) - COALESCE(e.event_count, 0) as missing
			FROM msg_counts m
			FULL OUTER JOIN event_counts e ON m.event_type = e.event_type
			ORDER BY 1
		`)
		for (const row of mismatch.rows) {
			const status = row.missing > 0 ? "[WARN]" : "[OK]"
			console.log(
				`   ${status} ${row.event_type}: ${row.events}/${row.messages} (missing: ${row.missing})`
			)
		}

		console.log("\n=== Diagnostic Complete ===")
	} finally {
		await pool.end()
	}
}

main().catch((err) => {
	console.error("Error:", err)
	process.exit(1)
})
