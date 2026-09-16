#!/usr/bin/env bun
/**
 * Backfill bech32 consensus addresses into validator_consensus_addresses table.
 *
 * The mapping table originally only had base64 entries (from block signatures).
 * CometBFT finalize_block_events use bech32 format (raivalcons1...).
 * This script derives bech32 from the hex addresses in the validators table
 * and inserts them so SQL JOINs work via exact match.
 *
 * Safe to run multiple times (uses ON CONFLICT).
 */

import { bech32 } from "bech32"
import postgres from "postgres"

const DATABASE_URL = process.env.DATABASE_URL
if (!DATABASE_URL) {
	console.error("DATABASE_URL is required")
	process.exit(1)
}

const BECH32_PREFIX = process.env.BECH32_CONS_PREFIX || "raivalcons"
const sql = postgres(DATABASE_URL)

async function main() {
	// Get all validators with hex consensus addresses
	const validators = await sql`
		SELECT operator_address, consensus_address
		FROM api.validators
		WHERE consensus_address IS NOT NULL
	`

	console.log(`[bech32-backfill] Found ${validators.length} validators with hex consensus addresses`)

	let inserted = 0
	let skipped = 0

	for (const v of validators) {
		const hexAddr = v.consensus_address as string

		try {
			const bytes = Buffer.from(hexAddr, "hex")
			const words = bech32.toWords(bytes)
			const bech32Addr = bech32.encode(BECH32_PREFIX, words)

			const result = await sql`
				INSERT INTO api.validator_consensus_addresses
					(consensus_address, operator_address, hex_address)
				VALUES
					(${bech32Addr}, ${v.operator_address}, ${hexAddr.toUpperCase()})
				ON CONFLICT (consensus_address) DO UPDATE SET
					hex_address = EXCLUDED.hex_address,
					operator_address = EXCLUDED.operator_address
			`
			inserted++
		} catch (err) {
			skipped++
		}
	}

	// Also backfill hex_address for existing base64 entries
	const base64Updated = await sql`
		UPDATE api.validator_consensus_addresses
		SET hex_address = UPPER(encode(decode(consensus_address, 'base64'), 'hex'))
		WHERE hex_address IS NULL
		AND consensus_address ~ '^[A-Za-z0-9+/=]+$'
	`

	console.log(`[bech32-backfill] Inserted ${inserted} bech32 entries, skipped ${skipped}`)
	console.log(`[bech32-backfill] Updated ${base64Updated.count} base64 entries with hex_address`)

	await sql.end()
}

main().catch((err) => {
	console.error("[bech32-backfill] Fatal:", err)
	process.exit(1)
})
