#!/usr/bin/env npx tsx
import pg from 'pg'
import { writeFileSync } from 'fs'
const { Pool } = pg

const DATABASE_URL = process.env.DATABASE_URL || 'postgres://postgres:bOqwmcryOQcdmrO@localhost:15432/postgres?sslmode=disable'

async function main() {
	const pool = new Pool({ connectionString: DATABASE_URL })
	let output = '-- Current schema export\n\n'

	try {
		// Get table definitions
		const tables = await pool.query(`
			SELECT tablename FROM pg_tables
			WHERE schemaname = 'api'
			ORDER BY tablename
		`)

		for (const row of tables.rows) {
			const def = await pool.query(`
				SELECT pg_get_tabledef('api', $1)
			`, [row.tablename])
			output += `-- Table: api.${row.tablename}\n`
			output += `-- Exists\n\n`
		}

		// Get view definitions  
		const views = await pool.query(`
			SELECT viewname, definition FROM pg_views
			WHERE schemaname = 'api'
			ORDER BY viewname
		`)

		for (const row of views.rows) {
			output += `-- View: api.${row.viewname}\n`
			output += `CREATE OR REPLACE VIEW api.${row.viewname} AS\n${row.definition}\n\n`
		}

		writeFileSync('migrations/current_schema.sql', output)
		console.log('✓ Schema exported to migrations/current_schema.sql')

	} finally {
		await pool.end()
	}
}

main()
