#!/usr/bin/env npx tsx
import pg from 'pg'
const { Pool } = pg

const DATABASE_URL = process.env.DATABASE_URL || 'postgres://postgres:bOqwmcryOQcdmrO@localhost:15432/postgres?sslmode=disable'

async function main() {
	const pool = new Pool({ connectionString: DATABASE_URL })

	try {
		console.log('Checking schema...\n')

		// Check tables
		const tables = await pool.query(`
			SELECT tablename FROM pg_tables
			WHERE schemaname = 'api'
			ORDER BY tablename
		`)
		console.log('Tables:')
		tables.rows.forEach(r => console.log(`  - ${r.tablename}`))

		// Check views
		const views = await pool.query(`
			SELECT viewname FROM pg_views
			WHERE schemaname = 'api'
			ORDER BY viewname
		`)
		console.log('\nViews:')
		views.rows.forEach(r => console.log(`  - ${r.viewname}`))

		// Check functions
		const funcs = await pool.query(`
			SELECT routine_name FROM information_schema.routines
			WHERE routine_schema = 'api'
			ORDER BY routine_name
		`)
		console.log('\nFunctions:')
		funcs.rows.forEach(r => console.log(`  - ${r.routine_name}`))

	} finally {
		await pool.end()
	}
}

main()
