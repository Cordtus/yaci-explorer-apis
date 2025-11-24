#!/usr/bin/env npx tsx
import pg from 'pg'
const { Pool } = pg

const DATABASE_URL = process.env.DATABASE_URL || 'postgres://postgres:bOqwmcryOQcdmrO@localhost:15432/postgres?sslmode=disable'

async function main() {
	console.log('🗑️  Resetting database...')
	console.log(`Database: ${DATABASE_URL.replace(/:[^:@]+@/, ':***@')}`)

	const pool = new Pool({ connectionString: DATABASE_URL })

	try {
		await pool.query('SELECT 1')
		console.log('✓ Connected')

		console.log('Dropping api schema...')
		await pool.query('DROP SCHEMA IF EXISTS api CASCADE')
		console.log('✓ Schema dropped')

		console.log('✓ Database reset complete!')
		console.log('  Ready for fresh migration')

	} catch (err) {
		console.error('✗ Reset failed:', err)
		process.exit(1)
	} finally {
		await pool.end()
	}
}

main()
