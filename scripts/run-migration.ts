#!/usr/bin/env npx tsx
import pg from 'pg'
import { readFileSync } from 'fs'
import { join, dirname } from 'path'
import { fileURLToPath } from 'url'

const { Pool } = pg

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)

const DATABASE_URL = process.env.DATABASE_URL ||  'postgres://postgres:bOqwmcryOQcdmrO@localhost:15432/postgres?sslmode=disable'

async function main() {
	console.log('Running migration...')
	console.log(`Database: ${DATABASE_URL.replace(/:[^:@]+@/, ':***@')}`)

	const pool = new Pool({ connectionString: DATABASE_URL })

	try {
		await pool.query('SELECT 1')
		console.log('✓ Connected to database')

		const migrationPath = join(__dirname, '..', 'migrations', '001_complete_schema.sql')
		const sql = readFileSync(migrationPath, 'utf-8')

		console.log('✓ Loaded migration file')
		console.log('  Running SQL commands...')

		await pool.query(sql)

		console.log('✓ Migration complete!')
	} catch (err) {
		console.error('✗ Migration failed:', err)
		process.exit(1)
	} finally {
		await pool.end()
	}
}

main()
