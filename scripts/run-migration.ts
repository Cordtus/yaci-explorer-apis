#!/usr/bin/env npx tsx
/**
 * SQL Migration Runner
 * Runs all migrations in order, tracking applied migrations in a schema_migrations table.
 */
import pg from 'pg'
import { readFileSync, readdirSync } from 'fs'
import { join, dirname } from 'path'
import { fileURLToPath } from 'url'

const { Pool } = pg

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)

const DATABASE_URL = process.env.DATABASE_URL || 'postgres://postgres:bOqwmcryOQcdmrO@localhost:15432/postgres?sslmode=disable'

async function main() {
	console.log('Running migrations...')
	console.log(`Database: ${DATABASE_URL.replace(/:[^:@]+@/, ':***@')}`)

	const pool = new Pool({ connectionString: DATABASE_URL })

	try {
		await pool.query('SELECT 1')
		console.log('✓ Connected to database')

		// Create migrations tracking table if it doesn't exist
		await pool.query(`
			CREATE TABLE IF NOT EXISTS schema_migrations (
				filename TEXT PRIMARY KEY,
				applied_at TIMESTAMPTZ DEFAULT NOW()
			)
		`)

		// Get list of already applied migrations
		const { rows: applied } = await pool.query('SELECT filename FROM schema_migrations')
		const appliedSet = new Set(applied.map(r => r.filename))

		// Get all migration files sorted by name
		const migrationsDir = join(__dirname, '..', 'migrations')
		const files = readdirSync(migrationsDir)
			.filter(f => f.endsWith('.sql'))
			.sort()

		console.log(`Found ${files.length} migration files, ${appliedSet.size} already applied`)

		let appliedCount = 0
		for (const file of files) {
			if (appliedSet.has(file)) {
				continue
			}

			console.log(`  Applying: ${file}`)
			const sql = readFileSync(join(migrationsDir, file), 'utf-8')

			try {
				await pool.query(sql)
				await pool.query('INSERT INTO schema_migrations (filename) VALUES ($1)', [file])
				appliedCount++
				console.log(`  ✓ ${file}`)
			} catch (err: any) {
				console.error(`  ✗ ${file} failed:`, err.message)
				throw err
			}
		}

		if (appliedCount === 0) {
			console.log('✓ All migrations already applied')
		} else {
			console.log(`✓ Applied ${appliedCount} new migration(s)`)
		}
	} catch (err) {
		console.error('✗ Migration failed:', err)
		process.exit(1)
	} finally {
		await pool.end()
	}
}

main()
