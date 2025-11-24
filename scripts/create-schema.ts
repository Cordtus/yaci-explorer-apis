#!/usr/bin/env npx tsx
import pg from 'pg'
const { Pool } = pg

const DATABASE_URL = process.env.DATABASE_URL || 'postgres://postgres:bOqwmcryOQcdmrO@localhost:15432/postgres?sslmode=disable'

async function main() {
	console.log('Creating schema...')
	const pool = new Pool({ connectionString: DATABASE_URL })

	try {
		await pool.query('CREATE SCHEMA IF NOT EXISTS api')
		console.log('✓ Schema api created')

		await pool.query('CREATE ROLE web_anon NOLOGIN')
		console.log('✓ Role web_anon created')
	} catch (err: any) {
		if (err.code === '42710') {
			console.log('✓ Role already exists')
		} else {
			throw err
		}
	} finally {
		await pool.end()
	}
}

main()
