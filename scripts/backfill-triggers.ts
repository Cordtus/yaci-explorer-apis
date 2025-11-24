/**
 * Backfill script to trigger parsing of existing transactions_raw data
 *
 * This script updates transactions_raw records in batches to fire the triggers
 * that populate transactions_main, messages_main, and events_main tables.
 */

import pg from 'pg'

const DATABASE_URL = process.env.DATABASE_URL
if (!DATABASE_URL) {
  console.error('ERROR: DATABASE_URL environment variable is required')
  process.exit(1)
}

const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '100', 10)

async function backfillBatch(pool: pg.Pool, offset: number): Promise<number> {
  const client = await pool.connect()

  try {
    console.log(`Processing batch starting at offset ${offset}...`)

    const result = await client.query(
      `UPDATE api.transactions_raw
       SET data = data
       WHERE id IN (
         SELECT id FROM api.transactions_raw
         ORDER BY id
         LIMIT $1 OFFSET $2
       )`,
      [BATCH_SIZE, offset]
    )

    const count = result.rowCount || 0
    console.log(`  Updated ${count} transactions`)
    return count
  } finally {
    client.release()
  }
}

async function getTotalCount(pool: pg.Pool): Promise<number> {
  const result = await pool.query('SELECT COUNT(*) as count FROM api.transactions_raw')
  return parseInt(result.rows[0].count, 10)
}

async function getParseCount(pool: pg.Pool): Promise<number> {
  const result = await pool.query('SELECT COUNT(*) as count FROM api.transactions_main')
  return parseInt(result.rows[0].count, 10)
}

async function main() {
  const pool = new pg.Pool({ connectionString: DATABASE_URL })

  try {
    const totalTxs = await getTotalCount(pool)
    const parsedTxs = await getParseCount(pool)
    const remaining = totalTxs - parsedTxs

    console.log(`Total transactions in transactions_raw: ${totalTxs}`)
    console.log(`Already parsed in transactions_main: ${parsedTxs}`)
    console.log(`Remaining to parse: ${remaining}`)
    console.log()

    if (remaining === 0) {
      console.log('All transactions already parsed!')
      return
    }

    let offset = parsedTxs
    let processedTotal = 0

    while (true) {
      const processed = await backfillBatch(pool, offset)

      if (processed === 0) {
        console.log('No more transactions to process')
        break
      }

      processedTotal += processed
      offset += processed

      console.log(`Progress: ${processedTotal}/${remaining} (${Math.round(processedTotal / remaining * 100)}%)`)

      // Small delay to avoid overwhelming the database
      await new Promise(resolve => setTimeout(resolve, 100))
    }

    console.log()
    console.log(`Backfill complete! Processed ${processedTotal} transactions.`)

    const finalParsed = await getParseCount(pool)
    console.log(`Final count in transactions_main: ${finalParsed}`)

  } finally {
    await pool.end()
  }
}

main().catch(err => {
  console.error('Fatal error:', err)
  process.exit(1)
})
