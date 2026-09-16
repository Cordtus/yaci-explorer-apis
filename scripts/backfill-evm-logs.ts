#!/usr/bin/env npx tsx
/**
 * Backfill EVM logs from existing transactions
 * Re-processes transactions_raw to extract logs that were missed
 */

import pg from 'pg'
import protobuf from 'protobufjs'
import { join, dirname } from 'path'
import { fileURLToPath } from 'url'

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)

const DATABASE_URL = process.env.DATABASE_URL || 'postgres://postgres:foobar@localhost:5432/yaci'
const BATCH_SIZE = 100

interface DecodedLog {
	tx_id: string
	log_index: number
	address: string
	topics: string[]
	data: string
}

async function decodeTxResponse(
	hexData: string,
	root: protobuf.Root
): Promise<{ logs: DecodedLog[]; gasUsed: number; vmError: string | null } | null> {
	try {
		const hex = hexData.startsWith('0x') ? hexData.slice(2) : hexData
		const bytes = Buffer.from(hex, 'hex')

		const TxMsgData = root.lookupType('cosmos.evm.vm.v1.TxMsgData')
		const txMsgData = TxMsgData.decode(bytes) as any

		if (!txMsgData.msgResponses || txMsgData.msgResponses.length === 0) {
			return null
		}

		const msgResponse = txMsgData.msgResponses[0]
		if (!msgResponse.typeUrl?.includes('MsgEthereumTxResponse')) {
			return null
		}

		const MsgEthereumTxResponse = root.lookupType('cosmos.evm.vm.v1.MsgEthereumTxResponse')
		const response = MsgEthereumTxResponse.decode(msgResponse.value) as any

		const logs: DecodedLog[] = (response.logs || []).map((log: any, index: number) => ({
			tx_id: '',
			log_index: log.index?.toNumber?.() ?? index,
			address: log.address?.toLowerCase() || '',
			topics: log.topics || [],
			data: log.data ? '0x' + Buffer.from(log.data).toString('hex') : '0x',
		}))

		return {
			logs,
			gasUsed: response.gasUsed?.toNumber?.() ?? 0,
			vmError: response.vmError || null,
		}
	} catch (err) {
		return null
	}
}

async function backfillLogs(pool: pg.Pool, root: protobuf.Root): Promise<{ logsInserted: number; tokensInserted: number; transfersInserted: number }> {
	console.log('\n=== Backfilling EVM Logs ===')

	const TRANSFER_SIG = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'

	let logsInserted = 0
	let tokensInserted = 0
	let transfersInserted = 0
	let offset = 0

	while (true) {
		// Get EVM transactions that don't have logs yet
		const result = await pool.query(`
			SELECT e.tx_id, e."to", t.height, tr.data->'txResponse'->>'data' as response_data
			FROM api.evm_transactions e
			JOIN api.transactions_main t ON e.tx_id = t.id
			JOIN api.transactions_raw tr ON e.tx_id = tr.id
			WHERE NOT EXISTS (SELECT 1 FROM api.evm_logs l WHERE l.tx_id = e.tx_id)
			  AND tr.data->'txResponse'->>'data' IS NOT NULL
			ORDER BY t.height
			LIMIT $1 OFFSET $2
		`, [BATCH_SIZE, offset])

		if (result.rows.length === 0) {
			break
		}

		console.log(`Processing batch at offset ${offset}, ${result.rows.length} transactions...`)

		for (const row of result.rows) {
			if (!row.response_data) continue

			const decoded = await decodeTxResponse(row.response_data, root)
			if (!decoded || decoded.logs.length === 0) continue

			const client = await pool.connect()
			try {
				await client.query('BEGIN')

				for (const log of decoded.logs) {
					// Insert log
					const logResult = await client.query(
						`INSERT INTO api.evm_logs (tx_id, log_index, address, topics, data)
						 VALUES ($1, $2, $3, $4, $5)
						 ON CONFLICT (tx_id, log_index) DO NOTHING
						 RETURNING tx_id`,
						[row.tx_id, log.log_index, log.address, log.topics, log.data]
					)

					if (logResult.rows.length > 0) {
						logsInserted++
					}

					// Check for Transfer event
					if (log.topics[0] === TRANSFER_SIG && log.topics.length >= 3) {
						const fromAddr = '0x' + log.topics[1].slice(-40)
						const toAddr = '0x' + log.topics[2].slice(-40)
						const value = log.data || '0x0'

						// Insert token
						const tokenResult = await client.query(
							`INSERT INTO api.evm_tokens (address, type, first_seen_tx, first_seen_height)
							 VALUES ($1, 'ERC20', $2, $3)
							 ON CONFLICT (address) DO NOTHING
							 RETURNING address`,
							[log.address, row.tx_id, row.height]
						)

						if (tokenResult.rows.length > 0) {
							tokensInserted++
						}

						// Insert transfer
						const transferResult = await client.query(
							`INSERT INTO api.evm_token_transfers (tx_id, log_index, token_address, from_address, to_address, value)
							 VALUES ($1, $2, $3, $4, $5, $6)
							 ON CONFLICT (tx_id, log_index) DO NOTHING
							 RETURNING tx_id`,
							[row.tx_id, log.log_index, log.address, fromAddr.toLowerCase(), toAddr.toLowerCase(), value]
						)

						if (transferResult.rows.length > 0) {
							transfersInserted++
						}
					}
				}

				await client.query('COMMIT')
			} catch (err) {
				await client.query('ROLLBACK')
				console.error(`Error processing ${row.tx_id}:`, err)
			} finally {
				client.release()
			}
		}

		offset += result.rows.length
		console.log(`  Logs: ${logsInserted}, Tokens: ${tokensInserted}, Transfers: ${transfersInserted}`)
	}

	return { logsInserted, tokensInserted, transfersInserted }
}

async function main() {
	console.log('EVM Logs Backfill')
	console.log('=================')
	console.log(`Database: ${DATABASE_URL.replace(/:[^:@]+@/, ':***@')}`)

	const pool = new pg.Pool({ connectionString: DATABASE_URL })

	const protoPath = join(__dirname, '..', 'proto', 'evm.proto')
	const root = await protobuf.load(protoPath)

	try {
		const { logsInserted, tokensInserted, transfersInserted } = await backfillLogs(pool, root)

		console.log('\n=== Summary ===')
		console.log(`Logs backfilled: ${logsInserted}`)
		console.log(`Tokens discovered: ${tokensInserted}`)
		console.log(`Transfers backfilled: ${transfersInserted}`)
	} finally {
		await pool.end()
	}
}

main().catch(err => {
	console.error('Fatal error:', err)
	process.exit(1)
})
