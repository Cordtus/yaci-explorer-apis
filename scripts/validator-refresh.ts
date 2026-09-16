#!/usr/bin/env npx tsx
/**
 * Validator Refresh Daemon (Event-Driven)
 *
 * Listens for pg_notify('validator_refresh', operator_address) events emitted by
 * SQL triggers (staking messages, finalize_block jailing events) and fetches
 * updated validator data from the chain via gRPC.
 *
 * Three concurrent loops:
 * A. NOTIFY listener  -- event-driven, debounced batch fetch of affected validators
 * B. MV refresh timer -- refreshes materialized views on a fixed cadence
 * C. Full sync safety net -- full validator set fetch on startup + periodic interval
 *
 * Environment:
 *   DATABASE_URL           - PostgreSQL connection string
 *   CHAIN_QUERY_URL        - chain-query-service base URL (default: https://yaci-explorer-apis.fly.dev)
 *   DEBOUNCE_MS            - Batch window for NOTIFY events (default: 2000)
 *   MV_REFRESH_INTERVAL_MS - Materialized view refresh interval (default: 900000 = 15 min)
 *   FULL_SYNC_INTERVAL_MS  - Full validator sync interval (default: 21600000 = 6 hours)
 */

import pg from 'pg'
const { Pool } = pg

const DATABASE_URL = process.env.DATABASE_URL || 'postgres://postgres:bOqwmcryOQcdmrO@localhost:15432/postgres?sslmode=disable'
const CHAIN_QUERY_URL = process.env.CHAIN_QUERY_URL || 'https://yaci-explorer-apis.fly.dev'
const DEBOUNCE_MS = parseInt(process.env.DEBOUNCE_MS || '2000', 10)
const MV_REFRESH_INTERVAL_MS = parseInt(process.env.MV_REFRESH_INTERVAL_MS || '900000', 10)
const FULL_SYNC_INTERVAL_MS = parseInt(process.env.FULL_SYNC_INTERVAL_MS || '21600000', 10)

// ============================================================================
// Shared helpers
// ============================================================================

/** Maps gRPC status enum to DB status string */
function mapStatus(status: number | string): string {
	const statusMap: Record<string, string> = {
		'0': 'BOND_STATUS_UNSPECIFIED',
		'1': 'BOND_STATUS_UNBONDED',
		'2': 'BOND_STATUS_UNBONDING',
		'3': 'BOND_STATUS_BONDED',
		'BOND_STATUS_UNSPECIFIED': 'BOND_STATUS_UNSPECIFIED',
		'BOND_STATUS_UNBONDED': 'BOND_STATUS_UNBONDED',
		'BOND_STATUS_UNBONDING': 'BOND_STATUS_UNBONDING',
		'BOND_STATUS_BONDED': 'BOND_STATUS_BONDED',
	}
	return statusMap[String(status)] || 'BOND_STATUS_UNSPECIFIED'
}

/** Converts Cosmos SDK Dec format (10^18) commission rates to decimal */
function parseRate(val: string | undefined): string | null {
	if (!val) return null
	const n = Number(val)
	if (n === 0) return '0'
	if (n > 1) return (n / 1e18).toString()
	return val
}

/** Parses numeric fields, defaulting to null on failure */
function parseNum(val: string | undefined): string | null {
	if (!val || val === '0') return val === '0' ? '0' : null
	return val
}

/** Upserts a single validator into the DB */
async function upsertValidator(client: pg.PoolClient, validator: any): Promise<void> {
	const operatorAddress = validator.operatorAddress || ''
	if (!operatorAddress) return

	const status = mapStatus(validator.status)
	const desc = validator.description || {}
	const rates = validator.commission?.commissionRates || {}

	await client.query(
		`INSERT INTO api.validators (
			operator_address, moniker, identity, website, details,
			commission_rate, commission_max_rate, commission_max_change_rate,
			min_self_delegation, tokens, delegator_shares,
			status, jailed
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (operator_address) DO UPDATE SET
			moniker = COALESCE(EXCLUDED.moniker, api.validators.moniker),
			identity = COALESCE(EXCLUDED.identity, api.validators.identity),
			website = COALESCE(EXCLUDED.website, api.validators.website),
			details = COALESCE(EXCLUDED.details, api.validators.details),
			commission_rate = COALESCE(EXCLUDED.commission_rate, api.validators.commission_rate),
			commission_max_rate = COALESCE(EXCLUDED.commission_max_rate, api.validators.commission_max_rate),
			commission_max_change_rate = COALESCE(EXCLUDED.commission_max_change_rate, api.validators.commission_max_change_rate),
			min_self_delegation = COALESCE(EXCLUDED.min_self_delegation, api.validators.min_self_delegation),
			tokens = EXCLUDED.tokens,
			delegator_shares = EXCLUDED.delegator_shares,
			status = EXCLUDED.status,
			jailed = EXCLUDED.jailed,
			updated_at = NOW()`,
		[
			operatorAddress,
			desc.moniker || null,
			desc.identity || null,
			desc.website || null,
			desc.details || null,
			parseRate(rates.rate),
			parseRate(rates.maxRate),
			parseRate(rates.maxChangeRate),
			parseNum(validator.minSelfDelegation),
			parseNum(validator.tokens),
			parseNum(validator.delegatorShares),
			status,
			validator.jailed || false,
		]
	)
}

// ============================================================================
// HTTP fetch helpers
// ============================================================================

/** Fetches all validators from chain-query-service */
async function fetchValidators(): Promise<any[]> {
	const controller = new AbortController()
	const timeoutId = setTimeout(() => controller.abort(), 30000)

	try {
		const response = await fetch(`${CHAIN_QUERY_URL}/chain/staking/validators`, {
			signal: controller.signal,
		})
		clearTimeout(timeoutId)

		if (!response.ok) {
			throw new Error(`Failed to fetch validators: ${response.status} ${response.statusText}`)
		}

		const data = await response.json()
		return data.validators || []
	} catch (err) {
		clearTimeout(timeoutId)
		throw err
	}
}

/** Fetches a single validator from chain-query-service */
async function fetchValidator(operatorAddress: string): Promise<any | null> {
	const controller = new AbortController()
	const timeoutId = setTimeout(() => controller.abort(), 10000)

	try {
		const response = await fetch(`${CHAIN_QUERY_URL}/chain/staking/validator/${operatorAddress}`, {
			signal: controller.signal,
		})
		clearTimeout(timeoutId)

		if (!response.ok) {
			if (response.status === 500) {
				// gRPC NotFound gets mapped to 500 by the chain-query-service
				console.warn(`[ValidatorRefresh] Validator ${operatorAddress} not found on chain`)
				return null
			}
			throw new Error(`Failed to fetch validator ${operatorAddress}: ${response.status}`)
		}

		const data = await response.json()
		return data.validator || null
	} catch (err) {
		clearTimeout(timeoutId)
		throw err
	}
}

/** Fetches staking pool from chain-query-service */
async function fetchStakingPool(): Promise<{ bondedTokens: string; notBondedTokens: string }> {
	const controller = new AbortController()
	const timeoutId = setTimeout(() => controller.abort(), 10000)

	try {
		const response = await fetch(`${CHAIN_QUERY_URL}/chain/staking/pool`, {
			signal: controller.signal,
		})
		clearTimeout(timeoutId)

		if (!response.ok) {
			throw new Error(`Failed to fetch staking pool: ${response.status} ${response.statusText}`)
		}

		const data = await response.json()
		return data.pool || { bondedTokens: '0', notBondedTokens: '0' }
	} catch (err) {
		clearTimeout(timeoutId)
		throw err
	}
}

// ============================================================================
// A. NOTIFY Listener (event-driven, debounced)
// ============================================================================

let listenerClient: pg.PoolClient | null = null
let reconnectTimeout: NodeJS.Timeout | null = null
let debounceTimeout: NodeJS.Timeout | null = null
const pendingAddresses = new Set<string>()

async function processPendingBatch(pool: pg.Pool): Promise<void> {
	if (pendingAddresses.size === 0) return

	const batch = Array.from(pendingAddresses)
	pendingAddresses.clear()

	console.log(`[ValidatorRefresh] Processing batch of ${batch.length} validators: ${batch.join(', ')}`)

	const client = await pool.connect()
	let updated = 0
	let errors = 0

	try {
		for (const addr of batch) {
			try {
				const validator = await fetchValidator(addr)
				if (validator) {
					await upsertValidator(client, validator)
					updated++
				}
			} catch (err: any) {
				errors++
				console.error(`[ValidatorRefresh] Error refreshing ${addr}: ${err.message}`)
			}
		}
	} finally {
		client.release()
	}

	console.log(`[ValidatorRefresh] Batch done: ${updated} updated, ${errors} errors`)
}

function scheduleBatch(pool: pg.Pool): void {
	if (debounceTimeout) return
	debounceTimeout = setTimeout(async () => {
		debounceTimeout = null
		try {
			await processPendingBatch(pool)
		} catch (err: any) {
			console.error(`[ValidatorRefresh] Batch processing failed: ${err.message}`)
		}
	}, DEBOUNCE_MS)
}

async function startNotifyListener(pool: pg.Pool): Promise<void> {
	console.log(`[ValidatorRefresh] Starting NOTIFY listener (debounce: ${DEBOUNCE_MS}ms)`)

	async function connect() {
		try {
			listenerClient = await pool.connect()
			console.log('[ValidatorRefresh] Listening for validator_refresh notifications')

			await listenerClient.query('LISTEN validator_refresh')

			listenerClient.on('notification', (msg) => {
				if (msg.channel === 'validator_refresh') {
					const addr = msg.payload
					if (!addr) return

					pendingAddresses.add(addr)
					scheduleBatch(pool)
				}
			})

			listenerClient.on('error', (err) => {
				console.error('[ValidatorRefresh] Listener connection error:', err.message)
				scheduleReconnect()
			})

			listenerClient.on('end', () => {
				console.log('[ValidatorRefresh] Listener connection ended')
				scheduleReconnect()
			})

			console.log('[ValidatorRefresh] NOTIFY listener ready')
		} catch (err) {
			console.error('[ValidatorRefresh] Failed to start listener:', err)
			scheduleReconnect()
		}
	}

	function scheduleReconnect() {
		if (reconnectTimeout) return
		if (shuttingDown) return

		listenerClient = null
		console.log('[ValidatorRefresh] Reconnecting listener in 5 seconds...')
		reconnectTimeout = setTimeout(() => {
			reconnectTimeout = null
			connect()
		}, 5000)
	}

	await connect()
}

// ============================================================================
// B. MV Refresh Timer
// ============================================================================

async function refreshMaterializedViews(pool: pg.Pool): Promise<void> {
	const startTime = Date.now()
	console.log(`[MVRefresh] Refreshing analytics views...`)

	// Only refresh MVs that are NOT replaced by trigger-updated tables.
	// After migration 055, the following are real-time and no longer need periodic refresh:
	//   mv_daily_tx_stats -> rt_daily_tx_stats
	//   mv_hourly_tx_stats -> rt_hourly_tx_stats
	//   mv_message_type_stats -> rt_message_type_stats
	//   mv_chain_stats -> rt_chain_stats
	//   mv_hourly_rewards -> rt_hourly_rewards
	//   mv_network_overview -> get_network_overview() reads live tables
	//   mv_daily_rewards -> rt_daily_rewards (migration 065)
	const views = [
		'api.mv_validator_delegator_counts',
		'api.mv_validator_leaderboard',
	]

	const client = await pool.connect()
	try {
		// Advisory lock prevents concurrent MV refresh overlap (e.g. if a previous
		// refresh is still running when the next interval fires)
		await client.query("SELECT pg_advisory_lock(hashtext('mv_refresh'))")

		try {
			for (const view of views) {
				try {
					await client.query(`REFRESH MATERIALIZED VIEW CONCURRENTLY ${view}`)
				} catch (err: any) {
					// View may not exist yet if migration hasn't run
					console.warn(`[MVRefresh] Skipping ${view}: ${err.message}`)
				}
			}

			// Refresh chain stats (latest_block, total_transactions) since triggers were
			// removed in migration 061 to prevent deadlocks under concurrent block inserts
			try {
				await client.query(`SELECT api.refresh_rt_chain_stats()`)
			} catch (err: any) {
				console.warn(`[MVRefresh] Skipping refresh_rt_chain_stats: ${err.message}`)
			}

			// Reconcile rt_chain_stats.unique_addresses from the expensive DISTINCT query
			// This is too costly for triggers but fine at 15-minute intervals
			try {
				await client.query(
					`UPDATE api.rt_chain_stats SET unique_addresses = (
						SELECT COUNT(*) FROM (
							SELECT DISTINCT sender AS addr FROM api.messages_main WHERE sender IS NOT NULL
							UNION
							SELECT DISTINCT "from" AS addr FROM api.evm_transactions
							UNION
							SELECT DISTINCT "to" AS addr FROM api.evm_transactions WHERE "to" IS NOT NULL
							UNION
							SELECT DISTINCT unnest(mentions) AS addr FROM api.messages_main WHERE mentions IS NOT NULL
						) all_addresses
					), updated_at = NOW()`
				)
			} catch (err: any) {
				// Table may not exist yet if migration 055 hasn't run
				console.warn(`[MVRefresh] Skipping unique_addresses reconciliation: ${err.message}`)
			}
		} finally {
			await client.query("SELECT pg_advisory_unlock(hashtext('mv_refresh'))")
		}
	} finally {
		client.release()
	}

	const elapsed = Date.now() - startTime
	console.log(`[MVRefresh] Done in ${elapsed}ms (${views.length} views + reconciliation)`)
}

async function startMVRefreshLoop(pool: pg.Pool): Promise<void> {
	console.log(`[MVRefresh] Materialized view refresh every ${MV_REFRESH_INTERVAL_MS / 60000} min`)

	// Initial refresh
	try {
		await refreshMaterializedViews(pool)
	} catch (err: any) {
		console.error(`[MVRefresh] Initial refresh failed: ${err.message}`)
	}

	// Periodic refresh
	while (!shuttingDown) {
		await new Promise(resolve => setTimeout(resolve, MV_REFRESH_INTERVAL_MS))
		if (shuttingDown) break

		try {
			await refreshMaterializedViews(pool)
		} catch (err: any) {
			console.error(`[MVRefresh] Refresh failed: ${err.message}`)
		}
	}
}

// ============================================================================
// C. Full Sync Safety Net
// ============================================================================

async function refreshValidators(pool: pg.Pool): Promise<void> {
	const startTime = Date.now()
	console.log(`[ValidatorRefresh] Full sync starting...`)

	const [validators, pool_info] = await Promise.all([
		fetchValidators(),
		fetchStakingPool().catch(err => {
			console.warn(`[ValidatorRefresh] Staking pool fetch failed (non-fatal): ${err.message}`)
			return { bondedTokens: '0', notBondedTokens: '0' }
		}),
	])

	console.log(`[ValidatorRefresh] Fetched ${validators.length} validators from chain`)
	console.log(`[ValidatorRefresh] Pool: bonded=${pool_info.bondedTokens}, not_bonded=${pool_info.notBondedTokens}`)

	const client = await pool.connect()
	let updated = 0
	let errors = 0

	try {
		await client.query('BEGIN')

		// Build set of operator addresses from chain response
		const chainAddresses = new Set(validators.map((v: any) => v.operatorAddress || '').filter(Boolean))

		for (const validator of validators) {
			try {
				await upsertValidator(client, validator)
				updated++
			} catch (err: any) {
				errors++
				console.error(`[ValidatorRefresh] Error upserting ${validator.operatorAddress}: ${err.message}`)
			}
		}

		// Mark validators in DB that are no longer in chain response as UNBONDED.
		// Do NOT set jailed=true here -- being absent from the active set does not
		// mean jailed. Jailing is detected separately by block-signature analysis
		// and finalize_block events.
		if (chainAddresses.size === 0) {
			console.warn(`[ValidatorRefresh] Chain returned 0 validators -- skipping ghost check to avoid mass-marking`)
		} else {
			const { rowCount: ghostCount } = await client.query(
				`UPDATE api.validators
				 SET status = 'BOND_STATUS_UNBONDED', updated_at = NOW()
				 WHERE status = 'BOND_STATUS_BONDED'
				   AND operator_address NOT IN (SELECT unnest($1::text[]))`,
				[Array.from(chainAddresses)]
			)
			if (ghostCount && ghostCount > 0) {
				console.warn(`[ValidatorRefresh] Marked ${ghostCount} validators as unbonded (absent from chain response)`)
			}
		}

		// Fill in NULL consensus_address from the mapping table (populated by block
		// signature extraction) so jailing detection can resolve addresses correctly
		await client.query(
			`UPDATE api.validators v
			 SET consensus_address = vca.consensus_address, updated_at = NOW()
			 FROM api.validator_consensus_addresses vca
			 WHERE vca.operator_address = v.operator_address
			   AND (v.consensus_address IS NULL OR v.consensus_address = '')`
		)

		await client.query('COMMIT')
	} catch (err) {
		await client.query('ROLLBACK')
		throw err
	} finally {
		client.release()
	}

	const elapsed = Date.now() - startTime
	console.log(`[ValidatorRefresh] Full sync done in ${elapsed}ms: ${updated} updated, ${errors} errors`)
}

async function startFullSyncLoop(pool: pg.Pool): Promise<void> {
	console.log(`[ValidatorRefresh] Full sync every ${FULL_SYNC_INTERVAL_MS / 3600000}h`)

	// Initial full sync on startup
	try {
		await refreshValidators(pool)
	} catch (err: any) {
		console.error(`[ValidatorRefresh] Initial full sync failed: ${err.message}`)
	}

	// Periodic full sync
	while (!shuttingDown) {
		await new Promise(resolve => setTimeout(resolve, FULL_SYNC_INTERVAL_MS))
		if (shuttingDown) break

		try {
			await refreshValidators(pool)
		} catch (err: any) {
			console.error(`[ValidatorRefresh] Full sync failed: ${err.message}`)
		}
	}
}

// ============================================================================
// Main + shutdown
// ============================================================================

let shuttingDown = false

async function main() {
	const pool = new Pool({ connectionString: DATABASE_URL })

	console.log(`[ValidatorRefresh] Starting event-driven validator refresh service`)
	console.log(`[ValidatorRefresh] Chain query URL: ${CHAIN_QUERY_URL}`)
	console.log(`[ValidatorRefresh] Debounce: ${DEBOUNCE_MS}ms`)

	// Start all three loops concurrently
	await Promise.all([
		startNotifyListener(pool),
		startFullSyncLoop(pool),
		startMVRefreshLoop(pool),
	])
}

async function shutdown() {
	if (shuttingDown) return
	shuttingDown = true
	console.log('\n[ValidatorRefresh] Shutting down...')

	if (debounceTimeout) {
		clearTimeout(debounceTimeout)
	}
	if (reconnectTimeout) {
		clearTimeout(reconnectTimeout)
	}
	if (listenerClient) {
		listenerClient.release()
	}
	process.exit(0)
}

process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)

main().catch(err => {
	console.error('[ValidatorRefresh] Fatal error:', err)
	process.exit(1)
})
