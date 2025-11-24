/**
 * YACI Explorer API Client
 * Provides typed access to PostgREST RPC endpoints
 */

import type {
	PaginatedResponse,
	Transaction,
	TransactionDetail,
	AddressStats,
	ChainStats,
	SearchResult
} from './types'

export interface YaciClientConfig {
	baseUrl: string
}

/**
 * Main API client for YACI Explorer
 * No internal caching - relies on TanStack Query for cache management
 */
export class YaciClient {
	private baseUrl: string

	constructor(config: YaciClientConfig) {
		this.baseUrl = config.baseUrl.replace(/\/$/, '')
	}

	/**
	 * Call a PostgREST RPC function
	 */
	private async rpc<T>(fn: string, params?: Record<string, unknown>): Promise<T> {
		const url = new URL(`${this.baseUrl}/rpc/${fn}`)
		if (params) {
			Object.entries(params).forEach(([key, value]) => {
				if (value !== undefined && value !== null) {
					url.searchParams.set(key, String(value))
				}
			})
		}

		const res = await fetch(url.toString(), {
			headers: { 'Accept': 'application/json' }
		})

		if (!res.ok) {
			throw new Error(`RPC ${fn} failed: ${res.status} ${res.statusText}`)
		}

		return res.json()
	}

	/**
	 * Query a PostgREST table directly
	 */
	private async query<T>(table: string, params?: Record<string, string>): Promise<T> {
		const url = new URL(`${this.baseUrl}/${table}`)
		if (params) {
			Object.entries(params).forEach(([key, value]) => {
				url.searchParams.set(key, value)
			})
		}

		const res = await fetch(url.toString(), {
			headers: { 'Accept': 'application/json' }
		})

		if (!res.ok) {
			throw new Error(`Query ${table} failed: ${res.status} ${res.statusText}`)
		}

		return res.json()
	}

	// Address endpoints

	/**
	 * Get paginated transactions for an address
	 */
	async getTransactionsByAddress(
		address: string,
		limit = 50,
		offset = 0
	): Promise<PaginatedResponse<Transaction>> {
		return this.rpc('get_transactions_by_address', {
			_address: address,
			_limit: limit,
			_offset: offset
		})
	}

	/**
	 * Get address statistics
	 */
	async getAddressStats(address: string): Promise<AddressStats> {
		return this.rpc('get_address_stats', { _address: address })
	}

	// Transaction endpoints

	/**
	 * Get full transaction detail including messages, events, and EVM data
	 */
	async getTransaction(hash: string): Promise<TransactionDetail> {
		return this.rpc('get_transaction_detail', { _hash: hash })
	}

	/**
	 * Get paginated transactions with optional filters
	 */
	async getTransactions(
		limit = 20,
		offset = 0,
		filters?: {
			status?: 'success' | 'failed'
			blockHeight?: number
			messageType?: string
		}
	): Promise<PaginatedResponse<Transaction>> {
		return this.rpc('get_transactions_paginated', {
			_limit: limit,
			_offset: offset,
			_status: filters?.status,
			_block_height: filters?.blockHeight,
			_message_type: filters?.messageType
		})
	}

	// Block endpoints

	/**
	 * Get block by height
	 */
	async getBlock(height: number): Promise<unknown> {
		const result = await this.query('blocks_raw', {
			id: `eq.${height}`,
			limit: '1'
		})
		return Array.isArray(result) ? result[0] : result
	}

	/**
	 * Get recent blocks
	 */
	async getBlocks(limit = 20, offset = 0): Promise<unknown[]> {
		return this.query('blocks_raw', {
			order: 'id.desc',
			limit: String(limit),
			offset: String(offset)
		})
	}

	// Search endpoint

	/**
	 * Universal search across blocks, transactions, addresses
	 */
	async search(query: string): Promise<SearchResult[]> {
		return this.rpc('universal_search', { _query: query })
	}

	// Analytics endpoints

	/**
	 * Get chain statistics
	 */
	async getChainStats(): Promise<ChainStats> {
		const result = await this.query<ChainStats[]>('chain_stats')
		return result[0]
	}

	/**
	 * Get daily transaction volume
	 */
	async getTxVolumeDaily(): Promise<Array<{ date: string; count: number }>> {
		return this.query('tx_volume_daily', { order: 'date.desc' })
	}

	/**
	 * Get message type statistics
	 */
	async getMessageTypeStats(): Promise<Array<{ type: string; count: number }>> {
		return this.query('message_type_stats')
	}

	/**
	 * Get transaction success rate
	 */
	async getTxSuccessRate(): Promise<{
		total: number
		successful: number
		failed: number
		success_rate_percent: number
	}> {
		const result = await this.query<Array<{
			total: number
			successful: number
			failed: number
			success_rate_percent: number
		}>>('tx_success_rate')
		return result[0]
	}
}

/**
 * Create a new YaciClient instance
 */
export function createClient(baseUrl: string): YaciClient {
	return new YaciClient({ baseUrl })
}
