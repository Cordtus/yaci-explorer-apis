/**
 * Type definitions for YACI Explorer API
 */

// Pagination

export interface Pagination {
	total: number
	limit: number
	offset: number
	has_next: boolean
	has_prev: boolean
}

export interface PaginatedResponse<T> {
	data: T[]
	pagination: Pagination
}

// Transactions

export interface Transaction {
	id: string
	fee: TransactionFee
	memo: string | null
	error: string | null
	height: number
	timestamp: string
	proposal_ids: number[] | null
	messages: Message[]
	events: Event[]
	ingest_error: IngestError | null
}

export interface TransactionDetail extends Transaction {
	evm_data: EvmData | null
	raw_data: unknown
}

export interface TransactionFee {
	amount: Array<{ denom: string; amount: string }>
	gasLimit: string
}

export interface IngestError {
	message: string
	reason: string
	hash: string
}

// Messages

export interface Message {
	id: string
	message_index: number
	type: string
	sender: string | null
	mentions: string[]
	metadata: Record<string, unknown>
}

// Events

export interface Event {
	id: string
	event_index: number
	attr_index: number
	event_type: string
	attr_key: string
	attr_value: string
	msg_index: number | null
}

// EVM

export interface EvmData {
	ethereum_tx_hash: string | null
	recipient: string | null
	gas_used: number | null
	tx_type: number | null
}

// Address

export interface AddressStats {
	address: string
	transaction_count: number
	first_seen: string | null
	last_seen: string | null
	total_sent: number
	total_received: number
}

// Chain Stats

export interface ChainStats {
	latest_block: number
	total_transactions: number
	unique_addresses: number
	avg_block_time: number
	min_block_time: number
	max_block_time: number
	active_validators: number
}

// Search

export interface SearchResult {
	type: 'block' | 'transaction' | 'evm_transaction' | 'address' | 'evm_address'
	value: unknown
	score: number
}

// Blocks

export interface BlockRaw {
	id: number
	data: {
		block: {
			header: {
				height: string
				time: string
				chain_id: string
				proposer_address: string
			}
			data: {
				txs: string[]
			}
			last_commit?: {
				signatures: Array<{
					validator_address: string
					signature: string
				}>
			}
		}
	}
}
