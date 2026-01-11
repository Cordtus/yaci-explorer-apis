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
	fee: TransactionFee | null
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
	evm_logs: EvmLog[]
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
	data?: Record<string, unknown>
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
	hash: string
	from: string
	to: string | null
	nonce: number
	gasLimit: string
	gasPrice: string
	maxFeePerGas: string | null
	maxPriorityFeePerGas: string | null
	value: string
	data: string | null
	type: number
	chainId: string | null
	gasUsed: number | null
	status: number
	functionName: string | null
	functionSignature: string | null
}

export interface EvmLog {
	logIndex: number
	address: string
	topics: string[]
	data: string
}

// Address

export interface AddressStats {
	address: string
	transaction_count: number
	first_seen: string | null
	last_seen: string | null
}

// Chain Stats

export interface ChainStats {
	latest_block: number
	total_transactions: number
	unique_addresses: number
	evm_transactions: number
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

// Governance

export interface GovernanceProposal {
	proposal_id: number
	title: string | null
	summary: string | null
	status: string
	submit_time: string
	deposit_end_time: string | null
	voting_start_time: string | null
	voting_end_time: string | null
	proposer: string | null
	tally: {
		yes: string | null
		no: string | null
		abstain: string | null
		no_with_veto: string | null
	}
	last_updated: string
	last_snapshot_time: string | null
}

export interface ProposalSnapshot {
	proposal_id: number
	status: string
	yes_count: string
	no_count: string
	abstain_count: string
	no_with_veto_count: string
	snapshot_time: string
}

// Token types

export interface EvmToken {
	address: string
	name: string | null
	symbol: string | null
	decimals: number | null
	type: 'ERC20' | 'ERC721' | 'ERC1155'
	total_supply: string | null
	verified: boolean
}

export interface EvmTokenTransfer {
	tx_id: string
	log_index: number
	token_address: string
	from_address: string
	to_address: string
	value: string
}

// IBC Types

export interface IbcStats {
	outgoing_transfers: number
	incoming_transfers: number
	completed_transfers: number
	timed_out_transfers: number
	relayer_updates: number
	total_channels: number
	open_channels: number
	active_channels: number
	connected_chains: number
	total_denoms: number
}

export interface IbcTransfer {
	tx_hash: string
	height: number
	timestamp: string
	direction: 'outgoing' | 'incoming' | 'other'
	sender: string | null
	receiver: string | null
	source_channel: string | null
	token_denom: string | null
	token_amount: string | null
	resolved_denom: ResolvedDenom | null
	counterparty_chain: string | null
	success: boolean
}

export interface ResolvedDenom {
	denom: string
	symbol: string
	decimals: number
	is_native: boolean | null
	source_chain: string | null
	source_denom: string | null
}

export interface IbcConnection {
	channel_id: string
	port_id: string
	connection_id: string | null
	client_id: string | null
	counterparty_chain_id: string | null
	counterparty_channel_id: string | null
	counterparty_port_id: string | null
	counterparty_client_id: string | null
	counterparty_connection_id: string | null
	state: string | null
	ordering: string | null
	client_status: string | null
	is_active: boolean
	updated_at: string
}

export interface IbcDenomTrace {
	ibc_denom: string
	base_denom: string
	path: string
	source_channel: string | null
	source_chain_id: string | null
	symbol: string | null
	decimals: number
	updated_at: string
}

export interface IbcDenomResolution {
	ibc_denom: string
	base_denom: string
	path: string
	source_channel: string | null
	source_chain_id: string | null
	symbol: string | null
	decimals: number
	route: {
		channel_id: string | null
		connection_id: string | null
		client_id: string | null
		counterparty_channel_id: string | null
		counterparty_connection_id: string | null
		counterparty_client_id: string | null
	}
}

export interface IbcChainSummary {
	chain_id: string
	channel_count: number
	open_channels: number
	active_channels: number
}

export interface IbcChannelActivity {
	channel_id: string
	transfer_count: number
	successful_transfers: number
	counterparty_chain_id: string | null
	state: string | null
	client_status: string | null
}

export interface IbcVolumeTimeSeries {
	hours: number
	channel_filter: string | null
	data: Array<{
		hour: string
		outgoing_count: number
		incoming_count: number
		outgoing_volume: number
		incoming_volume: number
	}>
	channels: string[]
}
