#!/usr/bin/env npx tsx
/**
 * Chain Query Service (Multi-Chain)
 *
 * HTTP proxy for chain queries via gRPC. Supports multiple chains via chains.toml.
 * Each chain gets a lazily-initialized gRPC client.
 *
 * URL patterns:
 *   /chain/:chainId/<endpoint>   -- query a specific chain
 *   /chain/<endpoint>            -- query the default chain (backward compat)
 *   /chain/health                -- all chains health
 *   /chain/:chainId/health       -- single chain health
 *
 * Endpoints per chain:
 * - GET  balances/:address
 * - GET  spendable/:address
 * - GET  supply/:denom
 * - GET  staking/validators?status=...
 * - GET  staking/validator/:address
 * - GET  staking/pool
 * - GET  auth/account/:address
 * - GET  slashing/params
 * - GET  slashing/signing_info/:cons_address
 * - GET  slashing/signing_infos
 * - GET  ibc/channel/:channelId/:portId
 * - GET  ibc/channel/:channelId/:portId/client_state
 * - GET  ibc/denom_trace/:hash
 * - POST tx/broadcast
 */

import * as http from 'http'
import * as grpc from '@grpc/grpc-js'
import protobuf from 'protobufjs'
import { loadChainsConfig, getChainConfig, getChainIds, getDefaultChainId } from './lib/chain-config.js'

const PORT = parseInt(process.env.CHAIN_QUERY_PORT || '3001', 10)

// Proto definitions for chain queries
const PROTOS: Record<string, string> = {
	auth: `
		syntax = "proto3";
		package cosmos.auth.v1beta1;

		service Query {
			rpc Account(QueryAccountRequest) returns (QueryAccountResponse);
		}

		message QueryAccountRequest {
			string address = 1;
		}

		message QueryAccountResponse {
			Any account = 1;
		}

		message Any {
			string type_url = 1;
			bytes value = 2;
		}

		// BaseAccount for decoding
		message BaseAccount {
			string address = 1;
			Any pub_key = 2;
			uint64 account_number = 3;
			uint64 sequence = 4;
		}

		// EthAccount wraps BaseAccount (used by EVM chains like Republic)
		message EthAccount {
			BaseAccount base_account = 1;
			bytes code_hash = 2;
		}
	`,
	tx: `
		syntax = "proto3";
		package cosmos.tx.v1beta1;

		service Service {
			rpc BroadcastTx(BroadcastTxRequest) returns (BroadcastTxResponse);
		}

		message BroadcastTxRequest {
			bytes tx_bytes = 1;
			int32 mode = 2;
		}

		message BroadcastTxResponse {
			TxResponse tx_response = 1;
		}

		message TxResponse {
			int64 height = 1;
			string txhash = 2;
			string codespace = 3;
			uint32 code = 4;
			string data = 5;
			string raw_log = 6;
			repeated AbciMessageLog logs = 7;
			string info = 8;
			int64 gas_wanted = 9;
			int64 gas_used = 10;
			Any tx = 11;
			string timestamp = 12;
			repeated Event events = 13;
		}

		message AbciMessageLog {
			uint32 msg_index = 1;
			string log = 2;
			repeated StringEvent events = 3;
		}

		message StringEvent {
			string type = 1;
			repeated Attribute attributes = 2;
		}

		message Attribute {
			string key = 1;
			string value = 2;
		}

		message Event {
			string type = 1;
			repeated EventAttribute attributes = 2;
		}

		message EventAttribute {
			bytes key = 1;
			bytes value = 2;
			bool index = 3;
		}

		message Any {
			string type_url = 1;
			bytes value = 2;
		}

		// Transaction encoding types
		message TxRaw {
			bytes body_bytes = 1;
			bytes auth_info_bytes = 2;
			repeated bytes signatures = 3;
		}

		message TxBody {
			repeated Any messages = 1;
			string memo = 2;
			uint64 timeout_height = 3;
			repeated Any extension_options = 1023;
			repeated Any non_critical_extension_options = 2047;
		}

		message AuthInfo {
			repeated SignerInfo signer_infos = 1;
			Fee fee = 2;
		}

		message SignerInfo {
			Any public_key = 1;
			ModeInfo mode_info = 2;
			uint64 sequence = 3;
		}

		message ModeInfo {
			oneof sum {
				Single single = 1;
				Multi multi = 2;
			}
			message Single {
				int32 mode = 1;
			}
			message Multi {
				CompactBitArray bitarray = 1;
				repeated ModeInfo mode_infos = 2;
			}
		}

		message CompactBitArray {
			uint32 extra_bits_stored = 1;
			bytes elems = 2;
		}

		message Fee {
			repeated Coin amount = 1;
			uint64 gas_limit = 2;
			string payer = 3;
			string granter = 4;
		}

		message Coin {
			string denom = 1;
			string amount = 2;
		}
	`,
	bank: `
		syntax = "proto3";
		package cosmos.bank.v1beta1;

		service Query {
			rpc AllBalances(QueryAllBalancesRequest) returns (QueryAllBalancesResponse);
			rpc SpendableBalances(QuerySpendableBalancesRequest) returns (QuerySpendableBalancesResponse);
			rpc TotalSupply(QueryTotalSupplyRequest) returns (QueryTotalSupplyResponse);
			rpc SupplyOf(QuerySupplyOfRequest) returns (QuerySupplyOfResponse);
		}

		message QueryAllBalancesRequest {
			string address = 1;
			PageRequest pagination = 2;
		}

		message QueryAllBalancesResponse {
			repeated Coin balances = 1;
			PageResponse pagination = 2;
		}

		message QuerySpendableBalancesRequest {
			string address = 1;
			PageRequest pagination = 2;
		}

		message QuerySpendableBalancesResponse {
			repeated Coin balances = 1;
			PageResponse pagination = 2;
		}

		message QueryTotalSupplyRequest {
			PageRequest pagination = 1;
		}

		message QueryTotalSupplyResponse {
			repeated Coin supply = 1;
			PageResponse pagination = 2;
		}

		message QuerySupplyOfRequest {
			string denom = 1;
		}

		message QuerySupplyOfResponse {
			Coin amount = 1;
		}

		message Coin {
			string denom = 1;
			string amount = 2;
		}

		message PageRequest {
			bytes key = 1;
			uint64 offset = 2;
			uint64 limit = 3;
			bool count_total = 4;
			bool reverse = 5;
		}

		message PageResponse {
			bytes next_key = 1;
			uint64 total = 2;
		}
	`,
	staking: `
		syntax = "proto3";
		package cosmos.staking.v1beta1;

		service Query {
			rpc Validators(QueryValidatorsRequest) returns (QueryValidatorsResponse);
			rpc Validator(QueryValidatorRequest) returns (QueryValidatorResponse);
			rpc Pool(QueryPoolRequest) returns (QueryPoolResponse);
		}

		message QueryValidatorsRequest {
			string status = 1;
			PageRequest pagination = 2;
		}

		message QueryValidatorsResponse {
			repeated Validator validators = 1;
			PageResponse pagination = 2;
		}

		message QueryValidatorRequest {
			string validator_addr = 1;
		}

		message QueryValidatorResponse {
			Validator validator = 1;
		}

		message QueryPoolRequest {}

		message QueryPoolResponse {
			Pool pool = 1;
		}

		message Validator {
			string operator_address = 1;
			bytes consensus_pubkey = 2;
			bool jailed = 3;
			int32 status = 4;
			string tokens = 5;
			string delegator_shares = 6;
			Description description = 7;
			int64 unbonding_height = 8;
			string unbonding_time = 9;
			Commission commission = 10;
			string min_self_delegation = 11;
		}

		message Description {
			string moniker = 1;
			string identity = 2;
			string website = 3;
			string security_contact = 4;
			string details = 5;
		}

		message Commission {
			CommissionRates commission_rates = 1;
			string update_time = 2;
		}

		message CommissionRates {
			string rate = 1;
			string max_rate = 2;
			string max_change_rate = 3;
		}

		message Pool {
			string not_bonded_tokens = 1;
			string bonded_tokens = 2;
		}

		message PageRequest {
			bytes key = 1;
			uint64 offset = 2;
			uint64 limit = 3;
			bool count_total = 4;
			bool reverse = 5;
		}

		message PageResponse {
			bytes next_key = 1;
			uint64 total = 2;
		}
	`,
	slashing: `
		syntax = "proto3";
		package cosmos.slashing.v1beta1;

		service Query {
			rpc Params(QueryParamsRequest) returns (QueryParamsResponse);
			rpc SigningInfo(QuerySigningInfoRequest) returns (QuerySigningInfoResponse);
			rpc SigningInfos(QuerySigningInfosRequest) returns (QuerySigningInfosResponse);
		}

		message QueryParamsRequest {}

		message QueryParamsResponse {
			Params params = 1;
		}

		message Params {
			int64 signed_blocks_window = 1;
			bytes min_signed_per_window = 2;
			string downtime_jail_duration = 3;
			bytes slash_fraction_double_sign = 4;
			bytes slash_fraction_downtime = 5;
		}

		message QuerySigningInfoRequest {
			string cons_address = 1;
		}

		message QuerySigningInfoResponse {
			ValidatorSigningInfo val_signing_info = 1;
		}

		message QuerySigningInfosRequest {
			PageRequest pagination = 1;
		}

		message QuerySigningInfosResponse {
			repeated ValidatorSigningInfo info = 1;
			PageResponse pagination = 2;
		}

		message ValidatorSigningInfo {
			string address = 1;
			int64 start_height = 2;
			int64 index_offset = 3;
			string jailed_until = 4;
			bool tombstoned = 5;
			int64 missed_blocks_counter = 6;
		}

		message PageRequest {
			bytes key = 1;
			uint64 offset = 2;
			uint64 limit = 3;
			bool count_total = 4;
			bool reverse = 5;
		}

		message PageResponse {
			bytes next_key = 1;
			uint64 total = 2;
		}
	`,
	ibc_channel: `
		syntax = "proto3";
		package ibc.core.channel.v1;

		service Query {
			rpc Channel(QueryChannelRequest) returns (QueryChannelResponse);
			rpc ChannelClientState(QueryChannelClientStateRequest) returns (QueryChannelClientStateResponse);
		}

		message QueryChannelRequest {
			string port_id = 1;
			string channel_id = 2;
		}

		message QueryChannelResponse {
			Channel channel = 1;
			bytes proof = 2;
			Height proof_height = 3;
		}

		message QueryChannelClientStateRequest {
			string port_id = 1;
			string channel_id = 2;
		}

		message QueryChannelClientStateResponse {
			IdentifiedClientState identified_client_state = 1;
			bytes proof = 2;
			Height proof_height = 3;
		}

		message Channel {
			int32 state = 1;
			int32 ordering = 2;
			Counterparty counterparty = 3;
			repeated string connection_hops = 4;
			string version = 5;
		}

		message Counterparty {
			string port_id = 1;
			string channel_id = 2;
		}

		message IdentifiedClientState {
			string client_id = 1;
			Any client_state = 2;
		}

		message Any {
			string type_url = 1;
			bytes value = 2;
		}

		message Height {
			uint64 revision_number = 1;
			uint64 revision_height = 2;
		}

		// Tendermint client state for decoding chain_id
		message TendermintClientState {
			string chain_id = 1;
		}
	`,
	ibc_transfer: `
		syntax = "proto3";
		package ibc.applications.transfer.v1;

		service Query {
			rpc DenomTrace(QueryDenomTraceRequest) returns (QueryDenomTraceResponse);
		}

		message QueryDenomTraceRequest {
			string hash = 1;
		}

		message QueryDenomTraceResponse {
			DenomTrace denom_trace = 1;
		}

		message DenomTrace {
			string path = 1;
			string base_denom = 2;
		}
	`,
}

// Generic gRPC query client
class ChainQueryClient {
	private endpoint: string
	private credentials: grpc.ChannelCredentials
	public roots: Map<string, protobuf.Root> = new Map()
	private stubs: Map<string, any> = new Map()

	constructor(endpoint: string, insecure: boolean) {
		this.endpoint = endpoint
		this.credentials = insecure
			? grpc.credentials.createInsecure()
			: grpc.credentials.createSsl()

		console.log(`[ChainQuery] Connecting to ${endpoint} (insecure: ${insecure})`)

		// Parse all proto definitions
		for (const [name, proto] of Object.entries(PROTOS)) {
			const root = protobuf.Root.fromJSON(protobuf.parse(proto).root.toJSON())
			this.roots.set(name, root)
		}
	}

	// Create a stub for a specific service.
	// Cache key includes method names so different method sets for the same
	// service each get their own stub (e.g. bank AllBalances vs SupplyOf).
	private getStub(service: string, moduleName: string, methods: Record<string, { path: string; requestType: string; responseType: string }>): any {
		const cacheKey = `${service}:${Object.keys(methods).sort().join(',')}`
		if (this.stubs.has(cacheKey)) {
			return this.stubs.get(cacheKey)
		}

		const root = this.roots.get(moduleName)
		if (!root) {
			throw new Error(`Unknown module: ${moduleName} (from service: ${service})`)
		}

		const methodDefs: Record<string, any> = {}

		for (const [methodName, config] of Object.entries(methods)) {
			const requestType = root.lookupType(config.requestType)
			const responseType = root.lookupType(config.responseType)

			methodDefs[methodName] = {
				path: config.path,
				requestStream: false,
				responseStream: false,
				requestSerialize: (value: any) => Buffer.from(requestType.encode(requestType.create(value)).finish()),
				requestDeserialize: (buffer: Buffer) => requestType.decode(buffer),
				responseSerialize: (value: any) => Buffer.from(responseType.encode(value).finish()),
				responseDeserialize: (buffer: Buffer) => responseType.decode(buffer),
			}
		}

		const ClientClass = grpc.makeGenericClientConstructor(methodDefs, service, {})
		const stub = new ClientClass(this.endpoint, this.credentials)
		this.stubs.set(cacheKey, stub)
		return stub
	}

	// Backward-compat wrapper: infer module name from service path
	private getStubAuto(service: string, methods: Record<string, { path: string; requestType: string; responseType: string }>): any {
		const parts = service.split('.')
		const moduleName = parts.length >= 2 ? parts[1] : parts[0]
		return this.getStub(service, moduleName, methods)
	}

	// Bank queries
	async getAllBalances(address: string): Promise<any> {
		const stub = this.getStubAuto('cosmos.bank.v1beta1.Query', {
			AllBalances: {
				path: '/cosmos.bank.v1beta1.Query/AllBalances',
				requestType: 'cosmos.bank.v1beta1.QueryAllBalancesRequest',
				responseType: 'cosmos.bank.v1beta1.QueryAllBalancesResponse',
			},
		})

		return new Promise((resolve, reject) => {
			stub.AllBalances({ address, pagination: { limit: 100 } }, (err: Error | null, response: any) => {
				if (err) {
					reject(err)
					return
				}
				resolve({
					balances: (response.balances || []).map((b: any) => ({
						denom: b.denom,
						amount: b.amount,
					})),
				})
			})
		})
	}

	async getSpendableBalances(address: string): Promise<any> {
		const stub = this.getStubAuto('cosmos.bank.v1beta1.Query', {
			SpendableBalances: {
				path: '/cosmos.bank.v1beta1.Query/SpendableBalances',
				requestType: 'cosmos.bank.v1beta1.QuerySpendableBalancesRequest',
				responseType: 'cosmos.bank.v1beta1.QuerySpendableBalancesResponse',
			},
		})

		return new Promise((resolve, reject) => {
			stub.SpendableBalances({ address, pagination: { limit: 100 } }, (err: Error | null, response: any) => {
				if (err) {
					reject(err)
					return
				}
				resolve({
					balances: (response.balances || []).map((b: any) => ({
						denom: b.denom,
						amount: b.amount,
					})),
				})
			})
		})
	}

	async getSupplyOf(denom: string): Promise<any> {
		const stub = this.getStubAuto('cosmos.bank.v1beta1.Query', {
			SupplyOf: {
				path: '/cosmos.bank.v1beta1.Query/SupplyOf',
				requestType: 'cosmos.bank.v1beta1.QuerySupplyOfRequest',
				responseType: 'cosmos.bank.v1beta1.QuerySupplyOfResponse',
			},
		})

		return new Promise((resolve, reject) => {
			stub.SupplyOf({ denom }, (err: Error | null, response: any) => {
				if (err) {
					reject(err)
					return
				}
				resolve({
					amount: response.amount ? {
						denom: response.amount.denom,
						amount: response.amount.amount,
					} : null,
				})
			})
		})
	}

	// Staking queries

	/** Returns all staking gRPC method definitions (shared across staking methods to avoid stub cache conflicts) */
	private stakingMethods() {
		return {
			Validators: {
				path: '/cosmos.staking.v1beta1.Query/Validators',
				requestType: 'cosmos.staking.v1beta1.QueryValidatorsRequest',
				responseType: 'cosmos.staking.v1beta1.QueryValidatorsResponse',
			},
			Validator: {
				path: '/cosmos.staking.v1beta1.Query/Validator',
				requestType: 'cosmos.staking.v1beta1.QueryValidatorRequest',
				responseType: 'cosmos.staking.v1beta1.QueryValidatorResponse',
			},
			Pool: {
				path: '/cosmos.staking.v1beta1.Query/Pool',
				requestType: 'cosmos.staking.v1beta1.QueryPoolRequest',
				responseType: 'cosmos.staking.v1beta1.QueryPoolResponse',
			},
		}
	}

	/** Fetches validators from chain, optionally filtered by status */
	async getValidators(status?: string): Promise<any> {
		const stub = this.getStubAuto('cosmos.staking.v1beta1.Query', this.stakingMethods())

		return new Promise((resolve, reject) => {
			stub.Validators(
				{ status: status || '', pagination: { limit: 500, countTotal: true } },
				(err: Error | null, response: any) => {
					if (err) {
						reject(err)
						return
					}
					const validators = (response.validators || []).map((v: any) => ({
						operatorAddress: v.operatorAddress || v.operator_address || '',
						jailed: v.jailed || false,
						status: v.status,
						tokens: v.tokens || '0',
						delegatorShares: v.delegatorShares || v.delegator_shares || '0',
						description: {
							moniker: v.description?.moniker || '',
							identity: v.description?.identity || '',
							website: v.description?.website || '',
							securityContact: v.description?.securityContact || v.description?.security_contact || '',
							details: v.description?.details || '',
						},
						commission: {
							commissionRates: {
								rate: v.commission?.commissionRates?.rate || v.commission?.commission_rates?.rate || '0',
								maxRate: v.commission?.commissionRates?.maxRate || v.commission?.commission_rates?.max_rate || '0',
								maxChangeRate: v.commission?.commissionRates?.maxChangeRate || v.commission?.commission_rates?.max_change_rate || '0',
							},
						},
						minSelfDelegation: v.minSelfDelegation || v.min_self_delegation || '0',
						unbondingHeight: v.unbondingHeight || v.unbonding_height || '0',
					}))
					resolve({ validators })
				}
			)
		})
	}

	/** Fetches a single validator by operator address */
	async getValidator(validatorAddr: string): Promise<any> {
		const stub = this.getStubAuto('cosmos.staking.v1beta1.Query', this.stakingMethods())

		return new Promise((resolve, reject) => {
			stub.Validator(
				{ validatorAddr },
				(err: Error | null, response: any) => {
					if (err) {
						reject(err)
						return
					}
					const v = response.validator
					if (!v) {
						resolve({ validator: null })
						return
					}
					resolve({
						validator: {
							operatorAddress: v.operatorAddress || v.operator_address || '',
							jailed: v.jailed || false,
							status: v.status,
							tokens: v.tokens || '0',
							delegatorShares: v.delegatorShares || v.delegator_shares || '0',
							description: {
								moniker: v.description?.moniker || '',
								identity: v.description?.identity || '',
								website: v.description?.website || '',
								securityContact: v.description?.securityContact || v.description?.security_contact || '',
								details: v.description?.details || '',
							},
							commission: {
								commissionRates: {
									rate: v.commission?.commissionRates?.rate || v.commission?.commission_rates?.rate || '0',
									maxRate: v.commission?.commissionRates?.maxRate || v.commission?.commission_rates?.max_rate || '0',
									maxChangeRate: v.commission?.commissionRates?.maxChangeRate || v.commission?.commission_rates?.max_change_rate || '0',
								},
							},
							minSelfDelegation: v.minSelfDelegation || v.min_self_delegation || '0',
							unbondingHeight: v.unbondingHeight || v.unbonding_height || '0',
						},
					})
				}
			)
		})
	}

	/** Fetches the staking pool (bonded/not-bonded totals) */
	async getStakingPool(): Promise<any> {
		const stub = this.getStubAuto('cosmos.staking.v1beta1.Query', this.stakingMethods())

		return new Promise((resolve, reject) => {
			stub.Pool({}, (err: Error | null, response: any) => {
				if (err) {
					reject(err)
					return
				}
				resolve({
					pool: {
						notBondedTokens: response.pool?.notBondedTokens || response.pool?.not_bonded_tokens || '0',
						bondedTokens: response.pool?.bondedTokens || response.pool?.bonded_tokens || '0',
					},
				})
			})
		})
	}

	// Slashing queries

	/** Returns all slashing gRPC method definitions */
	private slashingMethods() {
		return {
			Params: {
				path: '/cosmos.slashing.v1beta1.Query/Params',
				requestType: 'cosmos.slashing.v1beta1.QueryParamsRequest',
				responseType: 'cosmos.slashing.v1beta1.QueryParamsResponse',
			},
			SigningInfo: {
				path: '/cosmos.slashing.v1beta1.Query/SigningInfo',
				requestType: 'cosmos.slashing.v1beta1.QuerySigningInfoRequest',
				responseType: 'cosmos.slashing.v1beta1.QuerySigningInfoResponse',
			},
			SigningInfos: {
				path: '/cosmos.slashing.v1beta1.Query/SigningInfos',
				requestType: 'cosmos.slashing.v1beta1.QuerySigningInfosRequest',
				responseType: 'cosmos.slashing.v1beta1.QuerySigningInfosResponse',
			},
		}
	}

	/** Fetches slashing params (signed_blocks_window, etc.) */
	async getSlashingParams(): Promise<any> {
		const stub = this.getStubAuto('cosmos.slashing.v1beta1.Query', this.slashingMethods())

		return new Promise((resolve, reject) => {
			stub.Params({}, (err: Error | null, response: any) => {
				if (err) {
					reject(err)
					return
				}
				const params = response.params || {}
				resolve({
					params: {
						signed_blocks_window: params.signed_blocks_window?.toString() || params.signedBlocksWindow?.toString() || '0',
						min_signed_per_window: params.min_signed_per_window?.toString() || params.minSignedPerWindow?.toString() || '0',
						downtime_jail_duration: params.downtime_jail_duration || params.downtimeJailDuration || '0s',
						slash_fraction_double_sign: params.slash_fraction_double_sign?.toString() || params.slashFractionDoubleSign?.toString() || '0',
						slash_fraction_downtime: params.slash_fraction_downtime?.toString() || params.slashFractionDowntime?.toString() || '0',
					},
				})
			})
		})
	}

	/** Fetches signing info for a specific validator by consensus address */
	async getSigningInfo(consAddress: string): Promise<any> {
		const stub = this.getStubAuto('cosmos.slashing.v1beta1.Query', this.slashingMethods())

		return new Promise((resolve, reject) => {
			stub.SigningInfo({ consAddress }, (err: Error | null, response: any) => {
				if (err) {
					// Not found is ok - validator may not have signing info yet
					if (err.message?.includes('NotFound') || err.message?.includes('not found')) {
						resolve({ val_signing_info: null })
						return
					}
					reject(err)
					return
				}
				const info = response.val_signing_info || response.valSigningInfo || {}
				resolve({
					val_signing_info: {
						address: info.address || '',
						start_height: info.start_height?.toString() || info.startHeight?.toString() || '0',
						index_offset: info.index_offset?.toString() || info.indexOffset?.toString() || '0',
						jailed_until: info.jailed_until || info.jailedUntil || null,
						tombstoned: info.tombstoned || false,
						missed_blocks_counter: info.missed_blocks_counter?.toString() || info.missedBlocksCounter?.toString() || '0',
					},
				})
			})
		})
	}

	/** Fetches signing info for all validators */
	async getAllSigningInfos(): Promise<any> {
		const stub = this.getStubAuto('cosmos.slashing.v1beta1.Query', this.slashingMethods())

		return new Promise((resolve, reject) => {
			stub.SigningInfos({ pagination: { limit: 500 } }, (err: Error | null, response: any) => {
				if (err) {
					reject(err)
					return
				}
				const infos = (response.info || []).map((info: any) => ({
					address: info.address || '',
					start_height: info.start_height?.toString() || info.startHeight?.toString() || '0',
					index_offset: info.index_offset?.toString() || info.indexOffset?.toString() || '0',
					jailed_until: info.jailed_until || info.jailedUntil || null,
					tombstoned: info.tombstoned || false,
					missed_blocks_counter: info.missed_blocks_counter?.toString() || info.missedBlocksCounter?.toString() || '0',
				}))
				resolve({ info: infos })
			})
		})
	}

	// Auth queries

	/** Fetches account info (account number, sequence) for signing transactions */
	async getAccount(address: string): Promise<any> {
		const root = this.roots.get('auth')
		if (!root) throw new Error('Auth proto not loaded')

		const stub = this.getStubAuto('cosmos.auth.v1beta1.Query', {
			Account: {
				path: '/cosmos.auth.v1beta1.Query/Account',
				requestType: 'cosmos.auth.v1beta1.QueryAccountRequest',
				responseType: 'cosmos.auth.v1beta1.QueryAccountResponse',
			},
		})

		return new Promise((resolve, reject) => {
			stub.Account({ address }, (err: Error | null, response: any) => {
				if (err) {
					// Check if account not found (new account)
					if (err.message?.includes('NotFound') || err.message?.includes('not found')) {
						resolve({
							account: null,
							account_number: '0',
							sequence: '0',
						})
						return
					}
					reject(err)
					return
				}

				const account = response.account
				if (!account || !account.value) {
					resolve({ account: null, account_number: '0', sequence: '0' })
					return
				}

				try {
					const typeUrl = account.type_url || account.typeUrl || ''
					let accountNumber = '0'
					let sequence = '0'

					// Decode based on account type
					if (typeUrl.includes('EthAccount')) {
						// Decode EthAccount which wraps BaseAccount
						const EthAccount = root.lookupType('cosmos.auth.v1beta1.EthAccount')
						const decoded = EthAccount.decode(account.value)
						const baseAccount = (decoded as any).base_account || (decoded as any).baseAccount
						if (baseAccount) {
							accountNumber = String(baseAccount.account_number || baseAccount.accountNumber || 0)
							sequence = String(baseAccount.sequence || 0)
						}
					} else {
						// Decode BaseAccount directly
						const BaseAccount = root.lookupType('cosmos.auth.v1beta1.BaseAccount')
						const decoded = BaseAccount.decode(account.value)
						accountNumber = String((decoded as any).account_number || (decoded as any).accountNumber || 0)
						sequence = String((decoded as any).sequence || 0)
					}

					// Return in REST API compatible format
					resolve({
						account: {
							'@type': typeUrl,
							base_account: {
								address,
								account_number: accountNumber,
								sequence: sequence,
							},
						},
					})
				} catch (decodeErr: any) {
					console.error('[ChainQuery] Failed to decode account:', decodeErr.message)
					// Return raw data if decode fails
					resolve({
						account: {
							'@type': account.type_url || account.typeUrl || '',
							raw_value: Buffer.from(account.value).toString('base64'),
						},
						account_number: '0',
						sequence: '0',
					})
				}
			})
		})
	}

	// Transaction broadcast

	/** Broadcasts a signed transaction to the chain */
	async broadcastTx(txBytes: Buffer, mode: number = 2): Promise<any> {
		const stub = this.getStubAuto('cosmos.tx.v1beta1.Service', {
			BroadcastTx: {
				path: '/cosmos.tx.v1beta1.Service/BroadcastTx',
				requestType: 'cosmos.tx.v1beta1.BroadcastTxRequest',
				responseType: 'cosmos.tx.v1beta1.BroadcastTxResponse',
			},
		})

		return new Promise((resolve, reject) => {
			stub.BroadcastTx({ txBytes, mode }, (err: Error | null, response: any) => {
				if (err) {
					reject(err)
					return
				}

				const txResponse = response.tx_response || response.txResponse || {}
				resolve({
					tx_response: {
						height: txResponse.height?.toString() || '0',
						txhash: txResponse.txhash || txResponse.txHash || '',
						codespace: txResponse.codespace || '',
						code: txResponse.code || 0,
						data: txResponse.data || '',
						raw_log: txResponse.raw_log || txResponse.rawLog || '',
						info: txResponse.info || '',
						gas_wanted: txResponse.gas_wanted?.toString() || txResponse.gasWanted?.toString() || '0',
						gas_used: txResponse.gas_used?.toString() || txResponse.gasUsed?.toString() || '0',
					},
				})
			})
		})
	}

	// IBC queries

	/** Fetches IBC channel info */
	async getChannel(channelId: string, portId: string): Promise<any> {
		const stub = this.getStub('ibc.core.channel.v1.Query', 'ibc_channel', {
			Channel: {
				path: '/ibc.core.channel.v1.Query/Channel',
				requestType: 'ibc.core.channel.v1.QueryChannelRequest',
				responseType: 'ibc.core.channel.v1.QueryChannelResponse',
			},
		})

		return new Promise((resolve, reject) => {
			stub.Channel({ portId, channelId }, (err: Error | null, response: any) => {
				if (err) {
					reject(err)
					return
				}
				const ch = response.channel || {}
				const stateMap: Record<number, string> = {
					0: 'STATE_UNINITIALIZED_UNSPECIFIED',
					1: 'STATE_INIT',
					2: 'STATE_TRYOPEN',
					3: 'STATE_OPEN',
					4: 'STATE_CLOSED',
				}
				resolve({
					channel: {
						state: stateMap[ch.state] || String(ch.state),
						ordering: ch.ordering,
						counterparty: {
							port_id: ch.counterparty?.portId || ch.counterparty?.port_id || '',
							channel_id: ch.counterparty?.channelId || ch.counterparty?.channel_id || '',
						},
						connection_hops: ch.connectionHops || ch.connection_hops || [],
						version: ch.version || '',
					},
				})
			})
		})
	}

	/** Fetches IBC channel client state (for counterparty chain ID) */
	async getChannelClientState(channelId: string, portId: string): Promise<any> {
		const stub = this.getStub('ibc.core.channel.v1.Query', 'ibc_channel', {
			ChannelClientState: {
				path: '/ibc.core.channel.v1.Query/ChannelClientState',
				requestType: 'ibc.core.channel.v1.QueryChannelClientStateRequest',
				responseType: 'ibc.core.channel.v1.QueryChannelClientStateResponse',
			},
		})

		const root = this.roots.get('ibc_channel')

		return new Promise((resolve, reject) => {
			stub.ChannelClientState({ portId, channelId }, (err: Error | null, response: any) => {
				if (err) {
					reject(err)
					return
				}
				const ics = response.identifiedClientState || response.identified_client_state || {}
				const clientState = ics.clientState || ics.client_state || {}
				let chainId = ''

				// Try to decode the client state Any to get chain_id
				if (clientState.value && root) {
					try {
						const TendermintClientState = root.lookupType('ibc.core.channel.v1.TendermintClientState')
						const decoded = TendermintClientState.decode(clientState.value) as any
						chainId = decoded.chain_id || decoded.chainId || ''
					} catch {
						// Decode failed, try typeUrl-based approach
					}
				}

				resolve({
					identified_client_state: {
						client_id: ics.clientId || ics.client_id || '',
						client_state: {
							'@type': clientState.typeUrl || clientState.type_url || '',
							chain_id: chainId,
						},
					},
				})
			})
		})
	}

	/** Resolves an IBC denom hash to path + base denom */
	async getDenomTrace(hash: string): Promise<any> {
		const stub = this.getStub('ibc.applications.transfer.v1.Query', 'ibc_transfer', {
			DenomTrace: {
				path: '/ibc.applications.transfer.v1.Query/DenomTrace',
				requestType: 'ibc.applications.transfer.v1.QueryDenomTraceRequest',
				responseType: 'ibc.applications.transfer.v1.QueryDenomTraceResponse',
			},
		})

		return new Promise((resolve, reject) => {
			stub.DenomTrace({ hash }, (err: Error | null, response: any) => {
				if (err) {
					if (err.message?.includes('NotFound') || err.message?.includes('not found')) {
						resolve({ denom_trace: null })
						return
					}
					reject(err)
					return
				}
				const trace = response.denom_trace || response.denomTrace || {}
				resolve({
					denom_trace: {
						path: trace.path || '',
						base_denom: trace.base_denom || trace.baseDenom || '',
					},
				})
			})
		})
	}
}

// ============================================================================
// Multi-chain client pool
// ============================================================================

const clientPool = new Map<string, ChainQueryClient>()

/** Get or lazily create a gRPC client for the given chain ID */
function getClient(chainId: string): ChainQueryClient {
	let client = clientPool.get(chainId)
	if (!client) {
		const cfg = getChainConfig(chainId)
		client = new ChainQueryClient(cfg.grpcEndpoint, !cfg.tls)
		clientPool.set(chainId, client)
	}
	return client
}

// ============================================================================
// Route definitions
// ============================================================================

// Route handler receives chain's client, params, and query string
type RouteHandler = (client: ChainQueryClient, params: Record<string, string>, query: URLSearchParams) => Promise<any>

// Each route pattern matches the path AFTER /chain/:chainId/
const routes: Array<{ pattern: RegExp; paramNames: string[]; handler: RouteHandler }> = [
	{
		pattern: /^balances\/([a-zA-Z0-9]+)$/,
		paramNames: ['address'],
		handler: async (c, p) => c.getAllBalances(p.address),
	},
	{
		pattern: /^spendable\/([a-zA-Z0-9]+)$/,
		paramNames: ['address'],
		handler: async (c, p) => c.getSpendableBalances(p.address),
	},
	{
		pattern: /^supply\/([a-zA-Z0-9]+)$/,
		paramNames: ['denom'],
		handler: async (c, p) => c.getSupplyOf(p.denom),
	},
	{
		pattern: /^staking\/validators$/,
		paramNames: [],
		handler: async (c, _p, q) => c.getValidators(q.get('status') || undefined),
	},
	{
		pattern: /^staking\/validator\/([a-zA-Z0-9]+)$/,
		paramNames: ['validatorAddr'],
		handler: async (c, p) => c.getValidator(p.validatorAddr),
	},
	{
		pattern: /^staking\/pool$/,
		paramNames: [],
		handler: async (c) => c.getStakingPool(),
	},
	{
		pattern: /^auth\/account\/([a-zA-Z0-9]+)$/,
		paramNames: ['address'],
		handler: async (c, p) => c.getAccount(p.address),
	},
	{
		pattern: /^slashing\/params$/,
		paramNames: [],
		handler: async (c) => c.getSlashingParams(),
	},
	{
		pattern: /^slashing\/signing_info\/([a-zA-Z0-9]+)$/,
		paramNames: ['consAddress'],
		handler: async (c, p) => c.getSigningInfo(p.consAddress),
	},
	{
		pattern: /^slashing\/signing_infos$/,
		paramNames: [],
		handler: async (c) => c.getAllSigningInfos(),
	},
	// IBC endpoints
	{
		pattern: /^ibc\/channel\/([a-zA-Z0-9-]+)\/([a-zA-Z0-9-]+)\/client_state$/,
		paramNames: ['channelId', 'portId'],
		handler: async (c, p) => c.getChannelClientState(p.channelId, p.portId),
	},
	{
		pattern: /^ibc\/channel\/([a-zA-Z0-9-]+)\/([a-zA-Z0-9-]+)$/,
		paramNames: ['channelId', 'portId'],
		handler: async (c, p) => c.getChannel(p.channelId, p.portId),
	},
	{
		pattern: /^ibc\/denom_trace\/([a-fA-F0-9]+)$/,
		paramNames: ['hash'],
		handler: async (c, p) => c.getDenomTrace(p.hash),
	},
]

// Known endpoint prefixes -- used to distinguish endpoint names from chain IDs
// in the backward-compat rewrite logic
const ENDPOINT_PREFIXES = new Set([
	'balances', 'spendable', 'supply', 'staking', 'auth', 'slashing',
	'tx', 'health', 'ibc',
])

// ============================================================================
// HTTP server
// ============================================================================

/** Helper to read request body */
async function readBody(req: http.IncomingMessage): Promise<string> {
	return new Promise((resolve, reject) => {
		let body = ''
		req.on('data', (chunk) => { body += chunk.toString() })
		req.on('end', () => resolve(body))
		req.on('error', reject)
	})
}

/**
 * Parse the URL to extract chain ID and the remaining endpoint path.
 * URL format: /chain/:chainId/<endpoint> or /chain/<endpoint> (uses default chain)
 * Returns { chainId, endpointPath } or null if the URL doesn't match /chain/...
 */
function parseChainUrl(pathname: string): { chainId: string; endpointPath: string } | null {
	// Must start with /chain/
	if (!pathname.startsWith('/chain/')) return null

	// Strip /chain/ prefix
	const rest = pathname.slice('/chain/'.length)
	if (!rest) return null

	// Split into segments
	const firstSlash = rest.indexOf('/')
	const firstSegment = firstSlash === -1 ? rest : rest.slice(0, firstSlash)
	const remaining = firstSlash === -1 ? '' : rest.slice(firstSlash + 1)

	// Is the first segment an endpoint name or a chain ID?
	if (ENDPOINT_PREFIXES.has(firstSegment)) {
		// No chain ID in URL -- use default chain
		return { chainId: getDefaultChainId(), endpointPath: rest }
	}

	// First segment is a chain ID
	const chainIds = getChainIds()
	if (chainIds.has(firstSegment)) {
		return { chainId: firstSegment, endpointPath: remaining }
	}

	// Unknown first segment -- treat as endpoint for backward compat
	return { chainId: getDefaultChainId(), endpointPath: rest }
}

// Load config at startup to validate chains.toml early
const chainsConfig = loadChainsConfig()
console.log(`[ChainQuery] Loaded ${Object.keys(chainsConfig.chains).length} chain(s), default: ${chainsConfig.defaultChain}`)

const server = http.createServer(async (req, res) => {
	// CORS headers
	res.setHeader('Access-Control-Allow-Origin', '*')
	res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
	res.setHeader('Access-Control-Allow-Headers', 'Content-Type')
	res.setHeader('Content-Type', 'application/json')

	if (req.method === 'OPTIONS') {
		res.writeHead(204)
		res.end()
		return
	}

	const url = new URL(req.url || '/', `http://localhost:${PORT}`)

	// Global health check -- all chains
	if (url.pathname === '/chain/health') {
		const chains: Record<string, { endpoint: string; tls: boolean }> = {}
		for (const [id, cfg] of Object.entries(chainsConfig.chains)) {
			chains[id] = { endpoint: cfg.grpcEndpoint, tls: cfg.tls }
		}
		res.writeHead(200)
		res.end(JSON.stringify({
			status: 'ok',
			defaultChain: chainsConfig.defaultChain,
			chains,
		}))
		return
	}

	// Parse chain ID and endpoint path from URL
	const parsed = parseChainUrl(url.pathname)
	if (!parsed) {
		res.writeHead(404)
		res.end(JSON.stringify({ error: 'Not found' }))
		return
	}

	const { chainId, endpointPath } = parsed

	// Validate chain ID
	let client: ChainQueryClient
	try {
		client = getClient(chainId)
	} catch (err: any) {
		res.writeHead(400)
		res.end(JSON.stringify({ error: err.message }))
		return
	}

	// Per-chain health check
	if (endpointPath === 'health') {
		const cfg = chainsConfig.chains[chainId]
		res.writeHead(200)
		res.end(JSON.stringify({
			status: 'ok',
			chainId,
			endpoint: cfg?.grpcEndpoint,
			tls: cfg?.tls,
		}))
		return
	}

	// POST tx/broadcast
	if (endpointPath === 'tx/broadcast' && req.method === 'POST') {
		try {
			const body = await readBody(req)
			const data = JSON.parse(body)

			let txBytes: Buffer
			let mode = 2 // Default BROADCAST_MODE_SYNC

			if (data.tx_bytes) {
				// Direct protobuf bytes (base64 encoded)
				txBytes = Buffer.from(data.tx_bytes, 'base64')
				mode = data.mode || 2
			} else if (data.tx) {
				// JSON tx format - encode to protobuf
				const txRoot = client.roots.get('tx')
				if (!txRoot) throw new Error('TX proto not loaded')

				const tx = data.tx

				// Encode TxBody
				const TxBody = txRoot.lookupType('cosmos.tx.v1beta1.TxBody')
				const Any = txRoot.lookupType('cosmos.tx.v1beta1.Any')

				const messages = (tx.body?.messages || []).map((msg: any) => {
					const typeUrl = msg['@type'] || ''
					const msgCopy = { ...msg }
					delete msgCopy['@type']
					const valueBytes = Buffer.from(JSON.stringify(msgCopy), 'utf8')
					return Any.create({ typeUrl, value: valueBytes })
				})

				const txBodyObj = TxBody.create({
					messages,
					memo: tx.body?.memo || '',
					timeoutHeight: 0,
				})
				const bodyBytes = TxBody.encode(txBodyObj).finish()

				// Encode AuthInfo
				const AuthInfo = txRoot.lookupType('cosmos.tx.v1beta1.AuthInfo')
				const SignerInfo = txRoot.lookupType('cosmos.tx.v1beta1.SignerInfo')
				const ModeInfo = txRoot.lookupType('cosmos.tx.v1beta1.ModeInfo')
				const Fee = txRoot.lookupType('cosmos.tx.v1beta1.Fee')
				const Coin = txRoot.lookupType('cosmos.tx.v1beta1.Coin')

				const signerInfos = (tx.auth_info?.signer_infos || []).map((si: any) => {
					const pubKey = si.public_key
					const pubKeyAny = Any.create({
						typeUrl: pubKey?.['@type'] || '',
						value: pubKey?.key ? Buffer.from(pubKey.key, 'base64') : Buffer.alloc(0),
					})

					// SIGN_MODE_LEGACY_AMINO_JSON = 127
					const modeInfo = ModeInfo.create({
						single: { mode: 127 }
					})

					return SignerInfo.create({
						publicKey: pubKeyAny,
						modeInfo,
						sequence: parseInt(si.sequence || '0', 10),
					})
				})

				const feeAmounts = (tx.auth_info?.fee?.amount || []).map((c: any) =>
					Coin.create({ denom: c.denom, amount: c.amount })
				)

				const fee = Fee.create({
					amount: feeAmounts,
					gasLimit: parseInt(tx.auth_info?.fee?.gas || '0', 10),
				})

				const authInfoObj = AuthInfo.create({
					signerInfos,
					fee,
				})
				const authInfoBytes = AuthInfo.encode(authInfoObj).finish()

				// Encode TxRaw
				const TxRaw = txRoot.lookupType('cosmos.tx.v1beta1.TxRaw')
				const signatures = (tx.signatures || []).map((sig: string) =>
					Buffer.from(sig, 'base64')
				)

				const txRawObj = TxRaw.create({
					bodyBytes,
					authInfoBytes,
					signatures,
				})
				txBytes = Buffer.from(TxRaw.encode(txRawObj).finish())

				// Parse mode string
				const modeStr = data.mode || 'BROADCAST_MODE_SYNC'
				if (modeStr === 'BROADCAST_MODE_BLOCK') mode = 1
				else if (modeStr === 'BROADCAST_MODE_SYNC') mode = 2
				else if (modeStr === 'BROADCAST_MODE_ASYNC') mode = 3
			} else {
				res.writeHead(400)
				res.end(JSON.stringify({ error: 'Missing tx_bytes or tx in request body' }))
				return
			}

			const result = await client.broadcastTx(txBytes, mode)
			res.writeHead(200)
			res.end(JSON.stringify(result))
		} catch (err: any) {
			console.error(`[ChainQuery] Broadcast error:`, err.message)
			res.writeHead(500)
			res.end(JSON.stringify({ error: err.message }))
		}
		return
	}

	// Match GET routes against the endpoint path (after chain ID)
	if (req.method === 'GET') {
		for (const route of routes) {
			const match = endpointPath.match(route.pattern)
			if (match) {
				const params: Record<string, string> = {}
				for (let i = 0; i < route.paramNames.length; i++) {
					params[route.paramNames[i]] = match[i + 1]
				}

				try {
					const result = await route.handler(client, params, url.searchParams)
					res.writeHead(200)
					res.end(JSON.stringify(result))
				} catch (err: any) {
					console.error(`[ChainQuery] Error (${chainId}):`, err.message)
					res.writeHead(500)
					res.end(JSON.stringify({ error: err.message }))
				}
				return
			}
		}
	}

	// 404
	res.writeHead(404)
	res.end(JSON.stringify({ error: 'Not found', endpoints: [
		'GET  /chain/health',
		'GET  /chain/:chainId/health',
		'GET  /chain/:chainId/balances/:address',
		'GET  /chain/:chainId/spendable/:address',
		'GET  /chain/:chainId/supply/:denom',
		'GET  /chain/:chainId/staking/validators?status=BOND_STATUS_BONDED',
		'GET  /chain/:chainId/staking/validator/:address',
		'GET  /chain/:chainId/staking/pool',
		'GET  /chain/:chainId/auth/account/:address',
		'GET  /chain/:chainId/slashing/params',
		'GET  /chain/:chainId/slashing/signing_info/:cons_address',
		'GET  /chain/:chainId/slashing/signing_infos',
		'GET  /chain/:chainId/ibc/channel/:channelId/:portId',
		'GET  /chain/:chainId/ibc/channel/:channelId/:portId/client_state',
		'GET  /chain/:chainId/ibc/denom_trace/:hash',
		'POST /chain/:chainId/tx/broadcast',
	]}))
})

// Start server
server.listen(PORT, () => {
	console.log(`[ChainQuery] Running on port ${PORT}`)
	console.log(`[ChainQuery] Default chain: ${chainsConfig.defaultChain}`)
	for (const [id, cfg] of Object.entries(chainsConfig.chains)) {
		console.log(`[ChainQuery]   ${id}: ${cfg.grpcEndpoint} (tls: ${cfg.tls})`)
	}
	console.log(`[ChainQuery] Endpoints: /chain/:chainId/<endpoint> (or /chain/<endpoint> for default)`)
})
