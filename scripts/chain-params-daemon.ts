#!/usr/bin/env bun
/**
 * Chain Parameters and IBC Data Daemon
 *
 * Queries chain gRPC endpoints using reflection for:
 * - Staking params (bond_denom)
 * - Bank supply
 * - IBC denom traces
 * - IBC channel/connection info
 * - Client status (for determining active channels)
 *
 * Populates database tables for explorer display.
 */

import * as grpc from '@grpc/grpc-js'
import protobuf from 'protobufjs'
import descriptorJson from 'protobufjs/google/protobuf/descriptor.json' with { type: 'json' }
import pg from 'pg'
import { createHash } from 'crypto'

const { Pool } = pg

const DATABASE_URL = process.env.DATABASE_URL
const CHAIN_GRPC_ENDPOINT = process.env.CHAIN_GRPC_ENDPOINT
const POLL_INTERVAL_MS = parseInt(process.env.CHAIN_PARAMS_POLL_INTERVAL_MS || '60000', 10)
const USE_TLS = process.env.YACI_INSECURE !== 'true'

if (!DATABASE_URL) {
	console.error('DATABASE_URL environment variable is required')
	process.exit(1)
}

if (!CHAIN_GRPC_ENDPOINT) {
	console.error('CHAIN_GRPC_ENDPOINT environment variable is required')
	process.exit(1)
}

// Reflection proto definitions (v1alpha is most common)
const REFLECTION_PROTO_V1ALPHA = `
syntax = "proto3";
package grpc.reflection.v1alpha;

service ServerReflection {
  rpc ServerReflectionInfo(stream ServerReflectionRequest)
      returns (stream ServerReflectionResponse);
}

message ServerReflectionRequest {
  string host = 1;
  oneof message_request {
    string file_by_filename = 3;
    string file_containing_symbol = 4;
    ExtensionRequest file_containing_extension = 5;
    string all_extension_numbers_of_type = 6;
    string list_services = 7;
  }
}

message ServerReflectionResponse {
  string valid_host = 1;
  ServerReflectionRequest original_request = 2;
  oneof message_response {
    FileDescriptorResponse file_descriptor_response = 4;
    ExtensionNumberResponse all_extension_numbers_response = 5;
    ListServiceResponse list_services_response = 6;
    ErrorResponse error_response = 7;
  }
}

message FileDescriptorResponse { repeated bytes file_descriptor_proto = 1; }
message ExtensionRequest { string containing_type = 1; int32 extension_number = 2; }
message ExtensionNumberResponse { string base_type_name = 1; repeated int32 extension_number = 2; }
message ListServiceResponse { repeated ServiceResponse service = 1; }
message ServiceResponse { string name = 1; }
message ErrorResponse { int32 error_code = 1; string error_message = 2; }
`

/**
 * gRPC Reflection Client for dynamic proto loading
 */
class ReflectionClient {
	private reflectionStub: any
	private root: protobuf.Root
	private seenFiles = new Set<string>()
	private descriptorRoot: protobuf.Root | null = null
	private endpoint: string
	private credentials: grpc.ChannelCredentials

	constructor(endpoint: string, tls: boolean) {
		this.endpoint = endpoint
		this.credentials = tls ? grpc.credentials.createSsl() : grpc.credentials.createInsecure()
		this.root = new protobuf.Root()
	}

	async initialize(): Promise<void> {
		this.descriptorRoot = protobuf.Root.fromJSON(descriptorJson)
		const reflectionRoot = protobuf.parse(REFLECTION_PROTO_V1ALPHA).root

		const ServerReflectionClient = grpc.makeGenericClientConstructor({
			ServerReflectionInfo: {
				path: '/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo',
				requestStream: true,
				responseStream: true,
				requestSerialize: (value: any) => Buffer.from(
					reflectionRoot.lookupType('grpc.reflection.v1alpha.ServerReflectionRequest').encode(value).finish()
				),
				requestDeserialize: (buffer: Buffer) =>
					reflectionRoot.lookupType('grpc.reflection.v1alpha.ServerReflectionRequest').decode(buffer),
				responseSerialize: (value: any) => Buffer.from(
					reflectionRoot.lookupType('grpc.reflection.v1alpha.ServerReflectionResponse').encode(value).finish()
				),
				responseDeserialize: (buffer: Buffer) =>
					reflectionRoot.lookupType('grpc.reflection.v1alpha.ServerReflectionResponse').decode(buffer),
			},
		}, 'ServerReflection', {})

		this.reflectionStub = new ServerReflectionClient(this.endpoint, this.credentials)
	}

	async loadService(serviceName: string): Promise<void> {
		return new Promise((resolve, reject) => {
			const call = this.reflectionStub.ServerReflectionInfo()
			let resolved = false

			call.on('data', (response: any) => {
				if (response.fileDescriptorResponse) {
					for (const fdBytes of response.fileDescriptorResponse.fileDescriptorProto) {
						this.processFileDescriptor(Buffer.from(fdBytes))
					}
					if (!resolved) {
						resolved = true
						resolve()
					}
				} else if (response.errorResponse) {
					if (!resolved) {
						resolved = true
						reject(new Error(response.errorResponse.errorMessage))
					}
				}
			})

			call.on('error', (err: Error) => {
				if (!resolved) {
					resolved = true
					reject(err)
				}
			})

			call.write({ fileContainingSymbol: serviceName })
			call.end()
		})
	}

	private processFileDescriptor(fdBytes: Buffer): void {
		if (!this.descriptorRoot) return

		const FileDescriptorProto = this.descriptorRoot.lookupType('google.protobuf.FileDescriptorProto')
		const descriptor = FileDescriptorProto.decode(fdBytes) as any
		const filename = descriptor.name || 'unknown'

		if (this.seenFiles.has(filename)) return
		this.seenFiles.add(filename)

		this.addDescriptorToRoot(descriptor)
	}

	private addDescriptorToRoot(descriptor: any): void {
		const pkg = descriptor.package || ''
		let namespace: protobuf.Namespace = this.root

		if (pkg) {
			for (const part of pkg.split('.')) {
				let next = namespace.get(part)
				if (!next) {
					next = new protobuf.Namespace(part)
					namespace.add(next)
				}
				namespace = next as protobuf.Namespace
			}
		}

		// Add enums
		for (const enumType of descriptor.enumType || []) {
			try {
				const values: Record<string, number> = {}
				for (const v of enumType.value || []) values[v.name] = v.number
				namespace.add(new protobuf.Enum(enumType.name, values))
			} catch { /* ignore duplicates */ }
		}

		// Add messages
		for (const msgType of descriptor.messageType || []) {
			try {
				this.addMessageType(namespace, msgType)
			} catch { /* ignore duplicates */ }
		}

		// Add services
		for (const svcType of descriptor.service || []) {
			try {
				const service = new protobuf.Service(svcType.name)
				for (const method of svcType.method || []) {
					service.add(new protobuf.Method(
						method.name,
						'rpc',
						method.inputType.replace(/^\./, ''),
						method.outputType.replace(/^\./, ''),
						method.clientStreaming || false,
						method.serverStreaming || false
					))
				}
				namespace.add(service)
			} catch { /* ignore duplicates */ }
		}
	}

	private addMessageType(namespace: protobuf.Namespace, msgType: any): void {
		const message = new protobuf.Type(msgType.name)

		for (const field of msgType.field || []) {
			const type = this.getFieldType(field)
			const rule = field.label === 3 ? 'repeated' : undefined
			message.add(new protobuf.Field(field.name, field.number, type, rule))
		}

		namespace.add(message)

		// Nested enums
		for (const nested of msgType.enumType || []) {
			const values: Record<string, number> = {}
			for (const v of nested.value || []) values[v.name] = v.number
			message.add(new protobuf.Enum(nested.name, values))
		}

		// Nested messages
		for (const nested of msgType.nestedType || []) {
			this.addMessageType(message, nested)
		}
	}

	private getFieldType(field: any): string {
		const typeMap: Record<number, string> = {
			1: 'double', 2: 'float', 3: 'int64', 4: 'uint64',
			5: 'int32', 6: 'fixed64', 7: 'fixed32', 8: 'bool',
			9: 'string', 12: 'bytes', 13: 'uint32', 15: 'sfixed32',
			16: 'sfixed64', 17: 'sint32', 18: 'sint64',
		}
		if (field.type in typeMap) return typeMap[field.type]
		if (field.typeName) return field.typeName.replace(/^\./, '')
		return 'string'
	}

	async invokeMethod<T>(serviceName: string, methodName: string, params: any = {}): Promise<T> {
		// Ensure service is loaded
		try {
			this.root.lookupService(serviceName)
		} catch {
			await this.loadService(serviceName)
		}

		const service = this.root.lookupService(serviceName)
		const method = service.methods[methodName]
		if (!method) throw new Error(`Method ${methodName} not found in ${serviceName}`)

		// Load request/response types if needed
		let requestType: protobuf.Type
		let responseType: protobuf.Type

		try {
			requestType = this.root.lookupType(method.requestType)
		} catch {
			await this.loadService(method.requestType)
			requestType = this.root.lookupType(method.requestType)
		}

		try {
			responseType = this.root.lookupType(method.responseType)
		} catch {
			await this.loadService(method.responseType)
			responseType = this.root.lookupType(method.responseType)
		}

		const methodPath = `/${serviceName}/${methodName}`
		const requestMessage = requestType.fromObject(params)
		const requestBuffer = Buffer.from(requestType.encode(requestMessage).finish())

		return new Promise((resolve, reject) => {
			const client = new grpc.Client(this.endpoint, this.credentials, {
				'grpc.max_receive_message_length': -1,
				'grpc.max_send_message_length': -1,
			})

			const deadline = new Date(Date.now() + 30000)

			client.makeUnaryRequest(
				methodPath,
				(buf: Buffer) => buf,
				(buf: Buffer) => buf,
				requestBuffer,
				{ deadline },
				async (error, response) => {
					client.close()

					if (error) {
						reject(new Error(`gRPC Error (${error.code}): ${error.message}`))
						return
					}

					if (!response) {
						reject(new Error('No response received'))
						return
					}

					try {
						const decoded = await this.decodeWithMissingTypes(responseType, response)
						resolve(decoded as T)
					} catch (err) {
						reject(err)
					}
				}
			)
		})
	}

	private async decodeWithMissingTypes(responseType: protobuf.Type, buffer: Buffer, depth = 0): Promise<any> {
		if (depth > 20) throw new Error('Max decode depth exceeded')

		try {
			const decoded = responseType.decode(new Uint8Array(buffer))
			return responseType.toObject(decoded, {
				longs: String,
				enums: String,
				bytes: String,
				defaults: true,
				arrays: true,
				objects: true,
				oneofs: true,
			})
		} catch (err: any) {
			const match = err.message?.match(/no such Type or Enum '([^']+)'/)
			if (match) {
				await this.loadService(match[1])
				return this.decodeWithMissingTypes(responseType, buffer, depth + 1)
			}
			throw err
		}
	}

	close(): void {
		if (this.reflectionStub) {
			try { this.reflectionStub.close() } catch { /* ignore */ }
		}
	}
}

// Global reflection client
let reflectionClient: ReflectionClient | null = null

async function getClient(): Promise<ReflectionClient> {
	if (!reflectionClient) {
		reflectionClient = new ReflectionClient(CHAIN_GRPC_ENDPOINT!, USE_TLS)
		await reflectionClient.initialize()
	}
	return reflectionClient
}

// Decode Tendermint client state to extract chain_id
function decodeClientStateChainId(clientState: Uint8Array | string): string | null {
	try {
		const bytes = typeof clientState === 'string'
			? Buffer.from(clientState, 'base64')
			: Buffer.from(clientState)

		let offset = 0
		while (offset < bytes.length) {
			const tag = bytes[offset] >> 3
			const wireType = bytes[offset] & 0x07
			offset++

			if (tag === 1 && wireType === 2) {
				const len = bytes[offset]
				offset++
				return bytes.subarray(offset, offset + len).toString('utf8')
			} else if (wireType === 0) {
				while (bytes[offset] & 0x80) offset++
				offset++
			} else if (wireType === 2) {
				const len = bytes[offset]
				offset += 1 + len
			} else {
				break
			}
		}
	} catch (err) {
		console.error('Failed to decode client state:', err)
	}
	return null
}

async function fetchStakingParams(pool: pg.Pool): Promise<void> {
	console.log('Fetching staking params...')

	try {
		const client = await getClient()
		const result = await client.invokeMethod<any>(
			'cosmos.staking.v1beta1.Query',
			'Params',
			{}
		)

		if (!result?.params) {
			console.log('No staking params returned')
			return
		}

		const params = result.params
		const bondDenom = params.bondDenom || params.bond_denom
		console.log(`Bond denom: ${bondDenom}`)

		// Update chain_params table
		await pool.query(`
			INSERT INTO api.chain_params (key, value, updated_at)
			VALUES
				('bond_denom', $1, NOW()),
				('unbonding_time', $2, NOW()),
				('max_validators', $3, NOW())
			ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
		`, [
			bondDenom,
			params.unbondingTime?.seconds?.toString() || params.unbonding_time || '0',
			(params.maxValidators || params.max_validators || 0).toString()
		])

		// Update denom_metadata with bond denom
		const symbol = bondDenom.startsWith('u')
			? bondDenom.slice(1).toUpperCase()
			: bondDenom.startsWith('a')
				? bondDenom.slice(1).toUpperCase()
				: bondDenom.toUpperCase()

		const decimals = bondDenom.startsWith('a') ? 18 : 6

		await pool.query(`
			INSERT INTO api.denom_metadata (denom, symbol, decimals, description, is_native)
			VALUES ($1, $2, $3, 'Native staking token', true)
			ON CONFLICT (denom) DO UPDATE SET
				symbol = EXCLUDED.symbol,
				decimals = EXCLUDED.decimals,
				is_native = true
		`, [bondDenom, symbol, decimals])

		console.log(`Updated bond denom: ${bondDenom} -> ${symbol}`)
	} catch (err) {
		console.error('Error fetching staking params:', err)
	}
}

async function fetchTotalSupply(pool: pg.Pool): Promise<void> {
	console.log('Fetching total supply...')

	try {
		const bondDenomResult = await pool.query(
			`SELECT value FROM api.chain_params WHERE key = 'bond_denom'`
		)

		if (bondDenomResult.rows.length === 0) {
			console.log('Bond denom not yet fetched, skipping supply')
			return
		}

		const bondDenom = bondDenomResult.rows[0].value
		const client = await getClient()

		const result = await client.invokeMethod<any>(
			'cosmos.bank.v1beta1.Query',
			'SupplyOf',
			{ denom: bondDenom }
		)

		if (!result?.amount) {
			console.log('No supply returned')
			return
		}

		await pool.query(`
			INSERT INTO api.chain_params (key, value, updated_at)
			VALUES ('total_supply', $1, NOW())
			ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
		`, [result.amount.amount])

		console.log(`Total supply: ${result.amount.amount} ${result.amount.denom}`)
	} catch (err) {
		console.error('Error fetching total supply:', err)
	}
}

async function fetchIBCDenomTraces(pool: pg.Pool): Promise<void> {
	console.log('Fetching IBC denom traces...')

	try {
		const client = await getClient()
		const result = await client.invokeMethod<any>(
			'ibc.applications.transfer.v1.Query',
			'DenomTraces',
			{ pagination: null }
		)

		const traces = result?.denomTraces || result?.denom_traces || []
		if (traces.length === 0) {
			console.log('No IBC denom traces returned')
			return
		}

		console.log(`Found ${traces.length} IBC denom traces`)

		for (const trace of traces) {
			const path = trace.path
			const baseDenom = trace.baseDenom || trace.base_denom

			// Compute IBC hash: SHA256(path/base_denom)
			const ibcPath = `${path}/${baseDenom}`
			const hash = createHash('sha256').update(ibcPath).digest('hex').toUpperCase()
			const ibcDenom = `ibc/${hash}`

			// Extract source channel from path
			const pathParts = path.split('/')
			const sourceChannel = pathParts.length >= 2 ? pathParts[1] : null

			// Determine symbol and decimals
			let symbol = baseDenom
			let decimals = 6

			if (baseDenom.startsWith('u')) {
				symbol = baseDenom.slice(1).toUpperCase()
				decimals = 6
			} else if (baseDenom.startsWith('a')) {
				symbol = baseDenom.slice(1).toUpperCase()
				decimals = 18
			} else {
				symbol = baseDenom.toUpperCase()
			}

			await pool.query(`
				INSERT INTO api.ibc_denom_traces (
					ibc_denom, base_denom, path, source_channel, symbol, decimals, updated_at
				) VALUES ($1, $2, $3, $4, $5, $6, NOW())
				ON CONFLICT (ibc_denom) DO UPDATE SET
					base_denom = EXCLUDED.base_denom,
					path = EXCLUDED.path,
					source_channel = EXCLUDED.source_channel,
					symbol = EXCLUDED.symbol,
					decimals = EXCLUDED.decimals,
					updated_at = NOW()
			`, [ibcDenom, baseDenom, path, sourceChannel, symbol, decimals])

			// Also update denom_metadata
			await pool.query(`
				INSERT INTO api.denom_metadata (denom, symbol, decimals, ibc_hash, is_native)
				VALUES ($1, $2, $3, $4, false)
				ON CONFLICT (denom) DO UPDATE SET
					symbol = EXCLUDED.symbol,
					decimals = EXCLUDED.decimals,
					ibc_hash = EXCLUDED.ibc_hash
			`, [ibcDenom, symbol, decimals, hash])
		}

		console.log(`Processed ${traces.length} IBC denom traces`)
	} catch (err) {
		console.error('Error fetching IBC denom traces:', err)
	}
}

async function fetchIBCChannels(pool: pg.Pool): Promise<void> {
	console.log('Fetching IBC channels...')

	try {
		const client = await getClient()
		const result = await client.invokeMethod<any>(
			'ibc.core.channel.v1.Query',
			'Channels',
			{ pagination: null }
		)

		const channels = result?.channels || []
		if (channels.length === 0) {
			console.log('No IBC channels returned')
			return
		}

		console.log(`Found ${channels.length} IBC channels`)

		for (const channel of channels) {
			const channelId = channel.channelId || channel.channel_id
			const portId = channel.portId || channel.port_id
			const connectionHops = channel.connectionHops || channel.connection_hops || []
			const connectionId = connectionHops[0] || null

			let counterpartyChainId: string | null = null
			let clientId: string | null = null
			let counterpartyClientId: string | null = null
			let counterpartyConnectionId: string | null = null
			let clientStatus: string | null = null

			if (connectionId) {
				// Get connection details
				try {
					// Try both snake_case and camelCase for the parameter
					const connResult = await client.invokeMethod<any>(
						'ibc.core.connection.v1.Query',
						'Connection',
						{ connection_id: connectionId }
					)
					if (connResult?.connection) {
						clientId = connResult.connection.clientId || connResult.connection.client_id
						counterpartyClientId = connResult.connection.counterparty?.clientId ||
							connResult.connection.counterparty?.client_id
						counterpartyConnectionId = connResult.connection.counterparty?.connectionId ||
							connResult.connection.counterparty?.connection_id
					}
				} catch (err) {
					console.error(`Error fetching connection ${connectionId}:`, (err as Error).message)
				}

				// Get client state for chain ID
				try {
					const clientStateResult = await client.invokeMethod<any>(
						'ibc.core.channel.v1.Query',
						'ChannelClientState',
						{ channel_id: channelId, port_id: portId }
					)
					const clientState = clientStateResult?.identifiedClientState?.clientState ||
						clientStateResult?.identified_client_state?.client_state

					if (clientState?.value) {
						counterpartyChainId = decodeClientStateChainId(clientState.value)
					}
				} catch (err) {
					console.error(`Error fetching client state for ${channelId}:`, (err as Error).message)
				}

				// Get client status
				if (clientId) {
					try {
						const statusResult = await client.invokeMethod<any>(
							'ibc.core.client.v1.Query',
							'ClientStatus',
							{ client_id: clientId }
						)
						clientStatus = statusResult?.status || null
					} catch (err) {
						console.error(`Error fetching client status for ${clientId}:`, (err as Error).message)
					}
				}
			}

			const counterparty = channel.counterparty || {}
			const state = channel.state?.toString() || 'STATE_UNINITIALIZED'
			const ordering = channel.ordering?.toString() || 'ORDER_UNORDERED'

			await pool.query(`
				INSERT INTO api.ibc_connections (
					channel_id, port_id, connection_id, client_id,
					counterparty_chain_id, counterparty_channel_id, counterparty_port_id,
					counterparty_client_id, counterparty_connection_id,
					state, ordering, version, client_status, updated_at
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW())
				ON CONFLICT (channel_id, port_id) DO UPDATE SET
					connection_id = EXCLUDED.connection_id,
					client_id = EXCLUDED.client_id,
					counterparty_chain_id = EXCLUDED.counterparty_chain_id,
					counterparty_channel_id = EXCLUDED.counterparty_channel_id,
					counterparty_port_id = EXCLUDED.counterparty_port_id,
					counterparty_client_id = EXCLUDED.counterparty_client_id,
					counterparty_connection_id = EXCLUDED.counterparty_connection_id,
					state = EXCLUDED.state,
					ordering = EXCLUDED.ordering,
					version = EXCLUDED.version,
					client_status = EXCLUDED.client_status,
					updated_at = NOW()
			`, [
				channelId,
				portId,
				connectionId,
				clientId,
				counterpartyChainId,
				counterparty.channelId || counterparty.channel_id || null,
				counterparty.portId || counterparty.port_id || null,
				counterpartyClientId,
				counterpartyConnectionId,
				state,
				ordering,
				channel.version || null,
				clientStatus
			])

			const isActive = state === 'STATE_OPEN' && clientStatus === 'Active'
			console.log(`Processed channel ${channelId} -> ${counterpartyChainId || 'unknown'} (${isActive ? 'active' : 'inactive'})`)
		}
	} catch (err) {
		console.error('Error fetching IBC channels:', err)
	}
}

// Resolve a single IBC denom using DenomTrace query
async function resolveIbcDenom(pool: pg.Pool, ibcDenom: string): Promise<boolean> {
	console.log(`Resolving IBC denom: ${ibcDenom}`)

	try {
		// Extract hash from ibc/HASH format
		const hash = ibcDenom.replace('ibc/', '')

		const client = await getClient()
		const result = await client.invokeMethod<any>(
			'ibc.applications.transfer.v1.Query',
			'DenomTrace',
			{ hash }
		)

		const trace = result?.denomTrace || result?.denom_trace
		if (!trace) {
			console.log(`No trace found for ${ibcDenom}`)
			return false
		}

		const path = trace.path
		const baseDenom = trace.baseDenom || trace.base_denom

		// Extract source channel from path
		const pathParts = path.split('/')
		const sourceChannel = pathParts.length >= 2 ? pathParts[1] : null

		// Look up source chain from ibc_connections
		let sourceChainId: string | null = null
		if (sourceChannel) {
			const connResult = await pool.query(
				`SELECT counterparty_chain_id FROM api.ibc_connections WHERE channel_id = $1 AND port_id = 'transfer'`,
				[sourceChannel]
			)
			if (connResult.rows.length > 0) {
				sourceChainId = connResult.rows[0].counterparty_chain_id
			}
		}

		// Determine symbol and decimals
		let symbol = baseDenom
		let decimals = 6

		if (baseDenom.startsWith('u')) {
			symbol = baseDenom.slice(1).toUpperCase()
			decimals = 6
		} else if (baseDenom.startsWith('a')) {
			symbol = baseDenom.slice(1).toUpperCase()
			decimals = 18
		} else {
			symbol = baseDenom.toUpperCase()
		}

		// Insert into ibc_denom_traces
		await pool.query(`
			INSERT INTO api.ibc_denom_traces (
				ibc_denom, base_denom, path, source_channel, source_chain_id, symbol, decimals, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
			ON CONFLICT (ibc_denom) DO UPDATE SET
				base_denom = EXCLUDED.base_denom,
				path = EXCLUDED.path,
				source_channel = EXCLUDED.source_channel,
				source_chain_id = COALESCE(EXCLUDED.source_chain_id, api.ibc_denom_traces.source_chain_id),
				symbol = EXCLUDED.symbol,
				decimals = EXCLUDED.decimals,
				updated_at = NOW()
		`, [ibcDenom, baseDenom, path, sourceChannel, sourceChainId, symbol, decimals])

		// Also update denom_metadata
		await pool.query(`
			INSERT INTO api.denom_metadata (denom, symbol, decimals, ibc_hash, is_native, ibc_source_chain, ibc_source_denom)
			VALUES ($1, $2, $3, $4, false, $5, $6)
			ON CONFLICT (denom) DO UPDATE SET
				symbol = EXCLUDED.symbol,
				decimals = EXCLUDED.decimals,
				ibc_hash = EXCLUDED.ibc_hash,
				ibc_source_chain = COALESCE(EXCLUDED.ibc_source_chain, api.denom_metadata.ibc_source_chain),
				ibc_source_denom = EXCLUDED.ibc_source_denom
		`, [ibcDenom, symbol, decimals, hash, sourceChainId, baseDenom])

		// Mark as resolved
		await pool.query(`DELETE FROM api.ibc_denom_pending WHERE ibc_denom = $1`, [ibcDenom])

		console.log(`Resolved ${ibcDenom} -> ${baseDenom} (${symbol}) from ${sourceChainId || 'unknown'}`)
		return true
	} catch (err) {
		console.error(`Error resolving ${ibcDenom}:`, err)
		// Mark as failed
		await pool.query(
			`UPDATE api.ibc_denom_pending SET attempts = attempts + 1, last_attempt = NOW(), error = $2 WHERE ibc_denom = $1`,
			[ibcDenom, (err as Error).message]
		)
		return false
	}
}

// Process all pending IBC denoms
async function processPendingDenoms(pool: pg.Pool): Promise<void> {
	const result = await pool.query(`
		SELECT ibc_denom FROM api.ibc_denom_pending
		WHERE attempts < 5
		AND (last_attempt IS NULL OR last_attempt < NOW() - INTERVAL '1 minute')
		ORDER BY created_at
		LIMIT 50
	`)

	if (result.rows.length === 0) return

	console.log(`Processing ${result.rows.length} pending IBC denoms...`)

	for (const row of result.rows) {
		await resolveIbcDenom(pool, row.ibc_denom)
	}
}

async function runDaemon(): Promise<void> {
	console.log('Starting Chain Params Daemon (gRPC Reflection)...')
	console.log(`Database: ${DATABASE_URL!.replace(/:[^:@]+@/, ':***@')}`)
	console.log(`Chain gRPC endpoint: ${CHAIN_GRPC_ENDPOINT}`)
	console.log(`TLS: ${USE_TLS}`)
	console.log(`Poll interval: ${POLL_INTERVAL_MS}ms`)

	const pool = new Pool({ connectionString: DATABASE_URL })

	// Set up LISTEN for new pending IBC denoms
	const listenClient = await pool.connect()
	await listenClient.query('LISTEN ibc_denom_pending')
	listenClient.on('notification', async (msg) => {
		if (msg.channel === 'ibc_denom_pending' && msg.payload) {
			console.log(`Received notification for IBC denom: ${msg.payload}`)
			// Small delay to allow transaction to commit
			setTimeout(() => resolveIbcDenom(pool, msg.payload!), 100)
		}
	})
	console.log('Listening for ibc_denom_pending notifications...')

	// Initial fetch
	await fetchStakingParams(pool)
	await fetchTotalSupply(pool)
	await fetchIBCDenomTraces(pool)
	await fetchIBCChannels(pool)

	// Process any existing pending denoms
	await processPendingDenoms(pool)

	// Poll loop
	setInterval(async () => {
		try {
			await fetchStakingParams(pool)
			await fetchTotalSupply(pool)
			await fetchIBCDenomTraces(pool)
			await fetchIBCChannels(pool)
			await processPendingDenoms(pool)
		} catch (err) {
			console.error('Poll cycle error:', err)
		}
	}, POLL_INTERVAL_MS)
}

runDaemon().catch(err => {
	console.error('Fatal error:', err)
	process.exit(1)
})
