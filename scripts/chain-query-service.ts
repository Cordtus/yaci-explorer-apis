#!/usr/bin/env npx tsx
/**
 * Chain Query Service
 *
 * HTTP proxy for direct chain gRPC queries.
 * Extensible design to add more query types as needed.
 *
 * Current endpoints:
 * - GET /chain/balances/:address - Get account balances
 * - GET /chain/health - Health check
 *
 * Future endpoints can include:
 * - /chain/staking/delegations/:address
 * - /chain/gov/proposals
 * - /chain/supply/:denom
 * etc.
 */

import * as http from 'http'
import * as grpc from '@grpc/grpc-js'
import protobuf from 'protobufjs'

const GRPC_ENDPOINT = process.env.CHAIN_GRPC_ENDPOINT || 'localhost:9090'
const PORT = parseInt(process.env.CHAIN_QUERY_PORT || '3001', 10)
// Default to TLS unless explicitly set to insecure
const INSECURE = process.env.YACI_INSECURE === 'true'

// Proto definitions for chain queries
const PROTOS = {
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
}

// Generic gRPC query client
class ChainQueryClient {
	private endpoint: string
	private credentials: grpc.ChannelCredentials
	private roots: Map<string, protobuf.Root> = new Map()
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

	// Create a stub for a specific service
	private getStub(service: string, methods: Record<string, { path: string; requestType: string; responseType: string }>): any {
		const cacheKey = service
		if (this.stubs.has(cacheKey)) {
			return this.stubs.get(cacheKey)
		}

		// Extract module name from service (e.g., "cosmos.bank.v1beta1.Query" -> "bank")
		const parts = service.split('.')
		const moduleName = parts.length >= 2 ? parts[1] : parts[0]
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

	// Bank queries
	async getAllBalances(address: string): Promise<any> {
		const stub = this.getStub('cosmos.bank.v1beta1.Query', {
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
		const stub = this.getStub('cosmos.bank.v1beta1.Query', {
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
		const stub = this.getStub('cosmos.bank.v1beta1.Query', {
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
}

// Initialize client
let client: ChainQueryClient

function initClient() {
	client = new ChainQueryClient(GRPC_ENDPOINT, INSECURE)
}

// Route handlers
type RouteHandler = (params: Record<string, string>, query: URLSearchParams) => Promise<any>

const routes: Array<{ pattern: RegExp; handler: RouteHandler }> = [
	// GET /chain/balances/:address
	{
		pattern: /^\/chain\/balances\/([a-zA-Z0-9]+)$/,
		handler: async (params) => client.getAllBalances(params.address),
	},
	// GET /chain/spendable/:address
	{
		pattern: /^\/chain\/spendable\/([a-zA-Z0-9]+)$/,
		handler: async (params) => client.getSpendableBalances(params.address),
	},
	// GET /chain/supply/:denom
	{
		pattern: /^\/chain\/supply\/([a-zA-Z0-9]+)$/,
		handler: async (params) => client.getSupplyOf(params.denom),
	},
]

// HTTP server
const server = http.createServer(async (req, res) => {
	// CORS headers
	res.setHeader('Access-Control-Allow-Origin', '*')
	res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS')
	res.setHeader('Access-Control-Allow-Headers', 'Content-Type')
	res.setHeader('Content-Type', 'application/json')

	if (req.method === 'OPTIONS') {
		res.writeHead(204)
		res.end()
		return
	}

	const url = new URL(req.url || '/', `http://localhost:${PORT}`)

	// Health check
	if (url.pathname === '/chain/health') {
		res.writeHead(200)
		res.end(JSON.stringify({ status: 'ok', endpoint: GRPC_ENDPOINT }))
		return
	}

	// Match routes
	for (const route of routes) {
		const match = url.pathname.match(route.pattern)
		if (match && req.method === 'GET') {
			const params: Record<string, string> = {}

			// Extract named params based on pattern groups
			if (url.pathname.includes('/balances/')) params.address = match[1]
			else if (url.pathname.includes('/spendable/')) params.address = match[1]
			else if (url.pathname.includes('/supply/')) params.denom = match[1]

			try {
				const result = await route.handler(params, url.searchParams)
				res.writeHead(200)
				res.end(JSON.stringify(result))
			} catch (err: any) {
				console.error(`[ChainQuery] Error:`, err.message)
				res.writeHead(500)
				res.end(JSON.stringify({ error: err.message }))
			}
			return
		}
	}

	// 404
	res.writeHead(404)
	res.end(JSON.stringify({ error: 'Not found', endpoints: [
		'GET /chain/health',
		'GET /chain/balances/:address',
		'GET /chain/spendable/:address',
		'GET /chain/supply/:denom',
	]}))
})

// Start server
initClient()
server.listen(PORT, () => {
	console.log(`[ChainQuery] Running on port ${PORT}`)
	console.log(`[ChainQuery] gRPC endpoint: ${GRPC_ENDPOINT}`)
	console.log(`[ChainQuery] Endpoints:`)
	console.log(`  GET /chain/health`)
	console.log(`  GET /chain/balances/:address`)
	console.log(`  GET /chain/spendable/:address`)
	console.log(`  GET /chain/supply/:denom`)
})
