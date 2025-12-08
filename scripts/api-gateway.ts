#!/usr/bin/env npx tsx
/**
 * API Gateway
 *
 * Routes requests between PostgREST and chain query service.
 * - /chain/* -> Chain Query Service (gRPC queries for balances, supply, etc.)
 * - /* -> PostgREST
 */

import * as http from 'http'
import { spawn, ChildProcess } from 'child_process'

const GATEWAY_PORT = parseInt(process.env.GATEWAY_PORT || '3000', 10)
const POSTGREST_PORT = 3001
const CHAIN_QUERY_PORT = 3002

let postgrestProcess: ChildProcess | null = null
let chainQueryProcess: ChildProcess | null = null

// Start PostgREST
function startPostgREST() {
	console.log('[Gateway] Starting PostgREST on port', POSTGREST_PORT)
	postgrestProcess = spawn('/usr/local/bin/postgrest', [], {
		env: {
			...process.env,
			PGRST_SERVER_PORT: String(POSTGREST_PORT),
		},
		stdio: 'inherit',
	})

	postgrestProcess.on('exit', (code) => {
		console.log(`[Gateway] PostgREST exited with code ${code}`)
		process.exit(code || 1)
	})
}

// Start Chain Query Service
function startChainQueryService() {
	console.log('[Gateway] Starting Chain Query Service on port', CHAIN_QUERY_PORT)
	chainQueryProcess = spawn('npx', ['tsx', 'scripts/chain-query-service.ts'], {
		env: {
			...process.env,
			CHAIN_QUERY_PORT: String(CHAIN_QUERY_PORT),
		},
		stdio: 'inherit',
	})

	chainQueryProcess.on('exit', (code) => {
		console.log(`[Gateway] Chain Query Service exited with code ${code}`)
	})
}

// Proxy request to target port
function proxyRequest(
	req: http.IncomingMessage,
	res: http.ServerResponse,
	targetPort: number
) {
	const options: http.RequestOptions = {
		hostname: 'localhost',
		port: targetPort,
		path: req.url,
		method: req.method,
		headers: req.headers,
	}

	const proxyReq = http.request(options, (proxyRes) => {
		res.writeHead(proxyRes.statusCode || 500, proxyRes.headers)
		proxyRes.pipe(res)
	})

	proxyReq.on('error', (err) => {
		console.error(`[Gateway] Proxy error to port ${targetPort}:`, err.message)
		res.writeHead(502)
		res.end(JSON.stringify({ error: 'Bad Gateway', message: err.message }))
	})

	req.pipe(proxyReq)
}

// Gateway server
const server = http.createServer((req, res) => {
	const url = req.url || '/'

	// Route /chain/* to chain query service
	if (url.startsWith('/chain')) {
		proxyRequest(req, res, CHAIN_QUERY_PORT)
		return
	}

	// Everything else to PostgREST
	proxyRequest(req, res, POSTGREST_PORT)
})

// Graceful shutdown
function shutdown() {
	console.log('[Gateway] Shutting down...')
	if (postgrestProcess) postgrestProcess.kill()
	if (chainQueryProcess) chainQueryProcess.kill()
	process.exit(0)
}

process.on('SIGTERM', shutdown)
process.on('SIGINT', shutdown)

// Start services and gateway
console.log('[Gateway] Starting API Gateway')
startPostgREST()
startChainQueryService()

// Wait a bit for services to start
setTimeout(() => {
	server.listen(GATEWAY_PORT, () => {
		console.log(`[Gateway] Listening on port ${GATEWAY_PORT}`)
		console.log(`[Gateway] Routes:`)
		console.log(`  /chain/* -> Chain Query Service (port ${CHAIN_QUERY_PORT})`)
		console.log(`  /* -> PostgREST (port ${POSTGREST_PORT})`)
	})
}, 2000)
