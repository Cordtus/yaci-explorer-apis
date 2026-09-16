#!/usr/bin/env npx tsx
/**
 * API Gateway
 *
 * Routes requests between PostgREST and chain query service.
 * Includes in-memory response cache with per-route TTL.
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

// ============================================================================
// Response Cache
// ============================================================================

interface CacheEntry {
	statusCode: number
	headers: http.OutgoingHttpHeaders
	body: Buffer
	expiresAt: number
}

const cache = new Map<string, CacheEntry>()
let cacheHits = 0
let cacheMisses = 0

/**
 * Cache tier definitions.
 * Each entry has a gateway TTL (in-memory) and an HTTP Cache-Control max-age
 * that tells browsers/CDNs how long to hold the response.
 */
interface CacheTier {
	gatewayTtl: number
	maxAge: number
	staleWhileRevalidate: number
}

const CACHE_TIERS: Record<string, CacheTier> = {
	// Trigger-updated endpoints - data is near-real-time at DB level
	'/rpc/get_network_overview': { gatewayTtl: 10_000, maxAge: 5, staleWhileRevalidate: 10 },
	'/rpc/get_hourly_rewards': { gatewayTtl: 30_000, maxAge: 15, staleWhileRevalidate: 30 },
	'/rpc/get_validators_paginated': { gatewayTtl: 10_000, maxAge: 5, staleWhileRevalidate: 10 },
	'/rpc/get_validator_performance': { gatewayTtl: 30_000, maxAge: 15, staleWhileRevalidate: 30 },
	'/rpc/get_validator_events_summary': { gatewayTtl: 30_000, maxAge: 15, staleWhileRevalidate: 30 },
	// Remaining materialized views (still on 15-min refresh)
	'/mv_': { gatewayTtl: 60_000, maxAge: 30, staleWhileRevalidate: 60 },
	// Trigger-updated chain stats + validator tables
	'/chain_stats': { gatewayTtl: 10_000, maxAge: 5, staleWhileRevalidate: 10 },
	'/validator_stats': { gatewayTtl: 10_000, maxAge: 5, staleWhileRevalidate: 10 },
	'/validators': { gatewayTtl: 10_000, maxAge: 5, staleWhileRevalidate: 10 },
	// Chain gRPC proxy - live data, short cache
	'/chain/': { gatewayTtl: 6_000, maxAge: 3, staleWhileRevalidate: 6 },
	// Default for other PostgREST endpoints (blocks, transactions, etc.)
	default: { gatewayTtl: 10_000, maxAge: 5, staleWhileRevalidate: 10 },
}

/** Routes that should never be cached */
function shouldSkipCache(method: string, url: string): boolean {
	if (method !== 'GET') return true
	if (url.startsWith('/chain/tx/')) return true
	if (url === '/cache/stats') return true
	return false
}

/** Get cache tier for a given URL */
function getCacheTier(url: string): CacheTier {
	for (const [prefix, tier] of Object.entries(CACHE_TIERS)) {
		if (prefix !== 'default' && url.startsWith(prefix)) return tier
	}
	return CACHE_TIERS.default
}

/** Build cache key from method + url */
function cacheKey(method: string, url: string): string {
	return `${method}:${url}`
}

/** Evict expired entries */
function cleanupCache() {
	const now = Date.now()
	for (const [key, entry] of cache) {
		if (entry.expiresAt <= now) cache.delete(key)
	}
}

// Run cleanup every 60s
const cleanupInterval = setInterval(cleanupCache, 60_000)
cleanupInterval.unref()

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

/**
 * Proxy request to target port, buffering response for cache storage.
 * On cache hit, returns cached response immediately.
 */
function proxyRequest(
	req: http.IncomingMessage,
	res: http.ServerResponse,
	targetPort: number
) {
	const method = req.method || 'GET'
	const url = req.url || '/'
	const key = cacheKey(method, url)
	const skipCache = shouldSkipCache(method, url)

	const tier = getCacheTier(url)

	// Check cache
	if (!skipCache) {
		const cached = cache.get(key)
		if (cached && cached.expiresAt > Date.now()) {
			cacheHits++
			res.setHeader('X-Cache', 'HIT')
			res.writeHead(cached.statusCode, cached.headers)
			res.end(cached.body)
			return
		}
	}

	cacheMisses++

	const options: http.RequestOptions = {
		hostname: 'localhost',
		port: targetPort,
		path: req.url,
		method: req.method,
		headers: req.headers,
	}

	const proxyReq = http.request(options, (proxyRes) => {
		if (skipCache) {
			// Stream directly without buffering
			res.setHeader('X-Cache', 'BYPASS')
			res.writeHead(proxyRes.statusCode || 500, proxyRes.headers)
			proxyRes.pipe(res)
			return
		}

		// Buffer response for caching
		const chunks: Buffer[] = []
		proxyRes.on('data', (chunk: Buffer) => chunks.push(chunk))
		proxyRes.on('end', () => {
			const body = Buffer.concat(chunks)
			const statusCode = proxyRes.statusCode || 500

			// Only cache successful responses
			if (statusCode >= 200 && statusCode < 400) {
				const headers: http.OutgoingHttpHeaders = {
					...proxyRes.headers,
					'cache-control': `public, max-age=${tier.maxAge}, stale-while-revalidate=${tier.staleWhileRevalidate}`,
				}
				cache.set(key, {
					statusCode,
					headers,
					body,
					expiresAt: Date.now() + tier.gatewayTtl,
				})
			}

			res.writeHead(statusCode, {
				...proxyRes.headers,
				'x-cache': 'MISS',
				'cache-control': `public, max-age=${tier.maxAge}, stale-while-revalidate=${tier.staleWhileRevalidate}`,
			})
			res.end(body)
		})
	})

	proxyReq.on('error', (err) => {
		console.error(`[Gateway] Proxy error to port ${targetPort}:`, err.message)
		res.writeHead(502, { 'Content-Type': 'application/json' })
		res.end(JSON.stringify({ error: 'Bad Gateway', message: err.message }))
	})

	req.pipe(proxyReq)
}

// Add CORS headers to response
function setCorsHeaders(res: http.ServerResponse) {
	res.setHeader('Access-Control-Allow-Origin', '*')
	res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PATCH, DELETE')
	res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, Prefer, Range, Range-Unit')
	res.setHeader('Access-Control-Expose-Headers', 'Content-Range, Range-Unit, X-Cache, Cache-Control')
}

// Gateway server
const server = http.createServer((req, res) => {
	const url = req.url || '/'

	// Handle CORS preflight
	if (req.method === 'OPTIONS') {
		setCorsHeaders(res)
		res.writeHead(204)
		res.end()
		return
	}

	// Add CORS headers to all responses
	setCorsHeaders(res)

	// Cache stats endpoint
	if (url === '/cache/stats') {
		res.writeHead(200, { 'Content-Type': 'application/json' })
		res.end(JSON.stringify({
			entries: cache.size,
			hits: cacheHits,
			misses: cacheMisses,
			hit_rate: cacheHits + cacheMisses > 0
				? ((cacheHits / (cacheHits + cacheMisses)) * 100).toFixed(1) + '%'
				: '0%',
		}))
		return
	}

	// Route /chain/* to chain query service (but not /chain_stats, etc.)
	if (url.startsWith('/chain/') || url === '/chain') {
		proxyRequest(req, res, CHAIN_QUERY_PORT)
		return
	}

	// Everything else to PostgREST
	proxyRequest(req, res, POSTGREST_PORT)
})

// Graceful shutdown
function shutdown() {
	console.log('[Gateway] Shutting down...')
	clearInterval(cleanupInterval)
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
		console.log(`[Gateway] Cache enabled with tiered TTLs and Cache-Control headers`)
	})
}, 2000)
