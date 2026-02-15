/**
 * Multi-chain configuration loader
 *
 * Reads chains.toml at startup to build a map of chain ID -> gRPC config.
 * Falls back to env vars (CHAIN_GRPC_ENDPOINT, YACI_INSECURE, BECH32_CONS_PREFIX)
 * when chains.toml is not found, preserving backward compatibility.
 */

import { readFileSync } from 'fs'
import { resolve } from 'path'
import { parse } from 'smol-toml'

export interface ChainConfig {
	grpcEndpoint: string
	tls: boolean
	bech32ConsPrefix: string
}

export interface ChainsConfig {
	defaultChain: string
	chains: Record<string, ChainConfig>
}

let cached: ChainsConfig | null = null

/**
 * Load chain configurations from chains.toml.
 * Falls back to env vars if the file is missing.
 * @param configPath - Override path to chains.toml (defaults to project root)
 */
export function loadChainsConfig(configPath?: string): ChainsConfig {
	if (cached) return cached

	const filePath = configPath || resolve(process.cwd(), 'chains.toml')

	try {
		const raw = readFileSync(filePath, 'utf-8')
		const doc = parse(raw) as Record<string, unknown>

		const defaultChain = doc.default_chain as string | undefined
		const chainsRaw = doc.chains as Record<string, Record<string, unknown>> | undefined

		if (!chainsRaw || Object.keys(chainsRaw).length === 0) {
			throw new Error('chains.toml has no [chains.*] entries')
		}

		const chains: Record<string, ChainConfig> = {}
		for (const [id, entry] of Object.entries(chainsRaw)) {
			if (!entry.grpc_endpoint) {
				throw new Error(`Chain "${id}" is missing required field "grpc_endpoint"`)
			}
			if (!entry.bech32_cons_prefix) {
				throw new Error(`Chain "${id}" is missing required field "bech32_cons_prefix"`)
			}
			chains[id] = {
				grpcEndpoint: entry.grpc_endpoint as string,
				tls: entry.tls !== false,
				bech32ConsPrefix: entry.bech32_cons_prefix as string,
			}
		}

		const resolvedDefault = defaultChain || Object.keys(chains)[0]
		if (!chains[resolvedDefault]) {
			throw new Error(
				`default_chain "${resolvedDefault}" not found in configured chains: ${Object.keys(chains).join(', ')}`
			)
		}

		cached = { defaultChain: resolvedDefault, chains }
		return cached
	} catch (err: any) {
		// File not found -- fall back to env vars for single-chain backward compat
		if (err.code === 'ENOENT') {
			const endpoint = process.env.CHAIN_GRPC_ENDPOINT || 'localhost:9090'
			const insecure = process.env.YACI_INSECURE === 'true'
			const bech32 = process.env.BECH32_CONS_PREFIX || 'raivalcons'

			console.warn(`[ChainConfig] chains.toml not found, falling back to env vars (endpoint: ${endpoint})`)

			cached = {
				defaultChain: 'default',
				chains: {
					default: {
						grpcEndpoint: endpoint,
						tls: !insecure,
						bech32ConsPrefix: bech32,
					},
				},
			}
			return cached
		}
		throw err
	}
}

/**
 * Get config for a specific chain, or throw with available chain IDs.
 * @param chainId - The chain identifier
 */
export function getChainConfig(chainId: string): ChainConfig {
	const config = loadChainsConfig()
	const chain = config.chains[chainId]
	if (!chain) {
		const available = Object.keys(config.chains).join(', ')
		throw new Error(`Unknown chain "${chainId}". Available chains: ${available}`)
	}
	return chain
}

/**
 * Get the set of all configured chain IDs.
 */
export function getChainIds(): Set<string> {
	return new Set(Object.keys(loadChainsConfig().chains))
}

/**
 * Get the default chain ID.
 */
export function getDefaultChainId(): string {
	return loadChainsConfig().defaultChain
}

/**
 * Reset cached config (for testing).
 */
export function resetConfigCache(): void {
	cached = null
}
