/**
 * Governance Proposal Polling Worker
 *
 * Polls chain API for active governance proposals and updates database
 * - Detects proposals via database trigger on MsgSubmitProposal
 * - Enriches proposal data from chain API
 * - Polls for vote tallies and status updates
 * - Creates periodic snapshots
 * - Stops polling when proposals end
 */

import pg from 'pg'

const DATABASE_URL = process.env.DATABASE_URL
const CHAIN_REST_ENDPOINT = process.env.CHAIN_REST_ENDPOINT || 'http://localhost:1317'
const POLL_INTERVAL_MS = parseInt(process.env.GOV_POLL_INTERVAL_MS || '60000') // 1 minute default
const SNAPSHOT_INTERVAL_MS = parseInt(process.env.GOV_SNAPSHOT_INTERVAL_MS || '300000') // 5 minutes default

interface ChainProposal {
  id: string
  messages: any[]
  status: string
  final_tally_result: {
    yes_count: string
    abstain_count: string
    no_count: string
    no_with_veto_count: string
  }
  submit_time: string
  deposit_end_time: string
  total_deposit: Array<{ denom: string; amount: string }>
  voting_start_time: string
  voting_end_time: string
  metadata: string
  title: string
  summary: string
  proposer: string
}

interface ActiveProposal {
  proposal_id: number
  status: string
  voting_end_time: string | null
  deposit_end_time: string | null
}

class GovernancePoller {
  private pool: pg.Pool
  private lastSnapshotTime = new Map<number, number>()

  constructor() {
    if (!DATABASE_URL) {
      throw new Error('DATABASE_URL environment variable is required')
    }
    this.pool = new pg.Pool({ connectionString: DATABASE_URL })
  }

  async checkTablesExist(): Promise<boolean> {
    try {
      const result = await this.pool.query(`
        SELECT EXISTS (
          SELECT 1 FROM information_schema.tables
          WHERE table_schema = 'api' AND table_name = 'governance_proposals'
        ) AS exists
      `)
      return result.rows[0]?.exists === true
    } catch {
      return false
    }
  }

  async pollActiveProposals() {
    console.log('[Governance Poller] Starting poll cycle...')

    // Check if governance tables exist
    const tablesExist = await this.checkTablesExist()
    if (!tablesExist) {
      console.log('[Governance Poller] Governance tables not yet created, skipping cycle')
      return
    }

    const result = await this.pool.query<ActiveProposal>(
      'SELECT * FROM api.governance_active_proposals'
    )

    console.log(`[Governance Poller] Found ${result.rows.length} active proposals`)

    for (const proposal of result.rows) {
      try {
        await this.updateProposal(proposal)
      } catch (error) {
        console.error(
          `[Governance Poller] Error updating proposal ${proposal.proposal_id}:`,
          error
        )
      }
    }

    console.log('[Governance Poller] Poll cycle complete\n')
  }

  async updateProposal(activeProposal: ActiveProposal) {
    const { proposal_id } = activeProposal

    // Fetch proposal details from chain
    const chainProposal = await this.fetchChainProposal(proposal_id)
    if (!chainProposal) {
      console.log(`[Proposal ${proposal_id}] Not found on chain, skipping`)
      return
    }

    // Update proposal details
    await this.updateProposalDetails(proposal_id, chainProposal)

    // Check if we should create a snapshot
    const now = Date.now()
    const lastSnapshot = this.lastSnapshotTime.get(proposal_id) || 0

    if (now - lastSnapshot >= SNAPSHOT_INTERVAL_MS) {
      await this.createSnapshot(proposal_id, chainProposal)
      this.lastSnapshotTime.set(proposal_id, now)
    }

    // Check if proposal has ended
    await this.checkProposalEnd(proposal_id, chainProposal)
  }

  async fetchChainProposal(proposalId: number): Promise<ChainProposal | null> {
    try {
      const url = `${CHAIN_REST_ENDPOINT}/cosmos/gov/v1/proposals/${proposalId}`
      const response = await fetch(url)

      if (!response.ok) {
        if (response.status === 404) {
          return null
        }
        throw new Error(`HTTP ${response.status}: ${response.statusText}`)
      }

      const data = await response.json()
      return data.proposal
    } catch (error) {
      console.error(`[Proposal ${proposalId}] Error fetching from chain:`, error)
      return null
    }
  }

  async updateProposalDetails(proposalId: number, chainProposal: ChainProposal) {
    const { status, final_tally_result, title, summary, metadata } = chainProposal

    await this.pool.query(
      `UPDATE api.governance_proposals
       SET
         title = $1,
         summary = $2,
         metadata = $3,
         status = $4,
         deposit_end_time = $5,
         voting_start_time = $6,
         voting_end_time = $7,
         yes_count = $8,
         no_count = $9,
         abstain_count = $10,
         no_with_veto_count = $11,
         last_updated = NOW()
       WHERE proposal_id = $12`,
      [
        title,
        summary,
        metadata,
        status,
        chainProposal.deposit_end_time,
        chainProposal.voting_start_time,
        chainProposal.voting_end_time,
        final_tally_result.yes_count,
        final_tally_result.no_count,
        final_tally_result.abstain_count,
        final_tally_result.no_with_veto_count,
        proposalId,
      ]
    )

    console.log(`[Proposal ${proposalId}] Updated details (status: ${status})`)
  }

  async createSnapshot(proposalId: number, chainProposal: ChainProposal) {
    const { status, final_tally_result } = chainProposal

    await this.pool.query(
      `INSERT INTO api.governance_snapshots
       (proposal_id, status, yes_count, no_count, abstain_count, no_with_veto_count)
       VALUES ($1, $2, $3, $4, $5, $6)
       ON CONFLICT (proposal_id, snapshot_time) DO NOTHING`,
      [
        proposalId,
        status,
        final_tally_result.yes_count,
        final_tally_result.no_count,
        final_tally_result.abstain_count,
        final_tally_result.no_with_veto_count,
      ]
    )

    console.log(
      `[Proposal ${proposalId}] Created snapshot (yes: ${final_tally_result.yes_count}, no: ${final_tally_result.no_count})`
    )
  }

  async checkProposalEnd(proposalId: number, chainProposal: ChainProposal) {
    const { status } = chainProposal

    // Check if proposal has moved to a final state
    const finalStatuses = [
      'PROPOSAL_STATUS_PASSED',
      'PROPOSAL_STATUS_REJECTED',
      'PROPOSAL_STATUS_FAILED',
      'PROPOSAL_STATUS_REMOVED',
    ]

    if (finalStatuses.includes(status)) {
      console.log(`[Proposal ${proposalId}] Ended with status: ${status}`)

      // Create final snapshot
      await this.createSnapshot(proposalId, chainProposal)

      // Cleanup
      this.lastSnapshotTime.delete(proposalId)
    }
  }

  async start() {
    console.log('[Governance Poller] Starting governance polling worker')
    console.log(`[Governance Poller] Chain REST endpoint: ${CHAIN_REST_ENDPOINT}`)
    console.log(`[Governance Poller] Poll interval: ${POLL_INTERVAL_MS}ms`)
    console.log(`[Governance Poller] Snapshot interval: ${SNAPSHOT_INTERVAL_MS}ms\n`)

    // Initial poll
    await this.pollActiveProposals()

    // Set up recurring poll
    setInterval(() => {
      this.pollActiveProposals().catch((error) => {
        console.error('[Governance Poller] Error in poll cycle:', error)
      })
    }, POLL_INTERVAL_MS)
  }

  async stop() {
    console.log('[Governance Poller] Stopping...')
    await this.pool.end()
  }
}

// ESM entry point check
const isMainModule = import.meta.url === `file://${process.argv[1]}`

if (isMainModule) {
  const poller = new GovernancePoller()

  process.on('SIGINT', async () => {
    console.log('\n[Governance Poller] Received SIGINT')
    await poller.stop()
    process.exit(0)
  })

  process.on('SIGTERM', async () => {
    console.log('\n[Governance Poller] Received SIGTERM')
    await poller.stop()
    process.exit(0)
  })

  poller.start().catch((error) => {
    console.error('[Governance Poller] Fatal error:', error)
    process.exit(1)
  })
}

export default GovernancePoller
