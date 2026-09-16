BEGIN;

-- Migration 067: Per-table autovacuum tuning and fillfactor
--
-- Default autovacuum_vacuum_scale_factor is 0.2 (20% dead tuples before vacuum).
-- For high-churn tables this is too lax, causing bloat and degraded scan performance.
-- Lowering to 0.05 (5%) with analyze at 0.02 (2%) keeps these tables healthy.
--
-- Fillfactor < 100 reserves page space for HOT (Heap-Only Tuple) updates,
-- avoiding index churn on frequently-UPDATEd rows.

-- ============================================================================
-- Autovacuum tuning for high-write-throughput tables
-- ============================================================================

ALTER TABLE api.transactions_main SET (
  autovacuum_vacuum_scale_factor = 0.05,
  autovacuum_analyze_scale_factor = 0.02
);

ALTER TABLE api.messages_main SET (
  autovacuum_vacuum_scale_factor = 0.05,
  autovacuum_analyze_scale_factor = 0.02
);

ALTER TABLE api.events_main SET (
  autovacuum_vacuum_scale_factor = 0.05,
  autovacuum_analyze_scale_factor = 0.02
);

-- 23.4M rows, 3.6M dead (15%)
ALTER TABLE api.validator_block_signatures SET (
  autovacuum_vacuum_scale_factor = 0.05,
  autovacuum_analyze_scale_factor = 0.02
);

-- 592 live, 7,784 dead (1,315% dead ratio)
-- fillfactor 70: constant UPDATEs from validator refresh daemon
ALTER TABLE api.validators SET (
  autovacuum_vacuum_scale_factor = 0.05,
  autovacuum_analyze_scale_factor = 0.02,
  fillfactor = 70
);

-- 35 live, 5,410 dead (15,457% dead ratio)
-- fillfactor 70: frequent upserts per block
ALTER TABLE api.rt_hourly_rewards SET (
  autovacuum_vacuum_scale_factor = 0.05,
  autovacuum_analyze_scale_factor = 0.02,
  fillfactor = 70
);

-- fillfactor 70: frequent upserts per block
ALTER TABLE api.rt_daily_rewards SET (
  autovacuum_vacuum_scale_factor = 0.05,
  autovacuum_analyze_scale_factor = 0.02,
  fillfactor = 70
);

-- ============================================================================
-- Fillfactor-only tables (not high-write, but constant UPDATEs)
-- ============================================================================

-- rt_chain_stats: single-row table, constant UPDATEs
ALTER TABLE api.rt_chain_stats SET (fillfactor = 50);

COMMIT;
