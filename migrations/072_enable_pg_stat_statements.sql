BEGIN;

-- Migration 072: Enable pg_stat_statements for query performance monitoring
--
-- pg_stat_statements tracks execution statistics for all SQL statements.
-- Requires shared_preload_libraries to include 'pg_stat_statements' (standard
-- on Fly.io managed Postgres and most managed providers).
--
-- If the extension is not available in shared_preload_libraries, this CREATE
-- will fail and the migration will roll back harmlessly.
--
-- Usage after enabling:
--   SELECT query, calls, mean_exec_time, total_exec_time
--   FROM pg_stat_statements
--   ORDER BY total_exec_time DESC
--   LIMIT 20;
--
-- Reset stats periodically:
--   SELECT pg_stat_statements_reset();

CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

COMMIT;
