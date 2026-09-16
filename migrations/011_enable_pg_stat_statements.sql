BEGIN;

CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

CREATE OR REPLACE VIEW api.query_stats AS
SELECT
  LEFT(query, 100) AS query,
  calls,
  total_exec_time,
  mean_exec_time,
  rows
FROM pg_stat_statements
WHERE query LIKE '%api.%'
ORDER BY mean_exec_time DESC;

GRANT SELECT ON api.query_stats TO web_anon;

COMMIT;
