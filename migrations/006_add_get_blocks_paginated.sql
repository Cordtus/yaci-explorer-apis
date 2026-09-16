-- Add paginated blocks endpoint with filtering
-- Supports filtering by transaction count and date ranges

BEGIN;

CREATE OR REPLACE FUNCTION api.get_blocks_paginated(
  _limit int DEFAULT 20,
  _offset int DEFAULT 0,
  _min_tx_count int DEFAULT NULL,
  _from_date timestamp DEFAULT NULL,
  _to_date timestamp DEFAULT NULL
)
RETURNS jsonb
LANGUAGE sql STABLE
AS $$
  WITH block_tx_counts AS (
    -- Count transactions per block
    SELECT
      t.height AS block_id,
      COUNT(*) AS computed_tx_count
    FROM api.transactions_main t
    GROUP BY t.height
  ),
  filtered_blocks AS (
    SELECT b.id, b.data
    FROM api.blocks_raw b
    LEFT JOIN block_tx_counts btc ON b.id = btc.block_id
    WHERE
      (_min_tx_count IS NULL OR COALESCE(btc.computed_tx_count, 0) >= _min_tx_count)
      AND (_from_date IS NULL OR (b.data->'block'->'header'->>'time')::timestamp >= _from_date)
      AND (_to_date IS NULL OR (b.data->'block'->'header'->>'time')::timestamp <= _to_date)
    ORDER BY b.id DESC
  ),
  total AS (
    SELECT COUNT(*) AS count FROM filtered_blocks
  ),
  paginated AS (
    SELECT f.id, f.data, btc.computed_tx_count AS tx_count
    FROM filtered_blocks f
    LEFT JOIN block_tx_counts btc ON f.id = btc.block_id
    LIMIT _limit OFFSET _offset
  )
  SELECT jsonb_build_object(
    'data', COALESCE(jsonb_agg(
      jsonb_build_object(
        'id', p.id,
        'data', p.data,
        'tx_count', COALESCE(p.tx_count, 0)
      ) ORDER BY p.id DESC
    ), '[]'::jsonb),
    'pagination', jsonb_build_object(
      'total', (SELECT count FROM total),
      'limit', _limit,
      'offset', _offset,
      'has_next', _offset + _limit < (SELECT count FROM total),
      'has_prev', _offset > 0
    )
  )
  FROM paginated p;
$$;

GRANT EXECUTE ON FUNCTION api.get_blocks_paginated(int, int, int, timestamp, timestamp) TO web_anon;

COMMIT;
