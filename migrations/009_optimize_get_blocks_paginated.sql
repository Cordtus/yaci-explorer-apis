-- Migration 009: Optimize get_blocks_paginated to use tx_count column
-- Eliminates full table scan of transactions_main on every request

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
  WITH filtered_blocks AS (
    SELECT b.id, b.data, b.tx_count
    FROM api.blocks_raw b
    WHERE
      (_min_tx_count IS NULL OR b.tx_count >= _min_tx_count)
      AND (_from_date IS NULL OR (b.data->'block'->'header'->>'time')::timestamp >= _from_date)
      AND (_to_date IS NULL OR (b.data->'block'->'header'->>'time')::timestamp <= _to_date)
    ORDER BY b.id DESC
  ),
  total AS (
    SELECT COUNT(*) AS count FROM filtered_blocks
  ),
  paginated AS (
    SELECT * FROM filtered_blocks
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

COMMIT;
