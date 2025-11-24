-- =============================================================================
-- YACI Explorer Complete Schema
-- Clean start with proper EVM and Cosmos domain support
-- =============================================================================

-- =============================================================================
-- CORE TABLES (populated by Yaci indexer)
-- =============================================================================

-- Raw block data
CREATE TABLE IF NOT EXISTS api.blocks_raw (
  id BIGINT PRIMARY KEY,
  data JSONB NOT NULL
);

-- Raw transaction data
CREATE TABLE IF NOT EXISTS api.transactions_raw (
  id TEXT PRIMARY KEY,
  data JSONB NOT NULL
);

-- Parsed transaction metadata
CREATE TABLE IF NOT EXISTS api.transactions_main (
  id TEXT PRIMARY KEY,
  height BIGINT NOT NULL,
  timestamp TIMESTAMP WITH TIME ZONE,
  fee JSONB,
  memo TEXT,
  error TEXT,
  proposal_ids BIGINT[]
);

-- Raw message data
CREATE TABLE IF NOT EXISTS api.messages_raw (
  id TEXT NOT NULL,
  message_index INT NOT NULL,
  data JSONB,
  PRIMARY KEY (id, message_index)
);

-- Parsed message metadata
CREATE TABLE IF NOT EXISTS api.messages_main (
  id TEXT NOT NULL,
  message_index INT NOT NULL,
  type TEXT,
  sender TEXT,
  mentions TEXT[],
  metadata JSONB,
  PRIMARY KEY (id, message_index)
);

-- Parsed events
CREATE TABLE IF NOT EXISTS api.events_main (
  id TEXT NOT NULL,
  event_index INT NOT NULL,
  attr_index INT NOT NULL,
  event_type TEXT NOT NULL,
  attr_key TEXT,
  attr_value TEXT,
  msg_index INT,
  PRIMARY KEY (id, event_index, attr_index)
);

-- Core indexes
CREATE INDEX IF NOT EXISTS idx_tx_height ON api.transactions_main(height DESC);
CREATE INDEX IF NOT EXISTS idx_tx_timestamp ON api.transactions_main(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_msg_type ON api.messages_main(type);
CREATE INDEX IF NOT EXISTS idx_msg_sender ON api.messages_main(sender);
CREATE INDEX IF NOT EXISTS idx_msg_mentions ON api.messages_main USING GIN(mentions);
CREATE INDEX IF NOT EXISTS idx_event_type ON api.events_main(event_type);

-- =============================================================================
-- EVM DOMAIN TABLES
-- =============================================================================

-- Decoded EVM transactions
CREATE TABLE IF NOT EXISTS api.evm_transactions (
  tx_id TEXT PRIMARY KEY REFERENCES api.transactions_main(id) ON DELETE CASCADE,
  hash TEXT NOT NULL UNIQUE,
  "from" TEXT NOT NULL,
  "to" TEXT,
  nonce BIGINT NOT NULL,
  gas_limit BIGINT NOT NULL,
  gas_price NUMERIC NOT NULL,
  max_fee_per_gas NUMERIC,
  max_priority_fee_per_gas NUMERIC,
  value NUMERIC NOT NULL,
  data TEXT,
  type SMALLINT NOT NULL DEFAULT 0,
  chain_id BIGINT,
  gas_used BIGINT,
  status SMALLINT DEFAULT 1,
  function_name TEXT,
  function_signature TEXT,
  decoded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_evm_tx_hash ON api.evm_transactions(hash);
CREATE INDEX IF NOT EXISTS idx_evm_tx_from ON api.evm_transactions("from");
CREATE INDEX IF NOT EXISTS idx_evm_tx_to ON api.evm_transactions("to");

-- EVM logs (from tx receipts)
CREATE TABLE IF NOT EXISTS api.evm_logs (
  tx_id TEXT NOT NULL REFERENCES api.evm_transactions(tx_id) ON DELETE CASCADE,
  log_index INT NOT NULL,
  address TEXT NOT NULL,
  topics TEXT[] NOT NULL,
  data TEXT,
  PRIMARY KEY (tx_id, log_index)
);

CREATE INDEX IF NOT EXISTS idx_evm_log_address ON api.evm_logs(address);
CREATE INDEX IF NOT EXISTS idx_evm_log_topic0 ON api.evm_logs((topics[1]));

-- Known EVM tokens (ERC-20, ERC-721, ERC-1155)
CREATE TABLE IF NOT EXISTS api.evm_tokens (
  address TEXT PRIMARY KEY,
  name TEXT,
  symbol TEXT,
  decimals INT,
  type TEXT NOT NULL,  -- ERC20, ERC721, ERC1155
  total_supply NUMERIC,
  first_seen_tx TEXT,
  first_seen_height BIGINT,
  verified BOOLEAN DEFAULT FALSE,
  metadata JSONB
);

-- EVM token transfers (parsed from Transfer events)
CREATE TABLE IF NOT EXISTS api.evm_token_transfers (
  tx_id TEXT NOT NULL,
  log_index INT NOT NULL,
  token_address TEXT NOT NULL REFERENCES api.evm_tokens(address),
  from_address TEXT NOT NULL,
  to_address TEXT NOT NULL,
  value NUMERIC NOT NULL,
  PRIMARY KEY (tx_id, log_index),
  FOREIGN KEY (tx_id, log_index) REFERENCES api.evm_logs(tx_id, log_index) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_token_transfer_from ON api.evm_token_transfers(from_address);
CREATE INDEX IF NOT EXISTS idx_token_transfer_to ON api.evm_token_transfers(to_address);
CREATE INDEX IF NOT EXISTS idx_token_transfer_token ON api.evm_token_transfers(token_address);

-- EVM contracts (metadata, ABI storage)
CREATE TABLE IF NOT EXISTS api.evm_contracts (
  address TEXT PRIMARY KEY,
  creator TEXT,
  creation_tx TEXT,
  creation_height BIGINT,
  bytecode_hash TEXT,
  is_verified BOOLEAN DEFAULT FALSE,
  name TEXT,
  abi JSONB,
  source_code TEXT,
  compiler_version TEXT,
  metadata JSONB
);

-- =============================================================================
-- COSMOS DOMAIN TABLES
-- =============================================================================

-- Validators (enriched via RPC queries)
CREATE TABLE IF NOT EXISTS api.validators (
  operator_address TEXT PRIMARY KEY,
  consensus_address TEXT,
  moniker TEXT,
  identity TEXT,
  website TEXT,
  details TEXT,
  commission_rate NUMERIC,
  commission_max_rate NUMERIC,
  commission_max_change_rate NUMERIC,
  min_self_delegation NUMERIC,
  tokens NUMERIC,
  delegator_shares NUMERIC,
  status TEXT,
  jailed BOOLEAN DEFAULT FALSE,
  creation_height BIGINT,
  first_seen_tx TEXT,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_validator_status ON api.validators(status);
CREATE INDEX IF NOT EXISTS idx_validator_tokens ON api.validators(tokens DESC);

-- Governance proposals (enriched via RPC queries)
CREATE TABLE IF NOT EXISTS api.proposals (
  id BIGINT PRIMARY KEY,
  title TEXT,
  summary TEXT,
  proposer TEXT,
  status TEXT,
  submit_time TIMESTAMP WITH TIME ZONE,
  deposit_end_time TIMESTAMP WITH TIME ZONE,
  voting_start_time TIMESTAMP WITH TIME ZONE,
  voting_end_time TIMESTAMP WITH TIME ZONE,
  total_deposit JSONB,
  final_tally JSONB,
  metadata JSONB,
  creation_tx TEXT,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_proposal_status ON api.proposals(status);
CREATE INDEX IF NOT EXISTS idx_proposal_proposer ON api.proposals(proposer);

-- Governance votes
CREATE TABLE IF NOT EXISTS api.proposal_votes (
  proposal_id BIGINT NOT NULL REFERENCES api.proposals(id) ON DELETE CASCADE,
  voter TEXT NOT NULL,
  option TEXT NOT NULL,
  weight NUMERIC DEFAULT 1,
  tx_id TEXT,
  timestamp TIMESTAMP WITH TIME ZONE,
  PRIMARY KEY (proposal_id, voter)
);

CREATE INDEX IF NOT EXISTS idx_vote_voter ON api.proposal_votes(voter);

-- IBC channels
CREATE TABLE IF NOT EXISTS api.ibc_channels (
  channel_id TEXT NOT NULL,
  port_id TEXT NOT NULL,
  counterparty_channel_id TEXT,
  counterparty_port_id TEXT,
  connection_id TEXT,
  state TEXT,
  ordering TEXT,
  version TEXT,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  PRIMARY KEY (channel_id, port_id)
);

-- Denomination metadata (for display)
CREATE TABLE IF NOT EXISTS api.denom_metadata (
  denom TEXT PRIMARY KEY,
  symbol TEXT,
  name TEXT,
  decimals INT DEFAULT 6,
  description TEXT,
  logo_uri TEXT,
  coingecko_id TEXT,
  is_native BOOLEAN DEFAULT FALSE,
  ibc_source_chain TEXT,
  ibc_source_denom TEXT,
  evm_contract TEXT REFERENCES api.evm_tokens(address),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- =============================================================================
-- ANALYTICS VIEWS
-- =============================================================================

-- Chain statistics
CREATE OR REPLACE VIEW api.chain_stats AS
SELECT
  (SELECT MAX(id) FROM api.blocks_raw) AS latest_block,
  (SELECT COUNT(*) FROM api.transactions_main) AS total_transactions,
  (SELECT COUNT(DISTINCT sender) FROM api.messages_main WHERE sender IS NOT NULL) AS unique_addresses,
  (SELECT COUNT(*) FROM api.evm_transactions) AS evm_transactions,
  (SELECT COUNT(*) FROM api.validators WHERE status = 'BOND_STATUS_BONDED') AS active_validators;

-- Transaction volume daily
CREATE OR REPLACE VIEW api.tx_volume_daily AS
SELECT
  DATE(timestamp) AS date,
  COUNT(*) AS count
FROM api.transactions_main
WHERE timestamp IS NOT NULL
GROUP BY DATE(timestamp)
ORDER BY date DESC;

-- Transaction volume hourly
CREATE OR REPLACE VIEW api.tx_volume_hourly AS
SELECT
  DATE_TRUNC('hour', timestamp) AS hour,
  COUNT(*) AS count
FROM api.transactions_main
WHERE timestamp IS NOT NULL
GROUP BY DATE_TRUNC('hour', timestamp)
ORDER BY hour DESC;

-- Message type distribution
CREATE OR REPLACE VIEW api.message_type_stats AS
SELECT
  type,
  COUNT(*) AS count
FROM api.messages_main
WHERE type IS NOT NULL
GROUP BY type
ORDER BY count DESC;

-- Gas usage distribution
CREATE OR REPLACE VIEW api.gas_usage_distribution AS
SELECT
  CASE
    WHEN (fee->>'gasLimit')::bigint < 100000 THEN '0-100k'
    WHEN (fee->>'gasLimit')::bigint < 250000 THEN '100k-250k'
    WHEN (fee->>'gasLimit')::bigint < 500000 THEN '250k-500k'
    WHEN (fee->>'gasLimit')::bigint < 1000000 THEN '500k-1M'
    ELSE '1M+'
  END AS gas_range,
  COUNT(*) AS count
FROM api.transactions_main
WHERE fee->>'gasLimit' IS NOT NULL
GROUP BY 1
ORDER BY MIN((fee->>'gasLimit')::bigint);

-- Transaction success rate
CREATE OR REPLACE VIEW api.tx_success_rate AS
SELECT
  COUNT(*) AS total,
  COUNT(*) FILTER (WHERE error IS NULL) AS successful,
  COUNT(*) FILTER (WHERE error IS NOT NULL) AS failed,
  ROUND(100.0 * COUNT(*) FILTER (WHERE error IS NULL) / NULLIF(COUNT(*), 0), 2) AS success_rate_percent
FROM api.transactions_main;

-- Fee revenue by denomination
CREATE OR REPLACE VIEW api.fee_revenue AS
SELECT
  fee_item->>'denom' AS denom,
  SUM((fee_item->>'amount')::numeric) AS total_amount
FROM api.transactions_main,
  jsonb_array_elements(fee->'amount') AS fee_item
GROUP BY fee_item->>'denom';

-- EVM transaction map (Cosmos hash to ETH hash)
CREATE OR REPLACE VIEW api.evm_tx_map AS
SELECT
  tx_id,
  hash AS ethereum_tx_hash,
  "from",
  "to",
  gas_used
FROM api.evm_transactions;

-- Pending EVM transactions to decode
CREATE OR REPLACE VIEW api.evm_pending_decode AS
SELECT
  t.id AS tx_id,
  t.height,
  t.timestamp,
  m.data->>'raw' AS raw_bytes,
  MAX(CASE WHEN e.attr_key = 'ethereumTxHash' THEN e.attr_value END) AS ethereum_tx_hash,
  MAX(CASE WHEN e.attr_key = 'txGasUsed' THEN e.attr_value::bigint END) AS gas_used
FROM api.transactions_main t
JOIN api.messages_main mm ON t.id = mm.id
JOIN api.messages_raw m ON mm.id = m.id AND mm.message_index = m.message_index
JOIN api.events_main e ON t.id = e.id AND e.event_type = 'ethereum_tx'
WHERE mm.type LIKE '%MsgEthereumTx%'
  AND NOT EXISTS (SELECT 1 FROM api.evm_transactions ev WHERE ev.tx_id = t.id)
GROUP BY t.id, t.height, t.timestamp, m.data->>'raw';

-- =============================================================================
-- RPC FUNCTIONS
-- =============================================================================

-- Universal search
CREATE OR REPLACE FUNCTION api.universal_search(_query text)
RETURNS jsonb
LANGUAGE plpgsql STABLE
AS $$
DECLARE
  results jsonb := '[]'::jsonb;
  trimmed text := trim(_query);
  block_result jsonb;
  tx_result jsonb;
  evm_tx_result jsonb;
  addr_result jsonb;
BEGIN
  -- Check for block height (numeric)
  IF trimmed ~ '^\d+$' THEN
    SELECT jsonb_build_object(
      'type', 'block',
      'value', jsonb_build_object('height', id),
      'score', 100
    ) INTO block_result
    FROM api.blocks_raw
    WHERE id = trimmed::bigint;

    IF block_result IS NOT NULL THEN
      results := results || block_result;
    END IF;
  END IF;

  -- Check for EVM hash (0x prefix, 64 hex chars)
  IF trimmed ~* '^0x[a-f0-9]{64}$' THEN
    SELECT jsonb_build_object(
      'type', 'evm_transaction',
      'value', jsonb_build_object('tx_id', tx_id, 'hash', hash),
      'score', 100
    ) INTO evm_tx_result
    FROM api.evm_transactions
    WHERE hash = lower(trimmed);

    IF evm_tx_result IS NOT NULL THEN
      results := results || evm_tx_result;
    END IF;
  END IF;

  -- Check for Cosmos tx hash (64 hex, no 0x)
  IF trimmed ~ '^[a-fA-F0-9]{64}$' THEN
    SELECT jsonb_build_object(
      'type', 'transaction',
      'value', jsonb_build_object('id', id),
      'score', 100
    ) INTO tx_result
    FROM api.transactions_main
    WHERE id = upper(trimmed);

    IF tx_result IS NOT NULL THEN
      results := results || tx_result;
    END IF;
  END IF;

  -- Check for EVM address (0x prefix, 40 hex chars)
  IF trimmed ~* '^0x[a-f0-9]{40}$' THEN
    results := results || jsonb_build_object(
      'type', 'evm_address',
      'value', jsonb_build_object('address', lower(trimmed)),
      'score', 90
    );
  END IF;

  -- Check for Cosmos address (bech32)
  IF trimmed ~ '^[a-z]+1[a-z0-9]{38,}$' THEN
    results := results || jsonb_build_object(
      'type', 'address',
      'value', jsonb_build_object('address', trimmed),
      'score', 90
    );
  END IF;

  RETURN results;
END;
$$;

-- Get transaction detail with EVM data
CREATE OR REPLACE FUNCTION api.get_transaction_detail(_hash text)
RETURNS jsonb
LANGUAGE sql STABLE
AS $$
  WITH
  tx_main AS (
    SELECT * FROM api.transactions_main WHERE id = _hash
  ),
  tx_raw AS (
    SELECT * FROM api.transactions_raw WHERE id = _hash
  ),
  tx_messages AS (
    SELECT jsonb_agg(
      jsonb_build_object(
        'id', m.id,
        'message_index', m.message_index,
        'type', m.type,
        'sender', m.sender,
        'mentions', m.mentions,
        'metadata', m.metadata,
        'data', r.data
      ) ORDER BY m.message_index
    ) AS messages
    FROM api.messages_main m
    LEFT JOIN api.messages_raw r ON m.id = r.id AND m.message_index = r.message_index
    WHERE m.id = _hash
  ),
  tx_events AS (
    SELECT jsonb_agg(
      jsonb_build_object(
        'id', e.id,
        'event_index', e.event_index,
        'attr_index', e.attr_index,
        'event_type', e.event_type,
        'attr_key', e.attr_key,
        'attr_value', e.attr_value,
        'msg_index', e.msg_index
      ) ORDER BY e.event_index, e.attr_index
    ) AS events
    FROM api.events_main e
    WHERE e.id = _hash
  ),
  evm_data AS (
    SELECT jsonb_build_object(
      'hash', ev.hash,
      'from', ev."from",
      'to', ev."to",
      'nonce', ev.nonce,
      'gasLimit', ev.gas_limit::text,
      'gasPrice', ev.gas_price::text,
      'maxFeePerGas', ev.max_fee_per_gas::text,
      'maxPriorityFeePerGas', ev.max_priority_fee_per_gas::text,
      'value', ev.value::text,
      'data', ev.data,
      'type', ev.type,
      'chainId', ev.chain_id::text,
      'gasUsed', ev.gas_used,
      'status', ev.status,
      'functionName', ev.function_name,
      'functionSignature', ev.function_signature
    ) AS evm
    FROM api.evm_transactions ev
    WHERE ev.tx_id = _hash
  ),
  evm_logs_data AS (
    SELECT jsonb_agg(
      jsonb_build_object(
        'logIndex', l.log_index,
        'address', l.address,
        'topics', l.topics,
        'data', l.data
      ) ORDER BY l.log_index
    ) AS logs
    FROM api.evm_logs l
    WHERE l.tx_id = _hash
  )
  SELECT jsonb_build_object(
    'id', t.id,
    'fee', t.fee,
    'memo', t.memo,
    'error', t.error,
    'height', t.height,
    'timestamp', t.timestamp,
    'proposal_ids', t.proposal_ids,
    'messages', COALESCE(m.messages, '[]'::jsonb),
    'events', COALESCE(e.events, '[]'::jsonb),
    'evm_data', ev.evm,
    'evm_logs', COALESCE(el.logs, '[]'::jsonb),
    'raw_data', r.data
  )
  FROM tx_raw r
  LEFT JOIN tx_main t ON TRUE
  LEFT JOIN tx_messages m ON TRUE
  LEFT JOIN tx_events e ON TRUE
  LEFT JOIN evm_data ev ON TRUE
  LEFT JOIN evm_logs_data el ON TRUE;
$$;

-- Get paginated transactions
CREATE OR REPLACE FUNCTION api.get_transactions_paginated(
  _limit int DEFAULT 20,
  _offset int DEFAULT 0,
  _status text DEFAULT NULL,
  _block_height bigint DEFAULT NULL,
  _message_type text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE sql STABLE
AS $$
  WITH filtered_txs AS (
    SELECT DISTINCT t.id
    FROM api.transactions_main t
    LEFT JOIN api.messages_main m ON t.id = m.id
    WHERE (_status IS NULL OR
           (_status = 'success' AND t.error IS NULL) OR
           (_status = 'failed' AND t.error IS NOT NULL))
      AND (_block_height IS NULL OR t.height = _block_height)
      AND (_message_type IS NULL OR m.type = _message_type)
  ),
  paginated AS (
    SELECT t.*
    FROM api.transactions_main t
    JOIN filtered_txs f ON t.id = f.id
    ORDER BY t.height DESC, t.id
    LIMIT _limit OFFSET _offset
  ),
  total AS (
    SELECT COUNT(*) AS count FROM filtered_txs
  )
  SELECT jsonb_build_object(
    'data', COALESCE(jsonb_agg(
      jsonb_build_object(
        'id', p.id,
        'height', p.height,
        'timestamp', p.timestamp,
        'fee', p.fee,
        'memo', p.memo,
        'error', p.error
      ) ORDER BY p.height DESC
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

-- Get transactions by address
CREATE OR REPLACE FUNCTION api.get_transactions_by_address(
  _address text,
  _limit int DEFAULT 50,
  _offset int DEFAULT 0
)
RETURNS jsonb
LANGUAGE sql STABLE
AS $$
  WITH addr_txs AS (
    SELECT DISTINCT m.id
    FROM api.messages_main m
    WHERE m.sender = _address OR _address = ANY(m.mentions)
  ),
  paginated AS (
    SELECT t.*
    FROM api.transactions_main t
    JOIN addr_txs a ON t.id = a.id
    ORDER BY t.height DESC
    LIMIT _limit OFFSET _offset
  ),
  total AS (
    SELECT COUNT(*) AS count FROM addr_txs
  )
  SELECT jsonb_build_object(
    'data', COALESCE(jsonb_agg(
      jsonb_build_object(
        'id', p.id,
        'height', p.height,
        'timestamp', p.timestamp,
        'fee', p.fee,
        'error', p.error
      ) ORDER BY p.height DESC
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

-- Get address statistics
CREATE OR REPLACE FUNCTION api.get_address_stats(_address text)
RETURNS jsonb
LANGUAGE sql STABLE
AS $$
  WITH tx_ids AS (
    SELECT DISTINCT m.id
    FROM api.messages_main m
    WHERE m.sender = _address OR _address = ANY(m.mentions)
  ),
  aggregated AS (
    SELECT
      COUNT(DISTINCT t.id) AS transaction_count,
      MIN(t.timestamp) AS first_seen,
      MAX(t.timestamp) AS last_seen
    FROM api.transactions_main t
    JOIN tx_ids ON t.id = tx_ids.id
  )
  SELECT jsonb_build_object(
    'address', _address,
    'transaction_count', transaction_count,
    'first_seen', first_seen,
    'last_seen', last_seen
  )
  FROM aggregated;
$$;

-- Get block time analysis
CREATE OR REPLACE FUNCTION api.get_block_time_analysis(_limit int DEFAULT 100)
RETURNS jsonb
LANGUAGE sql STABLE
AS $$
  WITH block_times AS (
    SELECT
      id,
      (data->'block'->'header'->>'time')::timestamp AS block_time,
      LAG((data->'block'->'header'->>'time')::timestamp) OVER (ORDER BY id) AS prev_time
    FROM api.blocks_raw
    ORDER BY id DESC
    LIMIT _limit
  ),
  diffs AS (
    SELECT EXTRACT(EPOCH FROM (block_time - prev_time)) AS diff_seconds
    FROM block_times
    WHERE prev_time IS NOT NULL
  )
  SELECT jsonb_build_object(
    'avg', ROUND(AVG(diff_seconds)::numeric, 2),
    'min', ROUND(MIN(diff_seconds)::numeric, 2),
    'max', ROUND(MAX(diff_seconds)::numeric, 2)
  )
  FROM diffs;
$$;

-- =============================================================================
-- PERMISSIONS
-- =============================================================================

-- Grant read access to all tables and views
GRANT SELECT ON ALL TABLES IN SCHEMA api TO web_anon;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA api TO web_anon;

-- Grant usage on schema
GRANT USAGE ON SCHEMA api TO web_anon;
