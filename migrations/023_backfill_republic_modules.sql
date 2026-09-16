-- Migration 023: Backfill Republic module tables from existing indexed data
-- Handles data that was indexed before triggers were active

BEGIN;

-- ============================================================================
-- Backfill compute jobs from MsgSubmitJob
-- ============================================================================

INSERT INTO api.compute_jobs (
  job_id, creator, target_validator, execution_image,
  result_upload_endpoint, result_fetch_endpoint, verification_image,
  fee_denom, fee_amount, status,
  submit_tx_hash, submit_height, submit_time
)
SELECT DISTINCT ON (e.attr_value::bigint)
  e.attr_value::bigint AS job_id,
  COALESCE(mr.data->>'creator', m.sender) AS creator,
  COALESCE(mr.data->>'targetValidator', '') AS target_validator,
  mr.data->>'executionImage' AS execution_image,
  mr.data->>'resultUploadEndpoint' AS result_upload_endpoint,
  mr.data->>'resultFetchEndpoint' AS result_fetch_endpoint,
  mr.data->>'verificationImage' AS verification_image,
  mr.data->'fee'->>'denom' AS fee_denom,
  mr.data->'fee'->>'amount' AS fee_amount,
  'PENDING' AS status,
  t.id AS submit_tx_hash,
  t.height AS submit_height,
  t.timestamp AS submit_time
FROM api.events_main e
JOIN api.transactions_main t ON e.id = t.id
JOIN api.messages_main m ON t.id = m.id AND m.type LIKE '%MsgSubmitJob'
LEFT JOIN api.messages_raw mr ON m.id = mr.id AND m.message_index = mr.message_index
WHERE (e.event_type = 'job_submitted' OR e.event_type = 'submit_job')
  AND e.attr_key = 'job_id'
  AND e.attr_value IS NOT NULL
ORDER BY e.attr_value::bigint, t.height
ON CONFLICT (job_id) DO NOTHING;

-- Update jobs that have results
UPDATE api.compute_jobs j SET
  status = 'COMPLETED',
  result_hash = mr.data->>'resultHash',
  result_tx_hash = t.id,
  result_height = t.height,
  result_time = t.timestamp,
  updated_at = NOW()
FROM api.messages_main m
JOIN api.transactions_main t ON m.id = t.id
LEFT JOIN api.messages_raw mr ON m.id = mr.id AND m.message_index = mr.message_index
WHERE m.type LIKE '%MsgSubmitJobResult'
AND (mr.data->>'jobId')::BIGINT = j.job_id
AND j.status = 'PENDING';

-- ============================================================================
-- Backfill compute benchmarks from MsgBenchmarkRequest
-- ============================================================================

INSERT INTO api.compute_benchmarks (
  benchmark_id, creator, benchmark_type,
  upload_endpoint, retrieve_endpoint, status,
  submit_tx_hash, submit_height, submit_time
)
SELECT DISTINCT ON (e.attr_value::bigint)
  e.attr_value::bigint AS benchmark_id,
  COALESCE(mr.data->>'creator', m.sender) AS creator,
  mr.data->>'benchmarkType' AS benchmark_type,
  mr.data->>'uploadEndpoint' AS upload_endpoint,
  mr.data->>'retrieveEndpoint' AS retrieve_endpoint,
  'PENDING' AS status,
  t.id AS submit_tx_hash,
  t.height AS submit_height,
  t.timestamp AS submit_time
FROM api.events_main e
JOIN api.transactions_main t ON e.id = t.id
JOIN api.messages_main m ON t.id = m.id AND m.type LIKE '%MsgBenchmarkRequest'
LEFT JOIN api.messages_raw mr ON m.id = mr.id AND m.message_index = mr.message_index
WHERE e.attr_key = 'benchmark_id'
  AND e.attr_value IS NOT NULL
ORDER BY e.attr_value::bigint, t.height
ON CONFLICT (benchmark_id) DO NOTHING;

-- Update benchmarks that have results
UPDATE api.compute_benchmarks b SET
  status = 'COMPLETED',
  result_file_hash = mr.data->>'resultFileHash',
  result_validator = COALESCE(mr.data->>'creator', m.sender),
  result_tx_hash = t.id,
  result_height = t.height,
  result_time = t.timestamp,
  updated_at = NOW()
FROM api.messages_main m
JOIN api.transactions_main t ON m.id = t.id
LEFT JOIN api.messages_raw mr ON m.id = mr.id AND m.message_index = mr.message_index
WHERE m.type LIKE '%MsgBenchmarkResult'
AND (mr.data->>'benchmarkId')::BIGINT = b.benchmark_id
AND b.status = 'PENDING';

-- ============================================================================
-- Backfill seed contributions
-- ============================================================================

INSERT INTO api.compute_seed_contributions (
  validator, benchmark_id, tx_hash, height, timestamp
)
SELECT
  COALESCE(mr.data->>'creator', m.sender) AS validator,
  (mr.data->>'benchmarkId')::BIGINT AS benchmark_id,
  t.id AS tx_hash,
  t.height,
  t.timestamp
FROM api.messages_main m
JOIN api.transactions_main t ON m.id = t.id
LEFT JOIN api.messages_raw mr ON m.id = mr.id AND m.message_index = mr.message_index
WHERE m.type LIKE '%MsgSubmitSeed'
AND mr.data->>'benchmarkId' IS NOT NULL
ON CONFLICT (validator, benchmark_id) DO NOTHING;

-- ============================================================================
-- Backfill committee proposals
-- ============================================================================

INSERT INTO api.compute_committee_proposals (
  proposer, target_height, tx_hash, height, timestamp, weighted_validators
)
SELECT
  COALESCE(mr.data->>'creator', m.sender) AS proposer,
  (mr.data->>'targetHeight')::BIGINT AS target_height,
  t.id AS tx_hash,
  t.height,
  t.timestamp,
  CASE WHEN mr.data ? 'weightedValidators' THEN mr.data->'weightedValidators' ELSE NULL END
FROM api.messages_main m
JOIN api.transactions_main t ON m.id = t.id
LEFT JOIN api.messages_raw mr ON m.id = mr.id AND m.message_index = mr.message_index
WHERE m.type LIKE '%MsgSubmitCommitteeProposal';

-- ============================================================================
-- Backfill validator IPFS addresses
-- ============================================================================

INSERT INTO api.validator_ipfs_addresses (
  validator_address, ipfs_multiaddrs, ipfs_peer_id,
  tx_hash, height, timestamp
)
SELECT DISTINCT ON (COALESCE(mr.data->>'validatorAddress', m.sender))
  COALESCE(mr.data->>'validatorAddress', m.sender) AS validator_address,
  CASE
    WHEN mr.data ? 'ipfsMultiaddrs' AND jsonb_typeof(mr.data->'ipfsMultiaddrs') = 'array'
    THEN ARRAY(SELECT jsonb_array_elements_text(mr.data->'ipfsMultiaddrs'))
    ELSE NULL
  END AS ipfs_multiaddrs,
  mr.data->>'ipfsPeerId' AS ipfs_peer_id,
  t.id AS tx_hash,
  t.height,
  t.timestamp
FROM api.messages_main m
JOIN api.transactions_main t ON m.id = t.id
LEFT JOIN api.messages_raw mr ON m.id = mr.id AND m.message_index = mr.message_index
WHERE m.type LIKE '%MsgSetIPFSAddress'
ORDER BY COALESCE(mr.data->>'validatorAddress', m.sender), t.height DESC
ON CONFLICT (validator_address) DO UPDATE SET
  ipfs_multiaddrs = EXCLUDED.ipfs_multiaddrs,
  ipfs_peer_id = COALESCE(EXCLUDED.ipfs_peer_id, api.validator_ipfs_addresses.ipfs_peer_id),
  tx_hash = EXCLUDED.tx_hash,
  height = EXCLUDED.height,
  timestamp = EXCLUDED.timestamp,
  updated_at = NOW();

-- ============================================================================
-- Backfill slashing records
-- ============================================================================

INSERT INTO api.slashing_records (
  validator_address, submitter, condition,
  evidence_type, evidence_data,
  tx_hash, height, timestamp
)
SELECT
  COALESCE(mr.data->>'validatorAddress', mr.data->'evidence'->>'validatorAddress', '') AS validator_address,
  COALESCE(mr.data->>'submitter', m.sender) AS submitter,
  CASE
    WHEN m.type LIKE '%ComputeMisconduct%' THEN 'COMPUTE_MISCONDUCT'
    WHEN m.type LIKE '%ReputationDegradation%' THEN 'REPUTATION_DEGRADATION'
    WHEN m.type LIKE '%DelegatedCollusion%' THEN 'DELEGATED_COLLUSION'
  END AS condition,
  mr.data->'evidence'->>'@type' AS evidence_type,
  mr.data->'evidence' AS evidence_data,
  t.id AS tx_hash,
  t.height,
  t.timestamp
FROM api.messages_main m
JOIN api.transactions_main t ON m.id = t.id
LEFT JOIN api.messages_raw mr ON m.id = mr.id AND m.message_index = mr.message_index
WHERE m.type LIKE '%republic.slashingplus%'
AND (m.type LIKE '%Evidence%');

-- Re-ensure triggers exist
DROP TRIGGER IF EXISTS trigger_detect_compute_validation ON api.transactions_main;
CREATE TRIGGER trigger_detect_compute_validation
  AFTER INSERT ON api.transactions_main
  FOR EACH ROW
  EXECUTE FUNCTION api.detect_compute_validation();

DROP TRIGGER IF EXISTS trigger_detect_reputation ON api.transactions_main;
CREATE TRIGGER trigger_detect_reputation
  AFTER INSERT ON api.transactions_main
  FOR EACH ROW
  EXECUTE FUNCTION api.detect_reputation_messages();

DROP TRIGGER IF EXISTS trigger_detect_slashing ON api.transactions_main;
CREATE TRIGGER trigger_detect_slashing
  AFTER INSERT ON api.transactions_main
  FOR EACH ROW
  EXECUTE FUNCTION api.detect_slashing_messages();

COMMIT;
