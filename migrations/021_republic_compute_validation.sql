-- Migration 021: Republic Compute Validation module support
-- Tables, triggers, and functions for compute jobs, benchmarks, seeds, and committee proposals

BEGIN;

-- ============================================================================
-- Tables
-- ============================================================================

CREATE TABLE IF NOT EXISTS api.compute_jobs (
  job_id BIGINT PRIMARY KEY,
  creator TEXT NOT NULL,
  target_validator TEXT NOT NULL,
  execution_image TEXT,
  result_upload_endpoint TEXT,
  result_fetch_endpoint TEXT,
  verification_image TEXT,
  fee_denom TEXT,
  fee_amount TEXT,
  status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'COMPLETED', 'FAILED')),
  result_hash TEXT,
  submit_tx_hash TEXT NOT NULL,
  submit_height BIGINT,
  submit_time TIMESTAMPTZ,
  result_tx_hash TEXT,
  result_height BIGINT,
  result_time TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS api.compute_benchmarks (
  benchmark_id BIGINT PRIMARY KEY,
  creator TEXT NOT NULL,
  benchmark_type TEXT,
  upload_endpoint TEXT,
  retrieve_endpoint TEXT,
  result_file_hash TEXT,
  status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'COMPLETED', 'FAILED')),
  submit_tx_hash TEXT NOT NULL,
  submit_height BIGINT,
  submit_time TIMESTAMPTZ,
  result_tx_hash TEXT,
  result_validator TEXT,
  result_height BIGINT,
  result_time TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS api.compute_seed_contributions (
  id SERIAL PRIMARY KEY,
  validator TEXT NOT NULL,
  benchmark_id BIGINT REFERENCES api.compute_benchmarks(benchmark_id),
  tx_hash TEXT NOT NULL,
  height BIGINT,
  timestamp TIMESTAMPTZ,
  UNIQUE(validator, benchmark_id)
);

CREATE TABLE IF NOT EXISTS api.compute_committee_proposals (
  id SERIAL PRIMARY KEY,
  proposer TEXT NOT NULL,
  target_height BIGINT,
  tx_hash TEXT NOT NULL,
  height BIGINT,
  timestamp TIMESTAMPTZ,
  weighted_validators JSONB
);

-- ============================================================================
-- Indexes
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_compute_jobs_creator ON api.compute_jobs(creator);
CREATE INDEX IF NOT EXISTS idx_compute_jobs_validator ON api.compute_jobs(target_validator);
CREATE INDEX IF NOT EXISTS idx_compute_jobs_status ON api.compute_jobs(status);
CREATE INDEX IF NOT EXISTS idx_compute_jobs_submit_time ON api.compute_jobs(submit_time DESC);

CREATE INDEX IF NOT EXISTS idx_benchmarks_creator ON api.compute_benchmarks(creator);
CREATE INDEX IF NOT EXISTS idx_benchmarks_status ON api.compute_benchmarks(status);
CREATE INDEX IF NOT EXISTS idx_benchmarks_submit_time ON api.compute_benchmarks(submit_time DESC);

CREATE INDEX IF NOT EXISTS idx_committee_proposals_height ON api.compute_committee_proposals(target_height);

-- ============================================================================
-- Trigger function: detect compute validation messages
-- ============================================================================

CREATE OR REPLACE FUNCTION api.detect_compute_validation()
RETURNS TRIGGER AS $$
DECLARE
  msg_record RECORD;
  raw_data JSONB;
  extracted_id BIGINT;
BEGIN
  FOR msg_record IN
    SELECT m.id, m.message_index, m.type, m.metadata, m.sender
    FROM api.messages_main m
    WHERE m.id = NEW.id
    AND m.type LIKE '%republic.computevalidation%'
  LOOP
    raw_data := NULL;
    SELECT data INTO raw_data
    FROM api.messages_raw
    WHERE id = msg_record.id AND message_index = msg_record.message_index;

    -- MsgSubmitJob
    IF msg_record.type LIKE '%MsgSubmitJob' THEN
      -- Extract job_id from events
      extracted_id := NULL;
      SELECT (e.attr_value)::BIGINT INTO extracted_id
      FROM api.events_main e
      WHERE e.id = NEW.id
      AND e.event_type = 'job_submitted'
      AND e.attr_key = 'job_id'
      LIMIT 1;

      -- Fallback: try submit_job event type
      IF extracted_id IS NULL THEN
        SELECT (e.attr_value)::BIGINT INTO extracted_id
        FROM api.events_main e
        WHERE e.id = NEW.id
        AND e.event_type = 'submit_job'
        AND e.attr_key = 'job_id'
        LIMIT 1;
      END IF;

      IF extracted_id IS NOT NULL AND raw_data IS NOT NULL THEN
        INSERT INTO api.compute_jobs (
          job_id, creator, target_validator, execution_image,
          result_upload_endpoint, result_fetch_endpoint, verification_image,
          fee_denom, fee_amount, status,
          submit_tx_hash, submit_height, submit_time
        ) VALUES (
          extracted_id,
          COALESCE(raw_data->>'creator', msg_record.sender),
          COALESCE(raw_data->>'targetValidator', ''),
          raw_data->>'executionImage',
          raw_data->>'resultUploadEndpoint',
          raw_data->>'resultFetchEndpoint',
          raw_data->>'verificationImage',
          raw_data->'fee'->>'denom',
          raw_data->'fee'->>'amount',
          'PENDING',
          NEW.id, NEW.height, NEW.timestamp
        )
        ON CONFLICT (job_id) DO UPDATE SET
          execution_image = COALESCE(EXCLUDED.execution_image, api.compute_jobs.execution_image),
          updated_at = NOW();
      END IF;

    -- MsgSubmitJobResult
    ELSIF msg_record.type LIKE '%MsgSubmitJobResult' THEN
      extracted_id := NULL;
      IF raw_data IS NOT NULL AND raw_data ? 'jobId' THEN
        extracted_id := (raw_data->>'jobId')::BIGINT;
      END IF;

      -- Fallback: events
      IF extracted_id IS NULL THEN
        SELECT (e.attr_value)::BIGINT INTO extracted_id
        FROM api.events_main e
        WHERE e.id = NEW.id
        AND e.attr_key = 'job_id'
        LIMIT 1;
      END IF;

      IF extracted_id IS NOT NULL THEN
        UPDATE api.compute_jobs SET
          status = 'COMPLETED',
          result_hash = raw_data->>'resultHash',
          result_tx_hash = NEW.id,
          result_height = NEW.height,
          result_time = NEW.timestamp,
          updated_at = NOW()
        WHERE job_id = extracted_id;
      END IF;

    -- MsgBenchmarkRequest
    ELSIF msg_record.type LIKE '%MsgBenchmarkRequest' THEN
      extracted_id := NULL;
      SELECT (e.attr_value)::BIGINT INTO extracted_id
      FROM api.events_main e
      WHERE e.id = NEW.id
      AND e.attr_key = 'benchmark_id'
      LIMIT 1;

      IF extracted_id IS NOT NULL AND raw_data IS NOT NULL THEN
        INSERT INTO api.compute_benchmarks (
          benchmark_id, creator, benchmark_type,
          upload_endpoint, retrieve_endpoint, status,
          submit_tx_hash, submit_height, submit_time
        ) VALUES (
          extracted_id,
          COALESCE(raw_data->>'creator', msg_record.sender),
          raw_data->>'benchmarkType',
          raw_data->>'uploadEndpoint',
          raw_data->>'retrieveEndpoint',
          'PENDING',
          NEW.id, NEW.height, NEW.timestamp
        )
        ON CONFLICT (benchmark_id) DO UPDATE SET
          benchmark_type = COALESCE(EXCLUDED.benchmark_type, api.compute_benchmarks.benchmark_type),
          updated_at = NOW();
      END IF;

    -- MsgBenchmarkResult
    ELSIF msg_record.type LIKE '%MsgBenchmarkResult' THEN
      extracted_id := NULL;
      IF raw_data IS NOT NULL AND raw_data ? 'benchmarkId' THEN
        extracted_id := (raw_data->>'benchmarkId')::BIGINT;
      END IF;

      IF extracted_id IS NULL THEN
        SELECT (e.attr_value)::BIGINT INTO extracted_id
        FROM api.events_main e
        WHERE e.id = NEW.id
        AND e.attr_key = 'benchmark_id'
        LIMIT 1;
      END IF;

      IF extracted_id IS NOT NULL THEN
        UPDATE api.compute_benchmarks SET
          status = 'COMPLETED',
          result_file_hash = raw_data->>'resultFileHash',
          result_validator = COALESCE(raw_data->>'creator', msg_record.sender),
          result_tx_hash = NEW.id,
          result_height = NEW.height,
          result_time = NEW.timestamp,
          updated_at = NOW()
        WHERE benchmark_id = extracted_id;
      END IF;

    -- MsgSubmitSeed
    ELSIF msg_record.type LIKE '%MsgSubmitSeed' THEN
      extracted_id := NULL;
      IF raw_data IS NOT NULL AND raw_data ? 'benchmarkId' THEN
        extracted_id := (raw_data->>'benchmarkId')::BIGINT;
      END IF;

      IF extracted_id IS NOT NULL THEN
        INSERT INTO api.compute_seed_contributions (
          validator, benchmark_id, tx_hash, height, timestamp
        ) VALUES (
          COALESCE(raw_data->>'creator', msg_record.sender),
          extracted_id,
          NEW.id, NEW.height, NEW.timestamp
        )
        ON CONFLICT (validator, benchmark_id) DO NOTHING;
      END IF;

    -- MsgSubmitCommitteeProposal
    ELSIF msg_record.type LIKE '%MsgSubmitCommitteeProposal' THEN
      INSERT INTO api.compute_committee_proposals (
        proposer, target_height, tx_hash, height, timestamp, weighted_validators
      ) VALUES (
        COALESCE(raw_data->>'creator', msg_record.sender),
        (raw_data->>'targetHeight')::BIGINT,
        NEW.id, NEW.height, NEW.timestamp,
        CASE WHEN raw_data ? 'weightedValidators' THEN raw_data->'weightedValidators' ELSE NULL END
      );
    END IF;
  END LOOP;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger
DROP TRIGGER IF EXISTS trigger_detect_compute_validation ON api.transactions_main;
CREATE TRIGGER trigger_detect_compute_validation
  AFTER INSERT ON api.transactions_main
  FOR EACH ROW
  EXECUTE FUNCTION api.detect_compute_validation();

-- ============================================================================
-- SQL Functions (API endpoints)
-- ============================================================================

CREATE OR REPLACE FUNCTION api.get_compute_jobs(
  _limit INT DEFAULT 20,
  _offset INT DEFAULT 0,
  _status TEXT DEFAULT NULL,
  _creator TEXT DEFAULT NULL,
  _validator TEXT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE sql STABLE
AS $$
  WITH filtered AS (
    SELECT *
    FROM api.compute_jobs
    WHERE (_status IS NULL OR status = _status)
    AND (_creator IS NULL OR creator = _creator)
    AND (_validator IS NULL OR target_validator = _validator)
  ),
  total AS (
    SELECT COUNT(*) AS cnt FROM filtered
  ),
  page AS (
    SELECT * FROM filtered
    ORDER BY submit_time DESC NULLS LAST
    LIMIT _limit OFFSET _offset
  )
  SELECT jsonb_build_object(
    'data', COALESCE(jsonb_agg(to_jsonb(p)), '[]'::jsonb),
    'pagination', jsonb_build_object(
      'total', (SELECT cnt FROM total),
      'limit', _limit,
      'offset', _offset,
      'has_next', _offset + _limit < (SELECT cnt FROM total),
      'has_prev', _offset > 0
    )
  )
  FROM page p;
$$;

CREATE OR REPLACE FUNCTION api.get_compute_job(_job_id BIGINT)
RETURNS JSONB
LANGUAGE sql STABLE
AS $$
  SELECT to_jsonb(j)
  FROM api.compute_jobs j
  WHERE j.job_id = _job_id;
$$;

CREATE OR REPLACE FUNCTION api.get_compute_benchmarks(
  _limit INT DEFAULT 20,
  _offset INT DEFAULT 0,
  _status TEXT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE sql STABLE
AS $$
  WITH filtered AS (
    SELECT *
    FROM api.compute_benchmarks
    WHERE (_status IS NULL OR status = _status)
  ),
  total AS (
    SELECT COUNT(*) AS cnt FROM filtered
  ),
  page AS (
    SELECT * FROM filtered
    ORDER BY submit_time DESC NULLS LAST
    LIMIT _limit OFFSET _offset
  )
  SELECT jsonb_build_object(
    'data', COALESCE(jsonb_agg(to_jsonb(p)), '[]'::jsonb),
    'pagination', jsonb_build_object(
      'total', (SELECT cnt FROM total),
      'limit', _limit,
      'offset', _offset,
      'has_next', _offset + _limit < (SELECT cnt FROM total),
      'has_prev', _offset > 0
    )
  )
  FROM page p;
$$;

-- ============================================================================
-- Views
-- ============================================================================

CREATE OR REPLACE VIEW api.compute_stats AS
SELECT
  (SELECT COUNT(*) FROM api.compute_jobs) AS total_jobs,
  (SELECT COUNT(*) FROM api.compute_jobs WHERE status = 'PENDING') AS pending_jobs,
  (SELECT COUNT(*) FROM api.compute_jobs WHERE status = 'COMPLETED') AS completed_jobs,
  (SELECT COUNT(*) FROM api.compute_jobs WHERE status = 'FAILED') AS failed_jobs,
  (SELECT COUNT(*) FROM api.compute_benchmarks) AS total_benchmarks,
  (SELECT COUNT(*) FROM api.compute_benchmarks WHERE status = 'COMPLETED') AS completed_benchmarks;

-- ============================================================================
-- Grants
-- ============================================================================

GRANT SELECT ON api.compute_jobs TO web_anon;
GRANT SELECT ON api.compute_benchmarks TO web_anon;
GRANT SELECT ON api.compute_seed_contributions TO web_anon;
GRANT SELECT ON api.compute_committee_proposals TO web_anon;
GRANT SELECT ON api.compute_stats TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_compute_jobs(INT, INT, TEXT, TEXT, TEXT) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_compute_job(BIGINT) TO web_anon;
GRANT EXECUTE ON FUNCTION api.get_compute_benchmarks(INT, INT, TEXT) TO web_anon;

COMMIT;
