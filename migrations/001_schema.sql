-- =============================================================================
-- YACI Explorer APIs - base schema
--
-- This is the complete current database layout, generated from the historical
-- migration chain. It is the baseline for fresh deployments; re-running it is
-- safe (all statements are idempotent). Future schema changes belong in a new
-- numbered migration applied after this file.
-- =============================================================================

BEGIN;

DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'web_anon') THEN
    CREATE ROLE web_anon NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'analytics_admin') THEN
    CREATE ROLE analytics_admin NOLOGIN;
  END IF;
END
$$;

-- Schema, extensions, tables, views, functions, triggers, grants
--
--


SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: api; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA IF NOT EXISTS api;


--
-- Name: pg_stat_statements; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public;


--
-- Name: EXTENSION pg_stat_statements; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pg_stat_statements IS 'track planning and execution statistics of all SQL statements executed';


--
-- Name: pgcrypto; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;


--
-- Name: EXTENSION pgcrypto; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pgcrypto IS 'cryptographic functions';


--
-- Name: message_with_error; Type: TYPE; Schema: api; Owner: -
--

DO $$
BEGIN
    CREATE TYPE api.message_with_error AS (
    	id text,
    	message_index integer,
    	type text,
    	sender text,
    	mentions text[],
    	metadata jsonb,
    	error text
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;


--
-- Name: _normalize_rate(numeric); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api._normalize_rate(val numeric) RETURNS numeric
    LANGUAGE sql IMMUTABLE
    AS $$
  SELECT CASE WHEN val IS NOT NULL AND val > 1 THEN val / 1e18 ELSE val END;
$$;


--
-- Name: backfill_block_signatures(bigint, integer); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.backfill_block_signatures(_start_height bigint DEFAULT NULL::bigint, _batch_size integer DEFAULT 1000) RETURNS TABLE(blocks_processed integer, signatures_extracted integer)
    LANGUAGE plpgsql
    AS $$
DECLARE
  rec RECORD;
  total_blocks INTEGER := 0;
  total_sigs INTEGER := 0;
  extracted INTEGER;
  actual_start BIGINT;
BEGIN
  -- Determine start height
  IF _start_height IS NULL THEN
    -- Start from where we left off, or block 1
    SELECT COALESCE(MAX(height), 0) + 1 INTO actual_start FROM api.validator_block_signatures;
  ELSE
    actual_start := _start_height;
  END IF;

  -- Process blocks in batches
  FOR rec IN
    SELECT id, data
    FROM api.blocks_raw
    WHERE id >= actual_start
    ORDER BY id
    LIMIT _batch_size
  LOOP
    extracted := api.extract_block_signatures(rec.id, rec.data);
    total_blocks := total_blocks + 1;
    total_sigs := total_sigs + extracted;
  END LOOP;

  RETURN QUERY SELECT total_blocks, total_sigs;
END;
$$;


--
-- Name: backfill_finalize_block_events(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.backfill_finalize_block_events() RETURNS TABLE(events_processed integer, jailing_events_created integer)
    LANGUAGE plpgsql
    AS $$
DECLARE
  rec RECORD;
  events JSONB;
  event_item JSONB;
  event_idx INTEGER;
  event_type TEXT;
  attrs JSONB;
  attr_item JSONB;
  total_events INTEGER := 0;
  total_jailing INTEGER := 0;
BEGIN
  FOR rec IN SELECT height, data FROM api.block_results_raw ORDER BY height
  LOOP
    events := COALESCE(
      rec.data->'finalizeBlockEvents',
      rec.data->'finalize_block_events',
      '[]'::JSONB
    );

    IF jsonb_array_length(events) = 0 THEN
      CONTINUE;
    END IF;

    event_idx := 0;
    FOR event_item IN SELECT * FROM jsonb_array_elements(events)
    LOOP
      event_type := event_item->>'type';

      attrs := '{}';
      FOR attr_item IN SELECT * FROM jsonb_array_elements(COALESCE(event_item->'attributes', '[]'::JSONB))
      LOOP
        attrs := attrs || jsonb_build_object(
          COALESCE(attr_item->>'key', ''),
          COALESCE(attr_item->>'value', '')
        );
      END LOOP;

      INSERT INTO api.finalize_block_events (height, event_index, event_type, attributes)
      VALUES (rec.height, event_idx, event_type, attrs)
      ON CONFLICT (height, event_index) DO NOTHING;

      total_events := total_events + 1;

      IF event_type IN ('slash', 'liveness', 'jail') THEN
        INSERT INTO api.jailing_events (
          validator_address,
          height,
          prev_block_flag,
          current_block_flag
        ) VALUES (
          COALESCE(attrs->>'validator', attrs->>'address', ''),
          rec.height,
          'FINALIZE_BLOCK_EVENT',
          event_type
        )
        ON CONFLICT (validator_address, height) DO NOTHING;

        total_jailing := total_jailing + 1;
      END IF;

      event_idx := event_idx + 1;
    END LOOP;
  END LOOP;

  RETURN QUERY SELECT total_events, total_jailing;
END;
$$;


--
-- Name: backfill_jailing_events(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.backfill_jailing_events() RETURNS TABLE(events_found integer, addresses_mapped integer)
    LANGUAGE plpgsql
    AS $$
DECLARE
  block_rec RECORD;
  prev_block_rec RECORD;
  current_sigs JSONB;
  prev_sigs JSONB;
  sig JSONB;
  prev_sig JSONB;
  val_addr TEXT;
  current_flag TEXT;
  prev_flag TEXT;
  events_count INT := 0;
  addr_count INT := 0;
BEGIN
  -- Iterate through all blocks starting from height 2
  FOR block_rec IN
    SELECT id, data FROM api.blocks_raw WHERE id > 1 ORDER BY id
  LOOP
    -- Get current block signatures
    current_sigs := COALESCE(
      block_rec.data->'block'->'last_commit'->'signatures',
      block_rec.data->'block'->'lastCommit'->'signatures',
      '[]'::JSONB
    );

    -- Get previous block
    SELECT id, data INTO prev_block_rec
    FROM api.blocks_raw
    WHERE id = block_rec.id - 1;

    IF prev_block_rec IS NULL THEN
      CONTINUE;
    END IF;

    -- Get previous block signatures
    prev_sigs := COALESCE(
      prev_block_rec.data->'block'->'last_commit'->'signatures',
      prev_block_rec.data->'block'->'lastCommit'->'signatures',
      '[]'::JSONB
    );

    -- Compare signatures
    FOR sig IN SELECT * FROM jsonb_array_elements(current_sigs)
    LOOP
      val_addr := COALESCE(sig->>'validatorAddress', sig->>'validator_address');
      current_flag := COALESCE(sig->>'blockIdFlag', sig->>'block_id_flag');

      IF val_addr IS NULL OR val_addr = '' THEN
        CONTINUE;
      END IF;

      -- Map consensus addresses
      IF current_flag IN ('BLOCK_ID_FLAG_COMMIT', 'BLOCK_ID_FLAG_NIL') THEN
        INSERT INTO api.validator_consensus_addresses (consensus_address, first_seen_height)
        VALUES (val_addr, block_rec.id)
        ON CONFLICT (consensus_address) DO NOTHING;
        GET DIAGNOSTICS addr_count = ROW_COUNT;
      END IF;

      -- Look for jailing (ABSENT now, was signing before)
      IF current_flag != 'BLOCK_ID_FLAG_ABSENT' THEN
        CONTINUE;
      END IF;

      prev_flag := NULL;
      FOR prev_sig IN SELECT * FROM jsonb_array_elements(prev_sigs)
      LOOP
        IF COALESCE(prev_sig->>'validatorAddress', prev_sig->>'validator_address') = val_addr THEN
          prev_flag := COALESCE(prev_sig->>'blockIdFlag', prev_sig->>'block_id_flag');
          EXIT;
        END IF;
      END LOOP;

      IF prev_flag IN ('BLOCK_ID_FLAG_COMMIT', 'BLOCK_ID_FLAG_NIL') THEN
        INSERT INTO api.jailing_events (
          validator_address, height, prev_block_flag, current_block_flag
        ) VALUES (
          val_addr, block_rec.id - 1, prev_flag, 'BLOCK_ID_FLAG_ABSENT'
        )
        ON CONFLICT (validator_address, height) DO NOTHING;
        GET DIAGNOSTICS events_count = ROW_COUNT;
      END IF;
    END LOOP;
  END LOOP;

  RETURN QUERY SELECT events_count, addr_count;
END;
$$;


--
-- Name: backfill_validator_consensus_addresses(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.backfill_validator_consensus_addresses() RETURNS TABLE(processed integer, updated integer)
    LANGUAGE plpgsql
    AS $$
DECLARE
  msg RECORD;
  raw_data JSONB;
  pubkey_data JSONB;
  pubkey_base64 TEXT;
  consensus_addr TEXT;
  valoper_addr TEXT;
  processed_count INTEGER := 0;
  updated_count INTEGER := 0;
BEGIN
  FOR msg IN
    SELECT m.id, m.type, t.height, mr.data
    FROM api.messages_main m
    JOIN api.messages_raw mr ON mr.id = m.id
    JOIN api.transactions_main t ON t.id = m.id
    WHERE m.type LIKE '%MsgCreateValidator'
    ORDER BY t.height
  LOOP
    processed_count := processed_count + 1;
    raw_data := msg.data;

    -- Extract pubkey
    pubkey_data := COALESCE(raw_data->'pubkey', raw_data->'pub_key');
    IF pubkey_data IS NULL THEN
      CONTINUE;
    END IF;

    pubkey_base64 := pubkey_data->>'key';
    IF pubkey_base64 IS NULL OR pubkey_base64 = '' THEN
      CONTINUE;
    END IF;

    -- Compute consensus address
    consensus_addr := api.compute_consensus_address(pubkey_base64);

    -- Get validator operator address
    valoper_addr := COALESCE(raw_data->>'validatorAddress', raw_data->>'validator_address');
    IF valoper_addr IS NULL OR valoper_addr = '' THEN
      CONTINUE;
    END IF;

    -- Update validators table
    UPDATE api.validators
    SET consensus_address = consensus_addr
    WHERE operator_address = valoper_addr
      AND (consensus_address IS NULL OR consensus_address = '');

    IF FOUND THEN
      updated_count := updated_count + 1;
    END IF;

    -- Update mapping table
    INSERT INTO api.validator_consensus_addresses (
      consensus_address,
      operator_address,
      first_seen_height
    ) VALUES (
      consensus_addr,
      valoper_addr,
      msg.height
    )
    ON CONFLICT (consensus_address) DO UPDATE
    SET operator_address = EXCLUDED.operator_address
    WHERE api.validator_consensus_addresses.operator_address IS NULL;
  END LOOP;

  RETURN QUERY SELECT processed_count, updated_count;
END;
$$;


--
-- Name: compute_consensus_address(text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.compute_consensus_address(_pubkey_base64 text) RETURNS text
    LANGUAGE plpgsql IMMUTABLE
    AS $$
DECLARE
  pubkey_bytes BYTEA;
  hash_bytes BYTEA;
  address_bytes BYTEA;
BEGIN
  -- Decode base64 pubkey
  pubkey_bytes := decode(_pubkey_base64, 'base64');

  -- SHA256 hash of pubkey bytes
  hash_bytes := digest(pubkey_bytes, 'sha256');

  -- Take first 20 bytes
  address_bytes := substring(hash_bytes from 1 for 20);

  -- Return as uppercase hex (matching CometBFT format)
  RETURN upper(encode(address_bytes, 'hex'));
END;
$$;


--
-- Name: compute_proposal_tally(bigint); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.compute_proposal_tally(_proposal_id bigint) RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  SELECT jsonb_build_object(
    'yes', COUNT(*) FILTER (WHERE m.metadata->>'option' = 'VOTE_OPTION_YES'),
    'no', COUNT(*) FILTER (WHERE m.metadata->>'option' = 'VOTE_OPTION_NO'),
    'abstain', COUNT(*) FILTER (WHERE m.metadata->>'option' = 'VOTE_OPTION_ABSTAIN'),
    'no_with_veto', COUNT(*) FILTER (WHERE m.metadata->>'option' = 'VOTE_OPTION_NO_WITH_VETO')
  )
  FROM api.messages_main m
  WHERE m.type LIKE '%MsgVote%'
  AND (m.metadata->>'proposalId')::bigint = _proposal_id;
$$;


--
-- Name: detect_compute_validation(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.detect_compute_validation() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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
$$;


--
-- Name: detect_jailing_from_block(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.detect_jailing_from_block() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  current_height BIGINT;
  prev_block RECORD;
  current_sigs JSONB;
  prev_sigs JSONB;
  sig JSONB;
  prev_sig JSONB;
  val_addr TEXT;
  current_flag TEXT;
  prev_flag TEXT;
  is_absent BOOLEAN;
  was_active BOOLEAN;
BEGIN
  current_height := (NEW.data->'block'->'header'->>'height')::BIGINT;

  current_sigs := COALESCE(
    NEW.data->'block'->'last_commit'->'signatures',
    NEW.data->'block'->'lastCommit'->'signatures',
    '[]'::JSONB
  );

  SELECT data INTO prev_block
  FROM api.blocks_raw
  WHERE id = current_height - 1;

  IF prev_block IS NULL THEN
    RETURN NEW;
  END IF;

  prev_sigs := COALESCE(
    prev_block.data->'block'->'last_commit'->'signatures',
    prev_block.data->'block'->'lastCommit'->'signatures',
    '[]'::JSONB
  );

  FOR sig IN SELECT * FROM jsonb_array_elements(current_sigs)
  LOOP
    val_addr := api.normalize_consensus_address(
      COALESCE(sig->>'validatorAddress', sig->>'validator_address')
    );
    current_flag := COALESCE(sig->>'blockIdFlag', sig->>'block_id_flag');

    IF val_addr IS NULL OR val_addr = '' THEN
      CONTINUE;
    END IF;

    -- Handle both string enum and integer formats for ABSENT check
    is_absent := (current_flag = 'BLOCK_ID_FLAG_ABSENT' OR current_flag = '1');

    IF NOT is_absent THEN
      -- Record consensus address mapping for active validators
      was_active := (current_flag IN ('BLOCK_ID_FLAG_COMMIT', 'BLOCK_ID_FLAG_NIL', '2', '3'));
      IF was_active THEN
        INSERT INTO api.validator_consensus_addresses (consensus_address, first_seen_height)
        VALUES (val_addr, current_height)
        ON CONFLICT (consensus_address) DO NOTHING;
      END IF;
      CONTINUE;
    END IF;

    -- Validator is now absent, check previous block status
    prev_flag := NULL;
    FOR prev_sig IN SELECT * FROM jsonb_array_elements(prev_sigs)
    LOOP
      IF api.normalize_consensus_address(
        COALESCE(prev_sig->>'validatorAddress', prev_sig->>'validator_address')
      ) = val_addr THEN
        prev_flag := COALESCE(prev_sig->>'blockIdFlag', prev_sig->>'block_id_flag');
        EXIT;
      END IF;
    END LOOP;

    -- Was actively signing before (handle both string enum and integer formats)
    was_active := (prev_flag IN ('BLOCK_ID_FLAG_COMMIT', 'BLOCK_ID_FLAG_NIL', '2', '3'));

    IF was_active THEN
      INSERT INTO api.jailing_events (
        validator_address, height, prev_block_flag, current_block_flag
      ) VALUES (
        val_addr, current_height - 1, prev_flag, current_flag
      )
      ON CONFLICT (validator_address, height) DO NOTHING;
    END IF;
  END LOOP;

  RETURN NEW;
END;
$$;


--
-- Name: detect_proposal_submission(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.detect_proposal_submission() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  msg_record RECORD;
  raw_data JSONB;
  prop_id BIGINT;
  prop_title TEXT;
  prop_summary TEXT;
  prop_metadata TEXT;
BEGIN
  -- Check each message in the transaction for governance proposals
  FOR msg_record IN
    SELECT m.id, m.message_index, m.type, m.metadata, m.sender
    FROM api.messages_main m
    WHERE m.id = NEW.id
    AND m.type LIKE '%MsgSubmitProposal%'
  LOOP
    prop_id := NULL;
    prop_title := NULL;
    prop_summary := NULL;
    prop_metadata := NULL;

    -- Get raw message data for full content
    SELECT data INTO raw_data
    FROM api.messages_raw
    WHERE id = msg_record.id AND message_index = msg_record.message_index;

    -- Extract proposal_id from metadata or events
    IF msg_record.metadata ? 'proposalId' THEN
      prop_id := (msg_record.metadata->>'proposalId')::BIGINT;
    END IF;

    -- Fallback: get from submit_proposal event
    IF prop_id IS NULL THEN
      SELECT (e.attr_value)::BIGINT INTO prop_id
      FROM api.events_main e
      WHERE e.id = NEW.id
      AND e.event_type = 'submit_proposal'
      AND e.attr_key = 'proposal_id'
      LIMIT 1;
    END IF;

    -- Extract title and summary from raw message data
    IF raw_data IS NOT NULL THEN
      prop_title := raw_data->>'title';
      prop_summary := raw_data->>'summary';
      prop_metadata := raw_data->>'metadata';
    END IF;

    IF prop_id IS NOT NULL THEN
      INSERT INTO api.governance_proposals (
        proposal_id,
        submit_tx_hash,
        submit_height,
        submit_time,
        proposer,
        title,
        summary,
        metadata,
        status
      ) VALUES (
        prop_id,
        NEW.id,
        NEW.height,
        NEW.timestamp,
        msg_record.sender,
        prop_title,
        prop_summary,
        prop_metadata,
        'PROPOSAL_STATUS_DEPOSIT_PERIOD'
      )
      ON CONFLICT (proposal_id) DO UPDATE SET
        title = COALESCE(EXCLUDED.title, api.governance_proposals.title),
        summary = COALESCE(EXCLUDED.summary, api.governance_proposals.summary),
        metadata = COALESCE(EXCLUDED.metadata, api.governance_proposals.metadata),
        last_updated = NOW();
    END IF;
  END LOOP;

  RETURN NEW;
END;
$$;


--
-- Name: detect_reputation_messages(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.detect_reputation_messages() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  msg_record RECORD;
  raw_data JSONB;
BEGIN
  FOR msg_record IN
    SELECT m.id, m.message_index, m.type, m.metadata, m.sender
    FROM api.messages_main m
    WHERE m.id = NEW.id
    AND m.type LIKE '%republic.reputation%'
  LOOP
    raw_data := NULL;
    SELECT data INTO raw_data
    FROM api.messages_raw
    WHERE id = msg_record.id AND message_index = msg_record.message_index;

    -- MsgSetIPFSAddress
    IF msg_record.type LIKE '%MsgSetIPFSAddress' AND raw_data IS NOT NULL THEN
      INSERT INTO api.validator_ipfs_addresses (
        validator_address, ipfs_multiaddrs, ipfs_peer_id,
        tx_hash, height, timestamp
      ) VALUES (
        COALESCE(raw_data->>'validatorAddress', msg_record.sender),
        CASE
          WHEN raw_data ? 'ipfsMultiaddrs' AND jsonb_typeof(raw_data->'ipfsMultiaddrs') = 'array'
          THEN ARRAY(SELECT jsonb_array_elements_text(raw_data->'ipfsMultiaddrs'))
          ELSE NULL
        END,
        raw_data->>'ipfsPeerId',
        NEW.id, NEW.height, NEW.timestamp
      )
      ON CONFLICT (validator_address) DO UPDATE SET
        ipfs_multiaddrs = EXCLUDED.ipfs_multiaddrs,
        ipfs_peer_id = COALESCE(EXCLUDED.ipfs_peer_id, api.validator_ipfs_addresses.ipfs_peer_id),
        tx_hash = EXCLUDED.tx_hash,
        height = EXCLUDED.height,
        timestamp = EXCLUDED.timestamp,
        updated_at = NOW();
    END IF;
  END LOOP;

  RETURN NEW;
END;
$$;


--
-- Name: detect_slashing_messages(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.detect_slashing_messages() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  msg_record RECORD;
  raw_data JSONB;
  slash_condition TEXT;
  slash_id BIGINT;
BEGIN
  FOR msg_record IN
    SELECT m.id, m.message_index, m.type, m.metadata, m.sender
    FROM api.messages_main m
    WHERE m.id = NEW.id
    AND m.type LIKE '%republic.slashingplus%'
  LOOP
    raw_data := NULL;
    SELECT data INTO raw_data
    FROM api.messages_raw
    WHERE id = msg_record.id AND message_index = msg_record.message_index;

    IF raw_data IS NULL THEN
      CONTINUE;
    END IF;

    -- Determine condition from message type
    slash_condition := CASE
      WHEN msg_record.type LIKE '%ComputeMisconduct%' THEN 'COMPUTE_MISCONDUCT'
      WHEN msg_record.type LIKE '%ReputationDegradation%' THEN 'REPUTATION_DEGRADATION'
      WHEN msg_record.type LIKE '%DelegatedCollusion%' THEN 'DELEGATED_COLLUSION'
      ELSE NULL
    END;

    IF slash_condition IS NULL THEN
      CONTINUE;
    END IF;

    -- Extract slashing_id from events if available
    slash_id := NULL;
    SELECT (e.attr_value)::BIGINT INTO slash_id
    FROM api.events_main e
    WHERE e.id = NEW.id
    AND e.attr_key = 'slashing_id'
    LIMIT 1;

    INSERT INTO api.slashing_records (
      slashing_id, validator_address, submitter, condition,
      evidence_type, evidence_data,
      tx_hash, height, timestamp
    ) VALUES (
      slash_id,
      COALESCE(raw_data->>'validatorAddress', raw_data->'evidence'->>'validatorAddress', ''),
      COALESCE(raw_data->>'submitter', msg_record.sender),
      slash_condition,
      raw_data->'evidence'->>'@type',
      raw_data->'evidence',
      NEW.id, NEW.height, NEW.timestamp
    )
    ON CONFLICT (slashing_id) DO NOTHING;
  END LOOP;

  RETURN NEW;
END;
$$;


--
-- Name: detect_staking_from_message(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.detect_staking_from_message() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  raw_data JSONB;
  tx_record RECORD;
  val_addr TEXT;
  del_addr TEXT;
  dst_addr TEXT;
  src_addr TEXT;
BEGIN
  -- Only process staking message types
  IF NEW.type NOT LIKE '%MsgDelegate'
     AND NEW.type NOT LIKE '%MsgUndelegate'
     AND NEW.type NOT LIKE '%MsgBeginRedelegate'
     AND NEW.type NOT LIKE '%MsgCreateValidator'
     AND NEW.type NOT LIKE '%MsgEditValidator'
     AND NEW.type NOT LIKE '%MsgUnjail' THEN
    RETURN NEW;
  END IF;

  -- Get transaction context (height, timestamp)
  SELECT height, timestamp INTO tx_record
  FROM api.transactions_main
  WHERE id = NEW.id;

  -- If transaction not found (shouldn't happen), skip
  IF tx_record IS NULL THEN
    RETURN NEW;
  END IF;

  -- Get raw message data
  SELECT data INTO raw_data
  FROM api.messages_raw
  WHERE id = NEW.id AND message_index = NEW.message_index;

  -- Extract addresses with fallbacks for both camelCase and snake_case
  val_addr := COALESCE(
    raw_data->>'validatorAddress',
    raw_data->>'validator_address',
    NEW.metadata->>'validatorAddress',
    NEW.metadata->>'validator_address',
    ''
  );
  del_addr := COALESCE(
    raw_data->>'delegatorAddress',
    raw_data->>'delegator_address',
    NEW.sender,
    ''
  );

  -- Skip if no validator address found
  IF val_addr = '' THEN
    RETURN NEW;
  END IF;

  -- MsgDelegate (but not MsgBeginRedelegate)
  IF NEW.type LIKE '%MsgDelegate' AND NEW.type NOT LIKE '%MsgBeginRedelegate' THEN
    INSERT INTO api.delegation_events (
      event_type, delegator_address, validator_address,
      amount, denom, tx_hash, height, timestamp
    ) VALUES (
      'DELEGATE', del_addr, val_addr,
      NULLIF(COALESCE(raw_data->'amount'->>'amount', raw_data->'coin'->>'amount'), '')::NUMERIC,
      COALESCE(raw_data->'amount'->>'denom', raw_data->'coin'->>'denom'),
      NEW.id, tx_record.height, tx_record.timestamp
    )
    ON CONFLICT DO NOTHING;

    PERFORM pg_notify('validator_refresh', val_addr);

  -- MsgUndelegate
  ELSIF NEW.type LIKE '%MsgUndelegate' THEN
    INSERT INTO api.delegation_events (
      event_type, delegator_address, validator_address,
      amount, denom, tx_hash, height, timestamp
    ) VALUES (
      'UNDELEGATE', del_addr, val_addr,
      NULLIF(COALESCE(raw_data->'amount'->>'amount', raw_data->'coin'->>'amount'), '')::NUMERIC,
      COALESCE(raw_data->'amount'->>'denom', raw_data->'coin'->>'denom'),
      NEW.id, tx_record.height, tx_record.timestamp
    )
    ON CONFLICT DO NOTHING;

    PERFORM pg_notify('validator_refresh', val_addr);

  -- MsgBeginRedelegate
  ELSIF NEW.type LIKE '%MsgBeginRedelegate' THEN
    dst_addr := COALESCE(raw_data->>'validatorDstAddress', raw_data->>'validator_dst_address', '');
    src_addr := COALESCE(raw_data->>'validatorSrcAddress', raw_data->>'validator_src_address', '');

    INSERT INTO api.delegation_events (
      event_type, delegator_address, validator_address,
      src_validator_address, amount, denom,
      tx_hash, height, timestamp
    ) VALUES (
      'REDELEGATE', del_addr,
      dst_addr, src_addr,
      NULLIF(COALESCE(raw_data->'amount'->>'amount', raw_data->'coin'->>'amount'), '')::NUMERIC,
      COALESCE(raw_data->'amount'->>'denom', raw_data->'coin'->>'denom'),
      NEW.id, tx_record.height, tx_record.timestamp
    )
    ON CONFLICT DO NOTHING;

    -- Notify both source and destination validators
    IF dst_addr <> '' THEN
      PERFORM pg_notify('validator_refresh', dst_addr);
    END IF;
    IF src_addr <> '' THEN
      PERFORM pg_notify('validator_refresh', src_addr);
    END IF;

  -- MsgCreateValidator
  ELSIF NEW.type LIKE '%MsgCreateValidator' THEN
    INSERT INTO api.delegation_events (
      event_type, delegator_address, validator_address,
      amount, denom, tx_hash, height, timestamp
    ) VALUES (
      'CREATE_VALIDATOR', del_addr, val_addr,
      NULLIF(COALESCE(
        raw_data->'value'->>'amount',
        raw_data->'selfDelegation'->>'amount',
        raw_data->'self_delegation'->>'amount'
      ), '')::NUMERIC,
      COALESCE(
        raw_data->'value'->>'denom',
        raw_data->'selfDelegation'->>'denom',
        raw_data->'self_delegation'->>'denom'
      ),
      NEW.id, tx_record.height, tx_record.timestamp
    )
    ON CONFLICT DO NOTHING;

    -- Upsert validator record
    INSERT INTO api.validators (
      operator_address, moniker, identity, website, details,
      commission_rate, commission_max_rate, commission_max_change_rate,
      min_self_delegation, tokens, status, creation_height, first_seen_tx
    ) VALUES (
      val_addr,
      COALESCE(raw_data->'description'->>'moniker', raw_data->>'moniker'),
      COALESCE(raw_data->'description'->>'identity', raw_data->>'identity'),
      COALESCE(raw_data->'description'->>'website', raw_data->>'website'),
      COALESCE(raw_data->'description'->>'details', raw_data->>'details'),
      NULLIF(COALESCE(
        raw_data->'commission'->'commissionRates'->>'rate',
        raw_data->'commission'->'commission_rates'->>'rate'
      ), '')::NUMERIC,
      NULLIF(COALESCE(
        raw_data->'commission'->'commissionRates'->>'maxRate',
        raw_data->'commission'->'commission_rates'->>'max_rate'
      ), '')::NUMERIC,
      NULLIF(COALESCE(
        raw_data->'commission'->'commissionRates'->>'maxChangeRate',
        raw_data->'commission'->'commission_rates'->>'max_change_rate'
      ), '')::NUMERIC,
      NULLIF(COALESCE(raw_data->>'minSelfDelegation', raw_data->>'min_self_delegation'), '')::NUMERIC,
      NULLIF(COALESCE(
        raw_data->'value'->>'amount',
        raw_data->'selfDelegation'->>'amount'
      ), '')::NUMERIC,
      'BOND_STATUS_BONDED',
      tx_record.height,
      NEW.id
    )
    ON CONFLICT (operator_address) DO UPDATE SET
      moniker = COALESCE(EXCLUDED.moniker, api.validators.moniker),
      identity = COALESCE(EXCLUDED.identity, api.validators.identity),
      website = COALESCE(EXCLUDED.website, api.validators.website),
      details = COALESCE(EXCLUDED.details, api.validators.details),
      creation_height = COALESCE(api.validators.creation_height, EXCLUDED.creation_height),
      first_seen_tx = COALESCE(api.validators.first_seen_tx, EXCLUDED.first_seen_tx),
      updated_at = NOW();

    PERFORM pg_notify('validator_refresh', val_addr);

  -- MsgEditValidator
  ELSIF NEW.type LIKE '%MsgEditValidator' THEN
    INSERT INTO api.delegation_events (
      event_type, delegator_address, validator_address,
      tx_hash, height, timestamp
    ) VALUES (
      'EDIT_VALIDATOR', NEW.sender, val_addr,
      NEW.id, tx_record.height, tx_record.timestamp
    )
    ON CONFLICT DO NOTHING;

    -- Update validator record
    UPDATE api.validators SET
      moniker = COALESCE(
        NULLIF(COALESCE(raw_data->'description'->>'moniker', raw_data->>'moniker'), '[do-not-modify]'),
        moniker
      ),
      identity = COALESCE(
        NULLIF(COALESCE(raw_data->'description'->>'identity', raw_data->>'identity'), '[do-not-modify]'),
        identity
      ),
      website = COALESCE(
        NULLIF(COALESCE(raw_data->'description'->>'website', raw_data->>'website'), '[do-not-modify]'),
        website
      ),
      details = COALESCE(
        NULLIF(COALESCE(raw_data->'description'->>'details', raw_data->>'details'), '[do-not-modify]'),
        details
      ),
      commission_rate = COALESCE(
        NULLIF(raw_data->>'commissionRate', '')::NUMERIC,
        commission_rate
      ),
      updated_at = NOW()
    WHERE operator_address = val_addr;

    PERFORM pg_notify('validator_refresh', val_addr);

  -- MsgUnjail
  ELSIF NEW.type LIKE '%MsgUnjail' THEN
    INSERT INTO api.delegation_events (
      event_type, delegator_address, validator_address,
      tx_hash, height, timestamp
    ) VALUES (
      'UNJAIL', del_addr, val_addr,
      NEW.id, tx_record.height, tx_record.timestamp
    )
    ON CONFLICT DO NOTHING;

    -- Immediately clear jailed flag
    UPDATE api.validators SET
      jailed = FALSE,
      updated_at = NOW()
    WHERE operator_address = val_addr;

    PERFORM pg_notify('validator_refresh', val_addr);
  END IF;

  RETURN NEW;
END;
$$;


--
-- Name: detect_staking_messages(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.detect_staking_messages() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  msg_record RECORD;
  raw_data JSONB;
BEGIN
  FOR msg_record IN
    SELECT m.id, m.message_index, m.type, m.metadata, m.sender
    FROM api.messages_main m
    WHERE m.id = NEW.id
    AND (
      m.type LIKE '%MsgDelegate'
      OR m.type LIKE '%MsgUndelegate'
      OR m.type LIKE '%MsgBeginRedelegate'
      OR m.type LIKE '%MsgCreateValidator'
      OR m.type LIKE '%MsgEditValidator'
    )
  LOOP
    raw_data := NULL;
    SELECT data INTO raw_data
    FROM api.messages_raw
    WHERE id = msg_record.id AND message_index = msg_record.message_index;

    -- MsgDelegate
    IF msg_record.type LIKE '%MsgDelegate' AND msg_record.type NOT LIKE '%MsgBeginRedelegate' THEN
      INSERT INTO api.delegation_events (
        event_type, delegator_address, validator_address,
        amount, denom, tx_hash, height, timestamp
      ) VALUES (
        'DELEGATE',
        COALESCE(raw_data->>'delegatorAddress', msg_record.sender),
        COALESCE(raw_data->>'validatorAddress', ''),
        NULLIF(raw_data->'amount'->>'amount', '')::NUMERIC,
        raw_data->'amount'->>'denom',
        NEW.id, NEW.height, NEW.timestamp
      );

    -- MsgUndelegate
    ELSIF msg_record.type LIKE '%MsgUndelegate' THEN
      INSERT INTO api.delegation_events (
        event_type, delegator_address, validator_address,
        amount, denom, tx_hash, height, timestamp
      ) VALUES (
        'UNDELEGATE',
        COALESCE(raw_data->>'delegatorAddress', msg_record.sender),
        COALESCE(raw_data->>'validatorAddress', ''),
        NULLIF(raw_data->'amount'->>'amount', '')::NUMERIC,
        raw_data->'amount'->>'denom',
        NEW.id, NEW.height, NEW.timestamp
      );

    -- MsgBeginRedelegate
    ELSIF msg_record.type LIKE '%MsgBeginRedelegate' THEN
      INSERT INTO api.delegation_events (
        event_type, delegator_address, validator_address,
        src_validator_address, amount, denom,
        tx_hash, height, timestamp
      ) VALUES (
        'REDELEGATE',
        COALESCE(raw_data->>'delegatorAddress', msg_record.sender),
        COALESCE(raw_data->>'validatorDstAddress', ''),
        COALESCE(raw_data->>'validatorSrcAddress', ''),
        NULLIF(raw_data->'amount'->>'amount', '')::NUMERIC,
        raw_data->'amount'->>'denom',
        NEW.id, NEW.height, NEW.timestamp
      );

    -- MsgCreateValidator
    ELSIF msg_record.type LIKE '%MsgCreateValidator' THEN
      INSERT INTO api.delegation_events (
        event_type, delegator_address, validator_address,
        amount, denom, tx_hash, height, timestamp
      ) VALUES (
        'CREATE_VALIDATOR',
        COALESCE(raw_data->>'delegatorAddress', msg_record.sender),
        COALESCE(raw_data->>'validatorAddress', ''),
        NULLIF(raw_data->'value'->>'amount', '')::NUMERIC,
        raw_data->'value'->>'denom',
        NEW.id, NEW.height, NEW.timestamp
      );

      -- Upsert into validators table
      INSERT INTO api.validators (
        operator_address, moniker, identity, website, details,
        commission_rate, commission_max_rate, commission_max_change_rate,
        min_self_delegation, tokens, status,
        creation_height, first_seen_tx
      ) VALUES (
        COALESCE(raw_data->>'validatorAddress', ''),
        raw_data->'description'->>'moniker',
        raw_data->'description'->>'identity',
        raw_data->'description'->>'website',
        raw_data->'description'->>'details',
        NULLIF(raw_data->'commission'->'commissionRates'->>'rate', '')::NUMERIC,
        NULLIF(raw_data->'commission'->'commissionRates'->>'maxRate', '')::NUMERIC,
        NULLIF(raw_data->'commission'->'commissionRates'->>'maxChangeRate', '')::NUMERIC,
        NULLIF(raw_data->>'minSelfDelegation', '')::NUMERIC,
        NULLIF(raw_data->'value'->>'amount', '')::NUMERIC,
        'BOND_STATUS_BONDED',
        NEW.height,
        NEW.id
      )
      ON CONFLICT (operator_address) DO UPDATE SET
        moniker = COALESCE(EXCLUDED.moniker, api.validators.moniker),
        identity = COALESCE(EXCLUDED.identity, api.validators.identity),
        website = COALESCE(EXCLUDED.website, api.validators.website),
        details = COALESCE(EXCLUDED.details, api.validators.details),
        commission_rate = COALESCE(EXCLUDED.commission_rate, api.validators.commission_rate),
        commission_max_rate = COALESCE(EXCLUDED.commission_max_rate, api.validators.commission_max_rate),
        commission_max_change_rate = COALESCE(EXCLUDED.commission_max_change_rate, api.validators.commission_max_change_rate),
        min_self_delegation = COALESCE(EXCLUDED.min_self_delegation, api.validators.min_self_delegation),
        creation_height = COALESCE(api.validators.creation_height, EXCLUDED.creation_height),
        first_seen_tx = COALESCE(api.validators.first_seen_tx, EXCLUDED.first_seen_tx),
        updated_at = NOW();

    -- MsgEditValidator
    ELSIF msg_record.type LIKE '%MsgEditValidator' THEN
      INSERT INTO api.delegation_events (
        event_type, delegator_address, validator_address,
        tx_hash, height, timestamp
      ) VALUES (
        'EDIT_VALIDATOR',
        msg_record.sender,
        COALESCE(raw_data->>'validatorAddress', ''),
        NEW.id, NEW.height, NEW.timestamp
      );

      -- Update validators table
      UPDATE api.validators SET
        moniker = COALESCE(
          NULLIF(raw_data->'description'->>'moniker', '[do-not-modify]'),
          moniker
        ),
        identity = COALESCE(
          NULLIF(raw_data->'description'->>'identity', '[do-not-modify]'),
          identity
        ),
        website = COALESCE(
          NULLIF(raw_data->'description'->>'website', '[do-not-modify]'),
          website
        ),
        details = COALESCE(
          NULLIF(raw_data->'description'->>'details', '[do-not-modify]'),
          details
        ),
        commission_rate = COALESCE(
          NULLIF(raw_data->>'commissionRate', '')::NUMERIC,
          commission_rate
        ),
        min_self_delegation = COALESCE(
          NULLIF(raw_data->>'minSelfDelegation', '')::NUMERIC,
          min_self_delegation
        ),
        updated_at = NOW()
      WHERE operator_address = COALESCE(raw_data->>'validatorAddress', '');
    END IF;
  END LOOP;

  RETURN NEW;
END;
$$;


--
-- Name: extract_and_queue_ibc_denoms(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.extract_and_queue_ibc_denoms() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  _denom TEXT;
  _token_denom TEXT;
BEGIN
  -- Only process IBC transfer messages
  IF NEW.type NOT IN (
    '/ibc.applications.transfer.v1.MsgTransfer',
    '/ibc.core.channel.v1.MsgRecvPacket'
  ) THEN
    RETURN NEW;
  END IF;

  -- Extract denom from metadata
  _token_denom := NEW.metadata->'token'->>'denom';

  -- For MsgRecvPacket, try to extract from packet data
  IF _token_denom IS NULL AND NEW.type = '/ibc.core.channel.v1.MsgRecvPacket' THEN
    -- Packet data might be in different locations
    _token_denom := NEW.metadata->'packet'->'data'->>'denom';
    IF _token_denom IS NULL THEN
      _token_denom := NEW.metadata->'packetData'->>'denom';
    END IF;
  END IF;

  -- Queue if it's an IBC denom
  IF _token_denom IS NOT NULL AND _token_denom LIKE 'ibc/%' THEN
    PERFORM api.queue_unknown_ibc_denom(_token_denom);
  END IF;

  RETURN NEW;
END;
$$;


--
-- Name: extract_block_signatures(bigint, jsonb); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.extract_block_signatures(_height bigint, _block_data jsonb) RETURNS integer
    LANGUAGE plpgsql
    AS $_$
DECLARE
  block_time TIMESTAMPTZ;
  extracted_count INTEGER;
BEGIN
  block_time := (_block_data->'block'->'header'->>'time')::TIMESTAMPTZ;

  WITH sigs AS (
    SELECT
      row_number() OVER () - 1 AS sig_idx,
      api.normalize_consensus_address(COALESCE(
        s->>'validatorAddress',
        s->>'validator_address',
        ''
      )) AS validator_addr,
      CASE
        -- Handle string enum format
        WHEN COALESCE(s->>'blockIdFlag', s->>'block_id_flag', '') = 'BLOCK_ID_FLAG_ABSENT' THEN 1
        WHEN COALESCE(s->>'blockIdFlag', s->>'block_id_flag', '') = 'BLOCK_ID_FLAG_COMMIT' THEN 2
        WHEN COALESCE(s->>'blockIdFlag', s->>'block_id_flag', '') = 'BLOCK_ID_FLAG_NIL' THEN 3
        -- Handle integer format
        WHEN COALESCE(s->>'blockIdFlag', s->>'block_id_flag', '') ~ '^\d+$'
          THEN COALESCE(s->>'blockIdFlag', s->>'block_id_flag', '1')::INTEGER
        ELSE 1
      END AS flag
    FROM jsonb_array_elements(
      COALESCE(
        _block_data->'block'->'lastCommit'->'signatures',
        _block_data->'block'->'last_commit'->'signatures',
        '[]'::JSONB
      )
    ) AS s
  )
  INSERT INTO api.validator_block_signatures (
    height, validator_index, consensus_address, signed, block_id_flag, block_time
  )
  SELECT
    _height,
    sig_idx,
    validator_addr,
    (flag = 2),
    flag,
    block_time
  FROM sigs
  WHERE validator_addr != ''
  ORDER BY sig_idx
  ON CONFLICT (height, validator_index) DO UPDATE SET
    consensus_address = EXCLUDED.consensus_address,
    signed = EXCLUDED.signed,
    block_id_flag = EXCLUDED.block_id_flag,
    block_time = EXCLUDED.block_time;

  GET DIAGNOSTICS extracted_count = ROW_COUNT;
  RETURN extracted_count;
END;
$_$;


--
-- Name: extract_event_msg_index(jsonb); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.extract_event_msg_index(ev jsonb) RETURNS bigint
    LANGUAGE sql STABLE
    AS $$
  SELECT NULLIF(a->>'value','')::bigint
  FROM jsonb_array_elements(ev->'attributes') a
  WHERE a->>'key' = 'msg_index'
  LIMIT 1
$$;


--
-- Name: extract_finalize_block_events(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.extract_finalize_block_events() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  events JSONB;
BEGIN
  events := COALESCE(
    NEW.data->'finalizeBlockEvents',
    NEW.data->'finalize_block_events',
    '[]'::JSONB
  );

  IF jsonb_array_length(events) = 0 THEN
    RETURN NEW;
  END IF;

  -- Set-based INSERT for all events at once
  WITH event_data AS (
    SELECT
      row_number() OVER () - 1 AS event_idx,
      e->>'type' AS event_type,
      (
        SELECT jsonb_object_agg(
          COALESCE(a->>'key', ''),
          COALESCE(a->>'value', '')
        )
        FROM jsonb_array_elements(COALESCE(e->'attributes', '[]'::JSONB)) a
      ) AS attrs
    FROM jsonb_array_elements(events) AS e
  )
  INSERT INTO api.finalize_block_events (height, event_index, event_type, attributes)
  SELECT NEW.height, event_idx, event_type, attrs
  FROM event_data
  ORDER BY event_idx
  ON CONFLICT (height, event_index) DO UPDATE SET
    event_type = EXCLUDED.event_type,
    attributes = EXCLUDED.attributes;

  -- Handle jailing events: set-based INSERT
  WITH event_data AS (
    SELECT
      e->>'type' AS event_type,
      (
        SELECT jsonb_object_agg(
          COALESCE(a->>'key', ''),
          COALESCE(a->>'value', '')
        )
        FROM jsonb_array_elements(COALESCE(e->'attributes', '[]'::JSONB)) a
      ) AS attrs
    FROM jsonb_array_elements(events) AS e
    WHERE e->>'type' IN ('slash', 'liveness', 'jail')
  )
  INSERT INTO api.jailing_events (
    validator_address, height, prev_block_flag, current_block_flag
  )
  SELECT
    COALESCE(attrs->>'validator', attrs->>'address', ''),
    NEW.height,
    'FINALIZE_BLOCK_EVENT',
    event_type
  FROM event_data
  ON CONFLICT (validator_address, height) DO NOTHING;

  -- Update validator jailed status with advisory lock to prevent deadlocks
  -- across concurrent transactions updating the same validator rows
  PERFORM pg_advisory_xact_lock(hashtext('validator_jail_update'));

  WITH jail_addrs AS (
    SELECT DISTINCT COALESCE(
      (
        SELECT jsonb_object_agg(
          COALESCE(a->>'key', ''),
          COALESCE(a->>'value', '')
        )
        FROM jsonb_array_elements(COALESCE(e->'attributes', '[]'::JSONB)) a
      )->>'validator',
      (
        SELECT jsonb_object_agg(
          COALESCE(a->>'key', ''),
          COALESCE(a->>'value', '')
        )
        FROM jsonb_array_elements(COALESCE(e->'attributes', '[]'::JSONB)) a
      )->>'address',
      ''
    ) AS addr
    FROM jsonb_array_elements(events) AS e
    WHERE e->>'type' IN ('slash', 'liveness', 'jail')
  )
  UPDATE api.validators SET
    jailed = TRUE,
    updated_at = NOW()
  WHERE consensus_address IN (SELECT addr FROM jail_addrs WHERE addr != '')
     OR operator_address IN (
       SELECT vca.operator_address
       FROM api.validator_consensus_addresses vca
       WHERE vca.consensus_address IN (SELECT addr FROM jail_addrs WHERE addr != '')
     );

  RETURN NEW;
END;
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: messages_main; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.messages_main (
    id text NOT NULL,
    message_index integer NOT NULL,
    type text,
    sender text,
    mentions text[],
    metadata jsonb
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02');


--
-- Name: extract_ibc_transfer_details(api.messages_main); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.extract_ibc_transfer_details(_message api.messages_main) RETURNS jsonb
    LANGUAGE plpgsql STABLE
    AS $$
DECLARE
  _result jsonb;
  _packet_data jsonb;
  _token_denom text;
  _token_amount text;
  _sender text;
  _receiver text;
  _source_channel text;
BEGIN
  IF _message.type = '/ibc.applications.transfer.v1.MsgTransfer' THEN
    _sender := _message.sender;
    _receiver := _message.metadata->>'receiver';
    _source_channel := COALESCE(
      _message.metadata->>'sourceChannel',
      _message.metadata->>'source_channel'
    );
    _token_denom := COALESCE(
      _message.metadata->'token'->>'denom',
      _message.metadata->>'denom'
    );
    _token_amount := COALESCE(
      _message.metadata->'token'->>'amount',
      _message.metadata->>'amount'
    );

  ELSIF _message.type = '/ibc.core.channel.v1.MsgRecvPacket' THEN
    _packet_data := COALESCE(
      _message.metadata->'packet'->'data',
      _message.metadata->'packetData',
      _message.metadata->'packet_data'
    );

    IF jsonb_typeof(_packet_data) = 'string' THEN
      BEGIN
        _packet_data := (_packet_data #>> '{}')::jsonb;
      EXCEPTION WHEN OTHERS THEN
        _packet_data := NULL;
      END;
    END IF;

    _sender := COALESCE(
      _packet_data->>'sender',
      _message.metadata->>'sender'
    );
    _receiver := COALESCE(
      _packet_data->>'receiver',
      _message.metadata->>'receiver'
    );
    _source_channel := COALESCE(
      _message.metadata->'packet'->>'sourceChannel',
      _message.metadata->'packet'->>'source_channel',
      _message.metadata->>'sourceChannel'
    );
    _token_denom := COALESCE(
      _packet_data->>'denom',
      _message.metadata->>'denom'
    );
    _token_amount := COALESCE(
      _packet_data->>'amount',
      _message.metadata->>'amount'
    );
  END IF;

  RETURN jsonb_build_object(
    'sender', _sender,
    'receiver', _receiver,
    'source_channel', _source_channel,
    'token_denom', _token_denom,
    'token_amount', _token_amount
  );
END;
$$;


--
-- Name: extract_rewards_from_events(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.extract_rewards_from_events() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
	events JSONB;
BEGIN
	-- Get finalize_block_events array
	events := COALESCE(
		NEW.data->'finalizeBlockEvents',
		NEW.data->'finalize_block_events',
		'[]'::JSONB
	);

	IF jsonb_array_length(events) = 0 THEN
		RETURN NEW;
	END IF;

	-- Single set-based INSERT with deterministic ordering to prevent deadlocks.
	-- Replaces the row-at-a-time loop that caused deadlocks under high concurrency.
	INSERT INTO api.validator_rewards (height, validator_address, rewards, commission)
	SELECT
		NEW.height,
		agg.validator_addr,
		SUM(agg.reward_amount),
		SUM(agg.commission_amount)
	FROM (
		SELECT
			e.attrs->>'validator' AS validator_addr,
			CASE WHEN e.event_type = 'rewards' THEN
				COALESCE(
					NULLIF(regexp_replace(e.attrs->>'amount', '[^0-9.]', '', 'g'), '')::NUMERIC,
					0
				)
			ELSE 0 END AS reward_amount,
			CASE WHEN e.event_type = 'commission' THEN
				COALESCE(
					NULLIF(regexp_replace(e.attrs->>'amount', '[^0-9.]', '', 'g'), '')::NUMERIC,
					0
				)
			ELSE 0 END AS commission_amount
		FROM (
			SELECT
				ei->>'type' AS event_type,
				(
					SELECT jsonb_object_agg(
						COALESCE(a->>'key', ''),
						COALESCE(a->>'value', '')
					)
					FROM jsonb_array_elements(COALESCE(ei->'attributes', '[]'::JSONB)) a
				) AS attrs
			FROM jsonb_array_elements(events) ei
		) e
		WHERE e.event_type IN ('rewards', 'commission')
			AND e.attrs->>'validator' IS NOT NULL
			AND e.attrs->>'validator' <> ''
	) agg
	GROUP BY agg.validator_addr
	ORDER BY agg.validator_addr
	ON CONFLICT (height, validator_address)
	DO UPDATE SET
		rewards = api.validator_rewards.rewards + EXCLUDED.rewards,
		commission = api.validator_rewards.commission + EXCLUDED.commission;

	RETURN NEW;
END;
$$;


--
-- Name: extract_validator_consensus_pubkey(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.extract_validator_consensus_pubkey() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  msg_type TEXT;
  raw_data JSONB;
  pubkey_data JSONB;
  pubkey_base64 TEXT;
  consensus_addr TEXT;
  valoper_addr TEXT;
BEGIN
  msg_type := NEW.type;
  IF msg_type NOT LIKE '%MsgCreateValidator' THEN
    RETURN NEW;
  END IF;

  SELECT data INTO raw_data
  FROM api.messages_raw
  WHERE id = NEW.id;

  IF raw_data IS NULL THEN
    RETURN NEW;
  END IF;

  -- Extract pubkey (handles both camelCase and snake_case)
  pubkey_data := COALESCE(raw_data->'pubkey', raw_data->'pub_key');
  IF pubkey_data IS NULL THEN
    RETURN NEW;
  END IF;

  pubkey_base64 := pubkey_data->>'key';
  IF pubkey_base64 IS NULL OR pubkey_base64 = '' THEN
    RETURN NEW;
  END IF;

  -- Compute consensus address (hex)
  consensus_addr := api.compute_consensus_address(pubkey_base64);

  -- Get validator operator address
  valoper_addr := COALESCE(raw_data->>'validatorAddress', raw_data->>'validator_address');
  IF valoper_addr IS NULL OR valoper_addr = '' THEN
    RETURN NEW;
  END IF;

  -- Insert/update hex entry in mapping table
  INSERT INTO api.validator_consensus_addresses (
    consensus_address, operator_address, hex_address, first_seen_height
  ) VALUES (
    consensus_addr, valoper_addr, consensus_addr, 1
  )
  ON CONFLICT (consensus_address) DO UPDATE
  SET operator_address = valoper_addr,
      hex_address = consensus_addr;

  -- Update validators table
  UPDATE api.validators
  SET consensus_address = consensus_addr
  WHERE operator_address = valoper_addr
    AND (consensus_address IS NULL OR consensus_address = '');

  -- Propagate to any base64 entries with matching hex_address
  UPDATE api.validator_consensus_addresses
  SET operator_address = valoper_addr
  WHERE hex_address = consensus_addr
    AND operator_address IS NULL;

  RETURN NEW;
END;
$$;


--
-- Name: get_address_stats(text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_address_stats(_address text) RETURNS jsonb
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


--
-- Name: get_address_stats(text, text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_address_stats(_address text, _alt_address text DEFAULT NULL::text) RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  WITH tx_ids AS (
    SELECT DISTINCT m.id
    FROM api.messages_main m
    WHERE m.sender = _address
       OR _address = ANY(m.mentions)
       OR (_alt_address IS NOT NULL AND (
            m.sender = _alt_address
            OR _alt_address = ANY(m.mentions)
          ))
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


--
-- Name: get_all_validators_signing_stats(integer); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_all_validators_signing_stats(_window_size integer DEFAULT 10000) RETURNS TABLE(consensus_address text, total_blocks integer, blocks_signed integer, blocks_missed integer, signing_percentage numeric)
    LANGUAGE plpgsql STABLE
    AS $$
DECLARE
  max_height BIGINT;
BEGIN
  SELECT MAX(id) INTO max_height FROM api.blocks_raw;

  RETURN QUERY
  SELECT
    vbs.consensus_address,
    COUNT(*)::INTEGER as total_blocks,
    COUNT(*) FILTER (WHERE vbs.signed)::INTEGER as blocks_signed,
    COUNT(*) FILTER (WHERE NOT vbs.signed)::INTEGER as blocks_missed,
    CASE
      WHEN COUNT(*) > 0 THEN
        ROUND((COUNT(*) FILTER (WHERE vbs.signed)::NUMERIC / COUNT(*)::NUMERIC) * 100, 2)
      ELSE 100
    END as signing_percentage
  FROM api.validator_block_signatures vbs
  WHERE vbs.height > max_height - _window_size
  GROUP BY vbs.consensus_address;
END;
$$;


--
-- Name: get_block_time_analysis(integer); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_block_time_analysis(_limit integer DEFAULT 100) RETURNS jsonb
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


--
-- Name: get_blocks_paginated(integer, integer, integer, timestamp without time zone, timestamp without time zone); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_blocks_paginated(_limit integer DEFAULT 20, _offset integer DEFAULT 0, _min_tx_count integer DEFAULT NULL::integer, _from_date timestamp without time zone DEFAULT NULL::timestamp without time zone, _to_date timestamp without time zone DEFAULT NULL::timestamp without time zone) RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  WITH filtered_blocks AS (
    SELECT b.id, b.data, b.tx_count
    FROM api.blocks_raw b
    WHERE
      (_min_tx_count IS NULL OR b.tx_count >= _min_tx_count)
      AND (_from_date IS NULL OR b.block_time >= _from_date)
      AND (_to_date IS NULL OR b.block_time <= _to_date)
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


--
-- Name: get_chain_params(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_chain_params() RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  SELECT COALESCE(jsonb_object_agg(key, value), '{}'::jsonb)
  FROM api.chain_params;
$$;


--
-- Name: get_compute_benchmarks(integer, integer, text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_compute_benchmarks(_limit integer DEFAULT 20, _offset integer DEFAULT 0, _status text DEFAULT NULL::text) RETURNS jsonb
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


--
-- Name: get_compute_job(bigint); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_compute_job(_job_id bigint) RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  SELECT to_jsonb(j)
  FROM api.compute_jobs j
  WHERE j.job_id = _job_id;
$$;


--
-- Name: get_compute_jobs(integer, integer, text, text, text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_compute_jobs(_limit integer DEFAULT 20, _offset integer DEFAULT 0, _status text DEFAULT NULL::text, _creator text DEFAULT NULL::text, _validator text DEFAULT NULL::text) RETURNS jsonb
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


--
-- Name: get_delegation_events(text, integer, integer, text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_delegation_events(_validator_address text, _limit integer DEFAULT 20, _offset integer DEFAULT 0, _event_type text DEFAULT NULL::text) RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  WITH filtered AS (
    SELECT *
    FROM api.delegation_events
    WHERE validator_address = _validator_address
    AND (_event_type IS NULL OR event_type = _event_type)
  ),
  total AS (
    SELECT COUNT(*) AS cnt FROM filtered
  ),
  page AS (
    SELECT * FROM filtered
    ORDER BY timestamp DESC NULLS LAST
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


--
-- Name: get_delegator_delegations(text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_delegator_delegations(_delegator_address text) RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  WITH delegation_totals AS (
    SELECT
      de.validator_address,
      v.moniker as validator_moniker,
      v.commission_rate,
      v.status as validator_status,
      v.jailed as validator_jailed,
      de.denom,
      SUM(
        CASE
          WHEN de.event_type IN ('DELEGATE', 'CREATE_VALIDATOR') THEN COALESCE(de.amount, 0)
          WHEN de.event_type = 'UNDELEGATE' THEN -COALESCE(de.amount, 0)
          WHEN de.event_type = 'REDELEGATE' THEN
            CASE
              WHEN de.validator_address = de.src_validator_address THEN -COALESCE(de.amount, 0)
              ELSE COALESCE(de.amount, 0)
            END
          ELSE 0
        END
      ) as total_delegated
    FROM api.delegation_events de
    LEFT JOIN api.validators v ON de.validator_address = v.operator_address
    WHERE de.delegator_address = _delegator_address
    GROUP BY de.validator_address, v.moniker, v.commission_rate, v.status, v.jailed, de.denom
    HAVING SUM(
      CASE
        WHEN de.event_type IN ('DELEGATE', 'CREATE_VALIDATOR') THEN COALESCE(de.amount, 0)
        WHEN de.event_type = 'UNDELEGATE' THEN -COALESCE(de.amount, 0)
        WHEN de.event_type = 'REDELEGATE' THEN
          CASE
            WHEN de.validator_address = de.src_validator_address THEN -COALESCE(de.amount, 0)
            ELSE COALESCE(de.amount, 0)
          END
        ELSE 0
      END
    ) > 0
  )
  SELECT jsonb_build_object(
    'delegations', COALESCE(jsonb_agg(to_jsonb(dt)), '[]'::jsonb),
    'total_staked', COALESCE((SELECT SUM(total_delegated) FROM delegation_totals), 0)::TEXT,
    'validator_count', (SELECT COUNT(*) FROM delegation_totals)
  )
  FROM delegation_totals dt;
$$;


--
-- Name: get_delegator_history(text, integer, integer, text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_delegator_history(_delegator_address text, _limit integer DEFAULT 50, _offset integer DEFAULT 0, _event_type text DEFAULT NULL::text) RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  WITH filtered AS (
    SELECT
      de.*,
      v.moniker as validator_moniker
    FROM api.delegation_events de
    LEFT JOIN api.validators v ON de.validator_address = v.operator_address
    WHERE de.delegator_address = _delegator_address
    AND (_event_type IS NULL OR de.event_type = _event_type)
  ),
  total AS (
    SELECT COUNT(*) AS cnt FROM filtered
  ),
  page AS (
    SELECT * FROM filtered
    ORDER BY timestamp DESC NULLS LAST
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


--
-- Name: get_delegator_stats(text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_delegator_stats(_delegator_address text) RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  SELECT jsonb_build_object(
    'total_delegations', COUNT(*) FILTER (WHERE event_type = 'DELEGATE'),
    'total_undelegations', COUNT(*) FILTER (WHERE event_type = 'UNDELEGATE'),
    'total_redelegations', COUNT(*) FILTER (WHERE event_type = 'REDELEGATE'),
    'first_delegation', MIN(timestamp),
    'last_activity', MAX(timestamp),
    'unique_validators', COUNT(DISTINCT validator_address) FILTER (
      WHERE event_type IN ('DELEGATE', 'CREATE_VALIDATOR')
    )
  )
  FROM api.delegation_events
  WHERE delegator_address = _delegator_address;
$$;


--
-- Name: get_delegator_validator_history(text, text, integer, integer); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_delegator_validator_history(_delegator_address text, _validator_address text, _limit integer DEFAULT 50, _offset integer DEFAULT 0) RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  WITH filtered AS (
    SELECT *
    FROM api.delegation_events
    WHERE delegator_address = _delegator_address
    AND (validator_address = _validator_address OR src_validator_address = _validator_address)
  ),
  total AS (
    SELECT COUNT(*) AS cnt FROM filtered
  ),
  page AS (
    SELECT * FROM filtered
    ORDER BY timestamp DESC NULLS LAST
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


--
-- Name: get_governance_proposals(integer, integer, text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_governance_proposals(_limit integer DEFAULT 20, _offset integer DEFAULT 0, _status text DEFAULT NULL::text) RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  WITH filtered AS (
    SELECT p.*
    FROM api.governance_proposals p
    WHERE (_status IS NULL OR p.status = _status)
    ORDER BY p.proposal_id DESC
    LIMIT _limit OFFSET _offset
  ),
  total AS (
    SELECT COUNT(*) AS count
    FROM api.governance_proposals
    WHERE (_status IS NULL OR status = _status)
  ),
  with_snapshots AS (
    SELECT
      f.*,
      s.snapshot_time AS last_snapshot_time
    FROM filtered f
    LEFT JOIN LATERAL (
      SELECT snapshot_time
      FROM api.governance_snapshots
      WHERE proposal_id = f.proposal_id
      ORDER BY snapshot_time DESC
      LIMIT 1
    ) s ON TRUE
  )
  SELECT jsonb_build_object(
    'data', COALESCE(jsonb_agg(
      jsonb_build_object(
        'proposal_id', ws.proposal_id,
        'title', ws.title,
        'summary', ws.summary,
        'status', ws.status,
        'submit_time', ws.submit_time,
        'deposit_end_time', ws.deposit_end_time,
        'voting_start_time', ws.voting_start_time,
        'voting_end_time', ws.voting_end_time,
        'proposer', ws.proposer,
        'tally', jsonb_build_object(
          'yes', ws.yes_count,
          'no', ws.no_count,
          'abstain', ws.abstain_count,
          'no_with_veto', ws.no_with_veto_count
        ),
        'last_updated', ws.last_updated,
        'last_snapshot_time', ws.last_snapshot_time
      ) ORDER BY ws.proposal_id DESC
    ), '[]'::jsonb),
    'pagination', jsonb_build_object(
      'total', (SELECT count FROM total),
      'limit', _limit,
      'offset', _offset,
      'has_next', _offset + _limit < (SELECT count FROM total),
      'has_prev', _offset > 0
    )
  )
  FROM with_snapshots ws;
$$;


--
-- Name: get_hourly_rewards(integer); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_hourly_rewards(_hours integer DEFAULT 24) RETURNS TABLE(hour timestamp with time zone, rewards numeric, commission numeric)
    LANGUAGE plpgsql STABLE
    AS $$
BEGIN
  RETURN QUERY
  SELECT rt.hour, rt.rewards, rt.commission
  FROM api.rt_hourly_rewards rt
  WHERE rt.hour > NOW() - (_hours || ' hours')::INTERVAL
  ORDER BY rt.hour DESC;
END;
$$;


--
-- Name: get_ibc_chains(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_ibc_chains() RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  SELECT COALESCE(jsonb_agg(
    jsonb_build_object(
      'chain_id', counterparty_chain_id,
      'channel_count', channel_count,
      'open_channels', open_channels,
      'active_channels', active_channels
    )
    ORDER BY counterparty_chain_id
  ), '[]'::jsonb)
  FROM (
    SELECT
      counterparty_chain_id,
      COUNT(*) AS channel_count,
      COUNT(*) FILTER (WHERE state = 'STATE_OPEN') AS open_channels,
      COUNT(*) FILTER (WHERE state = 'STATE_OPEN' AND client_status = 'Active') AS active_channels
    FROM api.ibc_connections
    WHERE counterparty_chain_id IS NOT NULL
    GROUP BY counterparty_chain_id
  ) chains;
$$;


--
-- Name: get_ibc_channel_activity(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_ibc_channel_activity() RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  WITH channel_activity AS (
    SELECT
      COALESCE(
        m.metadata->>'sourceChannel',
        m.metadata->>'source_channel'
      ) as channel_id,
      COUNT(*) as transfer_count,
      COUNT(*) FILTER (WHERE t.error IS NULL OR t.error = '') as successful_transfers
    FROM api.messages_main m
    JOIN api.transactions_main t ON m.id = t.id
    WHERE m.type = '/ibc.applications.transfer.v1.MsgTransfer'
    AND (m.metadata->>'sourceChannel' IS NOT NULL OR m.metadata->>'source_channel' IS NOT NULL)
    GROUP BY COALESCE(m.metadata->>'sourceChannel', m.metadata->>'source_channel')
  )
  SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'channel_id', ca.channel_id,
    'transfer_count', ca.transfer_count,
    'successful_transfers', ca.successful_transfers,
    'counterparty_chain_id', c.counterparty_chain_id,
    'state', c.state,
    'client_status', c.client_status
  ) ORDER BY ca.transfer_count DESC), '[]'::jsonb)
  FROM channel_activity ca
  LEFT JOIN api.ibc_connections c ON ca.channel_id = c.channel_id AND c.port_id = 'transfer';
$$;


--
-- Name: get_ibc_connection(text, text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_ibc_connection(_channel_id text, _port_id text DEFAULT 'transfer'::text) RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  SELECT jsonb_build_object(
    'channel_id', channel_id,
    'port_id', port_id,
    'connection_id', connection_id,
    'client_id', client_id,
    'counterparty_chain_id', counterparty_chain_id,
    'counterparty_channel_id', counterparty_channel_id,
    'counterparty_port_id', counterparty_port_id,
    'counterparty_client_id', counterparty_client_id,
    'counterparty_connection_id', counterparty_connection_id,
    'state', state,
    'ordering', ordering,
    'client_status', client_status,
    'is_active', state = 'STATE_OPEN' AND client_status = 'Active',
    'updated_at', updated_at
  )
  FROM api.ibc_connections
  WHERE channel_id = _channel_id AND port_id = _port_id;
$$;


--
-- Name: get_ibc_connections(integer, integer, text, text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_ibc_connections(_limit integer DEFAULT 50, _offset integer DEFAULT 0, _chain_id text DEFAULT NULL::text, _state text DEFAULT NULL::text) RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  WITH filtered AS (
    SELECT *
    FROM api.ibc_connections
    WHERE (_chain_id IS NULL OR counterparty_chain_id = _chain_id)
      AND (_state IS NULL OR state = _state)
    ORDER BY counterparty_chain_id, channel_id
    LIMIT _limit OFFSET _offset
  ),
  total AS (
    SELECT COUNT(*) AS count
    FROM api.ibc_connections
    WHERE (_chain_id IS NULL OR counterparty_chain_id = _chain_id)
      AND (_state IS NULL OR state = _state)
  )
  SELECT jsonb_build_object(
    'data', COALESCE(jsonb_agg(
      jsonb_build_object(
        'channel_id', f.channel_id,
        'port_id', f.port_id,
        'connection_id', f.connection_id,
        'client_id', f.client_id,
        'counterparty_chain_id', f.counterparty_chain_id,
        'counterparty_channel_id', f.counterparty_channel_id,
        'counterparty_port_id', f.counterparty_port_id,
        'counterparty_client_id', f.counterparty_client_id,
        'counterparty_connection_id', f.counterparty_connection_id,
        'state', f.state,
        'ordering', f.ordering,
        'client_status', f.client_status,
        'is_active', f.state = 'STATE_OPEN' AND f.client_status = 'Active',
        'updated_at', f.updated_at
      )
      ORDER BY f.counterparty_chain_id, f.channel_id
    ), '[]'::jsonb),
    'pagination', jsonb_build_object(
      'total', (SELECT count FROM total),
      'limit', _limit,
      'offset', _offset,
      'has_next', _offset + _limit < (SELECT count FROM total),
      'has_prev', _offset > 0
    )
  )
  FROM filtered f;
$$;


--
-- Name: get_ibc_denom_traces(integer, integer, text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_ibc_denom_traces(_limit integer DEFAULT 50, _offset integer DEFAULT 0, _base_denom text DEFAULT NULL::text) RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  WITH filtered AS (
    SELECT t.*, c.counterparty_chain_id
    FROM api.ibc_denom_traces t
    LEFT JOIN api.ibc_connections c ON t.source_channel = c.channel_id AND c.port_id = 'transfer'
    WHERE (_base_denom IS NULL OR t.base_denom ILIKE '%' || _base_denom || '%')
    ORDER BY t.base_denom, t.ibc_denom
    LIMIT _limit OFFSET _offset
  ),
  total AS (
    SELECT COUNT(*) AS count
    FROM api.ibc_denom_traces
    WHERE (_base_denom IS NULL OR base_denom ILIKE '%' || _base_denom || '%')
  )
  SELECT jsonb_build_object(
    'data', COALESCE(jsonb_agg(
      jsonb_build_object(
        'ibc_denom', f.ibc_denom,
        'base_denom', f.base_denom,
        'path', f.path,
        'source_channel', f.source_channel,
        'source_chain_id', COALESCE(f.source_chain_id, f.counterparty_chain_id),
        'symbol', f.symbol,
        'decimals', f.decimals,
        'updated_at', f.updated_at
      )
      ORDER BY f.base_denom, f.ibc_denom
    ), '[]'::jsonb),
    'pagination', jsonb_build_object(
      'total', (SELECT count FROM total),
      'limit', _limit,
      'offset', _offset,
      'has_next', _offset + _limit < (SELECT count FROM total),
      'has_prev', _offset > 0
    )
  )
  FROM filtered f;
$$;


--
-- Name: get_ibc_heatmap_data(text, text, text, text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_ibc_heatmap_data(_timeframe text DEFAULT '7d'::text, _metric text DEFAULT 'count'::text, _direction text DEFAULT NULL::text, _channel text DEFAULT NULL::text) RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  WITH params AS (
    SELECT
      CASE _timeframe
        WHEN '24h' THEN interval '24 hours'
        WHEN '48h' THEN interval '48 hours'
        WHEN '7d' THEN interval '7 days'
        WHEN '30d' THEN interval '30 days'
        ELSE interval '7 days'
      END AS lookback,
      CASE _timeframe
        WHEN '24h' THEN 'hour'
        WHEN '48h' THEN 'hour'
        WHEN '7d' THEN 'hour'
        WHEN '30d' THEN 'day'
        ELSE 'hour'
      END AS granularity
  ),
  ibc_data AS (
    SELECT
      m.*,
      t.timestamp,
      t.error,
      api.extract_ibc_transfer_details(m) as details,
      CASE
        WHEN m.type = '/ibc.applications.transfer.v1.MsgTransfer' THEN 'outgoing'
        WHEN m.type = '/ibc.core.channel.v1.MsgRecvPacket' THEN 'incoming'
      END as direction
    FROM api.messages_main m
    JOIN api.transactions_main t ON m.id = t.id
    CROSS JOIN params p
    WHERE m.type IN (
      '/ibc.applications.transfer.v1.MsgTransfer',
      '/ibc.core.channel.v1.MsgRecvPacket'
    )
    AND t.timestamp >= NOW() - p.lookback
    AND (t.error IS NULL OR t.error = '')
    AND (_direction IS NULL OR
      (_direction = 'outgoing' AND m.type = '/ibc.applications.transfer.v1.MsgTransfer') OR
      (_direction = 'incoming' AND m.type = '/ibc.core.channel.v1.MsgRecvPacket')
    )
  ),
  filtered AS (
    SELECT
      timestamp,
      direction,
      details->>'source_channel' AS channel,
      COALESCE((details->>'token_amount')::numeric, 0) AS amount
    FROM ibc_data
    WHERE _channel IS NULL OR details->>'source_channel' = _channel
  ),
  hourly_data AS (
    SELECT
      date_trunc('hour', timestamp) AS bucket,
      EXTRACT(DOW FROM timestamp)::int AS day_of_week,
      EXTRACT(HOUR FROM timestamp)::int AS hour_of_day,
      direction,
      COUNT(*) AS transfer_count,
      SUM(amount) AS total_volume
    FROM filtered
    WHERE (SELECT granularity FROM params) = 'hour'
    GROUP BY 1, 2, 3, 4
  ),
  daily_data AS (
    SELECT
      date_trunc('day', timestamp) AS bucket,
      EXTRACT(DOW FROM timestamp)::int AS day_of_week,
      NULL::int AS hour_of_day,
      direction,
      COUNT(*) AS transfer_count,
      SUM(amount) AS total_volume
    FROM filtered
    WHERE (SELECT granularity FROM params) = 'day'
    GROUP BY 1, 2, 4
  ),
  combined AS (
    SELECT * FROM hourly_data
    UNION ALL
    SELECT * FROM daily_data
  ),
  timeseries AS (
    SELECT
      bucket,
      COALESCE(SUM(transfer_count) FILTER (WHERE direction = 'outgoing'), 0) AS outgoing_count,
      COALESCE(SUM(transfer_count) FILTER (WHERE direction = 'incoming'), 0) AS incoming_count,
      COALESCE(SUM(total_volume) FILTER (WHERE direction = 'outgoing'), 0) AS outgoing_volume,
      COALESCE(SUM(total_volume) FILTER (WHERE direction = 'incoming'), 0) AS incoming_volume
    FROM combined
    GROUP BY bucket
    ORDER BY bucket
  ),
  heatmap_matrix AS (
    SELECT
      day_of_week,
      hour_of_day,
      COALESCE(SUM(transfer_count), 0) AS count,
      COALESCE(SUM(total_volume), 0) AS volume
    FROM combined
    WHERE hour_of_day IS NOT NULL
    GROUP BY day_of_week, hour_of_day
  ),
  summary AS (
    SELECT
      COALESCE(SUM(transfer_count) FILTER (WHERE direction = 'outgoing'), 0) AS total_outgoing_count,
      COALESCE(SUM(transfer_count) FILTER (WHERE direction = 'incoming'), 0) AS total_incoming_count,
      COALESCE(SUM(total_volume) FILTER (WHERE direction = 'outgoing'), 0) AS total_outgoing_volume,
      COALESCE(SUM(total_volume) FILTER (WHERE direction = 'incoming'), 0) AS total_incoming_volume,
      COALESCE(MAX(transfer_count), 0) AS peak_count,
      COALESCE(MAX(total_volume), 0) AS peak_volume
    FROM combined
  ),
  channel_breakdown AS (
    SELECT
      details->>'source_channel' AS channel,
      COUNT(*) AS transfer_count,
      SUM(COALESCE((details->>'token_amount')::numeric, 0)) AS total_volume
    FROM ibc_data
    WHERE details->>'source_channel' IS NOT NULL
    GROUP BY 1
    ORDER BY transfer_count DESC
    LIMIT 10
  )
  SELECT jsonb_build_object(
    'timeframe', _timeframe,
    'metric', _metric,
    'direction_filter', _direction,
    'channel_filter', _channel,
    'granularity', (SELECT granularity FROM params),
    'timeseries', COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'timestamp', bucket,
        'outgoing_count', outgoing_count,
        'incoming_count', incoming_count,
        'outgoing_volume', outgoing_volume,
        'incoming_volume', incoming_volume,
        'total_count', outgoing_count + incoming_count,
        'total_volume', outgoing_volume + incoming_volume
      ) ORDER BY bucket)
      FROM timeseries
    ), '[]'::jsonb),
    'heatmap', COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'day', day_of_week,
        'hour', hour_of_day,
        'count', count,
        'volume', volume
      ))
      FROM heatmap_matrix
    ), '[]'::jsonb),
    'summary', (
      SELECT jsonb_build_object(
        'total_outgoing_count', total_outgoing_count,
        'total_incoming_count', total_incoming_count,
        'total_outgoing_volume', total_outgoing_volume,
        'total_incoming_volume', total_incoming_volume,
        'total_transfers', total_outgoing_count + total_incoming_count,
        'total_volume', total_outgoing_volume + total_incoming_volume,
        'peak_hourly_count', peak_count,
        'peak_hourly_volume', peak_volume
      )
      FROM summary
    ),
    'top_channels', COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'channel', channel,
        'count', transfer_count,
        'volume', total_volume
      ))
      FROM channel_breakdown
    ), '[]'::jsonb)
  );
$$;


--
-- Name: get_ibc_stats(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_ibc_stats() RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  WITH transfer_stats AS (
    SELECT
      COUNT(*) FILTER (WHERE type = '/ibc.applications.transfer.v1.MsgTransfer') as outgoing_transfers,
      COUNT(*) FILTER (WHERE type = '/ibc.core.channel.v1.MsgRecvPacket') as incoming_transfers,
      COUNT(*) FILTER (WHERE type = '/ibc.core.channel.v1.MsgAcknowledgement') as completed_transfers,
      COUNT(*) FILTER (WHERE type = '/ibc.core.channel.v1.MsgTimeout') as timed_out_transfers,
      COUNT(*) FILTER (WHERE type = '/ibc.core.client.v1.MsgUpdateClient') as relayer_updates
    FROM api.messages_main
    WHERE type LIKE '/ibc%'
  ),
  channel_stats AS (
    SELECT
      COUNT(*) as total_channels,
      COUNT(*) FILTER (WHERE state = 'STATE_OPEN') as open_channels,
      COUNT(*) FILTER (WHERE state = 'STATE_OPEN' AND client_status = 'Active') as active_channels,
      COUNT(DISTINCT counterparty_chain_id) as connected_chains
    FROM api.ibc_connections
  ),
  denom_stats AS (
    SELECT COUNT(*) as total_denoms
    FROM api.ibc_denom_traces
  )
  SELECT jsonb_build_object(
    'outgoing_transfers', ts.outgoing_transfers,
    'incoming_transfers', ts.incoming_transfers,
    'completed_transfers', ts.completed_transfers,
    'timed_out_transfers', ts.timed_out_transfers,
    'relayer_updates', ts.relayer_updates,
    'total_channels', cs.total_channels,
    'open_channels', cs.open_channels,
    'active_channels', cs.active_channels,
    'connected_chains', cs.connected_chains,
    'total_denoms', ds.total_denoms
  )
  FROM transfer_stats ts, channel_stats cs, denom_stats ds;
$$;


--
-- Name: get_ibc_transfers(integer, integer, text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_ibc_transfers(_limit integer DEFAULT 20, _offset integer DEFAULT 0, _direction text DEFAULT NULL::text) RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  WITH filtered_messages AS (
    SELECT
      m.*,
      t.height,
      t.timestamp,
      t.error,
      CASE
        WHEN m.type = '/ibc.applications.transfer.v1.MsgTransfer' THEN 'outgoing'
        WHEN m.type = '/ibc.core.channel.v1.MsgRecvPacket' THEN 'incoming'
        ELSE 'other'
      END as direction
    FROM api.messages_main m
    JOIN api.transactions_main t ON m.id = t.id
    WHERE m.type IN (
      '/ibc.applications.transfer.v1.MsgTransfer',
      '/ibc.core.channel.v1.MsgRecvPacket'
    )
    AND (_direction IS NULL OR
      (_direction = 'outgoing' AND m.type = '/ibc.applications.transfer.v1.MsgTransfer') OR
      (_direction = 'incoming' AND m.type = '/ibc.core.channel.v1.MsgRecvPacket')
    )
  ),
  total AS (
    SELECT COUNT(*)::int as count FROM filtered_messages
  ),
  paginated AS (
    SELECT * FROM filtered_messages
    ORDER BY height DESC, message_index
    LIMIT _limit OFFSET _offset
  ),
  transfers AS (
    SELECT
      p.id as tx_hash,
      p.height,
      p.timestamp,
      p.direction,
      p.error,
      api.extract_ibc_transfer_details(ROW(p.id, p.message_index, p.type, p.sender, p.mentions, p.metadata)::api.messages_main) as details
    FROM paginated p
  )
  SELECT jsonb_build_object(
    'data', COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'tx_hash', tr.tx_hash,
        'height', tr.height,
        'timestamp', tr.timestamp,
        'direction', tr.direction,
        'sender', tr.details->>'sender',
        'receiver', tr.details->>'receiver',
        'source_channel', tr.details->>'source_channel',
        'token_denom', tr.details->>'token_denom',
        'token_amount', tr.details->>'token_amount',
        'resolved_denom', api.resolve_denom(tr.details->>'token_denom'),
        'counterparty_chain', (
          SELECT c.counterparty_chain_id
          FROM api.ibc_connections c
          WHERE c.channel_id = tr.details->>'source_channel' AND c.port_id = 'transfer'
        ),
        'success', tr.error IS NULL OR tr.error = ''
      ) ORDER BY tr.height DESC)
      FROM transfers tr
    ), '[]'::jsonb),
    'pagination', jsonb_build_object(
      'total', (SELECT count FROM total),
      'limit', _limit,
      'offset', _offset,
      'has_next', _offset + _limit < (SELECT count FROM total),
      'has_prev', _offset > 0
    )
  );
$$;


--
-- Name: get_ibc_transfers_by_address(text, integer, integer); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_ibc_transfers_by_address(_address text, _limit integer DEFAULT 10, _offset integer DEFAULT 0) RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  WITH filtered_messages AS (
    SELECT
      m.*,
      t.height,
      t.timestamp,
      t.error,
      CASE
        WHEN m.type = '/ibc.applications.transfer.v1.MsgTransfer' THEN 'outgoing'
        WHEN m.type = '/ibc.core.channel.v1.MsgRecvPacket' THEN 'incoming'
        ELSE 'other'
      END as direction
    FROM api.messages_main m
    JOIN api.transactions_main t ON m.id = t.id
    WHERE m.type IN (
      '/ibc.applications.transfer.v1.MsgTransfer',
      '/ibc.core.channel.v1.MsgRecvPacket'
    )
    AND (
      m.sender = _address
      OR m.metadata->>'receiver' = _address
      OR _address = ANY(m.mentions)
      -- Also check packet data for incoming transfers
      OR m.metadata->'packet'->'data'->>'receiver' = _address
      OR m.metadata->'packetData'->>'receiver' = _address
    )
  ),
  total AS (
    SELECT COUNT(*)::int as count FROM filtered_messages
  ),
  paginated AS (
    SELECT * FROM filtered_messages
    ORDER BY height DESC, message_index
    LIMIT _limit OFFSET _offset
  ),
  transfers AS (
    SELECT
      p.id as tx_hash,
      p.height,
      p.timestamp,
      p.direction,
      p.error,
      api.extract_ibc_transfer_details(ROW(p.id, p.message_index, p.type, p.sender, p.mentions, p.metadata)::api.messages_main) as details
    FROM paginated p
  )
  SELECT jsonb_build_object(
    'data', COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'tx_hash', tr.tx_hash,
        'height', tr.height,
        'timestamp', tr.timestamp,
        'direction', tr.direction,
        'sender', tr.details->>'sender',
        'receiver', tr.details->>'receiver',
        'source_channel', tr.details->>'source_channel',
        'token_denom', tr.details->>'token_denom',
        'token_amount', tr.details->>'token_amount',
        'resolved_denom', api.resolve_denom(tr.details->>'token_denom'),
        'counterparty_chain', (
          SELECT c.counterparty_chain_id
          FROM api.ibc_connections c
          WHERE c.channel_id = tr.details->>'source_channel' AND c.port_id = 'transfer'
        ),
        'success', tr.error IS NULL OR tr.error = ''
      ) ORDER BY tr.height DESC)
      FROM transfers tr
    ), '[]'::jsonb),
    'pagination', jsonb_build_object(
      'total', (SELECT count FROM total),
      'limit', _limit,
      'offset', _offset,
      'has_next', _offset + _limit < (SELECT count FROM total),
      'has_prev', _offset > 0
    )
  );
$$;


--
-- Name: get_ibc_volume_timeseries(integer, text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_ibc_volume_timeseries(_hours integer DEFAULT 24, _channel text DEFAULT NULL::text) RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  WITH ibc_messages AS (
    SELECT
      m.*,
      t.timestamp,
      t.error,
      api.extract_ibc_transfer_details(m) as details,
      CASE
        WHEN m.type = '/ibc.applications.transfer.v1.MsgTransfer' THEN 'outgoing'
        WHEN m.type = '/ibc.core.channel.v1.MsgRecvPacket' THEN 'incoming'
        ELSE 'other'
      END as direction
    FROM api.messages_main m
    JOIN api.transactions_main t ON m.id = t.id
    WHERE m.type IN (
      '/ibc.applications.transfer.v1.MsgTransfer',
      '/ibc.core.channel.v1.MsgRecvPacket'
    )
    AND t.timestamp >= NOW() - (_hours || ' hours')::interval
    AND (t.error IS NULL OR t.error = '')
  ),
  filtered AS (
    SELECT
      timestamp,
      direction,
      details->>'source_channel' AS channel
    FROM ibc_messages
    WHERE _channel IS NULL OR details->>'source_channel' = _channel
  ),
  hourly_volume AS (
    SELECT
      date_trunc('hour', timestamp) AS hour_bucket,
      direction,
      COUNT(*) AS transfer_count
    FROM filtered
    WHERE timestamp IS NOT NULL
    GROUP BY date_trunc('hour', timestamp), direction
  ),
  time_series AS (
    SELECT generate_series(
      date_trunc('hour', NOW() - (_hours || ' hours')::interval),
      date_trunc('hour', NOW()),
      '1 hour'::interval
    ) AS hour_bucket
  ),
  timeseries_data AS (
    SELECT
      ts.hour_bucket AS hour,
      COALESCE(SUM(hv.transfer_count) FILTER (WHERE hv.direction = 'outgoing'), 0)::int AS outgoing_count,
      COALESCE(SUM(hv.transfer_count) FILTER (WHERE hv.direction = 'incoming'), 0)::int AS incoming_count,
      COALESCE(SUM(hv.transfer_count), 0)::int AS total_count
    FROM time_series ts
    LEFT JOIN hourly_volume hv ON hv.hour_bucket = ts.hour_bucket
    GROUP BY ts.hour_bucket
    ORDER BY ts.hour_bucket DESC
  ),
  summary_data AS (
    SELECT
      COALESCE(SUM(transfer_count) FILTER (WHERE direction = 'outgoing'), 0)::int AS total_outgoing,
      COALESCE(SUM(transfer_count) FILTER (WHERE direction = 'incoming'), 0)::int AS total_incoming,
      COALESCE(SUM(transfer_count), 0)::int AS total_transfers
    FROM hourly_volume
  ),
  peak_data AS (
    SELECT
      hour_bucket AS peak_hour,
      SUM(transfer_count)::int AS peak_count
    FROM hourly_volume
    GROUP BY hour_bucket
    ORDER BY SUM(transfer_count) DESC
    LIMIT 1
  )
  SELECT jsonb_build_object(
    'timeseries', COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'hour', td.hour,
        'outgoing_count', td.outgoing_count,
        'incoming_count', td.incoming_count,
        'total_count', td.total_count
      ) ORDER BY td.hour DESC)
      FROM timeseries_data td
    ), '[]'::jsonb),
    'summary', jsonb_build_object(
      'total_outgoing', (SELECT total_outgoing FROM summary_data),
      'total_incoming', (SELECT total_incoming FROM summary_data),
      'total_transfers', (SELECT total_transfers FROM summary_data),
      'peak_hour', (SELECT peak_hour FROM peak_data),
      'peak_count', COALESCE((SELECT peak_count FROM peak_data), 0)
    )
  );
$$;


--
-- Name: get_messages_for_address(text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_messages_for_address(_address text) RETURNS SETOF api.message_with_error
    LANGUAGE sql STABLE
    AS $$
  SELECT
    m.id,
    m.message_index,
    m.type,
    m.sender,
    m.mentions,
    m.metadata,
    t.error
  FROM api.messages_main m
  JOIN api.transactions_main t ON m.id = t.id
  WHERE m.sender = _address
     OR _address = ANY(m.mentions)
     OR m.metadata->>'toAddress' = _address
  ORDER BY t.height DESC, m.message_index;
$$;


--
-- Name: get_network_overview(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_network_overview() RETURNS TABLE(total_validators integer, active_validators integer, jailed_validators integer, total_bonded_tokens numeric, total_rewards_24h numeric, total_commission_24h numeric, avg_block_time numeric, total_transactions bigint, unique_addresses bigint, max_validators integer)
    LANGUAGE plpgsql STABLE
    AS $$
BEGIN
  RETURN QUERY SELECT
    (SELECT COUNT(*)::INTEGER FROM api.validators) AS total_validators,
    (SELECT COUNT(*)::INTEGER FROM api.validators WHERE status = 'BOND_STATUS_BONDED' AND NOT jailed) AS active_validators,
    (SELECT COUNT(*)::INTEGER FROM api.validators WHERE jailed = TRUE) AS jailed_validators,
    (SELECT COALESCE(SUM(tokens), 0) FROM api.validators WHERE status = 'BOND_STATUS_BONDED') AS total_bonded_tokens,
    -- Sum last 24 hours from real-time hourly rewards
    (SELECT COALESCE(SUM(r.rewards), 0) FROM api.rt_hourly_rewards r WHERE r.hour > NOW() - INTERVAL '24 hours') AS total_rewards_24h,
    (SELECT COALESCE(SUM(r.commission), 0) FROM api.rt_hourly_rewards r WHERE r.hour > NOW() - INTERVAL '24 hours') AS total_commission_24h,
    -- Avg block time from last 100 blocks (lightweight query)
    (SELECT COALESCE(AVG(
      EXTRACT(EPOCH FROM (
        (b1.data->'block'->'header'->>'time')::timestamptz -
        (b2.data->'block'->'header'->>'time')::timestamptz
      ))
    ), 6)
    FROM api.blocks_raw b1
    JOIN api.blocks_raw b2 ON b2.id = b1.id - 1
    WHERE b1.id > (SELECT MAX(id) - 100 FROM api.blocks_raw)) AS avg_block_time,
    -- Read from rt_chain_stats
    (SELECT cs.total_transactions FROM api.rt_chain_stats cs WHERE cs.id = 1) AS total_transactions,
    (SELECT cs.unique_addresses FROM api.rt_chain_stats cs WHERE cs.id = 1) AS unique_addresses,
    (SELECT COUNT(*)::INTEGER FROM api.validators WHERE status = 'BOND_STATUS_BONDED') AS max_validators;
END;
$$;


--
-- Name: get_recent_validator_events(text[], integer, integer); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_recent_validator_events(_event_types text[] DEFAULT ARRAY['slash'::text, 'liveness'::text, 'jail'::text], _limit integer DEFAULT 50, _offset integer DEFAULT 0) RETURNS TABLE(height bigint, event_type text, validator_address text, operator_address text, moniker text, reason text, power text, created_at timestamp with time zone, block_time timestamp with time zone, attributes jsonb)
    LANGUAGE plpgsql STABLE
    AS $$
BEGIN
  RETURN QUERY
  SELECT
    f.height,
    f.event_type,
    COALESCE(f.attributes->>'address', f.attributes->>'validator', '') as validator_address,
    v.operator_address,
    v.moniker,
    COALESCE(f.attributes->>'reason', '') as reason,
    COALESCE(f.attributes->>'power', '') as power,
    f.created_at,
    (b.data->'block'->'header'->>'time')::timestamptz as block_time,
    f.attributes
  FROM api.finalize_block_events f
  LEFT JOIN api.validator_consensus_addresses vca
    ON vca.consensus_address = COALESCE(f.attributes->>'address', f.attributes->>'validator', '')
  LEFT JOIN api.validators v ON v.operator_address = vca.operator_address
  LEFT JOIN api.blocks_raw b ON b.id = f.height
  WHERE f.event_type = ANY(_event_types)
  ORDER BY f.height DESC
  LIMIT _limit
  OFFSET _offset;
END;
$$;


--
-- Name: get_slashing_records(integer, integer, text, text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_slashing_records(_limit integer DEFAULT 20, _offset integer DEFAULT 0, _validator text DEFAULT NULL::text, _condition text DEFAULT NULL::text) RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  WITH filtered AS (
    SELECT *
    FROM api.slashing_records
    WHERE (_validator IS NULL OR validator_address = _validator)
    AND (_condition IS NULL OR condition = _condition)
  ),
  total AS (
    SELECT COUNT(*) AS cnt FROM filtered
  ),
  page AS (
    SELECT * FROM filtered
    ORDER BY timestamp DESC NULLS LAST
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


--
-- Name: get_transaction_detail(text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_transaction_detail(_hash text) RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  WITH
  resolved AS (
    -- Resolve EVM hash to Cosmos tx_id if needed, otherwise use input directly
    SELECT COALESCE(ev.tx_id, _hash) AS hash
    FROM (SELECT _hash AS input) i
    LEFT JOIN api.evm_transactions ev ON ev.hash = lower(_hash)
  ),
  tx_main AS (
    SELECT * FROM api.transactions_main WHERE id = (SELECT hash FROM resolved)
  ),
  tx_raw AS (
    SELECT * FROM api.transactions_raw WHERE id = (SELECT hash FROM resolved)
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
    WHERE m.id = (SELECT hash FROM resolved)
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
    WHERE e.id = (SELECT hash FROM resolved)
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
    WHERE ev.tx_id = (SELECT hash FROM resolved)
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
    WHERE l.tx_id = (SELECT hash FROM resolved)
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


--
-- Name: get_transactions_by_address(text, integer, integer); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_transactions_by_address(_address text, _limit integer DEFAULT 50, _offset integer DEFAULT 0) RETURNS jsonb
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
  ),
  tx_messages AS (
    SELECT
      m.id,
      jsonb_agg(
        jsonb_build_object(
          'id', m.id,
          'message_index', m.message_index,
          'type', m.type,
          'sender', m.sender,
          'mentions', m.mentions,
          'metadata', m.metadata
        ) ORDER BY m.message_index
      ) AS messages
    FROM api.messages_main m
    WHERE m.id IN (SELECT id FROM paginated)
    GROUP BY m.id
  ),
  tx_events AS (
    SELECT
      e.id,
      jsonb_agg(
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
    WHERE e.id IN (SELECT id FROM paginated)
    GROUP BY e.id
  )
  SELECT jsonb_build_object(
    'data', COALESCE(jsonb_agg(
      jsonb_build_object(
        'id', p.id,
        'height', p.height,
        'timestamp', p.timestamp,
        'fee', p.fee,
        'memo', p.memo,
        'error', p.error,
        'proposal_ids', p.proposal_ids,
        'messages', COALESCE(m.messages, '[]'::jsonb),
        'events', COALESCE(e.events, '[]'::jsonb),
        'ingest_error', NULL
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
  FROM paginated p
  LEFT JOIN tx_messages m ON p.id = m.id
  LEFT JOIN tx_events e ON p.id = e.id;
$$;


--
-- Name: get_transactions_by_address(text, integer, integer, text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_transactions_by_address(_address text, _limit integer DEFAULT 50, _offset integer DEFAULT 0, _alt_address text DEFAULT NULL::text) RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  WITH addr_txs AS (
    SELECT DISTINCT m.id
    FROM api.messages_main m
    WHERE m.sender = _address
       OR _address = ANY(m.mentions)
       OR (_alt_address IS NOT NULL AND (
            m.sender = _alt_address
            OR _alt_address = ANY(m.mentions)
          ))
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
  ),
  tx_messages AS (
    SELECT
      m.id,
      jsonb_agg(
        jsonb_build_object(
          'id', m.id,
          'message_index', m.message_index,
          'type', m.type,
          'sender', m.sender,
          'mentions', m.mentions,
          'metadata', m.metadata
        ) ORDER BY m.message_index
      ) AS messages
    FROM api.messages_main m
    WHERE m.id IN (SELECT id FROM paginated)
    GROUP BY m.id
  ),
  tx_events AS (
    SELECT
      e.id,
      jsonb_agg(
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
    WHERE e.id IN (SELECT id FROM paginated)
    GROUP BY e.id
  )
  SELECT jsonb_build_object(
    'data', COALESCE(jsonb_agg(
      jsonb_build_object(
        'id', p.id,
        'height', p.height,
        'timestamp', p.timestamp,
        'fee', p.fee,
        'memo', p.memo,
        'error', p.error,
        'proposal_ids', p.proposal_ids,
        'messages', COALESCE(m.messages, '[]'::jsonb),
        'events', COALESCE(e.events, '[]'::jsonb),
        'ingest_error', NULL
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
  FROM paginated p
  LEFT JOIN tx_messages m ON p.id = m.id
  LEFT JOIN tx_events e ON p.id = e.id;
$$;


--
-- Name: get_transactions_paginated(integer, integer, text, bigint, bigint, bigint, text, timestamp with time zone, timestamp with time zone); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_transactions_paginated(_limit integer DEFAULT 20, _offset integer DEFAULT 0, _status text DEFAULT NULL::text, _block_height bigint DEFAULT NULL::bigint, _block_height_min bigint DEFAULT NULL::bigint, _block_height_max bigint DEFAULT NULL::bigint, _message_type text DEFAULT NULL::text, _timestamp_min timestamp with time zone DEFAULT NULL::timestamp with time zone, _timestamp_max timestamp with time zone DEFAULT NULL::timestamp with time zone) RETURNS jsonb
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
      AND (_block_height_min IS NULL OR t.height >= _block_height_min)
      AND (_block_height_max IS NULL OR t.height <= _block_height_max)
      AND (_message_type IS NULL OR m.type = _message_type)
      AND (_timestamp_min IS NULL OR t.timestamp >= _timestamp_min)
      AND (_timestamp_max IS NULL OR t.timestamp <= _timestamp_max)
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
  ),
  tx_messages AS (
    SELECT
      m.id,
      jsonb_agg(
        jsonb_build_object(
          'id', m.id,
          'message_index', m.message_index,
          'type', m.type,
          'sender', m.sender,
          'mentions', m.mentions,
          'metadata', m.metadata
        ) ORDER BY m.message_index
      ) AS messages
    FROM api.messages_main m
    WHERE m.id IN (SELECT id FROM paginated)
    GROUP BY m.id
  ),
  tx_events AS (
    SELECT
      e.id,
      jsonb_agg(
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
    WHERE e.id IN (SELECT id FROM paginated)
    GROUP BY e.id
  )
  SELECT jsonb_build_object(
    'data', COALESCE(jsonb_agg(
      jsonb_build_object(
        'id', p.id,
        'height', p.height,
        'timestamp', p.timestamp,
        'fee', p.fee,
        'memo', p.memo,
        'error', p.error,
        'proposal_ids', p.proposal_ids,
        'messages', COALESCE(m.messages, '[]'::jsonb),
        'events', COALESCE(e.events, '[]'::jsonb),
        'ingest_error', NULL
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
  FROM paginated p
  LEFT JOIN tx_messages m ON p.id = m.id
  LEFT JOIN tx_events e ON p.id = e.id;
$$;


--
-- Name: get_validator_detail(text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_validator_detail(_operator_address text) RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  WITH total_bonded AS (
    SELECT COALESCE(SUM(tokens), 0) AS total
    FROM api.validators
    WHERE status = 'BOND_STATUS_BONDED' AND tokens IS NOT NULL
  ),
  validator AS (
    SELECT
      v.*,
      ipfs.ipfs_peer_id,
      ipfs.ipfs_multiaddrs,
      CASE
        WHEN v.status = 'BOND_STATUS_BONDED' AND tb.total > 0 AND v.tokens IS NOT NULL
        THEN ROUND((v.tokens / tb.total) * 100, 4)
        ELSE 0
      END AS voting_power_pct,
      COALESCE(dc.delegator_count, 0) AS delegator_count
    FROM api.validators v
    CROSS JOIN total_bonded tb
    LEFT JOIN api.mv_validator_delegator_counts dc
      ON dc.validator_address = v.operator_address
    LEFT JOIN api.validator_ipfs_addresses ipfs
      ON ipfs.validator_address = v.operator_address
    WHERE v.operator_address = _operator_address
  )
  SELECT to_jsonb(validator) FROM validator;
$$;


--
-- Name: get_validator_events_summary(integer); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_validator_events_summary(_limit integer DEFAULT 20) RETURNS TABLE(height bigint, event_type text, validator_moniker text, operator_address text, details jsonb, block_time timestamp with time zone)
    LANGUAGE plpgsql STABLE
    AS $$
BEGIN
  RETURN QUERY
  SELECT
    f.height,
    f.event_type,
    v.moniker as validator_moniker,
    v.operator_address,
    f.attributes as details,
    (b.data->'block'->'header'->>'time')::timestamptz as block_time
  FROM api.finalize_block_events f
  LEFT JOIN api.validator_consensus_addresses vca
    ON vca.consensus_address = COALESCE(f.attributes->>'address', f.attributes->>'validator', '')
  LEFT JOIN api.validators v ON v.operator_address = vca.operator_address
  LEFT JOIN api.blocks_raw b ON b.id = f.height
  WHERE f.event_type IN ('slash', 'liveness', 'jail', 'rewards', 'commission')
  ORDER BY f.height DESC
  LIMIT _limit;
END;
$$;


--
-- Name: get_validator_jailing_events(text, integer, integer); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_validator_jailing_events(_operator_address text, _limit integer DEFAULT 50, _offset integer DEFAULT 0) RETURNS TABLE(height bigint, event_type text, reason text, power text, detected_at timestamp with time zone)
    LANGUAGE plpgsql STABLE
    AS $$
BEGIN
  RETURN QUERY
  SELECT
    j.height,
    COALESCE(j.current_block_flag, 'unknown') as event_type,
    COALESCE(f.attributes->>'reason', '') as reason,
    COALESCE(f.attributes->>'power', '') as power,
    j.detected_at
  FROM api.jailing_events j
  LEFT JOIN api.finalize_block_events f
    ON f.height = j.height
    AND f.event_type IN ('slash', 'liveness', 'jail')
    AND (f.attributes->>'validator' = j.validator_address OR f.attributes->>'address' = j.validator_address)
  WHERE j.operator_address = _operator_address
     OR j.validator_address IN (
       SELECT consensus_address
       FROM api.validator_consensus_addresses
       WHERE operator_address = _operator_address
     )
  ORDER BY j.height DESC
  LIMIT _limit
  OFFSET _offset;
END;
$$;


--
-- Name: get_validator_performance(text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_validator_performance(_operator_address text) RETURNS TABLE(uptime_percentage numeric, blocks_signed integer, blocks_missed integer, total_jailing_events integer, last_jailed_height bigint, rewards_rank integer, delegation_rank integer)
    LANGUAGE plpgsql STABLE
    AS $$
DECLARE
  consensus_addr TEXT;
  signing_stats RECORD;
  has_signing_stats BOOLEAN := FALSE;
BEGIN
  -- Get consensus address for this operator (prefer hex entries)
  SELECT COALESCE(vca.hex_address, vca.consensus_address) INTO consensus_addr
  FROM api.validator_consensus_addresses vca
  WHERE vca.operator_address = _operator_address
  ORDER BY (vca.hex_address IS NOT NULL) DESC
  LIMIT 1;

  -- Get signing stats from block signatures
  IF consensus_addr IS NOT NULL THEN
    SELECT * INTO signing_stats
    FROM api.get_validator_signing_stats(consensus_addr, 10000);
    has_signing_stats := FOUND;
  END IF;

  RETURN QUERY
  SELECT
    CASE WHEN has_signing_stats
      THEN COALESCE(signing_stats.signing_percentage, 100)
      ELSE 100
    END as uptime_percentage,

    CASE WHEN has_signing_stats
      THEN COALESCE(signing_stats.blocks_signed, 0)
      ELSE 0
    END as blocks_signed,

    CASE WHEN has_signing_stats
      THEN COALESCE(signing_stats.blocks_missed, 0)
      ELSE 0
    END as blocks_missed,

    (SELECT COUNT(*)::INTEGER
     FROM api.jailing_events
     WHERE operator_address = _operator_address) as total_jailing_events,

    (SELECT MAX(height)
     FROM api.jailing_events
     WHERE operator_address = _operator_address) as last_jailed_height,

    (SELECT rank::INTEGER
     FROM (
       SELECT operator_address,
              RANK() OVER (ORDER BY lifetime_rewards DESC NULLS LAST) as rank
       FROM api.mv_validator_leaderboard
     ) ranked
     WHERE operator_address = _operator_address) as rewards_rank,

    (SELECT rank::INTEGER
     FROM (
       SELECT operator_address,
              RANK() OVER (ORDER BY delegator_count DESC NULLS LAST) as rank
       FROM api.mv_validator_leaderboard
     ) ranked
     WHERE operator_address = _operator_address) as delegation_rank;
END;
$$;


--
-- Name: get_validator_rewards_history(text, integer, integer); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_validator_rewards_history(_operator_address text, _limit integer DEFAULT 100, _offset integer DEFAULT 0) RETURNS TABLE(height bigint, rewards numeric, commission numeric, block_time timestamp with time zone)
    LANGUAGE plpgsql STABLE
    AS $$
BEGIN
  RETURN QUERY
  SELECT
    vr.height,
    vr.rewards,
    vr.commission,
    b.block_time
  FROM api.validator_rewards vr
  LEFT JOIN api.block_metrics b ON b.height = vr.height
  WHERE vr.validator_address IN (
    SELECT consensus_address
    FROM api.validator_consensus_addresses
    WHERE operator_address = _operator_address
    UNION
    SELECT _operator_address
  )
  ORDER BY vr.height DESC
  LIMIT _limit
  OFFSET _offset;
END;
$$;


--
-- Name: get_validator_signing_stats(text, integer); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_validator_signing_stats(_consensus_address text, _window_size integer DEFAULT 10000) RETURNS TABLE(total_blocks integer, blocks_signed integer, blocks_missed integer, signing_percentage numeric, recent_missed_count integer, first_signed_height bigint, last_signed_height bigint)
    LANGUAGE plpgsql STABLE
    AS $$
DECLARE
  max_height BIGINT;
BEGIN
  -- Get current max height
  SELECT MAX(id) INTO max_height FROM api.blocks_raw;

  RETURN QUERY
  SELECT
    COUNT(*)::INTEGER as total_blocks,
    COUNT(*) FILTER (WHERE vbs.signed)::INTEGER as blocks_signed,
    COUNT(*) FILTER (WHERE NOT vbs.signed)::INTEGER as blocks_missed,
    CASE
      WHEN COUNT(*) > 0 THEN
        ROUND((COUNT(*) FILTER (WHERE vbs.signed)::NUMERIC / COUNT(*)::NUMERIC) * 100, 2)
      ELSE 100
    END as signing_percentage,
    -- Recent missed in last 1000 blocks
    (SELECT COUNT(*)::INTEGER
     FROM api.validator_block_signatures
     WHERE consensus_address = UPPER(_consensus_address)
       AND NOT signed
       AND height > max_height - 1000) as recent_missed_count,
    MIN(vbs.height) FILTER (WHERE vbs.signed) as first_signed_height,
    MAX(vbs.height) FILTER (WHERE vbs.signed) as last_signed_height
  FROM api.validator_block_signatures vbs
  WHERE vbs.consensus_address = UPPER(_consensus_address)
    AND vbs.height > max_height - _window_size;
END;
$$;


--
-- Name: get_validator_total_rewards(text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_validator_total_rewards(_operator_address text) RETURNS TABLE(total_rewards numeric, total_commission numeric, blocks_with_rewards integer)
    LANGUAGE plpgsql STABLE
    AS $$
BEGIN
  RETURN QUERY
  SELECT
    COALESCE(SUM(vr.rewards), 0) as total_rewards,
    COALESCE(SUM(vr.commission), 0) as total_commission,
    COUNT(DISTINCT vr.height)::INTEGER as blocks_with_rewards
  FROM api.validator_rewards vr
  WHERE vr.validator_address IN (
    SELECT consensus_address
    FROM api.validator_consensus_addresses
    WHERE operator_address = _operator_address
    UNION
    SELECT _operator_address
  );
END;
$$;


--
-- Name: get_validators_paginated(integer, integer, text, text, text, text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_validators_paginated(_limit integer DEFAULT 20, _offset integer DEFAULT 0, _sort_by text DEFAULT 'tokens'::text, _sort_dir text DEFAULT 'desc'::text, _status text DEFAULT NULL::text, _search text DEFAULT NULL::text) RETURNS jsonb
    LANGUAGE plpgsql STABLE
    AS $$
DECLARE
  result JSONB;
BEGIN
  WITH total_bonded AS (
    SELECT COALESCE(SUM(tokens), 0) AS total
    FROM api.validators
    WHERE status = 'BOND_STATUS_BONDED' AND tokens IS NOT NULL
  ),
  filtered AS (
    SELECT
      v.*,
      COALESCE(v.consensus_address, vca.consensus_address) AS resolved_consensus_address,
      ipfs.ipfs_peer_id,
      CASE
        WHEN v.status = 'BOND_STATUS_BONDED' AND tb.total > 0 AND v.tokens IS NOT NULL
        THEN ROUND((v.tokens / tb.total) * 100, 4)
        ELSE 0
      END AS voting_power_pct,
      COALESCE(dc.delegator_count, 0) AS delegator_count
    FROM api.validators v
    CROSS JOIN total_bonded tb
    LEFT JOIN LATERAL (
      SELECT vca_inner.consensus_address
      FROM api.validator_consensus_addresses vca_inner
      WHERE vca_inner.operator_address = v.operator_address
      LIMIT 1
    ) vca ON true
    LEFT JOIN api.validator_ipfs_addresses ipfs
      ON ipfs.validator_address = v.operator_address
    LEFT JOIN api.mv_validator_delegator_counts dc
      ON dc.validator_address = v.operator_address
    WHERE (_status IS NULL OR v.status = _status)
    AND (_search IS NULL OR v.moniker ILIKE '%' || _search || '%' OR v.operator_address ILIKE '%' || _search || '%')
  ),
  total AS (
    SELECT COUNT(*) AS cnt FROM filtered
  ),
  sorted AS (
    SELECT * FROM filtered
    ORDER BY
      CASE WHEN _sort_by = 'tokens' AND _sort_dir = 'desc' THEN tokens END DESC NULLS LAST,
      CASE WHEN _sort_by = 'tokens' AND _sort_dir = 'asc' THEN tokens END ASC NULLS LAST,
      CASE WHEN _sort_by = 'moniker' AND _sort_dir = 'desc' THEN moniker END DESC NULLS LAST,
      CASE WHEN _sort_by = 'moniker' AND _sort_dir = 'asc' THEN moniker END ASC NULLS LAST,
      CASE WHEN _sort_by = 'commission' AND _sort_dir = 'desc' THEN commission_rate END DESC NULLS LAST,
      CASE WHEN _sort_by = 'commission' AND _sort_dir = 'asc' THEN commission_rate END ASC NULLS LAST,
      CASE WHEN _sort_by = 'status' AND _sort_dir = 'desc' THEN status END DESC NULLS LAST,
      CASE WHEN _sort_by = 'status' AND _sort_dir = 'asc' THEN status END ASC NULLS LAST,
      CASE WHEN _sort_by = 'delegators' AND _sort_dir = 'desc' THEN delegator_count END DESC,
      CASE WHEN _sort_by = 'delegators' AND _sort_dir = 'asc' THEN delegator_count END ASC,
      CASE WHEN _sort_by = 'uptime' AND _sort_dir = 'desc' THEN signing_percentage END DESC NULLS LAST,
      CASE WHEN _sort_by = 'uptime' AND _sort_dir = 'asc' THEN signing_percentage END ASC NULLS LAST,
      CASE WHEN _sort_by = 'voting_power' AND _sort_dir = 'desc' THEN voting_power_pct END DESC NULLS LAST,
      CASE WHEN _sort_by = 'voting_power' AND _sort_dir = 'asc' THEN voting_power_pct END ASC NULLS LAST,
      tokens DESC NULLS LAST
    LIMIT _limit OFFSET _offset
  )
  SELECT jsonb_build_object(
    'data', COALESCE(jsonb_agg(
      to_jsonb(s) - 'consensus_address' || jsonb_build_object('consensus_address', s.resolved_consensus_address)
    ), '[]'::jsonb),
    'pagination', jsonb_build_object(
      'total', (SELECT cnt FROM total),
      'limit', _limit,
      'offset', _offset,
      'has_next', _offset + _limit < (SELECT cnt FROM total),
      'has_prev', _offset > 0
    )
  )
  INTO result
  FROM sorted s;

  RETURN result;
END;
$$;


--
-- Name: get_validators_with_signing_stats(integer, integer); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.get_validators_with_signing_stats(_limit integer DEFAULT 100, _offset integer DEFAULT 0) RETURNS TABLE(operator_address text, moniker text, status text, jailed boolean, tokens numeric, voting_power_pct numeric, commission_rate numeric, signing_percentage numeric, blocks_missed integer)
    LANGUAGE plpgsql STABLE
    AS $$
BEGIN
  RETURN QUERY
  SELECT
    v.operator_address,
    v.moniker,
    v.status,
    v.jailed,
    v.tokens,
    v.voting_power_pct,
    v.commission_rate,
    COALESCE(ss.signing_percentage, 100) as signing_percentage,
    COALESCE(ss.blocks_missed::INTEGER, 0) as blocks_missed
  FROM api.validators v
  LEFT JOIN api.mv_validator_signing_stats ss ON ss.operator_address = v.operator_address
  WHERE v.status = 'BOND_STATUS_BONDED'
  ORDER BY v.tokens DESC NULLS LAST
  LIMIT _limit
  OFFSET _offset;
END;
$$;


--
-- Name: maybe_priority_decode(text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.maybe_priority_decode(_tx_id text) RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
  -- Check if this is a pending EVM tx that needs decoding
  IF EXISTS (SELECT 1 FROM api.evm_pending_decode WHERE tx_id = _tx_id) THEN
    PERFORM pg_notify('evm_decode_priority', _tx_id);
  END IF;
END;
$$;


--
-- Name: normalize_consensus_address(text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.normalize_consensus_address(addr text) RETURNS text
    LANGUAGE sql IMMUTABLE
    AS $$
  SELECT CASE
    WHEN addr IS NULL OR addr = '' THEN ''
    WHEN addr ~ '[+/=]' THEN UPPER(encode(decode(addr, 'base64'), 'hex'))
    ELSE UPPER(addr)
  END;
$$;


--
-- Name: notify_ibc_denom_pending(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.notify_ibc_denom_pending() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  PERFORM pg_notify('ibc_denom_pending', NEW.ibc_denom);
  RETURN NEW;
END;
$$;


--
-- Name: parse_block_time(jsonb); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.parse_block_time(data jsonb) RETURNS timestamp with time zone
    LANGUAGE sql IMMUTABLE STRICT
    AS $$
  SELECT (data->'block'->'header'->>'time')::timestamptz;
$$;


--
-- Name: propagate_jailing_event(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.propagate_jailing_event() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  resolved_operator TEXT;
BEGIN
  -- Resolve consensus address to operator address
  SELECT vca.operator_address INTO resolved_operator
  FROM api.validator_consensus_addresses vca
  WHERE vca.consensus_address = NEW.validator_address
  LIMIT 1;

  IF resolved_operator IS NOT NULL AND resolved_operator <> '' THEN
    UPDATE api.validators SET
      jailed = TRUE,
      updated_at = NOW()
    WHERE operator_address = resolved_operator;

    -- Trigger daemon to fetch fresh chain state
    PERFORM pg_notify('validator_refresh', resolved_operator);
  END IF;

  RETURN NEW;
END;
$$;


--
-- Name: queue_unknown_ibc_denom(text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.queue_unknown_ibc_denom(_ibc_denom text) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
BEGIN
  -- Only queue if it looks like an IBC denom and isn't already resolved
  IF _ibc_denom IS NULL OR NOT _ibc_denom LIKE 'ibc/%' THEN
    RETURN FALSE;
  END IF;

  -- Check if already in denom_traces (resolved)
  IF EXISTS (SELECT 1 FROM api.ibc_denom_traces WHERE ibc_denom = _ibc_denom) THEN
    RETURN FALSE;
  END IF;

  -- Check if already in denom_metadata (resolved)
  IF EXISTS (SELECT 1 FROM api.denom_metadata WHERE denom = _ibc_denom AND ibc_source_denom IS NOT NULL) THEN
    RETURN FALSE;
  END IF;

  -- Insert into pending queue (ignore if already queued)
  INSERT INTO api.ibc_denom_pending (ibc_denom)
  VALUES (_ibc_denom)
  ON CONFLICT (ibc_denom) DO NOTHING;

  RETURN TRUE;
END;
$$;


--
-- Name: refresh_analytics_views(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.refresh_analytics_views() RETURNS void
    LANGUAGE sql
    AS $$
  REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_validator_delegator_counts;
  REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_validator_leaderboard;
$$;


--
-- Name: refresh_rt_chain_stats(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.refresh_rt_chain_stats() RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
  -- Refresh chain stats from source tables
  UPDATE api.rt_chain_stats SET
    latest_block = COALESCE((SELECT MAX(id) FROM api.blocks_raw), 0),
    total_transactions = COALESCE((SELECT count(*) FROM api.transactions_main), 0),
    updated_at = NOW()
  WHERE id = 1;

  -- Prune old hourly tx stats (>7 days)
  DELETE FROM api.rt_hourly_tx_stats WHERE hour < NOW() - INTERVAL '7 days';

  -- Prune old hourly rewards (>48 hours)
  DELETE FROM api.rt_hourly_rewards WHERE hour < NOW() - INTERVAL '48 hours';
END;
$$;


--
-- Name: register_validator_consensus_address(text, text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.register_validator_consensus_address(_operator_address text, _pubkey_base64 text) RETURNS text
    LANGUAGE plpgsql
    AS $$
DECLARE
  hex_addr TEXT;
BEGIN
  -- Compute hex consensus address from pubkey
  hex_addr := api.compute_consensus_address(_pubkey_base64);

  -- Insert/update hex entry in mapping table
  INSERT INTO api.validator_consensus_addresses (
    consensus_address, operator_address, hex_address, first_seen_height
  ) VALUES (
    hex_addr, _operator_address, hex_addr, 1
  )
  ON CONFLICT (consensus_address) DO UPDATE
  SET operator_address = _operator_address,
      hex_address = hex_addr;

  -- Update validators table
  UPDATE api.validators
  SET consensus_address = hex_addr
  WHERE operator_address = _operator_address
    AND (consensus_address IS NULL OR consensus_address = '');

  -- Also update any base64 entries that have matching hex_address
  UPDATE api.validator_consensus_addresses
  SET operator_address = _operator_address
  WHERE hex_address = hex_addr
    AND operator_address IS NULL;

  RETURN hex_addr;
END;
$$;


--
-- Name: request_evm_decode(text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.request_evm_decode(_tx_hash text) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
BEGIN
  -- Check if this tx exists and is pending EVM decode
  IF EXISTS (SELECT 1 FROM api.evm_pending_decode WHERE tx_id = _tx_hash) THEN
    PERFORM pg_notify('evm_decode_priority', _tx_hash);
    RETURN jsonb_build_object('success', true);
  END IF;

  -- Already decoded or not an EVM tx
  RETURN jsonb_build_object('success', false);
END;
$$;


--
-- Name: request_validator_refresh(text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.request_validator_refresh(_operator_address text) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
BEGIN
  -- Validate that the address looks like a valoper address
  IF _operator_address IS NULL OR _operator_address = '' THEN
    RETURN jsonb_build_object('success', false, 'message', 'operator_address required');
  END IF;

  -- Check if this validator exists in our table
  IF EXISTS (SELECT 1 FROM api.validators WHERE operator_address = _operator_address) THEN
    PERFORM pg_notify('validator_refresh', _operator_address);
    RETURN jsonb_build_object('success', true);
  END IF;

  -- Unknown validator - still notify in case it's new and not yet in table
  PERFORM pg_notify('validator_refresh', _operator_address);
  RETURN jsonb_build_object('success', true, 'message', 'validator not in table, refresh requested');
END;
$$;


--
-- Name: resolve_denom(text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.resolve_denom(_denom text) RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  SELECT COALESCE(
    -- First try denom_metadata (most comprehensive)
    (SELECT jsonb_build_object(
      'denom', denom,
      'symbol', symbol,
      'decimals', decimals,
      'is_native', is_native,
      'source_chain', ibc_source_chain,
      'source_denom', ibc_source_denom
    ) FROM api.denom_metadata WHERE denom = _denom),
    -- Then try ibc_denom_traces
    (SELECT jsonb_build_object(
      'denom', ibc_denom,
      'symbol', COALESCE(symbol, base_denom),
      'decimals', COALESCE(decimals, 6),
      'is_native', false,
      'source_chain', source_chain_id,
      'source_denom', base_denom
    ) FROM api.ibc_denom_traces WHERE ibc_denom = _denom),
    -- Fallback: parse common patterns
    CASE
      WHEN _denom LIKE 'u%' THEN jsonb_build_object(
        'denom', _denom,
        'symbol', UPPER(SUBSTRING(_denom FROM 2)),
        'decimals', 6,
        'is_native', true,
        'source_chain', NULL,
        'source_denom', NULL
      )
      WHEN _denom LIKE 'a%' THEN jsonb_build_object(
        'denom', _denom,
        'symbol', UPPER(SUBSTRING(_denom FROM 2)),
        'decimals', 18,
        'is_native', true,
        'source_chain', NULL,
        'source_denom', NULL
      )
      WHEN _denom LIKE 'ibc/%' THEN jsonb_build_object(
        'denom', _denom,
        'symbol', 'IBC/' || SUBSTRING(_denom FROM 5 FOR 8) || '...',
        'decimals', 6,
        'is_native', false,
        'source_chain', NULL,
        'source_denom', NULL
      )
      ELSE jsonb_build_object(
        'denom', _denom,
        'symbol', UPPER(_denom),
        'decimals', 6,
        'is_native', NULL,
        'source_chain', NULL,
        'source_denom', NULL
      )
    END
  );
$$;


--
-- Name: resolve_ibc_denom(text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.resolve_ibc_denom(_ibc_denom text) RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  SELECT jsonb_build_object(
    'ibc_denom', t.ibc_denom,
    'base_denom', t.base_denom,
    'path', t.path,
    'source_channel', t.source_channel,
    'source_chain_id', COALESCE(t.source_chain_id, c.counterparty_chain_id),
    'symbol', t.symbol,
    'decimals', t.decimals,
    'route', jsonb_build_object(
      'channel_id', c.channel_id,
      'connection_id', c.connection_id,
      'client_id', c.client_id,
      'counterparty_channel_id', c.counterparty_channel_id,
      'counterparty_connection_id', c.counterparty_connection_id,
      'counterparty_client_id', c.counterparty_client_id
    )
  )
  FROM api.ibc_denom_traces t
  LEFT JOIN api.ibc_connections c ON t.source_channel = c.channel_id AND c.port_id = 'transfer'
  WHERE t.ibc_denom = _ibc_denom;
$$;


--
-- Name: track_governance_vote(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.track_governance_vote() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  msg_record RECORD;
  prop_id BIGINT;
  vote_option TEXT;
BEGIN
  FOR msg_record IN
    SELECT m.id, m.metadata, m.sender
    FROM api.messages_main m
    WHERE m.id = NEW.id
    AND m.type LIKE '%MsgVote%'
  LOOP
    prop_id := (msg_record.metadata->>'proposalId')::BIGINT;
    vote_option := msg_record.metadata->>'option';

    IF prop_id IS NOT NULL AND vote_option IS NOT NULL THEN
      -- Update vote tallies based on vote option
      UPDATE api.governance_proposals
      SET
        yes_count = CASE WHEN vote_option = 'VOTE_OPTION_YES'
          THEN COALESCE(yes_count::bigint, 0) + 1 END::TEXT,
        no_count = CASE WHEN vote_option = 'VOTE_OPTION_NO'
          THEN COALESCE(no_count::bigint, 0) + 1 END::TEXT,
        abstain_count = CASE WHEN vote_option = 'VOTE_OPTION_ABSTAIN'
          THEN COALESCE(abstain_count::bigint, 0) + 1 END::TEXT,
        no_with_veto_count = CASE WHEN vote_option = 'VOTE_OPTION_NO_WITH_VETO'
          THEN COALESCE(no_with_veto_count::bigint, 0) + 1 END::TEXT,
        status = 'PROPOSAL_STATUS_VOTING_PERIOD',
        last_updated = NOW()
      WHERE proposal_id = prop_id;
    END IF;
  END LOOP;

  RETURN NEW;
END;
$$;


--
-- Name: trg_populate_block_metrics(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.trg_populate_block_metrics() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  INSERT INTO api.block_metrics (height, block_time, tx_count)
  VALUES (NEW.id, NEW.block_time, COALESCE(NEW.tx_count, 0))
  ON CONFLICT (height) DO UPDATE SET
    block_time = EXCLUDED.block_time,
    tx_count = EXCLUDED.tx_count;

  RETURN NEW;
END;
$$;


--
-- Name: trg_rt_chain_stats_block(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.trg_rt_chain_stats_block() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  UPDATE api.rt_chain_stats SET latest_block = NEW.id, updated_at = NOW() WHERE id = 1;
  RETURN NEW;
END;
$$;


--
-- Name: trg_rt_chain_stats_evm(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.trg_rt_chain_stats_evm() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  UPDATE api.rt_chain_stats SET evm_transactions = evm_transactions + 1, updated_at = NOW() WHERE id = 1;
  RETURN NEW;
END;
$$;


--
-- Name: trg_rt_chain_stats_tx(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.trg_rt_chain_stats_tx() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  UPDATE api.rt_chain_stats SET total_transactions = total_transactions + 1, updated_at = NOW() WHERE id = 1;
  RETURN NEW;
END;
$$;


--
-- Name: trg_rt_chain_stats_validators(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.trg_rt_chain_stats_validators() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  -- Only recount if status or jailed actually changed
  IF OLD.status IS DISTINCT FROM NEW.status OR OLD.jailed IS DISTINCT FROM NEW.jailed THEN
    UPDATE api.rt_chain_stats SET
      active_validators = (SELECT COUNT(*)::INTEGER FROM api.validators WHERE status = 'BOND_STATUS_BONDED' AND NOT jailed),
      updated_at = NOW()
    WHERE id = 1;
  END IF;
  RETURN NEW;
END;
$$;


--
-- Name: trg_rt_daily_rewards(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.trg_rt_daily_rewards() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  block_time TIMESTAMPTZ;
  reward_date DATE;
BEGIN
  SELECT b.block_time INTO block_time
  FROM api.blocks_raw b
  WHERE b.id = NEW.height;

  IF block_time IS NULL THEN
    block_time := NOW();
  END IF;

  reward_date := block_time::DATE;

  INSERT INTO api.rt_daily_rewards (date, total_rewards, total_commission)
  VALUES (reward_date, COALESCE(NEW.rewards, 0), COALESCE(NEW.commission, 0))
  ON CONFLICT (date) DO UPDATE SET
    total_rewards = api.rt_daily_rewards.total_rewards + COALESCE(NEW.rewards, 0),
    total_commission = api.rt_daily_rewards.total_commission + COALESCE(NEW.commission, 0);

  RETURN NEW;
END;
$$;


--
-- Name: trg_rt_hourly_rewards(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.trg_rt_hourly_rewards() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  block_time TIMESTAMPTZ;
  reward_hour TIMESTAMPTZ;
BEGIN
  SELECT b.block_time INTO block_time
  FROM api.blocks_raw b
  WHERE b.id = NEW.height;

  IF block_time IS NULL THEN
    block_time := NOW();
  END IF;

  reward_hour := date_trunc('hour', block_time);

  INSERT INTO api.rt_hourly_rewards (hour, rewards, commission)
  VALUES (reward_hour, COALESCE(NEW.rewards, 0), COALESCE(NEW.commission, 0))
  ON CONFLICT (hour) DO UPDATE SET
    rewards = api.rt_hourly_rewards.rewards + COALESCE(NEW.rewards, 0),
    commission = api.rt_hourly_rewards.commission + COALESCE(NEW.commission, 0);

  -- Prune moved to periodic refresh (was causing deadlocks)
  RETURN NEW;
END;
$$;


--
-- Name: trg_rt_message_type_stats(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.trg_rt_message_type_stats() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  INSERT INTO api.rt_message_type_stats (message_type, count)
  VALUES (NEW.type, 1)
  ON CONFLICT (message_type) DO UPDATE SET count = api.rt_message_type_stats.count + 1;
  RETURN NEW;
END;
$$;


--
-- Name: trg_rt_tx_stats(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.trg_rt_tx_stats() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  tx_date DATE;
  tx_hour TIMESTAMPTZ;
  is_error BOOLEAN;
BEGIN
  IF NEW.timestamp IS NULL THEN
    RETURN NEW;
  END IF;

  tx_date := NEW.timestamp::DATE;
  tx_hour := date_trunc('hour', NEW.timestamp);
  is_error := NEW.error IS NOT NULL;

  INSERT INTO api.rt_daily_tx_stats (date, total_txs, successful_txs, failed_txs)
  VALUES (
    tx_date,
    1,
    CASE WHEN NOT is_error THEN 1 ELSE 0 END,
    CASE WHEN is_error THEN 1 ELSE 0 END
  )
  ON CONFLICT (date) DO UPDATE SET
    total_txs = api.rt_daily_tx_stats.total_txs + 1,
    successful_txs = api.rt_daily_tx_stats.successful_txs + CASE WHEN NOT is_error THEN 1 ELSE 0 END,
    failed_txs = api.rt_daily_tx_stats.failed_txs + CASE WHEN is_error THEN 1 ELSE 0 END;

  INSERT INTO api.rt_hourly_tx_stats (hour, tx_count)
  VALUES (tx_hour, 1)
  ON CONFLICT (hour) DO UPDATE SET tx_count = api.rt_hourly_tx_stats.tx_count + 1;

  -- Prune moved to periodic refresh (was causing deadlocks)
  RETURN NEW;
END;
$$;


--
-- Name: trg_set_block_time(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.trg_set_block_time() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  NEW.block_time := api.parse_block_time(NEW.data);
  RETURN NEW;
END;
$$;


--
-- Name: trg_update_validator_liveness(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.trg_update_validator_liveness() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  addr TEXT;
  op_addr TEXT;
BEGIN
  IF NEW.event_type NOT IN ('slash', 'liveness', 'jail') THEN
    RETURN NEW;
  END IF;

  addr := COALESCE(NEW.attributes->>'address', NEW.attributes->>'validator', '');
  IF addr = '' THEN
    RETURN NEW;
  END IF;

  -- Resolve to operator address (exact match - table has bech32, base64, hex entries)
  SELECT vca.operator_address INTO op_addr
  FROM api.validator_consensus_addresses vca
  WHERE vca.consensus_address = addr
  LIMIT 1;

  IF op_addr IS NULL THEN
    RETURN NEW;
  END IF;

  UPDATE api.validators
  SET
    missed_blocks_counter = COALESCE(
      NULLIF(NEW.attributes->>'missed_blocks', '')::INTEGER,
      NULLIF(NEW.attributes->>'missed_blocks_counter', '')::INTEGER,
      missed_blocks_counter
    ),
    last_jailed_height = CASE
      WHEN NEW.event_type = 'jail' THEN NEW.height
      ELSE last_jailed_height
    END,
    last_jailed_at = CASE
      WHEN NEW.event_type = 'jail' THEN NOW()
      ELSE last_jailed_at
    END,
    updated_at = NOW()
  WHERE operator_address = op_addr;

  RETURN NEW;
END;
$$;


--
-- Name: trg_update_validator_signing_stats(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.trg_update_validator_signing_stats() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  op_addr TEXT;
  oldest_was_signed BOOLEAN;
  new_signed BIGINT;
  new_missed BIGINT;
BEGIN
  -- Resolve consensus address to operator address
  SELECT vca.operator_address INTO op_addr
  FROM api.validator_consensus_addresses vca
  WHERE vca.consensus_address = NEW.consensus_address
    OR vca.hex_address = UPPER(NEW.consensus_address)
  LIMIT 1;

  IF op_addr IS NULL THEN
    RETURN NEW;
  END IF;

  -- Check if we need to evict the oldest entry from the 10K window.
  -- Look up the row exactly 10,000 blocks ago for this validator.
  SELECT signed INTO oldest_was_signed
  FROM api.validator_block_signatures
  WHERE consensus_address = NEW.consensus_address
    AND height = NEW.height - 10000;

  -- Compute new counters using local variables for clarity
  SELECT
    GREATEST(0, COALESCE(v.blocks_signed, 0)
      + (CASE WHEN NEW.signed THEN 1 ELSE 0 END)
      - (CASE WHEN oldest_was_signed IS TRUE THEN 1 ELSE 0 END)),
    GREATEST(0, COALESCE(v.blocks_missed, 0)
      + (CASE WHEN NOT NEW.signed THEN 1 ELSE 0 END)
      - (CASE WHEN oldest_was_signed IS FALSE THEN 1 ELSE 0 END))
  INTO new_signed, new_missed
  FROM api.validators v
  WHERE v.operator_address = op_addr;

  UPDATE api.validators
  SET
    blocks_signed = new_signed,
    blocks_missed = new_missed,
    signing_percentage = CASE
      WHEN new_signed + new_missed > 0
      THEN ROUND(new_signed::NUMERIC / (new_signed + new_missed)::NUMERIC * 100, 2)
      ELSE NULL
    END,
    last_signed_height = CASE WHEN NEW.signed THEN NEW.height ELSE last_signed_height END,
    updated_at = NOW()
  WHERE operator_address = op_addr;

  RETURN NEW;
END;
$$;


--
-- Name: trigger_extract_signatures(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.trigger_extract_signatures() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  PERFORM api.extract_block_signatures(NEW.id, NEW.data);
  RETURN NEW;
END;
$$;


--
-- Name: universal_search(text); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.universal_search(_query text) RETURNS jsonb
    LANGUAGE plpgsql STABLE
    AS $_$
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
      'value', jsonb_build_object('id', id),
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
  -- Use lower() since transactions_main stores hashes in lowercase
  IF trimmed ~ '^[a-fA-F0-9]{64}$' THEN
    SELECT jsonb_build_object(
      'type', 'transaction',
      'value', jsonb_build_object('id', id),
      'score', 100
    ) INTO tx_result
    FROM api.transactions_main
    WHERE id = lower(trimmed);

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
$_$;


--
-- Name: update_block_tx_count(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.update_block_tx_count() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  UPDATE api.blocks_raw
  SET tx_count = tx_count + 1
  WHERE id = NEW.height;
  RETURN NEW;
END;
$$;


--
-- Name: update_event_main(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.update_event_main() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  a jsonb;
  a_ord int;
  msg_idx bigint;
  ev_type text;
BEGIN
  msg_idx := api.extract_event_msg_index(NEW.data);
  ev_type := NEW.data->>'type';

  DELETE FROM api.events_main
  WHERE id = NEW.id AND event_index = NEW.event_index;

  FOR a, a_ord IN
    SELECT attr, (ord::int - 1)
    FROM jsonb_array_elements(NEW.data->'attributes') WITH ORDINALITY AS t(attr, ord)
  LOOP
    INSERT INTO api.events_main (
      id, event_index, attr_index, event_type, attr_key, attr_value, msg_index
    ) VALUES (
      NEW.id,
      NEW.event_index,
      a_ord,
      ev_type,
      a->>'key',
      a->>'value',
      msg_idx
    );
  END LOOP;

  RETURN NEW;
END $$;


--
-- Name: update_events_raw(); Type: FUNCTION; Schema: api; Owner: -
--

CREATE OR REPLACE FUNCTION api.update_events_raw() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  ev jsonb;
  ev_ord int;
BEGIN
  -- Skip error-metadata transactions (no txResponse means fetch failed)
  IF NEW.data->'txResponse' IS NULL THEN
    RETURN NEW;
  END IF;

  DELETE FROM api.events_raw WHERE id = NEW.id;

  FOR ev, ev_ord IN
    SELECT e, (ord::int - 1)
    FROM jsonb_array_elements(NEW.data->'txResponse'->'events') WITH ORDINALITY AS t(e, ord)
  LOOP
    INSERT INTO api.events_raw (id, event_index, data)
    VALUES (NEW.id, ev_ord, ev);
  END LOOP;

  RETURN NEW;
END
$$;


--
-- Name: base64_to_hex_address(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE OR REPLACE FUNCTION public.base64_to_hex_address(b64 text) RETURNS text
    LANGUAGE plpgsql STABLE
    AS $$
DECLARE
  raw_bytes BYTEA;
BEGIN
  IF b64 IS NULL OR b64 = '' THEN
    RETURN NULL;
  END IF;

  BEGIN
    raw_bytes := decode(b64, 'base64');
    -- EVM addresses are 20 bytes
    IF length(raw_bytes) = 20 THEN
      RETURN '0x' || encode(raw_bytes, 'hex');
    ELSE
      RETURN NULL;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
  END;
END;
$$;


--
-- Name: extract_addresses(jsonb); Type: FUNCTION; Schema: public; Owner: -
--

CREATE OR REPLACE FUNCTION public.extract_addresses(msg jsonb) RETURNS text[]
    LANGUAGE sql STABLE
    AS $_$
WITH
  -- Extract Bech32 addresses (Cosmos standard)
  bech32_addresses AS (
    SELECT unnest(
      regexp_matches(
        msg::text,
        E'(?<=[\\"\'\\\\s]|^)([a-z0-9]{2,83}1[qpzry9x8gf2tvdw0s3jn54khce6mua7l]{38,})(?=[\\"\'\\\\s]|$)',
        'g'
      )
    ) AS addr
  ),
  -- Extract EVM hex addresses (0x followed by 40 hex chars)
  evm_addresses AS (
    SELECT unnest(
      regexp_matches(
        msg::text,
        E'(0x[a-fA-F0-9]{40})(?=[\\"\'\\\\s,}\\]]|$)',
        'gi'
      )
    ) AS addr
  ),
  all_addresses AS (
    SELECT addr FROM bech32_addresses
    UNION
    SELECT addr FROM evm_addresses
  )
SELECT array_agg(DISTINCT addr)
FROM all_addresses
WHERE addr IS NOT NULL;
$_$;


--
-- Name: extract_metadata(jsonb); Type: FUNCTION; Schema: public; Owner: -
--

CREATE OR REPLACE FUNCTION public.extract_metadata(msg jsonb) RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  WITH keys_to_remove AS (
      SELECT ARRAY['@type', 'sender', 'executor', 'admin', 'voter', 'messages', 'proposalId', 'proposers', 'authority', 'fromAddress']::text[] AS keys
  )
  SELECT msg - (SELECT keys FROM keys_to_remove)
$$;


--
-- Name: extract_proposal_failure_logs(jsonb); Type: FUNCTION; Schema: public; Owner: -
--

CREATE OR REPLACE FUNCTION public.extract_proposal_failure_logs(json_data jsonb) RETURNS text
    LANGUAGE sql
    AS $$
WITH
  events AS (
    SELECT jsonb_array_elements(json_data->'txResponse'->'events') AS event
  ),
  typed_attributes AS (
    SELECT
      event->>'type' AS event_type,
      jsonb_array_elements(event->'attributes') AS attribute
    FROM events
  )
  SELECT
    TRIM(BOTH '"' FROM typed_attributes.attribute->>'value') AS logs
  FROM typed_attributes
  WHERE
    typed_attributes.event_type = 'cosmos.group.v1.EventExec'
    AND typed_attributes.attribute->>'key' = 'logs'
    AND EXISTS (
      SELECT 1
      FROM typed_attributes t2
      WHERE t2.event_type = typed_attributes.event_type
        AND t2.attribute->>'key' = 'result'
        AND t2.attribute->>'value' = '"PROPOSAL_EXECUTOR_RESULT_FAILURE"'
    )
  LIMIT 1;
$$;


--
-- Name: extract_proposal_ids(jsonb); Type: FUNCTION; Schema: public; Owner: -
--

CREATE OR REPLACE FUNCTION public.extract_proposal_ids(events jsonb) RETURNS text[]
    LANGUAGE plpgsql
    AS $$
DECLARE
  proposal_ids TEXT[];
BEGIN
   SELECT
     ARRAY_AGG(DISTINCT TRIM(BOTH '"' FROM attr->>'value'))
   INTO proposal_ids
   FROM jsonb_array_elements(events) AS ev(event)
   CROSS JOIN LATERAL jsonb_array_elements(ev.event->'attributes') AS attr
   WHERE attr->>'key' = 'proposal_id';

  RETURN proposal_ids;
END;
$$;


--
-- Name: update_message_main(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE OR REPLACE FUNCTION public.update_message_main() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  sender TEXT;
  mentions TEXT[];
  metadata JSONB;
  decoded_bytes BYTEA;
  decoded_text TEXT;
  decoded_json JSONB;
  new_addresses TEXT[];
  evm_from_hex TEXT;
BEGIN
  -- Try to extract EVM from address first (for MsgEthereumTx)
  IF NEW.data->>'@type' = '/cosmos.evm.vm.v1.MsgEthereumTx' THEN
    evm_from_hex := base64_to_hex_address(NEW.data->>'from');
  END IF;

  sender := COALESCE(
    -- EVM from address (converted to hex)
    evm_from_hex,
    -- Standard Cosmos sender fields
    NULLIF(NEW.data->>'sender', ''),
    NULLIF(NEW.data->>'fromAddress', ''),
    -- Delegator address for staking messages
    NULLIF(NEW.data->>'delegatorAddress', ''),
    -- Derive from validator address if no delegator (MsgCreateValidator)
    valoper_to_delegator(NEW.data->>'validatorAddress'),
    -- Other common sender fields
    NULLIF(NEW.data->>'admin', ''),
    NULLIF(NEW.data->>'voter', ''),
    NULLIF(NEW.data->>'depositor', ''),
    NULLIF(NEW.data->>'address', ''),
    NULLIF(NEW.data->>'executor', ''),
    NULLIF(NEW.data->>'authority', ''),
    NULLIF(NEW.data->>'granter', ''),
    NULLIF(NEW.data->>'grantee', ''),
    NULLIF(NEW.data->>'signer', ''),
    -- Group proposal proposers
    (
      SELECT jsonb_array_elements_text(NEW.data->'proposers')
      LIMIT 1
    ),
    -- Multi-send inputs
    (
      CASE
        WHEN jsonb_typeof(NEW.data->'inputs') = 'array'
             AND jsonb_array_length(NEW.data->'inputs') > 0
        THEN NEW.data->'inputs'->0->>'address'
        ELSE NULL
      END
    )
  );

  mentions := extract_addresses(NEW.data);
  metadata := extract_metadata(NEW.data);

  -- Extract decoded data from IBC packet
  IF NEW.data->>'@type' = '/ibc.core.channel.v1.MsgRecvPacket' THEN
    IF metadata->'packet' ? 'data' THEN
      BEGIN
        decoded_bytes := decode(metadata->'packet'->>'data', 'base64');
        decoded_text := convert_from(decoded_bytes, 'UTF8');
        decoded_json := decoded_text::jsonb;
        metadata := metadata || jsonb_build_object('decodedData', decoded_json);
        IF decoded_json ? 'sender' THEN
          sender := decoded_json->>'sender';
        END IF;
        new_addresses := extract_addresses(decoded_json);
        SELECT array_agg(DISTINCT addr) INTO mentions
        FROM unnest(mentions || new_addresses) AS addr;
      EXCEPTION WHEN OTHERS THEN
        UPDATE api.transactions_main
        SET error = 'Error decoding base64 packet data'
        WHERE id = NEW.id;
      END;
    END IF;
  END IF;

  INSERT INTO api.messages_main (id, message_index, type, sender, mentions, metadata)
  VALUES (
           NEW.id,
           NEW.message_index,
           NEW.data->>'@type',
           sender,
           mentions,
           metadata
         )
  ON CONFLICT (id, message_index) DO UPDATE
  SET type = EXCLUDED.type,
      sender = EXCLUDED.sender,
      mentions = EXCLUDED.mentions,
      metadata = EXCLUDED.metadata;

  RETURN NEW;
END;
$$;


--
-- Name: update_transaction_main(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE OR REPLACE FUNCTION public.update_transaction_main() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  error_text TEXT;
  proposal_ids TEXT[];
BEGIN
  -- Skip error-metadata transactions (no txResponse means fetch failed)
  IF NEW.data->'txResponse' IS NULL THEN
    RETURN NEW;
  END IF;

  error_text := NEW.data->'txResponse'->>'rawLog';

  IF error_text IS NULL THEN
    error_text := extract_proposal_failure_logs(NEW.data);
  END IF;

  proposal_ids := extract_proposal_ids(NEW.data->'txResponse'->'events');

  INSERT INTO api.transactions_main (id, fee, memo, error, height, timestamp, proposal_ids)
  VALUES (
            NEW.id,
            NEW.data->'tx'->'authInfo'->'fee',
            NEW.data->'tx'->'body'->>'memo',
            error_text,
            (NEW.data->'txResponse'->>'height')::BIGINT,
            (NEW.data->'txResponse'->>'timestamp')::TIMESTAMPTZ,
            proposal_ids
         )
  ON CONFLICT (id) DO UPDATE
  SET fee = EXCLUDED.fee,
      memo = EXCLUDED.memo,
      error = EXCLUDED.error,
      height = EXCLUDED.height,
      timestamp = EXCLUDED.timestamp,
      proposal_ids = EXCLUDED.proposal_ids;

  -- Insert top level messages
  INSERT INTO api.messages_raw (id, message_index, data)
  SELECT
    NEW.id,
    message_index - 1,
    message
  FROM jsonb_array_elements(NEW.data->'tx'->'body'->'messages') WITH ORDINALITY AS message(message, message_index)
  ON CONFLICT (id, message_index) DO UPDATE
  SET data = EXCLUDED.data;

  -- Insert nested messages (e.g., within proposals)
  INSERT INTO api.messages_raw (id, message_index, data)
  SELECT
    NEW.id,
    10000 + ((top_level.msg_index - 1) * 1000) + sub_level.sub_index,
    sub_level.sub_msg
  FROM jsonb_array_elements(NEW.data->'tx'->'body'->'messages')
       WITH ORDINALITY AS top_level(msg, msg_index)
       CROSS JOIN LATERAL (
         SELECT sub_msg, sub_index
         FROM jsonb_array_elements(top_level.msg->'messages')
              WITH ORDINALITY AS inner_msg(sub_msg, sub_index)
       ) AS sub_level
  WHERE top_level.msg->>'@type' = '/cosmos.group.v1.MsgSubmitProposal'
    AND top_level.msg->'messages' IS NOT NULL
  ON CONFLICT (id, message_index) DO UPDATE
  SET data = EXCLUDED.data;

  RETURN NEW;
END;
$$;


--
-- Name: valoper_to_delegator(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE OR REPLACE FUNCTION public.valoper_to_delegator(valoper_addr text) RETURNS text
    LANGUAGE plpgsql STABLE
    AS $$
DECLARE
  prefix_end INT;
  prefix TEXT;
BEGIN
  IF valoper_addr IS NULL OR valoper_addr = '' THEN
    RETURN NULL;
  END IF;

  -- Find 'valoper1' and extract prefix before it
  prefix_end := position('valoper1' in valoper_addr);
  IF prefix_end = 0 THEN
    RETURN NULL;
  END IF;

  prefix := substring(valoper_addr from 1 for prefix_end - 1);
  -- Return prefix + '1' + rest after 'valoper1'
  RETURN prefix || substring(valoper_addr from prefix_end + 7);
END;
$$;


--
-- Name: block_metrics; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.block_metrics (
    height bigint NOT NULL,
    block_time timestamp with time zone,
    tx_count integer DEFAULT 0,
    gas_used bigint DEFAULT 0,
    total_rewards numeric(78,18) DEFAULT 0,
    total_commission numeric(78,18) DEFAULT 0,
    validator_count integer DEFAULT 0,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: block_results_raw; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.block_results_raw (
    id bigint NOT NULL,
    height bigint NOT NULL,
    data jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: blocks_raw; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.blocks_raw (
    id bigint NOT NULL,
    data jsonb NOT NULL,
    tx_count integer DEFAULT 0,
    block_time timestamp with time zone
);


--
-- Name: chain_features; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.chain_features (
    chain_id text NOT NULL,
    features text[] DEFAULT '{}'::text[] NOT NULL
);


--
-- Name: chain_params; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.chain_params (
    key text NOT NULL,
    value text NOT NULL,
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: rt_chain_stats; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.rt_chain_stats (
    id integer DEFAULT 1 NOT NULL,
    latest_block bigint DEFAULT 0 NOT NULL,
    total_transactions bigint DEFAULT 0 NOT NULL,
    unique_addresses bigint DEFAULT 0 NOT NULL,
    evm_transactions bigint DEFAULT 0 NOT NULL,
    active_validators integer DEFAULT 0 NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT rt_chain_stats_id_check CHECK ((id = 1))
)
WITH (fillfactor='50');


--
-- Name: chain_stats; Type: VIEW; Schema: api; Owner: -
--

CREATE OR REPLACE VIEW api.chain_stats AS
 SELECT latest_block,
    total_transactions,
    unique_addresses,
    evm_transactions,
    active_validators
   FROM api.rt_chain_stats
  WHERE (id = 1);


--
-- Name: compute_benchmarks; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.compute_benchmarks (
    benchmark_id bigint NOT NULL,
    creator text NOT NULL,
    benchmark_type text,
    upload_endpoint text,
    retrieve_endpoint text,
    result_file_hash text,
    status text DEFAULT 'PENDING'::text NOT NULL,
    submit_tx_hash text NOT NULL,
    submit_height bigint,
    submit_time timestamp with time zone,
    result_tx_hash text,
    result_validator text,
    result_height bigint,
    result_time timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT compute_benchmarks_status_check CHECK ((status = ANY (ARRAY['PENDING'::text, 'COMPLETED'::text, 'FAILED'::text])))
);


--
-- Name: compute_committee_proposals; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.compute_committee_proposals (
    id integer NOT NULL,
    proposer text NOT NULL,
    target_height bigint,
    tx_hash text NOT NULL,
    height bigint,
    "timestamp" timestamp with time zone,
    weighted_validators jsonb
);


--
-- Name: compute_committee_proposals_id_seq; Type: SEQUENCE; Schema: api; Owner: -
--

CREATE SEQUENCE IF NOT EXISTS api.compute_committee_proposals_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: compute_committee_proposals_id_seq; Type: SEQUENCE OWNED BY; Schema: api; Owner: -
--

ALTER SEQUENCE api.compute_committee_proposals_id_seq OWNED BY api.compute_committee_proposals.id;


--
-- Name: compute_jobs; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.compute_jobs (
    job_id bigint NOT NULL,
    creator text NOT NULL,
    target_validator text NOT NULL,
    execution_image text,
    result_upload_endpoint text,
    result_fetch_endpoint text,
    verification_image text,
    fee_denom text,
    fee_amount text,
    status text DEFAULT 'PENDING'::text NOT NULL,
    result_hash text,
    submit_tx_hash text NOT NULL,
    submit_height bigint,
    submit_time timestamp with time zone,
    result_tx_hash text,
    result_height bigint,
    result_time timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT compute_jobs_status_check CHECK ((status = ANY (ARRAY['PENDING'::text, 'COMPLETED'::text, 'FAILED'::text])))
);


--
-- Name: compute_seed_contributions; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.compute_seed_contributions (
    id integer NOT NULL,
    validator text NOT NULL,
    benchmark_id bigint,
    tx_hash text NOT NULL,
    height bigint,
    "timestamp" timestamp with time zone
);


--
-- Name: compute_seed_contributions_id_seq; Type: SEQUENCE; Schema: api; Owner: -
--

CREATE SEQUENCE IF NOT EXISTS api.compute_seed_contributions_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: compute_seed_contributions_id_seq; Type: SEQUENCE OWNED BY; Schema: api; Owner: -
--

ALTER SEQUENCE api.compute_seed_contributions_id_seq OWNED BY api.compute_seed_contributions.id;


--
-- Name: compute_stats; Type: VIEW; Schema: api; Owner: -
--

CREATE OR REPLACE VIEW api.compute_stats AS
 SELECT ( SELECT count(*) AS count
           FROM api.compute_jobs) AS total_jobs,
    ( SELECT count(*) AS count
           FROM api.compute_jobs
          WHERE (compute_jobs.status = 'PENDING'::text)) AS pending_jobs,
    ( SELECT count(*) AS count
           FROM api.compute_jobs
          WHERE (compute_jobs.status = 'COMPLETED'::text)) AS completed_jobs,
    ( SELECT count(*) AS count
           FROM api.compute_jobs
          WHERE (compute_jobs.status = 'FAILED'::text)) AS failed_jobs,
    ( SELECT count(*) AS count
           FROM api.compute_benchmarks) AS total_benchmarks,
    ( SELECT count(*) AS count
           FROM api.compute_benchmarks
          WHERE (compute_benchmarks.status = 'COMPLETED'::text)) AS completed_benchmarks;


--
-- Name: transactions_main; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.transactions_main (
    id text NOT NULL,
    height bigint NOT NULL,
    "timestamp" timestamp with time zone,
    fee jsonb,
    memo text,
    error text,
    proposal_ids text[]
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02');


--
-- Name: daily_active_addresses; Type: VIEW; Schema: api; Owner: -
--

CREATE OR REPLACE VIEW api.daily_active_addresses AS
 SELECT date(t."timestamp") AS date,
    count(DISTINCT m.sender) AS active_addresses
   FROM (api.messages_main m
     JOIN api.transactions_main t ON ((t.id = m.id)))
  WHERE ((t."timestamp" IS NOT NULL) AND (m.sender IS NOT NULL))
  GROUP BY (date(t."timestamp"))
  ORDER BY (date(t."timestamp")) DESC;


--
-- Name: rt_daily_tx_stats; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.rt_daily_tx_stats (
    date date NOT NULL,
    total_txs bigint DEFAULT 0 NOT NULL,
    successful_txs bigint DEFAULT 0 NOT NULL,
    failed_txs bigint DEFAULT 0 NOT NULL,
    unique_senders bigint DEFAULT 0 NOT NULL
);


--
-- Name: daily_tx_stats; Type: VIEW; Schema: api; Owner: -
--

CREATE OR REPLACE VIEW api.daily_tx_stats AS
 SELECT date,
    total_txs,
    successful_txs,
    failed_txs,
    unique_senders
   FROM api.rt_daily_tx_stats
  ORDER BY date DESC;


--
-- Name: delegation_events; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.delegation_events (
    id integer NOT NULL,
    event_type text NOT NULL,
    delegator_address text,
    validator_address text NOT NULL,
    src_validator_address text,
    denom text,
    tx_hash text NOT NULL,
    height bigint,
    "timestamp" timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    amount numeric,
    CONSTRAINT delegation_events_event_type_check CHECK ((event_type = ANY (ARRAY['DELEGATE'::text, 'UNDELEGATE'::text, 'REDELEGATE'::text, 'CREATE_VALIDATOR'::text, 'EDIT_VALIDATOR'::text, 'UNJAIL'::text])))
);


--
-- Name: delegation_events_id_seq; Type: SEQUENCE; Schema: api; Owner: -
--

CREATE SEQUENCE IF NOT EXISTS api.delegation_events_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: delegation_events_id_seq; Type: SEQUENCE OWNED BY; Schema: api; Owner: -
--

ALTER SEQUENCE api.delegation_events_id_seq OWNED BY api.delegation_events.id;


--
-- Name: denom_metadata; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.denom_metadata (
    denom text NOT NULL,
    symbol text,
    name text,
    decimals integer DEFAULT 6,
    description text,
    logo_uri text,
    coingecko_id text,
    is_native boolean DEFAULT false,
    ibc_source_chain text,
    ibc_source_denom text,
    evm_contract text,
    updated_at timestamp with time zone DEFAULT now(),
    ibc_hash text
);


--
-- Name: events_main; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.events_main (
    id text NOT NULL,
    event_index integer NOT NULL,
    attr_index integer NOT NULL,
    event_type text NOT NULL,
    attr_key text,
    attr_value text,
    msg_index integer
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02');


--
-- Name: events_raw; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.events_raw (
    id text NOT NULL,
    event_index bigint NOT NULL,
    data jsonb NOT NULL
);


--
-- Name: evm_contracts; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.evm_contracts (
    address text NOT NULL,
    creator text,
    creation_tx text,
    creation_height bigint,
    bytecode_hash text,
    is_verified boolean DEFAULT false,
    name text,
    abi jsonb,
    source_code text,
    compiler_version text,
    metadata jsonb
);


--
-- Name: evm_logs; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.evm_logs (
    tx_id text NOT NULL,
    log_index integer NOT NULL,
    address text NOT NULL,
    topics text[] NOT NULL,
    data text
);


--
-- Name: evm_transactions; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.evm_transactions (
    tx_id text NOT NULL,
    hash text NOT NULL,
    "from" text NOT NULL,
    "to" text,
    nonce bigint NOT NULL,
    gas_limit bigint NOT NULL,
    gas_price numeric NOT NULL,
    max_fee_per_gas numeric,
    max_priority_fee_per_gas numeric,
    value numeric NOT NULL,
    data text,
    type smallint DEFAULT 0 NOT NULL,
    chain_id bigint,
    gas_used bigint,
    status smallint DEFAULT 1,
    function_name text,
    function_signature text,
    decoded_at timestamp with time zone DEFAULT now()
);


--
-- Name: evm_missing_contracts; Type: VIEW; Schema: api; Owner: -
--

CREATE OR REPLACE VIEW api.evm_missing_contracts AS
 SELECT e.tx_id,
    e."from" AS creator,
    e.nonce,
    e.data AS bytecode,
    t.height AS creation_height
   FROM (api.evm_transactions e
     JOIN api.transactions_main t ON ((e.tx_id = t.id)))
  WHERE ((e."to" IS NULL) AND (e.status = 1) AND (NOT (EXISTS ( SELECT 1
           FROM api.evm_contracts c
          WHERE (c.creation_tx = e.tx_id)))));


--
-- Name: messages_raw; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.messages_raw (
    id text NOT NULL,
    message_index integer NOT NULL,
    data jsonb
);


--
-- Name: evm_pending_decode; Type: VIEW; Schema: api; Owner: -
--

CREATE OR REPLACE VIEW api.evm_pending_decode AS
 SELECT t.id AS tx_id,
    t.height,
    t."timestamp",
    (m.data ->> 'raw'::text) AS raw_bytes,
    max(
        CASE
            WHEN (e.attr_key = 'ethereumTxHash'::text) THEN e.attr_value
            ELSE NULL::text
        END) AS ethereum_tx_hash,
    max(
        CASE
            WHEN (e.attr_key = 'txGasUsed'::text) THEN (e.attr_value)::bigint
            ELSE NULL::bigint
        END) AS gas_used
   FROM (((api.transactions_main t
     JOIN api.messages_main mm ON ((t.id = mm.id)))
     JOIN api.messages_raw m ON (((mm.id = m.id) AND (mm.message_index = m.message_index))))
     JOIN api.events_main e ON (((t.id = e.id) AND (e.event_type = 'ethereum_tx'::text))))
  WHERE ((mm.type ~~ '%MsgEthereumTx%'::text) AND (NOT (EXISTS ( SELECT 1
           FROM api.evm_transactions ev
          WHERE (ev.tx_id = t.id)))))
  GROUP BY t.id, t.height, t."timestamp", (m.data ->> 'raw'::text);


--
-- Name: evm_token_transfers; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.evm_token_transfers (
    tx_id text NOT NULL,
    log_index integer NOT NULL,
    token_address text NOT NULL,
    from_address text NOT NULL,
    to_address text NOT NULL,
    value numeric NOT NULL
);


--
-- Name: evm_tokens; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.evm_tokens (
    address text NOT NULL,
    name text,
    symbol text,
    decimals integer,
    type text NOT NULL,
    total_supply numeric,
    first_seen_tx text,
    first_seen_height bigint,
    verified boolean DEFAULT false,
    metadata jsonb
);


--
-- Name: evm_tokens_missing_metadata; Type: VIEW; Schema: api; Owner: -
--

CREATE OR REPLACE VIEW api.evm_tokens_missing_metadata AS
 SELECT address,
    type,
    first_seen_tx,
    first_seen_height
   FROM api.evm_tokens t
  WHERE ((name IS NULL) OR (symbol IS NULL) OR (decimals IS NULL));


--
-- Name: evm_tx_map; Type: VIEW; Schema: api; Owner: -
--

CREATE OR REPLACE VIEW api.evm_tx_map AS
 SELECT tx_id,
    hash AS ethereum_tx_hash,
    "from",
    "to",
    gas_used
   FROM api.evm_transactions;


--
-- Name: fee_revenue; Type: VIEW; Schema: api; Owner: -
--

CREATE OR REPLACE VIEW api.fee_revenue AS
 SELECT (fee_item.value ->> 'denom'::text) AS denom,
    sum(((fee_item.value ->> 'amount'::text))::numeric) AS total_amount
   FROM api.transactions_main,
    LATERAL jsonb_array_elements((transactions_main.fee -> 'amount'::text)) fee_item(value)
  GROUP BY (fee_item.value ->> 'denom'::text);


--
-- Name: finalize_block_events; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.finalize_block_events (
    id integer NOT NULL,
    height bigint NOT NULL,
    event_index integer NOT NULL,
    event_type text NOT NULL,
    attributes jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: finalize_block_events_id_seq; Type: SEQUENCE; Schema: api; Owner: -
--

CREATE SEQUENCE IF NOT EXISTS api.finalize_block_events_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: finalize_block_events_id_seq; Type: SEQUENCE OWNED BY; Schema: api; Owner: -
--

ALTER SEQUENCE api.finalize_block_events_id_seq OWNED BY api.finalize_block_events.id;


--
-- Name: gas_usage_distribution; Type: VIEW; Schema: api; Owner: -
--

CREATE OR REPLACE VIEW api.gas_usage_distribution AS
 SELECT
        CASE
            WHEN (((fee ->> 'gasLimit'::text))::bigint < 100000) THEN '0-100k'::text
            WHEN (((fee ->> 'gasLimit'::text))::bigint < 250000) THEN '100k-250k'::text
            WHEN (((fee ->> 'gasLimit'::text))::bigint < 500000) THEN '250k-500k'::text
            WHEN (((fee ->> 'gasLimit'::text))::bigint < 1000000) THEN '500k-1M'::text
            ELSE '1M+'::text
        END AS gas_range,
    count(*) AS count
   FROM api.transactions_main
  WHERE ((fee ->> 'gasLimit'::text) IS NOT NULL)
  GROUP BY
        CASE
            WHEN (((fee ->> 'gasLimit'::text))::bigint < 100000) THEN '0-100k'::text
            WHEN (((fee ->> 'gasLimit'::text))::bigint < 250000) THEN '100k-250k'::text
            WHEN (((fee ->> 'gasLimit'::text))::bigint < 500000) THEN '250k-500k'::text
            WHEN (((fee ->> 'gasLimit'::text))::bigint < 1000000) THEN '500k-1M'::text
            ELSE '1M+'::text
        END
  ORDER BY (min(((fee ->> 'gasLimit'::text))::bigint));


--
-- Name: governance_proposals; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.governance_proposals (
    proposal_id bigint NOT NULL,
    submit_tx_hash text NOT NULL,
    submit_height bigint NOT NULL,
    submit_time timestamp with time zone NOT NULL,
    proposer text,
    title text,
    summary text,
    metadata text,
    proposal_type text,
    status text DEFAULT 'PROPOSAL_STATUS_DEPOSIT_PERIOD'::text NOT NULL,
    deposit_end_time timestamp with time zone,
    voting_start_time timestamp with time zone,
    voting_end_time timestamp with time zone,
    yes_count text,
    no_count text,
    abstain_count text,
    no_with_veto_count text,
    last_updated timestamp with time zone DEFAULT now(),
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: governance_active_proposals; Type: VIEW; Schema: api; Owner: -
--

CREATE OR REPLACE VIEW api.governance_active_proposals AS
 SELECT proposal_id,
    status,
    voting_end_time,
    deposit_end_time
   FROM api.governance_proposals
  WHERE (status = ANY (ARRAY['PROPOSAL_STATUS_DEPOSIT_PERIOD'::text, 'PROPOSAL_STATUS_VOTING_PERIOD'::text]))
  ORDER BY proposal_id DESC;


--
-- Name: governance_snapshots; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.governance_snapshots (
    id integer NOT NULL,
    proposal_id bigint,
    status text NOT NULL,
    yes_count text NOT NULL,
    no_count text NOT NULL,
    abstain_count text NOT NULL,
    no_with_veto_count text NOT NULL,
    total_voting_power text,
    snapshot_time timestamp with time zone DEFAULT now()
);


--
-- Name: governance_snapshots_id_seq; Type: SEQUENCE; Schema: api; Owner: -
--

CREATE SEQUENCE IF NOT EXISTS api.governance_snapshots_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: governance_snapshots_id_seq; Type: SEQUENCE OWNED BY; Schema: api; Owner: -
--

ALTER SEQUENCE api.governance_snapshots_id_seq OWNED BY api.governance_snapshots.id;


--
-- Name: rt_hourly_tx_stats; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.rt_hourly_tx_stats (
    hour timestamp with time zone NOT NULL,
    tx_count bigint DEFAULT 0 NOT NULL
);


--
-- Name: hourly_tx_stats; Type: VIEW; Schema: api; Owner: -
--

CREATE OR REPLACE VIEW api.hourly_tx_stats AS
 SELECT hour,
    tx_count
   FROM api.rt_hourly_tx_stats
  ORDER BY hour DESC;


--
-- Name: ibc_channels; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.ibc_channels (
    channel_id text NOT NULL,
    port_id text NOT NULL,
    counterparty_channel_id text,
    counterparty_port_id text,
    connection_id text,
    state text,
    ordering text,
    version text,
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: ibc_connections; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.ibc_connections (
    channel_id text NOT NULL,
    port_id text NOT NULL,
    connection_id text,
    client_id text,
    counterparty_chain_id text,
    counterparty_channel_id text,
    counterparty_port_id text,
    counterparty_client_id text,
    counterparty_connection_id text,
    state text,
    ordering text,
    version text,
    client_status text,
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: ibc_denom_pending; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.ibc_denom_pending (
    ibc_denom text NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    last_attempt timestamp with time zone,
    attempts integer DEFAULT 0,
    error text
);


--
-- Name: ibc_denom_traces; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.ibc_denom_traces (
    ibc_denom text NOT NULL,
    base_denom text NOT NULL,
    path text NOT NULL,
    source_channel text,
    source_chain_id text,
    symbol text,
    decimals integer DEFAULT 6,
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: jailing_events; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.jailing_events (
    id integer NOT NULL,
    validator_address text NOT NULL,
    operator_address text,
    height bigint NOT NULL,
    detected_at timestamp with time zone DEFAULT now(),
    prev_block_flag text,
    current_block_flag text
);


--
-- Name: jailing_events_id_seq; Type: SEQUENCE; Schema: api; Owner: -
--

CREATE SEQUENCE IF NOT EXISTS api.jailing_events_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: jailing_events_id_seq; Type: SEQUENCE OWNED BY; Schema: api; Owner: -
--

ALTER SEQUENCE api.jailing_events_id_seq OWNED BY api.jailing_events.id;


--
-- Name: rt_message_type_stats; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.rt_message_type_stats (
    message_type text NOT NULL,
    count bigint DEFAULT 0 NOT NULL
);


--
-- Name: message_type_stats; Type: VIEW; Schema: api; Owner: -
--

CREATE OR REPLACE VIEW api.message_type_stats AS
 SELECT message_type,
    count,
    round((((count)::numeric / NULLIF(sum(count) OVER (), (0)::numeric)) * (100)::numeric), 2) AS percentage
   FROM api.rt_message_type_stats
  ORDER BY count DESC;


--
-- Name: validators; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.validators (
    operator_address text NOT NULL,
    consensus_address text,
    moniker text,
    identity text,
    website text,
    details text,
    commission_rate numeric,
    commission_max_rate numeric,
    commission_max_change_rate numeric,
    min_self_delegation numeric,
    tokens numeric,
    delegator_shares numeric,
    status text,
    jailed boolean DEFAULT false,
    creation_height bigint,
    first_seen_tx text,
    updated_at timestamp with time zone DEFAULT now(),
    signing_percentage numeric,
    blocks_signed integer DEFAULT 0,
    blocks_missed integer DEFAULT 0,
    last_signed_height bigint,
    missed_blocks_counter integer DEFAULT 0,
    last_jailed_height bigint,
    last_jailed_at timestamp with time zone
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', fillfactor='70');


--
-- Name: mv_chain_stats; Type: MATERIALIZED VIEW; Schema: api; Owner: -
--

CREATE MATERIALIZED VIEW IF NOT EXISTS api.mv_chain_stats AS
 SELECT ( SELECT max(blocks_raw.id) AS max
           FROM api.blocks_raw) AS latest_block,
    ( SELECT count(*) AS count
           FROM api.transactions_main) AS total_transactions,
    ( SELECT count(*) AS count
           FROM ( SELECT DISTINCT messages_main.sender AS addr
                   FROM api.messages_main
                  WHERE (messages_main.sender IS NOT NULL)
                UNION
                 SELECT DISTINCT evm_transactions."from" AS addr
                   FROM api.evm_transactions
                UNION
                 SELECT DISTINCT evm_transactions."to" AS addr
                   FROM api.evm_transactions
                  WHERE (evm_transactions."to" IS NOT NULL)
                UNION
                 SELECT DISTINCT unnest(messages_main.mentions) AS addr
                   FROM api.messages_main
                  WHERE (messages_main.mentions IS NOT NULL)) all_addresses) AS unique_addresses,
    ( SELECT count(*) AS count
           FROM api.evm_transactions) AS evm_transactions,
    ( SELECT (count(*))::integer AS count
           FROM api.validators
          WHERE ((validators.status = 'BOND_STATUS_BONDED'::text) AND (NOT validators.jailed))) AS active_validators
  WITH NO DATA;


--
-- Name: rt_daily_rewards; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.rt_daily_rewards (
    date date NOT NULL,
    total_rewards numeric DEFAULT 0 NOT NULL,
    total_commission numeric DEFAULT 0 NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', fillfactor='70');


--
-- Name: mv_daily_rewards; Type: VIEW; Schema: api; Owner: -
--

CREATE OR REPLACE VIEW api.mv_daily_rewards AS
 SELECT date,
    total_rewards,
    total_commission,
    (0)::bigint AS validators_earning
   FROM api.rt_daily_rewards;


--
-- Name: mv_daily_tx_stats; Type: MATERIALIZED VIEW; Schema: api; Owner: -
--

CREATE MATERIALIZED VIEW IF NOT EXISTS api.mv_daily_tx_stats AS
 WITH daily_txs AS (
         SELECT (date_trunc('day'::text, transactions_main."timestamp"))::date AS date,
            count(*) AS total_txs,
            count(*) FILTER (WHERE (transactions_main.error IS NULL)) AS successful_txs,
            count(*) FILTER (WHERE (transactions_main.error IS NOT NULL)) AS failed_txs
           FROM api.transactions_main
          GROUP BY ((date_trunc('day'::text, transactions_main."timestamp"))::date)
        ), daily_senders AS (
         SELECT (date_trunc('day'::text, t."timestamp"))::date AS date,
            count(DISTINCT m.sender) AS unique_senders
           FROM (api.transactions_main t
             JOIN api.messages_main m ON ((m.id = t.id)))
          GROUP BY ((date_trunc('day'::text, t."timestamp"))::date)
        )
 SELECT dt.date,
    dt.total_txs,
    dt.successful_txs,
    dt.failed_txs,
    COALESCE(ds.unique_senders, (0)::bigint) AS unique_senders
   FROM (daily_txs dt
     LEFT JOIN daily_senders ds ON ((ds.date = dt.date)))
  WITH NO DATA;


--
-- Name: mv_fee_revenue_daily; Type: MATERIALIZED VIEW; Schema: api; Owner: -
--

CREATE MATERIALIZED VIEW IF NOT EXISTS api.mv_fee_revenue_daily AS
 SELECT (date_trunc('day'::text, transactions_main."timestamp"))::date AS date,
    (f.value ->> 'denom'::text) AS fee_denom,
    sum((NULLIF((f.value ->> 'amount'::text), ''::text))::numeric) AS total_fees,
    count(*) AS tx_count
   FROM api.transactions_main,
    LATERAL jsonb_array_elements(
        CASE
            WHEN (jsonb_typeof(transactions_main.fee) = 'array'::text) THEN transactions_main.fee
            ELSE '[]'::jsonb
        END) f(value)
  WHERE ((transactions_main.fee IS NOT NULL) AND (jsonb_typeof(transactions_main.fee) = 'array'::text))
  GROUP BY ((date_trunc('day'::text, transactions_main."timestamp"))::date), (f.value ->> 'denom'::text)
  WITH NO DATA;


--
-- Name: validator_rewards; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.validator_rewards (
    id integer NOT NULL,
    height bigint NOT NULL,
    validator_address text NOT NULL,
    rewards numeric(78,18) DEFAULT 0,
    commission numeric(78,18) DEFAULT 0,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: mv_hourly_rewards; Type: MATERIALIZED VIEW; Schema: api; Owner: -
--

CREATE MATERIALIZED VIEW IF NOT EXISTS api.mv_hourly_rewards AS
 SELECT date_trunc('hour'::text, ((((b.data -> 'block'::text) -> 'header'::text) ->> 'time'::text))::timestamp with time zone) AS hour,
    COALESCE(sum(vr.rewards), (0)::numeric) AS rewards,
    COALESCE(sum(vr.commission), (0)::numeric) AS commission
   FROM (api.validator_rewards vr
     JOIN api.blocks_raw b ON ((b.id = vr.height)))
  WHERE (((((b.data -> 'block'::text) -> 'header'::text) ->> 'time'::text))::timestamp with time zone > (now() - '48:00:00'::interval))
  GROUP BY (date_trunc('hour'::text, ((((b.data -> 'block'::text) -> 'header'::text) ->> 'time'::text))::timestamp with time zone))
  WITH NO DATA;


--
-- Name: mv_hourly_tx_stats; Type: MATERIALIZED VIEW; Schema: api; Owner: -
--

CREATE MATERIALIZED VIEW IF NOT EXISTS api.mv_hourly_tx_stats AS
 SELECT date_trunc('hour'::text, "timestamp") AS hour,
    count(*) AS tx_count
   FROM api.transactions_main
  WHERE ("timestamp" >= (now() - '7 days'::interval))
  GROUP BY (date_trunc('hour'::text, "timestamp"))
  WITH NO DATA;


--
-- Name: mv_message_type_stats; Type: MATERIALIZED VIEW; Schema: api; Owner: -
--

CREATE MATERIALIZED VIEW IF NOT EXISTS api.mv_message_type_stats AS
 WITH totals AS (
         SELECT (count(*))::numeric AS total
           FROM api.messages_main
        ), type_counts AS (
         SELECT messages_main.type AS message_type,
            count(*) AS count
           FROM api.messages_main
          GROUP BY messages_main.type
        )
 SELECT tc.message_type,
    tc.count,
    round((((tc.count)::numeric / t.total) * (100)::numeric), 2) AS percentage
   FROM (type_counts tc
     CROSS JOIN totals t)
  WITH NO DATA;


--
-- Name: mv_network_overview; Type: MATERIALIZED VIEW; Schema: api; Owner: -
--

CREATE MATERIALIZED VIEW IF NOT EXISTS api.mv_network_overview AS
 SELECT ( SELECT (count(*))::integer AS count
           FROM api.validators) AS total_validators,
    ( SELECT (count(*))::integer AS count
           FROM api.validators
          WHERE ((validators.status = 'BOND_STATUS_BONDED'::text) AND (NOT validators.jailed))) AS active_validators,
    ( SELECT (count(*))::integer AS count
           FROM api.validators
          WHERE (validators.jailed = true)) AS jailed_validators,
    ( SELECT COALESCE(sum(validators.tokens), (0)::numeric) AS "coalesce"
           FROM api.validators
          WHERE (validators.status = 'BOND_STATUS_BONDED'::text)) AS total_bonded_tokens,
    ( SELECT COALESCE(sum(vr.rewards), (0)::numeric) AS "coalesce"
           FROM (api.validator_rewards vr
             JOIN api.blocks_raw b ON ((b.id = vr.height)))
          WHERE (((((b.data -> 'block'::text) -> 'header'::text) ->> 'time'::text))::timestamp with time zone > (now() - '24:00:00'::interval))) AS total_rewards_24h,
    ( SELECT COALESCE(sum(vr.commission), (0)::numeric) AS "coalesce"
           FROM (api.validator_rewards vr
             JOIN api.blocks_raw b ON ((b.id = vr.height)))
          WHERE (((((b.data -> 'block'::text) -> 'header'::text) ->> 'time'::text))::timestamp with time zone > (now() - '24:00:00'::interval))) AS total_commission_24h,
    ( SELECT COALESCE(avg(EXTRACT(epoch FROM (((((b1.data -> 'block'::text) -> 'header'::text) ->> 'time'::text))::timestamp with time zone - ((((b2.data -> 'block'::text) -> 'header'::text) ->> 'time'::text))::timestamp with time zone))), (6)::numeric) AS "coalesce"
           FROM (api.blocks_raw b1
             JOIN api.blocks_raw b2 ON ((b2.id = (b1.id - 1))))
          WHERE (b1.id > ( SELECT (max(blocks_raw.id) - 100)
                   FROM api.blocks_raw))) AS avg_block_time,
    ( SELECT count(*) AS count
           FROM api.transactions_main) AS total_transactions,
    ( SELECT count(DISTINCT messages_main.sender) AS count
           FROM api.messages_main
          WHERE (messages_main.sender IS NOT NULL)) AS unique_addresses,
    ( SELECT (count(*))::integer AS count
           FROM api.validators
          WHERE (validators.status = 'BOND_STATUS_BONDED'::text)) AS max_validators
  WITH NO DATA;


--
-- Name: mv_validator_delegator_counts; Type: MATERIALIZED VIEW; Schema: api; Owner: -
--

CREATE MATERIALIZED VIEW IF NOT EXISTS api.mv_validator_delegator_counts AS
 SELECT validator_address,
    count(DISTINCT delegator_address) AS delegator_count,
    max("timestamp") AS last_delegation_at
   FROM api.delegation_events
  WHERE (event_type = ANY (ARRAY['DELEGATE'::text, 'CREATE_VALIDATOR'::text]))
  GROUP BY validator_address
  WITH NO DATA;


--
-- Name: validator_consensus_addresses; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.validator_consensus_addresses (
    consensus_address text NOT NULL,
    operator_address text,
    first_seen_height bigint,
    created_at timestamp with time zone DEFAULT now(),
    hex_address text
);


--
-- Name: mv_validator_leaderboard; Type: MATERIALIZED VIEW; Schema: api; Owner: -
--

CREATE MATERIALIZED VIEW IF NOT EXISTS api.mv_validator_leaderboard AS
 SELECT v.operator_address,
    v.moniker,
    v.tokens,
    v.commission_rate,
    v.jailed,
    COALESCE(d.delegator_count, (0)::bigint) AS delegator_count,
    COALESCE(r.total_rewards, (0)::numeric) AS lifetime_rewards,
    COALESCE(r.total_commission, (0)::numeric) AS lifetime_commission,
    COALESCE(j.jail_count, (0)::bigint) AS jail_count,
    COALESCE(j.last_jailed_height, (0)::bigint) AS last_jailed_height
   FROM (((api.validators v
     LEFT JOIN api.mv_validator_delegator_counts d ON ((d.validator_address = v.operator_address)))
     LEFT JOIN ( SELECT vca.operator_address,
            sum(vr.rewards) AS total_rewards,
            sum(vr.commission) AS total_commission
           FROM (api.validator_rewards vr
             JOIN api.validator_consensus_addresses vca ON ((vca.consensus_address = vr.validator_address)))
          GROUP BY vca.operator_address) r ON ((r.operator_address = v.operator_address)))
     LEFT JOIN ( SELECT j_1.operator_address,
            count(*) AS jail_count,
            max(j_1.height) AS last_jailed_height
           FROM api.jailing_events j_1
          GROUP BY j_1.operator_address) j ON ((j.operator_address = v.operator_address)))
  WHERE (v.status = 'BOND_STATUS_BONDED'::text)
  WITH NO DATA;


--
-- Name: validator_block_signatures; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.validator_block_signatures (
    id integer NOT NULL,
    height bigint NOT NULL,
    validator_index integer NOT NULL,
    consensus_address text NOT NULL,
    signed boolean NOT NULL,
    block_id_flag text NOT NULL,
    block_time timestamp with time zone,
    created_at timestamp with time zone DEFAULT now()
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02');


--
-- Name: mv_validator_signing_stats; Type: MATERIALIZED VIEW; Schema: api; Owner: -
--

CREATE MATERIALIZED VIEW IF NOT EXISTS api.mv_validator_signing_stats AS
 SELECT vbs.consensus_address,
    vca.operator_address,
    count(*) AS total_blocks,
    count(*) FILTER (WHERE vbs.signed) AS blocks_signed,
    count(*) FILTER (WHERE (NOT vbs.signed)) AS blocks_missed,
        CASE
            WHEN (count(*) > 0) THEN round((((count(*) FILTER (WHERE vbs.signed))::numeric / (count(*))::numeric) * (100)::numeric), 2)
            ELSE (100)::numeric
        END AS signing_percentage,
    max(vbs.height) AS last_height
   FROM (api.validator_block_signatures vbs
     LEFT JOIN api.validator_consensus_addresses vca ON ((vca.consensus_address = vbs.consensus_address)))
  WHERE (vbs.height > ( SELECT (max(blocks_raw.id) - 10000)
           FROM api.blocks_raw))
  GROUP BY vbs.consensus_address, vca.operator_address
  WITH NO DATA;


--
-- Name: proposal_votes; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.proposal_votes (
    proposal_id bigint NOT NULL,
    voter text NOT NULL,
    option text NOT NULL,
    weight numeric DEFAULT 1,
    tx_id text,
    "timestamp" timestamp with time zone
);


--
-- Name: proposals; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.proposals (
    id bigint NOT NULL,
    title text,
    summary text,
    proposer text,
    status text,
    submit_time timestamp with time zone,
    deposit_end_time timestamp with time zone,
    voting_start_time timestamp with time zone,
    voting_end_time timestamp with time zone,
    total_deposit jsonb,
    final_tally jsonb,
    metadata jsonb,
    creation_tx text,
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: query_stats; Type: VIEW; Schema: api; Owner: -
--

CREATE OR REPLACE VIEW api.query_stats AS
 SELECT "left"(query, 100) AS query,
    calls,
    total_exec_time,
    mean_exec_time,
    rows
   FROM public.pg_stat_statements
  WHERE (query ~~ '%api.%'::text)
  ORDER BY mean_exec_time DESC;


--
-- Name: rt_hourly_rewards; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.rt_hourly_rewards (
    hour timestamp with time zone NOT NULL,
    rewards numeric DEFAULT 0 NOT NULL,
    commission numeric DEFAULT 0 NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', fillfactor='70');


--
-- Name: slashing_records; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.slashing_records (
    id integer NOT NULL,
    slashing_id bigint,
    validator_address text NOT NULL,
    submitter text NOT NULL,
    condition text NOT NULL,
    evidence_type text,
    evidence_data jsonb,
    tx_hash text NOT NULL,
    height bigint,
    "timestamp" timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT slashing_records_condition_check CHECK ((condition = ANY (ARRAY['COMPUTE_MISCONDUCT'::text, 'REPUTATION_DEGRADATION'::text, 'DELEGATED_COLLUSION'::text])))
);


--
-- Name: slashing_records_id_seq; Type: SEQUENCE; Schema: api; Owner: -
--

CREATE SEQUENCE IF NOT EXISTS api.slashing_records_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: slashing_records_id_seq; Type: SEQUENCE OWNED BY; Schema: api; Owner: -
--

ALTER SEQUENCE api.slashing_records_id_seq OWNED BY api.slashing_records.id;


--
-- Name: transactions_raw; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.transactions_raw (
    id text NOT NULL,
    data jsonb NOT NULL
);


--
-- Name: tx_success_rate; Type: VIEW; Schema: api; Owner: -
--

CREATE OR REPLACE VIEW api.tx_success_rate AS
 SELECT count(*) AS total,
    count(*) FILTER (WHERE (error IS NULL)) AS successful,
    count(*) FILTER (WHERE (error IS NOT NULL)) AS failed,
    round(((100.0 * (count(*) FILTER (WHERE (error IS NULL)))::numeric) / (NULLIF(count(*), 0))::numeric), 2) AS success_rate_percent
   FROM api.transactions_main;


--
-- Name: tx_volume_daily; Type: VIEW; Schema: api; Owner: -
--

CREATE OR REPLACE VIEW api.tx_volume_daily AS
 SELECT date("timestamp") AS date,
    count(*) AS count
   FROM api.transactions_main
  WHERE ("timestamp" IS NOT NULL)
  GROUP BY (date("timestamp"))
  ORDER BY (date("timestamp")) DESC;


--
-- Name: tx_volume_hourly; Type: VIEW; Schema: api; Owner: -
--

CREATE OR REPLACE VIEW api.tx_volume_hourly AS
 SELECT date_trunc('hour'::text, "timestamp") AS hour,
    count(*) AS count
   FROM api.transactions_main
  WHERE ("timestamp" IS NOT NULL)
  GROUP BY (date_trunc('hour'::text, "timestamp"))
  ORDER BY (date_trunc('hour'::text, "timestamp")) DESC;


--
-- Name: validator_block_signatures_id_seq; Type: SEQUENCE; Schema: api; Owner: -
--

CREATE SEQUENCE IF NOT EXISTS api.validator_block_signatures_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: validator_block_signatures_id_seq; Type: SEQUENCE OWNED BY; Schema: api; Owner: -
--

ALTER SEQUENCE api.validator_block_signatures_id_seq OWNED BY api.validator_block_signatures.id;


--
-- Name: validator_ipfs_addresses; Type: TABLE; Schema: api; Owner: -
--

CREATE TABLE IF NOT EXISTS api.validator_ipfs_addresses (
    validator_address text NOT NULL,
    ipfs_multiaddrs text[],
    ipfs_peer_id text,
    tx_hash text NOT NULL,
    height bigint,
    "timestamp" timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: validator_rewards_id_seq; Type: SEQUENCE; Schema: api; Owner: -
--

CREATE SEQUENCE IF NOT EXISTS api.validator_rewards_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: validator_rewards_id_seq; Type: SEQUENCE OWNED BY; Schema: api; Owner: -
--

ALTER SEQUENCE api.validator_rewards_id_seq OWNED BY api.validator_rewards.id;


--
-- Name: validator_stats; Type: VIEW; Schema: api; Owner: -
--

CREATE OR REPLACE VIEW api.validator_stats AS
 SELECT ( SELECT count(*) AS count
           FROM api.validators) AS total_validators,
    ( SELECT count(*) AS count
           FROM api.validators
          WHERE ((validators.status = 'BOND_STATUS_BONDED'::text) AND (NOT validators.jailed))) AS active_validators,
    ( SELECT count(*) AS count
           FROM api.validators
          WHERE ((validators.status = ANY (ARRAY['BOND_STATUS_UNBONDED'::text, 'BOND_STATUS_UNBONDING'::text])) AND (NOT validators.jailed))) AS inactive_validators,
    ( SELECT count(*) AS count
           FROM api.validators
          WHERE (validators.jailed = true)) AS jailed_validators,
    ( SELECT COALESCE(sum(validators.tokens), (0)::numeric) AS "coalesce"
           FROM api.validators
          WHERE (validators.status = 'BOND_STATUS_BONDED'::text)) AS total_bonded_tokens;


--
-- Name: validators_with_consensus; Type: VIEW; Schema: api; Owner: -
--

CREATE OR REPLACE VIEW api.validators_with_consensus AS
 SELECT v.operator_address,
    v.consensus_address,
    v.moniker,
    v.identity,
    v.website,
    v.details,
    v.commission_rate,
    v.commission_max_rate,
    v.commission_max_change_rate,
    v.min_self_delegation,
    v.tokens,
    v.delegator_shares,
    v.status,
    v.jailed,
    v.creation_height,
    v.first_seen_tx,
    v.updated_at,
    v.signing_percentage,
    v.blocks_signed,
    v.blocks_missed,
    v.last_signed_height,
    v.missed_blocks_counter,
    v.last_jailed_height,
    v.last_jailed_at,
    COALESCE(v.consensus_address, vca.consensus_address) AS resolved_consensus_address
   FROM (api.validators v
     LEFT JOIN api.validator_consensus_addresses vca ON ((vca.operator_address = v.operator_address)));


--
-- Name: compute_committee_proposals id; Type: DEFAULT; Schema: api; Owner: -
--

ALTER TABLE ONLY api.compute_committee_proposals ALTER COLUMN id SET DEFAULT nextval('api.compute_committee_proposals_id_seq'::regclass);


--
-- Name: compute_seed_contributions id; Type: DEFAULT; Schema: api; Owner: -
--

ALTER TABLE ONLY api.compute_seed_contributions ALTER COLUMN id SET DEFAULT nextval('api.compute_seed_contributions_id_seq'::regclass);


--
-- Name: delegation_events id; Type: DEFAULT; Schema: api; Owner: -
--

ALTER TABLE ONLY api.delegation_events ALTER COLUMN id SET DEFAULT nextval('api.delegation_events_id_seq'::regclass);


--
-- Name: finalize_block_events id; Type: DEFAULT; Schema: api; Owner: -
--

ALTER TABLE ONLY api.finalize_block_events ALTER COLUMN id SET DEFAULT nextval('api.finalize_block_events_id_seq'::regclass);


--
-- Name: governance_snapshots id; Type: DEFAULT; Schema: api; Owner: -
--

ALTER TABLE ONLY api.governance_snapshots ALTER COLUMN id SET DEFAULT nextval('api.governance_snapshots_id_seq'::regclass);


--
-- Name: jailing_events id; Type: DEFAULT; Schema: api; Owner: -
--

ALTER TABLE ONLY api.jailing_events ALTER COLUMN id SET DEFAULT nextval('api.jailing_events_id_seq'::regclass);


--
-- Name: slashing_records id; Type: DEFAULT; Schema: api; Owner: -
--

ALTER TABLE ONLY api.slashing_records ALTER COLUMN id SET DEFAULT nextval('api.slashing_records_id_seq'::regclass);


--
-- Name: validator_block_signatures id; Type: DEFAULT; Schema: api; Owner: -
--

ALTER TABLE ONLY api.validator_block_signatures ALTER COLUMN id SET DEFAULT nextval('api.validator_block_signatures_id_seq'::regclass);


--
-- Name: validator_rewards id; Type: DEFAULT; Schema: api; Owner: -
--

ALTER TABLE ONLY api.validator_rewards ALTER COLUMN id SET DEFAULT nextval('api.validator_rewards_id_seq'::regclass);


--
-- Name: block_metrics block_metrics_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'block_metrics_pkey' AND conrelid = 'api.block_metrics'::regclass) THEN
    ALTER TABLE ONLY api.block_metrics
        ADD CONSTRAINT block_metrics_pkey PRIMARY KEY (height);
  END IF;
END $$;


--
-- Name: block_results_raw block_results_raw_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'block_results_raw_pkey' AND conrelid = 'api.block_results_raw'::regclass) THEN
    ALTER TABLE ONLY api.block_results_raw
        ADD CONSTRAINT block_results_raw_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: blocks_raw blocks_raw_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'blocks_raw_pkey' AND conrelid = 'api.blocks_raw'::regclass) THEN
    ALTER TABLE ONLY api.blocks_raw
        ADD CONSTRAINT blocks_raw_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: chain_features chain_features_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chain_features_pkey' AND conrelid = 'api.chain_features'::regclass) THEN
    ALTER TABLE ONLY api.chain_features
        ADD CONSTRAINT chain_features_pkey PRIMARY KEY (chain_id);
  END IF;
END $$;


--
-- Name: chain_params chain_params_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chain_params_pkey' AND conrelid = 'api.chain_params'::regclass) THEN
    ALTER TABLE ONLY api.chain_params
        ADD CONSTRAINT chain_params_pkey PRIMARY KEY (key);
  END IF;
END $$;


--
-- Name: compute_benchmarks compute_benchmarks_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'compute_benchmarks_pkey' AND conrelid = 'api.compute_benchmarks'::regclass) THEN
    ALTER TABLE ONLY api.compute_benchmarks
        ADD CONSTRAINT compute_benchmarks_pkey PRIMARY KEY (benchmark_id);
  END IF;
END $$;


--
-- Name: compute_committee_proposals compute_committee_proposals_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'compute_committee_proposals_pkey' AND conrelid = 'api.compute_committee_proposals'::regclass) THEN
    ALTER TABLE ONLY api.compute_committee_proposals
        ADD CONSTRAINT compute_committee_proposals_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: compute_jobs compute_jobs_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'compute_jobs_pkey' AND conrelid = 'api.compute_jobs'::regclass) THEN
    ALTER TABLE ONLY api.compute_jobs
        ADD CONSTRAINT compute_jobs_pkey PRIMARY KEY (job_id);
  END IF;
END $$;


--
-- Name: compute_seed_contributions compute_seed_contributions_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'compute_seed_contributions_pkey' AND conrelid = 'api.compute_seed_contributions'::regclass) THEN
    ALTER TABLE ONLY api.compute_seed_contributions
        ADD CONSTRAINT compute_seed_contributions_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: compute_seed_contributions compute_seed_contributions_validator_benchmark_id_key; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'compute_seed_contributions_validator_benchmark_id_key' AND conrelid = 'api.compute_seed_contributions'::regclass) THEN
    ALTER TABLE ONLY api.compute_seed_contributions
        ADD CONSTRAINT compute_seed_contributions_validator_benchmark_id_key UNIQUE (validator, benchmark_id);
  END IF;
END $$;


--
-- Name: delegation_events delegation_events_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'delegation_events_pkey' AND conrelid = 'api.delegation_events'::regclass) THEN
    ALTER TABLE ONLY api.delegation_events
        ADD CONSTRAINT delegation_events_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: denom_metadata denom_metadata_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'denom_metadata_pkey' AND conrelid = 'api.denom_metadata'::regclass) THEN
    ALTER TABLE ONLY api.denom_metadata
        ADD CONSTRAINT denom_metadata_pkey PRIMARY KEY (denom);
  END IF;
END $$;


--
-- Name: events_main events_main_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'events_main_pkey' AND conrelid = 'api.events_main'::regclass) THEN
    ALTER TABLE ONLY api.events_main
        ADD CONSTRAINT events_main_pkey PRIMARY KEY (id, event_index, attr_index);
  END IF;
END $$;


--
-- Name: events_raw events_raw_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'events_raw_pkey' AND conrelid = 'api.events_raw'::regclass) THEN
    ALTER TABLE ONLY api.events_raw
        ADD CONSTRAINT events_raw_pkey PRIMARY KEY (id, event_index);
  END IF;
END $$;


--
-- Name: evm_contracts evm_contracts_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'evm_contracts_pkey' AND conrelid = 'api.evm_contracts'::regclass) THEN
    ALTER TABLE ONLY api.evm_contracts
        ADD CONSTRAINT evm_contracts_pkey PRIMARY KEY (address);
  END IF;
END $$;


--
-- Name: evm_logs evm_logs_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'evm_logs_pkey' AND conrelid = 'api.evm_logs'::regclass) THEN
    ALTER TABLE ONLY api.evm_logs
        ADD CONSTRAINT evm_logs_pkey PRIMARY KEY (tx_id, log_index);
  END IF;
END $$;


--
-- Name: evm_token_transfers evm_token_transfers_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'evm_token_transfers_pkey' AND conrelid = 'api.evm_token_transfers'::regclass) THEN
    ALTER TABLE ONLY api.evm_token_transfers
        ADD CONSTRAINT evm_token_transfers_pkey PRIMARY KEY (tx_id, log_index);
  END IF;
END $$;


--
-- Name: evm_tokens evm_tokens_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'evm_tokens_pkey' AND conrelid = 'api.evm_tokens'::regclass) THEN
    ALTER TABLE ONLY api.evm_tokens
        ADD CONSTRAINT evm_tokens_pkey PRIMARY KEY (address);
  END IF;
END $$;


--
-- Name: evm_transactions evm_transactions_hash_key; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'evm_transactions_hash_key' AND conrelid = 'api.evm_transactions'::regclass) THEN
    ALTER TABLE ONLY api.evm_transactions
        ADD CONSTRAINT evm_transactions_hash_key UNIQUE (hash);
  END IF;
END $$;


--
-- Name: evm_transactions evm_transactions_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'evm_transactions_pkey' AND conrelid = 'api.evm_transactions'::regclass) THEN
    ALTER TABLE ONLY api.evm_transactions
        ADD CONSTRAINT evm_transactions_pkey PRIMARY KEY (tx_id);
  END IF;
END $$;


--
-- Name: finalize_block_events finalize_block_events_height_event_index_key; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'finalize_block_events_height_event_index_key' AND conrelid = 'api.finalize_block_events'::regclass) THEN
    ALTER TABLE ONLY api.finalize_block_events
        ADD CONSTRAINT finalize_block_events_height_event_index_key UNIQUE (height, event_index);
  END IF;
END $$;


--
-- Name: finalize_block_events finalize_block_events_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'finalize_block_events_pkey' AND conrelid = 'api.finalize_block_events'::regclass) THEN
    ALTER TABLE ONLY api.finalize_block_events
        ADD CONSTRAINT finalize_block_events_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: governance_proposals governance_proposals_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'governance_proposals_pkey' AND conrelid = 'api.governance_proposals'::regclass) THEN
    ALTER TABLE ONLY api.governance_proposals
        ADD CONSTRAINT governance_proposals_pkey PRIMARY KEY (proposal_id);
  END IF;
END $$;


--
-- Name: governance_snapshots governance_snapshots_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'governance_snapshots_pkey' AND conrelid = 'api.governance_snapshots'::regclass) THEN
    ALTER TABLE ONLY api.governance_snapshots
        ADD CONSTRAINT governance_snapshots_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: governance_snapshots governance_snapshots_proposal_id_snapshot_time_key; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'governance_snapshots_proposal_id_snapshot_time_key' AND conrelid = 'api.governance_snapshots'::regclass) THEN
    ALTER TABLE ONLY api.governance_snapshots
        ADD CONSTRAINT governance_snapshots_proposal_id_snapshot_time_key UNIQUE (proposal_id, snapshot_time);
  END IF;
END $$;


--
-- Name: ibc_channels ibc_channels_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ibc_channels_pkey' AND conrelid = 'api.ibc_channels'::regclass) THEN
    ALTER TABLE ONLY api.ibc_channels
        ADD CONSTRAINT ibc_channels_pkey PRIMARY KEY (channel_id, port_id);
  END IF;
END $$;


--
-- Name: ibc_connections ibc_connections_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ibc_connections_pkey' AND conrelid = 'api.ibc_connections'::regclass) THEN
    ALTER TABLE ONLY api.ibc_connections
        ADD CONSTRAINT ibc_connections_pkey PRIMARY KEY (channel_id, port_id);
  END IF;
END $$;


--
-- Name: ibc_denom_pending ibc_denom_pending_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ibc_denom_pending_pkey' AND conrelid = 'api.ibc_denom_pending'::regclass) THEN
    ALTER TABLE ONLY api.ibc_denom_pending
        ADD CONSTRAINT ibc_denom_pending_pkey PRIMARY KEY (ibc_denom);
  END IF;
END $$;


--
-- Name: ibc_denom_traces ibc_denom_traces_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ibc_denom_traces_pkey' AND conrelid = 'api.ibc_denom_traces'::regclass) THEN
    ALTER TABLE ONLY api.ibc_denom_traces
        ADD CONSTRAINT ibc_denom_traces_pkey PRIMARY KEY (ibc_denom);
  END IF;
END $$;


--
-- Name: jailing_events jailing_events_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'jailing_events_pkey' AND conrelid = 'api.jailing_events'::regclass) THEN
    ALTER TABLE ONLY api.jailing_events
        ADD CONSTRAINT jailing_events_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: jailing_events jailing_events_validator_address_height_key; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'jailing_events_validator_address_height_key' AND conrelid = 'api.jailing_events'::regclass) THEN
    ALTER TABLE ONLY api.jailing_events
        ADD CONSTRAINT jailing_events_validator_address_height_key UNIQUE (validator_address, height);
  END IF;
END $$;


--
-- Name: messages_main messages_main_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'messages_main_pkey' AND conrelid = 'api.messages_main'::regclass) THEN
    ALTER TABLE ONLY api.messages_main
        ADD CONSTRAINT messages_main_pkey PRIMARY KEY (id, message_index);
  END IF;
END $$;


--
-- Name: messages_raw messages_raw_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'messages_raw_pkey' AND conrelid = 'api.messages_raw'::regclass) THEN
    ALTER TABLE ONLY api.messages_raw
        ADD CONSTRAINT messages_raw_pkey PRIMARY KEY (id, message_index);
  END IF;
END $$;


--
-- Name: proposal_votes proposal_votes_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'proposal_votes_pkey' AND conrelid = 'api.proposal_votes'::regclass) THEN
    ALTER TABLE ONLY api.proposal_votes
        ADD CONSTRAINT proposal_votes_pkey PRIMARY KEY (proposal_id, voter);
  END IF;
END $$;


--
-- Name: proposals proposals_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'proposals_pkey' AND conrelid = 'api.proposals'::regclass) THEN
    ALTER TABLE ONLY api.proposals
        ADD CONSTRAINT proposals_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: rt_chain_stats rt_chain_stats_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'rt_chain_stats_pkey' AND conrelid = 'api.rt_chain_stats'::regclass) THEN
    ALTER TABLE ONLY api.rt_chain_stats
        ADD CONSTRAINT rt_chain_stats_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: rt_daily_rewards rt_daily_rewards_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'rt_daily_rewards_pkey' AND conrelid = 'api.rt_daily_rewards'::regclass) THEN
    ALTER TABLE ONLY api.rt_daily_rewards
        ADD CONSTRAINT rt_daily_rewards_pkey PRIMARY KEY (date);
  END IF;
END $$;


--
-- Name: rt_daily_tx_stats rt_daily_tx_stats_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'rt_daily_tx_stats_pkey' AND conrelid = 'api.rt_daily_tx_stats'::regclass) THEN
    ALTER TABLE ONLY api.rt_daily_tx_stats
        ADD CONSTRAINT rt_daily_tx_stats_pkey PRIMARY KEY (date);
  END IF;
END $$;


--
-- Name: rt_hourly_rewards rt_hourly_rewards_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'rt_hourly_rewards_pkey' AND conrelid = 'api.rt_hourly_rewards'::regclass) THEN
    ALTER TABLE ONLY api.rt_hourly_rewards
        ADD CONSTRAINT rt_hourly_rewards_pkey PRIMARY KEY (hour);
  END IF;
END $$;


--
-- Name: rt_hourly_tx_stats rt_hourly_tx_stats_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'rt_hourly_tx_stats_pkey' AND conrelid = 'api.rt_hourly_tx_stats'::regclass) THEN
    ALTER TABLE ONLY api.rt_hourly_tx_stats
        ADD CONSTRAINT rt_hourly_tx_stats_pkey PRIMARY KEY (hour);
  END IF;
END $$;


--
-- Name: rt_message_type_stats rt_message_type_stats_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'rt_message_type_stats_pkey' AND conrelid = 'api.rt_message_type_stats'::regclass) THEN
    ALTER TABLE ONLY api.rt_message_type_stats
        ADD CONSTRAINT rt_message_type_stats_pkey PRIMARY KEY (message_type);
  END IF;
END $$;


--
-- Name: slashing_records slashing_records_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'slashing_records_pkey' AND conrelid = 'api.slashing_records'::regclass) THEN
    ALTER TABLE ONLY api.slashing_records
        ADD CONSTRAINT slashing_records_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: slashing_records slashing_records_slashing_id_key; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'slashing_records_slashing_id_key' AND conrelid = 'api.slashing_records'::regclass) THEN
    ALTER TABLE ONLY api.slashing_records
        ADD CONSTRAINT slashing_records_slashing_id_key UNIQUE (slashing_id);
  END IF;
END $$;


--
-- Name: transactions_main transactions_main_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'transactions_main_pkey' AND conrelid = 'api.transactions_main'::regclass) THEN
    ALTER TABLE ONLY api.transactions_main
        ADD CONSTRAINT transactions_main_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: transactions_raw transactions_raw_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'transactions_raw_pkey' AND conrelid = 'api.transactions_raw'::regclass) THEN
    ALTER TABLE ONLY api.transactions_raw
        ADD CONSTRAINT transactions_raw_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: validator_block_signatures validator_block_signatures_height_validator_index_key; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'validator_block_signatures_height_validator_index_key' AND conrelid = 'api.validator_block_signatures'::regclass) THEN
    ALTER TABLE ONLY api.validator_block_signatures
        ADD CONSTRAINT validator_block_signatures_height_validator_index_key UNIQUE (height, validator_index);
  END IF;
END $$;


--
-- Name: validator_block_signatures validator_block_signatures_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'validator_block_signatures_pkey' AND conrelid = 'api.validator_block_signatures'::regclass) THEN
    ALTER TABLE ONLY api.validator_block_signatures
        ADD CONSTRAINT validator_block_signatures_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: validator_consensus_addresses validator_consensus_addresses_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'validator_consensus_addresses_pkey' AND conrelid = 'api.validator_consensus_addresses'::regclass) THEN
    ALTER TABLE ONLY api.validator_consensus_addresses
        ADD CONSTRAINT validator_consensus_addresses_pkey PRIMARY KEY (consensus_address);
  END IF;
END $$;


--
-- Name: validator_ipfs_addresses validator_ipfs_addresses_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'validator_ipfs_addresses_pkey' AND conrelid = 'api.validator_ipfs_addresses'::regclass) THEN
    ALTER TABLE ONLY api.validator_ipfs_addresses
        ADD CONSTRAINT validator_ipfs_addresses_pkey PRIMARY KEY (validator_address);
  END IF;
END $$;


--
-- Name: validator_rewards validator_rewards_height_validator_address_key; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'validator_rewards_height_validator_address_key' AND conrelid = 'api.validator_rewards'::regclass) THEN
    ALTER TABLE ONLY api.validator_rewards
        ADD CONSTRAINT validator_rewards_height_validator_address_key UNIQUE (height, validator_address);
  END IF;
END $$;


--
-- Name: validator_rewards validator_rewards_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'validator_rewards_pkey' AND conrelid = 'api.validator_rewards'::regclass) THEN
    ALTER TABLE ONLY api.validator_rewards
        ADD CONSTRAINT validator_rewards_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: validators validators_pkey; Type: CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'validators_pkey' AND conrelid = 'api.validators'::regclass) THEN
    ALTER TABLE ONLY api.validators
        ADD CONSTRAINT validators_pkey PRIMARY KEY (operator_address);
  END IF;
END $$;


--
-- Name: idx_benchmarks_creator; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_benchmarks_creator ON api.compute_benchmarks USING btree (creator);


--
-- Name: idx_benchmarks_status; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_benchmarks_status ON api.compute_benchmarks USING btree (status);


--
-- Name: idx_benchmarks_submit_time; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_benchmarks_submit_time ON api.compute_benchmarks USING btree (submit_time DESC);


--
-- Name: idx_block_metrics_time; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_block_metrics_time ON api.block_metrics USING btree (block_time DESC);


--
-- Name: idx_blocks_raw_block_time; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_blocks_raw_block_time ON api.blocks_raw USING btree (block_time DESC);


--
-- Name: idx_blocks_tx_count; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_blocks_tx_count ON api.blocks_raw USING btree (tx_count) WHERE (tx_count > 0);


--
-- Name: idx_committee_proposals_height; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_committee_proposals_height ON api.compute_committee_proposals USING btree (target_height);


--
-- Name: idx_compute_jobs_creator; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_compute_jobs_creator ON api.compute_jobs USING btree (creator);


--
-- Name: idx_compute_jobs_status; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_compute_jobs_status ON api.compute_jobs USING btree (status);


--
-- Name: idx_compute_jobs_submit_time; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_compute_jobs_submit_time ON api.compute_jobs USING btree (submit_time DESC);


--
-- Name: idx_compute_jobs_validator; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_compute_jobs_validator ON api.compute_jobs USING btree (target_validator);


--
-- Name: idx_delegation_events_delegator; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_delegation_events_delegator ON api.delegation_events USING btree (delegator_address);


--
-- Name: idx_delegation_events_delegator_validator; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_delegation_events_delegator_validator ON api.delegation_events USING btree (delegator_address, validator_address);


--
-- Name: idx_delegation_events_timestamp; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_delegation_events_timestamp ON api.delegation_events USING btree ("timestamp" DESC);


--
-- Name: idx_delegation_events_type; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_delegation_events_type ON api.delegation_events USING btree (event_type);


--
-- Name: idx_delegation_events_unique; Type: INDEX; Schema: api; Owner: -
--

CREATE UNIQUE INDEX IF NOT EXISTS idx_delegation_events_unique ON api.delegation_events USING btree (tx_hash, event_type, validator_address, COALESCE(delegator_address, ''::text));


--
-- Name: idx_delegation_events_validator; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_delegation_events_validator ON api.delegation_events USING btree (validator_address);


--
-- Name: idx_delegation_events_validator_type; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_delegation_events_validator_type ON api.delegation_events USING btree (validator_address, event_type);


--
-- Name: idx_event_type; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_event_type ON api.events_main USING btree (event_type);


--
-- Name: idx_events_id; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_events_id ON api.events_main USING btree (id);


--
-- Name: idx_events_main_id_type_key; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_events_main_id_type_key ON api.events_main USING btree (id, event_type, attr_key);


--
-- Name: idx_evm_contracts_creation_tx; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_evm_contracts_creation_tx ON api.evm_contracts USING btree (creation_tx);


--
-- Name: idx_evm_log_address; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_evm_log_address ON api.evm_logs USING btree (address);


--
-- Name: idx_evm_log_topic0; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_evm_log_topic0 ON api.evm_logs USING btree ((topics[1]));


--
-- Name: idx_evm_transactions_to_null; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_evm_transactions_to_null ON api.evm_transactions USING btree ("from", nonce) WHERE ("to" IS NULL);


--
-- Name: idx_evm_tx_from; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_evm_tx_from ON api.evm_transactions USING btree ("from");


--
-- Name: idx_evm_tx_hash; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_evm_tx_hash ON api.evm_transactions USING btree (hash);


--
-- Name: idx_evm_tx_to; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_evm_tx_to ON api.evm_transactions USING btree ("to");


--
-- Name: idx_finalize_events_height; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_finalize_events_height ON api.finalize_block_events USING btree (height);


--
-- Name: idx_finalize_events_height_type; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_finalize_events_height_type ON api.finalize_block_events USING btree (height DESC, event_type);


--
-- Name: idx_finalize_events_type; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_finalize_events_type ON api.finalize_block_events USING btree (event_type);


--
-- Name: idx_finalize_events_type_height; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_finalize_events_type_height ON api.finalize_block_events USING btree (event_type, height DESC);


--
-- Name: idx_finalize_events_validator; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_finalize_events_validator ON api.finalize_block_events USING btree (((attributes ->> 'validator'::text))) WHERE ((attributes ->> 'validator'::text) IS NOT NULL);


--
-- Name: idx_ibc_connections_chain; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_ibc_connections_chain ON api.ibc_connections USING btree (counterparty_chain_id);


--
-- Name: idx_ibc_connections_state; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_ibc_connections_state ON api.ibc_connections USING btree (state);


--
-- Name: idx_ibc_denom_base; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_ibc_denom_base ON api.ibc_denom_traces USING btree (base_denom);


--
-- Name: idx_ibc_denom_channel; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_ibc_denom_channel ON api.ibc_denom_traces USING btree (source_channel);


--
-- Name: idx_ibc_denom_pending_attempts; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_ibc_denom_pending_attempts ON api.ibc_denom_pending USING btree (attempts, last_attempt);


--
-- Name: idx_jailing_events_height; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_jailing_events_height ON api.jailing_events USING btree (height);


--
-- Name: idx_jailing_events_operator; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_jailing_events_operator ON api.jailing_events USING btree (operator_address);


--
-- Name: idx_messages_id; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_messages_id ON api.messages_main USING btree (id);


--
-- Name: idx_messages_sender; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_messages_sender ON api.messages_main USING btree (sender);


--
-- Name: idx_msg_main_id_sender; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_msg_main_id_sender ON api.messages_main USING btree (id, sender);


--
-- Name: idx_msg_type; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_msg_type ON api.messages_main USING btree (type);


--
-- Name: idx_proposal_proposer; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_proposal_proposer ON api.proposals USING btree (proposer);


--
-- Name: idx_proposal_status; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_proposal_status ON api.proposals USING btree (status);


--
-- Name: idx_proposals_status; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_proposals_status ON api.governance_proposals USING btree (status);


--
-- Name: idx_proposals_submit_time; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_proposals_submit_time ON api.governance_proposals USING btree (submit_time DESC);


--
-- Name: idx_proposals_voting_end; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_proposals_voting_end ON api.governance_proposals USING btree (voting_end_time);


--
-- Name: idx_slashing_condition; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_slashing_condition ON api.slashing_records USING btree (condition);


--
-- Name: idx_slashing_time; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_slashing_time ON api.slashing_records USING btree ("timestamp" DESC);


--
-- Name: idx_slashing_validator; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_slashing_validator ON api.slashing_records USING btree (validator_address);


--
-- Name: idx_snapshots_proposal; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_snapshots_proposal ON api.governance_snapshots USING btree (proposal_id, snapshot_time DESC);


--
-- Name: idx_token_transfer_from; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_token_transfer_from ON api.evm_token_transfers USING btree (from_address);


--
-- Name: idx_token_transfer_to; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_token_transfer_to ON api.evm_token_transfers USING btree (to_address);


--
-- Name: idx_token_transfer_token; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_token_transfer_token ON api.evm_token_transfers USING btree (token_address);


--
-- Name: idx_transactions_height; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_transactions_height ON api.transactions_main USING btree (height);


--
-- Name: idx_transactions_timestamp; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON api.transactions_main USING btree ("timestamp" DESC);


--
-- Name: idx_tx_error_not_null; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_tx_error_not_null ON api.transactions_main USING btree (id) WHERE (error IS NOT NULL);


--
-- Name: idx_tx_main_height_timestamp; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_tx_main_height_timestamp ON api.transactions_main USING btree (height DESC, "timestamp" DESC);


--
-- Name: idx_val_consensus_operator; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_val_consensus_operator ON api.validator_consensus_addresses USING btree (operator_address);


--
-- Name: idx_validator_rewards_address; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_validator_rewards_address ON api.validator_rewards USING btree (validator_address);


--
-- Name: idx_validator_rewards_height; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_validator_rewards_height ON api.validator_rewards USING btree (height DESC);


--
-- Name: idx_validator_rewards_validator; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_validator_rewards_validator ON api.validator_rewards USING btree (validator_address);


--
-- Name: idx_validator_rewards_validator_height; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_validator_rewards_validator_height ON api.validator_rewards USING btree (validator_address, height DESC);


--
-- Name: idx_validator_status; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_validator_status ON api.validators USING btree (status);


--
-- Name: idx_validators_status_tokens; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_validators_status_tokens ON api.validators USING btree (status, tokens DESC NULLS LAST);


--
-- Name: idx_vbs_consensus_addr; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_vbs_consensus_addr ON api.validator_block_signatures USING btree (consensus_address);


--
-- Name: idx_vbs_consensus_height; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_vbs_consensus_height ON api.validator_block_signatures USING btree (consensus_address, height DESC);


--
-- Name: idx_vbs_height_brin; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_vbs_height_brin ON api.validator_block_signatures USING brin (height) WITH (pages_per_range='32');


--
-- Name: idx_vbs_missed; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_vbs_missed ON api.validator_block_signatures USING btree (consensus_address, height DESC) WHERE (NOT signed);


--
-- Name: idx_vca_hex_address; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_vca_hex_address ON api.validator_consensus_addresses USING btree (hex_address);


--
-- Name: idx_vote_voter; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_vote_voter ON api.proposal_votes USING btree (voter);


--
-- Name: mv_chain_stats_singleton_idx; Type: INDEX; Schema: api; Owner: -
--

CREATE UNIQUE INDEX IF NOT EXISTS mv_chain_stats_singleton_idx ON api.mv_chain_stats USING btree ((1));


--
-- Name: mv_daily_tx_stats_date_idx; Type: INDEX; Schema: api; Owner: -
--

CREATE UNIQUE INDEX IF NOT EXISTS mv_daily_tx_stats_date_idx ON api.mv_daily_tx_stats USING btree (date);


--
-- Name: mv_fee_revenue_daily_date_denom_idx; Type: INDEX; Schema: api; Owner: -
--

CREATE UNIQUE INDEX IF NOT EXISTS mv_fee_revenue_daily_date_denom_idx ON api.mv_fee_revenue_daily USING btree (date, fee_denom);


--
-- Name: mv_hourly_rewards_hour_idx; Type: INDEX; Schema: api; Owner: -
--

CREATE UNIQUE INDEX IF NOT EXISTS mv_hourly_rewards_hour_idx ON api.mv_hourly_rewards USING btree (hour);


--
-- Name: mv_hourly_tx_stats_hour_idx; Type: INDEX; Schema: api; Owner: -
--

CREATE UNIQUE INDEX IF NOT EXISTS mv_hourly_tx_stats_hour_idx ON api.mv_hourly_tx_stats USING btree (hour);


--
-- Name: mv_message_type_stats_type_idx; Type: INDEX; Schema: api; Owner: -
--

CREATE UNIQUE INDEX IF NOT EXISTS mv_message_type_stats_type_idx ON api.mv_message_type_stats USING btree (message_type);


--
-- Name: mv_network_overview_singleton_idx; Type: INDEX; Schema: api; Owner: -
--

CREATE UNIQUE INDEX IF NOT EXISTS mv_network_overview_singleton_idx ON api.mv_network_overview USING btree ((1));


--
-- Name: mv_validator_delegator_counts_addr_idx; Type: INDEX; Schema: api; Owner: -
--

CREATE UNIQUE INDEX IF NOT EXISTS mv_validator_delegator_counts_addr_idx ON api.mv_validator_delegator_counts USING btree (validator_address);


--
-- Name: mv_validator_leaderboard_operator_idx; Type: INDEX; Schema: api; Owner: -
--

CREATE UNIQUE INDEX IF NOT EXISTS mv_validator_leaderboard_operator_idx ON api.mv_validator_leaderboard USING btree (operator_address);


--
-- Name: mv_validator_signing_stats_consensus_idx; Type: INDEX; Schema: api; Owner: -
--

CREATE UNIQUE INDEX IF NOT EXISTS mv_validator_signing_stats_consensus_idx ON api.mv_validator_signing_stats USING btree (consensus_address);


--
-- Name: mv_validator_signing_stats_operator_idx; Type: INDEX; Schema: api; Owner: -
--

CREATE INDEX IF NOT EXISTS mv_validator_signing_stats_operator_idx ON api.mv_validator_signing_stats USING btree (operator_address);


--
-- Name: events_raw new_event_update; Type: TRIGGER; Schema: api; Owner: -
--

DROP TRIGGER IF EXISTS new_event_update ON api.events_raw;
CREATE TRIGGER new_event_update AFTER INSERT OR UPDATE OF data ON api.events_raw FOR EACH ROW EXECUTE FUNCTION api.update_event_main();


--
-- Name: messages_raw new_message_update; Type: TRIGGER; Schema: api; Owner: -
--

DROP TRIGGER IF EXISTS new_message_update ON api.messages_raw;
CREATE TRIGGER new_message_update AFTER INSERT OR UPDATE ON api.messages_raw FOR EACH ROW EXECUTE FUNCTION public.update_message_main();


--
-- Name: transactions_raw new_transaction_events_raw; Type: TRIGGER; Schema: api; Owner: -
--

DROP TRIGGER IF EXISTS new_transaction_events_raw ON api.transactions_raw;
CREATE TRIGGER new_transaction_events_raw AFTER INSERT OR UPDATE OF data ON api.transactions_raw FOR EACH ROW EXECUTE FUNCTION api.update_events_raw();


--
-- Name: transactions_raw new_transaction_update; Type: TRIGGER; Schema: api; Owner: -
--

DROP TRIGGER IF EXISTS new_transaction_update ON api.transactions_raw;
CREATE TRIGGER new_transaction_update AFTER INSERT OR UPDATE ON api.transactions_raw FOR EACH ROW EXECUTE FUNCTION public.update_transaction_main();


--
-- Name: finalize_block_events trg_fbe_update_validator_liveness; Type: TRIGGER; Schema: api; Owner: -
--

DROP TRIGGER IF EXISTS trg_fbe_update_validator_liveness ON api.finalize_block_events;
CREATE TRIGGER trg_fbe_update_validator_liveness AFTER INSERT ON api.finalize_block_events FOR EACH ROW EXECUTE FUNCTION api.trg_update_validator_liveness();


--
-- Name: ibc_denom_pending trg_ibc_denom_pending_notify; Type: TRIGGER; Schema: api; Owner: -
--

DROP TRIGGER IF EXISTS trg_ibc_denom_pending_notify ON api.ibc_denom_pending;
CREATE TRIGGER trg_ibc_denom_pending_notify AFTER INSERT ON api.ibc_denom_pending FOR EACH ROW EXECUTE FUNCTION api.notify_ibc_denom_pending();


--
-- Name: blocks_raw trg_populate_block_metrics; Type: TRIGGER; Schema: api; Owner: -
--

DROP TRIGGER IF EXISTS trg_populate_block_metrics ON api.blocks_raw;
CREATE TRIGGER trg_populate_block_metrics AFTER INSERT ON api.blocks_raw FOR EACH ROW EXECUTE FUNCTION api.trg_populate_block_metrics();


--
-- Name: messages_main trg_queue_ibc_denoms; Type: TRIGGER; Schema: api; Owner: -
--

DROP TRIGGER IF EXISTS trg_queue_ibc_denoms ON api.messages_main;
CREATE TRIGGER trg_queue_ibc_denoms AFTER INSERT ON api.messages_main FOR EACH ROW EXECUTE FUNCTION api.extract_and_queue_ibc_denoms();


--
-- Name: evm_transactions trg_rt_chain_stats_evm; Type: TRIGGER; Schema: api; Owner: -
--

DROP TRIGGER IF EXISTS trg_rt_chain_stats_evm ON api.evm_transactions;
CREATE TRIGGER trg_rt_chain_stats_evm AFTER INSERT ON api.evm_transactions FOR EACH ROW EXECUTE FUNCTION api.trg_rt_chain_stats_evm();


--
-- Name: validators trg_rt_chain_stats_validators; Type: TRIGGER; Schema: api; Owner: -
--

DROP TRIGGER IF EXISTS trg_rt_chain_stats_validators ON api.validators;
CREATE TRIGGER trg_rt_chain_stats_validators AFTER UPDATE ON api.validators FOR EACH ROW EXECUTE FUNCTION api.trg_rt_chain_stats_validators();


--
-- Name: validator_rewards trg_rt_daily_rewards; Type: TRIGGER; Schema: api; Owner: -
--

DROP TRIGGER IF EXISTS trg_rt_daily_rewards ON api.validator_rewards;
CREATE TRIGGER trg_rt_daily_rewards AFTER INSERT ON api.validator_rewards FOR EACH ROW EXECUTE FUNCTION api.trg_rt_daily_rewards();


--
-- Name: validator_rewards trg_rt_hourly_rewards; Type: TRIGGER; Schema: api; Owner: -
--

DROP TRIGGER IF EXISTS trg_rt_hourly_rewards ON api.validator_rewards;
CREATE TRIGGER trg_rt_hourly_rewards AFTER INSERT ON api.validator_rewards FOR EACH ROW EXECUTE FUNCTION api.trg_rt_hourly_rewards();


--
-- Name: messages_main trg_rt_message_type_stats; Type: TRIGGER; Schema: api; Owner: -
--

DROP TRIGGER IF EXISTS trg_rt_message_type_stats ON api.messages_main;
CREATE TRIGGER trg_rt_message_type_stats AFTER INSERT ON api.messages_main FOR EACH ROW EXECUTE FUNCTION api.trg_rt_message_type_stats();


--
-- Name: transactions_main trg_rt_tx_stats; Type: TRIGGER; Schema: api; Owner: -
--

DROP TRIGGER IF EXISTS trg_rt_tx_stats ON api.transactions_main;
CREATE TRIGGER trg_rt_tx_stats AFTER INSERT ON api.transactions_main FOR EACH ROW EXECUTE FUNCTION api.trg_rt_tx_stats();


--
-- Name: blocks_raw trg_set_block_time; Type: TRIGGER; Schema: api; Owner: -
--

DROP TRIGGER IF EXISTS trg_set_block_time ON api.blocks_raw;
CREATE TRIGGER trg_set_block_time BEFORE INSERT ON api.blocks_raw FOR EACH ROW EXECUTE FUNCTION api.trg_set_block_time();


--
-- Name: validator_block_signatures trg_vbs_update_validator_stats; Type: TRIGGER; Schema: api; Owner: -
--

DROP TRIGGER IF EXISTS trg_vbs_update_validator_stats ON api.validator_block_signatures;
CREATE TRIGGER trg_vbs_update_validator_stats AFTER INSERT ON api.validator_block_signatures FOR EACH ROW EXECUTE FUNCTION api.trg_update_validator_signing_stats();


--
-- Name: transactions_main trigger_detect_compute_validation; Type: TRIGGER; Schema: api; Owner: -
--

DROP TRIGGER IF EXISTS trigger_detect_compute_validation ON api.transactions_main;
CREATE TRIGGER trigger_detect_compute_validation AFTER INSERT ON api.transactions_main FOR EACH ROW EXECUTE FUNCTION api.detect_compute_validation();


--
-- Name: blocks_raw trigger_detect_jailing; Type: TRIGGER; Schema: api; Owner: -
--

DROP TRIGGER IF EXISTS trigger_detect_jailing ON api.blocks_raw;
CREATE TRIGGER trigger_detect_jailing AFTER INSERT ON api.blocks_raw FOR EACH ROW EXECUTE FUNCTION api.detect_jailing_from_block();


--
-- Name: transactions_main trigger_detect_proposals; Type: TRIGGER; Schema: api; Owner: -
--

DROP TRIGGER IF EXISTS trigger_detect_proposals ON api.transactions_main;
CREATE TRIGGER trigger_detect_proposals AFTER INSERT ON api.transactions_main FOR EACH ROW EXECUTE FUNCTION api.detect_proposal_submission();


--
-- Name: transactions_main trigger_detect_reputation; Type: TRIGGER; Schema: api; Owner: -
--

DROP TRIGGER IF EXISTS trigger_detect_reputation ON api.transactions_main;
CREATE TRIGGER trigger_detect_reputation AFTER INSERT ON api.transactions_main FOR EACH ROW EXECUTE FUNCTION api.detect_reputation_messages();


--
-- Name: transactions_main trigger_detect_slashing; Type: TRIGGER; Schema: api; Owner: -
--

DROP TRIGGER IF EXISTS trigger_detect_slashing ON api.transactions_main;
CREATE TRIGGER trigger_detect_slashing AFTER INSERT ON api.transactions_main FOR EACH ROW EXECUTE FUNCTION api.detect_slashing_messages();


--
-- Name: messages_main trigger_detect_staking_from_message; Type: TRIGGER; Schema: api; Owner: -
--

DROP TRIGGER IF EXISTS trigger_detect_staking_from_message ON api.messages_main;
CREATE TRIGGER trigger_detect_staking_from_message AFTER INSERT ON api.messages_main FOR EACH ROW WHEN (((new.type ~~ '%MsgDelegate'::text) OR (new.type ~~ '%MsgUndelegate'::text) OR (new.type ~~ '%MsgBeginRedelegate'::text) OR (new.type ~~ '%MsgCreateValidator'::text) OR (new.type ~~ '%MsgEditValidator'::text) OR (new.type ~~ '%MsgUnjail'::text))) EXECUTE FUNCTION api.detect_staking_from_message();


--
-- Name: blocks_raw trigger_extract_block_signatures; Type: TRIGGER; Schema: api; Owner: -
--

DROP TRIGGER IF EXISTS trigger_extract_block_signatures ON api.blocks_raw;
CREATE TRIGGER trigger_extract_block_signatures AFTER INSERT ON api.blocks_raw FOR EACH ROW EXECUTE FUNCTION api.trigger_extract_signatures();


--
-- Name: block_results_raw trigger_extract_finalize_events; Type: TRIGGER; Schema: api; Owner: -
--

DROP TRIGGER IF EXISTS trigger_extract_finalize_events ON api.block_results_raw;
CREATE TRIGGER trigger_extract_finalize_events AFTER INSERT ON api.block_results_raw FOR EACH ROW EXECUTE FUNCTION api.extract_finalize_block_events();


--
-- Name: block_results_raw trigger_extract_rewards; Type: TRIGGER; Schema: api; Owner: -
--

DROP TRIGGER IF EXISTS trigger_extract_rewards ON api.block_results_raw;
CREATE TRIGGER trigger_extract_rewards AFTER INSERT ON api.block_results_raw FOR EACH ROW EXECUTE FUNCTION api.extract_rewards_from_events();


--
-- Name: messages_main trigger_extract_validator_consensus_pubkey; Type: TRIGGER; Schema: api; Owner: -
--

DROP TRIGGER IF EXISTS trigger_extract_validator_consensus_pubkey ON api.messages_main;
CREATE TRIGGER trigger_extract_validator_consensus_pubkey AFTER INSERT ON api.messages_main FOR EACH ROW EXECUTE FUNCTION api.extract_validator_consensus_pubkey();


--
-- Name: jailing_events trigger_propagate_jailing_event; Type: TRIGGER; Schema: api; Owner: -
--

DROP TRIGGER IF EXISTS trigger_propagate_jailing_event ON api.jailing_events;
CREATE TRIGGER trigger_propagate_jailing_event AFTER INSERT ON api.jailing_events FOR EACH ROW EXECUTE FUNCTION api.propagate_jailing_event();


--
-- Name: transactions_main trigger_track_votes; Type: TRIGGER; Schema: api; Owner: -
--

DROP TRIGGER IF EXISTS trigger_track_votes ON api.transactions_main;
CREATE TRIGGER trigger_track_votes AFTER INSERT ON api.transactions_main FOR EACH ROW EXECUTE FUNCTION api.track_governance_vote();


--
-- Name: transactions_main trigger_update_block_tx_count; Type: TRIGGER; Schema: api; Owner: -
--

DROP TRIGGER IF EXISTS trigger_update_block_tx_count ON api.transactions_main;
CREATE TRIGGER trigger_update_block_tx_count AFTER INSERT ON api.transactions_main FOR EACH ROW EXECUTE FUNCTION api.update_block_tx_count();


--
-- Name: compute_seed_contributions compute_seed_contributions_benchmark_id_fkey; Type: FK CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'compute_seed_contributions_benchmark_id_fkey' AND conrelid = 'api.compute_seed_contributions'::regclass) THEN
    ALTER TABLE ONLY api.compute_seed_contributions
        ADD CONSTRAINT compute_seed_contributions_benchmark_id_fkey FOREIGN KEY (benchmark_id) REFERENCES api.compute_benchmarks(benchmark_id);
  END IF;
END $$;


--
-- Name: denom_metadata denom_metadata_evm_contract_fkey; Type: FK CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'denom_metadata_evm_contract_fkey' AND conrelid = 'api.denom_metadata'::regclass) THEN
    ALTER TABLE ONLY api.denom_metadata
        ADD CONSTRAINT denom_metadata_evm_contract_fkey FOREIGN KEY (evm_contract) REFERENCES api.evm_tokens(address);
  END IF;
END $$;


--
-- Name: events_raw events_raw_id_fkey; Type: FK CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'events_raw_id_fkey' AND conrelid = 'api.events_raw'::regclass) THEN
    ALTER TABLE ONLY api.events_raw
        ADD CONSTRAINT events_raw_id_fkey FOREIGN KEY (id) REFERENCES api.transactions_raw(id) ON DELETE CASCADE;
  END IF;
END $$;


--
-- Name: evm_logs evm_logs_tx_id_fkey; Type: FK CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'evm_logs_tx_id_fkey' AND conrelid = 'api.evm_logs'::regclass) THEN
    ALTER TABLE ONLY api.evm_logs
        ADD CONSTRAINT evm_logs_tx_id_fkey FOREIGN KEY (tx_id) REFERENCES api.evm_transactions(tx_id) ON DELETE CASCADE;
  END IF;
END $$;


--
-- Name: evm_token_transfers evm_token_transfers_token_address_fkey; Type: FK CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'evm_token_transfers_token_address_fkey' AND conrelid = 'api.evm_token_transfers'::regclass) THEN
    ALTER TABLE ONLY api.evm_token_transfers
        ADD CONSTRAINT evm_token_transfers_token_address_fkey FOREIGN KEY (token_address) REFERENCES api.evm_tokens(address);
  END IF;
END $$;


--
-- Name: evm_token_transfers evm_token_transfers_tx_id_log_index_fkey; Type: FK CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'evm_token_transfers_tx_id_log_index_fkey' AND conrelid = 'api.evm_token_transfers'::regclass) THEN
    ALTER TABLE ONLY api.evm_token_transfers
        ADD CONSTRAINT evm_token_transfers_tx_id_log_index_fkey FOREIGN KEY (tx_id, log_index) REFERENCES api.evm_logs(tx_id, log_index) ON DELETE CASCADE;
  END IF;
END $$;


--
-- Name: evm_transactions evm_transactions_tx_id_fkey; Type: FK CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'evm_transactions_tx_id_fkey' AND conrelid = 'api.evm_transactions'::regclass) THEN
    ALTER TABLE ONLY api.evm_transactions
        ADD CONSTRAINT evm_transactions_tx_id_fkey FOREIGN KEY (tx_id) REFERENCES api.transactions_main(id) ON DELETE CASCADE;
  END IF;
END $$;


--
-- Name: governance_snapshots governance_snapshots_proposal_id_fkey; Type: FK CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'governance_snapshots_proposal_id_fkey' AND conrelid = 'api.governance_snapshots'::regclass) THEN
    ALTER TABLE ONLY api.governance_snapshots
        ADD CONSTRAINT governance_snapshots_proposal_id_fkey FOREIGN KEY (proposal_id) REFERENCES api.governance_proposals(proposal_id);
  END IF;
END $$;


--
-- Name: proposal_votes proposal_votes_proposal_id_fkey; Type: FK CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'proposal_votes_proposal_id_fkey' AND conrelid = 'api.proposal_votes'::regclass) THEN
    ALTER TABLE ONLY api.proposal_votes
        ADD CONSTRAINT proposal_votes_proposal_id_fkey FOREIGN KEY (proposal_id) REFERENCES api.proposals(id) ON DELETE CASCADE;
  END IF;
END $$;


--
-- Name: validator_consensus_addresses validator_consensus_addresses_operator_address_fkey; Type: FK CONSTRAINT; Schema: api; Owner: -
--

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'validator_consensus_addresses_operator_address_fkey' AND conrelid = 'api.validator_consensus_addresses'::regclass) THEN
    ALTER TABLE ONLY api.validator_consensus_addresses
        ADD CONSTRAINT validator_consensus_addresses_operator_address_fkey FOREIGN KEY (operator_address) REFERENCES api.validators(operator_address);
  END IF;
END $$;


--
-- Name: SCHEMA api; Type: ACL; Schema: -; Owner: -
--

GRANT USAGE ON SCHEMA api TO web_anon;


--
-- Name: FUNCTION _normalize_rate(val numeric); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api._normalize_rate(val numeric) TO web_anon;


--
-- Name: FUNCTION backfill_block_signatures(_start_height bigint, _batch_size integer); Type: ACL; Schema: api; Owner: -
--

REVOKE ALL ON FUNCTION api.backfill_block_signatures(_start_height bigint, _batch_size integer) FROM PUBLIC;


--
-- Name: FUNCTION backfill_finalize_block_events(); Type: ACL; Schema: api; Owner: -
--

REVOKE ALL ON FUNCTION api.backfill_finalize_block_events() FROM PUBLIC;


--
-- Name: FUNCTION backfill_jailing_events(); Type: ACL; Schema: api; Owner: -
--

REVOKE ALL ON FUNCTION api.backfill_jailing_events() FROM PUBLIC;


--
-- Name: FUNCTION backfill_validator_consensus_addresses(); Type: ACL; Schema: api; Owner: -
--

REVOKE ALL ON FUNCTION api.backfill_validator_consensus_addresses() FROM PUBLIC;


--
-- Name: FUNCTION compute_consensus_address(_pubkey_base64 text); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.compute_consensus_address(_pubkey_base64 text) TO web_anon;


--
-- Name: FUNCTION compute_proposal_tally(_proposal_id bigint); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.compute_proposal_tally(_proposal_id bigint) TO web_anon;


--
-- Name: FUNCTION extract_block_signatures(_height bigint, _block_data jsonb); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.extract_block_signatures(_height bigint, _block_data jsonb) TO web_anon;


--
-- Name: TABLE messages_main; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.messages_main TO web_anon;


--
-- Name: FUNCTION extract_ibc_transfer_details(_message api.messages_main); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.extract_ibc_transfer_details(_message api.messages_main) TO web_anon;


--
-- Name: FUNCTION get_address_stats(_address text); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_address_stats(_address text) TO web_anon;


--
-- Name: FUNCTION get_all_validators_signing_stats(_window_size integer); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_all_validators_signing_stats(_window_size integer) TO web_anon;


--
-- Name: FUNCTION get_block_time_analysis(_limit integer); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_block_time_analysis(_limit integer) TO web_anon;


--
-- Name: FUNCTION get_blocks_paginated(_limit integer, _offset integer, _min_tx_count integer, _from_date timestamp without time zone, _to_date timestamp without time zone); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_blocks_paginated(_limit integer, _offset integer, _min_tx_count integer, _from_date timestamp without time zone, _to_date timestamp without time zone) TO web_anon;


--
-- Name: FUNCTION get_chain_params(); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_chain_params() TO web_anon;


--
-- Name: FUNCTION get_compute_benchmarks(_limit integer, _offset integer, _status text); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_compute_benchmarks(_limit integer, _offset integer, _status text) TO web_anon;


--
-- Name: FUNCTION get_compute_job(_job_id bigint); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_compute_job(_job_id bigint) TO web_anon;


--
-- Name: FUNCTION get_compute_jobs(_limit integer, _offset integer, _status text, _creator text, _validator text); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_compute_jobs(_limit integer, _offset integer, _status text, _creator text, _validator text) TO web_anon;


--
-- Name: FUNCTION get_delegation_events(_validator_address text, _limit integer, _offset integer, _event_type text); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_delegation_events(_validator_address text, _limit integer, _offset integer, _event_type text) TO web_anon;


--
-- Name: FUNCTION get_delegator_delegations(_delegator_address text); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_delegator_delegations(_delegator_address text) TO web_anon;


--
-- Name: FUNCTION get_delegator_history(_delegator_address text, _limit integer, _offset integer, _event_type text); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_delegator_history(_delegator_address text, _limit integer, _offset integer, _event_type text) TO web_anon;


--
-- Name: FUNCTION get_delegator_stats(_delegator_address text); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_delegator_stats(_delegator_address text) TO web_anon;


--
-- Name: FUNCTION get_delegator_validator_history(_delegator_address text, _validator_address text, _limit integer, _offset integer); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_delegator_validator_history(_delegator_address text, _validator_address text, _limit integer, _offset integer) TO web_anon;


--
-- Name: FUNCTION get_governance_proposals(_limit integer, _offset integer, _status text); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_governance_proposals(_limit integer, _offset integer, _status text) TO web_anon;


--
-- Name: FUNCTION get_hourly_rewards(_hours integer); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_hourly_rewards(_hours integer) TO web_anon;


--
-- Name: FUNCTION get_ibc_chains(); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_ibc_chains() TO web_anon;


--
-- Name: FUNCTION get_ibc_channel_activity(); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_ibc_channel_activity() TO web_anon;


--
-- Name: FUNCTION get_ibc_connection(_channel_id text, _port_id text); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_ibc_connection(_channel_id text, _port_id text) TO web_anon;


--
-- Name: FUNCTION get_ibc_connections(_limit integer, _offset integer, _chain_id text, _state text); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_ibc_connections(_limit integer, _offset integer, _chain_id text, _state text) TO web_anon;


--
-- Name: FUNCTION get_ibc_denom_traces(_limit integer, _offset integer, _base_denom text); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_ibc_denom_traces(_limit integer, _offset integer, _base_denom text) TO web_anon;


--
-- Name: FUNCTION get_ibc_heatmap_data(_timeframe text, _metric text, _direction text, _channel text); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_ibc_heatmap_data(_timeframe text, _metric text, _direction text, _channel text) TO web_anon;


--
-- Name: FUNCTION get_ibc_stats(); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_ibc_stats() TO web_anon;


--
-- Name: FUNCTION get_ibc_transfers(_limit integer, _offset integer, _direction text); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_ibc_transfers(_limit integer, _offset integer, _direction text) TO web_anon;


--
-- Name: FUNCTION get_ibc_transfers_by_address(_address text, _limit integer, _offset integer); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_ibc_transfers_by_address(_address text, _limit integer, _offset integer) TO web_anon;


--
-- Name: FUNCTION get_ibc_volume_timeseries(_hours integer, _channel text); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_ibc_volume_timeseries(_hours integer, _channel text) TO web_anon;


--
-- Name: FUNCTION get_messages_for_address(_address text); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_messages_for_address(_address text) TO web_anon;


--
-- Name: FUNCTION get_network_overview(); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_network_overview() TO web_anon;


--
-- Name: FUNCTION get_recent_validator_events(_event_types text[], _limit integer, _offset integer); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_recent_validator_events(_event_types text[], _limit integer, _offset integer) TO web_anon;


--
-- Name: FUNCTION get_slashing_records(_limit integer, _offset integer, _validator text, _condition text); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_slashing_records(_limit integer, _offset integer, _validator text, _condition text) TO web_anon;


--
-- Name: FUNCTION get_transaction_detail(_hash text); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_transaction_detail(_hash text) TO web_anon;


--
-- Name: FUNCTION get_transactions_by_address(_address text, _limit integer, _offset integer); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_transactions_by_address(_address text, _limit integer, _offset integer) TO web_anon;


--
-- Name: FUNCTION get_transactions_paginated(_limit integer, _offset integer, _status text, _block_height bigint, _block_height_min bigint, _block_height_max bigint, _message_type text, _timestamp_min timestamp with time zone, _timestamp_max timestamp with time zone); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_transactions_paginated(_limit integer, _offset integer, _status text, _block_height bigint, _block_height_min bigint, _block_height_max bigint, _message_type text, _timestamp_min timestamp with time zone, _timestamp_max timestamp with time zone) TO web_anon;


--
-- Name: FUNCTION get_validator_detail(_operator_address text); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_validator_detail(_operator_address text) TO web_anon;


--
-- Name: FUNCTION get_validator_events_summary(_limit integer); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_validator_events_summary(_limit integer) TO web_anon;


--
-- Name: FUNCTION get_validator_jailing_events(_operator_address text, _limit integer, _offset integer); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_validator_jailing_events(_operator_address text, _limit integer, _offset integer) TO web_anon;


--
-- Name: FUNCTION get_validator_performance(_operator_address text); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_validator_performance(_operator_address text) TO web_anon;


--
-- Name: FUNCTION get_validator_rewards_history(_operator_address text, _limit integer, _offset integer); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_validator_rewards_history(_operator_address text, _limit integer, _offset integer) TO web_anon;


--
-- Name: FUNCTION get_validator_signing_stats(_consensus_address text, _window_size integer); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_validator_signing_stats(_consensus_address text, _window_size integer) TO web_anon;


--
-- Name: FUNCTION get_validator_total_rewards(_operator_address text); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_validator_total_rewards(_operator_address text) TO web_anon;


--
-- Name: FUNCTION get_validators_paginated(_limit integer, _offset integer, _sort_by text, _sort_dir text, _status text, _search text); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_validators_paginated(_limit integer, _offset integer, _sort_by text, _sort_dir text, _status text, _search text) TO web_anon;


--
-- Name: FUNCTION get_validators_with_signing_stats(_limit integer, _offset integer); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.get_validators_with_signing_stats(_limit integer, _offset integer) TO web_anon;


--
-- Name: FUNCTION normalize_consensus_address(addr text); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.normalize_consensus_address(addr text) TO web_anon;


--
-- Name: FUNCTION queue_unknown_ibc_denom(_ibc_denom text); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.queue_unknown_ibc_denom(_ibc_denom text) TO web_anon;


--
-- Name: FUNCTION refresh_analytics_views(); Type: ACL; Schema: api; Owner: -
--

REVOKE ALL ON FUNCTION api.refresh_analytics_views() FROM PUBLIC;


--
-- Name: FUNCTION register_validator_consensus_address(_operator_address text, _pubkey_base64 text); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.register_validator_consensus_address(_operator_address text, _pubkey_base64 text) TO web_anon;


--
-- Name: FUNCTION request_evm_decode(_tx_hash text); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.request_evm_decode(_tx_hash text) TO web_anon;


--
-- Name: FUNCTION request_validator_refresh(_operator_address text); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.request_validator_refresh(_operator_address text) TO web_anon;


--
-- Name: FUNCTION resolve_denom(_denom text); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.resolve_denom(_denom text) TO web_anon;


--
-- Name: FUNCTION resolve_ibc_denom(_ibc_denom text); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.resolve_ibc_denom(_ibc_denom text) TO web_anon;


--
-- Name: FUNCTION universal_search(_query text); Type: ACL; Schema: api; Owner: -
--

GRANT ALL ON FUNCTION api.universal_search(_query text) TO web_anon;


--
-- Name: TABLE block_metrics; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.block_metrics TO web_anon;


--
-- Name: TABLE block_results_raw; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE api.block_results_raw TO web_anon;


--
-- Name: TABLE blocks_raw; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.blocks_raw TO web_anon;


--
-- Name: TABLE chain_features; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.chain_features TO web_anon;


--
-- Name: TABLE chain_params; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.chain_params TO web_anon;


--
-- Name: TABLE rt_chain_stats; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.rt_chain_stats TO web_anon;


--
-- Name: TABLE chain_stats; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.chain_stats TO web_anon;


--
-- Name: TABLE compute_benchmarks; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.compute_benchmarks TO web_anon;


--
-- Name: TABLE compute_committee_proposals; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.compute_committee_proposals TO web_anon;


--
-- Name: TABLE compute_jobs; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.compute_jobs TO web_anon;


--
-- Name: TABLE compute_seed_contributions; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.compute_seed_contributions TO web_anon;


--
-- Name: TABLE compute_stats; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.compute_stats TO web_anon;


--
-- Name: TABLE transactions_main; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.transactions_main TO web_anon;


--
-- Name: TABLE daily_active_addresses; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.daily_active_addresses TO web_anon;


--
-- Name: TABLE rt_daily_tx_stats; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.rt_daily_tx_stats TO web_anon;


--
-- Name: TABLE daily_tx_stats; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.daily_tx_stats TO web_anon;


--
-- Name: TABLE delegation_events; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.delegation_events TO web_anon;


--
-- Name: TABLE denom_metadata; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.denom_metadata TO web_anon;


--
-- Name: TABLE events_main; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.events_main TO web_anon;


--
-- Name: TABLE events_raw; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.events_raw TO web_anon;


--
-- Name: TABLE evm_contracts; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.evm_contracts TO web_anon;


--
-- Name: TABLE evm_logs; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.evm_logs TO web_anon;


--
-- Name: TABLE evm_transactions; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.evm_transactions TO web_anon;


--
-- Name: TABLE evm_missing_contracts; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.evm_missing_contracts TO web_anon;


--
-- Name: TABLE messages_raw; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.messages_raw TO web_anon;


--
-- Name: TABLE evm_pending_decode; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.evm_pending_decode TO web_anon;


--
-- Name: TABLE evm_token_transfers; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.evm_token_transfers TO web_anon;


--
-- Name: TABLE evm_tokens; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.evm_tokens TO web_anon;


--
-- Name: TABLE evm_tokens_missing_metadata; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.evm_tokens_missing_metadata TO web_anon;


--
-- Name: TABLE evm_tx_map; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.evm_tx_map TO web_anon;


--
-- Name: TABLE fee_revenue; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.fee_revenue TO web_anon;


--
-- Name: TABLE finalize_block_events; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.finalize_block_events TO web_anon;


--
-- Name: TABLE gas_usage_distribution; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.gas_usage_distribution TO web_anon;


--
-- Name: TABLE governance_proposals; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.governance_proposals TO web_anon;


--
-- Name: TABLE governance_active_proposals; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.governance_active_proposals TO web_anon;


--
-- Name: TABLE governance_snapshots; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.governance_snapshots TO web_anon;


--
-- Name: TABLE rt_hourly_tx_stats; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.rt_hourly_tx_stats TO web_anon;


--
-- Name: TABLE hourly_tx_stats; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.hourly_tx_stats TO web_anon;


--
-- Name: TABLE ibc_channels; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.ibc_channels TO web_anon;


--
-- Name: TABLE ibc_connections; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.ibc_connections TO web_anon;


--
-- Name: TABLE ibc_denom_pending; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.ibc_denom_pending TO web_anon;


--
-- Name: TABLE ibc_denom_traces; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.ibc_denom_traces TO web_anon;


--
-- Name: TABLE jailing_events; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.jailing_events TO web_anon;


--
-- Name: TABLE rt_message_type_stats; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.rt_message_type_stats TO web_anon;


--
-- Name: TABLE message_type_stats; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.message_type_stats TO web_anon;


--
-- Name: TABLE validators; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.validators TO web_anon;


--
-- Name: TABLE mv_chain_stats; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.mv_chain_stats TO web_anon;


--
-- Name: TABLE rt_daily_rewards; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.rt_daily_rewards TO web_anon;


--
-- Name: TABLE mv_daily_rewards; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.mv_daily_rewards TO web_anon;


--
-- Name: TABLE mv_daily_tx_stats; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.mv_daily_tx_stats TO web_anon;


--
-- Name: TABLE mv_fee_revenue_daily; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.mv_fee_revenue_daily TO web_anon;


--
-- Name: TABLE validator_rewards; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.validator_rewards TO web_anon;


--
-- Name: TABLE mv_hourly_rewards; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.mv_hourly_rewards TO web_anon;


--
-- Name: TABLE mv_hourly_tx_stats; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.mv_hourly_tx_stats TO web_anon;


--
-- Name: TABLE mv_message_type_stats; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.mv_message_type_stats TO web_anon;


--
-- Name: TABLE mv_network_overview; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.mv_network_overview TO web_anon;


--
-- Name: TABLE mv_validator_delegator_counts; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.mv_validator_delegator_counts TO web_anon;


--
-- Name: TABLE validator_consensus_addresses; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.validator_consensus_addresses TO web_anon;


--
-- Name: TABLE mv_validator_leaderboard; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.mv_validator_leaderboard TO web_anon;


--
-- Name: TABLE validator_block_signatures; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.validator_block_signatures TO web_anon;


--
-- Name: TABLE mv_validator_signing_stats; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.mv_validator_signing_stats TO web_anon;


--
-- Name: TABLE proposal_votes; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.proposal_votes TO web_anon;


--
-- Name: TABLE proposals; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.proposals TO web_anon;


--
-- Name: TABLE query_stats; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.query_stats TO web_anon;


--
-- Name: TABLE rt_hourly_rewards; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.rt_hourly_rewards TO web_anon;


--
-- Name: TABLE slashing_records; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.slashing_records TO web_anon;


--
-- Name: TABLE transactions_raw; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.transactions_raw TO web_anon;


--
-- Name: TABLE tx_success_rate; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.tx_success_rate TO web_anon;


--
-- Name: TABLE tx_volume_daily; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.tx_volume_daily TO web_anon;


--
-- Name: TABLE tx_volume_hourly; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.tx_volume_hourly TO web_anon;


--
-- Name: TABLE validator_ipfs_addresses; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.validator_ipfs_addresses TO web_anon;


--
-- Name: TABLE validator_stats; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.validator_stats TO web_anon;


--
-- Name: TABLE validators_with_consensus; Type: ACL; Schema: api; Owner: -
--

GRANT SELECT ON TABLE api.validators_with_consensus TO web_anon;


--
-- PostgreSQL database dump complete
--

COMMIT;
