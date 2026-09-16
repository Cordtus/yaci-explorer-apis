BEGIN;

-- Migration 066: Drop unused indexes identified from live pg_stat_user_indexes
--
-- All indexes below have 0 scans (or near-zero) and are duplicated by
-- actively-used counterparts from later migrations. Total savings: ~59 MB.

-- transactions_main: idx_tx_height (0 scans) duplicated by idx_transactions_height (22K scans)
DROP INDEX IF EXISTS api.idx_tx_height;

-- transactions_main: idx_tx_timestamp (4 scans) duplicated by idx_transactions_timestamp (9.7K scans)
DROP INDEX IF EXISTS api.idx_tx_timestamp;

-- messages_main: idx_msg_sender (0 scans) duplicated by idx_messages_sender (20K scans)
DROP INDEX IF EXISTS api.idx_msg_sender;

-- messages_main: two GIN indexes on mentions, never used
DROP INDEX IF EXISTS api.idx_msg_mentions;
DROP INDEX IF EXISTS api.idx_messages_mentions_gin;

-- messages_main: composite (type, sender), never used
DROP INDEX IF EXISTS api.idx_msg_main_type_sender;

-- validators: idx_validator_tokens (0 scans, 25 MB)
DROP INDEX IF EXISTS api.idx_validator_tokens;

-- jailing_events: idx_jailing_events_validator (0 scans, 9 MB)
DROP INDEX IF EXISTS api.idx_jailing_events_validator;

-- block_results_raw: both indexes unused
DROP INDEX IF EXISTS api.idx_block_results_raw_height;
DROP INDEX IF EXISTS api.idx_block_results_has_events;

-- delegation_events: both indexes unused
DROP INDEX IF EXISTS api.idx_delegation_events_tx_hash;
DROP INDEX IF EXISTS api.idx_delegation_events_delegator_timestamp;

COMMIT;
