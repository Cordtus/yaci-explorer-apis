-- Migration 059: Fix deadlock in extract_rewards_from_events trigger
--
-- Problem: During catch-up indexing with high concurrency (100 goroutines),
-- the row-at-a-time INSERT loop in the trigger causes deadlocks when multiple
-- concurrent block_results_raw inserts fire triggers that compete for row-level
-- locks on api.validator_rewards.
--
-- Solution: Replace the PL/pgSQL loop with a single set-based INSERT that
-- processes all events atomically with deterministic ordering. This prevents
-- deadlocks by ensuring consistent lock acquisition order.

BEGIN;

CREATE OR REPLACE FUNCTION api.extract_rewards_from_events()
RETURNS TRIGGER AS $$
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
$$ LANGUAGE plpgsql;

COMMIT;
