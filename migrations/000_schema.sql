-- =============================================================================
-- Bootstrap: schema, roles, extensions
-- The numbered migrations assume the api schema and web_anon already exist;
-- create them here so the set can be applied to a fresh database.
-- =============================================================================

BEGIN;

CREATE SCHEMA IF NOT EXISTS api;

CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

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

GRANT USAGE ON SCHEMA api TO web_anon;

COMMIT;
