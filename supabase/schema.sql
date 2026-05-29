-- ─────────────────────────────────────────────────────────────────────────────
-- Forerunner schema for Supabase / Postgres
-- ─────────────────────────────────────────────────────────────────────────────
-- Run this in your Supabase SQL editor (Database → SQL editor → New query).
-- Safe to re-run: every CREATE uses IF NOT EXISTS, every DROP is guarded.
--
-- Layout intent:
--   - One row per trade (immutable history)
--   - One row per open position (mutable, deleted when closed)
--   - One row per equity snapshot (append-only time series)
--   - One row per "session" of params (single-user, so always id=1)
--
-- All tables include user_id (uuid) so we can later add multi-user support
-- without a schema migration. Until then, all rows use a fixed
-- DEFAULT_USER_ID constant. RLS policies enforce one user can't see
-- another's data even though we don't authenticate today.
-- ─────────────────────────────────────────────────────────────────────────────

-- The default single-user UUID. Frontend hardcodes this.
-- Replace with auth.uid() once you wire Supabase auth.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pgcrypto') THEN
    CREATE EXTENSION pgcrypto;
  END IF;
END $$;

-- ─── trades ──────────────────────────────────────────────────────────────────
-- Closed paper trades. Append-only.
CREATE TABLE IF NOT EXISTS trades (
  id            BIGSERIAL PRIMARY KEY,
  user_id       UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
  client_id     BIGINT NOT NULL,           -- frontend-generated, stable for dedup
  chain         TEXT NOT NULL,
  symbol        TEXT NOT NULL,
  address       TEXT,
  qty           NUMERIC NOT NULL,
  entry_px      NUMERIC NOT NULL,
  exit_px       NUMERIC NOT NULL,
  pnl_usd       NUMERIC NOT NULL,
  pnl_pct       NUMERIC NOT NULL,
  reason        TEXT NOT NULL,             -- 'tp', 'sl', 'timeout'
  opened_at     TIMESTAMPTZ NOT NULL,
  closed_at     TIMESTAMPTZ NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS trades_user_closed_idx ON trades (user_id, closed_at DESC);
-- Dedup index: same (user, client_id) can never be inserted twice.
CREATE UNIQUE INDEX IF NOT EXISTS trades_user_client_idx ON trades (user_id, client_id);

-- ─── positions ───────────────────────────────────────────────────────────────
-- Currently open paper positions. Frontend deletes rows when closing.
CREATE TABLE IF NOT EXISTS positions (
  id            BIGSERIAL PRIMARY KEY,
  user_id       UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
  client_id     BIGINT NOT NULL,
  chain         TEXT NOT NULL,
  symbol        TEXT NOT NULL,
  address       TEXT,
  qty           NUMERIC NOT NULL,
  entry_px      NUMERIC NOT NULL,
  mark_px       NUMERIC NOT NULL,
  bias          NUMERIC NOT NULL DEFAULT 0,
  tp_pct        NUMERIC NOT NULL,
  sl_pct        NUMERIC NOT NULL,
  opened_at     TIMESTAMPTZ NOT NULL,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS positions_user_client_idx ON positions (user_id, client_id);

-- ─── equity ──────────────────────────────────────────────────────────────────
-- Time series of total account equity. Heavy-write table; we downsample
-- on read rather than on write (keeping every tick for max fidelity, then
-- the frontend picks every Nth point to render).
CREATE TABLE IF NOT EXISTS equity (
  id            BIGSERIAL PRIMARY KEY,
  user_id       UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
  t             BIGINT NOT NULL,            -- monotonic tick counter
  v             NUMERIC NOT NULL,           -- USD value
  recorded_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS equity_user_t_idx ON equity (user_id, t DESC);

-- ─── settings ────────────────────────────────────────────────────────────────
-- Singleton (per user) row holding strategy params + cash + start balance.
-- Upsert pattern: frontend always writes user_id = DEFAULT and uses
-- ON CONFLICT to update.
CREATE TABLE IF NOT EXISTS settings (
  user_id          UUID PRIMARY KEY DEFAULT '00000000-0000-0000-0000-000000000001',
  start_balance    NUMERIC NOT NULL DEFAULT 500,
  cash             NUMERIC NOT NULL DEFAULT 500,
  params           JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ─── Row Level Security ──────────────────────────────────────────────────────
-- For now, single-user with a fixed UUID. RLS is on so that when you
-- later add Supabase Auth, you only need to flip the policy from
-- "allow the default UUID" to "allow auth.uid()".

ALTER TABLE trades    ENABLE ROW LEVEL SECURITY;
ALTER TABLE positions ENABLE ROW LEVEL SECURITY;
ALTER TABLE equity    ENABLE ROW LEVEL SECURITY;
ALTER TABLE settings  ENABLE ROW LEVEL SECURITY;

-- Drop existing policies if re-running
DROP POLICY IF EXISTS "default user full access trades" ON trades;
DROP POLICY IF EXISTS "default user full access positions" ON positions;
DROP POLICY IF EXISTS "default user full access equity" ON equity;
DROP POLICY IF EXISTS "default user full access settings" ON settings;

-- Single-user policies. Lets the anon key do everything as the default user.
-- WHEN you add auth, replace 'using (true)' with 'using (auth.uid() = user_id)'.
CREATE POLICY "default user full access trades"    ON trades    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "default user full access positions" ON positions FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "default user full access equity"    ON equity    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "default user full access settings"  ON settings  FOR ALL USING (true) WITH CHECK (true);

-- ─── Helper: prune old equity points ─────────────────────────────────────────
-- Optional. Run periodically (manually or via pg_cron) to keep equity
-- table from growing unbounded. Keeps every point from last 24h, then
-- every 10th point older than that.
CREATE OR REPLACE FUNCTION prune_equity_for_user(target_user UUID, keep_recent_hours INT DEFAULT 24)
RETURNS INT AS $$
DECLARE
  deleted_count INT;
BEGIN
  WITH ranked AS (
    SELECT id, ROW_NUMBER() OVER (ORDER BY t) AS rn
    FROM equity
    WHERE user_id = target_user
      AND recorded_at < NOW() - (keep_recent_hours || ' hours')::INTERVAL
  )
  DELETE FROM equity
  WHERE id IN (SELECT id FROM ranked WHERE rn % 10 != 0);
  GET DIAGNOSTICS deleted_count = ROW_COUNT;
  RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- ═══════════════════════════════════════════════════════════════════════════
-- Observability tables added later: signals, trending observations, logs.
-- ═══════════════════════════════════════════════════════════════════════════
-- These are for "what did we see?" replay and analysis. Trades/positions
-- record what the engine DID; these record what the engine SAW. Together
-- they let you ask "why did the bot do/not do X at time Y?"

-- ─── signals ─────────────────────────────────────────────────────────────────
-- Every pool detection that passed the audit gate. Phase 1 INSERT happens
-- when the pool is detected; phase 2 UPDATE happens when 30s enrichment
-- completes. We dedup on (user_id, chain, pool_address) so the audit
-- doesn't fire twice for the same pool.
--
-- Source field tags WHERE the signal came from: "scanner" for on-chain
-- new-pool detection, "dexscreener-trending" for the recent-launch feed.
CREATE TABLE IF NOT EXISTS signals (
  id                BIGSERIAL PRIMARY KEY,
  user_id           UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
  source            TEXT NOT NULL,             -- 'scanner' | 'dexscreener-trending'
  chain             TEXT NOT NULL,
  dex               TEXT,
  pool_address      TEXT NOT NULL,
  token_address     TEXT,
  quote_address     TEXT,
  symbol            TEXT,
  -- Phase 1 (audit)
  audit_passed      BOOLEAN,
  audit_score       INT,
  audit_reason      TEXT,
  -- Phase 2 (enrichment); null until ~30s after detection
  initial_liq_usd   NUMERIC,
  final_liq_usd     NUMERIC,
  buy_count         INT,
  sell_count        INT,
  unique_buyers     INT,
  price_change_pct  NUMERIC,
  final_price_usd   NUMERIC,
  breakout_triggered BOOLEAN,
  breakout_score    INT,
  breakout_reason   TEXT,
  enrich_error      TEXT,
  -- Whether the engine acted on this signal (opened a position)
  acted_on          BOOLEAN NOT NULL DEFAULT FALSE,
  acted_position_id BIGINT,                    -- joins to trades.client_id if acted
  detected_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  enriched_at       TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS signals_user_detected_idx ON signals (user_id, detected_at DESC);
CREATE INDEX IF NOT EXISTS signals_source_idx ON signals (user_id, source, detected_at DESC);
-- Dedup index: same pool isn't recorded twice
CREATE UNIQUE INDEX IF NOT EXISTS signals_user_chain_pool_idx
  ON signals (user_id, chain, pool_address);

-- ─── trending_observations ───────────────────────────────────────────────────
-- Every DEXScreener trending poll's new tokens. We dedup write-side at
-- the (user, chain, address, hour) granularity — same token can be recorded
-- in multiple hours but not within the same hour. This keeps the table
-- bounded while preserving "when did this token start/stop trending"
-- analysis windows.
CREATE TABLE IF NOT EXISTS trending_observations (
  id                BIGSERIAL PRIMARY KEY,
  user_id           UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
  chain             TEXT NOT NULL,
  address           TEXT NOT NULL,
  symbol            TEXT,
  name              TEXT,
  price_usd         NUMERIC,
  liq_usd           NUMERIC,
  volume_24h        NUMERIC,
  price_change_24h  NUMERIC,
  price_change_h1   NUMERIC,
  market_cap        NUMERIC,
  pair_address      TEXT,
  pair_url          TEXT,
  pair_created_at   TIMESTAMPTZ,
  boost_amount      INT,
  observed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  observation_hour  TIMESTAMPTZ NOT NULL DEFAULT date_trunc('hour', NOW())
);
CREATE INDEX IF NOT EXISTS trending_obs_user_observed_idx ON trending_observations (user_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS trending_obs_chain_addr_idx ON trending_observations (user_id, chain, address, observed_at DESC);
-- Dedup: one observation per token per hour
CREATE UNIQUE INDEX IF NOT EXISTS trending_obs_dedup_idx
  ON trending_observations (user_id, chain, address, observation_hour);

-- ─── system_logs ─────────────────────────────────────────────────────────────
-- Diagnostic context. Auto-prunes via the helper below (or pg_cron if
-- you set it up). Default retention: 7 days. Long enough to debug weekend
-- behavior on Monday, short enough that storage stays small.
CREATE TABLE IF NOT EXISTS system_logs (
  id            BIGSERIAL PRIMARY KEY,
  user_id       UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
  level         TEXT NOT NULL,          -- 'info' | 'warn' | 'error'
  category      TEXT NOT NULL,          -- 'engine' | 'scanner' | 'audit' | 'dexscreener' | 'sys'
  message       TEXT NOT NULL,
  recorded_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS sys_logs_user_recorded_idx ON system_logs (user_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS sys_logs_category_idx ON system_logs (user_id, category, recorded_at DESC);

-- Helper: prune old system logs. Call manually or via pg_cron.
CREATE OR REPLACE FUNCTION prune_system_logs(target_user UUID, keep_days INT DEFAULT 7)
RETURNS INT AS $$
DECLARE
  deleted_count INT;
BEGIN
  DELETE FROM system_logs
  WHERE user_id = target_user
    AND recorded_at < NOW() - (keep_days || ' days')::INTERVAL;
  GET DIAGNOSTICS deleted_count = ROW_COUNT;
  RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- ─── RLS policies for new tables ─────────────────────────────────────────────
ALTER TABLE signals               ENABLE ROW LEVEL SECURITY;
ALTER TABLE trending_observations ENABLE ROW LEVEL SECURITY;
ALTER TABLE system_logs           ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "default user full access signals" ON signals;
DROP POLICY IF EXISTS "default user full access trending_observations" ON trending_observations;
DROP POLICY IF EXISTS "default user full access system_logs" ON system_logs;

CREATE POLICY "default user full access signals"
  ON signals FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "default user full access trending_observations"
  ON trending_observations FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "default user full access system_logs"
  ON system_logs FOR ALL USING (true) WITH CHECK (true);

-- ─── Positions: add pool_address + quote_address for live price oracle ──────
-- These columns are needed so engine restart can resume mark-to-market on
-- live AMM prices. Safe migration: ADD COLUMN IF NOT EXISTS is idempotent.
ALTER TABLE positions ADD COLUMN IF NOT EXISTS pool_address TEXT;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS quote_address TEXT;

-- ─── Positions: add dex hint so the price oracle knows which AMM to read ────
-- "pumpfun" → bonding-curve PDA, "raydium" → V4 pool vaults, "uniswap-v2" → V2 reserves.
ALTER TABLE positions ADD COLUMN IF NOT EXISTS dex TEXT;
