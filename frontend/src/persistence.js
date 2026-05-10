// ─────────────────────────────────────────────────────────────────────────────
// Supabase persistence layer.
//
// All functions return null when Supabase isn't configured (env vars missing)
// so the caller can fall back to localStorage. This means the app works
// identically with or without Supabase — the only difference is whether
// state survives across browsers/devices.
//
// Configuration:
//   VITE_SUPABASE_URL     — your project's https URL
//   VITE_SUPABASE_ANON_KEY — the public anon key (RLS protects the data)
//
// Both are read at build time by Vite. Restart `npm run dev` after
// changing .env. NEVER commit a service_role key — anon key is the right
// one for browser code.
// ─────────────────────────────────────────────────────────────────────────────

import { createClient } from "@supabase/supabase-js";

const URL = import.meta.env.VITE_SUPABASE_URL;
const KEY = import.meta.env.VITE_SUPABASE_ANON_KEY;

export const isConfigured = Boolean(URL && KEY);

// Single shared client. Lazy-init so unconfigured deployments don't even
// try to construct one (which would otherwise log a warning).
let _client = null;
function client() {
  if (!isConfigured) return null;
  if (_client === null) {
    _client = createClient(URL, KEY, {
      auth: { persistSession: false },  // single-user mode, no sessions yet
    });
  }
  return _client;
}

// All rows belong to this fixed UUID until we wire Supabase Auth. Matches
// the DEFAULT in schema.sql.
const DEFAULT_USER_ID = "00000000-0000-0000-0000-000000000001";

// ─────────────────────────────────────────────────────────────────────────────
// Trades — append-only history
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Insert one trade. Idempotent on (user_id, client_id) — calling twice
 * with the same client_id is a no-op. Returns the inserted row, or null
 * if Supabase is unconfigured / the call failed.
 */
export async function insertTrade(trade) {
  const c = client();
  if (!c) return null;
  try {
    const row = {
      user_id: DEFAULT_USER_ID,
      client_id: trade.id,
      chain: trade.chain,
      symbol: trade.symbol,
      address: trade.address || null,
      qty: trade.qty,
      entry_px: trade.entryPx,
      exit_px: trade.exitPx,
      pnl_usd: trade.pnlUsd,
      pnl_pct: trade.pnlPct,
      reason: trade.reason,
      opened_at: new Date(trade.openedAt).toISOString(),
      closed_at: new Date(trade.closedAt).toISOString(),
    };
    const { data, error } = await c
      .from("trades")
      .upsert(row, { onConflict: "user_id,client_id", ignoreDuplicates: true })
      .select()
      .maybeSingle();
    if (error) {
      console.warn("supabase insertTrade failed:", error.message);
      return null;
    }
    return data;
  } catch (err) {
    console.warn("supabase insertTrade threw:", err);
    return null;
  }
}

/**
 * Fetch the most recent N trades, newest first.
 * Returns [] on error / unconfigured.
 */
export async function fetchTrades(limit = 200) {
  const c = client();
  if (!c) return null;  // null signals "Supabase not used", caller falls back
  try {
    const { data, error } = await c
      .from("trades")
      .select("*")
      .eq("user_id", DEFAULT_USER_ID)
      .order("closed_at", { ascending: false })
      .limit(limit);
    if (error) {
      console.warn("supabase fetchTrades failed:", error.message);
      return null;
    }
    // Reshape DB columns back into the frontend's camelCase trade shape.
    return data.map((r) => ({
      id: Number(r.client_id),
      chain: r.chain,
      symbol: r.symbol,
      address: r.address,
      qty: Number(r.qty),
      entryPx: Number(r.entry_px),
      exitPx: Number(r.exit_px),
      pnlUsd: Number(r.pnl_usd),
      pnlPct: Number(r.pnl_pct),
      reason: r.reason,
      openedAt: new Date(r.opened_at).getTime(),
      closedAt: new Date(r.closed_at).getTime(),
    }));
  } catch (err) {
    console.warn("supabase fetchTrades threw:", err);
    return null;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Positions — current open state. Frontend replaces the whole set on save.
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Replace the open-positions set with the given list. Deletes anything
 * not present, upserts everything else. Two SQL ops total regardless of
 * position count.
 */
export async function syncPositions(positions) {
  const c = client();
  if (!c) return null;
  try {
    // Delete positions not in the current set
    const keepIds = positions.map((p) => p.id);
    if (keepIds.length === 0) {
      const { error: delErr } = await c
        .from("positions")
        .delete()
        .eq("user_id", DEFAULT_USER_ID);
      if (delErr) console.warn("supabase positions delete failed:", delErr.message);
    } else {
      const { error: delErr } = await c
        .from("positions")
        .delete()
        .eq("user_id", DEFAULT_USER_ID)
        .not("client_id", "in", `(${keepIds.join(",")})`);
      if (delErr) console.warn("supabase positions delete failed:", delErr.message);
    }

    // Upsert current open set
    if (positions.length > 0) {
      const rows = positions.map((p) => ({
        user_id: DEFAULT_USER_ID,
        client_id: p.id,
        chain: p.chain,
        symbol: p.symbol,
        address: p.address || null,
        qty: p.qty,
        entry_px: p.entryPx,
        mark_px: p.markPx,
        bias: p.bias || 0,
        tp_pct: p.tpPct,
        sl_pct: p.slPct,
        opened_at: new Date(p.openedAt).toISOString(),
        updated_at: new Date().toISOString(),
      }));
      const { error: upErr } = await c
        .from("positions")
        .upsert(rows, { onConflict: "user_id,client_id" });
      if (upErr) console.warn("supabase positions upsert failed:", upErr.message);
    }
    return positions.length;
  } catch (err) {
    console.warn("supabase syncPositions threw:", err);
    return null;
  }
}

export async function fetchPositions() {
  const c = client();
  if (!c) return null;
  try {
    const { data, error } = await c
      .from("positions")
      .select("*")
      .eq("user_id", DEFAULT_USER_ID)
      .order("opened_at", { ascending: false });
    if (error) {
      console.warn("supabase fetchPositions failed:", error.message);
      return null;
    }
    return data.map((r) => ({
      id: Number(r.client_id),
      chain: r.chain,
      symbol: r.symbol,
      address: r.address,
      qty: Number(r.qty),
      entryPx: Number(r.entry_px),
      markPx: Number(r.mark_px),
      bias: Number(r.bias),
      tpPct: Number(r.tp_pct),
      slPct: Number(r.sl_pct),
      openedAt: new Date(r.opened_at).getTime(),
      stale: true,  // restored = needs fresh mark
    }));
  } catch (err) {
    console.warn("supabase fetchPositions threw:", err);
    return null;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Equity — append-only time series. Throttled writes to avoid hammering DB.
// ─────────────────────────────────────────────────────────────────────────────

let _lastEquityWriteAt = 0;
const EQUITY_WRITE_INTERVAL_MS = 5000;  // write at most every 5s

/**
 * Append a single equity point IF enough time has passed since the last
 * write. This is the throttle that keeps a 700ms tick loop from spamming
 * Supabase with ~5000 inserts per hour.
 */
export async function appendEquityPoint(t, v) {
  const c = client();
  if (!c) return null;
  const now = Date.now();
  if (now - _lastEquityWriteAt < EQUITY_WRITE_INTERVAL_MS) return null;
  _lastEquityWriteAt = now;
  try {
    const { error } = await c.from("equity").insert({
      user_id: DEFAULT_USER_ID,
      t,
      v,
    });
    if (error) {
      console.warn("supabase appendEquityPoint failed:", error.message);
      return null;
    }
    return true;
  } catch (err) {
    console.warn("supabase appendEquityPoint threw:", err);
    return null;
  }
}

export async function fetchEquity(limit = 500) {
  const c = client();
  if (!c) return null;
  try {
    const { data, error } = await c
      .from("equity")
      .select("t,v")
      .eq("user_id", DEFAULT_USER_ID)
      .order("t", { ascending: false })
      .limit(limit);
    if (error) {
      console.warn("supabase fetchEquity failed:", error.message);
      return null;
    }
    // Reverse so oldest-first for the line chart
    return data.reverse().map((r) => ({ t: Number(r.t), v: Number(r.v) }));
  } catch (err) {
    console.warn("supabase fetchEquity threw:", err);
    return null;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Settings — single row holding params + cash + start_balance
// ─────────────────────────────────────────────────────────────────────────────

export async function saveSettings({ startBalance, cash, params }) {
  const c = client();
  if (!c) return null;
  try {
    const { error } = await c.from("settings").upsert({
      user_id: DEFAULT_USER_ID,
      start_balance: startBalance,
      cash,
      params,
      updated_at: new Date().toISOString(),
    });
    if (error) {
      console.warn("supabase saveSettings failed:", error.message);
      return null;
    }
    return true;
  } catch (err) {
    console.warn("supabase saveSettings threw:", err);
    return null;
  }
}

export async function fetchSettings() {
  const c = client();
  if (!c) return null;
  try {
    const { data, error } = await c
      .from("settings")
      .select("*")
      .eq("user_id", DEFAULT_USER_ID)
      .maybeSingle();
    if (error) {
      console.warn("supabase fetchSettings failed:", error.message);
      return null;
    }
    if (!data) return null;
    return {
      startBalance: Number(data.start_balance),
      cash: Number(data.cash),
      params: data.params || {},
    };
  } catch (err) {
    console.warn("supabase fetchSettings threw:", err);
    return null;
  }
}

/**
 * Wipe all data for the default user. Used by the "reset" button.
 * Returns true if all four deletes succeeded (or Supabase is unconfigured).
 */
export async function clearAll() {
  const c = client();
  if (!c) return true;
  try {
    await c.from("equity").delete().eq("user_id", DEFAULT_USER_ID);
    await c.from("positions").delete().eq("user_id", DEFAULT_USER_ID);
    await c.from("trades").delete().eq("user_id", DEFAULT_USER_ID);
    // Settings: reset to defaults instead of deleting
    await c.from("settings").upsert({
      user_id: DEFAULT_USER_ID,
      start_balance: 500,
      cash: 500,
      params: {},
      updated_at: new Date().toISOString(),
    });
    return true;
  } catch (err) {
    console.warn("supabase clearAll threw:", err);
    return false;
  }
}
