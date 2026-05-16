// ─────────────────────────────────────────────────────────────────────────────
// Backend engine API client.
//
// Talks HTTP to the FastAPI server. The WebSocket client (for /events) is
// separate; that handles the live pool detection feed. This module is for
// the engine control surface — /state, /params, /control.
//
// All calls authenticate via VITE_DASHBOARD_TOKEN if set. If unset, no
// Authorization header is sent (matches backend behavior — auth disabled
// in local dev).
// ─────────────────────────────────────────────────────────────────────────────

const TOKEN = import.meta.env.VITE_DASHBOARD_TOKEN || "";

/** Derive the HTTP base URL from the WebSocket URL env var. */
function backendHttpUrl() {
  const ws = import.meta.env.VITE_BACKEND_WS_URL || "ws://localhost:8080/events";
  return ws
    .replace(/^wss:/, "https:")
    .replace(/^ws:/, "http:")
    .replace(/\/events$/, "");
}

function headers() {
  const h = { "Content-Type": "application/json" };
  if (TOKEN) h.Authorization = `Bearer ${TOKEN}`;
  return h;
}

async function safeFetch(path, init) {
  try {
    const resp = await fetch(backendHttpUrl() + path, init);
    if (!resp.ok) {
      return { ok: false, status: resp.status, error: await resp.text().catch(() => "") };
    }
    return { ok: true, data: await resp.json() };
  } catch (err) {
    return { ok: false, status: 0, error: String(err) };
  }
}

/** Poll engine state. Returns the snapshot or null on failure. */
export async function fetchState() {
  const r = await safeFetch("/state", { headers: headers() });
  return r.ok ? r.data : null;
}

/** Send strategy param updates. Partial objects are fine; backend merges. */
export async function updateParams(partial) {
  return safeFetch("/params", {
    method: "POST",
    headers: headers(),
    body: JSON.stringify(partial),
  });
}

/** action: "start" | "stop" | "reset" */
export async function controlEngine(action) {
  return safeFetch("/control", {
    method: "POST",
    headers: headers(),
    body: JSON.stringify({ action }),
  });
}

/** Token to append to WebSocket URL for auth. Empty string if not set. */
export function wsAuthSuffix() {
  return TOKEN ? `?token=${encodeURIComponent(TOKEN)}` : "";
}

// ─────────────────────────────────────────────────────────────────────────────
// Observability reads — for restoring the dashboard on browser refresh.
// All three return arrays of plain row objects (snake_case from the DB);
// the caller is responsible for mapping into the in-memory frontend shape.
// Each returns [] on any error or when Supabase isn't configured (the
// backend handles that with its `configured: false` flag).
// ─────────────────────────────────────────────────────────────────────────────

export async function fetchRecentSignals(limit = 50) {
  const r = await safeFetch(`/signals?limit=${limit}`, { headers: headers() });
  if (!r.ok || !r.data) return [];
  return r.data.signals || [];
}

export async function fetchRecentTrending(limit = 50) {
  const r = await safeFetch(`/trending-history?limit=${limit}`, { headers: headers() });
  if (!r.ok || !r.data) return [];
  return r.data.observations || [];
}

export async function fetchRecentLogs(limit = 100, category = "") {
  const qs = category
    ? `?limit=${limit}&category=${encodeURIComponent(category)}`
    : `?limit=${limit}`;
  const r = await safeFetch(`/logs${qs}`, { headers: headers() });
  if (!r.ok || !r.data) return [];
  return r.data.logs || [];
}
