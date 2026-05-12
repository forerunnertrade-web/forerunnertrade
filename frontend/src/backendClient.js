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
