import React, { useState, useEffect, useRef, useMemo } from "react";
import * as persistence from "./persistence";
import * as backendClient from "./backendClient";
import {
  LineChart,
  Line,
  ResponsiveContainer,
  YAxis,
  XAxis,
  Tooltip,
  ReferenceLine,
} from "recharts";
import {
  Play,
  Square,
  RotateCcw,
  Activity,
  Settings2,
  Radio,
  Clock,
  Zap,
  Flame,
} from "lucide-react";

// ─────────────────────────────────────────────────────────────────────────────
// Pure helpers
// ─────────────────────────────────────────────────────────────────────────────
const fmt = (n, d = 2) =>
  Number(n).toLocaleString("en-US", {
    minimumFractionDigits: d,
    maximumFractionDigits: d,
  });
const fmtUsd = (n) => `$${fmt(n, 2)}`;
const fmtUsdK = (n) =>
  n >= 1_000_000 ? `$${(n / 1_000_000).toFixed(1)}M`
  : n >= 1_000 ? `$${(n / 1_000).toFixed(1)}k`
  : `$${n.toFixed(0)}`;
const pct = (n) => `${n >= 0 ? "+" : ""}${fmt(n, 2)}%`;
const shortAddr = (a) => (a && a.length > 12 ? `${a.slice(0, 6)}…${a.slice(-4)}` : a);

const CHAINS = ["ETH", "SOL", "SUI"];
const DEXES = {
  ETH: ["uniswap_v2", "uniswap_v3"],
  SOL: ["raydium", "orca"],
  SUI: ["cetus", "turbos"],
};

const SYMBOL_POOL = [
  "PEPU", "WAGMI", "MOON", "DEGEN", "BONK2", "FLOKI3", "GIGA",
  "ANON", "VIBE", "RUGZ", "APE2", "MEME", "NORM", "SNAKE", "FROG",
];

const rand = (a, b) => a + Math.random() * (b - a);
const choice = (arr) => arr[Math.floor(Math.random() * arr.length)];
const fakeAddr = () =>
  "0x" +
  Array.from({ length: 40 }, () =>
    "0123456789abcdef"[Math.floor(Math.random() * 16)]
  ).join("");

let _sigId = 0;
let _liveSigId = 1_000_000;

/**
 * Synthetic signal generator.
 *
 * To make synthetic mode useful for filter calibration, we generate signals
 * with a realistic distribution: ~30% organic (high buyers, modest pump),
 * ~40% bot-launches (few buyers, sharp pump-then-flat), ~30% duds (low
 * liquidity, no buyers). The strategy filter should learn to find the 30%
 * organic launches.
 *
 * `enabledChains` is an array like ["ETH", "SOL"]. Signals are only generated
 * for chains in that list. Returns null if no chains are enabled.
 */
function makeSyntheticSignal(enabledChains = ["ETH", "SOL", "SUI"]) {
  if (enabledChains.length === 0) return null;
  const chain = choice(enabledChains);
  const archetype = Math.random();
  let liqUsd, buyCount, sellCount, uniqueBuyers, priceChange, auditScore;

  if (archetype < 0.3) {
    // Organic launch
    liqUsd = rand(8000, 50000);
    buyCount = Math.floor(rand(8, 25));
    sellCount = Math.floor(rand(0, 4));
    uniqueBuyers = Math.floor(rand(7, buyCount));
    priceChange = rand(5, 40);
    auditScore = Math.floor(rand(70, 100));
  } else if (archetype < 0.7) {
    // Bot launch — looks pumpy but unique buyers tells the truth
    liqUsd = rand(2000, 20000);
    buyCount = Math.floor(rand(15, 50));
    sellCount = Math.floor(rand(2, 10));
    uniqueBuyers = Math.floor(rand(1, 4));
    priceChange = rand(15, 80);
    auditScore = Math.floor(rand(40, 75));
  } else {
    // Dud
    liqUsd = rand(500, 4000);
    buyCount = Math.floor(rand(0, 4));
    sellCount = Math.floor(rand(0, 2));
    uniqueBuyers = Math.floor(rand(0, 3));
    priceChange = rand(-15, 5);
    auditScore = Math.floor(rand(30, 65));
  }

  // Synthesise a 10-point price series matching the archetype.
  // Organic launches show smooth uptrend, bots show pump-then-flat, duds wander.
  const priceBase = rand(0.0001, 5);
  const samples = [];
  let lastP = priceBase;
  for (let i = 0; i < 10; i++) {
    let drift;
    if (archetype < 0.3) {
      // organic — steady up
      drift = rand(0.005, 0.025);
    } else if (archetype < 0.7) {
      // bot pump — sharp first half, flat after
      drift = i < 5 ? rand(0.02, 0.05) : rand(-0.005, 0.005);
    } else {
      // dud — drift sideways/down
      drift = rand(-0.015, 0.005);
    }
    lastP = lastP * (1 + drift);
    samples.push(lastP);
  }
  // Recompute priceChange from the actual generated series for consistency
  priceChange = ((samples[samples.length - 1] - samples[0]) / samples[0]) * 100;

  // Breakout heuristic on synthetic series — same logic as the backend
  // detector, simplified: true if last sample > prior max by >5% AND
  // second-half avg > first-half avg by >3%.
  const priorMax = Math.max(...samples.slice(0, -1));
  const last = samples[samples.length - 1];
  const breakoutPct = ((last - priorMax) / priorMax) * 100;
  const firstHalfAvg = samples.slice(0, 5).reduce((s, x) => s + x, 0) / 5;
  const lastHalfAvg = samples.slice(5).reduce((s, x) => s + x, 0) / 5;
  const momentumPct = ((lastHalfAvg - firstHalfAvg) / firstHalfAvg) * 100;
  const breakoutTriggered = breakoutPct >= 5 && momentumPct >= 3;
  const breakoutScore = breakoutTriggered
    ? Math.min(100, 50 + Math.floor(breakoutPct * 2 + momentumPct))
    : Math.max(0, Math.floor(breakoutPct + momentumPct));

  return {
    id: ++_sigId,
    chain,
    dex: choice(DEXES[chain]),
    symbol: choice(SYMBOL_POOL) + Math.floor(rand(0, 999)),
    address: fakeAddr(),
    poolAddress: fakeAddr(),
    price: samples[samples.length - 1],
    finalPriceUsd: samples[samples.length - 1],
    priceSamples: samples,
    breakoutTriggered,
    breakoutScore,
    breakoutReason: breakoutTriggered
      ? `breakout +${breakoutPct.toFixed(1)}% momentum +${momentumPct.toFixed(1)}%`
      : "no breakout",
    ts: Date.now(),
    status: "ready",
    liqUsd,
    buyCount,
    sellCount,
    uniqueBuyers,
    priceChange,
    auditScore,
    auditReason: `score=${auditScore}`,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// UI primitives
// ─────────────────────────────────────────────────────────────────────────────
const Panel = ({ title, right, children, className = "" }) => (
  <div className={`border border-zinc-800 bg-black ${className}`}>
    {title && (
      <div className="flex items-center justify-between border-b border-zinc-800 px-3 py-2">
        <div className="text-[10px] uppercase tracking-[0.2em] text-zinc-500">{title}</div>
        <div className="text-[10px] text-zinc-500">{right}</div>
      </div>
    )}
    <div>{children}</div>
  </div>
);

const Stat = ({ label, value, tone = "default" }) => {
  const toneClass =
    tone === "up" ? "text-emerald-400"
    : tone === "down" ? "text-red-400"
    : tone === "amber" ? "text-amber-400"
    : "text-zinc-100";
  return (
    <div className="flex flex-col gap-1 border-r border-zinc-800 px-4 py-3 last:border-r-0">
      <div className="text-[9px] uppercase tracking-[0.2em] text-zinc-500">{label}</div>
      <div className={`text-base tabular-nums ${toneClass}`}>{value}</div>
    </div>
  );
};

const PulseDot = ({ on }) => (
  <span className="relative inline-flex h-2 w-2">
    {on && <span className="absolute inline-flex h-full w-full animate-ping bg-emerald-400 opacity-60" />}
    <span className={`relative inline-flex h-2 w-2 ${on ? "bg-emerald-400" : "bg-zinc-600"}`} />
  </span>
);

/**
 * Inline price sparkline. Shows the price series collected during the
 * 30s enrichment window. Color-coded: green for breakout-triggered,
 * zinc otherwise. Renders as SVG (cheap; no recharts overhead per row).
 */
const Sparkline = ({ samples, triggered, width = 56, height = 16 }) => {
  if (!samples || samples.length < 2) {
    return <span className="text-zinc-700 text-[9px]">—</span>;
  }
  const min = Math.min(...samples);
  const max = Math.max(...samples);
  const range = max - min || 1;
  const stepX = width / (samples.length - 1);
  const points = samples
    .map((v, i) => {
      const x = i * stepX;
      const y = height - ((v - min) / range) * height;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
  const stroke = triggered ? "#10b981" : samples[samples.length - 1] >= samples[0] ? "#71717a" : "#dc2626";
  return (
    <svg width={width} height={height} className="inline-block align-middle">
      <polyline
        fill="none"
        stroke={stroke}
        strokeWidth="1"
        strokeLinecap="round"
        strokeLinejoin="round"
        points={points}
      />
    </svg>
  );
};

// ─────────────────────────────────────────────────────────────────────────────
// Strategy filter — operates on launch metrics, not RSI proxies.
//
// Returns null if signal isn't ready yet (still being enriched), otherwise
// returns true/false. Pending signals are skipped without consuming a slot.
//
// Null metrics: SOL/SUI signals come back "ready" but with null buy/liq/price
// fields because their enrichers aren't implemented yet. We treat null as
// "unknown" — it passes a threshold of 0 (caller doesn't care) but fails any
// positive threshold (caller actively filters on it). This makes audit-only
// trading possible by setting min_buyers / min_liq / min_price_change to 0.
// ─────────────────────────────────────────────────────────────────────────────
function strategyPasses(sig, params) {
  if (sig.status !== "ready") return null;
  if (sig.auditScore < params.minAuditScore) return false;

  if (params.minLiqUsd > 0 && (sig.liqUsd == null || sig.liqUsd < params.minLiqUsd)) return false;
  if (params.minBuyers > 0 && (sig.uniqueBuyers == null || sig.uniqueBuyers < params.minBuyers)) return false;
  if (params.minPriceChange > 0 && (sig.priceChange == null || sig.priceChange < params.minPriceChange)) return false;

  // Breakout filter (optional). If on, the signal must have triggered the
  // local breakout detector. Audit-only signals (no priceSamples) fail this
  // check, which is the conservative behavior.
  if (params.requireBreakout && !sig.breakoutTriggered) return false;

  return true;
}

// ─────────────────────────────────────────────────────────────────────────────
// Main app
// ─────────────────────────────────────────────────────────────────────────────
export default function App() {
  const [running, setRunning] = useState(false);

  // ─── Persisted preferences (load once, save on change) ──────────────────
  // Lazy-init form ensures localStorage is only touched on mount, not on
  // every re-render. All loads are wrapped in try/catch — corrupted JSON
  // or disabled storage falls back to defaults rather than crashing.
  const [params, setParams] = useState(() => {
    const defaults = {
      minLiqUsd: 5000,
      minBuyers: 7,
      minPriceChange: 5,
      minAuditScore: 70,
      requireBreakout: false,
      positionSizeUsd: 50,
      takeProfitPct: 25,
      stopLossPct: 10,
      maxConcurrent: 5,
    };
    try {
      const saved = localStorage.getItem("forerunner.params");
      if (saved) return { ...defaults, ...JSON.parse(saved) };
    } catch (_) { /* fall through */ }
    return defaults;
  });

  const [startBalance, setStartBalance] = useState(() => {
    try {
      const v = parseFloat(localStorage.getItem("forerunner.startBalance"));
      return isFinite(v) && v > 0 ? v : 500;
    } catch (_) { return 500; }
  });

  // Cash and positions persist together — they're a coupled pair.
  // Cash without positions would mis-state account state on refresh.
  const [cash, setCash] = useState(() => {
    try {
      const v = parseFloat(localStorage.getItem("forerunner.cash"));
      return isFinite(v) && v >= 0 ? v : 500;
    } catch (_) { return 500; }
  });

  const [positions, setPositions] = useState(() => {
    try {
      const saved = localStorage.getItem("forerunner.positions");
      const arr = saved ? JSON.parse(saved) : [];
      // Mark all restored positions as STALE — their mark prices haven't
      // updated since last save. They'll refresh on the next tick once
      // the bot is started. This is honest: refusing to persist them at
      // all hides session continuity; persisting silently would imply
      // their P&L is current.
      return arr.map((p) => ({ ...p, stale: true }));
    } catch (_) { return []; }
  });

  const [trades, setTrades] = useState(() => {
    try {
      const saved = localStorage.getItem("forerunner.trades");
      if (saved) return JSON.parse(saved);
    } catch (_) { /* fall through */ }
    return [];
  });

  // Equity history is downsampled before save (see useEffect below).
  // Up to 500 points kept on disk — at 700ms ticks that's ~6 minutes raw,
  // or hours after downsampling. Live mode (sparse ticks) easily covers
  // multi-day sessions.
  const [equity, setEquity] = useState(() => {
    try {
      const saved = localStorage.getItem("forerunner.equity");
      const arr = saved ? JSON.parse(saved) : null;
      if (Array.isArray(arr) && arr.length > 0) return arr;
    } catch (_) { /* fall through */ }
    return [{ t: 0, v: 500 }];
  });

  const [tickCount, setTickCount] = useState(0);

  // ─── Save on change ──────────────────────────────────────────────────────
  // setTimeout(0) defers writes off the render path. localStorage is
  // synchronous so this matters once arrays grow.
  useEffect(() => {
    const id = setTimeout(() => {
      try { localStorage.setItem("forerunner.params", JSON.stringify(params)); }
      catch (_) {}
    }, 0);
    return () => clearTimeout(id);
  }, [params]);

  useEffect(() => {
    const id = setTimeout(() => {
      try { localStorage.setItem("forerunner.startBalance", String(startBalance)); }
      catch (_) {}
    }, 0);
    return () => clearTimeout(id);
  }, [startBalance]);

  useEffect(() => {
    const id = setTimeout(() => {
      try { localStorage.setItem("forerunner.cash", String(cash)); }
      catch (_) {}
    }, 0);
    return () => clearTimeout(id);
  }, [cash]);

  useEffect(() => {
    const id = setTimeout(() => {
      try {
        // Strip the transient `stale` flag before persisting so it doesn't
        // ratchet (would survive across refreshes even after refresh).
        const clean = positions.map(({ stale: _s, ...rest }) => rest);
        localStorage.setItem("forerunner.positions", JSON.stringify(clean));
      } catch (_) {}
    }, 0);
    return () => clearTimeout(id);
  }, [positions]);

  useEffect(() => {
    const id = setTimeout(() => {
      try { localStorage.setItem("forerunner.trades", JSON.stringify(trades)); }
      catch (_) {}
    }, 0);
    return () => clearTimeout(id);
  }, [trades]);

  // Equity downsample-on-save: keep only every Nth point if length > 500.
  // This is lossless for short sessions, lossy but readable for long ones.
  useEffect(() => {
    const id = setTimeout(() => {
      try {
        let toSave = equity;
        if (toSave.length > 500) {
          const stride = Math.ceil(toSave.length / 500);
          toSave = toSave.filter((_, i) => i % stride === 0 || i === toSave.length - 1);
        }
        localStorage.setItem("forerunner.equity", JSON.stringify(toSave));
      } catch (_) {}
    }, 0);
    return () => clearTimeout(id);
  }, [equity]);

  // ─── Supabase hydration (one-time on mount) ──────────────────────────────
  // Runs after the localStorage lazy-init populated initial state. If
  // Supabase is configured and returns data, we override local state so
  // Supabase is authoritative. The hydration is silent if Supabase isn't
  // configured — the app continues to work with localStorage only.
  const [supabaseHydrated, setSupabaseHydrated] = useState(!persistence.isConfigured);
  useEffect(() => {
    if (!persistence.isConfigured) return;
    let cancelled = false;
    (async () => {
      try {
        const [s, t, p, e] = await Promise.all([
          persistence.fetchSettings(),
          persistence.fetchTrades(200),
          persistence.fetchPositions(),
          persistence.fetchEquity(500),
        ]);
        if (cancelled) return;
        if (s) {
          setStartBalance(s.startBalance);
          setCash(s.cash);
          if (s.params && Object.keys(s.params).length > 0) {
            setParams((prev) => ({ ...prev, ...s.params }));
          }
        }
        if (t) setTrades(t);
        if (p) setPositions(p);
        if (e && e.length > 0) setEquity(e);
      } catch (err) {
        console.warn("supabase hydration failed:", err);
      } finally {
        if (!cancelled) setSupabaseHydrated(true);
      }
    })();
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ─── Supabase write-through ──────────────────────────────────────────────
  // Mirrors localStorage saves to Supabase. Each effect waits for
  // Engine mode: "local" runs the React tick loop (legacy/synthetic-friendly);
  // "remote" hands control to the backend engine (runs 24/7 even with no
  // dashboard open). Default to local so existing users aren't surprised.
  //
  // Declared BEFORE the persistence write-through effects below so they
  // can gate on it — in remote mode the backend is the authoritative
  // Supabase writer, so client-side writes would create double-writes.
  const [engineMode, setEngineMode] = useState(() => {
    try { return localStorage.getItem("forerunner.engineMode") || "local"; }
    catch (_) { return "local"; }
  });
  useEffect(() => {
    try { localStorage.setItem("forerunner.engineMode", engineMode); } catch (_) {}
  }, [engineMode]);
  const [remoteEngineHealthy, setRemoteEngineHealthy] = useState(false);

  // ─── Supabase write-through (LOCAL ENGINE ONLY) ──────────────────────────
  // In remote mode the backend writes to Supabase directly via db.py. If we
  // also write from here, we'd double-write and potentially clobber backend
  // state (the frontend's view of "trades" is a downsampled snapshot, not
  // the source of truth in remote mode).
  //
  // Each effect waits for hydration so we don't overwrite cloud state with
  // stale defaults during the brief window between mount and hydration.
  useEffect(() => {
    if (engineMode === "remote") return;
    if (!supabaseHydrated || !persistence.isConfigured) return;
    persistence.saveSettings({ startBalance, cash, params });
  }, [engineMode, supabaseHydrated, startBalance, cash, params]);

  useEffect(() => {
    if (engineMode === "remote") return;
    if (!supabaseHydrated || !persistence.isConfigured) return;
    persistence.syncPositions(positions);
  }, [engineMode, supabaseHydrated, positions]);

  // Trades are append-only — write each new one individually to avoid
  // re-uploading the whole history on every change. A ref tracks IDs
  // we've already pushed.
  const pushedTradeIdsRef = useRef(new Set());
  useEffect(() => {
    if (engineMode === "remote") return;
    if (!supabaseHydrated || !persistence.isConfigured) return;
    trades.forEach((t) => {
      if (!pushedTradeIdsRef.current.has(t.id)) {
        pushedTradeIdsRef.current.add(t.id);
        persistence.insertTrade(t);
      }
    });
  }, [engineMode, supabaseHydrated, trades]);

  // Equity throttled inside persistence module (5s minimum interval)
  useEffect(() => {
    if (engineMode === "remote") return;
    if (!supabaseHydrated || !persistence.isConfigured) return;
    const last = equity[equity.length - 1];
    if (last) persistence.appendEquityPoint(last.t, last.v);
  }, [engineMode, supabaseHydrated, equity]);

  const [signals, setSignals] = useState([]);
  const [logs, setLogs] = useState([]);
  const [trending, setTrending] = useState([]);  // DEXScreener-sourced tokens

  const [signalSource, setSignalSource] = useState("synthetic");
  const [enabledChains, setEnabledChains] = useState(["ETH", "SOL", "SUI"]);
  const [wsStatus, setWsStatus] = useState("disconnected");
  const wsRef = useRef(null);
  const liveSignalQueueRef = useRef([]);

  const cashRef = useRef(cash);
  const positionsRef = useRef(positions);
  useEffect(() => { cashRef.current = cash; }, [cash]);
  useEffect(() => { positionsRef.current = positions; }, [positions]);

  const idCounter = useRef(0);
  const nextId = () => ++idCounter.current;

  const pushLog = (kind, msg) =>
    setLogs((prev) =>
      [{
        id: nextId(),
        ts: new Date().toLocaleTimeString("en-US", { hour12: false }),
        kind,
        msg,
      }, ...prev].slice(0, 80)
    );

  // Seed
  useEffect(() => {
    setSignals(Array.from({ length: 4 }, () => makeSyntheticSignal()));
    pushLog("sys", "paper-trading harness ready. press START to begin.");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ───────────────────────── WebSocket lifecycle ───────────────────────────
  useEffect(() => {
    if (signalSource !== "live") {
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
      setWsStatus("disconnected");
      return;
    }

    setWsStatus("connecting");
    // Backend URL: VITE_BACKEND_WS_URL takes precedence; falls back to
    // localhost for dev. Use wss:// in prod (browser blocks ws:// on https).
    // Auth token (if set) goes as a query param — browser WebSocket API
    // doesn't support custom headers.
    const baseWsUrl = import.meta.env.VITE_BACKEND_WS_URL || "ws://localhost:8080/events";
    const wsUrl = baseWsUrl + backendClient.wsAuthSuffix();
    pushLog("sys", `connecting to backend ${baseWsUrl}…`);
    const ws = new WebSocket(wsUrl);
    wsRef.current = ws;

    ws.onopen = () => {
      setWsStatus("connected");
      pushLog("sys", "✓ live feed connected");
    };

    ws.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data);

        // Phase 1: a new pool was detected. Show as PENDING — strategy
        // filter will skip it until metrics arrive.
        if (msg.type === "pool_event") {
          const ev = msg.event;
          const audit = msg.audit;
          const sig = {
            id: ++_liveSigId,
            chain: ev.chain.toUpperCase().slice(0, 3),
            dex: ev.dex,
            symbol: (ev.token0 || "").slice(2, 8).toUpperCase(),
            address: ev.token0,
            poolAddress: ev.pool_address,
            price: 0,
            ts: Date.now(),
            status: "pending",
            liqUsd: null,
            buyCount: null,
            sellCount: null,
            uniqueBuyers: null,
            priceChange: null,
            auditScore: audit.score,
            auditReason: audit.reason,
          };
          liveSignalQueueRef.current.push(sig);
        }

        // Phase 2: metrics for an existing pool have arrived. Merge them in
        // to whichever signal matches by poolAddress.
        if (msg.type === "pool_metrics") {
          const m = msg.metrics;
          // SOL/SUI come back with error="...not implemented" — those are
          // intentional sentinels meaning "audit only". Treat as ready.
          const isAuditOnly = m.error && m.error.includes("not implemented");
          const newStatus = m.error && !isAuditOnly ? "error" : "ready";

          setSignals((prev) =>
            prev.map((s) =>
              s.poolAddress === msg.pool_address
                ? {
                    ...s,
                    status: newStatus,
                    liqUsd: m.final_liq_usd ?? m.initial_liq_usd ?? null,
                    buyCount: m.buy_count,
                    sellCount: m.sell_count,
                    uniqueBuyers: m.unique_buyers,
                    priceChange: m.price_change_pct,
                    finalPriceUsd: m.final_price_usd,
                    priceSamples: m.price_samples,
                    breakoutTriggered: m.breakout_triggered,
                    breakoutScore: m.breakout_score,
                    breakoutReason: m.breakout_reason,
                    enrichError: m.error,
                  }
                : s
            )
          );
          if (!isAuditOnly) {
            pushLog(
              "metrics",
              `metrics ${msg.pool_address.slice(0, 10)}… liq=${m.final_liq_usd ? fmtUsdK(m.final_liq_usd) : "?"} buyers=${m.unique_buyers ?? "?"} Δ=${m.price_change_pct?.toFixed(1) ?? "?"}%`
            );
          }
        }

        // DEXScreener trending — separate panel, separate intent.
        // These are NOT actionable signals for paper trading; they're
        // a market-context cross-check.
        if (msg.type === "trending_token") {
          const t = msg.token;
          setTrending((prev) => {
            // Dedup by chain+address; refresh existing entry if we get an update.
            const key = `${t.chain}:${t.address.toLowerCase()}`;
            const filtered = prev.filter(
              (x) => `${x.chain}:${x.address.toLowerCase()}` !== key
            );
            return [{ ...t, ts: Date.now() }, ...filtered].slice(0, 30);
          });
          pushLog(
            "trending",
            `trending ${t.chain} ${t.symbol || t.address.slice(0, 8)} liq=${t.liq_usd ? fmtUsdK(t.liq_usd) : "?"}`
          );
        }
      } catch (err) {
        console.error("ws parse error", err);
      }
    };

    ws.onerror = () => pushLog("sys", "✗ websocket error — backend running on :8080?");
    ws.onclose = () => {
      setWsStatus("disconnected");
      if (wsRef.current === ws) wsRef.current = null;
    };

    return () => ws.close();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [signalSource]);

  // ────────────────────── Remote engine polling ────────────────────────────
  // In remote mode, the dashboard becomes a thin viewer. Poll /state every
  // second and overwrite local state from the backend snapshot. This means
  // closing the browser doesn't stop trading.
  //
  // The local tick loop (below) early-returns when engineMode === "remote",
  // so there's no double-execution.
  useEffect(() => {
    if (engineMode !== "remote") {
      setRemoteEngineHealthy(false);
      return;
    }

    let cancelled = false;
    const poll = async () => {
      const state = await backendClient.fetchState();
      if (cancelled) return;
      if (!state) {
        setRemoteEngineHealthy(false);
        return;
      }
      setRemoteEngineHealthy(true);
      setRunning(state.running);
      setStartBalance(state.start_balance);
      setCash(state.cash);
      // Backend uses snake_case; reshape to the frontend's camelCase
      setPositions(state.positions.map((p) => ({
        id: p.id, chain: p.chain, symbol: p.symbol, address: p.address,
        qty: p.qty, entryPx: p.entry_px, markPx: p.mark_px,
        bias: p.bias, tpPct: p.tp_pct, slPct: p.sl_pct,
        openedAt: p.opened_at, stale: false,
      })));
      setTrades(state.trades.map((t) => ({
        id: t.id, chain: t.chain, symbol: t.symbol, address: t.address,
        qty: t.qty, entryPx: t.entry_px, exitPx: t.exit_px,
        pnlUsd: t.pnl_usd, pnlPct: t.pnl_pct, reason: t.reason,
        openedAt: t.opened_at, closedAt: t.closed_at,
      })));
      setEquity(state.equity);
      // Params: merge so we don't blow away local-only fields (none today,
      // but defensive against future drift).
      setParams((prev) => ({ ...prev, ...state.params }));
    };

    poll();  // immediate first read
    const id = setInterval(poll, 1000);
    return () => { cancelled = true; clearInterval(id); };
  }, [engineMode]);

  // ───────────────────────── Main simulation loop ──────────────────────────
  useEffect(() => {
    if (!running) return;
    // In remote mode, the backend runs the engine. Skip the local loop.
    if (engineMode === "remote") return;

    const interval = setInterval(() => {
      // 1. drift open positions' marks. Clears `stale` if it was set
      //    by a refresh-restore — first new mark price means the value
      //    is fresh again.
      setPositions((prev) =>
        prev.map((p) => {
          const drift = rand(-0.04, 0.05) + p.bias;
          return {
            ...p,
            markPx: Math.max(1e-7, p.markPx * (1 + drift)),
            stale: false,
          };
        })
      );

      // 2. acquire a new signal this tick
      let sig = null;
      if (signalSource === "live") {
        // Skip queued signals for disabled chains
        while (liveSignalQueueRef.current.length > 0) {
          const candidate = liveSignalQueueRef.current.shift();
          if (enabledChains.includes(candidate.chain)) {
            sig = candidate;
            break;
          }
        }
      } else if (Math.random() < 0.55) {
        sig = makeSyntheticSignal(enabledChains);
      }

      if (sig) {
        setSignals((prev) => [sig, ...prev].slice(0, 16));

        if (sig.status === "pending") {
          pushLog(
            "signal",
            `${sig.chain} ${sig.dex} → ${shortAddr(sig.address)} (pending metrics)`
          );
        } else {
          pushLog(
            "signal",
            `${sig.chain} ${sig.dex} → ${sig.symbol} liq=${fmtUsdK(sig.liqUsd)} buyers=${sig.uniqueBuyers}`
          );
        }

        // 3. strategy filter — only fires on READY signals
        const passes = strategyPasses(sig, params);
        const currentOpen = positionsRef.current;
        const currentCash = cashRef.current;
        const canOpen =
          passes === true &&
          currentOpen.length < params.maxConcurrent &&
          currentCash >= params.positionSizeUsd;

        if (canOpen) {
          // Live signals carry final_price_usd from the enricher's last-block
          // reserves snapshot. Synthetic signals use their generated price.
          // Fallback to $1 only if both are missing (audit-only SOL/SUI).
          const entryPx = sig.finalPriceUsd || sig.price || 1;
          const qty = params.positionSizeUsd / entryPx;
          const pos = {
            id: nextId(),
            chain: sig.chain,
            dex: sig.dex,
            symbol: sig.symbol,
            address: sig.address,
            qty,
            entryPx,
            markPx: entryPx,
            sizeUsd: params.positionSizeUsd,
            tpPct: params.takeProfitPct,
            slPct: params.stopLossPct,
            openedAt: Date.now(),
            bias: rand(-0.005, 0.012),
          };
          setPositions((prev) => [pos, ...prev]);
          setCash((prev) => prev - params.positionSizeUsd);
          pushLog(
            "open",
            `OPEN ${sig.symbol} ${qty.toFixed(4)} @ ${fmtUsd(entryPx)}`
          );
        }
      }

      setTickCount((c) => c + 1);
    }, 700);

    return () => clearInterval(interval);
  }, [running, params, signalSource, enabledChains, engineMode]);

  // ────────────────────────── Visibility recovery ─────────────────────────
  // When the OS locks the screen or the user switches to another tab for
  // an extended time, browsers throttle setInterval to once a minute or
  // longer. The simulation looks frozen when you come back. Worse, any
  // "elapsed time" math would be wrong — we never observed those minutes.
  //
  // Strategy: when the tab becomes visible again, log the gap, clear the
  // `stale` flag on positions (they'll get a fresh mark on the next tick),
  // and let the existing tick interval resume naturally. We do NOT try to
  // fast-forward the simulation — synthesizing fake price movement would
  // be misleading.
  const lastVisibleRef = useRef(Date.now());
  useEffect(() => {
    const onVisibilityChange = () => {
      if (document.visibilityState === "visible") {
        const gapSec = (Date.now() - lastVisibleRef.current) / 1000;
        lastVisibleRef.current = Date.now();
        if (gapSec > 5) {
          // Gaps under 5s are normal context switches; ignore.
          pushLog(
            "sys",
            `tab resumed after ${gapSec < 60 ? `${gapSec.toFixed(0)}s` : `${(gapSec / 60).toFixed(1)}m`} away — positions will refresh on next tick`
          );
          // Clear stale flag so positions stop showing as stale once
          // they get their first new mark.
          setPositions((prev) => prev.map((p) => ({ ...p, stale: false })));
        }
      } else {
        lastVisibleRef.current = Date.now();
      }
    };
    document.addEventListener("visibilitychange", onVisibilityChange);
    return () => document.removeEventListener("visibilitychange", onVisibilityChange);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Position close pass
  useEffect(() => {
    if (tickCount === 0) return;
    setPositions((prev) => {
      const stillOpen = [];
      const newlyClosed = [];
      const now = Date.now();
      prev.forEach((p) => {
        // Stale positions (restored from a previous session) skip the
        // close pass entirely. Their last-seen mark price is potentially
        // hours old; closing on it would book fictional P&L. They get
        // re-evaluated once the next tick refreshes their mark.
        if (p.stale) {
          stillOpen.push(p);
          return;
        }
        const pnlPct = ((p.markPx - p.entryPx) / p.entryPx) * 100;
        const ageMs = now - p.openedAt;
        let reason = null;
        if (pnlPct >= p.tpPct) reason = "tp";
        else if (pnlPct <= -p.slPct) reason = "sl";
        else if (ageMs > 45000) reason = "timeout";
        if (reason) {
          newlyClosed.push({
            ...p,
            closedAt: now,
            exitPx: p.markPx,
            pnlUsd: (p.markPx - p.entryPx) * p.qty,
            pnlPct,
            reason,
          });
        } else {
          stillOpen.push(p);
        }
      });

      if (newlyClosed.length > 0) {
        const proceeds = newlyClosed.reduce((s, t) => s + t.qty * t.exitPx, 0);
        setCash((c) => c + proceeds);
        setTrades((tlist) => [...newlyClosed, ...tlist].slice(0, 200));
        newlyClosed.forEach((t) =>
          pushLog(
            t.pnlUsd >= 0 ? "win" : "loss",
            `CLOSE ${t.symbol} ${t.reason.toUpperCase()} ${pct(t.pnlPct)} (${t.pnlUsd >= 0 ? "+" : ""}${fmtUsd(t.pnlUsd)})`
          )
        );
      }
      return stillOpen;
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tickCount]);

  // Equity curve point per tick
  useEffect(() => {
    if (!running) return;
    const openValue = positions.reduce((s, p) => s + p.qty * p.markPx, 0);
    const eq = cash + openValue;
    setEquity((prev) => [...prev, { t: prev.length, v: eq }].slice(-2000));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tickCount]);

  // Derived metrics
  const openValue = positions.reduce((s, p) => s + p.qty * p.markPx, 0);
  const totalEquity = cash + openValue;
  const totalPnl = totalEquity - startBalance;
  const totalPnlPct = (totalPnl / startBalance) * 100;

  const stats = useMemo(() => {
    if (trades.length === 0) {
      return { wins: 0, losses: 0, winRate: 0, avgWin: 0, avgLoss: 0, bestTrade: 0, worstTrade: 0, expectancy: 0 };
    }
    const wins = trades.filter((t) => t.pnlUsd > 0);
    const losses = trades.filter((t) => t.pnlUsd <= 0);
    const avgWin = wins.length ? wins.reduce((s, t) => s + t.pnlUsd, 0) / wins.length : 0;
    const avgLoss = losses.length ? losses.reduce((s, t) => s + t.pnlUsd, 0) / losses.length : 0;
    const wr = (wins.length / trades.length) * 100;
    const exp = (wr / 100) * avgWin + ((100 - wr) / 100) * avgLoss;
    return {
      wins: wins.length,
      losses: losses.length,
      winRate: wr,
      avgWin,
      avgLoss,
      bestTrade: Math.max(...trades.map((t) => t.pnlUsd)),
      worstTrade: Math.min(...trades.map((t) => t.pnlUsd)),
      expectancy: exp,
    };
  }, [trades]);

  // Cross-highlight: build Sets of "chain:address" keys present in each
  // feed so each row can quickly check if it has a counterpart in the
  // other table. This is the actual value-add of the trending feed —
  // when something the on-chain scanner caught ALSO shows up on
  // DEXScreener, that's a confidence signal worth surfacing visually.
  const matchKey = (chain, addr) =>
    `${(chain || "").toUpperCase()}:${(addr || "").toLowerCase()}`;

  const trendingKeys = useMemo(
    () => new Set(trending.map((t) => matchKey(t.chain, t.address))),
    [trending]
  );

  const signalKeys = useMemo(
    () => new Set(signals.filter((s) => s.address).map((s) => matchKey(s.chain, s.address))),
    [signals]
  );

  // Controls
  const handleStart = () => {
    if (engineMode === "remote") {
      backendClient.controlEngine("start").then((r) => {
        if (r.ok) pushLog("sys", "remote engine started");
        else pushLog("sys", `✗ remote start failed: ${r.error || r.status}`);
      });
      return;
    }
    setRunning(true);
    pushLog("sys", "bot started — listening for scanner events");
  };
  const handleStop = () => {
    if (engineMode === "remote") {
      backendClient.controlEngine("stop").then((r) => {
        if (r.ok) pushLog("sys", "remote engine stopped");
        else pushLog("sys", `✗ remote stop failed: ${r.error || r.status}`);
      });
      return;
    }
    setRunning(false);
    pushLog("sys", "bot stopped");
  };
  const handleReset = () => {
    if (engineMode === "remote") {
      backendClient.controlEngine("reset").then((r) => {
        if (r.ok) pushLog("sys", "remote engine reset");
        else pushLog("sys", `✗ remote reset failed: ${r.error || r.status}`);
      });
      return;
    }
    setRunning(false);
    setCash(startBalance);
    setPositions([]);
    setTrades([]);  // useEffect above will persist the empty array
    setSignals(Array.from({ length: 4 }, () => makeSyntheticSignal(enabledChains)).filter(Boolean));
    setEquity([{ t: 0, v: startBalance }]);
    setTickCount(0);
    setLogs([]);
    pushedTradeIdsRef.current = new Set();
    if (persistence.isConfigured) {
      persistence.clearAll().catch(() => {});
    }
    pushLog("sys", `harness reset — wallet refilled to ${fmtUsd(startBalance)}${persistence.isConfigured ? " · supabase cleared" : ""}`);
  };

  // When params change in remote mode, push the update to the backend.
  // Debounced so a slider drag doesn't fire 50 requests.
  const paramsPushTimerRef = useRef(null);
  useEffect(() => {
    if (engineMode !== "remote") return;
    if (paramsPushTimerRef.current) clearTimeout(paramsPushTimerRef.current);
    paramsPushTimerRef.current = setTimeout(() => {
      backendClient.updateParams({ ...params, start_balance: startBalance });
    }, 400);
    return () => {
      if (paramsPushTimerRef.current) clearTimeout(paramsPushTimerRef.current);
    };
  }, [engineMode, params, startBalance]);

  const pendingCount = signals.filter((s) => s.status === "pending").length;

  return (
    <div className="min-h-screen w-full bg-black font-mono text-zinc-200">
      {/* Header */}
      <header className="border-b border-zinc-800">
        <div className="flex flex-wrap items-center justify-between gap-4 px-4 py-3">
          <div className="flex items-center gap-4">
            <div className="flex items-center gap-2">
              <div className="text-base font-bold tracking-[0.3em] text-zinc-100">FORERUNNER</div>
              <div className="border border-zinc-800 px-2 py-0.5 text-[9px] uppercase tracking-[0.2em] text-zinc-500">paper · v0.2</div>
            </div>
            <div className="hidden h-5 w-px bg-zinc-800 sm:block" />
            <div className="flex items-center gap-2 text-[10px] uppercase tracking-[0.2em] text-zinc-500">
              <PulseDot on={running} />
              <span>{running ? "live" : "idle"}</span>
            </div>

            <div className="hidden items-center gap-1 border border-zinc-800 md:flex">
              <button onClick={() => setSignalSource("synthetic")} disabled={running}
                className={`px-2 py-1 text-[9px] uppercase tracking-[0.2em] transition-colors disabled:opacity-50 ${signalSource === "synthetic" ? "bg-zinc-800 text-zinc-100" : "text-zinc-500 hover:bg-zinc-900"}`}>
                synthetic
              </button>
              <button onClick={() => setSignalSource("live")} disabled={running}
                className={`px-2 py-1 text-[9px] uppercase tracking-[0.2em] transition-colors disabled:opacity-50 ${signalSource === "live" ? "bg-zinc-800 text-zinc-100" : "text-zinc-500 hover:bg-zinc-900"}`}>
                live
              </button>
            </div>

            {/* Engine mode: local (browser tick loop) vs remote (backend runs 24/7).
                Disabled while running so we don't lose state mid-session. */}
            <div className="hidden items-center gap-1 border border-zinc-800 md:flex"
                 title="engine: local = browser tick loop · remote = backend keeps running with browser closed">
              <button onClick={() => setEngineMode("local")} disabled={running}
                className={`px-2 py-1 text-[9px] uppercase tracking-[0.2em] transition-colors disabled:opacity-50 ${engineMode === "local" ? "bg-zinc-800 text-zinc-100" : "text-zinc-500 hover:bg-zinc-900"}`}>
                local
              </button>
              <button onClick={() => setEngineMode("remote")} disabled={running}
                className={`px-2 py-1 text-[9px] uppercase tracking-[0.2em] transition-colors disabled:opacity-50 ${engineMode === "remote" ? `${remoteEngineHealthy ? "bg-emerald-900/40 text-emerald-300" : "bg-amber-900/30 text-amber-300"}` : "text-zinc-500 hover:bg-zinc-900"}`}>
                remote{engineMode === "remote" && !remoteEngineHealthy ? " ⚠" : ""}
              </button>
            </div>

            {/* Chain selector — multi-select. At least one chain must stay
                enabled. Disabled while bot is running so the trade loop
                doesn't churn mid-session. */}
            <div className="hidden items-center gap-1 border border-zinc-800 md:flex">
              {CHAINS.map((c) => {
                const on = enabledChains.includes(c);
                const lastEnabled = enabledChains.length === 1 && on;
                return (
                  <button
                    key={c}
                    disabled={running || lastEnabled}
                    onClick={() => {
                      setEnabledChains((prev) =>
                        prev.includes(c) ? prev.filter((x) => x !== c) : [...prev, c]
                      );
                    }}
                    title={lastEnabled ? "at least one chain must be enabled" : `toggle ${c}`}
                    className={`px-2 py-1 text-[9px] uppercase tracking-[0.2em] transition-colors disabled:cursor-not-allowed ${
                      on
                        ? "bg-zinc-800 text-zinc-100 disabled:opacity-70"
                        : "text-zinc-600 hover:bg-zinc-900 disabled:opacity-30"
                    }`}
                  >
                    {c}
                  </button>
                );
              })}
            </div>

            {signalSource === "live" && (
              <div className="hidden items-center gap-1.5 text-[9px] uppercase tracking-[0.2em] md:flex">
                <span className={`inline-block h-1.5 w-1.5 ${wsStatus === "connected" ? "bg-emerald-400" : wsStatus === "connecting" ? "bg-amber-400" : "bg-red-500"}`} />
                <span className={wsStatus === "connected" ? "text-emerald-400" : wsStatus === "connecting" ? "text-amber-400" : "text-red-400"}>
                  ws {wsStatus}
                </span>
              </div>
            )}

            <div className="hidden items-center gap-1.5 text-[9px] uppercase tracking-[0.2em] md:flex"
                 title={persistence.isConfigured ? "trades sync to supabase" : "trades persist to browser only — set VITE_SUPABASE_URL to enable cloud sync"}>
              <span className={`inline-block h-1.5 w-1.5 ${persistence.isConfigured ? "bg-cyan-400" : "bg-zinc-700"}`} />
              <span className={persistence.isConfigured ? "text-cyan-400" : "text-zinc-600"}>
                db {persistence.isConfigured ? "supabase" : "local"}
              </span>
            </div>
          </div>

          <div className="flex items-center gap-2">
            {!running ? (
              <button onClick={handleStart}
                className="flex items-center gap-2 border border-emerald-500/40 bg-emerald-500/10 px-3 py-1.5 text-[10px] uppercase tracking-[0.2em] text-emerald-400 transition-colors hover:bg-emerald-500/20">
                <Play size={11} /> start
              </button>
            ) : (
              <button onClick={handleStop}
                className="flex items-center gap-2 border border-amber-500/40 bg-amber-500/10 px-3 py-1.5 text-[10px] uppercase tracking-[0.2em] text-amber-400 transition-colors hover:bg-amber-500/20">
                <Square size={11} /> stop
              </button>
            )}
            <button onClick={handleReset}
              className="flex items-center gap-2 border border-zinc-800 px-3 py-1.5 text-[10px] uppercase tracking-[0.2em] text-zinc-400 transition-colors hover:bg-zinc-900">
              <RotateCcw size={11} /> reset
            </button>
          </div>
        </div>

        <div className="grid grid-cols-2 border-t border-zinc-800 sm:grid-cols-3 md:grid-cols-6">
          <Stat label="equity" value={fmtUsd(totalEquity)} />
          <Stat label="P&L"
            value={`${totalPnl >= 0 ? "+" : ""}${fmtUsd(totalPnl)}  (${pct(totalPnlPct)})`}
            tone={totalPnl >= 0 ? "up" : "down"} />
          <Stat label="cash" value={fmtUsd(cash)} />
          <Stat label="open" value={`${positions.length} / ${params.maxConcurrent}`} />
          <Stat label="win rate"
            value={trades.length ? `${stats.winRate.toFixed(1)}%` : "—"}
            tone={!trades.length ? "default" : stats.winRate >= 50 ? "up" : "down"} />
          <Stat label="trades" value={`${stats.wins}W / ${stats.losses}L`} tone="amber" />
        </div>
      </header>

      <main className="grid grid-cols-1 gap-px bg-zinc-800 lg:grid-cols-12">
        <Panel title={<span className="flex items-center gap-1.5"><Settings2 size={10} /> strategy</span>}
          className="lg:col-span-3" right="editable">
          <ParamEditor
            params={params}
            onChange={setParams}
            startBalance={startBalance}
            onStartBalanceChange={(v) => {
              setStartBalance(v);
              setCash(v);
              setEquity([{ t: 0, v }]);
            }}
            disabled={running}
          />
        </Panel>

        <Panel title="equity curve" right={`${equity.length} ticks`} className="lg:col-span-6">
          <div className="h-56 px-2 py-2">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={equity}>
                <YAxis domain={["dataMin - 50", "dataMax + 50"]}
                  tick={{ fill: "#52525b", fontSize: 9 }}
                  axisLine={{ stroke: "#27272a" }} tickLine={false} width={50} />
                <XAxis dataKey="t" tick={{ fill: "#52525b", fontSize: 9 }}
                  axisLine={{ stroke: "#27272a" }} tickLine={false} />
                <Tooltip contentStyle={{ background: "#000", border: "1px solid #27272a", fontFamily: "JetBrains Mono, monospace", fontSize: 11 }}
                  labelStyle={{ color: "#71717a", fontSize: 9 }}
                  formatter={(v) => [fmtUsd(v), "equity"]} />
                <ReferenceLine y={startBalance} stroke="#3f3f46" strokeDasharray="2 4" />
                <Line type="monotone" dataKey="v"
                  stroke={totalPnl >= 0 ? "#10b981" : "#ef4444"}
                  strokeWidth={1.5} dot={false} isAnimationActive={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </Panel>

        <Panel title="performance" className="lg:col-span-3">
          <div className="divide-y divide-zinc-800">
            <PerfRow label="expectancy / trade" value={fmtUsd(stats.expectancy)} tone={stats.expectancy >= 0 ? "up" : "down"} />
            <PerfRow label="avg win" value={fmtUsd(stats.avgWin)} tone="up" />
            <PerfRow label="avg loss" value={fmtUsd(stats.avgLoss)} tone="down" />
            <PerfRow label="best trade" value={fmtUsd(stats.bestTrade)} tone="up" />
            <PerfRow label="worst trade" value={fmtUsd(stats.worstTrade)} tone="down" />
            <PerfRow label="profit factor"
              value={stats.avgLoss !== 0
                ? `${Math.abs((stats.avgWin * stats.wins) / (stats.avgLoss * stats.losses || 1)).toFixed(2)}x`
                : "—"} tone="amber" />
          </div>
        </Panel>

        <Panel title={<span className="flex items-center gap-1.5"><Radio size={10} /> live signals</span>}
          className="lg:col-span-5"
          right={pendingCount > 0 ? `${signals.length} (${pendingCount} pending)` : `${signals.length} buffered`}>
          <div className="scroll-thin max-h-72 overflow-y-auto">
            <table className="w-full text-[11px]">
              <thead className="sticky top-0 bg-black">
                <tr className="border-b border-zinc-800 text-[9px] uppercase tracking-[0.15em] text-zinc-500">
                  <th className="px-2 py-2 text-left">chain</th>
                  <th className="px-2 py-2 text-left">symbol</th>
                  <th className="px-2 py-2 text-center">chart</th>
                  <th className="px-2 py-2 text-center">BO</th>
                  <th className="px-2 py-2 text-right">liq</th>
                  <th className="px-2 py-2 text-right">buyers</th>
                  <th className="px-2 py-2 text-right">Δ%</th>
                  <th className="px-2 py-2 text-right">score</th>
                </tr>
              </thead>
              <tbody>
                {signals.length === 0 && (
                  <tr><td colSpan={8} className="px-3 py-6 text-center text-zinc-600">waiting for first scanner event…</td></tr>
                )}
                {signals.map((s) => {
                  const result = strategyPasses(s, params);
                  const passes = result === true;
                  const isPending = s.status === "pending";
                  const isError = s.status === "error";
                  const isTrending = s.address && trendingKeys.has(matchKey(s.chain, s.address));
                  return (
                    <tr
                      key={s.id}
                      className={`border-b border-zinc-900 tabular-nums hover:bg-zinc-950 ${
                        isTrending ? "bg-orange-950/20" : ""
                      }`}
                      title={isTrending ? "also trending on DEXScreener" : undefined}
                    >
                      <td className="px-2 py-2 text-zinc-400">{s.chain}</td>
                      <td className="px-2 py-2">
                        <span className={isPending ? "text-amber-500/70" : isError ? "text-red-500/70" : passes ? "text-emerald-400" : "text-zinc-300"}>
                          {isPending ? <Clock size={9} className="mr-1 inline" /> : passes ? "▸ " : "  "}
                          {s.symbol}
                          {isTrending && <Flame size={10} className="ml-1 inline text-orange-400" />}
                        </span>
                      </td>
                      <td className="px-2 py-1">
                        <Sparkline samples={s.priceSamples} triggered={s.breakoutTriggered} />
                      </td>
                      <td className="px-2 py-2 text-center" title={s.breakoutReason || ""}>
                        {s.breakoutTriggered ? (
                          <Zap size={11} className="inline text-amber-400" />
                        ) : s.priceSamples ? (
                          <span className="text-zinc-700">·</span>
                        ) : (
                          <span className="text-zinc-700">—</span>
                        )}
                      </td>
                      <td className="px-2 py-2 text-right text-zinc-300">
                        {s.liqUsd != null ? fmtUsdK(s.liqUsd) : <span className="text-zinc-600">—</span>}
                      </td>
                      <td className="px-2 py-2 text-right text-zinc-300">
                        {s.uniqueBuyers != null ? s.uniqueBuyers : <span className="text-zinc-600">—</span>}
                      </td>
                      <td className={`px-2 py-2 text-right ${s.priceChange == null ? "text-zinc-600" : s.priceChange >= 0 ? "text-emerald-400" : "text-red-400"}`}>
                        {s.priceChange != null ? `${s.priceChange >= 0 ? "+" : ""}${s.priceChange.toFixed(1)}%` : "—"}
                      </td>
                      <td className={`px-2 py-2 text-right ${s.auditScore >= 70 ? "text-emerald-400" : s.auditScore >= 50 ? "text-amber-400" : "text-red-400"}`}>
                        {s.auditScore}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </Panel>

        <Panel title={<span className="flex items-center gap-1.5"><Activity size={10} /> open positions</span>}
          className="lg:col-span-7" right={positions.length === 0 ? "no exposure" : `${positions.length} open`}>
          <div className="scroll-thin max-h-72 overflow-y-auto">
            <table className="w-full text-[11px]">
              <thead className="sticky top-0 bg-black">
                <tr className="border-b border-zinc-800 text-[9px] uppercase tracking-[0.15em] text-zinc-500">
                  <th className="px-3 py-2 text-left">symbol</th>
                  <th className="px-3 py-2 text-left">addr</th>
                  <th className="px-3 py-2 text-right">qty</th>
                  <th className="px-3 py-2 text-right">entry</th>
                  <th className="px-3 py-2 text-right">mark</th>
                  <th className="px-3 py-2 text-right">P&L</th>
                  <th className="px-3 py-2 text-right">%</th>
                </tr>
              </thead>
              <tbody>
                {positions.length === 0 && (
                  <tr><td colSpan={7} className="px-3 py-6 text-center text-zinc-600">no open positions</td></tr>
                )}
                {positions.map((p) => {
                  const upnl = (p.markPx - p.entryPx) * p.qty;
                  const upnlPct = ((p.markPx - p.entryPx) / p.entryPx) * 100;
                  const tone = p.stale ? "text-zinc-500" : upnl >= 0 ? "text-emerald-400" : "text-red-400";
                  return (
                    <tr key={p.id} className={`border-b border-zinc-900 tabular-nums hover:bg-zinc-950 ${p.stale ? "opacity-60" : ""}`}>
                      <td className="px-3 py-2 text-zinc-200">
                        <span className="text-zinc-500">{p.chain} </span>{p.symbol}
                        {p.stale && <span className="ml-1.5 text-[8px] uppercase tracking-[0.2em] text-amber-500/70" title="restored from previous session — start the bot to refresh mark price">stale</span>}
                      </td>
                      <td className="px-3 py-2 text-[10px] text-zinc-500">{shortAddr(p.address)}</td>
                      <td className="px-3 py-2 text-right text-zinc-300">{p.qty.toFixed(4)}</td>
                      <td className="px-3 py-2 text-right text-zinc-400">{fmtUsd(p.entryPx)}</td>
                      <td className="px-3 py-2 text-right text-zinc-200">{fmtUsd(p.markPx)}</td>
                      <td className={`px-3 py-2 text-right ${tone}`}>{upnl >= 0 ? "+" : ""}{fmtUsd(upnl)}</td>
                      <td className={`px-3 py-2 text-right ${tone}`}>{pct(upnlPct)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </Panel>

        <Panel title="trade history" className="lg:col-span-7"
          right={trades.length > 0 ? `${trades.length} closed · saved` : "no trades"}>
          <div className="scroll-thin max-h-64 overflow-y-auto">
            <table className="w-full text-[11px]">
              <thead className="sticky top-0 bg-black">
                <tr className="border-b border-zinc-800 text-[9px] uppercase tracking-[0.15em] text-zinc-500">
                  <th className="px-3 py-2 text-left">symbol</th>
                  <th className="px-3 py-2 text-left">reason</th>
                  <th className="px-3 py-2 text-right">entry</th>
                  <th className="px-3 py-2 text-right">exit</th>
                  <th className="px-3 py-2 text-right">P&L</th>
                  <th className="px-3 py-2 text-right">%</th>
                </tr>
              </thead>
              <tbody>
                {trades.length === 0 && (
                  <tr><td colSpan={6} className="px-3 py-6 text-center text-zinc-600">no closed trades yet</td></tr>
                )}
                {trades.map((t) => {
                  const tone = t.pnlUsd >= 0 ? "text-emerald-400" : "text-red-400";
                  const reasonStyle = t.reason === "tp" ? "text-emerald-500/80"
                                    : t.reason === "sl" ? "text-red-500/80" : "text-zinc-500";
                  return (
                    <tr key={t.id} className="border-b border-zinc-900 tabular-nums hover:bg-zinc-950">
                      <td className="px-3 py-2"><span className="text-zinc-500">{t.chain} </span>{t.symbol}</td>
                      <td className={`px-3 py-2 uppercase tracking-wider ${reasonStyle}`}>{t.reason}</td>
                      <td className="px-3 py-2 text-right text-zinc-400">{fmtUsd(t.entryPx)}</td>
                      <td className="px-3 py-2 text-right text-zinc-300">{fmtUsd(t.exitPx)}</td>
                      <td className={`px-3 py-2 text-right ${tone}`}>{t.pnlUsd >= 0 ? "+" : ""}{fmtUsd(t.pnlUsd)}</td>
                      <td className={`px-3 py-2 text-right ${tone}`}>{pct(t.pnlPct)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </Panel>

        {/* DEXScreener trending — market context, not paper-trade fodder.
            Shows tokens that DEXScreener is currently surfacing as boosted/
            trending across all chains we care about. Useful as a sanity
            check against the on-chain scanner: did anything we just
            detected ALSO show up here within minutes? */}
        <Panel
          title={<span className="flex items-center gap-1.5"><Flame size={10} /> dexscreener trending</span>}
          className="lg:col-span-12"
          right={trending.length > 0 ? `${trending.length} tracked` : "polling…"}
        >
          <div className="scroll-thin max-h-56 overflow-y-auto">
            <table className="w-full text-[11px]">
              <thead className="sticky top-0 bg-black">
                <tr className="border-b border-zinc-800 text-[9px] uppercase tracking-[0.15em] text-zinc-500">
                  <th className="px-3 py-2 text-left">chain</th>
                  <th className="px-3 py-2 text-left">symbol</th>
                  <th className="px-3 py-2 text-left">name</th>
                  <th className="px-3 py-2 text-right">price</th>
                  <th className="px-3 py-2 text-right">liq</th>
                  <th className="px-3 py-2 text-right">vol 24h</th>
                  <th className="px-3 py-2 text-right">Δ 24h</th>
                  <th className="px-3 py-2 text-right">mcap</th>
                  <th className="px-3 py-2 text-right">boosts</th>
                  <th className="px-3 py-2 text-right">link</th>
                </tr>
              </thead>
              <tbody>
                {trending.length === 0 && (
                  <tr><td colSpan={10} className="px-3 py-6 text-center text-zinc-600">
                    {signalSource === "live" ? "polling dexscreener every 60s…" : "live mode required for trending feed"}
                  </td></tr>
                )}
                {trending.map((t) => {
                  const pcTone = t.price_change_24h == null
                    ? "text-zinc-600"
                    : t.price_change_24h >= 0 ? "text-emerald-400" : "text-red-400";
                  const hasSignal = signalKeys.has(matchKey(t.chain, t.address));
                  return (
                    <tr
                      key={`${t.chain}:${t.address}`}
                      className={`border-b border-zinc-900 tabular-nums hover:bg-zinc-950 ${
                        hasSignal ? "bg-emerald-950/20" : ""
                      }`}
                      title={hasSignal ? "also detected by on-chain scanner" : undefined}
                    >
                      <td className="px-3 py-2 text-zinc-400">{t.chain}</td>
                      <td className="px-3 py-2 text-zinc-200">
                        {hasSignal && <span className="mr-1 text-emerald-400">▸</span>}
                        {t.symbol || "—"}
                      </td>
                      <td className="px-3 py-2 text-zinc-500 truncate max-w-[140px]">{t.name || ""}</td>
                      <td className="px-3 py-2 text-right text-zinc-300">
                        {t.price_usd ? `$${t.price_usd < 0.01 ? t.price_usd.toExponential(2) : t.price_usd.toFixed(t.price_usd < 1 ? 4 : 2)}` : "—"}
                      </td>
                      <td className="px-3 py-2 text-right text-zinc-300">{t.liq_usd ? fmtUsdK(t.liq_usd) : "—"}</td>
                      <td className="px-3 py-2 text-right text-zinc-300">{t.volume_24h ? fmtUsdK(t.volume_24h) : "—"}</td>
                      <td className={`px-3 py-2 text-right ${pcTone}`}>
                        {t.price_change_24h != null ? `${t.price_change_24h >= 0 ? "+" : ""}${t.price_change_24h.toFixed(1)}%` : "—"}
                      </td>
                      <td className="px-3 py-2 text-right text-zinc-300">{t.market_cap ? fmtUsdK(t.market_cap) : "—"}</td>
                      <td className="px-3 py-2 text-right text-amber-400">{t.boost_amount || 0}</td>
                      <td className="px-3 py-2 text-right">
                        {t.pair_url ? (
                          <a href={t.pair_url} target="_blank" rel="noopener noreferrer"
                             className="text-cyan-400/80 hover:text-cyan-300 underline">↗</a>
                        ) : <span className="text-zinc-700">—</span>}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </Panel>

        <Panel title="system log" className="lg:col-span-5" right="tail">
          <div className="scroll-thin max-h-64 overflow-y-auto px-3 py-2 text-[11px] leading-relaxed">
            {logs.length === 0 && <div className="text-zinc-600">— no events —</div>}
            {logs.map((l) => {
              const tone = l.kind === "win" ? "text-emerald-400"
                         : l.kind === "loss" ? "text-red-400"
                         : l.kind === "open" ? "text-amber-400"
                         : l.kind === "metrics" ? "text-cyan-400"
                         : l.kind === "trending" ? "text-orange-400"
                         : l.kind === "signal" ? "text-zinc-300"
                         : "text-zinc-500";
              return (
                <div key={l.id} className="flex gap-3">
                  <span className="text-zinc-700">{l.ts}</span>
                  <span className={tone}>{l.msg}</span>
                </div>
              );
            })}
          </div>
        </Panel>
      </main>

      <footer className="border-t border-zinc-800 px-4 py-3 text-[9px] uppercase tracking-[0.2em] text-zinc-600">
        paper-trade simulator · all positions virtual · {signalSource === "live" ? "live signals from backend ws://:8080 · 30s enrichment window" : "synthetic signals — calibrate filter against archetype mix"}
      </footer>
    </div>
  );
}

function ParamEditor({ params, onChange, startBalance, onStartBalanceChange, disabled }) {
  const fields = [
    { key: "minLiqUsd",       label: "min liquidity ($)",  min: 500,  max: 100000, step: 500 },
    { key: "minBuyers",       label: "min unique buyers",  min: 1,    max: 30,     step: 1 },
    { key: "minPriceChange",  label: "min Δ% in 30s",      min: -20,  max: 100,    step: 5 },
    { key: "minAuditScore",   label: "min audit score",    min: 0,    max: 100,    step: 5 },
    { key: "positionSizeUsd", label: "size (usd)",         min: 10,   max: 2000,   step: 10 },
    { key: "takeProfitPct",   label: "take profit %",      min: 5,    max: 100,    step: 5 },
    { key: "stopLossPct",     label: "stop loss %",        min: 2,    max: 50,     step: 1 },
    { key: "maxConcurrent",   label: "max concurrent",     min: 1,    max: 10,     step: 1 },
  ];

  const presets = [100, 500, 1000, 10000];
  const underfunded = startBalance < params.positionSizeUsd;

  return (
    <div className="divide-y divide-zinc-800">
      <div className="flex flex-col gap-2 px-3 py-3">
        <div className="flex items-center justify-between">
          <span className="text-[10px] uppercase tracking-[0.15em] text-zinc-500">starting equity</span>
          <div className="flex items-center gap-1">
            <span className="text-[11px] text-zinc-500">$</span>
            <input type="number" min={10} step={50} value={startBalance} disabled={disabled}
              onChange={(e) => {
                const v = Math.max(10, Number(e.target.value) || 0);
                onStartBalanceChange(v);
              }}
              className="w-20 border border-zinc-800 bg-black px-2 py-0.5 text-right text-[11px] tabular-nums text-zinc-100 focus:border-emerald-500/50 focus:outline-none disabled:opacity-40" />
          </div>
        </div>
        <div className="flex gap-1">
          {presets.map((p) => (
            <button key={p} disabled={disabled} onClick={() => onStartBalanceChange(p)}
              className={`flex-1 border px-1 py-1 text-[9px] tabular-nums tracking-wider transition-colors disabled:opacity-40 ${
                startBalance === p ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-400" : "border-zinc-800 text-zinc-500 hover:bg-zinc-900"
              }`}>${p}</button>
          ))}
        </div>
      </div>

      {fields.map((f) => (
        <div key={f.key} className="flex items-center justify-between px-3 py-2.5">
          <span className="text-[10px] uppercase tracking-[0.15em] text-zinc-500">{f.label}</span>
          <div className="flex items-center gap-2">
            <input type="range" min={f.min} max={f.max} step={f.step}
              value={params[f.key]} disabled={disabled}
              onChange={(e) => onChange({ ...params, [f.key]: Number(e.target.value) })}
              className="h-0.5 w-20 disabled:opacity-40" />
            <span className="w-16 text-right text-[11px] tabular-nums text-zinc-200">
              {params[f.key]}{f.key.includes("Pct") || f.key === "minPriceChange" ? "%" : ""}
            </span>
          </div>
        </div>
      ))}

      {/* Breakout toggle — checkbox style */}
      <div className="flex items-center justify-between px-3 py-2.5">
        <span className="text-[10px] uppercase tracking-[0.15em] text-zinc-500">
          require breakout
        </span>
        <button
          disabled={disabled}
          onClick={() => onChange({ ...params, requireBreakout: !params.requireBreakout })}
          className={`flex h-4 w-8 items-center px-0.5 transition-colors disabled:opacity-40 ${
            params.requireBreakout ? "bg-emerald-500/30" : "bg-zinc-800"
          }`}
        >
          <span
            className={`block h-3 w-3 transition-transform ${
              params.requireBreakout ? "translate-x-3.5 bg-emerald-400" : "bg-zinc-600"
            }`}
          />
        </button>
      </div>

      {underfunded && (
        <div className="border-t border-amber-900/40 bg-amber-950/20 px-3 py-2 text-[9px] uppercase tracking-[0.15em] text-amber-400/80">
          ⚠ size ${params.positionSizeUsd} &gt; equity ${startBalance} — no trades will open
        </div>
      )}

      {disabled && (
        <div className="px-3 py-2 text-[9px] uppercase tracking-[0.15em] text-amber-500/70">
          stop bot to edit params
        </div>
      )}
    </div>
  );
}

function PerfRow({ label, value, tone = "default" }) {
  const toneClass = tone === "up" ? "text-emerald-400"
                  : tone === "down" ? "text-red-400"
                  : tone === "amber" ? "text-amber-400"
                  : "text-zinc-200";
  return (
    <div className="flex items-center justify-between px-3 py-2.5">
      <span className="text-[10px] uppercase tracking-[0.15em] text-zinc-500">{label}</span>
      <span className={`text-[11px] tabular-nums ${toneClass}`}>{value}</span>
    </div>
  );
}
