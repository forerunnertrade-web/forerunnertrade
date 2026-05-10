# Forerunner — Run Guide (macOS)

A two-part app for testing crypto launch-sniper strategies:

```
forerunner/
├── backend/    Python — on-chain scanner, paper-trading engine, auditor
└── frontend/   React + Vite — paper-trading dashboard ("Forerunner")
```

You can run them **independently**. For first-time strategy validation, run only the
frontend — it has a self-contained simulator. Add the backend when you want real
on-chain signals from Ethereum / Solana / SUI.

---

## 1. Prerequisites

Install once on your macbook:

| Tool      | Min version | How to install                                                      |
| --------- | ----------- | ------------------------------------------------------------------- |
| Python    | 3.10+       | `brew install python@3.12` (or use the python.org installer)        |
| Node.js   | 18+         | `brew install node` (or `nvm install 20`)                           |
| Git       | any         | comes with Xcode CLT — `xcode-select --install` if missing          |

Verify:

```bash
python3 --version    # 3.10 or higher
node --version       # v18 or higher
npm --version        # 9 or higher
```

**If you installed Python from python.org (not Homebrew):** run the cert installer
once, or every TLS connection from Python will fail with `CERTIFICATE_VERIFY_FAILED`:

```bash
/Applications/Python\ 3.12/Install\ Certificates.command
```

Apple Silicon (M1/M2/M3): everything below works natively on arm64. No Rosetta needed.

---

## 2. Quick start — frontend only (5 minutes)

This is the fastest path to validating your strategy. The dashboard runs a fully
local simulation: synthetic scanner signals, paper positions, equity curve.

```bash
cd forerunner/frontend
npm install
npm run dev
```

Open http://localhost:5173 → click **START** → watch positions open and close
according to your strategy params. Adjust the sliders, hit **RESET**, run again.

**What you can validate here:**
- Position sizing math
- Take-profit / stop-loss exits behave correctly
- Win-rate and expectancy under different RSI / volume thresholds
- Whether the strategy *logic* is profitable on synthetic data

**What you cannot validate here (needs real data):**
- Actual signal quality from Uniswap / Raydium / Cetus
- Slippage and MEV impact at thin LPs
- Honeypot and rug detection

---

## 3. Backend — scanner & paper-trading engine

```bash
cd forerunner/backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

### 3a. Smoke test (no RPC keys needed)

```bash
python main.py
```

If you haven't filled in any RPC URLs, every scanner will log a warning and exit
its loop. That's expected — it confirms imports work.

### 3b. Wiring real RPCs

Edit `.env` with at least one chain. Recommended providers:

| Chain    | Provider     | Free tier?         | Notes                                               |
| -------- | ------------ | ------------------ | --------------------------------------------------- |
| Ethereum | Alchemy      | yes                | dashboard.alchemy.com → create app → copy WS + HTTP |
| Solana   | Helius       | yes (limited)      | helius.dev — public mainnet-beta WS will throttle   |
| SUI      | sui.io       | yes (public node)  | rate-limited but workable for dev                   |

```dotenv
# .env example for Ethereum-only
ETH_ENABLED=true
ETH_WS_URL=wss://eth-mainnet.g.alchemy.com/v2/YOUR_KEY
ETH_HTTP_URL=https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY

SOL_ENABLED=false
SUI_ENABLED=false
```

Then:

```bash
python main.py
```

You should see new pool events log within 1–5 minutes during US/EU market hours.

### 3c. Telegram / Discord alerts (optional)

Fill in either or both in `.env`:

```dotenv
TELEGRAM_BOT_TOKEN=123456:ABC...
TELEGRAM_CHAT_ID=123456789
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
```

To get a Telegram chat ID: message your bot once, then visit
`https://api.telegram.org/botYOUR_TOKEN/getUpdates` and find `"chat":{"id":...}`.

### 3d. Local breakout detection (no TradingView needed)

Forerunner runs entirely locally. The breakout pattern that the original Pine
Script detected — RSI threshold + volume spike + breakout above prior high —
now runs in `backend/breakout.py` against the price samples collected during
the 30-second enrichment window. No TradingView account, no webhook, no cloud
round-trip.

The detector samples the pool's reserves about 10 times over the window
(every ~3 seconds), builds a price series, and answers: did the price
break out above its prior max with positive momentum? Result rides along
with the metrics broadcast — the dashboard shows a green sparkline + ⚡
icon when triggered.

**Strategy filter integration:** there's a `require breakout` toggle in
the strategy panel. When on, only signals whose detector triggered will
be eligible for paper trading. Useful when you want to ignore launches
that pump-then-dump within the sample window.

**Endpoints still exposed by main.py:**
- `GET  /health`     — liveness check
- `WS   /events`     — dashboard live signal feed
- `POST /tv-webhook` — *deprecated*, kept for users who still want a
  TradingView feed. The Pine Script in `backend/pine/` works as before
  if you wire ngrok and an alert. Most users won't need this.

### 3e. Live mode — connecting the dashboard to real on-chain signals

Once the backend is running and emitting events, the dashboard can consume
them directly. Open it (`npm run dev` in `frontend/`), click the **LIVE**
toggle in the header, and the WS status indicator should go amber → emerald
within a second.

**Two-phase signals.** Each new pool arrives in two parts:

1. **Phase 1 (instant)** — a pool was detected and passed the safety audit.
   Shows in the signals table with a clock icon and `—` placeholders for the
   metric columns. The strategy filter intentionally skips pending signals.

2. **Phase 2 (~30 seconds later)** — the enricher has measured the pool's
   first 30 seconds of activity. The signal updates in place: liquidity in
   USD, unique buyers, and price change all populate. The strategy filter
   evaluates the signal here.

**Why 30 seconds?** A brand-new pool has no price history. RSI is undefined
on a single sample. So instead of fake technical indicators, we measure
*launch dynamics* — the things you can actually see at t=0+30s: how many
distinct human buyers showed up, how much liquidity stuck around, whether
the price moved up or down. Bot launches show 30 buys from 1-2 wallets;
organic launches show 10-20 buys from 8+ wallets. The strategy filter on
the left panel can use those numbers directly.

**Strategy params:** the dashboard now filters on `min liquidity`,
`min unique buyers`, `min Δ% in 30s`, and `min audit score`. Default values
are calibrated against the synthetic archetype mix (organic / bot-pump /
dud) so you can tune them in synthetic mode and the same thresholds will
behave sensibly on real launches.

**Env vars for tuning:**

```dotenv
ENRICH_ENABLED=true              # set false to skip enrichment entirely
ENRICH_WINDOW_SECONDS=30         # sample window length
AUDIT_MIN_SCORE=70               # backend filters audited events below this
ETH_HTTP_URL=https://...          # required for EVM enrichment
SOL_HTTP_URL=https://...          # required for Solana enrichment (Helius recommended)
```

**Solana enrichment** decodes Raydium AMM v4 pool state, reads both vaults,
samples balances at t=0 and t=+30s. Reports liquidity in USD (via CoinGecko
SOL/USD), price change %, and final entry price. Buyer counts on Solana are
not yet implemented (would require parsing each transaction's instruction
bytes); those fields stay null and the strategy filter ignores them when
`min unique buyers = 0`. Set that threshold higher only when filtering EVM.

**Chain selector** (header, between SYNTHETIC/LIVE and WS status): three
buttons ETH / SOL / SUI. Click to toggle. At least one must stay enabled.
Disabled chains skip both synthetic generation and live WS signals. Useful
when you want to focus on one chain's launches without noise from others.

### 3f. DEXScreener trending feed (cross-check signal)

Polls DEXScreener every 60s for recently boosted tokens, pushed to a
**dexscreener trending** panel at the bottom of the dashboard. This is
**market-context, not actionable signal** — it shows what the broader
market is reacting to, useful as a cross-check against your scanner.

**What it gives you:**
- Visibility into BASE / BSC / Polygon / Arbitrum launches without
  wiring scanners for those chains
- USD-denominated liquidity, 24h volume, market cap, price change
- Boost count (how many promotions paid for visibility)
- Direct link to DEXScreener pair page

**What it does NOT give you:**
- Real-time launch detection — DEXScreener's indexer adds latency,
  and the boosts list is "trending" not "newest". For sniping, the
  on-chain scanners remain your primary path.
- Anything actionable for paper trading. Trending tokens are NOT
  fed into the strategy filter or paper-trade engine.

**Cross-highlight (the actual value-add).** When a token appears in *both*
the signals table AND the DEXScreener trending feed, both rows light up:

- Signals row: gets an orange tint + 🔥 next to the symbol — meaning
  "the on-chain scanner found this AND the broader market is reacting".
- Trending row: gets a green tint + ▸ before the symbol — meaning
  "DEXScreener has this AND your scanner already has it tagged".

Match logic is `chain + token_address` (case-insensitive). This is the
strongest *passive* confidence signal you'll get: you didn't choose to
filter on it, two independent indexers happened to agree.

**Tuning via env vars:**

```dotenv
DEXSCREENER_ENABLED=true
DEXSCREENER_INTERVAL_SECONDS=60
DEXSCREENER_CHAINS=ETH,SOL,BASE,BSC,POLY,ARB
```

Set `DEXSCREENER_ENABLED=false` to disable the poller entirely. Free tier
limit is 60 requests/min on this endpoint; default 60s interval uses ~1
request/min so you have a comfortable margin even if you cut it down.

**Synthetic mode now generates archetype-mixed signals** — about 30%
organic, 40% bot-pumps, 30% duds. So when you tune your filter in synthetic
mode and find it accepts ~30% of signals, that's a healthy calibration. If
it accepts everything or nothing, the thresholds are off.

---

## 4. Project layout

```
forerunner/
├── HELP.md                      ← you are here
│
├── backend/
│   ├── requirements.txt
│   ├── .env.example
│   ├── main.py                  scanner orchestrator + FastAPI server
│   ├── config.py                env-driven config (RPCs, factory addrs)
│   ├── auditor.py               pre-trade safety scoring dispatch
│   ├── auditor_evm.py           EVM-specific eth_call checks (honeypot, owner, LP lock)
│   ├── auditor_solana.py        SPL Mint decoder — checks mint+freeze authority revoked
│   ├── enricher.py              EVM launch dynamics over a 30s sample window
│   ├── enricher_solana.py       Solana / Raydium AMM v4 liquidity + price tracking
│   ├── breakout.py              local breakout detector (replaces TradingView/Pine Script)
│   ├── dexscreener.py           DEXScreener trending poller (cross-check feed)
│   ├── price_oracle.py          Chainlink ETH/USD + CoinGecko SOL/USD pricing
│   ├── alerts.py                Telegram + Discord + /tv-webhook + /events WS
│   ├── scanners/
│   │   ├── base.py              shared PoolEvent dataclass
│   │   ├── ethereum.py          Uniswap V2/V3 WS subscriber
│   │   ├── solana.py            Raydium/Orca logsSubscribe + tx replay
│   │   └── sui.py               Cetus/Turbos suix_subscribeEvent
│   └── pine/
│       └── momentum_breakout.pine
│
└── frontend/
    ├── package.json
    ├── vite.config.js
    ├── tailwind.config.js
    ├── postcss.config.js
    ├── index.html
    └── src/
        ├── main.jsx
        ├── App.jsx              the dashboard
        └── index.css
```

---

## 5. Troubleshooting

**`npm install` fails with EACCES on macOS**
You probably installed Node as root. Fix with `sudo chown -R $(whoami) ~/.npm` or
switch to nvm: `brew install nvm && nvm install 20`.

**`python main.py` → `ModuleNotFoundError: dotenv`**
You forgot to activate the venv. Run `source .venv/bin/activate` first; your shell
prompt should show `(.venv)`.

**`pip install web3` fails to compile on Apple Silicon**
Rare on Python 3.11+. If it happens: `pip install --upgrade pip setuptools wheel`,
then retry. As a fallback, `arch -arm64 pip install web3`.

**Vite says port 5173 is in use**
Either kill it (`lsof -ti :5173 | xargs kill`) or run `npm run dev -- --port 5174`.

**Scanner connects but never logs new pools**
Off-hours (UTC nights/weekends) you may wait 10–30 minutes between events. To
verify connectivity: tail logs and look for the "subscription ack" line. If you
see that, the WS is healthy and you're just waiting for activity.

**Public Solana WS throttles you**
This is universal — `wss://api.mainnet-beta.solana.com` rate-limits aggressively.
Use Helius or Triton. The free Helius tier handles personal use comfortably.

**SUI WS reconnect loop**
The public fullnode drops connections every ~10 minutes. The scanner's
exponential backoff handles this. If you need stability, run your own SUI node
or use a paid endpoint (BlockVision, Shinami).

---

## 6. Going from paper to live

When the synthetic dashboard shows a strategy that holds up:

1. Run the backend scanner against real RPCs for at least 48 hours.
2. Pipe its `PoolEvent` output into the paper engine (instead of synthesised
   signals) and replay for another 48 hours. The hook is `on_pool()` in `main.py`.
3. Wire the auditor stubs to your provider (Alchemy traces, Helius DAS).
4. Only then connect a real wallet — and start with size much smaller than
   what your paper P&L suggests is optimal. Slippage and MEV will eat 20–40%
   of paper edge on micro-cap launches; size accordingly.

The frontend dashboard does not currently connect to the backend over WebSocket.
That integration is intentionally left out so the harness stays runnable without
a backend. If you want me to wire it up next, ask.
