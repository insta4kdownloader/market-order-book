# Binance Futures Order-Flow Scanner (static PWA, for GitHub Pages)

A fully static, no-backend PWA (companion to your NSE Volume & Gap Scanner) that watches **Binance USDT-M futures**:

1. Ranks **every live USDT-margined perpetual contract** by its **average daily turnover (USDT) over the last 30 completed days**, once a day. The **"Top N by turnover"** box lets you pick how many of the top-ranked symbols to actually track (default 100) — changing it just re-slices the cached ranking, no re-fetch needed.
2. Maintains a **live local order book** for every tracked symbol via Binance's diff-depth websocket stream (seeded from a REST snapshot, then updated tick by tick — the same method Binance's own docs recommend), and continuously sums resting quantity within **±0.5%** (configurable) of the current mid price on the **buy side** and the **sell side**. Rows are sorted by **Depth Gap ×** — `max(buy, sell) / min(buy, sell)` — so a symbol where sell-side resting quantity is 3× buy-side shows `S 3.00×` at the top. This is genuinely live: no polling interval, the book updates as fast as Binance pushes it (up to every 100ms) and the table redraws twice a second.
3. Streams **live executed trades** (the raw `@trade` stream — see note below on why not `@aggTrade`) for the same symbols and shows the **buy:sell executed-quantity ratio** over the last **10 seconds**, **30 seconds**, **60 seconds**, and **5 minutes**, also refreshed twice a second.
4. Runs a **forward-test (paper) trading loop** on top of all of that — see its own section below. It only ever simulates trades in your browser; it never places a real order.

There is no server. Every file here is static HTML/CSS/JS meant to be pushed straight to GitHub Pages (or opened locally). The page calls `fapi.binance.com` (REST, only for the daily ranking and one-time order-book snapshots) and `fstream.binance.com` (WebSocket, for everything live) **directly from your browser** — Binance's public futures market-data endpoints send permissive CORS headers and the streams need no authentication, so this works with no backend proxy and no API key.

## 1. Run it

No login, no credentials — just serve the folder and open it.

**Windows (PowerShell), if you have Python installed:**
```
cd path\to\webapp
python -m http.server 8000
```
Then open `http://127.0.0.1:8000/`.

**Or with Node.js:**
```
npx serve -l 8000
```

**Or publish to GitHub Pages** the same way as the NSE scanner: push this folder to a repo, enable Pages on it, open the resulting URL. No build step.

## 2. Using it

- **Top N by turnover** — how many of the highest-turnover symbols to track (default 100, max 300). Type a number and click **Apply top N** (or press Enter). Instant — it re-slices the already-ranked list and reconnects the live streams for the new set, no new ranking fetch.
- **Depth band (±%)** — how far from the current mid price to sum resting order-book quantity on each side. Defaults to 0.5%, matching the original ask; takes effect on the very next render tick (every 0.5s), since the whole book is already live in memory.
- **Min gap multiplier** — hide rows below this Depth Gap × (e.g. set to `2` to only show symbols where one side has at least 2x the other).
- **Pause / Resume** — disconnects the websockets entirely (useful to freeze the table while you read it, or to cut data usage).
- **Rebuild turnover ranking** — forces an immediate rebuild of the 30-day-turnover ranking instead of waiting for the daily auto-rebuild (useful right after a new contract lists).

Click any column header to re-sort by it (defaults to Depth Gap × descending). A row shows "syncing…" in the Depth Gap column for the second or two it takes to seed that symbol's local order book after startup or a reconnect.

## Columns

| Column | Meaning |
|---|---|
| Symbol | Futures contract (USDT-margined perpetual) |
| Price | Current mid price (best bid + best ask) / 2, from the live local order book |
| Buy Qty (band) | Live resting bid quantity within the depth band below current price |
| Sell Qty (band) | Live resting ask quantity within the depth band above current price |
| Depth Gap × | `max(buy,sell)/min(buy,sell)`, with a `B`/`S` tag showing which side is heavier — this is the default sort |
| Flow 10s / 30s / 60s / 5m (B:S) | Of trades **executed** (not resting orders) in that trailing window, which side dominates and by how much — `B 2.10×` means buyer-initiated (taker-buy) quantity was 2.1x taker-sell quantity in that window |

The **30-day average turnover** ranking is still what decides which symbols make the "Top N" cut — it's just no longer shown as its own column, to keep the table focused on the live order-flow numbers.

## How the numbers are computed

```
avgTurnover30d   = mean(dailyQuoteVolume) over the 30 most recently completed daily candles
buyQty(band)     = sum of live resting bid quantity at prices >= mid * (1 - band%/100)
sellQty(band)    = sum of live resting ask quantity at prices <= mid * (1 + band%/100)
depthGapX        = max(buyQty, sellQty) / min(buyQty, sellQty)

For each executed trade (@trade stream):
  side = 'sell' if the trade's buyer was the resting maker (i.e. a market sell hit the bid)
  side = 'buy'  if the trade's buyer was the taker (i.e. a market buy hit the ask)

flowRatio(window) = sum(qty where side='buy' in last `window`) : sum(qty where side='sell' in last `window`)
```

### How the live order book stays correct

This follows Binance's documented procedure for maintaining a local order book from the diff-depth stream: open the websocket, buffer incoming events, fetch a REST snapshot (`limit=1000`), discard buffered events older than the snapshot, find the first buffered event that straddles the snapshot's `lastUpdateId`, and apply everything from there forward. Every subsequent event's `pu` (previous update ID) is checked against the last applied `u` — any mismatch means an event was missed, so that symbol's book is thrown away and silently re-seeded from a fresh snapshot. You'll never see a torn/inconsistent book; at worst a symbol briefly shows "syncing…" again.

**A known limitation**: Binance's depth snapshot caps out at 1000 price levels per side. For extremely liquid, tight-tick symbols (BTC, ETH) with very deep books, 1000 levels can sometimes fall short of a full 0.5% price range, in which case the band sums whatever levels are actually available. This is a hard ceiling in Binance's own API, not something this app can work around — numbers for such symbols may slightly under-count true 0.5% depth. Levels are pruned once they fall outside `band% × 3` of the mid price to keep memory bounded, so widening the band box picks up more levels again automatically (as long as Binance sent them).

### Why `@trade` instead of `@aggTrade` for the Flow columns

Binance also offers an "aggregate trade" stream (`@aggTrade`) that bundles trades matched against the same order at the same price/time into one event. It looks like the more efficient choice, and this app used it originally — but in testing it turned out to silently deliver **zero** messages in some network/regional conditions (confirmed by connecting directly to Binance's websocket: `@depth` and `@bookTicker` streamed hundreds of updates while `@aggTrade` produced nothing at all for the same symbol over the same window), which is exactly the failure mode that showed up as permanently blank Flow columns while the order book kept working fine. The plain `@trade` stream (one event per individual executed trade, slightly more data volume but functionally identical fields — `q` for quantity, `m` for buyer-is-maker) delivered data immediately and reliably in the same test, so the app now uses that instead.

## Forward-test (paper) trading

**This is a simulation only. No real order is ever placed on your Binance account — the app makes no authenticated API calls at all, so it has no way to trade even if it wanted to.** It's a way to watch, in real time, what would have happened if you'd taken every setup this scanner flags.

**Entry logic**, checked every render tick (twice a second) whenever no trade is open:

1. A symbol's **Depth Gap ×** must be below **"Entry: max depth ×"** (default `2.5`).
2. Its **Flow 10s, 30s, 60s, and 5m** columns must all point the **same direction** — all green (buy-heavy) or all red (sell-heavy). A symbol with, say, green 10s/30s/60s but red 5m does not qualify; the signal has to agree across every window.
3. Every one of those four Flow multipliers must be at least **"Entry: min flow ×"** (default `1.4`).
4. Among every symbol that clears all three bars, the one with the **highest average** of its four Flow multipliers is picked (ties broken by the **lowest** Depth Gap ×). A green pick opens a simulated **long**; a red pick opens a simulated **short**. The entry price is that instant's live mid price.

**Exit logic**: TP and SL are fixed percentage moves from the entry price — **"TP (%)"** and **"SL (%)"**, default `0.3%` each. Every tick, the live price is checked against both; whichever is touched first closes the trade (`TP HIT` or `SL HIT` in the log). You can also hit **"Force close open trade"** at any time to exit manually (logged as `CLOSED`).

**Only one trade is open at a time.** The moment a trade opens, the main table's rendered rows shrink to just that one symbol — a yellow banner above the table says so — but every tracked symbol keeps being scanned and scored in the background the whole time, so the next best candidate is ready to compare against the instant the open trade closes. As soon as it closes (TP, SL, or manual), the scanner immediately starts looking for the next qualifying setup — there's no cooldown, and the table goes back to showing the full ranked list.

**Forward-test: ON/OFF** toggles the whole loop; it defaults to ON, so trades can start opening as soon as data is live. Turning it OFF stops new entries from opening but does not close whatever trade is currently open — use "Force close" for that.

**The Forward-Test Trade Log** below the main table shows the currently open trade (highlighted, PnL updating live every tick) followed by up to 200 most-recent closed trades — entry/TP/SL/exit price, side, realized PnL %, how it closed, and when it opened/closed. **Nothing here is persisted.** The open trade and the whole log live only in memory for that page session — reload the page and both are gone; the scanner starts with a clean slate and begins looking for a fresh setup immediately. (Only the Forward-test ON/OFF toggle preference survives a reload.)

**Design choices worth knowing**: the "highest average Flow ×" ranking rewards a symbol where all four windows are strongly aligned, not just one window spiking briefly. PnL is quoted in price-percentage terms only (no leverage, fees, funding, or slippage modeled) — real fills would differ, sometimes significantly, especially around the exact TP/SL price on a fast-moving symbol. Treat this purely as a way to sanity-check the strategy's logic against live data, not as a return estimate.

## Rate limits and data-source notes

- **Turnover ranking**: one `klines` request per symbol (weight 1 each, since limit ≤ 100) for ~300–400 perpetual contracts — roughly 300–400 weight, done once a day with 8-way concurrency. Cached in `localStorage` so it only runs once per calendar day unless you click "Rebuild turnover ranking".
- **Order-book seeding**: one `GET /fapi/v1/depth?limit=1000` (weight 20) per tracked symbol, but only once at startup (and again, per-symbol, on the rare sequence-gap resync) — paced to roughly one request every 700ms (~1700 weight/minute), well inside Binance's 2400-weight/minute public budget. After seeding, the book updates for free over the websocket.
- **Live data**: one combined WebSocket connection per ~15 symbols, each carrying both `@trade` and `@depth@100ms` streams, reconnecting automatically with a 3s backoff if a connection drops. Zero REST weight cost — both the order book and the trade flow are push feeds.
- This is a personal research/monitoring tool, not investment advice, and no order is ever placed — everything here is read-only market data.

## File map

```
index.html        App shell / UI
app.js            Turnover ranking, live order-book (diff-depth) maintenance, websocket trade-flow, table rendering — all client-side
style.css         Styling
manifest.json     PWA manifest
sw.js             Service worker (caches the app shell only, never live market data)
icons/            App icons
```
