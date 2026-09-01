// Binance Futures Order-Flow Scanner — 100% static, client-side app.
// Talks directly to fapi.binance.com and fstream.binance.com from the
// browser. Binance's public futures market-data REST endpoints send
// permissive CORS headers and the WebSocket streams need no auth at all,
// so this works with no backend. Nothing is sent to any server of mine;
// all settings live only in this browser's localStorage.
//
// WHAT THIS APP DOES
// 1. Once a day, ranks EVERY live USDT-margined PERPETUAL contract by its
//    average daily turnover (quote volume in USDT) over the last 30
//    completed days. The full ranked list is cached in localStorage; the
//    "Top N by turnover" box just slices the top N off that cached list,
//    so changing N is instant and never re-hits the network.
// 2. Maintains a LIVE local order book per symbol using Binance's
//    diff-depth websocket stream (the same method Binance's own docs
//    recommend): seed from a REST snapshot, then apply incremental
//    bid/ask updates as they arrive, checking sequence continuity and
//    silently resyncing on any gap. Every render tick it sums resting
//    quantity within ±band% of the current mid price on each side — the
//    "Depth Gap ×" column is max(buy,sell)/min(buy,sell), continuously
//    live, not a periodic poll.
// 3. Subscribes to the raw trade websocket stream for the same symbols and
//    keeps a short rolling trade log, classifying every trade as
//    buy-taker or sell-taker, to show live buy:sell executed-quantity
//    ratios over the last 10s / 30s / 60s / 5m.
// 4. FORWARD-TEST (PAPER) TRADING — simulated only, no real orders are ever
//    placed. Every render tick, with no trade currently open, every tracked
//    symbol is checked against: Depth Gap × below a threshold (default 2.5),
//    AND all four Flow windows pointing the same direction (all buy-heavy /
//    green, or all sell-heavy / red) AND each at least a threshold ×
//    (default 1.4). Among symbols that pass, the one with the highest
//    AVERAGE of its four Flow multipliers wins (ties broken by the lower
//    Depth Gap ×). A green pick opens a simulated long, a red pick a
//    simulated short, at that instant's mid price. TP/SL are fixed % moves
//    from entry (default 0.3% each), checked every tick against the live
//    price; the trade closes itself the moment either is touched. Only one
//    trade is open at a time — while one is open, the main table shows just
//    its top 10 rows (everything else keeps computing in the background so
//    the next pick is instant once the trade closes), and closed trades
//    accumulate in the Forward-Test Trade Log below it.
//
// RATE LIMITS: Binance USDT-M futures REST has a 2400-request-weight/min
// budget per IP. The 30-day-turnover ranking costs ~1 weight per symbol
// (klines, limit<=100) for ~300-400 symbols, once a day. Order-book
// snapshots (limit=1000, weight 20) are only fetched once per symbol at
// startup (and occasionally again on a resync), paced to ~1 every 700ms
// so the one-time seeding burst never comes close to the budget. After
// that, both the order book and the trade flow update for free over the
// websocket push streams — no further REST polling at all.

const FAPI = 'https://fapi.binance.com';
const WS_BASE = 'wss://fstream.binance.com/stream';

const LS = {
  ranking: 'bfscan-ranking-v2', // { dateKey, symbols: [{symbol, avgTurnover}] } — ALL eligible symbols, sorted desc
  topN: 'bfscan-topn-v1',
  activeTrade: 'bfscan-active-trade-v1',
  tradeLog: 'bfscan-trade-log-v1',
  tradingEnabled: 'bfscan-trading-enabled-v1',
};

const DEFAULT_TOP_N = 100;
const MAX_TOP_N = 300;
const KLINES_DAYS = 30;
const KLINES_CONCURRENCY = 8;
const RENDER_INTERVAL_MS = 500; // both depth-gap and trade-flow columns recompute this often
const TRADE_WINDOW_MS = 305000; // keep a little more than 5min for the widest ratio window
const WS_CHUNK_SIZE = 15; // symbols per websocket connection (2 streams/symbol -> 30 streams/conn)
const DEPTH_STREAM_SPEED = '100ms'; // fastest update speed Binance offers for diff depth
const SNAPSHOT_LIMIT = 1000; // max depth REST snapshot size (weight 20)
const SNAPSHOT_PACE_MS = 700; // one snapshot request roughly every 700ms -> ~1700 weight/min, safe
const PRUNE_MULTIPLIER = 3; // keep book levels out to (band% * this) away from mid, drop the rest
const TRADE_LOG_MAX = 200; // cap stored closed trades so localStorage doesn't grow unbounded
const DISPLAY_LIMIT_WHILE_TRADING = 10;

const $ = (sel) => document.querySelector(sel);
const els = {
  wsDot: $('#wsDot'),
  wsLabel: $('#wsLabel'),
  universeLabel: $('#universeLabel'),
  depthLabel: $('#depthLabel'),
  topNInput: $('#topNInput'),
  applyTopNBtn: $('#applyTopNBtn'),
  bandPct: $('#bandPct'),
  minMultiplier: $('#minMultiplier'),
  pauseBtn: $('#pauseBtn'),
  rebuildBtn: $('#rebuildBtn'),
  resultsBody: $('#resultsBody'),
  buildProgress: $('#buildProgress'),
  lastUpdate: $('#lastUpdate'),
  table: $('#resultsTable'),
  topbar: document.querySelector('.topbar'),
  controlsBar: document.querySelector('.controls'),
  tradeActiveNote: $('#tradeActiveNote'),
  tradeMaxDepth: $('#tradeMaxDepth'),
  tradeMinFlow: $('#tradeMinFlow'),
  tradeTpPct: $('#tradeTpPct'),
  tradeSlPct: $('#tradeSlPct'),
  tradeToggleBtn: $('#tradeToggleBtn'),
  tradeForceCloseBtn: $('#tradeForceCloseBtn'),
  tradeLogBody: $('#tradeLogBody'),
};

// ---------------------------------------------------------------------------
// sticky header layout — the topbar and controls bar heights change (extra
// controls wrapping to a second line on narrower windows, status text
// changing length, etc.), so the sticky offsets for the controls bar and
// the table header are measured live instead of hard-coded. Without this
// the table heading drifts out of position / overlaps whenever the bars
// above it change height.
// ---------------------------------------------------------------------------

function updateStickyOffsets() {
  const topbarH = els.topbar ? els.topbar.getBoundingClientRect().height : 0;
  const controlsH = els.controlsBar ? els.controlsBar.getBoundingClientRect().height : 0;
  document.documentElement.style.setProperty('--topbar-h', `${topbarH}px`);
  document.documentElement.style.setProperty('--sticky-h', `${topbarH + controlsH}px`);
}

if (typeof ResizeObserver !== 'undefined') {
  const ro = new ResizeObserver(() => updateStickyOffsets());
  if (els.topbar) ro.observe(els.topbar);
  if (els.controlsBar) ro.observe(els.controlsBar);
} else {
  window.addEventListener('resize', updateStickyOffsets);
}
window.addEventListener('load', updateStickyOffsets);
updateStickyOffsets();

// ---------------------------------------------------------------------------
// state
// ---------------------------------------------------------------------------

let fullRanking = []; // [{symbol, avgTurnover}] ALL eligible symbols, sorted desc
let universe = []; // [{symbol, avgTurnover}] top N slice currently in play (what's DISPLAYED/ranked)
let trackedSymbols = []; // symbols actually subscribed over websocket — universe, plus an in-flight
                          // trade's symbol even if it has since fallen out of the top-N slice, so an
                          // open forward-test trade always keeps getting live prices to close itself.
let rows = new Map(); // symbol -> row state
let sockets = [];
let paused = false;
let sortKey = 'multiplier';
let sortDir = 'desc';
let rowEls = new Map(); // symbol -> <tr>
let snapshotQueue = [];
let snapshotQueued = new Set();
let snapshotTimer = null;

// --- forward-test (paper) trading state ---
let tradingEnabled = true;
let activeTrade = null; // { id, symbol, side:'long'|'short', entryPrice, entryTime, tp, sl, tpPct, slPct,
                         //   depthMultAtEntry, avgFlowAtEntry, currentPrice, pnlPct, exitPrice?, exitTime?, exitReason? }
let tradeLog = []; // closed trades, newest first

function todayKey() {
  return new Date().toISOString().slice(0, 10);
}

function newBook() {
  return {
    bids: new Map(), // price(number) -> qty(number)
    asks: new Map(),
    lastUpdateId: 0,
    synced: false,
    buffer: [],
  };
}

function emptyRow(symbol) {
  return {
    symbol,
    book: newBook(),
    mid: null,
    buyQty: 0,
    sellQty: 0,
    multiplier: 1,
    heavySide: 'neutral',
    depthUpdatedAt: 0,
    trades: [], // {t, qty, side} side: 'buy' | 'sell'
  };
}

// ---------------------------------------------------------------------------
// step 1: rank ALL eligible symbols by 30-day avg turnover (once a day)
// ---------------------------------------------------------------------------

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText} for ${url}`);
  return res.json();
}

async function fetchExchangeInfo() {
  const data = await fetchJson(`${FAPI}/fapi/v1/exchangeInfo`);
  return data.symbols.filter(
    (s) => s.contractType === 'PERPETUAL' && s.quoteAsset === 'USDT' && s.status === 'TRADING'
  ).map((s) => s.symbol);
}

async function fetchAvgTurnover(symbol) {
  // Fetch one extra candle and drop the last (still-forming) one so the
  // average only covers fully completed trading days.
  const kl = await fetchJson(
    `${FAPI}/fapi/v1/klines?symbol=${symbol}&interval=1d&limit=${KLINES_DAYS + 1}`
  );
  const completed = kl.slice(0, -1);
  if (completed.length === 0) return 0;
  const sum = completed.reduce((acc, c) => acc + parseFloat(c[7]), 0); // index 7 = quote asset volume
  return sum / completed.length;
}

async function buildRanking(onProgress) {
  const symbols = await fetchExchangeInfo();
  const results = [];
  let done = 0;
  let idx = 0;

  async function worker() {
    while (idx < symbols.length) {
      const mySymbol = symbols[idx++];
      try {
        const avgTurnover = await fetchAvgTurnover(mySymbol);
        results.push({ symbol: mySymbol, avgTurnover });
      } catch (e) {
        // skip symbols that fail (e.g. newly listed with <30 days history)
      }
      done++;
      if (onProgress) onProgress(done, symbols.length);
    }
  }

  await Promise.all(Array.from({ length: KLINES_CONCURRENCY }, worker));
  results.sort((a, b) => b.avgTurnover - a.avgTurnover);
  return results;
}

async function loadRanking() {
  const cached = JSON.parse(localStorage.getItem(LS.ranking) || 'null');
  if (cached && cached.dateKey === todayKey() && Array.isArray(cached.symbols) && cached.symbols.length > 0) {
    fullRanking = cached.symbols;
    els.buildProgress.textContent = '';
    return;
  }
  els.buildProgress.textContent = 'Ranking all futures by 30-day turnover: 0%';
  const built = await buildRanking((done, total) => {
    els.buildProgress.textContent = `Ranking all futures by 30-day turnover: ${Math.round((done / total) * 100)}% (${done}/${total})`;
  });
  fullRanking = built;
  localStorage.setItem(LS.ranking, JSON.stringify({ dateKey: todayKey(), symbols: built }));
  els.buildProgress.textContent = '';
}

function readTopNInput() {
  let n = parseInt(els.topNInput.value, 10);
  if (!Number.isFinite(n) || n < 1) n = DEFAULT_TOP_N;
  n = Math.min(n, MAX_TOP_N, fullRanking.length || MAX_TOP_N);
  els.topNInput.value = n;
  return n;
}

// ---------------------------------------------------------------------------
// step 2: live local order book via diff-depth websocket
// ---------------------------------------------------------------------------

function applyDepthEvent(book, event) {
  for (const [priceStr, qtyStr] of event.b) {
    const price = parseFloat(priceStr);
    const qty = parseFloat(qtyStr);
    if (qty === 0) book.bids.delete(price); else book.bids.set(price, qty);
  }
  for (const [priceStr, qtyStr] of event.a) {
    const price = parseFloat(priceStr);
    const qty = parseFloat(qtyStr);
    if (qty === 0) book.asks.delete(price); else book.asks.set(price, qty);
  }
}

function queueSnapshot(symbol) {
  if (snapshotQueued.has(symbol)) return;
  snapshotQueued.add(symbol);
  snapshotQueue.push(symbol);
}

function startSnapshotPump() {
  if (snapshotTimer) clearInterval(snapshotTimer);
  snapshotTimer = setInterval(async () => {
    if (paused || snapshotQueue.length === 0) return;
    const symbol = snapshotQueue.shift();
    snapshotQueued.delete(symbol);
    const row = rows.get(symbol);
    if (!row) return;
    try {
      const snap = await fetchJson(`${FAPI}/fapi/v1/depth?symbol=${symbol}&limit=${SNAPSHOT_LIMIT}`);
      applySnapshot(row, snap);
    } catch (e) {
      // retry later
      queueSnapshot(symbol);
    }
  }, SNAPSHOT_PACE_MS);
}

function applySnapshot(row, snap) {
  const book = row.book;
  book.bids = new Map(snap.bids.map(([p, q]) => [parseFloat(p), parseFloat(q)]).filter(([, q]) => q > 0));
  book.asks = new Map(snap.asks.map(([p, q]) => [parseFloat(p), parseFloat(q)]).filter(([, q]) => q > 0));
  book.lastUpdateId = snap.lastUpdateId;

  // Bridge buffered live events onto the snapshot per Binance's documented
  // procedure: drop stale events, find the first event that straddles the
  // snapshot's lastUpdateId, then apply everything from there in order.
  const buffered = book.buffer.filter((e) => e.u > book.lastUpdateId);
  book.buffer = [];
  let bridged = false;
  for (const e of buffered) {
    if (!bridged) {
      if (e.U <= book.lastUpdateId + 1 && e.u >= book.lastUpdateId + 1) {
        bridged = true;
      } else {
        continue;
      }
    }
    applyDepthEvent(book, e);
    book.lastUpdateId = e.u;
  }
  book.synced = true;
}

function handleDepthEvent(row, event) {
  const book = row.book;
  if (!book.synced) {
    book.buffer.push(event);
    if (book.buffer.length > 3000) book.buffer.shift(); // guard against runaway buffering
    return;
  }
  if (event.pu !== book.lastUpdateId) {
    // Sequence gap — resync from a fresh snapshot rather than risk a wrong book.
    book.synced = false;
    book.bids = new Map();
    book.asks = new Map();
    book.buffer = [event];
    queueSnapshot(row.symbol);
    return;
  }
  applyDepthEvent(book, event);
  book.lastUpdateId = event.u;
}

function computeBandAndPrune(row, bandPct) {
  const book = row.book;
  let bestBid = -Infinity;
  let bestAsk = Infinity;
  for (const p of book.bids.keys()) if (p > bestBid) bestBid = p;
  for (const p of book.asks.keys()) if (p < bestAsk) bestAsk = p;
  if (!isFinite(bestBid) || !isFinite(bestAsk)) return false;

  const mid = (bestBid + bestAsk) / 2;
  const lower = mid * (1 - bandPct / 100);
  const upper = mid * (1 + bandPct / 100);
  const pruneLower = mid * (1 - (bandPct * PRUNE_MULTIPLIER) / 100);
  const pruneUpper = mid * (1 + (bandPct * PRUNE_MULTIPLIER) / 100);

  let buyQty = 0;
  for (const [p, q] of book.bids) {
    if (p < pruneLower) { book.bids.delete(p); continue; }
    if (p >= lower) buyQty += q;
  }
  let sellQty = 0;
  for (const [p, q] of book.asks) {
    if (p > pruneUpper) { book.asks.delete(p); continue; }
    if (p <= upper) sellQty += q;
  }

  row.mid = mid;
  row.buyQty = buyQty;
  row.sellQty = sellQty;
  row.multiplier = buyQty === 0 || sellQty === 0
    ? (buyQty === sellQty ? 1 : Infinity)
    : Math.max(buyQty, sellQty) / Math.min(buyQty, sellQty);
  row.heavySide = sellQty > buyQty ? 'sell' : buyQty > sellQty ? 'buy' : 'neutral';
  row.depthUpdatedAt = Date.now();
  return true;
}

// ---------------------------------------------------------------------------
// step 3: live trade flow via websocket trade streams
// ---------------------------------------------------------------------------

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function closeSockets() {
  sockets.forEach((ws) => {
    ws.onclose = null;
    ws.onerror = null;
    try { ws.close(); } catch (e) { /* ignore */ }
  });
  sockets = [];
}

function setWsStatus(state, label) {
  els.wsDot.className = `dot dot-${state}`;
  els.wsLabel.textContent = label;
}

function connectStreams() {
  closeSockets();
  if (trackedSymbols.length === 0) return;
  const chunks = chunk(trackedSymbols.map((s) => s.toLowerCase()), WS_CHUNK_SIZE);
  let openCount = 0;
  setWsStatus('pending', 'connecting…');

  chunks.forEach((symbolsChunk) => {
    const streams = symbolsChunk
      .flatMap((s) => [`${s}@trade`, `${s}@depth@${DEPTH_STREAM_SPEED}`])
      .join('/');
    const url = `${WS_BASE}?streams=${streams}`;
    const ws = new WebSocket(url);
    sockets.push(ws);

    ws.onopen = () => {
      openCount++;
      if (openCount === chunks.length) setWsStatus('on', `live (${trackedSymbols.length} symbols)`);
    };

    ws.onmessage = (evt) => {
      let msg;
      try { msg = JSON.parse(evt.data); } catch (e) { return; }
      const d = msg && msg.data;
      if (!d || !d.s) return;
      const row = rows.get(d.s);
      if (!row) return;

      if (d.e === 'trade' || d.e === 'aggTrade') {
        const side = d.m ? 'sell' : 'buy'; // m=true: buyer is maker -> taker sold
        // Use the LOCAL receipt time (not Binance's d.T) for windowing. The
        // 10s/30s/60s buckets are compared against Date.now() at render
        // time, so if this browser's system clock is even slightly off
        // from Binance's server clock, comparing against d.T can make
        // every trade look instantly "too old" (or, less obviously,
        // permanently "in the future") and the Flow columns would show
        // nothing at all even though trades are arriving correctly. Local
        // receipt time keeps the whole window self-consistent regardless
        // of clock skew; network latency here is a few tens of ms, far
        // smaller than the 10s window it could ever affect.
        row.trades.push({ t: Date.now(), qty: parseFloat(d.q), side });
      } else if (d.e === 'depthUpdate') {
        handleDepthEvent(row, d);
      }
    };

    ws.onerror = () => { /* onclose handles reconnect */ };

    ws.onclose = () => {
      setWsStatus('off', 'reconnecting…');
      setTimeout(() => {
        if (!paused) connectStreams();
      }, 3000);
    };
  });
}

function pruneTrades(row, now) {
  if (row.trades.length === 0) return;
  const cutoff = now - TRADE_WINDOW_MS;
  let i = 0;
  while (i < row.trades.length && row.trades[i].t < cutoff) i++;
  if (i > 0) row.trades.splice(0, i);
}

function flowRatio(row, now, windowMs) {
  const cutoff = now - windowMs;
  let buy = 0, sell = 0;
  for (let i = row.trades.length - 1; i >= 0; i--) {
    const tr = row.trades[i];
    if (tr.t < cutoff) break;
    if (tr.side === 'buy') buy += tr.qty; else sell += tr.qty;
  }
  return { buy, sell };
}

// ---------------------------------------------------------------------------
// rendering
// ---------------------------------------------------------------------------

function fmtNum(n, digits = 4) {
  if (n === null || n === undefined || !isFinite(n)) return '—';
  if (Math.abs(n) >= 1000) return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
  return n.toLocaleString(undefined, { maximumFractionDigits: digits });
}

function fmtMultiplier(m) {
  if (!isFinite(m)) return '∞×';
  return `${m.toFixed(2)}×`;
}

function fmtRatioPill(buy, sell) {
  if (buy === 0 && sell === 0) return { text: '—', cls: 'pill-neutral' };
  if (sell === 0) return { text: `${fmtNum(buy, 2)} : 0`, cls: 'pill-buy' };
  if (buy === 0) return { text: `0 : ${fmtNum(sell, 2)}`, cls: 'pill-sell' };
  const ratio = buy / sell;
  const cls = ratio >= 1 ? 'pill-buy' : 'pill-sell';
  const mult = ratio >= 1 ? ratio : sell / buy;
  const label = ratio >= 1 ? `B ${mult.toFixed(2)}×` : `S ${mult.toFixed(2)}×`;
  return { text: label, cls };
}

function buildRow(symbol) {
  const tr = document.createElement('tr');
  tr.dataset.symbol = symbol;
  tr.innerHTML = `
    <td class="col-rank"></td>
    <td class="col-symbol symbol-cell"></td>
    <td class="col-num c-price"></td>
    <td class="col-num c-buyqty"></td>
    <td class="col-num c-sellqty"></td>
    <td class="col-num c-mult"></td>
    <td class="col-num c-r10"></td>
    <td class="col-num c-r30"></td>
    <td class="col-num c-r60"></td>
    <td class="col-num c-r300"></td>
  `;
  return tr;
}

// Computed for the FULL ranked universe every tick, regardless of trade
// state or display filters — this is the "keep calculating rest in the
// background" data source both the table and the trading logic read from.
function computeRows(now, bandPct) {
  return universe.map((u) => {
    const row = rows.get(u.symbol) || emptyRow(u.symbol);
    computeBandAndPrune(row, bandPct);
    const r10 = flowRatio(row, now, 10000);
    const r30 = flowRatio(row, now, 30000);
    const r60 = flowRatio(row, now, 60000);
    const r300 = flowRatio(row, now, 300000);
    return {
      symbol: u.symbol,
      price: row.mid,
      buyQty: row.buyQty,
      sellQty: row.sellQty,
      multiplier: row.multiplier,
      heavySide: row.heavySide,
      synced: row.book.synced,
      depthAgeMs: row.depthUpdatedAt ? now - row.depthUpdatedAt : Infinity,
      r10, r30, r60, r300,
    };
  });
}

// Display-only filtering (Min gap multiplier box) and sorting — kept
// separate from computeRows() so the trading logic below always sees every
// tracked symbol regardless of what the user has filtered the table to.
function filterAndSortRows(allRows) {
  const minMult = parseFloat(els.minMultiplier.value) || 1;
  const list = allRows.filter((r) => !(isFinite(r.multiplier) && r.multiplier < minMult));

  list.sort((a, b) => {
    let av = a[sortKey], bv = b[sortKey];
    if (sortKey === 'symbol') { av = a.symbol; bv = b.symbol; }
    if (av === bv) return 0;
    if (av === null || av === undefined || !isFinite(av)) return 1;
    if (bv === null || bv === undefined || !isFinite(bv)) return -1;
    if (typeof av === 'string') return sortDir === 'desc' ? bv.localeCompare(av) : av.localeCompare(bv);
    return sortDir === 'desc' ? bv - av : av - bv;
  });

  return list;
}

// ---------------------------------------------------------------------------
// forward-test (paper) trading
// ---------------------------------------------------------------------------

// Classifies one Flow window's {buy, sell} pair into a side + multiplier,
// the same way the table's pills do (fmtRatioPill), so the trading logic's
// notion of "green"/"red" always matches what's on screen.
function windowSideAndMult(r) {
  if (!r) return null;
  const { buy, sell } = r;
  if (buy === 0 && sell === 0) return null; // no trades in this window at all — can't qualify
  if (buy > sell) return { side: 'buy', mult: sell === 0 ? Infinity : buy / sell };
  if (sell > buy) return { side: 'sell', mult: buy === 0 ? Infinity : sell / buy };
  return { side: 'neutral', mult: 1 };
}

// A candidate qualifies only if: the local order book is synced, the Depth
// Gap × is below maxDepth, AND all four Flow windows agree with the depth's
// heavy side (all green or all red — never a mix) AND each Flow window's ×
// is at least minFlow. Returns null if any condition fails.
function evaluateCandidate(r, maxDepth, minFlow) {
  if (!r.synced || !r.price) return null;
  if (!isFinite(r.multiplier) || r.multiplier >= maxDepth) return null;
  const dir = r.heavySide; // 'buy' (green) or 'sell' (red) — 'neutral' can't qualify
  if (dir !== 'buy' && dir !== 'sell') return null;

  const mults = [];
  for (const w of [r.r10, r.r30, r.r60, r.r300]) {
    const ws = windowSideAndMult(w);
    if (!ws || ws.side !== dir) return null;
    if (!(ws.mult >= minFlow)) return null;
    mults.push(isFinite(ws.mult) ? ws.mult : 1e6); // cap Infinity so the average stays a sane number
  }
  const avgFlow = mults.reduce((a, b) => a + b, 0) / mults.length;
  return { symbol: r.symbol, dir, depthMult: r.multiplier, avgFlow, price: r.price };
}

// Scans every tracked symbol (not just what's currently displayed) and, if
// anything qualifies, opens the single best candidate: highest average Flow
// × wins, ties broken by the lower Depth Gap ×.
function tryOpenTrade(allRows) {
  if (activeTrade) return;
  const maxDepth = parseFloat(els.tradeMaxDepth.value) || 2.5;
  const minFlow = parseFloat(els.tradeMinFlow.value) || 1.4;

  let best = null;
  for (const r of allRows) {
    const c = evaluateCandidate(r, maxDepth, minFlow);
    if (!c) continue;
    if (
      !best ||
      c.avgFlow > best.avgFlow ||
      (c.avgFlow === best.avgFlow && c.depthMult < best.depthMult)
    ) {
      best = c;
    }
  }
  if (best) openTrade(best);
}

function openTrade(candidate) {
  const tpPct = parseFloat(els.tradeTpPct.value) || 0.3;
  const slPct = parseFloat(els.tradeSlPct.value) || 0.3;
  const entryPrice = candidate.price;
  const side = candidate.dir === 'buy' ? 'long' : 'short';
  const tp = side === 'long' ? entryPrice * (1 + tpPct / 100) : entryPrice * (1 - tpPct / 100);
  const sl = side === 'long' ? entryPrice * (1 - slPct / 100) : entryPrice * (1 + slPct / 100);

  activeTrade = {
    id: `${candidate.symbol}-${Date.now()}`,
    symbol: candidate.symbol,
    side,
    entryPrice,
    entryTime: Date.now(),
    tpPct, slPct, tp, sl,
    depthMultAtEntry: candidate.depthMult,
    avgFlowAtEntry: candidate.avgFlow,
    currentPrice: entryPrice,
    pnlPct: 0,
  };

  // Make sure this symbol keeps streaming even if a future top-N re-slice
  // or ranking rebuild would otherwise have dropped it.
  if (!trackedSymbols.includes(candidate.symbol)) {
    trackedSymbols = [...trackedSymbols, candidate.symbol];
    if (!rows.has(candidate.symbol)) rows.set(candidate.symbol, emptyRow(candidate.symbol));
    queueSnapshot(candidate.symbol);
    connectStreams();
  }

  els.tradeForceCloseBtn.disabled = false;
  saveTradeState();
}

// Checks the live price against TP/SL every tick and updates live PnL —
// this is what makes the open row in the trade log update in real time.
function updateActiveTrade() {
  if (!activeTrade) return;
  const row = rows.get(activeTrade.symbol);
  const price = row ? row.mid : null;
  if (!price) return;

  activeTrade.currentPrice = price;
  activeTrade.pnlPct = activeTrade.side === 'long'
    ? ((price - activeTrade.entryPrice) / activeTrade.entryPrice) * 100
    : ((activeTrade.entryPrice - price) / activeTrade.entryPrice) * 100;

  if (activeTrade.side === 'long') {
    if (price >= activeTrade.tp) { closeTrade('TP'); return; }
    if (price <= activeTrade.sl) { closeTrade('SL'); return; }
  } else {
    if (price <= activeTrade.tp) { closeTrade('TP'); return; }
    if (price >= activeTrade.sl) { closeTrade('SL'); return; }
  }
}

function closeTrade(reason) {
  if (!activeTrade) return;
  const closed = {
    ...activeTrade,
    exitPrice: activeTrade.currentPrice,
    exitTime: Date.now(),
    exitReason: reason, // 'TP' | 'SL' | 'MANUAL'
  };
  tradeLog.unshift(closed);
  if (tradeLog.length > TRADE_LOG_MAX) tradeLog.length = TRADE_LOG_MAX;
  activeTrade = null;
  els.tradeForceCloseBtn.disabled = true;
  saveTradeState();
}

function saveTradeState() {
  try {
    localStorage.setItem(LS.activeTrade, activeTrade ? JSON.stringify(activeTrade) : '');
    localStorage.setItem(LS.tradeLog, JSON.stringify(tradeLog));
  } catch (e) {
    // localStorage full/unavailable — trading still works for this session, just won't persist
  }
}

function loadTradeState() {
  try {
    const at = localStorage.getItem(LS.activeTrade);
    if (at) activeTrade = JSON.parse(at);
    const tl = localStorage.getItem(LS.tradeLog);
    if (tl) tradeLog = JSON.parse(tl) || [];
    const te = localStorage.getItem(LS.tradingEnabled);
    if (te !== null) tradingEnabled = te === 'true';
  } catch (e) {
    // corrupt/unavailable state — start fresh
  }
}

function fmtPnl(pct) {
  if (pct === null || pct === undefined || !isFinite(pct)) return '—';
  const cls = pct > 0.0005 ? 'pnl-pos' : pct < -0.0005 ? 'pnl-neg' : 'pnl-flat';
  const sign = pct > 0 ? '+' : '';
  return `<span class="${cls}">${sign}${pct.toFixed(3)}%</span>`;
}

function fmtTime(ts) {
  return ts ? new Date(ts).toLocaleTimeString() : '—';
}

function statusBadge(trade) {
  if (!trade.exitReason) return '<span class="status-open">OPEN</span>';
  if (trade.exitReason === 'TP') return '<span class="status-tp">TP HIT</span>';
  if (trade.exitReason === 'SL') return '<span class="status-sl">SL HIT</span>';
  return '<span class="status-manual">CLOSED</span>';
}

function renderTradeLog() {
  const list = activeTrade ? [activeTrade, ...tradeLog] : tradeLog;
  if (list.length === 0) {
    els.tradeLogBody.innerHTML = '<tr class="empty-row"><td colspan="11">No trades yet — waiting for a symbol to qualify.</td></tr>';
    return;
  }
  els.tradeLogBody.innerHTML = list.map((t, i) => {
    const isOpen = !t.exitReason;
    const sideBadge = t.side === 'long'
      ? '<span class="pill pill-buy">LONG</span>'
      : '<span class="pill pill-sell">SHORT</span>';
    return `<tr class="${isOpen ? 'trade-row-open' : ''}">
      <td class="col-rank">${i + 1}</td>
      <td class="col-symbol symbol-cell">${t.symbol.replace(/USDT$/, '')}</td>
      <td class="col-num">${sideBadge}</td>
      <td class="col-num">${fmtNum(t.entryPrice, 4)}</td>
      <td class="col-num">${fmtNum(t.tp, 4)}</td>
      <td class="col-num">${fmtNum(t.sl, 4)}</td>
      <td class="col-num">${fmtNum(isOpen ? t.currentPrice : t.exitPrice, 4)}</td>
      <td class="col-num">${fmtPnl(t.pnlPct)}</td>
      <td class="col-num">${statusBadge(t)}</td>
      <td class="col-num">${fmtTime(t.entryTime)}</td>
      <td class="col-num">${isOpen ? '—' : fmtTime(t.exitTime)}</td>
    </tr>`;
  }).join('');
}

function render() {
  const now = Date.now();
  const bandPct = parseFloat(els.bandPct.value) || 0.5;

  // Always compute every tracked symbol — this keeps running in full even
  // while a trade is open and the table below is only showing 10 rows.
  const allRows = computeRows(now, bandPct);

  updateActiveTrade();
  if (tradingEnabled && !activeTrade) tryOpenTrade(allRows);

  const sorted = filterAndSortRows(allRows);
  const list = activeTrade ? sorted.slice(0, DISPLAY_LIMIT_WHILE_TRADING) : sorted;

  if (activeTrade) {
    els.tradeActiveNote.hidden = false;
    els.tradeActiveNote.textContent =
      `Trade active on ${activeTrade.symbol.replace(/USDT$/, '')} (${activeTrade.side.toUpperCase()}) — showing top ${DISPLAY_LIMIT_WHILE_TRADING} rows only; all ${allRows.length} tracked symbols keep updating in the background.`;
  } else {
    els.tradeActiveNote.hidden = true;
  }

  if (list.length === 0) {
    els.resultsBody.innerHTML = '<tr class="empty-row"><td colspan="10">No symbols match the current filters.</td></tr>';
    rowEls.clear();
  } else {
    // Rebuild from scratch each tick rather than only appending: while a
    // trade is active the displayed set shrinks to 10 rows, and once it
    // closes it grows back — leftover rows from a previous, larger list
    // would otherwise stick around since appendChild only moves nodes
    // that ARE in the new fragment, it doesn't remove ones that aren't.
    while (els.resultsBody.firstChild) els.resultsBody.removeChild(els.resultsBody.firstChild);
    const frag = document.createDocumentFragment();
    list.forEach((r, i) => {
      let tr = rowEls.get(r.symbol);
      if (!tr) {
        tr = buildRow(r.symbol);
        rowEls.set(r.symbol, tr);
      }
      tr.querySelector('.col-rank').textContent = i + 1;
      tr.querySelector('.col-symbol').textContent = r.symbol.replace(/USDT$/, '');
      tr.querySelector('.c-price').textContent = r.price ? fmtNum(r.price, 4) : '—';
      tr.querySelector('.c-buyqty').textContent = fmtNum(r.buyQty, 2);
      tr.querySelector('.c-sellqty').textContent = fmtNum(r.sellQty, 2);
      const multCell = tr.querySelector('.c-mult');
      multCell.innerHTML = r.synced
        ? `<span class="pill ${r.heavySide === 'sell' ? 'pill-sell' : r.heavySide === 'buy' ? 'pill-buy' : 'pill-neutral'}">${r.heavySide === 'sell' ? 'S' : r.heavySide === 'buy' ? 'B' : '—'} ${fmtMultiplier(r.multiplier)}</span>`
        : `<span class="pill pill-neutral">syncing…</span>`;
      const p10 = fmtRatioPill(r.r10.buy, r.r10.sell);
      const p30 = fmtRatioPill(r.r30.buy, r.r30.sell);
      const p60 = fmtRatioPill(r.r60.buy, r.r60.sell);
      const p300 = fmtRatioPill(r.r300.buy, r.r300.sell);
      tr.querySelector('.c-r10').innerHTML = `<span class="pill ${p10.cls}">${p10.text}</span>`;
      tr.querySelector('.c-r30').innerHTML = `<span class="pill ${p30.cls}">${p30.text}</span>`;
      tr.querySelector('.c-r60').innerHTML = `<span class="pill ${p60.cls}">${p60.text}</span>`;
      tr.querySelector('.c-r300').innerHTML = `<span class="pill ${p300.cls}">${p300.text}</span>`;
      tr.classList.toggle('stale', !r.synced);
      frag.appendChild(tr);
    });
    els.resultsBody.appendChild(frag);
    const totalSynced = allRows.filter((r) => r.synced).length;
    els.depthLabel.textContent = activeTrade
      ? `order book: live, ${totalSynced}/${allRows.length} synced (background)`
      : `order book: live, ${totalSynced}/${allRows.length} synced`;
  }

  els.universeLabel.textContent = `universe: ${universe.length} symbols`;
  els.lastUpdate.textContent = `updated ${new Date(now).toLocaleTimeString()}`;
  renderTradeLog();
}

function renderTick() {
  if (paused) return;
  const now = Date.now();
  rows.forEach((row) => pruneTrades(row, now));
  render();
}

// ---------------------------------------------------------------------------
// sorting header clicks
// ---------------------------------------------------------------------------

els.table.querySelectorAll('th.sortable').forEach((th) => {
  th.addEventListener('click', () => {
    const key = th.dataset.key;
    if (sortKey === key) {
      sortDir = sortDir === 'desc' ? 'asc' : 'desc';
    } else {
      sortKey = key;
      sortDir = 'desc';
    }
    els.table.querySelectorAll('th').forEach((h) => h.classList.remove('sorted-desc', 'sorted-asc'));
    th.classList.add(sortDir === 'desc' ? 'sorted-desc' : 'sorted-asc');
    render();
  });
});

// ---------------------------------------------------------------------------
// controls
// ---------------------------------------------------------------------------

els.pauseBtn.addEventListener('click', () => {
  paused = !paused;
  els.pauseBtn.textContent = paused ? 'Resume' : 'Pause';
  if (paused) {
    closeSockets();
    setWsStatus('off', 'paused');
  } else {
    connectStreams();
  }
});

els.rebuildBtn.addEventListener('click', async () => {
  els.rebuildBtn.disabled = true;
  localStorage.removeItem(LS.ranking);
  await startup();
  els.rebuildBtn.disabled = false;
});

els.applyTopNBtn.addEventListener('click', () => {
  applyTopN();
});
els.topNInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') applyTopN();
});

els.minMultiplier.addEventListener('input', render);
els.bandPct.addEventListener('change', render);

function setTradeToggleUI() {
  els.tradeToggleBtn.textContent = `Forward-test: ${tradingEnabled ? 'ON' : 'OFF'}`;
  els.tradeToggleBtn.classList.toggle('btn-primary', tradingEnabled);
  els.tradeToggleBtn.classList.toggle('btn-ghost', !tradingEnabled);
}

els.tradeToggleBtn.addEventListener('click', () => {
  tradingEnabled = !tradingEnabled;
  localStorage.setItem(LS.tradingEnabled, String(tradingEnabled));
  setTradeToggleUI();
});

els.tradeForceCloseBtn.addEventListener('click', () => {
  if (activeTrade) closeTrade('MANUAL');
});

function applyTopN() {
  const n = readTopNInput();
  localStorage.setItem(LS.topN, String(n));
  universe = fullRanking.slice(0, n);

  // Keep tracking an in-flight forward-test trade's symbol even if it falls
  // out of the top-N slice (e.g. after a reload with a smaller N, or the
  // daily ranking rebuild), so it keeps getting live prices to close itself.
  // It won't show as a ranked row in the main table, but stays live for the
  // trade log and TP/SL checking.
  const symbolSet = new Set(universe.map((u) => u.symbol));
  if (activeTrade && activeTrade.symbol) symbolSet.add(activeTrade.symbol);
  trackedSymbols = Array.from(symbolSet);

  rows = new Map(trackedSymbols.map((s) => [s, emptyRow(s)]));
  rowEls.clear();
  els.resultsBody.innerHTML = '<tr class="empty-row"><td colspan="10">Connecting…</td></tr>';
  snapshotQueue = [];
  snapshotQueued = new Set();
  trackedSymbols.forEach((s) => queueSnapshot(s));
  connectStreams();
  render();
}

// ---------------------------------------------------------------------------
// startup
// ---------------------------------------------------------------------------

async function startup() {
  try {
    await loadRanking();
  } catch (e) {
    els.buildProgress.textContent = `Failed to build turnover ranking: ${e.message}. Retrying in 10s…`;
    setTimeout(() => startup(), 10000);
    return;
  }

  const savedN = parseInt(localStorage.getItem(LS.topN), 10);
  els.topNInput.value = Number.isFinite(savedN) && savedN > 0 ? savedN : DEFAULT_TOP_N;
  applyTopN();
}

loadTradeState();
setTradeToggleUI();
els.tradeForceCloseBtn.disabled = !activeTrade;

startSnapshotPump();
setInterval(renderTick, RENDER_INTERVAL_MS);

startup();
