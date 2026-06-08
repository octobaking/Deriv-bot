// ================================================================
//  OCTOBER KING — DERIV MULTIPLIER BOT v2.0
//  Indices : V10–V100 (+ 1s variants), Step Index, XAUUSD
//  Strategy: ICT | BOS | FVG | OB | Liquidity | Momentum |
//            Reversion | Session Breakout | Fibonacci |
//            RSI Divergence | Round Number | Inside Bar
//  Telegram: Live trade alerts & daily summary
//  RR: 1:3  |  Stake: $5  |  Multiplier: x10  |  24/7
// ================================================================

const WebSocket = require('ws');
const https     = require('https');

// ===================== ⚙ CONFIG — EDIT HERE =====================
const CONFIG = {
  APP_ID:    '1089',
  API_TOKEN: 'YOUR_DERIV_API_TOKEN',   // ← Deriv API token

  TELEGRAM: {
    TOKEN:   'YOUR_TELEGRAM_BOT_TOKEN', // ← from @BotFather
    CHAT_ID: 'YOUR_TELEGRAM_CHAT_ID'    // ← your chat/group ID
  },

  STAKE:       5,
  TP_AMOUNT:   15,   // 1:3 RR
  SL_AMOUNT:   5,
  MIN_SIGNALS: 3,    // strategies that must agree (generic instruments)
  MIN_SIGNALS_GOLD: 3, // for XAUUSD (has more strategies)
  CANDLE_COUNT: 60,
  GRANULARITY:  60,  // 1-minute candles
  PING_INTERVAL: 25000, // keep-alive ping every 25s

  SYMBOLS: [
    { symbol: 'R_10',      name: 'V10',        multiplier: 10, type: 'volatility' },
    { symbol: '1HZ10V',    name: 'V10 (1s)',   multiplier: 10, type: 'volatility' },
    { symbol: 'R_25',      name: 'V25',        multiplier: 10, type: 'volatility' },
    { symbol: '1HZ25V',    name: 'V25 (1s)',   multiplier: 10, type: 'volatility' },
    { symbol: 'R_50',      name: 'V50',        multiplier: 10, type: 'volatility' },
    { symbol: '1HZ50V',    name: 'V50 (1s)',   multiplier: 10, type: 'volatility' },
    { symbol: 'R_75',      name: 'V75',        multiplier: 10, type: 'volatility' },
    { symbol: '1HZ75V',    name: 'V75 (1s)',   multiplier: 10, type: 'volatility' },
    { symbol: 'R_100',     name: 'V100',       multiplier: 10, type: 'volatility' },
    { symbol: '1HZ100V',   name: 'V100 (1s)',  multiplier: 10, type: 'volatility' },
    { symbol: 'stpRNG',    name: 'Step Index', multiplier: 10, type: 'step'       },
    { symbol: 'frxXAUUSD', name: 'XAUUSD',     multiplier: 10, type: 'gold'       },
  ]
};
// ================================================================

// ==================== STATE ====================
let ws               = null;
let pingInterval     = null;
let reconnectAttempts = 0;
let totalPnL         = 0;
let tradeCount       = 0;
let winCount         = 0;
let lossCount        = 0;
const MAX_RECONNECTS = 999; // infinite retries
const candleData     = {};
const openTrades     = {};

CONFIG.SYMBOLS.forEach(s => {
  candleData[s.symbol] = [];
  openTrades[s.symbol] = false;
});

// ==================== LOGGER ====================
function log(msg) {
  const t = new Date().toLocaleTimeString('en-KE', { timeZone: 'Africa/Nairobi' });
  console.log(`[${t}] ${msg}`);
}

// ================== TELEGRAM ====================
function sendTelegram(msg) {
  const { TOKEN, CHAT_ID } = CONFIG.TELEGRAM;
  if (!TOKEN || TOKEN === 'YOUR_TELEGRAM_BOT_TOKEN') return;
  const text = encodeURIComponent(msg);
  const url  = `https://api.telegram.org/bot${TOKEN}/sendMessage?chat_id=${CHAT_ID}&text=${text}&parse_mode=HTML`;
  https.get(url).on('error', err => log('Telegram err: ' + err.message));
}

// ==================== STRATEGIES ====================

// --- GENERIC (all instruments) ---

function detectBOS(c) {
  if (c.length < 15) return null;
  const body = c.slice(c.length - 15, c.length - 1);
  const hi   = Math.max(...body.map(x => x.high));
  const lo   = Math.min(...body.map(x => x.low));
  const last = c[c.length - 1];
  if (last.close > hi) return 'BUY';
  if (last.close < lo) return 'SELL';
  return null;
}

function detectFVG(c) {
  if (c.length < 3) return null;
  const len = c.length;
  const c1  = c[len - 3], c3 = c[len - 1];
  if (c3.low  > c1.high) return 'BUY';
  if (c3.high < c1.low)  return 'SELL';
  return null;
}

function detectOrderBlock(c) {
  if (c.length < 3) return null;
  const len  = c.length;
  const ob   = c[len - 2];
  const imp  = c[len - 1];
  const body = Math.abs(imp.close - imp.open) / ((imp.high - imp.low) || 1);
  if (body < 0.55) return null;
  if (ob.close < ob.open && imp.close > imp.open && imp.close > ob.high) return 'BUY';
  if (ob.close > ob.open && imp.close < imp.open && imp.close < ob.low)  return 'SELL';
  return null;
}

function detectLiquiditySweep(c) {
  if (c.length < 12) return null;
  const len      = c.length;
  const lookback = c.slice(len - 12, len - 1);
  const last     = c[len - 1];
  const hi       = Math.max(...lookback.map(x => x.high));
  const lo       = Math.min(...lookback.map(x => x.low));
  if (last.high > hi && last.close < hi) return 'SELL';
  if (last.low  < lo && last.close > lo) return 'BUY';
  return null;
}

function detectMomentum(c) {
  if (c.length < 7) return null;
  const recent = c.slice(c.length - 7);
  let bull = 0, bear = 0;
  for (let i = 1; i < recent.length; i++) {
    recent[i].close > recent[i - 1].close ? bull++ : bear++;
  }
  if (bull >= 5) return 'BUY';
  if (bear >= 5) return 'SELL';
  return null;
}

function detectBreakoutRetest(c) {
  if (c.length < 20) return null;
  const len  = c.length;
  const base = c.slice(len - 20, len - 8);
  const rec  = c.slice(len - 8);
  const bHi  = Math.max(...base.map(x => x.high));
  const bLo  = Math.min(...base.map(x => x.low));
  const last = rec[rec.length - 1];
  const prev = rec[rec.length - 2];
  const tol  = 0.001;
  const brokeBull  = rec.some(x => x.close > bHi);
  const retestBull = rec.some(x => x.low  <= bHi * (1 + tol) && x.low  >= bHi * (1 - tol));
  if (brokeBull  && retestBull && last.close > prev.close) return 'BUY';
  const brokeBear  = rec.some(x => x.close < bLo);
  const retestBear = rec.some(x => x.high >= bLo * (1 - tol) && x.high <= bLo * (1 + tol));
  if (brokeBear  && retestBear && last.close < prev.close) return 'SELL';
  return null;
}

function detectMeanReversion(c) {
  if (c.length < 20) return null;
  const recent = c.slice(c.length - 20);
  const sma    = recent.reduce((s, x) => s + x.close, 0) / 20;
  const last   = c[c.length - 1].close;
  const dev    = ((last - sma) / sma) * 100;
  if (dev >  1.5) return 'SELL';
  if (dev < -1.5) return 'BUY';
  return null;
}

// --- XAUUSD SPECIFIC STRATEGIES ---

// RSI helper
function calcRSI(c, period = 14) {
  if (c.length < period + 1) return null;
  const closes = c.slice(-(period + 1)).map(x => x.close);
  let gains = 0, losses = 0;
  for (let i = 1; i < closes.length; i++) {
    const d = closes[i] - closes[i - 1];
    d > 0 ? (gains += d) : (losses += Math.abs(d));
  }
  const ag = gains / period;
  const al = losses / period;
  if (al === 0) return 100;
  return 100 - (100 / (1 + ag / al));
}

// 1. RSI Divergence
function detectRSIDivergence(c) {
  if (c.length < 22) return null;
  const len    = c.length;
  const midC   = c.slice(len - 22, len - 11);
  const recC   = c.slice(len - 11);
  const midRSI = calcRSI(midC, 7);
  const recRSI = calcRSI(recC, 7);
  if (!midRSI || !recRSI) return null;
  const midLo  = Math.min(...midC.map(x => x.low));
  const recLo  = Math.min(...recC.map(x => x.low));
  const midHi  = Math.max(...midC.map(x => x.high));
  const recHi  = Math.max(...recC.map(x => x.high));
  // Bullish divergence: lower price low, higher RSI low
  if (recLo < midLo && recRSI > midRSI && recRSI < 42) return 'BUY';
  // Bearish divergence: higher price high, lower RSI high
  if (recHi > midHi && recRSI < midRSI && recRSI > 58) return 'SELL';
  return null;
}

// 2. Fibonacci 61.8% Retracement
function detectFibonacci(c) {
  if (c.length < 30) return null;
  const len      = c.length;
  const lookback = c.slice(len - 30, len - 5);
  const last     = c[len - 1];
  const hi       = Math.max(...lookback.map(x => x.high));
  const lo       = Math.min(...lookback.map(x => x.low));
  const range    = hi - lo;
  if (range === 0) return null;
  const fib618Bull = hi - range * 0.618;
  const fib618Bear = lo + range * 0.618;
  const tol        = range * 0.04;
  if (Math.abs(last.close - fib618Bull) < tol && last.close > last.open) return 'BUY';
  if (Math.abs(last.close - fib618Bear) < tol && last.close < last.open) return 'SELL';
  return null;
}

// 3. London / New York Session Breakout
function detectSessionBreakout(c) {
  if (c.length < 20) return null;
  const utcH     = new Date().getUTCHours();
  // London: 06–12 UTC | New York: 12–17 UTC
  const inSession = (utcH >= 6 && utcH < 17);
  if (!inSession) return null;
  const len      = c.length;
  const base     = c.slice(len - 20, len - 3);
  const recent   = c.slice(len - 3);
  const baseHi   = Math.max(...base.map(x => x.high));
  const baseLo   = Math.min(...base.map(x => x.low));
  const last     = recent[recent.length - 1];
  if (last.close > baseHi && last.close > last.open) return 'BUY';
  if (last.close < baseLo && last.close < last.open) return 'SELL';
  return null;
}

// 4. Round Number / Key Level ($50 intervals on Gold)
function detectRoundNumber(c) {
  if (c.length < 3) return null;
  const last    = c[c.length - 1];
  const price   = last.close;
  const nearest = Math.round(price / 50) * 50;
  const dist    = Math.abs(price - nearest) / price * 100;
  if (dist > 0.12) return null; // only within 0.12% of key level
  if (last.close > last.open) return 'BUY';
  if (last.close < last.open) return 'SELL';
  return null;
}

// 5. Inside Bar Breakout (compression → explosive move)
function detectInsideBar(c) {
  if (c.length < 3) return null;
  const len    = c.length;
  const mother = c[len - 2];
  const inside = c[len - 1];
  const isInside = inside.high < mother.high && inside.low > mother.low;
  if (!isInside) return null;
  if (mother.close > mother.open) return 'BUY';
  if (mother.close < mother.open) return 'SELL';
  return null;
}

// 6. Asian Range Breakout (00:00–06:00 UTC = consolidation)
function detectAsianRangeBreakout(c) {
  if (c.length < 25) return null;
  const utcH = new Date().getUTCHours();
  // Only trade breakout after Asian session closes (after 6 UTC)
  if (utcH < 6 || utcH > 20) return null;
  const len    = c.length;
  const asian  = c.slice(len - 25, len - 5);
  const recent = c.slice(len - 5);
  const asianHi = Math.max(...asian.map(x => x.high));
  const asianLo = Math.min(...asian.map(x => x.low));
  const last    = recent[recent.length - 1];
  if (last.close > asianHi && last.close > last.open) return 'BUY';
  if (last.close < asianLo && last.close < last.open) return 'SELL';
  return null;
}

// ==================== SIGNAL ENGINE ====================

function getSignal(candles, symbolObj) {
  let results;

  if (symbolObj.type === 'gold') {
    // XAUUSD — all 13 strategies
    results = [
      detectBOS(candles),
      detectFVG(candles),
      detectOrderBlock(candles),
      detectLiquiditySweep(candles),
      detectMomentum(candles),
      detectBreakoutRetest(candles),
      detectMeanReversion(candles),
      detectRSIDivergence(candles),
      detectFibonacci(candles),
      detectSessionBreakout(candles),
      detectRoundNumber(candles),
      detectInsideBar(candles),
      detectAsianRangeBreakout(candles),
    ];
  } else {
    // Volatility / Step — 7 generic strategies
    results = [
      detectBOS(candles),
      detectFVG(candles),
      detectOrderBlock(candles),
      detectLiquiditySweep(candles),
      detectMomentum(candles),
      detectBreakoutRetest(candles),
      detectMeanReversion(candles),
    ];
  }

  const buyVotes  = results.filter(r => r === 'BUY').length;
  const sellVotes = results.filter(r => r === 'SELL').length;
  const minSig    = symbolObj.type === 'gold' ? CONFIG.MIN_SIGNALS_GOLD : CONFIG.MIN_SIGNALS;

  if (buyVotes  >= minSig) return 'BUY';
  if (sellVotes >= minSig) return 'SELL';
  return null;
}

// ================== TRADE EXECUTION ====================

function placeTrade(symbolObj, direction) {
  const { symbol, name, multiplier } = symbolObj;
  if (openTrades[symbol]) {
    log(`⚠  ${name}: position open — skipped`);
    return;
  }

  const contractType = direction === 'BUY' ? 'MULTUP' : 'MULTDOWN';
  safeSend({
    buy: 1,
    price: CONFIG.STAKE,
    parameters: {
      amount:        CONFIG.STAKE,
      basis:         'stake',
      contract_type: contractType,
      currency:      'USD',
      symbol:        symbol,
      multiplier:    multiplier,
      limit_order: {
        stop_loss:   CONFIG.SL_AMOUNT,
        take_profit: CONFIG.TP_AMOUNT
      }
    }
  });

  openTrades[symbol] = true;
  tradeCount++;
  const msg = `🚀 <b>TRADE OPENED</b>\n📌 ${name}\n📊 ${direction}\n💵 $${CONFIG.STAKE} × ${multiplier}\n🎯 TP: $${CONFIG.TP_AMOUNT}  |  🛑 SL: $${CONFIG.SL_AMOUNT}\n📈 Total Trades: ${tradeCount}`;
  log(`🚀 ${name} | ${direction} | $${CONFIG.STAKE}×${multiplier} | TP:$${CONFIG.TP_AMOUNT} SL:$${CONFIG.SL_AMOUNT}`);
  sendTelegram(msg);

  // Safety fallback: release lock after 4 hours
  setTimeout(() => {
    if (openTrades[symbol]) {
      openTrades[symbol] = false;
      log(`⏰ ${name}: lock released (4h timeout)`);
    }
  }, 4 * 60 * 60 * 1000);
}

// ==================== WEBSOCKET ====================

function connect() {
  const url = `wss://ws.binaryws.com/websockets/v3?app_id=${CONFIG.APP_ID}`;
  ws = new WebSocket(url);

  ws.on('open', () => {
    log('✅ Connected to Deriv');
    reconnectAttempts = 0;
    startPing();
    authorize();
  });

  ws.on('message', raw => {
    try { handleMessage(JSON.parse(raw)); }
    catch (e) { log('Parse error: ' + e.message); }
  });

  ws.on('close', () => {
    log('🔌 Disconnected');
    stopPing();
    scheduleReconnect();
  });

  ws.on('error', err => {
    log('❌ WS Error: ' + err.message);
    ws.terminate();
  });
}

function scheduleReconnect() {
  reconnectAttempts++;
  const delay = Math.min(reconnectAttempts * 3000, 30000); // max 30s
  log(`🔄 Reconnect #${reconnectAttempts} in ${delay / 1000}s`);
  setTimeout(connect, delay);
}

function startPing() {
  pingInterval = setInterval(() => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      safeSend({ ping: 1 });
    }
  }, CONFIG.PING_INTERVAL);
}

function stopPing() {
  if (pingInterval) clearInterval(pingInterval);
}

function safeSend(payload) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(payload));
  } else {
    log('⚠ safeSend skipped — not connected');
  }
}

function authorize() {
  safeSend({ authorize: CONFIG.API_TOKEN });
}

function subscribeCandles(symbol) {
  safeSend({
    ticks_history:     symbol,
    adjust_start_time: 1,
    count:             CONFIG.CANDLE_COUNT,
    end:               'latest',
    granularity:       CONFIG.GRANULARITY,
    style:             'candles',
    subscribe:         1
  });
}

// ==================== MESSAGE HANDLER ====================

function handleMessage(msg) {
  if (msg.error) {
    log(`⚠ [${msg.msg_type || '?'}] ${msg.error.message}`);
    const sym = msg.echo_req?.parameters?.symbol;
    if (sym) openTrades[sym] = false;
    return;
  }

  switch (msg.msg_type) {

    case 'authorize': {
      const auth = msg.authorize;
      log(`✅ Auth OK | ${auth.email} | Balance: $${auth.balance}`);
      const startMsg = `🤖 <b>OCTOBER KING BOT STARTED</b>\n👤 ${auth.email}\n💰 Balance: $${auth.balance}\n📡 Watching ${CONFIG.SYMBOLS.length} instruments\n⚡ Strategy: ICT + BOS + FVG + OB + Gold Suite`;
      sendTelegram(startMsg);
      // Stagger subscriptions to avoid rate limits
      CONFIG.SYMBOLS.forEach((s, i) => {
        setTimeout(() => subscribeCandles(s.symbol), i * 700);
      });
      break;
    }

    case 'candles': {
      const sym = msg.echo_req.ticks_history;
      candleData[sym] = msg.candles.map(c => ({
        open:  parseFloat(c.open),
        high:  parseFloat(c.high),
        low:   parseFloat(c.low),
        close: parseFloat(c.close),
        epoch: c.epoch
      }));
      log(`📊 ${sym}: ${candleData[sym].length} candles loaded`);
      break;
    }

    case 'ohlc': {
      const o      = msg.ohlc;
      const sym    = o.symbol;
      const candle = {
        open:  parseFloat(o.open),
        high:  parseFloat(o.high),
        low:   parseFloat(o.low),
        close: parseFloat(o.close),
        epoch: o.epoch
      };
      const data = candleData[sym];
      const last = data[data.length - 1];

      if (last && last.epoch === candle.epoch) {
        data[data.length - 1] = candle; // update live candle
      } else {
        data.push(candle);
        if (data.length > 120) data.shift(); // rolling 120 candles

        // Analyze only on new confirmed candle close
        if (data.length >= 22 && !openTrades[sym]) {
          const symObj = CONFIG.SYMBOLS.find(x => x.symbol === sym);
          const signal = getSignal(data, symObj);
          if (signal) {
            log(`🎯 Signal | ${symObj.name} | ${signal}`);
            placeTrade(symObj, signal);
          }
        }
      }
      break;
    }

    case 'buy': {
      const b = msg.buy;
      log(`✅ Opened | ID: ${b.contract_id} | Entry: ${b.buy_price}`);
      // Track contract to detect close
      safeSend({
        proposal_open_contract: 1,
        contract_id: b.contract_id,
        subscribe: 1
      });
      break;
    }

    case 'proposal_open_contract': {
      const poc = msg.proposal_open_contract;
      if (poc.status === 'sold' || poc.is_expired || poc.is_settleable) {
        const sym    = poc.underlying;
        const profit = parseFloat(poc.profit || 0);
        openTrades[sym] = false;
        totalPnL += profit;
        profit >= 0 ? winCount++ : lossCount++;
        const emoji = profit >= 0 ? '🟢' : '🔴';
        const name  = CONFIG.SYMBOLS.find(x => x.symbol === sym)?.name || sym;
        const wr    = tradeCount > 0 ? ((winCount / (winCount + lossCount)) * 100).toFixed(1) : 0;
        log(`${emoji} Closed | ${name} | P&L: $${profit.toFixed(2)} | Total: $${totalPnL.toFixed(2)} | WR: ${wr}%`);
        const tgMsg = `${emoji} <b>TRADE CLOSED</b>\n📌 ${name}\n💰 P&L: <b>$${profit.toFixed(2)}</b>\n📊 Total P&L: $${totalPnL.toFixed(2)}\n🏆 Win Rate: ${wr}%\n✅ Wins: ${winCount}  ❌ Losses: ${lossCount}`;
        sendTelegram(tgMsg);
      }
      break;
    }

    case 'ping':
      // keep-alive confirmed, no action needed
      break;
  }
}

// ============== DAILY SUMMARY (midnight EAT) ==============
function scheduleDailySummary() {
  const now    = new Date();
  const eat    = new Date(now.toLocaleString('en-US', { timeZone: 'Africa/Nairobi' }));
  const msTo12 = ((23 - eat.getHours()) * 3600 + (59 - eat.getMinutes()) * 60 + (60 - eat.getSeconds())) * 1000;
  setTimeout(() => {
    const wr  = (winCount + lossCount) > 0 ? ((winCount / (winCount + lossCount)) * 100).toFixed(1) : 0;
    const msg = `📋 <b>DAILY SUMMARY</b>\n📅 ${new Date().toLocaleDateString('en-KE')}\n\n💰 Total P&L: <b>$${totalPnL.toFixed(2)}</b>\n📈 Trades: ${tradeCount}\n✅ Wins: ${winCount}  ❌ Losses: ${lossCount}\n🏆 Win Rate: ${wr}%\n\n🤖 October King Bot — Running 24/7`;
    sendTelegram(msg);
    log('📋 Daily summary sent');
    // Reset daily counters
    totalPnL = 0; tradeCount = 0; winCount = 0; lossCount = 0;
    scheduleDailySummary(); // reschedule for next day
  }, msTo12);
}

// ===================== START =======================
log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
log('   🤖 OCTOBER KING BOT v2.0');
log('   Instruments : 12');
log('   Strategies  : 13');
log('   Telegram    : Active');
log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
connect();
scheduleDailySummary();
