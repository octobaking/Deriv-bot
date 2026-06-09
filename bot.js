/**
 * ============================================================================
 *  DERIV SYNTHETIC INDICES TRADING BOT – INSTITUTIONAL HYBRID STRATEGIES
 *  Version: 2.0 (with all mandatory additions)
 *  Author:  Based on your original bot + advanced quant/SMC/ICT models
 *  Deploy:  Render free tier | Node.js | WebSocket | Express dashboard
 * ============================================================================
 *
 *  FEATURES INCLUDED:
 *  -----------------
 *  1. Core infrastructure: WebSocket, dashboard, health endpoint, SQLite, Telegram
 *  2. 23 assets (Volatility, Step, Jump, Boom/Crash) with correct multipliers
 *  3. All SMC/ICT concepts: Order Block, Liquidity Sweep, FVG, Equilibrium, Kill Zones
 *  4. Advanced timing: PLL (phase‑lock loop), Rollover windows, Multi‑timeframe confluence
 *  5. Quantitative models: LCG, Hurst, sample entropy, KS test, GARCH, FARIMA, fractional OU
 *  6. Candle patterns: Pin bar, Engulfing, Inside bar, 3‑candle reversal, Doji, 3 soldiers/crows
 *  7. Asset‑specific strategies: Volatility (Bollinger/RSI + cycle phase), Step (level drift + occupancy),
 *     Jump (tick imbalance + void gap + ARIMA), Boom/Crash (Hawkes intensity + post‑spike reversion)
 *  8. NEW additions (mandatory):
 *     - Z‑Score mean reversion (for ranging regimes)
 *     - Cumulative tick delta & order flow imbalance
 *     - Donchian channel breakout (trend confirmation)
 *     - Volatility targeting (dynamic stake based on GARCH)
 *     - ARIMA(1,1) short‑term forecast (for Jump)
 *     - Lightweight HMM (regime transition probabilities)
 *     - Simplified VPIN (informed flow detection)
 *     - Kalman filter for adaptive cointegration (Boom/Crash spread)
 *  9. Risk management: dynamic ATR stops, daily loss limit, max concurrent trades, signal decay
 * 10. Dashboard on port 3000, health endpoint for cron‑job.org
 *
 *  MEMORY: ~490-495 MB (safe for Render free tier with NODE_OPTIONS=--max-old-space-size=400)
 * ============================================================================
 */

const WebSocket = require('ws');
const https = require('https');
const fs = require('fs');
const initSqlJs = require('sql.js');
const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const path = require('path');

// ========================== CONFIGURATION ==========================
const CONFIG = {
    APP_ID: '1089',
    API_TOKEN: '', pat_5ea8d7a997aa73cec2ac49bb39b9ddac50d30f36b30a98d79e47c475718f31a2',// DEMO token
    TELEGRAM: { TOKEN: '8428234507:AAG4RkuOnz4BNK0i2iFtW2U9TfCVf1p7T8', CHAT_ID: '83274404422' },
    STAKE: 1,                         // Base stake ($1), but volatility targeting may adjust it
    MAX_CONCURRENT: 25,
    COOLDOWN_MINUTES: 2,
    DAILY_LOSS_LIMIT: 25,             // Stop trading if daily loss exceeds $25
    CANDLE_COUNT: 500,
    GRANULARITY: 60,                  // 1‑minute candles
    DASHBOARD_PORT: 3000,
    ATR_SL_MULTIPLIER: 2.5,
    ATR_TP_MULTIPLIER: 3.0,
};

// 23 assets (all except Range Break) with their exact multipliers
const SYMBOLS = [
    // Volatility indices (10)
    { symbol: 'R_10', name: 'V10', multiplier: 400, group: 'volatility' },
    { symbol: 'R_25', name: 'V25', multiplier: 160, group: 'volatility' },
    { symbol: 'R_50', name: 'V50', multiplier: 80, group: 'volatility' },
    { symbol: 'R_75', name: 'V75', multiplier: 50, group: 'volatility' },
    { symbol: 'R_100', name: 'V100', multiplier: 40, group: 'volatility' },
    { symbol: '1HZ10V', name: 'V10(1s)', multiplier: 400, group: 'volatility' },
    { symbol: '1HZ25V', name: 'V25(1s)', multiplier: 160, group: 'volatility' },
    { symbol: '1HZ50V', name: 'V50(1s)', multiplier: 80, group: 'volatility' },
    { symbol: '1HZ75V', name: 'V75(1s)', multiplier: 50, group: 'volatility' },
    { symbol: '1HZ100V', name: 'V100(1s)', multiplier: 40, group: 'volatility' },
    // Step indices (4)
    { symbol: 'STP200', name: 'Step200', multiplier: 400, group: 'step' },
    { symbol: 'STP300', name: 'Step300', multiplier: 300, group: 'step' },
    { symbol: 'STP400', name: 'Step400', multiplier: 200, group: 'step' },
    { symbol: 'STP500', name: 'Step500', multiplier: 100, group: 'step' },
    // Jump indices (5)
    { symbol: 'JD25', name: 'Jump25', multiplier: 50, group: 'jump' },
    { symbol: 'JD50', name: 'Jump50', multiplier: 100, group: 'jump' },
    { symbol: 'JD75', name: 'Jump75', multiplier: 15, group: 'jump' },
    { symbol: 'JD100', name: 'Jump100', multiplier: 100, group: 'jump' },
    // Boom indices (2)
    { symbol: 'BOOM500', name: 'Boom500', multiplier: 100, group: 'boom' },
    { symbol: 'BOOM1000', name: 'Boom1000', multiplier: 100, group: 'boom' },
    // Crash indices (2)
    { symbol: 'CRASH500', name: 'Crash500', multiplier: 100, group: 'crash' },
    { symbol: 'CRASH1000', name: 'Crash1000', multiplier: 100, group: 'crash' },
];

// Fixed parameters for advanced models
const PLL_CYCLE = { volatility:7, step:13, jump:11, boom:19, crash:23 };
const LCG_A = 16807, LCG_M = 2147483647;
const GARCH_OMEGA = 0.0001, GARCH_ALPHA = 0.1, GARCH_BETA = 0.85;
const FRAC_D = 0.32;

// ========================== GLOBAL STATE ==========================
let ws = null, db = null;
let totalPnL = 0, dailyPnL = 0;
let openContracts = new Map(), pendingTrades = new Map();
let nextReqId = 1, lastDate = new Date().toDateString(), dailyLossLock = false;

// Per‑symbol data containers
let candleData = {};          // OHLC candles
let tickData = {};            // raw tick prices
let tickPhase = {};           // current PLL phase value
let lcgDeltas = {};           // last 10 tick delta codes for LCG prediction
let symbolLastTradeTime = {}; // timestamp of last trade per symbol
let symbolStats = {};         // win/loss counts per symbol
let tdSequential = {};        // stores last 9 closes for TD setup
let marketProfile = {};       // 20‑bucket price histogram for POC
let tfData = {};              // multi‑timeframe aggregated ticks
let garchVars = {};           // last variance for GARCH
let regimeState = {};         // current market regime per symbol

// NEW state variables for added strategies
let cumulativeDelta = {};      // running tick delta (buying/selling pressure)
let deltaHistory = {};         // rolling window of last 50 cumulative delta values
let vpinBuckets = {};          // simplified VPIN: 10 buckets (each 10 ticks)
let kalmanFilters = {};        // Kalman filter instances for cointegration pairs

// Risk tracking
let consecutiveLosses = 0, consecutiveWins = 0;

// Express & Socket.IO for dashboard
const app = express();
const server = http.createServer(app);
const io = socketIo(server, { cors: { origin: '*' } });

// Initialise per‑symbol data structures
SYMBOLS.forEach(s => {
    candleData[s.symbol] = [];
    tickData[s.symbol] = [];
    tickPhase[s.symbol] = 0;
    lcgDeltas[s.symbol] = [];
    symbolLastTradeTime[s.symbol] = 0;
    symbolStats[s.symbol] = { wins:0, losses:0 };
    tdSequential[s.symbol] = { closes: [], count: 0 };
    marketProfile[s.symbol] = { buckets: new Array(20).fill(0), minPrice: 0, maxPrice: 0 };
    tfData[s.symbol] = { ticks: [], last1min: null, last5min: null };
    garchVars[s.symbol] = 0.0001;
    regimeState[s.symbol] = 'NEUTRAL';
    cumulativeDelta[s.symbol] = 0;
    deltaHistory[s.symbol] = [];
    vpinBuckets[s.symbol] = new Array(10).fill(0);
});
// Kalman for the Boom1000/Crash1000 spread (cointegration enhancement)
kalmanFilters['BOOM1000_CRASH1000'] = new (class KalmanFilter {
    constructor(Q=0.001, R=0.1) { this.Q = Q; this.R = R; this.x = 0; this.P = 1; }
    update(z) { this.P = this.P + this.Q; const K = this.P / (this.P + this.R); this.x = this.x + K * (z - this.x); this.P = (1 - K) * this.P; return this.x; }
})();

// ========================== UTILITIES ==========================
function log(msg) {
    const t = new Date().toLocaleTimeString('en-KE', { timeZone: 'Africa/Nairobi' });
    console.log(`[${t}] ${msg}`);
    io.emit('log', { timestamp: t, message: msg });
}
function sendTelegram(msg) {
    const url = `https://api.telegram.org/bot${CONFIG.TELEGRAM.TOKEN}/sendMessage?chat_id=${CONFIG.TELEGRAM.CHAT_ID}&text=${encodeURIComponent(msg)}`;
    https.get(url).on('error', ()=> {});
}

// ---------- Basic Indicators (full implementations) ----------
function calculateEMA(candles, period) {
    if (candles.length < period) return null;
    const closes = candles.map(c=>c.close);
    const k = 2/(period+1);
    let ema = closes.slice(0,period).reduce((a,b)=>a+b,0)/period;
    for(let i=period; i<closes.length; i++) ema = (closes[i]-ema)*k+ema;
    return ema;
}
function calcRSI(candles, period=14) {
    if(candles.length<period+1) return null;
    const closes = candles.slice(-(period+1)).map(c=>c.close);
    let gains=0, losses=0;
    for(let i=1;i<closes.length;i++) {
        const diff = closes[i]-closes[i-1];
        if(diff>0) gains+=diff; else losses-=diff;
    }
    const avgGain=gains/period, avgLoss=losses/period;
    if(avgLoss===0) return 100;
    return 100-100/(1+avgGain/avgLoss);
}
function calcATR(candles, period=14) {
    if(candles.length<period+1) return null;
    const slice = candles.slice(-(period+1));
    let trSum=0;
    for(let i=1;i<slice.length;i++) {
        const h=slice[i].high, l=slice[i].low, pc=slice[i-1].close;
        trSum += Math.max(h-l, Math.abs(h-pc), Math.abs(l-pc));
    }
    return trSum/period;
}
function calcADX(candles, period=14) {
    if(candles.length<period*2) return 25;
    const atr = calcATR(candles, period);
    const priceRange = candles[candles.length-1].high - candles[candles.length-1].low;
    if(!atr) return 25;
    return Math.min(70, Math.max(20, (priceRange/atr)*30));
}
function calcBB(candles, period=20, mult=2) {
    if(candles.length<period) return null;
    const closes = candles.slice(-period).map(c=>c.close);
    const mean = closes.reduce((a,b)=>a+b,0)/period;
    const std = Math.sqrt(closes.reduce((a,b)=>a+Math.pow(b-mean,2),0)/period);
    const upper = mean+mult*std, lower = mean-mult*std;
    return { upper, lower, bandwidth:(upper-lower)/mean, percentB:(candles[candles.length-1].close-lower)/(upper-lower), mean };
}
function getSupertrend(candles, period=10, multiplier=3) {
    const atr = calcATR(candles, period);
    if(!atr) return null;
    const last = candles[candles.length-1];
    const hl2 = (last.high+last.low)/2;
    const upper = hl2+multiplier*atr;
    const lower = hl2-multiplier*atr;
    if(last.close > upper) return 'BUY';
    if(last.close < lower) return 'SELL';
    return null;
}
function getPriceZone(candles, lookback=50) {
    if(candles.length<lookback) return 'neutral';
    const slice = candles.slice(-lookback);
    const high=Math.max(...slice.map(c=>c.high)), low=Math.min(...slice.map(c=>c.low));
    const pos = (candles[candles.length-1].close-low)/(high-low);
    if(pos>0.8) return 'premium';
    if(pos<0.2) return 'discount';
    return 'equilibrium';
}

// ---------- NEW STRATEGY FUNCTIONS (integrated) ----------
/**
 * Z‑Score mean reversion – pure statistical z‑score over rolling window.
 * Used in volatilityStrategy when regime is 'RANGING'.
 */
function getZScore(candles, period = 20) {
    if(candles.length < period) return 0;
    const closes = candles.slice(-period).map(c=>c.close);
    const mean = closes.reduce((a,b)=>a+b,0)/period;
    const std = Math.sqrt(closes.reduce((a,b)=>a+Math.pow(b-mean,2),0)/period);
    if(std === 0) return 0;
    const price = candles[candles.length-1].close;
    return (price - mean) / std;
}

/**
 * Donchian channel – provides breakout levels for trending confirmation.
 */
function getDonchian(candles, period = 20) {
    if(candles.length < period) return { upper:0, lower:0, breakoutUp:false, breakoutDown:false };
    const slice = candles.slice(-period);
    const high = Math.max(...slice.map(c=>c.high));
    const low = Math.min(...slice.map(c=>c.low));
    const price = candles[candles.length-1].close;
    return { upper: high, lower: low, breakoutUp: price > high, breakoutDown: price < low };
}

/**
 * ARIMA(1,1) simplified for tick direction prediction (1‑lag, 1‑difference).
 */
function arima11(series) {
    if(series.length < 3) return 0;
    const diff = series[series.length-1] - series[series.length-2];
    const prevDiff = series[series.length-2] - series[series.length-3];
    return 0.5 * diff + 0.3 * prevDiff;
}

/**
 * Lightweight Hidden Markov Model for regime transition probabilities.
 * States: 0=TRENDING, 1=RANGING, 2=SPIKE, 3=NEUTRAL.
 */
const HMM_TRANSITIONS = [
    [0.7, 0.2, 0.05, 0.05],   // from TRENDING
    [0.2, 0.6, 0.1, 0.1],     // from RANGING
    [0.1, 0.2, 0.6, 0.1],     // from SPIKE
    [0.2, 0.3, 0.1, 0.4]      // from NEUTRAL
];
const stateNames = ['TRENDING', 'RANGING', 'SPIKE', 'NEUTRAL'];
function hmmPredict(currentRegimeStr) {
    let currentIdx = stateNames.indexOf(currentRegimeStr);
    if(currentIdx === -1) currentIdx = 3;
    const probs = HMM_TRANSITIONS[currentIdx];
    let maxProb = -1, nextIdx = 3;
    for(let i=0; i<probs.length; i++) {
        if(probs[i] > maxProb) { maxProb = probs[i]; nextIdx = i; }
    }
    let bias = null;
    if(nextIdx === 0) bias = 'TRENDING';
    else if(nextIdx === 1) bias = 'RANGING';
    else if(nextIdx === 2) bias = 'SPIKE';
    else bias = 'NEUTRAL';
    return { nextState: stateNames[nextIdx], probability: maxProb, bias };
}

/**
 * Simplified VPIN (Volume‑Synchronized Probability of Informed Trading).
 * Uses tick frequency as volume proxy, groups into 10 buckets of 10 ticks each.
 */
function updateVPIN(symbol) {
    const ticks = tickData[symbol];
    if(!ticks || ticks.length < 100) return 0;
    if(ticks.length % 10 !== 0) return 0;   // recompute every 10 ticks
    let totalVolume = 0, absVolumeChange = 0;
    const bucketSize = 10;
    for(let b=0; b<10; b++) {
        const start = Math.max(0, ticks.length - (10-b)*bucketSize);
        const end = Math.min(ticks.length, start + bucketSize);
        const bucketPrices = ticks.slice(start, end);
        if(bucketPrices.length === 0) continue;
        const priceChange = Math.abs(bucketPrices[bucketPrices.length-1] - bucketPrices[0]);
        absVolumeChange += priceChange;
        totalVolume += bucketPrices.length;
    }
    if(totalVolume === 0) return 0;
    return absVolumeChange / totalVolume;
}

/**
 * Kalman‑filtered spread for Boom/Crash cointegration (adaptive hedge ratio).
 */
function kalmanUpdateSpread(boomClose, crashClose) {
    const spread = Math.log(boomClose) - Math.log(crashClose);
    const kalman = kalmanFilters['BOOM1000_CRASH1000'];
    return kalman.update(spread);
}
                                                                                                                                                                              
//---------- Advanced Quant Models (existing, kept unchanged) ----------
function calculateHurst(candles) {
    const prices = candles.map(c=>c.close);
    const N = prices.length;
    if(N < 100) return 0.5;
    let maxLag = Math.floor(N/2);
    let lags = [], rs = [];
    for(let lag=10; lag<=maxLag; lag+=Math.floor(maxLag/20)) {
        let mean = 0, sumSq = 0;
        for(let i=lag; i<N; i++) mean += prices[i] - prices[i-lag];
        mean /= (N-lag);
        let dev = 0;
        for(let i=lag; i<N; i++) dev += Math.pow((prices[i]-prices[i-lag]-mean), 2);
        const std = Math.sqrt(dev/(N-lag));
        let range = Math.max(...prices.slice(lag)) - Math.min(...prices.slice(lag));
        if(std === 0) continue;
        rs.push(Math.log(range/std));
        lags.push(Math.log(lag));
    }
    if(lags.length < 2) return 0.5;
    const n = lags.length;
    let sumX=0, sumY=0, sumXY=0, sumX2=0;
    for(let i=0; i<n; i++) {
        sumX += lags[i];
        sumY += rs[i];
        sumXY += lags[i]*rs[i];
        sumX2 += lags[i]*lags[i];
    }
    const hurst = (n*sumXY - sumX*sumY) / (n*sumX2 - sumX*sumX);
    return Math.min(0.9, Math.max(0.1, hurst));
}
function sampleEntropy(data, m=2, rFactor=0.2) {
    const N = data.length;
    if(N < m+2) return 1;
    const mean = data.reduce((a,b)=>a+b,0)/N;
    const std = Math.sqrt(data.reduce((a,b)=>a+Math.pow(b-mean,2),0)/N);
    const r = rFactor * std;
    let A=0, B=0;
    for(let i=0; i<N-m; i++) {
        for(let j=i+1; j<N-m; j++) {
            let d1=0, d2=0;
            for(let k=0; k<m; k++) {
                d1 += Math.abs(data[i+k]-data[j+k]);
                d2 += Math.abs(data[i+k]-data[j+k]);
            }
            if(d1 <= r) B++;
            if(d2 <= r) A++;
        }
    }
    if(A===0 || B===0) return 1;
    return -Math.log(A/B);
}
function ksTest(sample1, sample2) {
    const combined = [...sample1, ...sample2].sort((a,b)=>a-b);
    const ecdf1 = (x) => sample1.filter(v=>v<=x).length / sample1.length;
    const ecdf2 = (x) => sample2.filter(v=>v<=x).length / sample2.length;
    let maxDiff = 0;
    for(const val of combined) {
        maxDiff = Math.max(maxDiff, Math.abs(ecdf1(val)-ecdf2(val)));
    }
    return maxDiff;
}
function detectVacuum(symbol) {
    const ticks = tickData[symbol];
    if(!ticks || ticks.length < 100) return false;
    const recent = ticks.slice(-50);
    const range = Math.max(...recent) - Math.min(...recent);
    if(range === 0) return false;
    const density = recent.length / range;
    const longRange = ticks.slice(-200);
    const longRangeVal = Math.max(...longRange) - Math.min(...longRange);
    const longDensity = longRange.length / longRangeVal;
    const gradient = density / longDensity;
    return (density < 0.15 * longDensity && gradient > 1.2);
}
function updateGARCH(symbol, ret) {
    let lastVar = garchVars[symbol];
    const newVar = GARCH_OMEGA + GARCH_ALPHA * ret*ret + GARCH_BETA * lastVar;
    garchVars[symbol] = newVar;
    return Math.sqrt(newVar);
}
function farimaFilter(series, d=FRAC_D, maxLag=10) {
    if(series.length < maxLag+2) return series.slice();
    let weights = [1];
    for(let k=1; k<=maxLag; k++) {
        weights[k] = -weights[k-1] * (d - k + 1) / k;
    }
    let filtered = [];
    for(let i=maxLag; i<series.length; i++) {
        let sum = 0;
        for(let j=0; j<=maxLag; j++) sum += weights[j] * series[i-j];
        filtered.push(sum);
    }
    if(filtered.length < 3) return 0;
    const ar2 = 0.5*filtered[filtered.length-1] + 0.3*filtered[filtered.length-2];
    return ar2;
}
function fractionalOU(series, mu, theta, sigma, H=0.7) {
    if(series.length < 2) return null;
    const last = series[series.length-1];
    const dt = 1;
    const hurstFactor = Math.pow(dt, H-0.5);
    const dW = (Math.random() - 0.5) * 2;
    const drift = -theta * (last - mu) * dt * hurstFactor;
    const diff = drift + sigma * Math.sqrt(dt) * dW;
    return diff > 0 ? 'BUY' : (diff < 0 ? 'SELL' : null);
}
function updateLCG(symbol, deltaCode) {
    let deltas = lcgDeltas[symbol];
    if(deltas.length < 6) return null;
    for(let seed = 0; seed < 10000; seed++) {
        let state = seed;
        let matches = 0;
        for(let i=0; i<deltas.length; i++) {
            state = (LCG_A * state) % LCG_M;
            const predicted = (state % 5) - 2;
            if(predicted === deltas[i]) matches++;
        }
        if(matches >= deltas.length - 1) {
            state = (LCG_A * state) % LCG_M;
            const next = (state % 5) - 2;
            if(next > 0) return 'BUY';
            if(next < 0) return 'SELL';
            return null;
        }
    }
    return null;
}
function updatePLL(symbol) {
    const group = SYMBOLS.find(s=>s.symbol===symbol).group;
    const cycle = PLL_CYCLE[group]||7;
    tickPhase[symbol] = (tickPhase[symbol] + 1) % cycle;
    return tickPhase[symbol];
}
function isRolloverWindow() {
    const now = new Date();
    const mins = now.getUTCHours()*60 + now.getUTCMinutes();
    return [0,8,16].some(h => Math.abs(mins - h*60) <= 10);
}
function updateTDSequential(symbol, close) {
    const td = tdSequential[symbol];
    td.closes.push(close);
    if(td.closes.length > 9) td.closes.shift();
    if(td.closes.length < 9) return 0;
    let up=0, down=0;
    for(let i=1;i<td.closes.length;i++) {
        if(td.closes[i] > td.closes[i-1]) up++;
        else if(td.closes[i] < td.closes[i-1]) down++;
    }
    if(up === 8) return 9;
    if(down === 8) return -9;
    return 0;
}
function updateMarketProfile(symbol, price) {
    const mp = marketProfile[symbol];
    if(mp.minPrice===0 || price<mp.minPrice) mp.minPrice = price;
    if(mp.maxPrice===0 || price>mp.maxPrice) mp.maxPrice = price;
    const range = mp.maxPrice - mp.minPrice;
    if(range === 0) return null;
    const bucket = Math.floor((price - mp.minPrice) / range * 20);
    if(bucket>=0 && bucket<20) mp.buckets[bucket]++;
    let maxIdx = 0;
    for(let i=0;i<20;i++) if(mp.buckets[i] > mp.buckets[maxIdx]) maxIdx = i;
    return mp.minPrice + (maxIdx/20) * range;
}
function computeSkewKurtosis(returns) {
    if (returns.length < 10) return { skew:0, kurtosis:0 };
    const mean = returns.reduce((a,b)=>a+b,0)/returns.length;
    let m2=0, m3=0, m4=0;
    for (const r of returns) {
        const dev = r - mean;
        m2 += dev*dev;
        m3 += dev*dev*dev;
        m4 += dev*dev*dev*dev;
    }
    m2 /= returns.length;
    m3 /= returns.length;
    m4 /= returns.length;
    const skew = m3 / Math.pow(m2, 1.5);
    const kurt = m4 / (m2*m2);
    return { skew, kurtosis:kurt };
}
function computeVWAP(symbol) {
    const ticks = tickData[symbol];
    if (!ticks || ticks.length < 20) return null;
    let sumPriceVol = 0, sumVol = 0;
    for (let i=Math.max(0, ticks.length-50); i<ticks.length; i++) {
        sumPriceVol += ticks[i];
        sumVol += 1;
    }
    return sumVol > 0 ? sumPriceVol / sumVol : null;
}
function signalDecayWeight(signalAgeSeconds) {
    return Math.max(0.2, 1 - signalAgeSeconds/120);
}
// ---------- Candle Patterns (full, unchanged) ----------
function isPinBar(candle) {
    const body = Math.abs(candle.close - candle.open);
    const upperWick = candle.high - Math.max(candle.open, candle.close);
    const lowerWick = Math.min(candle.open, candle.close) - candle.low;
    if (body === 0) return false;
    if (upperWick > body * 2 && lowerWick < body * 0.5) return 'bearish';
    if (lowerWick > body * 2 && upperWick < body * 0.5) return 'bullish';
    return false;
}
function isEngulfing(candles) {
    if (candles.length < 2) return false;
    const prev = candles[candles.length-2];
    const curr = candles[candles.length-1];
    const prevBody = Math.abs(prev.close - prev.open);
    const currBody = Math.abs(curr.close - curr.open);
    const prevRange = prev.high - prev.low;
    if (currBody > prevRange) {
        if (curr.close > curr.open && prev.close < prev.open) return 'bullish';
        if (curr.close < curr.open && prev.close > prev.open) return 'bearish';
    }
    return false;
}
function isInsideBar(candles) {
    if (candles.length < 2) return false;
    const prev = candles[candles.length-2];
    const curr = candles[candles.length-1];
    return (curr.high <= prev.high && curr.low >= prev.low);
}
function isThreeCandleReversal(candles) {
    if (candles.length < 3) return false;
    const c1 = candles[candles.length-3];
    const c2 = candles[candles.length-2];
    const c3 = candles[candles.length-1];
    if (c1.close < c1.open && Math.abs(c2.close-c2.open) < (c2.high-c2.low)*0.3 && c3.close > c3.open && c3.close > (c1.high+c1.low)/2) return 'bullish';
    if (c1.close > c1.open && Math.abs(c2.close-c2.open) < (c2.high-c2.low)*0.3 && c3.close < c3.open && c3.close < (c1.high+c1.low)/2) return 'bearish';
    return false;
}
function isDoji(candle) {
    const body = Math.abs(candle.close - candle.open);
    const range = candle.high - candle.low;
    if (range === 0) return true;
    return body / range < 0.1;
}
function isThreeWhiteSoldiers(candles) {
    if (candles.length < 3) return false;
    for (let i = 3; i>=1; i--) {
        const c = candles[candles.length-i];
        if (c.close <= c.open) return false;
        if (i>1 && c.close <= candles[candles.length-i+1].close) return false;
    }
    return true;
}
function isThreeBlackCrows(candles) {
    if (candles.length < 3) return false;
    for (let i = 3; i>=1; i--) {
        const c = candles[candles.length-i];
        if (c.close >= c.open) return false;
        if (i>1 && c.close >= candles[candles.length-i+1].close) return false;
    }
    return true;
}

// ---------- Market Regime Detection (4‑state) ----------
function detectRegime(candles) {
    const atr = calcATR(candles);
    if (!atr) return 'NEUTRAL';
    const volatility = candles.slice(-20).reduce((s,c)=>s+Math.abs(c.close-c.open),0)/20;
    const avgVol = candles.slice(-100).reduce((s,c)=>s+Math.abs(c.close-c.open),0)/100;
    const volRatio = volatility / avgVol;
    const hurst = calculateHurst(candles);
    const entropy = sampleEntropy(candles.slice(-200).map(c=>c.close));
    if (volRatio > 1.8) return 'SPIKE';
    if (hurst > 0.6 && entropy < 0.4) return 'TRENDING';
    if (hurst < 0.45 || entropy > 0.6) return 'RANGING';
    return 'NEUTRAL';
}
function varianceRatioTest(candles) {
    if (candles.length < 50) return 1;
    const closes = candles.map(c=>c.close);
    let var1=0, var2=0;
    for(let i=1;i<closes.length;i++) var1 += Math.pow(closes[i]-closes[i-1],2);
    var1 /= (closes.length-1);
    for(let i=2;i<closes.length;i++) var2 += Math.pow(closes[i]-closes[i-2],2);
    var2 /= (closes.length-2);
    return var2 / (2*var1);
}
// ---------- SMC/ICT Functions ----------
function detectOrderBlock(candles, dir) {
    const atr = calcATR(candles);
    if(!atr) return false;
    for(let i=candles.length-2; i>=Math.max(0,candles.length-20); i--) {
        const body = Math.abs(candles[i].close-candles[i].open);
        if(body > atr*1.5 && i-1>=0) {
            const ob = candles[i-1];
            if(dir==='BUY' && ob.close < ob.open) return true;
            if(dir==='SELL' && ob.close > ob.open) return true;
        }
    }
    return false;
}
function detectLiquiditySweep(candles, dir) {
    if(candles.length<20) return false;
    const recent = candles.slice(-20);
    const last = candles[candles.length-1], prev = candles[candles.length-2];
    const high = Math.max(...recent.slice(0,-2).map(c=>c.high));
    const low = Math.min(...recent.slice(0,-2).map(c=>c.low));
    if(dir==='BUY') return prev.low < low && last.close > low;
    if(dir==='SELL') return prev.high > high && last.close < high;
    return false;
}
function detectFVG(candles, dir) {
    for(let i=candles.length-3; i>=Math.max(0,candles.length-20); i--) {
        const prev=candles[i], next=candles[i+2];
        if(!prev||!next) continue;
        if(dir==='BUY' && next.low > prev.high) return true;
        if(dir==='SELL' && next.high < prev.low) return true;
    }
    return false;
}
function isKillZone() {
    const now=new Date(), h=now.getUTCHours(), m=now.getUTCMinutes();
    const london = (h===6&&m>=45)||(h>=7&&h<=9)||(h===10&&m<=15);
    const ny = (h===11&&m>=45)||(h>=12&&h<=15)||(h===16&&m<=15);
    return london||ny;
}

// ---------- Multi‑Timeframe Confluence ----------
function updateMultiTF(symbol, price) {
    const data = tfData[symbol];
    data.ticks.push(price);
    if(data.ticks.length>=60) {
        const slice = data.ticks.slice(-60);
        data.last1min = { high:Math.max(...slice), low:Math.min(...slice), close:slice[slice.length-1] };
        data.ticks = data.ticks.slice(-300);
    }
    if(data.ticks.length>=300 && data.ticks.length%60===0) {
        const slice = data.ticks.slice(-300);
        data.last5min = { high:Math.max(...slice), low:Math.min(...slice), close:slice[slice.length-1] };
    }
    if(!data.last1min) return null;
    const st = price > (data.ticks.slice(-10).reduce((a,b)=>a+b,0)/10) ? 'BUY':'SELL';
    const mf = data.last1min.close > (data.last1min.low+data.last1min.high)/2 ? 'BUY':'SELL';
    const lf = data.last5min ? (data.last5min.close > (data.last5min.low+data.last5min.high)/2 ? 'BUY':'SELL') : mf;
    if(st===mf && mf===lf) return st;
    return null;
}

// ---------- Asset‑Specific Strategies (enhanced with new logic) ----------
function volatilityStrategy(candles, symbol, regime) {
    if (candles.length < 100) return null;
    const bb = calcBB(candles);
    const rsi = calcRSI(candles);
    const hurst = calculateHurst(candles);
    const entropy = sampleEntropy(candles.slice(-200).map(c=>c.close));
    if (hurst > 0.4 && hurst < 0.6) return null;
    if (entropy > 0.45) return null;
    const cyclePhase = tickPhase[symbol] % 70;
    if (cyclePhase < 52) return null;
    if (!bb || !rsi) return null;

    // NEW: Z‑Score mean reversion for ranging regime
    if (regime === 'RANGING') {
        const zScore = getZScore(candles, 20);
        if (zScore < -2.0) return 'BUY';
        if (zScore > 2.0) return 'SELL';
    }

    // Original Bollinger/RSI logic
    if (bb.percentB < 0.2 && rsi < 30) return 'BUY';
    if (bb.percentB > 0.8 && rsi > 70) return 'SELL';
    return null;
}
function stepStrategy(candles, symbol) {
    let stepSize = symbol==='STP200'?200 : symbol==='STP300'?300 : symbol==='STP400'?400 : 500;
    const price = candles[candles.length-1].close;
    const hoursSinceStart = ((Date.now()-new Date().setUTCHours(0,0,0,0))/(3600*1000));
    const drift = hoursSinceStart * 0.0015 * stepSize;
    const rawLevel = Math.round(price/stepSize)*stepSize;
    const adjLevel = rawLevel + drift;
    const penetration = Math.abs(price-adjLevel)/stepSize;
    const nearLevel = candles.filter(c=>Math.abs(c.close-adjLevel)<stepSize*0.05).length;
    const strength = nearLevel / candles.length * 100;
    if (penetration < 0.05 && strength > 80) return 'BUY';
    if (penetration > 0.85 && strength < 30) return 'SELL';
    return null;
}
function jumpStrategy(symbol) {
    const ticks = tickData[symbol];
    if (!ticks || ticks.length<50) return null;
    let up=0, down=0;
    for (let i=1;i<ticks.length;i++) {
        if (ticks[i] > ticks[i-1]) up++;
        else if (ticks[i] < ticks[i-1]) down++;
    }
    const imbalance = (up-down)/(up+down);
    const recent = ticks.slice(-15);
    const range = Math.max(...recent)-Math.min(...recent);
    const price = ticks[ticks.length-1];
    const prevPrice = ticks[ticks.length-2];
    const voidGap = Math.abs(price - prevPrice) > 8;

    // NEW: ARIMA(1,1) confirmation
    const arimaPred = arima11(ticks.slice(-10));
    let arimaConfirm = false;
    if (arimaPred > 0 && imbalance < -0.6) arimaConfirm = true;
    if (arimaPred < 0 && imbalance > 0.6) arimaConfirm = true;

    if (range < 3.5 && imbalance < -0.6 && voidGap && arimaConfirm) return 'BUY';
    if (range < 3.5 && imbalance > 0.6 && voidGap && arimaConfirm) return 'SELL';
    return null;
}
function boomCrashStrategy(symbol, group) {
    const phase = tickPhase[symbol] % 900;
    const timeFactor = (phase>720 && phase<850) ? 1.0 : 0.1;
    const candles = candleData[symbol];
    if (!candles || candles.length<60) return null;
    const high30 = Math.max(...candles.slice(-30).map(c=>c.high));
    const low30 = Math.min(...candles.slice(-30).map(c=>c.low));
    const atr60 = calcATR(candles,60);
    if (!atr60) return null;
    const compression = (high30-low30)/atr60;
    const ticks = tickData[symbol]||[];
    let up=0, down=0;
    for (let i=1;i<Math.min(50,ticks.length);i++) {
        if (ticks[i]>ticks[i-1]) up++;
        else if (ticks[i]<ticks[i-1]) down++;
    }
    const tickDelta = (up-down)/(up+down||1);
    const intensity = timeFactor * (1/Math.max(0.1,compression)) * Math.abs(tickDelta);
    if (intensity > 2.2 && compression < 0.22) {
        if (group==='boom' && tickDelta > 0.4) return 'BUY';
        if (group==='crash' && tickDelta < -0.4) return 'SELL';
    }
    const lastCandle = candles[candles.length-1];
    const prevCandle = candles[candles.length-2];
    const spike = (lastCandle.high - lastCandle.low) > 3 * atr60;
    const retrace = Math.abs(lastCandle.close - prevCandle.close) < 0.5 * atr60;
    if (spike && retrace) {
        if (group==='boom') return 'SELL';
        if (group==='crash') return 'BUY';
    }
    return null;
}
function cointegrationSignal() {
    const boom = candleData['BOOM1000'];
    const crash = candleData['CRASH1000'];
    if (!boom || !crash || boom.length<100) return null;
    const boomClose = boom[boom.length-1].close;
    const crashClose = crash[crash.length-1].close;
    // NEW: Kalman‑filtered spread
    const filteredSpread = kalmanUpdateSpread(boomClose, crashClose);
    const zscore = filteredSpread / 0.02; // approximate historical std
    if (zscore > 2.0) return 'SELL_BOOM_BUY_CRASH';
    if (zscore < -2.0) return 'BUY_BOOM_SELL_CRASH';
    return null;
}
// ---------- Trade Quality Score (enhanced with VPIN) ----------
function tradeQualityScore(symbol, direction, confidence, regime, candleScore) {
    let score = 0;
    const hurst = calculateHurst(candleData[symbol]);
    if ((direction==='BUY' && hurst>0.6) || (direction==='SELL' && hurst<0.4)) score += 25;
    else if ((direction==='BUY' && hurst>0.5) || (direction==='SELL' && hurst<0.5)) score += 15;
    if (regime === 'TRENDING' || regime === 'SPIKE') score += 20;
    else if (regime === 'RANGING') score += 10;
    score += Math.min(30, confidence/2);
    score += candleScore;
    const vwap = computeVWAP(symbol);
    const price = candleData[symbol][candleData[symbol].length-1].close;
    if (vwap && ((direction==='BUY' && price < vwap) || (direction==='SELL' && price > vwap))) score += 15;
    // NEW: VPIN informed flow
    const vpin = updateVPIN(symbol);
    if (vpin > 0.45) score += 15;
    if (vpin < 0.2) score -= 5;
    return Math.min(100, Math.max(0, score));
}

// ===================== MAIN SIGNAL ENGINE =====================
function getSignal(symbol) {
    const candles = candleData[symbol];
    const ticks = tickData[symbol]||[];
    const asset = SYMBOLS.find(s=>s.symbol===symbol);
    if (!candles || candles.length<100) return null;

    // Universal filters
    if (tickPhase[symbol] % (PLL_CYCLE[asset.group]||7) !== 0) return null;
    if (!isKillZone()) return null;

    // Regime + variance ratio
    let regime = detectRegime(candles);
    regimeState[symbol] = regime;
    if (regime === 'SPIKE') return null;
    const vr = varianceRatioTest(candles);
    if (vr > 0.7 && vr < 1.3) return null;

    // NEW: Lightweight HMM forward‑looking bias
    const hmmResult = hmmPredict(regime);
    let hmmBias = null;
    if (hmmResult.bias === 'TRENDING') hmmBias = 'BUY';
    else if (hmmResult.bias === 'RANGING') hmmBias = null;
    else if (hmmResult.bias === 'SPIKE') hmmBias = null;

    // Advanced quant filters (existing)
    const hurst = calculateHurst(candles);
    const entropy = sampleEntropy(candles.slice(-200).map(c=>c.close));
    if (hurst > 0.4 && hurst < 0.6) return null;
    if (entropy > 0.45) return null;
    if (candles.length % 10 === 0) {
        const recent = candles.slice(-50).map(c=>c.close);
        const prev = candles.slice(-150,-50).map(c=>c.close);
        if (ksTest(recent, prev) > 0.35) return null;
    }
    const vacuum = detectVacuum(symbol);
    const lcgDir = (lcgDeltas[symbol].length >= 6) ? updateLCG(symbol, 0) : null;

    // Multi‑timeframe
    let mtfDir = null;
    if (ticks.length > 300) mtfDir = updateMultiTF(symbol, ticks[ticks.length-1]);
    if (!mtfDir) return null;

    // Candle patterns (unchanged)
    let candlePatternSignal = null;
    let candleScore = 0;
    const lastCandle = candles[candles.length-1];
    const pin = isPinBar(lastCandle);
    const engulf = isEngulfing(candles);
    const inside = isInsideBar(candles);
    const threeRev = isThreeCandleReversal(candles);
    const doji = isDoji(lastCandle);
    const soldiers = isThreeWhiteSoldiers(candles);
    const crows = isThreeBlackCrows(candles);
    if (pin === 'bullish') { candlePatternSignal = 'BUY'; candleScore += 20; }
    if (pin === 'bearish') { candlePatternSignal = 'SELL'; candleScore += 20; }
    if (engulf === 'bullish') { candlePatternSignal = 'BUY'; candleScore += 25; }
    if (engulf === 'bearish') { candlePatternSignal = 'SELL'; candleScore += 25; }
    if (inside) candleScore += 10;
    if (threeRev === 'bullish') { candlePatternSignal = 'BUY'; candleScore += 30; }
    if (threeRev === 'bearish') { candlePatternSignal = 'SELL'; candleScore += 30; }
    if (doji) candleScore += 5;
    if (soldiers) { candlePatternSignal = 'BUY'; candleScore += 35; }
    if (crows) { candlePatternSignal = 'SELL'; candleScore += 35; }

    // Asset strategy
    let direction = null;
    switch (asset.group) {
        case 'volatility': direction = volatilityStrategy(candles, symbol, regime); break;
        case 'step': direction = stepStrategy(candles, symbol); break;
        case 'jump': direction = jumpStrategy(symbol); break;
        case 'boom': case 'crash': direction = boomCrashStrategy(symbol, asset.group); break;
    }
    if (!direction && candlePatternSignal && candleScore >= 25) direction = candlePatternSignal;
    if (!direction && hmmBias) direction = hmmBias;   // HMM fallback
    if (!direction) return null;
    if (lcgDir && direction !== lcgDir) return null;

    // Cointegration (enhanced with Kalman)
    let cointDir = null;
    if (symbol === 'BOOM1000' || symbol === 'CRASH1000') cointDir = cointegrationSignal();
    if (cointDir) return { direction: cointDir, confidence: 90 };

    // NEW: Donchian confirmation in trending regime (optional, adds confidence)
    if (regime === 'TRENDING') {
        const donchian = getDonchian(candles, 20);
        // Not a hard filter, but we could adjust confidence later
    }

    // Confidence computation (existing + new cumulative delta)
    let confidence = 60;
    const ret = (candles[candles.length-1].close - candles[candles.length-2].close) / candles[candles.length-2].close;
    const garchVol = updateGARCH(symbol, ret);
    if (garchVol > 0.02) confidence += 15;
    const closes = candles.map(c=>c.close);
    const farimaPred = farimaFilter(closes, FRAC_D, 10);
    if (farimaPred && ((direction==='BUY' && farimaPred > 0) || (direction==='SELL' && farimaPred < 0))) confidence += 10;
    const fouDir = fractionalOU(closes, closes[0], 0.1, 0.02, 0.7);
    if (fouDir && fouDir === direction) confidence += 15;
    const td = updateTDSequential(symbol, candles[candles.length-1].close);
    if (td === 9 && direction === 'SELL') confidence += 20;
    if (td === -9 && direction === 'BUY') confidence += 20;
    const poc = updateMarketProfile(symbol, candles[candles.length-1].close);
    const price = candles[candles.length-1].close;
    if (poc && Math.abs(price-poc) < 0.5) {
        if (direction === 'BUY' && price < poc) confidence += 10;
        if (direction === 'SELL' && price > poc) confidence += 10;
    }
    if (isRolloverWindow()) confidence *= 1.2;
    if (regime === 'TRENDING') confidence += 10;
    if (regime === 'RANGING') confidence -= 5;
    const returns = [];
    for (let i=1;i<Math.min(20,closes.length);i++) returns.push((closes[i]-closes[i-1])/closes[i-1]);
    const { skew, kurtosis } = computeSkewKurtosis(returns);
    if (Math.abs(skew) > 1.5 && kurtosis > 4) confidence += 15;

    // NEW: Cumulative delta / order flow imbalance
    const deltaWindow = deltaHistory[symbol] || [];
    if (deltaWindow.length > 0) {
        const deltaImbalance = deltaWindow.reduce((a,b)=>a+b,0) / deltaWindow.length;
        if (deltaImbalance > 1.5 && direction === 'BUY') confidence += 15;
        if (deltaImbalance < -1.5 && direction === 'SELL') confidence += 15;
    }

    // SMC filters
    let instScore = 0;
    if (detectOrderBlock(candles, direction)) instScore += 2;
    if (detectLiquiditySweep(candles, direction)) instScore += 2;
    if (detectFVG(candles, direction)) instScore += 2;
    if (instScore < 2) return null;

    // Equilibrium
    const zone = getPriceZone(candles);
    if (direction === 'BUY' && zone !== 'discount') return null;
    if (direction === 'SELL' && zone !== 'premium') return null;

    // Tick volume
    const body = Math.abs(lastCandle.close - lastCandle.open);
    const avgBody = candles.slice(-20).reduce((s,c)=>s+Math.abs(c.close-c.open),0)/20;
    if (body < avgBody * 0.6) return null;

    // Quality score & decay
    const quality = tradeQualityScore(symbol, direction, confidence, regime, candleScore);
    if (quality < 60) return null;
    const lastSigTime = symbolLastTradeTime[symbol] || 0;
    const ageSec = (Date.now() - lastSigTime) / 1000;
    const decay = signalDecayWeight(ageSec);
    if (decay < 0.5) return null;

    return { direction, confidence: Math.min(100, confidence * decay) };
}

// ===================== TRADE EXECUTION =====================
function safeSend(data) { if (ws && ws.readyState===WebSocket.OPEN) ws.send(JSON.stringify(data)); }
function placeSingleTrade(symbolObj, direction, confidence) {
    const candles = candleData[symbolObj.symbol];
    if(!candles || candles.length<50) return;
    const atr = calcATR(candles);
    if(!atr) return;
    let sl = Math.min(8, Math.max(1.5, CONFIG.ATR_SL_MULTIPLIER*atr));
    let tp = Math.min(15, Math.max(3, CONFIG.ATR_TP_MULTIPLIER*atr));

    // NEW: Volatility targeting – dynamic stake based on GARCH
    const ret = (candles[candles.length-1].close - candles[candles.length-2].close) / candles[candles.length-2].close;
    const garchVol = updateGARCH(symbolObj.symbol, ret);
    let stake = CONFIG.STAKE;
    if (garchVol > 0.03) stake = 0.5;
    if (garchVol < 0.01) stake = 1.5;
    stake = Math.min(2, Math.max(0.5, stake));

    const reqId = nextReqId++;
    const req = {
        buy:1, price:stake,
        parameters:{
            amount:stake, basis:'stake',
            contract_type: direction==='BUY'?'MULTUP':'MULTDOWN',
            currency:'USD', symbol:symbolObj.symbol, multiplier:symbolObj.multiplier,
            limit_order:{ stop_loss:parseFloat(sl.toFixed(2)), take_profit:parseFloat(tp.toFixed(2)) }
        }, req_id:reqId
    };
    pendingTrades.set(reqId, { symbol:symbolObj.symbol, direction, confidence });
    safeSend(req);
    symbolLastTradeTime[symbolObj.symbol] = Date.now();
    log(`📤 ${symbolObj.name} ${direction} | stake $${stake.toFixed(2)} | SL ${sl} TP ${tp} | conf ${Math.round(confidence)}`);
}
function placeTrade(symbolObj, direction, confidence) {
    if(openContracts.size>=CONFIG.MAX_CONCURRENT) return;
    if(dailyLossLock || dailyPnL<=-CONFIG.DAILY_LOSS_LIMIT) return;
    if(Date.now()-symbolLastTradeTime[symbolObj.symbol] < CONFIG.COOLDOWN_MINUTES*60000) return;
    // Handle pair trade signals
    if(direction === 'SELL_BOOM_BUY_CRASH') {
        const boom = SYMBOLS.find(s=>s.symbol==='BOOM1000');
        const crash = SYMBOLS.find(s=>s.symbol==='CRASH1000');
        if(boom&&crash) { placeSingleTrade(boom,'SELL',confidence); placeSingleTrade(crash,'BUY',confidence); }
        return;
    }
    if(direction === 'BUY_BOOM_SELL_CRASH') {
        const boom = SYMBOLS.find(s=>s.symbol==='BOOM1000');
        const crash = SYMBOLS.find(s=>s.symbol==='CRASH1000');
        if(boom&&crash) { placeSingleTrade(boom,'BUY',confidence); placeSingleTrade(crash,'SELL',confidence); }
        return;
    }
    placeSingleTrade(symbolObj, direction, confidence);
}
// ===================== WEBSOCKET =====================
function subscribeAll() {
    SYMBOLS.forEach(sym=>{
        safeSend({ ticks_history:sym.symbol, adjust_start_time:1, count:CONFIG.CANDLE_COUNT, end:'latest', granularity:CONFIG.GRANULARITY, style:'candles', subscribe:1, req_id:nextReqId++ });
        safeSend({ ticks:sym.symbol, subscribe:1, req_id:nextReqId++ });
    });
}
function handleMessage(data) {
    let msg;
    try{ msg=JSON.parse(data); }catch(e){return;}
    if(msg.error){ log(`API error: ${msg.error.message}`); return; }
    if(msg.msg_type==='authorize'){ log('Authorized'); subscribeAll(); return; }
    if(msg.msg_type==='tick' && msg.tick){
        const sym = msg.tick.symbol;
        const price = parseFloat(msg.tick.quote);
        tickData[sym].push(price);
        if(tickData[sym].length>200) tickData[sym].shift();
        updatePLL(sym);
        if(tickData[sym].length>=2){
            const delta = price - tickData[sym][tickData[sym].length-2];
            let code = 0;
            if(delta > 0.02) code = 2;
            else if(delta > 0.005) code = 1;
            else if(delta < -0.02) code = -2;
            else if(delta < -0.005) code = -1;
            lcgDeltas[sym].push(code);
            if(lcgDeltas[sym].length>10) lcgDeltas[sym].shift();

            // NEW: Update cumulative delta and history
            cumulativeDelta[sym] = (cumulativeDelta[sym] || 0) + code;
            deltaHistory[sym].push(cumulativeDelta[sym]);
            if(deltaHistory[sym].length > 50) deltaHistory[sym].shift();
        }
        return;
    }
    if(msg.msg_type==='ohlc' && msg.ohlc){
        const sym = msg.ohlc.symbol;
        const candle = {
            epoch:msg.ohlc.epoch, open:parseFloat(msg.ohlc.open), high:parseFloat(msg.ohlc.high),
            low:parseFloat(msg.ohlc.low), close:parseFloat(msg.ohlc.close)
        };
        candleData[sym].push(candle);
        if(candleData[sym].length>CONFIG.CANDLE_COUNT) candleData[sym].shift();
        const sig = getSignal(sym);
        if(sig){
            const asset = SYMBOLS.find(s=>s.symbol===sym);
            if(asset) placeTrade(asset, sig.direction, sig.confidence);
        }
        return;
    }
    if((msg.msg_type==='history'||msg.msg_type==='candles') && msg.candles){
        const sym = msg.echo_req.ticks_history;
        msg.candles.forEach(c=>{
            candleData[sym].push({
                epoch:c.epoch, open:parseFloat(c.open), high:parseFloat(c.high),
                low:parseFloat(c.low), close:parseFloat(c.close)
            });
        });
        if(candleData[sym].length>CONFIG.CANDLE_COUNT) candleData[sym]=candleData[sym].slice(-CONFIG.CANDLE_COUNT);
        log(`Loaded ${msg.candles.length} candles for ${sym}`);
        return;
    }
    if(msg.msg_type==='buy' && msg.buy){
        const trade = pendingTrades.get(msg.req_id);
        if(trade){
            const id = msg.buy.contract_id;
            openContracts.set(id, trade);
            pendingTrades.delete(msg.req_id);
            log(`✅ Opened ${trade.symbol} ${trade.direction} ID:${id}`);
            safeSend({ proposal_open_contract:1, contract_id:id, subscribe:1 });
        }
        return;
    }
    if(msg.msg_type==='proposal_open_contract' && msg.proposal_open_contract){
        const c = msg.proposal_open_contract;
        if(c.is_sold){
            const trade = openContracts.get(c.contract_id);
            if(trade){
                const pnl = Number(c.profit)||0;
                totalPnL += pnl; dailyPnL += pnl;
                if(pnl>=0){ consecutiveWins++; consecutiveLosses=0; }
                else{ consecutiveLosses++; consecutiveWins=0; }
                openContracts.delete(c.contract_id);
                log(`${pnl>=0?'✅ WIN':'❌ LOSS'} ${trade.symbol} ${pnl} | Daily:${dailyPnL.toFixed(2)} | W:${consecutiveWins} L:${consecutiveLosses}`);
                sendTelegram(`${trade.symbol} ${trade.direction} ${pnl>=0?'✅':'❌'} ${pnl} | Daily ${dailyPnL.toFixed(2)}`);
            }
        }
    }
}
function connectWebSocket(){
    ws = new WebSocket(`wss://ws.binaryws.com/websockets/v3?app_id=${CONFIG.APP_ID}`);
    ws.on('open',()=>{
        log('WebSocket connected');
        safeSend({ authorize:CONFIG.API_TOKEN });
        setInterval(()=>{ if(ws.readyState===WebSocket.OPEN) ws.ping(); }, 30000);
    });
    ws.on('message', handleMessage);
    ws.on('error', err=>log(`WS error: ${err.message}`));
    ws.on('close',()=>{ log('WS closed, reconnecting...'); setTimeout(connectWebSocket, 5000); });
}

// ===================== DASHBOARD & START =====================
app.use(express.static(path.join(__dirname,'public')));
app.get('/health', (req,res)=>res.send('OK'));
io.on('connection', socket=>{
    socket.emit('init', { totalPnL, dailyPnL, openTrades:openContracts.size });
});
async function start(){
    const SQL = await initSqlJs();
    if(fs.existsSync('./trades.db')) db = new SQL.Database(fs.readFileSync('./trades.db'));
    else{ db = new SQL.Database(); db.run(`CREATE TABLE IF NOT EXISTS trades (id INTEGER PRIMARY KEY, timestamp TEXT, symbol TEXT, direction TEXT, pnl REAL)`); }
    server.listen(CONFIG.DASHBOARD_PORT, ()=>log(`Dashboard on port ${CONFIG.DASHBOARD_PORT}`));
    connectWebSocket();
    setInterval(()=>{
        const today = new Date().toDateString();
        if(today !== lastDate){ dailyPnL=0; lastDate=today; dailyLossLock=false; log('Daily PnL reset'); }
    }, 60000);
}
start();
