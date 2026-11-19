// 台股量化查詢的後端:代理證交所(TWSE)與櫃買中心(TPEx)公開資料。
// 零依賴,需要 Node 18 以上。  用法:node build.js && node server.js
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 8787;
const UA = 'Mozilla/5.0 (compatible; tw-quant/1.0)';
const MIN_GAP_MS = 400; // 對交易所的請求間隔,避免被限流
const sleep = ms => new Promise(r => setTimeout(r, ms));

/* ---------- 請求:節流 + 快取 ---------- */
const cache = new Map();
let chain = Promise.resolve();
function throttled(fn) {
  const p = chain.then(fn);
  chain = p.catch(() => {}).then(() => sleep(MIN_GAP_MS));
  return p;
}
async function getJSON(url, ttl) {
  const hit = cache.get(url);
  if (hit && Date.now() - hit.t < ttl) return hit.v;
  const v = await throttled(async () => {
    const r = await fetch(url, { headers: { 'user-agent': UA, accept: 'application/json' }, signal: AbortSignal.timeout(15000) });
    if (!r.ok) throw new Error('上游回應 HTTP ' + r.status);
    return r.json();
  });
  cache.set(url, { t: Date.now(), v });
  return v;
}
const MIN = 60e3, HOUR = 60 * MIN, DAY = 24 * HOUR;

/* ---------- 解析 ---------- */
function num(s) {
  if (s == null) return null;
  const t = String(s).replace(/,/g, '').trim();
  if (!t || t === '--' || t === '---') return null;
  const v = parseFloat(t);
  return Number.isFinite(v) ? v : null;
}
function rocToISO(s) {
  const m = String(s).trim().match(/^(\d{2,3})[\/\-](\d{1,2})[\/\-](\d{1,2})$/);
  return m ? `${+m[1] + 1911}-${m[2].padStart(2, '0')}-${m[3].padStart(2, '0')}` : null;
}
function rocCompactToISO(s) {
  const m = String(s).trim().match(/^(\d{2,3})(\d{2})(\d{2})$/);
  return m ? `${+m[1] + 1911}-${m[2]}-${m[3]}` : null;
}
function fieldIndex(fields, re) { return fields.findIndex(f => re.test(String(f))); }

// TWSE 個股日成交資訊(STOCK_DAY),成交股數為「股」
function parseTwseDay(j) {
  if (!j || j.stat !== 'OK' || !Array.isArray(j.data) || !Array.isArray(j.fields)) return { name: null, bars: [] };
  const f = j.fields;
  const iD = fieldIndex(f, /日期/), iV = fieldIndex(f, /成交股數/), iO = fieldIndex(f, /開盤/), iH = fieldIndex(f, /最高/), iL = fieldIndex(f, /最低/), iC = fieldIndex(f, /收盤/);
  const tm = String(j.title || '').match(/\s(\d{4,6}[A-Z]?)\s+(\S+)\s+各日成交資訊/);
  const bars = [];
  for (const r of j.data) {
    const d = rocToISO(r[iD]), c = num(r[iC]);
    if (!d || c == null) continue;
    bars.push({ d, o: num(r[iO]) ?? c, h: num(r[iH]) ?? c, l: num(r[iL]) ?? c, c, v: num(r[iV]) || 0 });
  }
  return { name: tm ? tm[2] : null, bars };
}
// TPEx 個股日成交資訊,成交量單位為「仟股」。新舊兩種回傳格式都處理。
function parseTpexDay(j) {
  const rows = (j && j.tables && j.tables[0] && j.tables[0].data) || (j && j.aaData) || [];
  const bars = [];
  for (const r of rows) {
    const d = rocToISO(r[0]), c = num(r[6]);
    if (!d || c == null) continue;
    bars.push({ d, o: num(r[3]) ?? c, h: num(r[4]) ?? c, l: num(r[5]) ?? c, c, v: (num(r[1]) || 0) * 1000 });
  }
  return { name: null, bars };
}

/* ---------- 資料抓取 ---------- */
function monthsBack(count) {
  const out = [], now = new Date(Date.now() + 8 * HOUR); // 台北時間
  let y = now.getUTCFullYear(), m = now.getUTCMonth() + 1;
  for (let i = 0; i < count; i++) {
    out.push({ y, m });
    m--; if (m === 0) { m = 12; y--; }
  }
  return out.reverse();
}
const isCurrentMonth = ({ y, m }) => { const n = new Date(Date.now() + 8 * HOUR); return n.getUTCFullYear() === y && n.getUTCMonth() + 1 === m; };

async function twseMonth(code, { y, m }) {
  const url = `https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date=${y}${String(m).padStart(2, '0')}01&stockNo=${code}`;
  return parseTwseDay(await getJSON(url, isCurrentMonth({ y, m }) ? 10 * MIN : 30 * DAY));
}
async function tpexMonth(code, { y, m }) {
  const mm = String(m).padStart(2, '0');
  const ttl = isCurrentMonth({ y, m }) ? 10 * MIN : 30 * DAY;
  try {
    const r = parseTpexDay(await getJSON(`https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock?code=${code}&date=${y}/${mm}/01&response=json`, ttl));
    if (r.bars.length) return r;
  } catch (e) { /* 改試舊版端點 */ }
  return parseTpexDay(await getJSON(`https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&d=${y - 1911}/${mm}&stkno=${code}`, ttl));
}

const marketCache = new Map();
async function detectMarket(code) {
  const hit = marketCache.get(code);
  if (hit && Date.now() - hit.t < DAY) return hit.v;
  const recent = monthsBack(2).reverse();
  let v = null;
  for (const mo of recent) { if ((await twseMonth(code, mo)).bars.length) { v = 'TWSE'; break; } }
  if (!v) for (const mo of recent) { if ((await tpexMonth(code, mo)).bars.length) { v = 'TPEX'; break; } }
  if (v) marketCache.set(code, { t: Date.now(), v });
  return v;
}

async function fundamentals(code, market) {
  try {
    if (market === 'TWSE') {
      const all = await getJSON('https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_ALL', HOUR);
      const r = all.find(x => String(x.Code).trim() === code);
      if (!r) return { name: null, f: null };
      return { name: r.Name || null, f: { pe: num(r.PEratio), pb: num(r.PBratio), dy: num(r.DividendYield), date: rocCompactToISO(r.Date) } };
    }
    const all = await getJSON('https://www.tpex.org.tw/openapi/v1/tpex_mainboard_peratio_analysis', HOUR);
    const key = (o, re) => Object.keys(o).find(k => re.test(k));
    const r = all.find(x => String(x[key(x, /Code$/i)] || '').trim() === code);
    if (!r) return { name: null, f: null };
    return {
      name: r[key(r, /Name$/i)] || null,
      f: { pe: num(r[key(r, /PriceEarning|PEratio/i)]), pb: num(r[key(r, /PriceBook|PBratio/i)]), dy: num(r[key(r, /Yield/i)]), date: rocCompactToISO(r[key(r, /^Date$/i)]) }
    };
  } catch (e) { return { name: null, f: null }; }
}

// 三大法人買賣超日報(T86),單位為「股」
async function t86(dateISO) {
  const ymd = dateISO.replace(/-/g, '');
  const j = await getJSON(`https://www.twse.com.tw/rwd/zh/fund/T86?date=${ymd}&selectType=ALL&response=json`, 12 * HOUR);
  if (!j || j.stat !== 'OK' || !Array.isArray(j.data)) return null;
  const f = j.fields;
  const idx = {
    foreign: fieldIndex(f, /^外陸資買賣超股數\(不含外資自營商\)$|^外資買賣超股數$/),
    trust: fieldIndex(f, /^投信買賣超股數$/),
    dealer: fieldIndex(f, /^自營商買賣超股數$/),
    total: fieldIndex(f, /^三大法人買賣超股數$/)
  };
  if (idx.foreign < 0) idx.foreign = fieldIndex(f, /外.*買賣超股數/);
  return { idx, rows: j.data };
}
async function institutional(code, dates) {
  const out = [];
  for (const d of dates) {
    let day = null;
    try { day = await t86(d); } catch (e) { continue; }
    if (!day) continue;
    const r = day.rows.find(x => String(x[0]).trim() === code);
    if (!r) continue;
    const g = i => (i >= 0 ? num(r[i]) || 0 : 0);
    out.push({ d, foreign: g(day.idx.foreign), trust: g(day.idx.trust), dealer: g(day.idx.dealer), total: g(day.idx.total) });
  }
  return out;
}

/* ---------- 路由 ---------- */
const send = (res, status, obj) => { res.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' }); res.end(JSON.stringify(obj)); };
const validCode = c => /^[0-9]{4,6}[A-Z]?$/.test(c);

async function apiStock(code) {
  const market = await detectMarket(code);
  if (!market) return [404, { error: `找不到代號 ${code}。請確認是上市或上櫃的股票/ETF 代號。` }];
  const bars = [];
  let name = null;
  for (const mo of monthsBack(14)) {
    const r = market === 'TWSE' ? await twseMonth(code, mo) : await tpexMonth(code, mo);
    if (r.name) name = r.name;
    bars.push(...r.bars);
  }
  if (!bars.length) return [502, { error: '交易所沒有回傳價格資料,請稍後再試。' }];
  const m = new Map(); bars.forEach(b => m.set(b.d, b));
  const sorted = Array.from(m.values()).sort((a, b) => (a.d < b.d ? -1 : 1));
  const fu = await fundamentals(code, market);
  return [200, { code, name: name || fu.name || code, market: market === 'TWSE' ? '上市' : '上櫃', bars: sorted, fundamental: fu.f }];
}
async function apiInst(code) {
  const market = await detectMarket(code);
  if (market !== 'TWSE') return [200, { rows: [] }];
  const [, stock] = await apiStock(code);
  const dates = (stock.bars || []).slice(-10).map(b => b.d);
  return [200, { rows: await institutional(code, dates) }];
}

/* ---------- 即時報價(證交所 MIS,非官方公開端點,盤中約每 5 秒更新) ---------- */
let misCookie = { v: '', t: 0 };
async function misSession() {
  if (misCookie.v && Date.now() - misCookie.t < 10 * MIN) return misCookie.v;
  const r = await fetch('https://mis.twse.com.tw/stock/index.jsp', { headers: { 'user-agent': UA }, signal: AbortSignal.timeout(10000) });
  const sc = typeof r.headers.getSetCookie === 'function' ? r.headers.getSetCookie() : [];
  const v = sc.map(s => s.split(';')[0]).join('; ');
  misCookie = { v, t: Date.now() };
  return v;
}
// 欄位:c 代號 n 名稱 z 最新成交價 y 昨收 o 開 h 高 l 低 v 累計成交量(張) t 時間 d 日期(YYYYMMDD) a/b 五檔賣價/買價(以 _ 分隔)
// 尚未成交時 z 為 "-",此時 price 為 null,前端不更新價格。
function parseMis(j) {
  const m = j && Array.isArray(j.msgArray) ? j.msgArray[0] : null;
  if (!m) return null;
  const first = s => { const x = String(s || '').split('_').map(num).find(v => v != null && v > 0); return x == null ? null : x; };
  const d = String(m.d || '');
  return {
    code: m.c, name: m.n || null,
    price: num(m.z), prev: num(m.y), open: num(m.o), high: num(m.h), low: num(m.l),
    volLots: num(m.v), time: m.t || null,
    date: /^\d{8}$/.test(d) ? `${d.slice(0, 4)}-${d.slice(4, 6)}-${d.slice(6, 8)}` : null,
    bid: first(m.b), ask: first(m.a)
  };
}
const quoteCache = new Map();
async function apiQuote(code) {
  const hit = quoteCache.get(code);
  if (hit && Date.now() - hit.t < 4000) return [200, hit.v];
  const market = await detectMarket(code);
  if (!market) return [404, { error: `找不到代號 ${code}` }];
  const ex = `${market === 'TWSE' ? 'tse' : 'otc'}_${code}.tw`;
  const url = `https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=${ex}&json=1&delay=0&_=${Date.now()}`;
  let q = null;
  for (let attempt = 0; attempt < 2 && !q; attempt++) {
    const cookie = await misSession();
    const r = await fetch(url, { headers: { 'user-agent': UA, accept: 'application/json', referer: 'https://mis.twse.com.tw/stock/fibest.jsp', cookie }, signal: AbortSignal.timeout(10000) });
    if (!r.ok) throw new Error('即時報價上游回應 HTTP ' + r.status);
    q = parseMis(await r.json());
    if (!q) misCookie = { v: '', t: 0 }; // 可能是 session 過期,重新取得後再試一次
  }
  if (!q) return [502, { error: '即時報價暫時沒有資料' }];
  quoteCache.set(code, { t: Date.now(), v: q });
  return [200, q];
}

// 同時支援兩種擺法:完整專案(dist/standalone.html)與只有 3 個檔案的精簡部署包(根目錄的 standalone.html)
const DIST = [path.join(__dirname, 'standalone.html'), path.join(__dirname, 'dist', 'standalone.html')].find(p => fs.existsSync(p)) || path.join(__dirname, 'dist', 'standalone.html');
const server = http.createServer(async (req, res) => {
  const u = new URL(req.url, 'http://x');
  try {
    if (u.pathname === '/' || u.pathname === '/index.html') {
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
      return res.end(fs.readFileSync(DIST));
    }
    if (u.pathname === '/healthz') return send(res, 200, { ok: true });
    if (u.pathname === '/api/stock' || u.pathname === '/api/inst' || u.pathname === '/api/quote') {
      const code = String(u.searchParams.get('code') || '').toUpperCase();
      if (!validCode(code)) return send(res, 400, { error: '股票代號格式不正確' });
      const [status, body] = u.pathname === '/api/stock' ? await apiStock(code) : u.pathname === '/api/inst' ? await apiInst(code) : await apiQuote(code);
      return send(res, status, body);
    }
    send(res, 404, { error: 'not found' });
  } catch (e) {
    send(res, 502, { error: '讀取交易所資料失敗:' + e.message });
  }
});

if (require.main === module) {
  if (!fs.existsSync(DIST)) { console.error('找不到 dist/standalone.html,請先執行 node build.js'); process.exit(1); }
  server.listen(PORT, () => console.log(`台股量化查詢 http://localhost:${PORT}/#2330`));
}
module.exports = { parseMis, parseTwseDay, parseTpexDay, rocToISO, rocCompactToISO, num, monthsBack };
