// 몸으로 말해요 — 게임 서버
// 방·팀·자리·제시어 상태를 서버 메모리에 보관하고, 바뀔 때마다 접속한 모두에게 실시간으로 보내요.
// 로그인 없이 링크만으로 접속할 수 있어요.
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const PAGE = path.join(__dirname, 'public', 'index.html');
const MAX_DOC_BYTES = 64 * 1024;
const MAX_DOCS = 20000;
const ROOM_TTL_MS = 24 * 60 * 60 * 1000; // 만든 지 하루 지난 방은 정리
const SEG = /^[A-Za-z0-9_\-.~:@+]{1,120}$/;

const ICON = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#ff4d8d"/><stop offset="1" stop-color="#ff7a59"/></linearGradient></defs><rect width="512" height="512" rx="112" fill="#1d1238"/><circle cx="256" cy="256" r="196" fill="url(#g)"/><text x="256" y="320" text-anchor="middle" font-size="230">🙆</text></svg>`;

/* ---------- 저장소 (메모리) ---------- */
const store = new Map(); // path -> data
const conns = new Set();

function validPath(p, wantDoc) {
  if (typeof p !== 'string' || p.length > 600) return false;
  const s = p.split('/');
  if (s.length > 16 || s[0] !== 'rooms' || !s.every(x => SEG.test(x) && x !== '.' && x !== '..')) return false;
  return wantDoc ? s.length % 2 === 0 : s.length % 2 === 1;
}
const isObj = v => v && typeof v === 'object' && !Array.isArray(v);
function merge(a, b) {
  const o = Object.assign({}, a);
  for (const k of Object.keys(b)) {
    const v = b[k];
    o[k] = isObj(v) && isObj(o[k]) ? merge(o[k], v) : v;
  }
  return o;
}
function getDoc(p) {
  const id = p.split('/').pop();
  return store.has(p) ? { id, exists: true, data: store.get(p) } : { id, exists: false };
}
const OPS = {
  '==': (x, v) => x === v, '!=': (x, v) => x !== v, '<': (x, v) => x < v, '<=': (x, v) => x <= v,
  '>': (x, v) => x > v, '>=': (x, v) => x >= v,
  'in': (x, v) => Array.isArray(v) && v.includes(x), 'not-in': (x, v) => Array.isArray(v) && !v.includes(x),
  'array-contains': (x, v) => Array.isArray(x) && x.includes(v),
};
function runQuery(q) {
  const col = q.path, n = col.split('/').length;
  let docs = [];
  for (const [k, data] of store) {
    if (k.startsWith(col + '/') && k.split('/').length === n + 1) docs.push({ id: k.slice(col.length + 1), data });
  }
  for (const f of (Array.isArray(q.filters) ? q.filters : []).slice(0, 10)) {
    const fn = OPS[f[1]]; if (!fn) continue;
    docs = docs.filter(d => fn(d.data[f[0]], f[2]));
  }
  if (Array.isArray(q.order)) {
    const [f, dir] = q.order;
    docs.sort((a, b) => {
      const x = a.data[f], y = b.data[f];
      if (x === undefined && y === undefined) return a.id < b.id ? -1 : 1;
      if (x === undefined) return 1; if (y === undefined) return -1;
      const d = x < y ? -1 : x > y ? 1 : 0;
      return dir === 'desc' ? -d : d;
    });
  } else docs.sort((a, b) => (a.id < b.id ? -1 : 1));
  const lim = Math.min(1000, Math.max(0, +q.limit || 0)) || 1000;
  return { docs: docs.slice(0, lim) };
}
const evalSub = s => (s.kind === 'doc' ? getDoc(s.path) : runQuery(s));

let pushQueued = false;
function schedulePush() {
  if (pushQueued) return; pushQueued = true;
  setImmediate(() => {
    pushQueued = false;
    for (const c of conns) for (const s of c.subs.values()) sendSnap(c, s);
  });
}
function sendSnap(c, s) {
  const r = evalSub(s); const j = JSON.stringify(r);
  if (j === s.last) return; s.last = j;
  send(c.ws, { t: 'snap', sid: s.sid, r });
}
function send(ws, m) { try { if (ws.readyState === 1) ws.send(JSON.stringify(m)); } catch (e) {} }
const bad = msg => ({ code: 'invalid_argument', message: msg });

function handle(c, m) {
  const reply = (ok, r, err) => send(c.ws, { t: 'res', rid: m.rid, ok, r, err });
  switch (m.t) {
    case 'sub': {
      const s = { sid: m.sid, kind: m.kind === 'doc' ? 'doc' : 'query', path: m.path, filters: m.filters, order: m.order, limit: m.limit };
      if (!validPath(s.path, s.kind === 'doc') || c.subs.size >= 64) return send(c.ws, { t: 'snaperr', sid: m.sid, err: bad('bad subscription') });
      c.subs.set(m.sid, s); sendSnap(c, s); return;
    }
    case 'unsub': c.subs.delete(m.sid); return;
    case 'get': {
      const q = m.q || {}; const kind = q.kind === 'doc' ? 'doc' : 'query';
      if (!validPath(q.path, kind === 'doc')) return reply(false, null, bad('bad path'));
      return reply(true, kind === 'doc' ? getDoc(q.path) : runQuery(q));
    }
    case 'set': case 'update': {
      if (!validPath(m.path, true) || !isObj(m.data)) return reply(false, null, bad('bad write'));
      if (JSON.stringify(m.data).length > MAX_DOC_BYTES) return reply(false, null, bad('too big'));
      if (m.t === 'update' && !store.has(m.path)) return reply(false, null, bad('document missing'));
      if (m.t === 'set' && !store.has(m.path) && store.size >= MAX_DOCS) return reply(false, null, { code: 'quota_exceeded', message: 'full' });
      store.set(m.path, m.t === 'set' ? m.data : merge(store.get(m.path), m.data));
      reply(true); schedulePush(); return;
    }
    case 'delete': {
      if (!validPath(m.path, true)) return reply(false, null, bad('bad path'));
      store.delete(m.path); reply(true); schedulePush(); return;
    }
  }
}

/* ---------- HTTP ---------- */
const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/healthz') { res.writeHead(200); res.end('ok'); return; }
  if (url === '/icon.svg') { res.writeHead(200, { 'content-type': 'image/svg+xml', 'cache-control': 'public, max-age=86400' }); res.end(ICON); return; }
  if (url === '/manifest.json') {
    res.writeHead(200, { 'content-type': 'application/manifest+json' });
    res.end(JSON.stringify({ name: '몸으로 말해요', short_name: '몸으로말해요', start_url: '/', display: 'standalone', background_color: '#1d1238', theme_color: '#1d1238', icons: [{ src: '/icon.svg', sizes: 'any', type: 'image/svg+xml' }] }));
    return;
  }
  fs.readFile(PAGE, (err, buf) => {
    if (err) { res.writeHead(500); res.end('page error'); return; }
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' });
    res.end(buf);
  });
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: MAX_DOC_BYTES + 4096 });
wss.on('connection', ws => {
  const c = { ws, subs: new Map(), n: 0, t0: Date.now() };
  ws.alive = true; conns.add(c);
  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', buf => {
    let m; try { m = JSON.parse(buf); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;
    // 너무 잦은 요청 막기 (초당 150개) — 막힌 요청엔 바로 '잠시 후 다시' 응답
    const now = Date.now(); if (now - c.t0 > 1000) { c.t0 = now; c.n = 0; }
    if (++c.n > 150) { if (m.rid) send(ws, { t: 'res', rid: m.rid, ok: false, err: { code: 'resource_exhausted', message: 'too fast' } }); return; }
    try { handle(c, m); } catch (e) { console.error('handler error', e); }
  });
  ws.on('close', () => conns.delete(c));
});

// 끊긴 연결 + 오래된 방 정리
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) { try { ws.terminate(); } catch (e) {} continue; }
    ws.alive = false; try { ws.ping(); } catch (e) {}
  }
  const now = Date.now(); let changed = false;
  for (const [k, d] of store) {
    const s = k.split('/');
    if (s.length === 2 && d && d.createdAt && now - d.createdAt > ROOM_TTL_MS) {
      for (const k2 of [...store.keys()]) if (k2 === k || k2.startsWith(k + '/')) store.delete(k2);
      changed = true;
    }
  }
  if (changed) schedulePush();
}, 25000).unref();

if (require.main === module) server.listen(PORT, () => console.log(`몸으로 말해요 서버 실행 중: http://localhost:${PORT}`));
module.exports = { server, store };
