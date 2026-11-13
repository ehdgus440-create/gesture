const test = require('node:test');
const assert = require('node:assert');
const WebSocket = require('ws');
const { server, store } = require('../server.js');

let port;
test.before(() => new Promise(r => server.listen(0, () => { port = server.address().port; r(); })));
test.after(() => new Promise(r => { for (const c of [...openSockets]) c.terminate(); server.close(() => r()); }));

const openSockets = new Set();
function client() {
  return new Promise(res => {
    const ws = new WebSocket(`ws://localhost:${port}/ws`); openSockets.add(ws);
    const inbox = []; const waiters = [];
    ws.on('message', b => { const m = JSON.parse(b); const i = waiters.findIndex(w => w.f(m)); if (i >= 0) waiters.splice(i, 1)[0].r(m); else inbox.push(m); });
    const next = f => new Promise(r => { const i = inbox.findIndex(f); if (i >= 0) return r(inbox.splice(i, 1)[0]); waiters.push({ f, r }); });
    let rid = 0;
    const call = m => { m.rid = ++rid; ws.send(JSON.stringify(m)); return next(x => x.t === 'res' && x.rid === m.rid); };
    ws.on('open', () => res({ ws, next, call, send: m => ws.send(JSON.stringify(m)) }));
  });
}

test('healthz and page are served', async () => {
  const h = await fetch(`http://localhost:${port}/healthz`); assert.equal(await h.text(), 'ok');
  const p = await fetch(`http://localhost:${port}/?room=ABCD`); const html = await p.text();
  assert.match(html, /<title>몸으로 말해요<\/title>/);
});

test('set → live snapshot to another client, update merges, delete', async () => {
  const a = await client(), b = await client();
  b.send({ t: 'sub', sid: 1, kind: 'doc', path: 'rooms/TEST' });
  const first = await b.next(m => m.t === 'snap' && m.sid === 1);
  assert.equal(first.r.exists, false);
  assert.equal((await a.call({ t: 'set', path: 'rooms/TEST', data: { x: 1, n: { a: 1 } } })).ok, true);
  const s2 = await b.next(m => m.t === 'snap' && m.sid === 1);
  assert.deepEqual(s2.r.data, { x: 1, n: { a: 1 } });
  await a.call({ t: 'update', path: 'rooms/TEST', data: { n: { b: 2 }, arr: [1] } });
  const s3 = await b.next(m => m.t === 'snap' && m.sid === 1);
  assert.deepEqual(s3.r.data, { x: 1, n: { a: 1, b: 2 }, arr: [1] });
  await a.call({ t: 'delete', path: 'rooms/TEST' });
  const s4 = await b.next(m => m.t === 'snap' && m.sid === 1);
  assert.equal(s4.r.exists, false);
});

test('update on missing doc and bad paths are rejected', async () => {
  const a = await client();
  assert.equal((await a.call({ t: 'update', path: 'rooms/NOPE', data: { x: 1 } })).err.code, 'invalid_argument');
  assert.equal((await a.call({ t: 'set', path: 'other/X', data: { x: 1 } })).ok, false);
  assert.equal((await a.call({ t: 'set', path: 'rooms/A/players', data: { x: 1 } })).ok, false);
});

test('query with where/orderBy/limit', async () => {
  const a = await client();
  for (const [id, t] of [['r1', 10], ['r2', 30], ['r3', 20]]) await a.call({ t: 'set', path: 'rooms/' + id.toUpperCase() + 'Q', data: { createdAt: t } });
  const r = await a.call({ t: 'get', q: { kind: 'query', path: 'rooms', filters: [['createdAt', '>', 15]], order: ['createdAt', 'desc'], limit: 5 } });
  assert.deepEqual(r.r.docs.map(d => d.data.createdAt), [30, 20]);
  for (const k of [...store.keys()]) store.delete(k);
});
