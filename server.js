// 9ja Sega relay: pairs two phones in a room and passes their game messages back and forth.
// The games stay in charge of the rules; the relay only knows rooms, slots and who is connected.
import http from 'http';
import { WebSocketServer } from 'ws';
import crypto from 'crypto';

const PORT = process.env.PORT || 8787;
const GAMES = new Set(['biro', 'cork', 'ayo', 'fight']);
const ALPHA = 'ABCDEFGHJKMNPQRSTUVWXYZ';          // no I, L, O: easy to read out loud
const GRACE_MS = 2 * 60 * 1000;                   // a dropped player can rejoin within 2 minutes
const IDLE_MS = 45 * 60 * 1000;                    // empty or quiet rooms are cleaned up
const MAX_MSG = 16 * 1024, MAX_RATE = 150;         // bytes per message, messages per second

const rooms = new Map();   // code -> {game, slots: [slot|null, slot|null], last}
let served = 0;

const newCode = () => { let c; do c = Array.from({ length: 4 }, () => ALPHA[crypto.randomInt(ALPHA.length)]).join(''); while (rooms.has(c)); return c; };
const send = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };
const other = (room, i) => room.slots[1 - i];

const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.url === '/health') { res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify({ ok: true, rooms: rooms.size, served })); return; }
  res.setHeader('Content-Type', 'text/plain'); res.end('9ja Sega relay is up. Games connect to /ws.');
});
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: MAX_MSG });

function attach(ws, code, i, token) {
  const room = rooms.get(code);
  room.slots[i] = { ws, token, on: true, gone: 0 };
  room.last = Date.now();
  ws.room = code; ws.slot = i;
}

wss.on('connection', (ws) => {
  served++;
  ws.isAlive = true; ws.on('pong', () => (ws.isAlive = true));
  let bucket = 0, bucketT = Date.now();

  ws.on('message', (raw) => {
    const now = Date.now();
    if (now - bucketT > 1000) { bucket = 0; bucketT = now; }
    if (++bucket > MAX_RATE) return;
    let m; try { m = JSON.parse(raw); } catch { return; }
    const room = ws.room && rooms.get(ws.room);

    if (m.t === 'create') {
      if (!GAMES.has(m.game)) return send(ws, { t: 'error', code: 'bad-game' });
      const code = newCode(), token = crypto.randomUUID();
      rooms.set(code, { code, game: m.game, slots: [null, null], last: now });
      attach(ws, code, 0, token);
      send(ws, { t: 'room', code, slot: 0, token, game: m.game });
    } else if (m.t === 'join') {
      const code = String(m.code || '').toUpperCase(), r = rooms.get(code);
      if (!r) return send(ws, { t: 'error', code: 'no-room' });
      if (m.game && r.game !== m.game) return send(ws, { t: 'error', code: 'wrong-game', game: r.game });
      if (r.slots[1] && (r.slots[1].on || now - r.slots[1].gone < GRACE_MS)) return send(ws, { t: 'error', code: 'full' });
      const token = crypto.randomUUID();
      attach(ws, code, 1, token);
      send(ws, { t: 'room', code, slot: 1, token, game: r.game });
      const h = r.slots[0];
      if (h && h.on) { send(h.ws, { t: 'peer', on: true }); send(ws, { t: 'peer', on: true }); }
    } else if (m.t === 'rejoin') {
      const code = String(m.code || '').toUpperCase(), r = rooms.get(code);
      const i = r ? r.slots.findIndex((s) => s && s.token === m.token) : -1;
      if (i < 0) return send(ws, { t: 'error', code: 'no-room' });
      const old = r.slots[i].ws; if (old && old !== ws) { old.room = null; try { old.close(); } catch {} }
      attach(ws, code, i, m.token);
      send(ws, { t: 'room', code, slot: i, token: m.token, game: r.game, rejoined: true });
      const o = other(r, i);
      if (o && o.on) { send(o.ws, { t: 'peer', on: true, back: true }); send(ws, { t: 'peer', on: true }); }
    } else if (m.t === 'msg') {
      if (!room) return;
      room.last = now;
      const o = other(room, ws.slot);
      if (o && o.on) send(o.ws, { t: 'msg', d: m.d });
    } else if (m.t === 'ping') {
      send(ws, { t: 'pong', id: m.id });
    } else if (m.t === 'leave') {
      if (!room) return;
      const o = other(room, ws.slot);
      room.slots[ws.slot] = null; ws.room = null;
      if (o && o.on) send(o.ws, { t: 'peer', on: false, left: true });
      if (!room.slots[0] && !room.slots[1]) rooms.delete(room.code);
    }
  });

  ws.on('close', () => {
    const room = ws.room && rooms.get(ws.room);
    if (!room) return;
    const s = room.slots[ws.slot];
    if (s && s.ws === ws) { s.on = false; s.gone = Date.now(); }
    const o = other(room, ws.slot);
    if (o && o.on) send(o.ws, { t: 'peer', on: false });
  });
});

// heartbeat + cleanup
setInterval(() => {
  for (const ws of wss.clients) { if (!ws.isAlive) { ws.terminate(); continue; } ws.isAlive = false; try { ws.ping(); } catch {} }
  const now = Date.now();
  for (const [code, r] of rooms) {
    const dead = r.slots.every((s) => !s || (!s.on && now - s.gone > GRACE_MS));
    if (dead || now - r.last > IDLE_MS) rooms.delete(code);
  }
}, 15000);

server.listen(PORT, () => console.log('9ja Sega relay on :' + PORT));

// Railway stops old containers with SIGTERM on every deploy: close sockets and exit cleanly
for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, () => { for (const ws of wss.clients) try { ws.close(1012, 'restarting'); } catch {} server.close(() => process.exit(0)); setTimeout(() => process.exit(0), 1500); });
