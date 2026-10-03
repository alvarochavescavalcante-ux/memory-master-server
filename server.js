const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const WebSocket = require('ws');

const PORT = Number(process.env.PORT || 4173);
const HOST = process.env.HOST || '0.0.0.0';
const ROOM_TTL = 5 * 60 * 1000;
const CLIENT_ID = 'memory-master';
const SUBPROTOCOL = 'memory-master.v1';
const HTML_FILE = path.join(__dirname, 'SuperMemoryMaster.html');
const ALLOWED_ORIGINS = new Set((process.env.MEMORYMASTER_ORIGINS || [
  'http://127.0.0.1:4173',
  'http://localhost:4173',
  'https://memory-master-v2ud.onrender.com',
  'https://4173-i4lnt2y3lvivzrrr6pbqr-6844cfb5.us1.manus.computer'
].join(',')).split(',').map(v => v.trim()).filter(Boolean));

const rooms = new Map();
const publicQueue = [];
const clients = new Set();

function send(ws, type, payload = {}) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type, ...payload }));
}
function makeCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code;
  do { code = Array.from({length: 7}, () => chars[crypto.randomInt(chars.length)]).join(''); }
  while (rooms.has(code));
  return code;
}
function cleanQueue() {
  for (let i = publicQueue.length - 1; i >= 0; i--) {
    if (publicQueue[i].readyState !== WebSocket.OPEN || publicQueue[i].roomCode) publicQueue.splice(i, 1);
  }
}
function roomState(room) {
  return {
    code: room.code,
    players: [room.host, room.guest].filter(Boolean).length,
    ready: Number(Boolean(room.hostReady)) + Number(Boolean(room.guestReady))
  };
}
function broadcastRoom(room, type = 'roomState') {
  [room.host, room.guest].filter(Boolean).forEach(ws => send(ws, type, roomState(room)));
}
function removeRoom(room) {
  if (!room) return;
  clearTimeout(room.expiryTimer);
  rooms.delete(room.code);
  [room.host, room.guest].filter(Boolean).forEach(ws => {
    ws.roomCode = null;
    ws.role = null;
  });
}
function startCountdown(room) {
  if (!room || room.countdownStarted) return;
  room.countdownStarted = true;
  [3, 2, 1].forEach((number, index) => {
    setTimeout(() => broadcastRoom(room, 'countdown', { number }), index * 850);
  });
  setTimeout(() => {
    if (!rooms.has(room.code)) return;
    broadcastRoom(room, 'gameStart', { code: room.code });
    room.started = true;
  }, 3 * 850);
}
function maybeStart(room) {
  if (room && room.host && room.guest && room.hostReady && room.guestReady) startCountdown(room);
}
function attachToRoom(ws, room, role) {
  ws.roomCode = room.code;
  ws.role = role;
  if (role === 'host') room.host = ws; else room.guest = ws;
  send(ws, 'roomJoined', roomState(room));
  broadcastRoom(room);
  maybeStart(room);
}
function createRoom(ws) {
  if (ws.roomCode) return send(ws, 'error', { code: 'ALREADY_IN_ROOM', message: 'Você já está em uma sala.' });
  const room = { code: makeCode(), created: Date.now(), host: ws, guest: null, hostReady: false, guestReady: false, started: false, countdownStarted: false };
  rooms.set(room.code, room);
  ws.roomCode = room.code;
  ws.role = 'host';
  room.expiryTimer = setTimeout(() => {
    if (!room.started) {
      [room.host, room.guest].filter(Boolean).forEach(client => send(client, 'roomExpired'));
      removeRoom(room);
    }
  }, ROOM_TTL);
  send(ws, 'roomCreated', { code: room.code, expiresIn: ROOM_TTL, ...roomState(room) });
}
function joinRoom(ws, code) {
  const room = rooms.get(String(code || '').toUpperCase());
  if (!room || Date.now() - room.created > ROOM_TTL || room.guest || room.started) return send(ws, 'error', { code: 'INVALID_ROOM', message: 'Código inválido ou expirado.' });
  attachToRoom(ws, room, 'guest');
}
function sendProgress(ws, pairs, total) {
  const room = rooms.get(ws.roomCode);
  if (!room || !room.started) return;
  const safePairs = Math.max(0, Math.min(12, Number.isInteger(Number(pairs)) ? Number(pairs) : 0));
  const safeTotal = 12;
  const other = room.host === ws ? room.guest : room.host;
  if (other) send(other, 'opponentProgress', { pairs: safePairs, total: safeTotal });
}
function setReady(ws, value) {
  const room = rooms.get(ws.roomCode);
  if (!room || room.started) return;
  if (ws.role === 'host') room.hostReady = Boolean(value); else if (ws.role === 'guest') room.guestReady = Boolean(value);
  broadcastRoom(room);
  maybeStart(room);
}
function leaveRoom(ws) {
  const room = rooms.get(ws.roomCode);
  if (!room) return;
  const other = room.host === ws ? room.guest : room.host;
  if (other) send(other, 'opponentLeft');
  removeRoom(room);
}
function joinPublic(ws) {
  if (ws.roomCode) return;
  cleanQueue();
  const other = publicQueue.shift();
  if (!other) {
    publicQueue.push(ws);
    return send(ws, 'publicSearching', { players: 1 });
  }
  const room = { code: `PUBLIC-${crypto.randomBytes(4).toString('hex').toUpperCase()}`, created: Date.now(), host: other, guest: ws, hostReady: true, guestReady: true, started: false, countdownStarted: false };
  rooms.set(room.code, room);
  other.roomCode = room.code; other.role = 'host';
  ws.roomCode = room.code; ws.role = 'guest';
  broadcastRoom(room, 'publicMatchFound');
  startCountdown(room);
}
function onMessage(ws, raw) {
  let msg;
  try { msg = JSON.parse(raw); } catch { return send(ws, 'error', { code: 'BAD_MESSAGE' }); }
  switch (msg.type) {
    case 'createRoom': return createRoom(ws);
    case 'joinRoom': return joinRoom(ws, msg.code);
    case 'setReady': return setReady(ws, msg.ready);
    case 'progress': return sendProgress(ws, msg.pairs, msg.total);
    case 'publicQueue': return joinPublic(ws);
    case 'leaveRoom': return leaveRoom(ws);
    case 'ping': return send(ws, 'pong');
    default: return send(ws, 'error', { code: 'UNKNOWN_MESSAGE' });
  }
}

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, {'content-type': 'application/json'});
    return res.end(JSON.stringify({ok: true, service: 'memory-master', rooms: rooms.size, queued: publicQueue.length}));
  }
  if (req.url === '/' || req.url === '/SuperMemoryMaster.html') {
    res.writeHead(200, {'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store'});
    return fs.createReadStream(HTML_FILE).pipe(res);
  }
  res.writeHead(404); res.end('Not found');
});

const wss = new WebSocket.Server({
  noServer: true,
  handleProtocols(protocols) { return protocols.has(SUBPROTOCOL) ? SUBPROTOCOL : false; }
});
server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const origin = req.headers.origin || '';
  const protocolHeader = String(req.headers['sec-websocket-protocol'] || '').split(',').map(v => v.trim());
  const accepted = url.pathname === '/memorymaster' && url.searchParams.get('client') === CLIENT_ID && protocolHeader.includes(SUBPROTOCOL) && (!origin || ALLOWED_ORIGINS.has(origin));
  if (!accepted) { socket.write('HTTP/1.1 403 Forbidden\r\n\r\n'); return socket.destroy(); }
  wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req));
});
wss.on('connection', ws => {
  ws.id = crypto.randomUUID(); clients.add(ws);
  send(ws, 'connected', { service: 'memory-master', version: 1 });
  ws.on('message', data => onMessage(ws, data.toString()));
  ws.on('close', () => { clients.delete(ws); const i = publicQueue.indexOf(ws); if (i >= 0) publicQueue.splice(i, 1); leaveRoom(ws); });
  ws.on('error', () => {});
});
setInterval(() => { for (const room of rooms.values()) if (!room.started && Date.now() - room.created > ROOM_TTL) removeRoom(room); }, 30000);
server.listen(PORT, HOST, () => console.log(`Memory Master server listening on http://${HOST}:${PORT}`));
