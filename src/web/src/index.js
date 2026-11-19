import http from 'node:http';
import net from 'node:net';
import fs from 'node:fs/promises';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { WebSocketServer, WebSocket } from 'ws';
import Redis from 'ioredis';
import pg from 'pg';
import {
  RING_BUFFER_SIZE,
  RING_KEY,
  SEQ_KEY,
  SESSION_PREFIX,
  TRACE_CHANNEL,
} from '../../shared/constants.js';
import { createTraceEmitter } from '../../shared/trace.js';

const PORT = Number(process.env.PORT || 3000);
const LB_HOST = process.env.LB_HOST || '127.0.0.1';
const LB_PORT = Number(process.env.LB_PORT || 8080);
const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
const DATABASE_URL =
  process.env.DATABASE_URL || 'postgres://pulsews:pulsews@127.0.0.1:5432/pulsews';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PUBLIC_DIR = path.resolve(__dirname, '../public');

const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
};

const redis = new Redis(REDIS_URL);
const redisSub = new Redis(REDIS_URL);
const pgPool = new pg.Pool({ connectionString: DATABASE_URL });
const emitHop = createTraceEmitter(redis);

function sendJson(res, statusCode, data) {
  res.writeHead(statusCode, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
  });
  res.end(JSON.stringify(data));
}

async function handleNotifications(reqUrl, res) {
  const afterIdRaw = Number(reqUrl.searchParams.get('afterId') || 0);
  const afterId = Number.isFinite(afterIdRaw) && afterIdRaw >= 0 ? afterIdRaw : 0;
  const traceId = randomUUID();

  await emitHop({
    traceId,
    flow: 'polling',
    step: 1,
    from: 'client',
    to: 'web',
    protocol: 'HTTP GET',
    detail: `Short Polling /api/notifications?afterId=${afterId}`,
  });

  await emitHop({
    traceId,
    flow: 'polling',
    step: 2,
    from: 'web',
    to: 'postgres',
    protocol: 'SQL SELECT',
    detail: `WHERE id > ${afterId}`,
  });

  const { rows } = await pgPool.query(
    `SELECT id, type, user_id, message, created_at
     FROM notifications
     WHERE id > $1
     ORDER BY id ASC
     LIMIT 50`,
    [afterId]
  );

  const notifications = rows.map((row) => ({
    id: Number(row.id),
    type: row.type,
    userId: row.user_id,
    message: row.message,
    createdAt: row.created_at,
  }));

  const lastId =
    notifications.length > 0 ? notifications[notifications.length - 1].id : afterId;
  const empty = notifications.length === 0;

  await emitHop({
    traceId,
    flow: 'polling',
    step: 3,
    from: 'postgres',
    to: 'web',
    protocol: 'SQL Result',
    detail: empty ? '0 linhas' : `${notifications.length} notificacao(oes)`,
  });

  await emitHop({
    traceId,
    flow: 'polling',
    step: 4,
    from: 'web',
    to: 'client',
    protocol: 'HTTP 200',
    detail: empty ? 'Resposta vazia (requisicao fantasma)' : `${notifications.length} nova(s)`,
  });

  sendJson(res, 200, { notifications, lastId, empty });
}

async function handleClusterStatus(res) {
  const keys = await redis.keys(`${SESSION_PREFIX}*`);
  let sessions = [];

  if (keys.length > 0) {
    const values = await redis.mget(keys);
    sessions = values
      .map((val) => {
        if (!val) return null;
        try {
          return JSON.parse(val);
        } catch {
          return null;
        }
      })
      .filter(Boolean)
      .sort((a, b) => a.userId.localeCompare(b.userId));
  }

  const summaryByGateway = { 'gateway-1': 0, 'gateway-2': 0 };
  for (const s of sessions) {
    summaryByGateway[s.gatewayId] = (summaryByGateway[s.gatewayId] || 0) + 1;
  }

  const [seqRaw, ringLen] = await Promise.all([redis.get(SEQ_KEY), redis.llen(RING_KEY)]);

  sendJson(res, 200, {
    sessions,
    summaryByGateway,
    currentSeqId: Number(seqRaw || 0),
    ringBufferSize: Number(ringLen || 0),
    ringBufferCapacity: RING_BUFFER_SIZE,
  });
}

async function serveStatic(reqUrl, res) {
  const cleanPath = reqUrl.pathname === '/' ? '/index.html' : reqUrl.pathname;
  const resolved = path.resolve(PUBLIC_DIR, `.${cleanPath}`);
  if (!resolved.startsWith(PUBLIC_DIR)) {
    res.writeHead(403);
    res.end('Forbidden');
    return;
  }

  try {
    const content = await fs.readFile(resolved);
    const ext = path.extname(resolved);
    res.writeHead(200, {
      'Content-Type': MIME_TYPES[ext] || 'application/octet-stream',
      'Cache-Control': 'no-cache',
    });
    res.end(content);
  } catch {
    res.writeHead(404);
    res.end('Not Found');
  }
}

const server = http.createServer(async (req, res) => {
  const reqUrl = new URL(req.url || '/', `http://localhost:${PORT}`);

  try {
    if (req.method === 'GET' && reqUrl.pathname === '/api/notifications') {
      await handleNotifications(reqUrl, res);
      return;
    }

    if (req.method === 'GET' && reqUrl.pathname === '/api/cluster') {
      await handleClusterStatus(res);
      return;
    }

    if (req.method === 'GET' && reqUrl.pathname === '/health') {
      sendJson(res, 200, { status: 'ok' });
      return;
    }

    await serveStatic(reqUrl, res);
  } catch (err) {
    console.error('[web] request error:', err.message);
    sendJson(res, 500, { error: 'Internal server error' });
  }
});

const traceWss = new WebSocketServer({ noServer: true });

await redisSub.subscribe(TRACE_CHANNEL);
redisSub.on('message', (channel, payload) => {
  if (channel !== TRACE_CHANNEL) return;
  for (const client of traceWss.clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  }
});

function proxyWebSocketToLoadBalancer(req, socket, head) {
  const upstream = net.connect(LB_PORT, LB_HOST, () => {
    let rawRequest = `${req.method} ${req.url} HTTP/${req.httpVersion}\r\n`;
    for (let i = 0; i < req.rawHeaders.length; i += 2) {
      rawRequest += `${req.rawHeaders[i]}: ${req.rawHeaders[i + 1]}\r\n`;
    }
    rawRequest += '\r\n';
    upstream.write(rawRequest);
    if (head && head.length > 0) {
      upstream.write(head);
    }
    upstream.pipe(socket);
    socket.pipe(upstream);
  });

  const destroyBoth = () => {
    if (!socket.destroyed) socket.destroy();
    if (!upstream.destroyed) upstream.destroy();
  };

  upstream.on('error', (err) => {
    console.error('[web] upstream L4 proxy error:', err.message);
    destroyBoth();
  });
  socket.on('error', destroyBoth);
  upstream.on('close', destroyBoth);
  socket.on('close', destroyBoth);
}

server.on('upgrade', (req, socket, head) => {
  const reqUrl = new URL(req.url || '/', `http://localhost:${PORT}`);

  if (reqUrl.pathname === '/ws-trace') {
    traceWss.handleUpgrade(req, socket, head, (ws) => {
      traceWss.emit('connection', ws, req);
    });
    return;
  }

  if (reqUrl.pathname === '/ws') {
    proxyWebSocketToLoadBalancer(req, socket, head);
    return;
  }

  socket.destroy();
});

async function shutdown() {
  console.log('[web] shutting down...');
  traceWss.close();
  server.close();
  await Promise.allSettled([redisSub.quit(), redis.quit(), pgPool.end()]);
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

server.listen(PORT, () => {
  console.log(`[web] listening on :${PORT} (proxying /ws -> ${LB_HOST}:${LB_PORT})`);
});
