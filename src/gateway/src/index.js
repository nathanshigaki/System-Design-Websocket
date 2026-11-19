import http from 'node:http';
import { randomUUID } from 'node:crypto';
import { WebSocketServer, WebSocket } from 'ws';
import Redis from 'ioredis';
import pg from 'pg';
import {
  HEARTBEAT_INTERVAL_MS,
  JOBS_QUEUE,
  MAX_USER_ID_LENGTH,
  RING_KEY,
  ROOM_CHANNEL,
  SEQ_KEY,
  SESSION_TTL_SEC,
  sessionKey,
} from '../../shared/constants.js';
import { createTraceEmitter } from '../../shared/trace.js';

const GATEWAY_ID = process.env.GATEWAY_ID || 'gateway-1';
const PORT = Number(process.env.PORT || 4001);
const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
const DATABASE_URL =
  process.env.DATABASE_URL || 'postgres://pulsews:pulsews@127.0.0.1:5432/pulsews';

const redis = new Redis(REDIS_URL);
const redisSub = new Redis(REDIS_URL);
const pgPool = new pg.Pool({ connectionString: DATABASE_URL });
const emitHop = createTraceEmitter(redis);

/** @type {Set<{ ws: WebSocket, userId: string, connectionId: string }>} */
const localClients = new Set();

function sendJson(ws, payload) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(payload));
  }
}

async function readRingBufferAsc() {
  const raw = await redis.lrange(RING_KEY, 0, -1);
  return raw
    .map((entry) => {
      try {
        return JSON.parse(entry);
      } catch {
        return null;
      }
    })
    .filter(Boolean)
    .reverse();
}

async function recoverMissedMessages(ws, userId, lastSeenSeqId, currentSeqId) {
  const traceId = randomUUID();

  if (lastSeenSeqId >= currentSeqId) {
    sendJson(ws, {
      type: 'sync',
      recoverySource: 'up_to_date',
      currentSeqId,
      messages: [],
    });
    return;
  }

  await emitHop({
    traceId,
    flow: 'reconnect',
    step: 1,
    from: 'client',
    to: GATEWAY_ID,
    protocol: 'WebSocket',
    detail: `${userId} reconectou (lastSeenSeqId=${lastSeenSeqId})`,
  });

  const ringItems = await readRingBufferAsc();
  const minRingSeq = ringItems.length > 0 ? ringItems[0].seqId : Infinity;

  if (ringItems.length > 0 && lastSeenSeqId >= minRingSeq - 1) {
    const missed = ringItems.filter((item) => item.seqId > lastSeenSeqId);
    await emitHop({
      traceId,
      flow: 'reconnect',
      step: 2,
      from: 'redis',
      to: GATEWAY_ID,
      protocol: 'Ring Buffer',
      detail: `Replay em RAM: ${missed.length} msg(s) sem consultar PostgreSQL`,
    });
    await emitHop({
      traceId,
      flow: 'reconnect',
      step: 3,
      from: GATEWAY_ID,
      to: 'client',
      protocol: 'WebSocket',
      detail: `Entrega sync (${missed.length} msg(s))`,
    });

    sendJson(ws, {
      type: 'sync',
      recoverySource: 'ring_buffer',
      currentSeqId,
      messages: missed,
    });
    return;
  }

  // Fallback for long offline sessions older than Ring Buffer window
  const { rows } = await pgPool.query(
    `SELECT seq_id, user_id, content, gateway_id, created_at
     FROM messages
     WHERE seq_id > $1
     ORDER BY seq_id ASC
     LIMIT 200`,
    [lastSeenSeqId]
  );

  const missed = rows.map((row) => ({
    seqId: Number(row.seq_id),
    userId: row.user_id,
    content: row.content,
    gatewayId: row.gateway_id,
    createdAt: row.created_at,
  }));

  await emitHop({
    traceId,
    flow: 'reconnect',
    step: 2,
    from: 'postgres',
    to: GATEWAY_ID,
    protocol: 'SQL Fallback',
    detail: `Offline longo: ${missed.length} msg(s) recuperadas do PostgreSQL`,
  });
  await emitHop({
    traceId,
    flow: 'reconnect',
    step: 3,
    from: GATEWAY_ID,
    to: 'client',
    protocol: 'WebSocket',
    detail: `Entrega sync (${missed.length} msg(s))`,
  });

  sendJson(ws, {
    type: 'sync',
    recoverySource: 'postgres_offline',
    currentSeqId,
    messages: missed,
  });
}

async function registerSession(userId, connectionId) {
  const value = JSON.stringify({
    userId,
    gatewayId: GATEWAY_ID,
    connectionId,
    connectedAt: new Date().toISOString(),
  });
  await redis.set(sessionKey(userId), value, 'EX', SESSION_TTL_SEC);
}

async function cleanupSessionIfOwner(userId, connectionId) {
  try {
    const raw = await redis.get(sessionKey(userId));
    if (!raw) return;
    const parsed = JSON.parse(raw);
    if (parsed.connectionId === connectionId) {
      await redis.del(sessionKey(userId));
    }
  } catch (err) {
    console.error(`[${GATEWAY_ID}] session cleanup error:`, err.message);
  }
}

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', gatewayId: GATEWAY_ID, connections: localClients.size }));
    return;
  }
  res.writeHead(404);
  res.end('Not Found');
});

const wss = new WebSocketServer({ server });

wss.on('connection', async (ws, req) => {
  const reqUrl = new URL(req.url || '/', `http://localhost:${PORT}`);
  const rawUserId = (reqUrl.searchParams.get('userId') || '').trim();
  const rawLastSeq = reqUrl.searchParams.get('lastSeenSeqId');

  if (!rawUserId || rawUserId.length > MAX_USER_ID_LENGTH) {
    ws.close(4001, 'Invalid userId');
    return;
  }

  const userId = rawUserId;
  const lastSeenSeqId =
    rawLastSeq !== null && rawLastSeq !== '' && !Number.isNaN(Number(rawLastSeq))
      ? Number(rawLastSeq)
      : null;

  const connectionId = randomUUID();
  const clientEntry = { ws, userId, connectionId };
  ws.isAlive = true;
  localClients.add(clientEntry);

  try {
    await registerSession(userId, connectionId);
    const currentSeqId = Number((await redis.get(SEQ_KEY)) || 0);

    sendJson(ws, {
      type: 'welcome',
      userId,
      gatewayId: GATEWAY_ID,
      currentSeqId,
    });

    if (lastSeenSeqId !== null && lastSeenSeqId >= 0) {
      await recoverMissedMessages(ws, userId, lastSeenSeqId, currentSeqId);
    } else {
      const recent = (await readRingBufferAsc()).slice(-20);
      if (recent.length > 0) {
        sendJson(ws, {
          type: 'history',
          currentSeqId,
          messages: recent,
        });
      }
    }

    await redis.rpush(
      JOBS_QUEUE,
      JSON.stringify({
        type: 'user_joined',
        userId,
        gatewayId: GATEWAY_ID,
        receivedAt: new Date().toISOString(),
      })
    );
  } catch (err) {
    console.error(`[${GATEWAY_ID}] connection setup error:`, err.message);
  }

  ws.on('pong', async () => {
    ws.isAlive = true;
    try {
      await redis.expire(sessionKey(userId), SESSION_TTL_SEC);
    } catch (err) {
      console.error(`[${GATEWAY_ID}] session expire error:`, err.message);
    }
  });

  ws.on('message', async (data) => {
    let parsed;
    try {
      parsed = JSON.parse(data.toString());
    } catch {
      sendJson(ws, { type: 'error', message: 'JSON inválido' });
      return;
    }

    if (parsed.type === 'chat_message') {
      const traceId = parsed.traceId || randomUUID();
      await emitHop({
        traceId,
        flow: 'websocket',
        step: 3,
        from: GATEWAY_ID,
        to: 'redis',
        protocol: 'Redis List',
        detail: `RPUSH jobs:messages (${userId})`,
      });

      await redis.rpush(
        JOBS_QUEUE,
        JSON.stringify({
          traceId,
          type: 'chat',
          userId,
          content: String(parsed.content ?? ''),
          gatewayId: GATEWAY_ID,
          receivedAt: new Date().toISOString(),
        })
      );
    }
  });

  ws.on('close', async () => {
    localClients.delete(clientEntry);
    await cleanupSessionIfOwner(userId, connectionId);
    try {
      await redis.rpush(
        JOBS_QUEUE,
        JSON.stringify({
          type: 'user_left',
          userId,
          gatewayId: GATEWAY_ID,
          receivedAt: new Date().toISOString(),
        })
      );
    } catch (err) {
      console.error(`[${GATEWAY_ID}] enqueue user_left error:`, err.message);
    }
  });

  ws.on('error', (err) => {
    console.error(`[${GATEWAY_ID}] socket error (${userId}):`, err.message);
  });
});

await redisSub.subscribe(ROOM_CHANNEL);
redisSub.on('message', async (channel, rawPayload) => {
  if (channel !== ROOM_CHANNEL) return;
  let payload;
  try {
    payload = JSON.parse(rawPayload);
  } catch {
    return;
  }

  if (localClients.size === 0) return;

  await emitHop({
    traceId: payload.traceId,
    flow: 'websocket',
    step: 7,
    from: 'redis',
    to: GATEWAY_ID,
    protocol: 'Redis Pub/Sub',
    detail: `Entrega room:main (#seq ${payload.seqId})`,
  });

  await emitHop({
    traceId: payload.traceId,
    flow: 'websocket',
    step: 8,
    from: GATEWAY_ID,
    to: 'client',
    protocol: 'WebSocket',
    detail: `Push para ${localClients.size} cliente(s) local(is)`,
  });

  const outbound = {
    type: 'room_message',
    seqId: payload.seqId,
    userId: payload.userId,
    content: payload.content,
    gatewayId: payload.gatewayId,
    createdAt: payload.createdAt,
    traceId: payload.traceId,
  };

  for (const client of localClients) {
    sendJson(client.ws, outbound);
  }
});

const heartbeatTimer = setInterval(() => {
  for (const client of localClients) {
    if (client.ws.isAlive === false) {
      client.ws.terminate();
      continue;
    }
    client.ws.isAlive = false;
    client.ws.ping();
  }
}, HEARTBEAT_INTERVAL_MS);

async function shutdown() {
  console.log(`[${GATEWAY_ID}] shutting down...`);
  clearInterval(heartbeatTimer);
  for (const client of localClients) {
    client.ws.close(1001, 'Gateway shutting down');
  }
  wss.close();
  server.close();
  await Promise.allSettled([redisSub.quit(), redis.quit(), pgPool.end()]);
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

server.listen(PORT, () => {
  console.log(`[${GATEWAY_ID}] listening on :${PORT}`);
});
