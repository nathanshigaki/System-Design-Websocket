import http from 'node:http';
import { randomUUID } from 'node:crypto';
import { WebSocketServer, WebSocket } from 'ws';
import Redis from 'ioredis';
import pg from 'pg';
import {
  GW_ALIVE_TTL_SEC,
  GW_HEARTBEAT_INTERVAL_MS,
  HEARTBEAT_INTERVAL_MS,
  JOBS_STREAM,
  MAX_MESSAGE_LENGTH,
  MAX_USER_ID_LENGTH,
  RATE_LIMIT_MAX_MSGS,
  RATE_LIMIT_WINDOW_MS,
  RING_KEY,
  ROOM_CHANNEL,
  SEQ_KEY,
  SESSION_TTL_SEC,
  SQL_RECOVERY_PAGE_SIZE,
  STREAM_MAXLEN,
  WS_MAX_PAYLOAD_BYTES,
  gatewayAliveKey,
  sessionKey,
} from '../../shared/constants.js';
import {
  createPgPool,
  createRedisClient,
  getOrLoadRoomEpoch,
  installProcessErrorHandlers,
} from '../../shared/clients.js';
import { createTraceEmitter } from '../../shared/trace.js';

const GATEWAY_ID = process.env.GATEWAY_ID || 'gateway-1';
const GATEWAY_BOOT_ID = randomUUID();
const PORT = Number(process.env.PORT || 4001);
const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
const DATABASE_URL =
  process.env.DATABASE_URL || 'postgres://pulsews:pulsews@127.0.0.1:5432/pulsews';

const USER_ID_PATTERN = /^[\p{L}\p{N}_\- .]{1,32}$/u;

installProcessErrorHandlers(GATEWAY_ID);

const redis = createRedisClient(Redis, REDIS_URL, `${GATEWAY_ID}:cmd`);
const redisSub = createRedisClient(Redis, REDIS_URL, `${GATEWAY_ID}:sub`);
const pgPool = createPgPool(pg, DATABASE_URL, GATEWAY_ID);
const emitHop = createTraceEmitter(redis);

/**
 * @type {Set<{
 *   ws: WebSocket,
 *   userId: string,
 *   connectionId: string,
 *   lastSeqId: number,
 *   rateTimestamps: number[]
 * }>}
 */
const localClients = new Set();

function sendJson(ws, payload) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(payload));
  }
}

function withTimeout(promise, ms, label) {
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      const timer = setTimeout(() => reject(new Error(`${label} timeout (${ms}ms)`)), ms);
      timer.unref?.();
    }),
  ]);
}

async function enqueueJob(job) {
  await redis.xadd(
    JOBS_STREAM,
    'MAXLEN',
    '~',
    STREAM_MAXLEN,
    '*',
    'payload',
    JSON.stringify(job)
  );
}

async function publishGatewayAlive() {
  if (redis.status !== 'ready') return;
  try {
    await redis.set(
      gatewayAliveKey(GATEWAY_ID),
      JSON.stringify({
        gatewayId: GATEWAY_ID,
        bootId: GATEWAY_BOOT_ID,
        updatedAt: Date.now(),
      }),
      'EX',
      GW_ALIVE_TTL_SEC
    );
  } catch (err) {
    console.error(`[${GATEWAY_ID}] liveness heartbeat error:`, err.message);
  }
}

async function getAuthoritativeSeqId() {
  let redisSeq = 0;
  try {
    redisSeq = Number((await redis.get(SEQ_KEY)) || 0);
  } catch {
    redisSeq = 0;
  }

  if (redisSeq > 0) {
    return redisSeq;
  }

  try {
    const { rows } = await pgPool.query(
      'SELECT COALESCE(MAX(seq_id), 0) AS max_seq FROM messages'
    );
    const dbSeq = Number(rows[0]?.max_seq || 0);
    if (dbSeq > 0 && redis.status === 'ready') {
      await redis.set(SEQ_KEY, String(dbSeq), 'NX').catch(() => {});
    }
    return dbSeq;
  } catch {
    return redisSeq;
  }
}

async function readRingBufferAsc() {
  try {
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
  } catch {
    return [];
  }
}

async function readPostgresMessagesAfter(afterSeqId, limit = SQL_RECOVERY_PAGE_SIZE) {
  const { rows } = await pgPool.query(
    `SELECT seq_id, user_id, content, gateway_id, created_at
     FROM messages
     WHERE seq_id > $1
     ORDER BY seq_id ASC
     LIMIT $2`,
    [afterSeqId, limit + 1]
  );

  const hasMore = rows.length > limit;
  const slice = hasMore ? rows.slice(0, limit) : rows;
  const messages = slice.map((row) => ({
    seqId: Number(row.seq_id),
    userId: row.user_id,
    content: row.content,
    gatewayId: row.gateway_id,
    createdAt: row.created_at,
  }));

  return { messages, hasMore };
}

async function recoverMissedMessages(
  clientEntry,
  lastSeenSeqId,
  currentSeqId,
  epoch,
  clientEpoch = null
) {
  const { ws, userId } = clientEntry;
  const traceId = randomUUID();

  const epochChanged = Boolean(clientEpoch && epoch && clientEpoch !== epoch);
  const seqResetDetected = lastSeenSeqId > currentSeqId;
  const resetRequired = epochChanged || seqResetDetected;
  const effectiveAfterSeqId = resetRequired ? 0 : lastSeenSeqId;

  if (!resetRequired && effectiveAfterSeqId >= currentSeqId) {
    clientEntry.lastSeqId = Math.max(clientEntry.lastSeqId, currentSeqId);
    sendJson(ws, {
      type: 'sync',
      recoverySource: 'up_to_date',
      currentSeqId,
      epoch,
      resetRequired: false,
      hasMore: false,
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
  const maxRingSeq = ringItems.length > 0 ? ringItems[ringItems.length - 1].seqId : 0;

  if (
    !resetRequired &&
    ringItems.length > 0 &&
    effectiveAfterSeqId >= minRingSeq - 1 &&
    maxRingSeq >= currentSeqId
  ) {
    const missed = ringItems.filter((item) => item.seqId > effectiveAfterSeqId);
    if (missed.length > 0) {
      clientEntry.lastSeqId = Math.max(
        clientEntry.lastSeqId,
        missed[missed.length - 1].seqId
      );
    }

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
      epoch,
      resetRequired: false,
      hasMore: false,
      messages: missed,
    });
    return;
  }

  // Fallback for long offline sessions older than Ring Buffer window or after Redis flush
  const { messages: missed, hasMore } = await readPostgresMessagesAfter(
    effectiveAfterSeqId,
    SQL_RECOVERY_PAGE_SIZE
  );

  if (missed.length > 0) {
    clientEntry.lastSeqId = resetRequired
      ? missed[missed.length - 1].seqId
      : Math.max(clientEntry.lastSeqId, missed[missed.length - 1].seqId);
  } else if (resetRequired) {
    clientEntry.lastSeqId = currentSeqId;
  }

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
    epoch,
    resetRequired,
    hasMore,
    messages: missed,
  });
}

async function registerSession(userId, connectionId) {
  const value = JSON.stringify({
    userId,
    gatewayId: GATEWAY_ID,
    bootId: GATEWAY_BOOT_ID,
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

function checkRateLimit(clientEntry) {
  const now = Date.now();
  const cutoff = now - RATE_LIMIT_WINDOW_MS;
  while (clientEntry.rateTimestamps.length > 0 && clientEntry.rateTimestamps[0] < cutoff) {
    clientEntry.rateTimestamps.shift();
  }
  if (clientEntry.rateTimestamps.length >= RATE_LIMIT_MAX_MSGS) {
    return false;
  }
  clientEntry.rateTimestamps.push(now);
  return true;
}

const server = http.createServer(async (req, res) => {
  if (req.url === '/health') {
    try {
      await Promise.all([
        withTimeout(redis.ping(), 800, 'redis ping'),
        withTimeout(pgPool.query('SELECT 1'), 800, 'postgres ping'),
      ]);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(
        JSON.stringify({
          status: 'ok',
          gatewayId: GATEWAY_ID,
          bootId: GATEWAY_BOOT_ID,
          connections: localClients.size,
        })
      );
    } catch (err) {
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(
        JSON.stringify({
          status: 'degraded',
          gatewayId: GATEWAY_ID,
          error: err.message,
        })
      );
    }
    return;
  }
  res.writeHead(404);
  res.end('Not Found');
});

const wss = new WebSocketServer({
  server,
  maxPayload: WS_MAX_PAYLOAD_BYTES,
});

wss.on('connection', async (ws, req) => {
  const reqUrl = new URL(req.url || '/', `http://localhost:${PORT}`);
  const rawUserId = (reqUrl.searchParams.get('userId') || '').trim();
  const rawLastSeq = reqUrl.searchParams.get('lastSeenSeqId');
  const clientEpoch = reqUrl.searchParams.get('epoch') || null;

  if (
    !rawUserId ||
    rawUserId.length > MAX_USER_ID_LENGTH ||
    !USER_ID_PATTERN.test(rawUserId)
  ) {
    ws.close(4001, 'Invalid userId');
    return;
  }

  const userId = rawUserId;
  const lastSeenSeqId =
    rawLastSeq !== null && rawLastSeq !== '' && !Number.isNaN(Number(rawLastSeq))
      ? Math.max(0, Number(rawLastSeq))
      : null;

  const connectionId = randomUUID();
  const clientEntry = {
    ws,
    userId,
    connectionId,
    lastSeqId: lastSeenSeqId ?? 0,
    rateTimestamps: [],
  };
  ws.isAlive = true;
  localClients.add(clientEntry);

  // Register socket event listeners synchronously before any async I/O so fast
  // clients sending right after `welcome` never race with listener attachment.
  ws.on('pong', async () => {
    ws.isAlive = true;
    try {
      // Recreate session via SET ... EX so a Redis restart never leaves active users invisible (B1/B2)
      await registerSession(userId, connectionId);
    } catch (err) {
      console.error(`[${GATEWAY_ID}] session refresh error:`, err.message);
    }
  });

  ws.on('message', async (data) => {
    if (Buffer.byteLength(data) > WS_MAX_PAYLOAD_BYTES) {
      ws.close(1009, 'Message too big');
      return;
    }

    let parsed;
    try {
      parsed = JSON.parse(data.toString());
    } catch {
      sendJson(ws, { type: 'error', code: 'INVALID_JSON', message: 'JSON inválido' });
      return;
    }

    if (parsed.type === 'sync_more') {
      const afterSeqId = Number(parsed.afterSeqId);
      if (Number.isNaN(afterSeqId) || afterSeqId < 0) return;
      try {
        const [currentSeqId, epoch, page] = await Promise.all([
          getAuthoritativeSeqId(),
          getOrLoadRoomEpoch(redis, pgPool).catch(() => null),
          readPostgresMessagesAfter(afterSeqId, SQL_RECOVERY_PAGE_SIZE),
        ]);
        if (page.messages.length > 0) {
          clientEntry.lastSeqId = Math.max(
            clientEntry.lastSeqId,
            page.messages[page.messages.length - 1].seqId
          );
        }
        sendJson(ws, {
          type: 'sync',
          recoverySource: 'postgres_offline',
          currentSeqId,
          epoch,
          resetRequired: false,
          hasMore: page.hasMore,
          messages: page.messages,
        });
      } catch (err) {
        console.error(`[${GATEWAY_ID}] sync_more error:`, err.message);
      }
      return;
    }

    if (parsed.type === 'chat_message') {
      if (!checkRateLimit(clientEntry)) {
        sendJson(ws, {
          type: 'error',
          code: 'RATE_LIMITED',
          message: 'Muitas mensagens em sequência. Aguarde um instante.',
        });
        return;
      }

      if (typeof parsed.content !== 'string') {
        sendJson(ws, {
          type: 'error',
          code: 'INVALID_PAYLOAD',
          message: 'Conteúdo da mensagem inválido.',
        });
        return;
      }

      const content = parsed.content.trim();
      if (!content || content.length > MAX_MESSAGE_LENGTH) {
        sendJson(ws, {
          type: 'error',
          code: 'INVALID_PAYLOAD',
          message: `Mensagem deve ter entre 1 e ${MAX_MESSAGE_LENGTH} caracteres.`,
        });
        return;
      }

      const rawClientMsgId =
        typeof parsed.clientMsgId === 'string' && parsed.clientMsgId.trim().length > 0
          ? parsed.clientMsgId.trim().slice(0, 64)
          : randomUUID();
      const traceId = parsed.traceId || randomUUID();

      void emitHop({
        traceId,
        flow: 'websocket',
        step: 1,
        from: 'client',
        to: 'web',
        protocol: 'WSS /ws',
        detail: `${userId} enviou mensagem`,
      });
      void emitHop({
        traceId,
        flow: 'websocket',
        step: 2,
        from: 'web',
        to: 'haproxy',
        protocol: 'TCP L4',
        detail: 'Proxy /ws -> HAProxy',
      });
      void emitHop({
        traceId,
        flow: 'websocket',
        step: 2,
        from: 'haproxy',
        to: GATEWAY_ID,
        protocol: 'TCP Passthrough',
        detail: `Tunel ativo em ${GATEWAY_ID}`,
      });
      void emitHop({
        traceId,
        flow: 'websocket',
        step: 3,
        from: GATEWAY_ID,
        to: 'redis',
        protocol: 'Redis Stream',
        detail: `XADD ${JOBS_STREAM} (${userId})`,
      });

      try {
        await enqueueJob({
          traceId,
          clientMsgId: rawClientMsgId,
          type: 'chat',
          userId,
          content,
          gatewayId: GATEWAY_ID,
          receivedAt: new Date().toISOString(),
        });
      } catch (err) {
        console.error(`[${GATEWAY_ID}] enqueue chat error:`, err.message);
        sendJson(ws, {
          type: 'error',
          code: 'ENQUEUE_FAILED',
          message: 'Falha temporária ao enfileirar mensagem.',
        });
      }
    }
  });

  ws.on('close', async () => {
    localClients.delete(clientEntry);
    await cleanupSessionIfOwner(userId, connectionId);
    try {
      await enqueueJob({
        type: 'user_left',
        userId,
        gatewayId: GATEWAY_ID,
        receivedAt: new Date().toISOString(),
      });
    } catch (err) {
      console.error(`[${GATEWAY_ID}] enqueue user_left error:`, err.message);
    }
  });

  ws.on('error', (err) => {
    console.error(`[${GATEWAY_ID}] socket error (${userId}):`, err.message);
  });

  try {
    await registerSession(userId, connectionId);
    const [currentSeqId, epoch] = await Promise.all([
      getAuthoritativeSeqId(),
      getOrLoadRoomEpoch(redis, pgPool).catch(() => null),
    ]);

    sendJson(ws, {
      type: 'welcome',
      userId,
      gatewayId: GATEWAY_ID,
      currentSeqId,
      epoch,
    });

    if (lastSeenSeqId !== null && lastSeenSeqId >= 0) {
      await recoverMissedMessages(clientEntry, lastSeenSeqId, currentSeqId, epoch, clientEpoch);
    } else {
      let recent = (await readRingBufferAsc()).slice(-20);
      if (recent.length === 0 && currentSeqId > 0) {
        const fromSeq = Math.max(0, currentSeqId - 20);
        const pgFallback = await readPostgresMessagesAfter(fromSeq, 20);
        recent = pgFallback.messages;
      }
      if (recent.length > 0) {
        clientEntry.lastSeqId = Math.max(
          clientEntry.lastSeqId,
          recent[recent.length - 1].seqId
        );
        sendJson(ws, {
          type: 'history',
          currentSeqId,
          epoch,
          messages: recent,
        });
      } else {
        clientEntry.lastSeqId = currentSeqId;
      }
    }

    await enqueueJob({
      type: 'user_joined',
      userId,
      gatewayId: GATEWAY_ID,
      receivedAt: new Date().toISOString(),
    });
  } catch (err) {
    console.error(`[${GATEWAY_ID}] connection setup error:`, err.message);
  }
});

let cmdConnectedOnce = false;
redis.on('ready', async () => {
  await publishGatewayAlive();
  if (!cmdConnectedOnce) {
    cmdConnectedOnce = true;
    return;
  }
  // Re-register all active local sessions immediately after a Redis restart (B1/B2)
  for (const client of localClients) {
    await registerSession(client.userId, client.connectionId).catch(() => {});
  }
});

let subConnectedOnce = false;
redisSub.on('ready', async () => {
  if (!subConnectedOnce) {
    subConnectedOnce = true;
    return;
  }
  // Resynchronize local clients with messages published while Redis Pub/Sub was down (Page 9)
  try {
    const [currentSeqId, epoch] = await Promise.all([
      getAuthoritativeSeqId(),
      getOrLoadRoomEpoch(redis, pgPool).catch(() => null),
    ]);
    for (const client of localClients) {
      if (client.ws.readyState === WebSocket.OPEN && client.lastSeqId < currentSeqId) {
        await recoverMissedMessages(client, client.lastSeqId, currentSeqId, epoch).catch(() => {});
      }
    }
  } catch (err) {
    console.error(`[${GATEWAY_ID}] Pub/Sub reconnect resync error:`, err.message);
  }
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

  void emitHop({
    traceId: payload.traceId,
    flow: 'websocket',
    step: 7,
    from: 'redis',
    to: GATEWAY_ID,
    protocol: 'Redis Pub/Sub',
    detail: `Entrega room:main (#seq ${payload.seqId})`,
  });

  void emitHop({
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
    if (typeof payload.seqId === 'number' && payload.seqId > client.lastSeqId) {
      client.lastSeqId = payload.seqId;
    }
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

const gatewayAliveTimer = setInterval(() => {
  void publishGatewayAlive();
}, GW_HEARTBEAT_INTERVAL_MS);

void publishGatewayAlive();

async function shutdown() {
  console.log(`[${GATEWAY_ID}] shutting down...`);
  clearInterval(heartbeatTimer);
  clearInterval(gatewayAliveTimer);
  for (const client of localClients) {
    client.ws.close(1001, 'Gateway shutting down');
  }
  wss.close();
  server.close();
  await redis.del(gatewayAliveKey(GATEWAY_ID)).catch(() => {});
  await Promise.allSettled([redisSub.quit(), redis.quit(), pgPool.end()]);
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

server.listen(PORT, () => {
  console.log(`[${GATEWAY_ID}] listening on :${PORT} (bootId=${GATEWAY_BOOT_ID})`);
});
