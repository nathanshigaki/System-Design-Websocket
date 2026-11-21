import { randomUUID } from 'node:crypto';
import Redis from 'ioredis';
import pg from 'pg';
import {
  JOBS_GROUP,
  JOBS_STREAM,
  MAX_MESSAGE_LENGTH,
  RING_BUFFER_SIZE,
  RING_KEY,
  ROOM_CHANNEL,
  SEQ_KEY,
  STREAM_CLAIM_IDLE_MS,
} from '../../shared/constants.js';
import {
  createPgPool,
  createRedisClient,
  ensureDbSchema,
  getOrLoadRoomEpoch,
  installProcessErrorHandlers,
} from '../../shared/clients.js';
import { createTraceEmitter } from '../../shared/trace.js';

const WORKER_ID = process.env.WORKER_ID || 'worker-1';
const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
const DATABASE_URL =
  process.env.DATABASE_URL || 'postgres://pulsews:pulsews@127.0.0.1:5432/pulsews';

installProcessErrorHandlers(WORKER_ID);

const redisQueue = createRedisClient(Redis, REDIS_URL, `${WORKER_ID}:stream`);
const redis = createRedisClient(Redis, REDIS_URL, `${WORKER_ID}:cmd`);
const pgPool = createPgPool(pg, DATABASE_URL, WORKER_ID);
const emitHop = createTraceEmitter(redis);

let running = true;
let shuttingDown = false;
let inFlightJobPromise = null;
let lastAutoClaimAt = 0;

const SYNC_SEQ_LUA = `
local cur = tonumber(redis.call('GET', KEYS[1]) or '0')
local target = tonumber(ARGV[1])
if cur < target then
  redis.call('SET', KEYS[1], tostring(target))
  return target
end
return cur
`;

async function syncSeqAndEpochFromDb() {
  const { rows } = await pgPool.query(
    'SELECT COALESCE(MAX(seq_id), 0) AS max_seq FROM messages'
  );
  const maxSeq = Number(rows[0]?.max_seq || 0);
  await redis.eval(SYNC_SEQ_LUA, 1, SEQ_KEY, String(maxSeq));
  await getOrLoadRoomEpoch(redis, pgPool);
  return maxSeq;
}

async function ensureStreamGroup() {
  try {
    await redis.xgroup('CREATE', JOBS_STREAM, JOBS_GROUP, '0', 'MKSTREAM');
  } catch (err) {
    if (!String(err.message || '').includes('BUSYGROUP')) {
      throw err;
    }
  }
}

async function bootstrapState() {
  await ensureDbSchema(pgPool);
  await ensureStreamGroup();
  const maxSeq = await syncSeqAndEpochFromDb();
  console.log(
    `[${WORKER_ID}] bootstrapped (stream="${JOBS_STREAM}", group="${JOBS_GROUP}", maxSeq=${maxSeq})`
  );
}

async function pushToRingBufferIfMissing(event) {
  const recentRaw = await redis.lrange(RING_KEY, 0, 15);
  for (const item of recentRaw) {
    try {
      const parsed = JSON.parse(item);
      if (parsed.seqId === event.seqId) {
        return;
      }
    } catch {
      // Ignore malformed entry
    }
  }

  await redis
    .multi()
    .lpush(RING_KEY, JSON.stringify(event))
    .ltrim(RING_KEY, 0, RING_BUFFER_SIZE - 1)
    .exec();
}

async function findExistingByClientMsgId(clientMsgId) {
  if (!clientMsgId) return null;
  const { rows } = await pgPool.query(
    `SELECT seq_id, user_id, content, gateway_id, created_at
     FROM messages
     WHERE client_msg_id = $1`,
    [clientMsgId]
  );
  if (rows.length === 0) return null;
  const row = rows[0];
  return {
    seqId: Number(row.seq_id),
    userId: row.user_id,
    content: row.content,
    gatewayId: row.gateway_id,
    createdAt: row.created_at,
  };
}

async function handleChatJob(job) {
  const traceId = job.traceId || randomUUID();
  const content = String(job.content || '').trim().slice(0, MAX_MESSAGE_LENGTH);
  if (!content) return;

  const clientMsgId =
    typeof job.clientMsgId === 'string' && job.clientMsgId.trim().length > 0
      ? job.clientMsgId.trim().slice(0, 64)
      : null;

  void emitHop({
    traceId,
    flow: 'websocket',
    step: 4,
    from: 'redis',
    to: 'worker',
    protocol: 'XREADGROUP',
    detail: `Worker consumiu mensagem de ${job.userId}`,
  });

  // Step 1: Persist to PostgreSQL FIRST (idempotently via client_msg_id) — fixes B1 & B4
  let event = null;
  const alreadySaved = await findExistingByClientMsgId(clientMsgId);
  if (alreadySaved) {
    event = { ...alreadySaved, traceId };
  } else {
    for (let attempt = 0; attempt < 4; attempt++) {
      const seqExists = await redis.exists(SEQ_KEY);
      if (!seqExists) {
        await syncSeqAndEpochFromDb();
      }

      const seqId = await redis.incr(SEQ_KEY);
      const createdAt = new Date().toISOString();

      try {
        const insertRes = await pgPool.query(
          `INSERT INTO messages (seq_id, user_id, content, gateway_id, client_msg_id, created_at)
           VALUES ($1, $2, $3, $4, $5, $6)
           ON CONFLICT (client_msg_id) WHERE client_msg_id IS NOT NULL DO NOTHING
           RETURNING seq_id, user_id, content, gateway_id, created_at`,
          [seqId, job.userId, content, job.gatewayId, clientMsgId, createdAt]
        );

        if (insertRes.rows.length > 0) {
          const row = insertRes.rows[0];
          event = {
            seqId: Number(row.seq_id),
            userId: row.user_id,
            content: row.content,
            gatewayId: row.gateway_id,
            createdAt: row.created_at,
            traceId,
          };
          break;
        }

        // Duplicate client_msg_id race: load existing row
        const existing = await findExistingByClientMsgId(clientMsgId);
        if (existing) {
          event = { ...existing, traceId };
          break;
        }
      } catch (err) {
        // B1 safeguard: if Redis lost seq counter mid-flight, resync from Postgres and retry
        if (err.code === '23505' && String(err.constraint || '').includes('seq_id')) {
          console.warn(
            `[${WORKER_ID}] seq_id=${seqId} conflict with PostgreSQL; resyncing counter...`
          );
          await syncSeqAndEpochFromDb();
          continue;
        }
        throw err;
      }
    }
  }

  if (!event) {
    throw new Error(`Failed to persist message for user ${job.userId} after retries`);
  }

  void emitHop({
    traceId,
    flow: 'websocket',
    step: 5,
    from: 'worker',
    to: 'postgres',
    protocol: 'SQL INSERT',
    detail: `Single-Writer gravou msg #${event.seqId}`,
  });

  // Step 2: Write to Ring Buffer ONLY AFTER PostgreSQL confirmed the INSERT (fixes B1)
  await pushToRingBufferIfMissing(event);

  void emitHop({
    traceId,
    flow: 'websocket',
    step: 5,
    from: 'worker',
    to: 'redis',
    protocol: 'Ring Buffer',
    detail: `INCR seq=${event.seqId} + Ring Buffer (${RING_BUFFER_SIZE} max)`,
  });

  // Step 3: Publish to Pub/Sub so Gateways fan out to connected WebSocket clients
  await redis.publish(ROOM_CHANNEL, JSON.stringify(event));

  void emitHop({
    traceId,
    flow: 'websocket',
    step: 6,
    from: 'worker',
    to: 'redis',
    protocol: 'Redis Pub/Sub',
    detail: `PUBLISH room:main (#seq ${event.seqId})`,
  });
}

async function handlePresenceJob(job) {
  const message =
    job.type === 'user_joined'
      ? `${job.userId} entrou na sala via ${job.gatewayId}`
      : `${job.userId} saiu da sala (${job.gatewayId})`;

  await pgPool.query(
    `INSERT INTO notifications (type, user_id, message, created_at)
     VALUES ($1, $2, $3, $4)`,
    [job.type, job.userId, message, job.receivedAt || new Date().toISOString()]
  );
}

function extractPayloadFromFields(fields) {
  for (let i = 0; i < fields.length; i += 2) {
    if (fields[i] === 'payload') {
      return fields[i + 1];
    }
  }
  return null;
}

async function processStreamEntry(streamId, fields) {
  const rawJob = extractPayloadFromFields(fields);
  if (!rawJob) {
    await redis.xack(JOBS_STREAM, JOBS_GROUP, streamId);
    return;
  }

  let job;
  try {
    job = JSON.parse(rawJob);
  } catch {
    await redis.xack(JOBS_STREAM, JOBS_GROUP, streamId);
    return;
  }

  if (job.type === 'chat') {
    await handleChatJob(job);
  } else if (job.type === 'user_joined' || job.type === 'user_left') {
    await handlePresenceJob(job);
  }

  // Step 4: XACK ONLY AFTER Postgres + Ring Buffer + Publish succeeded (fixes B4)
  await redis.xack(JOBS_STREAM, JOBS_GROUP, streamId);
}

async function runEntryTracked(streamId, fields) {
  const p = processStreamEntry(streamId, fields);
  inFlightJobPromise = p;
  try {
    await p;
  } finally {
    if (inFlightJobPromise === p) {
      inFlightJobPromise = null;
    }
  }
}

async function drainOwnPending() {
  while (running) {
    const res = await redisQueue.xreadgroup(
      'GROUP',
      JOBS_GROUP,
      WORKER_ID,
      'COUNT',
      20,
      'STREAMS',
      JOBS_STREAM,
      '0'
    );
    if (!res || res.length === 0) return;
    const [, entries] = res[0];
    if (!entries || entries.length === 0) return;

    console.log(`[${WORKER_ID}] recovering ${entries.length} pending job(s) from PEL...`);
    for (const [streamId, fields] of entries) {
      if (!running) return;
      await runEntryTracked(streamId, fields);
    }
  }
}

async function reclaimStalePending() {
  const now = Date.now();
  if (now - lastAutoClaimAt < 2000) return;
  lastAutoClaimAt = now;

  const claimed = await redis.xautoclaim(
    JOBS_STREAM,
    JOBS_GROUP,
    WORKER_ID,
    STREAM_CLAIM_IDLE_MS,
    '0-0',
    'COUNT',
    20
  );
  const entries = claimed?.[1] || [];
  if (entries.length === 0) return;

  console.log(`[${WORKER_ID}] XAUTOCLAIM recovered ${entries.length} orphaned job(s)`);
  for (const [streamId, fields] of entries) {
    if (!running) return;
    if (!fields) {
      await redis.xack(JOBS_STREAM, JOBS_GROUP, streamId).catch(() => {});
      continue;
    }
    await runEntryTracked(streamId, fields);
  }
}

let cmdConnectedOnce = false;
redis.on('ready', async () => {
  if (!cmdConnectedOnce) {
    cmdConnectedOnce = true;
    return;
  }
  try {
    await ensureStreamGroup();
    await syncSeqAndEpochFromDb();
  } catch (err) {
    console.error(`[${WORKER_ID}] post-reconnect sync error:`, err.message);
  }
});

async function runLoop() {
  // Wait until initial bootstrap succeeds (handles transient DB/Redis boot races)
  while (running) {
    try {
      await bootstrapState();
      break;
    } catch (err) {
      console.error(`[${WORKER_ID}] bootstrap error (retrying in 1s):`, err.message);
      await new Promise((r) => setTimeout(r, 1000));
    }
  }

  // Immediately recover any jobs left un-ACKed before a crash (B4)
  try {
    await drainOwnPending();
  } catch (err) {
    console.error(`[${WORKER_ID}] initial PEL drain error:`, err.message);
  }

  console.log(`[${WORKER_ID}] consuming stream "${JOBS_STREAM}" (group "${JOBS_GROUP}")...`);
  while (running) {
    try {
      await reclaimStalePending();
      if (!running) break;

      const result = await redisQueue.xreadgroup(
        'GROUP',
        JOBS_GROUP,
        WORKER_ID,
        'COUNT',
        10,
        'BLOCK',
        1000,
        'STREAMS',
        JOBS_STREAM,
        '>'
      );
      if (!result || result.length === 0) continue;

      const [, entries] = result[0];
      for (const [streamId, fields] of entries) {
        if (!running) break;
        await runEntryTracked(streamId, fields);
      }
    } catch (err) {
      if (!running) break;
      const msg = String(err.message || '');
      console.error(`[${WORKER_ID}] loop error:`, msg);
      if (msg.includes('NOGROUP')) {
        try {
          await ensureStreamGroup();
          await syncSeqAndEpochFromDb();
        } catch (setupErr) {
          console.error(`[${WORKER_ID}] group recovery failed:`, setupErr.message);
        }
      }
      await new Promise((r) => setTimeout(r, 500));
    }
  }
}

async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`[${WORKER_ID}] received ${signal}, shutting down gracefully...`);
  running = false;

  // B3: wait for the in-flight job (INSERT + Ring Buffer + PUBLISH + XACK) to finish
  if (inFlightJobPromise) {
    console.log(`[${WORKER_ID}] awaiting in-flight job before closing connections...`);
    try {
      await inFlightJobPromise;
    } catch (err) {
      console.error(`[${WORKER_ID}] in-flight job ended with error:`, err.message);
    }
  }

  try {
    redisQueue.disconnect();
  } catch {
    // Ignore disconnect errors on shutdown
  }
  await Promise.allSettled([redis.quit(), pgPool.end()]);
  process.exit(0);
}

process.on('SIGINT', () => void shutdown('SIGINT'));
process.on('SIGTERM', () => void shutdown('SIGTERM'));

runLoop();
