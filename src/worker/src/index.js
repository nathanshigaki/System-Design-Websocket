import { randomUUID } from 'node:crypto';
import Redis from 'ioredis';
import pg from 'pg';
import {
  JOBS_QUEUE,
  MAX_MESSAGE_LENGTH,
  RING_BUFFER_SIZE,
  RING_KEY,
  ROOM_CHANNEL,
  SEQ_KEY,
} from '../../shared/constants.js';
import { createTraceEmitter } from '../../shared/trace.js';

const WORKER_ID = process.env.WORKER_ID || 'worker-1';
const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
const DATABASE_URL =
  process.env.DATABASE_URL || 'postgres://pulsews:pulsews@127.0.0.1:5432/pulsews';

const redisQueue = new Redis(REDIS_URL);
const redis = new Redis(REDIS_URL);
const pgPool = new pg.Pool({ connectionString: DATABASE_URL });
const emitHop = createTraceEmitter(redis);

let running = true;

async function handleChatJob(job) {
  const traceId = job.traceId || randomUUID();
  const content = String(job.content || '').trim().slice(0, MAX_MESSAGE_LENGTH);
  if (!content) return;

  await emitHop({
    traceId,
    flow: 'websocket',
    step: 4,
    from: 'redis',
    to: 'worker',
    protocol: 'BLPOP',
    detail: `Worker consumiu mensagem de ${job.userId}`,
  });

  const seqId = await redis.incr(SEQ_KEY);
  const createdAt = new Date().toISOString();
  const event = {
    seqId,
    userId: job.userId,
    content,
    gatewayId: job.gatewayId,
    createdAt,
    traceId,
  };

  await redis
    .multi()
    .lpush(RING_KEY, JSON.stringify(event))
    .ltrim(RING_KEY, 0, RING_BUFFER_SIZE - 1)
    .exec();

  await emitHop({
    traceId,
    flow: 'websocket',
    step: 5,
    from: 'worker',
    to: 'redis',
    protocol: 'Ring Buffer',
    detail: `INCR seq=${seqId} + Ring Buffer (${RING_BUFFER_SIZE} max)`,
  });

  await pgPool.query(
    `INSERT INTO messages (seq_id, user_id, content, gateway_id, created_at)
     VALUES ($1, $2, $3, $4, $5)`,
    [seqId, job.userId, content, job.gatewayId, createdAt]
  );

  await emitHop({
    traceId,
    flow: 'websocket',
    step: 5,
    from: 'worker',
    to: 'postgres',
    protocol: 'SQL INSERT',
    detail: `Single-Writer gravou msg #${seqId}`,
  });

  await redis.publish(ROOM_CHANNEL, JSON.stringify(event));

  await emitHop({
    traceId,
    flow: 'websocket',
    step: 6,
    from: 'worker',
    to: 'redis',
    protocol: 'Redis Pub/Sub',
    detail: `PUBLISH room:main (#seq ${seqId})`,
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

async function processJob(rawJob) {
  let job;
  try {
    job = JSON.parse(rawJob);
  } catch {
    return;
  }

  if (job.type === 'chat') {
    await handleChatJob(job);
  } else if (job.type === 'user_joined' || job.type === 'user_left') {
    await handlePresenceJob(job);
  }
}

async function runLoop() {
  console.log(`[${WORKER_ID}] consuming queue "${JOBS_QUEUE}"...`);
  while (running) {
    try {
      const result = await redisQueue.blpop(JOBS_QUEUE, 2);
      if (!result) continue;
      const [, rawJob] = result;
      await processJob(rawJob);
    } catch (err) {
      if (!running) break;
      console.error(`[${WORKER_ID}] loop error:`, err.message);
      await new Promise((r) => setTimeout(r, 1000));
    }
  }
}

async function shutdown() {
  console.log(`[${WORKER_ID}] shutting down gracefully...`);
  running = false;
  await Promise.allSettled([redisQueue.quit(), redis.quit(), pgPool.end()]);
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

runLoop();
