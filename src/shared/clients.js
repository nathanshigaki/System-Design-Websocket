import { randomUUID } from 'node:crypto';
import { ROOM_EPOCH_KEY } from './constants.js';

/**
 * Creates a resilient ioredis client that never crashes the process on transient
 * network/restart errors and uses bounded exponential backoff on reconnect.
 */
export function createRedisClient(RedisCtor, url, label, extraOptions = {}) {
  const client = new RedisCtor(url, {
    maxRetriesPerRequest: null,
    enableReadyCheck: true,
    retryStrategy(times) {
      return Math.min(200 * Math.pow(1.5, Math.min(times, 10)), 3000);
    },
    ...extraOptions,
  });

  client.on('error', (err) => {
    console.error(`[${label}] redis error:`, err.message);
  });

  client.on('reconnecting', (delay) => {
    console.warn(`[${label}] redis reconnecting in ${delay}ms...`);
  });

  return client;
}

/**
 * Creates a resilient pg.Pool with an error listener so idle connections terminated
 * by a PostgreSQL restart do not trigger an unhandled error crash (B5).
 */
export function createPgPool(pgModule, connectionString, label) {
  const pool = new pgModule.Pool({
    connectionString,
    connectionTimeoutMillis: 5000,
    idleTimeoutMillis: 30000,
  });

  pool.on('error', (err) => {
    console.error(`[${label}] postgres pool error (idle client):`, err.message);
  });

  return pool;
}

/**
 * Non-blocking replacement for `redis.keys(pattern)` using cursor-based SCAN.
 */
export async function scanKeys(redis, pattern, count = 100) {
  let cursor = '0';
  const keys = [];
  do {
    const [nextCursor, batch] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', count);
    cursor = nextCursor;
    if (batch.length > 0) {
      keys.push(...batch);
    }
  } while (cursor !== '0');
  return keys;
}

/**
 * Idempotently guarantees the schema extensions (client_msg_id & room_meta)
 * even when running against an existing pgdata volume.
 */
export async function ensureDbSchema(pgPool) {
  await pgPool.query(`
    ALTER TABLE messages
    ADD COLUMN IF NOT EXISTS client_msg_id VARCHAR(64);
  `);

  await pgPool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_client_msg_id
    ON messages (client_msg_id)
    WHERE client_msg_id IS NOT NULL;
  `);

  await pgPool.query(`
    CREATE TABLE IF NOT EXISTS room_meta (
      id SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
      epoch VARCHAR(64) NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pgPool.query(
    `INSERT INTO room_meta (id, epoch) VALUES (1, $1) ON CONFLICT (id) DO NOTHING`,
    [randomUUID()]
  );
}

/**
 * Reads the authoritative room epoch from Redis or falls back to PostgreSQL `room_meta`.
 */
export async function getOrLoadRoomEpoch(redis, pgPool) {
  try {
    const cached = await redis.get(ROOM_EPOCH_KEY);
    if (cached) return cached;
  } catch {
    // Fallback to Postgres below
  }

  const { rows } = await pgPool.query('SELECT epoch FROM room_meta WHERE id = 1');
  let epoch = rows[0]?.epoch;
  if (!epoch) {
    epoch = randomUUID();
    await pgPool.query(
      'INSERT INTO room_meta (id, epoch) VALUES (1, $1) ON CONFLICT (id) DO NOTHING',
      [epoch]
    );
    const recheck = await pgPool.query('SELECT epoch FROM room_meta WHERE id = 1');
    epoch = recheck.rows[0]?.epoch || epoch;
  }

  try {
    await redis.set(ROOM_EPOCH_KEY, epoch);
  } catch {
    // Non-fatal if Redis is temporarily restarting
  }

  return epoch;
}

/**
 * Installs process-level error guards so transient unhandled rejections or
 * socket errors from backend restarts are logged instead of killing the container.
 */
export function installProcessErrorHandlers(serviceName) {
  process.on('unhandledRejection', (reason) => {
    const msg = reason instanceof Error ? reason.stack || reason.message : String(reason);
    console.error(`[${serviceName}] unhandledRejection:`, msg);
  });

  process.on('uncaughtException', (err) => {
    console.error(`[${serviceName}] uncaughtException:`, err.stack || err.message);
  });
}
