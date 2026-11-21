export const ROOM_ID = 'main';
export const ROOM_CHANNEL = 'room:main';
export const TRACE_CHANNEL = 'trace:events';

// Legacy key retained for backwards visibility; active queue uses Redis Streams
export const JOBS_QUEUE = 'jobs:messages';
export const JOBS_STREAM = 'stream:jobs';
export const JOBS_GROUP = 'workers-group';
export const STREAM_MAXLEN = 2000;
export const STREAM_CLAIM_IDLE_MS = 4000;

export const SEQ_KEY = 'seq:room:main';
export const RING_KEY = 'ring:room:main';
export const ROOM_EPOCH_KEY = 'room:main:epoch';
export const SESSION_PREFIX = 'session:';
export const GW_ALIVE_PREFIX = 'gw:alive:';

export const RING_BUFFER_SIZE = 50;
export const SQL_RECOVERY_PAGE_SIZE = 200;
export const SESSION_TTL_SEC = 45;
export const HEARTBEAT_INTERVAL_MS = 15000;
export const GW_ALIVE_TTL_SEC = 6;
export const GW_HEARTBEAT_INTERVAL_MS = 2000;

export const MAX_MESSAGE_LENGTH = 500;
export const MAX_USER_ID_LENGTH = 32;
export const WS_MAX_PAYLOAD_BYTES = 4096;
export const RATE_LIMIT_MAX_MSGS = Number(process.env.RATE_LIMIT_MAX_MSGS || 400);
export const RATE_LIMIT_WINDOW_MS = Number(process.env.RATE_LIMIT_WINDOW_MS || 10000);

export function sessionKey(userId) {
  return `${SESSION_PREFIX}${userId}`;
}

export function gatewayAliveKey(gatewayId) {
  return `${GW_ALIVE_PREFIX}${gatewayId}`;
}
