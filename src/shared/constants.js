export const ROOM_ID = 'main';
export const ROOM_CHANNEL = 'room:main';
export const TRACE_CHANNEL = 'trace:events';
export const JOBS_QUEUE = 'jobs:messages';

export const SEQ_KEY = 'seq:room:main';
export const RING_KEY = 'ring:room:main';
export const SESSION_PREFIX = 'session:';

export const RING_BUFFER_SIZE = 50;
export const SESSION_TTL_SEC = 45;
export const HEARTBEAT_INTERVAL_MS = 15000;
export const MAX_MESSAGE_LENGTH = 500;
export const MAX_USER_ID_LENGTH = 32;

export function sessionKey(userId) {
  return `${SESSION_PREFIX}${userId}`;
}
