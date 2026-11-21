import { TRACE_CHANNEL } from './constants.js';

/**
 * Creates a safe trace emitter that publishes hop telemetry events to Redis Pub/Sub
 * so the frontend animated SVG can render real message routes in real time.
 */
export function createTraceEmitter(redisPub) {
  return async function emitHop({
    traceId,
    flow,
    step,
    from,
    to,
    protocol,
    detail = '',
  }) {
    if (redisPub.status && redisPub.status !== 'ready') {
      return;
    }
    try {
      const payload = JSON.stringify({
        type: 'trace_hop',
        traceId,
        flow,
        step,
        from,
        to,
        protocol,
        detail,
        ts: Date.now(),
      });
      await redisPub.publish(TRACE_CHANNEL, payload);
    } catch (err) {
      console.error('[trace] failed to publish hop:', err.message);
    }
  };
}
