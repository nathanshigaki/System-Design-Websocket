import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import WebSocket from 'ws';

const BASE_HTTP = 'http://127.0.0.1:3000';
const BASE_WS = 'ws://127.0.0.1:3000/ws';

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function dockerCompose(args) {
  return execFileSync('docker', ['compose', ...args], {
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
  }).trim();
}

function psqlScalar(sql) {
  return dockerCompose([
    'exec',
    '-T',
    'postgres',
    'psql',
    '-U',
    'pulsews',
    '-d',
    'pulsews',
    '-t',
    '-A',
    '-c',
    sql,
  ]).trim();
}

function redisCli(args) {
  return dockerCompose(['exec', '-T', 'redis', 'redis-cli', ...args]).trim();
}

async function waitForCondition(fn, timeoutMs, label) {
  const start = Date.now();
  let lastErr = null;
  while (Date.now() - start < timeoutMs) {
    try {
      const val = await fn();
      if (val) return val;
    } catch (err) {
      lastErr = err;
    }
    await sleep(150);
  }
  throw new Error(
    `Timeout (${timeoutMs}ms) waiting for ${label}${lastErr ? `: ${lastErr.message}` : ''}`
  );
}

async function waitForClusterHealthy(timeoutMs = 20000) {
  await waitForCondition(async () => {
    const res = await fetch(`${BASE_HTTP}/health`);
    return res.status === 200;
  }, timeoutMs, 'web /health 200');
}

async function getCluster() {
  const res = await fetch(`${BASE_HTTP}/api/cluster`);
  assert.equal(res.status, 200, '/api/cluster should return 200');
  return res.json();
}

function connectClient(userId, opts = {}) {
  const { lastSeenSeqId, epoch, waitSync = false } = opts;
  return new Promise((resolve, reject) => {
    const params = new URLSearchParams({ userId });
    if (lastSeenSeqId !== undefined) {
      params.set('lastSeenSeqId', String(lastSeenSeqId));
    }
    if (epoch) {
      params.set('epoch', epoch);
    }

    const ws = new WebSocket(`${BASE_WS}?${params.toString()}`);
    const state = {
      ws,
      userId,
      gatewayId: null,
      welcome: null,
      history: [],
      syncs: [],
      messages: [],
      errors: [],
      closed: false,
      closeCode: null,
    };

    let resolved = false;
    const timeout = setTimeout(() => {
      if (!resolved) {
        resolved = true;
        ws.terminate();
        reject(new Error(`Connect timeout for ${userId}`));
      }
    }, 8000);

    const maybeResolve = () => {
      if (resolved) return;
      if (state.welcome && (!waitSync || state.syncs.length > 0)) {
        resolved = true;
        clearTimeout(timeout);
        resolve(state);
      }
    };

    ws.on('message', (raw) => {
      const msg = JSON.parse(raw.toString());
      if (msg.type === 'welcome') {
        state.welcome = msg;
        state.gatewayId = msg.gatewayId;
        maybeResolve();
      } else if (msg.type === 'history') {
        state.history.push(...(msg.messages || []));
      } else if (msg.type === 'sync') {
        state.syncs.push(msg);
        maybeResolve();
      } else if (msg.type === 'room_message') {
        state.messages.push(msg);
      } else if (msg.type === 'error') {
        state.errors.push(msg);
      }
    });

    ws.on('close', (code) => {
      state.closed = true;
      state.closeCode = code;
      if (!resolved) {
        resolved = true;
        clearTimeout(timeout);
        reject(new Error(`Socket closed before welcome (${userId}, code=${code})`));
      }
    });

    ws.on('error', (err) => {
      if (!resolved) {
        resolved = true;
        clearTimeout(timeout);
        reject(err);
      }
    });
  });
}

function sendChat(client, content, clientMsgId = randomUUID()) {
  client.ws.send(
    JSON.stringify({
      type: 'chat_message',
      content,
      clientMsgId,
    })
  );
  return clientMsgId;
}

async function closeClients(clients) {
  for (const c of clients) {
    try {
      if (c && c.ws && c.ws.readyState === WebSocket.OPEN) {
        c.ws.close(1000, 'test done');
      }
    } catch {
      // ignore
    }
  }
  await sleep(150);
}

// ---------------------------------------------------------------------------
// B1 & B2: Redis restart preserves seqId monotonicity, DB/Ring consistency,
// active session recovery and replay for old/divergent lastSeenSeqId.
// ---------------------------------------------------------------------------
async function testB1AndB2RedisRestart() {
  console.log('\n### [B1 & B2] Reiniciar Redis + reconexão com lastSeenSeqId antigo/divergente');
  const ana = await connectClient('b1-ana');
  const tag = `b1-${Date.now()}`;

  try {
    sendChat(ana, `${tag}-1`);
    sendChat(ana, `${tag}-2`);

    await waitForCondition(
      () => ana.messages.filter((m) => m.content.startsWith(tag)).length >= 2,
      8000,
      'Ana receives first 2 messages'
    );

    const beforeMsgs = ana.messages.filter((m) => m.content.startsWith(tag));
    const seq1 = beforeMsgs[0].seqId;
    const seq2 = beforeMsgs[1].seqId;
    assert.ok(seq2 > seq1, `Expected seq2 (${seq2}) > seq1 (${seq1})`);

    // Restart Redis
    dockerCompose(['restart', 'redis']);
    await waitForCondition(() => redisCli(['ping']) === 'PONG', 15000, 'Redis PONG');
    await waitForClusterHealthy(15000);
    await sleep(600);

    // Send 2 more messages after Redis restart
    sendChat(ana, `${tag}-3`);
    sendChat(ana, `${tag}-4`);

    await waitForCondition(
      () => ana.messages.filter((m) => m.content.startsWith(tag)).length >= 4,
      10000,
      'Ana receives messages 3 & 4 after Redis restart'
    );

    const afterMsgs = ana.messages.filter((m) => m.content.startsWith(tag));
    const seq3 = afterMsgs[2].seqId;
    const seq4 = afterMsgs[3].seqId;
    assert.ok(seq3 > seq2 && seq4 > seq3, `seqId must continue monotonically (${seq2} -> ${seq3} -> ${seq4})`);

    // Verify new user Bruno sees identical seqIds
    const bruno = await connectClient('b1-bruno');
    try {
      await waitForCondition(
        () => bruno.history.some((m) => m.content === `${tag}-4`),
        5000,
        'Bruno receives consistent history'
      );
      const b3 = bruno.history.find((m) => m.content === `${tag}-3`);
      const b4 = bruno.history.find((m) => m.content === `${tag}-4`);
      assert.equal(b3?.seqId, seq3, 'Bruno must see same seqId for msg 3');
      assert.equal(b4?.seqId, seq4, 'Bruno must see same seqId for msg 4');

      // Verify Postgres has all 4 messages
      const dbCount = Number(
        psqlScalar(`SELECT COUNT(*) FROM messages WHERE content LIKE '${tag}-%';`)
      );
      assert.equal(dbCount, 4, `Postgres must persist all 4 messages (got ${dbCount})`);

      // Verify Ana's session was recreated automatically after Redis restart
      const cluster = await getCluster();
      assert.ok(
        cluster.sessions.some((s) => s.userId === 'b1-ana'),
        'Ana session must still appear in /api/cluster after Redis restart'
      );

      // B2: Client reconnecting with old lastSeenSeqId receives all missed messages
      const oldClient = await connectClient('b2-old', {
        lastSeenSeqId: seq1,
        waitSync: true,
      });
      try {
        const sync = oldClient.syncs[0];
        assert.notEqual(sync.recoverySource, 'up_to_date', 'Should not return up_to_date');
        const recoveredContents = sync.messages.map((m) => m.content);
        assert.ok(
          recoveredContents.includes(`${tag}-2`) &&
            recoveredContents.includes(`${tag}-3`) &&
            recoveredContents.includes(`${tag}-4`),
          `Expected msgs 2, 3, 4 in sync, got: ${recoveredContents.join(', ')}`
        );
      } finally {
        await closeClients([oldClient]);
      }

      // B2 edge case: Client reconnecting with divergent future lastSeenSeqId gets resetRequired=true
      // Also exercises Page 9 SQL fallback pagination (sync_more) when DB has >200 messages
      const futureClient = await connectClient('b2-future', {
        lastSeenSeqId: seq4 + 9999,
        waitSync: true,
      });
      try {
        const firstSync = futureClient.syncs[0];
        assert.equal(
          firstSync.resetRequired,
          true,
          'Divergent lastSeenSeqId must trigger resetRequired=true'
        );

        const allRecovered = [...firstSync.messages];
        let latestSync = firstSync;
        while (latestSync.hasMore) {
          const prevCount = futureClient.syncs.length;
          const lastSeq = allRecovered[allRecovered.length - 1]?.seqId || 0;
          futureClient.ws.send(JSON.stringify({ type: 'sync_more', afterSeqId: lastSeq }));
          await waitForCondition(
            () => futureClient.syncs.length > prevCount,
            5000,
            'next sync_more page'
          );
          latestSync = futureClient.syncs[futureClient.syncs.length - 1];
          allRecovered.push(...latestSync.messages);
        }

        assert.ok(
          allRecovered.some((m) => m.content === `${tag}-4`),
          'Reset sync (+ pagination) must deliver current valid room messages'
        );
      } finally {
        await closeClients([futureClient]);
      }
    } finally {
      await closeClients([bruno]);
    }

    console.log(`  PASS B1 & B2: seqId contínuo (${seq1}->${seq4}), 4/4 no DB, sessão preservada e sync consistente.`);
  } finally {
    await closeClients([ana]);
  }
}

// ---------------------------------------------------------------------------
// B3: Graceful SIGTERM restart on worker-1 loses 0 messages
// ---------------------------------------------------------------------------
async function testB3WorkerSigterm() {
  console.log('\n### [B3] restart worker-1 (SIGTERM) durante envio');
  const sender = await connectClient('b3-sender');
  const watcher = await connectClient('b3-watcher');
  const tag = `fila-${Date.now()}`;

  try {
    sendChat(sender, `${tag}-0`);
    sendChat(sender, `${tag}-1`);

    // Restart worker while jobs are being processed/enqueued
    const restartPromise = new Promise((resolve, reject) => {
      setTimeout(() => {
        try {
          dockerCompose(['restart', 'worker-1']);
          resolve();
        } catch (err) {
          reject(err);
        }
      }, 20);
    });

    await sleep(40);
    sendChat(sender, `${tag}-2`);
    sendChat(sender, `${tag}-3`);
    sendChat(sender, `${tag}-4`);

    await restartPromise;

    await waitForCondition(() => {
      const count = Number(
        psqlScalar(`SELECT COUNT(*) FROM messages WHERE content LIKE '${tag}-%';`)
      );
      return count === 5;
    }, 15000, 'all 5 B3 messages persisted in Postgres');

    await waitForCondition(
      () => watcher.messages.filter((m) => m.content.startsWith(tag)).length >= 5,
      10000,
      'watcher receives all 5 B3 messages'
    );

    console.log('  PASS B3: 5/5 mensagens entregues e persistidas durante SIGTERM do worker-1.');
  } finally {
    await closeClients([sender, watcher]);
  }
}

// ---------------------------------------------------------------------------
// B4: SIGKILL (kill -9) on worker-1 during a 300-message burst loses 0 of 300
// ---------------------------------------------------------------------------
async function testB4WorkerKill9Burst() {
  console.log('\n### [B4] kill -9 no worker-1 durante rajada de 300 mensagens');
  const sender = await connectClient('wk-sender');
  const watcher = await connectClient('wk-watcher');
  const tag = `burst-${Date.now()}`;
  const total = 300;

  try {
    for (let i = 0; i < total; i++) {
      sendChat(sender, `${tag}-${i}`, `${tag}-id-${i}`);
      if (i === 90) {
        dockerCompose(['kill', '-s', 'SIGKILL', 'worker-1']);
      }
      if (i % 40 === 0) {
        await sleep(5);
      }
    }

    // Ensure worker-1 is back up (restart policy or explicit up)
    dockerCompose(['up', '-d', 'worker-1']);

    await waitForCondition(() => {
      const count = Number(
        psqlScalar(`SELECT COUNT(*) FROM messages WHERE content LIKE '${tag}-%';`)
      );
      return count === total;
    }, 30000, `all ${total} B4 burst messages persisted in Postgres`);

    const distinctCount = Number(
      psqlScalar(`SELECT COUNT(DISTINCT content) FROM messages WHERE content LIKE '${tag}-%';`)
    );
    assert.equal(distinctCount, total, `Expected ${total} distinct messages, got ${distinctCount}`);

    await waitForCondition(() => {
      const seen = new Set(
        watcher.messages.filter((m) => m.content.startsWith(`${tag}-`)).map((m) => m.content)
      );
      return seen.size === total;
    }, 15000, `watcher receives all ${total} distinct burst messages`);

    console.log(`  PASS B4: ${total}/${total} mensagens preservadas com kill -9 no worker-1 (0 perdidas, 0 duplicadas).`);
  } finally {
    await closeClients([sender, watcher]);
  }
}

// ---------------------------------------------------------------------------
// B5: Restarting PostgreSQL does not kill worker-1, web or gateways
// ---------------------------------------------------------------------------
async function testB5PostgresRestart() {
  console.log('\n### [B5] Reiniciar PostgreSQL não derruba worker-1, web nem gateways');

  const getPid = (container) =>
    execFileSync('docker', ['inspect', '-f', '{{.State.Pid}}', container], {
      encoding: 'utf8',
    }).trim();

  const workerPidBefore = getPid('pulsews-worker-1');
  const webPidBefore = getPid('pulsews-web');

  dockerCompose(['restart', 'postgres']);

  await waitForClusterHealthy(20000);
  await sleep(1000);

  const workerPidAfter = getPid('pulsews-worker-1');
  const webPidAfter = getPid('pulsews-web');

  assert.equal(workerPidAfter, workerPidBefore, 'worker-1 must survive Postgres restart without crashing');
  assert.equal(webPidAfter, webPidBefore, 'web must survive Postgres restart without crashing');

  // Verify end-to-end chat still delivers right after Postgres restart
  const tester = await connectClient('b5-tester');
  const tag = `b5-after-pg-${Date.now()}`;
  try {
    sendChat(tester, tag);
    await waitForCondition(
      () => tester.messages.some((m) => m.content === tag),
      10000,
      'message delivered after Postgres restart'
    );
    console.log('  PASS B5: worker-1 e web mantiveram o mesmo PID e o chat continuou operacional após restart do Postgres.');
  } finally {
    await closeClients([tester]);
  }
}

// ---------------------------------------------------------------------------
// B6 & B7: kill -9 on gateway-1 triggers <2s failover reconnect and removes
// orphaned ghost session from /api/cluster well before the 45s TTL.
// ---------------------------------------------------------------------------
async function testB6AndB7GatewayFailover() {
  console.log('\n### [B6 & B7] kill -9 no gateway-1: failover < 2s e sem sessão fantasma');

  // Connect clients until one lands on gateway-1
  const pool = [];
  let gw1Client = null;
  try {
    for (let i = 0; i < 6; i++) {
      const c = await connectClient(`fo-u${i}`);
      pool.push(c);
      if (c.gatewayId === 'gateway-1' && !gw1Client) {
        gw1Client = c;
        break;
      }
    }
    assert.ok(gw1Client, 'Should have at least one client on gateway-1');
    const ghostUser = gw1Client.userId;

    // Confirm ghostUser is currently listed in /api/cluster on gateway-1
    const clusterBefore = await getCluster();
    assert.ok(
      clusterBefore.sessions.some((s) => s.userId === ghostUser && s.gatewayId === 'gateway-1'),
      `${ghostUser} must be visible on gateway-1 before crash`
    );

    const closePromise = new Promise((resolve) => {
      gw1Client.ws.once('close', () => resolve(Date.now()));
    });

    const killStart = Date.now();
    dockerCompose(['kill', '-s', 'SIGKILL', 'gateway-1']);

    await closePromise;

    // Immediately attempt reconnect (simulating frontend auto-reconnect)
    const reconnectStart = Date.now();
    let reconnected = null;
    try {
      reconnected = await connectClient('fo-reconn', { lastSeenSeqId: 0, waitSync: true });
      const elapsedMs = Date.now() - reconnectStart;
      const totalMs = Date.now() - killStart;
      assert.ok(
        elapsedMs < 2000,
        `Reconnect after gateway kill should take < 2000ms (took ${elapsedMs}ms)`
      );

      // B7: Wait up to 8s (far less than 45s TTL) for ghost session to disappear from /api/cluster
      await waitForCondition(async () => {
        const cluster = await getCluster();
        return !cluster.sessions.some((s) => s.userId === ghostUser);
      }, 8000, `ghost session ${ghostUser} purged from /api/cluster`);

      const purgeMs = Date.now() - killStart;
      console.log(
        `  PASS B6 & B7: reconexão em ${elapsedMs}ms (total ${totalMs}ms, via ${reconnected.gatewayId}) e sessão fantasma limpa em ${purgeMs}ms (< 45s TTL).`
      );
    } finally {
      await closeClients([reconnected]);
    }
  } finally {
    await closeClients(pool);
    dockerCompose(['up', '-d', 'gateway-1']);
    await waitForClusterHealthy(15000);
    await sleep(1500);
  }
}

// ---------------------------------------------------------------------------
// B8: 5 MB payload rejected at gateway, >500 chars rejected, rate limit works
// ---------------------------------------------------------------------------
async function testB8PayloadAndRateLimit() {
  console.log('\n### [B8] Rejeição de payload de 5 MB, >500 chars e rate limit no gateway');

  const getRedisUsedMemBytes = () => {
    const info = redisCli(['INFO', 'memory']);
    const match = info.match(/used_memory:(\d+)/);
    return match ? Number(match[1]) : 0;
  };

  const memBefore = getRedisUsedMemBytes();

  // 1) 5 MB payload must close the socket (1009) without entering Redis
  const bigClient = await connectClient('big-payload');
  const closedPromise = new Promise((resolve) => {
    bigClient.ws.once('close', (code) => resolve(code));
  });

  const fiveMb = 'x'.repeat(5 * 1024 * 1024);
  bigClient.ws.send(JSON.stringify({ type: 'chat_message', content: fiveMb }));

  const closeCode = await waitForCondition(
    () => Promise.race([closedPromise, sleep(200).then(() => null)]),
    5000,
    'big-payload socket closed by maxPayload'
  );
  assert.equal(closeCode, 1009, `Expected WS close code 1009 (Message Too Big), got ${closeCode}`);

  const memAfter = getRedisUsedMemBytes();
  const memDeltaMb = (memAfter - memBefore) / (1024 * 1024);
  assert.ok(
    memDeltaMb < 2,
    `Redis memory should not spike on 5 MB payload (delta=${memDeltaMb.toFixed(2)} MB)`
  );

  // 2) >500 char payload (<4096 bytes) rejected in gateway with INVALID_PAYLOAD
  const overClient = await connectClient('over-500');
  try {
    sendChat(overClient, 'a'.repeat(800));
    await waitForCondition(
      () => overClient.errors.some((e) => e.code === 'INVALID_PAYLOAD'),
      5000,
      'INVALID_PAYLOAD error for >500 chars'
    );
  } finally {
    await closeClients([overClient]);
  }

  // 3) Flood above per-connection rate limit triggers RATE_LIMITED
  const floodClient = await connectClient('flood-user');
  try {
    for (let i = 0; i < 430; i++) {
      sendChat(floodClient, `f-${i}`);
    }
    await waitForCondition(
      () => floodClient.errors.some((e) => e.code === 'RATE_LIMITED'),
      5000,
      'RATE_LIMITED error on flood'
    );
  } finally {
    await closeClients([floodClient]);
  }

  console.log(
    `  PASS B8: 5 MB fechado com código ${closeCode} (delta RAM Redis: ${memDeltaMb.toFixed(2)} MB), >500 chars bloqueado e rate limit ativo.`
  );
}

// ---------------------------------------------------------------------------
// Regression check (Report p.8): Redis unavailable briefly while sending
// ---------------------------------------------------------------------------
async function testRegressionRedisBriefOutage() {
  console.log('\n### [Regressão Pág. 8] Redis fora brevemente enquanto cliente envia mensagem');
  const client = await connectClient('reg-redis-out');
  const tag = `redis-outage-${Date.now()}`;

  try {
    dockerCompose(['stop', 'redis']);
    await sleep(500);

    // Send while Redis is offline
    sendChat(client, tag);

    await sleep(3000);
    dockerCompose(['start', 'redis']);

    await waitForCondition(() => redisCli(['ping']) === 'PONG', 15000, 'Redis back online');
    await waitForClusterHealthy(15000);

    await waitForCondition(() => {
      const count = Number(
        psqlScalar(`SELECT COUNT(*) FROM messages WHERE content = '${tag}';`)
      );
      return count === 1;
    }, 15000, 'message queued during Redis outage persisted');

    console.log('  PASS Regressão: mensagem enviada durante indisponibilidade do Redis foi recuperada e persistida.');
  } finally {
    await closeClients([client]);
  }
}

async function run() {
  await waitForClusterHealthy();
  await testB1AndB2RedisRestart();
  await testB3WorkerSigterm();
  await testB4WorkerKill9Burst();
  await testB5PostgresRestart();
  await testB6AndB7GatewayFailover();
  await testB8PayloadAndRateLimit();
  await testRegressionRedisBriefOutage();

  console.log('\n==============================================================');
  console.log('ALL FAULT-INJECTION CHECKS (B1-B8 + REGRESSION) PASSED!');
  console.log('==============================================================');
}

run().catch((err) => {
  console.error('\nFAULT TEST FAILED:', err);
  process.exit(1);
});
