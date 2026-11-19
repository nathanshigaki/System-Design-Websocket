import assert from 'node:assert/strict';
import { WebSocket } from 'ws';

const BASE_HTTP = process.env.BASE_HTTP || 'http://127.0.0.1:3000';
const BASE_WS = process.env.BASE_WS || 'ws://127.0.0.1:3000/ws';
const TRACE_WS = process.env.TRACE_WS || 'ws://127.0.0.1:3000/ws-trace';

function connectClient(userId, lastSeenSeqId = null) {
  return new Promise((resolve, reject) => {
    const params = new URLSearchParams({ userId });
    if (lastSeenSeqId !== null) {
      params.set('lastSeenSeqId', String(lastSeenSeqId));
    }

    const ws = new WebSocket(`${BASE_WS}?${params.toString()}`);
    const receivedMessages = [];
    let welcome = null;
    let sync = null;

    const timer = setTimeout(() => {
      reject(new Error(`Timeout waiting for welcome on ${userId}`));
    }, 8000);

    ws.on('message', (raw) => {
      const msg = JSON.parse(raw.toString());
      if (msg.type === 'welcome') {
        welcome = msg;
        if (lastSeenSeqId === null) {
          clearTimeout(timer);
          resolve({ ws, welcome, receivedMessages, getSync: () => sync });
        }
      } else if (msg.type === 'sync') {
        sync = msg;
        clearTimeout(timer);
        resolve({ ws, welcome, receivedMessages, getSync: () => sync });
      } else if (msg.type === 'room_message') {
        receivedMessages.push(msg);
      }
    });

    ws.on('error', (err) => {
      clearTimeout(timer);
      reject(err);
    });
  });
}

function waitForCondition(predicate, timeoutMs = 8000, label = 'condition') {
  return new Promise((resolve, reject) => {
    const start = Date.now();
    const interval = setInterval(() => {
      if (predicate()) {
        clearInterval(interval);
        resolve();
      } else if (Date.now() - start > timeoutMs) {
        clearInterval(interval);
        reject(new Error(`Timeout waiting for ${label}`));
      }
    }, 50);
  });
}

async function run() {
  console.log('1) Conectando ao stream /ws-trace...');
  const traceHops = [];
  const traceWs = new WebSocket(TRACE_WS);
  await new Promise((resolve, reject) => {
    traceWs.once('open', resolve);
    traceWs.once('error', reject);
  });
  traceWs.on('message', (raw) => {
    traceHops.push(JSON.parse(raw.toString()));
  });

  console.log('2) Conectando 6 clientes WebSocket via /ws -> HAProxy L4 (leastconn)...');
  const clients = [];
  for (let i = 1; i <= 6; i += 1) {
    const c = await connectClient(`test-user-${i}`);
    clients.push(c);
  }

  const byGw = { 'gateway-1': 0, 'gateway-2': 0 };
  for (const c of clients) {
    byGw[c.welcome.gatewayId] = (byGw[c.welcome.gatewayId] || 0) + 1;
  }
  console.log('   Distribuicao L4:', byGw);
  assert.ok(byGw['gateway-1'] >= 2, `Expected >=2 on gateway-1, got ${byGw['gateway-1']}`);
  assert.ok(byGw['gateway-2'] >= 2, `Expected >=2 on gateway-2, got ${byGw['gateway-2']}`);

  const senderGw1 = clients.find((c) => c.welcome.gatewayId === 'gateway-1');
  const receiverGw2 = clients.find((c) => c.welcome.gatewayId === 'gateway-2');
  assert.ok(senderGw1 && receiverGw2, 'Need clients on both gateway-1 and gateway-2');

  console.log('3) Testando broadcast cross-gateway (gateway-1 -> gateway-2)...');
  const probeText = `cross-gw-probe-${Date.now()}`;
  senderGw1.ws.send(JSON.stringify({ type: 'chat_message', content: probeText }));

  await waitForCondition(
    () => clients.every((c) => c.receivedMessages.some((m) => m.content === probeText)),
    8000,
    'cross-gateway delivery to all 6 clients'
  );

  const probeMsg = receiverGw2.receivedMessages.find((m) => m.content === probeText);
  assert.ok(probeMsg.seqId > 0, 'Expected positive seqId');
  console.log(`   Mensagem recebida no gateway-2 com #seqId=${probeMsg.seqId}`);

  console.log('4) Testando simulacao de queda + replay via Ring Buffer...');
  const dropClient = clients[5];
  const lastSeenSeqId = dropClient.receivedMessages.at(-1).seqId;
  dropClient.ws.close(1000, 'Test simulated drop');
  await new Promise((r) => setTimeout(r, 200));

  const missedTexts = [1, 2, 3].map((n) => `missed-msg-${n}-${Date.now()}`);
  for (const text of missedTexts) {
    senderGw1.ws.send(JSON.stringify({ type: 'chat_message', content: text }));
  }

  await waitForCondition(
    () =>
      missedTexts.every((text) =>
        receiverGw2.receivedMessages.some((m) => m.content === text)
      ),
    8000,
    'delivery of 3 missed messages while client offline'
  );

  const reconnected = await connectClient('test-user-6', lastSeenSeqId);
  const sync = reconnected.getSync();
  assert.equal(sync.recoverySource, 'ring_buffer', 'Expected recovery from Redis Ring Buffer');
  assert.equal(sync.messages.length, 3, 'Expected exactly 3 recovered messages');
  assert.deepEqual(
    sync.messages.map((m) => m.content),
    missedTexts,
    'Expected recovered messages in exact seqId order'
  );
  console.log('   Reconexao recuperou 3 mensagens via ring_buffer (0 queries no PostgreSQL)!');

  console.log('5) Testando endpoint de Short Polling (/api/notifications)...');
  const pollRes = await fetch(`${BASE_HTTP}/api/notifications?afterId=0`);
  assert.equal(pollRes.status, 200);
  const pollData = await pollRes.json();
  assert.ok(Array.isArray(pollData.notifications) && pollData.notifications.length > 0);
  assert.ok(
    pollData.notifications.every((n) => n.type !== 'message'),
    'Short Polling should only contain presence/system notifications, not duplicate chat messages'
  );
  console.log(`   Polling retornou ${pollData.notifications.length} notificacoes de presenca.`);

  console.log('6) Testando /api/cluster e eventos recebidos em /ws-trace...');
  const clusterRes = await fetch(`${BASE_HTTP}/api/cluster`);
  assert.equal(clusterRes.status, 200);
  const clusterData = await clusterRes.json();
  assert.ok(clusterData.currentSeqId >= probeMsg.seqId + 3);
  assert.ok(clusterData.ringBufferSize >= 4);

  await waitForCondition(
    () =>
      traceHops.some((h) => h.flow === 'websocket') &&
      traceHops.some((h) => h.flow === 'reconnect') &&
      traceHops.some((h) => h.flow === 'polling'),
    5000,
    'all 3 trace flows in /ws-trace'
  );
  console.log(`   /ws-trace capturou ${traceHops.length} saltos reais (websocket, reconnect, polling).`);

  for (const c of [...clients.slice(0, 5), reconnected]) {
    c.ws.close(1000, 'Test done');
  }
  traceWs.close(1000, 'Test done');

  console.log('\nPASS: Todos os testes do sistema distribuido passaram com sucesso!');
}

run().catch((err) => {
  console.error('\nFAIL:', err);
  process.exit(1);
});
