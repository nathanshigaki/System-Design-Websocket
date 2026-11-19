const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
const WS_URL = `${wsProtocol}//${window.location.host}/ws`;
const TRACE_WS_URL = `${wsProtocol}//${window.location.host}/ws-trace`;

const STEP_STAGGER_MS = 260;
const HOP_DURATION_MS = 320;
const REPLAY_STAGGER_MS = 680;
const REPLAY_DURATION_MS = 600;

const AVATAR_PALETTE = [
  '#38bdf8',
  '#22c55e',
  '#f59e0b',
  '#14b8a6',
  '#fb7185',
  '#60a5fa',
];

const state = {
  ws: null,
  traceWs: null,
  userId: '',
  gatewayId: '',
  lastSeenSeqId: 0,
  canReconnect: false,
  seenSeqIds: new Set(),
  pollingEnabled: true,
  lastNotificationId: 0,
  pollStats: { total: 0, hits: 0, empty: 0 },
  tracesById: new Map(),
  traceBuffers: new Map(),
  traceFlushTimers: new Map(),
  lastWebSocketTraceId: null,
  replaying: false,
};

const el = {
  haproxyLink: document.getElementById('haproxy-link'),
  liveRing: document.getElementById('live-ring'),
  connMeta: document.getElementById('conn-meta'),
  connectForm: document.getElementById('connect-form'),
  usernameInput: document.getElementById('username-input'),
  btnConnect: document.getElementById('btn-connect'),
  btnSwitchUser: document.getElementById('btn-switch-user'),
  syncNotice: document.getElementById('sync-notice'),
  chatMessages: document.getElementById('chat-messages'),
  chatForm: document.getElementById('chat-form'),
  messageInput: document.getElementById('message-input'),
  btnSend: document.getElementById('btn-send'),
  btnTogglePolling: document.getElementById('btn-toggle-polling'),
  pollTotal: document.getElementById('poll-total'),
  pollHits: document.getElementById('poll-hits'),
  pollEmpty: document.getElementById('poll-empty'),
  pollList: document.getElementById('poll-list'),
  clusterGw1: document.getElementById('cluster-gw1'),
  clusterGw2: document.getElementById('cluster-gw2'),
  clusterRing: document.getElementById('cluster-ring'),
  sessionList: document.getElementById('session-list'),
  togglePollingAnim: document.getElementById('toggle-polling-anim'),
  btnReplay: document.getElementById('btn-replay'),
  packetsLayer: document.getElementById('packets-layer'),
  traceBanner: document.getElementById('trace-step-banner'),
  svgGw1Count: document.getElementById('svg-gw1-count'),
  svgGw2Count: document.getElementById('svg-gw2-count'),
  svgRingCount: document.getElementById('svg-ring-count'),
  svgSeqCount: document.getElementById('svg-seq-count'),
};

if (window.location.hostname !== 'localhost' && window.location.hostname !== '127.0.0.1') {
  el.haproxyLink.classList.add('hidden');
}

const ROUTE_MAP = {
  'client->web': { pathId: 'path-client-web', reverse: false },
  'web->client': { pathId: 'path-client-web', reverse: true },
  'web->haproxy': { pathId: 'path-web-haproxy', reverse: false },
  'haproxy->gateway-1': { pathId: 'path-haproxy-gw1', reverse: false },
  'haproxy->gateway-2': { pathId: 'path-haproxy-gw2', reverse: false },
  'gateway-1->client': { pathId: 'path-gw1-client', reverse: false },
  'client->gateway-1': { pathId: 'path-gw1-client', reverse: true },
  'gateway-2->client': { pathId: 'path-gw2-client', reverse: false },
  'client->gateway-2': { pathId: 'path-gw2-client', reverse: true },
  'gateway-1->redis': { pathId: 'path-gw1-redis', reverse: false },
  'redis->gateway-1': { pathId: 'path-gw1-redis', reverse: true },
  'gateway-2->redis': { pathId: 'path-gw2-redis', reverse: false },
  'redis->gateway-2': { pathId: 'path-gw2-redis', reverse: true },
  'redis->worker': { pathId: 'path-redis-worker', reverse: false },
  'worker->redis': { pathId: 'path-worker-redis', reverse: false },
  'worker->postgres': { pathId: 'path-worker-postgres', reverse: false },
  'web->postgres': { pathId: 'path-web-postgres', reverse: false },
  'postgres->web': { pathId: 'path-web-postgres', reverse: true },
  'postgres->gateway-1': { pathId: 'path-postgres-gw1', reverse: false },
  'postgres->gateway-2': { pathId: 'path-postgres-gw2', reverse: false },
};

const FLOW_COLORS = {
  websocket: '#38bdf8',
  polling: '#f59e0b',
  reconnect: '#22c55e',
};

function colorForUser(name) {
  let hash = 0;
  for (let i = 0; i < name.length; i += 1) {
    hash = (hash * 31 + name.charCodeAt(i)) >>> 0;
  }
  return AVATAR_PALETTE[hash % AVATAR_PALETTE.length];
}

function flashNode(nodeId, flow, durationMs) {
  const node = document.getElementById(`node-${nodeId}`);
  if (!node) return;
  const cls =
    flow === 'polling'
      ? 'active-polling'
      : flow === 'reconnect'
        ? 'active-reconnect'
        : 'active';
  node.classList.add(cls);
  setTimeout(() => node.classList.remove(cls), Math.max(280, durationMs));
}

function createPacketDot(radius, fill, opacity) {
  const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
  circle.setAttribute('r', String(radius));
  circle.setAttribute('fill', fill);
  circle.setAttribute('opacity', String(opacity));
  return circle;
}

function animateHop(hop, durationMs = HOP_DURATION_MS, updateText = false) {
  const route = ROUTE_MAP[`${hop.from}->${hop.to}`];
  flashNode(hop.from, hop.flow, durationMs);
  flashNode(hop.to, hop.flow, durationMs);

  if (updateText) {
    el.traceBanner.textContent = `${hop.from} -> ${hop.to} (${hop.protocol}): ${hop.detail}`;
  }

  if (!route) return;
  const pathEl = document.getElementById(route.pathId);
  if (!pathEl) return;

  pathEl.classList.add('active');
  setTimeout(() => pathEl.classList.remove('active'), durationMs + 80);

  const totalLength = pathEl.getTotalLength();
  const color = FLOW_COLORS[hop.flow] || '#38bdf8';

  const group = document.createElementNS('http://www.w3.org/2000/svg', 'g');
  const tail2 = createPacketDot(3.2, color, 0.35);
  const tail1 = createPacketDot(4.4, color, 0.65);
  const head = createPacketDot(6, color, 1);
  head.setAttribute('stroke', '#f8fafc');
  head.setAttribute('stroke-width', '1.4');
  head.setAttribute('filter', 'url(#packet-glow)');

  group.appendChild(tail2);
  group.appendChild(tail1);
  group.appendChild(head);
  el.packetsLayer.appendChild(group);

  const start = performance.now();
  function frame(now) {
    const t = Math.min(1, (now - start) / durationMs);
    const eased = t < 0.5 ? 2 * t * t : 1 - ((-2 * t + 2) ** 2) / 2;

    const placeDot = (dot, offset) => {
      const p = Math.max(0, Math.min(1, eased - offset));
      const dist = route.reverse ? totalLength * (1 - p) : totalLength * p;
      const pt = pathEl.getPointAtLength(dist);
      dot.setAttribute('cx', String(pt.x));
      dot.setAttribute('cy', String(pt.y));
    };

    placeDot(head, 0);
    placeDot(tail1, 0.06);
    placeDot(tail2, 0.12);

    if (t < 1) {
      requestAnimationFrame(frame);
    } else {
      group.remove();
    }
  }
  requestAnimationFrame(frame);
}

/**
 * Plays a trace sequence grouped by `step` with real visual pacing,
 * preventing simultaneous flashing when Redis/Worker complete in <5ms.
 */
async function playTraceSequence(hops, stepDelayMs, hopDurationMs, updateText) {
  const byStep = new Map();
  for (const hop of hops) {
    const s = hop.step || 1;
    if (!byStep.has(s)) byStep.set(s, []);
    byStep.get(s).push(hop);
  }

  const orderedSteps = [...byStep.keys()].sort((a, b) => a - b);
  for (let i = 0; i < orderedSteps.length; i += 1) {
    const stepHops = byStep.get(orderedSteps[i]);
    for (const hop of stepHops) {
      animateHop(hop, hopDurationMs, updateText);
    }
    if (i < orderedSteps.length - 1) {
      await new Promise((r) => setTimeout(r, stepDelayMs));
    }
  }
}

function scheduleTracePlayback(traceId, flow) {
  if (state.traceFlushTimers.has(traceId)) {
    clearTimeout(state.traceFlushTimers.get(traceId));
  }

  const timer = setTimeout(() => {
    state.traceFlushTimers.delete(traceId);
    const hops = state.traceBuffers.get(traceId);
    state.traceBuffers.delete(traceId);
    if (!hops || hops.length === 0 || state.replaying) return;

    const isChatOrReconnect = flow === 'websocket' || flow === 'reconnect';
    playTraceSequence(hops, STEP_STAGGER_MS, HOP_DURATION_MS, isChatOrReconnect);
  }, 90);

  state.traceFlushTimers.set(traceId, timer);
}

function recordTraceHop(hop) {
  const isChatOrReconnect = hop.flow === 'websocket' || hop.flow === 'reconnect';
  if (isChatOrReconnect) {
    if (!state.tracesById.has(hop.traceId)) {
      state.tracesById.set(hop.traceId, []);
      if (state.tracesById.size > 20) {
        const oldestKey = state.tracesById.keys().next().value;
        state.tracesById.delete(oldestKey);
      }
    }
    state.tracesById.get(hop.traceId).push(hop);
    state.lastWebSocketTraceId = hop.traceId;
    el.btnReplay.disabled = false;
  }

  if (hop.flow === 'polling' && !el.togglePollingAnim.checked) {
    return;
  }

  if (!state.traceBuffers.has(hop.traceId)) {
    state.traceBuffers.set(hop.traceId, []);
  }
  state.traceBuffers.get(hop.traceId).push(hop);
  scheduleTracePlayback(hop.traceId, hop.flow);
}

function connectTraceStream() {
  const ws = new WebSocket(TRACE_WS_URL);
  state.traceWs = ws;

  ws.onmessage = (event) => {
    try {
      const hop = JSON.parse(event.data);
      if (hop.type === 'trace_hop') {
        recordTraceHop(hop);
      }
    } catch {
      // ignore invalid frames
    }
  };
  ws.onclose = () => {
    setTimeout(connectTraceStream, 2000);
  };
}

el.btnReplay.addEventListener('click', async () => {
  if (state.replaying || !state.lastWebSocketTraceId) return;
  const hops = state.tracesById.get(state.lastWebSocketTraceId);
  if (!hops || hops.length === 0) return;

  state.replaying = true;
  el.btnReplay.disabled = true;
  el.btnReplay.textContent = 'Reproduzindo...';

  await playTraceSequence(hops, REPLAY_STAGGER_MS, REPLAY_DURATION_MS, true);

  state.replaying = false;
  el.btnReplay.disabled = false;
  el.btnReplay.textContent = 'Replay (Câmera Lenta)';
});

function clearEmptyState(container) {
  const empty = container.querySelector('.empty-state');
  if (empty) empty.remove();
}

function addSystemLine(text) {
  clearEmptyState(el.chatMessages);
  const div = document.createElement('div');
  div.className = 'msg-system';
  div.textContent = text;
  el.chatMessages.appendChild(div);
  el.chatMessages.scrollTop = el.chatMessages.scrollHeight;
}

function addChatMessage(msg, recovered = false) {
  if (state.seenSeqIds.has(msg.seqId)) return;
  state.seenSeqIds.add(msg.seqId);
  if (msg.seqId > state.lastSeenSeqId) {
    state.lastSeenSeqId = msg.seqId;
    updateMetaLine(Boolean(state.ws));
  }

  clearEmptyState(el.chatMessages);

  const row = document.createElement('div');
  const classes = ['msg-row'];
  if (msg.userId === state.userId) classes.push('is-own');
  if (recovered) classes.push('is-recovered');
  row.className = classes.join(' ');

  const avatar = document.createElement('div');
  avatar.className = 'msg-avatar';
  avatar.style.backgroundColor = colorForUser(msg.userId);
  avatar.textContent = msg.userId.slice(0, 2).toUpperCase();

  const contentWrap = document.createElement('div');
  contentWrap.className = 'msg-content-wrap';

  const head = document.createElement('div');
  head.className = 'msg-head';

  const authorWrap = document.createElement('span');
  authorWrap.className = 'msg-author-wrap';

  const author = document.createElement('span');
  author.className = 'msg-author';
  author.textContent = msg.userId;

  const gwChip = document.createElement('span');
  gwChip.className = `gw-chip ${msg.gatewayId === 'gateway-2' ? 'gw-chip-2' : 'gw-chip-1'}`;
  gwChip.textContent = msg.gatewayId;

  authorWrap.appendChild(author);
  authorWrap.appendChild(gwChip);

  const meta = document.createElement('span');
  meta.className = 'msg-meta';
  const time = new Date(msg.createdAt).toLocaleTimeString('pt-BR', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
  meta.textContent = `#${msg.seqId} · ${time}${recovered ? ' · recuperada' : ''}`;

  head.appendChild(authorWrap);
  head.appendChild(meta);

  const body = document.createElement('div');
  body.className = 'msg-body';
  body.textContent = msg.content;

  contentWrap.appendChild(head);
  contentWrap.appendChild(body);
  row.appendChild(avatar);
  row.appendChild(contentWrap);
  el.chatMessages.appendChild(row);
  el.chatMessages.scrollTop = el.chatMessages.scrollHeight;
}

function updateMetaLine(connected) {
  if (connected) {
    el.liveRing.className = 'live-ring is-live';
    el.connMeta.textContent = `${state.userId} · ${state.gatewayId} · #seq ${state.lastSeenSeqId}`;
    el.connMeta.className = 'meta-inline is-online';
  } else if (state.canReconnect) {
    el.liveRing.className = 'live-ring is-saved';
    el.connMeta.textContent = `Desconectado (${state.userId} · #seq ${state.lastSeenSeqId})`;
    el.connMeta.className = 'meta-inline is-offline-saved';
  } else {
    el.liveRing.className = 'live-ring';
    el.connMeta.textContent = 'Desconectado';
    el.connMeta.className = 'meta-inline';
  }
}

function updateConnectionUi(connected) {
  el.messageInput.disabled = !connected;
  el.btnSend.disabled = !connected;
  updateMetaLine(connected);

  if (connected) {
    el.usernameInput.classList.add('hidden');
    el.btnSwitchUser.classList.add('hidden');
    el.btnConnect.textContent = 'Desconectar';
    el.btnConnect.className = 'btn btn-secondary';
  } else if (state.canReconnect) {
    el.usernameInput.classList.add('hidden');
    el.btnSwitchUser.classList.remove('hidden');
    el.btnConnect.textContent = 'Reconectar';
    el.btnConnect.className = 'btn btn-primary';
  } else {
    el.usernameInput.classList.remove('hidden');
    el.usernameInput.disabled = false;
    el.btnSwitchUser.classList.add('hidden');
    el.btnConnect.textContent = 'Conectar';
    el.btnConnect.className = 'btn btn-primary';
  }
}

function openChatSocket(useLastSeq = false) {
  const params = new URLSearchParams({ userId: state.userId });
  if (useLastSeq) {
    params.set('lastSeenSeqId', String(state.lastSeenSeqId));
  }

  const ws = new WebSocket(`${WS_URL}?${params.toString()}`);
  state.ws = ws;

  ws.onmessage = (event) => {
    const msg = JSON.parse(event.data);

    if (msg.type === 'welcome') {
      state.gatewayId = msg.gatewayId;
      state.canReconnect = true;
      if (!useLastSeq && msg.currentSeqId > state.lastSeenSeqId) {
        state.lastSeenSeqId = msg.currentSeqId;
      }
      updateConnectionUi(true);
      refreshCluster();
      return;
    }

    if (msg.type === 'history') {
      for (const item of msg.messages || []) {
        addChatMessage(item, false);
      }
      return;
    }

    if (msg.type === 'sync') {
      const labels = {
        ring_buffer: 'Ring Buffer Redis (0 queries no DB)',
        postgres_offline: 'Fallback PostgreSQL (offline longo)',
        up_to_date: 'Nenhuma mensagem perdida',
      };
      const label = labels[msg.recoverySource] || msg.recoverySource;
      el.syncNotice.textContent = `Reconexão: ${label} · ${msg.messages.length} msg(s) recuperada(s).`;
      el.syncNotice.classList.remove('hidden');

      for (const item of msg.messages || []) {
        addChatMessage(item, true);
      }
      return;
    }

    if (msg.type === 'room_message') {
      addChatMessage(msg, false);
    }
  };

  ws.onclose = () => {
    if (state.ws === ws) {
      state.ws = null;
      updateConnectionUi(false);
      refreshCluster();
    }
  };
}

el.connectForm.addEventListener('submit', (e) => {
  e.preventDefault();
  if (state.ws) {
    state.ws.close(1000, 'User disconnect');
    addSystemLine(`Desconectado mantendo #seq ${state.lastSeenSeqId}. Clique em Reconectar para testar o Ring Buffer.`);
    return;
  }

  if (state.canReconnect && state.userId) {
    openChatSocket(true);
    return;
  }

  const name = el.usernameInput.value.trim();
  if (!name) return;
  state.userId = name;
  state.seenSeqIds.clear();
  state.lastSeenSeqId = 0;
  state.canReconnect = false;
  el.syncNotice.classList.add('hidden');
  el.chatMessages.innerHTML = '';
  openChatSocket(false);
});

el.btnSwitchUser.addEventListener('click', () => {
  state.userId = '';
  state.gatewayId = '';
  state.lastSeenSeqId = 0;
  state.canReconnect = false;
  state.seenSeqIds.clear();
  el.syncNotice.classList.add('hidden');
  el.chatMessages.innerHTML =
    '<div class="empty-state">Entre com um nome acima para conectar na sala WebSocket.</div>';
  updateConnectionUi(false);
  el.usernameInput.focus();
});

el.chatForm.addEventListener('submit', (e) => {
  e.preventDefault();
  const content = el.messageInput.value.trim();
  if (!content || !state.ws || state.ws.readyState !== WebSocket.OPEN) return;

  state.ws.send(JSON.stringify({ type: 'chat_message', content }));
  el.messageInput.value = '';
  el.messageInput.focus();
});

async function pollNotifications() {
  if (!state.pollingEnabled) return;
  try {
    const res = await fetch(`/api/notifications?afterId=${state.lastNotificationId}`);
    if (!res.ok) return;
    const data = await res.json();

    state.pollStats.total += 1;
    if (data.empty) {
      state.pollStats.empty += 1;
    } else {
      state.pollStats.hits += 1;
      state.lastNotificationId = data.lastId;
      for (const item of data.notifications) {
        if (item.type === 'message') continue;
        clearEmptyState(el.pollList);
        const row = document.createElement('div');
        row.className = 'feed-row';

        const msgSpan = document.createElement('span');
        msgSpan.textContent = item.message;

        const timeSpan = document.createElement('span');
        timeSpan.className = 'feed-time';
        timeSpan.textContent = new Date(item.createdAt).toLocaleTimeString('pt-BR', {
          hour: '2-digit',
          minute: '2-digit',
          second: '2-digit',
        });

        row.appendChild(msgSpan);
        row.appendChild(timeSpan);
        el.pollList.prepend(row);
      }
      while (el.pollList.children.length > 15) {
        el.pollList.lastChild.remove();
      }
    }

    el.pollTotal.textContent = String(state.pollStats.total);
    el.pollHits.textContent = String(state.pollStats.hits);
    el.pollEmpty.textContent = String(state.pollStats.empty);
  } catch {
    // ignore transient poll error
  }
}

el.btnTogglePolling.addEventListener('click', () => {
  state.pollingEnabled = !state.pollingEnabled;
  el.btnTogglePolling.textContent = state.pollingEnabled ? 'Pausar' : 'Retomar';
});

async function refreshCluster() {
  try {
    const res = await fetch('/api/cluster');
    if (!res.ok) return;
    const data = await res.json();

    const gw1 = data.summaryByGateway['gateway-1'] || 0;
    const gw2 = data.summaryByGateway['gateway-2'] || 0;

    el.clusterGw1.textContent = String(gw1);
    el.clusterGw2.textContent = String(gw2);
    el.clusterRing.textContent = `${data.ringBufferSize}/${data.ringBufferCapacity}`;

    el.svgGw1Count.textContent = `${gw1} conexões`;
    el.svgGw2Count.textContent = `${gw2} conexões`;
    el.svgRingCount.textContent = `Ring Buffer (${data.ringBufferSize}/${data.ringBufferCapacity})`;
    el.svgSeqCount.textContent = `#seqId: ${data.currentSeqId}`;

    el.sessionList.innerHTML = '';
    if (!data.sessions || data.sessions.length === 0) {
      const empty = document.createElement('div');
      empty.className = 'empty-state';
      empty.textContent = 'Nenhuma sessão ativa.';
      el.sessionList.appendChild(empty);
      return;
    }

    for (const session of data.sessions) {
      const row = document.createElement('div');
      row.className = 'feed-row';

      const left = document.createElement('span');
      left.textContent = session.userId;

      const right = document.createElement('span');
      right.className = `gw-chip ${session.gatewayId === 'gateway-2' ? 'gw-chip-2' : 'gw-chip-1'}`;
      right.textContent = session.gatewayId;

      row.appendChild(left);
      row.appendChild(right);
      el.sessionList.appendChild(row);
    }
  } catch {
    // ignore transient cluster poll error
  }
}

connectTraceStream();
pollNotifications();
refreshCluster();
setInterval(pollNotifications, 3000);
setInterval(refreshCluster, 2500);
