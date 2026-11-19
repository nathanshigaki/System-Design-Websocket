const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
const WS_URL = `${wsProtocol}//${window.location.host}/ws`;
const TRACE_WS_URL = `${wsProtocol}//${window.location.host}/ws-trace`;

const state = {
  ws: null,
  traceWs: null,
  userId: '',
  gatewayId: '',
  lastSeenSeqId: 0,
  simulatedDrop: false,
  seenSeqIds: new Set(),
  pollingEnabled: true,
  lastNotificationId: 0,
  pollStats: { total: 0, hits: 0, empty: 0 },
  tracesById: new Map(),
  lastWebSocketTraceId: null,
  replaying: false,
};

const el = {
  traceStatus: document.getElementById('trace-status'),
  haproxyLink: document.getElementById('haproxy-link'),
  togglePollingAnim: document.getElementById('toggle-polling-anim'),
  btnReplay: document.getElementById('btn-replay'),
  traceBanner: document.getElementById('trace-step-banner'),
  traceLog: document.getElementById('trace-log'),
  packetsLayer: document.getElementById('packets-layer'),
  svgGw1Count: document.getElementById('svg-gw1-count'),
  svgGw2Count: document.getElementById('svg-gw2-count'),
  svgRingCount: document.getElementById('svg-ring-count'),
  svgSeqCount: document.getElementById('svg-seq-count'),
  connectForm: document.getElementById('connect-form'),
  usernameInput: document.getElementById('username-input'),
  btnConnect: document.getElementById('btn-connect'),
  btnSimulateDrop: document.getElementById('btn-simulate-drop'),
  btnNewTab: document.getElementById('btn-new-tab'),
  wsConnBadge: document.getElementById('ws-conn-badge'),
  wsGwBadge: document.getElementById('ws-gw-badge'),
  wsSeqBadge: document.getElementById('ws-seq-badge'),
  wsRecoveryBadge: document.getElementById('ws-recovery-badge'),
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
};

if (window.location.hostname !== 'localhost' && window.location.hostname !== '127.0.0.1') {
  el.haproxyLink.style.display = 'none';
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

function flashNode(nodeId, flow) {
  const node = document.getElementById(`node-${nodeId}`);
  if (!node) return;
  const cls =
    flow === 'polling'
      ? 'active-polling'
      : flow === 'reconnect'
        ? 'active-reconnect'
        : 'active';
  node.classList.add(cls);
  setTimeout(() => node.classList.remove(cls), 550);
}

function animateHop(hop, durationMs = 420) {
  const route = ROUTE_MAP[`${hop.from}->${hop.to}`];
  flashNode(hop.from, hop.flow);
  flashNode(hop.to, hop.flow);

  el.traceBanner.textContent = `[${hop.flow.toUpperCase()}] ${hop.from} -> ${hop.to} (${hop.protocol}): ${hop.detail}`;

  if (!route) return;
  const pathEl = document.getElementById(route.pathId);
  if (!pathEl) return;

  pathEl.classList.add('active');
  setTimeout(() => pathEl.classList.remove('active'), durationMs + 120);

  const totalLength = pathEl.getTotalLength();
  const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
  circle.setAttribute('r', '6');
  circle.setAttribute('fill', FLOW_COLORS[hop.flow] || '#38bdf8');
  circle.setAttribute('stroke', '#ffffff');
  circle.setAttribute('stroke-width', '1.5');
  el.packetsLayer.appendChild(circle);

  const start = performance.now();
  function frame(now) {
    const t = Math.min(1, (now - start) / durationMs);
    const dist = route.reverse ? totalLength * (1 - t) : totalLength * t;
    const pt = pathEl.getPointAtLength(dist);
    circle.setAttribute('cx', String(pt.x));
    circle.setAttribute('cy', String(pt.y));
    if (t < 1) {
      requestAnimationFrame(frame);
    } else {
      circle.remove();
    }
  }
  requestAnimationFrame(frame);
}

function appendTraceChip(hop) {
  const chip = document.createElement('span');
  chip.className = 'trace-chip';
  chip.textContent = `${hop.from} -> ${hop.to} (${hop.protocol})`;
  el.traceLog.prepend(chip);
  while (el.traceLog.children.length > 14) {
    el.traceLog.lastChild.remove();
  }
}

function recordTraceHop(hop) {
  if (hop.flow === 'websocket' || hop.flow === 'reconnect') {
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
  appendTraceChip(hop);
  if (!state.replaying) {
    animateHop(hop, 400);
  }
}

function connectTraceStream() {
  const ws = new WebSocket(TRACE_WS_URL);
  state.traceWs = ws;

  ws.onopen = () => {
    el.traceStatus.textContent = 'Trace: conectado';
    el.traceStatus.className = 'pill pill-ok';
  };
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
    el.traceStatus.textContent = 'Trace: reconectando...';
    el.traceStatus.className = 'pill pill-warn';
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

  const ordered = [...hops].sort((a, b) => (a.step || 0) - (b.step || 0));
  for (const hop of ordered) {
    animateHop(hop, 620);
    await new Promise((r) => setTimeout(r, 680));
  }

  state.replaying = false;
  el.btnReplay.disabled = false;
  el.btnReplay.textContent = 'Replay Última Rota (Câmera Lenta)';
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
    el.wsSeqBadge.textContent = `#seqId: ${state.lastSeenSeqId}`;
  }

  clearEmptyState(el.chatMessages);
  const card = document.createElement('div');
  card.className = recovered ? 'msg-item msg-recovered' : 'msg-item';

  const meta = document.createElement('div');
  meta.className = 'msg-meta';

  const left = document.createElement('span');
  const author = document.createElement('span');
  author.className = 'msg-author';
  author.textContent = msg.userId;
  left.appendChild(author);
  left.appendChild(document.createTextNode(` • via ${msg.gatewayId}`));
  if (recovered) {
    left.appendChild(document.createTextNode(' • [recuperada]'));
  }

  const right = document.createElement('span');
  const time = new Date(msg.createdAt).toLocaleTimeString('pt-BR');
  right.textContent = `#${msg.seqId} • ${time}`;

  meta.appendChild(left);
  meta.appendChild(right);

  const body = document.createElement('div');
  body.className = 'msg-text';
  body.textContent = msg.content;

  card.appendChild(meta);
  card.appendChild(body);
  el.chatMessages.appendChild(card);
  el.chatMessages.scrollTop = el.chatMessages.scrollHeight;
}

function updateConnectionUi(connected) {
  el.messageInput.disabled = !connected;
  el.btnSend.disabled = !connected;
  el.btnSimulateDrop.disabled = !connected && !state.simulatedDrop;

  if (connected) {
    el.wsConnBadge.textContent = `Conectado (${state.userId})`;
    el.wsConnBadge.className = 'pill pill-ok';
    el.btnConnect.textContent = 'Desconectar';
    el.btnSimulateDrop.textContent = 'Simular Queda';
    el.btnSimulateDrop.disabled = false;
  } else if (state.simulatedDrop) {
    el.wsConnBadge.textContent = `Queda simulada (${state.userId})`;
    el.wsConnBadge.className = 'pill pill-warn';
    el.btnConnect.textContent = 'Cancelar';
    el.btnSimulateDrop.textContent = 'Reconectar (lastSeenSeqId)';
    el.btnSimulateDrop.disabled = false;
  } else {
    el.wsConnBadge.textContent = 'Desconectado';
    el.wsConnBadge.className = 'pill pill-muted';
    el.wsGwBadge.textContent = 'Gateway: —';
    el.wsGwBadge.className = 'pill pill-muted';
    el.btnConnect.textContent = 'Conectar';
    el.btnSimulateDrop.textContent = 'Simular Queda';
    el.btnSimulateDrop.disabled = true;
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
      if (!useLastSeq && msg.currentSeqId > state.lastSeenSeqId) {
        state.lastSeenSeqId = msg.currentSeqId;
        el.wsSeqBadge.textContent = `#seqId: ${state.lastSeenSeqId}`;
      }
      el.wsGwBadge.textContent = `Gateway: ${msg.gatewayId}`;
      el.wsGwBadge.className = 'pill pill-blue';
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
        up_to_date: 'Sem mensagens perdidas',
      };
      el.wsRecoveryBadge.textContent = `Sync: ${labels[msg.recoverySource] || msg.recoverySource}`;
      el.wsRecoveryBadge.className = 'pill pill-ok';
      for (const item of msg.messages || []) {
        addChatMessage(item, true);
      }
      addSystemLine(
        `Reconexão concluída (${labels[msg.recoverySource] || msg.recoverySource}) — ${msg.messages.length} msg(s) recuperadas.`
      );
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
    state.simulatedDrop = false;
    state.ws.close(1000, 'User disconnect');
    return;
  }
  if (state.simulatedDrop) {
    state.simulatedDrop = false;
    updateConnectionUi(false);
    return;
  }

  const name = el.usernameInput.value.trim();
  if (!name) return;
  state.userId = name;
  state.seenSeqIds.clear();
  state.lastSeenSeqId = 0;
  el.wsRecoveryBadge.className = 'pill pill-hidden';
  el.chatMessages.innerHTML = '';
  openChatSocket(false);
});

el.btnSimulateDrop.addEventListener('click', () => {
  if (state.ws) {
    state.simulatedDrop = true;
    state.ws.close(1000, 'Simulated drop');
    addSystemLine(`Queda simulada! Último #seqId visto: ${state.lastSeenSeqId}.`);
    return;
  }
  if (state.simulatedDrop) {
    state.simulatedDrop = false;
    openChatSocket(true);
  }
});

el.chatForm.addEventListener('submit', (e) => {
  e.preventDefault();
  const content = el.messageInput.value.trim();
  if (!content || !state.ws || state.ws.readyState !== WebSocket.OPEN) return;

  state.ws.send(JSON.stringify({ type: 'chat_message', content }));
  el.messageInput.value = '';
  el.messageInput.focus();
});

el.btnNewTab.addEventListener('click', () => {
  window.open(window.location.href, '_blank');
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
      clearEmptyState(el.pollList);
      for (const item of data.notifications) {
        const row = document.createElement('div');
        row.className = 'list-row';
        const msgSpan = document.createElement('span');
        msgSpan.textContent = item.message;
        const timeSpan = document.createElement('span');
        timeSpan.style.color = '#94a3b8';
        timeSpan.textContent = new Date(item.createdAt).toLocaleTimeString('pt-BR');
        row.appendChild(msgSpan);
        row.appendChild(timeSpan);
        el.pollList.prepend(row);
      }
      while (el.pollList.children.length > 20) {
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
  el.btnTogglePolling.textContent = state.pollingEnabled ? 'Pausar Polling' : 'Retomar Polling';
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
    el.clusterRing.textContent = `${data.ringBufferSize} / ${data.ringBufferCapacity}`;

    el.svgGw1Count.textContent = `${gw1} conexões`;
    el.svgGw2Count.textContent = `${gw2} conexões`;
    el.svgRingCount.textContent = `• Ring Buffer (${data.ringBufferSize}/${data.ringBufferCapacity})`;
    el.svgSeqCount.textContent = `#seqId atual: ${data.currentSeqId}`;

    el.sessionList.innerHTML = '';
    if (!data.sessions || data.sessions.length === 0) {
      const empty = document.createElement('div');
      empty.className = 'empty-state';
      empty.textContent = 'Nenhuma sessão ativa no momento.';
      el.sessionList.appendChild(empty);
      return;
    }

    for (const session of data.sessions) {
      const row = document.createElement('div');
      row.className = 'list-row';
      const left = document.createElement('span');
      left.textContent = session.userId;
      const right = document.createElement('span');
      right.className = 'pill pill-blue';
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
