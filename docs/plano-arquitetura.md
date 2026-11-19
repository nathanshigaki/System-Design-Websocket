# Plano de Projeto — Sistemas Distribuídos: System Design de Sala WebSocket Escalável (Docker)

## Goal Description

Implementar um **protótipo funcional de Sistema Distribuído** orquestrado com **Docker Compose** (`HAProxy` + `Redis` + `PostgreSQL` + múltiplos serviços `Node.js 24 ESM`), fundamentado diretamente nos dois vídeos de *System Design* de WebSocket:

1. **Vídeo 1** — *Websocket, SSE e Polling: Atualizações em tempo real para entrevistas (System Design #4)* (Pedro Camaforte): trade-offs entre Polling e WebSockets (requisições "fantasmas" vs baixa latência bidirecional), Load Balancer **Layer 4 vs Layer 7**, comunicação entre múltiplos servidores via **Redis Pub/Sub** e persistência para usuários offline.
2. **Vídeo 2** — *WebSockets em Escala: Design de Sistemas* (Code with Lucian): **Gateway Pattern** (*Dumb Gateways* + *Smart Workers*), **Distributed Session Manager**, balanceamento **L4 (*Least Connections*)** sem timeouts agressivos de HTTP, e recuperação de reconexão via **Sequence IDs + Ring Buffer** em memória para evitar *thundering herd* no banco de dados.

### Escopo Fechado e Entregáveis do Protótipo

* **Sala WebSocket única (`room:main`) multi-usuário:** múltiplos clientes conectados simultaneamente (abas no PC ou celulares da turma via **Cloudflare Quick Tunnel** `wss://.../ws`).
* **Notificações via Short Polling:** endpoint real `GET /api/notifications` com contagem de requisições totais vs requisições vazias ("fantasmas") para comparação prática com o WebSocket.
* **Mapa Visual Interativo de System Design & Arquitetura (Frontend):** painel gráfico moderno exibindo **todos os servidores, tecnologias, portas e rotas**, com **animação ao vivo de pacotes indo e voltando** a cada evento real (e botão de **Replay em Câmera Lenta** para a apresentação de 10 minutos).

*(Relatório do AVA e roteiro cronometrado de 10 minutos serão produzidos em etapa posterior.)*

---

## Arquitetura da Solução e System Design

![Arquitetura Distribuída - Sala WebSocket Escalável](./system-design-architecture.jpg)

### 1. Diagrama Topológico (Servidores, Tecnologias e Portas)

```mermaid
flowchart TB
    subgraph clients["Camada de Clientes e Exposicao Publica"]
        BRW["Navegadores / Celulares da Turma"]
        CF["Cloudflare Quick Tunnel (HTTPS / WSS - Opcional em Sala)"]
        BRW -->|"HTTPS / WSS"| CF
    end

    subgraph edge["Camada Edge e Balanceamento L4"]
        WEB["Web Server + WS Edge Proxy (Node.js 24 :3000)"]
        LB["HAProxy Load Balancer Layer 4 (mode tcp, leastconn :8080 / Stats :8404)"]
        CF -->|"HTTP/WS (:3000)"| WEB
        BRW -->|"Local HTTP/WS (:3000)"| WEB
        WEB -->|"/ws (1 socket TCP por cliente)"| LB
    end

    subgraph gateways["Camada de Conexao (Dumb Gateways - Sem Regra de Negocio)"]
        GW1["Dumb Gateway 1 (Node.js + ws :4001)"]
        GW2["Dumb Gateway 2 (Node.js + ws :4002)"]
        LB -->|"TCP Passthrough"| GW1
        LB -->|"TCP Passthrough"| GW2
    end

    subgraph backplane["Backplane Distribuido (Redis 7)"]
        RDS["Redis (Session Manager + Pub/Sub + Sequence ID + Ring Buffer + Fila :6380/6379)"]
        GW1 <-->|"Pub/Sub + Session + Ring Buffer"| RDS
        GW2 <-->|"Pub/Sub + Session + Ring Buffer"| RDS
        WEB <-->|"Cluster Status + Trace Stream"| RDS
    end

    subgraph processing["Camada de Negocio e Persistencia (Single-Writer)"]
        WK["Smart Worker (Node.js 24 - Regra de Negocio + seqId)"]
        PG["PostgreSQL 16 (Mensagens + Notificacoes Offline :5432)"]
        RDS -->|"BLPOP jobs:messages"| WK
        WK -->|"INCR seq + LPUSH Ring + PUBLISH"| RDS
        WK -->|"INSERT messages / notifications"| PG
        WEB -->|"SELECT /api/notifications (Short Polling)"| PG
        GW1 -.->|"SELECT Fallback Offline Longo"| PG
        GW2 -.->|"SELECT Fallback Offline Longo"| PG
    end
```

---

### 2. Fluxo das Rotas (Ida, Volta, Polling e Reconexão)

```mermaid
sequenceDiagram
    participant C1 as "Cliente A (Gateway 1)"
    participant WEB as "Web Edge (:3000)"
    participant LB as "HAProxy L4 (:8080)"
    participant GW1 as "Gateway 1 (:4001)"
    participant RDS as "Redis 7 (:6379)"
    participant WK as "Smart Worker"
    participant PG as "PostgreSQL 16 (:5432)"
    participant GW2 as "Gateway 2 (:4002)"
    participant C2 as "Cliente B (Gateway 2)"

    Note over C1,C2: Rota 1 - Conexao e Balanceamento L4 (Least Connections)
    C1->>WEB: "GET /ws (Upgrade: websocket)"
    WEB->>LB: "Abre TCP dedicado (:8080)"
    LB->>GW1: "TCP Passthrough (leastconn)"
    GW1->>RDS: "SET session:alice gateway-1 EX 45"
    GW1-->>C1: "welcome (gatewayId=gateway-1, seqId)"

    Note over C1,C2: Rota 2 - Envio e Broadcast na Sala WebSocket (Ida e Volta)
    C1->>GW1: "WS frame: chat_message"
    GW1->>RDS: "RPUSH jobs:messages"
    WK->>RDS: "BLPOP jobs:messages"
    WK->>RDS: "INCR seq:room:main -> LPUSH/LTRIM ring:room:main"
    WK->>PG: "INSERT INTO messages & notifications"
    WK->>RDS: "PUBLISH room:main (payload + seqId)"
    RDS-->>GW1: "Pub/Sub entrega para Gateway 1"
    RDS-->>GW2: "Pub/Sub entrega para Gateway 2"
    GW1-->>C1: "WS push (seqId)"
    GW2-->>C2: "WS push (seqId)"

    Note over C2,PG: Rota 3 - Notificacoes via Short Polling (HTTP)
    C2->>WEB: "GET /api/notifications?afterId=10"
    WEB->>PG: "SELECT id > 10 FROM notifications"
    WEB-->>C2: "200 OK (novidades ou array vazio)"

    Note over C2,PG: Rota 4 - Queda e Reconexao (Ring Buffer vs PostgreSQL)
    C2->>GW2: "Reconecta com ?lastSeenSeqId=12"
    GW2->>RDS: "LRANGE ring:room:main (busca seqId > 12)"
    alt "Dentro da janela do Ring Buffer (ultimas 50)"
        GW2-->>C2: "sync (source=ring_buffer, zero query no DB)"
    else "Offline longo (fora do Ring Buffer)"
        GW2->>PG: "SELECT seq_id > 12 FROM messages"
        GW2-->>C2: "sync (source=postgres_offline)"
    end
```

---

### 3. Inventário de Servidores, Tecnologias e Responsabilidades

| Servidor / Container | Imagem / Tecnologia | Porta | Protocolo | Papel na Arquitetura (Vídeos 1 e 2) |
| :--- | :--- | :--- | :--- | :--- |
| **`web`** | `node:24-alpine` (`node:http`, `ws`, `ioredis`, `pg`, HTML/CSS/JS + SVG) | `3000` | HTTP / WS Upgrade | Serve a UI animada, faz proxy de `/ws` para o HAProxy L4 (suporte a URL única + Cloudflare Tunnel), expõe `/ws-trace` e `/api/notifications`. |
| **`haproxy`** | `haproxy:2.9-alpine` | `8080` / `8404` | TCP L4 / HTTP Stats | **Load Balancer Layer 4 (`mode tcp`, `balance leastconn`)**. Roteia conexões brutas sem quebrar túneis WebSocket nem aplicar timeouts curtos de HTTP L7. |
| **`gateway-1`** | `node:24-alpine` (`ws`, `ioredis`, `pg`) | `4001` | WebSocket / TCP | **Dumb Gateway 1**. Mantém conexões WS, heartbeat/TTL no Session Manager e assina `room:main` no Redis. Zero regra de negócio. |
| **`gateway-2`** | `node:24-alpine` (`ws`, `ioredis`, `pg`) | `4002` | WebSocket / TCP | **Dumb Gateway 2**. Réplica idêntica para demonstrar escalabilidade horizontal, entrega cross-server e failover. |
| **`worker-1`** | `node:24-alpine` (`ioredis`, `pg`) | interna | Redis Queue / SQL | **Smart Worker**. Processa regras da sala, gera `seqId`, atualiza Ring Buffer e grava no PostgreSQL. Pode ser reiniciado sem derrubar sockets nos Gateways. |
| **`redis`** | `redis:7-alpine` | `6379` | RESP (TCP) | **Session Manager** (`session:*`), **Pub/Sub** (`room:main`, `trace:events`), **Sequence ID** (`seq:room:main`), **Ring Buffer** (`ring:room:main`) e **Fila** (`jobs:messages`). |
| **`postgres`** | `postgres:16-alpine` | `5432` | TCP / SQL | Persistência durável de mensagens (`messages`) e notificações (`notifications`), usada no Short Polling e fallback de offline longo. |

---

### 4. Catálogo Completo de Rotas e Canais

| Rota / Canal | Origem -> Destino | Método / Tipo | Descrição |
| :--- | :--- | :--- | :--- |
| `GET /` | Cliente -> `web` | HTTP | Entrega a interface web (Chat + Mapa Animado + Polling + Cluster). |
| `UPGRADE /ws` | Cliente -> `web` -> `haproxy:8080` | WebSocket (`ws://` / `wss://`) | Conexão da sala única (`?userId=...&lastSeenSeqId=...`). Cada cliente abre um socket TCP independente no L4. |
| `UPGRADE /ws-trace` | Cliente -> `web` | WebSocket | Stream exclusivo de telemetria (`trace_hop`) para animar as rotas das mensagens no SVG em tempo real. |
| `GET /api/notifications?afterId=N` | Cliente -> `web` -> `postgres` | HTTP (Short Polling) | Busca notificações mais recentes que `N`; retorna vazio quando não há novidade (contabiliza requisições "fantasmas"). |
| `GET /api/cluster` | Cliente -> `web` -> `redis` | HTTP JSON | Retorna sessões ativas por Gateway (`gateway-1` vs `gateway-2`), `seqId` atual e tamanho do Ring Buffer. |
| `http://localhost:8404` | Apresentador -> `haproxy` | HTTP (HAProxy Stats) | Painel nativo do HAProxy mostrando em tempo real conexões `leastconn` e saúde de `gateway-1` / `gateway-2`. |
| `RPUSH / BLPOP jobs:messages` | `gateway-*` -> `redis` -> `worker-1` | Redis List | Desacopla recepção de socket do processamento de negócio (*Gateway Pattern*). |
| `PUBLISH / SUBSCRIBE room:main` | `worker-1` -> `redis` -> `gateway-*` | Redis Pub/Sub | Entrega broadcast entre servidores para clientes em gateways diferentes. |
| `PUBLISH / SUBSCRIBE trace:events` | Todos os nós -> `redis` -> `web` | Redis Pub/Sub | Envia saltos reais (`from -> to`, protocolo, tempo) para animação ao vivo no frontend. |

---

## User Review Required

> [!IMPORTANT]
> **Pré-requisito para rodar o projeto:**
> Com a inclusão do Docker, você precisará apenas ter o **Docker Desktop** aberto/rodando na máquina para executar `docker compose up --build` (e, opcionalmente na apresentação em sala, `cloudflared tunnel --url http://localhost:3000` para a turma conectar pelos celulares).

---

## Proposed Changes

### Infraestrutura Docker & Banco de Dados

#### [NEW] `docker-compose.yml`
Orquestra os 7 serviços (`postgres`, `redis`, `haproxy`, `gateway-1`, `gateway-2`, `worker-1`, `web`) em rede interna `pulsews-net`, com `healthcheck` em `postgres` e `redis`, volume persistente `pgdata` e mapeamento das portas `3000` (Web + `/ws`), `8080` (HAProxy L4 direto) e `8404` (HAProxy Stats).

#### [NEW] `haproxy/haproxy.cfg`
Configuração oficial **Layer 4 (`mode tcp`)**:
- `frontend ws_frontend`: `bind *:8080`
- `backend ws_gateways`: `mode tcp`, `balance leastconn`, `option tcp-check`, `server gateway-1 gateway-1:4001 check inter 3s fall 2 rise 2`, `server gateway-2 gateway-2:4002 check inter 3s fall 2 rise 2`
- `listen stats`: `bind *:8404`, `mode http`, `stats enable`, `stats uri /`

#### [NEW] `db/init.sql`
Cria as tabelas no PostgreSQL:
- `messages`: `id SERIAL PRIMARY KEY`, `seq_id BIGINT UNIQUE NOT NULL`, `user_id VARCHAR(64) NOT NULL`, `content TEXT NOT NULL`, `gateway_id VARCHAR(32) NOT NULL`, `created_at TIMESTAMPTZ DEFAULT NOW()`.
- `notifications`: `id SERIAL PRIMARY KEY`, `type VARCHAR(32) NOT NULL`, `user_id VARCHAR(64) NOT NULL`, `message TEXT NOT NULL`, `created_at TIMESTAMPTZ DEFAULT NOW()`.

---

### Pacote Compartilhado (`src/shared/`)

#### [NEW] `src/shared/constants.js`
Define chaves Redis (`session:{userId}`, `seq:room:main`, `ring:room:main`, `jobs:messages`, `room:main`, `trace:events`) e constantes (`RING_BUFFER_SIZE = 50`, `SESSION_TTL_SEC = 45`).

#### [NEW] `src/shared/trace.js`
Helper para publicar eventos padronizados em `trace:events` (`{ traceId, flow, step, from, to, protocol, detail, ts }`), permitindo rastrear na UI cada salto real de mensagens WebSocket, Polling e Reconexão.

---

### Serviços Node.js (`src/gateway/`, `src/worker/`, `src/web/`)

#### [NEW] `src/gateway/Dockerfile` & `src/gateway/package.json` & `src/gateway/src/index.js`
**Dumb Gateway** (`ws`, `ioredis`, `pg`):
- Aceita `?userId=...&lastSeenSeqId=...` no upgrade WebSocket.
- Registra sessão no Redis (`SET session:{userId} {GATEWAY_ID} EX 45`) e renova no heartbeat (`ping`/`pong`).
- Na reconexão com `lastSeenSeqId >= 0`: consulta `LRANGE ring:room:main 0 -1`; se o `lastSeenSeqId` ainda estiver coberto pelo Ring Buffer, devolve `sync` com `recoverySource: "ring_buffer"`; caso contrário, consulta o PostgreSQL (`SELECT ... WHERE seq_id > $1`) e devolve `recoverySource: "postgres_offline"`.
- Envia comandos recebidos dos clientes para `RPUSH jobs:messages` e assina `room:main` para entregar aos sockets locais.

#### [NEW] `src/worker/Dockerfile` & `src/worker/package.json` & `src/worker/src/index.js`
**Smart Worker** (`ioredis`, `pg`):
- Consome `jobs:messages` via `BLPOP` (sem derrubar conexões de clientes se o container for reiniciado!).
- Para `chat`: incrementa `seq:room:main`, grava no `ring:room:main` (`LPUSH` + `LTRIM 0 49`), persiste em `messages` e `notifications` no PostgreSQL e publica em `room:main`.
- Para `user_joined` / `user_left`: grava notificação no PostgreSQL para o Short Polling capturar e emite trace.

#### [NEW] `src/web/Dockerfile` & `src/web/package.json` & `src/web/src/index.js`
**Web Edge Server** (`node:http`, `ws`, `ioredis`, `pg`):
- Serve `public/` na porta `3000`.
- Proxy TCP no evento `upgrade` para `/ws` -> `haproxy:8080` (mantendo 1 conexão TCP por cliente para o `leastconn` e compatibilidade total com `wss://<tunnel>.trycloudflare.com/ws`).
- WebSocket `/ws-trace` repassando `trace:events` do Redis para o diagrama animado.
- `GET /api/notifications?afterId=0`: Short Polling no PostgreSQL + emissão de trace de Polling.
- `GET /api/cluster`: lê chaves `session:*`, `seq:room:main` e tamanho de `ring:room:main` no Redis.

---

### Frontend Visual Bonito & Animado (`src/web/public/`)

#### [NEW] `src/web/public/index.html`
#### [NEW] `src/web/public/styles.css`
#### [NEW] `src/web/public/app.js`
Interface moderna (tema escuro estilo painel de engenharia/observabilidade) dividida em:
1. **Painel Principal — Mapa Vivo da Arquitetura & Rotas (SVG Interativo):**
   - Exibe cada servidor com nome, **tecnologia** (`Node.js`, `HAProxy L4`, `Redis 7`, `PostgreSQL 16`), **porta** e **badge de status/contagem**.
   - Anima pacotes coloridos percorrendo exatamente o caminho de ida e volta pelas setas SVG conforme eventos reais chegam de `/ws-trace`:
     - **Azul:** Rota WebSocket (`Cliente -> Web/Proxy -> HAProxy L4 -> Gateway -> Redis Queue -> Worker -> PostgreSQL + Ring Buffer -> Redis Pub/Sub -> Gateways 1 e 2 -> Clientes`).
     - **Âmbar:** Rota Short Polling (`Cliente -> Web -> PostgreSQL -> Cliente`).
     - **Verde:** Rota Reconexão (`Ring Buffer` ou fallback `PostgreSQL`).
   - Botão **"Replay Última Rota (Câmera Lenta)"** + painel lateral com o log passo a passo da rota (`traceId`).
2. **Sala WebSocket (`room:main`) + Simulador de Falhas:**
   - Conexão por nome de usuário, exibição do Gateway atribuído (`gateway-1` vs `gateway-2`), chat em tempo real com `#seqId`, botão **"Abrir Nova Aba"** e botão **"Simular Queda / Reconectar"** (mostrando se recuperou do `Ring Buffer` ou `PostgreSQL`).
3. **Painel de Notificações (Short Polling) & Cluster Map:**
   - Feed de notificações via Polling (`2.5s`) com contadores de **requisições totais** vs **requisições vazias ("fantasmas")**.
   - Tabela ao vivo de quem está conectado em `gateway-1` vs `gateway-2` segundo o *Distributed Session Manager* do Redis.

---

### Scripts de Teste & Documentação

#### [NEW] `package.json` (raiz) & `scripts/load-test.mjs`
- `npm test` executa `scripts/load-test.mjs` contra `http://localhost:3000` / `ws://localhost:3000/ws`, validando balanceamento L4 (`gateway-1` e `gateway-2`), broadcast cross-gateway, `seqId` crescente, replay no Ring Buffer, Short Polling e eventos em `/ws-trace`.

#### [NEW] `README.md`
Guia rápido de comandos (`docker compose up -d --build`, `docker compose restart worker-1`, `docker compose stop gateway-1`, `cloudflared tunnel --url http://localhost:3000`, `npm test`) e acesso ao HAProxy Stats (`http://localhost:8404`).

---

## Verification Plan

### Automated Tests
1. Subir todos os containers:
   ```bash
   docker compose up -d --build
   ```
2. Verificar saúde dos 7 serviços:
   ```bash
   docker compose ps
   ```
3. Instalar `ws` na raiz para o script de teste e executar:
   ```bash
   npm install
   npm test
   ```
   Validando distribuição L4 `leastconn` entre `gateway-1` e `gateway-2`, troca de mensagens cross-gateway, ordenação por `seqId`, replay via `ring_buffer`, endpoint `/api/notifications` e stream `/ws-trace`.
4. Testar resiliência do *Gateway Pattern* (Worker reinicia sem derrubar sockets):
   ```bash
   docker compose restart worker-1
   ```

### Manual Verification
1. Abrir `http://localhost:3000` em 2–3 abas (`Alice`, `Bob`, `Carol`) e confirmar a distribuição entre `gateway-1` e `gateway-2` (também visível em `http://localhost:8404`).
2. Enviar mensagens no chat e acompanhar no **Mapa Animado** o pacote percorrendo todos os servidores e voltando pelos dois gateways; testar o botão **"Replay Última Rota (Câmera Lenta)"**.
3. Observar as notificações no painel **Short Polling** e o contador de requisições vazias ("fantasmas").
4. Testar **"Simular Queda / Reconectar"** e conferir o badge de recuperação pelo **Ring Buffer**.
5. *(Para demonstração em sala)* Testar `cloudflared tunnel --url http://localhost:3000` e abrir a URL gerada no celular para confirmar interação no chat e animação ao vivo via `wss://.../ws`.
