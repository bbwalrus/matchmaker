# Distributed Competitive Matchmaker

## Backend layout

```text
cmd/server/              Process entry point and lifecycle
internal/
  httpapi/               HTTP routes, request validation, WebSocket endpoint
  matchmaking/           Redis queue operations and matching algorithm
  model/                 Shared API and domain types
  realtime/              WebSocket connection hub
```

Frontend:

```text
web/                     React + Vite dashboard
```

Recommended next additions as the system grows:

```text
internal/config/         Environment/config parsing
internal/persistence/    Optional Postgres match-history writer
internal/telemetry/      Metrics collection and metrics snapshots
deploy/                  Container and hosting configuration
```

## Run locally

Set `REDIS_URL` to a Redis connection URL, then run `go run ./cmd/server`.
The server listens on `:8080` by default; override it with `ADDR`.

`POST /api/queue/join` accepts `{"player_id":"p1","mmr":1200,"region":"na-west"}`.
`POST /api/queue/simulate` accepts `{"count":500,"region":"na-west","mean_mmr":1200,"stddev_mmr":250}`.
`GET /api/metrics?region=na-west` returns queue size, 500-point MMR buckets,
average wait, and an update timestamp. `GET /api/lobbies?region=na-west`
returns active lobbies for that region. The WebSocket endpoint is `/ws`; it
broadcasts formed match payloads with their lobby expiration timestamp.

Supported regions are `na-west`, `na-east`, and `eu-west`. Queue ticket keys are
derived from `player_id`, so callers should use a stable, unique ID per active
ticket and never pass sensitive user data as an ID.

## Run the dashboard locally

Start the Go server in one terminal, then from `web/` run `npm install` once
and `npm run dev`. Open the URL printed by Vite (normally
`http://localhost:5173`). The Vite development proxy forwards `/api`,
`/healthz`, and `/ws` to the Go server on port 8080, including the WebSocket
upgrade. For Vercel, set `VITE_API_BASE_URL` to the deployed Go backend origin
(for example, `https://your-service.onrender.com`) in the Vercel project
environment settings and redeploy. This is a public backend URL, not a secret.
On the backend host, set `FRONTEND_ORIGIN` to the exact deployed frontend origin
(for example, `https://your-project.vercel.app`) so browser API requests and
WebSocket upgrades from that site are allowed. Leave `VITE_API_BASE_URL`
unset for local development through the Vite proxy.

The dashboard supports queue joins, bot bursts, WebSocket status, a live queue
histogram, queue size, average wait, active lobby counts, and match cards with
five-minute expiration countdowns.

## Matching and atomicity

Each regional queue is a Redis sorted set whose score is MMR. Ticket hashes
store the join timestamp and lifecycle status. The Go worker discovers
candidate groups; a Lua script then rechecks queue membership, re-evaluates
the MMR range and each ticket's wait-based tolerance using Redis server time,
removes the tickets, marks them matched, and appends a Redis Stream event in a
single atomic operation. Competing workers may discover the same candidates,
but only one can claim them.

The current worker is intentionally a small single-process foundation.
`match:events` is the durable handoff point for a future Redis Streams
consumer; the in-process WebSocket hub alone does not broadcast across
multiple backend instances. The starter currently distributes ranked players
alternately between two teams; tune team balancing and matching rules with
simulation and tests before treating them as production policy. Candidate
discovery currently reads the regional queue snapshot on each worker tick; for
large pools, replace it with bounded MMR-range queries plus a wait-time index
before claiming high-throughput performance. The Lua script also assumes a
single Redis keyspace; Redis Cluster deployment needs region hash tags and all
script-touched keys passed through `KEYS` in the same slot.

Simulated bot tickets expire from the queue after five minutes. The matcher
cleans up expired bot ticket hashes and queue entries; the metrics endpoint
also performs cleanup before calculating queue size, MMR buckets, and average
wait. Real-player tickets do not expire automatically.

Active lobbies are stored with a five-minute Redis TTL and tracked in a
per-region sorted-set index. Expiration removes a lobby from the active-lobby
endpoint; it does not delete the corresponding entry from `match:events`.
The dashboard polls queue metrics and active lobbies every two seconds while
using WebSockets for immediate match events.

## Implementation milestones

1. **Foundation:** run the HTTP server and Redis; API and Redis-backed tests
   cover queue metrics, lobby expiry, retained history, and concurrent claims.
2. **Queue lifecycle:** define requeue/cancel semantics, ticket expiry and
   cleanup, idempotency rules, and a bounded candidate-discovery strategy for
   large queues.
3. **Realtime metrics:** queue size, MMR histogram, and average wait are
   available through the polling endpoint. Add ingestion rate, active sockets,
   and match latency, then publish snapshots and match events from Redis
   Streams so all backend instances can fan them out.
4. **Persistence:** add a bounded channel-backed writer for match history,
   explicit retry/backpressure behavior, and database migrations if history
   is needed.
5. **Dashboard:** the Vite control panel, polling histogram, lobby feed,
   expiry countdown, and WebSocket reconnect behavior are implemented. Refine
   telemetry and stale-state UX as the backend grows.
6. **Load and deployment:** benchmark 500+ ticket bursts, add race/load tests,
   container health checks, graceful shutdown, origin/auth policy, and
   deployment-specific Redis TLS/timeouts before deploying.
