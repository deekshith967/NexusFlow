# NexusFlow

**NexusFlow is a durable workflow orchestration engine built on NestJS and PostgreSQL.** Workflow definitions, versions, runs, task executions, attempts, leases, and fencing tokens are persisted in PostgreSQL, which is the authoritative store. A transactional outbox makes event publication intents durable; a scheduler claims eligible tasks with row locks, leases, and fencing tokens; workers execute deterministic built-in handlers; and an outbox relay publishes events to the message transport (Kafka in production, in-memory in local demos and tests).

> **Status: MVP (vertical slice, all phases A–G implemented and tested).** The full lifecycle — workflow identity, draft versions, DAG validation, publication, run creation, scheduling, worker execution with retries, lease expiry/crash recovery, and the HTTP control plane — is implemented and covered by unit and real-PostgreSQL integration tests. See [Limitations](#limitations) for what is explicitly **not** verified (in particular: no real Kafka broker has been exercised).

## Goals

- Immutable, statically defined directed acyclic graphs (DAGs) of tasks.
- Durable state transitions backed by PostgreSQL (authoritative state).
- Concurrent task claiming with `FOR UPDATE SKIP LOCKED`, leases, and fencing tokens so stale workers can never mutate current state.
- Task attempts, retry handling, dependency activation, and workflow completion.
- A transactional outbox for reliable event publication intent, with Kafka used for asynchronous dispatch and event transport in production.
- Explicit behavior for duplicate delivery, crashes, and recovery.

NexusFlow does **not** claim exactly-once task execution or exactly-once external side effects. Delivery is at-least-once; duplicate suppression is by deterministic event/task identity and conditional state updates. The full intended design and invariants live in [`docs/architecture/final-architecture-contract.md`](docs/architecture/final-architecture-contract.md).

## Features (implemented and tested)

- **Workflow identity** (Phase A): tenant-scoped workflow creation with generated IDs, blank-name validation, tenant/name uniqueness enforced by the PostgreSQL unique index (concurrent inserts handled — only genuine unique violations map to a domain conflict), and a stable `active` status.
- **Draft versions and publication** (Phase B): draft task/dependency definitions on the existing schema, reuse of the DAG validator (duplicate keys, unknown dependencies, self-dependencies, cycles), publication inside a single transaction with the `workflow.version.published` outbox event, published-version immutability, and version retirement.
- **Run creation** (Phase C): atomic creation of a run plus all initial task executions from a PUBLISHED version, idempotency-key support (replay returns the original run), rejection of invalid launches (unknown/retired version, no published version), tenant isolation.
- **Transactional outbox + relay** (Phase D): outbox rows committed atomically with state changes; the relay claims rows with `SKIP LOCKED` + claim TTL, publishes **outside** the transaction with a bounded per-publish timeout (`RELAY_PUBLISH_TIMEOUT_MS`), marks published conditionally (owner-checked), records failures only while still owning the claim, and is crash-safe (expired claims are reclaimed).
- **Scheduler** (Phase D): claims `READY`, due `RETRY_WAIT`, and lease-expired tasks of `RUNNING` runs in one atomic transaction — lease owner/expiry, `fencing_token` increment, attempt creation, input resolution, and `task.claimed` + `task.dispatch.requested` outbox events together. Multiple concurrent schedulers are safe.
- **Worker** (Phase E): consumes dispatches, validates eligibility (state, attempt, fencing token, live lease) before doing anything, transitions to `RUNNING`, renews leases conditionally, executes only **deterministic built-in handlers** (`echo`, `sleep`, `fail`, `merge` — no untrusted code), and reports success/failure through conditional updates that reject stale completions (zero affected rows ⇒ stale, no mutation).
- **Retry and failure semantics** (Phase E): retryable failures with `maxAttempts`/`retryDelayMs`, `FAIL_WORKFLOW` and `SKIP_DOWNSTREAM` failure policies, `BLOCKED` propagation for downstream of failed tasks, fan-in (all prerequisites) activation, and the exact workflow completion algorithm (B.5) with `workflow.run.succeeded` / `workflow.run.failed` events.
- **Crash recovery** (contract G): scheduler crash before/after commit (outbox is the source of dispatch truth), worker crash mid-execution (lease expiry + fencing-token rejection of the stale worker), relay crash after publication (idempotent re-marking), Kafka-outage behavior (outbox accumulates; no dispatches lost). All covered by real-PostgreSQL integration tests.
- **HTTP control plane** (Phase F): create/inspect workflows, draft versions (create/update/validate/publish/retire), start runs (with idempotency), inspect runs/tasks/attempts, health + readiness, API-key authentication with server-side tenant binding (`X-Tenant-Id` optional, must match), stable status codes (201/200/404/409/422/400/401/403), and no raw database diagnostics in responses.
- **Operations** (Phase G): role-gated single application (`api`, `scheduler`, `worker`, `outbox-relay`, `all`), fail-fast PostgreSQL check at startup, health/readiness endpoints, graceful shutdown (the outbox relay fully drains its in-flight publish cycle before its shutdown hook returns, so it never uses the pool or transport after shutdown; loops/transport/pool are then torn down), idempotent migration applier, and an executable end-to-end demo (`npm run demo`).

## Technology stack

- TypeScript (strict, `commonjs`)
- Node.js / npm (pinned to Node 22.22.0 via `.node-version`, `package.json` `engines`, and the `Dockerfile`; the demo uses the built-in `fetch`)
- NestJS 11
- PostgreSQL (`pg`) — authoritative state; migrations applied via an idempotent, tracked applier (`schema_migrations`)
- Kafka (`kafkajs`) — production transport for dispatch + lifecycle events (idempotent producer, consumer group, topic bootstrap)
- In-memory message transport — clearly-labeled test/local double that implements the same transport interface (used by `NEXUSFLOW_ROLE=all` + `NEXUSFLOW_TRANSPORT=memory` and by integration tests). It is **not** a Kafka integration and provides no cross-process delivery.
- Jest + ts-jest (unit and integration), ESLint
- `embedded-postgres` (dev dependency) — starts a real PostgreSQL server for integration tests; no external infrastructure needed for `npm test`.

## Repository layout

```text
.
├── AGENTS.md
├── Dockerfile                # multi-stage production image (all roles)
├── render.yaml               # Render blueprint: managed PG + one service per role
├── docs/
│   ├── architecture/final-architecture-contract.md
│   └── adr/                  # architecture decision records
├── scripts/
│   └── e2e-demo.ts           # executable end-to-end demo (npm run demo)
├── src/
│   ├── app.module.ts         # role-gated single application
│   ├── main.ts               # bootstrap: DB check, HTTP for api/all roles
│   ├── migrate.ts            # migration CLI (npm run migrate / migrate:prod)
│   ├── config/               # env parsing + validation (loadEnvConfig)
│   ├── database/             # Pool provider, DatabaseService, migrations + applier
│   ├── outbox/               # outbox event types, repository, relay module/service
│   ├── scheduler/            # atomic claim transaction + scan loop
│   ├── worker/               # dispatch consumer, lease handling, completion, built-in handlers
│   ├── transport/            # MessageTransport interface, Kafka + in-memory implementations
│   ├── workflow/
│   │   ├── application/      # WorkflowService, RunService (transactional domain operations)
│   │   ├── domain/           # DAG validator, failure policies, version state, errors
│   │   ├── persistence/      # row shapes
│   │   └── api/              # HTTP controllers, DTO validation, exception filter, health
│   └── types/                # minimal ambient declarations
├── test/
│   ├── unit/                 # domain + mocked-kafkajs transport tests
│   ├── integration/          # REAL PostgreSQL: lifecycle, scheduler, worker, relay, HTTP API, E2E
│   └── helpers/              # embedded-postgres test server, factories, engine harness
└── .env.example
```

## Prerequisites

- Node.js 22.22.x and npm (exact pin in `.node-version`; `engines` requires `>=22.22.0 <23.0.0`)
- A reachable **PostgreSQL** database (any version the `pg` driver supports; the test suite proves PostgreSQL 18)
- **Kafka** — only when running `NEXUSFLOW_TRANSPORT=kafka` (i.e., any multi-process deployment). Not needed for tests or the single-process demo.

No other infrastructure is required; integration tests spin up their own throwaway PostgreSQL server.

## Install

```bash
git clone https://github.com/deekshith967/NexusFlow.git
cd NexusFlow
npm ci
```

## Migrations

Migrations are idempotent and tracked in `schema_migrations`:

```bash
export DATABASE_URL='postgres://user:password@127.0.0.1:5432/nexusflow'
npm run migrate          # development: runs the TypeScript source via ts-node
```

From compiled output (production, no dev dependencies needed — the
migration SQL is packaged into `dist/` by the build):

```bash
npm run build
npm run migrate:prod     # runs node dist/migrate.js
```

Run migrations once per deploy, from a single process: concurrent
migrates are not coordinated. The migrator honors `DATABASE_SSL` /
`sslmode=require`, so it works against managed PostgreSQL that
requires TLS. `npm run demo` also applies migrations automatically.

`003_immutability_guards.sql` adds database-level guards (ADR 003):
published/retired/discarded versions and their task graph, terminal
runs, task executions and attempts, and published outbox events cannot
be modified, and fencing tokens cannot decrease. Violations raise
SQLSTATE `NF001`. The application never triggers them; they catch
defects and manual SQL.

## Configuration

Copy `.env.example` to `.env` (or export the variables) and adjust. Key variables:

| Variable | Default | Meaning |
| --- | --- | --- |
| `DATABASE_URL` | — (required) | PostgreSQL connection string |
| `DATABASE_SSL` | auto-detect | `true` \| `false`; when unset, TLS is auto-detected from `sslmode`/`ssl` parameters in `DATABASE_URL`. Required (`true`) for managed PostgreSQL that mandates TLS. |
| `NEXUSFLOW_API_KEYS` | — (required for `api`/`all`) | Comma-separated `name:secret:tenant-UUID` entries. Each key binds its bearer to one tenant; requests send `Authorization: Bearer <secret>`. Secrets >= 16 chars, unique per key, never committed. |
| `NEXUSFLOW_ROLE` | `api` | `api` \| `scheduler` \| `worker` \| `outbox-relay` \| `all` |
| `NEXUSFLOW_TRANSPORT` | `memory` | `kafka` \| `memory` (memory is rejected for the separate `worker` and `outbox-relay` processes) |
| `PORT` | `3000` | HTTP port for `api`/`all` roles |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka brokers (kafka transport only) |
| `KAFKA_SSL` | `false` | `true` \| `false`. Required (`true`) for managed brokers; mandatory when SASL is configured. |
| `KAFKA_SASL_MECHANISM` / `_USERNAME` / `_PASSWORD` | — (unset) | Broker auth: `plain` (Confluent Cloud) or `scram-sha-256` / `scram-sha-512` (Upstash, Redpanda) + credentials. All three set together, with `KAFKA_SSL=true`; partial or plaintext-SASL config is a boot error. |
| `NEXUSFLOW_DEFAULT_TENANT_ID` | `00000000-0000-0000-0000-000000000000` | Informational default echoed in `/health/ready` (must be a UUID). API identity comes from `NEXUSFLOW_API_KEYS`; the key's tenant always wins. |
| `SCHEDULER_POLL_MS` / `SCHEDULER_BATCH_LIMIT` / `NEXUSFLOW_LEASE_MS` | `250` / `20` / `30000` | Scheduler tuning |
| `RELAY_POLL_MS` / `RELAY_BATCH_LIMIT` / `RELAY_CLAIM_TTL_MS` / `RELAY_PUBLISH_TIMEOUT_MS` | `250` / `20` / `30000` / `30000` | Outbox relay tuning (publish timeout bounds each broker call and the shutdown drain) |

## Running

### Single-process local demo (recommended first step)

Zero prerequisites beyond Node.js and npm — boots a throwaway
PostgreSQL, runs the full lifecycle over HTTP, and exits (the first
run downloads a ~50 MB PostgreSQL binary):

```bash
npm run demo:local
```

With your own PostgreSQL instead, start a `role=all` process
(API + scheduler + worker + relay in one process, in-memory
transport):

```bash
export DATABASE_URL='postgres://user:password@127.0.0.1:5432/nexusflow'
export NEXUSFLOW_ROLE=all
export NEXUSFLOW_TRANSPORT=memory
# Required: at least one key (generate real secrets with: openssl rand -hex 24).
export NEXUSFLOW_API_KEYS='local:change-me-use-openssl-rand-hex:00000000-0000-0000-0000-000000000000'
npm run start:prod        # or: npm run start:dev
```

Or run the same scripted demo against your own database (applies migrations automatically, exits non-zero unless the run succeeds):

```bash
export DATABASE_URL='postgres://user:password@127.0.0.1:5432/nexusflow'
npm run demo
```

Demo output shows: workflow creation → draft version → validation → publication (with definition hash) → run start (+ idempotent replay) → scheduler/worker execution where one task deterministically fails twice and succeeds on attempt 3 → persisted task executions and attempt history.

### Multi-process deployment (production shape)

Requires PostgreSQL **and** Kafka:

```bash
export DATABASE_URL='postgres://user:password@host:5432/nexusflow'
export NEXUSFLOW_TRANSPORT=kafka
export KAFKA_BOOTSTRAP_SERVERS='host1:9092,host2:9092'
# Required for the api role (parsed everywhere, enforced for api/all):
export NEXUSFLOW_API_KEYS='<name>:<secret>=16 chars:<tenant-UUID>[,...]'
# Managed brokers (all require SASL_SSL):
export KAFKA_SSL=true
export KAFKA_SASL_MECHANISM=scram-sha-256   # or plain for Confluent Cloud
export KAFKA_SASL_USERNAME='<broker-username>'
export KAFKA_SASL_PASSWORD='<broker-password>'

# Terminal 1 — API
NEXUSFLOW_ROLE=api node dist/main.js
# Terminal 2 — scheduler (one or more)
NEXUSFLOW_ROLE=scheduler node dist/main.js
# Terminal 3 — worker (one or more)
NEXUSFLOW_ROLE=worker node dist/main.js
# Terminal 4 — outbox relay (one or more)
NEXUSFLOW_ROLE=outbox-relay node dist/main.js
```

Run `npm run build` first. All roles share the same codebase; roles are selected at bootstrap.

## Deploying

### Docker

The multi-stage `Dockerfile` builds a production image (compiled `dist/`
plus production-only dependencies; no `ts-node`, no dev dependencies).
The same image serves every role:

```bash
docker build -t nexusflow .
docker run --rm -e DATABASE_URL='...' nexusflow node dist/migrate.js
docker run -d -p 3000:3000 -e DATABASE_URL='...' -e NEXUSFLOW_ROLE=api nexusflow
# ... one container per role (api, scheduler, worker, outbox-relay),
# all with NEXUSFLOW_TRANSPORT=kafka and KAFKA_BOOTSTRAP_SERVERS set.
```

> Note: the `Dockerfile` is provided as reviewed, unexecuted
> infrastructure — no Docker daemon is available in this environment, so
> `docker build` has not been run here. Validate it in CI before
> depending on it.

### Render

`render.yaml` is a Render Blueprint provisioning managed PostgreSQL plus
one service per NexusFlow role (web `api` with `GET /health/ready` as
the health check; background workers for `scheduler`, `worker`, and
`outbox-relay`). Notes:

- Set `KAFKA_BOOTSTRAP_SERVERS` in the dashboard to an **external**
  broker (Render has no managed Kafka). Multi-process deployments
  require Kafka; the `worker` and `outbox-relay` roles refuse to boot
  with `NEXUSFLOW_TRANSPORT=memory`.
- Set `NEXUSFLOW_API_KEYS` (dashboard-prompted,
  `name:secret:tenant-UUID` entries) on the `api` service — it refuses
  to boot without keys. Health checks need no credentials
  (`/health/ready` is public).
- Set the `KAFKA_SASL_MECHANISM` / `_USERNAME` / `_PASSWORD` trio
  (dashboard-prompted) on `worker` and `relay`: `plain` for Confluent
  Cloud, `scram-sha-256` (or `-512`) for Upstash/Redpanda.
  `KAFKA_SSL=true` is already set — SASL requires TLS.
- Migrations run once per deploy via the `api` service's
  `preDeployCommand` (`npm run migrate:prod`). Do not add it to the
  other services — concurrent migrates are not coordinated.
- Plans: only the `api` web service and the database use the Free
  plan. Background workers have no free tier, so `scheduler`,
  `worker`, and `relay` use `0.5c-512mb` (the cheapest valid worker
  plan). The free web service sleeps after 15 minutes without traffic
  (~1 minute to wake); use a paid plan for an always-on API.
- TLS: `DATABASE_SSL` is deliberately **not** set. The blueprint
  injects Render's **internal** database URL, which uses the private
  network where TLS is optional — forcing TLS would crash every
  service at boot when the server does not offer it (the `pg` client
  fails closed instead of falling back to plaintext). TLS remains
  available for external or TLS-mandating endpoints via
  `DATABASE_SSL=true` or `?sslmode=require`.
- Shutdown grace: `relay` gets `maxShutdownDelaySeconds: 120` so its
  bounded in-flight drain (at most one `RELAY_PUBLISH_TIMEOUT_MS`
  publish plus overhead) completes instead of relying on claim-expiry
  recovery; `worker` gets `60` as headroom for consumer disconnect
  and in-flight completion writes (in-flight handlers are not
  awaited — lease expiry reclaims them either way). The API and
  scheduler keep the 30s default.

Free PostgreSQL limits (per [Render's docs](https://render.com/docs/free)):
only one free database per workspace, fixed 1 GB storage, **expiry 30
days after creation** (an expired database is inaccessible unless
upgraded), a 14-day grace period to upgrade before Render **deletes**
it with all its data, no backups, no managed connection pooling, and
possible maintenance/restarts at any time. Upgrade path: upgrade the
database to any paid compute plan from the Render Dashboard **before
the grace period ends** — the data stays in place. (Changing a
database's compute plan needs brief downtime.) Do not run production
data on the free database.

> Note: the blueprint has been syntax/structure-validated but not
> applied to a Render account from here — no credentials or live
> service are claimed. The first real deploy remains the verification
> step for the Render path (and for Kafka-backed multi-process
> operation in general — see [Limitations](#limitations)).

### Zero-cost demo options

Ways to run the engine for $0, most reliable first:

1. **`npm run demo:local` (verified, recommended).** $0 forever,
   zero prerequisites beyond Node.js: throwaway PostgreSQL plus
   the complete engine in one process (`role=all` runs the API,
   scheduler, worker, and relay). Limitations: single-process only
   (in-memory transport, no cross-process delivery), ephemeral
   database, fixed local-only demo secret.
2. **Local Docker, one container per role ($0, your machine).**
   Full production shape (separate api/scheduler/worker/relay plus
   local PostgreSQL and local Kafka) with no hosting bill — but the
   `Dockerfile` is reviewed, unexecuted infrastructure (no Docker
   daemon here), and you must provide the Kafka broker yourself.
3. **Hosted single-process demo (researched, NOT verified): $0.**
   One Render Free web service running `role=all`, plus a free
   Neon Postgres (no expiry, scales to zero) and a free Upstash
   Kafka tier (SASL_SSL, supported by the client). Hard
   limitations: the free web service **sleeps after 15 minutes
   idle and all in-process loops stop with it**; Render Free does
   not support **`preDeployCommand`**, so migrations must run at
   container startup (safe only with a single instance — never
   copy that pattern to multi-instance services); Neon
   scale-to-zero adds wake latency and requires TLS (supported);
   free Kafka quotas must be re-verified against current vendor
   limits. Demo-grade availability only.

There is **no suitable free hosted full multi-process
deployment**: Render has no free tier for background workers, so
the scheduler/worker/relay cannot each run as a separate
always-on service for $0. `render.yaml` stays the honest
production blueprint (paid worker plans) and is unchanged by
these demo options.

## HTTP API

Every workflow/version/run endpoint requires `Authorization: Bearer <api-key-secret>` (see `NEXUSFLOW_API_KEYS`). Identity is the key's tenant, bound server-side: the `X-Tenant-Id` header (UUID) is optional and, when present, must match the authenticated tenant. Missing/invalid credentials → 401, mismatched tenant header → 403, malformed header → 422. `/health` and `/health/ready` stay public for platform health checks.

| Method & path | Purpose | Success | Errors |
| --- | --- | --- | --- |
| `POST /workflows` | Create workflow identity (`name`, optional `description`) | 201 | 409 duplicate name in tenant, 422 invalid body |
| `GET /workflows` | List workflows for the tenant | 200 | 422 invalid tenant |
| `GET /workflows/:workflowId` | Inspect a workflow | 200 | 404 |
| `POST /workflows/:workflowId/versions` | Create draft version from `tasks[]` (validated: DAG rules, registered handler type/version, retry policy, timeout, failure policy) | 201 | 422 |
| `GET /workflows/:workflowId/versions` | List versions | 200 | — |
| `GET /versions/:workflowVersionId` | Version + tasks + dependencies | 200 | 404 |
| `PUT /versions/:workflowVersionId/definition` | Replace draft definition (drafts only) | 200 | 404, 409 published/retired, 422 |
| `POST /versions/:workflowVersionId/validate` | Validation report `{ valid, errors }` | 200 | 404 |
| `POST /versions/:workflowVersionId/publish` | Publish (atomic with outbox event) | 200 | 404, 409 invalid transition, 422 invalid DAG |
| `POST /versions/:workflowVersionId/retire` | Retire a published version | 200 | 404, 409 |
| `POST /workflows/:workflowId/runs` | Start run (`workflowVersionId?`, `input?`, `idempotencyKey?`) | 201 new / 200 idempotent replay | 404 unknown (or another tenant's) workflow/version, 422 launch rejected |
| `GET /runs` | List runs (`workflowId?`, `limit?` 1–200) | 200 | 400 bad limit |
| `GET /runs/:workflowRunId` | Run + task executions | 200 | 404 |
| `GET /runs/:workflowRunId/tasks` | Task executions | 200 | 404 |
| `GET /runs/:workflowRunId/tasks/:taskExecutionId/attempts` | Attempt history | 200 | 404 (also when the task is not in that run) |

Path identifiers (`:workflowId`, `:workflowVersionId`, `:workflowRunId`, `:taskExecutionId`) and the `workflowId` query parameter must be UUIDs; anything else is a 400.
| `GET /health` | Liveness | 200 | — |
| `GET /health/ready` | Readiness (checks PostgreSQL) | 200 `ready` / 503 `not_ready` (database down) | — |

Task body shape:

```json
{
  "taskKey": "transform",
  "handlerType": "fail",
  "handlerVersion": "1",
  "configuration": { "failUntilAttempt": 3 },
  "inputMapping": {},
  "outputMapping": {},
  "retryPolicy": { "maxAttempts": 3, "retryDelayMs": 500 },
  "timeoutMs": 10000,
  "failurePolicy": "FAIL_WORKFLOW",
  "dependencies": ["fetch"]
}
```

Built-in handlers: `echo` (v1), `sleep` (v1, `configuration.ms`), `fail` (v1, `configuration.failUntilAttempt` — deterministic retry testing), `merge` (v1, `configuration.with`).

Error responses are stable envelopes `{ statusCode, error, message }`; raw database/driver diagnostics are never returned.

### Example session

```bash
export BASE=http://127.0.0.1:3000
export TENANT=$NEXUSFLOW_DEFAULT_TENANT_ID
export APIKEY='<one secret from NEXUSFLOW_API_KEYS>'
H='-H content-type:application/json -H x-tenant-id:'"$TENANT"' -H authorization:Bearer '"$APIKEY"

curl -sS $H -d '{"name":"orders"}' $BASE/workflows
# -> {"workflow_id":"...","name":"orders","status":"active",...}

WF=<workflow_id from above>
curl -sS $H -d '{"tasks":[
  {"taskKey":"fetch","handlerType":"echo","handlerVersion":"1","timeoutMs":10000,"failurePolicy":"FAIL_WORKFLOW"},
  {"taskKey":"store","handlerType":"merge","handlerVersion":"1","configuration":{"with":{"stored":true}},"timeoutMs":10000,"failurePolicy":"FAIL_WORKFLOW","dependencies":["fetch"]}
]}' $BASE/workflows/$WF/versions
VER=<workflow_version_id from above>

curl -sS $H $BASE/versions/$VER/validate -X POST -d '{}'
curl -sS $H $BASE/versions/$VER/publish -X POST -d '{}'
curl -sS $H -d '{"idempotencyKey":"order-42","input":{"order":42}}' $BASE/workflows/$WF/runs
RUN=<workflow_run_id from above>

curl -sS $H $BASE/runs/$RUN            # run + task executions
curl -sS $H $BASE/runs/$RUN/tasks
```

## Testing

```bash
npm test            # full suite, serial (jest --runInBand)
npm run test:watch
npm run test:cov
npm run typecheck
npm run lint
npm run build
```

CI (`.github/workflows/main.yml`) runs the full suite, lint, typecheck, and build on every push/PR to `master`, plus a `demo` job that builds, runs the compiled migrations, and executes `npm run demo` against a PostgreSQL service.

Suite overview (at the current HEAD):

| Suite | Type | What it proves |
| --- | --- | --- |
| `test/workflow/workflow-domain.spec.ts` | unit | DAG validation, version state machine, failure policies |
| `test/unit/env.spec.ts` | unit | env parsing/validation: defaults, DATABASE_URL, role/transport, memory guard for worker + outbox-relay, tenant UUID, tuning ints, DATABASE_SSL explicit/auto-detect/override, Render internal-URL shape, pool/check consistency, API keys, Kafka security |
| `test/unit/api-key-auth.spec.ts` | unit | constant-time key lookup, Bearer guard (missing/malformed/valid), server-side tenant binding (match/403 mismatch/422 malformed/fail-closed) |
| `test/unit/health.controller.spec.ts` | unit | liveness plus the readiness status contract: 200 `ready` vs 503 `not_ready` when PostgreSQL is down (no database needed) |
| `test/unit/kafka-transport.spec.ts` | unit (mocked `kafkajs`) | client/producer/consumer wiring, topic bootstrap + already-exists tolerance, message mapping, crash reconnect, shutdown, TLS/SASL wiring, connect/send failure propagation |
| `test/database/database.module.spec.ts` | unit | pool error handler never leaks driver diagnostics to logs |
| `test/database/database.service.spec.ts` | unit (mocked `pg` pool) | query delegation, transaction commit/rollback/release paths, pool shutdown |
| `test/database/migrations-applier.spec.ts` | unit (mocked `pg`) | migration-directory resolution; TLS-aware reachability check (explicit DATABASE_SSL, sslmode auto-detect, plain local, override parity with the app pool) |
| `test/database/pg-tls-behavior.spec.ts` | **real PostgreSQL** | pins the `pg` no-plaintext-fallback behavior that forbids forcing TLS on Render's internal URL; boot check fails closed when `DATABASE_SSL=true` against a non-TLS server |
| `test/app.module.spec.ts` | unit | module compilation, pool shutdown |
| `test/integration/workflow-lifecycle.spec.ts` | **real PostgreSQL** | identity uniqueness incl. concurrent conflicts, draft creation, invalid-DAG rejection, publication atomicity + events, immutability, idempotent run start, task initialization |
| `test/integration/scheduler-claim.spec.ts` | **real PostgreSQL** | claim selection (READY/due RETRY_WAIT/expired lease), atomicity, fencing increment, concurrency (two schedulers, no double claim) |
| `test/integration/worker-execution.spec.ts` | **real PostgreSQL** | eligibility/fencing validation, stale-completion rejection, lease renewal, timeout, retry scheduling, SKIP_DOWNSTREAM, fan-in, completion algorithm |
| `test/integration/outbox-relay.spec.ts` | **real PostgreSQL** | SKIP LOCKED claims, publish-outside-tx, conditional marking, failure recording, claim expiry recovery, at-least-once dedupe |
| `test/integration/http-api.spec.ts` | **real PostgreSQL** + real HTTP | full control plane: status codes, tenant isolation, idempotent run replay, no DB diagnostics leakage |
| `test/integration/end-to-end.spec.ts` | **real PostgreSQL** | crash-recovery scenarios: worker crash mid-execution (lease expiry + stale-token rejection), FAIL_WORKFLOW, SKIP_DOWNSTREAM, retry-then-success, real handler timeout recorded as `TIMED_OUT`, full lifecycle event stream |
| `test/integration/failure-injection.spec.ts` | **real PostgreSQL** | PostgreSQL connection killed before COMMIT of claim, success, failure, and run-start transactions (no partial state, retry applies once); concurrent final-task completion, success-vs-FAIL_WORKFLOW race, duplicate concurrent reports |
| `test/database/immutability-guards.spec.ts` | **real PostgreSQL** | migration 003 guards: published definitions, terminal runs/tasks/attempts, published outbox events, fencing-token monotonicity |

Integration tests start a real, throwaway PostgreSQL server (via the `embedded-postgres` dev dependency) and apply the real migrations — schema and concurrency behavior are tested against an actual database, not a mock.

## Executable end-to-end demo

`npm run demo:local` runs the complete vertical slice with zero prerequisites: it boots a throwaway (ephemeral) PostgreSQL, applies migrations, drives workflow → draft → publish → run → execution over authenticated HTTP in one `role=all` process, prints the persisted results, and exits non-zero unless the run succeeds. `npm run demo` runs the same script against your own database (`DATABASE_URL` required) — see [Running](#running). The demo uses a fixed local-only API secret; never reuse it anywhere else.

## Limitations

Honest boundaries of what is verified:

- **No real Kafka broker has been exercised.** The Kafka transport is implemented and unit-tested with a mocked `kafkajs` client, but no broker is available in this environment, so dispatch over a real Kafka cluster, consumer-group rebalancing, broker outages, and the SASL_SSL handshake are **unverified** (TLS/SASL wiring is configured and unit-tested, including failure propagation, but no live broker has authenticated this client yet). The integration/E2E suites run on the in-memory transport, which is a same-process double of the transport interface — it validates the outbox/relay/scheduler/worker behavior, not Kafka.
- **No real multi-process deployment tested** (requires Kafka). Concurrency is instead proven by concurrent in-process schedulers/workers against real PostgreSQL row locking.
- **Single-tenant isolation is by column + query**, not by Postgres row-level security.
- **`IGNORE_FAILURE` failure policy is not implemented** (deferred by the contract; the API rejects it).
- **No cancellation API** (schema supports `cancellation_requested_at`; the endpoint is not part of this MVP).
- **API-key authentication only**: no OIDC/JWT, no per-key scopes or expiry, no rate limiting; keys rotate via redeploy. Suitable for trusted/small-tenant use; reassess before broad multi-tenant exposure.
- **No Redis** (reserved by the contract; not used anywhere).
- Handlers are deterministic built-ins only; there is no plugin/untrusted-code execution path.
- Exactly-once execution is **not** claimed: at-least-once delivery with idempotent, conditionally-guarded state transitions.
- **Lease-expiry reclaims are not capped by `retryPolicy.maxAttempts`.** A task whose worker dies on every attempt (for example, a handler that crashes the process) is reclaimed indefinitely; only reported failures consume the retry budget. The contract specifies reclaim on expiry but no cap, so this is left as-is and tracked on the roadmap.
- **The outbox relay has no per-event backoff.** A single event that fails publication permanently is retried every poll and, if a full batch of such events sits at the head of the queue, later events wait behind it.
- After `FAIL_WORKFLOW`, `WAITING` tasks become `BLOCKED`; tasks already `READY`/`CLAIMED`/`RUNNING`/`RETRY_WAIT` keep that state but can never be claimed or report successfully (contract B.2 "remain non-executable").

## Architecture constraints

The architecture contract is the source of truth:

- PostgreSQL is authoritative for durable workflow state.
- The scheduler owns durable task claiming and issues the lease/fencing token; workers do not acquire ownership independently.
- State transitions and their corresponding outbox publication intent commit atomically.
- Task delivery and execution are at-least-once; handlers must make externally visible side effects idempotent where required.
- Phase 1 failure policies are `FAIL_WORKFLOW` and `SKIP_DOWNSTREAM`.
- Architectural changes require an explicit proposal and approval rather than silently changing the contract.

Consult `AGENTS.md` before asking coding agents to make changes.

## Roadmap (remaining)

1. Verify the Kafka transport against a real broker (dispatch, consumer groups, broker outage) and run a multi-process deployment.
2. Cancellation API and cooperative task cancellation.
3. `IGNORE_FAILURE` failure policy (per contract deferral).
4. Observability (structured metrics/traces), per-tenant rate limiting, and stronger authn/z (OIDC/JWT, key scopes/expiry).
5. Pagination and query APIs for run listing at scale.
6. Cap or alert on repeated lease-expiry reclaims, and per-event backoff in the outbox relay (see Limitations).

## Project status and contribution

NexusFlow is under active development. Prefer small, verifiable changes, add tests with correctness-sensitive behavior, and run the relevant checks before committing. Do not describe roadmap items as available features until their implementation and tests demonstrate them.

The package currently declares its license as `UNLICENSED`. Public repository visibility does not itself grant permission to reuse or redistribute the project. A license should be selected explicitly before presenting the repository as open-source software.
