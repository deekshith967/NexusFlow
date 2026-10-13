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
- **Transactional outbox + relay** (Phase D): outbox rows committed atomically with state changes; the relay claims rows with `SKIP LOCKED` + claim TTL, publishes **outside** the transaction, marks published conditionally (owner-checked), records failures, and is crash-safe (expired claims are reclaimed).
- **Scheduler** (Phase D): claims `READY`, due `RETRY_WAIT`, and lease-expired tasks of `RUNNING` runs in one atomic transaction — lease owner/expiry, `fencing_token` increment, attempt creation, input resolution, and `task.claimed` + `task.dispatch.requested` outbox events together. Multiple concurrent schedulers are safe.
- **Worker** (Phase E): consumes dispatches, validates eligibility (state, attempt, fencing token, live lease) before doing anything, transitions to `RUNNING`, renews leases conditionally, executes only **deterministic built-in handlers** (`echo`, `sleep`, `fail`, `merge` — no untrusted code), and reports success/failure through conditional updates that reject stale completions (zero affected rows ⇒ stale, no mutation).
- **Retry and failure semantics** (Phase E): retryable failures with `maxAttempts`/`retryDelayMs`, `FAIL_WORKFLOW` and `SKIP_DOWNSTREAM` failure policies, `BLOCKED` propagation for downstream of failed tasks, fan-in (all prerequisites) activation, and the exact workflow completion algorithm (B.5) with `workflow.run.succeeded` / `workflow.run.failed` events.
- **Crash recovery** (contract G): scheduler crash before/after commit (outbox is the source of dispatch truth), worker crash mid-execution (lease expiry + fencing-token rejection of the stale worker), relay crash after publication (idempotent re-marking), Kafka-outage behavior (outbox accumulates; no dispatches lost). All covered by real-PostgreSQL integration tests.
- **HTTP control plane** (Phase F): create/inspect workflows, draft versions (create/update/validate/publish/retire), start runs (with idempotency), inspect runs/tasks/attempts, health + readiness, tenant via `X-Tenant-Id`, stable status codes (201/200/404/409/422/400), and no raw database diagnostics in responses.
- **Operations** (Phase G): role-gated single application (`api`, `scheduler`, `worker`, `outbox-relay`, `all`), fail-fast PostgreSQL check at startup, health/readiness endpoints, graceful shutdown of pool/transport/loops, idempotent migration applier, and an executable end-to-end demo (`npm run demo`).

## Technology stack

- TypeScript (strict, `commonjs`)
- Node.js / npm (Node 18+; the demo uses the built-in `fetch`)
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
├── docs/architecture/final-architecture-contract.md
├── scripts/
│   ├── migrate.ts            # apply migrations (npm run migrate)
│   └── e2e-demo.ts           # executable end-to-end demo (npm run demo)
├── src/
│   ├── app.module.ts         # role-gated single application
│   ├── main.ts               # bootstrap: DB check, HTTP for api/all roles
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

- Node.js 18+ and npm
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
npm run migrate
```

`npm run demo` also applies migrations automatically.

## Configuration

Copy `.env.example` to `.env` (or export the variables) and adjust. Key variables:

| Variable | Default | Meaning |
| --- | --- | --- |
| `DATABASE_URL` | — (required) | PostgreSQL connection string |
| `NEXUSFLOW_ROLE` | `api` | `api` \| `scheduler` \| `worker` \| `outbox-relay` \| `all` |
| `NEXUSFLOW_TRANSPORT` | `memory` | `kafka` \| `memory` (memory is rejected for the `worker` role) |
| `PORT` | `3000` | HTTP port for `api`/`all` roles |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka brokers (kafka transport only) |
| `NEXUSFLOW_DEFAULT_TENANT_ID` | `00000000-0000-0000-0000-000000000000` | Tenant when `X-Tenant-Id` is absent (must be a UUID) |
| `SCHEDULER_POLL_MS` / `SCHEDULER_BATCH_LIMIT` / `NEXUSFLOW_LEASE_MS` | `250` / `20` / `30000` | Scheduler tuning |
| `RELAY_POLL_MS` / `RELAY_BATCH_LIMIT` / `RELAY_CLAIM_TTL_MS` | `250` / `20` / `30000` | Outbox relay tuning |

## Running

### Single-process local demo (recommended first step)

Requires only PostgreSQL:

```bash
export DATABASE_URL='postgres://user:password@127.0.0.1:5432/nexusflow'
export NEXUSFLOW_ROLE=all
export NEXUSFLOW_TRANSPORT=memory
npm run start:prod        # or: npm run start:dev
```

Or simply run the end-to-end demo, which boots a role=all process, drives the full lifecycle over HTTP, and exits when the run succeeds:

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

## HTTP API

Tenants are selected with the `X-Tenant-Id` header (UUID), defaulting to `NEXUSFLOW_DEFAULT_TENANT_ID`.

| Method & path | Purpose | Success | Errors |
| --- | --- | --- | --- |
| `POST /workflows` | Create workflow identity (`name`, optional `description`) | 201 | 409 duplicate name in tenant, 422 invalid body |
| `GET /workflows` | List workflows for the tenant | 200 | 422 invalid tenant |
| `GET /workflows/:workflowId` | Inspect a workflow | 200 | 404 |
| `POST /workflows/:workflowId/versions` | Create draft version from `tasks[]` (validated: DAG rules, handler, retry policy, timeout, failure policy) | 201 | 422 |
| `GET /workflows/:workflowId/versions` | List versions | 200 | — |
| `GET /versions/:workflowVersionId` | Version + tasks + dependencies | 200 | 404 |
| `PUT /versions/:workflowVersionId/definition` | Replace draft definition (drafts only) | 200 | 404, 409 published/retired, 422 |
| `POST /versions/:workflowVersionId/validate` | Validation report `{ valid, errors }` | 200 | 404 |
| `POST /versions/:workflowVersionId/publish` | Publish (atomic with outbox event) | 200 | 404, 409 invalid transition, 422 invalid DAG |
| `POST /versions/:workflowVersionId/retire` | Retire a published version | 200 | 404, 409 |
| `POST /workflows/:workflowId/runs` | Start run (`workflowVersionId?`, `input?`, `idempotencyKey?`) | 201 new / 200 idempotent replay | 404 unknown version, 422 launch rejected |
| `GET /runs` | List runs (`workflowId?`, `limit?` 1–200) | 200 | 400 bad limit |
| `GET /runs/:workflowRunId` | Run + task executions | 200 | 404 |
| `GET /runs/:workflowRunId/tasks` | Task executions | 200 | 404 |
| `GET /runs/:workflowRunId/tasks/:taskExecutionId/attempts` | Attempt history | 200 | 404 |
| `GET /health` | Liveness | 200 | — |
| `GET /health/ready` | Readiness (checks PostgreSQL) | 200 `ready`/`not_ready` | — |

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
H='-H content-type:application/json -H x-tenant-id:'"$TENANT"

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

Suite overview (at the current HEAD):

| Suite | Type | What it proves |
| --- | --- | --- |
| `test/workflow/workflow-domain.spec.ts` | unit | DAG validation, version state machine, failure policies |
| `test/unit/kafka-transport.spec.ts` | unit (mocked `kafkajs`) | client/producer/consumer wiring, topic bootstrap + already-exists tolerance, message mapping, crash reconnect, shutdown |
| `test/app.module.spec.ts` | unit | module compilation, pool shutdown |
| `test/integration/workflow-lifecycle.spec.ts` | **real PostgreSQL** | identity uniqueness incl. concurrent conflicts, draft creation, invalid-DAG rejection, publication atomicity + events, immutability, idempotent run start, task initialization |
| `test/integration/scheduler-claim.spec.ts` | **real PostgreSQL** | claim selection (READY/due RETRY_WAIT/expired lease), atomicity, fencing increment, concurrency (two schedulers, no double claim) |
| `test/integration/worker-execution.spec.ts` | **real PostgreSQL** | eligibility/fencing validation, stale-completion rejection, lease renewal, timeout, retry scheduling, SKIP_DOWNSTREAM, fan-in, completion algorithm |
| `test/integration/outbox-relay.spec.ts` | **real PostgreSQL** | SKIP LOCKED claims, publish-outside-tx, conditional marking, failure recording, claim expiry recovery, at-least-once dedupe |
| `test/integration/http-api.spec.ts` | **real PostgreSQL** + real HTTP | full control plane: status codes, tenant isolation, idempotent run replay, no DB diagnostics leakage |
| `test/integration/end-to-end.spec.ts` | **real PostgreSQL** | crash-recovery scenarios: worker crash mid-execution (lease expiry + stale-token rejection), FAIL_WORKFLOW, SKIP_DOWNSTREAM, retry-then-success, full lifecycle event stream |

Integration tests start a real, throwaway PostgreSQL server (via the `embedded-postgres` dev dependency) and apply the real migrations — schema and concurrency behavior are tested against an actual database, not a mock.

## Executable end-to-end demo

`npm run demo` (requires `DATABASE_URL`) runs the complete vertical slice against real PostgreSQL in one process and prints the persisted results — see [Running](#running) for the exact command. It exits non-zero if the run does not succeed.

## Limitations

Honest boundaries of what is verified:

- **No real Kafka broker has been exercised.** The Kafka transport is implemented and unit-tested with a mocked `kafkajs` client, but no broker is available in this environment, so dispatch over a real Kafka cluster, consumer-group rebalancing, and broker outages are **unverified**. The integration/E2E suites run on the in-memory transport, which is a same-process double of the transport interface — it validates the outbox/relay/scheduler/worker behavior, not Kafka.
- **No real multi-process deployment tested** (requires Kafka). Concurrency is instead proven by concurrent in-process schedulers/workers against real PostgreSQL row locking.
- **Single-tenant isolation is by column + query**, not by Postgres row-level security.
- **`IGNORE_FAILURE` failure policy is not implemented** (deferred by the contract; the API rejects it).
- **No cancellation API** (schema supports `cancellation_requested_at`; the endpoint is not part of this MVP).
- **No authentication/authorization** beyond the tenant header; not suitable for untrusted networks as-is.
- **No Redis** (reserved by the contract; not used anywhere).
- Handlers are deterministic built-ins only; there is no plugin/untrusted-code execution path.
- Exactly-once execution is **not** claimed: at-least-once delivery with idempotent, conditionally-guarded state transitions.

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
4. Observability (structured metrics/traces), per-tenant rate limiting, and authn/z.
5. Pagination and query APIs for run listing at scale.

## Project status and contribution

NexusFlow is under active development. Prefer small, verifiable changes, add tests with correctness-sensitive behavior, and run the relevant checks before committing. Do not describe roadmap items as available features until their implementation and tests demonstrate them.

The package currently declares its license as `UNLICENSED`. Public repository visibility does not itself grant permission to reuse or redistribute the project. A license should be selected explicitly before presenting the repository as open-source software.
