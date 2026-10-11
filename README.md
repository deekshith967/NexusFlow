# NexusFlow

**NexusFlow is an early-stage implementation of a durable workflow orchestration engine.** The project is being developed in TypeScript with NestJS, with PostgreSQL intended to be the authoritative store for workflow definitions and execution state.

> **Status: Work in progress.** The repository currently contains the initial NestJS foundation, workflow-definition domain logic, DAG validation, and the first workflow-definition database migration. The scheduler/worker execution runtime described in the architecture contract has not yet been implemented in the current code snapshot.

## Goals

NexusFlow is intended to explore the engineering challenges involved in reliable multi-step workflows, including:

- Immutable, statically defined directed acyclic graphs (DAGs).
- Durable state transitions backed by PostgreSQL.
- Concurrent task claiming, leases, and fencing tokens to prevent stale workers from mutating current state.
- Task attempts, retry handling, dependency activation, and workflow completion.
- A transactional outbox for reliable event publication intent, with Kafka used for asynchronous dispatch and event transport.
- Explicit behavior for duplicate delivery, crashes, and recovery.

These are project goals defined in the architecture contract; they are not all implemented or verified yet. NexusFlow does **not** claim exactly-once task execution or exactly-once external side effects.

## Current implementation

Implemented in the current repository snapshot:

- NestJS application bootstrap.
- Workflow/task definition types and Phase 1 failure-policy definitions.
- Workflow-version lifecycle logic for draft, published, discarded, and retired states.
- DAG validation, including checks such as duplicate task keys, unknown dependencies, self-dependencies, and cycles.
- Initial PostgreSQL DDL for workflows, workflow versions, task definitions, and task dependencies.
- Unit tests for the existing domain logic and application module.

Not yet implemented in this snapshot:

- Runtime persistence/repositories and API endpoints for workflow definitions and runs.
- Workflow-run and task-execution lifecycle implementation.
- Scheduler claims, lease renewal/expiration, fencing-token enforcement, and durable task attempts.
- Worker processes and task-handler execution.
- Transactional outbox processing, Kafka relay/dispatch, and delivery deduplication.
- Retry orchestration, downstream activation, terminal workflow resolution, and crash-recovery tests.
- Automated migration execution and a complete local infrastructure setup.

For the full intended design and invariants, see [`docs/architecture/final-architecture-contract.md`](docs/architecture/final-architecture-contract.md).

## Technology stack

### Used in the current code

- TypeScript
- Node.js / npm
- NestJS
- Jest and `ts-jest`
- ESLint
- PostgreSQL client dependency (`pg`) and initial SQL migration

### Planned by the architecture contract

- PostgreSQL as authoritative state storage
- Kafka for asynchronous task dispatch and event transport
- Separate scheduler and worker processes
- Transactional outbox relay
- Redis reserved for a later phase, not required for Phase 1 ownership

The presence of a dependency or an architecture decision does not imply that its runtime integration is already complete.

## Repository layout

```text
.
├── AGENTS.md
├── docs/
│   └── architecture/
│       └── final-architecture-contract.md
├── src/
│   ├── app.module.ts
│   ├── main.ts
│   ├── database/
│   │   └── migrations/
│   │       └── 001_initial_workflow_definition.sql
│   └── workflow/
│       └── domain/
│           ├── dag-validator.ts
│           ├── failure-policy.ts
│           ├── workflow-definition.ts
│           ├── workflow-errors.ts
│           ├── workflow-version-state.ts
│           └── workflow-version.ts
├── test/
│   ├── app.module.spec.ts
│   └── workflow/
│       └── workflow-domain.spec.ts
├── package.json
└── package-lock.json
```

This tree describes the initial repository snapshot; files may be added as development progresses.

## Prerequisites

- Node.js and npm
- Git

PostgreSQL and Kafka are part of the planned runtime architecture, but the current application snapshot does not yet provide a complete automated local setup or connect the runtime to those services.

## Install and verify

Clone the repository and install the locked dependencies:

```bash
git clone https://github.com/deekshith967/NexusFlow.git
cd NexusFlow
npm ci
```

Run the existing checks:

```bash
npm test
npm run typecheck
npm run lint
npm run build
```

Available development commands include:

```bash
npm run start:dev
npm run test:watch
npm run test:cov
```

`npm run start:dev` starts the current NestJS application skeleton. It should not be interpreted as starting a complete durable workflow engine: the execution runtime and its infrastructure integrations remain planned work.

## Testing

The current tests cover the NestJS application module and workflow-domain behavior. At the last recorded baseline check, **18 tests passed across 2 test suites**, and the build, lint, and TypeScript typecheck completed successfully.

As features are implemented, tests should cover at least:

- Valid and invalid workflow definitions and dependency graphs.
- State transitions and terminal-state immutability.
- Concurrent scheduler claims and fencing-token checks.
- Lease expiration and stale-worker rejection.
- Transaction boundaries between state updates and outbox events.
- Retry policy and failure-policy behavior.
- Dependency activation, workflow completion, duplicate delivery, and crash recovery.

Passing the current unit tests is not evidence that these unimplemented runtime guarantees are already satisfied.

## Architecture constraints

The architecture contract is the source of truth for implementation decisions. Important constraints include:

- PostgreSQL is authoritative for durable workflow state.
- The scheduler owns durable task claiming and issues the lease/fencing token; workers do not acquire ownership independently.
- State transitions and their corresponding outbox publication intent must be committed atomically.
- Task delivery and task execution are at-least-once; handlers must make externally visible side effects idempotent where required.
- Phase 1 failure policies are `FAIL_WORKFLOW` and `SKIP_DOWNSTREAM`. `IGNORE_FAILURE` is deferred to a later phase.
- Architectural changes require an explicit proposal and approval rather than silently changing the contract.

Consult `AGENTS.md` before asking coding agents to make changes.

## Roadmap

1. Complete definition persistence, database access, lifecycle enforcement, and migration validation.
2. Add workflow-run, task-execution, task-attempt, and outbox persistence.
3. Implement scheduler claiming, leases, fencing tokens, and durable dispatch intent.
4. Implement worker execution, lease renewal, conditional completion, and task handlers.
5. Implement the outbox relay and Kafka dispatch with retryable, duplicate-tolerant delivery.
6. Complete retry/failure-policy semantics and transactional dependency activation.
7. Add integration, concurrency, failure-injection, and crash-recovery tests.
8. Document reproducible local development and demonstrated end-to-end usage.

## Project status and contribution

NexusFlow is under active development. Prefer small, verifiable changes, add tests with correctness-sensitive behavior, and run the relevant checks before committing. Do not describe roadmap items as available features until their implementation and tests demonstrate them.

The package currently declares its license as `UNLICENSED`. Public repository visibility does not itself grant permission to reuse or redistribute the project. A license should be selected explicitly before presenting the repository as open-source software.
