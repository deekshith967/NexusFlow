# Final Architecture Contract

This contract locks the Phase 0 architecture for NexusFlow. It preserves the existing terminology and decisions, resolving only the identified ambiguities.

## A. Final Architecture

NexusFlow is a PostgreSQL-authoritative durable workflow engine for immutable, statically defined DAGs.

Phase 1 consists of:

- One NestJS API/control-plane process
- One or more scheduler process instances
- One or more worker process instances
- PostgreSQL as authoritative state
- Kafka as asynchronous dispatch and event transport
- Transactional outbox relay included in Phase 1
- Redis reserved for later acceleration and rate limiting
- Shared TypeScript packages for domain models, application services, persistence, messaging, and contracts

### Final Execution Path

```text
Client
  |
  v
NestJS API
  |
  v
PostgreSQL
  | \
  |  \-- task/run state transitions
  |      plus outbox_events
  |
  v
Scheduler
  |
  v
PostgreSQL claim transaction
  |
  v
Outbox relay
  |
  v
Kafka task-dispatch topic
  |
  v
Worker
  |
  v
Handler execution
  |
  v
PostgreSQL conditional completion transaction
  |
  +--> task/attempt state
  +--> downstream activation
  +--> workflow completion
  +--> lifecycle outbox events
```

### LOCKED DECISION: Scheduler/Worker Ownership Model

The scheduler owns durable task claiming and assigns the lease. The worker does not acquire or adopt ownership.

The scheduler:

- Selects eligible task executions
- Creates the lease
- Increments the fencing token
- Creates the task attempt
- Persists the task-dispatch outbox event

The worker:

- Receives the dispatch message
- Validates the task execution, attempt, lease, and fencing token
- Executes the handler
- Renews the lease using the fencing token
- Reports success or failure using the same fencing token

Worker identity is recorded for diagnostics and reporting. It is not the ownership authority. Ownership is represented by:

```text
task_execution_id + fencing_token
```

The `lease_owner` identifies the scheduler-issued logical lease owner. Worker completion is authorized by the current fencing token and valid task lease, not by matching the scheduler identity.

### Execution Guarantees

NexusFlow guarantees:

- Durable workflow state transitions
- At-least-once task delivery
- At-least-once task execution
- Fencing against stale state mutation
- Transactional dependency activation
- Transactional state-to-event publication intent

NexusFlow does not guarantee:

- Exactly-once business execution
- Exactly-once external side effects
- Automatic rollback of external side effects
- Forced termination of arbitrary handler code

Handlers must provide idempotency for externally visible effects.

### Final Failure Policies

Phase 1 supports exactly two failure policies:

```text
FAIL_WORKFLOW
SKIP_DOWNSTREAM
```

`IGNORE_FAILURE` is deferred to Phase 3 and must not appear as a valid Phase 1 task policy.

Semantics:

- `FAIL_WORKFLOW`: task failure causes the workflow run to become `FAILED`; descendants do not execute.
- `SKIP_DOWNSTREAM`: the failed task becomes terminally `SKIPPED` for dependency propagation, and downstream tasks may continue if their other prerequisites satisfy their policies.

A task's own failure policy controls the impact of its failure on the run and descendants. Retry policy is evaluated before the failure policy.

### Transactional Outbox

The transactional outbox is mandatory in Phase 1.

Every database-derived event is inserted into `outbox_events` in the same PostgreSQL transaction as the state transition it describes.

The outbox relay:

- Claims unpublished events
- Publishes them to Kafka
- Marks them published after successful publication
- Retries failed publications
- Uses `event_id` for deduplication

Kafka publication is at-least-once. Consumers must tolerate duplicate events.

---

# B. Final State Machines

## B.1 Workflow Version

```text
DRAFT -> PUBLISHED
DRAFT -> DISCARDED
PUBLISHED -> RETIRED
```

### Rules

- A published version is immutable.
- A discarded version cannot be published or executed.
- A retired version cannot start new runs.
- Existing runs referencing a retired version continue.
- Publication is conditional on `state = DRAFT`.
- A version cannot transition out of a terminal version state.

## B.2 Workflow Run

Phase 1 states:

```text
CREATED -> RUNNING
RUNNING -> SUCCEEDED
RUNNING -> FAILED
RUNNING -> CANCEL_REQUESTED
CANCEL_REQUESTED -> CANCELLED
```

`PAUSED` is deferred and must not be implemented in Phase 1.

### `CREATED -> RUNNING`

The start transaction:

- Verifies the selected workflow version is `PUBLISHED`
- Creates the run
- Creates one task execution for every task definition
- Initializes root tasks as `READY`
- Initializes non-root tasks as `WAITING`
- Inserts the `workflow.run.started` outbox event

### `RUNNING -> SUCCEEDED`

A run succeeds only when the exact completion algorithm in section B.5 returns `SUCCEEDED`.

### `RUNNING -> FAILED`

A run fails when:

- A task exhausts retries and has `FAIL_WORKFLOW`
- A task has a permanent failure and has `FAIL_WORKFLOW`
- A run-level failure is recorded

After `FAILED` commits:

- No new tasks may become `READY`
- Existing active attempts may finish locally
- Their stale or late reports cannot make the run successful

### Cancellation

Cancellation is reserved in the Phase 1 model but the cancellation API and full worker cancellation behavior are outside the required Phase 1 core.

If implemented in Phase 1:

- `RUNNING -> CANCEL_REQUESTED` is durable
- The scheduler stops claiming new tasks
- Workers receive cooperative cancellation where possible
- `CANCEL_REQUESTED -> CANCELLED` occurs only after active work is terminal

## B.3 Task Execution

Phase 1 states:

```text
WAITING -> READY
READY -> CLAIMED
CLAIMED -> RUNNING
CLAIMED -> READY
CLAIMED -> RETRY_WAIT
CLAIMED -> FAILED
CLAIMED -> CANCELLED
RUNNING -> SUCCEEDED
RUNNING -> RETRY_WAIT
RUNNING -> FAILED
RUNNING -> CANCELLED
WAITING -> SKIPPED
WAITING -> BLOCKED
```

### State Definitions

- `WAITING`: prerequisites are not yet resolved.
- `READY`: eligible for scheduler claim.
- `CLAIMED`: scheduler has assigned a valid lease and created an attempt.
- `RUNNING`: worker has accepted the attempt and started handler execution.
- `RETRY_WAIT`: retryable failure occurred and the next retry time has not arrived.
- `SUCCEEDED`: logical task completed successfully.
- `FAILED`: logical task permanently failed.
- `SKIPPED`: task will not execute and is treated as satisfied for downstream dependency evaluation.
- `BLOCKED`: task cannot execute and does not satisfy downstream dependencies.
- `CANCELLED`: task was cancelled under the run cancellation policy.

### State Rules

- `CLAIMED -> READY` is permitted when dispatch fails before execution or the lease expires before execution.
- `RUNNING -> RETRY_WAIT` requires a retryable failure and remaining attempts.
- `RUNNING -> FAILED` occurs for permanent failure or exhausted retries.
- Terminal task states cannot transition back to nonterminal states in Phase 1.
- Manual retry is deferred to Phase 2.

### Failure Propagation

For `FAIL_WORKFLOW`:

```text
task failure -> workflow run FAILED
```

All nonterminal descendants become `BLOCKED` or remain non-executable after the run becomes failed.

For `SKIP_DOWNSTREAM`:

```text
task failure -> task becomes SKIPPED for dependency evaluation
```

The task's direct descendants may become `READY` if all their prerequisites satisfy dependency rules.

A descendant blocked by a failed prerequisite is transitioned to `BLOCKED` when its prerequisite outcomes make execution impossible.

## B.4 Task Attempt

```text
CREATED -> DISPATCHED
DISPATCHED -> STARTED
DISPATCHED -> ABANDONED
STARTED -> SUCCEEDED
STARTED -> FAILED
STARTED -> TIMED_OUT
STARTED -> CANCELLED
STARTED -> ABANDONED
```

### Rules

- Attempt numbers are unique and monotonically increasing per task execution.
- Attempt terminal states are immutable.
- An attempt cannot be created for a terminal task execution.
- A late attempt report is rejected when its fencing token is stale or its logical task is terminal.
- `ABANDONED` means the attempt lost ownership or became unrecoverable before a valid terminal report.

## B.5 Exact Workflow Completion Algorithm

Workflow completion is evaluated inside the same transaction that changes a task execution to a terminal state.

For a run `R`:

1. Lock the workflow run row using `FOR UPDATE`.
2. Verify the run is currently `RUNNING`.
3. Lock or consistently read all task executions belonging to `R`.
4. If any task execution is `FAILED`:
   - Transition the run to `FAILED`.
   - Mark all nonterminal tasks as `BLOCKED` where the run-level failure policy requires it.
   - Insert `workflow.run.failed`.
   - Commit.
5. Otherwise, if any task execution is in an executable or active state:
   - `WAITING`
   - `READY`
   - `CLAIMED`
   - `RUNNING`
   - `RETRY_WAIT`
   - Leave the run `RUNNING`.
6. Otherwise, if any task execution is `BLOCKED`:
   - Transition the run to `FAILED`.
   - Insert `workflow.run.failed`.
   - Commit.
7. Otherwise, if every task execution is one of:
   - `SUCCEEDED`
   - `SKIPPED`
   - Transition the run to `SUCCEEDED`.
   - Insert `workflow.run.succeeded`.
   - Commit.
8. If cancellation is active and no active work remains:
   - Transition the run to `CANCELLED`.
   - Insert `workflow.run.cancelled`.
   - Commit.

The run must never become `SUCCEEDED` while any task is nonterminal or `BLOCKED`.

---

# C. Final Database Model

The following is the finalized Phase 1 logical schema.

## C.1 `workflows`

Purpose: stable workflow identity.

Primary key:

```text
workflow_id
```

Columns:

```text
workflow_id
tenant_id
name
description
status
created_at
updated_at
archived_at
```

Constraints:

- `workflow_id` is globally unique.
- `(tenant_id, name)` is unique for non-archived workflows.
- `tenant_id` is present from the first migration.

Indexes:

```text
(tenant_id, status)
(tenant_id, name)
```

## C.2 `workflow_versions`

Purpose: immutable executable workflow snapshots.

Primary key:

```text
workflow_version_id
```

Columns:

```text
workflow_version_id
workflow_id
tenant_id
version_number
state
definition_hash
created_at
published_at
retired_at
```

Constraints:

- Foreign key to `workflows`.
- `(workflow_id, version_number)` is unique.
- Multiple versions may be `PUBLISHED`.
- Publication is conditional on `state = DRAFT`.
- Published rows are immutable.

Indexes:

```text
(workflow_id, state)
(tenant_id, state)
(definition_hash)
```

## C.3 `task_definitions`

Purpose: version-scoped DAG nodes.

Primary key:

```text
task_definition_id
```

Columns:

```text
task_definition_id
workflow_version_id
task_key
handler_type
handler_version
configuration
input_mapping
output_mapping
retry_policy
timeout_ms
failure_policy
created_at
```

Constraints:

- Foreign key to `workflow_versions`.
- `(workflow_version_id, task_key)` is unique.
- `failure_policy` is restricted to:
  - `FAIL_WORKFLOW`
  - `SKIP_DOWNSTREAM`
- `IGNORE_FAILURE` is invalid in Phase 1.
- Published-version rows are immutable.

Indexes:

```text
(workflow_version_id, task_key)
(workflow_version_id, handler_type)
```

## C.4 `task_dependencies`

Purpose: version-scoped directed edges.

Primary key:

```text
dependency_id
```

Columns:

```text
dependency_id
workflow_version_id
upstream_task_definition_id
downstream_task_definition_id
created_at
```

Constraints:

- Foreign key to `workflow_versions`.
- Foreign keys to both task definitions.
- Both task definitions must belong to the same workflow version.
- Self-dependencies are prohibited.
- `(workflow_version_id, upstream_task_definition_id, downstream_task_definition_id)` is unique.
- Published-version rows are immutable.
- DAG validation must succeed before publication.

Indexes:

```text
(workflow_version_id, upstream_task_definition_id)
(workflow_version_id, downstream_task_definition_id)
```

## C.5 `workflow_runs`

Purpose: one execution of one immutable workflow version.

Primary key:

```text
workflow_run_id
```

Columns:

```text
workflow_run_id
tenant_id
workflow_id
workflow_version_id
state
idempotency_key
input_payload
output_payload
correlation_id
created_at
started_at
completed_at
failure_code
failure_message
cancellation_requested_at
```

Constraints:

- Foreign keys to `workflows` and `workflow_versions`.
- Referenced workflow version must be `PUBLISHED` when the run starts.
- `(tenant_id, workflow_id, idempotency_key)` is unique when `idempotency_key` is present.
- Terminal run states are immutable.

Indexes:

```text
(tenant_id, created_at)
(workflow_id, state)
(workflow_version_id)
(state, created_at)
```

## C.6 `task_executions`

Purpose: one logical task instance per workflow run.

Primary key:

```text
task_execution_id
```

Columns:

```text
task_execution_id
workflow_run_id
task_definition_id
task_key
state
resolved_input
output_payload
failure_code
failure_message
attempt_count
next_attempt_at
lease_owner
lease_expires_at
fencing_token
claimed_at
started_at
completed_at
last_attempt_id
created_at
updated_at
```

Constraints:

- Foreign keys to `workflow_runs` and `task_definitions`.
- `(workflow_run_id, task_definition_id)` is unique.
- `fencing_token` increases on each ownership change.
- At most one current lease exists.
- Terminal states cannot be changed except by deferred manual retry functionality.

Indexes:

```text
(state, next_attempt_at)
(workflow_run_id, state)
(lease_expires_at)
(lease_owner, state)
(workflow_run_id, task_key)
```

## C.7 `task_attempts`

Purpose: concrete delivery and execution history.

Primary key:

```text
task_attempt_id
```

Columns:

```text
task_attempt_id
task_execution_id
attempt_number
state
worker_id
scheduler_id
fencing_token
dispatch_id
started_at
finished_at
timeout_at
result_payload
failure_code
failure_message
created_at
```

Constraints:

- Foreign key to `task_executions`.
- `(task_execution_id, attempt_number)` is unique.
- `dispatch_id` is unique.
- Attempt terminal states are immutable.
- Completion must match the current task execution fencing token.

Indexes:

```text
(task_execution_id, attempt_number)
(state, created_at)
(worker_id, state)
(dispatch_id)
```

## C.8 `outbox_events`

Purpose: durable publication intent.

Primary key:

```text
outbox_event_id
```

Columns:

```text
outbox_event_id
event_id
event_type
schema_version
aggregate_type
aggregate_id
tenant_id
workflow_id
workflow_version_id
workflow_run_id
task_execution_id
task_attempt_id
occurred_at
correlation_id
causation_id
producer
payload
headers
created_at
published_at
publish_attempts
last_error
claim_owner
claim_expires_at
```

Constraints:

- `event_id` is unique.
- Publication is idempotent by `event_id`.
- An outbox event is inserted in the same transaction as its associated state transition.

Indexes:

```text
(created_at) WHERE published_at IS NULL
(publish_attempts, created_at)
(claim_expires_at) WHERE published_at IS NULL
(aggregate_id, created_at)
```

### Event Envelope

Mandatory fields:

```text
event_id
event_type
schema_version
aggregate_type
aggregate_id
occurred_at
correlation_id
producer
payload
```

Conditionally mandatory identifiers:

- Workflow events include `workflow_id` and `workflow_version_id`.
- Run events include `workflow_run_id`.
- Task events include `workflow_run_id` and `task_execution_id`.
- Attempt events include `task_attempt_id`.

`causation_id` is required when an event is caused by another command or event.

Kafka message keys:

- Task dispatch events use `task_execution_id`.
- Run lifecycle events use `workflow_run_id`.
- Other lifecycle events use `aggregate_id`.

Kafka ordering is not a correctness mechanism.

---

# D. Final Transaction Map

## D.1 Create Workflow

Transaction:

1. Validate tenant-scoped workflow name.
2. Insert `workflows`.
3. Commit.

No Kafka publication is required unless a workflow-created event is part of the external contract. If emitted, insert its outbox row before commit.

## D.2 Create Draft Workflow Version

Transaction:

1. Lock parent workflow if required.
2. Insert `workflow_versions` with `DRAFT`.
3. Insert task definitions.
4. Insert dependency edges.
5. Validate handler references and configuration.
6. Commit.

The draft may be edited until publication.

## D.3 Publish Workflow Version

Transaction:

1. Lock the workflow version.
2. Verify `state = DRAFT`.
3. Validate all task definitions.
4. Validate all dependencies.
5. Detect cycles.
6. Compute and store `definition_hash`.
7. Update version to `PUBLISHED`.
8. Insert `workflow.version.published`.
9. Commit.

No publish operation is valid if validation fails.

## D.4 Retire Workflow Version

Transaction:

1. Lock the version.
2. Verify `state = PUBLISHED`.
3. Update to `RETIRED`.
4. Insert `workflow.version.retired`.
5. Commit.

Existing runs are unaffected.

## D.5 Start Workflow Run

Transaction:

1. Validate the selected version is `PUBLISHED`.
2. Resolve or create the tenant/workflow idempotency record through the unique constraint.
3. If an existing idempotent run exists, return it without creating another run.
4. Insert `workflow_runs` with `CREATED`.
5. Insert one `task_executions` row per task definition.
6. Set root tasks to `READY`.
7. Set non-root tasks to `WAITING`.
8. Update run to `RUNNING`.
9. Insert `workflow.run.started`.
10. Insert `task.ready` outbox events for root tasks.
11. Commit.

No root task is dispatchable before this transaction commits.

## D.6 Scheduler Claim

Transaction:

1. Select eligible task executions using `FOR UPDATE SKIP LOCKED`.
2. Verify the parent run is `RUNNING`.
3. Verify task state is `READY` or retry-eligible `RETRY_WAIT`.
4. Reclaim expired ownership if applicable.
5. Increment `fencing_token`.
6. Assign `lease_owner`.
7. Assign `lease_expires_at`.
8. Transition task to `CLAIMED`.
9. Create the next `task_attempt`.
10. Insert `task.claimed`.
11. Insert `task.dispatch.requested`.
12. Commit.

The claim, lease, attempt, and dispatch intent are atomic.

## D.7 Outbox Relay Claim

Transaction:

1. Select unpublished or reclaimable outbox rows using `FOR UPDATE SKIP LOCKED`.
2. Assign `claim_owner`.
3. Assign `claim_expires_at`.
4. Commit.

Kafka publication occurs outside the transaction.

## D.8 Outbox Relay Publish

For each claimed event:

1. Publish to Kafka using `event_id`.
2. On success, execute a conditional update:
   ```text
   WHERE event_id = ?
   AND published_at IS NULL
   AND claim_owner = ?
   ```
3. Set `published_at`.
4. On failure, increment `publish_attempts` and store `last_error`.
5. Clear or expire the claim.

If the relay crashes after Kafka publication and before marking the row published, the event may be published again. Consumers must deduplicate or tolerate duplicates.

## D.9 Worker Start

Transaction:

1. Validate task execution exists.
2. Validate task state is `CLAIMED`.
3. Validate the dispatch attempt matches.
4. Validate the fencing token matches.
5. Transition task execution to `RUNNING`.
6. Transition attempt to `STARTED`.
7. Record `worker_id` and `started_at`.
8. Insert `task.started`.
9. Commit.

If the transaction affects zero rows, the dispatch is stale or invalid and must not execute.

## D.10 Lease Renewal

Transaction:

1. Validate task execution ID.
2. Validate `lease_owner` where applicable.
3. Validate fencing token.
4. Validate task is nonterminal.
5. Validate lease has not expired.
6. Extend `lease_expires_at`.
7. Commit.

A zero-row update means renewal failed. The worker must treat itself as stale.

## D.11 Successful Completion

Transaction:

1. Lock the workflow run.
2. Conditionally validate:
   - Task execution ID
   - Attempt ID
   - `RUNNING` state
   - Current fencing token
   - Valid lease
3. Persist output.
4. Transition attempt to `SUCCEEDED`.
5. Transition task execution to `SUCCEEDED`.
6. Clear lease ownership.
7. Insert `task.succeeded`.
8. Evaluate and activate eligible downstream tasks.
9. Insert `task.ready` events for newly ready tasks.
10. Run the exact workflow completion algorithm.
11. Insert run lifecycle events if the run becomes terminal.
12. Commit.

A stale or duplicate report affects zero rows or returns the already committed outcome without repeating dependency activation.

## D.12 Failed Completion

Transaction:

1. Lock the workflow run.
2. Validate task execution, attempt, fencing token, and lease.
3. Persist structured failure details.
4. Transition attempt to `FAILED` or `TIMED_OUT`.
5. If retryable and attempts remain:
   - Set task execution to `RETRY_WAIT`.
   - Set `next_attempt_at`.
   - Insert `task.retry_scheduled`.
6. Otherwise:
   - For `FAIL_WORKFLOW`, transition task to `FAILED`.
   - For `SKIP_DOWNSTREAM`, transition task to `SKIPPED` for dependency propagation.
7. Activate or block downstream tasks according to dependency outcomes.
8. Evaluate workflow completion.
9. Insert relevant outbox events.
10. Commit.

## D.13 Dependency Activation

Dependency activation is performed inside the prerequisite completion transaction.

For each downstream task:

1. Read all prerequisite task executions.
2. Evaluate each outcome against the downstream task's policy.
3. If all prerequisites satisfy the policy and the downstream task is `WAITING`, transition it to `READY`.
4. If execution is impossible because of an unsatisfied failed prerequisite, transition it to `BLOCKED`.
5. Insert the corresponding outbox event.
6. Continue within the same transaction.

The `WAITING -> READY` and `WAITING -> BLOCKED` updates are conditional.

## D.14 Workflow Completion

Workflow completion is evaluated while the run row is locked and before commit.

The algorithm is defined in section B.5.

## D.15 Cancellation

If cancellation is included in Phase 1:

1. Lock the run.
2. Verify `RUNNING`.
3. Set `CANCEL_REQUESTED`.
4. Insert `workflow.run.cancellation_requested`.
5. Commit.

The scheduler must stop claiming work for that run. Active workers handle cancellation cooperatively.

---

# E. Final Crash/Recovery Matrix

| Scenario | Durable state | Recovery behavior | Duplicate business execution |
|---|---|---|---|
| Scheduler crashes before claim commit | Task remains `READY` or retry-eligible | Another scheduler claims it | No platform duplicate |
| Scheduler crashes after claim commit, before outbox relay | Task is `CLAIMED`; attempt and outbox row exist | Relay publishes dispatch; expired lease is reclaimed if necessary | Possible if dispatch and recovery overlap |
| Scheduler crashes after dispatch publication | Task remains claimed; outbox may be unpublished in database | Relay republishes if needed; worker validates current fencing token | Duplicate delivery possible |
| Outbox relay crashes before Kafka publication | Outbox row remains unpublished | Another relay claims and publishes | No business execution caused by this event |
| Outbox relay crashes after Kafka publication, before marking published | Kafka may contain the event; outbox remains unpublished | Event is republished | Duplicate event delivery possible |
| Worker receives a duplicate dispatch | Database state is checked | Only current claim/token may execute | Duplicate local execution must be prevented where possible |
| Worker crashes before starting handler | Task/attempt remains claimed or dispatched | Lease expiry allows scheduler recovery | Usually no business effect |
| Worker crashes during handler execution | Attempt remains active until lease expiry | Scheduler reclaims with a new fencing token | Yes, potentially |
| Worker crashes after external effect, before success report | Task may become reclaimable | Replacement attempt may execute | Yes, ambiguous success |
| Worker crashes after success commit | Task is `SUCCEEDED` | Duplicate messages are ignored | No additional platform execution |
| Worker reports with expired lease | No authoritative mutation | Report rejected as stale | Business effect may already have occurred |
| Worker reports with old fencing token | No authoritative mutation | Report rejected as stale | Business effect may already have occurred |
| Two schedulers claim the same task | Row locking and conditional update permit one claim | Losing scheduler receives no row | No duplicate valid claim |
| Two prerequisites complete concurrently | Completion transactions serialize relevant state | One transaction activates downstream; the other observes `READY` | No duplicate logical task |
| PostgreSQL becomes unavailable before transaction commit | No state transition is committed | Caller or worker retries | External business effect may be ambiguous |
| PostgreSQL becomes unavailable after transaction commit | State and outbox are durable | Relay and workers continue after recovery | Duplicate reports/events possible, not duplicate logical completion |
| Kafka becomes unavailable | Outbox rows remain durable | Relay retries publication | No loss of committed event intent |
| Run fails while tasks are active | Run becomes `FAILED` | Active reports are rejected or recorded without changing run outcome | Active handlers may finish externally |
| Process shuts down gracefully | New work is not claimed; active work drains | Unfinished leases expire | Possible duplicate execution after expiry |
| Process is force-killed | Active lease eventually expires | Scheduler reclaims work | Possible duplicate execution |

Recovery is based on PostgreSQL state, lease expiration, fencing tokens, retry policy, and outbox replay. Kafka offsets are never the authoritative recovery mechanism.

---

# F. 15 Non-Negotiable Invariants

1. A published workflow version is immutable.
2. A workflow run references exactly one immutable workflow version.
3. A run may start only from a `PUBLISHED` workflow version.
4. Every published workflow version contains a valid acyclic DAG.
5. Every task key is unique within its workflow version.
6. Every dependency references two task definitions in the same workflow version.
7. Every workflow run has at most one logical task execution per task definition.
8. Every task execution has at most one current valid lease.
9. Every ownership change increments the fencing token.
10. A task attempt is created atomically with its scheduler claim.
11. A worker completion is accepted only when task ID, attempt ID, fencing token, and lease validity all match.
12. A stale worker cannot mutate task state, persist authoritative output, activate dependencies, or complete a run.
13. A terminal task execution cannot become nonterminal in Phase 1.
14. A downstream task becomes `READY` only when all required prerequisites satisfy their dependency policies.
15. Every database state transition requiring an event inserts its outbox event in the same transaction.

Additional mandatory behavioral consequences:

- `IGNORE_FAILURE` is not a valid Phase 1 failure policy.
- Kafka publication is at-least-once.
- Task execution is at-least-once.
- Exactly-once business execution is not claimed.
- A run cannot become `SUCCEEDED` while any task is active, executable, or `BLOCKED`.

---

# G. 15 Required Tests

1. Reject a task definition with a self-dependency.
2. Reject a workflow version containing a dependency cycle.
3. Reject a dependency referencing a task from another workflow version.
4. Reject publication when handler, retry, timeout, or failure-policy configuration is invalid.
5. Verify published workflow versions cannot be mutated.
6. Verify retired workflow versions cannot start new runs.
7. Verify workflow-start idempotency returns the original run and creates no duplicate run.
8. Verify a run creates exactly one task execution per task definition.
9. Verify root tasks become `READY` and non-root tasks remain `WAITING`.
10. Verify concurrent schedulers create only one valid claim and one attempt.
11. Verify lease renewal succeeds only with the current fencing token and valid lease.
12. Verify an expired lease can be reclaimed with a higher fencing token.
13. Verify stale completion with an old fencing token is rejected.
14. Verify concurrent fan-in prerequisite completion activates the downstream task exactly once.
15. Verify successful completion, retry scheduling, terminal failure, dependency activation, workflow completion, and outbox insertion occur atomically.

Required failure-injection coverage within these tests:

- Scheduler crash before and after claim commit
- Worker crash during execution
- Completion report after lease expiration
- Outbox relay crash after Kafka publication
- Kafka outage with durable unpublished outbox rows

---

# H. Exact Phase 1 Completion Criteria

Phase 1 is complete only when all criteria below are met.

## Domain and Versioning

- Static workflow versions can be created as drafts.
- Task definitions and dependency edges can be persisted.
- Publication performs complete DAG validation, including cycle detection.
- Published versions are immutable.
- Retired versions cannot start new runs.
- Failure policies are restricted to `FAIL_WORKFLOW` and `SKIP_DOWNSTREAM`.

## Run and Task Execution

- A run references one published version.
- Workflow-start idempotency is enforced by a database constraint and transaction.
- Each run creates exactly one logical task execution per task definition.
- Root tasks become ready transactionally.
- Non-root tasks wait for dependency resolution.
- Retries create attempts without creating duplicate logical task executions.
- Attempt history is durable and queryable.

## Scheduler

- Multiple scheduler instances may operate concurrently.
- Claims use PostgreSQL locking and conditional updates.
- Claims create leases, fencing tokens, attempts, and outbox dispatch intents atomically.
- Expired leases are recoverable.
- Duplicate claims are prevented.
- Claimed-but-undispatched tasks are recoverable through outbox relay or reconciliation.

## Worker

- Workers validate current task state before executing.
- Workers report success and failure through conditional persistence operations.
- Lease renewal is conditional on the current fencing token.
- Stale workers cannot commit authoritative outcomes.
- Handler timeout and retry classification are recorded.
- Worker shutdown stops new consumption and permits lease-based recovery.

## Dependencies and Completion

- Fan-out activates all eligible downstream branches.
- Fan-in uses all-prerequisite semantics.
- Concurrent prerequisite completion activates a downstream task at most once.
- Failed prerequisites produce `BLOCKED` or policy-derived `SKIPPED` behavior.
- `FAIL_WORKFLOW` transitions the run to `FAILED`.
- `SKIP_DOWNSTREAM` permits eligible descendants to continue.
- The exact workflow completion algorithm is implemented and tested.
- No run reaches `SUCCEEDED` with active, executable, or blocked tasks.

## Outbox and Kafka

- Every required database-derived event is inserted transactionally.
- The outbox relay is included in Phase 1.
- Unpublished events are retryable.
- Relay claims are recoverable after relay failure.
- Duplicate publication is tolerated through `event_id`.
- Kafka is used for dispatch and event transport only.
- Kafka outages do not lose committed database state or event intent.

## Persistence

- All finalized Phase 1 tables, constraints, and indexes exist.
- Tenant identifiers exist from the first migration.
- State-transition updates are conditional.
- Lease comparisons use PostgreSQL database time.
- Attempt numbers and dispatch IDs are unique.
- Published definitions and terminal outcomes are immutable.

## Verification

- All 15 required tests pass.
- Failure-injection tests cover scheduler, worker, PostgreSQL, Kafka, lease, and outbox failure paths.
- Race tests cover duplicate claims, stale completion, concurrent fan-in, and workflow completion.
- No test relies on Kafka offsets as authoritative task state.
- The implementation makes no exactly-once execution claim.
- The implementation does not introduce technologies or deployment components outside the locked Phase 1 scope.

The final Phase 1 contract is therefore:

> PostgreSQL authoritatively manages immutable workflow versions, runs, task executions, attempts, leases, fencing tokens, dependency activation, and completion. Schedulers claim tasks and create durable dispatch intents. Outbox relays publish those intents to Kafka. Workers execute at least once and report conditionally. PostgreSQL accepts only current, fenced outcomes and performs downstream activation and workflow completion transactionally. External business effects remain the responsibility of idempotent handlers and downstream systems.