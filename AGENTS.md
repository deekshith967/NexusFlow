# NexusFlow Agent Instructions

## Source of Truth

The repository is the durable source of truth for NexusFlow.

The locked architecture contract is:

docs/architecture/final-architecture-contract.md

Before making implementation changes, inspect the repository and determine the exact current checkpoint.

## Continuation Rule

When starting work:

1. Inspect the current repository state.
2. Inspect the relevant documentation and ADRs.
3. Identify what is already completed.
4. Identify the exact remaining task.
5. Continue from that checkpoint.

Never restart the project because the model, account, session, or coding agent changed.

Never recreate files or phases that are already complete.

## Architecture Protection

The Final Architecture Contract is locked.

Do not silently change architectural decisions.

If an implementation appears to require an architectural change:

1. Explain why the change is necessary.
2. Identify affected invariants.
3. Identify concurrency implications.
4. Identify failure/recovery implications.
5. Identify affected transaction boundaries.
6. Identify testing implications.
7. Record the proposed decision in docs/adr/.
8. Obtain approval before implementing the architectural change.

Prefer the smallest implementation that satisfies the existing contract.

## Implementation Discipline

Work in small, verifiable tasks.

Do not attempt to build the entire project from one large request.

For each task:

1. Inspect existing code.
2. Implement only the requested scope.
3. Add or update tests.
4. Run the relevant tests.
5. Review the resulting changes.
6. Report what changed and what remains.

Do not introduce dependencies, services, abstractions, or infrastructure unless required by the current phase.

## Correctness First

NexusFlow is a durable workflow engine.

Correctness is more important than implementation speed.

Preserve the contract's guarantees around:

- PostgreSQL-authoritative state
- durable state transitions
- task execution uniqueness
- leases
- fencing tokens
- stale-worker protection
- transactional dependency activation
- transactional outbox
- at-least-once delivery
- at-least-once execution
- retry semantics
- failure policies
- crash recovery

Do not claim exactly-once execution or exactly-once external side effects.

## Phase Discipline

Implement the phases defined by the Final Architecture Contract in order.

Do not prematurely implement later-phase functionality unless explicitly requested.

Phase 1 must remain consistent with the contract.

In particular, Phase 1 failure policies are:

- FAIL_WORKFLOW
- SKIP_DOWNSTREAM

IGNORE_FAILURE is not a valid Phase 1 policy.

## Testing

Tests are part of the implementation, not an optional cleanup step.

When implementing correctness-sensitive behavior, test relevant:

- state transitions
- invalid transitions
- concurrency
- lease expiry
- fencing
- stale workers
- retries
- dependency activation
- crash/recovery behavior
- transactional outbox behavior
- idempotency

A passing test suite does not justify violating the architecture contract.

## Git Discipline

Keep commits small and meaningful.

Do not rewrite or squash existing project history unless explicitly requested.

Before committing:

- inspect git diff
- inspect git status
- ensure unrelated changes are not included

Commit messages should describe the actual change.

## Agent Communication

At the end of each task, report:

- what was implemented
- files changed
- tests added or changed
- tests executed and their result
- any unresolved issue
- the next logical checkpoint

Do not claim work was completed unless it was actually implemented and verified.

## Repository Safety

Do not delete or overwrite architectural documentation.

Do not modify the Final Architecture Contract to make an implementation fit.

If the implementation conflicts with the contract, stop and surface the conflict.

## Default Starting Instruction

When a new agent/session begins work on NexusFlow, use this principle:

"Inspect the current NexusFlow repository and determine the exact completed checkpoint and remaining work. Continue from that checkpoint. Do not restart or recreate completed work."
