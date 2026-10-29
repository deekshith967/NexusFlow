# ADR 003: Database immutability guards and control-plane hardening

- Status: Accepted
- Date: 2026-10-30
- Scope: enforcement and validation only. No architecture-contract
  change: state machines, transaction boundaries, lock order, outbox
  semantics, claim/lease/fencing ownership, and failure policies are
  unchanged. Nothing here claims exactly-once execution.

## Context

An audit of the Phase 1 completion criteria (contract H) against the
code at `1e7a19a` found these confirmed gaps:

1. "Published definitions and terminal outcomes are immutable" was
   enforced only by conditional application updates. Migrations 001/002
   explicitly deferred database enforcement. Any defect or manual SQL
   could rewrite a published task graph, reopen a terminal run, or move
   a fencing token backwards.
2. Handler references were checked only for non-empty strings. A draft
   naming an unregistered handler published successfully and then
   failed every run with `UNKNOWN_HANDLER` (contract D.2 step 5, D.3
   step 3, G-4).
3. Real handler timeouts were stored as `FAILED` attempts, not
   `TIMED_OUT` (contract B.4, H "Handler timeout and retry
   classification are recorded"). The worker raced the timeout but did
   not pass the classification to the failure report.
4. Starting a run on another tenant's workflow or version returned 422
   "does not belong to this tenant", revealing that the resource exists
   (contradicts ADR 001's 404-indistinguishable rule).
5. Non-UUID path identifiers reached PostgreSQL and surfaced as 500.
6. The attempts endpoint ignored the `:workflowRunId` in its path.

## Decision

1. **Migration 003 adds row-level `BEFORE` triggers** (SQLSTATE
   `NF001`) and a `workflow_versions.state` CHECK:
   - versions: identity columns fixed; `DRAFT -> PUBLISHED | DISCARDED`
     only; a published row may only become `RETIRED` (setting
     `retired_at`); `RETIRED`/`DISCARDED` rows are frozen; only drafts
     may be deleted;
   - task definitions and dependencies: insert/update/delete only while
     the parent version is `DRAFT`;
   - runs, task executions, attempts: no update once terminal; identity
     columns (run/version/task references, attempt number, dispatch id,
     attempt fencing token) fixed; task `fencing_token` never decreases;
   - outbox events: content fixed; a published event is frozen.
   The guards deliberately do **not** encode the full transition graph
   for tasks: D.6 (expired-lease reclaim `CLAIMED|RUNNING -> CLAIMED`,
   `RETRY_WAIT -> CLAIMED`) and D.12 (`RUNNING -> SKIPPED`) use edges
   that B.3's list omits, and choosing between them is a contract
   question, not an enforcement one. The full existing suite passes
   unchanged with the guards installed, confirming no application path
   relies on mutating a frozen row.
2. **Handler references are validated** against the built-in registry
   (the only handlers a Phase 1 worker can execute) at draft
   creation/update, in the validation report, and at publication.
3. **The worker marks an attempt `TIMED_OUT` only when its own timeout
   race fired**; a handler that returns the code `TIMEOUT` itself is
   still recorded as `FAILED`. The timeout timer is now cleared.
4. **Cross-tenant run launch returns 404**: the latest-version lookup is
   tenant-scoped, and a pinned version owned by another tenant is
   reported as not found.
5. **Path identifiers use `ParseUUIDPipe`** (400 on malformed input);
   the guard still runs first, so unauthenticated requests stay 401.
6. **Attempts are listed only through the owning run.**

## Consequences

- Operators can no longer "fix" terminal data with ad-hoc UPDATEs; that
  is intentional (Phase 2 manual retry would need an explicit, audited
  migration of these rules).
- Deployments must run migration 003. It validates existing rows
  against the new CHECK; only the four contract states are ever written.
- Clients that relied on 422 for cross-tenant launches or 500 for bad
  identifiers now get 404 / 400.
