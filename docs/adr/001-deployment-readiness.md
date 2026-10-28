# ADR 001: Deployment readiness fixes and assets

- Status: Accepted
- Date: 2026-10-27
- Scope: deployment readiness only. No architecture-contract change:
  PostgreSQL remains authoritative, the outbox/scheduler/worker/relay
  design is untouched, and no roadmap items (cancellation,
  `IGNORE_FAILURE`, Redis, authn/z) are introduced.

## Context

A pre-deployment audit of the MVP found six gaps between the
implementation and what a production deploy needs: tenant isolation on
version endpoints, readiness status semantics, the `memory` transport
guard, migration discovery/packaging, managed-PostgreSQL TLS support,
and missing deployment files. This record captures what was decided
for each and why.

## Decision

1. **Version endpoints are tenant-scoped (fix A).**
   `GET /versions/:id`, `PUT .../definition`,
   `POST .../validate|publish|retire`, and version listing now resolve
   the tenant from `X-Tenant-Id` and scope every read and mutation to
   it (`WorkflowService.assertVersionTenant`), matching the
   tenant-scoped behavior of workflows and runs. Cross-tenant access
   returns 404 (indistinguishable from missing), never another
   tenant's data.

2. **Readiness is 200 only when PostgreSQL is reachable (fix B).**
   `GET /health/ready` returns 200 `ready` when the database check
   passes and 503 `not_ready` when it fails, so orchestrators, health
   checks, and load balancers remove unready instances from rotation
   instead of routing to a process that cannot serve. The response
   shape is pinned by the exported `ReadinessBody` interface.

3. **The `outbox-relay` role rejects the `memory` transport (fix C).**
   Like `worker`, a standalone relay must use Kafka: with `memory`, a
   separate relay would publish into the void while marking events
   published (silent non-delivery). `memory` stays valid for
   single-process `api`/`all` roles, local demos, and tests.

4. **Migrations are packaged with, and runnable from, compiled output
   (fix D + deployment assets).** The build copies `*.sql` next to the
   compiled applier (`nest-cli.json` assets); the applier resolves its
   own directory in both source and `dist` layouts. The migration CLI
   moved from `scripts/migrate.ts` to `src/migrate.ts` so `nest build`
   compiles it: `npm run migrate` (dev, via ts-node) and
   `npm run migrate:prod` (production, `node dist/migrate.js`, no dev
   dependencies) share one source. Verified end to end: compiled
   migrate applies both migrations to real PostgreSQL 18 and
   idempotently skips on re-run.

5. **PostgreSQL TLS is supported on every connection path (fix E).**
   `DATABASE_SSL=true/false` wins when set; otherwise TLS is
   auto-detected from `sslmode`/`ssl` URL parameters (managed
   providers commonly use `?sslmode=require`). The app pool, the
   migration applier, and the boot-time reachability check
   (`main.ts`, previously a bare non-TLS pool that would have failed
   startup against TLS-mandating providers) all use the same
   resolution. TLS uses `{ rejectUnauthorized: false }`, matching the
   managed-provider convention where the platform terminates TLS with
   certs the container cannot verify out of the box.

6. **Deploy convention: single migrator, external Kafka.**
   `Dockerfile` (multi-stage, one image for all roles, selected by
   `NEXUSFLOW_ROLE`) and `render.yaml` (managed PostgreSQL + one
   service per role, health check on `GET /health/ready`, paid worker
   plans, shutdown grace on worker/relay) are provided. `DATABASE_SSL`
   is deliberately unset: the blueprint injects Render's internal URL
   (private network, TLS optional), and forcing TLS crashes boot when
   the server does not offer it; external endpoints keep working via
   `DATABASE_SSL=true` / `?sslmode=require`. Migrations run once per
   deploy
   from a single process (the `api` service's `preDeployCommand`) —
   concurrent migrates are not coordinated. Kafka stays external to
   the blueprint (Render has no managed Kafka); multi-process
   operation still requires a real broker, which remains unverified
   (see README Limitations).

## Consequences

- Tenant isolation is uniform across workflows, versions, and runs;
  new endpoints must follow the same header-plus-scope pattern.
- 503 readiness changes the health contract: dependents must treat
  non-200 as unready (the README API table was updated).
- `memory` + `outbox-relay` is now a boot error, not silent
  non-delivery; single-process demos are unaffected.
- Production deploys gain a working, verified migration step with no
  dev dependencies in the runtime image.
- Managed PostgreSQL works via `DATABASE_SSL=true` or
  `?sslmode=require`, on the app, migrate, and boot-check paths.
- The Docker image and Render blueprint are reviewed and
  syntax-validated but not executed against real infrastructure from
  here; first real deploy (and real-Kafka verification) is still open
  work, honestly tracked in the README.
