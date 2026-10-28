# ADR 002: API-key authentication and Kafka SASL/SSL

- Status: Accepted
- Date: 2026-10-28
- Scope: control-plane authentication and transport security only. No
  architecture-contract change: workflow state machines, transactions,
  outbox/relay/claim/lease semantics, and tenant-scoped queries are
  untouched. Authn/z was explicitly requested (it sits in the README
  roadmap); this is the smallest form that closes the public-API
  impersonation hole without new dependencies, services, or tables.

## Context

The PR #4 pre-merge review found two release blockers: (1) the public
API trusted the self-asserted `X-Tenant-Id` header, so any anonymous
caller could read and mutate any tenant's workflows; (2) the Kafka
client was plaintext/anonymous-only while every named managed broker
(Confluent Cloud, Upstash, Redpanda Cloud) requires SASL_SSL.

## Decision

1. **API keys bound to tenants, configured via environment.**
   `NEXUSFLOW_API_KEYS` holds comma-separated
   `name:secret:tenant-UUID` entries (secrets >= 16 chars, unique per
   key; names unique; tenants normalized to lowercase UUIDs). The
   api/all roles refuse to boot without at least one key (fail fast,
   no insecure mode). Secrets never appear in error messages or logs.

2. **Bearer guard + server-side tenant binding.** `ApiKeyAuthGuard`
   accepts `Authorization: Bearer <secret>` (constant-time lookup over
   SHA-256 digests) and attaches only `{ name, tenantId }` to the
   request. `TenantIdParam` resolves the tenant exclusively from the
   key; `X-Tenant-Id` is never trusted — when present it must match
   (403 on mismatch, 422 when malformed). Requests that never passed
   the guard are rejected. `/health` and `/health/ready` stay public
   so platform health checks (which carry no credentials) keep working;
   they expose no tenant data.

3. **Kafka TLS + SASL passthrough with fail-fast validation.**
   `KAFKA_SSL` plus `KAFKA_SASL_MECHANISM` (`plain`,
   `scram-sha-256`, `scram-sha-512`) / `_USERNAME` / `_PASSWORD` are
   passed to KafkaJS (`ssl: true` verifies against system CAs).
   Partial credentials, unknown mechanisms, credentials without a
   mechanism, and SASL-without-TLS (passwords must never travel over
   plaintext) are all boot errors. Secrets live in environment
   variables / dashboard-prompted blueprint values only.

## Alternatives considered

- JWT/OIDC: stronger lifecycle (expiry, rotation, scopes) but needs
  new dependencies, an issuer/JWKS story, and key management — beyond
  the smallest fix for the impersonation hole. Remains the roadmap
  direction (see README Roadmap).
- Private API (`type: pserv`) instead of auth: kept as the documented
  fallback if auth cannot be operated, but auth was implementable
  within the existing architecture so the public blueprint stands.
- SASL without TLS for self-hosted `SASL/PLAINTEXT` brokers:
  rejected — the supported posture is SASL_SSL only.

## Consequences

- Anonymous cross-tenant access is closed: every workflow/version/run
  request is authenticated and tenant-bound; 401/403 use the stable
  error envelope.
- Key rotation means updating `NEXUSFLOW_API_KEYS` and redeploying;
  there are no per-key scopes or expiry (documented limitation).
- Local demos/tests configure their own dev keys; `npm run demo`
  does so automatically.
- The Kafka security options are unit-tested (config matrix +
  failure propagation with a mocked client) but still unverified
  against a live broker — tracked in the README, not claimed.
