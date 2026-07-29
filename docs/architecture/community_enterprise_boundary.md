# Community / Enterprise Boundary

The proposed Open Core split, validated against the current code. **This is a
design/hypothesis document, not a decision** — no functionality has been moved behind
a license and the license itself is unchanged.

## 1. Boundary by area

| Area | Community | Enterprise |
|---|---|---|
| Authentication | API keys, fail-closed startup, per-service registries | OIDC, SAML, LDAP, service-identity federation |
| Authorization | fixed `service`/`operator` roles | projects, teams, custom roles, policy rules, dynamic RBAC |
| Audit | durable **technical** append-only events + values-free lineage + availability-change table | compliance retention, signing, immutable export, central search |
| Deployment | Compose + documented external dependencies | operator/Helm, multi-cluster, fleet management |
| Reliability | replay, retries, documented rebuild-from-offline | automated HA, failover, DR orchestration |
| Governance | versions + immutable bundles + approval **records** | approval **enforcement**, ownership policies, segregation of duties, managed promotion gates |
| Secrets | env / DSN (small-deploy appropriate) | Vault/KMS integration |
| Scale mgmt | single-node, `--scale worker=N` | quotas, multi-tenancy, isolated per-team execution |
| Serving/compute | full correctness, PIT, replay, model-as-feature | (nothing — never differentiated by tier) |

## 2. Challenge to the proposed extension defaults

The task proposed `AuthProvider=StaticTokenAuth`, `AuditSink=NoOpAuditSink`,
`PolicyEngine=AllowAllPolicy`, `SecretProvider=EnvSecretProvider`. Traced against the
code, **three of the four proposed Community defaults are wrong** — they would weaken
the current baseline:

| Proposed default | Verdict | Why |
|---|---|---|
| `AuthProvider = StaticTokenAuth` | **REJECT** — keep the current fail-closed API-key auth | The platform already ships opaque keys, constant-time compare, roles, startup validation and fail-closed mode. "StaticTokenAuth" as a permissive default would be a regression. Community default = the existing API-key provider. |
| `PolicyEngine = AllowAllPolicy` | **REJECT** — keep fixed service/operator roles | AllowAllPolicy defaults to *no* authorization. Community must keep the existing two-role enforcement. Enterprise adds dynamic RBAC *on top*, never by relaxing the default to allow-all. |
| `AuditSink = NoOpAuditSink` | **REJECT** — keep the durable technical audit | Community already has durable `request_events`, `raw_report_availability_changes`, and values-free lineage. A No-Op default would silently drop existing evidence. Enterprise adds a compliance **exporter/reader** over those durable tables. |
| `SecretProvider = EnvSecretProvider` | **ACCEPT** for Community | Env-based secrets (DSN, MinIO keys, `FSP_API_KEYS`) are appropriate for dev and small production. Enterprise adds Vault/KMS. This is the one proposed default that matches the invariant. |

Lesson encoded: a permissive plugin default is not a neutral default — it is a
downgrade of a shipped guarantee. Community defaults must equal today's fail-closed
behavior.

## 3. NEVER PAYWALL

Correct feature computation · PIT correctness · `available_at` semantics · batch/online
consistency · basic batch execution · basic online serving · offline/online stores ·
replay/idempotency correctness · basic (technical) lineage · basic failure evidence ·
security patches · baseline authentication · baseline authorization · health
endpoints · core metrics · public tests for core guarantees · no artificial
feature/entity/row/throughput limits.

## 4. VALID ENTERPRISE VALUE (adds, never repairs)

Identity federation (OIDC/SAML/LDAP) · dynamic RBAC + custom roles · projects/teams/
namespaces · multi-tenancy · quotas · approval-workflow **enforcement** + segregation
of duties · compliance-grade audit export (immutable/signed/retained/searchable) ·
Vault/KMS integration · HA/failover/DR automation · fleet/operator deployment ·
isolated per-team feature execution · managed promotion gates · admin console (rich) ·
support + LTS.

## 5. CUSTOMER-SPECIFIC SERVICES (professional services, not product tiers)

Bespoke source connectors · one-off DWH extraction pipelines · customer deployment
repositories · migration/onboarding · custom compliance mappings · bespoke dashboards.
These are billed engagements against the project roadmap, **not**
gated product features — do not confuse services with Enterprise product value.

## 6. License-activation rule (where license logic may live)

A license may **activate** private components (enable OIDC, dynamic RBAC, compliance
exporter, multi-tenant namespace manager, Enterprise admin service). A license may
**never** alter core correctness: it must not reduce throughput, limit feature/entity
counts, disable correct PIT, return stale/incorrect values, remove security patches,
make replay unsafe, or require online registration for Community startup.

License validation belongs in the **private Enterprise package** and/or a private
**admin/control-plane** component — **never in public ComputeCore or any data path**.
The audit found zero license checks in the current code; that property must be
preserved.

## 7. Why this split is architecturally safe today

- `fs_core` imports nothing from `api`; nothing imports "enterprise" (verified).
- Providers are Protocol-typed and settings/DI-selected (`AppBackend`,
  `create_app(backend, security)`), so Enterprise substitutes implementations without
  editing core — with **one** real gap: adding HTTP routes needs a router seam
  (`extension_point_audit.md` §6). Auth-strategy substitution needs a small
  `AuthProvider` extraction, also localized.
- No correctness guarantee needs to move to make Enterprise valuable — Enterprise
  value is entirely organizational/operational/compliance/scale.
