# Procurement Parser Architecture

## Overview

The service uses Clean Architecture Lite, vertical source slices, Strategy, and
a durable PostgreSQL ETL flow:

```text
discover → enqueue → extract → normalize → persist → discover relations → reconcile
```

```text
domain: models, errors, ports
  ↑
application: discovery, workers, scheduler, validation
  ↑
infrastructure: EEP, Zakup, PostgreSQL, network, CAPTCHA
  ↑
entrypoints: Typer CLI
```

Application code depends on domain contracts, not source-specific
infrastructure exceptions. Construction is isolated in the factory.

## Why This Architecture

Full hexagonal architecture would add ceremony around one driving surface
(CLI/scheduler) and one primary driven system (PostgreSQL). Ports exist only at
replaceable boundaries: source adapters, frontier, repositories, sessions,
CAPTCHA, CSV, and runtime state.

Vertical source slices isolate site-specific transports, parsers, mappings,
WAF behavior, and session handling. A Zakup change does not destabilize EEP.

## Domain and Identity

Entities include PlanItem, ProcurementNotice, Lot, Organization, DeliveryPlace,
PaymentTerms, DocumentMetadata, and EntityRelation.

The stable identity is:

```text
(source, entity_type, source_entity_id)
```

Business numbers are separate because EEP URL IDs do not always match visible
lot numbers. EEP is stored as a graph:

```text
PlanItem (/point) → ProcurementNotice (/buy) → Lot (/lot)
```

Organizations are not automatically merged across sources by BIN.

## Source Strategies

EEP:

```text
httpx SSR HTML → selectolax
                 ↘ curl-cffi WAF/transport fallback
```

Playwright is intentionally absent from the normal EEP path:

- detail data is returned in SSR HTML at stable URLs;
- pagination uses ordinary query parameters;
- `httpx` avoids JavaScript, image, and font downloads, making it faster and
  considerably cheaper in RAM and CPU;
- saved HTML can be parsed deterministically by `selectolax` in offline tests;
- a browser would add latency and failure modes without exposing more data.

`curl-cffi` is only a transport fallback when TLS/browser impersonation is
required. Playwright should be introduced for EEP only if required data moves
to a JavaScript-only API or a browser-bound challenge appears.

Zakup:

1. CurlCffi API;
2. browser fetch;
3. network interception;
4. controlled DOM fallback for list discovery only.

Playwright captures legitimate request URL, method, body, and headers while
excluding transport-owned headers. Detail DOM fallback is intentionally
disabled to prevent partial data from overwriting full API entities.

Zakup needs the layered strategy because it is an Angular SPA. JavaScript
establishes the API contract and current request headers, while WAF state,
cookies, browser storage, fingerprint, and reCAPTCHA belong to one
`SessionIdentity`. Playwright therefore bootstraps the session and captures the
request actually sent by the browser.

Compatible API calls are then replayed through `curl-cffi` with TLS
impersonation because that is faster and less resource-intensive than a browser
navigation per card. Requests whose signature, token, timestamp, or body cannot
be reproduced remain on `BrowserFetchStrategy` or
`NetworkInterceptStrategy`. DOM parsing is restricted to emergency list
identity discovery because detail DOM can be collapsed, virtualized, or
incomplete.

Playwright is therefore a session broker and authoritative network-request
source, not the preferred HTML parser. Persistent Chromium profiles and disk
cache amortize the roughly 58 MB immutable SPA bundle between lane runs.
Cookies, storage, and cache are never transferred between different proxy
identities.

## Durable Queue

`crawl_frontier` contains only active work. Workers claim with:

```sql
ORDER BY priority DESC, available_at ASC, id ASC
FOR UPDATE SKIP LOCKED
```

Successful tasks are deleted from the frontier and summarized in append-only
history. Low-priority capacity is reserved for backfill.

Lease heartbeats run every `lease_seconds / 3`. Graceful shutdown releases all
process-owned leases; hard crashes rely on lease expiration.

`--drain` waits for zero depth, zero active leases, no relation-created tasks
during idle grace, and exhausted tasks moved to failed history.

## Scheduler

PostgreSQL stores durable scheduler job state and leases. Jobs cover
incremental lists, active entities, recently closed entities, old entities,
and weekly reconciliation.

Only incremental discovery runs immediately on first startup. Heavy jobs are
initially deferred by their interval. Docker runs separate EEP and Zakup
schedulers for failure isolation.

## Sessions, Proxies, and Breakers

A lane binds proxy/IP, fingerprint, User-Agent, cookies, browser storage,
generation, and circuit breaker. Session data is never transferred between
IPs.

After repeated `403/418/429`, the lane opens its breaker, delays tasks, refreshes
the profile, and later performs one half-open probe. Residential/mobile
profiles may invoke a provider rotation API. Lane/source breaker state is
persisted in PostgreSQL.

## CAPTCHA

Disabled, manual, and 2captcha providers implement one port. A proxy-bound
challenge uses the same proxy and User-Agent as the browser lane.

PostgreSQL advisory locks serialize solving per lane. Hourly cost is checked
from persisted audit rows. Tokens are applied and discarded; they are never
stored or logged. Manual fallback requires a visible browser and TTY.

## Persistence

PostgreSQL is the system of record for identities, normalized entities, raw
JSONB, relations, revisions, work history, failures, lanes, CAPTCHA audit, and
scheduler state.

Regular ingestion uses hash-aware batch UPSERT. Bulk replay holds one physical
connection and transaction for:

```text
CREATE TEMP ON COMMIT DROP → COPY FROM → deduplicate → UPSERT → COMMIT
```

CSV export uses `COPY TO STDOUT`, explicit column lists, and either the
standard comma delimiter or a semicolon for locale-dependent Excel imports.

## Observability

Long-running workers and schedulers expose Prometheus with the library's
embedded HTTP server. A PostgreSQL sampler runs every 15 seconds. Structlog
writes JSONL with run/task/worker/source/lane/proxy/strategy context while
redacting secrets and session material.

## Rejected Components

Redis/Celery are unnecessary because PostgreSQL already provides durable work
claims, leases, history, and recovery. Adding them would create a second
stateful consistency boundary.

FastAPI/Granian are omitted because V1 exposes no business HTTP API. The
embedded Prometheus server and CLI cover the required interfaces.

## Deployment

Recommended topology:

```text
PostgreSQL
EEP worker + EEP scheduler
Zakup worker + Zakup scheduler
Prometheus
```

Local `--source all` acts as a supervisor over two independent pipelines and
uses one aggregate metrics port.
