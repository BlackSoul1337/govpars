# Product Readiness Review

Audit date: June 11, 2026.

## Findings

### High

1. **The 10,000-entity production pilot is not a deterministic CI test.**

   Unit, integration, and migration checks are automated. A live pilot still
   depends on external websites, WAF behavior, CAPTCHA, and proxy quality.
   Before a public product-ready claim, record a run with at least 1,000
   entities per source and 10,000 total, including database integrity and CSV
   validation results.

2. **Zakup SK remains an unstable external transport boundary.**

   API, browser, interception, list DOM fallback, cache, lanes, and breakers
   are implemented. Frontend bundle, API, or WAF changes can still require
   request-profile updates.

### Medium

1. Public proxies are suitable for experiments, not production SLA.
2. Persistent Chromium profiles contain sensitive session state on disk.
   Production volumes require restricted access and disk encryption.
3. Zakup DOM fallback intentionally covers discovery only. Partial detail DOM
   is not allowed to overwrite complete API data.

### Low

1. Windows tests may emit a curl-cffi Proactor compatibility warning.
2. `public_pool.toml` intentionally expects a locally generated working pool.

## Verified

- Ruff passes.
- Unit and PostgreSQL integration tests pass.
- Coverage is at least 80%.
- Fresh Alembic `0001 → head` passes.
- Existing `0006 → head` passes.
- TEMP staging remains on one physical connection with pool size greater than
  one.
- Docker Compose full-profile validation passes.
- Single-source and combined CLI modes exist.
- CSV encoding, headers, duplicate identities, and combined/split counts are
  validated automatically.
- A direct live `--source all` smoke run passed on a clean database.

June 11, 2026 live smoke:

- discovery: 150 EEP and 20 Zakup initial tasks;
- relation expansion: 506 primary entities;
- outcomes: 496 success and one permanent 404 (0.2%);
- queue after drain: zero;
- stale leases: zero;
- duplicates, orphans, and missing raw payload: zero;
- exports: 998 relations and 1,833 documents;
- combined/split CSV validation: valid.

Live acceptance is intentionally outside normal CI so an external WAF cannot
make builds nondeterministic.

Public-pool verification:

- 10,006 free proxy candidates normalized;
- 27 passed the lightweight Zakup probe;
- Zakup public-pool discovery/worker: 41 successes, zero queue;
- EEP public-pool discovery/worker: 438 successes, one permanent 404,
  zero queue;
- dynamic replacement confirmed: three initial Zakup lanes and three initial
  EEP lanes were replaced without restarting;
- public-pool database: zero duplicates, orphans, stale leases, or missing raw;
- proxy-smoke CSV: 24 combined/split files, valid.

This successful smoke does not make free proxies suitable for production SLA.

## Requirements Matrix

| Requirement | Status | Notes |
|---|---|---|
| Clean Architecture Lite | ready | Domain/application do not import infrastructure/entrypoints; enforced by an AST test |
| Vertical source slices | ready | EEP and Zakup are isolated |
| Durable ETL | ready | Discovery, queue, extraction, persistence, relations |
| PostgreSQL SSOT | ready | Data, queue, revisions, runtime and scheduler state |
| Single/all source modes | ready | discover, worker, run, scheduler |
| EEP HTTP parsing | ready | curl-cffi WAF fallback included |
| Zakup API/browser strategies | ready | Controlled list DOM fallback |
| Stable identity and graph | ready | URL ID separated from business number |
| Priority queue and bloat controls | ready | SKIP LOCKED, delete-on-success |
| Once/drain/heartbeats | ready | Graceful lease evacuation |
| Proxy lanes and rotation | ready | direct/static/public/residential/mobile |
| Persistent breakers | ready | session_lanes |
| Manual/2captcha | ready | DB budget, advisory lock, token disposal |
| Durable scheduler | ready | Refresh and weekly reconciliation |
| Embedded Prometheus | ready | No FastAPI/Granian |
| Eight Excel-safe CSV datasets | ready | combined/split/both, one snapshot per run, atomic publication |
| Hash-aware revisions | ready | changes/off |
| TEMP COPY replay | ready | One-connection integration test |
| Clean and upgrade migrations | ready | 0001 rewritten, 0006 path checked |
| Docker source profiles | ready | eep/zakup/full |
| CI and GHCR release | ready | Coverage, scans, SBOM, provenance |
| Direct live EEP/Zakup/all smoke | ready | 506 entities after relation expansion |
| Public proxy live smoke | ready | EEP and Zakup discovery/worker drain |
| Production proxy live smoke | not completed | No paid proxy credentials |
| 10k live pilot | not completed | Requires stable external access and credentials |

## Acceptance Runbook

1. Create a dedicated test/pilot database and run fresh migrations.
2. Run EEP direct discovery and drain.
3. Run EEP through the production proxy profile.
4. Run Zakup direct discovery and drain.
5. Run Zakup through residential/mobile proxies.
6. Run combined `--source all`.
7. Reach 10,000 entities with at least 1,000 per source.
8. Send `SIGTERM` during a long Zakup task and verify zero stale leases.
9. Repeat discovery and verify idempotency.
10. Exercise scheduler refresh and reconciliation.
11. Export all datasets with `--layout both`.
12. Run `validate-export`.
13. Verify zero duplicate identities, orphan relations, and stale leases.

Presentation criteria:

- zero duplicate identities;
- zero orphan relations;
- zero lost tasks;
- zero stale leases after graceful shutdown;
- zero queue depth after drain;
- raw payload on all primary entities;
- permanent failure rate at most 1%;
- valid CSV;
- independent EEP, Zakup, and combined runs.

## Conclusion

The codebase is ready for a controlled pilot and architecture demonstration.
A full production-ready claim still requires a recorded 10k live run and
production proxy/CAPTCHA credentials.
