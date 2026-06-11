# Procurement Parser

Product-oriented durable ETL service for:

- `eep.mitwork.kz`: plan items, notices, lots, and organizations from SSR HTML;
- `zakup.sk.kz`: lots and procurement notices through JSON APIs with
  browser-assisted sessions, CAPTCHA, and controlled fallbacks.

Russian documentation: [README.md](README.md). Architecture:
[ARCHITECTURE.en.md](ARCHITECTURE.en.md). Readiness:
[PRODUCT_READINESS.en.md](PRODUCT_READINESS.en.md).

## Quick Start

```powershell
Copy-Item .env.example .env
docker compose up -d postgres
$env:UV_CACHE_DIR=".uv-cache"
uv sync --frozen --extra dev
uv run playwright install chromium
uv run alembic upgrade head

uv run procurement-parser discover --source eep-mitwork --pages 2
uv run procurement-parser worker --source eep-mitwork --drain
uv run procurement-parser export --dataset all --output exports/all
uv run procurement-parser validate-export --input exports/all
```

PostgreSQL is exposed at `127.0.0.1:15432` by default.

## Execution Modes

Every main command accepts `--source eep-mitwork|zakup-sk|all`.

```powershell
# EEP only
uv run procurement-parser discover --source eep-mitwork --pages 0
uv run procurement-parser worker --source eep-mitwork --drain

# Zakup only
uv run procurement-parser discover --source zakup-sk --pages 0 `
  --runtime local --network direct --captcha manual
uv run procurement-parser worker --source zakup-sk --drain `
  --runtime local --network direct --captcha manual

# Both, with independent network profiles
uv run procurement-parser discover --source all --pages 0 `
  --eep-network direct --zakup-network residential --captcha 2captcha
uv run procurement-parser worker --source all --drain `
  --eep-network direct --zakup-network residential --captcha 2captcha
```

`discover` only enumerates list pages and enqueues identities. `worker`
extracts details, normalizes data, persists entities/relations, and enqueues
new relation targets.

Worker modes:

- default: long-running process;
- `--once`: one claimed batch;
- `--drain`: run until the selected source queue is stably empty;
- `--idle-grace-seconds`: empty-queue confirmation interval.

`--once` and `--drain` are mutually exclusive. Lease heartbeats protect long
Zakup requests. `SIGINT/SIGTERM` releases worker-owned leases; `SIGKILL` falls
back to lease expiration.

## Docker

```powershell
docker compose --profile eep up -d --build
docker compose --profile zakup up -d --build
docker compose --profile full up -d --build
```

Each source has a separate worker and scheduler. The profiles do not start a
historical full-site discovery. Stop safely with:

```powershell
docker compose --profile full stop -t 600
docker compose --profile full down -t 600
```

## Scheduler

Durable PostgreSQL-coordinated jobs:

- incremental catalog pages every 5 minutes;
- active entities every 15 minutes;
- recently closed entities daily for 14 days;
- old entities weekly;
- full reconciliation weekly.

Only incremental list discovery runs immediately on first startup. Heavy jobs
are initially deferred by their configured interval.

```powershell
uv run procurement-parser scheduler --source all `
  --eep-network direct --zakup-network residential --captcha 2captcha
```

Stop the scheduler before a finite `worker --drain` run.

## Configuration

Profiles are separated by dimension:

```text
config/
├── app.toml
├── database.toml
├── sources/{eep_mitwork,zakup_sk}.toml
├── network/{direct,static_proxy,public_pool,residential,mobile}.toml
├── captcha/{disabled,manual,2captcha}.toml
└── runtime/{local,server,aggressive_backfill,slow_internet}.toml
```

Precedence: CLI → environment/secrets → TOML → defaults.

Network profiles:

- `direct`: no proxy;
- `static_proxy`: one stable endpoint;
- `public_pool`: replaceable lanes from a generated test pool;
- `residential`: sticky endpoint with provider rotation API;
- `mobile`: sticky mobile endpoint with shorter TTL.

A lane binds proxy, fingerprint, User-Agent, cookies, browser storage, and
circuit-breaker state. Session material is never transferred between IPs.

```powershell
$env:PROXY_URL="http://user:password@host:port"
$env:PROXY_ROTATE_URL="https://provider.example/rotate"
uv run procurement-parser worker --source zakup-sk `
  --network residential --captcha 2captcha
```

Real public proxy lists and generated pools are ignored by Git.
`config/proxy_pools/public_pool.example.json` documents the format.

## CAPTCHA

Providers: `disabled`, `manual`, and `2captcha`.

```powershell
$env:TWOCAPTCHA_API_KEY="..."
uv run procurement-parser worker --source zakup-sk `
  --network residential --captcha 2captcha
```

Proxy-bound challenges use the same proxy and User-Agent as the Playwright
lane. Hourly cost and audit metadata are persisted in PostgreSQL under an
advisory lane lock. Tokens are not persisted. Manual fallback requires a
visible browser and interactive TTY.

Enable CAPTCHA integration only when legally authorized.

## Slow Internet and Zakup Bundle Cache

```powershell
uv run procurement-parser zakup-cache --runtime slow_internet --network direct
uv run procurement-parser zakup-spike --runtime slow_internet `
  --network direct --captcha manual
```

The profile uses 360-second browser timeouts and a 900-second lease. Chromium
disk cache and persistent profiles reduce repeat bundle downloads, but cache
validity remains controlled by the website and browser.

## CSV Export

Datasets: `lots`, `notices`, `plan_items`, `organizations`, `relations`,
`delivery_places`, `payment_terms`, and `documents`.

```powershell
uv run procurement-parser export --dataset all `
  --output exports/combined --layout combined
uv run procurement-parser export --dataset all `
  --output exports/split --layout split
uv run procurement-parser export --dataset all `
  --output exports/all --layout both
uv run procurement-parser export --dataset all `
  --output exports/excel --layout split --delimiter semicolon
uv run procurement-parser validate-export --input exports/all
```

CSV files use UTF-8 BOM. Validation checks headers, replacement characters,
duplicate identities, and combined/split row counts.
Comma remains the standard delimiter. Use `--delimiter semicolon` for Excel
installations whose regional settings expect semicolon-separated CSV.

## PostgreSQL and Operations

```powershell
uv run procurement-parser db-stats
uv run procurement-parser release-stale-leases
uv run procurement-parser prune-history --retention-days 90
docker compose exec postgres psql -U procurement -d procurement
```

Regular ingestion uses hash-aware UPSERT. Bulk replay keeps
`CREATE TEMP → COPY → UPSERT` on one physical connection.

Metrics use the embedded Prometheus HTTP server:

- EEP worker `9108`;
- Zakup worker `9109`;
- EEP scheduler `9110`;
- Zakup scheduler `9111`.

JSONL logs are written to `logs/` with task, worker, source, lane, proxy,
strategy, and attempt context. Secrets and session tokens are redacted.

## Verification

```powershell
$env:UV_CACHE_DIR=".uv-cache"
uv run ruff check .
$env:TEST_DATABASE_URL="postgresql+asyncpg://procurement:procurement@127.0.0.1:15432/procurement_test"
uv run pytest --cov=procurement_parser --cov-report=term-missing
docker compose --profile full config --quiet
docker build -t procurement-parser:local .
```

Integration-test database names must end with `_test`.
