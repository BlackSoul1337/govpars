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
uv run procurement-parser discover --source all --pages 0 --no-resume `
  --eep-network direct --zakup-network residential --captcha 2captcha
uv run procurement-parser worker --source all --drain `
  --eep-network direct --zakup-network residential --captcha 2captcha
```

`discover` only enumerates list pages and enqueues identities. `worker`
extracts details, normalizes data, persists entities/relations, and enqueues
new relation targets.

EEP discovers `lots`, `buys`, and `points` in parallel windows. All three
catalogs share one source-level semaphore: the default is 6 concurrent list
requests for direct access and 10 for proxy profiles. Zakup discovery remains
strictly sequential (`1`) because requests are bound to browser/session lanes.

```powershell
# Use the source profile default
uv run procurement-parser discover --source eep-mitwork --pages 100

# Temporary EEP-only override
uv run procurement-parser discover --source all --pages 100 `
  --discovery-concurrency 3 `
  --eep-network direct --zakup-network direct
```

`--discovery-concurrency` accepts `1..64`; with `--source all` it never changes
Zakup. A window is fetched and parsed completely, sorted by page, and persisted
with one sequential enqueue. The first empty page terminates the catalog.
Failures from already-issued speculative pages after that terminal page are
ignored; a failure before it rejects the whole window without advancing the
checkpoint.

The console emits `discovery_window_started`, a
`discovery_window_progress` heartbeat every five seconds, and
`discovery_window_complete`. While a window is downloading it has not yet been
enqueued; this is expected atomic-window behavior rather than a hang.

Worker modes:

- default: long-running process;
- `--once`: each internal worker claims at most one batch, so the process can
  handle up to roughly `workers × batch_size` cards rather than one card;
- `--drain`: process the selected source, including relation-discovered tasks,
  until queue depth and active leases stay at zero for the idle grace period;
- `--idle-grace-seconds`: empty-queue confirmation interval.

`--once` and `--drain` are mutually exclusive. With `--source all`, EEP and
Zakup drain independently in parallel. Stop the scheduler before a finite
drain, otherwise it can refill the queue. Lease heartbeats protect long Zakup
requests. `SIGINT/SIGTERM` releases worker-owned leases; `SIGKILL`, an OS crash,
or power loss falls back to `leased_until` expiration.

## Data completeness

Data is stored at two levels:

1. normalized columns for querying, relations, and CSV;
2. the complete received source response in the `source_payload` JSONB column.

Zakup keeps the original API JSON. EEP keeps extracted SSR fields, tables,
views, and the page title. The normalized layer covers identities, business
numbers, titles, descriptions and characteristics, statuses, procurement
method and subject type, amounts, quantities, units, application dates,
delivery, payment, contacts, organizations, document metadata, and relations.

"Complete" means everything returned by the public endpoint or page used by
the parser, not hidden internal data or document binaries. Missing source
fields remain `NULL`. For example, a Zakup notice often has no description
because descriptions belong to its child lots, and a publication timestamp is
not inferred from the `PUBLISHED` status alone. Source-specific fields without
a shared EEP/Zakup meaning remain available in `source_payload`.

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

### Quick catalog estimate

This command only performs sample list requests and does not write to PostgreSQL:

```powershell
uv run procurement-parser catalog-stats --source all --samples 1 `
  --eep-network direct --zakup-network direct --captcha manual
```

Compare several profiles and probe `extract → parse → relations`:

The safest form for every shell is a single line:

```text
uv run procurement-parser catalog-stats --source all --samples 1 --worker-samples 2 --runtime-profiles local,slow_internet --network-profiles direct,public_pool --captcha disabled --progress-interval-seconds 5 --probe-timeout-seconds 180
```

PowerShell uses a backtick at the end of each continued line:

```powershell
uv run procurement-parser catalog-stats --source all --samples 1 `
  --worker-samples 2 `
  --runtime-profiles local,slow_internet `
  --network-profiles direct,public_pool `
  --captcha disabled `
  --progress-interval-seconds 5 `
  --probe-timeout-seconds 180
```

Bash uses a backslash:

```bash
uv run procurement-parser catalog-stats --source all --samples 1 \
  --worker-samples 2 \
  --runtime-profiles local,slow_internet \
  --network-profiles direct,public_pool \
  --captcha disabled \
  --progress-interval-seconds 5 \
  --probe-timeout-seconds 180
```

Progress is written to stderr as `START`, `WAIT`, `DONE`, `FAIL`, and `SKIP`
messages every 10 seconds. Change it with `--progress-interval-seconds 30` or
disable it with `--no-progress`. The final JSON remains on stdout. Each
catalog/detail probe is limited to 600 seconds; change it with
`--probe-timeout-seconds`.

Save the complete report directly when terminal scrollback is limited:

```powershell
uv run procurement-parser catalog-stats --source all --samples 1 --worker-samples 2 --output reports/catalog-stats.json
```

The result includes total catalog counts, response time, list discovery speed,
an estimated discovery duration, and sample detail worker throughput. The
worker probe does not write to PostgreSQL and therefore excludes queue and
UPSERT cost. `full_run_forecasts` reports the combined `discovery + detail`
estimate for each source/runtime/network combination. Retries, UPSERT,
reconciliation, and duplicate relation discovery are excluded.
For EEP, `catalog-stats` uses the configured parallel window:
`elapsed_seconds` is wall-clock time, while `sequential_elapsed_seconds` and
`estimated_discovery_seconds_sequential` preserve a sequential baseline.
For `--source all`, `combined_forecasts.parallel_wall_clock` is the expected
parallel wall time, while `sequential_total` represents running sources one
after another.

### Discovery checkpoints

`--resume` continues from the PostgreSQL checkpoint `next_page` and exits
immediately when the full scope is already marked completed. `--no-resume`
ignores the checkpoint as a starting position and begins at page one, or at
`--start-page`. It neither clears the database nor forces already successful
detail cards to be downloaded again: the repeated list scan updates summaries
and queues new or previously unfinished IDs.

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

Raw public proxy lists and generated test pools are ignored by Git.
`config/proxy_pools/public_pool.example.json` documents the format. The
repository also contains `config/proxy_pools/public_working.json`, validated on
June 11, 2026. These free public proxies have no availability, safety, or IP
stability guarantees. Revalidate them before every run and use a paid sticky
residential/mobile pool for a full backfill.

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
