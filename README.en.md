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
uv run procurement-parser export --dataset all `
  --output exports/raw --layout split --raw-csv
uv run procurement-parser validate-export --input exports/all
```

CSV files use UTF-8 BOM. Validation checks headers, replacement characters,
row width, spreadsheet formula-like values, duplicate identities, and
combined/split row counts. Formula-like values beginning with `=`, `+`, `-`, or
`@` are escaped by default for safe spreadsheet opening. Use `--raw-csv` only
for controlled machine-to-machine processing. Every export writes
`export_manifest.json` with generation time, mode, and per-file row counts.
Procurement datasets retain canonical UTC columns (`published_at`,
`application_start_at`, `application_end_at`) and also expose readable
`published_at_local`, `application_start_at_local`, and
`application_end_at_local` values for `source_timezone=Asia/Almaty`.
All files produced by one `all`/`both` run use one PostgreSQL `REPEATABLE READ`
snapshot. Each CSV and the manifest are published with an atomic replacement,
so an interrupted export does not leave a truncated final file.

Comma remains the standard delimiter. Use `--delimiter semicolon` for Excel
installations whose regional settings expect semicolon-separated CSV.

## PostgreSQL and Operations

```powershell
uv run procurement-parser db-stats
uv run procurement-parser release-stale-leases
uv run procurement-parser prune-history --retention-days 90
docker compose exec postgres psql -U procurement -d procurement
```

### Querying collected data

Use the `export_*` views for queries and analytics. They already join internal
`source_entity_fk` values to `source_entities` identities and match the CSV
dataset structures.

| View | Content |
| --- | --- |
| `export_lots` | lots |
| `export_procurement_notices` | EEP `/buy` notices and Zakup `advert` notices |
| `export_plan_items` | EEP `/point` plan items |
| `export_organizations` | organizations |
| `export_entity_relations` | directed entity relations |
| `export_delivery_places` | delivery places |
| `export_payment_terms` | payment terms |
| `export_documents` | document metadata and URLs without file downloads |

An entity is uniquely identified by
`(source, entity_type, source_entity_id)`. `source_entity_id` is the page/API
identifier, while `business_number` is the displayed procurement or lot
number; they are not required to match. Internal `source_entities.id` values
exist only for database foreign keys.

Main relation types are `plan_to_notice`, `plan_to_lot`, `notice_to_lot`,
`organizer`, and `customer`. Relations are directed from `parent_*` to
`child_*`. A missing relation is valid: an EEP plan item may not have a notice
yet, or a source link may point to a card that is no longer available.

Count persisted entities:

```sql
SELECT source, entity_type, count(*)
FROM source_entities
WHERE last_success_at IS NOT NULL
GROUP BY source, entity_type
ORDER BY source, entity_type;
```

Find the 50 notices with the shortest application period:

```sql
SELECT
    source,
    source_entity_id,
    title_ru,
    application_start_at_local AS start_local,
    application_end_at_local AS end_local,
    application_end_at - application_start_at AS duration,
    canonical_url
FROM export_procurement_notices
WHERE application_start_at IS NOT NULL
  AND application_end_at IS NOT NULL
  AND application_end_at > application_start_at
ORDER BY duration
LIMIT 50;
```

Retrieve lots belonging to one notice:

```sql
SELECT
    n.source,
    n.source_entity_id AS notice_id,
    n.title_ru AS notice_title,
    l.source_entity_id AS lot_id,
    l.business_number AS lot_number,
    l.title_ru AS lot_title,
    l.total_amount,
    l.currency,
    l.canonical_url
FROM export_procurement_notices n
JOIN export_entity_relations r
  ON r.parent_source = n.source
 AND r.parent_type = 'notice'
 AND r.parent_id = n.source_entity_id
 AND r.relation_type = 'notice_to_lot'
JOIN export_lots l
  ON l.source = r.child_source
 AND r.child_type = 'lot'
 AND l.source_entity_id = r.child_id
WHERE n.source = 'zakup-sk'
  AND n.source_entity_id = '1229637'
ORDER BY l.source_entity_id;
```

Retrieve complete EEP `plan → notice → lot` chains:

```sql
SELECT
    p.source_entity_id AS plan_id,
    p.title_ru AS plan_title,
    n.source_entity_id AS notice_id,
    n.title_ru AS notice_title,
    l.source_entity_id AS lot_id,
    l.title_ru AS lot_title
FROM export_plan_items p
JOIN export_entity_relations pn
  ON pn.parent_source = p.source
 AND pn.parent_type = 'plan_item'
 AND pn.parent_id = p.source_entity_id
 AND pn.relation_type = 'plan_to_notice'
JOIN export_procurement_notices n
  ON n.source = pn.child_source
 AND pn.child_type = 'notice'
 AND n.source_entity_id = pn.child_id
JOIN export_entity_relations nl
  ON nl.parent_source = n.source
 AND nl.parent_type = 'notice'
 AND nl.parent_id = n.source_entity_id
 AND nl.relation_type = 'notice_to_lot'
JOIN export_lots l
  ON l.source = nl.child_source
 AND nl.child_type = 'lot'
 AND l.source_entity_id = nl.child_id
WHERE p.source = 'eep-mitwork'
ORDER BY p.source_entity_id::bigint
LIMIT 100;
```

The `JOIN` clauses above return complete chains only. Query plan items that do
not have a notice yet separately:

```sql
SELECT
    p.source_entity_id AS plan_id,
    p.title_ru,
    p.status,
    p.canonical_url
FROM export_plan_items p
WHERE p.source = 'eep-mitwork'
  AND NOT EXISTS (
      SELECT 1
      FROM export_entity_relations r
      WHERE r.parent_source = p.source
        AND r.parent_type = 'plan_item'
        AND r.parent_id = p.source_entity_id
        AND r.relation_type = 'plan_to_notice'
  )
ORDER BY p.source_entity_id::bigint
LIMIT 100;
```

Retrieve lot documents or inspect source-specific JSON:

```sql
SELECT filename, category, extension, url, size_bytes
FROM export_documents
WHERE source = 'zakup-sk'
  AND entity_type = 'lot'
  AND source_entity_id = '4452106';

SELECT
    source_entity_id,
    source_payload ->> 'status' AS raw_status,
    jsonb_pretty(source_payload) AS raw_payload
FROM export_lots
WHERE source = 'zakup-sk'
  AND source_entity_id = '4452106';
```

UTC columns remain canonical. Columns ending in `_local` are calculated for
`source_timezone` and are intended for display and reporting. Use PostgreSQL
JSONB operators such as `->`, `->>`, and `@>` for source-specific fields.

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
