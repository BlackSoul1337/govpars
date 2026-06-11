# Procurement Parser (archived documentation)

Durable ETL parser for:

- `eep.mitwork.kz`: plans, notices, lots, and organizations over SSR HTML.
- `zakup.sk.kz`: lots and adverts over public JSON endpoints with browser-assisted
  session capture when the WAF rejects direct requests.

## Quick start

```powershell
Copy-Item .env.example .env
docker compose up -d postgres
uv sync --extra dev
uv run playwright install chromium
uv run alembic upgrade head
uv run procurement-parser discover --source eep-mitwork --entity-type lot
uv run procurement-parser worker --source eep-mitwork
```

Local PostgreSQL is published on `127.0.0.1:15432` by default to avoid common
Windows conflicts on port `5432`. Override `POSTGRES_PORT` and `DATABASE_URL`
together when another port is required.

The complete Docker stack includes the migration gate, both workers, and the
scheduler:

```powershell
docker compose --profile workers up --build
```

This command does not start a historical full-site discovery. It drains the
durable queue already present in PostgreSQL and starts the scheduler. The
scheduler refreshes page 1 for each supported catalog every five minutes. Run
`discover --pages 0` explicitly for a full catalog walk.

Start only one source worker:

```powershell
docker compose --profile workers up --build worker-eep
docker compose --profile workers up --build worker-zakup
```

Start both workers without the scheduler:

```powershell
docker compose --profile workers up --build worker-eep worker-zakup
```

EEP and Zakup may use different network and runtime profiles in Docker:

```powershell
$env:EEP_NETWORK_PROFILE = "direct"
$env:ZAKUP_NETWORK_PROFILE = "public_pool"
$env:EEP_NETWORK_MAX_LANES = "8"
$env:ZAKUP_NETWORK_MAX_LANES = "4"
$env:EEP_WORKER_COUNT = "16"
$env:ZAKUP_WORKER_COUNT = "12"
$env:ZAKUP_BROWSER_LANES = "2"
docker compose --profile workers up --build
```

`config/` and `logs/` are mounted from the project directory. Zakup Chromium
profiles and its bundle cache are kept in the `zakup_browser_data` Docker volume,
so a container recreation does not force another full bundle download.

The scheduler also accepts independent network profiles:

```powershell
uv run procurement-parser scheduler `
  --eep-network direct `
  --zakup-network public_pool `
  --captcha 2captcha
```

Profiles are independent:

```powershell
uv run procurement-parser run `
  --source zakup-sk `
  --runtime local `
  --network residential `
  --captcha 2captcha
```

Local runtime and proxy selection are independent. For example:

```powershell
$env:PROXY_URL = "http://user:password@proxy.example:8000"
uv run procurement-parser zakup-spike `
  --runtime local `
  --network residential `
  --captcha manual
```

For catalog-specific statuses or date ranges, pass source API filters explicitly:

```powershell
uv run procurement-parser discover `
  --source zakup-sk `
  --entity-type notice `
  --filters-json '{"adst":"PUBLISHED","lst":"PUBLISHED"}' `
  --priority 100
```

Use `--pages 0` for a full catalog walk. Numeric entity IDs are never scanned
blindly; detail tasks come from catalog pages and discovered relations.

For a very slow Zakup connection, use the temporary runtime profile with
six-minute Playwright navigation, capture, and API-response timeouts:

```powershell
uv run procurement-parser discover `
  --source zakup-sk `
  --entity-type lot `
  --runtime slow_internet `
  --network direct `
  --captcha manual `
  --pages 2
```

The `slow_internet` profile also downloads the current approximately 58 MB
`main-es2015.*.js` bundle in resumable 1 MB ranges into
`playwright/.cache/zakup`. An interrupted `.part` file is continued on the next
run. Once complete, Playwright serves that exact hashed bundle from disk.
Third-party analytics are blocked so they do not compete with the portal bundle.
The regular persistent Chromium cache remains enabled for other runtime profiles.

### Public proxy test pool

Public proxy lists are untrusted and short-lived. Build a normalized candidate
pool, then validate it against Zakup before use:

```powershell
uv run procurement-parser proxy-pool-build `
  --structured-json C:\path\to\free-proxy-list.json `
  --generic-json C:\path\to\proxies.json `
  --spys-text C:\path\to\spys-list.txt

uv run procurement-parser proxy-pool-check --limit 100
$env:PROXY_POOL_INDEX = "0"
uv run procurement-parser discover --source zakup-sk --entity-type all `
  --pages 15 --network public_pool --captcha manual --runtime local
```

`PROXY_POOL_INDEX` selects the first entry from the validated pool. Up to
`max_lanes` proxies are assigned to independent sticky lanes and used
round-robin. A lane with an open circuit breaker is skipped until its cooldown
expires. Each proxy gets a separate Chromium profile so cookies and CAPTCHA
state are not transferred between IP addresses.

The bundled public profile defaults to four active lanes deliberately. Four is
not a parser limit: it is a conservative starting point because each Zakup lane
can own a Chromium process/profile, while free proxies are unstable and often
share upstream capacity. Raising the number can reduce throughput by increasing
timeouts, CAPTCHA frequency, RAM use, and WAF pressure. Set
`NETWORK_MAX_LANES` locally or `ZAKUP_NETWORK_MAX_LANES` in Docker after checking
CPU, memory, proxy success rate, and response latency. EEP lanes are lightweight
`httpx` clients without Chromium, so `EEP_NETWORK_MAX_LANES` can usually be
higher.

The bundle can be prepared separately and resumed as many times as needed:

```powershell
uv run procurement-parser zakup-cache --runtime slow_internet --network direct
```

Secrets are read from environment variables. They are never stored in TOML files.
See `.env.example` for database, proxy, rotation, and 2captcha variables.

## Discover And Worker

`discover` reads catalog/list pages only. It stores list summaries, creates
`source_entities`, and enqueues detail tasks in `crawl_frontier`. It does not
fully parse detail pages.

`worker` claims existing tasks from PostgreSQL, opens each detail page, normalizes
and persists its data, then enqueues newly discovered relations such as notices
and organizations. It continues until stopped. `--once` processes one claimed
batch per worker and exits.

The worker does not independently enumerate the whole site. It processes the
durable graph frontier produced by `discover` and by previously processed detail
pages. Therefore, its total work can exceed the initial discovery count.

Supported discovery entity types:

- EEP: `lot`, `notice`, `plan_item`, or `all`.
- Zakup SK: `lot`, `notice`, or `all`.
- `organization` is relation-discovered and is not a standalone EEP catalog.

Examples:

```powershell
uv run procurement-parser discover --source eep-mitwork --entity-type lot --pages 6
uv run procurement-parser worker --source eep-mitwork
uv run procurement-parser worker --source eep-mitwork --once
```

### Worker command reference

`worker` does not accept `--pages` or `--entity-type`: those options belong to
`discover`. A worker processes all queued entity types for the selected source.

EEP, direct connection, continuous processing:

```powershell
uv run procurement-parser worker `
  --source eep-mitwork `
  --runtime local `
  --network direct `
  --captcha disabled
```

Zakup, direct connection with manual CAPTCHA fallback:

```powershell
uv run procurement-parser worker `
  --source zakup-sk `
  --runtime local `
  --network direct `
  --captcha manual
```

`manual` opens a visible Chromium window unless `PLAYWRIGHT_HEADLESS` is
explicitly set. The browser is visible from launch; an already running headless
browser cannot be converted to headed mode after CAPTCHA detection. Solve the
challenge in that window and press Enter in the terminal. Do not set
`PLAYWRIGHT_HEADLESS=1` with manual CAPTCHA.

Process one claimed batch per worker and exit:

```powershell
uv run procurement-parser worker `
  --source zakup-sk `
  --runtime local `
  --network direct `
  --captcha manual `
  --once
```

Zakup through the validated public pool:

```powershell
$env:PROXY_POOL_INDEX = "0"
uv run procurement-parser worker `
  --source zakup-sk `
  --runtime local `
  --network public_pool `
  --captcha manual
```

Zakup through one static proxy:

```powershell
$env:PROXY_URL = "http://user:password@proxy.example:8000"
uv run procurement-parser worker `
  --source zakup-sk `
  --runtime local `
  --network static_proxy `
  --captcha 2captcha
```

Zakup through a residential or mobile gateway with provider-side IP rotation:

```powershell
$env:PROXY_URL = "http://user:password@gateway.example:8000"
$env:PROXY_ROTATE_URL = "https://provider.example/api/rotate?token=secret"

uv run procurement-parser worker `
  --source zakup-sk `
  --runtime server `
  --network residential `
  --captcha 2captcha
```

Replace `residential` with `mobile` for the mobile profile. These profiles use
one sticky lane by default. When the lane reaches the block threshold or its
sticky TTL expires, the rotation endpoint is called and the browser session is
recreated without transferring cookies.

### Runtime profiles

| Profile | Workers | Claim batch | Browser lanes | Intended use |
| --- | ---: | ---: | ---: | --- |
| `local` | 4 | 10 | 2 | Normal local development |
| `slow_internet` | 2 | 5 | 1 | Slow links and resumable Zakup bundle |
| `server` | 12 | 25 | 2 | Continuous server processing |
| `aggressive_backfill` | 24 | 50 | 4 | Controlled full backfill |

Network `max_lanes` can further limit browser concurrency. Throughput is also
constrained by proxy quality, PostgreSQL latency, CAPTCHA, and queue depth.
For direct Zakup parsing, `BROWSER_LANES` controls the number of parallel
Chromium lanes; `WORKER_COUNT` controls task consumers. For proxy pools,
`NETWORK_MAX_LANES` controls active proxy lanes. Increasing workers above
available lanes does not make browser-bound parsing faster.

`WORKER_COUNT`, `BROWSER_LANES`, and `NETWORK_MAX_LANES` override every selected
TOML profile when they are present in `.env`. Leave them unset to use the values
from `config/runtime/*.toml` and `config/network/*.toml`.

### Network profiles

| Profile | Required settings | Rotation |
| --- | --- | --- |
| `direct` | None | No |
| `static_proxy` | `PROXY_URL` or `PROXY_POOL_FILE` | Pool reserve only |
| `public_pool` | `config/proxy_pools/public_working.json` | Automatic reserve replacement |
| `residential` | `PROXY_URL`, optional `PROXY_ROTATE_URL` | Provider API and sticky TTL |
| `mobile` | `PROXY_URL`, optional `PROXY_ROTATE_URL` | Provider API and sticky TTL |

For a private static pool, create a JSON file:

```json
{
  "proxies": [
    {"url": "http://user:password@proxy-1.example:8000"},
    {"url": "socks5://user:password@proxy-2.example:1080"}
  ]
}
```

Then select it without changing TOML:

```powershell
$env:PROXY_URL = ""
$env:PROXY_POOL_FILE = "C:\proxy\my-pool.json"
$env:PROXY_POOL_INDEX = "0"
$env:NETWORK_MAX_LANES = "4"

uv run procurement-parser worker `
  --source zakup-sk `
  --runtime server `
  --network static_proxy `
  --captcha 2captcha
```

Each lane owns its proxy, Chromium profile, cookies, request profiles, and
CAPTCHA state. After repeated block or transport failures, the lane is replaced
with the next non-active proxy from the reserve pool. A failed proxy enters
cooldown and is not immediately reused. Rotation never changes IP inside a live
browser session.

EEP uses the same pool/reserve/cooldown and provider-rotation settings, but its
lanes contain only `httpx` connection pools. Direct EEP requests remain
concurrent; a proxy pool spreads requests across up to `max_lanes` addresses.

## Operations

```powershell
uv run procurement-parser db-stats
uv run procurement-parser release-stale-leases
uv run procurement-parser release-stale-leases --source zakup-sk
uv run procurement-parser prune-history --days 90
uv run procurement-parser export --dataset lots --output exports/lots.csv
uv run procurement-parser export --dataset all --output exports
uv run procurement-parser export --dataset lots --layout split --output exports
uv run procurement-parser export --dataset all --layout both --output exports/all
```

In `db-stats`, `attempted` means "claimed at least once", not "failed".
`leased` is currently being processed, `stale_leases` is reclaimable after an
unclean stop, `delayed` is waiting for `available_at`, and `with_error` has a
recorded retry error.

Available export datasets are `lots`, `notices`, `plan_items`, `organizations`,
`relations`, `delivery_places`, `payment_terms`, and `documents`. Export
layouts:

- `combined`: one CSV containing EEP and Zakup rows;
- `split`: separate `*_eep_mitwork.csv` and `*_zakup_sk.csv` files;
- `both`: combined and split variants together.

CSV files use UTF-8 with BOM for Excel compatibility.

Inspect PostgreSQL from the running container:

```powershell
docker compose exec postgres psql -U procurement -d procurement
```

Useful SQL:

```sql
\dt
SELECT source, entity_type, count(*) FROM source_entities
GROUP BY source, entity_type ORDER BY source, entity_type;
SELECT * FROM export_lots LIMIT 20;
SELECT * FROM export_procurement_notices LIMIT 20;
SELECT * FROM crawl_frontier ORDER BY priority DESC, available_at LIMIT 20;
```

GUI clients such as DBeaver or pgAdmin can connect to `127.0.0.1:15432`,
database/user/password `procurement` by default.

### Moving to another machine

Do not copy the live PostgreSQL volume directory. Create a logical backup:

```powershell
docker compose exec postgres pg_dump -U procurement -d procurement -Fc `
  -f /tmp/procurement.dump
docker compose cp postgres:/tmp/procurement.dump .\procurement.dump
```

On the target machine, copy the project, `.env`, custom proxy pool files, and
`procurement.dump`, then run:

```powershell
docker compose up -d postgres
docker compose run --rm migrate
docker compose cp .\procurement.dump postgres:/tmp/procurement.dump
docker compose exec postgres pg_restore -U procurement -d procurement `
  --clean --if-exists --no-owner /tmp/procurement.dump
docker compose --profile workers up -d --build
```

For a fresh database, omit `pg_restore`; `migrate` creates the complete schema.
Keep secrets out of `config/*.toml` and transfer them through `.env` or the target
machine's secret manager.

Prometheus metrics are exposed on the configured `METRICS_PORT` (default `9108`).
Logs include task, worker, source, entity, session lane, proxy, strategy, and
attempt context while cookies, tokens, and authentication headers are excluded.

Docker publishes EEP worker metrics on `http://127.0.0.1:9108/metrics`, Zakup
worker metrics on `http://127.0.0.1:9109/metrics`, and scheduler metrics on
`http://127.0.0.1:9110/metrics`. Prometheus scrapes these endpoints and stores
time-series counters, gauges, and latency histograms; it is not a log store.

Logs are written both to stdout and rotating JSONL files:

```text
logs/procurement-parser.jsonl
logs/procurement-parser.jsonl.1
...
```

Defaults are 10 MB per file and five backups. Override with `LOG_DIR`,
`LOG_FILENAME`, or disable file output with `LOG_TO_FILE=false`.

Follow a local worker log:

```powershell
Get-Content logs\procurement-parser.jsonl -Wait
```

Docker uses separate files to avoid multi-process rotation conflicts:

```text
logs/worker-eep.jsonl
logs/worker-zakup.jsonl
logs/scheduler.jsonl
```

Each JSONL record is flushed by the logging handler when emitted; there is no
five-minute write delay. A safe Docker shutdown stops new claims, lets current
tasks finish, releases the unprocessed remainder of every claimed batch, and
then closes browser/network sessions:

```powershell
docker compose --profile workers stop -t 600 worker-eep worker-zakup scheduler
docker compose --profile workers down --remove-orphans
```

If Docker reports `Network ... is still in use`, an orphan or one-off Compose
container is still attached. Inspect it with `docker compose ps -a` and
`docker network inspect govpars_default`; `--remove-orphans` handles the common
case.

## Operational notes

- PostgreSQL is the source of truth and the durable task queue.
- Successful tasks are deleted from `crawl_frontier` and appended to
  `crawl_task_history`.
- PostgreSQL dead tuples are obsolete MVCC row versions left by queue updates and
  deletes. Autovacuum reclaims them; `db-stats` reports their estimate and ratio.
- `COPY TO STDOUT` is used only for CSV export. Normal ingestion uses idempotent
  UPSERT. Bulk replay has a separate temporary staging-table path.
- Document metadata is stored; document binaries are not downloaded.
- The Zakup direct API can return HTTP 418. Run `zakup-spike` to capture a valid
  browser request profile before enabling aggressive backfill. The command
  inspects the real User-Agent, cookies, request-scoped headers, API payload,
  CAPTCHA behavior, and WAF response; it does not select or benchmark user
  agents.
- A lane is one sticky network identity: proxy/IP, Chromium profile,
  fingerprint, cookies, request headers, and circuit breaker. Lanes are never
  shared across proxies. `static_proxy` and `public_pool` use the same transport
  mechanics, but `public_pool` points at a validated rotating reserve while a
  single static proxy has no replacement unless a pool or provider rotation URL
  is configured.
- The large Zakup `main-es2015.<hash>.js` filename changes when the portal is
  deployed. The cache serves the exact known hash and downloads a new bundle
  when the hash changes. Worker and scheduler share the bundle cache but use
  separate Chromium profile directories.
- Zakup uses `curl-cffi` first, then browser fetch and network interception.
  The measured `e-tag` and `tor` headers are request-scoped, so affected
  endpoints go directly through network interception after the first capture.
  DOM scraping is intentionally not used as a silent fallback because it drops
  fields that remain present in API responses.
- 2captcha is disabled by default and is only activated by selecting the
  `2captcha` profile with `TWOCAPTCHA_API_KEY` set.
