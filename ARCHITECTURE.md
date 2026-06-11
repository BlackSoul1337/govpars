# Архитектура Procurement Parser

## Цели

Сервис должен:

- независимо собирать EEP MITWORK и Zakup SK;
- переживать рестарты без потери очереди;
- хранить максимум доступных исходных и нормализованных данных;
- поддерживать полный backfill и incremental refresh;
- изолировать proxy/browser/CAPTCHA сессии;
- экспортировать воспроизводимые CSV из PostgreSQL.

## Выбранный подход

Используется Clean Architecture Lite с вертикальными source slices, Strategy и
durable ETL:

```text
discover → enqueue → extract → normalize → persist → discover relations → reconcile
```

```text
domain
  models, errors, ports
       ↑
application
  discovery, workers, scheduler, export validation
       ↑
infrastructure
  sources/eep_mitwork
  sources/zakup_sk
  persistence/postgres
  network
  captcha
       ↑
entrypoints
  Typer CLI
```

Application зависит от domain contracts, но не от source-specific
infrastructure exceptions. Wiring выполняется в factory.

## Почему не полный Hexagonal

Driving surface ограничен CLI/scheduler, а основной driven port один:
PostgreSQL. Полный набор inbound/outbound adapter abstractions увеличил бы
количество ритуального кода без реального isolation gain. Порты оставлены там,
где есть заменяемая граница: source, frontier, repository, sessions, CAPTCHA,
CSV и runtime state.

## Почему вертикальные slices

Каждый сайт содержит собственные:

- transport/client strategies;
- parser;
- mapping в domain entities;
- session and WAF behavior.

Изменение Zakup не должно затрагивать EEP. Новый источник добавляется отдельным
модулем и регистрируется в factory/CLI.

## Домен

Основные entities:

- `PlanItem`;
- `ProcurementNotice`;
- `Lot`;
- `Organization`;
- `DeliveryPlace`;
- `PaymentTerms`;
- `DocumentMetadata`;
- `EntityRelation`.

Identity:

```text
(source, entity_type, source_entity_id)
```

`business_number` не используется как primary identity. Это критично для EEP,
где номер лота и ID в URL могут отличаться.

EEP моделируется как граф:

```text
PlanItem (/point) → ProcurementNotice (/buy) → Lot (/lot)
```

Организации разных сайтов не объединяются автоматически по БИН; БИН является
только matching candidate.

## Discovery и extraction

Discovery:

1. загружает list page;
2. сохраняет summary;
3. создаёт/обновляет `source_entities`;
4. ставит detail task в `crawl_frontier`;
5. обновляет checkpoint.

Worker:

1. claims batch через `SKIP LOCKED`;
2. запускает lease heartbeat;
3. извлекает detail;
4. нормализует entity и source-specific JSONB;
5. выполняет hash-aware UPSERT;
6. сохраняет relations и новые discovery targets;
7. переносит compact result в history и удаляет active task.

## EEP strategy

Основной путь:

```text
httpx SSR HTML → selectolax parser
```

При transport failure или WAF `403/418/429` клиент может выполнить
`curl-cffi` request с TLS/browser impersonation. Playwright для EEP не нужен.

Обходятся list pages plans/notices/lots и relation URLs
`/point`, `/buy`, `/lot`, `/subject`.

## Zakup strategy

Порядок:

1. `CurlCffiApiStrategy`;
2. `BrowserFetchStrategy`;
3. `NetworkInterceptStrategy`;
4. `DomFallbackStrategy` только для list discovery.

Playwright получает легитимную сессию и перехватывает URL, method, body и
headers. Transport-owned headers (`Host`, `Content-Length`, `Connection`) не
копируются. Заголовки разделяются на static, session-scoped и request-scoped.

DOM fallback намеренно не заменяет detail API: частичный DOM не должен
перезаписать полную нормализованную карточку.

Большой JS bundle обслуживается persistent Chromium profile и disk cache.
`slow_internet` увеличивает browser timeouts и lease.

## Queue и leases

`crawl_frontier` содержит только активные задачи. Ключевые поля:

- `priority`;
- `available_at`;
- `lease_owner`;
- `leased_until`;
- `attempt`;
- `task_type`;
- `source_entity_fk`.

Claim:

```sql
ORDER BY priority DESC, available_at ASC, id ASC
FOR UPDATE SKIP LOCKED
```

Свежие задачи имеют высокий priority, backfill низкий. Часть capacity
резервируется под backfill для защиты от starvation.

Worker heartbeat продлевает lease каждые `lease_seconds / 3`. При graceful
shutdown все leases process-owned worker IDs сбрасываются. При hard crash
работает timeout lease.

`fillfactor=75` и агрессивный autovacuum уменьшают MVCC bloat. Успешные строки
удаляются из frontier, история append-only.

## Drain

`--drain` завершает процесс только когда:

- queue depth выбранного источника равен нулю;
- активных leases нет;
- за idle grace не появились relation tasks;
- exhausted tasks перенесены в failed history.

Scheduler должен быть остановлен для конечного drain, иначе он законно создаёт
новые задачи.

## Scheduler

Scheduler state хранится в `scheduler_job_state`. PostgreSQL lease исключает
одновременное выполнение одной job несколькими scheduler processes.

Jobs:

- incremental lists;
- active entities refresh;
- recently closed refresh;
- old entities refresh;
- weekly full reconciliation.

Docker использует отдельные `scheduler-eep` и `scheduler-zakup`. Это даёт
failure isolation и независимую конфигурацию proxy/CAPTCHA.

## Session lanes и circuit breakers

`SessionIdentity` логически связывает:

- proxy endpoint/IP;
- fingerprint и User-Agent;
- cookies;
- browser local/session storage;
- generation;
- lane breaker.

Cookies нельзя переносить между IP. При трёх последовательных
`403/418/429` lane открывает breaker, задача возвращается с `available_at`,
профиль обновляется, затем выполняется half-open probe. Provider rotation
выполняется только для профилей с rotation API.

Состояние lane и source breaker сохраняется в `session_lanes`, поэтому рестарт
не обнуляет cooldown.

## CAPTCHA lifecycle

Providers:

- disabled;
- manual;
- 2captcha.

Flow:

1. browser определяет challenge;
2. lane получает PostgreSQL advisory lock;
3. solver проверяет persisted hourly budget;
4. proxy-bound task использует proxy и User-Agent lane;
5. результат применяется в browser session;
6. audit metadata и cost сохраняются;
7. token удаляется и не сохраняется в БД/логах.

Manual fallback в headless/non-TTY среде отклоняется явной ошибкой.

## PostgreSQL

Основные группы таблиц:

- identity/current state: `source_entities`, normalized entity tables;
- graph: `entity_relations`;
- children: delivery/payment/document tables;
- revisions: `entity_revisions`;
- work: `crawl_frontier`, `crawl_task_history`, `fetch_failures`;
- runtime: `session_lanes`, `captcha_challenges`, `scheduler_job_state`.

Нормализованные поля оптимизируют аналитику. `source_payload JSONB` сохраняет
source-specific максимум и позволяет переиграть mapping.

Regular ingestion:

```text
batch INSERT ... ON CONFLICT ... WHERE content_hash changed
```

Bulk replay:

```text
one physical connection + one transaction
CREATE TEMP ON COMMIT DROP
COPY FROM
deduplicate staging
INSERT ... ON CONFLICT
COMMIT
```

CSV использует `COPY TO STDOUT` с явными column lists. Поддерживаются
стандартный comma delimiter и semicolon для Excel в локалях, где системный
list separator равен `;`.

## Revisions

`REVISION_MODE=changes` сохраняет revision только при изменении canonical hash.
`off` оставляет только current state. Fetch timestamps и transport metadata не
должны влиять на canonical content hash.

## Observability

Workers/schedulers являются долгоживущими процессами, поэтому каждый поднимает
embedded Prometheus HTTP server. Pushgateway не нужен.

DB sampler каждые 15 секунд публикует queue, leases, outcomes, lane states,
CAPTCHA spend и scheduler success. Task/request/CAPTCHA latency измеряется
histograms.

Structlog пишет JSONL с correlation context. Секреты, cookies, CAPTCHA tokens и
auth headers редактируются.

## Почему нет Redis/Celery

PostgreSQL уже нужен как SSOT. `SKIP LOCKED`, leases и history покрывают
требования очереди без второго stateful компонента. Redis/Celery добавили бы
двойную согласованность, отдельный backup и новый failure mode.

## Почему нет FastAPI/Granian

V1 не предоставляет business HTTP API. Prometheus обслуживается встроенным
микро-сервером, CLI выполняет operations. Добавлять ASGI runtime без API было бы
лишней эксплуатационной поверхностью.

## Deployment topology

Минимум:

```text
PostgreSQL
EEP worker + EEP scheduler
Zakup worker + Zakup scheduler
Prometheus scraper
```

Для локального режима `--source all` supervisor запускает два независимых
pipeline в одном процессе и агрегирует метрики на одном порту.
