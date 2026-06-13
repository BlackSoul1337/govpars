# Procurement Parser

Product-oriented ETL-сервис для сбора государственных и квазигосударственных
закупок Казахстана:

- `eep.mitwork.kz`: планы, объявления, лоты и организации из SSR HTML;
- `zakup.sk.kz`: лоты и закупки через JSON API с browser-assisted сессиями,
  CAPTCHA и контролируемыми fallback-стратегиями.

Английская версия: [README.en.md](README.en.md). Архитектура:
[ARCHITECTURE.md](ARCHITECTURE.md). Статус готовности:
[PRODUCT_READINESS.md](PRODUCT_READINESS.md).

## Что реализовано

Pipeline: `discover → enqueue → extract → normalize → persist → relations → reconcile`.

- PostgreSQL является единственным источником истины, durable queue и хранилищем.
- Источники можно запускать отдельно или параллельно через `--source all`.
- EEP использует `httpx + selectolax`, при WAF доступен `curl-cffi` fallback.
- Zakup использует `curl-cffi API → browser fetch → network intercept → DOM fallback`.
- Есть direct/static/public/residential/mobile network profiles и lane rotation.
- Поддерживаются manual CAPTCHA и 2captcha с лимитом стоимости в PostgreSQL.
- Экспортирует восемь datasets в combined, split или both CSV.
- Workers поддерживают `--once`, `--drain`, heartbeat leases и graceful shutdown.
- Scheduler хранит состояние jobs в PostgreSQL и разделён по источникам в Docker.
- Prometheus работает через встроенный HTTP server, без FastAPI/Granian.

## Требования

- Python 3.12;
- `uv`;
- PostgreSQL 17 (поддерживаемый основной вариант);
- Docker/Compose для контейнерного запуска;
- Chromium Playwright для Zakup SK.

## Быстрый старт через uv

```powershell
Copy-Item .env.example .env
docker compose up -d postgres
$env:UV_CACHE_DIR=".uv-cache"
uv sync --frozen --extra dev
uv run playwright install chromium
uv run alembic upgrade head
```

По умолчанию PostgreSQL доступен на `127.0.0.1:15432`. Порт и
`DATABASE_URL` должны изменяться согласованно.

Минимальный EEP-проход:

```powershell
uv run procurement-parser discover --source eep-mitwork --pages 2
uv run procurement-parser worker --source eep-mitwork --drain
uv run procurement-parser export --dataset all --output exports/all
uv run procurement-parser validate-export --input exports/all
```

## Discovery и Worker

`discover` обходит list pages, сохраняет summary и ставит detail identities в
`crawl_frontier`. Он не извлекает полную карточку.

EEP обходит страницы `lots`, `buys` и `points` параллельными окнами. Все три
каталога делят один source-level semaphore: профиль `direct` по умолчанию
разрешает 6 одновременных list-запросов, proxy-профили — 10. Zakup discovery
намеренно остаётся последовательным (`1`), поскольку его запросы связаны с
browser/session lane.

```powershell
# Профильное значение из config/sources/eep_mitwork.toml
uv run procurement-parser discover --source eep-mitwork --pages 100

# Временный override только для EEP
uv run procurement-parser discover --source all --pages 100 `
  --discovery-concurrency 3 `
  --eep-network direct --zakup-network direct
```

Допустимый диапазон `--discovery-concurrency`: `1..64`. При `--source all`
override не меняет Zakup. Каждое окно сначала полностью загружается и
разбирается, затем результаты сортируются по номеру страницы и сохраняются
одним последовательным enqueue. Первая пустая страница завершает каталог.
Ошибки уже отправленных спекулятивных страниц после неё игнорируются; ошибка
до неё отменяет всё окно без продвижения checkpoint.

В консоли выводятся `discovery_window_started`, промежуточный
`discovery_window_progress` каждые 5 секунд и итоговый
`discovery_window_complete`. Пока окно загружается, данные ещё не enqueue-нуты:
это нормальное поведение, а не зависание.

`worker` забирает задания через `FOR UPDATE SKIP LOCKED`, извлекает полную
карточку, сохраняет сущности/связи и добавляет найденные relation tasks.

```powershell
# Только EEP, весь доступный каталог
uv run procurement-parser discover --source eep-mitwork --pages 0
uv run procurement-parser worker --source eep-mitwork --drain

# Только Zakup
uv run procurement-parser discover --source zakup-sk --pages 0 `
  --runtime local --network direct --captcha manual
uv run procurement-parser worker --source zakup-sk --drain `
  --runtime local --network direct --captcha manual

# Оба источника параллельно
uv run procurement-parser discover --source all --pages 0 --no-resume `
  --eep-network direct --zakup-network residential --captcha 2captcha
uv run procurement-parser worker --source all --drain `
  --eep-network direct --zakup-network residential --captcha 2captcha
```

`--source all` создаёт независимые adapters, worker groups, lanes и breakers.
Ошибка одного источника не отменяет другой.

Дополнительные режимы worker:

```powershell
# Один batch
uv run procurement-parser worker --source eep-mitwork --once

# До устойчиво пустой очереди
uv run procurement-parser worker --source all --drain `
  --idle-grace-seconds 15

# Discovery и worker одной командой
uv run procurement-parser run --source all --pages 10 --drain
```

`--once` и `--drain` взаимоисключающие:

- обычный `worker` работает постоянно: при пустой очереди ждёт новые задания;
- `--once` даёт каждому внутреннему worker право получить не более одного
  batch и после его обработки завершает процесс. Это не обязательно одна
  карточка: верхняя граница примерно равна `workers × batch_size`;
- `--drain` продолжает обрабатывать выбранный источник, включая новые задания,
  найденные через связи, пока очередь и активные leases не будут равны нулю
  непрерывно в течение `--idle-grace-seconds`;
- с `--source all` EEP и Zakup drain-ятся параллельно и независимо;
- scheduler следует остановить перед конечным `--drain`, иначе он может снова
  наполнить очередь.

При `SIGINT/SIGTERM` процесс завершает текущую операцию и освобождает свои
leases. При `SIGKILL`, падении ОС или отключении питания cleanup невозможен:
задания снова станут доступными после `leased_until`.

## Полнота данных

Парсер хранит данные в двух уровнях:

1. нормализованные колонки для поиска, связей и CSV;
2. полный полученный ответ источника в `source_payload` JSONB.

Zakup сохраняет исходный API JSON, EEP — извлечённые поля, таблицы, представления
и заголовок SSR-страницы. Нормализуются идентификаторы, номера, названия,
описания и характеристики, статусы, метод и тип предмета закупки, суммы,
количество, единицы, даты приёма заявок, доставка, оплата, контакты,
организации, документы и связи.

Это означает «все данные, которые вернул используемый публичный endpoint или
страница», но не скрытые внутренние поля и не содержимое файлов. Документы не
скачиваются: сохраняются их метаданные и URL. Поле остаётся `NULL`, если
источник его не отдал. Например, у объявления Zakup описание часто находится
только в дочерних лотах, а дата публикации не выводится из одного лишь статуса
`PUBLISHED`. Source-specific поля, для которых нет общей семантики EEP/Zakup,
остаются доступными в `source_payload`.

## Возобновление и полный backfill

`discover` по умолчанию использует checkpoints:

```powershell
uv run procurement-parser discover --source eep-mitwork --resume
uv run procurement-parser discover --source eep-mitwork --no-resume --pages 0
```

`--resume` продолжает с `next_page` из PostgreSQL checkpoint и сразу
завершается, если полный scope уже отмечен как completed. `--no-resume`
игнорирует checkpoint как стартовую позицию и начинает с первой страницы
(либо с `--start-page`). Он не удаляет БД и не заставляет повторно скачивать
уже успешно сохранённые detail-карточки: повторный list scan обновляет summary
и ставит в очередь новые или ранее не завершённые ID.

Для полного backfill:

1. остановите scheduler выбранного источника;
2. запустите `discover --pages 0 --no-resume`;
3. запустите `worker --drain`;
4. повторите discovery для reconciliation;
5. экспортируйте и проверьте CSV.

Числовые detail ID вслепую не перебираются.

### Быстрая оценка каталогов

Команда делает только пробные list-запросы, ничего не пишет в PostgreSQL:

```powershell
uv run procurement-parser catalog-stats --source all --samples 1 `
  --eep-network direct --zakup-network direct --captcha manual
```

Сравнение нескольких профилей и пробный `extract → parse → relations`:

Самый надёжный вариант для любого shell — одна строка:

```text
uv run procurement-parser catalog-stats --source all --samples 1 --worker-samples 2 --runtime-profiles local,slow_internet --network-profiles direct,public_pool --captcha disabled --progress-interval-seconds 5 --probe-timeout-seconds 180
```

PowerShell использует обратный апостроф в конце каждой продолжаемой строки:

```powershell
uv run procurement-parser catalog-stats --source all --samples 1 `
  --worker-samples 2 `
  --runtime-profiles local,slow_internet `
  --network-profiles direct,public_pool `
  --captcha disabled `
  --progress-interval-seconds 5 `
  --probe-timeout-seconds 180
```

Bash использует обратный слеш:

```bash
uv run procurement-parser catalog-stats --source all --samples 1 \
  --worker-samples 2 \
  --runtime-profiles local,slow_internet \
  --network-profiles direct,public_pool \
  --captcha disabled \
  --progress-interval-seconds 5 \
  --probe-timeout-seconds 180
```

Прогресс выводится в stderr строками `START`, `WAIT`, `DONE`, `FAIL` и `SKIP`
каждые 10 секунд. Интервал можно изменить через
`--progress-interval-seconds 30`, отключить через `--no-progress`. Финальный
JSON остаётся в stdout. Один catalog/detail probe ограничен 600 секундами;
изменить лимит можно через `--probe-timeout-seconds`.

Результат содержит общее количество элементов, время ответа, скорость обхода
list-страниц и пробную скорость detail worker. Worker probe не пишет данные в
PostgreSQL. Поле `full_run_forecasts` показывает итоговый прогноз
`discovery + detail` для каждой комбинации source/runtime/network. Прогноз не
учитывает retries, UPSERT, reconciliation и повторное обнаружение связей.
Для EEP `catalog-stats` использует профильную оконную параллельность:
`elapsed_seconds` является реальным wall-clock временем, а
`sequential_elapsed_seconds` и `estimated_discovery_seconds_sequential`
сохраняют последовательный baseline для сравнения.
Для `--source all` поле `combined_forecasts.parallel_wall_clock` показывает
время при параллельной работе сайтов, а `sequential_total` — при запуске по
очереди.

## Scheduler

Scheduler создаёт durable jobs:

- первые страницы каталогов каждые 5 минут;
- активные сущности каждые 15 минут;
- недавно закрытые ежедневно в течение 14 дней;
- старые еженедельно;
- полный reconciliation еженедельно.

Только `incremental_lists` запускается сразу при первом старте. Тяжёлые jobs
получают первоначальную отсрочку по своему интервалу.

```powershell
uv run procurement-parser scheduler --source eep-mitwork
uv run procurement-parser scheduler --source zakup-sk `
  --network residential --captcha 2captcha
uv run procurement-parser scheduler --source all `
  --eep-network direct --zakup-network residential --captcha 2captcha
```

Не запускайте scheduler одновременно с конечным `worker --drain`, если очередь
должна гарантированно дойти до нуля.

## Docker

```powershell
# Только EEP: DB, migration gate, EEP worker и scheduler
docker compose --profile eep up -d --build

# Только Zakup
docker compose --profile zakup up -d --build

# Оба источника
docker compose --profile full up -d --build
```

Профили не запускают исторический backfill сами. Scheduler сразу ставит только
incremental list jobs; полный каталог запускается явной CLI-командой.

Безопасная остановка:

```powershell
docker compose --profile full stop -t 600
docker compose --profile full down -t 600
```

`stop_grace_period` равен 10 минутам, чтобы Zakup успел завершить браузерный
запрос и освободить leases.

## Конфигурация

Независимые измерения конфигурации:

```text
config/
├── app.toml
├── database.toml
├── sources/{eep_mitwork,zakup_sk}.toml
├── network/{direct,static_proxy,public_pool,residential,mobile}.toml
├── captcha/{disabled,manual,2captcha}.toml
└── runtime/{local,server,aggressive_backfill,slow_internet}.toml
```

Приоритет: CLI → environment/secrets → TOML → defaults.

Runtime profiles:

| Profile | Назначение |
|---|---|
| `local` | 4 workers, 2 browser lanes |
| `server` | 12 workers, preload Zakup bundle |
| `aggressive_backfill` | 24 workers, 4 browser lanes |
| `slow_internet` | таймауты 360 секунд, lease 900 секунд, 1 lane |

Глобальные overrides: `WORKER_COUNT`, `BROWSER_LANES`,
`NETWORK_MAX_LANES`, `METRICS_PORT`.

## Прокси и lanes

- `direct`: без прокси;
- `static_proxy`: один стабильный endpoint, без provider rotation;
- `public_pool`: тестовый файл бесплатных прокси с заменой исчерпанного lane;
- `residential`: sticky residential endpoint + `PROXY_ROTATE_URL`;
- `mobile`: sticky mobile endpoint + более короткий TTL.

Lane связывает proxy, fingerprint, User-Agent, cookies, browser storage и
circuit breaker. Cookies и CAPTCHA state не переносятся между IP.

Пример static proxy:

```powershell
$env:PROXY_URL="http://user:password@host:port"
uv run procurement-parser worker --source zakup-sk `
  --network static_proxy --captcha 2captcha
```

Public pool сначала нужно построить и проверить. Файлы реальных прокси не
хранятся в Git:

```powershell
uv run procurement-parser proxy-pool-build `
  --structured-json .\free-proxy-list.json `
  --generic-json .\proxies.json `
  --spys-text .\spys-list.txt

uv run procurement-parser proxy-pool-check `
  --input config/proxy_pools/public_test.json `
  --output config/proxy_pools/public_working.json
```

`config/proxy_pools/public_pool.example.json` показывает формат. Репозиторий
также содержит `config/proxy_pools/public_working.json`, проверенный 11 июня
2026 года. Это бесплатные публичные прокси без гарантий доступности,
безопасности или сохранения адреса: перед каждым запуском их нужно повторно
валидировать. Для полного backfill используйте платный sticky
residential/mobile pool.

## CAPTCHA

```powershell
# Без решения: задача завершится ошибкой при CAPTCHA
--captcha disabled

# Видимый браузер и интерактивный TTY
--captcha manual

# Автоматический solver
$env:TWOCAPTCHA_API_KEY="..."
uv run procurement-parser worker --source zakup-sk `
  --network residential --captcha 2captcha
```

Для proxy-bound challenge в 2captcha передаются тот же proxy и User-Agent.
Стоимость за час и audit metadata сохраняются в PostgreSQL. Token после
применения не сохраняется. `fallback_provider=manual` работает только при
интерактивном TTY; headless Docker выдаёт явную ошибку.

Используйте CAPTCHA-интеграцию только при наличии правового разрешения.

## Zakup bundle cache и медленный интернет

```powershell
uv run procurement-parser zakup-cache --runtime slow_internet --network direct
uv run procurement-parser zakup-spike --runtime local --network direct `
  --captcha manual
```

`slow_internet` увеличивает browser navigation/capture/response timeouts и
lease. Disk cache и persistent browser profile уменьшают повторную загрузку
большого JS bundle, но cache invalidation контролирует Chromium и сам сайт.

## CSV

Datasets: `lots`, `notices`, `plan_items`, `organizations`, `relations`,
`delivery_places`, `payment_terms`, `documents`.

```powershell
# Общие таблицы
uv run procurement-parser export --dataset all `
  --output exports/combined --layout combined

# Отдельно EEP и Zakup
uv run procurement-parser export --dataset all `
  --output exports/split --layout split

# Оба варианта
uv run procurement-parser export --dataset all `
  --output exports/all --layout both

# Для Excel с локалью ru-RU
uv run procurement-parser export --dataset all `
  --output exports/excel --layout split --delimiter semicolon

# Только для машинной обработки без Excel-защиты
uv run procurement-parser export --dataset all `
  --output exports/raw --layout split --raw-csv

uv run procurement-parser validate-export --input exports/all
```

CSV создаются как UTF-8 с BOM. Validator проверяет заголовки, U+FFFD,
ширину строк, spreadsheet formula-like значения, duplicate identities и
соответствие combined/split row counts. По умолчанию потенциально активные для
Excel значения (`=`, `+`, `-`, `@`) экранируются апострофом. `--raw-csv`
отключает защиту и предназначен только для контролируемой машинной обработки.
Для procurement datasets UTC-колонки `published_at`,
`application_start_at`, `application_end_at` дополнены удобными для чтения
`published_at_local`, `application_start_at_local`,
`application_end_at_local`. Локальные значения рассчитаны для
`source_timezone=Asia/Almaty`; UTC остаётся каноническим временем БД.
После экспорта создаётся `export_manifest.json` с временем генерации, режимом и
числом строк в каждом файле.
Все файлы одного запуска `all`/`both` читаются из одного PostgreSQL
`REPEATABLE READ` snapshot. Каждый CSV и manifest публикуются атомарной заменой,
поэтому незавершённый экспорт не оставляет обрезанный финальный файл.

Стандартный delimiter — запятая. При открытии двойным кликом русская версия
Excel часто ожидает `;`; используйте `--delimiter semicolon` либо импорт через
«Данные → Из текста/CSV».

## PostgreSQL

```powershell
uv run procurement-parser db-stats
uv run procurement-parser release-stale-leases
uv run procurement-parser prune-history --retention-days 90
```

Просмотр через `psql`:

```powershell
docker compose exec postgres psql -U procurement -d procurement
```

### Как читать собранные данные

Для выборок и аналитики используйте представления `export_*`: они уже
соединяют внутренний ключ `source_entity_fk` с идентичностью из
`source_entities` и совпадают по структуре с CSV datasets.

| Представление | Содержимое |
| --- | --- |
| `export_lots` | лоты |
| `export_procurement_notices` | объявления EEP `/buy` и закупки Zakup `advert` |
| `export_plan_items` | пункты планов EEP `/point` |
| `export_organizations` | организации |
| `export_entity_relations` | направленные связи между сущностями |
| `export_delivery_places` | места поставки |
| `export_payment_terms` | условия оплаты |
| `export_documents` | метаданные и URL документов, без скачивания файлов |

Идентичность сущности задаётся тройкой
`(source, entity_type, source_entity_id)`. `source_entity_id` является ID
страницы/API, а `business_number` — отображаемым номером закупки или лота; они
не обязаны совпадать. Внутренний `source_entities.id` предназначен для
внешних ключей БД и не является ID сайта.

Основные типы связей:

- `plan_to_notice`: пункт плана EEP → объявление;
- `plan_to_lot`: пункт плана EEP → лот;
- `notice_to_lot`: объявление/закупка → лот;
- `organizer`: сущность → организация-организатор;
- `customer`: сущность → организация-заказчик.

Связь хранится направленно: колонки `parent_*` указывают исходную сущность,
`child_*` — связанную. Отсутствие связи допустимо: например, пункт плана EEP
может ещё не иметь объявления, а ссылка источника может вести на уже
недоступную карточку.

Количество сохранённых сущностей:

```sql
SELECT source, entity_type, count(*)
FROM source_entities
WHERE last_success_at IS NOT NULL
GROUP BY source, entity_type
ORDER BY source, entity_type;
```

50 закупок с самым коротким приёмом заявок:

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

Лоты конкретной закупки:

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

Полные цепочки EEP «план → объявление → лот»:

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

`JOIN` выше возвращает только существующие полные цепочки. Чтобы найти планы,
у которых пока нет объявления, используйте отдельный запрос:

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

Документы лота и доступ к source-specific данным:

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

UTC-поля являются каноническими. Колонки с суффиксом `_local` рассчитаны для
`source_timezone` и предназначены для отображения и отчётов. Для программной
обработки JSONB используйте PostgreSQL-операторы `->`, `->>` и `@>`.

Backup/restore:

```powershell
docker compose exec postgres pg_dump -U procurement -Fc procurement `
  -f /tmp/procurement.dump
docker compose cp postgres:/tmp/procurement.dump .\procurement.dump

docker compose cp .\procurement.dump postgres:/tmp/procurement.dump
docker compose exec postgres pg_restore -U procurement -d procurement `
  --clean --if-exists /tmp/procurement.dump
```

Обычный ingestion использует hash-aware UPSERT. Bulk replay выполняет
`TEMP TABLE → COPY → UPSERT` на одном физическом соединении.

## Метрики и логи

Prometheus endpoint создаётся `prometheus_client.start_http_server()`.

- EEP worker: host `9108`;
- Zakup worker: host `9109`;
- EEP scheduler: host `9110`;
- Zakup scheduler: host `9111`.

Локальный `--source all` использует один `METRICS_PORT`.

JSONL-логи находятся в `logs/`. Контекст включает `run_id`, `task_id`,
`worker_id`, `source`, `entity_type`, `session_lane_id`, `proxy_id`,
`strategy`, `attempt`. Cookies, tokens и auth headers не логируются.

## Проверка и разработка

```powershell
$env:UV_CACHE_DIR=".uv-cache"
uv run ruff check .
$env:TEST_DATABASE_URL="postgresql+asyncpg://procurement:procurement@127.0.0.1:15432/procurement_test"
uv run pytest --cov=procurement_parser --cov-report=term-missing
docker compose --profile full config --quiet
docker build -t procurement-parser:local .
```

Integration tests требуют отдельную БД, имя которой заканчивается на `_test`.

## Troubleshooting

- `No such option --source`: опция должна идти после команды:
  `procurement-parser worker --source ...`.
- Queue не обнуляется в drain: остановите scheduler и проверьте delayed tasks.
- Stale leases после crash: дождитесь lease expiry или выполните
  `release-stale-leases`.
- Zakup завис на bundle: используйте `slow_internet`, persistent cache и
  проверьте реальную пропускную способность proxy.
- `418/429`: lane открывает circuit breaker; не увеличивайте retries вслепую.
- Manual CAPTCHA в Docker: detached/headless контейнер не имеет TTY; используйте
  2captcha или локальный видимый браузер.
- Ошибка public pool: создайте `public_working.json` или задайте
  `PROXY_POOL_FILE`.
