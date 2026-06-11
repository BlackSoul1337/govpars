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
uv run procurement-parser discover --source all --pages 0 `
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

`--once` и `--drain` взаимоисключающие. При `SIGINT/SIGTERM` процесс
освобождает свои leases. При `SIGKILL` задания возвращаются после
`leased_until`.

## Возобновление и полный backfill

`discover` по умолчанию использует checkpoints:

```powershell
uv run procurement-parser discover --source eep-mitwork --resume
uv run procurement-parser discover --source eep-mitwork --no-resume --pages 0
```

Для полного backfill:

1. остановите scheduler выбранного источника;
2. запустите `discover --pages 0 --no-resume`;
3. запустите `worker --drain`;
4. повторите discovery для reconciliation;
5. экспортируйте и проверьте CSV.

Числовые detail ID вслепую не перебираются.

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

`config/proxy_pools/public_pool.example.json` показывает только формат.
Бесплатные прокси непригодны для гарантированного SLA.

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

uv run procurement-parser validate-export --input exports/all
```

CSV создаются как UTF-8 с BOM. Validator проверяет заголовки, U+FFFD,
duplicate identities и соответствие combined/split row counts.
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
