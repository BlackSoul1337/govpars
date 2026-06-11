# Product Readiness Review

Дата аудита: 11 июня 2026.

## Findings

### High

1. **Полный production pilot на 10 000 сущностей ещё не является
   воспроизводимым CI-тестом.**

   Unit/integration/migration проверки автоматизированы, но live pilot зависит
   от внешних сайтов, WAF, CAPTCHA и качества proxy pool. Перед публичной
   презентацией нужен отдельный acceptance run минимум по 1 000 сущностей на
   источник и суммарно 10 000, с сохранённым отчётом `db-stats`, integrity SQL
   и CSV validator output.

2. **Zakup SK остаётся внешне нестабильным transport boundary.**

   Реализованы API, browser, intercept и list DOM fallback, persistent cache,
   lanes и breakers. Однако изменение frontend bundle, API contract или WAF
   может потребовать обновления request profiles. Это эксплуатационный риск, а
   не устранимый кодом дефект.

### Medium

1. **Public proxy pool не является production network profile.**

   Dynamic lane replacement реализован, но бесплатные прокси не обеспечивают
   identity stability, bandwidth или CAPTCHA affinity. Профиль допустим только
   для экспериментов.

2. **Browser profile содержит чувствительный session state на диске.**

   Cookies/tokens не сохраняются в PostgreSQL и не логируются, но persistent
   Chromium profile находится в Docker volume/локальном каталоге. Production
   host должен ограничивать доступ и шифровать диск/volume.

3. **DOM fallback Zakup намеренно ограничен discovery.**

   Detail DOM parsing не включён, потому что частичные данные могут
   перезаписать API entity. При полном отказе API detail tasks будут retry/fail,
   а не сохранят неполную карточку. Это принятое отклонение ради целостности.

### Low

1. Windows Proactor loop выводит warning `curl-cffi` в unit tests. Library
   автоматически создаёт selector thread; на Linux containers warning
   отсутствует.

2. `public_pool.toml` ожидает сгенерированный `public_working.json`. Это
   намеренно: рабочие public proxy addresses исключены из Git.

## Проверенное состояние

- Ruff: проходит.
- Unit и PostgreSQL integration tests: проходят.
- Coverage: не ниже 80%.
- Alembic fresh install `0001 → head`: проходит.
- Alembic existing path `0006 → head`: проходит.
- TEMP staging с pool size > 1: проходит на одном physical connection.
- Docker Compose `full` validation: проходит.
- EEP/Zakup/all CLI modes: присутствуют.
- UTF-8 BOM, headers, duplicate identity и split/combined validation:
  автоматизированы.
- Live `--source all` direct smoke на чистой БД: пройден.

Live smoke 11 июня 2026:

- discovery: 150 EEP + 20 Zakup initial tasks;
- relation expansion: 506 основных entities;
- outcomes: 496 success, 1 permanent 404 (0,2%);
- queue после drain: 0;
- stale leases: 0;
- duplicates/orphans/missing raw payload: 0;
- exports: 998 relations, 1 833 documents;
- combined/split CSV validation: valid.

Live acceptance и 10k pilot должны выполняться в отдельной БД и не входят в
обычный CI, чтобы внешний WAF не делал build недетерминированным.

Проверка public pool:

- нормализовано 10 006 бесплатных proxy candidates;
- 27 прошли лёгкий Zakup probe;
- Zakup discovery/worker через public pool: 41 success, queue 0;
- EEP discovery/worker через public pool: 438 success, 1 permanent 404,
  queue 0;
- dynamic replacement подтверждён: три первых Zakup lanes и три первых EEP
  lanes были заменены без перезапуска;
- public-pool DB: duplicates/orphans/stale leases/missing raw = 0;
- CSV proxy smoke: 24 combined/split files, valid.

Бесплатный pool остаётся непредсказуемым и не становится production profile
только потому, что данный smoke прошёл.

## Соответствие исходному ТЗ

| Требование | Статус | Комментарий |
|---|---|---|
| Clean Architecture Lite | готово | Domain/application больше не зависят от source-specific exceptions |
| Vertical source slices | готово | EEP и Zakup изолированы |
| Strategy + durable ETL | готово | Discovery, queue, extraction, persistence, relations, reconciliation |
| PostgreSQL SSOT | готово | Очередь, entities, revisions, runtime state, scheduler |
| `--source` single/all | готово | discover, worker, run, scheduler |
| Failure isolation в `all` | готово | Отдельные contexts/tasks |
| EEP httpx/selectolax | готово | curl-cffi WAF fallback добавлен |
| Zakup curl-cffi/API first | готово | Browser/intercept/list DOM fallback |
| Максимальные raw payload | готово | JSONB хранится у основных сущностей |
| Stable identity | готово | source/type/source ID, business number отдельно |
| EEP relation graph | готово | point/buy/lot/organization relations |
| Durable priority queue | готово | SKIP LOCKED, priority, delayed retries |
| Bloat controls | готово | fillfactor, autovacuum, delete-on-success/history |
| Worker once/drain | готово | idle grace и relation tasks учитываются |
| Lease heartbeat/evacuation | готово | owner registry и release on shutdown |
| Proxy profiles | готово | direct/static/public/residential/mobile |
| Dynamic lane replacement | готово | Public pool и provider rotation |
| Persistent breakers | готово | session_lanes |
| CAPTCHA providers | готово | disabled/manual/2captcha |
| DB CAPTCHA budget/lock | готово | hourly spend + advisory lock |
| Token disposal | готово | token не сохраняется |
| Scheduler policies | готово | durable source-specific jobs |
| Weekly reconciliation | готово | full discovery + refresh |
| Prometheus без FastAPI | готово | embedded HTTP server + DB sampler |
| CSV combined/split/both | готово | 8 datasets |
| UTF-8 CSV validation | готово | BOM/U+FFFD/header/identity/counts |
| Hash-aware revisions | готово | changes/off |
| COPY staging contract | готово | one connection/transaction test |
| Clean migration 0001 | готово | metadata import удалён |
| Upgrade from 0006 | готово | протестировано |
| Docker source profiles | готово | eep/zakup/full |
| CI | готово | Ruff, tests, coverage, migrations, build, scans |
| GHCR CD | готово | AMD64, tags, SBOM, provenance, no SSH deploy |
| Direct live EEP/Zakup/all smoke | готово | 506 entities после relation expansion |
| Public proxy live smoke | готово | EEP и Zakup discovery/worker drain |
| Production proxy live smoke | не выполнено | Нет paid proxy credentials |
| 10k live pilot | не выполнено | Требует стабильного внешнего доступа/proxy/CAPTCHA |

## Acceptance Runbook

Использовать отдельную БД с именем, заканчивающимся на `_test` или `_pilot`.

1. Поднять PostgreSQL и применить fresh migrations.
2. Выполнить EEP direct discovery и `worker --drain`.
3. Повторить EEP через production proxy profile.
4. Выполнить Zakup direct discovery и drain.
5. Повторить Zakup через production residential/mobile profile.
6. Выполнить совместный `--source all`.
7. Довести выборку до 10 000 сущностей, минимум 1 000 на источник.
8. Послать `SIGTERM` во время длинной Zakup task и проверить ноль stale leases.
9. Повторить discovery и проверить idempotency.
10. Запустить scheduler jobs и reconciliation.
11. Экспортировать `--dataset all --layout both`.
12. Запустить `validate-export`.
13. Проверить integrity SQL:

```sql
SELECT source, entity_type, source_entity_id, count(*)
FROM source_entities
GROUP BY 1,2,3
HAVING count(*) > 1;

SELECT count(*)
FROM entity_relations r
LEFT JOIN source_entities p ON p.id = r.parent_entity_fk
LEFT JOIN source_entities c ON c.id = r.child_entity_fk
WHERE p.id IS NULL OR c.id IS NULL;

SELECT count(*)
FROM crawl_frontier
WHERE leased_until < now() AND lease_owner IS NOT NULL;
```

Критерии презентации:

- duplicate identities: 0;
- orphan relations: 0;
- stale leases после graceful shutdown: 0;
- queue после drain: 0;
- raw payload у основных entities: 100%;
- permanent failures: ≤ 1%;
- CSV validator: valid;
- EEP, Zakup и all проходят независимо.

## Итог

Кодовая база готова к controlled pilot и демонстрации архитектуры. Называть
сервис полностью production-ready без зафиксированного 10k live run и
production proxy/CAPTCHA credentials преждевременно.
