# SEO Service

Внутренний FastAPI-сервис EqSiteCMS: нормализация поисковых запросов и страниц,
пакетный find-or-create со стабильными UUID, списки с поиском/пагинацией и два
независимых черных списка нормализации страниц для каждой конюшни.
Оркестрирует ручные и ежедневные команды парсинга через NATS JetStream.
Настройки не изменяют ранее сохранённые страницы. API изменения/удаления запросов и страниц нет.

## Стек и архитектура

Python 3.14, FastAPI, SQLAlchemy Core + asyncpg, PostgreSQL с `pg_trgm`, Alembic,
Dependency Injector. Sentry опционален (`SENTRY_ENABLED`, `SENTRY_DSN`).

```text
src/
├── api/                  # HTTP, tenant preflight, транзакционные маршруты
├── clients/              # NATS publisher и private peer HTTP клиенты
├── containers/           # settings/repositories/services, request-local session
├── core/
│   ├── entities/         # сущности и PaginatedEntities
│   ├── exceptions/       # безопасные доменные ошибки
│   ├── protocols/        # контракты репозиториев
│   ├── schemas/api/      # входные/выходные DTO
│   └── services/         # нормализаторы и сценарии
├── depends/              # tenant selector и session lifecycle
├── models/               # SQLAlchemy Core tables
├── repositories/         # PostgreSQL, tenant isolation, find-or-create
├── providers/            # ParseProviderProtocol implementations
├── scheduler/            # отдельный процесс, без HTTP
├── migration/            # Alembic revisions
├── utils/                # инфраструктура и TransactionalRoute
├── main.py               # wiring, routers, error boundary, health
└── settings.py
```

## Доступ и граница доверия

Это **не публичный API**. `/api/*` доступен только доверенным внутренним сервисам
в `eqsitecms_network`: парсеры обращаются напрямую, CMS — через backend с ролью
**ADMIN** (без SUPERUSER/DEVELOPER override). seo-service не проверяет сервисные ключи,
Authorization или cookie; их наличие не меняет права/результат. Это согласованное
исключение из Protected Write и Public Read для внутренних SEO-данных.

Каждый зарегистрированный `/api/*` маршрут требует только несекретный UUID-заголовок
`X-Equestrian-Id`. Отсутствующий, пустой или невалидный selector → `401` **до разбора
тела и открытия session**. Существование конюшни не проверяется. Конюшню нельзя
передать через body/query вместо заголовка; лишние поля body запрещены.
Чужой элемент черного списка неотличим от отсутствующего (`404`).

Host-порт `EXPOSE_SEO_SERVICE_PORT` предназначен только для локальной разработки.
**Перед production необходимо подтвердить недоступность сервиса внешним callers;
без подтверждённой сетевой изоляции выпуск блокируется.** UUID selector не заменяет
авторизацию и не защищает от недоверенного caller внутри сети.

### Access matrix

В таблице ITN = Internal Trusted Network; роли ITN — доверенные сервисы, а не роли пользователей.
`with auth` означает любые cookie/Authorization: они игнорируются. Все пути без завершающего `/`.

| method | path | access class | roles | expected without auth | expected with auth |
|---|---|---|---|---|---|
| GET | `/health` | Public Read | любой | `200` | то же |
| POST | `/api/search-queries/resolve` | ITN Write (исключение) | парсеры | `200`; tenant `401`; body `400` | то же |
| POST | `/api/parse-jobs` | ITN Write (исключение) | backend | selector missing/invalid `401` до body; valid selector `202`; body/provider/нет привязки `400`; источник целей `502`; очередь `503` | то же, credentials игнорируются |
| POST | `/api/pages/resolve` | ITN Write (исключение) | парсеры | `200`; tenant `401`; body `400` | то же |
| GET | `/api/search-queries` | ITN Read (исключение) | backend | `200` с `metrics_table`; tenant `401` до БД/peer; query `400` до peer; сбой peer `502` | то же |
| GET | `/api/pages` | ITN Read (исключение) | backend | `200` с `metrics_table`; tenant `401` до БД/peer; query `400` до peer; сбой peer `502` | то же |
| GET | `/api/page-normalization/blacklisted-query-params` | ITN Read (исключение) | backend | `200`; tenant `401` | то же |
| POST | `/api/page-normalization/blacklisted-query-params` | ITN Write (исключение) | backend | `201`; `400`; `409`; tenant `401` | то же |
| PATCH | `/api/page-normalization/blacklisted-query-params/{item_id}` | ITN Write (исключение) | backend | `200`; `400`; `404` чужой/нет; `409`; tenant `401` | то же |
| DELETE | `/api/page-normalization/blacklisted-query-params/{item_id}` | ITN Write (исключение) | backend | `204`; `400`; `404`; tenant `401` | то же |
| GET | `/api/page-normalization/blacklisted-hashes` | ITN Read (исключение) | backend | `200`; tenant `401` | то же |
| POST | `/api/page-normalization/blacklisted-hashes` | ITN Write (исключение) | backend | `201`; `400`; `409`; tenant `401` | то же |
| PATCH | `/api/page-normalization/blacklisted-hashes/{item_id}` | ITN Write (исключение) | backend | `200`; `400`; `404`; `409`; tenant `401` | то же |
| DELETE | `/api/page-normalization/blacklisted-hashes/{item_id}` | ITN Write (исключение) | backend | `204`; `400`; `404`; tenant `401` | то же |

Неожиданная ошибка любого API-запроса → `500`. Формат ошибки: только `{"detail": ...}`.
Для валидации `400` detail — список `{type: "request_validation", loc: [...],
msg: "Некорректные параметры запроса"}` без input/ctx. Иные ошибки содержат безопасную
строку класса, `500` — `"Внутренняя ошибка сервиса"`; SQL, входные значения,
traceback и параметры подключения не возвращаются. Одна транзакция на обычный запрос (ledger парсинга — отдельные короткие транзакции):
commit до успешного ответа, rollback при ошибке. `204` не содержит тела.
Health статический `{"status":"ok"}`, без обращения к БД.

## Контракты

- Resolve: `{"items":["строка", ...]}`, 1..1000 строк без ограничения длины отдельной
  строки. `200 {"items":[{"source":"...","normalized":"...","id":"UUID"}]}`
  сохраняет порядок и дубли входа; одинаковые нормализованные значения получают один UUID
  в конюшне. Ошибка любого элемента отклоняет весь batch (`400`).
- Список запросов: `{items: [...], total: int, metrics_table: {...}}`; элементы запросов — `id/query/created_at`,
  страниц (см. ниже) — `id/page/created_at`. Внутренние hash и equestrian_id не выдаются.
  `search` — trim и регистронезависимое вхождение, `%`, `_`, `\` буквальны;
  `ordering=query|-query|created_at|-created_at|metric:YYYY-MM-DD|-metric:YYYY-MM-DD`, по умолчанию `created_at DESC, id DESC`.
  `limit=20` приводится к 1..100, `offset=0` — к ≥0; нечисловые значения и неизвестная
  сортировка → `400`. `total` учитывает поиск, но не пагинацию.
- Черные списки: body `{"value":"..."}`, список без пагинации по `value ASC`.
  Trim, у хеша удаляется один ведущий `#`. Значение 1..255 символов, без whitespace,
  управляющих символов и `#`; у query-параметра также запрещены `&`, `=`, `?`.
  Дубль внутри tenant и списка → `409`, malformed item_id → `400`.

### Страницы с метриками: `GET /api/pages`

Ответ: `{"items": [{"id": "UUID", "page": "/a", "created_at": "..."}],
"total": 1, "metrics_table": {...}}`. `total` зависит от tenant и поиска, но не
от пагинации или статистики. `search` — те же trim и буквальное вхождение.
`limit` — 1..100 (default 20), `offset` ≥0 (default 0); выход за диапазон → `400`
(список «Запросы» сохраняет прежнее приведение диапазона).

| Query | Значения / по умолчанию |
|---|---|
| `mode` | `day` (default), `week`, `month`, `year` |
| `metric` | `visits` (default), `users`, `pageviews`, `bounces`, `bounce_rate`, `page_depth`, `avg_visit_duration_seconds`, `visit_duration_seconds` |
| `date_from`, `date_to` | Только вместе, `YYYY-MM-DD`, начало ≤ конца |
| `ordering` | `page`, `-page`, `created_at`, `-created_at`, `metric:YYYY-MM-DD`, `-metric:YYYY-MM-DD`; default `created_at DESC, id DESC` |

Некорректные mode/metric/даты/ordering/пагинация → `400` до БД и ya-metrica.
«Сегодня» вычисляется в UTC со сдвигом `SEO_UTC_OFFSET_HOURS`. Без дат период
заканчивается сегодня и начинается: day — 6 дней назад; week — в понедельник
3 недели назад; month — 1-го числа 5 месяцев назад; year — 1 января прошлого года.
Для week/month/year явные даты расширяются до календарных границ. Будущее начало
заменяется сегодня до выравнивания; затем диапазон ограничивается числом единиц
от начала (day 31, week 16, month 12, year 5), после этого конец — сегодняшним днём.
Колонки идут по возрастанию; последняя текущая единица может быть неполной.

`metrics_table` — универсальный DTO: `version=1`, `row_key="id"`, `metric`
(key/title/value_type), `period` (mode, date_from/date_to, requested_date_from/to,
defaulted/adjusted/clamped, today), фактически применённый `ordering` и `columns`.
Колонки имеют `key="metric:YYYY-MM-DD"`, title/hint, sortable, value_type, align,
`empty_text="—"`, разреженный `values`, где ключ — UUID страницы, значение —
`{text, value, hint}`. Пропуск означает отсутствие данных, не ноль.

Поток: валидация → период → M4 ranking (только если сортируемая колонка есть)
→ SQL выборка и count → M3 table (только для непустой выборки) → builder.
M4 получает фактические границы колонки и направление asc/desc; SQL применяет
поиск, ранг и пагинацию, страницы без статистики идут последними, затем
`created_at DESC, id DESC`. Если колонки нет, M4 не вызывается, используется
дефолтная сортировка и `metrics_table.ordering=null`. M3 получает все границы
колонок и UUID текущей выборки; seo не агрегирует и не сортирует данные в памяти.
На запрос — ровно 2 SQL и до 2 peer-вызовов; пустая выборка без metric ordering
не вызывает peer, но сохраняет период и пустые values в ответе.

Зависимость: ya-metrica-service M3 `/api/metrica/page-stats/table`, M4
`/api/metrica/page-stats/ranking`. Передаётся tenant UUID без credentials.
Любой не-200 (включая 400), timeout, транспортная ошибка или неверная схема
peer → `502 {"detail":"Сервис статистики недоступен"}`, транзакция откатывается;
тело/URL peer не возвращаются.

### Запросы с метриками: `GET /api/search-queries`

Ответ: `{"items":[{"id":"UUID","query":"Лошадь","created_at":"..."}],
"total":1,"metrics_table":{...}}`. Таблица обязательна, `version=1`, `row_key="id"`;
значения содержат только UUID текущей страницы. Используются **те же** resolver,
registry, builder и форматтеры, что у страниц: восемь metrics, четыре режима,
периоды/defaults/limits и разреженные ячейки из раздела выше. Пропуск — `—`, не ноль.

Параметры: `search`, `ordering=query|-query|created_at|-created_at|±metric:YYYY-MM-DD`,
`mode=day`, `metric=visits`, парные `date_from/date_to`, `limit=20`, `offset=0`.
В отличие от страниц числовые limit/offset сохраняют clamp 1..100 / ≥0.
Нечисловые limit/offset и невалидные mode/metric/даты/ordering → `400` до данных/peer.
Default — полный `created_at DESC, id DESC`.

Поток: validation/clamp → общий период → M6 query ranking при существующей колонке
→ SQL list/count с tenant/search/rank до pagination → M5 batch table с UUID текущей
страницы и всеми buckets → адаптер query values → общий builder. M5:
`/api/metrica/search-query-stats/table`; M6: `/api/metrica/search-query-stats/ranking`.
Ровно два SEO SQL, ≤2 peer-вызовов; без metric ordering нет M6, без строк нет M5.
Валидная metric-дата вне периода даёт default SQL order и `metrics_table.ordering=null`
без M6. Нет агрегации или сортировки строк в Python, total не зависит от статистики.
Строки без статистики идут последними в обоих направлениях, затем default order.

Доступ — Internal Trusted Network Read: валидный UUID `X-Equestrian-Id` обязателен;
missing/empty/invalid → `401` до БД/peer. Cookie/Bearer игнорируются и не заменяют
selector. Tenant из selector используется и в SQL, и в M5/M6. Метрика non-200,
timeout/transport/schema/metric/direction mismatch → безопасный `502`
`Сервис статистики недоступен`, rollback, без частичного успеха или утечки URL/тела.
Новые порты не открываются. Production isolation остаётся отдельным release gate:
успешные тесты не подтверждают внешнюю недоступность peers и PostgreSQL.

| Env seo-service | Default | Назначение |
|---|---|---|
| `SEO_UTC_OFFSET_HOURS` | `3` | Целое −12..14; неверное значение блокирует запуск |
| `YA_METRICA_SERVICE_URL` | `http://eqsitecms-ya-metrica-service:8000` | Внутренний peer |
| `YA_METRICA_TIMEOUT_SECONDS` | `4` | Таймаут одного вызова, два последовательных ≤8 с (backend 10 с) |

**Согласуйте `SEO_UTC_OFFSET_HOURS` с `YA_METRICA_UTC_OFFSET_HOURS` ya-metrica.**
Сервисы читают только собственный env, автоматической синхронизации нет.
HTTP-клиент использует `trust_env=False`, без cookie/Authorization.

## Ручной запуск парсинга

`POST /api/parse-jobs`, JSON ровно `{"provider":"ya_metrica"}` и UUID
`X-Equestrian-Id`. Код — `^[a-z][a-z0-9_]{1,31}$`, лишние поля запрещены.
Конюшня берётся только из selector, не из body/query. После успешного PubAck и
аудита ledger ответ `202 {"job_id":"UUID","provider":"ya_metrica","status":"accepted"}`.
Это подтверждение постановки в очередь, не окончания парсинга: при уже активном
запуске исполнитель может подтвердить команду без нового запуска.
Неизвестный/отключённый провайдер, отсутствие привязки и невалидное тело → `400`;
недоступность yandex → `502`, NATS → `503` с безопасным detail.

CMS вызывает только backend `POST /api/seo/parse-jobs` (Protected Write): anonymous
`401`, только ADMIN `202`, остальные роли, включая SUPERUSER/DEVELOPER без ADMIN,
`403` до downstream/body. Backend берёт tenant из actor и передаёт только selector,
не credentials; upstream ошибки `400/502/503` сохраняют безопасный контракт.

## NATS JetStream

| Stream | Subject | Durable потребителя | Назначение | Роль |
|---|---|---|---|---|
| `PARSE_COMMANDS` | `commands.parse.ya_metrica.run` | `ya-metrica-parse-run` | Парсинг статистики конюшни | исходящий (Publisher) |

Канонический producer-контракт — [docs/asyncapi.yaml](docs/asyncapi.yaml), consumer —
`services/ya-metrica-service/docs/asyncapi.yaml`; из корня: `make asyncapi-validate-parse`.
API и scheduler используют общий клиент и идемпотентный `add_stream`: wildcard
`commands.parse.>`, FILE, LIMITS, max_age 604800 с (7 суток). Consumer создаёт только
свой durable: filter `commands.parse.ya_metrica.run`, explicit ack, deliver all,
ack_wait 30 с, max_deliver 5. Прямого HTTP-запуска парсинга в ya-metrica нет.

Envelope: `schema_version=1`, timezone-aware `occurred_at`, `job_id`, `provider`,
`equestrian_id`, `trigger=schedule|manual`, `window`, `params`; лишние поля запрещены.
Текущие dispatch-пути публикуют `window={"mode":"auto"}`, `params={}`; контракт также
описывает explicit-окно с парой дат. Даты вычисляет исполнитель, не seo: при
вчерашней последней статистике — вчера..сегодня, без статистики — последние 2 года.

`NAMESPACE_PARSE_COMMANDS = uuid5(NAMESPACE_DNS, "parse-commands.eqcms")` в обоих
сервисах. Schedule job_id = uuid5(namespace,
`"schedule:<provider>:<equestrian_id>:<локальная дата YYYY-MM-DD>"`), manual — uuid4.
`Nats-Msg-Id = uuid5(namespace, "<provider>:<job_id>")`; `PubAck.duplicate` проверяется
и логируется warning. Дополнительно consumer дедуплицирует сохранённый job_id.
Секретов, ciphertext, профиля и ID аккаунта Яндекса в команде нет.

NATS — Internal Trusted Network Write без auth, не Public API. **Production release
блокируется до подтверждения внешней недоступности peers, NATS и БД**; наличие
Compose network/успешных тестов этого не доказывает. Сетевые настройки здесь не меняются.
Недоступный NATS не блокирует HTTP startup/статический health: фоновый connect/setup
повторяется с backoff; scheduler также повторяет подключение и корректно останавливается.

## Расписание, ledger и настройки

Один отдельный процесс `seo-scheduler`, без HTTP/портов, тот же образ и собственный
`.env` seo-service. Локальный запуск: `PYTHONPATH=src uv run python -m scheduler.main`.
SIGTERM/SIGINT останавливают worker, закрывают NATS и сессии/engine БД.

| Env | Default | Назначение |
|---|---|---|
| `PARSE_SCHEDULE_ENABLED` | `true` | false отключает только расписание |
| `PARSE_SCHEDULE_TIME` | `03:00` | Строго HH:MM, локальное время |
| `PARSE_SCHEDULER_TICK_SECONDS` | `30` | Положительный интервал tick |
| `PARSE_PROVIDERS_ENABLED` | `ya_metrica` | CSV уникальных зарегистрированных кодов |
| `PARSE_TARGETS_PAGE_SIZE` | `100` | Размер страницы целей, 1..500 |
| `PARSE_DISPATCH_MAX_ATTEMPTS` | `5` | Лимит повторов failed-публикации |
| `YANDEX_SERVICE_URL` | `http://eqsitecms-yandex-service:8000` | Private peer источник привязок |
| `YANDEX_SERVICE_TIMEOUT_SECONDS` | `4` | Положительный timeout |
| `NATS_SERVERS` | `nats://eqsitecms-nats:4222` | CSV адресов брокера |
| `NATS_STREAM_PARSE_COMMANDS` | `PARSE_COMMANDS` | Stream-владелец seo |
| `NATS_SUBJECTS_PARSE_COMMANDS` | `commands.parse.>` | CSV subjects stream |
| `NATS_PARSE_COMMANDS_MAX_AGE_SECONDS` | `604800` | Хранение команд |
| `NATS_PUBLISH_TIMEOUT_SECONDS` | `5` | Timeout PubAck |
| `NATS_ERROR_REPORT_AFTER_ATTEMPTS` | `3` | Порог эскалации ошибки соединения |
| `NATS_SETUP_MAX_ATTEMPTS` | `10` | Число попыток topology setup за вызов |
| `NATS_SETUP_BACKOFF_SECONDS` | `2` | База backoff topology setup |

Начальные connect/setup дополнительно повторяются supervisor с backoff 1..30 с.
Невалидные настройки/неизвестный enabled provider отклоняются при старте/сборке DI.
Пример — [.env.example](.env.example); реальные env меняет только оператор.

Слот наступает при локальном времени ≥ расписания; дата слота — локальная дата.
Текущий слот догоняется до конца суток, вчерашний не переносится на завтра.
`SEO_UTC_OFFSET_HOURS` (−12..14, default +3) — общий пояс конюшен, тот же источник
«сегодня», что у метрик. **Он должен совпадать с `YA_METRICA_UTC_OFFSET_HOURS`**:
автосинхронизации нет, расхождение может сдвинуть окно на сутки. Расширение под
пояс конюшни — `TimezoneProvider.utc_offset_hours(equestrian_id)`.

`YaMetricaParseProvider` получает цели постранично через credential-less Private
Peer Read `GET /api/connections/metrica?limit=100&offset=0` yandex-service:
`{total, items:[{equestrian_id,counter_id}]}`, ORDER BY equestrian_id, default limit
100, допустимо 1..500, offset ≥0 (нарушение → `400`). Anonymous и с credentials —
одинаковый `200`; credentials игнорируются. Это внутренний реестр, не Public Read
для сайтов. Ручной preflight использует GET привязки по equestrian_id.

Ledger `parse_dispatches` в PostgreSQL seo: `pending/published/failed`, attempts,
безопасный last_error_code, timestamps. Partial unique index
`(provider,equestrian_id,schedule_date) WHERE trigger='schedule'` и атомарный claim
разделяют конкурентных scheduler. Порядок: claim → publish → mark, отдельные короткие
транзакции; failed повторяется до cap, stale pending возвращается после lease 5 мин.
После рестарта опубликованные цели пропускаются; детерминированный job_id сохраняется.
Это не гарантия exactly-once доставки: сбой после PubAck до mark покрывается повтором
и дедупликацией broker/consumer. Manual audit записывается после PubAck, без занятия
слота; при ошибке брокера записи нет. Ошибка цели не останавливает остальных;
ошибка списка/незавершённый проход не кэширует слот, следующий tick повторяет его.

### Как подключить нового провайдера парсинга

1. Реализовать `ParseProviderProtocol` в `providers/`: `code`, subject
   `commands.parse.<code>.run`, async `list_targets(*,offset,limit)` →
   `(list[ParseTarget], total)`, async `is_target(*,equestrian_id)` и
   `build_params(*,trigger)`. Клиент источника целей — через отдельный Protocol/DI.
2. Зарегистрировать провайдера в реестре DI (`containers/clients.py`; общий dispatch —
   `containers/services.py`), включить code в `PARSE_PROVIDERS_ENABLED`. Дубли и
   незарегистрированные коды отвергаются; registered, но disabled → API `400`.
3. В исполнителе создать durable с filter `commands.parse.<code>.run` и handler:
   валидировать envelope/subject/params, обеспечить job_id idempotency и ack/nak/term.
4. Добавить канал producer/consumer AsyncAPI, обновить README и выполнить validator,
   provider/dispatch unit tests и реальный broker gate с изолированными ресурсами.

Stream/wildcard, envelope, scheduler, API, ledger и общий dispatch менять не нужно.

## Запуск

Из корня монорепозитория, после настройки локальных env и общей БД/сети:

```bash
make seo-build
make seo
```

Монорепозиторий использует `.docker-compose/docker-compose.seo.yml` с сервисами
`seo-service`, `seo-scheduler` и `seo-migration`. Миграции запускайте через `seo-migration` с той же
compose-конфигурацией/env, что и приложение; `SKIP_MIGRATIONS` не должен быть `true`.
`scripts/stackctl` пока не поддерживает seo: используйте штатные make/docker compose.
До старта API/scheduler должны быть готовы БД, миграции и внутренняя сеть;
NATS/yandex нужны для dispatch. Compose не обеспечивает migration readiness barrier.
Backend использует `SEO_SERVICE_URL=http://eqsitecms-seo-service:8000` по умолчанию.

Локально из каталога сервиса (PostgreSQL настраивается через локальный env):

```bash
cp .env.example .env  # только при первичной настройке, не перезаписывать существующий
uv sync --locked
uv run alembic -c src/alembic.ini upgrade head
uv run uvicorn main:app --app-dir src --reload
```

Swagger — `/docs`. Отдельный `docker-compose.yaml` в каталоге сервиса — унаследованный
standalone dev-контур (`fastapi-template`, postgres:16-alpine), не compose монорепозитория.

## Тесты и проверки

Из каталога сервиса:

```bash
uv sync --locked  # httpx — runtime-зависимость peer-клиента
uv run pytest tests/unit tests/api
make lint
# Форматировать только изменяемые файлы, не весь сервис:
uv run ruff check tests/api/test_resolve_api.py --fix
uv run ruff format tests/api/test_resolve_api.py
```

IT-SEO-01..06 требуют **реальную PostgreSQL**, поддержку `pg_trgm` и права
CREATE SCHEMA/EXTENSION. Передайте `TEST_DATABASE_URL` через окружение/секрет-хранилище
(драйвер `postgresql+asyncpg`); не печатайте URL с паролем в терминал или логи.
Runtime settings для подключения IT не используются. Без переменной IT пропускаются.

```bash
# TEST_DATABASE_URL уже безопасно установлен в окружении:
uv run pytest tests/integration -vv
make test        # unit/API + IT миграций/моделей с тем же TEST_DATABASE_URL
make test-infra  # IT репозиториев, включая concurrency; вместе это полный suite
```

Каждый IT создаёт случайную схему `test_seo_<uuid>`, применяет реальные миграции и
удаляет только свою схему в finally, если не задан `TEST_KEEP_SCHEMA=1`.
Для недеструктивного прогона задайте `TEST_KEEP_SCHEMA=1`: схемы остаются в тестовой БД. Используйте выделенную тестовую БД/инстанс,
никогда рабочие данные. IT проверяют upgrade/downgrade/upgrade, constraints/hash/TEXT,
идемпотентность, конкурентные batch, поиск/сортировку/tenant isolation и blacklists.
Live smoke — отдельный этап, не часть этого тестового прогона.

Broker gate (из корня, отдельный от default suite):

```bash
TEST_NATS_URL=nats://127.0.0.1:4222 uv run --directory services/seo-service pytest -m infrastructure tests/integration/test_nats_broker.py -q
make asyncapi-validate-parse
```

Отсутствие env/реального JetStream — FAIL, не skip. Тест использует уникальные
stream/subjects и обязательный cleanup, проверяет FILE/LIMITS/max_age,
идемпотентный add_stream, публикацию и duplicate; consumer broker gate отдельно
проверяет durable/filter/ack/nak/redelivery/max_deliver/term в ya-metrica-service.

### Additive normalization storage (106–107, runtime OFF)

Alembic head `20261102_0004` (parent `20261028_0003`) adds a sequence-backed
`pages.scan_seq`, unique global sequence index and `(equestrian_id, scan_seq)`
keyset index. Existing pages receive values from `nextval` during migration:
apply only in the agreed maintenance window with admissions stopped. There is
no historical URL rewrite or statistics mutation in this migration.

New tables: `normalization_tenants`, `normalization_revisions`,
`normalization_snapshots`, `normalization_scans`, `normalization_mappings`,
`normalization_redirects`, `normalization_inbox`, `normalization_outbox`.
Snapshots retain both rule lists; mappings/UUID redirects deliberately do not
reference physical pages (they must survive source deletion). Revision links
are tenant-qualified. A partial unique index allows at most one non-pending,
non-finalized revision per tenant; blocked revisions retain this barrier.

This schema alone does **not** enable producers, scans or consumers. Storage
repositories enforce tenant membership/cycle checks and oldest-revision
activation; callers must lock before page inserts. S-1c adds atomic effect/checkpoint/
lease CAS below. Database CHECK/UNIQUE constraints are not
substitutes for those guards. Domain contracts live in
`core/entities/normalization.py` and
`core/protocols/repositories/normalization_repository.py`.

Rollback: disable new producers/workers and retain additive schema. Downgrade
refuses **any** nonempty normalization table, including early inbox results,
so active mappings, redirect chains, barriers and delivery intents cannot be
silently lost. Empty-schema downgrade removes only the additive storage and
scan sequence/column/indexes; existing page UUIDs/paths remain. After the first
merge/delete, use a coordinated backup restore or forward-fix, never discard
ledgers or restart old parsing over unfinished operations.

Schema PostgreSQL checks (require explicit `TEST_DATABASE_URL`, isolated test
schema): `uv run pytest tests/integration/test_normalization_schema_postgres.py
 tests/integration/test_migrations_postgres.py -m infrastructure`. Missing URL
means not verified, not acceptance; no runtime database is chosen implicitly.


### Репозитории нормализации S-1b (runtime OFF)

`repositories.normalization_repository.NormalizationRepository(session)` работает
только в **транзакции вызывающего кода**, без commit/rollback и отдельной сессии.
`lock_tenant(equestrian_id=...)` создаёт lock-row идемпотентно и берёт row lock до
конца транзакции. `create_revision` / `activate_next` / `store_mappings` /
`resolve_redirect` берут этот же lock сами. Для rule POST caller сначала берёт
lock, меняет правило, затем вызывает `create_revision` с полным снимком **обоих**
списков в той же транзакции. Page resolve обязан взять lock **до чтения правил и
INSERT страницы / выделения scan_seq**. Текущий PageRepository/runtime пока не
переключён: это интеграция S-2, не разрешение включать scan.

- `create_revision`: монотонный tenant revision, immutable snapshot и pending
  scan вместе; дубликат operation — ошибка, не замена снимка. При rollback
  откатываются номер и все строки. Duplicate rule POST caller отсекает до этого.
- `activate_next`: только oldest unfinished и только pending; любой активный,
  включая blocked, удерживает барьер. High-water `max(scan_seq)` своей конюшни
  фиксируется при **активации**, не при enqueue; пустая конюшня получает 0.
- `get_revision` / `get_checkpoint`: строго tenant + operation. Неизвестная
  ревизия даёт None, неизвестный checkpoint — безопасную ValueError.
- `scan_pages`: `cursor < scan_seq <= high_water`, сортировка по scan_seq,
  limit 1..500, без OFFSET. Pending scan без high-water не читается. Caller
  продвигает cursor, а на последней/пустой выборке доводит его до high-water;
  отсутствующие sequence значения не означают незавершённость scan.
- `store_mappings`: 1..500 элементов, все source/target — live pages своей
  конюшни; source_scan_seq сверяется с реальной страницей и high-water.
  Источники уникальны внутри batch и revision, точный повтор идемпотентен,
  конфликт target/seq/part_index отклоняется. Target обязан быть терминальным
  (не source в batch и не существующий alias), что запрещает цепь/цикл нового
  batch. Mapping+redirect записываются **после всех проверок**, без N+1:
  два lock SQL + пять bounded чтений + два bulk INSERT независимо от размера
  batch; membership/alias запросы содержат не более 1000 UUID.
- `get_mappings`: tenant/operation keyset `source_scan_seq > after_seq`,
  limit 1..500. Подходит для последующих bounded delete, но сам ничего не удаляет.
- `resolve_redirect`: tenant-scoped chain, максимум 64 перехода; цикл, слишком
  длинная цепь или отсутствие живой терминальной страницы дают ValueError без
  частичной compression. Успешная цепь сжимается под tenant lock. Старые aliases
  разрешаются после physical source deletion; FK на pages намеренно отсутствует.
  Превышение лимита — fail closed, не возврат устаревшего source UUID.

`store_mappings` — **не worker entrypoint**: он не проверяет lease, не двигает
checkpoint и не пишет outbox. Worker использует `commit_scan_batch`: lease/checkpoint
CAS, mappings/redirects/outbox/checkpoint внутри savepoint общей caller transaction.
Прямое использование `store_mappings` требует caller fencing/rollback. После seal запись mappings запрещена;
awaiting_cleanup допускается лишь пока scan не sealed. Никакие ранние cleanup
results сами по себе не разрешают удаление SEO sources.

Репозиторные PG fixtures: `uv run pytest -m infrastructure
 tests/integration/test_normalization_repository_postgres.py`. Покрывают >2
keyset/mapping batch, snapshot/revision queue, late insert/high-water, tenant
isolation, идемпотентность/конфликты, cycle/compression/deleted alias, caller
rollback и lock races. Это **fixtures, не evidence успешного PG acceptance**
при отсутствующем TEST_DATABASE_URL; delivery/checkpoint fixtures описаны ниже,
полный IT-N-01 и cross-service acceptance остаются S-3/QG.


### Delivery/checkpoint storage S-1c (runtime OFF)

Новых HTTP endpoints, NATS consumers или scheduler wiring нет. Head/schema не
менялись; все primitives используют переданный `AsyncSession`, без outer
commit/rollback. SQL/driver ошибки санитизируются, payload/credentials не логируются.

- `claim_scan(equestrian_id, operation_id, now, duration) -> StorageLease | None`:
  короткий tenant lock, только scanning/awaiting_cleanup/deleting с high-water;
  свободная/истёкшая lease заменяется новым UUID token, attempts увеличивается.
  Claim нужно **commit до внешнего I/O**, не держать транзакцию на HTTP/NATS.
  Эта lease принадлежит operation; разрешение physical delete и correlation
  completed/finalized реализует S-3, не claim. Сам claim никогда ничего не удаляет.
- `commit_scan_batch(... lease, expected, checkpoint, mappings, messages, now)`:
  tenant lock → savepoint → token/until/attempts/full checkpoint CAS под row lock
  → membership/cycle-checked `store_mappings` → outbox → checkpoint. UTC-aware
  clock обязателен; expiry проверяется и по `now`, и по реальному PostgreSQL
  `clock_timestamp()` **до и после эффектов**. Lease loss/conflict откатывает
  savepoint целиком даже если caller поймал ValueError. Outer commit остаётся
  ответственностью caller. Не оставлять ORM pending changes перед begin_nested
  (SQLAlchemy может flush их до savepoint).
- Batch: максимум 500 mappings, один part при непустом mapping batch, part_index
  равен expected.next_part_index, новый next_part_index увеличивается на 1.
  Все sources лежат в `(expected.cursor, checkpoint.cursor]`, watermark неизменен,
  delete_cursor scan-метод не меняет. No-op/unchanged страницы двигают cursor без
  part. Seal требует cursor==high_water и одно terminal message (part_index=None);
  рекомендован отдельный пустой seal batch. Каждый успешный batch **освобождает
  lease**; следующий batch заново claim в новой транзакции. Старый checkpoint/token
  не принимается. С S-2b seal также атомарно сохраняет revision.part_count и
  awaiting_result (либо сохраняет awaiting_cleanup) внутри того же savepoint и
  final clock fence. Это НЕ finalized и НЕ разрешение удаления source.
- `enqueue_outbox(message)`: tenant operation обязана существовать; точный retry
  no-op, конфликт message_id либо `(tenant,operation,kind,part_index-or-terminal)`
  даёт ValueError, не overwrite. `MaintenanceMessage.payload_hash` — SHA-256
  канонического `json.dumps(payload, sort_keys=True, separators=(",", ":"),
  allow_nan=False)` (default ensure_ascii=True), как в metrica storage. JSON object
  и canonical body ≤256 KiB; schema/UUID-only/correlation валидирует transport/S-3.
- `claim_outbox(equestrian_id, now, duration, limit=100)` (1..500) возвращает
  `Sequence[OutboxClaim(message, lease)]`: ready unpublished rows, tenant-scoped
  `FOR UPDATE SKIP LOCKED`, новый token/attempts. Commit → publish вне транзакции
  → `finish_outbox(claim, now, published=True)` **только после PubAck** → commit.
  False от finish означает stale/expired claim: опубликованный payload может
  прийти повторно, это нормально. При ошибке `published=False, retry_after=1..60s`
  оставляет намерение без cap/purge, очищает lease и ставит next_attempt_at.
- `accept_inbox(consumer, message)` возвращает True только на новую durable запись;
  exact identity/hash/content retry — False независимо от broker window, foreign
  tenant/operation или другой hash с тем же `(consumer,message_id)` — ValueError.
  Unknown early cleanup result разрешено сохранить **без revision FK**, но это
  НЕ разрешение SEO delete. False НЕ означает processed: потерянный ACK либо ранний
  result требует replay/correlation. `mark_inbox_processed(consumer,message,now)`
  вызывается вместе с business effects в outer transaction; ACK лишь после commit.

PG fixtures: `uv run pytest -m infrastructure
 tests/integration/test_normalization_delivery_postgres.py` — migration-backed
atomic rollback/restart, >2 bounded batches/seal, lease/checkpoint fencing,
конкурентные claims/inbox, lost PubAck/reclaim и outbox conflict rollback.
Без TEST_DATABASE_URL это **не PG acceptance**. Runtime остаётся OFF; полный suite
и результат/cleanup/delete/finalized integration относятся к S-3/QG. Rollback
по-прежнему оставляет additive schema и все ledger/outbox/inbox: не сбрасывать
leases/барьеры вручную и не downgrade непустые таблицы.

### Request-side normalization (106–107, S-2a; worker runtime OFF)

`NormalizationService` — обязательная зависимость page/blacklist services; DI
создаёт все участвующие repositories с одной context-local request session.
`TenantTransactionalRoute` владеет outer transaction: commit до ответа, rollback
при любой ошибке. Сервисы не коммитят и не обращаются к брокеру.

- POST обоих списков валидирует значение, берёт общий tenant lock **до** duplicate
  lookup/insert, сохраняет правило, отсортированный snapshot обоих списков,
  монотонную pending revision и scan row в одной транзакции. Эти строки — durable
  scan intention, не синхронный обход pages. Duplicate/invalid POST не планирует
  работу; ошибка последующего snapshot/revision откатывает весь запрос.
- Resolve берёт тот же lock **до** чтения текущих правил и выделения `scan_seq`.
  Используется прежний `PageNormalizer`, затем обычный page resolve и перевод UUID
  по действующим tenant-scoped redirects. Поле `normalized` остаётся результатом
  текущих настроек; aliases переводят UUID, а не навсегда переписывают URL.
- PATCH/DELETE тоже сериализуются lock, но **не создают** revision/scan и не меняют
  уже сохранённый snapshot. Будущий resolve видит новые настройки. Уже слитые
  варианты/распределения не восстанавливаются; после удаления правила допустим
  новый UUID ранее разрешённого варианта, но не воскрешение удалённого UUID.
- POST не активирует revision: `activate_next` будущего worker фиксирует high-water
  под тем же lock только для oldest pending после finalized предшественника.
  Нельзя обрабатывать следующую revision при blocked/awaiting_result/cleanup.

HTTP DTO/status/selector policy не менялись: seo peer routes — Internal Trusted
Network exceptions из matrix выше, Cookie/Bearer не заменяет tenant selector;
Protected Write gateway остаётся строго SEO (105), без ADMIN override.

S-2a не запускает stack/scan scheduler/transport, не удаляет source pages и не
снимает parse gates. Bounded scan, chunk/seal orchestration описаны ниже (S-2b);
consumer correlation, safe delete и finalized — S-3.

Request tests: `uv run pytest tests/unit/test_normalization_service.py
 tests/unit/test_blacklist_services.py tests/api/test_blacklist_api.py
 tests/api/test_resolve_api.py -m "not infrastructure"`.
`tests/integration/test_normalization_service_postgres.py` содержит migration-backed
IT-N-01 fixtures для atomic rollback/duplicate и обоих порядков POST↔resolve,
activation watermark/queued snapshot/tenant isolation. Без `TEST_DATABASE_URL`
они не выполнялись; collection/skips **не** являются PG acceptance.


### Bounded scan orchestration (106–107, S-2b; runtime OFF)

`ServicesContainer.normalization_scan_service()` создаёт
`NormalizationScanService(normalization_repository, page_repository, page_normalizer,
batch_size=500, max_body_bytes=262144)` с той же caller-owned session.
Ни lifespan, ни scheduler/worker/transport этим unit не подключены.

- `run_batch(equestrian_id, now, duration=30s) -> (operation_id, ScanCheckpoint) | None`
  выполняет **ровно одну** ограниченную транзакционную единицу. Caller открывает
  transaction, вызывает метод и commit; при **любой** ошибке — полный rollback,
  включая canonical targets, созданные до storage savepoint. Не ловить ошибку
  lease с последующим commit. Между вызовами — новая TX и новый claim; никаких
  HTTP/NATS calls под tenant lock. Работа с миллионом страниц — повторные вызовы,
  не цикл в одной транзакции и не загрузка всех mappings.
- Под tenant lock `get_active_revision` читает oldest unfinished. Pending
  активируется через `activate_next`, high-water фиксируется один раз. Scanning
  и unsealed awaiting_cleanup возобновляются с durable cursor; leased возвращает
  None. Awaiting_result, sealed awaiting_cleanup, deleting и blocked блокируют
  последующие revisions **до finalized**, включая пустую/no-op revision.
  None означает idle/wait/lease, а не успешную финализацию или разрешение parse.
- Keyset `(cursor, high_water]`, максимум 500 страниц: неизменяемый snapshot
  обоих списков и существующий PageNormalizer. Изменённый путь использует обычный
  PageRepository.resolve (find-or-create), затем terminal UUID через redirects;
  bounded cache target paths живёт только один batch. Новые targets/resolve
  после watermark не расширяют scan. Unchanged и source==terminal target не
  создают mapping. Sequence gaps продвигают cursor до high-water после короткого
  последнего/пустого batch.
- Один непустой mapping batch → один part, максимум 500 пар и 256 KiB полного
  canonical JSON body; на меньшем bytes limit часть заканчивается раньше, cursor
  не перескакивает первую не записанную source. Все URLs остаются в SEO, в outbox
  только UUID. Message ID: uuid5(NAMESPACE_URL,
  `eqcms:maintenance:v1:commands.seo.pages.merge:<operation>:<part_index-or-seal>`).
  Envelope v1 с UTC occurred_at, normalization_revision и kind=mappings/seal;
  hash — SHA-256 canonical JSON. Relay S-3 передаёт сохранённое тело/ID **без
  пересоздания timestamp**, публикует на `commands.seo.pages.merge` с этим
  Nats-Msg-Id и отмечает published только после PubAck.
- `commit_scan_batch` атомарно сохраняет mappings+aliases+outbox+checkpoint,
  проверяет lease/CAS/реальный DB clock до и после эффектов и освобождает lease.
  Следующий вызов после cursor==high_water — отдельный seal-only batch, part_count
  из durable next_part_index. Даже empty/no-op отправляет seal(0) и ждёт результат
  S-3, без shortcut finalized. Seal+revision status/part_count теперь входят в тот
  же savepoint: awaiting_result либо сохранённое awaiting_cleanup с correlation.
  Рестарт после committed seal не создаёт его снова и не активирует следующую
  revision. Очередная revision после finalized получает новый watermark.
- Cleanup supersession НЕ останавливает ограниченный SEO scan/aliases и НЕ
  разрешает удаление источников. S-3 должен дождаться одновременно seal и
  correlated cleanup.completed, безопасно удалить только mapped source UUID,
  сохранить finalized+outbox и лишь затем разрешить следующую revision. Метрика
  удерживает awaiting_seo до correlated finalized. В этом unit source не удаляются.

Targeted: `uv run pytest tests -k "normaliz or merge or page" -m "not infrastructure"`.
`tests/integration/test_normalization_scan_postgres.py` содержит migration-backed
fixtures для >2 batches, existing/new targets, watermark/restart, stale fence
rollback, seal atomic rollback, queued revisions, concurrent workers/foreign tenant
и durable lease/reclaim. Без TEST_DATABASE_URL выполнена только collection,
**не PostgreSQL acceptance**; full suite и transport runtime остаются S-3.

### Maintenance transport foundation (S-3a, runtime OFF)

| Stream / owner | Subject | Durable | Роль SEO |
|---|---|---|---|
| SEO_MAINTENANCE / seo-service | commands.seo.pages.merge | ya-metrica-pages-merge | исходящий mappings/seal |
| SEO_MAINTENANCE / seo-service | events.seo.pages.merge.finalized | ya-metrica-pages-merge-finalized | исходящий finalized |
| METRICA_MAINTENANCE_RESULTS / ya-metrica-service | events.metrica.pages.merge.completed | seo-metrica-pages-merge-completed | входящий result |
| METRICA_MAINTENANCE_RESULTS / ya-metrica-service | events.metrica.cleanup.completed | seo-metrica-cleanup-completed | входящий cleanup |

`NatsJetstreamClient.setup_maintenance()` — отдельный opt-in setup, НЕ вызывается
API, parse scheduler или lifespan. Создаёт только SEO_MAINTENANCE (FILE/LIMITS,
без expiry); регистрирует два pull/ALL/explicit ack=30s/max_deliver=-1 durable на
чужом results stream. Только NotFound повторяется (10 попыток, линейный шаг 2s);
несовместимый config падает сразу. Существующий PARSE_COMMANDS не меняется.
Настройки stream/durable — `NATS_STREAM_SEO_MAINTENANCE`,
`NATS_STREAM_MAINTENANCE_RESULTS`, `NATS_CONSUMER_MERGE_COMPLETED`,
`NATS_CONSUMER_CLEANUP_COMPLETED`. Изменение имён требует согласованной topology.

`MaintenancePublisher` публикует сохранённый `MaintenanceMessage.payload_json`
как bytes и прежний `message_id` (Nats-Msg-Id); проверяет UUID-only schema,
canonical hash, tenant/operation и детерминированный ID. Timestamp/envelope не
пересоздаются. `finalized` suffix: `finalized:<result_message_id>`.
PubAck.duplicate логируется warning и считается доставленным повтором.

`NormalizationOutboxRelay(transaction=..., publisher=..., clock=..., batch_size=100,
lease_duration=30s).run_batch(equestrian_id=...) -> int` возвращает число успешно
отмеченных доставок. `transaction()` обязан выдавать repository одной session,
commit при успешном выходе и rollback при ошибке. Claim фиксируется отдельной TX,
publish выполняется без открытой TX, finish — новой короткой TX. Retry 1..60s без
cap, с безопасной эскалацией после 5 попыток; claim expiry/CAS не позволяет
устаревшему relay отметить delivery. Ошибка mark/commit или отмена процесса
оставляет durable claim: после expiry повторяются тот же body/ID, включая потерю
PubAck и crash между PubAck и mark. Не удалять outbox/ledger ради recovery.

DI использует factory `clients_container.maintenance_publisher()`;
relay transaction adapter, tenant enumeration, result handlers, scan/delete
runner и supervision/readiness подключены отдельным opt-in process (см. ниже). Созданный durable
сам по себе НЕ readiness и НЕ доказательство consumer startup. Empty scan
обязательно публикует seal(0), ждёт result и finalized; relay не снимает barrier.
До реальных PostgreSQL/JetStream gates runtime не включать; mocks/collection не
заменяют acceptance. Broker storage loss требует операторского replay ledger,
не сброса барьеров. Новых HTTP endpoints/ролей нет.

### Результаты и финализация нормализации (S-3b, runtime OFF)

`NormalizationFinalizationService(repository, batch_size=500)` не запускается
из API/parse scheduler. Runtime adapter, subscriptions, tenant dispatcher и
supervision остаются отдельным S-3c; до реальных PG/NATS gates runtime выключен.
Новых HTTP endpoints и изменений peer access policy нет.

- `accept_result(message=MaintenanceMessage, now=UTC) -> bool`: виды
  `merge_completed` / `cleanup_completed`; строгие UUID-only DTO, canonical
  SHA-256 JSON, версия/UTC/tenant/operation/revision/полнота проверяются до
  разрешения удаления. Caller начинает TX, **commit до ACK**, rollback при
  ошибке. `False` означает точный duplicate, **не** processed. ValueError —
  безопасная permanent validation/correlation ошибка; временные storage ошибки
  требуют retry/nak. Не логировать payload или текст driver/Pydantic exception.
- Inbox сохраняет ранние cleanup и merge reports независимо от redelivery.
  `get_inbox_result(consumer, equestrian_id, operation_id)` читает также processed
  cleanup ledger; поиск ограничен двумя строками для обнаружения конфликта.
  Другой message ID/hash для уже принятого результата операции — conflict,
  а не перезапись разрешения. Неизвестный/чужой merge не принимается.
- Superseded report переводит ревизию в `awaiting_cleanup`, не останавливая
  неоконченный bounded scan/aliases. **Оба** барьера обязательны: scan sealed и
  cleanup.completed с теми же tenant, cleanup operation ID и seq. Ранний
  completed также сохраняется, но не разрешает delete до seal/точного part_count.
- `run_batch(equestrian_id, now, duration=30s) -> (operation_id, checkpoint) | None`:
  одна caller TX и максимум 500 durable mappings. `None` — ожидание/idle/lease,
  не finalized. На ошибке обязательно rollback всей caller TX; перед SAVEPOINT
  не оставлять ORM pending changes. Claim допускает `awaiting_result`,
  `awaiting_cleanup`, `deleting`; storage повторно проверяет все барьеры.
- `commit_delete_batch` удаляет только source UUID своего tenant по mapping
  keyset `source_scan_seq > delete_cursor`; targets, UUID redirects и ledger
  сохраняются. Эффект/checkpoint/release lease атомарны; token, attempts,
  expected checkpoint и expiry проверяются до эффекта и DB clock — после.
  После полного батча нужен следующий (возможно пустой) batch для завершения.
  После crash сервис продолжает durable delete cursor, без повторного эффекта.
- Последний batch атомарно сохраняет `finalized`, processed marker merge result
  и outbox с `result_message_id`. UUID5 suffix `finalized:<result_message_id>`;
  relay отправляет сохранённый body/ID. Empty revision тоже проходит seal(0),
  correlated result и этот путь финализации. Следующий scan активируется только
  после finalized, в новой TX с актуальным high-water; до доставки finalized
  Метрика продолжает удерживать свой parse gate.

Scoped fixtures: `tests/unit/test_normalization_finalization_service.py` и
`tests/integration/test_normalization_finalization_postgres.py` (IT-N-01).
Collection/skip без `TEST_DATABASE_URL` не является PG acceptance; без
`TEST_NATS_URL` реальный broker gate также остаётся OPEN.

### Адаптер результатов maintenance (S-3c, runtime по-прежнему OFF)

`repositories.normalization_transaction.NormalizationTransaction(SessionFactory)`
предоставляет новый `AsyncSession` и короткую `session.begin()` на каждый вызов;
commit выполняется при успешном выходе, ошибки и cancellation откатывают TX.
Эта фабрика совместима с `NormalizationOutboxRelay`: публикация остаётся **вне** TX.

`clients.nats.maintenance_results.MaintenanceResultHandler` переводит только два
result subject в строгий DTO и сохранённый `MaintenanceMessage` (исходное тело,
SHA-256 canonical JSON). Он вызывает `accept_result` и возвращается только после
commit inbox. `MaintenanceResultConsumer.process()` выполняет ACK после commit,
включая exact redelivery; invalid schema/tenant/correlation → TERM, временный
storage/commit failure → NAK с backoff 1..60s и безопасной эскалацией с 5-й ошибки.
Cancellation не подтверждает сообщение. Payload, headers и текст исключения не
попадают в diagnostics. Батчи scan/delete в обработчике не выполняются.

`MaintenanceResultConsumer.run()` привязывает существующий durable, fetch по одному
сообщению; fetch/ACK failures выходят к process supervisor, а не скрываются в
отсоединённой задаче. При остановке локальная подписка закрывается без удаления
durable/stream. Ни импорт модуля, ни HTTP/parse scheduler не запускают consumer.

**Service runtime подключён в S-3c2b:** отдельный DI/process, startup
retry/supervision/readiness и opt-in Compose описаны ниже; rollout остаётся OFF.
До реальных gates запускать consumer/relay в рабочем стеке нельзя. Настройка
включения default OFF, auto-start API/scheduler не добавлен. PG/NATS gates OPEN;
unit/mocked tests и collection PostgreSQL fixtures их не закрывают. Полный unit
suite выполнен S-3c2b; результат и оставшиеся infra gates — в change tasks.md.

### Bounded tenant dispatch (S-3c2a, runtime OFF)

`NormalizationRepository.list_work_tenants(now, after=None, limit=100)` возвращает
не более 1..500 tenant UUID в порядке UUID, без OFFSET/tenant locks. Объединяются
незавершённые revisions (включая blocked/waiting) и **due unpublished outbox**, у
которого нет живой lease. Поэтому finalized revision с ещё не доставленным
финальным событием не исчезает из dispatch. Каждая distinct ветка и общий результат
ограничены limit; одна ревизия/конюшня с множеством сообщений не заполняет страницу
дубликатами. Discovery — hint, не claim; проверки состояния/tenant и fencing
повторно выполняются существующими batch services/relay.

`NormalizationDispatcher.tick() -> int` обрабатывает одну страницу tenants, по
одному scan, finalization и relay batch для каждого tenant; возвращает количество
ошибок стадий. Discovery/commit failure выходит к process supervisor, stage error
изолирован и логируется без exception text/payload. Supervisor обязан учитывать
ненулевой результат для health/backoff, а не выдавать успешную readiness. Cancellation
распространяется немедленно. Short/empty page сбрасывает UUID cursor; полный page
продолжается в следующем tick, без бесконечного обхода внутри одного tick. После
restart cursor начинается с начала, но scan/delete/revision checkpoints остаются
в БД. Новые меньшие UUID подхватываются на следующем проходе.

`depends.normalization_runtime.NormalizationBatchTransactions(SessionFactory,
normalizer=..., scan_batch_size=500, delete_batch_size=500)` предоставляет `scan()`
и `finalization()` async contexts. Каждый контекст создаёт отдельные session/begin;
page и normalization repositories внутри scan используют одну сессию. Commit/rollback
заканчивается **до** следующей стадии и до network relay. HTTP ContextLocalResource
не используется. Scan successor запускается только в следующей TX после финализации
предшественника; переноса checkpoint в память dispatcher нет.

Эти фабрики подключены `build_runtime` отдельного process (S-3c2b, см. ниже).
Parse scheduler/lifespan не изменены; Compose opt-in, readiness локальная.
Новые fixtures: `tests/unit/test_normalization_dispatcher.py`,
`tests/unit/test_normalization_batch_transactions.py`,
`tests/integration/test_normalization_dispatcher_postgres.py`. Последний проверяет
реальную SQL discovery/restart на выделенной PostgreSQL; его collection не acceptance,
а mocked relay в нём не является real NATS gate.

### Отдельный normalization process (код подключён, rollout OFF)

`PYTHONPATH=src uv run python -m maintenance.main` — отдельный процесс, не HTTP
lifespan и не parse scheduler. По умолчанию завершается без network/DB startup:
нужен `SEO_NORMALIZATION_ENABLED=true`. Compose дополнительно требует явный
профиль `seo-maintenance`; обычный `compose up` его не запускает. Один экземпляр
`seo-maintenance`, не добавлять второй запуск в API/scheduler. **Не включать в
рабочем стеке до реальных PostgreSQL/JetStream gates**: unit/mock/collection не
являются acceptance. Новых HTTP endpoints нет; `/health` не readiness worker.

| Переменная | Default | Границы / назначение |
|---|---|---|
| `SEO_NORMALIZATION_ENABLED` | false | явное разрешение отдельного process |
| `SEO_NORMALIZATION_TICK_SECONDS` | 2 | 0.1..60 s между bounded ticks |
| `SEO_NORMALIZATION_TENANT_PAGE_SIZE` | 100 | 1..500 tenants/tick, UUID keyset |
| `SEO_NORMALIZATION_SCAN_BATCH_SIZE` | 500 | 1..500 pages/scan TX |
| `SEO_NORMALIZATION_DELETE_BATCH_SIZE` | 500 | 1..500 source pages/delete TX |
| `SEO_NORMALIZATION_RELAY_BATCH_SIZE` | 100 | 1..500 claimed messages/tenant |
| `SEO_NORMALIZATION_READINESS_FILE` | .seo-maintenance-ready | локальный writable heartbeat, не shared volume |

Фабрика `depends.normalization_runtime.build_runtime` собирает prepared dispatcher,
relay, два result consumers и fresh session TX; никакой request session/container
в app.state не требуется. Startup создаёт только отсутствующий свой SEO_MAINTENANCE
(FILE/LIMITS, max_age=0, max_msgs/max_bytes=-1, discard=new, duplicate_window=120s)
и отсутствующие durables на чужом results stream. До consumers выполняется stream_info
обоих streams: точные subjects и полный canonical config владельца; чужой stream
не создаётся/не исправляется. Existing durable проверяется как pull/ALL/explicit/30s/-1
с точным filter, без backoff/push/headers-only; silent update запрещён.
NotFound и transient network/503: lookup retry `NATS_SETUP_MAX_ATTEMPTS` × линейный
`NATS_SETUP_BACKOFF_SECONDS`. `MaintenanceTopologyError` (BadRequestError) — fail-fast
существующего supervisor; несовместимый ресурс исправляется только отдельным решением оператора;
временные setup/worker ошибки — общий restart с backoff 1..60s и безопасным логом.
Неисправный consumer не остаётся detached: sibling tasks отменяются, subscriptions
закрываются, NATS закрывается, epoch создаётся заново. SIGINT/SIGTERM отменяет
workers, сбрасывает readiness, закрывает NATS и database pool.

`PYTHONPATH=src uv run python -m maintenance.readiness` — локальная probe (exit
0/1), не новый endpoint. Требует свежий heartbeat <5s: соединение, **две реально
bound subscriptions**, живые задачи и успешный dispatcher tick. Failed batch,
consumer DB retry, disconnect/exit/startup/shutdown => not ready. Heartbeat
обновляется раз в секунду; зависший процесс не сохраняет readiness. Долгий первый
tick держит readiness false. Broker restart/fetch failure пересоздаёт обе подписки;
committed scan/delete checkpoints, inbox и outbox остаются в PostgreSQL. Повтор
после PubAck/ACK потери использует saved body/Msg-ID и ledger, не окно dedup.

После сбоя повторный запуск возвращается к ledger: discovery cursor может
начаться сначала, но scan/delete эффекты и checkpoint атомарны, outbox claim
возобновляется после lease expiry. Не удалять retained stream/ledger/redirects
для «восстановления». Потеря broker storage требует операторского replay из
producer ledger и повторного acceptance; не снимать барьеры вручную.
