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
