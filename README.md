# SEO Service

Внутренний FastAPI-сервис EqSiteCMS: нормализация поисковых запросов и страниц,
пакетный find-or-create со стабильными UUID, списки с поиском/пагинацией и два
независимых черных списка нормализации страниц для каждой конюшни.
Настройки не изменяют ранее сохранённые страницы. API изменения/удаления запросов и страниц нет.

## Стек и архитектура

Python 3.14, FastAPI, SQLAlchemy Core + asyncpg, PostgreSQL с `pg_trgm`, Alembic,
Dependency Injector. Sentry опционален (`SENTRY_ENABLED`, `SENTRY_DSN`).

```text
src/
├── api/                  # HTTP, tenant preflight, транзакционные маршруты
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
| POST | `/api/pages/resolve` | ITN Write (исключение) | парсеры | `200`; tenant `401`; body `400` | то же |
| GET | `/api/search-queries` | ITN Read (исключение) | backend | `200`; tenant `401`; query `400` | то же |
| GET | `/api/pages` | ITN Read (исключение) | backend | `200`; tenant `401`; query `400` | то же |
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
traceback и параметры подключения не возвращаются. Одна транзакция на запрос:
commit до успешного ответа, rollback при ошибке. `204` не содержит тела.
Health статический `{"status":"ok"}`, без обращения к БД.

## Контракты

- Resolve: `{"items":["строка", ...]}`, 1..1000 строк без ограничения длины отдельной
  строки. `200 {"items":[{"source":"...","normalized":"...","id":"UUID"}]}`
  сохраняет порядок и дубли входа; одинаковые нормализованные значения получают один UUID
  в конюшне. Ошибка любого элемента отклоняет весь batch (`400`).
- Списки: `{items: [...], total: int}`; элементы запросов — `id/query/created_at`,
  страниц — `id/page/created_at`. Внутренние hash и equestrian_id не выдаются.
  `search` — trim и регистронезависимое вхождение, `%`, `_`, `\` буквальны;
  `ordering=query|-query` либо `page|-page`, по умолчанию `created_at DESC, id DESC`.
  `limit=20` приводится к 1..100, `offset=0` — к ≥0; нечисловые значения и неизвестная
  сортировка → `400`. `total` учитывает поиск, но не пагинацию.
- Черные списки: body `{"value":"..."}`, список без пагинации по `value ASC`.
  Trim, у хеша удаляется один ведущий `#`. Значение 1..255 символов, без whitespace,
  управляющих символов и `#`; у query-параметра также запрещены `&`, `=`, `?`.
  Дубль внутри tenant и списка → `409`, malformed item_id → `400`.

## Запуск

Из корня монорепозитория, после настройки локальных env и общей БД/сети:

```bash
make seo-build
make seo
```

Монорепозиторий использует `.docker-compose/docker-compose.seo.yml` с сервисами
`seo-service` и `seo-migration`. Миграции запускайте через `seo-migration` с той же
compose-конфигурацией/env, что и приложение; `SKIP_MIGRATIONS` не должен быть `true`.
`scripts/stackctl` пока не поддерживает seo: используйте штатные make/docker compose.
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
uv sync --locked  # dev-группа включает httpx для ASGI API-тестов
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
удаляет только свою схему в finally. Используйте выделенную тестовую БД/инстанс,
никогда рабочие данные. IT проверяют upgrade/downgrade/upgrade, constraints/hash/TEXT,
идемпотентность, конкурентные batch, поиск/сортировку/tenant isolation и blacklists.
Live smoke — отдельный этап, не часть этого тестового прогона.
