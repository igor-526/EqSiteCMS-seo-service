# SEO Service

Центральный оркестратор SEO-анализа для EqSiteCMS. Управляет расписанием парсингов, координирует парсеры через NATS JetStream и хранит общие SEO-сущности.

## Назначение

`seo-service` выполняет роль координатора в микросервисной SEO-инфраструктуре:

- **Хранение общих сущностей**: search queries, site paths, parsing schedules
- **Планирование парсинга**: Celery периодически проверяет расписание и ставит задачи парсерам
- **Координация через NATS**: асинхронная постановка задач парсерам (yandex-metrics, google-analytics и др.)
- **API для backend**: предоставляет данные основному backend сервису

**Важно**: Результаты парсинга остаются в сервисах-парсерах. `seo-service` не собирает их обратно, а только координирует процесс.

## Стек

- Python 3.14.6
- FastAPI + SQLAlchemy Core + asyncpg
- PostgreSQL 17 (выделенная БД `seo_service`)
- NATS JetStream (stream `SEO_TASKS`)
- Celery + Redis (планирование задач)
- Alembic (миграции)
- Sentry (опционально)

## Архитектура

```text
src/
├── api/             # HTTP-контракты (REST endpoints)
├── core/            # сущности, схемы, протоколы и бизнес-логика
├── depends/         # сборка зависимостей FastAPI
├── models/          # SQLAlchemy Core tables (parsing_schedules, search_queries, site_paths)
├── repositories/    # реализации repository protocols
├── migration/       # Alembic миграции
├── utils/           # База данных и инфраструктурные утилиты
├── main.py
└── settings.py
```

## Запуск в Docker

```bash
# Из корня монорепозитория
make seo-up
```

Или напрямую через docker-compose:

```bash
docker compose -f .docker-compose/docker-compose.seo.yml up --build
```

Сервис доступен на `http://localhost:8005`. Swagger: `http://localhost:8005/docs`.

## Локальная разработка

```bash
cd services/seo-service
cp .env.example .env
uv sync
docker compose -f ../../.docker-compose/docker-compose.seo.yml up -d db-seo redis nats
uv run alembic -c src/alembic.ini upgrade head
uv run uvicorn main:app --app-dir src --reload --port 8005
```

```bash
make format
make lint
make test
```

## NATS Integration

`seo-service` публикует задачи парсинга в NATS JetStream:

- **Stream**: `SEO_TASKS` (WorkQueue retention, max age 24h)
- **Subjects**:
  - `seo.tasks.yandex_metrics` — задачи для Яндекс.Метрики
  - `seo.tasks.google_analytics` — задачи для Google Analytics (будущее расширение)

Формат сообщения (JSON):
```json
{
  "task_id": "uuid",
  "parser_type": "yandex_metrics",
  "site_id": 123,
  "params": {...}
}
```

Подробнее: `docs/seo/protocols.md` (будет создан в следующих задачах).

## Celery Tasks

Периодические задачи для планирования парсинга:

- `schedule_parsing` — проверяет `parsing_schedules` в БД и ставит задачи парсерам в NATS

Celery использует общий Redis backend (`redis://redis:6379/0`).

## API

| Метод | Путь | Назначение | Access |
|---|---|---|---|
| GET | `/health` | Healthcheck | Public |
| GET | `/api/schedules` | Список расписаний парсинга | Public Read (для backend) |
| POST | `/api/schedules` | Создать расписание | Protected Write (для CMS) |

На данном этапе реализован только `/health`. Остальные endpoints будут добавлены в следующих execution units.

## Environment Variables

См. `.env.example`. Основные переменные:

- `POSTGRES_*` — подключение к БД `seo_service`
- `NATS_URL` — NATS connection string
- `REDIS_URL` — Redis для Celery
- `CELERY_BROKER_URL`, `CELERY_RESULT_BACKEND` — Celery конфигурация

## Git Repository

Сервис живёт в монорепозитории `services/seo-service/`, но также синхронизируется в отдельный Git-репозиторий для независимого версионирования:

- Remote: `git@github.com:igor-526/EqSiteCMS-seo-service.git`
- Ветки: `main` и `release`

## Documentation

- `docs/seo/architecture.md` — общая архитектура SEO-модуля
- `docs/seo/services.md` — описание seo-service и парсеров
- `docs/seo/protocols.md` — контракты NATS и Celery

Агенты Backend и Quality Gate должны читать эту документацию при работе с SEO-инфраструктурой.
