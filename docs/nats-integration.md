# NATS JetStream Integration

## Обзор

`seo-service` использует NATS JetStream для асинхронной постановки задач парсерам. Результаты парсинга остаются в сервисах-парсерах и не возвращаются через NATS.

## Архитектура

```
seo-service (Publisher)
    ↓
NATS JetStream Stream: SEO_TASKS
    ↓
Subject: seo.tasks.<parser_type>
    ↓
Parsers (Consumers)
```

## Streams и Subjects

| Stream | Subject | Назначение | Роль |
|--------|---------|------------|------|
| SEO_TASKS | seo.tasks.yandex_metrics | Постановка задач парсинга Яндекс.Метрики | исходящий |
| SEO_TASKS | seo.tasks.<parser_type> | Постановка задач другим парсерам | исходящий |

## Компоненты

### NatsJetstreamClient (`infrastructure/nats/client.py`)

Низкоуровневый wrapper для NATS JetStream:
- Управление connection lifecycle (connect/close)
- Graceful shutdown при SIGTERM
- Error policy для подавления шума reconnect'ов

### TaskPublisher (`infrastructure/nats/publisher.py`)

Publisher для постановки задач парсинга:
- `publish_task(parser_type, task_id, site_id, params, trace_id)` — публикация задачи
- Автоматическая генерация trace_id для корреляции
- Structured logging с event, trace_id, timestamp
- Дедупликация через `Nats-Msg-Id`

### NatsConnectionErrorPolicy (`infrastructure/nats/lifecycle.py`)

Политика логирования ошибок соединения:
- Подавляет шум транзиентных reconnect'ов
- Эскалирует затяжную недоступность брокера ровно один раз
- Сбрасывает состояние при успешном reconnect

## Message Format

### Payload (JSON)
```json
{
  "task_id": "uuid4-string",
  "site_id": 123,
  "params": {
    "custom_key": "custom_value"
  }
}
```

### Headers
```
trace_id: uuid4-string
Nats-Msg-Id: task_id (для идемпотентности)
```

## Использование

### В FastAPI endpoint

```python
from depends.nats import get_publisher
from fastapi import Depends

@app.post("/schedule-parsing")
async def schedule_parsing(
    publisher: TaskPublisher = Depends(get_publisher)
):
    task_id = str(uuid.uuid4())
    message_id, duplicate = await publisher.publish_task(
        parser_type="yandex_metrics",
        task_id=task_id,
        site_id=1,
        params={"metric": "pageviews"},
    )
    return {"task_id": task_id, "duplicate": duplicate}
```

## Graceful Shutdown

NATS client корректно закрывается в `lifespan` через `client.close()`:
1. Завершает обработку текущих операций
2. Вызывает `drain()` для graceful завершения pending messages
3. Обрабатывает `ConnectionReconnectingError` при остановке во время reconnect

## Structured Logging

Все NATS события логируются с:
- `event`: тип события (nats_publish, nats_publish_success, nats_publish_error, nats_publish_duplicate)
- `trace_id`: корреляция между сообщениями
- `subject`: NATS subject
- `task_id`: идентификатор задачи
- `timestamp`: UTC timestamp

## Дедупликация

JetStream обеспечивает at-least-once delivery через:
- `Nats-Msg-Id` header с уникальным идентификатором (используем `task_id`)
- Duplicate window 120 секунд (default)
- Publisher проверяет `PubAck.duplicate` и логирует warning при обнаружении дубликата

## Settings

Настройки NATS в `.env`:
```bash
NATS_SERVERS=nats://nats:4222
NATS_STREAM_SEO_TASKS=SEO_TASKS
NATS_ERROR_REPORT_AFTER_ATTEMPTS=3
```

## Тестирование

### Unit tests (без инфраструктуры)
```bash
pytest tests/infrastructure/test_nats_settings.py
```

### Integration tests (требуют NATS)
```bash
pytest tests/infrastructure/test_nats_client.py -m infrastructure
```

## Real-broker Acceptance Gate

Canonical AsyncAPI и runtime config должны совпадать по stream, subject, durable, filter и payload/header contract. Blocking integration gate использует реальный NATS JetStream без mocked broker.

## Протоколы

Детальные протоколы работы с NATS JetStream описаны в:
- `agents/howto/nats-jetstream-protocols.md` — обязательный reference для всех агентов
