#!/bin/bash
# Celery beat entrypoint

set -e

echo "Starting Celery beat for seo-service..."

# Ждём готовности Redis
echo "Waiting for Redis..."
timeout 30 bash -c 'until printf "" 2>>/dev/null >>/dev/tcp/redis/6379; do sleep 1; done'
echo "Redis is ready"

# Удаляем старый pid файл если он есть
rm -f /tmp/celerybeat.pid

# Запускаем Celery beat
exec celery -A src.celery_app beat \
    --loglevel=info \
    --pidfile=/tmp/celerybeat.pid
