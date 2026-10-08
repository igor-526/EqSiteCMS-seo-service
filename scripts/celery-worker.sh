#!/bin/bash
# Celery worker entrypoint

set -e

echo "Starting Celery worker for seo-service..."

# Ждём готовности Redis
echo "Waiting for Redis..."
timeout 30 bash -c 'until printf "" 2>>/dev/null >>/dev/tcp/redis/6379; do sleep 1; done'
echo "Redis is ready"

# Запускаем Celery worker
exec celery -A src.celery_app worker \
    --loglevel=info \
    --concurrency=2 \
    --max-tasks-per-child=100 \
    --time-limit=300 \
    --soft-time-limit=240
