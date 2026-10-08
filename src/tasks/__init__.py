"""Celery tasks for seo-service."""

from tasks.schedule_parsing import schedule_parsing

__all__ = ["schedule_parsing"]
