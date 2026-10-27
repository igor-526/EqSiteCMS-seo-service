from core.exceptions import ClientError
from core.schemas.api.metrics_table import MetricOutDto


class MetricRegistry:
    _metrics = {
        "visits": ("Визиты", "integer"),
        "users": ("Посетители", "integer"),
        "pageviews": ("Просмотры", "integer"),
        "bounces": ("Отказы", "integer"),
        "bounce_rate": ("Доля отказов", "percent"),
        "page_depth": ("Глубина просмотра", "decimal"),
        "avg_visit_duration_seconds": ("Время на сайте", "duration"),
        "visit_duration_seconds": ("Суммарное время визитов", "duration"),
    }

    def get(self, key: str = "visits") -> MetricOutDto:
        if key not in self._metrics:
            raise ClientError("Некорректный показатель")
        title, value_type = self._metrics[key]
        return MetricOutDto(key=key, title=title, value_type=value_type)
