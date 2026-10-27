from core.exceptions.base import AppError, ClientError


class ParseQueueUnavailableError(AppError):
    status_code = 503
    message = "Очередь парсинга временно недоступна"


class ParseProviderUnknownError(ClientError):
    message = "Провайдер парсинга неизвестен или отключён"


class ParseTargetMissingError(ClientError):
    message = "Интеграция для парсинга не настроена"


class ParseTargetsUnavailableError(AppError):
    status_code = 502
    message = "Источник целей парсинга временно недоступен"
