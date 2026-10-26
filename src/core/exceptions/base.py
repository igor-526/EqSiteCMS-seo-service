class AppError(Exception):
    """Только безопасное сообщение; значения входа сюда не передаются."""

    status_code = 500
    message = "Внутренняя ошибка сервиса"

    def __init__(self, message: str | None = None) -> None:
        self.message = self.message if message is None else message
        super().__init__(self.message)


class ClientError(AppError):
    status_code = 400
    message = "Некорректные параметры запроса"


class UnauthorizedError(AppError):
    status_code = 401
    message = "Не указан корректный идентификатор конюшни"


class NotFoundError(AppError):
    status_code = 404
    message = "Элемент не найден"


class AlreadyExistsError(ClientError):
    status_code = 409
    message = "Элемент уже существует"


class RepositoryError(AppError):
    pass


class InvalidValueError(ClientError):
    """Нормализатор не знает индекс; batch-сервис добавляет его через at_index."""

    message = "Некорректное значение"

    def at_index(self, index: int) -> ClientError:
        return ClientError(f"Некорректное значение элемента items[{index}]")
