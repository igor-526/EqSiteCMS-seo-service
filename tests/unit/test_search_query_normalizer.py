import unicodedata

import pytest

from core.exceptions import (
    AlreadyExistsError,
    AppError,
    ClientError,
    InvalidValueError,
    NotFoundError,
    RepositoryError,
    UnauthorizedError,
)
from core.services.search_query_normalizer import SearchQueryNormalizer


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("  КУПИТЬ   Лошадь в МОСКВЕ ", "Купить лошадь в москве"),
        ("2-КОМНАТНЫЙ денник", "2-комнатный денник"),
        ("а\t\n\u2003Б", "А б"),
        (unicodedata.normalize("NFD", "ЙОГА"), "Йога"),
        ("а" * 10_000, "А" + "а" * 9_999),
        ("ßTEST", "SStest"),
    ],
)
def test_normalize(raw: str, expected: str) -> None:
    assert SearchQueryNormalizer().normalize(raw) == expected


@pytest.mark.parametrize("raw", ["", " ", "\t\n\u2003"])
def test_invalid_value_has_safe_indexed_error(raw: str) -> None:
    with pytest.raises(InvalidValueError) as error:
        SearchQueryNormalizer().normalize(raw)
    assert error.value.message == "Некорректное значение"
    indexed = error.value.at_index(1)
    assert isinstance(indexed, ClientError)
    assert indexed.status_code == 400
    assert indexed.message == "Некорректное значение элемента items[1]"


@pytest.mark.parametrize(
    ("error", "status"),
    [
        (AppError, 500),
        (ClientError, 400),
        (UnauthorizedError, 401),
        (NotFoundError, 404),
        (AlreadyExistsError, 409),
        (RepositoryError, 500),
    ],
)
def test_safe_error_defaults(error: type[AppError], status: int) -> None:
    instance = error()
    assert instance.status_code == status
    assert str(instance) == instance.message
    assert instance.message
