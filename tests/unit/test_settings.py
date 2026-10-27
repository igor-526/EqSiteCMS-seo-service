import pytest
from pydantic import ValidationError

from settings import Settings


@pytest.fixture(autouse=True)
def disable_env_file(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(Settings.model_config, "env_file", None)


def test_default_offset_ut_sp_08(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SEO_UTC_OFFSET_HOURS", raising=False)
    monkeypatch.setenv("YA_METRICA_UTC_OFFSET_HOURS", "-5")
    assert Settings().seo_utc_offset_hours == 3


@pytest.mark.parametrize("value", ["-12", "14", "0", "+3"])
def test_valid_offset_ut_sp_08(value: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEO_UTC_OFFSET_HOURS", value)
    assert Settings().seo_utc_offset_hours == int(value)


@pytest.mark.parametrize("value", ["-13", "15", "3.5", "3.0", "abc", "true", ""])
def test_invalid_offset_ut_sp_08(value: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEO_UTC_OFFSET_HOURS", value)
    with pytest.raises(ValidationError):
        Settings()


@pytest.mark.parametrize("value", [True, 3.0, 3.5])
def test_non_integer_offset_ut_sp_08(value: object) -> None:
    with pytest.raises(ValidationError):
        Settings.model_validate({"SEO_UTC_OFFSET_HOURS": value})
