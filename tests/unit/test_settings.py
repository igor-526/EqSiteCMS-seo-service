import pytest
from pydantic import ValidationError

from settings import NatsSettings, ParseSettings, Settings


@pytest.fixture(autouse=True)
def disable_env_file(monkeypatch: pytest.MonkeyPatch) -> None:
    for cls in (Settings, NatsSettings, ParseSettings):
        monkeypatch.setitem(cls.model_config, "env_file", None)


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


# UT-PS-01: unknown provider membership is checked by DI after registry construction.
def test_parse_defaults_ut_ps_01() -> None:
    settings = ParseSettings()
    assert settings.parse_schedule_enabled is True
    assert settings.parse_schedule_time == "03:00"
    assert settings.schedule_time.hour == 3
    assert settings.parse_scheduler_tick_seconds == 30
    assert settings.parse_providers_enabled == ["ya_metrica"]
    assert settings.parse_targets_page_size == 100
    settings.validate_registered_providers({"ya_metrica"})


@pytest.mark.parametrize("value", ["25:00", "3:7", "", "03:00:00", " 03:00", "12:60"])
def test_parse_invalid_time_ut_ps_01(value: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PARSE_SCHEDULE_TIME", value)
    with pytest.raises(ValidationError):
        ParseSettings()


def test_parse_disabled_and_registry_validation_ut_ps_01(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PARSE_SCHEDULE_ENABLED", "false")
    monkeypatch.setenv("PARSE_PROVIDERS_ENABLED", "ya_metrica, demo")
    settings = ParseSettings()
    assert settings.parse_schedule_enabled is False
    with pytest.raises(ValueError, match="unregistered provider"):
        settings.validate_registered_providers({"ya_metrica"})
    settings.validate_registered_providers({"ya_metrica", "demo"})


@pytest.mark.parametrize(
    "key,value",
    [
        ("PARSE_PROVIDERS_ENABLED", ""),
        ("PARSE_PROVIDERS_ENABLED", "ya_metrica,ya_metrica"),
        ("PARSE_PROVIDERS_ENABLED", "Bad-Code"),
        ("PARSE_SCHEDULER_TICK_SECONDS", "0"),
        ("PARSE_SCHEDULER_TICK_SECONDS", "nan"),
        ("PARSE_TARGETS_PAGE_SIZE", "501"),
        ("PARSE_DISPATCH_MAX_ATTEMPTS", "0"),
        ("YANDEX_SERVICE_TIMEOUT_SECONDS", "inf"),
        ("PARSE_SCHEDULE_ENABLED", "maybe"),
    ],
)
def test_parse_invalid_settings_ut_ps_01(key: str, value: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(key, value)
    with pytest.raises(ValidationError):
        ParseSettings()


def test_nats_defaults_and_csv_ut_ps_01(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = NatsSettings()
    assert settings.nats_servers == ["nats://eqsitecms-nats:4222"]
    assert settings.nats_stream_parse_commands == "PARSE_COMMANDS"
    assert settings.nats_subjects_parse_commands == ["commands.parse.>"]
    assert settings.nats_parse_commands_max_age_seconds == 7 * 86400
    monkeypatch.setenv("NATS_SERVERS", "nats://one:4222, nats://two:4222")
    assert len(NatsSettings().nats_servers) == 2


@pytest.mark.parametrize(
    "key,value",
    [
        ("NATS_SERVERS", ""),
        ("NATS_SERVERS", " , "),
        ("NATS_SUBJECTS_PARSE_COMMANDS", ""),
        ("NATS_PARSE_COMMANDS_MAX_AGE_SECONDS", "0"),
        ("NATS_PUBLISH_TIMEOUT_SECONDS", "nan"),
        ("NATS_SETUP_MAX_ATTEMPTS", "0"),
        ("NATS_SETUP_BACKOFF_SECONDS", "-1"),
    ],
)
def test_nats_invalid_settings_ut_ps_01(key: str, value: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(key, value)
    with pytest.raises(ValidationError):
        NatsSettings()
