import re
from collections.abc import Collection
from datetime import time
from functools import cached_property

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    environment: str = Field(default="development", alias="ENVIRONMENT")
    debug: bool = Field(default=True, alias="DEBUG")
    app_title: str = Field(default="FastAPI Template", alias="APP_TITLE")

    seo_utc_offset_hours: int = Field(default=3, alias="SEO_UTC_OFFSET_HOURS", ge=-12, le=14)

    ya_metrica_service_url: str = Field(
        default="http://eqsitecms-ya-metrica-service:8000", alias="YA_METRICA_SERVICE_URL"
    )
    ya_metrica_timeout_seconds: float = Field(
        default=4.0, alias="YA_METRICA_TIMEOUT_SECONDS", gt=0, allow_inf_nan=False
    )

    sentry_enabled: bool = Field(default=False, alias="SENTRY_ENABLED")
    sentry_dsn: str = Field(default="", alias="SENTRY_DSN")
    sentry_environment: str = Field(default="development", alias="SENTRY_ENVIRONMENT")
    sentry_traces_sample_rate: float = Field(default=0.0, alias="SENTRY_TRACES_SAMPLE_RATE", ge=0.0, le=1.0)
    sentry_release: str | None = Field(default=None, alias="SENTRY_RELEASE")

    postgres_user: str = Field(default="app", alias="POSTGRES_USER")
    postgres_password: str = Field(default="app", alias="POSTGRES_PASSWORD")
    postgres_host: str = Field(default="localhost", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_db: str = Field(default="app", alias="POSTGRES_DB")

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    @field_validator("seo_utc_offset_hours", mode="before")
    @classmethod
    def validate_utc_offset(cls, value: object) -> int:
        if type(value) is int:
            return value
        if isinstance(value, str) and value.strip().lstrip("+-").isascii():
            try:
                return int(value)
            except ValueError:
                pass
        raise ValueError("SEO_UTC_OFFSET_HOURS must be an integer")

    @model_validator(mode="after")
    def validate_production_secrets(self) -> Settings:
        if self.sentry_enabled and not self.sentry_dsn:
            raise ValueError("SENTRY_DSN is required when SENTRY_ENABLED=true")
        return self

    @cached_property
    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


settings = Settings()


class NatsSettings(BaseSettings):
    nats_servers_raw: str = Field(default="nats://eqsitecms-nats:4222", alias="NATS_SERVERS", min_length=1)
    nats_stream_parse_commands: str = Field(default="PARSE_COMMANDS", alias="NATS_STREAM_PARSE_COMMANDS", min_length=1)
    nats_subjects_parse_commands_raw: str = Field(
        default="commands.parse.>", alias="NATS_SUBJECTS_PARSE_COMMANDS", min_length=1
    )
    nats_parse_commands_max_age_seconds: int = Field(default=604800, alias="NATS_PARSE_COMMANDS_MAX_AGE_SECONDS", ge=1)
    nats_error_report_after_attempts: int = Field(default=3, alias="NATS_ERROR_REPORT_AFTER_ATTEMPTS", ge=1)
    nats_publish_timeout_seconds: float = Field(
        default=5.0, alias="NATS_PUBLISH_TIMEOUT_SECONDS", gt=0, allow_inf_nan=False
    )
    nats_setup_max_attempts: int = Field(default=10, alias="NATS_SETUP_MAX_ATTEMPTS", ge=1)
    nats_setup_backoff_seconds: float = Field(
        default=2.0, alias="NATS_SETUP_BACKOFF_SECONDS", ge=0, allow_inf_nan=False
    )
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore", populate_by_name=True
    )

    @field_validator("nats_servers_raw", "nats_subjects_parse_commands_raw")
    @classmethod
    def validate_csv(cls, value: str) -> str:
        if any(not item.strip() for item in value.split(",")):
            raise ValueError("NATS list must contain non-empty entries")
        return value

    @property
    def nats_servers(self) -> list[str]:
        return [item.strip() for item in self.nats_servers_raw.split(",")]

    @property
    def nats_subjects_parse_commands(self) -> list[str]:
        return [item.strip() for item in self.nats_subjects_parse_commands_raw.split(",")]


class ParseSettings(BaseSettings):
    parse_schedule_enabled: bool = Field(default=True, alias="PARSE_SCHEDULE_ENABLED")
    parse_schedule_time: str = Field(default="03:00", alias="PARSE_SCHEDULE_TIME")
    parse_scheduler_tick_seconds: float = Field(
        default=30, alias="PARSE_SCHEDULER_TICK_SECONDS", gt=0, allow_inf_nan=False
    )
    parse_providers_enabled_raw: str = Field(default="ya_metrica", alias="PARSE_PROVIDERS_ENABLED")
    parse_targets_page_size: int = Field(default=100, alias="PARSE_TARGETS_PAGE_SIZE", ge=1, le=500)
    parse_dispatch_max_attempts: int = Field(default=5, alias="PARSE_DISPATCH_MAX_ATTEMPTS", ge=1)
    yandex_service_url: str = Field(default="http://eqsitecms-yandex-service:8000", alias="YANDEX_SERVICE_URL")
    yandex_service_timeout_seconds: float = Field(
        default=4.0, alias="YANDEX_SERVICE_TIMEOUT_SECONDS", gt=0, allow_inf_nan=False
    )
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore", populate_by_name=True
    )

    @field_validator("parse_schedule_time")
    @classmethod
    def validate_schedule_time(cls, value: str) -> str:
        if re.fullmatch(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]", value) is None:
            raise ValueError("PARSE_SCHEDULE_TIME must be HH:MM")
        return value

    @field_validator("parse_providers_enabled_raw")
    @classmethod
    def validate_provider_codes(cls, value: str) -> str:
        codes = [code.strip() for code in value.split(",")]
        if any(re.fullmatch(r"[a-z][a-z0-9_]{1,31}", code) is None for code in codes):
            raise ValueError("PARSE_PROVIDERS_ENABLED must contain provider codes")
        if len(codes) != len(set(codes)):
            raise ValueError("PARSE_PROVIDERS_ENABLED contains duplicate codes")
        return value

    @property
    def parse_providers_enabled(self) -> list[str]:
        return [code.strip() for code in self.parse_providers_enabled_raw.split(",")]

    @property
    def schedule_time(self) -> time:
        return time.fromisoformat(self.parse_schedule_time)

    def validate_registered_providers(self, registered_codes: Collection[str]) -> None:
        """Вызывается DI после сборки реестра (SEO-3)."""
        if set(self.parse_providers_enabled) - set(registered_codes):
            raise ValueError("PARSE_PROVIDERS_ENABLED contains an unregistered provider")
