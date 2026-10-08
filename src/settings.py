from functools import cached_property

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class NatsSettings(BaseSettings):
    """NATS JetStream settings for seo-service."""

    # Базовые настройки
    nats_servers_raw: str = Field(
        default="nats://localhost:4222",
        alias="NATS_SERVERS",
    )

    @property
    def nats_servers(self) -> list[str]:
        if self.nats_servers_raw.strip():
            return [server.strip() for server in self.nats_servers_raw.split(",") if server.strip()]
        return ["nats://localhost:4222"]

    # Streams
    nats_stream_seo_tasks: str = Field(
        default="SEO_TASKS",
        alias="NATS_STREAM_SEO_TASKS",
    )

    # Connection error policy
    nats_error_report_after_attempts: int = Field(
        default=3,
        alias="NATS_ERROR_REPORT_AFTER_ATTEMPTS",
        ge=1,
    )

    model_config = SettingsConfigDict(populate_by_name=True)


class Settings(BaseSettings):
    environment: str = Field(default="development", alias="ENVIRONMENT")
    debug: bool = Field(default=True, alias="DEBUG")
    app_title: str = Field(default="FastAPI Template", alias="APP_TITLE")

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

    # Celery / Redis settings
    celery_broker_url: str = Field(default="redis://redis:6379/5", alias="CELERY_BROKER_URL")
    celery_result_backend: str = Field(default="redis://redis:6379/6", alias="CELERY_RESULT_BACKEND")

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

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
nats_settings = NatsSettings()
