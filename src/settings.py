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
