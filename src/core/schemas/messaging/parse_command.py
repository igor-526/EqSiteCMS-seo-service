from datetime import date
from typing import Literal, Self
from uuid import NAMESPACE_DNS, UUID, uuid5

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, model_validator

NAMESPACE_PARSE_COMMANDS = uuid5(NAMESPACE_DNS, "parse-commands.eqcms")


def schedule_job_id(provider: str, equestrian_id: UUID, schedule_date: date) -> UUID:
    return uuid5(NAMESPACE_PARSE_COMMANDS, f"schedule:{provider}:{equestrian_id}:{schedule_date.isoformat()}")


def msg_id(provider: str, job_id: UUID) -> UUID:
    return uuid5(NAMESPACE_PARSE_COMMANDS, f"{provider}:{job_id}")


class ParseWindow(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    mode: Literal["auto", "explicit"] = "auto"
    date_from: date | None = None
    date_to: date | None = None

    @model_validator(mode="after")
    def validate_dates(self) -> Self:
        if self.mode == "auto":
            if self.model_fields_set & {"date_from", "date_to"}:
                raise ValueError("auto window must not contain dates")
        elif self.date_from is None or self.date_to is None or self.date_from > self.date_to:
            raise ValueError("explicit window requires an ordered date range")
        return self


class ParseCommand(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = 1
    occurred_at: AwareDatetime
    job_id: UUID
    provider: str = Field(pattern=r"^[a-z][a-z0-9_]{1,31}$")
    equestrian_id: UUID
    trigger: Literal["schedule", "manual"]
    window: ParseWindow = Field(default_factory=ParseWindow)
    params: dict[str, JsonValue] = Field(default_factory=dict)
