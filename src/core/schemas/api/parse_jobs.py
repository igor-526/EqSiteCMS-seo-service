from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class ParseJobInDto(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider: Annotated[str, Field(strict=True, pattern=r"^[a-z][a-z0-9_]{1,31}$")]


class ParseJobOutDto(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    job_id: UUID
    provider: str
    status: Literal["accepted"]
