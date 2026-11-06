"""Strict producer wire models. No URLs, credentials or extension fields."""

import re
from datetime import datetime, timedelta
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_validator, model_validator

BigPositive = Annotated[int, Field(strict=True, gt=0, le=2**63 - 1)]
BigNonNegative = Annotated[int, Field(strict=True, ge=0, le=2**63 - 1)]


class Envelope(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal[1]
    message_id: UUID
    operation_id: UUID
    equestrian_id: UUID
    occurred_at: datetime
    normalization_revision: BigPositive

    @field_validator("schema_version", mode="before")
    @classmethod
    def exact_version(cls, value: object) -> object:
        if type(value) is not int or value != 1:
            raise ValueError("unsupported schema version")
        return value

    @field_validator("occurred_at", mode="before")
    @classmethod
    def rfc3339_string(cls, value: object) -> object:
        if not isinstance(value, str):
            raise ValueError("RFC3339 string required")
        pattern = r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?(?:Z|[+-][0-9]{2}:[0-9]{2})"
        if re.fullmatch(pattern, value) is None:
            raise ValueError("RFC3339 string required")
        return value

    @field_validator("occurred_at")
    @classmethod
    def utc_required(cls, value: datetime) -> datetime:
        if value.utcoffset() != timedelta(0):
            raise ValueError("UTC timestamp required")
        return value


class Mapping(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source_page_id: UUID
    target_page_id: UUID

    @model_validator(mode="after")
    def distinct(self) -> Mapping:
        if self.source_page_id == self.target_page_id:
            raise ValueError("source equals target")
        return self


class MergeMappings(Envelope):
    kind: Literal["mappings"]
    part_index: BigNonNegative
    mappings: Annotated[list[Mapping], Field(min_length=1, max_length=500)]

    @model_validator(mode="after")
    def unique_sources(self) -> MergeMappings:
        if len({item.source_page_id for item in self.mappings}) != len(self.mappings):
            raise ValueError("duplicate source")
        return self


class MergeSeal(Envelope):
    kind: Literal["seal"]
    part_count: BigNonNegative


class MergeFinalized(Envelope):
    result_message_id: UUID


merge_command: TypeAdapter[MergeMappings | MergeSeal] = TypeAdapter(
    Annotated[MergeMappings | MergeSeal, Field(discriminator="kind")]
)


class CleanupCompleted(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal[1]
    message_id: UUID
    operation_id: UUID
    equestrian_id: UUID
    occurred_at: datetime
    cleanup_seq: BigPositive
    status: Literal["completed"]

    @field_validator("schema_version", mode="before")
    @classmethod
    def exact_version(cls, value: object) -> object:
        return Envelope.exact_version(value)

    @field_validator("occurred_at", mode="before")
    @classmethod
    def rfc3339_string(cls, value: object) -> object:
        return Envelope.rfc3339_string(value)

    @field_validator("occurred_at")
    @classmethod
    def utc_required(cls, value: datetime) -> datetime:
        return Envelope.utc_required(value)


class MergeCompleted(Envelope):
    status: Literal["completed", "superseded_by_delete"]
    part_count: BigNonNegative | None
    cleanup_operation_id: UUID | None = None
    cleanup_seq: BigPositive | None = None

    @model_validator(mode="after")
    def correlation(self) -> MergeCompleted:
        if self.status == "completed":
            if self.part_count is None or self.cleanup_operation_id is not None or self.cleanup_seq is not None:
                raise ValueError("invalid completed correlation")
        elif self.cleanup_operation_id is None or self.cleanup_seq is None:
            raise ValueError("missing cleanup correlation")
        return self
