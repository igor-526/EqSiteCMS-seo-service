from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class MetricaBinding(BaseModel):
    model_config = ConfigDict(extra="forbid")
    equestrian_id: UUID
    counter_id: str = Field(strict=True, min_length=1)


class MetricaBindingList(BaseModel):
    model_config = ConfigDict(extra="forbid")
    total: int = Field(strict=True, ge=0)
    items: list[MetricaBinding]


class ConnectedMetricaBinding(MetricaBinding):
    # Existing detail endpoint includes account metadata; never retain it.
    model_config = ConfigDict(extra="ignore")


class ConnectedMetricaCounter(BaseModel):
    model_config = ConfigDict(extra="forbid")
    # Required private peer metadata, including when the binding is absent.
    # SEO only preflights identity; authoritative maintenance admission stays in M.
    binding_generation: int = Field(strict=True, ge=0)
    cleanup_requested_seq: int = Field(strict=True, ge=0)
    cleanup_completed_seq: int = Field(strict=True, ge=0)
    counter: ConnectedMetricaBinding | None
