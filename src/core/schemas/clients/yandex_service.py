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
    counter: ConnectedMetricaBinding | None
