"""ParsingSchedule model for SEO service."""

from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, Integer, String, UniqueConstraint
from sqlalchemy.orm import declarative_base

from utils.basemodel import metadata

Base = declarative_base(metadata=metadata)


class ParsingSchedule(Base):
    """Model for storing parsing schedules.

    Attributes:
        id: Unique identifier for the schedule
        site_id: ID of the site to parse
        parser_type: Type of parser (e.g., "yandex_metrics")
        cron_expression: Cron expression for scheduling (e.g., "0 0 * * *")
        is_active: Whether the schedule is currently active
        created_at: Timestamp when the schedule was created
        updated_at: Timestamp when the schedule was last updated
    """

    __tablename__ = "parsing_schedules"

    id = Column(Integer, primary_key=True, autoincrement=True)
    site_id = Column(Integer, nullable=False, index=True)
    parser_type = Column(String(100), nullable=False)
    cron_expression = Column(String(100), nullable=False)
    is_active = Column(Boolean, nullable=False, default=True, index=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (UniqueConstraint("site_id", "parser_type", name="uq_site_parser"),)

    def __repr__(self) -> str:
        return (
            f"<ParsingSchedule(id={self.id}, site_id={self.site_id}, "
            f"parser_type={self.parser_type}, is_active={self.is_active})>"
        )
