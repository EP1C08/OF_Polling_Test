"""SQLAlchemy models for timewaster detection tables.

Stores RPM (Revenue Per Message) analysis per fan per model,
and cross-model timewaster flags for action.
"""

from sqlalchemy import Text, Boolean, Index, Integer, DECIMAL, TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column
from datetime import datetime
from typing import Optional
from models.db_models import Base


class FanTimewasterAnalysis(Base):
    """Per-fan per-model RPM analysis.

    Stores calculated metrics for each fan-model pair:
    - Message counts (total, from fan, from creator)
    - Spend amounts (message purchases, bundle purchases)
    - RPM (Revenue Per Message)
    """

    __tablename__ = 'fan_timewaster_analysis'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    fan_id: Mapped[str] = mapped_column(Text, nullable=False)
    model_id: Mapped[str] = mapped_column(Text, nullable=False)
    model_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    total_messages: Mapped[int] = mapped_column(Integer, default=0)
    messages_from_fan: Mapped[int] = mapped_column(Integer, default=0)
    messages_from_creator: Mapped[int] = mapped_column(Integer, default=0)

    total_spend: Mapped[float] = mapped_column(DECIMAL(10, 2), default=0.00)
    bundle_spend: Mapped[float] = mapped_column(DECIMAL(10, 2), default=0.00)
    message_spend: Mapped[float] = mapped_column(DECIMAL(10, 2), default=0.00)

    rpm: Mapped[float] = mapped_column(DECIMAL(10, 4), default=0.0000)

    first_message_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    last_message_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    analyzed_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), default=datetime.utcnow
    )

    __table_args__ = (
        Index('idx_fan_timewaster_rpm', 'rpm'),
        Index('idx_fan_timewaster_spend', 'total_spend'),
        Index('idx_fan_timewaster_fan', 'fan_id'),
        Index('idx_fan_timewaster_model', 'model_id'),
        Index('idx_fan_timewaster_unique', 'fan_id', 'model_id', unique=True),
    )


class FanTimewasterFlag(Base):
    """Cross-model timewaster flags.

    Aggregates fan behavior across ALL models and flags for action:
    - 'auto_restrict': $0 RPM across all models, recommend restriction
    - 'review': Varying RPM across models, needs manual review
    - 'cleared': Reviewed and cleared (not a timewaster)
    """

    __tablename__ = 'fan_timewaster_flags'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    fan_id: Mapped[str] = mapped_column(Text, unique=True, nullable=False)

    total_models: Mapped[int] = mapped_column(Integer, default=0)
    total_messages_all_models: Mapped[int] = mapped_column(Integer, default=0)
    total_spend_all_models: Mapped[float] = mapped_column(DECIMAL(10, 2), default=0.00)
    avg_rpm_all_models: Mapped[float] = mapped_column(DECIMAL(10, 4), default=0.0000)
    min_rpm: Mapped[float] = mapped_column(DECIMAL(10, 4), default=0.0000)
    max_rpm: Mapped[float] = mapped_column(DECIMAL(10, 4), default=0.0000)

    flag_type: Mapped[str] = mapped_column(Text, nullable=False)
    flag_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    restriction_applied: Mapped[bool] = mapped_column(Boolean, default=False)
    restricted_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    reviewed_by: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), default=datetime.utcnow
    )

    __table_args__ = (
        Index('idx_timewaster_flags_type', 'flag_type'),
        Index('idx_timewaster_flags_fan', 'fan_id'),
    )
