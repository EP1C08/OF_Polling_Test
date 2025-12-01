"""SQLAlchemy models for OnlyFans polling database schema.

Matches the existing database schema from db_worker.py.
Tables are created by the existing db_worker table creation logic.
These models are for querying (WebSocket system and cutoff_manager).
"""

from sqlalchemy import Text, Boolean, Index, Integer, DECIMAL, TIMESTAMP, ForeignKey, CheckConstraint
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from datetime import datetime
from typing import Optional


class Base(AsyncAttrs, DeclarativeBase):
    """Base class for all SQLAlchemy models."""
    pass


class Message(Base):
    """Message table model - matches db_worker.py schema."""
    __tablename__ = 'messages_new'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    message_id: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    model_id: Mapped[str] = mapped_column(Text, nullable=False)
    fan_id: Mapped[str] = mapped_column(Text, nullable=False)
    sender_id: Mapped[str] = mapped_column(Text, nullable=False)
    model_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    sender_username: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    message_type: Mapped[str] = mapped_column(Text, default='text')
    media_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    media_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    price: Mapped[float] = mapped_column(DECIMAL(10, 2), default=0.00)
    is_free: Mapped[bool] = mapped_column(Boolean, default=True)
    is_purchased: Mapped[bool] = mapped_column(Boolean, default=False)
    is_from_me: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=datetime.now)

    __table_args__ = (
        Index('idx_messages_message_id', 'message_id'),
        Index('idx_messages_model_id', 'model_id'),
        Index('idx_messages_fan_id', 'fan_id'),
        Index('idx_messages_created_at', 'created_at'),
        Index('idx_messages_is_purchased', 'is_purchased'),
        Index('idx_messages_model_fan', 'model_id', 'fan_id', 'created_at'),
    )


class Bundle(Base):
    """Bundle table model - matches db_worker.py schema."""
    __tablename__ = 'bundles_new'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    bundle_id: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    message_id: Mapped[str] = mapped_column(Text, nullable=False)
    creator_id: Mapped[str] = mapped_column(Text, nullable=False)
    creator_username: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    total_price: Mapped[float] = mapped_column(DECIMAL(10, 2), nullable=False)
    media_count: Mapped[int] = mapped_column(Integer, nullable=False)
    photo_count: Mapped[int] = mapped_column(Integer, default=0)
    video_count: Mapped[int] = mapped_column(Integer, default=0)
    audio_count: Mapped[int] = mapped_column(Integer, default=0)
    is_mass_message: Mapped[bool] = mapped_column(Boolean, default=False)
    queue_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=datetime.now)

    __table_args__ = (
        Index('idx_bundles_creator', 'creator_id'),
        Index('idx_bundles_created', 'created_at'),
    )


class BundleItem(Base):
    """Bundle items table model - matches db_worker.py schema."""
    __tablename__ = 'bundle_items_new'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    bundle_id: Mapped[str] = mapped_column(Text, ForeignKey('bundles_new.bundle_id', ondelete='CASCADE'), nullable=False)
    media_id: Mapped[str] = mapped_column(Text, nullable=False)
    media_type: Mapped[str] = mapped_column(Text, nullable=False)
    duration: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=datetime.now)

    __table_args__ = (
        Index('idx_bundle_items_bundle', 'bundle_id'),
        Index('idx_bundle_items_composite', 'bundle_id', 'media_id', unique=True),
    )


class BundleFanInteraction(Base):
    """Bundle fan interactions table model - matches db_worker.py schema."""
    __tablename__ = 'bundle_fan_interactions_new'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    bundle_id: Mapped[str] = mapped_column(Text, ForeignKey('bundles_new.bundle_id', ondelete='CASCADE'), nullable=False)
    fan_user_id: Mapped[str] = mapped_column(Text, nullable=False)
    message_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    sent_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    is_purchased: Mapped[bool] = mapped_column(Boolean, default=False)
    purchased_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=datetime.now)

    __table_args__ = (
        Index('idx_bundle_interactions_bundle', 'bundle_id'),
        Index('idx_bundle_interactions_fan', 'fan_user_id'),
        Index('idx_bundle_interactions_composite', 'bundle_id', 'fan_user_id', unique=True),
    )


class BundleAnalytics(Base):
    """Bundle analytics table model - matches db_worker.py schema."""
    __tablename__ = 'bundle_analytics_new'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    bundle_id: Mapped[str] = mapped_column(Text, ForeignKey('bundles_new.bundle_id', ondelete='CASCADE'), unique=True, nullable=False)
    api_sent_count: Mapped[int] = mapped_column(Integer, default=0)
    api_viewed_count: Mapped[int] = mapped_column(Integer, default=0)
    api_purchased_count: Mapped[int] = mapped_column(Integer, default=0)
    tracked_offers: Mapped[int] = mapped_column(Integer, default=0)
    tracked_purchases: Mapped[int] = mapped_column(Integer, default=0)
    view_rate: Mapped[float] = mapped_column(DECIMAL(5, 2), default=0.00)
    conversion_rate: Mapped[float] = mapped_column(DECIMAL(5, 2), default=0.00)
    total_revenue: Mapped[float] = mapped_column(DECIMAL(10, 2), default=0.00)
    net_revenue: Mapped[float] = mapped_column(DECIMAL(10, 2), default=0.00)
    average_time_to_purchase: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    best_time_of_day: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    best_day_of_week: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    last_synced: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    last_updated: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=datetime.now)

    __table_args__ = (
        Index('idx_bundle_analytics_bundle', 'bundle_id'),
        Index('idx_bundle_analytics_conversion', 'conversion_rate'),
    )


class CreatorCredential(Base):
    """Creator credentials table model - stores encrypted OnlyFans credentials."""
    __tablename__ = 'creator_credentials'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    model_id: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    account_name: Mapped[str] = mapped_column(Text, nullable=False)
    encrypted_email: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    encrypted_password: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    encrypted_auth_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    gologin_profile_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    auth_status: Mapped[str] = mapped_column(Text, default='not_authenticated', nullable=False)
    status: Mapped[str] = mapped_column(Text, default='active', nullable=False)
    last_auth_attempt: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    last_successful_auth: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=datetime.now, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=datetime.now, onupdate=datetime.now, nullable=False)
    created_by: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey('webapp_users.id'), nullable=True)

    __table_args__ = (
        Index('idx_creator_credentials_status', 'status'),
        Index('idx_creator_credentials_auth_status', 'auth_status'),
        CheckConstraint("auth_status IN ('not_authenticated', 'authenticated', 'failed')", name='ck_auth_status'),
        CheckConstraint("status IN ('active', 'inactive')", name='ck_status'),
    )
