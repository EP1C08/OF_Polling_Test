"""Cutoff Manager - Query last message IDs from database for incremental fetching.

Uses SQLAlchemy to query PostgreSQL for the last known message_id per fan.
This enables incremental message fetching (only fetch new messages since last known).
"""

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import select, func
from models.db_models import Message
from typing import Optional, Dict, List
import logging


class CutoffManager:
    """Manages cutoff_id queries for incremental message fetching."""

    def __init__(self, db_url: str, logger: Optional[logging.Logger] = None):
        """Initialize cutoff manager.

        :param db_url: PostgreSQL connection string
        :param logger: Optional logger instance
        """
        if db_url.startswith('postgresql://'):
            db_url = db_url.replace('postgresql://', 'postgresql+asyncpg://', 1)
        elif not db_url.startswith('postgresql+asyncpg://'):
            raise ValueError("Database URL must start with postgresql:// or postgresql+asyncpg://")

        # Remove PostgreSQL-specific parameters that asyncpg doesn't support
        import re
        unsupported_params = ['sslmode', 'channel_binding', 'gssencmode', 'krbsrvname', 'passfile', 'service']
        for param in unsupported_params:
            db_url = re.sub(rf'[?&]{param}=[^&]*', '', db_url)
        # Clean up any trailing ? or &
        db_url = db_url.rstrip('?&')

        self.db_url = db_url
        self.engine = None
        self.async_session_maker = None
        self.logger = logger or logging.getLogger(__name__)

    async def initialize(self) -> bool:
        """Initialize database engine.

        :return: True if successful, False otherwise
        """
        try:
            self.engine = create_async_engine(
                self.db_url,
                pool_size=1,
                max_overflow=2,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False
            )

            self.async_session_maker = async_sessionmaker(
                self.engine,
                class_=AsyncSession,
                expire_on_commit=False
            )

            async with self.engine.connect() as conn:
                await conn.execute(select(1))

            self.logger.info("✓ Cutoff manager initialized")
            return True
        except Exception as e:
            self.logger.error(f"✗ Failed to initialize cutoff manager: {str(e)}")
            return False

    async def get_cutoff_id(self, model_id: str, fan_id: str) -> Optional[int]:
        """Get the last known message_id for a specific fan.

        :param model_id: Creator's OnlyFans ID
        :param fan_id: Fan's OnlyFans ID
        :return: Last message_id as integer, or None if no messages found
        """
        try:
            async with self.async_session_maker() as session:
                stmt = select(func.max(Message.message_id)).where(
                    Message.model_id == str(model_id),
                    Message.fan_id == str(fan_id)
                )
                result = await session.execute(stmt)
                cutoff_id = result.scalar()

                if cutoff_id:
                    try:
                        return int(cutoff_id)
                    except (ValueError, TypeError):
                        self.logger.warning(f"⚠️ Invalid message_id format: {cutoff_id}")
                        return None

                return None
        except Exception as e:
            self.logger.error(f"✗ Failed to get cutoff_id for fan {fan_id}: {str(e)}")
            return None

    async def get_all_fan_cutoffs(self, model_id: str) -> Dict[str, int]:
        """Get all fan cutoff_ids for a creator (for fan sync).

        :param model_id: Creator's OnlyFans ID
        :return: Dictionary mapping fan_id to last message_id
        """
        try:
            async with self.async_session_maker() as session:
                stmt = select(
                    Message.fan_id,
                    func.max(Message.message_id).label('last_message_id')
                ).where(
                    Message.model_id == str(model_id)
                ).group_by(Message.fan_id)

                # Use streaming for memory efficiency
                result = await session.stream(stmt)
                cutoffs = {}

                async for row in result:
                    fan_id = row.fan_id
                    last_msg_id = row.last_message_id

                    try:
                        cutoffs[fan_id] = int(last_msg_id)
                    except (ValueError, TypeError):
                        self.logger.warning(f"⚠️ Invalid message_id for fan {fan_id}: {last_msg_id}")
                        continue

                self.logger.info(f"✓ Retrieved cutoff_ids for {len(cutoffs)} fans")
                return cutoffs
        except Exception as e:
            self.logger.error(f"✗ Failed to get all fan cutoffs: {str(e)}")
            return {}

    async def get_known_fans(self, model_id: str) -> List[str]:
        """Get list of all known fan IDs for a creator.

        :param model_id: Creator's OnlyFans ID
        :return: List of fan IDs
        """
        try:
            async with self.async_session_maker() as session:
                stmt = select(Message.fan_id).where(
                    Message.model_id == str(model_id)
                ).distinct()

                result = await session.stream(stmt)
                fans = []

                async for row in result:
                    fans.append(row.fan_id)

                self.logger.info(f"✓ Found {len(fans)} known fans for creator {model_id}")
                return fans
        except Exception as e:
            self.logger.error(f"✗ Failed to get known fans: {str(e)}")
            return []

    async def close(self):
        """Close database engine and cleanup resources."""
        if self.engine:
            try:
                await self.engine.dispose()
                self.logger.info("✓ Cutoff manager closed")
            except Exception as e:
                self.logger.warning(f"⚠️ Error closing cutoff manager: {str(e)}")
