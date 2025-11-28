"""Database Worker - Multi-Creator Support with SQLAlchemy ORM.

Single container that spawns one worker per creator.
Each worker processes its creator's Redis lists and saves to database using SQLAlchemy.
Includes automatic table creation and batch insert optimization.
"""

import asyncio
import json
import os
import sys
import gc as garbage_collector
import redis.asyncio as aioredis
from typing import Dict, List, Optional
from datetime import datetime
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from models.db_models import Base, Message, Bundle, BundleItem, BundleFanInteraction, BundleAnalytics
from modules.logger import setup_logger


class DatabaseWorker:
    """Database worker that manages multiple creator consumers using SQLAlchemy ORM."""

    def __init__(self, db_url: str, redis_host: str = 'redis', redis_port: int = 6379):
        """Initialize database worker.

        :param db_url: PostgreSQL connection string (will be converted to asyncpg format)
        :param redis_host: Redis hostname
        :param redis_port: Redis port
        """
        from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

        if db_url.startswith('postgresql://'):
            db_url = db_url.replace('postgresql://', 'postgresql+asyncpg://', 1)
        elif not db_url.startswith('postgresql+asyncpg://'):
            raise ValueError("Database URL must start with postgresql:// or postgresql+asyncpg://")

        parsed = urlparse(db_url)
        query_params = parse_qs(parsed.query)

        asyncpg_incompatible_params = {
            'sslmode',
            'channel_binding',
            'target_session_attrs',
            'passfile',
            'connect_timeout',
            'options',
            'application_name',
            'fallback_application_name'
        }

        ssl_required = False
        if 'sslmode' in query_params:
            sslmode_value = query_params['sslmode'][0]
            if sslmode_value in ('require', 'verify-ca', 'verify-full'):
                ssl_required = True

        clean_params = {}
        for key, value in query_params.items():
            if key not in asyncpg_incompatible_params:
                clean_params[key] = value

        if ssl_required:
            clean_params['ssl'] = ['true']

        new_query = urlencode(clean_params, doseq=True) if clean_params else ''
        db_url = urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            '',
            new_query,
            ''
        ))

        self.db_url = db_url
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.engine = None
        self.async_session_maker = None
        self.redis: Optional[aioredis.Redis] = None
        self.logger = setup_logger('db_worker', log_type='db_worker')
        self.tasks: List[asyncio.Task] = []

        self.logger.info(f"Database configured ({parsed.scheme})")

    async def initialize_db_engine(self) -> bool:
        """Initialize SQLAlchemy async engine and session maker.

        :return: True if successful, False otherwise
        """
        try:
            self.logger.info("Initializing SQLAlchemy async engine...")

            connect_args = {}
            if 'ssl=true' in self.db_url.lower():
                connect_args['ssl'] = 'require'
                self.db_url = self.db_url.replace('?ssl=true', '').replace('&ssl=true', '')

            self.engine = create_async_engine(
                self.db_url,
                pool_size=1,
                max_overflow=4,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False,
                connect_args=connect_args
            )

            self.async_session_maker = async_sessionmaker(
                self.engine,
                class_=AsyncSession,
                expire_on_commit=False
            )

            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)

            self.logger.info("✓ Database ready")
            return True
        except Exception as e:
            self.logger.error(f"✗ Failed to initialize database engine: {str(e)}")
            return False

    async def connect_redis(self) -> bool:
        """Connect to Redis.

        :return: True if successful, False otherwise
        """
        try:
            self.redis = await aioredis.from_url(
                f"redis://{self.redis_host}:{self.redis_port}/0",
                encoding="utf-8",
                decode_responses=True
            )
            await self.redis.ping()
            self.logger.info("✓ Redis connected")
            return True
        except Exception as e:
            self.logger.error(f"✗ Failed to connect to Redis: {str(e)}")
            return False


    async def load_creators_from_auth(self, auth_file: str = 'auth_multi.json') -> List[Dict]:
        """Load creator list from auth_multi.json.

        :param auth_file: Path to authentication file
        :return: List of creator dictionaries with id and name
        """
        try:
            with open(auth_file, 'r') as f:
                auth_data = json.load(f)

            creators = []
            if isinstance(auth_data, dict) and 'accounts' in auth_data:
                for account in auth_data['accounts']:
                    if not account.get('active', True):
                        continue

                    creator_id = None
                    if 'auth' in account and 'id' in account['auth']:
                        creator_id = account['auth']['id']
                    elif 'id' in account:
                        creator_id = account['id']

                    creator_name = account.get('name', str(creator_id))

                    if creator_id:
                        creators.append({
                            'id': str(creator_id),
                            'name': creator_name
                        })

            self.logger.info(f"✓ Loaded {len(creators)} creator(s) from {auth_file}")
            return creators
        except Exception as e:
            self.logger.error(f"✗ Failed to load creators: {str(e)}")
            return []

    def _get_list_key(self, creator_id: str, list_type: str) -> str:
        """Generate Redis list key.

        :param creator_id: Creator's ID
        :param list_type: Type of list (messages, bundles, etc.)
        :return: Redis list key
        """
        return f"of:{creator_id}:{list_type}"

    async def reconnect_database(self) -> bool:
        """Reconnect to database after connection loss.

        :return: True if successful, False otherwise
        """
        try:
            if self.engine:
                await self.engine.dispose()
                self.logger.info("Closed old database engine")

            self.engine = create_async_engine(
                self.db_url,
                pool_size=1,
                max_overflow=4,
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
                await conn.execute(text("SELECT 1"))

            self.logger.info("✓ Database reconnected successfully")
            return True
        except Exception as e:
            self.logger.error(f"✗ Failed to reconnect database: {str(e)}")
            return False

    def convert_timestamps(self, data: Dict) -> Dict:
        """Convert ISO timestamp strings to datetime objects and remove metadata fields.

        :param data: Dictionary that may contain timestamp strings
        :return: Dictionary with timestamps converted to datetime objects
        """
        # Remove metadata fields that shouldn't be inserted
        data.pop('pushed_at', None)
        data.pop('id', None)  # Remove auto-increment ID, let DB generate it

        # Convert timestamp strings to datetime objects
        timestamp_fields = ['created_at', 'fetched_at', 'first_seen_at', 'sent_at', 'purchased_at', 'last_synced', 'last_updated']
        for field in timestamp_fields:
            if field in data:
                if isinstance(data[field], str):
                    if data[field] == '':
                        data[field] = None
                    else:
                        try:
                            data[field] = datetime.fromisoformat(data[field].replace('Z', '+00:00'))
                        except (ValueError, AttributeError):
                            pass
        return data

    async def save_messages_batch(self, session: AsyncSession, messages_data: List[Dict]) -> int:
        """Save multiple messages to database using bulk insert.

        :param session: SQLAlchemy async session
        :param messages_data: List of message dictionaries
        :return: Number of messages saved
        """
        if not messages_data:
            return 0

        try:
            cleaned_data = [self.convert_timestamps(msg.copy()) for msg in messages_data]
            stmt = insert(Message).values(cleaned_data)
            stmt = stmt.on_conflict_do_nothing(index_elements=['message_id'])
            await session.execute(stmt)
            await session.commit()
            return len(messages_data)
        except Exception as e:
            await session.rollback()
            self.logger.error(f"✗ Failed to save messages batch: {str(e)}")
            return 0

    async def save_bundles_batch(self, session: AsyncSession, bundles_data: List[Dict]) -> int:
        """Save multiple bundles to database using bulk insert.

        :param session: SQLAlchemy async session
        :param bundles_data: List of bundle dictionaries
        :return: Number of bundles saved
        """
        if not bundles_data:
            return 0

        try:
            cleaned_data = [self.convert_timestamps(bundle.copy()) for bundle in bundles_data]
            stmt = insert(Bundle).values(cleaned_data)
            stmt = stmt.on_conflict_do_nothing(index_elements=['bundle_id'])
            await session.execute(stmt)
            await session.commit()
            return len(bundles_data)
        except Exception as e:
            await session.rollback()
            self.logger.error(f"✗ Failed to save bundles batch: {str(e)}")
            return 0

    async def save_bundle_items_batch(self, session: AsyncSession, items_data: List[Dict]) -> int:
        """Save multiple bundle items to database using bulk insert.

        :param session: SQLAlchemy async session
        :param items_data: List of bundle item dictionaries
        :return: Number of items saved
        """
        if not items_data:
            return 0

        try:
            cleaned_data = [self.convert_timestamps(item.copy()) for item in items_data]
            stmt = insert(BundleItem).values(cleaned_data)
            stmt = stmt.on_conflict_do_nothing(index_elements=['bundle_id', 'media_id'])
            await session.execute(stmt)
            await session.commit()
            return len(items_data)
        except Exception as e:
            await session.rollback()
            self.logger.error(f"✗ Failed to save bundle items batch: {str(e)}")
            return 0

    async def save_interactions_batch(self, session: AsyncSession, interactions_data: List[Dict]) -> int:
        """Save multiple fan interactions to database using bulk insert.

        :param session: SQLAlchemy async session
        :param interactions_data: List of interaction dictionaries
        :return: Number of interactions saved
        """
        if not interactions_data:
            return 0

        try:
            cleaned_data = [self.convert_timestamps(inter.copy()) for inter in interactions_data]
            stmt = insert(BundleFanInteraction).values(cleaned_data)
            stmt = stmt.on_conflict_do_nothing(index_elements=['bundle_id', 'fan_user_id'])
            await session.execute(stmt)
            await session.commit()
            return len(interactions_data)
        except Exception as e:
            await session.rollback()
            self.logger.error(f"✗ Failed to save interactions batch: {str(e)}")
            return 0

    async def save_analytics_batch(self, session: AsyncSession, analytics_data: List[Dict]) -> int:
        """Save multiple analytics records to database using bulk insert with upsert.

        :param session: SQLAlchemy async session
        :param analytics_data: List of analytics dictionaries
        :return: Number of analytics saved
        """
        if not analytics_data:
            return 0

        try:
            cleaned_data = [self.convert_timestamps(ana.copy()) for ana in analytics_data]
            stmt = insert(BundleAnalytics).values(cleaned_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['bundle_id'],
                set_={
                    'api_sent_count': stmt.excluded.api_sent_count,
                    'api_viewed_count': stmt.excluded.api_viewed_count,
                    'api_purchased_count': stmt.excluded.api_purchased_count,
                    'tracked_offers': stmt.excluded.tracked_offers,
                    'tracked_purchases': stmt.excluded.tracked_purchases,
                    'view_rate': stmt.excluded.view_rate,
                    'conversion_rate': stmt.excluded.conversion_rate,
                    'total_revenue': stmt.excluded.total_revenue,
                    'net_revenue': stmt.excluded.net_revenue,
                    'average_time_to_purchase': stmt.excluded.average_time_to_purchase,
                    'best_time_of_day': stmt.excluded.best_time_of_day,
                    'best_day_of_week': stmt.excluded.best_day_of_week,
                    'last_synced': stmt.excluded.last_synced,
                    'last_updated': stmt.excluded.last_updated,
                }
            )
            await session.execute(stmt)
            await session.commit()
            return len(analytics_data)
        except Exception as e:
            await session.rollback()
            self.logger.error(f"✗ Failed to save analytics batch: {str(e)}")
            return 0

    async def process_creator_stream(self, creator_id: str, creator_name: str):
        """Process Redis lists for a specific creator using batch inserts.

        :param creator_id: Creator's OnlyFans ID
        :param creator_name: Creator's display name
        """
        self.logger.info(f"[{creator_name}] Worker started")

        messages_processed = 0
        bundles_processed = 0
        items_processed = 0
        interactions_processed = 0
        analytics_processed = 0

        batch_size = 50
        gc_interval = 1000

        try:
            while True:
                done_key = f"of:{creator_id}:producer_done"
                is_done = await self.redis.get(done_key)

                has_data = False

                messages_key = self._get_list_key(creator_id, 'messages')
                messages_batch = []
                for _ in range(batch_size):
                    message_data = await self.redis.lpop(messages_key)
                    if not message_data:
                        break
                    has_data = True
                    msg = json.loads(message_data)
                    messages_batch.append(msg)
                    del message_data

                if messages_batch:
                    async with self.async_session_maker() as session:
                        saved = await self.save_messages_batch(session, messages_batch)
                        messages_processed += saved
                    del messages_batch

                    if messages_processed > 0 and messages_processed % 100 == 0:
                        self.logger.info(f"[{creator_name}] Progress: {messages_processed} messages")

                bundles_key = self._get_list_key(creator_id, 'bundles')
                bundles_batch = []
                for _ in range(batch_size):
                    bundle_data = await self.redis.lpop(bundles_key)
                    if not bundle_data:
                        break
                    has_data = True
                    bundle = json.loads(bundle_data)
                    bundles_batch.append(bundle)
                    del bundle_data

                if bundles_batch:
                    async with self.async_session_maker() as session:
                        saved = await self.save_bundles_batch(session, bundles_batch)
                        bundles_processed += saved
                    del bundles_batch

                items_key = self._get_list_key(creator_id, 'bundle_items')
                items_batch = []
                for _ in range(batch_size):
                    item_data = await self.redis.lpop(items_key)
                    if not item_data:
                        break
                    has_data = True
                    item = json.loads(item_data)
                    items_batch.append(item)
                    del item_data

                if items_batch:
                    async with self.async_session_maker() as session:
                        saved = await self.save_bundle_items_batch(session, items_batch)
                        items_processed += saved
                    del items_batch

                interactions_key = self._get_list_key(creator_id, 'fan_interactions')
                interactions_batch = []
                for _ in range(batch_size):
                    interaction_data = await self.redis.lpop(interactions_key)
                    if not interaction_data:
                        break
                    has_data = True
                    interaction = json.loads(interaction_data)
                    interactions_batch.append(interaction)
                    del interaction_data

                if interactions_batch:
                    async with self.async_session_maker() as session:
                        saved = await self.save_interactions_batch(session, interactions_batch)
                        interactions_processed += saved
                    del interactions_batch

                analytics_key = self._get_list_key(creator_id, 'analytics')
                analytics_batch = []
                for _ in range(batch_size):
                    analytics_data = await self.redis.lpop(analytics_key)
                    if not analytics_data:
                        break
                    has_data = True
                    analytics = json.loads(analytics_data)
                    analytics_batch.append(analytics)
                    del analytics_data

                if analytics_batch:
                    async with self.async_session_maker() as session:
                        saved = await self.save_analytics_batch(session, analytics_batch)
                        analytics_processed += saved
                    del analytics_batch

                total_processed = messages_processed + bundles_processed + items_processed + interactions_processed + analytics_processed
                if total_processed > 0 and total_processed % gc_interval == 0:
                    garbage_collector.collect()
                    self.logger.info(f"[{creator_name}] Memory cleanup at {total_processed} items")

                if not has_data:
                    if is_done == "1" and not getattr(self, f'_producer_done_logged_{creator_id}', False):
                        self.logger.info(f"[{creator_name}] Producer finished bulk fetch. Continuing to process WebSocket/fan-sync messages...")
                        setattr(self, f'_producer_done_logged_{creator_id}', True)
                    await asyncio.sleep(0.5)

        except Exception as e:
            self.logger.error(f"[{creator_name}] Error: {str(e)}")
            import traceback
            traceback.print_exc()

        self.logger.info(f"[{creator_name}] Finished - Messages: {messages_processed}, Bundles: {bundles_processed}, Items: {items_processed}, Interactions: {interactions_processed}, Analytics: {analytics_processed}")

    async def run(self):
        """Run the database worker with multiple creator consumers."""
        self.logger.info("Database Worker Starting...")

        try:
            if not await self.initialize_db_engine():
                self.logger.error("Failed to initialize database engine")
                sys.exit(1)

            if not await self.connect_redis():
                self.logger.error("Failed to connect to Redis")
                sys.exit(1)

            auth_file = os.getenv('AUTH_FILE', '/app/auth_multi.json')
            creators = await self.load_creators_from_auth(auth_file)

            if not creators:
                self.logger.error("No creators found in auth file")
                sys.exit(1)

            self.logger.info(f"Starting {len(creators)} workers: {', '.join([c['name'] for c in creators])}")

            for creator in creators:
                task = asyncio.create_task(
                    self.process_creator_stream(creator['id'], creator['name'])
                )
                self.tasks.append(task)

            await asyncio.gather(*self.tasks, return_exceptions=True)

            self.logger.info("All workers completed")

        except KeyboardInterrupt:
            self.logger.info("\n⚠️ Interrupted by user, cleaning up...")
        except Exception as e:
            self.logger.error(f"✗ Fatal error: {str(e)}")
            import traceback
            traceback.print_exc()
        finally:
            self.logger.info("Cleaning up connections...")

            if self.redis:
                try:
                    await self.redis.close()
                    await self.redis.connection_pool.disconnect()
                    self.logger.info("✓ Redis connection closed")
                except Exception as e:
                    self.logger.warning(f"⚠️ Error closing Redis: {str(e)}")

            if self.engine:
                try:
                    await self.engine.dispose()
                    self.logger.info("✓ Database engine disposed")
                except Exception as e:
                    self.logger.warning(f"⚠️ Error disposing engine: {str(e)}")

            garbage_collector.collect()
            self.logger.info("✓ Memory cleanup completed")


async def main():
    """Main entry point for database worker."""
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        print("✗ DATABASE_URL environment variable not set")
        sys.exit(1)

    redis_host = os.getenv('REDIS_HOST', 'redis')
    redis_port = int(os.getenv('REDIS_PORT', '6379'))

    worker = DatabaseWorker(db_url, redis_host, redis_port)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
