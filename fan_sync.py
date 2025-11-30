"""Fan Sync - Periodic New Subscriber Detection.

Runs every 4 hours to detect new subscribers:
1. Query database for all known fan IDs
2. Load conversations JSON (all current subscribers from API)
3. Compare to find new fans
4. Fetch complete message history for new fans
5. Push to Redis for db_worker to save

One sync worker per creator, runs on schedule.
"""

import asyncio
import json
import os
import sys
from typing import List, Set
import redis.asyncio as aioredis
from modules.logger import setup_logger
from modules.authentication import load_auth, create_api_helper
from modules.cutoff_manager import CutoffManager
from modules.conversation_loader import load_conversations_from_json
from modules.message_fetcher import fetch_all_messages
from modules.bundle_processor import process_bundle_from_message, is_bundle
from modules.redis_producer import RedisProducer


class FanSync:
    """Periodic fan synchronization to detect new subscribers."""

    def __init__(
        self,
        creator_id: str,
        creator_name: str,
        db_url: str,
        redis_host: str = 'redis',
        redis_port: int = 6385,
        sync_interval: int = 14400
    ):
        """Initialize fan sync worker.

        :param creator_id: Creator's OnlyFans ID
        :param creator_name: Creator's display name
        :param db_url: PostgreSQL connection string
        :param redis_host: Redis hostname
        :param redis_port: Redis port
        :param sync_interval: Sync interval in seconds (default: 14400 = 4 hours)
        """
        self.creator_id = str(creator_id)
        self.creator_name = creator_name
        self.db_url = db_url
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.sync_interval = sync_interval

        self.logger = setup_logger(f'fan_sync_{creator_name}', log_type='fan_sync')
        self.api = None
        self.authed = None
        self.cutoff_manager = None
        self.redis_producer = None

    async def initialize(self) -> bool:
        """Initialize all components.

        :return: True if successful, False otherwise
        """
        try:
            self.logger.info(f"Initializing fan sync for {self.creator_name}...")

            auth_details = load_auth(creator_id=self.creator_id)
            if not auth_details:
                self.logger.error("=" * 70)
                self.logger.error("✗ AUTHENTICATION FAILED: Unable to load credentials")
                self.logger.error(f"✗ Creator ID: {self.creator_id}")
                self.logger.error(f"✗ Creator Name: {self.creator_name}")
                self.logger.error("✗ Check auth_multi.json for this creator")
                self.logger.error("=" * 70)
                return False

            self.api, self.authed = await create_api_helper(auth_details, self.logger)
            if not self.authed:
                self.logger.error("=" * 70)
                self.logger.error("✗ AUTHENTICATION FAILED: Unable to authenticate with OnlyFans")
                self.logger.error(f"✗ Creator: {self.creator_name} (ID: {self.creator_id})")
                self.logger.error("✗ Possible causes:")
                self.logger.error("  - Invalid cookie/x_bc token")
                self.logger.error("  - Expired session")
                self.logger.error("  - Account disabled/inactive in auth_multi.json")
                self.logger.error("=" * 70)
                return False

            self.logger.info("✓ Authenticated with OnlyFans API")

            self.redis_producer = RedisProducer(
                redis_host=self.redis_host,
                redis_port=self.redis_port,
                redis_db=0
            )
            await self.redis_producer.connect()
            self.logger.info("✓ Connected to Redis")

            self.cutoff_manager = CutoffManager(db_url=self.db_url, logger=self.logger)
            if not await self.cutoff_manager.initialize():
                self.logger.error("Failed to initialize cutoff manager")
                return False

            self.logger.info("✓ All components initialized")
            return True

        except Exception as e:
            self.logger.error(f"✗ Initialization failed: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    async def get_known_fans_from_db(self) -> Set[str]:
        """Query database for all known fan IDs.

        :return: Set of known fan IDs
        """
        try:
            self.logger.info("Querying database for known fans...")
            known_fans = await self.cutoff_manager.get_known_fans(self.creator_id)
            self.logger.info(f"✓ Found {len(known_fans)} known fans in database")
            return set(known_fans)
        except Exception as e:
            self.logger.error(f"✗ Failed to query known fans: {str(e)}")
            return set()

    async def get_all_fans_from_conversations(self) -> Set[str]:
        """Load all current subscribers from conversations JSON.

        :return: Set of all fan IDs from conversations
        """
        try:
            self.logger.info("Loading conversations from JSON...")
            conversations = load_conversations_from_json(self.creator_name)

            if not conversations:
                self.logger.warning("No conversations loaded, falling back to API")
                return set()

            all_fans = set()
            for conv in conversations:
                fan_id = str(conv.get('id'))
                all_fans.add(fan_id)

            self.logger.info(f"✓ Loaded {len(all_fans)} total fans from conversations")
            return all_fans

        except Exception as e:
            self.logger.error(f"✗ Failed to load conversations: {str(e)}")
            return set()

    async def process_new_fan(self, fan_id: str) -> bool:
        """Fetch and save complete message history for a new fan.

        :param fan_id: New fan's OnlyFans ID
        :return: True if successful, False otherwise
        """
        try:
            self.logger.info(f"Processing new fan: {fan_id}")

            user = await self.authed.get_user(fan_id)
            if not user:
                self.logger.warning(f"⚠️ Could not get user object for fan {fan_id}")
                return False

            messages = await fetch_all_messages(
                user=user,
                limit=20,
                cutoff_id=None,
                authed=self.authed
            )

            if not messages:
                self.logger.info(f"No messages found for new fan {fan_id}")
                return True

            from datetime import datetime

            fetched_at = datetime.now()
            bundle_row_id = 1
            bundles_dict = {}
            bundle_items_list = []
            fan_interactions_list = []

            for message in messages:
                author_id = str(message.author.id)
                is_from_creator = (author_id == self.creator_id)

                media_id = ''
                media_type = ''
                if message.media and len(message.media) > 0:
                    first_media = message.media[0]
                    media_id = str(first_media.get('id', ''))
                    media_type = first_media.get('type', '').lower()

                message_dict = {
                    'message_id': str(message.id),
                    'model_id': self.creator_id,
                    'fan_id': fan_id,
                    'sender_id': author_id,
                    'model_name': self.creator_name,
                    'sender_username': message.author.username,
                    'message': message.text or '',
                    'message_type': 'bundle' if is_bundle(message) else ('media' if media_id else 'text'),
                    'media_id': media_id,
                    'media_type': media_type,
                    'price': float(message.price) if message.price else 0.0,
                    'is_free': message.isFree if hasattr(message, 'isFree') else True,
                    'is_purchased': message.canPurchase is False if hasattr(message, 'canPurchase') else False,
                    'is_from_me': is_from_creator,
                    'created_at': message.created_at.isoformat() if message.created_at else None,
                    'fetched_at': fetched_at.isoformat()
                }
                try:
                    await self.redis_producer.push_message(self.creator_id, message_dict)
                except Exception as push_error:
                    self.logger.error(f"✗ Failed to push message {message_dict.get('message_id')} for fan {fan_id}")
                    self.logger.error(f"  Error: {str(push_error)}")
                    self.logger.error(f"  Redis key would be: of:{self.creator_id}:messages")
                    import traceback
                    self.logger.error(f"  Traceback:\n{traceback.format_exc()}")
                    raise

                if is_bundle(message):
                    bundle_data, bundle_items, fan_interaction = process_bundle_from_message(
                        message, self.creator_id, self.creator_name, fan_id, bundle_row_id, fetched_at
                    )
                    if bundle_data:
                        bundle_id = bundle_data['bundle_id']
                        if bundle_id not in bundles_dict:
                            bundles_dict[bundle_id] = bundle_data
                        bundle_items_list.extend(bundle_items)
                        if fan_interaction:
                            fan_interactions_list.append(fan_interaction)
                        bundle_row_id += 1

            for bundle in bundles_dict.values():
                await self.redis_producer.push_bundle(self.creator_id, bundle)
            if bundle_items_list:
                await self.redis_producer.push_bundle_items(self.creator_id, bundle_items_list)
            if fan_interactions_list:
                await self.redis_producer.push_fan_interactions(self.creator_id, fan_interactions_list)

            self.logger.info(f"✓ Processed {len(messages)} messages for new fan {fan_id}")
            return True

        except Exception as e:
            self.logger.error(f"✗ Error processing new fan {fan_id}: {str(e)}")
            import traceback
            self.logger.error(f"Traceback:\n{traceback.format_exc()}")
            return False

    async def sync_fans(self):
        """Perform fan synchronization (detect and process new subscribers)."""
        try:
            self.logger.info("=" * 70)
            self.logger.info("Starting fan synchronization...")
            self.logger.info("=" * 70)

            known_fans = await self.get_known_fans_from_db()
            all_fans = await self.get_all_fans_from_conversations()

            if not all_fans:
                self.logger.warning("No fans loaded from conversations, skipping sync")
                return

            new_fans = all_fans - known_fans

            if not new_fans:
                self.logger.info("✓ No new fans detected")
                return

            self.logger.info(f"🆕 Found {len(new_fans)} new fan(s)!")

            for fan_id in new_fans:
                await self.process_new_fan(fan_id)
                await asyncio.sleep(2)

            self.logger.info("=" * 70)
            self.logger.info(f"✓ Fan sync completed: processed {len(new_fans)} new fans")
            self.logger.info("=" * 70)

        except Exception as e:
            self.logger.error(f"✗ Fan sync failed: {str(e)}")
            import traceback
            traceback.print_exc()

    async def run(self):
        """Run fan sync worker with periodic synchronization."""
        self.logger.info("=" * 70)
        self.logger.info(f"FAN SYNC WORKER - {self.creator_name}")
        self.logger.info(f"Sync interval: {self.sync_interval} seconds ({self.sync_interval // 3600} hours)")
        self.logger.info("=" * 70)

        try:
            while True:
                await self.sync_fans()

                self.logger.info(f"💤 Sleeping for {self.sync_interval // 3600} hours...")
                await asyncio.sleep(self.sync_interval)

        except KeyboardInterrupt:
            self.logger.info("\n⚠️ Interrupted by user")
        except Exception as e:
            self.logger.error(f"✗ Fatal error: {str(e)}")
            import traceback
            traceback.print_exc()
        finally:
            await self.cleanup()

    async def cleanup(self):
        """Cleanup all resources."""
        self.logger.info("Cleaning up resources...")

        if self.cutoff_manager:
            try:
                await self.cutoff_manager.close()
            except Exception as e:
                self.logger.warning(f"⚠️ Error closing cutoff manager: {str(e)}")

        if self.redis_producer:
            try:
                await self.redis_producer.close()
                self.logger.info("✓ Redis connection closed")
            except Exception as e:
                self.logger.warning(f"⚠️ Error closing Redis: {str(e)}")

        if self.api and hasattr(self.api, 'close_pool'):
            try:
                await self.api.close_pool()
                self.logger.info("✓ API session closed")
            except Exception as e:
                self.logger.warning(f"⚠️ Error closing API: {str(e)}")

        import gc
        gc.collect()
        self.logger.info("✓ Cleanup completed")


async def main():
    """Main entry point for fan sync worker."""
    creator_id = os.getenv('CREATOR_ID')
    creator_name = os.getenv('CREATOR_NAME', creator_id)
    db_url = os.getenv('DATABASE_URL')
    redis_host = os.getenv('REDIS_HOST', 'redis')
    redis_port = int(os.getenv('REDIS_PORT', '6385'))
    sync_interval = int(os.getenv('SYNC_INTERVAL', '14400'))

    if not creator_id:
        print("✗ CREATOR_ID environment variable not set")
        sys.exit(1)

    if not db_url:
        print("✗ DATABASE_URL environment variable not set")
        sys.exit(1)

    fan_sync = FanSync(
        creator_id=creator_id,
        creator_name=creator_name,
        db_url=db_url,
        redis_host=redis_host,
        redis_port=redis_port,
        sync_interval=sync_interval
    )

    if not await fan_sync.initialize():
        print("=" * 70)
        print("✗ FATAL: Failed to initialize fan sync")
        print("✗ Authentication or component initialization failed")
        print("✗ Container will exit now")
        print("=" * 70)
        sys.exit(1)

    await fan_sync.run()


if __name__ == "__main__":
    asyncio.run(main())
