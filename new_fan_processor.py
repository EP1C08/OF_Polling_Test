"""New Fan Processor - Priority Queue for New Subscriber Full History Fetch.

Continuously monitors Redis priority queue for new fans detected by WebSocket listeners.
Processes multiple fans in PARALLEL for maximum throughput.

When a new fan is detected:
1. Pop from Redis priority queue (non-blocking RPOP in batch)
2. Check checkpoint to avoid duplicates
3. Fetch FULL message history (cutoff_id=None)
4. Push to Redis for db_worker to save
5. Mark as completed in checkpoint

One processor per creator, runs 24/7 alongside WebSocket listeners.
"""

import asyncio
import json
import os
import sys
from typing import Optional
import redis.asyncio as aioredis
from modules.logger import setup_logger
from modules.authentication import create_api_helper
from modules.db_credential_loader import load_credentials_from_db
from modules.checkpoint import CheckpointManager
from ultima_scraper_api.apis.onlyfans.classes.extras import AuthDetails
from modules.message_fetcher import fetch_all_messages_fast
from modules.bundle_processor import process_bundle_from_message, is_bundle
from modules.redis_producer import RedisProducer
from modules.chat_metadata_fetcher import ChatMetadataFetcher
from modules.message_age_filter import is_message_old_enough


class NewFanProcessor:
    """Priority queue processor for new fans requiring full message history fetch."""

    def __init__(
        self,
        creator_id: str,
        creator_name: str,
        redis_host: str = 'redis',
        redis_port: int = 6385,
        concurrent_fans: int = 3
    ):
        """Initialize new fan processor.

        :param creator_id: Creator's OnlyFans ID
        :param creator_name: Creator's display name
        :param redis_host: Redis hostname
        :param redis_port: Redis port
        :param concurrent_fans: Number of fans to process in parallel
        """
        self.creator_id = str(creator_id)
        self.creator_name = creator_name
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.concurrent_fans = concurrent_fans

        self.logger = setup_logger(f'new_fan_processor_{creator_name}', log_type='new_fan_processor')
        self.api = None
        self.authed = None
        self.redis_client: Optional[aioredis.Redis] = None
        self.redis_producer: Optional[RedisProducer] = None
        self.checkpoint: Optional[CheckpointManager] = None
        self.semaphore: Optional[asyncio.Semaphore] = None
        self.active_tasks: set = set()
        self.gologin_profile_id: Optional[str] = None

    async def initialize(self) -> bool:
        """Initialize all components.

        :return: True if successful, False otherwise
        """
        try:
            self.logger.info(f"Initializing new fan processor for {self.creator_name}...")

            # Load credentials from database (includes gologin_profile_id)
            credentials = await load_credentials_from_db(model_id=self.creator_id)
            if not credentials:
                self.logger.error("=" * 70)
                self.logger.error("✗ AUTHENTICATION FAILED: Unable to load credentials")
                self.logger.error(f"✗ Creator ID: {self.creator_id}")
                self.logger.error(f"✗ Creator Name: {self.creator_name}")
                self.logger.error("✗ Check creator_credentials table for this creator")
                self.logger.error("=" * 70)
                return False

            cred = credentials[0]
            self.gologin_profile_id = cred.get('gologin_profile_id')
            if self.gologin_profile_id:
                self.logger.info(f"GoLogin profile ID: {self.gologin_profile_id}")

            # Create AuthDetails from credential
            auth_obj = cred.get('auth', {})
            auth_details = AuthDetails(
                id=cred.get('id'),
                username=cred.get('username', cred.get('name', '')),
                cookie=auth_obj.get('cookie', ''),
                x_bc=auth_obj.get('x_bc', ''),
                user_agent=auth_obj.get('user_agent', ''),
                email=cred.get('email', auth_obj.get('email', '')),
                password=cred.get('password', auth_obj.get('password', '')),
                support_2fa=auth_obj.get('support_2fa', True)
            )

            # Create API with GoLogin proxy support
            self.api, self.authed = await create_api_helper(
                auth_details,
                self.logger,
                gologin_profile_id=self.gologin_profile_id
            )
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

            self.redis_client = aioredis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                db=0,
                decode_responses=False
            )
            await self.redis_client.ping()
            self.logger.info("✓ Connected to Redis")

            self.redis_producer = RedisProducer(
                redis_host=self.redis_host,
                redis_port=self.redis_port,
                redis_db=0
            )
            await self.redis_producer.connect()
            self.logger.info("✓ Redis producer initialized")

            self.checkpoint = CheckpointManager(self.creator_name)
            self.logger.info("Checkpoint manager initialized")

            self.semaphore = asyncio.Semaphore(self.concurrent_fans)
            self.logger.info(f"Parallel processing enabled: {self.concurrent_fans} concurrent fans")

            self.logger.info("All components initialized")
            return True

        except Exception as e:
            self.logger.error(f"✗ Initialization failed: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    async def process_priority_fan(self, fan_id: str) -> bool:
        """Fetch and save complete message history for a new fan.

        :param fan_id: New fan's OnlyFans ID
        :return: True if successful, False otherwise
        """
        try:
            self.logger.info("=" * 70)
            self.logger.info(f"🔥 PRIORITY FAN PROCESSING: {fan_id}")
            self.logger.info("=" * 70)

            # Check if already processed (avoid duplicates)
            if self.checkpoint.is_completed(fan_id):
                self.logger.info(f"✓ Fan {fan_id} already processed, skipping")
                return True

            # Mark as in-progress in checkpoint
            self.checkpoint.mark_in_progress(fan_id)
            self.logger.info(f"✓ Marked fan {fan_id} as in-progress")

            # Get user object
            user = await self.authed.get_user(fan_id)
            if not user:
                self.logger.warning(f"⚠️ Could not get user object for fan {fan_id}")
                self.checkpoint.mark_in_progress(fan_id, remove=True)  # Remove from in-progress
                return False

            # 24-hour pre-check: Skip if last message is too recent
            metadata_fetcher = ChatMetadataFetcher(self.authed, self.logger)
            last_msg_time = await metadata_fetcher.get_last_message_time(fan_id)

            min_age_hours = int(os.getenv('MIN_MESSAGE_AGE_HOURS', '24'))
            if last_msg_time and not is_message_old_enough(last_msg_time, min_age_hours):
                self.logger.info(f"New fan {fan_id} last message < {min_age_hours}h old, deferring")
                self.checkpoint.mark_in_progress(fan_id, remove=True)  # Remove from in-progress
                return True  # Skip for now, will be re-queued when message ages

            self.logger.info(f"Fetching FULL message history for fan {fan_id} (@{user.username})...")

            # Fetch FULL message history (cutoff_id=None)
            messages = await fetch_all_messages_fast(
                user=user,
                authed=self.authed,
                cutoff_id=None,  # FULL fetch from beginning
                logger=self.logger
            )

            if not messages:
                self.logger.info(f"No messages found for new fan {fan_id}")
                # Still mark as completed to avoid re-processing
                self.checkpoint.mark_completed(fan_id)
                return True

            self.logger.info(f"✓ Fetched {len(messages)} messages for fan {fan_id}")

            # Process messages and bundles (same logic as fan_sync.py)
            from datetime import datetime

            fetched_at = datetime.now()
            bundle_row_id = 1
            bundles_dict = {}
            bundle_items_list = []
            fan_interactions_list = []

            for message in messages:
                # Handle both dict and object formats
                if isinstance(message, dict):
                    from_user = message.get("fromUser", {}) or {}
                    author_id = str(from_user.get("id", ""))
                    author_username = from_user.get("username", f"u{from_user.get('id', '')}")
                    message_id = str(message.get("id", ""))
                    message_text = message.get("text", "") or ""
                    message_price = message.get("price", 0) or 0
                    message_is_free = message.get("isFree", True)
                    message_can_purchase = message.get("canPurchase", True)
                    message_created_at = message.get("createdAt") or message.get("created_at")
                    message_media = message.get("media", []) or []
                else:
                    author_id = str(message.author.id)
                    author_username = message.author.username
                    message_id = str(message.id)
                    message_text = message.text or ""
                    message_price = message.price if hasattr(message, 'price') else 0
                    message_is_free = message.isFree if hasattr(message, 'isFree') else True
                    message_can_purchase = message.canPurchase if hasattr(message, 'canPurchase') else True
                    message_created_at = message.created_at
                    message_media = message.media if hasattr(message, 'media') else []

                is_from_creator = (author_id == self.creator_id)

                media_id = ''
                media_type = ''
                if message_media and len(message_media) > 0:
                    first_media = message_media[0]
                    media_id = str(first_media.get('id', ''))
                    media_type = first_media.get('type', '').lower()

                created_at_str = None
                if message_created_at:
                    if isinstance(message_created_at, str):
                        created_at_str = message_created_at
                    else:
                        created_at_str = message_created_at.isoformat()

                message_dict = {
                    'message_id': message_id,
                    'model_id': self.creator_id,
                    'fan_id': fan_id,
                    'sender_id': author_id,
                    'model_name': self.creator_name,
                    'sender_username': author_username,
                    'message': message_text,
                    'message_type': 'bundle' if is_bundle(message) else ('media' if media_id else 'text'),
                    'media_id': media_id,
                    'media_type': media_type,
                    'price': float(message_price) if message_price else 0.0,
                    'is_free': message_is_free,
                    'is_purchased': message_can_purchase is False,
                    'is_from_me': is_from_creator,
                    'created_at': created_at_str,
                    'fetched_at': fetched_at.isoformat()
                }

                try:
                    await self.redis_producer.push_message(self.creator_id, message_dict)
                except Exception as push_error:
                    self.logger.error(f"✗ Failed to push message {message_dict.get('message_id')} for fan {fan_id}")
                    self.logger.error(f"  Error: {str(push_error)}")
                    self.logger.error(f"  Redis key: of:{self.creator_id}:messages")
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

            # Push bundles, items, and interactions
            for bundle in bundles_dict.values():
                await self.redis_producer.push_bundle(self.creator_id, bundle)
            if bundle_items_list:
                await self.redis_producer.push_bundle_items(self.creator_id, bundle_items_list)
            if fan_interactions_list:
                await self.redis_producer.push_fan_interactions(self.creator_id, fan_interactions_list)

            # Mark as completed in checkpoint
            self.checkpoint.mark_completed(fan_id)

            self.logger.info("=" * 70)
            self.logger.info(f"✅ PRIORITY FAN COMPLETED: {fan_id}")
            self.logger.info(f"   Messages processed: {len(messages)}")
            self.logger.info(f"   Bundles: {len(bundles_dict)}")
            self.logger.info(f"   Bundle items: {len(bundle_items_list)}")
            self.logger.info("=" * 70)

            return True

        except Exception as e:
            self.logger.error(f"✗ Error processing priority fan {fan_id}: {str(e)}")
            import traceback
            self.logger.error(f"Traceback:\n{traceback.format_exc()}")

            # Remove from in-progress to allow retry
            try:
                self.checkpoint.mark_in_progress(fan_id, remove=True)
            except:
                pass

            return False

    async def _process_fan_with_semaphore(self, fan_id: str):
        """Process a fan with semaphore control for concurrency limiting.

        :param fan_id: Fan's OnlyFans ID
        """
        async with self.semaphore:
            try:
                success = await self.process_priority_fan(fan_id)
                if success:
                    self.logger.info(f"Successfully processed priority fan {fan_id}")
                else:
                    self.logger.warning(f"Failed to process priority fan {fan_id}")
            except Exception as e:
                self.logger.error(f"Error processing fan {fan_id}: {str(e)}")

    async def run(self):
        """Run new fan processor with parallel Redis queue monitoring."""
        self.logger.info("=" * 70)
        self.logger.info(f"NEW FAN PROCESSOR (PARALLEL) - {self.creator_name}")
        self.logger.info(f"Priority Queue: of:{self.creator_id}:new_fans_priority")
        self.logger.info(f"Concurrent fans: {self.concurrent_fans}")
        self.logger.info("=" * 70)

        queue_key = f"of:{self.creator_id}:new_fans_priority"

        try:
            while True:
                try:
                    # Clean up completed tasks
                    self.active_tasks = {t for t in self.active_tasks if not t.done()}

                    # Check how many slots are available
                    available_slots = self.concurrent_fans - len(self.active_tasks)

                    if available_slots > 0:
                        # Try to pop multiple fans from queue (non-blocking)
                        fans_to_process = []
                        for _ in range(available_slots):
                            result = await self.redis_client.rpop(queue_key)
                            if result:
                                fan_id = result.decode('utf-8')
                                fans_to_process.append(fan_id)
                            else:
                                break

                        if fans_to_process:
                            self.logger.info(
                                f"Popped {len(fans_to_process)} new fan(s) from queue, "
                                f"active tasks: {len(self.active_tasks)}"
                            )

                            # Start processing each fan in parallel
                            for fan_id in fans_to_process:
                                task = asyncio.create_task(
                                    self._process_fan_with_semaphore(fan_id)
                                )
                                self.active_tasks.add(task)

                    # If no fans were found and no active tasks, wait a bit
                    if not self.active_tasks:
                        # Use blocking pop when idle to avoid busy waiting
                        result = await self.redis_client.brpop(queue_key, timeout=5)
                        if result:
                            _, fan_id_bytes = result
                            fan_id = fan_id_bytes.decode('utf-8')
                            task = asyncio.create_task(
                                self._process_fan_with_semaphore(fan_id)
                            )
                            self.active_tasks.add(task)
                    else:
                        # Give other tasks a chance to run
                        await asyncio.sleep(0.1)

                except asyncio.CancelledError:
                    self.logger.info("Processor cancelled, waiting for active tasks...")
                    if self.active_tasks:
                        await asyncio.gather(*self.active_tasks, return_exceptions=True)
                    break
                except Exception as e:
                    self.logger.error(f"Error in main loop: {str(e)}")
                    import traceback
                    self.logger.error(traceback.format_exc())
                    await asyncio.sleep(5)

        except KeyboardInterrupt:
            self.logger.info("\nInterrupted by user")
            if self.active_tasks:
                self.logger.info(f"Waiting for {len(self.active_tasks)} active tasks to complete...")
                await asyncio.gather(*self.active_tasks, return_exceptions=True)
        except Exception as e:
            self.logger.error(f"Fatal error: {str(e)}")
            import traceback
            traceback.print_exc()
        finally:
            await self.cleanup()

    async def cleanup(self):
        """Cleanup all resources."""
        self.logger.info("Cleaning up resources...")

        if self.redis_client:
            try:
                await self.redis_client.close()
                self.logger.info("✓ Redis client closed")
            except Exception as e:
                self.logger.warning(f"⚠️ Error closing Redis client: {str(e)}")

        if self.redis_producer:
            try:
                await self.redis_producer.close()
                self.logger.info("✓ Redis producer closed")
            except Exception as e:
                self.logger.warning(f"⚠️ Error closing Redis producer: {str(e)}")

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
    """Main entry point for new fan processor."""
    creator_id = os.getenv('CREATOR_ID')
    creator_name = os.getenv('CREATOR_NAME', creator_id)
    redis_host = os.getenv('REDIS_HOST', 'redis')
    redis_port = int(os.getenv('REDIS_PORT', '6385'))
    concurrent_fans = int(os.getenv('CONCURRENT_FANS', '3'))

    if not creator_id:
        print("CREATOR_ID environment variable not set")
        sys.exit(1)

    processor = NewFanProcessor(
        creator_id=creator_id,
        creator_name=creator_name,
        redis_host=redis_host,
        redis_port=redis_port,
        concurrent_fans=concurrent_fans
    )

    if not await processor.initialize():
        print("=" * 70)
        print("✗ FATAL: Failed to initialize new fan processor")
        print("✗ Authentication or component initialization failed")
        print("✗ Container will exit now")
        print("=" * 70)
        sys.exit(1)

    await processor.run()


if __name__ == "__main__":
    asyncio.run(main())
