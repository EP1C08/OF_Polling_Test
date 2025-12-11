"""Known Fan Processor - Queue Consumer for Incremental Message Fetching.

Continuously monitors Redis queue for known fans detected by WebSocket listeners.
Processes multiple fans in PARALLEL for maximum throughput.

When a known fan event is received:
1. Pop from Redis queue (non-blocking RPOP in batch)
2. Get cutoff_id from database (last known message_id)
3. Fetch ONLY new messages using incremental fetcher
4. Push to Redis for db_worker to save
5. Publish to Pub/Sub for external consumers

One processor per creator, runs 24/7 alongside WebSocket listeners.
"""

import asyncio
import os
import sys
from datetime import datetime
from typing import Optional
import redis.asyncio as aioredis
from modules.logger import setup_logger
from modules.authentication import load_auth, create_api_helper
from modules.cutoff_manager import CutoffManager
from modules.incremental_fetcher import IncrementalFetcher
from modules.bundle_processor import process_bundle_from_message, is_bundle
from modules.redis_producer import RedisProducer


class KnownFanProcessor:
    """Queue processor for known fans requiring incremental message fetch."""

    def __init__(
        self,
        creator_id: str,
        creator_name: str,
        db_url: str,
        redis_host: str = 'redis',
        redis_port: int = 6385,
        concurrent_fans: int = 3
    ):
        """Initialize known fan processor.

        :param creator_id: Creator's OnlyFans ID
        :param creator_name: Creator's display name
        :param db_url: PostgreSQL connection string
        :param redis_host: Redis hostname
        :param redis_port: Redis port
        :param concurrent_fans: Number of fans to process in parallel
        """
        self.creator_id = str(creator_id)
        self.creator_name = creator_name
        self.db_url = db_url
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.concurrent_fans = concurrent_fans

        self.logger = setup_logger(
            f'known_fan_processor_{creator_name}',
            log_type='known_fan_processor'
        )
        self.api = None
        self.authed = None
        self.redis_client: Optional[aioredis.Redis] = None
        self.redis_producer: Optional[RedisProducer] = None
        self.cutoff_manager: Optional[CutoffManager] = None
        self.incremental_fetcher: Optional[IncrementalFetcher] = None
        self.semaphore: Optional[asyncio.Semaphore] = None
        self.active_tasks: set = set()

    async def initialize(self) -> bool:
        """Initialize all components.

        :return: True if successful, False otherwise
        """
        try:
            self.logger.info(f"Initializing known fan processor for {self.creator_name}...")

            auth_details = load_auth(creator_id=self.creator_id)
            if not auth_details:
                self.logger.error("=" * 70)
                self.logger.error("AUTHENTICATION FAILED: Unable to load credentials")
                self.logger.error(f"Creator ID: {self.creator_id}")
                self.logger.error(f"Creator Name: {self.creator_name}")
                self.logger.error("Check auth_multi.json for this creator")
                self.logger.error("=" * 70)
                return False

            self.api, self.authed = await create_api_helper(auth_details, self.logger)
            if not self.authed:
                self.logger.error("=" * 70)
                self.logger.error("AUTHENTICATION FAILED: Unable to authenticate with OnlyFans")
                self.logger.error(f"Creator: {self.creator_name} (ID: {self.creator_id})")
                self.logger.error("Possible causes:")
                self.logger.error("  - Invalid cookie/x_bc token")
                self.logger.error("  - Expired session")
                self.logger.error("  - Account disabled/inactive in auth_multi.json")
                self.logger.error("=" * 70)
                return False

            self.logger.info("Authenticated with OnlyFans API")

            self.redis_client = aioredis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                db=0,
                decode_responses=False
            )
            await self.redis_client.ping()
            self.logger.info("Connected to Redis")

            self.redis_producer = RedisProducer(
                redis_host=self.redis_host,
                redis_port=self.redis_port,
                redis_db=0
            )
            await self.redis_producer.connect()
            self.logger.info("Redis producer initialized")

            self.cutoff_manager = CutoffManager(db_url=self.db_url, logger=self.logger)
            if not await self.cutoff_manager.initialize():
                self.logger.error("Failed to initialize cutoff manager")
                return False
            self.logger.info("Cutoff manager initialized")

            self.incremental_fetcher = IncrementalFetcher(
                cutoff_manager=self.cutoff_manager,
                logger=self.logger
            )
            self.logger.info("Incremental fetcher initialized")

            self.semaphore = asyncio.Semaphore(self.concurrent_fans)
            self.logger.info(f"Parallel processing enabled: {self.concurrent_fans} concurrent fans")

            self.logger.info("All components initialized")
            return True

        except Exception as e:
            self.logger.error(f"Initialization failed: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    async def process_known_fan(self, fan_id: str) -> bool:
        """Fetch and save new messages for a known fan.

        :param fan_id: Known fan's OnlyFans ID
        :return: True if successful, False otherwise
        """
        try:
            self.logger.info("=" * 70)
            self.logger.info(f"KNOWN FAN PROCESSING: {fan_id}")
            self.logger.info("=" * 70)

            user = await self.authed.get_user(fan_id)
            if not user:
                self.logger.warning(f"Could not get user object for fan {fan_id}")
                return False

            self.logger.info(
                f"Fetching new messages for fan {fan_id} (@{user.username})..."
            )

            messages = await self.incremental_fetcher.fetch_new_messages(
                user=user,
                model_id=self.creator_id,
                fan_id=fan_id,
                authed=self.authed,
                limit=20
            )

            if not messages:
                self.logger.debug(f"No new messages for fan {fan_id}")
                return True

            self.logger.info(f"Fetched {len(messages)} new message(s) for fan {fan_id}")

            fetched_at = datetime.now()
            bundle_row_id = 1
            bundles_dict = {}
            bundle_items_list = []
            fan_interactions_list = []

            for message in messages:
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
                    self.logger.error(
                        f"Failed to push message {message_dict.get('message_id')} for fan {fan_id}"
                    )
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

            for bundle in bundles_dict.values():
                await self.redis_producer.push_bundle(self.creator_id, bundle)
            if bundle_items_list:
                await self.redis_producer.push_bundle_items(self.creator_id, bundle_items_list)
            if fan_interactions_list:
                await self.redis_producer.push_fan_interactions(self.creator_id, fan_interactions_list)

            self.logger.info("=" * 70)
            self.logger.info(f"KNOWN FAN COMPLETED: {fan_id}")
            self.logger.info(f"   Messages processed: {len(messages)}")
            self.logger.info(f"   Bundles: {len(bundles_dict)}")
            self.logger.info(f"   Bundle items: {len(bundle_items_list)}")
            self.logger.info("=" * 70)

            if messages:
                latest_msg = messages[0]
                msg_text = latest_msg.get('text', '') or ''
                msg_created_at = latest_msg.get('createdAt', '') or ''

                try:
                    await self.redis_producer.publish_to_pubsub(
                        creator_id=self.creator_id,
                        creator_name=self.creator_name,
                        fan_id=fan_id,
                        message=msg_text,
                        created_at=msg_created_at
                    )
                    self.logger.info(f"Published to Pub/Sub for fan {fan_id}")
                except Exception as pubsub_err:
                    self.logger.warning(f"Failed to publish to Pub/Sub: {str(pubsub_err)}")

            return True

        except Exception as e:
            self.logger.error(f"Error processing known fan {fan_id}: {str(e)}")
            import traceback
            self.logger.error(f"Traceback:\n{traceback.format_exc()}")
            return False

    async def _process_fan_with_semaphore(self, fan_id: str):
        """Process a fan with semaphore control for concurrency limiting.

        :param fan_id: Fan's OnlyFans ID
        """
        async with self.semaphore:
            try:
                success = await self.process_known_fan(fan_id)
                if success:
                    self.logger.info(f"Successfully processed known fan {fan_id}")
                else:
                    self.logger.warning(f"Failed to process known fan {fan_id}")
            except Exception as e:
                self.logger.error(f"Error processing fan {fan_id}: {str(e)}")

    async def run(self):
        """Run known fan processor with parallel Redis queue monitoring."""
        self.logger.info("=" * 70)
        self.logger.info(f"KNOWN FAN PROCESSOR (PARALLEL) - {self.creator_name}")
        self.logger.info(f"Queue: of:{self.creator_id}:known_fans_queue")
        self.logger.info(f"Concurrent fans: {self.concurrent_fans}")
        self.logger.info("=" * 70)

        queue_key = f"of:{self.creator_id}:known_fans_queue"

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
                                f"Popped {len(fans_to_process)} fan(s) from queue, "
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

        if self.cutoff_manager:
            try:
                await self.cutoff_manager.close()
                self.logger.info("Cutoff manager closed")
            except Exception as e:
                self.logger.warning(f"Error closing cutoff manager: {str(e)}")

        if self.redis_client:
            try:
                await self.redis_client.close()
                self.logger.info("Redis client closed")
            except Exception as e:
                self.logger.warning(f"Error closing Redis client: {str(e)}")

        if self.redis_producer:
            try:
                await self.redis_producer.close()
                self.logger.info("Redis producer closed")
            except Exception as e:
                self.logger.warning(f"Error closing Redis producer: {str(e)}")

        if self.api and hasattr(self.api, 'close_pool'):
            try:
                await self.api.close_pool()
                self.logger.info("API session closed")
            except Exception as e:
                self.logger.warning(f"Error closing API: {str(e)}")

        import gc
        gc.collect()
        self.logger.info("Cleanup completed")


async def main():
    """Main entry point for known fan processor."""
    creator_id = os.getenv('CREATOR_ID')
    creator_name = os.getenv('CREATOR_NAME', creator_id)
    db_url = os.getenv('DATABASE_URL')
    redis_host = os.getenv('REDIS_HOST', 'redis')
    redis_port = int(os.getenv('REDIS_PORT', '6385'))
    concurrent_fans = int(os.getenv('CONCURRENT_FANS', '3'))

    if not creator_id:
        print("CREATOR_ID environment variable not set")
        sys.exit(1)

    if not db_url:
        print("DATABASE_URL environment variable not set")
        sys.exit(1)

    processor = KnownFanProcessor(
        creator_id=creator_id,
        creator_name=creator_name,
        db_url=db_url,
        redis_host=redis_host,
        redis_port=redis_port,
        concurrent_fans=concurrent_fans
    )

    if not await processor.initialize():
        print("=" * 70)
        print("FATAL: Failed to initialize known fan processor")
        print("Authentication or component initialization failed")
        print("Container will exit now")
        print("=" * 70)
        sys.exit(1)

    await processor.run()


if __name__ == "__main__":
    asyncio.run(main())
