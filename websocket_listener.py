"""WebSocket Listener - 24/7 Real-Time Message Notifications.

Listens to OnlyFans WebSocket for instant message notifications.
When a new message event is received:
1. Query database for last known message_id (cutoff_id)
2. Fetch only new messages using incremental fetcher
3. Push to Redis lists for db_worker to save

One listener per creator, runs continuously.
"""

import asyncio
import json
import os
import sys
import redis.asyncio as aioredis
from typing import Optional
from modules.logger import setup_logger
from modules.authentication import load_auth, create_api_helper
from modules.cutoff_manager import CutoffManager
from modules.incremental_fetcher import IncrementalFetcher
from modules.redis_producer import RedisProducer
from modules.message_fetcher import fetch_all_messages_fast
from modules.bundle_processor import process_bundle_from_message


class WebSocketListener:
    """24/7 WebSocket listener for real-time message notifications."""

    def __init__(
        self,
        creator_id: str,
        creator_name: str,
        db_url: str,
        redis_host: str = 'redis',
        redis_port: int = 6385
    ):
        """Initialize WebSocket listener.

        :param creator_id: Creator's OnlyFans ID
        :param creator_name: Creator's display name
        :param db_url: PostgreSQL connection string
        :param redis_host: Redis hostname
        :param redis_port: Redis port
        """
        self.creator_id = str(creator_id)
        self.creator_name = creator_name
        self.db_url = db_url
        self.redis_host = redis_host
        self.redis_port = redis_port

        self.logger = setup_logger(f'websocket_{creator_name}', log_type='websocket')
        self.api = None
        self.authed = None
        self.cutoff_manager: Optional[CutoffManager] = None
        self.incremental_fetcher: Optional[IncrementalFetcher] = None
        self.redis_producer: Optional[RedisProducer] = None

    async def initialize(self) -> bool:
        """Initialize all components.

        :return: True if successful, False otherwise
        """
        try:
            self.logger.info(f"Initializing WebSocket listener for {self.creator_name}...")

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

            self.incremental_fetcher = IncrementalFetcher(
                cutoff_manager=self.cutoff_manager,
                logger=self.logger
            )

            self.logger.info("✓ All components initialized")
            return True

        except Exception as e:
            self.logger.error(f"✗ Initialization failed: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    async def process_message_event(self, event: dict):
        """Process a new message event from WebSocket.

        :param event: WebSocket event dictionary
        """
        try:
            # Log raw event to understand structure
            self.logger.debug(f"Raw WebSocket event: {event}")

            # Parse the data field which contains raw JSON string
            import json
            raw_data = event.get('data')
            if raw_data and isinstance(raw_data, str):
                try:
                    parsed_data = json.loads(raw_data)
                    self.logger.info(f"✓ Parsed data keys: {parsed_data.keys() if isinstance(parsed_data, dict) else type(parsed_data)}")

                    # Log fromUser and toUser for api2_chat_message events
                    if isinstance(parsed_data, dict) and 'api2_chat_message' in parsed_data:
                        message_data = parsed_data['api2_chat_message']
                        from_user = message_data.get('fromUser', {})
                        to_user = message_data.get('toUser', {})
                        from_id = from_user.get('id') if isinstance(from_user, dict) else None
                        to_id = to_user.get('id') if isinstance(to_user, dict) else None
                        self.logger.info(f"📧 fromUser.id={from_id}, toUser.id={to_id}")

                    # Extract fan_id from OnlyFans WebSocket message format
                    fan_id = None
                    if isinstance(parsed_data, dict):
                        # Check for api2_chat_message format
                        if 'api2_chat_message' in parsed_data:
                            message_data = parsed_data['api2_chat_message']
                            if isinstance(message_data, dict):
                                from_user = message_data.get('fromUser', {})
                                to_user = message_data.get('toUser', {})

                                from_user_id = str(from_user.get('id')) if isinstance(from_user, dict) and from_user.get('id') else None
                                to_user_id = str(to_user.get('id')) if isinstance(to_user, dict) and to_user.get('id') else None

                                # Determine which user is the fan (not the creator)
                                # Case 1: fromUser has ID and it's not the creator = fan sent message
                                if from_user_id and from_user_id != self.creator_id:
                                    fan_id = from_user_id
                                    self.logger.info(f"✓ Message FROM fan {fan_id} (fan→creator)")
                                # Case 2: fromUser is None/creator, toUser has ID = creator sent message to fan
                                elif to_user_id and to_user_id != self.creator_id:
                                    fan_id = to_user_id
                                    self.logger.info(f"✓ Message TO fan {fan_id} (creator→fan)")
                                # Case 3: Both are creator = ignore
                                elif from_user_id == self.creator_id and to_user_id == self.creator_id:
                                    self.logger.debug(f"Ignoring message from creator to creator (self)")
                                    return
                                else:
                                    self.logger.debug(f"Ignoring non-message event")

                        # Fallback to other formats
                        if not fan_id:
                            fan_id = (parsed_data.get('from_user_id') or
                                     parsed_data.get('fromUserId') or
                                     parsed_data.get('fromUser', {}).get('id') if isinstance(parsed_data.get('fromUser'), dict) else None or
                                     parsed_data.get('user_id') or
                                     parsed_data.get('userId'))

                            if fan_id and str(fan_id) == self.creator_id:
                                # Try to get the other participant
                                to_user_id = (parsed_data.get('to_user_id') or
                                             parsed_data.get('toUserId') or
                                             parsed_data.get('toUser', {}).get('id') if isinstance(parsed_data.get('toUser'), dict) else None)
                                if to_user_id and str(to_user_id) != self.creator_id:
                                    fan_id = str(to_user_id)
                                    self.logger.info(f"✓ Creator sent message to fan {fan_id}")
                                else:
                                    self.logger.debug(f"Ignoring creator self-message")
                                    return

                        if not fan_id:
                            # Not a message event, ignore it
                            self.logger.debug(f"Ignoring non-message event with keys: {list(parsed_data.keys())[:20]}")
                except json.JSONDecodeError as e:
                    self.logger.warning(f"Failed to parse WebSocket data: {str(e)}")
                    fan_id = None
            else:
                # Fallback to old method
                fan_id = str(event.get('from_user_id') or event.get('fromUser', {}).get('id') or '')

            if not fan_id or fan_id == 'None':
                return

            self.logger.info(f"📨 New message from fan {fan_id}")

            # Process this fan's messages in the background to avoid blocking other notifications
            import asyncio
            asyncio.create_task(self._fetch_and_process_messages(fan_id))

        except Exception as e:
            self.logger.error(f"✗ Error processing message event: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())

    async def _fetch_and_process_messages(self, fan_id: str):
        """Fetch and process messages for a fan in the background.

        :param fan_id: Fan's OnlyFans ID
        """
        try:
            # Check if this is a new fan (not in database)
            cutoff_id = await self.cutoff_manager.get_cutoff_id(self.creator_id, fan_id)

            if cutoff_id is None:
                # NEW FAN DETECTED - Route to priority queue for full history fetch
                self.logger.info(f"🆕 NEW FAN DETECTED: {fan_id} - Adding to priority queue")

                # Push to Redis priority queue (LPUSH = add to front)
                await self.redis_producer.redis_client.lpush(
                    f"of:{self.creator_id}:new_fans_priority",
                    fan_id
                )

                self.logger.info(f"✓ Fan {fan_id} queued for priority processing (full history fetch)")
                return  # Don't fetch here, let new_fan_processor handle it

            # KNOWN FAN - Fetch only new messages (incremental)
            self.logger.debug(f"Known fan {fan_id}, fetching incremental messages (cutoff_id={cutoff_id})")

            user = await self.authed.get_user(fan_id)
            if not user:
                self.logger.warning(f"⚠️ Could not get user object for fan {fan_id}")
                return

            messages = await self.incremental_fetcher.fetch_new_messages(
                user=user,
                model_id=self.creator_id,
                fan_id=fan_id,
                authed=self.authed,
                limit=20
            )

            if not messages:
                self.logger.debug(f"No new messages fetched for fan {fan_id}")
                return

            from datetime import datetime
            from modules.bundle_processor import is_bundle

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

                await self.redis_producer.push_message(self.creator_id, message_dict)

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

            self.logger.info(f"✓ Processed {len(messages)} new message(s) from fan {fan_id}")

        except Exception as e:
            self.logger.error(f"✗ Error fetching/processing messages for fan {fan_id}: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())

    async def listen(self):
        """Start WebSocket listener (24/7 operation)."""
        self.logger.info("=" * 70)
        self.logger.info(f"WEBSOCKET LISTENER - {self.creator_name}")
        self.logger.info("=" * 70)

        try:
            await self.authed.listen()
            self.logger.info("✓ WebSocket connection started")

            event_queue = self.authed.subscribe(max_queue_size=1000)
            self.logger.info("✓ Subscribed to event queue (max 1000 events)")

            self.logger.info("🎧 Listening for real-time events...")

            while True:
                try:
                    event = await asyncio.wait_for(event_queue.get(), timeout=30.0)

                    event_type = event.get('type')
                    if event_type == 'message':
                        await self.process_message_event(event)
                    else:
                        self.logger.debug(f"Received event type: {event_type}")

                except asyncio.TimeoutError:
                    self.logger.debug("No events received in last 30 seconds (normal)")
                    continue

        except KeyboardInterrupt:
            self.logger.info("\n⚠️ Interrupted by user")
        except Exception as e:
            self.logger.error(f"✗ WebSocket listener error: {str(e)}")
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
    """Main entry point for WebSocket listener."""
    creator_id = os.getenv('CREATOR_ID')
    creator_name = os.getenv('CREATOR_NAME', creator_id)
    db_url = os.getenv('DATABASE_URL')
    redis_host = os.getenv('REDIS_HOST', 'redis')
    redis_port = int(os.getenv('REDIS_PORT', '6385'))

    if not creator_id:
        print("✗ CREATOR_ID environment variable not set")
        sys.exit(1)

    if not db_url:
        print("✗ DATABASE_URL environment variable not set")
        sys.exit(1)

    listener = WebSocketListener(
        creator_id=creator_id,
        creator_name=creator_name,
        db_url=db_url,
        redis_host=redis_host,
        redis_port=redis_port
    )

    if not await listener.initialize():
        print("=" * 70)
        print("✗ FATAL: Failed to initialize WebSocket listener")
        print("✗ Authentication or component initialization failed")
        print("✗ Container will exit now")
        print("=" * 70)
        sys.exit(1)

    await listener.listen()


if __name__ == "__main__":
    asyncio.run(main())
