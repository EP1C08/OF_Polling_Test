"""WebSocket Listener - 24/7 Real-Time Event Detection.

Listens to OnlyFans WebSocket for instant message notifications.
When a new message event is received:
1. Query database to check if fan exists (has messages in messages_new table)
2. Route to appropriate Redis queue:
   - NEW fan (not in DB) → new_fans_priority queue → new_fan_processor
   - KNOWN fan (in DB) → known_fans_queue → known_fan_processor

This listener is EVENT-ONLY - it does NOT fetch messages.
Message fetching is delegated to the processor containers.

One listener per creator, runs continuously.
"""

import asyncio
import json
import os
import sys
from typing import Optional
from modules.logger import setup_logger
from modules.authentication import load_auth, create_api_helper
from modules.cutoff_manager import CutoffManager
from modules.redis_producer import RedisProducer


class WebSocketListener:
    """24/7 WebSocket listener for real-time event detection (event-only, no fetching)."""

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

            self.redis_producer = RedisProducer(
                redis_host=self.redis_host,
                redis_port=self.redis_port,
                redis_db=0
            )
            await self.redis_producer.connect()
            self.logger.info("Connected to Redis")

            self.cutoff_manager = CutoffManager(db_url=self.db_url, logger=self.logger)
            if not await self.cutoff_manager.initialize():
                self.logger.error("Failed to initialize cutoff manager")
                return False
            self.logger.info("Cutoff manager initialized")

            self.logger.info("All components initialized")
            return True

        except Exception as e:
            self.logger.error(f"Initialization failed: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    async def process_message_event(self, event: dict):
        """Process a new message event from WebSocket.

        :param event: WebSocket event dictionary
        """
        try:
            self.logger.debug(f"Raw WebSocket event: {event}")

            raw_data = event.get('data')
            if raw_data and isinstance(raw_data, str):
                try:
                    parsed_data = json.loads(raw_data)
                    self.logger.debug(
                        f"Parsed data keys: {parsed_data.keys() if isinstance(parsed_data, dict) else type(parsed_data)}"
                    )

                    if isinstance(parsed_data, dict) and 'api2_chat_message' in parsed_data:
                        message_data = parsed_data['api2_chat_message']
                        from_user = message_data.get('fromUser', {})
                        to_user = message_data.get('toUser', {})
                        from_id = from_user.get('id') if isinstance(from_user, dict) else None
                        to_id = to_user.get('id') if isinstance(to_user, dict) else None
                        self.logger.info(f"fromUser.id={from_id}, toUser.id={to_id}")

                    fan_id = None
                    if isinstance(parsed_data, dict):
                        if 'api2_chat_message' in parsed_data:
                            message_data = parsed_data['api2_chat_message']
                            if isinstance(message_data, dict):
                                from_user = message_data.get('fromUser', {})
                                to_user = message_data.get('toUser', {})

                                from_user_id = str(from_user.get('id')) if isinstance(from_user, dict) and from_user.get('id') else None
                                to_user_id = str(to_user.get('id')) if isinstance(to_user, dict) and to_user.get('id') else None

                                if from_user_id and from_user_id != self.creator_id:
                                    fan_id = from_user_id
                                    self.logger.info(f"Message FROM fan {fan_id} (fan->creator)")
                                elif to_user_id and to_user_id != self.creator_id:
                                    fan_id = to_user_id
                                    self.logger.info(f"Message TO fan {fan_id} (creator->fan)")
                                elif from_user_id == self.creator_id and to_user_id == self.creator_id:
                                    self.logger.debug("Ignoring message from creator to creator (self)")
                                    return
                                else:
                                    self.logger.debug("Ignoring non-message event")

                        if not fan_id:
                            fan_id = (parsed_data.get('from_user_id') or
                                     parsed_data.get('fromUserId') or
                                     parsed_data.get('fromUser', {}).get('id') if isinstance(parsed_data.get('fromUser'), dict) else None or
                                     parsed_data.get('user_id') or
                                     parsed_data.get('userId'))

                            if fan_id and str(fan_id) == self.creator_id:
                                to_user_id = (parsed_data.get('to_user_id') or
                                             parsed_data.get('toUserId') or
                                             parsed_data.get('toUser', {}).get('id') if isinstance(parsed_data.get('toUser'), dict) else None)
                                if to_user_id and str(to_user_id) != self.creator_id:
                                    fan_id = str(to_user_id)
                                    self.logger.info(f"Creator sent message to fan {fan_id}")
                                else:
                                    self.logger.debug("Ignoring creator self-message")
                                    return

                        if not fan_id:
                            self.logger.debug(f"Ignoring non-message event with keys: {list(parsed_data.keys())[:20]}")
                except json.JSONDecodeError as e:
                    self.logger.warning(f"Failed to parse WebSocket data: {str(e)}")
                    fan_id = None
            else:
                fan_id = str(event.get('from_user_id') or event.get('fromUser', {}).get('id') or '')

            if not fan_id or fan_id == 'None':
                return

            self.logger.info(f"New message event for fan {fan_id}")

            asyncio.create_task(self._route_fan_to_queue(fan_id))

        except Exception as e:
            self.logger.error(f"Error processing message event: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())

    async def _route_fan_to_queue(self, fan_id: str):
        """Route fan to appropriate queue based on database check.

        Uses Redis lock to prevent duplicate queue pushes for same fan.

        :param fan_id: Fan's OnlyFans ID
        """
        lock_key = f"of:{self.creator_id}:processing_lock:{fan_id}"
        lock_ttl = 60

        try:
            lock_acquired = await self.redis_producer.redis.set(
                lock_key,
                "1",
                ex=lock_ttl,
                nx=True
            )

            if not lock_acquired:
                self.logger.debug(f"Fan {fan_id} already being processed, skipping duplicate event")
                return

            self.logger.debug(f"Acquired processing lock for fan {fan_id}")

            cutoff_id = await self.cutoff_manager.get_cutoff_id(self.creator_id, fan_id)

            if cutoff_id is None:
                await self.redis_producer.redis.lpush(
                    f"of:{self.creator_id}:new_fans_priority",
                    fan_id
                )
                self.logger.info(f"NEW FAN {fan_id} -> new_fans_priority queue")
            else:
                await self.redis_producer.redis.lpush(
                    f"of:{self.creator_id}:known_fans_queue",
                    fan_id
                )
                self.logger.info(f"KNOWN FAN {fan_id} -> known_fans_queue")

        except Exception as e:
            self.logger.error(f"Error routing fan {fan_id} to queue: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
        finally:
            try:
                await self.redis_producer.redis.delete(lock_key)
                self.logger.debug(f"Released processing lock for fan {fan_id}")
            except Exception as e:
                self.logger.debug(f"Could not release lock for fan {fan_id}: {str(e)}")

    async def listen(self):
        """Start WebSocket listener (24/7 operation)."""
        self.logger.info("=" * 70)
        self.logger.info(f"WEBSOCKET LISTENER (EVENT-ONLY) - {self.creator_name}")
        self.logger.info("=" * 70)

        try:
            await self.authed.listen()
            self.logger.info("WebSocket connection started")

            event_queue = self.authed.subscribe(max_queue_size=1000)
            self.logger.info("Subscribed to event queue (max 1000 events)")

            self.logger.info("Listening for real-time events...")

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
            self.logger.info("\nInterrupted by user")
        except Exception as e:
            self.logger.error(f"WebSocket listener error: {str(e)}")
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

        if self.redis_producer:
            try:
                await self.redis_producer.close()
                self.logger.info("Redis connection closed")
            except Exception as e:
                self.logger.warning(f"Error closing Redis: {str(e)}")

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
    """Main entry point for WebSocket listener."""
    creator_id = os.getenv('CREATOR_ID')
    creator_name = os.getenv('CREATOR_NAME', creator_id)
    db_url = os.getenv('DATABASE_URL')
    redis_host = os.getenv('REDIS_HOST', 'redis')
    redis_port = int(os.getenv('REDIS_PORT', '6385'))

    if not creator_id:
        print("CREATOR_ID environment variable not set")
        sys.exit(1)

    if not db_url:
        print("DATABASE_URL environment variable not set")
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
        print("FATAL: Failed to initialize WebSocket listener")
        print("Authentication or component initialization failed")
        print("Container will exit now")
        print("=" * 70)
        sys.exit(1)

    await listener.listen()


if __name__ == "__main__":
    asyncio.run(main())
