"""Pending Fan Processor - Processes fans after 24h waiting period.

Continuously monitors the pending_fans/{creator}.json file and processes
fans whose check_after time has passed. For each ready fan:
1. Route to appropriate queue (new_fans_priority or known_fans_queue)
2. Remove fan from pending list

IMPORTANT: Timer NEVER resets. Once check_after time passes, fan is ALWAYS
processed and removed. This guarantees fetch happens exactly 24h after
first message, even if fan keeps chatting.

One processor per creator, runs 24/7 alongside WebSocket listeners.
"""

import asyncio
import os
import sys
from datetime import datetime, timezone
from typing import Optional
from modules.logger import setup_logger
from modules.authentication import create_api_helper
from modules.db_credential_loader import load_credentials_from_db
from modules.cutoff_manager import CutoffManager
from modules.redis_producer import RedisProducer
from modules.pending_fans_manager import PendingFansManager
from modules.timewaster import TimewasterHandler
from ultima_scraper_api.apis.onlyfans.classes.extras import AuthDetails

# Creators that use GALEN_API_TOKEN (different GoLogin account)
GALEN_CREATORS = ["Juno", "avabarham2", "Luna", "luna"]


def get_gologin_token(creator_name: str) -> str:
    """Get the correct GoLogin API token for a creator.

    :param creator_name: Creator's username/name.
    :return: GoLogin API token string.
    """
    if creator_name in GALEN_CREATORS:
        return os.getenv("GALEN_API_TOKEN")
    return os.getenv("GOLOGIN_API_TOKEN")


class PendingFanProcessor:
    """Processor for fans waiting in the 24h pending queue."""

    def __init__(
        self,
        creator_id: str,
        creator_name: str,
        db_url: str,
        redis_host: str = 'redis',
        redis_port: int = 6385,
        check_interval: int = 300
    ):
        """Initialize pending fan processor.

        :param creator_id: Creator's OnlyFans ID.
        :param creator_name: Creator's display name.
        :param db_url: PostgreSQL connection string.
        :param redis_host: Redis hostname.
        :param redis_port: Redis port.
        :param check_interval: Seconds between checks (default 300 = 5 min).
        """
        self.creator_id = str(creator_id)
        self.creator_name = creator_name
        self.db_url = db_url
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.check_interval = check_interval

        self.logger = setup_logger(
            f'pending_fan_processor_{creator_name}',
            log_type='pending_fan_processor'
        )
        self.api = None
        self.authed = None
        self.cutoff_manager: Optional[CutoffManager] = None
        self.redis_producer: Optional[RedisProducer] = None
        self.pending_fans_manager: Optional[PendingFansManager] = None
        self.timewaster_handler: Optional[TimewasterHandler] = None
        self.gologin_profile_id: Optional[str] = None

    async def initialize(self) -> bool:
        """Initialize all components.

        :return: True if successful, False otherwise.
        """
        try:
            self.logger.info(f"Initializing pending fan processor for {self.creator_name}...")

            # Load credentials from database
            credentials = await load_credentials_from_db(model_id=self.creator_id)
            if not credentials:
                self.logger.error("=" * 70)
                self.logger.error("AUTHENTICATION FAILED: Unable to load credentials")
                self.logger.error(f"Creator ID: {self.creator_id}")
                self.logger.error(f"Creator Name: {self.creator_name}")
                self.logger.error("Check creator_credentials table for this creator")
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
            gologin_token = get_gologin_token(self.creator_name)
            self.api, self.authed = await create_api_helper(
                auth_details,
                self.logger,
                gologin_profile_id=self.gologin_profile_id,
                gologin_api_token=gologin_token
            )
            if not self.authed:
                self.logger.error("=" * 70)
                self.logger.error("AUTHENTICATION FAILED: Unable to authenticate with OnlyFans")
                self.logger.error(f"Creator: {self.creator_name} (ID: {self.creator_id})")
                self.logger.error("=" * 70)
                return False

            self.logger.info("Authenticated with OnlyFans API")

            # Initialize timewaster handler
            tw_enabled = os.getenv('TW_ENABLED', 'true').lower() == 'true'
            if tw_enabled:
                self.timewaster_handler = TimewasterHandler(
                    authed=self.authed,
                    model_id=self.creator_id,
                    model_name=self.creator_name,
                    logger=self.logger
                )
                self.logger.info("Timewaster handler initialized")
            else:
                self.logger.info("Timewaster checking disabled (TW_ENABLED=false)")

            # Initialize Redis
            self.redis_producer = RedisProducer(
                redis_host=self.redis_host,
                redis_port=self.redis_port,
                redis_db=0
            )
            await self.redis_producer.connect()
            self.logger.info("Connected to Redis")

            # Initialize cutoff manager
            self.cutoff_manager = CutoffManager(db_url=self.db_url, logger=self.logger)
            if not await self.cutoff_manager.initialize():
                self.logger.error("Failed to initialize cutoff manager")
                return False
            self.logger.info("Cutoff manager initialized")

            # Initialize pending fans manager
            min_age_hours = int(os.getenv('MIN_MESSAGE_AGE_HOURS', '24'))
            self.pending_fans_manager = PendingFansManager(
                creator_id=self.creator_id,
                creator_name=self.creator_name,
                pending_dir='/app/pending_fans',
                min_age_hours=min_age_hours,
                logger=self.logger
            )
            self.logger.info(f"Pending fans manager initialized (min_age: {min_age_hours}h)")

            self.logger.info("All components initialized")
            return True

        except Exception as e:
            self.logger.error(f"Initialization failed: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    async def process_pending_fan(self, fan_id: str, fan_data: dict) -> bool:
        """Process a single pending fan.

        Timer NEVER resets - once check_after passes, fan is ALWAYS processed.
        This guarantees fetch happens exactly 24h after first message.

        :param fan_id: Fan's OnlyFans ID.
        :param fan_data: Fan data from pending list.
        :return: True if processed successfully, False otherwise.
        """
        try:
            first_msg = fan_data.get('first_message_at', 'unknown')
            last_msg = fan_data.get('last_message_at', 'unknown')
            self.logger.info(
                f"Processing pending fan {fan_id} "
                f"(first_msg: {first_msg}, last_msg: {last_msg})"
            )

            # Route to queue and remove from pending list
            # No timer reset - guaranteed fetch after 24h from first message
            await self._route_fan_to_queue(fan_id)
            self.pending_fans_manager.remove_fan(fan_id)
            return True

        except Exception as e:
            self.logger.error(f"Error processing pending fan {fan_id}: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False

    async def _route_fan_to_queue(self, fan_id: str):
        """Route fan to appropriate Redis queue.

        :param fan_id: Fan's OnlyFans ID.
        """
        try:
            # Check timewaster status first
            if self.timewaster_handler:
                try:
                    analysis = await self.timewaster_handler.check_and_mark_timewaster(fan_id)
                    if analysis and analysis.is_timewaster:
                        self.logger.info(
                            f"TIMEWASTER SKIPPED: Fan {fan_id} - "
                            f"${analysis.total_spend:.2f} spend, "
                            f"{analysis.total_messages} msgs, "
                            f"${analysis.rpm:.4f} RPM"
                        )
                        return
                except Exception as tw_error:
                    self.logger.warning(
                        f"Timewaster check failed for {fan_id}, continuing: {str(tw_error)}"
                    )

            # Check if fan is new or known
            cutoff_id = await self.cutoff_manager.get_cutoff_id(self.creator_id, fan_id)

            if cutoff_id is None:
                # New fan - route to new_fans_priority queue
                await self.redis_producer.redis.lpush(
                    f"of:{self.creator_id}:new_fans_priority",
                    fan_id
                )
                self.logger.info(f"NEW FAN {fan_id} -> new_fans_priority queue")
            else:
                # Known fan - route to known_fans_queue
                await self.redis_producer.redis.lpush(
                    f"of:{self.creator_id}:known_fans_queue",
                    fan_id
                )
                self.logger.info(f"KNOWN FAN {fan_id} -> known_fans_queue")

        except Exception as e:
            self.logger.error(f"Error routing fan {fan_id} to queue: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())

    async def run(self):
        """Run pending fan processor continuously."""
        self.logger.info("=" * 70)
        self.logger.info(f"PENDING FAN PROCESSOR - {self.creator_name}")
        self.logger.info(f"Check interval: {self.check_interval} seconds")
        self.logger.info(f"Pending file: /app/pending_fans/{self.creator_name}.json")
        self.logger.info("=" * 70)

        try:
            while True:
                try:
                    # Get fans ready to process
                    ready_fans = self.pending_fans_manager.get_fans_ready_to_process()
                    stats = self.pending_fans_manager.get_stats()

                    if ready_fans:
                        self.logger.info(
                            f"Found {len(ready_fans)} fan(s) ready to process "
                            f"(Total: {stats['total']}, Waiting: {stats['waiting']})"
                        )

                        processed_count = 0

                        for fan_id, fan_data in ready_fans:
                            result = await self.process_pending_fan(fan_id, fan_data)
                            if result:
                                processed_count += 1

                            # Small delay between processing fans
                            await asyncio.sleep(1)

                        self.logger.info(f"Batch complete: {processed_count} fan(s) routed to queue")
                    else:
                        self.logger.info(
                            f"No fans ready to process "
                            f"(Total: {stats['total']}, Waiting: {stats['waiting']})"
                        )

                    # Wait before next check
                    self.logger.info(f"Sleeping {self.check_interval} seconds...")
                    await asyncio.sleep(self.check_interval)

                except asyncio.CancelledError:
                    self.logger.info("Processor cancelled")
                    break
                except Exception as e:
                    self.logger.error(f"Error in main loop: {str(e)}")
                    import traceback
                    self.logger.error(traceback.format_exc())
                    await asyncio.sleep(60)

        except KeyboardInterrupt:
            self.logger.info("\nInterrupted by user")
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
    """Main entry point for pending fan processor."""
    creator_id = os.getenv('CREATOR_ID')
    creator_name = os.getenv('CREATOR_NAME', creator_id)
    db_url = os.getenv('DATABASE_URL')
    redis_host = os.getenv('REDIS_HOST', 'redis')
    redis_port = int(os.getenv('REDIS_PORT', '6385'))
    check_interval = int(os.getenv('PENDING_CHECK_INTERVAL', '300'))

    if not creator_id:
        print("CREATOR_ID environment variable not set")
        sys.exit(1)

    if not db_url:
        print("DATABASE_URL environment variable not set")
        sys.exit(1)

    processor = PendingFanProcessor(
        creator_id=creator_id,
        creator_name=creator_name,
        db_url=db_url,
        redis_host=redis_host,
        redis_port=redis_port,
        check_interval=check_interval
    )

    if not await processor.initialize():
        print("=" * 70)
        print("FATAL: Failed to initialize pending fan processor")
        print("Authentication or component initialization failed")
        print("Container will exit now")
        print("=" * 70)
        sys.exit(1)

    await processor.run()


if __name__ == "__main__":
    asyncio.run(main())
