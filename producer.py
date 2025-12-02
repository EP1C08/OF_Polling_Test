"""Producer Script - Fetches messages and pushes to Redis Streams.

Run one instance per creator.

Features:
- Concurrent fan processing (3 fans at once by default)
- Rate limit detection and retry with exponential backoff
- Heavy fan handling (timeout deferral)
- Thread-safe checkpoint management
"""

import asyncio
import os
import sys
import time
import gc
from modules.logger import setup_logger
import json
from modules.authentication import load_auth_credentials, authenticate_account
from modules.message_fetcher import fetch_all_messages, fetch_all_messages_fast, process_messages_and_bundles
from modules.redis_producer import RedisProducer
from modules.conversation_loader import load_conversations_from_json, create_user_object_from_json
from modules.checkpoint import CheckpointManager
from ultima_scraper_api import OnlyFansAPI


async def process_single_fan(
    fan_user,
    authed,
    checkpoint,
    producer,
    creator_id_str: str,
    creator_username: str,
    fetch_timeout: int,
    logger,
    semaphore,
) -> dict:
    """Process a single fan with concurrent-safe error handling.

    :param fan_user: User object to fetch messages from
    :param authed: Authenticated API object
    :param checkpoint: CheckpointManager instance
    :param producer: RedisProducer instance
    :param creator_id_str: Creator's ID as string
    :param creator_username: Creator's username
    :param fetch_timeout: Timeout in seconds for fetching
    :param logger: Logger instance
    :param semaphore: asyncio.Semaphore for concurrency control
    :return: Dict with status: 'success', 'timeout', 'rate_limit', or 'error'
    """
    async with semaphore:
        checkpoint.mark_in_progress(fan_user.id)
        fan_start_time = time.time()

        try:
            # Fetch messages using fast fetcher (2.5x faster with 50-msg batches + retry)
            # NO TIMEOUT - let it complete naturally (fast fetcher has built-in retry logic)
            messages = await fetch_all_messages_fast(fan_user, authed, logger=logger)

            if not messages:
                logger.info(f"  No messages found for {fan_user.username}")
                checkpoint.mark_completed(fan_user.id)
                return {'status': 'empty', 'fan_id': fan_user.id, 'fan_username': fan_user.username}

            fan_duration = time.time() - fan_start_time
            logger.info(f"  ✓ {fan_user.username}: Fetched {len(messages)} messages in {fan_duration:.2f}s")

            # Process messages and bundles
            messages_data, bundles_data, bundle_items_data, interactions_data, analytics_data = \
                await process_messages_and_bundles(
                    messages,
                    creator_id_str,
                    creator_username,
                    str(fan_user.id),
                    authed
                )

            # Push to Redis streams
            if messages_data:
                await producer.push_messages_batch(creator_id_str, messages_data)
            if bundles_data:
                for bundle in bundles_data:
                    await producer.push_bundle(creator_id_str, bundle)
            if bundle_items_data:
                await producer.push_bundle_items(creator_id_str, bundle_items_data)
            if interactions_data:
                await producer.push_fan_interactions(creator_id_str, interactions_data)
            if analytics_data:
                await producer.push_analytics(creator_id_str, analytics_data)

            # Clear processed data from memory immediately
            del messages_data, bundles_data, bundle_items_data, interactions_data, analytics_data, messages

            # Mark fan as completed
            checkpoint.mark_completed(fan_user.id)

            total_duration = time.time() - fan_start_time
            return {
                'status': 'success',
                'fan_id': fan_user.id,
                'fan_username': fan_user.username,
                'elapsed': total_duration
            }

        except Exception as e:
            error_msg = str(e)
            fan_duration = time.time() - fan_start_time

            # Detect rate limiting
            if 'rate limit' in error_msg.lower() or '429' in error_msg or 'too many requests' in error_msg.lower():
                logger.warning(f"  ⚠️ {fan_user.username}: Rate limit detected - Adding to retry queue")
                return {
                    'status': 'rate_limit',
                    'fan_id': fan_user.id,
                    'fan_username': fan_user.username,
                    'error': error_msg,
                    'elapsed': fan_duration
                }

            # Other errors
            logger.error(f"  ✗ {fan_user.username}: Error - {error_msg} (failed after {fan_duration:.2f}s)")
            # Error tracking disabled (push_error removed with Stream conversion)
            return {
                'status': 'error',
                'fan_id': fan_user.id,
                'fan_username': fan_user.username,
                'error': error_msg,
                'elapsed': fan_duration
            }


async def retry_rate_limited_fan(
    fan_data: dict,
    authed,
    checkpoint,
    producer,
    creator_id_str: str,
    creator_username: str,
    fetch_timeout: int,
    logger,
    max_retries: int = 3,
) -> dict:
    """Retry a rate-limited fan with exponential backoff.

    :param fan_data: Dict with fan_id, fan_username, retry_count
    :param authed: Authenticated API object
    :param checkpoint: CheckpointManager instance
    :param producer: RedisProducer instance
    :param creator_id_str: Creator's ID as string
    :param creator_username: Creator's username
    :param fetch_timeout: Timeout in seconds
    :param logger: Logger instance
    :param max_retries: Maximum retry attempts (default 3)
    :return: Dict with status: 'success', 'rate_limit', or 'failed'
    """
    fan_id = fan_data['fan_id']
    fan_username = fan_data['fan_username']
    retry_count = fan_data.get('retry_count', 0)

    if retry_count >= max_retries:
        # Max retries exceeded - mark as permanently failed
        error = f"Rate limit persisted after {max_retries} retries"
        logger.error(f"  ✗ {fan_username} (ID: {fan_id}): {error}")
        checkpoint.mark_permanently_failed(fan_id, fan_username, error)
        return {'status': 'failed', 'fan_id': fan_id, 'fan_username': fan_username}

    # Calculate exponential backoff: 30s, 60s, 120s
    backoff_delay = 30 * (2 ** retry_count)
    logger.info(f"  🔄 {fan_username}: Retry attempt {retry_count + 1}/{max_retries} after {backoff_delay}s backoff...")
    await asyncio.sleep(backoff_delay)

    # Get user object
    try:
        user_obj = await authed.get_user(int(fan_id))
        if not user_obj:
            error = "User not found"
            logger.error(f"  ✗ {fan_username}: {error}")
            checkpoint.mark_permanently_failed(fan_id, fan_username, error)
            return {'status': 'failed', 'fan_id': fan_id, 'fan_username': fan_username}
    except Exception as e:
        error_msg = str(e)
        logger.error(f"  ✗ {fan_username}: Could not get user object - {error_msg}")
        checkpoint.mark_rate_limited(fan_id, fan_username, retry_count + 1)
        return {'status': 'rate_limit', 'fan_id': fan_id, 'fan_username': fan_username}

    # Try fetching again using fast fetcher (no timeout)
    try:
        messages = await fetch_all_messages_fast(user_obj, authed, logger=logger)

        if not messages:
            logger.info(f"  No messages found for {fan_username}")
            checkpoint.mark_completed(fan_id)
            return {'status': 'success', 'fan_id': fan_id, 'fan_username': fan_username}

        logger.info(f"  ✓ {fan_username}: Fetched {len(messages)} messages")

        # Process and push to Redis
        messages_data, bundles_data, bundle_items_data, interactions_data, analytics_data = \
            await process_messages_and_bundles(messages, creator_id_str, creator_username, fan_id, authed)

        if messages_data:
            await producer.push_messages_batch(creator_id_str, messages_data)
        if bundles_data:
            for bundle in bundles_data:
                await producer.push_bundle(creator_id_str, bundle)
        if bundle_items_data:
            await producer.push_bundle_items(creator_id_str, bundle_items_data)
        if interactions_data:
            await producer.push_fan_interactions(creator_id_str, interactions_data)
        if analytics_data:
            await producer.push_analytics(creator_id_str, analytics_data)

        # Cleanup memory
        del messages_data, bundles_data, bundle_items_data, interactions_data, analytics_data, messages

        # Mark completed
        checkpoint.mark_completed(fan_id)
        logger.info(f"  ✓ {fan_username}: Retry successful!")
        return {'status': 'success', 'fan_id': fan_id, 'fan_username': fan_username}

    except Exception as e:
        error_msg = str(e)

        if 'rate limit' in error_msg.lower() or '429' in error_msg or 'too many requests' in error_msg.lower():
            # Still rate limited - increment retry count
            logger.warning(f"  ⚠️ {fan_username}: Still rate limited (attempt {retry_count + 1}/{max_retries})")
            checkpoint.mark_rate_limited(fan_id, fan_username, retry_count + 1)
            return {'status': 'rate_limit', 'fan_id': fan_id, 'fan_username': fan_username}
        else:
            # Different error
            logger.error(f"  ✗ {fan_username}: Error during retry - {error_msg}")
            # Error tracking disabled (push_error removed with Stream conversion)
            checkpoint.mark_rate_limited(fan_id, fan_username, retry_count + 1)
            return {'status': 'error', 'fan_id': fan_id, 'fan_username': fan_username}


async def main() -> None:
    """Main function to run producer for a single creator."""
    creator_id = os.getenv('CREATOR_ID')
    if not creator_id:
        print("✗ CREATOR_ID environment variable not set")
        sys.exit(1)

    # Get creator name for logging
    creator_name = os.getenv('CREATOR_NAME', creator_id)

    # Setup logging
    logger = setup_logger(creator_name, log_type="producer")

    # Get configuration from environment
    redis_host = os.getenv('REDIS_HOST', 'redis')
    redis_port = int(os.getenv('REDIS_PORT', '6385'))
    reauth_interval = int(os.getenv('REAUTH_INTERVAL', '100'))  # Default: every 100 fans
    fan_delay = int(os.getenv('FAN_DELAY', '5'))  # Default: 5 seconds between batches
    fetch_timeout = int(os.getenv('FETCH_TIMEOUT', '600'))  # Default: 10 minutes
    concurrent_fans = int(os.getenv('CONCURRENT_FANS', '3'))  # Default: 3 fans at once

    logger.info("=" * 60)
    logger.info(f"OnlyFans Message Producer")
    logger.info("=" * 60)
    logger.info(f"Creator: {creator_name} (ID: {creator_id})")
    logger.info(f"Redis: {redis_host}:{redis_port}")
    logger.info(f"Mode: Fast Fetcher (50-msg batches, auto-retry, NO TIMEOUT)")
    logger.info(f"Concurrent fans: {concurrent_fans} workers (rolling window)")
    logger.info(f"Reauth interval: Every {reauth_interval} fans")
    logger.info(f"Progress logging: Every 20 fans")
    logger.info("=" * 60)

    # Load credentials for this specific creator
    auth_file = os.getenv('AUTH_FILE', 'auth_multi.json')
    try:
        all_auth_details = await load_auth_credentials(auth_file)
    except Exception as e:
        logger.error(f"✗ Failed to load auth credentials: {str(e)}")
        sys.exit(1)

    # Find auth details for this creator
    auth_details = None
    for auth in all_auth_details:
        if str(auth.id) == creator_id or auth.username == creator_id:
            auth_details = auth
            break

    if not auth_details:
        logger.error(f"✗ No auth credentials found for creator {creator_id}")
        sys.exit(1)

    # Connect to Redis
    producer = RedisProducer(redis_host=redis_host, redis_port=redis_port)

    try:
        await producer.connect()

        # Authenticate creator account
        logger.info(f"\nAuthenticating creator: {auth_details.username}...")
        try:
            api = OnlyFansAPI()
            authed = await authenticate_account(api, auth_details)

            if not authed:
                logger.error(f"✗ Authentication failed for {auth_details.username}")
                sys.exit(1)
        except Exception as e:
            logger.error(f"✗ Authentication error for {auth_details.username}: {str(e)}")
            sys.exit(1)

        creator_username = authed.user.username
        creator_id_str = str(authed.user.id)

        logger.info(f"✓ Authenticated: {creator_username} (ID: {creator_id_str})")

        # Load conversations from JSON file (lightweight, just data)
        logger.info(f"\nLoading conversations from JSON...")
        try:
            conversations_data = load_conversations_from_json(auth_details.username)
            total_users = len(conversations_data)
        except FileNotFoundError as e:
            logger.info(f"⚠ {str(e)}")
            logger.info(f"Falling back to API get_chats()...")
            chats = await authed.get_chats()
            logger.info(f"✓ Found {len(chats)} conversation(s) from API")
            # Convert to same format as JSON for consistency
            conversations_data = [{'id': chat.user.id, 'username': chat.user.username, 'name': chat.user.name} for chat in chats]
            total_users = len(conversations_data)

        # TEST MODE: Limit number of fans to process
        test_limit = os.getenv('TEST_LIMIT')
        if test_limit:
            test_limit = int(test_limit)
            if test_limit > 0 and test_limit < len(conversations_data):
                logger.info(f"🧪 TEST MODE: Limiting to {test_limit} fans (from {len(conversations_data)} total)")
                conversations_data = conversations_data[:test_limit]
                total_users = len(conversations_data)

        # Initialize checkpoint manager
        checkpoint = CheckpointManager(creator_name)
        progress_stats = checkpoint.get_progress_stats(total_users)

        logger.info(f"✓ Total conversations: {total_users}")
        logger.info(f"  Completed: {progress_stats['completed']}")
        logger.info(f"  In-progress (will retry): {progress_stats['in_progress']}")
        logger.info(f"  Remaining: {progress_stats['remaining']}")
        if progress_stats['completed'] > 0:
            logger.info(f"  Progress: {progress_stats['progress_percent']:.1f}%")
        logger.info("")

        # Check if there are any conversations to process
        if total_users == 0:
            logger.info("⚠ No conversations found, marking producer as done")
            await producer.mark_producer_done(creator_id_str)
            logger.info("\n✓ Producer finished (no conversations to process)")
            return

        # Check if all fans already processed
        if progress_stats['remaining'] == 0 and progress_stats['in_progress'] == 0:
            logger.info("✓ All fans already completed (checkpoint shows 100% complete)")
            await producer.mark_producer_done(creator_id_str)
            logger.info("\n✓ Producer finished (resuming from checkpoint)")
            return

        total_duration = 0.0
        start_time_overall = time.time()
        processed_count = 0
        skipped_count = 0

        # Create semaphore for concurrency control
        semaphore = asyncio.Semaphore(concurrent_fans)

        # Result tracking (initialize before processing)
        success_count = 0
        error_count = 0
        rate_limit_count = 0

        # Filter out already completed, heavy, or rate-limited fans upfront
        fans_to_process = []
        for user_data in conversations_data:
            fan_id = user_data['id']
            if checkpoint.is_completed(fan_id) or checkpoint.is_heavy_fan(fan_id) or checkpoint.is_rate_limited(fan_id):
                skipped_count += 1
                if skipped_count % 100 == 0:
                    logger.info(f"⏩ Skipped {skipped_count} already-processed fans...")
                continue
            fans_to_process.append(user_data)

        if not fans_to_process:
            logger.info("No fans to process (all completed/skipped)")
        else:
            # Create queue with all fans
            fan_queue = asyncio.Queue()
            for fan_data in fans_to_process:
                await fan_queue.put(fan_data)

            # Shared state for progress tracking
            processed_lock = asyncio.Lock()
            reauth_event = asyncio.Event()
            reauth_event.set()  # Initially not reauthenticating

            logger.info(f"\n{'=' * 60}")
            logger.info(f"Starting {concurrent_fans} workers with rolling window...")
            logger.info(f"Total fans to process: {len(fans_to_process)}")
            logger.info(f"{'=' * 60}\n")

            # Worker function
            async def worker(worker_id: int):
                nonlocal processed_count, authed, api, success_count, error_count, rate_limit_count

                logger.info(f"  🚀 Worker {worker_id} started")

                while True:
                    try:
                        fan_data = fan_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        logger.info(f"  ⏹️ Worker {worker_id} exiting (queue empty)")
                        break

                    logger.info(f"  📥 Worker {worker_id} grabbed fan {fan_data['username']} (ID: {fan_data['id']}) from queue")

                    # Wait if reauthentication is happening
                    await reauth_event.wait()

                    fan_user = create_user_object_from_json(fan_data)
                    logger.info(f"  🔄 Worker {worker_id} starting to process {fan_user.username}...")

                    # Process the fan
                    result = await process_single_fan(
                        fan_user, authed, checkpoint, producer,
                        creator_id_str, creator_username,
                        fetch_timeout, logger, semaphore
                    )

                    logger.info(f"  ✅ Worker {worker_id} completed processing {fan_user.username} (result: {result.get('status') if isinstance(result, dict) else 'exception'})")

                    # Update processed count and track results
                    async with processed_lock:
                        # Track result status
                        if isinstance(result, Exception):
                            error_count += 1
                        else:
                            status = result.get('status')
                            if status == 'success' or status == 'empty':
                                success_count += 1
                                processed_count += 1
                            elif status == 'rate_limit':
                                rate_limit_count += 1
                                fan_id = result['fan_id']
                                fan_username = result['fan_username']
                                checkpoint.mark_rate_limited(fan_id, fan_username, retry_count=0)
                            elif status == 'error':
                                error_count += 1

                        current_count = processed_count
                        remaining = fan_queue.qsize()

                        # Log worker continuation
                        if remaining > 0:
                            logger.info(f"  ⚡ Worker {worker_id} finished {fan_user.username}, picking up next fan...")

                        # Log progress every 20 fans
                        if current_count % 20 == 0 and current_count > 0:
                            logger.info(f"\n📊 Progress: {current_count} fans processed, {remaining} remaining\n")

                        # Reauthenticate every 100 fans
                        if current_count % reauth_interval == 0 and current_count > 0:
                            # Pause all workers
                            reauth_event.clear()
                            logger.info(f"\n{'=' * 60}")
                            logger.info(f"📊 Progress: {current_count} fans processed, {remaining} remaining")
                            logger.info(f"{'=' * 60}")
                            logger.info(f"🔄 Reauthenticating after {current_count} fans...")
                            logger.info(f"⏸️  Pausing all workers...")

                            # Wait a moment for other workers to pause
                            await asyncio.sleep(0.5)
                            logger.info(f"✓ All workers paused")

                            # Close existing API session properly
                            try:
                                if hasattr(api, 'close_pool'):
                                    await api.close_pool()
                                    logger.info("✓ Closing existing API pool...")
                            except Exception as e:
                                logger.warning(f"⚠️ Warning during session close: {str(e)}")

                            # Create new API instance and reauthenticate
                            try:
                                logger.info("✓ Creating new API instance...")
                                api = OnlyFansAPI()
                                authed = await authenticate_account(api, auth_details)
                                if not authed:
                                    logger.error(f"✗ Reauthentication failed for {auth_details.username}")
                                    sys.exit(1)
                                logger.info(f"✓ Reauthenticated successfully as {auth_details.username} (ID: {creator_id_str})")
                            except Exception as e:
                                logger.error(f"✗ Reauthentication error: {str(e)}")
                                sys.exit(1)

                            # Resume workers
                            logger.info(f"▶️  Resuming all workers...")
                            logger.info(f"{'=' * 60}\n")
                            reauth_event.set()

                    fan_queue.task_done()

            # Launch workers
            workers = [asyncio.create_task(worker(i)) for i in range(concurrent_fans)]
            await asyncio.gather(*workers)

            logger.info(f"\n{'=' * 60}")
            logger.info(f"✓ All workers completed")
            logger.info(f"{'=' * 60}")

        # Calculate overall duration
        overall_duration = time.time() - start_time_overall

        # Print processing summary
        logger.info(f"\n{'=' * 60}")
        logger.info("Phase 1 Summary:")
        logger.info(f"{'=' * 60}")
        logger.info(f"  Total processed: {processed_count} fans")
        if fans_to_process:
            logger.info(f"  Success: {success_count}")
            if rate_limit_count > 0:
                logger.info(f"  Rate limited (will retry): {rate_limit_count}")
            if error_count > 0:
                logger.info(f"  Errors: {error_count}")
        logger.info(f"  Skipped (already processed): {skipped_count}")
        logger.info(f"\n  Elapsed time: {overall_duration:.2f}s ({overall_duration/60:.2f} minutes)")
        if processed_count > 0:
            logger.info(f"  Average per fan: {overall_duration/processed_count:.2f}s")
        logger.info(f"{'=' * 60}")

        # PHASE 2: Retry rate-limited fans (BEFORE heavy fans)
        rate_limited_fans = checkpoint.get_rate_limited_fans()
        if rate_limited_fans:
            logger.info(f"\n{'=' * 60}")
            logger.info(f"PHASE 2: Processing {len(rate_limited_fans)} rate-limited fan(s)")
            logger.info("Retrying with exponential backoff (30s, 60s, 120s)")
            logger.info(f"{'=' * 60}")

            for idx, fan_data in enumerate(rate_limited_fans, 1):
                fan_username = fan_data.get('fan_username', 'unknown')
                retry_count = fan_data.get('retry_count', 0)
                logger.info(f"\n[Rate-Limited {idx}/{len(rate_limited_fans)}] {fan_username} (retry {retry_count}/3)")

                result = await retry_rate_limited_fan(fan_data, authed, checkpoint, producer,
                                                     creator_id_str, creator_username,
                                                     fetch_timeout, logger, max_retries=3)

                if result['status'] == 'success':
                    logger.info(f"  ✓ Successfully recovered from rate limit")
                elif result['status'] == 'failed':
                    logger.error(f"  ✗ Permanently failed after 3 retries")

            logger.info(f"\n✓ Completed rate-limited fan retries")

        # PHASE 3: Process heavy fans (fans that timed out during normal processing)
        # Use checkpoint as primary source (persists across Redis restarts)
        heavy_fans = checkpoint.get_heavy_fans()
        if heavy_fans:
            logger.info(f"\n{'=' * 60}")
            logger.info(f"PHASE 3: Processing {len(heavy_fans)} heavy fan(s)")
            logger.info("These fans have many messages and need more time (no timeout)")
            logger.info(f"{'=' * 60}")

            for idx, heavy_fan in enumerate(heavy_fans, 1):
                fan_id = heavy_fan['fan_id']
                fan_username = heavy_fan['fan_username']

                logger.info(f"\n[Heavy {idx}/{len(heavy_fans)}] Processing: {fan_username} (ID: {fan_id})")
                heavy_start_time = time.time()

                try:
                    # Get user object
                    user_obj = await authed.get_user(int(fan_id))
                    if not user_obj:
                        logger.warning(f"  ✗ User not found, skipping")
                        continue

                    # Fetch messages with NO timeout (let it take as long as needed) using fast fetcher
                    logger.info(f"  Fetching all messages (no timeout)...")
                    messages = await fetch_all_messages_fast(user_obj, authed, logger=logger)

                    if not messages:
                        logger.info(f"  ⚠ No messages found")
                        continue

                    heavy_duration = time.time() - heavy_start_time
                    logger.info(f"  ✓ Fetched {len(messages)} messages in {heavy_duration:.2f}s ({heavy_duration/60:.2f} minutes)")

                    # Process and push to Redis
                    messages_data, bundles_data, bundle_items_data, interactions_data, analytics_data = \
                        await process_messages_and_bundles(messages, creator_id_str, creator_username, fan_id, authed)

                    if messages_data:
                        await producer.push_messages_batch(creator_id_str, messages_data)
                    if bundles_data:
                        for bundle in bundles_data:
                            await producer.push_bundle(creator_id_str, bundle)
                    if bundle_items_data:
                        await producer.push_bundle_items(creator_id_str, bundle_items_data)
                    if interactions_data:
                        await producer.push_fan_interactions(creator_id_str, interactions_data)
                    if analytics_data:
                        await producer.push_analytics(creator_id_str, analytics_data)

                    # Clear from memory immediately
                    del messages_data, bundles_data, bundle_items_data, interactions_data, analytics_data, messages

                    # Mark as completed in checkpoint (removes from heavy, adds to completed)
                    checkpoint.mark_completed(fan_id)
                    logger.info(f"  ✓ Heavy fan {fan_username} processed successfully")

                except Exception as e:
                    error_msg = str(e)
                    heavy_duration = time.time() - heavy_start_time
                    logger.error(f"  ✗ Error processing heavy fan: {error_msg} (failed after {heavy_duration:.2f}s)")
                    # Error tracking disabled (push_error removed with Stream conversion)

            logger.info(f"\n✓ Completed processing {len(heavy_fans)} heavy fan(s)")

        # Retry failed fans (up to 3 retries) - DISABLED (uses Stream-based queue)
        # TODO: Implement List-based failed fan queue if needed
        failed_fans = []  # await producer.get_failed_fans(creator_id_str, max_retries=3)
        if False and failed_fans:
            logger.info(f"\n{'=' * 60}")
            logger.info(f"Retrying {len(failed_fans)} failed fan(s)...")
            logger.info(f"{'=' * 60}")

            for failed in failed_fans:
                fan_id = failed['fan_id']
                fan_username = failed['fan_username']
                retry_count = failed['retry_count']

                logger.info(f"\nRetrying fan: {fan_username} (ID: {fan_id}) - Attempt {retry_count + 1}/3")

                try:
                    # Get user object
                    user_obj = await authed.get_user(int(fan_id))
                    if not user_obj:
                        logger.info(f"  ✗ User not found, skipping")
                        continue

                    # Fetch messages using fast fetcher
                    messages = await fetch_all_messages_fast(user_obj, authed, logger=logger)
                    if not messages:
                        logger.info(f"  ⚠ No messages found")
                        continue

                    logger.info(f"  ✓ Fetched {len(messages)} messages")

                    # Process and push to Redis
                    messages_data, bundles_data, bundle_items_data, interactions_data, analytics_data = \
                        await process_messages_and_bundles(messages, creator_id_str, creator_username, fan_id, authed)

                    if messages_data:
                        await producer.push_messages_batch(creator_id_str, messages_data)
                    if bundles_data:
                        for bundle in bundles_data:
                            await producer.push_bundle(creator_id_str, bundle)
                    if bundle_items_data:
                        await producer.push_bundle_items(creator_id_str, bundle_items_data)
                    if interactions_data:
                        await producer.push_fan_interactions(creator_id_str, interactions_data)
                    if analytics_data:
                        await producer.push_analytics(creator_id_str, analytics_data)

                    # Clear from memory immediately
                    del messages_data, bundles_data, bundle_items_data, interactions_data, analytics_data, messages

                    logger.info(f"  ✓ Retry successful for {fan_username}")

                except Exception as e:
                    error_msg = str(e)
                    logger.info(f"  ✗ Retry failed: {error_msg}")
                    # Error tracking disabled (push_error removed with Stream conversion)

        # Mark producer as done so consumer knows to finish
        await producer.mark_producer_done(creator_id_str)
        logger.info("\n✓ Producer finished successfully")

    except Exception as e:
        logger.error(f"\n✗ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        # Close API session properly
        try:
            if 'api' in locals() and hasattr(api, 'close_pool'):
                await api.close_pool()
                logger.info("✓ API session closed")
        except Exception as e:
            logger.warning(f"⚠ Warning during API cleanup: {str(e)}")

        # Close Redis connection
        try:
            await producer.close()
        except Exception as e:
            logger.warning(f"⚠ Warning during Redis cleanup: {str(e)}")

        # Final garbage collection
        gc.collect()
        logger.info("✓ Memory cleanup completed")

    logger.info("\n✓ Producer finished successfully")


if __name__ == "__main__":
    asyncio.run(main())
