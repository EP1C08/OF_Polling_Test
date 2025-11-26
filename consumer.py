"""
Consumer Script - Reads from Redis Streams and exports to CSV
Run one instance per creator
"""

import asyncio
import os
import sys
import gc
from modules.redis_consumer import RedisConsumer
from modules.logger import setup_logger


async def main():
    # Get creator ID from environment variable
    creator_id = os.getenv('CREATOR_ID')
    if not creator_id:
        print("✗ CREATOR_ID environment variable not set")
        sys.exit(1)

    # Get creator name (for folder naming)
    creator_name = os.getenv('CREATOR_NAME', creator_id)

    # Setup logging
    logger = setup_logger(creator_name, log_type="consumer")

    logger.info("=" * 60)
    logger.info(f"OnlyFans CSV Consumer")
    logger.info(f"Creator ID: {creator_id}")
    logger.info(f"Creator Name: {creator_name}")
    logger.info("=" * 60)

    # Connect to Redis
    redis_host = os.getenv('REDIS_HOST', 'redis')
    redis_port = int(os.getenv('REDIS_PORT', '6379'))
    output_dir = os.getenv('OUTPUT_DIR', '/app/output')

    consumer = RedisConsumer(
        creator_id=creator_id,
        redis_host=redis_host,
        redis_port=redis_port,
        output_dir=output_dir,
        creator_name=creator_name
    )

    try:
        await consumer.connect()

        # Consume and export all streams
        await consumer.consume_and_export_all()

        logger.info(f"\n✓ Consumer finished successfully")

    except Exception as e:
        logger.error(f"\n✗ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        await consumer.close()


if __name__ == "__main__":
    asyncio.run(main())
