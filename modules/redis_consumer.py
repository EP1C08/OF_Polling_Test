"""Redis Stream Consumer.

Consumes messages from Redis Streams and generates CSV files.
"""

import redis.asyncio as aioredis
import json
import asyncio
import csv
import gc
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any


class RedisConsumer:
    """Redis consumer for reading from Redis Streams and exporting to CSV."""

    def __init__(
        self,
        creator_id: str,
        redis_host: str = 'redis',
        redis_port: int = 6379,
        redis_db: int = 0,
        output_dir: str = 'output',
        creator_name: str = None,
    ):
        """Initialize Redis consumer.

        :param creator_id: Creator's OnlyFans ID
        :param redis_host: Redis server hostname
        :param redis_port: Redis server port
        :param redis_db: Redis database number
        :param output_dir: Output directory for CSV files
        :param creator_name: Creator's name for folder naming (defaults to creator_id)
        """
        self.creator_id = creator_id
        self.creator_name = creator_name or creator_id
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.output_dir = output_dir
        self.redis: Optional[aioredis.Redis] = None
        self.consumer_group = f"csv_writer_{creator_id}"
        self.consumer_name = f"consumer_{creator_id}"

    async def connect(self) -> None:
        """Connect to Redis server."""
        try:
            self.redis = await aioredis.from_url(
                f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}",
                encoding="utf-8",
                decode_responses=True
            )
            await self.redis.ping()
            print(f"✓ Consumer connected to Redis at {self.redis_host}:{self.redis_port}")
        except Exception as e:
            print(f"✗ Failed to connect to Redis: {str(e)}")
            raise

    async def close(self) -> None:
        """Close Redis connection."""
        if self.redis:
            await self.redis.close()
            print("✓ Consumer Redis connection closed")

    def _get_stream_key(self, stream_type: str) -> str:
        """Generate Redis stream key.

        :param stream_type: Type of stream (messages, bundles, etc.)
        :return: Redis stream key
        """
        return f"of:{self.creator_id}:{stream_type}"

    async def is_producer_done(self) -> bool:
        """Check if producer has finished processing.

        :return: True if producer is done, False otherwise
        """
        if not self.redis:
            return False

        done_key = f"of:{self.creator_id}:producer_done"
        result = await self.redis.get(done_key)
        return result == "1"

    async def create_consumer_group(self, stream_type: str):
        """Create consumer group for stream if it doesn't exist"""
        stream_key = self._get_stream_key(stream_type)
        try:
            await self.redis.xgroup_create(stream_key, self.consumer_group, id='0', mkstream=True)
            print(f"✓ Created consumer group '{self.consumer_group}' for {stream_key}")
        except aioredis.ResponseError as e:
            if 'BUSYGROUP' in str(e):
                # Group already exists
                pass
            else:
                raise

    async def read_stream(
        self,
        stream_type: str,
        count: int = 5000,
        block: int = 5000
    ) -> List[Dict[str, Any]]:
        """
        Read messages from Redis stream using consumer group

        Args:
            stream_type: Type of stream (messages, bundles, etc.)
            count: Number of messages to read
            block: Milliseconds to block waiting for messages

        Returns:
            List of message dictionaries
        """
        if not self.redis:
            raise RuntimeError("Redis not connected")

        stream_key = self._get_stream_key(stream_type)

        # Ensure consumer group exists
        await self.create_consumer_group(stream_type)

        # Read from stream
        try:
            streams = await self.redis.xreadgroup(
                groupname=self.consumer_group,
                consumername=self.consumer_name,
                streams={stream_key: '>'},
                count=count,
                block=block
            )

            messages = []
            if streams:
                for stream_name, stream_messages in streams:
                    for message_id, data in stream_messages:
                        # Parse JSON fields back to objects
                        parsed_data = {}
                        for key, value in data.items():
                            if key == 'pushed_at':
                                parsed_data[key] = value
                                continue

                            # Try to parse as JSON
                            try:
                                parsed_value = json.loads(value)
                                parsed_data[key] = parsed_value
                            except (json.JSONDecodeError, TypeError):
                                parsed_data[key] = value

                        parsed_data['_message_id'] = message_id
                        messages.append(parsed_data)

            return messages

        except Exception as e:
            print(f"⚠ Error reading from stream {stream_key}: {str(e)}")
            return []

    async def acknowledge_messages(self, stream_type: str, message_ids: List[str]):
        """
        Acknowledge processed messages and delete them from stream to free memory

        Args:
            stream_type: Type of stream
            message_ids: List of message IDs to acknowledge
        """
        if not self.redis or not message_ids:
            return

        stream_key = self._get_stream_key(stream_type)

        # Acknowledge messages
        await self.redis.xack(stream_key, self.consumer_group, *message_ids)

        # Delete messages from stream to free RAM
        await self.redis.xdel(stream_key, *message_ids)

        # Trim stream to prevent unbounded growth (keep last 10k entries max)
        await self.redis.xtrim(stream_key, maxlen=10000, approximate=True)

        print(f"✓ Acknowledged and deleted {len(message_ids)} messages from {stream_key}")

    def _ensure_output_dir(self) -> Path:
        """Create output directory if it doesn't exist - ONE folder per creator"""
        output_path = Path(self.output_dir) / self.creator_name
        output_path.mkdir(parents=True, exist_ok=True)
        return output_path

    def _get_last_id_from_csv(self, filepath: Path) -> int:
        """Get the last ID from existing CSV file"""
        if not filepath.exists():
            return 0

        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                last_id = 0
                for row in reader:
                    if 'id' in row and row['id']:
                        try:
                            last_id = int(row['id'])
                        except ValueError:
                            pass
                return last_id
        except Exception:
            return 0

    async def write_to_csv(
        self,
        stream_type: str,
        headers: List[str],
        filename: str
    ) -> int:
        """
        Read from stream and APPEND to CSV file (incremental)

        Args:
            stream_type: Type of stream to read from
            headers: CSV column headers
            filename: Output CSV filename

        Returns:
            Number of rows written
        """
        output_path = self._ensure_output_dir()
        filepath = output_path / filename

        # Read messages from stream (larger batch for faster processing)
        try:
            messages = await self.read_stream(stream_type, count=5000)
        except Exception as e:
            print(f"✗ Error reading stream {stream_type}: {str(e)}")
            return 0

        if not messages:
            return 0

        # Get last ID from existing CSV
        try:
            last_id = self._get_last_id_from_csv(filepath)
        except Exception as e:
            print(f"⚠ Error reading last ID from CSV, starting from 0: {str(e)}")
            last_id = 0

        # Check if file exists to determine if we need to write headers
        file_exists = filepath.exists()

        # Write to CSV (append mode, streaming batches to reduce memory)
        written_count = 0
        message_ids = []
        BATCH_SIZE = 1000  # Write in smaller batches to avoid memory spikes

        try:
            with open(filepath, 'a', newline='', encoding='utf-8', buffering=8192*16) as f:
                writer = csv.DictWriter(f, fieldnames=headers, extrasaction='ignore')

                # Only write header if file is new
                if not file_exists:
                    writer.writeheader()

                # Process messages in batches
                for i in range(0, len(messages), BATCH_SIZE):
                    batch = messages[i:i+BATCH_SIZE]

                    for message in batch:
                        try:
                            # Extract message ID for acknowledgment
                            message_id = message.pop('_message_id', None)
                            if message_id:
                                message_ids.append(message_id)

                            # Increment ID if this row has an 'id' field
                            if 'id' in message:
                                last_id += 1
                                message['id'] = last_id

                            # Write row (only fields in headers)
                            writer.writerow(message)
                            written_count += 1
                        except Exception as e:
                            print(f"⚠ Error writing message to CSV: {str(e)}")
                            continue

                    # Flush to disk every batch to prevent buffering buildup
                    f.flush()

            # Acknowledge processed messages
            if message_ids:
                await self.acknowledge_messages(stream_type, message_ids)

            if written_count > 0:
                print(f"✓ Appended {written_count} rows to {filepath}")
            return written_count
        except Exception as e:
            print(f"✗ Error writing to CSV {filepath}: {str(e)}")
            return 0

    async def consume_and_export_all(self, poll_interval: int = 5, max_empty_polls: int = 60):
        """
        Consume all streams and export to CSV files

        Args:
            poll_interval: Seconds to wait between polls when no messages
            max_empty_polls: Number of consecutive empty polls before giving up (default 60 = 5 minutes)
        """
        print(f"\n{'=' * 60}")
        print(f"Consuming streams for creator: {self.creator_id}")
        print(f"{'=' * 60}")

        # Define headers for each CSV type
        messages_headers = [
            'id', 'message_id', 'model_id', 'fan_id', 'sender_id', 'model_name',
            'sender_username', 'message', 'message_type', 'media_id', 'media_type',
            'price', 'is_free', 'is_purchased', 'is_from_me', 'created_at', 'fetched_at'
        ]

        bundles_headers = [
            'id', 'bundle_id', 'message_id', 'creator_id', 'creator_username', 'total_price',
            'media_count', 'photo_count', 'video_count', 'audio_count', 'is_mass_message',
            'queue_id', 'name', 'description', 'created_at', 'first_seen_at'
        ]

        bundle_items_headers = ['id', 'bundle_id', 'media_id', 'media_type', 'duration', 'created_at']

        fan_interactions_headers = [
            'id', 'bundle_id', 'fan_user_id', 'message_id', 'sent_at',
            'is_purchased', 'purchased_at', 'created_at'
        ]

        analytics_headers = [
            'id', 'bundle_id', 'api_sent_count', 'api_viewed_count', 'api_purchased_count',
            'tracked_offers', 'tracked_purchases', 'view_rate', 'conversion_rate',
            'total_revenue', 'net_revenue', 'average_time_to_purchase', 'best_time_of_day',
            'best_day_of_week', 'last_synced', 'last_updated'
        ]

        # Export each stream type
        exports = [
            ('messages', messages_headers, 'messages.csv'),
            ('bundles', bundles_headers, 'bundles.csv'),
            ('bundle_items', bundle_items_headers, 'bundle_items.csv'),
            ('fan_interactions', fan_interactions_headers, 'bundle_fan_interactions.csv'),
            ('analytics', analytics_headers, 'bundle_analytics.csv')
        ]

        # Process messages in real-time while checking for DONE signal
        total_rows = 0
        producer_done = False

        print(f"\n⏳ Processing messages in real-time, waiting for producer DONE signal...")

        while True:
            current_batch_rows = 0

            # Process all stream types
            for stream_type, headers, filename in exports:
                try:
                    count = await self.write_to_csv(stream_type, headers, filename)
                    total_rows += count
                    current_batch_rows += count
                except Exception as e:
                    print(f"✗ Error exporting {stream_type}: {str(e)}")

            if current_batch_rows > 0:
                print(f"\n✓ Exported {current_batch_rows} rows in this batch")

            # Force garbage collection after each export cycle
            gc.collect()

            # Check if producer is done
            if not producer_done:
                producer_done = await self.is_producer_done()
                if producer_done:
                    print(f"\n✓ Producer DONE signal received! Processing remaining messages...")

            # If producer is done and no more messages, exit
            if producer_done and current_batch_rows == 0:
                print(f"\n✓ All messages processed, producer is done")
                break

            # Wait before next poll
            await asyncio.sleep(poll_interval)

        print(f"\n✓ Total rows exported: {total_rows}")
        print(f"{'=' * 60}")

        return total_rows
