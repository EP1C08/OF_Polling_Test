"""
Redis Stream Producer
Fetches raw messages and pushes them to Redis Streams
"""

import redis.asyncio as aioredis
import json
import asyncio
from datetime import datetime
from typing import Optional, Dict, Any
from modules.sanitizer import sanitize_dict, sanitize_text


class RedisProducer:
    def __init__(self, redis_host: str = 'redis', redis_port: int = 6379, redis_db: int = 0):
        """
        Initialize Redis producer

        Args:
            redis_host: Redis server hostname
            redis_port: Redis server port
            redis_db: Redis database number
        """
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis: Optional[aioredis.Redis] = None

    async def connect(self):
        """Connect to Redis server"""
        try:
            self.redis = await aioredis.from_url(
                f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}",
                encoding="utf-8",
                decode_responses=True
            )
            await self.redis.ping()
            print(f"✓ Connected to Redis at {self.redis_host}:{self.redis_port}")
        except Exception as e:
            print(f"✗ Failed to connect to Redis: {str(e)}")
            raise

    async def close(self):
        """Close Redis connection"""
        if self.redis:
            await self.redis.close()
            print("✓ Redis connection closed")

    def _get_stream_key(self, creator_id: str, stream_type: str = 'messages') -> str:
        """
        Generate Redis stream key for creator

        Args:
            creator_id: Creator's OnlyFans ID
            stream_type: Type of stream (messages, bundles, etc.)

        Returns:
            Redis stream key
        """
        return f"of:{creator_id}:{stream_type}"

    async def push_message(self, creator_id: str, message_data: Dict[str, Any]) -> str:
        """
        Push a single message to Redis stream

        Args:
            creator_id: Creator's OnlyFans ID
            message_data: Message dictionary

        Returns:
            Message ID from Redis
        """
        if not self.redis:
            raise RuntimeError("Redis not connected")

        # Sanitize text fields
        text_fields = ['message', 'model_name', 'sender_username']
        sanitized_data = sanitize_dict(message_data, text_fields)

        # Convert to JSON string for complex fields
        stream_data = {}
        for key, value in sanitized_data.items():
            if isinstance(value, (dict, list)):
                stream_data[key] = json.dumps(value, ensure_ascii=False)
            elif isinstance(value, bool):
                stream_data[key] = str(value).lower()
            elif value is None:
                stream_data[key] = ''
            else:
                stream_data[key] = str(value)

        # Add metadata
        stream_data['pushed_at'] = datetime.now().isoformat()

        # Push to stream
        stream_key = self._get_stream_key(creator_id, 'messages')
        message_id = await self.redis.xadd(stream_key, stream_data)

        return message_id

    async def push_messages_batch(self, creator_id: str, messages: list) -> int:
        """
        Push multiple messages to Redis stream in batch

        Args:
            creator_id: Creator's OnlyFans ID
            messages: List of message dictionaries

        Returns:
            Number of messages pushed
        """
        count = 0
        for message_data in messages:
            try:
                await self.push_message(creator_id, message_data)
                count += 1
            except Exception as e:
                print(f"⚠ Failed to push message {message_data.get('message_id')}: {str(e)}")

        print(f"✓ Pushed {count}/{len(messages)} messages to Redis stream")
        return count

    async def push_bundle(self, creator_id: str, bundle_data: Dict[str, Any]) -> str:
        """
        Push bundle data to Redis stream

        Args:
            creator_id: Creator's OnlyFans ID
            bundle_data: Bundle dictionary

        Returns:
            Message ID from Redis
        """
        if not self.redis:
            raise RuntimeError("Redis not connected")

        # Sanitize text fields
        text_fields = ['name', 'description', 'creator_username']
        sanitized_data = sanitize_dict(bundle_data, text_fields)

        # Convert to stream data
        stream_data = {}
        for key, value in sanitized_data.items():
            if isinstance(value, (dict, list)):
                stream_data[key] = json.dumps(value, ensure_ascii=False)
            elif isinstance(value, bool):
                stream_data[key] = str(value).lower()
            elif value is None:
                stream_data[key] = ''
            else:
                stream_data[key] = str(value)

        stream_data['pushed_at'] = datetime.now().isoformat()

        stream_key = self._get_stream_key(creator_id, 'bundles')
        message_id = await self.redis.xadd(stream_key, stream_data)

        return message_id

    async def push_bundle_items(self, creator_id: str, bundle_items: list) -> int:
        """Push bundle items to Redis stream"""
        stream_key = self._get_stream_key(creator_id, 'bundle_items')
        count = 0

        for item in bundle_items:
            sanitized = sanitize_dict(item)
            stream_data = {k: json.dumps(v) if isinstance(v, (dict, list)) else str(v) for k, v in sanitized.items()}
            stream_data['pushed_at'] = datetime.now().isoformat()
            await self.redis.xadd(stream_key, stream_data)
            count += 1

        print(f"✓ Pushed {count} bundle items to Redis")
        return count

    async def push_fan_interactions(self, creator_id: str, interactions: list) -> int:
        """Push fan interactions to Redis stream"""
        stream_key = self._get_stream_key(creator_id, 'fan_interactions')
        count = 0

        for interaction in interactions:
            sanitized = sanitize_dict(interaction)
            stream_data = {k: json.dumps(v) if isinstance(v, (dict, list)) else str(v) for k, v in sanitized.items()}
            stream_data['pushed_at'] = datetime.now().isoformat()
            await self.redis.xadd(stream_key, stream_data)
            count += 1

        print(f"✓ Pushed {count} fan interactions to Redis")
        return count

    async def push_analytics(self, creator_id: str, analytics: list) -> int:
        """Push analytics data to Redis stream"""
        stream_key = self._get_stream_key(creator_id, 'analytics')
        count = 0

        for analytic in analytics:
            sanitized = sanitize_dict(analytic)
            stream_data = {k: json.dumps(v) if isinstance(v, (dict, list)) else str(v) for k, v in sanitized.items()}
            stream_data['pushed_at'] = datetime.now().isoformat()
            await self.redis.xadd(stream_key, stream_data)
            count += 1

        print(f"✓ Pushed {count} analytics records to Redis")
        return count

    async def push_error(self, creator_id: str, fan_id: str, fan_username: str, error_message: str, retry_count: int = 0):
        """Push error to Redis stream for tracking and retry"""
        if not self.redis:
            raise RuntimeError("Redis not connected")

        stream_key = self._get_stream_key(creator_id, 'errors')
        error_data = {
            'fan_id': str(fan_id),
            'fan_username': fan_username,
            'error': error_message,
            'retry_count': str(retry_count),
            'timestamp': datetime.now().isoformat()
        }
        await self.redis.xadd(stream_key, error_data)

    async def get_failed_fans(self, creator_id: str, max_retries: int = 3):
        """Get list of failed fans that can be retried"""
        if not self.redis:
            raise RuntimeError("Redis not connected")

        stream_key = self._get_stream_key(creator_id, 'errors')

        try:
            # Read all error messages
            messages = await self.redis.xrange(stream_key)
            failed_fans = []

            for msg_id, data in messages:
                retry_count = int(data.get('retry_count', 0))
                error_msg = data.get('error', '').lower()

                # Skip if already retried too many times
                if retry_count >= max_retries:
                    continue

                # Skip unrecoverable errors
                if any(skip_word in error_msg for skip_word in ['not found', 'does not exist', 'deleted', 'banned']):
                    continue

                failed_fans.append({
                    'fan_id': data.get('fan_id'),
                    'fan_username': data.get('fan_username'),
                    'retry_count': retry_count
                })

            return failed_fans
        except Exception:
            return []

    async def push_heavy_fan(self, creator_id: str, fan_id: str, fan_username: str):
        """
        Push fan to heavy_fans stream for deferred processing
        Used when fan has too many messages and times out

        Args:
            creator_id: Creator's OnlyFans ID
            fan_id: Fan's OnlyFans ID
            fan_username: Fan's username
        """
        if not self.redis:
            raise RuntimeError("Redis not connected")

        stream_key = self._get_stream_key(creator_id, 'heavy_fans')
        data = {
            'fan_id': str(fan_id),
            'fan_username': fan_username,
            'deferred_at': datetime.now().isoformat()
        }
        await self.redis.xadd(stream_key, data)
        print(f"✓ Deferred heavy fan {fan_username} (ID: {fan_id}) to heavy_fans queue")

    async def get_heavy_fans(self, creator_id: str) -> list:
        """
        Get list of heavy fans that were deferred for later processing

        Args:
            creator_id: Creator's OnlyFans ID

        Returns:
            List of dictionaries with fan_id and fan_username
        """
        if not self.redis:
            raise RuntimeError("Redis not connected")

        stream_key = self._get_stream_key(creator_id, 'heavy_fans')

        try:
            messages = await self.redis.xrange(stream_key)
            heavy_fans = []

            for msg_id, data in messages:
                heavy_fans.append({
                    'fan_id': data.get('fan_id'),
                    'fan_username': data.get('fan_username'),
                    'deferred_at': data.get('deferred_at')
                })

            return heavy_fans
        except Exception:
            return []

    async def mark_producer_done(self, creator_id: str):
        """Mark that producer has finished processing all messages"""
        if not self.redis:
            raise RuntimeError("Redis not connected")

        done_key = f"of:{creator_id}:producer_done"
        await self.redis.set(done_key, "1", ex=3600)  # Expire in 1 hour
        print(f"✓ Marked producer as done for creator {creator_id}")

    async def get_stream_length(self, creator_id: str, stream_type: str = 'messages') -> int:
        """Get the length of a Redis stream"""
        if not self.redis:
            raise RuntimeError("Redis not connected")

        stream_key = self._get_stream_key(creator_id, stream_type)
        length = await self.redis.xlen(stream_key)
        return length

    async def trim_stream(self, creator_id: str, stream_type: str = 'messages', max_length: int = 10000):
        """
        Trim stream to maximum length (remove old entries)

        Args:
            creator_id: Creator's OnlyFans ID
            stream_type: Type of stream
            max_length: Maximum number of entries to keep
        """
        if not self.redis:
            raise RuntimeError("Redis not connected")

        stream_key = self._get_stream_key(creator_id, stream_type)
        await self.redis.xtrim(stream_key, maxlen=max_length, approximate=True)
        print(f"✓ Trimmed stream {stream_key} to ~{max_length} entries")
