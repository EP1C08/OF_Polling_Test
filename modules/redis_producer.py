"""Redis List Producer.

Fetches raw messages and pushes them to Redis Lists.
"""

import redis.asyncio as aioredis
import json
import asyncio
from datetime import datetime
from typing import Optional, Dict, Any
from modules.sanitizer import sanitize_dict, sanitize_text


class RedisProducer:
    """Redis producer for pushing messages to Redis Lists."""

    def __init__(
        self,
        redis_host: str = 'redis',
        redis_port: int = 6379,
        redis_db: int = 0,
    ):
        """Initialize Redis producer.

        :param redis_host: Redis server hostname
        :param redis_port: Redis server port
        :param redis_db: Redis database number
        """
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis: Optional[aioredis.Redis] = None

    async def connect(self) -> None:
        """Connect to Redis server."""
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

    async def close(self) -> None:
        """Close Redis connection."""
        if self.redis:
            await self.redis.close()
            print("✓ Redis connection closed")

    def _get_list_key(self, creator_id: str, list_type: str = 'messages') -> str:
        """Generate Redis list key for creator.

        :param creator_id: Creator's OnlyFans ID
        :param list_type: Type of list (messages, bundles, etc.)
        :return: Redis list key
        """
        return f"of:{creator_id}:{list_type}"

    async def push_message(self, creator_id: str, message_data: Dict[str, Any]) -> int:
        """Push a single message to Redis list.

        :param creator_id: Creator's OnlyFans ID
        :param message_data: Message dictionary
        :return: Length of list after push
        """
        if not self.redis:
            raise RuntimeError("Redis not connected")

        # Sanitize text fields
        text_fields = ['message', 'model_name', 'sender_username']
        sanitized_data = sanitize_dict(message_data, text_fields)

        # Add metadata
        sanitized_data['pushed_at'] = datetime.now().isoformat()

        # Convert entire object to JSON string
        json_data = json.dumps(sanitized_data, ensure_ascii=False)

        # Push to list
        list_key = self._get_list_key(creator_id, 'messages')
        list_length = await self.redis.lpush(list_key, json_data)

        return list_length

    async def push_messages_batch(self, creator_id: str, messages: list) -> int:
        """Push multiple messages to Redis list in batch."""
        count = 0
        for message_data in messages:
            try:
                await self.push_message(creator_id, message_data)
                count += 1
            except Exception as e:
                print(f"⚠ Failed to push message {message_data.get('message_id')}: {str(e)}")

        print(f"✓ Pushed {count}/{len(messages)} messages to Redis list")
        return count

    async def push_bundle(self, creator_id: str, bundle_data: Dict[str, Any]) -> int:
        """Push bundle data to Redis list."""
        if not self.redis:
            raise RuntimeError("Redis not connected")

        sanitized_data = sanitize_dict(bundle_data, ['name', 'description', 'creator_username'])
        sanitized_data['pushed_at'] = datetime.now().isoformat()
        json_data = json.dumps(sanitized_data, ensure_ascii=False)

        list_key = self._get_list_key(creator_id, 'bundles')
        return await self.redis.lpush(list_key, json_data)

    async def push_bundle_items(self, creator_id: str, bundle_items: list) -> int:
        """Push bundle items to Redis list."""
        list_key = self._get_list_key(creator_id, 'bundle_items')
        count = 0

        for item in bundle_items:
            sanitized = sanitize_dict(item)
            sanitized['pushed_at'] = datetime.now().isoformat()
            json_data = json.dumps(sanitized, ensure_ascii=False)
            await self.redis.lpush(list_key, json_data)
            count += 1

        print(f"✓ Pushed {count} bundle items to Redis")
        return count

    async def push_fan_interactions(self, creator_id: str, interactions: list) -> int:
        """Push fan interactions to Redis list."""
        list_key = self._get_list_key(creator_id, 'fan_interactions')
        count = 0

        for interaction in interactions:
            sanitized = sanitize_dict(interaction)
            sanitized['pushed_at'] = datetime.now().isoformat()
            json_data = json.dumps(sanitized, ensure_ascii=False)
            await self.redis.lpush(list_key, json_data)
            count += 1

        print(f"✓ Pushed {count} fan interactions to Redis")
        return count

    async def push_analytics(self, creator_id: str, analytics: list) -> int:
        """Push analytics data to Redis list."""
        list_key = self._get_list_key(creator_id, 'analytics')
        count = 0

        for analytic in analytics:
            sanitized = sanitize_dict(analytic)
            sanitized['pushed_at'] = datetime.now().isoformat()
            json_data = json.dumps(sanitized, ensure_ascii=False)
            await self.redis.lpush(list_key, json_data)
            count += 1

        print(f"✓ Pushed {count} analytics records to Redis")
        return count

    async def push_heavy_fan(self, creator_id: str, fan_id: str, fan_username: str) -> None:
        """Push heavy fan info to Redis list for tracking.

        :param creator_id: Creator's OnlyFans ID
        :param fan_id: Fan's OnlyFans ID
        :param fan_username: Fan's username
        """
        if not self.redis:
            raise RuntimeError("Redis not connected")

        heavy_fan_data = {
            'fan_id': fan_id,
            'fan_username': fan_username,
            'deferred_at': datetime.now().isoformat()
        }

        list_key = self._get_list_key(creator_id, 'heavy_fans')
        json_data = json.dumps(heavy_fan_data, ensure_ascii=False)
        await self.redis.lpush(list_key, json_data)
        print(f"✓ Pushed heavy fan {fan_username} to Redis")

    async def mark_producer_done(self, creator_id: str):
        """Mark that producer has finished processing all messages."""
        if not self.redis:
            raise RuntimeError("Redis not connected")

        done_key = f"of:{creator_id}:producer_done"
        await self.redis.set(done_key, "1", ex=3600)  # Expire in 1 hour
        print(f"✓ Marked producer as done for creator {creator_id}")
