"""Fast Message Fetcher - High-performance direct API implementation.

Uses authed.get_requester() for direct API calls with automatic retry logic.
Falls back to library's get_messages() if API structure changes.

Performance:
- 2.5x faster than standard method (50-msg batches vs 20-msg)
- Automatic retry on network errors
- Falls back to library method if API changes
"""

import asyncio
import time
from typing import Optional, Dict, List, Any


class FastMessageFetcher:
    """High-performance message fetcher with retry logic and fallback support."""

    BATCH_SIZE = 50
    MAX_RETRIES = 3
    RETRY_BACKOFF = 2.0
    API_BASE = "https://onlyfans.com/api2/v2"

    def __init__(self, authed, logger=None):
        """Initialize fetcher.

        :param authed: Authenticated OnlyFans API object
        :param logger: Optional logger instance
        """
        self.authed = authed
        self.logger = logger
        self.requester = authed.get_requester()
        self.use_fallback = False

    async def fetch_all_messages(
        self,
        user,
        cutoff_id: Optional[int] = None,
    ) -> List:
        """Fetch all messages from a user (fast method with fallback).

        :param user: OnlyFans UserModel or SimpleUser object
        :param cutoff_id: Optional message ID to stop at (incremental fetch)
        :return: List of message dicts/objects
        """
        if self.use_fallback:
            return await self._fetch_library_method(user, cutoff_id)

        try:
            return await self._fetch_direct_api(user, cutoff_id)
        except Exception as e:
            if self._is_api_structure_error(e):
                self._log_warning(f"API structure changed, falling back to library method: {str(e)}")
                self.use_fallback = True
                return await self._fetch_library_method(user, cutoff_id)
            raise e

    async def _fetch_direct_api(
        self,
        user,
        cutoff_id: Optional[int] = None,
    ) -> List[Dict]:
        """Fast direct API implementation with retry logic.

        :param user: User object with id attribute
        :param cutoff_id: Stop fetching at this message_id
        :return: List of raw message dicts
        """
        start_time = time.time()
        all_messages = []
        offset_id = None
        batch_num = 0
        has_more = True
        total_retries = 0
        consecutive_errors = 0

        fan_id = str(user.id)
        fan_username = getattr(user, 'username', f'user_{fan_id}')

        while has_more:
            batch_num += 1

            url = f"{self.API_BASE}/chats/{fan_id}/messages?limit={self.BATCH_SIZE}&order=desc"
            if offset_id:
                url += f"&id={offset_id}"

            try:
                response = await self.requester.json_request(url)
                consecutive_errors = 0

            except Exception as batch_error:
                consecutive_errors += 1
                error_type = type(batch_error).__name__

                retryable = any(
                    keyword in error_type.lower() or keyword in str(batch_error).lower()
                    for keyword in ['timeout', 'connection', 'network', 'reset', 'refused', 'eof']
                )

                if retryable and consecutive_errors <= self.MAX_RETRIES:
                    wait_time = self.RETRY_BACKOFF ** consecutive_errors
                    self._log_info(
                        f"  Retry {consecutive_errors}/{self.MAX_RETRIES} for {fan_username} "
                        f"after {error_type}, waiting {wait_time:.1f}s..."
                    )
                    await asyncio.sleep(wait_time)
                    total_retries += 1
                    batch_num -= 1
                    continue
                else:
                    raise batch_error

            messages = response.get("list", [])
            has_more = response.get("hasMore", False)

            if not messages:
                break

            if cutoff_id:
                filtered_messages = []
                for msg in messages:
                    if int(msg.get("id", 0)) <= cutoff_id:
                        has_more = False
                        break
                    filtered_messages.append(msg)
                messages = filtered_messages

            all_messages.extend(messages)

            if messages:
                offset_id = str(messages[-1]["id"])

            if cutoff_id and not has_more:
                break

        duration = time.time() - start_time

        if total_retries > 0:
            self._log_info(
                f"  {fan_username}: {len(all_messages)} msgs in {duration:.2f}s "
                f"({batch_num} batches, {total_retries} retries)"
            )

        return all_messages

    async def _fetch_library_method(
        self,
        user,
        cutoff_id: Optional[int] = None,
    ) -> List:
        """Fallback to library's get_messages method.

        :param user: OnlyFans UserModel
        :param cutoff_id: Stop at this message_id
        :return: List of MessageModel objects
        """
        self._log_info(f"  Using library fallback method for {getattr(user, 'username', 'unknown')}")

        if hasattr(user, 'get_messages'):
            return await user.get_messages(limit=50, cutoff_id=cutoff_id)
        elif self.authed:
            user_obj = await self.authed.get_user(user.id)
            if user_obj:
                return await user_obj.get_messages(limit=50, cutoff_id=cutoff_id)

        raise ValueError(f"Cannot fetch messages for user {user.id}")

    def _is_api_structure_error(self, error: Exception) -> bool:
        """Check if error indicates API structure changed.

        :param error: Exception to check
        :return: True if API structure likely changed
        """
        error_str = str(error).lower()
        indicators = [
            'keyerror',
            'not found',
            '404',
            'endpoint',
            'invalid response',
            'unexpected structure'
        ]
        return any(indicator in error_str for indicator in indicators)

    def _log_info(self, message: str):
        """Log info message."""
        if self.logger:
            self.logger.info(message)

    def _log_warning(self, message: str):
        """Log warning message."""
        if self.logger:
            self.logger.warning(message)
