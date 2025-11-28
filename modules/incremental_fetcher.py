"""Incremental Fetcher - Fetch only new messages using cutoff_id.

Wraps the existing fetch_all_messages() function to support incremental fetching.
Only fetches messages newer than the last known message_id from database.
"""

from typing import Optional, List, Dict
from modules.message_fetcher import fetch_all_messages
from modules.cutoff_manager import CutoffManager
import logging


class IncrementalFetcher:
    """Fetches only new messages using cutoff_id from database."""

    def __init__(self, cutoff_manager: CutoffManager, logger: Optional[logging.Logger] = None):
        """Initialize incremental fetcher.

        :param cutoff_manager: CutoffManager instance for querying database
        :param logger: Optional logger instance
        """
        self.cutoff_manager = cutoff_manager
        self.logger = logger or logging.getLogger(__name__)

    async def fetch_new_messages(
        self,
        user,
        model_id: str,
        fan_id: str,
        authed=None,
        limit: int = 20
    ) -> List[Dict]:
        """Fetch only new messages for a fan using cutoff_id.

        :param user: OnlyFans user object (fan)
        :param model_id: Creator's OnlyFans ID
        :param fan_id: Fan's OnlyFans ID
        :param authed: Authenticated API instance
        :param limit: Pagination limit (default: 20)
        :return: List of new message dictionaries
        """
        try:
            cutoff_id = await self.cutoff_manager.get_cutoff_id(model_id, fan_id)

            if cutoff_id:
                self.logger.debug(f"Fetching messages for fan {fan_id} with cutoff_id={cutoff_id}")
            else:
                self.logger.debug(f"No cutoff_id found for fan {fan_id}, fetching all messages")

            messages = await fetch_all_messages(
                user=user,
                limit=limit,
                cutoff_id=cutoff_id,
                authed=authed
            )

            if messages:
                self.logger.info(f"✓ Fetched {len(messages)} new message(s) for fan {fan_id}")
            else:
                self.logger.debug(f"No new messages for fan {fan_id}")

            return messages

        except Exception as e:
            self.logger.error(f"✗ Failed to fetch new messages for fan {fan_id}: {str(e)}")
            return []

    async def fetch_new_messages_bulk(
        self,
        users: List,
        model_id: str,
        authed=None,
        limit: int = 20,
        concurrent_limit: int = 3
    ) -> Dict[str, List[Dict]]:
        """Fetch new messages for multiple fans concurrently.

        :param users: List of OnlyFans user objects
        :param model_id: Creator's OnlyFans ID
        :param authed: Authenticated API instance
        :param limit: Pagination limit
        :param concurrent_limit: Number of concurrent fetch operations
        :return: Dictionary mapping fan_id to list of messages
        """
        import asyncio

        results = {}
        semaphore = asyncio.Semaphore(concurrent_limit)

        async def fetch_with_semaphore(user):
            async with semaphore:
                fan_id = str(user.id)
                messages = await self.fetch_new_messages(
                    user=user,
                    model_id=model_id,
                    fan_id=fan_id,
                    authed=authed,
                    limit=limit
                )
                return fan_id, messages

        try:
            tasks = [fetch_with_semaphore(user) for user in users]
            completed = await asyncio.gather(*tasks, return_exceptions=True)

            for result in completed:
                if isinstance(result, Exception):
                    self.logger.error(f"✗ Bulk fetch error: {str(result)}")
                    continue

                fan_id, messages = result
                if messages:
                    results[fan_id] = messages

            self.logger.info(f"✓ Bulk fetch completed: {len(results)} fans with new messages")
            return results

        except Exception as e:
            self.logger.error(f"✗ Bulk fetch failed: {str(e)}")
            return {}
