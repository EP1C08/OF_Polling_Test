"""Fast chat metadata fetcher using direct API.

Uses GET /api2/v2/chats/{fan_id} to get chat metadata without marking
messages as read. Much faster than searching through get_chats() results.
"""

from typing import Optional, Dict

API_BASE = "https://onlyfans.com/api2/v2"


class ChatMetadataFetcher:
    """Fetches chat metadata without marking messages as read."""

    def __init__(self, authed, logger=None):
        """Initialize with authenticated OnlyFans session.

        :param authed: Authenticated OnlyFans user object.
        :param logger: Optional logger instance.
        """
        self.authed = authed
        self.logger = logger
        self.requester = authed.get_requester()

    async def get_chat_metadata(self, fan_id: str) -> Optional[Dict]:
        """Get chat metadata for a specific fan (FAST, doesn't mark as read).

        :param fan_id: Fan's OnlyFans ID.
        :return: Chat metadata dict with lastMessage, or None if error.
        """
        url = f"{API_BASE}/chats/{fan_id}"
        try:
            return await self.requester.json_request(url)
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Failed to get chat metadata for {fan_id}: {e}")
            return None

    async def get_last_message_time(self, fan_id: str) -> Optional[str]:
        """Get last message timestamp for a fan.

        :param fan_id: Fan's OnlyFans ID.
        :return: ISO timestamp string or None.
        """
        chat_data = await self.get_chat_metadata(fan_id)
        if chat_data:
            last_msg = chat_data.get('lastMessage', {})
            return last_msg.get('createdAt') or last_msg.get('created_at')
        return None
