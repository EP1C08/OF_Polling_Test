"""Message age filter for 24-hour rule.

Prevents fetching messages that are less than 24 hours old to avoid
marking them as read before the creator has a chance to see them.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional


def is_message_old_enough(created_at: Optional[str], min_age_hours: int = 24) -> bool:
    """Check if message is old enough to fetch (>= min_age_hours).

    :param created_at: ISO timestamp string from WebSocket event.
    :param min_age_hours: Minimum age in hours (default 24).
    :return: True if old enough to fetch, False to skip.
    """
    if not created_at:
        return True

    try:
        # Handle 'Z' suffix for UTC
        if created_at.endswith('Z'):
            created_at = created_at.replace('Z', '+00:00')

        msg_time = datetime.fromisoformat(created_at)

        # Ensure timezone aware
        if msg_time.tzinfo is None:
            msg_time = msg_time.replace(tzinfo=timezone.utc)

        now = datetime.now(timezone.utc)
        age = now - msg_time

        return age >= timedelta(hours=min_age_hours)
    except (ValueError, TypeError):
        return True
