"""Pending fans manager for 24-hour delayed processing.

Manages a JSON file per creator that tracks fans waiting for 24h to pass
before their messages can be fetched. This prevents marking messages as
read before the creator has a chance to see them.

IMPORTANT: The 24h timer (check_after) is set ONLY on first message and
NEVER resets. This guarantees fetch always happens after 24h from first message,
even if fan keeps chatting.

JSON Structure:
{
    "creator_id": "12345",
    "creator_name": "Ayumi",
    "pending_fans": {
        "fan_123": {
            "first_message_at": "2024-01-15T10:00:00+00:00",
            "last_message_at": "2024-01-15T18:00:00+00:00",
            "check_after": "2024-01-16T10:00:00+00:00"
        }
    },
    "last_updated": "2024-01-15T18:00:00+00:00"
}

Note: check_after is based on first_message_at, not last_message_at.
"""

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import asyncio
from filelock import FileLock


PENDING_FANS_DIR = "pending_fans"


class PendingFansManager:
    """Thread-safe manager for pending fans JSON files."""

    def __init__(
        self,
        creator_id: str,
        creator_name: str,
        pending_dir: str = PENDING_FANS_DIR,
        min_age_hours: int = 24,
        logger=None
    ):
        """Initialize pending fans manager.

        :param creator_id: Creator's OnlyFans ID.
        :param creator_name: Creator's display name.
        :param pending_dir: Directory for pending fans JSON files.
        :param min_age_hours: Minimum hours before processing (default 24).
        :param logger: Optional logger instance.
        """
        self.creator_id = str(creator_id)
        self.creator_name = creator_name
        self.pending_dir = Path(pending_dir)
        self.min_age_hours = min_age_hours
        self.logger = logger

        self.pending_dir.mkdir(parents=True, exist_ok=True)

        self.json_path = self.pending_dir / f"{creator_name}.json"
        self.lock_path = self.pending_dir / f"{creator_name}.json.lock"
        self.lock = FileLock(str(self.lock_path), timeout=30)

    def _log(self, level: str, message: str):
        """Log message if logger is available.

        :param level: Log level (info, debug, warning, error).
        :param message: Message to log.
        """
        if self.logger:
            getattr(self.logger, level)(message)

    def _load_data(self) -> Dict:
        """Load pending fans data from JSON file.

        :return: Dictionary with pending fans data.
        """
        if not self.json_path.exists():
            return {
                "creator_id": self.creator_id,
                "creator_name": self.creator_name,
                "pending_fans": {},
                "last_updated": None
            }

        try:
            with open(self.json_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            self._log('warning', f"Error loading {self.json_path}: {e}, starting fresh")
            return {
                "creator_id": self.creator_id,
                "creator_name": self.creator_name,
                "pending_fans": {},
                "last_updated": None
            }

    def _save_data(self, data: Dict):
        """Save pending fans data to JSON file.

        :param data: Dictionary with pending fans data.
        """
        data["last_updated"] = datetime.now(timezone.utc).isoformat()
        with open(self.json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def add_or_update_fan(self, fan_id: str, message_timestamp: str) -> bool:
        """Add a new fan or update existing fan's last_message_at.

        IMPORTANT: check_after is set ONLY when fan is first added.
        Subsequent messages update last_message_at but do NOT reset the 24h timer.
        This guarantees we ALWAYS fetch after 24h from the FIRST message.

        :param fan_id: Fan's OnlyFans ID.
        :param message_timestamp: ISO timestamp of the message.
        :return: True if added new, False if updated existing.
        """
        with self.lock:
            data = self._load_data()
            pending = data.get("pending_fans", {})

            fan_id = str(fan_id)
            is_new = fan_id not in pending

            # Parse message timestamp
            try:
                if message_timestamp.endswith('Z'):
                    message_timestamp = message_timestamp.replace('Z', '+00:00')
                msg_time = datetime.fromisoformat(message_timestamp)
                if msg_time.tzinfo is None:
                    msg_time = msg_time.replace(tzinfo=timezone.utc)
            except (ValueError, TypeError):
                msg_time = datetime.now(timezone.utc)

            if is_new:
                # NEW fan: Set check_after = first message time + 24h
                check_after = msg_time + timedelta(hours=self.min_age_hours)
                pending[fan_id] = {
                    "first_message_at": msg_time.isoformat(),
                    "last_message_at": msg_time.isoformat(),
                    "check_after": check_after.isoformat()
                }
                self._log('info', f"Added fan {fan_id} - check after {check_after.isoformat()}")
            else:
                # EXISTING fan: Only update last_message_at, keep check_after unchanged
                pending[fan_id]["last_message_at"] = msg_time.isoformat()
                self._log('debug', f"Updated fan {fan_id} last_message_at (check_after unchanged)")

            data["pending_fans"] = pending
            self._save_data(data)

            return is_new

    def get_fans_ready_to_process(self) -> List[Tuple[str, Dict]]:
        """Get list of fans whose check_after time has passed.

        :return: List of (fan_id, fan_data) tuples ready for processing.
        """
        with self.lock:
            data = self._load_data()
            pending = data.get("pending_fans", {})

            now = datetime.now(timezone.utc)
            ready = []

            for fan_id, fan_data in pending.items():
                try:
                    check_after_str = fan_data.get("check_after", "")
                    if check_after_str.endswith('Z'):
                        check_after_str = check_after_str.replace('Z', '+00:00')
                    check_after = datetime.fromisoformat(check_after_str)

                    if check_after.tzinfo is None:
                        check_after = check_after.replace(tzinfo=timezone.utc)

                    if now >= check_after:
                        ready.append((fan_id, fan_data))
                except (ValueError, TypeError) as e:
                    self._log('warning', f"Invalid check_after for fan {fan_id}: {e}")
                    ready.append((fan_id, fan_data))

            return ready

    def update_fan_check_time(self, fan_id: str, new_message_timestamp: str):
        """Update a fan's check_after time (when they sent a new message).

        :param fan_id: Fan's OnlyFans ID.
        :param new_message_timestamp: ISO timestamp of new message.
        """
        self.add_or_update_fan(fan_id, new_message_timestamp)

    def remove_fan(self, fan_id: str) -> bool:
        """Remove a fan from the pending list (after successful processing).

        :param fan_id: Fan's OnlyFans ID.
        :return: True if removed, False if not found.
        """
        with self.lock:
            data = self._load_data()
            pending = data.get("pending_fans", {})

            fan_id = str(fan_id)
            if fan_id in pending:
                del pending[fan_id]
                data["pending_fans"] = pending
                self._save_data(data)
                self._log('info', f"Removed fan {fan_id} from pending list")
                return True

            return False

    def get_pending_count(self) -> int:
        """Get count of pending fans.

        :return: Number of fans in pending list.
        """
        with self.lock:
            data = self._load_data()
            return len(data.get("pending_fans", {}))

    def get_all_pending(self) -> Dict[str, Dict]:
        """Get all pending fans.

        :return: Dictionary of all pending fans.
        """
        with self.lock:
            data = self._load_data()
            return data.get("pending_fans", {})

    def get_stats(self) -> Dict:
        """Get statistics about pending fans.

        :return: Dictionary with stats (total, ready, waiting).
        """
        with self.lock:
            data = self._load_data()
            pending = data.get("pending_fans", {})

            now = datetime.now(timezone.utc)
            ready_count = 0
            waiting_count = 0

            for fan_id, fan_data in pending.items():
                try:
                    check_after_str = fan_data.get("check_after", "")
                    if check_after_str.endswith('Z'):
                        check_after_str = check_after_str.replace('Z', '+00:00')
                    check_after = datetime.fromisoformat(check_after_str)

                    if check_after.tzinfo is None:
                        check_after = check_after.replace(tzinfo=timezone.utc)

                    if now >= check_after:
                        ready_count += 1
                    else:
                        waiting_count += 1
                except (ValueError, TypeError):
                    ready_count += 1

            return {
                "total": len(pending),
                "ready": ready_count,
                "waiting": waiting_count,
                "last_updated": data.get("last_updated")
            }
