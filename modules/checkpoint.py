"""
Checkpoint module for tracking and resuming progress
Saves processed fan IDs so producer can resume from where it left off

Two-phase checkpoint system:
1. mark_in_progress() - Save BEFORE processing (prevents duplicates)
2. mark_completed() - Update AFTER success (confirms completion)

On crash/restart, any "in_progress" fans are re-processed from scratch.

Heavy fan system:
- Fans that timeout during fetching are marked as "heavy"
- Heavy fans are processed at the end with no timeout
- Tracked in checkpoint for persistence even after Redis restart

Rate limit retry system:
- Fans that hit rate limits are added to retry queue
- 3 retry attempts with exponential backoff (30s, 60s, 120s)
- Processed BEFORE heavy fans
- After 3 failures, marked as permanently failed

Thread-safety:
- Uses threading.Lock() to prevent concurrent write corruption
- Safe for parallel fan processing (multiple fans at once)
"""

import json
import threading
from pathlib import Path
from datetime import datetime
from typing import Set, Optional, Dict, List


class CheckpointManager:
    def __init__(self, creator_name: str, checkpoint_dir: str = 'checkpoints'):
        """
        Initialize checkpoint manager

        Args:
            creator_name: Creator's name for checkpoint file naming
            checkpoint_dir: Directory to store checkpoint files
        """
        self.creator_name = creator_name
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_file = self.checkpoint_dir / f"{creator_name}.json"
        self.completed_fan_ids: Set[str] = set()
        self.in_progress_fan_ids: Set[str] = set()
        self.heavy_fan_ids: Set[str] = set()
        self.heavy_fan_details: Dict[str, Dict] = {}
        self.rate_limited_fans: List[Dict] = []
        self.failed_fan_ids: Set[str] = set()
        self.failed_fan_details: Dict[str, Dict] = {}

        # Thread lock for concurrent write protection
        self._lock = threading.Lock()

        # Create checkpoint directory if it doesn't exist
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

        # Load existing checkpoint if available
        self._load_checkpoint()

    def _load_checkpoint(self):
        """Load checkpoint from file if it exists"""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.completed_fan_ids = set(data.get('completed_fan_ids', []))
                    self.in_progress_fan_ids = set(data.get('in_progress_fan_ids', []))
                    self.heavy_fan_ids = set(data.get('heavy_fan_ids', []))
                    self.heavy_fan_details = data.get('heavy_fan_details', {})
                    self.rate_limited_fans = data.get('rate_limited_fans', [])
                    self.failed_fan_ids = set(data.get('failed_fan_ids', []))
                    self.failed_fan_details = data.get('failed_fan_details', {})

                total_processed = len(self.completed_fan_ids)
                total_heavy = len(self.heavy_fan_ids)
                total_rate_limited = len(self.rate_limited_fans)
                total_failed = len(self.failed_fan_ids)

                if self.in_progress_fan_ids or self.heavy_fan_ids or self.rate_limited_fans or self.failed_fan_ids:
                    status_parts = [f"{total_processed} completed"]
                    if self.in_progress_fan_ids:
                        status_parts.append(f"{len(self.in_progress_fan_ids)} in-progress (will retry)")
                    if self.rate_limited_fans:
                        status_parts.append(f"{total_rate_limited} rate-limited (will retry)")
                    if self.heavy_fan_ids:
                        status_parts.append(f"{total_heavy} heavy (will process at end)")
                    if self.failed_fan_ids:
                        status_parts.append(f"{total_failed} failed")
                    print(f"✓ Loaded checkpoint: {', '.join(status_parts)}")
                else:
                    print(f"✓ Loaded checkpoint: {total_processed} fans already processed")
            except Exception as e:
                print(f"⚠ Could not load checkpoint: {str(e)}")
                self.completed_fan_ids = set()
                self.in_progress_fan_ids = set()
                self.heavy_fan_ids = set()
                self.heavy_fan_details = {}
                self.rate_limited_fans = []
                self.failed_fan_ids = set()
                self.failed_fan_details = {}

    def is_completed(self, fan_id: str) -> bool:
        """
        Check if a fan has been fully completed

        Args:
            fan_id: Fan's OnlyFans ID

        Returns:
            True if fan was fully processed and completed
        """
        return str(fan_id) in self.completed_fan_ids

    def is_in_progress(self, fan_id: str) -> bool:
        """
        Check if a fan is currently marked as in-progress

        Args:
            fan_id: Fan's OnlyFans ID

        Returns:
            True if fan is in-progress
        """
        return str(fan_id) in self.in_progress_fan_ids

    def should_process(self, fan_id: str) -> bool:
        """
        Check if a fan should be processed (not completed, even if in-progress)

        Args:
            fan_id: Fan's OnlyFans ID

        Returns:
            True if fan should be processed (either new or was in-progress)
        """
        return str(fan_id) not in self.completed_fan_ids

    def mark_in_progress(self, fan_id: str):
        """
        Mark a fan as in-progress BEFORE processing starts
        This prevents duplicates on crash/restart
        Thread-safe for concurrent processing

        Args:
            fan_id: Fan's OnlyFans ID
        """
        with self._lock:
            fan_id_str = str(fan_id)
            self.in_progress_fan_ids.add(fan_id_str)
            self._save_checkpoint()

    def mark_completed(self, fan_id: str):
        """
        Mark a fan as completed AFTER successful processing
        Removes from all queues and adds to completed
        Thread-safe for concurrent processing

        Args:
            fan_id: Fan's OnlyFans ID
        """
        with self._lock:
            fan_id_str = str(fan_id)
            self.in_progress_fan_ids.discard(fan_id_str)  # Remove from in-progress
            self.heavy_fan_ids.discard(fan_id_str)        # Remove from heavy (if it was heavy)
            if fan_id_str in self.heavy_fan_details:
                del self.heavy_fan_details[fan_id_str]    # Remove heavy details
            # Remove from rate limited queue
            self.rate_limited_fans = [f for f in self.rate_limited_fans if f['fan_id'] != fan_id_str]
            self.completed_fan_ids.add(fan_id_str)        # Add to completed
            self._save_checkpoint()

    def mark_heavy_fan(self, fan_id: str, fan_username: str):
        """
        Mark a fan as heavy (timed out during fetching)
        Heavy fans will be processed at the end with no timeout
        Thread-safe for concurrent processing

        Args:
            fan_id: Fan's OnlyFans ID
            fan_username: Fan's username for reference
        """
        with self._lock:
            fan_id_str = str(fan_id)
            self.in_progress_fan_ids.discard(fan_id_str)  # Remove from in-progress
            self.heavy_fan_ids.add(fan_id_str)            # Add to heavy
            self.heavy_fan_details[fan_id_str] = {
                'username': fan_username,
                'deferred_at': datetime.now().isoformat()
            }
            self._save_checkpoint()

    def is_heavy_fan(self, fan_id: str) -> bool:
        """
        Check if a fan is marked as heavy

        Args:
            fan_id: Fan's OnlyFans ID

        Returns:
            True if fan is marked as heavy
        """
        return str(fan_id) in self.heavy_fan_ids

    def get_heavy_fans(self) -> List[Dict]:
        """
        Get list of heavy fans that need to be processed

        Returns:
            List of dicts with fan_id, fan_username, and deferred_at
        """
        heavy_fans = []
        for fan_id in self.heavy_fan_ids:
            details = self.heavy_fan_details.get(fan_id, {})
            heavy_fans.append({
                'fan_id': fan_id,
                'fan_username': details.get('username', 'unknown'),
                'deferred_at': details.get('deferred_at', '')
            })
        return heavy_fans

    def mark_rate_limited(self, fan_id: str, fan_username: str, retry_count: int = 0):
        """
        Mark fan as rate limited for retry
        Thread-safe for concurrent processing

        Args:
            fan_id: Fan's OnlyFans ID
            fan_username: Fan's username
            retry_count: Current retry attempt count
        """
        with self._lock:
            fan_id_str = str(fan_id)

            # Remove from in-progress
            self.in_progress_fan_ids.discard(fan_id_str)

            # Check if already in rate limit queue
            existing = next((f for f in self.rate_limited_fans if f['fan_id'] == fan_id_str), None)

            if existing:
                existing['retry_count'] = retry_count
                existing['last_attempt'] = datetime.now().isoformat()
            else:
                self.rate_limited_fans.append({
                    'fan_id': fan_id_str,
                    'fan_username': fan_username,
                    'retry_count': retry_count,
                    'last_attempt': datetime.now().isoformat()
                })

            self._save_checkpoint()

    def mark_permanently_failed(self, fan_id: str, fan_username: str, error: str):
        """
        Mark fan as permanently failed after max retries
        Thread-safe for concurrent processing

        Args:
            fan_id: Fan's OnlyFans ID
            fan_username: Fan's username
            error: Error message describing the failure
        """
        with self._lock:
            fan_id_str = str(fan_id)

            # Remove from all other queues
            self.in_progress_fan_ids.discard(fan_id_str)
            self.rate_limited_fans = [f for f in self.rate_limited_fans if f['fan_id'] != fan_id_str]

            # Add to failed
            self.failed_fan_ids.add(fan_id_str)
            self.failed_fan_details[fan_id_str] = {
                'username': fan_username,
                'error': error,
                'failed_at': datetime.now().isoformat()
            }

            self._save_checkpoint()

    def get_rate_limited_fans(self) -> List[Dict]:
        """
        Get fans that need rate limit retry

        Returns:
            List of dicts with fan_id, fan_username, retry_count, and last_attempt
        """
        return self.rate_limited_fans.copy()

    def is_rate_limited(self, fan_id: str) -> bool:
        """
        Check if fan is in rate limit queue

        Args:
            fan_id: Fan's OnlyFans ID

        Returns:
            True if fan is in rate limit queue
        """
        fan_id_str = str(fan_id)
        return any(f['fan_id'] == fan_id_str for f in self.rate_limited_fans)

    def _save_checkpoint(self):
        """Save checkpoint to file"""
        try:
            checkpoint_data = {
                'creator_name': self.creator_name,
                'completed_fan_ids': list(self.completed_fan_ids),
                'in_progress_fan_ids': list(self.in_progress_fan_ids),
                'heavy_fan_ids': list(self.heavy_fan_ids),
                'heavy_fan_details': self.heavy_fan_details,
                'rate_limited_fans': self.rate_limited_fans,
                'failed_fan_ids': list(self.failed_fan_ids),
                'failed_fan_details': self.failed_fan_details,
                'total_completed': len(self.completed_fan_ids),
                'total_in_progress': len(self.in_progress_fan_ids),
                'total_heavy': len(self.heavy_fan_ids),
                'total_rate_limited': len(self.rate_limited_fans),
                'total_failed': len(self.failed_fan_ids)
            }

            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(checkpoint_data, f, indent=2)
        except Exception as e:
            print(f"⚠ Could not save checkpoint: {str(e)}")

    def clear_checkpoint(self):
        """Delete checkpoint file (start fresh)"""
        try:
            if self.checkpoint_file.exists():
                self.checkpoint_file.unlink()
                print(f"✓ Cleared checkpoint for {self.creator_name}")
            self.completed_fan_ids = set()
            self.in_progress_fan_ids = set()
            self.heavy_fan_ids = set()
            self.heavy_fan_details = {}
        except Exception as e:
            print(f"⚠ Could not clear checkpoint: {str(e)}")

    def get_progress_stats(self, total_fans: int) -> dict:
        """
        Get progress statistics

        Args:
            total_fans: Total number of fans to process

        Returns:
            Dictionary with progress stats
        """
        completed = len(self.completed_fan_ids)
        in_progress = len(self.in_progress_fan_ids)
        remaining = max(0, total_fans - completed - in_progress)
        progress_pct = (completed / total_fans * 100) if total_fans > 0 else 0

        return {
            'completed': completed,
            'in_progress': in_progress,
            'remaining': remaining,
            'total': total_fans,
            'progress_percent': progress_pct
        }

    def get_in_progress_fans(self) -> list:
        """
        Get list of fans that were in-progress when system crashed

        Returns:
            List of fan IDs that need to be retried
        """
        return list(self.in_progress_fan_ids)
