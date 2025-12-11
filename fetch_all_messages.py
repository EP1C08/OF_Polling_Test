"""
Fetch All Messages for All Fans

Reads fans from CSV, fetches messages using manual fetcher (while loop),
and saves results incrementally to CSV.

Setup:
- 1 process per model
- Configurable concurrent workers (100 with proxy, 3 without)
- No timeout - runs to completion
- Saves after each fan completes

Sharding:
- Run multiple instances with different shards to parallelize
- python fetch_all_messages.py           # No sharding (all fans)
- python fetch_all_messages.py 1 2       # Shard 1 of 2 (first half)
- python fetch_all_messages.py 2 2       # Shard 2 of 2 (second half)
- python fetch_all_messages.py 1 3       # Shard 1 of 3 (every 3rd fan starting at 0)
"""

import argparse
import asyncio
import csv
import hashlib
import html
import json
import multiprocessing
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
os.chdir(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ultima_scraper_api import select_api
from ultima_scraper_api.config import UltimaScraperAPIConfig
from modules.credential_loader import CredentialLoader

# Fix Windows console encoding
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')


# =============================================================================
# CONFIGURATION - Toggle these settings as needed
# =============================================================================

# Proxy settings - Toggle to bypass rate limiting
USE_PROXY = False  # Set to True to use proxy

# Proxy pool (format: http://user:pass@ip:port)
PROXY_URLS = [
    "http://fandomadvertising:nnhEzbVj7M@208.214.161.203:51523",
    "http://fandomadvertising:nnhEzbVj7M@63.125.89.212:51523",
    "http://fandomadvertising:nnhEzbVj7M@63.125.95.248:51523",
    "http://fandomadvertising:nnhEzbVj7M@65.195.106.218:51523",
    "http://fandomadvertising:nnhEzbVj7M@63.125.93.157:51523",
    "http://fandomadvertising:nnhEzbVj7M@63.125.88.252:51523",
    "http://fandomadvertising:nnhEzbVj7M@63.125.94.115:51523",
    "http://fandomadvertising:nnhEzbVj7M@63.125.92.90:51523",
]

# Worker settings
NUM_PROCESSES = 1  # Number of multiprocessing.Process per model (1 = simpler asyncio approach)
NUM_WORKERS = 100  # Async workers per process
BATCH_SIZE = 50  # Messages per API call
SAVE_MESSAGES_TO_CSV = True  # Save individual messages
SAVE_SUMMARY_TO_CSV = True  # Save fetch summaries

# Model filtering - Set to None to include all
SKIP_MODELS = None  # e.g., ["jess"] to skip Jess, or None to include all
ONLY_MODELS = ["jess"]  # e.g., ["jess"] to ONLY fetch Jess, or None to include all

# Output directory
OUTPUT_DIR = "messages_export"

# Retry configuration
MAX_RETRIES = 3  # Max retries per fan on network error
RETRY_BACKOFF = 2.0  # Exponential backoff base (seconds)

# Checkpoint configuration (will be updated with shard suffix if sharding)
CHECKPOINT_FREQUENCY = 1  # Save checkpoint after every fan (prevents duplicates on resume)

# Sharding configuration (set via command line args)
SHARD_ID = None  # 1-based shard number (None = no sharding)
TOTAL_SHARDS = None  # Total number of shards


def parse_args():
    """Parse command line arguments for sharding."""
    global SHARD_ID, TOTAL_SHARDS

    parser = argparse.ArgumentParser(
        description="Fetch all messages for all fans",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python fetch_all_messages.py           # No sharding (all fans)
  python fetch_all_messages.py 1 2       # Shard 1 of 2 (first half)
  python fetch_all_messages.py 2 2       # Shard 2 of 2 (second half)
  python fetch_all_messages.py 1 3       # Shard 1 of 3
  python fetch_all_messages.py 2 3       # Shard 2 of 3
  python fetch_all_messages.py 3 3       # Shard 3 of 3
        """
    )
    parser.add_argument("shard", nargs="?", type=int, default=None,
                        help="Shard number (1-based)")
    parser.add_argument("total", nargs="?", type=int, default=None,
                        help="Total number of shards")

    args = parser.parse_args()

    # Validate sharding args
    if args.shard is not None or args.total is not None:
        if args.shard is None or args.total is None:
            parser.error("Both shard and total must be provided together")
        if args.shard < 1 or args.shard > args.total:
            parser.error(f"Shard must be between 1 and {args.total}")
        if args.total < 1:
            parser.error("Total shards must be at least 1")

        SHARD_ID = args.shard
        TOTAL_SHARDS = args.total

    return args


def get_checkpoint_file() -> str:
    """Get checkpoint file path, with shard suffix if sharding."""
    if SHARD_ID is not None:
        return f"{OUTPUT_DIR}/checkpoint_shard{SHARD_ID}.json"
    return f"{OUTPUT_DIR}/checkpoint.json"


def filter_fans_by_shard(fans: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filter fans list to only include fans for this shard.

    Uses modulo distribution: fan at index i goes to shard (i % total) + 1
    This ensures even distribution regardless of list size.

    :param fans: Full list of fans
    :return: Filtered list for this shard
    """
    if SHARD_ID is None or TOTAL_SHARDS is None:
        return fans

    # Shard 1 gets indices 0, 3, 6... (when total=3)
    # Shard 2 gets indices 1, 4, 7... (when total=3)
    # Shard 3 gets indices 2, 5, 8... (when total=3)
    return [fan for i, fan in enumerate(fans) if (i % TOTAL_SHARDS) == (SHARD_ID - 1)]


def generate_bundle_id(media_ids: List[str]) -> Optional[str]:
    """Generate deterministic bundle_id from media IDs using MD5 hash.

    Same media IDs always produce the same bundle_id regardless of:
    - Order of IDs (sorted internally)
    - Python session (MD5 is deterministic)

    :param media_ids: List of media ID strings
    :return: Bundle ID string like 'bundle_123456789' or None if empty
    """
    if not media_ids:
        return None

    # Sort and join for consistent hash
    sorted_ids = sorted(media_ids)
    bundle_content = '_'.join(sorted_ids)

    # Generate deterministic ID using MD5
    md5_hash = hashlib.md5(bundle_content.encode()).hexdigest()
    bundle_hash = int(md5_hash[:8], 16) & 0x7FFFFFFF
    return f"bundle_{bundle_hash}"


def sync_checkpoint_from_csvs() -> Dict[str, set]:
    """Scan fetch_summary CSVs to find all completed fans.

    This provides crash-resilience: even if checkpoint.json wasn't updated,
    we can derive completed fans from the CSV files that were written.

    :return: Dict mapping model_id -> set of completed fan_ids
    """
    completed = {}
    output_path = Path(OUTPUT_DIR)

    if not output_path.exists():
        return completed

    # Find all fetch_summary files
    for csv_file in output_path.glob("fetch_summary_*.csv"):
        # Extract model_id from filename: fetch_summary_{account}_{model_id}_{timestamp}.csv
        # or fetch_summary_{account}_{model_id}_{timestamp}_p1.csv
        parts = csv_file.stem.split("_")
        # Model ID is typically the part before the timestamp (8 digits starting with year)
        model_id = None
        for i, part in enumerate(parts):
            if part.isdigit() and len(part) >= 6:  # Model IDs are typically 6+ digits
                # Check if next part looks like a timestamp (starts with 2025, etc.)
                if i + 1 < len(parts) and parts[i + 1].startswith("202"):
                    model_id = part
                    break

        if not model_id:
            continue

        if model_id not in completed:
            completed[model_id] = set()

        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    fan_id = row.get('fan_id')
                    if fan_id:
                        completed[model_id].add(fan_id)
        except Exception as e:
            print(f"  WARNING: Could not read {csv_file.name}: {e}")

    return completed


def load_checkpoint() -> Dict[str, Any]:
    """Load checkpoint from file and sync with CSV files.

    When sharding, also loads the main (non-sharded) checkpoint to preserve
    progress from previous non-sharded runs.

    Additionally syncs with CSV files to recover from crashes where
    checkpoint wasn't updated but CSVs were written.
    """
    result = {"completed_fans": {}, "last_updated": None}

    # If sharding, first load the main checkpoint to preserve old progress
    if SHARD_ID is not None:
        main_checkpoint_path = Path(f"{OUTPUT_DIR}/checkpoint.json")
        if main_checkpoint_path.exists():
            try:
                with open(main_checkpoint_path, 'r') as f:
                    main_data = json.load(f)
                    # Merge completed fans from main checkpoint
                    for model_id, fans in main_data.get("completed_fans", {}).items():
                        result["completed_fans"][model_id] = list(fans)
                    print(f"  Loaded {sum(len(v) for v in result['completed_fans'].values())} fans from main checkpoint")
            except Exception as e:
                print(f"  WARNING: Could not load main checkpoint: {e}")

    # Then load shard-specific checkpoint (or main if not sharding)
    checkpoint_path = Path(get_checkpoint_file())
    if checkpoint_path.exists():
        try:
            with open(checkpoint_path, 'r') as f:
                shard_data = json.load(f)
                # Merge completed fans from shard checkpoint
                for model_id, fans in shard_data.get("completed_fans", {}).items():
                    if model_id not in result["completed_fans"]:
                        result["completed_fans"][model_id] = []
                    # Add fans not already in list
                    existing = set(result["completed_fans"][model_id])
                    for fan in fans:
                        if fan not in existing:
                            result["completed_fans"][model_id].append(fan)
                result["last_updated"] = shard_data.get("last_updated")
        except Exception as e:
            print(f"  WARNING: Could not load checkpoint: {e}")

    # Merge per-model checkpoint files (checkpoint_{model_id}.json)
    output_path = Path(OUTPUT_DIR)
    model_additions = 0
    for mcp_file in output_path.glob("checkpoint_*.json"):
        # Skip shard files, per-process files, and main checkpoint
        filename = mcp_file.name
        if filename == "checkpoint.json" or filename.startswith("checkpoint_shard") or filename.startswith("checkpoint_p"):
            continue

        try:
            with open(mcp_file, 'r') as f:
                mcp_data = json.load(f)
                # New format: {"model_id": "123", "completed_fans": [...]}
                model_id = mcp_data.get("model_id")
                fans = mcp_data.get("completed_fans", [])

                if model_id and fans:
                    if model_id not in result["completed_fans"]:
                        result["completed_fans"][model_id] = []
                    existing = set(result["completed_fans"][model_id])
                    for fan in fans:
                        if fan not in existing:
                            result["completed_fans"][model_id].append(fan)
                            model_additions += 1
        except Exception as e:
            print(f"  WARNING: Could not load {mcp_file.name}: {e}")

    if model_additions > 0:
        print(f"  Merged {model_additions} fans from per-model checkpoints")

    # Legacy: Merge old per-process checkpoint files (checkpoint_p1.json, etc.)
    process_additions = 0
    for pcp_file in output_path.glob("checkpoint_p*.json"):
        try:
            with open(pcp_file, 'r') as f:
                pcp_data = json.load(f)
                for model_id, fans in pcp_data.get("completed_fans", {}).items():
                    if model_id not in result["completed_fans"]:
                        result["completed_fans"][model_id] = []
                    existing = set(result["completed_fans"][model_id])
                    for fan in fans:
                        if fan not in existing:
                            result["completed_fans"][model_id].append(fan)
                            process_additions += 1
        except Exception as e:
            print(f"  WARNING: Could not load {pcp_file.name}: {e}")

    if process_additions > 0:
        print(f"  Merged {process_additions} fans from legacy per-process checkpoints")

    # Sync with CSV files to recover any fans processed but not checkpointed
    csv_completed = sync_checkpoint_from_csvs()
    csv_additions = 0
    for model_id, csv_fans in csv_completed.items():
        if model_id not in result["completed_fans"]:
            result["completed_fans"][model_id] = []
        existing = set(result["completed_fans"][model_id])
        new_fans = csv_fans - existing
        if new_fans:
            result["completed_fans"][model_id].extend(list(new_fans))
            csv_additions += len(new_fans)

    if csv_additions > 0:
        print(f"  Synced {csv_additions} fans from CSV files (recovered from incomplete checkpoint)")

    # Save updated checkpoint if there were any additions
    if process_additions > 0 or csv_additions > 0:
        save_checkpoint(result)

    return result


def save_checkpoint(checkpoint: Dict[str, Any]):
    """Save checkpoint to file."""
    checkpoint_path = Path(get_checkpoint_file())
    checkpoint_path.parent.mkdir(exist_ok=True)
    checkpoint["last_updated"] = datetime.now().isoformat()
    try:
        with open(checkpoint_path, 'w') as f:
            json.dump(checkpoint, f, indent=2)
    except Exception as e:
        print(f"  WARNING: Could not save checkpoint: {e}")


def clear_checkpoint():
    """Clear checkpoint file after successful completion."""
    checkpoint_path = Path(get_checkpoint_file())
    if checkpoint_path.exists():
        checkpoint_path.unlink()
        print("  Checkpoint cleared.")


async def load_credentials_from_db() -> List[Dict[str, Any]]:
    """Load credentials from database."""
    loader = CredentialLoader()
    data = await loader.get_active_credentials()

    accounts = []
    for account in data.get("accounts", []):
        if account.get("active", False):
            account_name = account.get("name", "Unknown")
            # Check if model should be included (ONLY_MODELS takes precedence)
            if ONLY_MODELS:
                include = False
                for only_name in ONLY_MODELS:
                    if only_name.lower() in account_name.lower():
                        include = True
                        break
                if not include:
                    print(f"  Skipping {account_name} (not in ONLY_MODELS)")
                    continue
            # Check if model should be skipped
            elif SKIP_MODELS:
                skip = False
                for skip_name in SKIP_MODELS:
                    if skip_name.lower() in account_name.lower():
                        print(f"  Skipping {account_name} (in SKIP_MODELS)")
                        skip = True
                        break
                if skip:
                    continue
            accounts.append({
                "name": account_name,
                "auth": account.get("auth", {})
            })

    return accounts


def load_fans_from_csv(csv_path: str) -> List[Dict[str, Any]]:
    """Load fans from CSV file."""
    fans = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            fans.append({
                "fan_id": row.get("fan_id"),
                "username": row.get("username", f"fan_{row.get('fan_id')}"),
                "display_name": row.get("display_name", ""),
                "model_id": row.get("model_id"),
            })
    return fans


def find_fans_csv(model_id: str) -> Optional[str]:
    """Find the most recent fans CSV for a model."""
    fans_dir = Path("fans_export")
    if not fans_dir.exists():
        return None

    # Look for CSV files matching pattern
    pattern = f"fans_*_{model_id}_*.csv"
    csv_files = list(fans_dir.glob(pattern))

    if not csv_files:
        return None

    # Return most recent
    return str(max(csv_files, key=lambda p: p.stat().st_mtime))


class MessageFetcher:
    """Fetches messages using manual while-loop pagination."""

    def __init__(self, authed, account_name: str):
        self.authed = authed
        self.account_name = account_name
        self.api_base = "https://onlyfans.com/api2/v2"
        self.requester = authed.get_requester()

    async def fetch_fan_messages(
        self,
        fan_id: str,
        fan_username: str,
        progress_callback: Optional[callable] = None
    ) -> Dict[str, Any]:
        """Fetch all messages for a single fan with auto-retry.

        :param fan_id: Fan ID to fetch
        :param fan_username: Fan username for logging
        :param progress_callback: Optional callback for progress updates
        :return: Dict with messages and stats
        """
        start_time = time.time()
        all_messages: List[Dict] = []
        offset_id: Optional[str] = None
        batch_num = 0
        has_more = True
        retries = 0
        consecutive_errors = 0

        try:
            while has_more:
                batch_num += 1
                url = f"{self.api_base}/chats/{fan_id}/messages?limit={BATCH_SIZE}&order=desc"
                if offset_id:
                    url += f"&id={offset_id}"

                try:
                    response = await self.requester.json_request(url)
                    consecutive_errors = 0  # Reset on success
                except Exception as batch_error:
                    consecutive_errors += 1
                    error_type = type(batch_error).__name__

                    # Check if retryable error
                    retryable = any(x in error_type.lower() or x in str(batch_error).lower()
                                    for x in ['timeout', 'connection', 'network', 'reset', 'refused'])

                    if retryable and consecutive_errors <= MAX_RETRIES:
                        wait_time = RETRY_BACKOFF ** consecutive_errors
                        print(f"    [{self.account_name}] {fan_username}: Retry {consecutive_errors}/{MAX_RETRIES} after {error_type}, waiting {wait_time:.1f}s...")
                        await asyncio.sleep(wait_time)
                        retries += 1
                        batch_num -= 1  # Don't count failed batch
                        continue
                    else:
                        raise batch_error

                messages = response.get("list", [])
                has_more = response.get("hasMore", False)

                if not messages:
                    break

                all_messages.extend(messages)
                offset_id = str(messages[-1]["id"]) if messages else None

                # Progress callback
                if progress_callback:
                    progress_callback(fan_id, len(all_messages), batch_num)

            duration = time.time() - start_time

            return {
                "fan_id": fan_id,
                "fan_username": fan_username,
                "success": True,
                "message_count": len(all_messages),
                "messages": all_messages,
                "duration": duration,
                "batches": batch_num,
                "retries": retries,
                "throughput": len(all_messages) / duration if duration > 0 else 0
            }

        except asyncio.CancelledError:
            duration = time.time() - start_time
            return {
                "fan_id": fan_id,
                "fan_username": fan_username,
                "success": False,
                "message_count": len(all_messages),
                "messages": all_messages,
                "duration": duration,
                "error": "Cancelled",
                "batches": batch_num,
                "retries": retries,
                "partial": len(all_messages) > 0
            }

        except Exception as e:
            duration = time.time() - start_time
            return {
                "fan_id": fan_id,
                "fan_username": fan_username,
                "success": False,
                "message_count": len(all_messages),
                "messages": all_messages,
                "duration": duration,
                "error": f"{type(e).__name__}: {str(e)[:200]}",
                "batches": batch_num,
                "retries": retries,
                "partial": len(all_messages) > 0
            }


class MessageExporter:
    """Exports messages to CSV incrementally."""

    def __init__(self, output_dir: str, model_id: str, account_name: str, resume: bool = False):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.model_id = model_id
        self.account_name = account_name
        self.resume = resume

        safe_name = account_name.replace(" ", "_").replace("-", "_")

        # Try to find existing files if resuming
        existing_files = self._find_existing_files(safe_name, model_id) if resume else None

        # Shard suffix for filenames (empty if no sharding)
        shard_suffix = f"_shard{SHARD_ID}" if SHARD_ID else ""

        if existing_files:
            # Use existing files
            self.messages_file = existing_files.get("messages")
            self.summary_file = existing_files.get("summary")
            self.bundles_file = existing_files.get("bundles")
            self.bundle_items_file = existing_files.get("bundle_items")
            print(f"  [{account_name}] Resuming - appending to existing CSV files")
        else:
            # Create new files with timestamp (and shard suffix if sharding)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.messages_file = self.output_dir / f"messages_{safe_name}_{model_id}_{timestamp}{shard_suffix}.csv"
            self.summary_file = self.output_dir / f"fetch_summary_{safe_name}_{model_id}_{timestamp}{shard_suffix}.csv"
            self.bundles_file = self.output_dir / f"bundles_{safe_name}_{model_id}_{timestamp}{shard_suffix}.csv"
            self.bundle_items_file = self.output_dir / f"bundle_items_{safe_name}_{model_id}_{timestamp}{shard_suffix}.csv"

        self.messages_writer = None
        self.messages_fh = None
        self.summary_writer = None
        self.summary_fh = None
        self.bundles_writer = None
        self.bundles_fh = None
        self.bundle_items_writer = None
        self.bundle_items_fh = None

        # Track seen bundle_ids for deduplication (same content = same bundle)
        self.seen_bundle_ids: set = set()

        self._init_files(append=existing_files is not None)

    def _find_existing_files(self, safe_name: str, model_id: str) -> Optional[Dict[str, Path]]:
        """Find most recent existing CSV files for this model (and shard if sharding).

        :param safe_name: Sanitized account name
        :param model_id: Model ID
        :return: Dict with file paths or None if not found
        """
        # Look for existing files matching pattern (with shard suffix if sharding)
        shard_suffix = f"_shard{SHARD_ID}" if SHARD_ID else ""
        messages_pattern = f"messages_{safe_name}_{model_id}_*{shard_suffix}.csv"
        messages_files = list(self.output_dir.glob(messages_pattern))

        if not messages_files:
            return None

        # Get most recent messages file
        latest_messages = max(messages_files, key=lambda p: p.stat().st_mtime)

        # Extract timestamp from filename
        # Format without shard: messages_Account_12345_20251202_171850.csv
        # Format with shard: messages_Account_12345_20251202_171850_shard1.csv
        stem_parts = latest_messages.stem.split("_")
        if SHARD_ID:
            # Remove shard suffix, then get timestamp
            timestamp = "_".join(stem_parts[-3:-1])  # Get the two parts before _shardN
        else:
            timestamp = "_".join(stem_parts[-2:])

        # Build paths for all related files
        result = {
            "messages": latest_messages,
            "summary": self.output_dir / f"fetch_summary_{safe_name}_{model_id}_{timestamp}{shard_suffix}.csv",
            "bundles": self.output_dir / f"bundles_{safe_name}_{model_id}_{timestamp}{shard_suffix}.csv",
            "bundle_items": self.output_dir / f"bundle_items_{safe_name}_{model_id}_{timestamp}{shard_suffix}.csv",
        }

        # Verify all files exist
        for key, path in result.items():
            if not path.exists():
                print(f"  [{self.account_name}] WARNING: Missing {key} file, creating new set")
                return None

        return result

    def _init_files(self, append: bool = False):
        """Initialize CSV files with headers.

        :param append: If True, open files in append mode and skip headers
        """
        mode = 'a' if append else 'w'

        # If appending, load existing bundle_ids first
        if append and self.bundles_file.exists():
            self._load_existing_bundle_ids()

        # Messages file - matches database schema
        self.messages_fh = open(self.messages_file, mode, newline='', encoding='utf-8')
        self.messages_writer = csv.writer(self.messages_fh)
        if not append:
            self.messages_writer.writerow([
                'message_id', 'model_id', 'fan_id', 'sender_id', 'model_name',
                'sender_username', 'message', 'message_type', 'media_id', 'media_type',
                'price', 'is_free', 'is_purchased', 'is_from_me', 'created_at', 'fetched_at'
            ])

        # Summary file
        self.summary_fh = open(self.summary_file, mode, newline='', encoding='utf-8')
        self.summary_writer = csv.writer(self.summary_fh)
        if not append:
            self.summary_writer.writerow([
                'fan_id', 'fan_username', 'success', 'message_count',
                'duration', 'throughput', 'batches', 'error', 'fetched_at'
            ])

        # Bundles file - master bundle table (one row per unique bundle)
        self.bundles_fh = open(self.bundles_file, mode, newline='', encoding='utf-8')
        self.bundles_writer = csv.writer(self.bundles_fh)
        if not append:
            self.bundles_writer.writerow([
                'bundle_id', 'message_id', 'creator_id', 'creator_username',
                'total_price', 'media_count', 'photo_count', 'video_count', 'audio_count',
                'is_mass_message', 'queue_id', 'name', 'description',
                'created_at', 'first_seen_at'
            ])

        # Bundle Items file - media items within bundles
        self.bundle_items_fh = open(self.bundle_items_file, mode, newline='', encoding='utf-8')
        self.bundle_items_writer = csv.writer(self.bundle_items_fh)
        if not append:
            self.bundle_items_writer.writerow([
                'bundle_id', 'media_id', 'media_type', 'duration', 'created_at'
            ])

    def _load_existing_bundle_ids(self):
        """Load existing bundle_ids from bundles file to avoid duplicates."""
        try:
            with open(self.bundles_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    bundle_id = row.get('bundle_id')
                    if bundle_id:
                        self.seen_bundle_ids.add(bundle_id)
            if self.seen_bundle_ids:
                print(f"  [{self.account_name}] Loaded {len(self.seen_bundle_ids)} existing bundle_ids")
        except Exception as e:
            print(f"  [{self.account_name}] WARNING: Could not load existing bundle_ids: {e}")

    def write_messages(self, fan_id: str, model_id: str, messages: List[Dict], model_name: str = ""):
        """Write messages for a fan to CSV matching database schema.

        For messages with multiple media items (len(media) > 1), computes bundle_id
        from all media IDs using deterministic MD5 hash. This ensures the same content
        always gets the same bundle_id across all fans and sessions.

        Also saves bundle metadata to bundles.csv and media items to bundle_items.csv.
        Bundle deduplication: same bundle_id is only saved once (first occurrence).
        """
        fetched_at = datetime.now().isoformat()

        for msg in messages:
            # Extract sender info
            from_user = msg.get("fromUser", {}) or {}
            sender_id = from_user.get("id", "")
            sender_username = from_user.get("username", f"u{sender_id}")

            # Determine if from model (is_from_me means sent BY the model)
            is_from_me = str(sender_id) == str(model_id)

            # Extract media info
            media = msg.get("media", []) or []
            price = msg.get("price", 0) or 0

            # Determine media_id and media_type based on content
            # Bundle = multiple media items (regardless of price)
            if len(media) > 1:
                # Bundle: Compute bundle_id from ALL media IDs
                media_ids = [str(m.get("id", "")) for m in media if m.get("id")]
                bundle_id = generate_bundle_id(media_ids) or ""
                media_id = bundle_id
                media_type = "bundle"
                message_type = "bundle"

                # Save bundle data if not seen before (deduplication)
                if bundle_id and bundle_id not in self.seen_bundle_ids:
                    self._write_bundle(msg, bundle_id, media, sender_id, sender_username, fetched_at)
                    self.seen_bundle_ids.add(bundle_id)

            elif media:
                # Single media item: use its ID directly
                media_id = str(media[0].get("id", "")) if media else ""
                media_type = media[0].get("type", "") if media else ""
                message_type = media[0].get("type", "text")
            else:
                # Text-only message
                media_id = ""
                media_type = ""
                message_type = "text"

            # Purchase info
            is_free = msg.get("isFree", True)
            is_purchased = msg.get("isPaid", False) or msg.get("isOpened", False)

            # Sanitize message text - strip HTML, decode entities, remove newlines
            message_text = msg.get("text", "") or ""
            message_text = re.sub(r'<[^>]+>', '', message_text)  # Strip HTML tags
            message_text = html.unescape(message_text)  # Decode &amp; &lt; etc.
            message_text = message_text.replace("\n", " ").replace("\r", "")  # Single-row

            self.messages_writer.writerow([
                msg.get("id"),           # message_id
                model_id,                # model_id
                fan_id,                  # fan_id
                sender_id,               # sender_id
                model_name,              # model_name
                sender_username,         # sender_username
                message_text,            # message (newlines removed)
                message_type,            # message_type
                media_id,                # media_id (bundle_id for PPV, first media_id otherwise)
                media_type,              # media_type ('bundle' for PPV)
                price,                   # price
                is_free,                 # is_free
                is_purchased,            # is_purchased
                is_from_me,              # is_from_me
                msg.get("createdAt"),    # created_at
                fetched_at               # fetched_at
            ])

        self.messages_fh.flush()

    def _write_bundle(
        self,
        msg: Dict,
        bundle_id: str,
        media: List[Dict],
        creator_id: str,
        creator_username: str,
        fetched_at: str
    ):
        """Write bundle metadata and items to their respective CSV files.

        :param msg: Original message dict from API
        :param bundle_id: Computed deterministic bundle_id
        :param media: List of media dicts
        :param creator_id: ID of the creator who sent the bundle
        :param creator_username: Username of the creator
        :param fetched_at: Timestamp when this was fetched
        """
        # Count media types
        photo_count = sum(1 for m in media if m.get("type") == "photo")
        video_count = sum(1 for m in media if m.get("type") == "video")
        audio_count = sum(1 for m in media if m.get("type") == "audio")

        # Extract bundle metadata
        price = msg.get("price", 0) or 0
        queue_id = msg.get("queueId") or ""
        is_mass_message = msg.get("isFromQueue", False) or bool(queue_id)
        created_at = msg.get("createdAt", "")

        # Sanitize name and description from message text
        message_text = msg.get("text", "") or ""
        message_text = re.sub(r'<[^>]+>', '', message_text)  # Strip HTML tags
        message_text = html.unescape(message_text)  # Decode entities
        bundle_name = message_text[:100].replace("\n", " ").replace("\r", "")
        bundle_description = message_text.replace("\n", " ").replace("\r", "")

        # Write to bundles.csv
        self.bundles_writer.writerow([
            bundle_id,              # bundle_id
            str(msg.get("id", "")), # message_id (first message where this bundle appeared)
            str(creator_id),        # creator_id
            creator_username,       # creator_username
            price,                  # total_price
            len(media),             # media_count
            photo_count,            # photo_count
            video_count,            # video_count
            audio_count,            # audio_count
            is_mass_message,        # is_mass_message
            queue_id,               # queue_id
            bundle_name,            # name (truncated message text)
            bundle_description,     # description (full message text)
            created_at,             # created_at
            fetched_at              # first_seen_at
        ])
        self.bundles_fh.flush()

        # Write each media item to bundle_items.csv
        for m in media:
            media_id = str(m.get("id", ""))
            if not media_id:
                continue

            self.bundle_items_writer.writerow([
                bundle_id,                  # bundle_id
                media_id,                   # media_id
                m.get("type", "photo"),     # media_type
                m.get("duration", 0) if m.get("type") in ["video", "audio"] else "",  # duration
                created_at                  # created_at (same as bundle)
            ])

        self.bundle_items_fh.flush()

    def write_summary(self, result: Dict[str, Any]):
        """Write fetch summary for a fan."""
        self.summary_writer.writerow([
            result.get("fan_id"),
            result.get("fan_username"),
            result.get("success"),
            result.get("message_count"),
            round(result.get("duration", 0), 2),
            round(result.get("throughput", 0), 2),
            result.get("batches"),
            result.get("error", ""),
            datetime.now().isoformat()
        ])
        self.summary_fh.flush()

    def close(self):
        """Close file handles and print bundle stats."""
        if self.messages_fh:
            self.messages_fh.close()
        if self.summary_fh:
            self.summary_fh.close()
        if self.bundles_fh:
            self.bundles_fh.close()
        if self.bundle_items_fh:
            self.bundle_items_fh.close()

        # Print bundle stats
        if self.seen_bundle_ids:
            print(f"  [{self.account_name}] Unique bundles saved: {len(self.seen_bundle_ids)}")

    def get_bundle_count(self) -> int:
        """Return count of unique bundles seen."""
        return len(self.seen_bundle_ids)


def merge_process_files(model_id: str, account_name: str, output_dir: str = OUTPUT_DIR):
    """Merge process-specific CSV files into parent CSVs after model completes.

    Finds all _p1, _p2, _p3, _p4 files for the model and merges them into parent
    files. Deduplicates based on:
    - messages: message_id
    - fetch_summary: fan_id
    - bundles: bundle_id
    - bundle_items: bundle_id + media_id

    :param model_id: Model ID to merge files for
    :param account_name: Account name for file matching
    :param output_dir: Output directory containing CSV files
    """
    safe_name = account_name.replace(" ", "_").replace("-", "_")
    output_path = Path(output_dir)

    file_configs = [
        {"type": "messages", "dedupe_cols": ["message_id"]},
        {"type": "fetch_summary", "dedupe_cols": ["fan_id"]},
        {"type": "bundles", "dedupe_cols": ["bundle_id"]},
        {"type": "bundle_items", "dedupe_cols": ["bundle_id", "media_id"]},
    ]

    print(f"\n  [{account_name}] Merging process files...")

    for config in file_configs:
        file_type = config["type"]
        dedupe_cols = config["dedupe_cols"]

        # Find process files (pattern: type_account_modelid_timestamp_p1.csv)
        pattern = f"{file_type}_{safe_name}_{model_id}_*_p*.csv"
        process_files = list(output_path.glob(pattern))

        if not process_files:
            continue

        # Find existing parent file (no _p suffix)
        parent_pattern = f"{file_type}_{safe_name}_{model_id}_*.csv"
        parent_candidates = [
            f for f in output_path.glob(parent_pattern)
            if not re.search(r'_p\d+\.csv$', str(f))
        ]

        # Read all process files using pandas
        dfs = []
        process_rows = 0
        for pf in process_files:
            try:
                df = pd.read_csv(pf)
                process_rows += len(df)
                dfs.append(df)
            except Exception as e:
                print(f"    WARNING: Could not read {pf.name}: {e}")

        if not dfs:
            continue

        # Read parent file if exists
        parent_file = None
        parent_count = 0
        if parent_candidates:
            parent_file = max(parent_candidates, key=lambda p: p.stat().st_mtime)
            try:
                parent_df = pd.read_csv(parent_file)
                parent_count = len(parent_df)
                dfs.insert(0, parent_df)
            except Exception as e:
                print(f"    WARNING: Could not read parent {parent_file.name}: {e}")

        # Combine all dataframes
        combined = pd.concat(dfs, ignore_index=True)
        before_count = len(combined)

        # Deduplicate (keep last = most recent)
        combined = combined.drop_duplicates(subset=dedupe_cols, keep="last")
        after_count = len(combined)

        # Create parent file if needed
        if not parent_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            parent_file = output_path / f"{file_type}_{safe_name}_{model_id}_{timestamp}.csv"

        # Save combined to parent
        combined.to_csv(parent_file, index=False)

        print(f"    {file_type}: {len(process_files)} process files ({process_rows} rows) + parent ({parent_count}) -> {after_count} (deduped {before_count - after_count})")

        # Delete process files
        for pf in process_files:
            try:
                pf.unlink()
            except Exception as e:
                print(f"    WARNING: Could not delete {pf.name}: {e}")

    print(f"  [{account_name}] Merge complete")


def save_model_checkpoint(model_id: str, completed_fans: List[str], account_name: str = ""):
    """Save per-model checkpoint file.

    Each model saves its own checkpoint_{model_id}.json file to avoid race conditions
    when multiple models are processed in parallel.

    :param model_id: Model ID being processed
    :param completed_fans: List of completed fan IDs for this model
    :param account_name: Account name for logging (optional)
    """
    checkpoint_file = Path(OUTPUT_DIR) / f"checkpoint_{model_id}.json"
    checkpoint_file.parent.mkdir(exist_ok=True)

    checkpoint_data = {
        "model_id": model_id,
        "completed_fans": completed_fans,
        "count": len(completed_fans),
        "last_updated": datetime.now().isoformat()
    }

    try:
        with open(checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f, indent=2)
    except Exception as e:
        log_prefix = f"[{account_name}]" if account_name else f"[{model_id}]"
        print(f"  {log_prefix} WARNING: Could not save checkpoint: {e}")


def process_model_with_multiprocessing(
    model: Dict[str, Any],
    checkpoint_completed_fans: Dict[str, List[str]]
) -> Dict[str, Any]:
    """Process a single model using multiprocessing workers.

    Must be at module level for Windows multiprocessing compatibility.

    :param model: Model dict with account_name, model_id, auth_data, fans
    :param checkpoint_completed_fans: Dict of model_id -> list of completed fan_ids
    :return: Dict with processing results
    """
    account_name = model["account_name"]
    model_id = model["model_id"]
    auth_data = model["auth_data"]
    fans = model["fans"]

    # Filter out already completed fans from checkpoint
    completed_fan_ids = set(checkpoint_completed_fans.get(model_id, []))
    remaining_fans = [f for f in fans if f["fan_id"] not in completed_fan_ids]
    skipped_count = len(fans) - len(remaining_fans)

    if skipped_count > 0:
        print(f"\n  [{account_name}] Resuming - {len(remaining_fans)} remaining ({skipped_count} already done)")
    else:
        print(f"\n  [{account_name}] Starting - {len(fans)} fans")

    if not remaining_fans:
        print(f"  [{account_name}] All fans already completed!")
        return {
            "model_id": model_id,
            "account_name": account_name,
            "fans_processed": len(fans),
            "success_count": len(fans),
            "total_messages": 0,
            "duration": 0,
            "skipped": skipped_count
        }

    print(f"  [{account_name}] Launching {NUM_PROCESSES} processes with {NUM_WORKERS} workers each...")

    # Distribute fans across processes using modulo
    process_fans = [[] for _ in range(NUM_PROCESSES)]
    for i, fan in enumerate(remaining_fans):
        process_fans[i % NUM_PROCESSES].append(fan)

    for p_id, p_fans in enumerate(process_fans, 1):
        print(f"    Process {p_id}: {len(p_fans)} fans")

    # Create result queue and spawn processes
    result_queue = multiprocessing.Queue()
    processes = []
    start_time = time.time()

    for p_id in range(1, NUM_PROCESSES + 1):
        p = multiprocessing.Process(
            target=process_worker,
            args=(p_id, auth_data, model_id, account_name, process_fans[p_id - 1], completed_fan_ids, result_queue)
        )
        processes.append(p)
        p.start()

    # Wait for all processes to complete
    for p in processes:
        p.join()

    # Collect results from queue
    total_messages = 0
    all_completed_fans = []
    total_bundles = 0

    while not result_queue.empty():
        result = result_queue.get()
        if result.get("success"):
            total_messages += result.get("total_messages", 0)
            all_completed_fans.extend(result.get("completed_fans", []))
            total_bundles += result.get("bundles", 0)
        else:
            print(f"  [P{result.get('process_id')}] ERROR: {result.get('error')}")

    duration = time.time() - start_time
    success_count = len(all_completed_fans) + skipped_count

    print(f"\n  [{account_name}] COMPLETE: {success_count}/{len(fans)} fans, {total_messages} msgs, {total_bundles} bundles in {duration/60:.1f}m")

    return {
        "model_id": model_id,
        "account_name": account_name,
        "fans_processed": len(fans),
        "success_count": success_count,
        "total_messages": total_messages,
        "duration": duration,
        "throughput": total_messages / duration if duration > 0 else 0,
        "skipped": skipped_count,
        "bundles": total_bundles
    }


def model_process_wrapper(model: Dict[str, Any], checkpoint_completed_fans: Dict[str, List[str]], result_queue):
    """Wrapper that runs in separate process and puts result in queue.

    Must be at module level for Windows multiprocessing compatibility.

    :param model: Model dict with account_name, model_id, auth_data, fans
    :param checkpoint_completed_fans: Dict of model_id -> list of completed fan_ids
    :param result_queue: Multiprocessing Queue to put results
    """
    try:
        result = process_model_with_multiprocessing(model, checkpoint_completed_fans)
        result_queue.put(result)
    except Exception as e:
        print(f"  [{model['account_name']}] ERROR: {str(e)}")
        result_queue.put({
            "account_name": model["account_name"],
            "error": str(e)
        })


def process_worker(
    process_id: int,
    auth_data: Dict[str, Any],
    model_id: str,
    account_name: str,
    fans: List[Dict[str, Any]],
    completed_fan_ids: set,
    result_queue: multiprocessing.Queue
):
    """Worker function that runs in a separate process.

    Each process:
    1. Authenticates with its own API session
    2. Processes its assigned fans with NUM_WORKERS async workers
    3. Writes to process-specific CSV files
    4. Saves per-process checkpoint after each fan
    5. Reports results back via queue

    :param process_id: Process number (1-4)
    :param auth_data: Authentication credentials
    :param model_id: Model ID
    :param account_name: Account name for logging
    :param fans: List of fans assigned to this process
    :param completed_fan_ids: Set of already completed fan IDs
    :param result_queue: Queue to send results back to main process
    """
    # Filter out already completed fans
    remaining_fans = [f for f in fans if f["fan_id"] not in completed_fan_ids]

    async def async_worker():
        """Async worker that runs inside the process."""
        # Authenticate in this process
        try:
            config = UltimaScraperAPIConfig()
            if USE_PROXY and PROXY_URLS:
                config.settings.network.proxies = PROXY_URLS

            api = select_api("onlyfans", config=config)
            authed = await api.login(auth_data)

            if not authed or not authed.is_authed():
                result_queue.put({
                    "process_id": process_id,
                    "success": False,
                    "error": "Authentication failed",
                    "completed_fans": [],
                    "total_messages": 0
                })
                return
        except Exception as e:
            result_queue.put({
                "process_id": process_id,
                "success": False,
                "error": f"Auth error: {str(e)[:100]}",
                "completed_fans": [],
                "total_messages": 0
            })
            return

        # Initialize fetcher and exporter with process suffix
        fetcher = MessageFetcher(authed, f"{account_name}-P{process_id}")

        # Use process-specific output suffix
        safe_name = account_name.replace(" ", "_").replace("-", "_")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        process_suffix = f"_p{process_id}"

        output_dir = Path(OUTPUT_DIR)
        output_dir.mkdir(exist_ok=True)

        messages_file = output_dir / f"messages_{safe_name}_{model_id}_{timestamp}{process_suffix}.csv"
        summary_file = output_dir / f"fetch_summary_{safe_name}_{model_id}_{timestamp}{process_suffix}.csv"
        bundles_file = output_dir / f"bundles_{safe_name}_{model_id}_{timestamp}{process_suffix}.csv"
        bundle_items_file = output_dir / f"bundle_items_{safe_name}_{model_id}_{timestamp}{process_suffix}.csv"

        # Use simple exporter (not class to avoid pickle issues)
        messages_fh = open(messages_file, 'w', newline='', encoding='utf-8')
        messages_writer = csv.writer(messages_fh)
        messages_writer.writerow([
            'message_id', 'model_id', 'fan_id', 'sender_id', 'model_name',
            'sender_username', 'message', 'message_type', 'media_id', 'media_type',
            'price', 'is_free', 'is_purchased', 'is_from_me', 'created_at', 'fetched_at'
        ])

        summary_fh = open(summary_file, 'w', newline='', encoding='utf-8')
        summary_writer = csv.writer(summary_fh)
        summary_writer.writerow([
            'fan_id', 'fan_username', 'success', 'message_count',
            'duration', 'throughput', 'batches', 'error', 'fetched_at'
        ])

        bundles_fh = open(bundles_file, 'w', newline='', encoding='utf-8')
        bundles_writer = csv.writer(bundles_fh)
        bundles_writer.writerow([
            'bundle_id', 'message_id', 'creator_id', 'creator_username',
            'total_price', 'media_count', 'photo_count', 'video_count', 'audio_count',
            'is_mass_message', 'queue_id', 'name', 'description',
            'created_at', 'first_seen_at'
        ])

        bundle_items_fh = open(bundle_items_file, 'w', newline='', encoding='utf-8')
        bundle_items_writer = csv.writer(bundle_items_fh)
        bundle_items_writer.writerow([
            'bundle_id', 'media_id', 'media_type', 'duration', 'created_at'
        ])

        seen_bundle_ids = set()
        completed_fans_list = []
        total_messages = 0
        completed = 0
        semaphore = asyncio.Semaphore(NUM_WORKERS)

        async def process_fan(fan: Dict[str, Any]):
            nonlocal completed, total_messages

            async with semaphore:
                fan_id = fan["fan_id"]
                fan_username = fan.get("username", f"fan_{fan_id}")

                result = await fetcher.fetch_fan_messages(fan_id, fan_username, None)

                # Write messages
                if result.get("messages"):
                    fetched_at = datetime.now().isoformat()
                    for msg in result["messages"]:
                        from_user = msg.get("fromUser", {}) or {}
                        sender_id = from_user.get("id", "")
                        sender_username = from_user.get("username", f"u{sender_id}")
                        is_from_me = str(sender_id) == str(model_id)
                        media = msg.get("media", []) or []
                        price = msg.get("price", 0) or 0

                        if len(media) > 1:
                            media_ids = [str(m.get("id", "")) for m in media if m.get("id")]
                            bundle_id = generate_bundle_id(media_ids) or ""
                            media_id = bundle_id
                            media_type = "bundle"
                            message_type = "bundle"

                            if bundle_id and bundle_id not in seen_bundle_ids:
                                # Write bundle
                                photo_count = sum(1 for m in media if m.get("type") == "photo")
                                video_count = sum(1 for m in media if m.get("type") == "video")
                                audio_count = sum(1 for m in media if m.get("type") == "audio")
                                queue_id = msg.get("queueId") or ""
                                is_mass = msg.get("isFromQueue", False) or bool(queue_id)
                                msg_text = msg.get("text", "") or ""
                                msg_text = re.sub(r'<[^>]+>', '', msg_text)
                                msg_text = html.unescape(msg_text)
                                bundle_name = msg_text[:100].replace("\n", " ").replace("\r", "")

                                bundles_writer.writerow([
                                    bundle_id, str(msg.get("id", "")), str(sender_id), sender_username,
                                    price, len(media), photo_count, video_count, audio_count,
                                    is_mass, queue_id, bundle_name, msg_text.replace("\n", " "),
                                    msg.get("createdAt"), fetched_at
                                ])

                                for m in media:
                                    mid = str(m.get("id", ""))
                                    if mid:
                                        bundle_items_writer.writerow([
                                            bundle_id, mid, m.get("type", "photo"),
                                            m.get("duration", 0) if m.get("type") in ["video", "audio"] else "",
                                            msg.get("createdAt")
                                        ])

                                seen_bundle_ids.add(bundle_id)
                        elif media:
                            media_id = str(media[0].get("id", ""))
                            media_type = media[0].get("type", "")
                            message_type = media[0].get("type", "text")
                        else:
                            media_id = ""
                            media_type = ""
                            message_type = "text"

                        is_free = msg.get("isFree", True)
                        is_purchased = msg.get("isPaid", False) or msg.get("isOpened", False)
                        message_text = msg.get("text", "") or ""
                        message_text = re.sub(r'<[^>]+>', '', message_text)
                        message_text = html.unescape(message_text)
                        message_text = message_text.replace("\n", " ").replace("\r", "")

                        messages_writer.writerow([
                            msg.get("id"), model_id, fan_id, sender_id, account_name,
                            sender_username, message_text, message_type, media_id, media_type,
                            price, is_free, is_purchased, is_from_me, msg.get("createdAt"), fetched_at
                        ])

                    messages_fh.flush()
                    bundles_fh.flush()
                    bundle_items_fh.flush()

                # Write summary
                summary_writer.writerow([
                    fan_id, fan_username, result.get("success"), result.get("message_count"),
                    round(result.get("duration", 0), 2), round(result.get("throughput", 0), 2),
                    result.get("batches"), result.get("error", ""), datetime.now().isoformat()
                ])
                summary_fh.flush()

                completed += 1
                msg_count = result.get("message_count", 0)
                total_messages += msg_count

                if result.get("success"):
                    completed_fans_list.append(fan_id)
                    # Save per-model checkpoint based on CHECKPOINT_FREQUENCY
                    if len(completed_fans_list) % CHECKPOINT_FREQUENCY == 0:
                        save_model_checkpoint(model_id, completed_fans_list, account_name)

                status = "OK" if result.get("success") else "FAIL"
                print(f"  [{account_name}] {completed}/{len(remaining_fans)} - {fan_username}: {status} ({msg_count} msgs)")

        # Process all fans
        if remaining_fans:
            tasks = [process_fan(fan) for fan in remaining_fans]
            await asyncio.gather(*tasks, return_exceptions=True)

        # Final checkpoint save
        if completed_fans_list:
            save_model_checkpoint(model_id, completed_fans_list, account_name)

        # Cleanup
        messages_fh.close()
        summary_fh.close()
        bundles_fh.close()
        bundle_items_fh.close()
        await api.close_pools()

        result_queue.put({
            "process_id": process_id,
            "success": True,
            "completed_fans": completed_fans_list,
            "total_messages": total_messages,
            "bundles": len(seen_bundle_ids)
        })

    # Run the async worker
    asyncio.run(async_worker())


async def main():
    """Main entry point."""

    # Parse command line args for sharding
    parse_args()

    proxy_status = f"ENABLED ({len(PROXY_URLS)} proxies)" if USE_PROXY else "DISABLED"
    if ONLY_MODELS:
        filter_status = f"ONLY: {', '.join(ONLY_MODELS)}"
    elif SKIP_MODELS:
        filter_status = f"SKIP: {', '.join(SKIP_MODELS)}"
    else:
        filter_status = "None (all models)"
    total_concurrent = NUM_PROCESSES * NUM_WORKERS

    print(f"""
    ======================================================================
                    FETCH ALL MESSAGES FOR ALL FANS
    ======================================================================

    Configuration:
    - Proxy:      {proxy_status}
    - Processes:  {NUM_PROCESSES} per model
    - Workers:    {NUM_WORKERS} async workers per process
    - Total:      {total_concurrent} concurrent fans per model
    - Filter:     {filter_status}
    - Output:     {OUTPUT_DIR}

    This script:
    1. Loads credentials from DATABASE (not auth_multi.json)
    2. Authenticates ALL models first
    3. Loads fans from CSV for each model
    4. Fetches ALL messages for each fan (no timeout)
    5. Saves messages incrementally to CSV
    6. Saves bundle data to bundles.csv and bundle_items.csv

    Output (to OUTPUT_DIR):
    - {OUTPUT_DIR}/messages_{{account}}_{{model_id}}_{{timestamp}}.csv
    - {OUTPUT_DIR}/fetch_summary_{{account}}_{{model_id}}_{{timestamp}}.csv
    - {OUTPUT_DIR}/bundles_{{account}}_{{model_id}}_{{timestamp}}.csv
    - {OUTPUT_DIR}/bundle_items_{{account}}_{{model_id}}_{{timestamp}}.csv
    """)

    # Step 1: Load credentials from database
    print("\n" + "="*70)
    print("STEP 1: LOADING CREDENTIALS FROM DATABASE")
    print("="*70)
    accounts = await load_credentials_from_db()
    print(f"  Found {len(accounts)} active authenticated accounts")

    if not accounts:
        print("  ERROR: No accounts found in database")
        return

    # Step 2: Authenticate ALL models first
    print("\n" + "="*70)
    print("STEP 2: AUTHENTICATING ALL MODELS")
    print("="*70)

    authenticated_models = []
    for account in accounts:
        account_name = account["name"]
        auth_data = account["auth"]
        model_id = str(auth_data.get("id"))

        print(f"\n  Authenticating: {account_name} (ID: {model_id})...")

        try:
            # Configure API with optional proxy pool
            config = UltimaScraperAPIConfig()
            if USE_PROXY and PROXY_URLS:
                config.settings.network.proxies = PROXY_URLS

            api = select_api("onlyfans", config=config)
            authed = await api.login(auth_data)

            if not authed or not authed.is_authed():
                print(f"    FAILED: Authentication returned invalid session")
                continue

            proxy_info = f" (via proxy)" if USE_PROXY else ""
            print(f"    SUCCESS: Authenticated{proxy_info}")

            authenticated_models.append({
                "account_name": account_name,
                "model_id": model_id,
                "auth_data": auth_data,
                "api": api,
                "authed": authed
            })

        except Exception as e:
            print(f"    FAILED: {type(e).__name__}: {str(e)[:100]}")
            continue

    print(f"\n  Authenticated {len(authenticated_models)}/{len(accounts)} models")

    if not authenticated_models:
        print("  ERROR: No models authenticated successfully")
        return

    # Step 3: Load fans for each model
    print("\n" + "="*70)
    print("STEP 3: LOADING FANS FROM CSV FILES")
    print("="*70)

    models_with_fans = []
    for model in authenticated_models:
        account_name = model["account_name"]
        model_id = model["model_id"]

        print(f"\n  {account_name} (ID: {model_id}):")

        # Find fans CSV
        fans_csv = find_fans_csv(model_id)
        if not fans_csv:
            print(f"    WARNING: No fans CSV found")
            print(f"    Looking for: fans_export/fans_*_{model_id}_*.csv")
            continue

        print(f"    Found: {fans_csv}")

        # Load fans
        all_fans = load_fans_from_csv(fans_csv)
        print(f"    Loaded {len(all_fans)} total fans")

        # Apply shard filtering if enabled
        fans = filter_fans_by_shard(all_fans)
        if SHARD_ID:
            print(f"    Shard {SHARD_ID}/{TOTAL_SHARDS}: {len(fans)} fans (of {len(all_fans)})")

        if not fans:
            print(f"    WARNING: No fans to process for this shard")
            continue

        model["fans"] = fans
        model["fans_csv"] = fans_csv
        models_with_fans.append(model)

    print(f"\n  {len(models_with_fans)} models have fans to process")

    if not models_with_fans:
        print("  ERROR: No models have fans to process")
        return

    # Step 4: Process models SEQUENTIALLY with MULTIPROCESSING
    print("\n" + "="*70)
    print(f"STEP 4: FETCHING MESSAGES ({NUM_PROCESSES} PROCESSES x {NUM_WORKERS} WORKERS = {NUM_PROCESSES * NUM_WORKERS} CONCURRENT)")
    print("="*70)

    # Load checkpoint once for all models
    checkpoint = load_checkpoint()
    if checkpoint.get("completed_fans"):
        print(f"\n  Checkpoint loaded: {sum(len(v) for v in checkpoint['completed_fans'].values())} fans already completed")
        print(f"  Last updated: {checkpoint.get('last_updated', 'Unknown')}")

    # Run models in PARALLEL using multiprocessing.Process
    print(f"\n  Processing {len(models_with_fans)} models in PARALLEL (multiprocessing)...")

    # Create a result queue to collect results from all processes
    model_result_queue = multiprocessing.Queue()

    # Extract completed fans dict to pass to module-level function
    checkpoint_completed_fans = checkpoint.get("completed_fans", {})

    # Spawn a process for each model
    model_processes = []
    for model in models_with_fans:
        # Create picklable version of model (exclude api/authed async objects)
        picklable_model = {
            "account_name": model["account_name"],
            "model_id": model["model_id"],
            "auth_data": model["auth_data"],
            "fans": model["fans"],
        }
        p = multiprocessing.Process(
            target=model_process_wrapper,
            args=(picklable_model, checkpoint_completed_fans, model_result_queue)
        )
        model_processes.append(p)
        p.start()
        print(f"    Started process for {model['account_name']}")

    # Wait for all model processes to complete
    for p in model_processes:
        p.join()

    # Collect results from queue
    all_results = []
    while not model_result_queue.empty():
        all_results.append(model_result_queue.get())

    # Final summary
    print("\n" + "="*70)
    print("FINAL SUMMARY")
    print("="*70)

    total_success = 0
    total_fans = 0
    total_messages_all = 0
    total_bundles_all = 0
    for result in all_results:
        if isinstance(result, dict) and not result.get('error'):
            print(f"\n  {result.get('account_name')}:")
            print(f"    Fans: {result.get('fans_processed')}")
            print(f"    Success: {result.get('success_count')}")
            if result.get('skipped', 0) > 0:
                print(f"    Skipped (from checkpoint): {result.get('skipped')}")
            print(f"    Messages: {result.get('total_messages', 0):,}")
            print(f"    Bundles: {result.get('bundles', 0):,}")
            print(f"    Duration: {result.get('duration', 0)/60:.1f} minutes")
            print(f"    Throughput: {result.get('throughput', 0):.1f} msgs/sec")

            total_success += result.get('success_count', 0)
            total_fans += result.get('fans_processed', 0)
            total_messages_all += result.get('total_messages', 0)
            total_bundles_all += result.get('bundles', 0)
        elif result.get('error'):
            print(f"\n  {result.get('account_name')}: ERROR - {result.get('error')}")

    print(f"\n  TOTALS:")
    print(f"    Messages: {total_messages_all:,}")
    print(f"    Bundles: {total_bundles_all:,}")

    # Clear checkpoint if all successful
    if total_success == total_fans and total_fans > 0:
        print("\n  All fans completed successfully!")
        clear_checkpoint()
    else:
        print(f"\n  {total_fans - total_success} fans failed - checkpoint preserved for retry")

    # Cleanup API sessions
    print("\nCleaning up API sessions...")
    for model in models_with_fans:
        try:
            await model["api"].close_pools()
        except:
            pass

    print("\n" + "="*70)
    print("COMPLETE")
    print("="*70)


if __name__ == "__main__":
    asyncio.run(main())
