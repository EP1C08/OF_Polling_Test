"""Timewaster Handler - Live API Spend Calculation and Marking.

Calculates fan spend using Live API (proven accurate), then marks timewasters:
1. Updates display name to "AI - Timewaster {original_name}"
2. Adds fan to "time waster" collection

Timewaster Criteria:
- Total spend < $50
- Total messages > 50
- RPM (Revenue Per Message) < $0.05

API endpoints from action-dispatcher service.
"""

import asyncio
import os
import time
from dataclasses import dataclass
from typing import Optional, Dict, Tuple
from urllib.parse import urljoin

from modules.message_fetcher import fetch_all_messages_fast
from modules.bundle_processor import is_bundle


@dataclass
class SpendAnalysis:
    """Result of fan spend analysis."""

    fan_id: str
    fan_username: str
    total_messages: int
    messages_from_fan: int
    messages_from_creator: int
    message_spend: float
    bundle_spend: float
    total_spend: float
    rpm: float
    is_timewaster: bool


@dataclass
class TimewasterConfig:
    """Configuration for timewaster detection thresholds."""

    max_spend: float = 50.0
    min_messages: int = 50
    max_rpm: float = 0.05
    collection_name: str = "time waster"
    display_name_prefix: str = "AI - Timewaster"


class TimewasterHandler:
    """Handles timewaster detection and marking using Live API."""

    API_BASE = "https://onlyfans.com/api2/v2/"
    MAX_RETRIES = 3
    RETRY_BACKOFF = 2.0

    def __init__(
        self,
        authed,
        model_id: str,
        model_name: str,
        logger=None,
        config: Optional[TimewasterConfig] = None
    ):
        """Initialize timewaster handler.

        :param authed: Authenticated OnlyFans API object.
        :param model_id: Creator's OnlyFans ID.
        :param model_name: Creator's display name.
        :param logger: Optional logger instance.
        :param config: Optional configuration overrides.
        """
        self.authed = authed
        self.model_id = str(model_id)
        self.model_name = model_name
        self.logger = logger
        self.config = config or self._load_config_from_env()
        self._timewaster_collection_id: Optional[str] = None

    def _load_config_from_env(self) -> TimewasterConfig:
        """Load configuration from environment variables.

        :return: TimewasterConfig with values from env or defaults.
        """
        return TimewasterConfig(
            max_spend=float(os.getenv('TW_MAX_SPEND', '50.0')),
            min_messages=int(os.getenv('TW_MIN_MESSAGES', '50')),
            max_rpm=float(os.getenv('TW_MAX_RPM', '0.05')),
            collection_name=os.getenv('TW_COLLECTION_NAME', 'time waster'),
            display_name_prefix=os.getenv('TW_DISPLAY_PREFIX', 'AI - Timewaster')
        )

    def _log_info(self, message: str):
        """Log info message."""
        if self.logger:
            self.logger.info(message)

    def _log_warning(self, message: str):
        """Log warning message."""
        if self.logger:
            self.logger.warning(message)

    def _log_error(self, message: str):
        """Log error message."""
        if self.logger:
            self.logger.error(message)

    def _log_debug(self, message: str):
        """Log debug message."""
        if self.logger:
            self.logger.debug(message)

    def _normalize_message(self, msg) -> dict:
        """Normalize message to dict format.

        :param msg: Message object or dict from API.
        :return: Normalized dict with standard fields.
        """
        if isinstance(msg, dict):
            from_user = msg.get("fromUser", {}) or {}
            return {
                "id": msg.get("id"),
                "author_id": str(from_user.get("id", "")),
                "price": msg.get("price", 0) or 0,
                "isFree": msg.get("isFree", True),
                "canPurchase": msg.get("canPurchase", True),
                "isPaid": msg.get("isPaid", False) or msg.get("isOpened", False),
                "created_at": msg.get("createdAt") or msg.get("created_at"),
                "media": msg.get("media", []) or [],
                "_raw": msg
            }
        else:
            return {
                "id": msg.id,
                "author_id": str(msg.author.id),
                "price": msg.price if hasattr(msg, "price") else 0,
                "isFree": msg.isFree if hasattr(msg, "isFree") else True,
                "canPurchase": msg.canPurchase if hasattr(msg, "canPurchase") else True,
                "isPaid": False,
                "created_at": msg.created_at,
                "media": msg.media if hasattr(msg, "media") else [],
                "_raw": msg
            }

    async def analyze_fan_spend(self, fan_id: str) -> Optional[SpendAnalysis]:
        """Calculate fan's total spend using Live API.

        Fetches all messages and calculates spend based on purchased PPV
        and bundles. Uses same logic as check_fan_spend.py (proven accurate).

        :param fan_id: Fan's OnlyFans ID.
        :return: SpendAnalysis or None if failed.
        """
        fan_id_clean = str(fan_id).lstrip("u")

        try:
            self._log_info(f"Analyzing spend for fan {fan_id_clean}...")

            fan_user = await self.authed.get_user(int(fan_id_clean))
            if not fan_user:
                self._log_warning(f"Fan {fan_id_clean} not found")
                return None

            fan_username = getattr(fan_user, 'username', f'user_{fan_id_clean}')
            self._log_debug(f"Fan username: {fan_username}")

            messages = await fetch_all_messages_fast(fan_user, self.authed, logger=self.logger)
            if not messages:
                self._log_debug(f"No messages found for fan {fan_id_clean}")
                return SpendAnalysis(
                    fan_id=fan_id_clean,
                    fan_username=fan_username,
                    total_messages=0,
                    messages_from_fan=0,
                    messages_from_creator=0,
                    message_spend=0.0,
                    bundle_spend=0.0,
                    total_spend=0.0,
                    rpm=0.0,
                    is_timewaster=False
                )

            total_messages = len(messages)
            messages_from_fan = 0
            messages_from_creator = 0
            message_spend = 0.0
            bundle_spend = 0.0
            seen_bundles = set()

            for msg in messages:
                norm = self._normalize_message(msg)

                if norm["author_id"] == self.model_id:
                    messages_from_creator += 1
                else:
                    messages_from_fan += 1

                price = float(norm["price"]) if norm["price"] else 0.0
                is_purchased = norm["canPurchase"] is False
                raw_msg = norm["_raw"]

                if is_bundle(raw_msg):
                    media = raw_msg.get("media", []) if isinstance(raw_msg, dict) else getattr(raw_msg, "media", [])
                    bundle_id = None
                    if media:
                        first_media = media[0] if media else None
                        if first_media:
                            bundle_id = first_media.get("id") if isinstance(first_media, dict) else getattr(first_media, "id", None)
                    if bundle_id and bundle_id not in seen_bundles:
                        seen_bundles.add(bundle_id)
                        if is_purchased and price > 0:
                            bundle_spend += price
                else:
                    if is_purchased and price > 0:
                        message_spend += price

            total_spend = message_spend + bundle_spend
            rpm = total_spend / total_messages if total_messages > 0 else 0.0

            is_timewaster = self._check_timewaster_criteria(
                total_spend, total_messages, rpm
            )

            self._log_info(
                f"Fan {fan_id_clean} ({fan_username}): "
                f"${total_spend:.2f} spend, {total_messages} msgs, ${rpm:.4f} RPM "
                f"-> {'TIMEWASTER' if is_timewaster else 'OK'}"
            )

            return SpendAnalysis(
                fan_id=fan_id_clean,
                fan_username=fan_username,
                total_messages=total_messages,
                messages_from_fan=messages_from_fan,
                messages_from_creator=messages_from_creator,
                message_spend=message_spend,
                bundle_spend=bundle_spend,
                total_spend=total_spend,
                rpm=rpm,
                is_timewaster=is_timewaster
            )

        except Exception as e:
            self._log_error(f"Error analyzing fan {fan_id} spend: {str(e)}")
            import traceback
            self._log_error(traceback.format_exc())
            return None

    def _check_timewaster_criteria(
        self,
        total_spend: float,
        total_messages: int,
        rpm: float
    ) -> bool:
        """Check if fan meets timewaster criteria.

        :param total_spend: Total spend in dollars.
        :param total_messages: Total message count.
        :param rpm: Revenue per message.
        :return: True if timewaster.
        """
        return (
            total_spend < self.config.max_spend
            and total_messages > self.config.min_messages
            and rpm < self.config.max_rpm
        )

    async def update_display_name(self, fan_id: str, new_name: str) -> bool:
        """Update fan's display name via OnlyFans API.

        :param fan_id: Fan's OnlyFans ID.
        :param new_name: New display name.
        :return: True if successful, False otherwise.
        """
        endpoint = urljoin(self.API_BASE, f"subscriptions/{fan_id}")

        try:
            target = await self.authed.get_user(int(fan_id))
            if not target:
                self._log_error(f"User {fan_id} does not exist for display name update")
                return False

            target_requester = target.get_requester()
            payload = {"displayName": new_name}

            for attempt in range(self.MAX_RETRIES):
                try:
                    headers = await target_requester.session_rules(endpoint)
                    headers["accept"] = "application/json, text/plain, */*"
                    headers["Connection"] = "keep-alive"

                    response = await target_requester.active_session.put(
                        endpoint, headers=headers, json=payload
                    )

                    if response.status == 200:
                        self._log_info(f"Updated display name for fan {fan_id} to '{new_name}'")
                        return True
                    else:
                        self._log_warning(
                            f"Display name update failed with status {response.status} "
                            f"(attempt {attempt + 1}/{self.MAX_RETRIES})"
                        )
                        if attempt < self.MAX_RETRIES - 1:
                            await asyncio.sleep(self.RETRY_BACKOFF ** (attempt + 1))
                            continue

                except Exception as e:
                    self._log_error(
                        f"Error updating display name (attempt {attempt + 1}/{self.MAX_RETRIES}): {e}"
                    )
                    if attempt < self.MAX_RETRIES - 1:
                        await asyncio.sleep(self.RETRY_BACKOFF ** (attempt + 1))
                        continue

            return False

        except Exception as e:
            self._log_error(f"Error in update_display_name: {e}")
            return False

    async def get_collections(self) -> Dict[str, str]:
        """Get all collections as {name: id} dict.

        :return: Dict mapping collection name to collection ID.
        """
        try:
            lists = await self.authed.get_lists()
            if not lists:
                return {}

            collections = {}
            for lst in lists:
                if isinstance(lst, dict):
                    name = lst.get('name', '')
                    list_id = lst.get('id', '')
                else:
                    name = getattr(lst, 'name', '')
                    list_id = getattr(lst, 'id', '')

                if name and list_id:
                    collections[name.lower()] = str(list_id)

            self._log_debug(f"Retrieved {len(collections)} collections")
            return collections

        except Exception as e:
            self._log_error(f"Error getting collections: {e}")
            return {}

    async def create_collection(self, name: str) -> Optional[str]:
        """Create a new collection.

        :param name: Collection name.
        :return: Collection ID if successful, None otherwise.
        """
        endpoint = urljoin(self.API_BASE, "lists")

        try:
            target_requester = self.authed.get_requester()
            payload = {"name": name}

            for attempt in range(self.MAX_RETRIES):
                try:
                    headers = await target_requester.session_rules(endpoint)
                    headers["accept"] = "application/json, text/plain, */*"
                    headers["Connection"] = "keep-alive"

                    response = await target_requester.active_session.post(
                        endpoint, headers=headers, json=payload
                    )

                    if response.status == 200:
                        result = await response.json()
                        collection_id = str(result.get('id', ''))
                        self._log_info(f"Created collection '{name}' with ID {collection_id}")
                        return collection_id
                    else:
                        self._log_warning(
                            f"Create collection failed with status {response.status} "
                            f"(attempt {attempt + 1}/{self.MAX_RETRIES})"
                        )
                        if attempt < self.MAX_RETRIES - 1:
                            await asyncio.sleep(self.RETRY_BACKOFF ** (attempt + 1))
                            continue

                except Exception as e:
                    self._log_error(
                        f"Error creating collection (attempt {attempt + 1}/{self.MAX_RETRIES}): {e}"
                    )
                    if attempt < self.MAX_RETRIES - 1:
                        await asyncio.sleep(self.RETRY_BACKOFF ** (attempt + 1))
                        continue

            return None

        except Exception as e:
            self._log_error(f"Error in create_collection: {e}")
            return None

    async def add_fan_to_collection(self, collection_id: str, fan_id: str) -> bool:
        """Add fan to a collection.

        :param collection_id: Collection/list ID.
        :param fan_id: Fan's OnlyFans ID.
        :return: True if successful, False otherwise.
        """
        endpoint = urljoin(self.API_BASE, "lists/users")

        try:
            target = await self.authed.get_user(int(fan_id))
            if not target:
                self._log_error(f"User {fan_id} does not exist for collection add")
                return False

            target_requester = target.get_requester()
            payload = {str(collection_id): [int(fan_id)]}

            for attempt in range(self.MAX_RETRIES):
                try:
                    headers = await target_requester.session_rules(endpoint)
                    headers["accept"] = "application/json, text/plain, */*"
                    headers["Connection"] = "keep-alive"

                    response = await target_requester.active_session.post(
                        endpoint, headers=headers, json=payload
                    )

                    if response.status == 200:
                        self._log_info(f"Added fan {fan_id} to collection {collection_id}")
                        return True
                    elif response.status == 400:
                        response_text = await response.text()
                        if "already" in response_text.lower() or "exist" in response_text.lower():
                            self._log_info(
                                f"Fan {fan_id} already in collection {collection_id} "
                                f"(idempotent - treating as success)"
                            )
                            return True
                        else:
                            self._log_warning(
                                f"Add to collection failed with status 400: {response_text} "
                                f"(attempt {attempt + 1}/{self.MAX_RETRIES})"
                            )
                            if attempt < self.MAX_RETRIES - 1:
                                await asyncio.sleep(self.RETRY_BACKOFF ** (attempt + 1))
                                continue
                    else:
                        self._log_warning(
                            f"Add to collection failed with status {response.status} "
                            f"(attempt {attempt + 1}/{self.MAX_RETRIES})"
                        )
                        if attempt < self.MAX_RETRIES - 1:
                            await asyncio.sleep(self.RETRY_BACKOFF ** (attempt + 1))
                            continue

                except Exception as e:
                    self._log_error(
                        f"Error adding fan to collection (attempt {attempt + 1}/{self.MAX_RETRIES}): {e}"
                    )
                    if attempt < self.MAX_RETRIES - 1:
                        await asyncio.sleep(self.RETRY_BACKOFF ** (attempt + 1))
                        continue

            return False

        except Exception as e:
            self._log_error(f"Error in add_fan_to_collection: {e}")
            return False

    async def get_or_create_collection(self, name: str) -> Optional[str]:
        """Get collection ID, creating if it doesn't exist.

        :param name: Collection name.
        :return: Collection ID or None if failed.
        """
        collections = await self.get_collections()
        collection_id = collections.get(name.lower())

        if collection_id:
            self._log_debug(f"Found existing collection '{name}' with ID {collection_id}")
            return collection_id

        self._log_info(f"Collection '{name}' not found, creating...")
        return await self.create_collection(name)

    async def ensure_timewaster_collection(self) -> Optional[str]:
        """Ensure timewaster collection exists, cache ID.

        :return: Collection ID or None if failed.
        """
        if self._timewaster_collection_id:
            return self._timewaster_collection_id

        self._timewaster_collection_id = await self.get_or_create_collection(
            self.config.collection_name
        )
        return self._timewaster_collection_id

    async def get_fan_display_name(self, fan_id: str) -> Optional[str]:
        """Get fan's current display name.

        :param fan_id: Fan's OnlyFans ID.
        :return: Display name or None.
        """
        try:
            fan_user = await self.authed.get_user(int(fan_id))
            if fan_user:
                return getattr(fan_user, 'name', None) or getattr(fan_user, 'username', None)
            return None
        except Exception as e:
            self._log_error(f"Error getting fan display name: {e}")
            return None

    async def mark_as_timewaster(self, fan_id: str, original_display_name: str) -> Tuple[bool, bool]:
        """Mark fan as timewaster (update display name + add to collection).

        :param fan_id: Fan's OnlyFans ID.
        :param original_display_name: Fan's current display name.
        :return: Tuple of (display_name_updated, added_to_collection).
        """
        display_name_updated = False
        added_to_collection = False

        try:
            new_display_name = f"{self.config.display_name_prefix} {original_display_name}"
            display_name_updated = await self.update_display_name(fan_id, new_display_name)

            if not display_name_updated:
                self._log_warning(f"Failed to update display name for fan {fan_id}")

            collection_id = await self.ensure_timewaster_collection()
            if collection_id:
                added_to_collection = await self.add_fan_to_collection(collection_id, fan_id)
                if not added_to_collection:
                    self._log_warning(f"Failed to add fan {fan_id} to timewaster collection")
            else:
                self._log_warning("Could not get/create timewaster collection")

            self._log_info(
                f"Mark timewaster result for fan {fan_id}: "
                f"display_name={display_name_updated}, collection={added_to_collection}"
            )

        except Exception as e:
            self._log_error(f"Error marking fan {fan_id} as timewaster: {e}")

        return display_name_updated, added_to_collection

    async def check_and_mark_timewaster(self, fan_id: str) -> Optional[SpendAnalysis]:
        """Full timewaster check and mark flow.

        1. Analyze fan spend via Live API
        2. If timewaster, mark (display name + collection)
        3. Return analysis result

        :param fan_id: Fan's OnlyFans ID.
        :return: SpendAnalysis or None if failed.
        """
        enabled = os.getenv('TW_ENABLED', 'true').lower() == 'true'
        if not enabled:
            self._log_debug("Timewaster checking disabled via TW_ENABLED=false")
            return None

        analysis = await self.analyze_fan_spend(fan_id)
        if not analysis:
            return None

        if analysis.is_timewaster:
            self._log_info("=" * 70)
            self._log_info(f"TIMEWASTER DETECTED: Fan {fan_id}")
            self._log_info(f"  Spend: ${analysis.total_spend:.2f} (threshold: <${self.config.max_spend})")
            self._log_info(f"  Messages: {analysis.total_messages} (threshold: >{self.config.min_messages})")
            self._log_info(f"  RPM: ${analysis.rpm:.4f} (threshold: <${self.config.max_rpm})")
            self._log_info("=" * 70)

            original_name = analysis.fan_username
            display_updated, collection_added = await self.mark_as_timewaster(
                fan_id, original_name
            )

            if display_updated or collection_added:
                self._log_info(f"Fan {fan_id} marked as timewaster successfully")
            else:
                self._log_warning(f"Failed to mark fan {fan_id} as timewaster")

        return analysis
