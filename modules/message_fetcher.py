"""Message fetching module for OnlyFans API.

Fetches complete conversation history from latest to oldest message.
Processes messages and bundles for CSV export.
"""

from typing import Optional, Dict, List, Any, Tuple
from datetime import datetime
from modules.bundle_processor import (
    is_bundle,
    process_bundle_from_message,
    deduplicate_bundles,
    process_mass_message_analytics
)
from modules.fast_message_fetcher import FastMessageFetcher


async def fetch_all_messages(
    user,
    limit: int = 20,
    cutoff_id: Optional[int] = None,
    authed=None,
) -> list:
    """Fetch all messages from a user conversation (latest to oldest).

    Uses recursive pagination to retrieve complete conversation history.
    This method calls the user's get_messages() which internally handles
    the pagination from latest message back to the first message.

    :param user: OnlyFans UserModel object or SimpleUser with id
    :param limit: Number of messages to fetch per request (default: 20)
    :param cutoff_id: Optional message ID to stop fetching at (for incremental updates)
    :param authed: Optional authenticated API object (required if user is SimpleUser)
    :return: List of MessageModel objects ordered from latest to oldest
    """
    try:
        print(f"Fetching messages for user: {user.username} (ID: {user.id})")

        # Check if user has get_messages method (real user object)
        if hasattr(user, 'get_messages') and callable(getattr(user, 'get_messages', None)):
            # The get_messages method handles recursive pagination internally
            messages = await user.get_messages(limit=limit, cutoff_id=cutoff_id)
        elif authed:
            # Get proper UserModel from API, then fetch messages
            user_obj = await authed.get_user(user.id)
            if not user_obj:
                raise ValueError(f"Could not get user object for ID {user.id}")
            messages = await user_obj.get_messages(limit=limit, cutoff_id=cutoff_id)
        else:
            raise ValueError("User object doesn't have get_messages method and no authed API provided")

        print(f"✓ Fetched {len(messages)} messages from {user.username}")

        if messages:
            first_msg_date = messages[-1].created_at if messages else None
            last_msg_date = messages[0].created_at if messages else None
            print(f"  Date range: {first_msg_date} to {last_msg_date}")

        return messages

    except Exception as e:
        print(f"✗ Error fetching messages from {user.username}: {str(e)}")
        return []


async def fetch_all_messages_fast(
    user,
    authed,
    cutoff_id: Optional[int] = None,
    logger=None,
) -> list:
    """Fetch messages using high-performance direct API method.

    This is 2.5x faster than the standard method due to:
    - Larger batch size (50 vs 20 messages)
    - Direct API calls with less overhead
    - Automatic retry on network errors
    - Fallback to library method if API changes

    :param user: OnlyFans UserModel or SimpleUser
    :param authed: Authenticated API object (required)
    :param cutoff_id: Stop at this message_id (for incremental)
    :param logger: Optional logger
    :return: List of message dicts
    """
    fetcher = FastMessageFetcher(authed, logger=logger)
    return await fetcher.fetch_all_messages(user, cutoff_id=cutoff_id)


def _normalize_message(msg):
    """Normalize message to dict format (handles both objects and dicts).

    :param msg: Message object or dict
    :return: Normalized dict with consistent field names
    """
    if isinstance(msg, dict):
        # Already a dict from fast fetcher - extract fromUser for author
        from_user = msg.get("fromUser", {}) or {}
        return {
            'id': msg.get("id"),
            'author_id': str(from_user.get("id", "")),
            'author_username': from_user.get("username", f"u{from_user.get('id', '')}"),
            'text': msg.get("text", "") or "",
            'price': msg.get("price", 0) or 0,
            'isFree': msg.get("isFree", True),
            'canPurchase': msg.get("canPurchase", True),
            'isPaid': msg.get("isPaid", False) or msg.get("isOpened", False),
            'created_at': msg.get("createdAt") or msg.get("created_at"),
            'media': msg.get("media", []) or [],
            'isFromQueue': msg.get("isFromQueue", False),
            'queueId': msg.get("queueId"),
            'responseType': msg.get("responseType", "text"),
            '_raw': msg  # Keep raw for bundle processing
        }
    else:
        # Message object from library
        return {
            'id': msg.id,
            'author_id': str(msg.author.id),
            'author_username': msg.author.username,
            'text': msg.text or "",
            'price': msg.price if hasattr(msg, 'price') else 0,
            'isFree': msg.isFree if hasattr(msg, 'isFree') else True,
            'canPurchase': msg.canPurchase if hasattr(msg, 'canPurchase') else True,
            'isPaid': False,
            'created_at': msg.created_at,
            'media': msg.media if hasattr(msg, 'media') else [],
            'isFromQueue': msg.isFromQueue if hasattr(msg, 'isFromQueue') else False,
            'queueId': msg.queueId if hasattr(msg, 'queueId') else None,
            'responseType': msg.responseType if hasattr(msg, 'responseType') else "text",
            '_raw': msg  # Keep raw for bundle processing
        }


async def fetch_messages_from_multiple_users(users: list, limit: int = 20) -> dict:
    """Fetch messages from multiple users.

    :param users: List of OnlyFans UserModel objects
    :param limit: Number of messages to fetch per request
    :return: Dictionary mapping user_id to list of messages
    """
    all_messages = {}

    print(f"\nFetching messages from {len(users)} user(s)...")

    for user in users:
        messages = await fetch_all_messages(user, limit=limit)
        all_messages[user.id] = {
            'username': user.username,
            'messages': messages,
            'count': len(messages)
        }

    total_messages = sum(data['count'] for data in all_messages.values())
    print(f"\n✓ Total messages fetched: {total_messages}")

    return all_messages


async def get_conversation_with_user(
    authed,
    user_identifier: int | str,
    limit: int = 20,
) -> list:
    """Get complete conversation history with a specific user.

    :param authed: Authenticated OnlyFansAuthModel object
    :param user_identifier: User ID or username to fetch messages from
    :param limit: Number of messages per request
    :return: List of MessageModel objects
    """
    try:
        user = await authed.get_user(user_identifier)

        if not user:
            print(f"✗ User not found: {user_identifier}")
            return []

        messages = await fetch_all_messages(user, limit=limit)
        return messages

    except Exception as e:
        print(f"✗ Error getting conversation with {user_identifier}: {str(e)}")
        return []


async def process_messages_and_bundles(
    messages: list,
    creator_id: str,
    creator_username: str,
    fan_id: str,
    authed=None,
) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict], List[Dict]]:
    """Process messages and extract bundle data for CSV export.

    :param messages: List of MessageModel objects
    :param creator_id: Creator's OnlyFans ID
    :param creator_username: Creator's username
    :param fan_id: Fan's OnlyFans ID
    :param authed: Authenticated OnlyFansAuthModel (for fetching mass message stats)
    :return: Tuple of (messages_data, bundles_data, bundle_items_data, interactions_data, analytics_data)
    """
    messages_data = []
    bundles_data = []
    bundle_items_data = []
    interactions_data = []
    analytics_data = []

    fetched_at = datetime.now()
    message_row_id = 1
    bundle_row_id = 1
    item_row_id = 1
    interaction_row_id = 1
    analytics_row_id = 1

    # Track bundles by bundle_id to deduplicate
    seen_bundles = {}
    bundle_to_analytics = {}  # Map bundle_id to tracked stats

    for msg in messages:
        # Normalize message format (handles both objects and dicts)
        norm_msg = _normalize_message(msg)

        # Determine sender and receiver
        author_id = norm_msg['author_id']
        is_from_creator = (author_id == creator_id)

        # Get media info if available
        media_id = ''
        media_type = ''
        media = norm_msg.get('media', [])
        if media and len(media) > 0:
            first_media = media[0]
            media_id = str(first_media.get('id', ''))
            media_type = first_media.get('type', '').lower()

        # Process message data
        message_dict = {
            'id': message_row_id,
            'message_id': str(norm_msg['id']),
            'model_id': creator_id,
            'fan_id': fan_id,
            'sender_id': author_id,
            'model_name': creator_username,
            'sender_username': norm_msg['author_username'],
            'message': norm_msg['text'],
            'message_type': 'bundle' if is_bundle(norm_msg['_raw']) else ('media' if media_id else 'text'),
            'media_id': media_id,
            'media_type': media_type,
            'price': float(norm_msg['price']) if norm_msg['price'] else 0.0,
            'is_free': norm_msg['isFree'],
            'is_purchased': norm_msg['isPaid'] or (not norm_msg['canPurchase']),
            'is_from_me': is_from_creator,
            'created_at': norm_msg['created_at'].isoformat() if isinstance(norm_msg['created_at'], datetime) else str(norm_msg['created_at']),
            'fetched_at': fetched_at.isoformat()
        }
        messages_data.append(message_dict)
        message_row_id += 1

        # Process bundle if applicable (use raw message object/dict)
        if is_bundle(norm_msg['_raw']):
            bundle_data, bundle_items, fan_interaction = process_bundle_from_message(
                norm_msg['_raw'], creator_id, creator_username, fan_id, bundle_row_id, fetched_at
            )

            if bundle_data:
                bundle_id = bundle_data['bundle_id']

                # Track bundle for deduplication
                if bundle_id not in seen_bundles:
                    seen_bundles[bundle_id] = bundle_data
                    bundles_data.append(bundle_data)
                    bundle_row_id += 1

                    # Initialize tracking stats
                    bundle_to_analytics[bundle_id] = {
                        'offers': 0,
                        'purchases': 0
                    }

                # Add bundle items
                for item in bundle_items:
                    item['id'] = item_row_id
                    bundle_items_data.append(item)
                    item_row_id += 1

                # Add fan interaction
                if fan_interaction:
                    fan_interaction['id'] = interaction_row_id
                    interactions_data.append(fan_interaction)
                    interaction_row_id += 1

                    # Update tracking stats
                    bundle_to_analytics[bundle_id]['offers'] += 1
                    if fan_interaction['is_purchased']:
                        bundle_to_analytics[bundle_id]['purchases'] += 1

    # Fetch mass message stats for analytics
    if authed:
        try:
            mass_message_stats = await authed.get_mass_message_stats()

            for stat in mass_message_stats:
                # Try to find corresponding bundle
                # Mass messages have queue_id which we stored
                matching_bundles = [b for b in bundles_data if b.get('queue_id') == str(stat.id)]

                for bundle in matching_bundles:
                    bundle_id = bundle['bundle_id']
                    tracked_stats = bundle_to_analytics.get(bundle_id, {'offers': 0, 'purchases': 0})

                    analytics = process_mass_message_analytics(
                        stat,
                        bundle_id,
                        analytics_row_id,
                        tracked_stats['offers'],
                        tracked_stats['purchases'],
                        fetched_at
                    )
                    analytics_data.append(analytics)
                    analytics_row_id += 1
        except Exception as e:
            print(f"⚠ Could not fetch mass message stats: {str(e)}")

    print(f"  Processed: {len(messages_data)} messages, {len(bundles_data)} unique bundles, {len(bundle_items_data)} media items")

    return messages_data, bundles_data, bundle_items_data, interactions_data, analytics_data
