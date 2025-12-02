"""
Bundle Data Processor module
Processes messages to extract bundle information, deduplicates, and prepares data for CSV export
"""

import hashlib
from datetime import datetime
from typing import List, Dict, Any, Tuple, Union


def _get_attr(obj, attr: str, default=None):
    """Get attribute from object or dict.

    :param obj: Object or dict
    :param attr: Attribute/key name
    :param default: Default value if not found
    :return: Attribute value or default
    """
    if isinstance(obj, dict):
        return obj.get(attr, default)
    else:
        return getattr(obj, attr, default)


def _call_method(obj, method: str, default=None):
    """Call method on object or return default for dict.

    :param obj: Object or dict
    :param method: Method name
    :param default: Default value for dict
    :return: Method result or default
    """
    if isinstance(obj, dict):
        # For dicts, check specific fields
        if method == 'is_mass_message':
            return obj.get('isFromQueue', False) or obj.get('is_mass_message', False)
        return default
    else:
        # For objects, call the method
        method_obj = getattr(obj, method, None)
        if callable(method_obj):
            return method_obj()
        return default


def generate_bundle_id(media_ids: List[str]) -> str:
    """
    Generate content-based hash for bundle deduplication
    Same media IDs = same bundle_id

    Args:
        media_ids: List of media IDs in the bundle

    Returns:
        SHA256 hash of sorted media IDs
    """
    # Sort media IDs to ensure consistent hash regardless of order
    sorted_ids = sorted(str(mid) for mid in media_ids)
    content = "-".join(sorted_ids)
    return hashlib.sha256(content.encode()).hexdigest()[:32]


def is_bundle(message) -> bool:
    """
    Determine if a message is a bundle
    Criteria: Has media AND (multiple media OR has price > 0)

    Args:
        message: MessageModel object or dict

    Returns:
        True if message is a bundle
    """
    # Get media info (works for both object and dict)
    media = _get_attr(message, 'media', [])
    media_count = _get_attr(message, 'media_count', len(media) if media else 0)
    price = _get_attr(message, 'price', 0)
    is_mass = _call_method(message, 'is_mass_message', False)

    has_media = media_count and media_count > 0
    is_paid_content = price and price > 0

    # Bundle if: (has media AND paid) OR (multiple media) OR (mass message with media)
    return has_media and (is_paid_content or media_count > 1 or is_mass)


def extract_media_counts(media_list: List[Dict[str, Any]]) -> Tuple[int, int, int]:
    """
    Count photos, videos, and audios from media list

    Args:
        media_list: List of media objects from message

    Returns:
        Tuple of (photo_count, video_count, audio_count)
    """
    photo_count = 0
    video_count = 0
    audio_count = 0

    for media in media_list:
        media_type = media.get('type', '').lower()
        if media_type in ['photo', 'image', 'gif']:
            photo_count += 1
        elif media_type in ['video']:
            video_count += 1
        elif media_type in ['audio']:
            audio_count += 1

    return photo_count, video_count, audio_count


def process_bundle_from_message(
    message,
    creator_id: str,
    creator_username: str,
    fan_id: str,
    row_id: int,
    fetched_at: datetime
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], Dict[str, Any]]:
    """
    Extract bundle data from a message

    Args:
        message: MessageModel object or dict
        creator_id: Creator's OnlyFans ID
        creator_username: Creator's username
        fan_id: Fan's OnlyFans ID
        row_id: Database row ID counter
        fetched_at: When the data was fetched

    Returns:
        Tuple of (bundle_dict, bundle_items_list, fan_interaction_dict)
    """
    # Get media list (works for both object and dict)
    media_list = _get_attr(message, 'media', [])

    # Extract media IDs
    media_ids = [str(media.get('id', '')) for media in media_list if media.get('id')]

    if not media_ids:
        return None, [], None

    # Generate bundle ID
    bundle_id = generate_bundle_id(media_ids)

    # Count media types
    photo_count, video_count, audio_count = extract_media_counts(media_list)

    # Extract bundle name from message text (first line or first 50 chars)
    message_text = _get_attr(message, 'text', '') or ''
    bundle_name = message_text.split('\n')[0][:100] if message_text else ""

    # Get other attributes (works for both object and dict)
    message_id = _get_attr(message, 'id')
    price = _get_attr(message, 'price', 0)
    media_count = _get_attr(message, 'media_count', len(media_ids))
    is_mass = _call_method(message, 'is_mass_message', False)
    queue_id = _get_attr(message, 'queue_id') or _get_attr(message, 'queueId')
    created_at = _get_attr(message, 'created_at') or _get_attr(message, 'createdAt')

    # Bundle data
    bundle_data = {
        'id': row_id,
        'bundle_id': bundle_id,
        'message_id': str(message_id),
        'creator_id': creator_id,
        'creator_username': creator_username,
        'total_price': float(price) if price else 0.0,
        'media_count': media_count,
        'photo_count': photo_count,
        'video_count': video_count,
        'audio_count': audio_count,
        'is_mass_message': is_mass,
        'queue_id': str(queue_id) if queue_id else '',
        'name': bundle_name,
        'description': message_text,
        'created_at': created_at.isoformat() if isinstance(created_at, datetime) else str(created_at),
        'first_seen_at': fetched_at.isoformat()
    }

    # Bundle items (media)
    bundle_items = []
    for idx, media in enumerate(media_list):
        media_id = str(media.get('id', ''))
        if not media_id:
            continue

        item = {
            'id': row_id + idx,
            'bundle_id': bundle_id,
            'media_id': media_id,
            'media_type': media.get('type', 'unknown').lower(),
            'duration': media.get('duration', 0) if media.get('type') in ['video', 'audio'] else 0,
            'created_at': fetched_at.isoformat()
        }
        bundle_items.append(item)

    # Fan interaction
    can_purchase = _get_attr(message, 'canPurchase', True)
    is_purchased = not can_purchase  # If can't purchase, already purchased

    fan_interaction = {
        'id': row_id,
        'bundle_id': bundle_id,
        'fan_user_id': fan_id,
        'message_id': str(message_id),
        'sent_at': created_at.isoformat() if isinstance(created_at, datetime) else str(created_at),
        'is_purchased': is_purchased,
        'purchased_at': '',  # Would need transaction data to fill this
        'created_at': fetched_at.isoformat()
    }

    return bundle_data, bundle_items, fan_interaction


def deduplicate_bundles(bundles_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Remove duplicate bundles based on bundle_id
    Keep the first occurrence

    Args:
        bundles_list: List of bundle dictionaries

    Returns:
        Deduplicated list of bundles
    """
    seen_bundle_ids = set()
    unique_bundles = []

    for bundle in bundles_list:
        bundle_id = bundle['bundle_id']
        if bundle_id not in seen_bundle_ids:
            seen_bundle_ids.add(bundle_id)
            unique_bundles.append(bundle)

    return unique_bundles


def process_mass_message_analytics(
    mass_message_stats,
    bundle_id: str,
    row_id: int,
    tracked_offers: int,
    tracked_purchases: int,
    fetched_at: datetime
) -> Dict[str, Any]:
    """
    Process mass message stats into bundle analytics

    Args:
        mass_message_stats: MassMessageStatModel object
        bundle_id: Bundle identifier
        row_id: Database row ID
        tracked_offers: Number of tracked offers (from our data)
        tracked_purchases: Number of tracked purchases (from our data)
        fetched_at: When data was fetched

    Returns:
        Analytics dictionary
    """
    api_sent = mass_message_stats.sent_count if hasattr(mass_message_stats, 'sent_count') else 0
    api_viewed = mass_message_stats.viewed_count if hasattr(mass_message_stats, 'viewed_count') else 0
    api_purchased = mass_message_stats.purchased_count if hasattr(mass_message_stats, 'purchased_count') else 0

    # Calculate rates
    view_rate = (api_viewed / api_sent * 100) if api_sent > 0 else 0.0
    conversion_rate = (api_purchased / api_sent * 100) if api_sent > 0 else 0.0

    # Calculate revenue
    price = float(mass_message_stats.price) if hasattr(mass_message_stats, 'price') else 0.0
    total_revenue = price * api_purchased
    net_revenue = total_revenue  # Could subtract platform fees here

    analytics = {
        'id': row_id,
        'bundle_id': bundle_id,
        'api_sent_count': api_sent,
        'api_viewed_count': api_viewed,
        'api_purchased_count': api_purchased,
        'tracked_offers': tracked_offers,
        'tracked_purchases': tracked_purchases,
        'view_rate': round(view_rate, 2),
        'conversion_rate': round(conversion_rate, 2),
        'total_revenue': round(total_revenue, 2),
        'net_revenue': round(net_revenue, 2),
        'average_time_to_purchase': 0,  # Would need time-series data
        'best_time_of_day': 0,  # Would need time-series analysis
        'best_day_of_week': 0,  # Would need time-series analysis
        'last_synced': fetched_at.isoformat(),
        'last_updated': fetched_at.isoformat()
    }

    return analytics
