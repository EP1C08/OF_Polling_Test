"""
CSV Export module for OnlyFans data
Exports messages, bundles, bundle items, fan interactions, and analytics to CSV files
"""

import csv
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any


def ensure_output_directory(output_dir: str = "output") -> Path:
    """Create output directory if it doesn't exist"""
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    return output_path


def export_messages_csv(messages_data: List[Dict[str, Any]], output_dir: str = "output") -> str:
    """
    Export messages to messages.csv

    Columns: id, message_id, model_id, fan_id, sender_id, model_name, sender_username,
             message, message_type, media_id, media_type, price, is_free, is_purchased,
             is_from_me, created_at, fetched_at
    """
    output_path = ensure_output_directory(output_dir)
    filepath = output_path / "messages.csv"

    headers = [
        'id', 'message_id', 'model_id', 'fan_id', 'sender_id', 'model_name',
        'sender_username', 'message', 'message_type', 'media_id', 'media_type',
        'price', 'is_free', 'is_purchased', 'is_from_me', 'created_at', 'fetched_at'
    ]

    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(messages_data)

    print(f"✓ Exported {len(messages_data)} messages to {filepath}")
    return str(filepath)


def export_bundles_csv(bundles_data: List[Dict[str, Any]], output_dir: str = "output") -> str:
    """
    Export bundles to bundles.csv

    Columns: id, bundle_id, message_id, creator_id, creator_username, total_price,
             media_count, photo_count, video_count, audio_count, is_mass_message,
             queue_id, name, description, created_at, first_seen_at
    """
    output_path = ensure_output_directory(output_dir)
    filepath = output_path / "bundles.csv"

    headers = [
        'id', 'bundle_id', 'message_id', 'creator_id', 'creator_username', 'total_price',
        'media_count', 'photo_count', 'video_count', 'audio_count', 'is_mass_message',
        'queue_id', 'name', 'description', 'created_at', 'first_seen_at'
    ]

    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(bundles_data)

    print(f"✓ Exported {len(bundles_data)} bundles to {filepath}")
    return str(filepath)


def export_bundle_items_csv(bundle_items_data: List[Dict[str, Any]], output_dir: str = "output") -> str:
    """
    Export bundle items to bundle_items.csv

    Columns: id, bundle_id, media_id, media_type, duration, created_at
    """
    output_path = ensure_output_directory(output_dir)
    filepath = output_path / "bundle_items.csv"

    headers = ['id', 'bundle_id', 'media_id', 'media_type', 'duration', 'created_at']

    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(bundle_items_data)

    print(f"✓ Exported {len(bundle_items_data)} bundle items to {filepath}")
    return str(filepath)


def export_bundle_fan_interactions_csv(interactions_data: List[Dict[str, Any]], output_dir: str = "output") -> str:
    """
    Export bundle fan interactions to bundle_fan_interactions.csv

    Columns: id, bundle_id, fan_user_id, message_id, sent_at, is_purchased,
             purchased_at, created_at
    """
    output_path = ensure_output_directory(output_dir)
    filepath = output_path / "bundle_fan_interactions.csv"

    headers = [
        'id', 'bundle_id', 'fan_user_id', 'message_id', 'sent_at',
        'is_purchased', 'purchased_at', 'created_at'
    ]

    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(interactions_data)

    print(f"✓ Exported {len(interactions_data)} fan interactions to {filepath}")
    return str(filepath)


def export_bundle_analytics_csv(analytics_data: List[Dict[str, Any]], output_dir: str = "output") -> str:
    """
    Export bundle analytics to bundle_analytics.csv

    Columns: id, bundle_id, api_sent_count, api_viewed_count, api_purchased_count,
             tracked_offers, tracked_purchases, view_rate, conversion_rate, total_revenue,
             net_revenue, average_time_to_purchase, best_time_of_day, best_day_of_week,
             last_synced, last_updated
    """
    output_path = ensure_output_directory(output_dir)
    filepath = output_path / "bundle_analytics.csv"

    headers = [
        'id', 'bundle_id', 'api_sent_count', 'api_viewed_count', 'api_purchased_count',
        'tracked_offers', 'tracked_purchases', 'view_rate', 'conversion_rate',
        'total_revenue', 'net_revenue', 'average_time_to_purchase', 'best_time_of_day',
        'best_day_of_week', 'last_synced', 'last_updated'
    ]

    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(analytics_data)

    print(f"✓ Exported {len(analytics_data)} bundle analytics to {filepath}")
    return str(filepath)


def export_all_csvs(
    messages_data: List[Dict[str, Any]],
    bundles_data: List[Dict[str, Any]],
    bundle_items_data: List[Dict[str, Any]],
    interactions_data: List[Dict[str, Any]],
    analytics_data: List[Dict[str, Any]],
    output_dir: str = "output"
) -> Dict[str, str]:
    """
    Export all CSV files at once

    Returns:
        Dictionary with CSV names as keys and file paths as values
    """
    print(f"\n{'=' * 60}")
    print("Exporting CSV Files")
    print(f"{'=' * 60}")

    output_path = ensure_output_directory(output_dir)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    timestamped_dir = output_path / timestamp
    timestamped_dir.mkdir(exist_ok=True)

    exported_files = {
        'messages': export_messages_csv(messages_data, str(timestamped_dir)),
        'bundles': export_bundles_csv(bundles_data, str(timestamped_dir)),
        'bundle_items': export_bundle_items_csv(bundle_items_data, str(timestamped_dir)),
        'bundle_fan_interactions': export_bundle_fan_interactions_csv(interactions_data, str(timestamped_dir)),
        'bundle_analytics': export_bundle_analytics_csv(analytics_data, str(timestamped_dir))
    }

    print(f"\n✓ All CSV files exported to: {timestamped_dir}")
    return exported_files
