"""Test script to fetch messages for a specific fan.

Usage:
    python test_fan_fetch.py
"""

import asyncio
import json
from pathlib import Path
from ultima_scraper_api import OnlyFansAPI
from modules.authentication import load_auth, create_api_helper
from modules.message_fetcher import fetch_all_messages, process_messages_and_bundles


async def test_fetch_fan_messages():
    """Test fetching messages for fan 526399746 using Ayumi creator."""
    # Configuration
    CREATOR_ID = "248797134"  # Ayumi
    CREATOR_NAME = "Ayumi"
    FAN_ID = "526399746"

    print("=" * 80)
    print(f"Test: Fetching messages for fan {FAN_ID} using {CREATOR_NAME}")
    print("=" * 80)

    # Load authentication
    print("\n[1/5] Loading authentication...")
    auth_details = load_auth(CREATOR_ID)
    if not auth_details:
        print(f"✗ Could not find auth for creator ID {CREATOR_ID}")
        return

    print(f"✓ Loaded auth for {auth_details.username}")

    # Create API and authenticate
    print("\n[2/5] Authenticating with OnlyFans API...")
    api = OnlyFansAPI()
    from ultima_scraper_api.apis.onlyfans.authenticator import OnlyFansAuthenticator

    authenticator = OnlyFansAuthenticator(api, auth_details)
    authed = await authenticator.login()

    if not authed or not authenticator.is_authed():
        print("✗ Authentication failed")
        if authenticator.errors:
            for error in authenticator.errors:
                print(f"  Error: {error.message}")
        return

    print(f"✓ Successfully authenticated as {authed.user.username}")

    # Get fan user object
    print(f"\n[3/5] Getting fan user object for ID {FAN_ID}...")
    fan_user = await authed.get_user(int(FAN_ID))

    if not fan_user:
        print(f"✗ Could not find fan with ID {FAN_ID}")
        return

    print(f"✓ Found fan: {fan_user.username} (ID: {fan_user.id})")

    # Fetch messages
    print(f"\n[4/5] Fetching messages...")
    messages = await fetch_all_messages(fan_user, limit=20)

    if not messages:
        print("✗ No messages found")
        return

    print(f"✓ Fetched {len(messages)} messages")
    print(f"  First message: {messages[-1].created_at if messages else 'N/A'}")
    print(f"  Last message: {messages[0].created_at if messages else 'N/A'}")

    # Process messages and bundles
    print(f"\n[5/5] Processing messages and bundles...")
    (
        messages_data,
        bundles_data,
        bundle_items_data,
        interactions_data,
        analytics_data
    ) = await process_messages_and_bundles(
        messages,
        CREATOR_ID,
        CREATOR_NAME,
        FAN_ID,
        authed
    )

    print(f"✓ Processing complete:")
    print(f"  Messages: {len(messages_data)}")
    print(f"  Bundles: {len(bundles_data)}")
    print(f"  Bundle items: {len(bundle_items_data)}")
    print(f"  Fan interactions: {len(interactions_data)}")
    print(f"  Analytics: {len(analytics_data)}")

    # Display sample messages
    print("\n" + "=" * 80)
    print("SAMPLE MESSAGES (first 5)")
    print("=" * 80)

    for i, msg_data in enumerate(messages_data[:5], 1):
        print(f"\n[Message {i}]")
        print(f"  ID: {msg_data['message_id']}")
        print(f"  From: {msg_data['sender_username']} ({msg_data['sender_id']})")
        print(f"  Type: {msg_data['message_type']}")
        print(f"  Text: {msg_data['message'][:100]}..." if len(msg_data['message']) > 100 else f"  Text: {msg_data['message']}")
        print(f"  Price: ${msg_data['price']}")
        print(f"  Created: {msg_data['created_at']}")
        if msg_data['media_id']:
            print(f"  Media: {msg_data['media_type']} (ID: {msg_data['media_id']})")

    # Display bundles
    if bundles_data:
        print("\n" + "=" * 80)
        print("BUNDLES")
        print("=" * 80)

        for i, bundle in enumerate(bundles_data, 1):
            print(f"\n[Bundle {i}]")
            print(f"  ID: {bundle['bundle_id']}")
            print(f"  Type: {bundle['bundle_type']}")
            print(f"  Price: ${bundle['price']}")
            print(f"  Media count: {bundle['media_count']}")
            print(f"  Text: {bundle['text'][:100]}..." if len(bundle['text']) > 100 else f"  Text: {bundle['text']}")

    # Save to JSON for inspection
    output_dir = Path("test_output")
    output_dir.mkdir(exist_ok=True)

    output_file = output_dir / f"fan_{FAN_ID}_test.json"

    test_data = {
        "creator_id": CREATOR_ID,
        "creator_name": CREATOR_NAME,
        "fan_id": FAN_ID,
        "fan_username": fan_user.username,
        "total_messages": len(messages_data),
        "messages": messages_data,
        "bundles": bundles_data,
        "bundle_items": bundle_items_data,
        "interactions": interactions_data,
        "analytics": analytics_data
    }

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(test_data, f, indent=2, ensure_ascii=False)

    print("\n" + "=" * 80)
    print(f"✓ Test data saved to: {output_file}")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(test_fetch_fan_messages())
