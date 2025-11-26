"""OnlyFans Message Polling System.

Main entry point for authentication and message fetching.
Exports messages and bundles to CSV files.
"""

import asyncio
from modules.authentication import authenticate_all_accounts
from modules.message_fetcher import (
    fetch_all_messages,
    process_messages_and_bundles
)
from modules.csv_exporter import export_all_csvs


async def main() -> None:
    """Main function to authenticate and fetch messages, then export to CSV."""
    print("=" * 60)
    print("OnlyFans Message Polling System")
    print("=" * 60)

    # Step 1: Authenticate all accounts from auth_multi.json
    authenticated_accounts = await authenticate_all_accounts("auth_multi.json")

    if not authenticated_accounts:
        print("\n✗ No accounts authenticated. Exiting.")
        return

    # Initialize data collectors for all accounts
    all_messages_data = []
    all_bundles_data = []
    all_bundle_items_data = []
    all_interactions_data = []
    all_analytics_data = []

    # Step 2: Process each authenticated account
    for authed in authenticated_accounts:
        print(f"\n{'=' * 60}")
        print(f"Processing account: {authed.user.username}")
        print(f"{'=' * 60}")

        creator_id = str(authed.user.id)
        creator_username = authed.user.username

        # Get list of subscriptions (fans to fetch messages from)
        try:
            subscriptions = await authed.get_subscriptions()
            print(f"\nFound {len(subscriptions)} subscription(s)")

            # Fetch and process messages from each fan
            if subscriptions:
                for idx, fan_user in enumerate(subscriptions, 1):
                    print(f"\n[{idx}/{len(subscriptions)}] Processing fan: {fan_user.username}")

                    # Fetch messages
                    messages = await fetch_all_messages(fan_user, limit=20)

                    if messages:
                        # Process messages and bundles
                        messages_data, bundles_data, bundle_items_data, interactions_data, analytics_data = \
                            await process_messages_and_bundles(
                                messages,
                                creator_id,
                                creator_username,
                                str(fan_user.id),
                                authed
                            )

                        # Accumulate data
                        all_messages_data.extend(messages_data)
                        all_bundles_data.extend(bundles_data)
                        all_bundle_items_data.extend(bundle_items_data)
                        all_interactions_data.extend(interactions_data)
                        all_analytics_data.extend(analytics_data)

        except Exception as e:
            print(f"✗ Error processing account: {str(e)}")
            import traceback
            traceback.print_exc()

    # Step 3: Export all data to CSV files
    if all_messages_data:
        print(f"\n{'=' * 60}")
        print(f"Summary:")
        print(f"  Total Messages: {len(all_messages_data)}")
        print(f"  Total Bundles: {len(all_bundles_data)}")
        print(f"  Total Media Items: {len(all_bundle_items_data)}")
        print(f"  Total Fan Interactions: {len(all_interactions_data)}")
        print(f"  Total Analytics Records: {len(all_analytics_data)}")
        print(f"{'=' * 60}")

        exported_files = export_all_csvs(
            all_messages_data,
            all_bundles_data,
            all_bundle_items_data,
            all_interactions_data,
            all_analytics_data
        )

        print(f"\n{'=' * 60}")
        print("Exported Files:")
        for csv_type, filepath in exported_files.items():
            print(f"  {csv_type}: {filepath}")
        print(f"{'=' * 60}")
    else:
        print("\n⚠ No messages found to export")

    print("\n" + "=" * 60)
    print("Processing complete")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
