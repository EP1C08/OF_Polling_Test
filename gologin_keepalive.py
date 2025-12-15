"""GoLogin Keep-Alive Worker - Pings all profiles every 5 minutes.

This dedicated container keeps GoLogin profiles alive to prevent
cookie/x_bc expiration.

Features:
- Loads all gologin_profile_ids from creator_credentials table
- Pings each profile every 5 minutes
- On failure: Logs alert and stops container for manual intervention
"""

import asyncio
import logging
import os
import sys
from datetime import datetime

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from modules.gologin_manager import GoLoginManager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("gologin_keepalive")

# Configuration
PING_INTERVAL = int(os.getenv("GOLOGIN_PING_INTERVAL", "300"))  # 5 minutes default
FAIL_ON_ERROR = os.getenv("GOLOGIN_FAIL_ON_ERROR", "true").lower() == "true"


async def load_profile_ids_from_db() -> list:
    """Load all active gologin_profile_ids from creator_credentials table.

    :return: List of gologin_profile_id strings.
    :raises ValueError: If DATABASE_URL is not set.
    """
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is required")

    # Convert to async driver
    if database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)

    engine = create_async_engine(
        database_url,
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True
    )

    async_session = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False
    )

    profile_ids = []

    try:
        async with async_session() as session:
            # Query for active creators with gologin_profile_id
            query = text("""
                SELECT gologin_profile_id, account_name, model_id
                FROM creator_credentials
                WHERE status = 'active'
                AND gologin_profile_id IS NOT NULL
                AND gologin_profile_id != ''
                ORDER BY account_name
            """)

            result = await session.execute(query)
            rows = result.fetchall()

            for row in rows:
                profile_id = row[0]
                account_name = row[1]
                model_id = row[2]
                profile_ids.append({
                    "profile_id": profile_id,
                    "account_name": account_name,
                    "model_id": model_id
                })
                logger.info(f"  Found profile: {account_name} (model_id: {model_id}, profile_id: {profile_id})")

    finally:
        await engine.dispose()

    return profile_ids


async def keepalive_loop(profiles: list, manager: GoLoginManager):
    """Main keep-alive loop - pings all profiles every PING_INTERVAL seconds.

    :param profiles: List of profile dictionaries with profile_id, account_name, model_id.
    :param manager: GoLoginManager instance.
    """
    profile_ids = [p["profile_id"] for p in profiles]
    profile_names = {p["profile_id"]: p["account_name"] for p in profiles}

    logger.info(f"Starting keep-alive loop for {len(profiles)} profiles")
    logger.info(f"Ping interval: {PING_INTERVAL} seconds ({PING_INTERVAL/60:.1f} minutes)")
    logger.info(f"Fail on error: {FAIL_ON_ERROR}")

    ping_count = 0

    while True:
        ping_count += 1
        start_time = datetime.now()
        logger.info(f"\n{'='*60}")
        logger.info(f"Ping cycle #{ping_count} started at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*60}")

        # Ping all profiles
        results = await manager.ping_all_profiles(profile_ids)

        # Analyze results
        success_count = 0
        failed_profiles = []

        for profile_id, success in results.items():
            account_name = profile_names.get(profile_id, "unknown")
            if success:
                success_count += 1
                logger.info(f"  ✓ {account_name}: OK")
            else:
                failed_profiles.append((profile_id, account_name))
                logger.error(f"  ✗ {account_name}: FAILED")

        # Log summary
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"\nPing cycle #{ping_count} complete: {success_count}/{len(profiles)} OK ({duration:.2f}s)")

        # Handle failures
        if failed_profiles:
            logger.error(f"\n{'!'*60}")
            logger.error(f"ALERT: {len(failed_profiles)} profile(s) failed ping!")
            logger.error(f"{'!'*60}")

            for profile_id, account_name in failed_profiles:
                logger.error(f"  - {account_name} (profile_id: {profile_id})")

            if FAIL_ON_ERROR:
                logger.error("\nFAIL_ON_ERROR is enabled - stopping container for manual intervention")
                logger.error("Please check GoLogin profiles and restart the container")
                sys.exit(1)
            else:
                logger.warning("FAIL_ON_ERROR is disabled - continuing despite failures")

        # Wait for next ping interval
        logger.info(f"\nNext ping in {PING_INTERVAL} seconds...")
        await asyncio.sleep(PING_INTERVAL)


async def main():
    """Main entry point for keep-alive worker."""
    logger.info("="*60)
    logger.info("GoLogin Keep-Alive Worker")
    logger.info("="*60)
    logger.info(f"Environment:")
    logger.info(f"  GOLOGIN_PING_INTERVAL: {PING_INTERVAL}s")
    logger.info(f"  GOLOGIN_FAIL_ON_ERROR: {FAIL_ON_ERROR}")
    logger.info(f"  DATABASE_URL: {'set' if os.getenv('DATABASE_URL') else 'NOT SET'}")
    logger.info(f"  GOLOGIN_API_TOKEN: {'set' if os.getenv('GOLOGIN_API_TOKEN') else 'NOT SET'}")
    logger.info("="*60)

    # Validate required environment variables
    if not os.getenv("DATABASE_URL"):
        logger.error("ERROR: DATABASE_URL environment variable is required")
        sys.exit(1)

    if not os.getenv("GOLOGIN_API_TOKEN"):
        logger.error("ERROR: GOLOGIN_API_TOKEN environment variable is required")
        sys.exit(1)

    # Load profile IDs from database
    logger.info("\nLoading GoLogin profile IDs from database...")
    try:
        profiles = await load_profile_ids_from_db()
    except Exception as e:
        logger.error(f"ERROR: Failed to load profiles from database: {str(e)}")
        sys.exit(1)

    if not profiles:
        logger.error("ERROR: No active profiles found with gologin_profile_id")
        logger.error("Please ensure creator_credentials table has profiles with gologin_profile_id set")
        sys.exit(1)

    logger.info(f"\nFound {len(profiles)} active profile(s):")
    for p in profiles:
        logger.info(f"  - {p['account_name']} (model_id: {p['model_id']})")

    # Initialize GoLogin manager
    logger.info("\nInitializing GoLogin manager...")
    try:
        manager = GoLoginManager()
    except ValueError as e:
        logger.error(f"ERROR: {str(e)}")
        sys.exit(1)

    # Initial ping to verify all profiles are accessible
    logger.info("\nPerforming initial ping to verify profiles...")
    profile_ids = [p["profile_id"] for p in profiles]
    initial_results = await manager.ping_all_profiles(profile_ids)

    failed_initial = [pid for pid, success in initial_results.items() if not success]
    if failed_initial:
        logger.error(f"\nERROR: Initial ping failed for {len(failed_initial)} profile(s):")
        for pid in failed_initial:
            account_name = next((p["account_name"] for p in profiles if p["profile_id"] == pid), "unknown")
            logger.error(f"  - {account_name} (profile_id: {pid})")

        if FAIL_ON_ERROR:
            logger.error("\nStopping due to initial ping failures")
            await manager.close()
            sys.exit(1)
    else:
        logger.info(f"✓ All {len(profiles)} profiles responded successfully")

    # Start keep-alive loop
    logger.info("\n" + "="*60)
    logger.info("Starting keep-alive loop...")
    logger.info("="*60)

    try:
        await keepalive_loop(profiles, manager)
    except KeyboardInterrupt:
        logger.info("\nReceived shutdown signal")
    except Exception as e:
        logger.error(f"\nUnexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        await manager.close()
        logger.info("GoLogin manager closed")


if __name__ == "__main__":
    asyncio.run(main())
