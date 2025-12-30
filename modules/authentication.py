"""Authentication module for OnlyFans API.

Handles loading credentials from database (primary) or auth_multi.json (fallback).
Database credentials are encrypted using Fernet encryption.
Supports GoLogin integration for fresh credentials and proxy routing.
"""

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Optional, List, Tuple
from ultima_scraper_api import OnlyFansAPI, select_api
from ultima_scraper_api.apis.onlyfans.classes.extras import AuthDetails, CookieParser
from ultima_scraper_api.apis.onlyfans.authenticator import OnlyFansAuthenticator
from ultima_scraper_api.config import UltimaScraperAPIConfig

from modules.db_credential_loader import load_credentials_from_db
from modules.gologin_manager import GoLoginManager

logger = logging.getLogger(__name__)


def _get_prefer_database() -> bool:
    """Get PREFER_DATABASE setting from environment variable.

    :return: True if database should be used (production), False for JSON only (dev/test).
    """
    prefer_db = os.getenv("PREFER_DATABASE", "true").lower()
    return prefer_db in ("true", "1", "yes", "on")


async def load_auth_credentials_from_db(
    model_id: Optional[str] = None,
    only_authenticated: bool = False
) -> List[AuthDetails]:
    """Load authentication credentials from database.

    :param model_id: Optional specific model_id to load. If None, loads all active credentials.
    :param only_authenticated: If True, only load credentials with auth_status='authenticated'.
    :return: List of AuthDetails objects containing authentication credentials.
    """
    try:
        credentials = await load_credentials_from_db(
            model_id=model_id,
            only_authenticated=only_authenticated
        )

        auth_details_list = []
        for cred in credentials:
            auth_obj = cred.get('auth', {})

            auth_details = AuthDetails(
                id=cred.get('id'),
                username=cred.get('username', cred.get('name', '')),
                cookie=auth_obj.get('cookie', ''),
                x_bc=auth_obj.get('x_bc', ''),
                user_agent=auth_obj.get('user_agent', ''),
                email=cred.get('email', auth_obj.get('email', '')),
                password=cred.get('password', auth_obj.get('password', '')),
                support_2fa=auth_obj.get('support_2fa', True)
            )
            auth_details_list.append(auth_details)

        logger.info(f"Loaded {len(auth_details_list)} credential(s) from database")
        return auth_details_list

    except Exception as e:
        logger.error(f"Failed to load credentials from database: {e}")
        return []


async def load_auth_credentials(auth_file: str = "auth_multi.json") -> list[AuthDetails]:
    """Load authentication credentials from auth_multi.json file.

    :param auth_file: Path to the authentication JSON file
    :return: List of AuthDetails objects containing authentication credentials
    :raises FileNotFoundError: If authentication file does not exist
    """
    auth_path = Path(auth_file)

    if not auth_path.exists():
        raise FileNotFoundError(f"Authentication file not found: {auth_file}")

    with open(auth_path, 'r') as f:
        auth_data = json.load(f)

    auth_details_list = []

    # Handle different auth file structures
    if isinstance(auth_data, dict) and 'accounts' in auth_data:
        # Structure: {"accounts": [...], "settings": {...}}
        accounts = auth_data['accounts']
        for account in accounts:
            # Only process active accounts
            if not account.get('active', True):
                continue

            # Get auth data from nested 'auth' object
            auth_obj = account.get('auth', {})

            auth_details = AuthDetails(
                id=auth_obj.get('id'),
                username=account.get('name', ''),  # Use 'name' field as username
                cookie=auth_obj.get('cookie', ''),
                x_bc=auth_obj.get('x_bc', ''),
                user_agent=auth_obj.get('user_agent', ''),
                email=auth_obj.get('email', ''),
                password=auth_obj.get('password', ''),
                support_2fa=auth_obj.get('support_2fa', True)
            )
            auth_details_list.append(auth_details)

    elif isinstance(auth_data, list):
        # Structure: [{...}, {...}]
        for account in auth_data:
            auth_details = AuthDetails(
                id=account.get('id'),
                username=account.get('username', ''),
                cookie=account.get('cookie', ''),
                x_bc=account.get('x_bc', ''),
                user_agent=account.get('user_agent', ''),
                email=account.get('email', ''),
                password=account.get('password', ''),
                support_2fa=account.get('support_2fa', True)
            )
            auth_details_list.append(auth_details)

    elif isinstance(auth_data, dict):
        # Structure: single account dict
        auth_details = AuthDetails(
            id=auth_data.get('id'),
            username=auth_data.get('username', ''),
            cookie=auth_data.get('cookie', ''),
            x_bc=auth_data.get('x_bc', ''),
            user_agent=auth_data.get('user_agent', ''),
            email=auth_data.get('email', ''),
            password=auth_data.get('password', ''),
            support_2fa=auth_data.get('support_2fa', True)
        )
        auth_details_list.append(auth_details)

    return auth_details_list


async def authenticate_account(api: OnlyFansAPI, auth_details: AuthDetails):
    """Authenticate a single OnlyFans account using provided credentials.

    :param api: OnlyFansAPI instance
    :param auth_details: AuthDetails object with authentication credentials
    :return: Authenticated OnlyFansAuthModel or None if authentication fails
    """
    try:
        # Create authenticator directly
        authenticator = OnlyFansAuthenticator(api, auth_details)
        authed = await authenticator.login()

        if not authed or not authenticator.is_authed():
            print(f"✗ Authentication failed for: {auth_details.username}")
            if authenticator.errors:
                for error in authenticator.errors:
                    print(f"  Error: {error.message}")
            return None

        print(f"✓ Successfully authenticated: {auth_details.username or authed.user.username}")
        return authed

    except Exception as e:
        print(f"✗ Exception during authentication for {auth_details.username}: {str(e)}")
        return None


async def load_auth_credentials_hybrid(
    auth_file: str = "auth_multi.json",
    prefer_database: Optional[bool] = None,
    only_authenticated: bool = False
) -> List[AuthDetails]:
    """Load credentials from database or JSON.

    :param auth_file: Path to the authentication JSON file (fallback).
    :param prefer_database: If True, loads from database (REQUIRED for production). If False, uses JSON only (dev/test). If None, uses PREFER_DATABASE env var.
    :param only_authenticated: If True, only load authenticated credentials from database.
    :return: List of AuthDetails objects.
    :raises Exception: If prefer_database=True and database fails (fail fast - no fallback in production).
    """
    if prefer_database is None:
        prefer_database = _get_prefer_database()

    if prefer_database:
        logger.info("Loading credentials from database (production mode)...")
        auth_details_list = await load_auth_credentials_from_db(only_authenticated=only_authenticated)

        if not auth_details_list:
            raise RuntimeError(
                "No credentials found in database. Cannot continue - database is required for both "
                "authentication AND data storage. Check database connectivity and creator_credentials table."
            )

        logger.info(f"✓ Loaded {len(auth_details_list)} credential(s) from database")
        return auth_details_list
    else:
        logger.warning("Loading credentials from JSON file (development/test mode only)")
        return await load_auth_credentials(auth_file)


async def authenticate_all_accounts(
    auth_file: str = "auth_multi.json",
    prefer_database: Optional[bool] = None,
    only_authenticated: bool = False
) -> list:
    """Load and authenticate all accounts from database or auth_multi.json.

    :param auth_file: Path to the authentication JSON file (fallback).
    :param prefer_database: If True, tries database first. If False, uses JSON only. If None, uses PREFER_DATABASE env var.
    :param only_authenticated: If True, only load authenticated credentials from database.
    :return: List of authenticated OnlyFansAuthModel objects.
    """
    if prefer_database is None:
        prefer_database = _get_prefer_database()

    auth_details_list = await load_auth_credentials_hybrid(
        auth_file=auth_file,
        prefer_database=prefer_database,
        only_authenticated=only_authenticated
    )

    logger.info(f"Loaded {len(auth_details_list)} account(s)")

    api = OnlyFansAPI()

    authenticated_accounts = []
    for auth_details in auth_details_list:
        authed = await authenticate_account(api, auth_details)
        if authed:
            authenticated_accounts.append(authed)

    logger.info(f"Authentication complete: {len(authenticated_accounts)}/{len(auth_details_list)} successful")
    return authenticated_accounts


async def load_auth_async(
    creator_id: str,
    auth_file: str = "auth_multi.json",
    prefer_database: Optional[bool] = None
) -> Optional[AuthDetails]:
    """Load authentication details for a specific creator (async version).

    :param creator_id: Creator's OnlyFans ID.
    :param auth_file: Path to the authentication JSON file (fallback).
    :param prefer_database: If True, requires database (production mode). If False, uses JSON only (dev/test mode). If None, uses PREFER_DATABASE env var.
    :return: AuthDetails object or None if not found.
    :raises RuntimeError: If prefer_database=True and credential not found in database.
    """
    if prefer_database is None:
        prefer_database = _get_prefer_database()

    if prefer_database:
        logger.info(f"Loading credential for model_id {creator_id} from database (production mode)...")
        auth_details_list = await load_auth_credentials_from_db(model_id=creator_id)

        if not auth_details_list:
            raise RuntimeError(
                f"No credential found in database for model_id {creator_id}. Cannot continue - "
                "database is required for both authentication AND data storage. "
                "Check database connectivity and creator_credentials table."
            )

        logger.info(f"✓ Loaded credential for model_id {creator_id} from database")
        return auth_details_list[0]
    else:
        logger.warning(f"Loading credential for creator_id {creator_id} from {auth_file} (development/test mode only)")
        return load_auth_sync(creator_id, auth_file)


def load_auth_sync(creator_id: str, auth_file: str = "auth_multi.json") -> Optional[AuthDetails]:
    """Load authentication details for a specific creator from JSON (sync version).

    :param creator_id: Creator's OnlyFans ID.
    :param auth_file: Path to the authentication JSON file.
    :return: AuthDetails object or None if not found.
    """
    auth_path = Path(auth_file)
    if not auth_path.exists():
        return None

    with open(auth_path, 'r') as f:
        auth_data = json.load(f)

    if isinstance(auth_data, dict) and 'accounts' in auth_data:
        accounts = auth_data['accounts']
        for account in accounts:
            if not account.get('active', True):
                continue
            auth_obj = account.get('auth', {})
            if str(auth_obj.get('id')) == str(creator_id):
                return AuthDetails(
                    id=auth_obj.get('id'),
                    username=account.get('name', ''),
                    cookie=auth_obj.get('cookie', ''),
                    x_bc=auth_obj.get('x_bc', ''),
                    user_agent=auth_obj.get('user_agent', ''),
                    email=auth_obj.get('email', ''),
                    password=auth_obj.get('password', ''),
                    support_2fa=auth_obj.get('support_2fa', True)
                )
    elif isinstance(auth_data, list):
        for account in auth_data:
            if str(account.get('id')) == str(creator_id):
                return AuthDetails(
                    id=account.get('id'),
                    username=account.get('username', ''),
                    cookie=account.get('cookie', ''),
                    x_bc=account.get('x_bc', ''),
                    user_agent=account.get('user_agent', ''),
                    email=account.get('email', ''),
                    password=account.get('password', ''),
                    support_2fa=account.get('support_2fa', True)
                )

    return None


def load_auth(creator_id: str, auth_file: str = "auth_multi.json") -> Optional[AuthDetails]:
    """Load authentication details for a specific creator (sync wrapper).

    :param creator_id: Creator's OnlyFans ID.
    :param auth_file: Path to the authentication JSON file.
    :return: AuthDetails object or None if not found.
    """
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            return load_auth_sync(creator_id, auth_file)
        else:
            return loop.run_until_complete(load_auth_async(creator_id, auth_file))
    except RuntimeError:
        return asyncio.run(load_auth_async(creator_id, auth_file))


async def create_api_helper(
    auth_details: AuthDetails,
    logger,
    gologin_profile_id: Optional[str] = None,
    gologin_api_token: Optional[str] = None,
    use_gologin: Optional[bool] = None,
    use_gologin_credentials: bool = False
) -> Tuple[Optional[OnlyFansAPI], Optional[object]]:
    """Create OnlyFans API and authenticate, optionally using GoLogin for proxy.

    By default, only uses GoLogin for proxy routing while keeping database credentials.
    Set use_gologin_credentials=True to also override cookies/x_bc from GoLogin.

    :param auth_details: AuthDetails object with credentials.
    :param logger: Logger instance.
    :param gologin_profile_id: Optional GoLogin profile ID for proxy routing.
    :param gologin_api_token: Optional GoLogin API token. If not provided, uses GOLOGIN_API_TOKEN env var.
    :param use_gologin: If True, uses GoLogin for proxy. If None, auto-detects from gologin_profile_id.
    :param use_gologin_credentials: If True, also use GoLogin cookies/x_bc (default: False, use DB credentials).
    :return: Tuple of (api, authed) or (None, None) if failed.
    """
    # Determine if we should use GoLogin
    token = gologin_api_token or os.getenv("GOLOGIN_API_TOKEN")
    if use_gologin is None:
        use_gologin = bool(gologin_profile_id) and bool(token)

    try:
        proxy_url = None

        # If GoLogin is enabled, get proxy (and optionally credentials)
        if use_gologin and gologin_profile_id:
            logger.info(f"Using GoLogin profile {gologin_profile_id} for {auth_details.username}")

            try:
                gologin_manager = GoLoginManager(api_token=token)
                profile_data = await gologin_manager.get_profile(gologin_profile_id)

                if profile_data:
                    # Always get proxy from GoLogin
                    proxy_url = gologin_manager._extract_proxy_url(profile_data)
                    if proxy_url:
                        # Mask password in log
                        masked = proxy_url.split('@')[-1] if '@' in proxy_url else proxy_url
                        logger.info(f"  Proxy: {masked}")
                    else:
                        logger.warning(f"  No proxy configured in GoLogin profile")

                    # Only override credentials if explicitly requested
                    if use_gologin_credentials:
                        logger.info(f"  Using GoLogin credentials (use_gologin_credentials=True)")
                        fresh_creds = await gologin_manager.get_fresh_credentials(gologin_profile_id)
                        if fresh_creds:
                            if fresh_creds.get("cookies"):
                                auth_details.cookie = CookieParser(fresh_creds["cookies"])
                                logger.debug(f"  Overriding cookies from GoLogin")
                            if fresh_creds.get("x_bc"):
                                auth_details.x_bc = fresh_creds["x_bc"]
                                logger.debug(f"  Overriding x_bc from GoLogin")
                            if fresh_creds.get("user_agent"):
                                auth_details.user_agent = fresh_creds["user_agent"]
                                logger.debug(f"  Overriding user_agent from GoLogin")
                    else:
                        logger.info(f"  Using database credentials (proxy-only mode)")
                else:
                    logger.warning(f"  Could not get GoLogin profile, proceeding without proxy")

                await gologin_manager.close()

            except Exception as e:
                logger.warning(f"  GoLogin error: {str(e)}, proceeding without proxy")

        # Create API with optional proxy configuration
        if proxy_url:
            config = UltimaScraperAPIConfig()
            config.settings.network.proxies = [proxy_url]
            api = select_api("onlyfans", config=config)
            logger.info(f"  API configured with proxy routing")
        else:
            api = OnlyFansAPI()
            if use_gologin and gologin_profile_id:
                logger.warning(f"  API created WITHOUT proxy (GoLogin proxy not available)")

        # Authenticate
        authenticator = OnlyFansAuthenticator(api, auth_details)
        authed = await authenticator.login()

        if not authed or not authenticator.is_authed():
            logger.error(f"Authentication failed for: {auth_details.username}")
            if authenticator.errors:
                for error in authenticator.errors:
                    logger.error(f"  Error: {error.message}")
            return None, None

        logger.info(f"Successfully authenticated: {auth_details.username or authed.user.username}")
        return api, authed

    except Exception as e:
        logger.error(f"Exception during authentication: {str(e)}")
        return None, None


async def create_api_with_gologin(
    auth_details: AuthDetails,
    gologin_profile_id: str,
    logger,
    use_gologin_credentials: bool = False
) -> Tuple[Optional[OnlyFansAPI], Optional[object]]:
    """Create OnlyFans API with GoLogin integration (convenience function).

    This is a shortcut for create_api_helper with GoLogin always enabled.
    By default only uses GoLogin proxy, keeping database credentials.

    :param auth_details: AuthDetails object with credentials.
    :param gologin_profile_id: GoLogin profile ID (required).
    :param logger: Logger instance.
    :param use_gologin_credentials: If True, also use GoLogin cookies/x_bc (default: False).
    :return: Tuple of (api, authed) or (None, None) if failed.
    """
    return await create_api_helper(
        auth_details=auth_details,
        logger=logger,
        gologin_profile_id=gologin_profile_id,
        use_gologin=True,
        use_gologin_credentials=use_gologin_credentials
    )
