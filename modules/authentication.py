"""Authentication module for OnlyFans API.

Handles loading credentials from auth_multi.json and authenticating with OnlyFans.
"""

import json
from pathlib import Path
from typing import Optional
from ultima_scraper_api import OnlyFansAPI
from ultima_scraper_api.apis.onlyfans.classes.extras import AuthDetails
from ultima_scraper_api.apis.onlyfans.authenticator import OnlyFansAuthenticator


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


async def authenticate_all_accounts(auth_file: str = "auth_multi.json") -> list:
    """Load and authenticate all accounts from auth_multi.json.

    :param auth_file: Path to the authentication JSON file
    :return: List of authenticated OnlyFansAuthModel objects
    """
    # Load credentials
    auth_details_list = await load_auth_credentials(auth_file)
    print(f"Loaded {len(auth_details_list)} account(s) from {auth_file}")

    # Create API instance
    api = OnlyFansAPI()

    # Authenticate all accounts
    authenticated_accounts = []
    for auth_details in auth_details_list:
        authed = await authenticate_account(api, auth_details)
        if authed:
            authenticated_accounts.append(authed)

    print(f"\nAuthentication complete: {len(authenticated_accounts)}/{len(auth_details_list)} successful")
    return authenticated_accounts


def load_auth(creator_id: str, auth_file: str = "auth_multi.json") -> Optional[AuthDetails]:
    """Load authentication details for a specific creator.

    :param creator_id: Creator's OnlyFans ID
    :param auth_file: Path to the authentication JSON file
    :return: AuthDetails object or None if not found
    """
    auth_path = Path(auth_file)
    if not auth_path.exists():
        return None

    with open(auth_path, 'r') as f:
        auth_data = json.load(f)

    # Handle different auth file structures
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


async def create_api_helper(auth_details: AuthDetails, logger):
    """Create OnlyFans API and authenticate.

    :param auth_details: AuthDetails object with credentials
    :param logger: Logger instance
    :return: Tuple of (api, authed) or (None, None) if failed
    """
    try:
        api = OnlyFansAPI()
        authenticator = OnlyFansAuthenticator(api, auth_details)
        authed = await authenticator.login()

        if not authed or not authenticator.is_authed():
            logger.error(f"✗ Authentication failed for: {auth_details.username}")
            if authenticator.errors:
                for error in authenticator.errors:
                    logger.error(f"  Error: {error.message}")
            return None, None

        logger.info(f"✓ Successfully authenticated: {auth_details.username or authed.user.username}")
        return api, authed

    except Exception as e:
        logger.error(f"✗ Exception during authentication: {str(e)}")
        return None, None
