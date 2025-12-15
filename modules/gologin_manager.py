"""GoLogin Manager - Manages GoLogin profiles for proxy and credential management.

Handles:
- Profile lifecycle (start, stop)
- Keep-alive pings to prevent session expiration
- Fresh credential retrieval (cookies, x_bc, user_agent)
- Proxy URL extraction from profiles
"""

import asyncio
import logging
import os
from typing import Dict, List, Optional

import aiohttp

logger = logging.getLogger(__name__)


class GoLoginManager:
    """Manages GoLogin profiles - keep-alive, credentials, proxy."""

    API_BASE = "https://api.gologin.com"
    PING_INTERVAL = 300  # 5 minutes

    def __init__(self, api_token: Optional[str] = None):
        """Initialize GoLogin manager.

        :param api_token: GoLogin API token. If not provided, reads from GOLOGIN_API_TOKEN env var.
        :raises ValueError: If no API token is provided or found in environment.
        """
        self.api_token = api_token or os.getenv("GOLOGIN_API_TOKEN")
        if not self.api_token:
            raise ValueError("GOLOGIN_API_TOKEN must be provided or set in environment variables")

        self.headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json"
        }
        self.active_profiles: Dict[str, dict] = {}
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(headers=self.headers)
        return self._session

    async def close(self):
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()

    async def get_profile(self, profile_id: str) -> Optional[dict]:
        """Get profile data from GoLogin API.

        :param profile_id: GoLogin profile ID.
        :return: Profile data dictionary or None if not found.
        """
        session = await self._get_session()
        url = f"{self.API_BASE}/browser/{profile_id}"

        try:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.debug(f"Retrieved profile {profile_id}")
                    return data
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to get profile {profile_id}: {response.status} - {error_text}")
                    return None
        except Exception as e:
            logger.error(f"Error getting profile {profile_id}: {str(e)}")
            return None

    async def get_profile_cookies(self, profile_id: str) -> Optional[str]:
        """Get cookies from GoLogin profile.

        :param profile_id: GoLogin profile ID.
        :return: Cookie string or None if not found.
        """
        session = await self._get_session()
        url = f"{self.API_BASE}/browser/{profile_id}/cookies"

        try:
            async with session.get(url) as response:
                if response.status == 200:
                    cookies_data = await response.json()
                    # Convert cookies array to cookie string format
                    if isinstance(cookies_data, list):
                        cookie_str = "; ".join(
                            f"{c.get('name')}={c.get('value')}"
                            for c in cookies_data
                            if c.get('name') and c.get('value')
                        )
                        logger.debug(f"Retrieved {len(cookies_data)} cookies for profile {profile_id}")
                        return cookie_str
                    return None
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to get cookies for {profile_id}: {response.status} - {error_text}")
                    return None
        except Exception as e:
            logger.error(f"Error getting cookies for {profile_id}: {str(e)}")
            return None

    def _extract_proxy_url(self, profile_data: dict) -> Optional[str]:
        """Extract proxy URL from profile data.

        :param profile_data: Profile data from GoLogin API.
        :return: Proxy URL in format http://user:pass@host:port or None.
        """
        proxy = profile_data.get("proxy", {})
        if not proxy:
            return None

        mode = proxy.get("mode", "")
        if mode == "none":
            return None

        host = proxy.get("host", "")
        port = proxy.get("port", "")

        if not host or not port:
            return None

        username = proxy.get("username", "")
        password = proxy.get("password", "")

        if username and password:
            return f"http://{username}:{password}@{host}:{port}"
        return f"http://{host}:{port}"

    def _extract_x_bc(self, profile_data: dict) -> Optional[str]:
        """Extract x_bc from profile data.

        x_bc is typically stored in the profile's fingerprint or extension data.
        It may also be in cookies with name 'fp' or similar.

        :param profile_data: Profile data from GoLogin API.
        :return: x_bc value or None.
        """
        # Check if x_bc is stored in profile extensions or custom data
        # This depends on how your automation stores x_bc in the profile
        extensions = profile_data.get("extensions", {})
        if isinstance(extensions, dict) and "x_bc" in extensions:
            return extensions.get("x_bc")

        # Check in profile's storage
        storage = profile_data.get("storage", {})
        if isinstance(storage, dict) and "x_bc" in storage:
            return storage.get("x_bc")

        # Check in profile's custom data
        custom_data = profile_data.get("customData", {})
        if isinstance(custom_data, dict) and "x_bc" in custom_data:
            return custom_data.get("x_bc")

        # x_bc might need to be extracted from cookies
        # Return None and let caller handle it
        return None

    async def get_fresh_credentials(self, profile_id: str) -> Optional[dict]:
        """Get fresh credentials from GoLogin profile.

        Retrieves cookies, x_bc, user_agent, and proxy URL from the profile.

        :param profile_id: GoLogin profile ID.
        :return: Dictionary with credentials or None if failed.
        """
        # Get profile data
        profile_data = await self.get_profile(profile_id)
        if not profile_data:
            logger.error(f"Could not retrieve profile {profile_id}")
            return None

        # Get cookies
        cookies = await self.get_profile_cookies(profile_id)

        # Extract x_bc from profile or cookies
        x_bc = self._extract_x_bc(profile_data)

        # If x_bc not in profile data, try to find it in cookies
        if not x_bc and cookies:
            # Look for 'fp' cookie which is often x_bc
            for cookie_pair in cookies.split("; "):
                if cookie_pair.startswith("fp="):
                    x_bc = cookie_pair.split("=", 1)[1]
                    break

        # Extract proxy URL
        proxy_url = self._extract_proxy_url(profile_data)

        # Get user agent
        user_agent = profile_data.get("navigator", {}).get("userAgent", "")
        if not user_agent:
            user_agent = profile_data.get("userAgent", "")

        credentials = {
            "cookies": cookies,
            "x_bc": x_bc,
            "user_agent": user_agent,
            "proxy_url": proxy_url,
            "profile_id": profile_id,
            "profile_name": profile_data.get("name", "")
        }

        logger.info(f"Retrieved fresh credentials for profile {profile_id} ({profile_data.get('name', 'unknown')})")
        logger.debug(f"  Cookies: {'Yes' if cookies else 'No'}")
        logger.debug(f"  x_bc: {'Yes' if x_bc else 'No'}")
        logger.debug(f"  Proxy: {proxy_url or 'None'}")

        return credentials

    async def ping_profile(self, profile_id: str) -> bool:
        """Ping a profile to keep it alive.

        This makes an API call to the profile endpoint to maintain the session.

        :param profile_id: GoLogin profile ID.
        :return: True if ping succeeded, False otherwise.
        """
        session = await self._get_session()
        url = f"{self.API_BASE}/browser/{profile_id}"

        try:
            async with session.get(url) as response:
                if response.status == 200:
                    logger.debug(f"Ping successful for profile {profile_id}")
                    return True
                else:
                    logger.warning(f"Ping failed for profile {profile_id}: {response.status}")
                    return False
        except Exception as e:
            logger.error(f"Ping error for profile {profile_id}: {str(e)}")
            return False

    async def ping_all_profiles(self, profile_ids: List[str]) -> Dict[str, bool]:
        """Ping multiple profiles to keep them alive.

        :param profile_ids: List of GoLogin profile IDs.
        :return: Dictionary mapping profile_id to ping success status.
        """
        results = {}
        for profile_id in profile_ids:
            results[profile_id] = await self.ping_profile(profile_id)
            # Small delay between pings to avoid rate limiting
            await asyncio.sleep(0.5)
        return results

    async def start_keepalive_loop(self, profile_ids: List[str], on_failure: Optional[callable] = None):
        """Start a keep-alive loop for multiple profiles.

        Pings all profiles every PING_INTERVAL seconds.

        :param profile_ids: List of GoLogin profile IDs to keep alive.
        :param on_failure: Optional callback function called when a ping fails.
                          Receives (profile_id, error_message) as arguments.
        """
        logger.info(f"Starting keep-alive loop for {len(profile_ids)} profiles (interval: {self.PING_INTERVAL}s)")

        while True:
            results = await self.ping_all_profiles(profile_ids)

            # Check for failures
            failed = [pid for pid, success in results.items() if not success]
            if failed:
                logger.error(f"Keep-alive failed for profiles: {failed}")
                if on_failure:
                    for profile_id in failed:
                        on_failure(profile_id, "Ping failed")

            # Log success count
            success_count = sum(1 for success in results.values() if success)
            logger.info(f"Keep-alive ping: {success_count}/{len(profile_ids)} profiles OK")

            # Wait for next ping interval
            await asyncio.sleep(self.PING_INTERVAL)


async def get_gologin_credentials(profile_id: str, api_token: Optional[str] = None) -> Optional[dict]:
    """Convenience function to get fresh credentials from a GoLogin profile.

    :param profile_id: GoLogin profile ID.
    :param api_token: Optional API token (uses env var if not provided).
    :return: Dictionary with cookies, x_bc, user_agent, proxy_url or None.
    """
    manager = GoLoginManager(api_token)
    try:
        return await manager.get_fresh_credentials(profile_id)
    finally:
        await manager.close()


async def get_gologin_proxy(profile_id: str, api_token: Optional[str] = None) -> Optional[str]:
    """Convenience function to get proxy URL from a GoLogin profile.

    :param profile_id: GoLogin profile ID.
    :param api_token: Optional API token (uses env var if not provided).
    :return: Proxy URL or None.
    """
    manager = GoLoginManager(api_token)
    try:
        profile_data = await manager.get_profile(profile_id)
        if profile_data:
            return manager._extract_proxy_url(profile_data)
        return None
    finally:
        await manager.close()
