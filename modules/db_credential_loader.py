"""Database credential loader for OnlyFans authentication.

Loads encrypted credentials from the creator_credentials database table,
decrypts them using Fernet encryption, and converts to AuthDetails format.
"""

import json
import logging
import os
from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from models.db_models import CreatorCredential
from modules.encryption import EncryptionManager

logger = logging.getLogger(__name__)


class DatabaseCredentialLoader:
    """Loads creator credentials from database with decryption support."""

    def __init__(self, database_url: Optional[str] = None, encryption_key: Optional[str] = None):
        """Initialize the database credential loader.

        :param database_url: PostgreSQL connection string. If not provided, reads from DATABASE_URL env var.
        :param encryption_key: Fernet encryption key. If not provided, reads from ENCRYPTION_KEY env var.
        :raises ValueError: If required environment variables are not set.
        """
        if database_url is None:
            database_url = os.getenv("DATABASE_URL")

        if not database_url:
            raise ValueError("DATABASE_URL must be provided or set in environment variables")

        if database_url.startswith("postgresql://"):
            database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
            logger.info("Converted postgresql:// to postgresql+asyncpg:// for async driver")

        self.engine = create_async_engine(
            database_url,
            pool_size=2,
            max_overflow=1,
            pool_pre_ping=True,
            pool_recycle=3600,
            echo=False
        )
        self.async_session = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )

        self.encryption_manager = EncryptionManager(encryption_key)
        logger.info("Database credential loader initialized")

    async def load_credential_by_model_id(self, model_id: str) -> Optional[dict]:
        """Load and decrypt a single creator credential by model_id.

        :param model_id: OnlyFans model/creator ID.
        :return: Decrypted credential dictionary with auth details, or None if not found.
        """
        async with self.async_session() as session:
            try:
                stmt = select(CreatorCredential).where(
                    (CreatorCredential.model_id == model_id) &
                    (CreatorCredential.status == 'active')
                )
                result = await session.execute(stmt)
                credential = result.scalar_one_or_none()

                if not credential:
                    logger.warning(f"No active credential found for model_id: {model_id}")
                    return None

                return self._decrypt_credential(credential)

            except Exception as e:
                logger.error(f"Failed to load credential for model_id {model_id}: {e}")
                return None

    async def load_all_active_credentials(self) -> List[dict]:
        """Load and decrypt all active creator credentials.

        :return: List of decrypted credential dictionaries.
        """
        async with self.async_session() as session:
            try:
                stmt = select(CreatorCredential).where(
                    CreatorCredential.status == 'active'
                ).order_by(CreatorCredential.account_name)

                result = await session.execute(stmt)
                credentials = result.scalars().all()

                decrypted_credentials = []
                for credential in credentials:
                    decrypted = self._decrypt_credential(credential)
                    if decrypted:
                        decrypted_credentials.append(decrypted)

                logger.info(f"Loaded {len(decrypted_credentials)} active credentials from database")
                return decrypted_credentials

            except Exception as e:
                logger.error(f"Failed to load active credentials: {e}")
                return []

    async def load_authenticated_credentials(self) -> List[dict]:
        """Load only successfully authenticated credentials.

        :return: List of decrypted credential dictionaries with auth_status='authenticated'.
        """
        async with self.async_session() as session:
            try:
                stmt = select(CreatorCredential).where(
                    (CreatorCredential.status == 'active') &
                    (CreatorCredential.auth_status == 'authenticated')
                ).order_by(CreatorCredential.account_name)

                result = await session.execute(stmt)
                credentials = result.scalars().all()

                decrypted_credentials = []
                for credential in credentials:
                    decrypted = self._decrypt_credential(credential)
                    if decrypted:
                        decrypted_credentials.append(decrypted)

                logger.info(f"Loaded {len(decrypted_credentials)} authenticated credentials from database")
                return decrypted_credentials

            except Exception as e:
                logger.error(f"Failed to load authenticated credentials: {e}")
                return []

    def _decrypt_credential(self, credential: CreatorCredential) -> Optional[dict]:
        """Decrypt a single credential record.

        :param credential: CreatorCredential SQLAlchemy model instance.
        :return: Dictionary with decrypted auth details, or None if decryption fails.
        """
        try:
            result = {
                "id": int(credential.model_id),
                "username": credential.account_name,
                "name": credential.account_name,
                "active": True,
                "auth_status": credential.auth_status,
                "gologin_profile_id": credential.gologin_profile_id
            }

            if credential.encrypted_auth_json:
                auth_json = self.encryption_manager.decrypt_json(credential.encrypted_auth_json)
                if auth_json:
                    result["auth"] = auth_json
                    logger.debug(f"Decrypted auth_json for {credential.account_name}")
                else:
                    logger.warning(f"Failed to decrypt auth_json for {credential.account_name}")
                    return None
            else:
                logger.warning(f"No encrypted_auth_json found for {credential.account_name}")
                return None

            if credential.encrypted_email:
                email = self.encryption_manager.decrypt(credential.encrypted_email)
                if email:
                    result["email"] = email

            if credential.encrypted_password:
                password = self.encryption_manager.decrypt(credential.encrypted_password)
                if password:
                    result["password"] = password

            return result

        except Exception as e:
            logger.error(f"Failed to decrypt credential for {credential.account_name}: {e}")
            return None

    async def close(self):
        """Close database connections."""
        await self.engine.dispose()
        logger.info("Database credential loader closed")


async def load_credentials_from_db(
    model_id: Optional[str] = None,
    database_url: Optional[str] = None,
    encryption_key: Optional[str] = None,
    only_authenticated: bool = False
) -> List[dict]:
    """Load credentials from database with automatic connection management.

    :param model_id: Optional specific model_id to load. If None, loads all active credentials.
    :param database_url: PostgreSQL connection string. If not provided, reads from DATABASE_URL env var.
    :param encryption_key: Fernet encryption key. If not provided, reads from ENCRYPTION_KEY env var.
    :param only_authenticated: If True, only load credentials with auth_status='authenticated'.
    :return: List of decrypted credential dictionaries.
    """
    loader = DatabaseCredentialLoader(database_url, encryption_key)

    try:
        if model_id:
            credential = await loader.load_credential_by_model_id(model_id)
            return [credential] if credential else []
        elif only_authenticated:
            return await loader.load_authenticated_credentials()
        else:
            return await loader.load_all_active_credentials()
    finally:
        await loader.close()
