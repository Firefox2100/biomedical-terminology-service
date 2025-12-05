from abc import ABC, abstractmethod
from datetime import datetime, timezone
from uuid import UUID, uuid4
from typing import Optional
from argon2.exceptions import VerifyMismatchError
from pydantic import Field, ConfigDict

from bioterms.etc.consts import PH
from .base import JsonModel


class UserApiKey(JsonModel):
    """
    A model for an API key associated with a user.
    """

    model_config = ConfigDict(
        extra='forbid',
        serialize_by_alias=True,
    )

    name: str = Field(
        ...,
        description='The name of the API key.',
    )
    key_id: UUID = Field(
        default_factory=uuid4,
        description='The unique identifier for the API key.',
        alias='keyId',
    )
    key_hash: str = Field(
        ...,
        description='The HMAC-SHA-256 hashed value of the API key.',
        alias='keyHash',
    )
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description='The timestamp when the API key was created.',
        alias='createdAt',
    )


class User(JsonModel):
    """
    A model for an admin user in the system
    """

    model_config = ConfigDict(
        extra='forbid',
        serialize_by_alias=True,
    )

    username: str = Field(
        ...,
        description='The username of the user.',
    )
    password: str = Field(
        ...,
        description='The password of the user. Stored in database as argon 2 hash.',
    )
    api_keys: Optional[list[UserApiKey]] = Field(
        None,
        description='A list of API keys associated with the user.',
        alias='apiKeys',
    )

    def validate_password(self, password: str) -> bool:
        """
        Validate the provided password against the stored password hash.
        :param password: The password to validate.
        :return: True if the password is valid, False otherwise.
        """
        try:
            PH.verify(self.password, password)
            return True
        except VerifyMismatchError:
            return False


class UserRepository(ABC):
    """
    An interface for database operations related to Network entities.
    """

    @abstractmethod
    async def get(self, username: str) -> User | None:
        """
        Retrieve a user by their username.
        :param username: The username of the user to retrieve.
        :return: User object or None if not found.
        """

    @abstractmethod
    async def filter(self) -> list[User]:
        """
        Get a list of all User entities.
        :return: A list of User instances.
        """

    @abstractmethod
    async def save(self, user: User):
        """
        Save a User entity to the database.
        :param user: An instance of User to be saved.
        """

    @abstractmethod
    async def update(self, user: User):
        """
        Update an existing User entity in the database.
        :param user: An instance of User to be updated.
        """

    @abstractmethod
    async def delete(self, username: str):
        """
        Delete a User entity from the database.
        :param username: The username of the user to be deleted.
        """

    @abstractmethod
    async def save_api_key(self,
                           username: str,
                           api_key: UserApiKey,
                           ):
        """
        Save an API key for a user.
        :param username: The username of the user to associate the API key with.
        :param api_key: The UserApiKey instance to be saved.
        """

    @abstractmethod
    async def delete_api_key(self,
                             username: str,
                             key_id: UUID,
                             ):
        """
        Delete an API key for a user.
        :param username: The username of the user to disassociate the API key from.
        :param key_id: The UUID of the API key to be deleted.
        """

    @abstractmethod
    async def get_user_by_api_key(self,
                                  key_hash: str,
                                  ) -> User | None:
        """
        Retrieve a user by their API key hash.
        :param key_hash: The HMAC-SHA-256 hashed value of the API key.
        :return: User object or None if not found.
        """
