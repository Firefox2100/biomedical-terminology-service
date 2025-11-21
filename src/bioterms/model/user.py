from abc import ABC, abstractmethod
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
    key_hash: str = Field(
        ...,
        description='The argon2 hashed value of the API key.',
        alias='keyHash',
    )
    key_md5: str = Field(
        ...,
        description='The MD5 checksum of the API key.',
        alias='keyMd5',
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
