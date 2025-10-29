"""
Extended pydantic.BaseModel for automatic JSON serialization.
"""

from typing import Any
from pydantic import BaseModel


class JsonModel(BaseModel):
    """
    A BaseModel that serialises using `mode='json'` by default
    """

    def model_dump(self, *, mode: str = "json", **kwargs: Any) -> dict:
        return super().model_dump(mode=mode, **kwargs)

    def model_dump_json(self, **kwargs: Any) -> str:
        return super().model_dump_json(**kwargs)
