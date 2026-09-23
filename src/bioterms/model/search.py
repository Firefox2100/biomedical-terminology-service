"""Public models for the version 2 hybrid terminology search API."""

from typing import Literal, Optional

from pydantic import ConfigDict, Field, SerializeAsAny

from bioterms.etc.enums import ConceptPrefix
from .base import JsonModel
from .concept import Concept


class SearchMatchV2(JsonModel):
    """Why a result was placed in the response, without exposing backend score scales."""

    model_config = ConfigDict(serialize_by_alias=True)

    type: Literal['exact', 'hybrid']
    exact: bool
    field: Optional[Literal['conceptId', 'label', 'synonym']] = None
    text: Optional[str] = None


class SearchResultV2(JsonModel):
    """One ranked search result."""

    rank: int = Field(..., ge=1)
    concept: SerializeAsAny[Concept]
    match: Optional[SearchMatchV2] = None


class SearchPipelineV2(JsonModel):
    """Search stages actually used for this response."""

    lexical: bool = True
    vector: bool
    mapped: bool = False
    reranker: bool


class SearchMetadataV2(JsonModel):
    """Stable response metadata for clients and operational diagnostics."""

    model_config = ConfigDict(serialize_by_alias=True)

    returned: int = Field(..., ge=0)
    limit: int = Field(..., ge=1)
    duration_ms: float = Field(..., ge=0, alias='durationMs')
    vocabularies: list[ConceptPrefix]
    pipeline: SearchPipelineV2


class SearchResponseV2(JsonModel):
    """Envelope returned by GET search V2."""

    query: str
    results: list[SearchResultV2]
    meta: SearchMetadataV2
