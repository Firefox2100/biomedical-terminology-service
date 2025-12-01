from datetime import datetime
from typing import List, Optional
from pydantic import Field, ConfigDict

from bioterms.etc.enums import ConceptPrefix, SimilarityMethod
from .base import JsonModel


class VocabularyStatus(JsonModel):
    """
    A vocabulary status model indicating the current state of a vocabulary.
    """

    model_config = ConfigDict(
        extra='forbid',
        serialize_by_alias=True,
    )

    prefix: ConceptPrefix = Field(
        ...,
        description='The prefix of the vocabulary.',
    )
    name: str = Field(
        ...,
        description='The name of the vocabulary.',
    )
    file_downloaded: bool = Field(
        False,
        description='Indicates whether the vocabulary file has been downloaded.',
        alias='fileDownloaded',
    )
    file_download_time: Optional[datetime] = Field(
        None,
        description='The timestamp when the vocabulary file was downloaded.',
        alias='fileDownloadTime',
    )
    loaded: bool = Field(
        False,
        description='Indicates whether the vocabulary is loaded in the system.',
    )
    concept_count: int = Field(
        0,
        description='The number of concepts in the vocabulary.',
        alias='conceptCount',
    )
    relationship_count: int = Field(
        0,
        description='The number of relationships in the vocabulary.',
        alias='relationshipCount',
    )
    annotations: List[ConceptPrefix] = Field(
        default_factory=list,
        description='A list of annotation vocabularies associated with this vocabulary.',
    )
    similarity_methods: List[SimilarityMethod] = Field(
        default_factory=list,
        description='A list of similarity methods available for this vocabulary.',
        alias='similarityMethods',
    )
