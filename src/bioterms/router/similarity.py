from typing import List, Optional, Union
from pydantic import Field, ConfigDict
from fastapi import APIRouter, Query, Depends
from fastapi.responses import StreamingResponse

from bioterms.etc.enums import ConceptPrefix, SimilarityMethod
from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.model.base import JsonModel
from bioterms.model.similar_term import SimilarTerm
from .utils import response_generator


similarity_router = APIRouter(
    prefix='/api/vocabularies',
    tags=['Similarity'],
)


class SimilarityRequestV1(JsonModel):
    """
    Request model for the expand terms endpoint (v1).
    """

    model_config = ConfigDict(
        extra='forbid',
        serialize_by_alias=True,
    )

    term_ids: List[str] = Field(
        ...,
        description='List of term IDs to query similarity scores on.',
        alias='termIds',
    )
    threshold: float = Field(
        1.0,
        description='The threshold of the similarity score to return. 0 to return all. '
                    'Please note that the server may have chosen to store only connections which '
                    'similarity score are above a certain value. In this case, since the data is '
                    'not stored, they will not be returned even if the threshold is set to lower.',
        ge=0.0,
        le=1.0,
    )


class TranslateRequestV1(JsonModel):
    """
    Request model for the translate terms endpoint (v1).
    """

    model_config = ConfigDict(
        extra='forbid',
        serialize_by_alias=True,
    )

    term_ids: List[str] = Field(
        ...,
        description='List of term IDs to translate.',
        alias='termIds',
    )
    constraint_ids: List[str] = Field(
        ...,
        description='List of constraint term IDs to filter the translations.',
        alias='constraintIds',
    )
    threshold: Union[float, List[float]] = Field(
        ...,
        description='The similarity score threshold(s) for the translations. '
                    'If a single float is provided, it will be applied to all constraint IDs. '
                    'If a list is provided, it must match the length of constraint IDs.',
    )


class SimilarTermV1(JsonModel):
    """
    Data model for an expanded term in the expand terms response (v1).
    """

    model_config = ConfigDict(
        extra='forbid',
        serialize_by_alias=True,
    )

    term_id: str = Field(
        ...,
        description='The term ID of the expanded term.',
        alias='termId',
    )
    similar_ids: List[str] = Field(
        ...,
        description='List of similar term IDs.',
        alias='similarIds',
    )
    similarity_threshold: float = Field(
        ...,
        description='The threshold of the similarity score to return',
        alias='similarityThreshold',
    )
    threshold: Optional[float] = Field(
        None,
        description='The maximum number of similar terms to return for one original term ID',
    )


class TranslatedTermV1(JsonModel):
    """
    Data model for a translated term in the translate terms response (v1).
    """

    model_config = ConfigDict(
        extra='forbid',
        serialize_by_alias=True,
    )

    term_id: str = Field(
        ...,
        description='The original term ID to be translated.',
        alias='termId',
    )
    score: float = Field(
        ...,
        description='The similarity score of the translated term.',
    )


@similarity_router.post('/{prefix}/similarity/v1', response_model=List[SimilarTermV1])
async def get_similar_terms_v1(prefix: ConceptPrefix,
                               requested_terms: SimilarityRequestV1,
                               result_threshold: int = Query(
                                   0,
                                   description='The maximum number of terms to return in the response. 0 for no limit.'
                               ),
                               graph_db: GraphDatabase = Depends(get_active_graph_db),
                               ):
    """
    Get similar terms for the requested term IDs (V1). This endpoint is compatible with Cafe Variome
    V3 backend.
    \f
    :param prefix: The vocabulary prefix.
    :param requested_terms: The requested terms for similarity search.
    :param result_threshold: The maximum number of terms to return in the response. 0 for no limit.
    :param graph_db: The graph database instance.
    :return: A list of similar terms with their similarity scores.
    """
    similarity_iter = graph_db.get_similar_terms_iter(
        prefix=prefix,
        concept_ids=requested_terms.term_ids,
        threshold=requested_terms.threshold,
        limit=result_threshold if result_threshold > 0 else None,
    )

    v1_similar_terms = []
    async for similar_term in similarity_iter:
        similar_concepts = similar_term.similar_groups[0].similar_concepts if similar_term.similar_groups else []

        v1_similar_terms.append(
            SimilarTermV1(
                termId=similar_term.concept_id,
                similarIds=[concept.concept_id for concept in similar_concepts],
                similarityThreshold=requested_terms.threshold,
                threshold=result_threshold if result_threshold > 0 else None,
            )
        )

    return v1_similar_terms


@similarity_router.get('/{prefix}/similarity/v2', response_model=List[SimilarTerm])
async def get_similar_terms_v2(prefix: ConceptPrefix,
                               concept_ids: List[str] = Query(
                                   ...,
                                   description='List of concept IDs to get similar concepts for.'
                               ),
                               threshold: float = Query(
                                   1.0,
                                   description='Minimum similarity score to consider a term as similar. '
                                               '0 to return all. Please note that the server may have chosen to '
                                               'store only connections which similarity score are above a '
                                               'certain value. In this case, since the data is not stored, '
                                               'they will not be returned even if the threshold is set to lower.',
                                   ge=0.0,
                                   le=1.0,
                               ),
                               same_prefix: bool = Query(
                                   True,
                                   description='Whether to only return similar terms with the same prefix as the '
                                               'original term.',
                                   alias='same-prefix',
                               ),
                               corpus: Optional[ConceptPrefix] = Query(
                                   None,
                                   description='If specified, only consider similarity scores calculated within '
                                               'the given corpus/prefix.',
                               ),
                               method: Optional[SimilarityMethod] = Query(
                                   None,
                                   description='If specified, only consider similarity scores calculated with the '
                                               'given method.',
                               ),
                               limit: Optional[int] = Query(
                                   None,
                                   description='Maximum number of descendants to return for each term.',
                                   ge=1,
                               ),
                               graph_db: GraphDatabase = Depends(get_active_graph_db),
                               ):
    similarity_iter = graph_db.get_similar_terms_iter(
        prefix=prefix,
        concept_ids=concept_ids,
        threshold=threshold,
        same_prefix=same_prefix,
        corpus_prefix=corpus,
        method=method,
        limit=limit,
    )

    return StreamingResponse(
        response_generator(similarity_iter),
        media_type='application/json'
    )


@similarity_router.post('/{prefix}/translate/v1', response_model=List[TranslatedTermV1])
async def translate_terms_v1(prefix: ConceptPrefix,
                             translate_request: TranslateRequestV1,
                             result_threshold: int = Query(
                                 0,
                                 description='The maximum number of terms to return in the response. 0 for no limit.'
                             ),
                             graph_db: GraphDatabase = Depends(get_active_graph_db),
                             ):
    """
    Translate terms for the requested term IDs (V1). This endpoint is compatible with Cafe Variome V3 backend.
    \f
    :param prefix: The vocabulary prefix.
    :param translate_request: The requested terms for translation.
    :param result_threshold: The maximum number of terms to return in the response. 0 for no limit.
    :param graph_db: The graph database instance.
    :return: A list of translated terms with their similarity scores.
    """
    translate_iter = graph_db.translate_terms_iter(
        original_ids=translate_request.original_ids,
        original_prefix=prefix,
        constraint_ids={
            prefix: set(translate_request.constraint_ids),
        },
        threshold=translate_request.threshold,
        limit=result_threshold if result_threshold > 0 else None,
    )

    v1_translated_terms = []
    async for translated_term in translate_iter:
        v1_translated_terms.append(TranslatedTermV1(
            termId=translated_term.concept_id,
            score=translated_term.similarity,
        ))

    return v1_translated_terms


@similarity_router.get('/{prefix}/translate/v2', response_model=List[SimilarTerm])
async def translate_terms_v2(prefix: ConceptPrefix,
                             original_ids: List[str] = Query(
                                 ...,
                                 description='List of concept IDs to get similar concepts for.'
                             ),
                             constraint_concepts: List[str] = Query(
                                 ...,
                                 description='List of constraint concept IDs to filter the translations. '
                                             'In prefix:id format.'
                             ),
                             threshold: float = Query(
                                 1.0,
                                 description='Minimum similarity score to consider a term as similar. '
                                             '0 to return all. Please note that the server may have chosen to '
                                             'store only connections which similarity score are above a '
                                             'certain value. In this case, since the data is not stored, '
                                             'they will not be returned even if the threshold is set to lower.',
                                 ge=0.0,
                                 le=1.0,
                             ),
                             limit: Optional[int] = Query(
                                 None,
                                 description='Maximum number of descendants to return for each term.',
                                 ge=1,
                             ),
                             graph_db: GraphDatabase = Depends(get_active_graph_db),
                             ):
    """
    Translate terms for the requested term IDs (V2).
    \f
    :param prefix: The vocabulary prefix.
    :param original_ids: The original concept IDs to translate.
    :param constraint_concepts: The constraint concept IDs to filter the translations.
    :param threshold: The minimum similarity score to consider a term as similar.
    :param limit: Maximum number of descendants to return for each term.
    :param graph_db: The graph database instance.
    :return:
    """
    constraint_dict = {}
    for concept in constraint_concepts:
        try:
            concept_prefix_str, concept_id = concept.split(':', 1)
            concept_prefix = ConceptPrefix(concept_prefix_str)
        except ValueError:
            raise ValueError(f'Invalid constraint concept format: {concept}. Expected format is prefix:id')

        if concept_prefix not in constraint_dict:
            constraint_dict[concept_prefix] = set()
        constraint_dict[concept_prefix].add(concept_id)

    translate_iter = graph_db.translate_terms_iter(
        original_ids=original_ids,
        original_prefix=prefix,
        constraint_ids=constraint_dict,
        threshold=threshold,
        limit=limit,
    )

    return StreamingResponse(
        response_generator(translate_iter),
        media_type='application/json'
    )
