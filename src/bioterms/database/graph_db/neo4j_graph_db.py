import asyncio
import time
from typing import LiteralString, AsyncIterator, Iterable, Optional
import networkx as nx
from neo4j import AsyncDriver, AsyncSession
from neo4j.exceptions import TransientError

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, SimilarityMethod, ConceptRelationshipType, AnnotationType
from bioterms.etc.utils import batch_iterable, verbose_print, aiter_progress
from bioterms.etc.metrics import GRAPHDB_OP_DURATION, GRAPHDB_OP_TTFR, GRAPHDB_OP_ERRORS, \
    GRAPHDB_OP_RETRYS, EXPAND_DESC_COUNT, MAP_COUNT, SIM_GROUPS, SIM_PER_GROUP, SIM_TOTAL
from bioterms.model.concept import Concept, GRAPH_NODE_EXTRA_PROPERTIES
from bioterms.model.annotation import Annotation
from bioterms.model.concept_path import NodeInPath, ConceptPath
from bioterms.model.related_term import RelatedTerm
from bioterms.model.similar_term import SimilarTermWithScores, SimilarTermByPrefix, SimilarTerm, \
    SimilarTermAggregate
from bioterms.model.translated_term import TranslatedTerm
from .graph_db import ReactomeRepository, GraphDatabase


async def _execute_query_with_retry(query: LiteralString,
                                    session: AsyncSession,
                                    parameters: dict = None,
                                    backoff_retries: int = 3,
                                    ):
    """
    Execute a Cypher query with retry logic.

    This is in case there are deadlocks or transient errors in the database, which can
    be safely retried.
    :param query: The Cypher query to execute.
    :param parameters: The parameters for the query.
    :param backoff_retries: The number of retries to attempt on failure.
    :return: The result of the query.
    """
    attempt = 0
    backoff_time = 1
    reason = 'success'

    while attempt < backoff_retries:
        try:
            result = await session.run(
                query,
                **(parameters or {}),
            )
            return result
        except TransientError as e:
            reason = 'transient_error_retry'
            if attempt < backoff_retries - 1:
                attempt += 1
                await asyncio.sleep(backoff_time)
                backoff_time *= 2
            else:
                reason = 'transient_error_exceeded_retries'
                raise e
        except Exception:
            reason = 'error_no_retry'
            raise
        finally:
            GRAPHDB_OP_RETRYS.labels(
                backend='neo4j',
                op='query_execution',
                reason=reason,
            )

    raise RuntimeError('Exceeded maximum retry attempts for query execution.')


class Neo4jReactomeRepository(ReactomeRepository):
    """
    An implementation of the ReactomeRepository interface for Neo4j.
    """

    def __init__(self,
                 client: AsyncDriver,
                 ):
        """
        Initialise the Neo4jReactomeRepository with an AsyncDriver instance.
        :param client: AsyncDriver instance.
        """
        self._client = client

    async def get_sub_pathways(self,
                               pathway_ids: list[str],
                               ) -> list[RelatedTerm]:
        """
        Get the sub-pathways of given pathways.
        :param pathway_ids: The IDs of the pathways to get sub-pathways for.
        :return: A list of RelatedTerm instances representing the sub-pathways.
        """
        async with self._client.session() as session:
            result = await _execute_query_with_retry(
                query="""
                UNWIND $pathway_ids AS pid
                MATCH (p:Concept:pathway {id: pid})
                OPTIONAL MATCH (p)<-[:part_of]-(sub:Concept:pathway)
                RETURN pid AS pathway_id, collect(DISTINCT sub.id) AS sub_pathways
                """,
                session=session,
                parameters={'pathway_ids': pathway_ids},
            )

            related_terms = []

            async for record in result:
                related_terms.append(RelatedTerm(
                    conceptId=record['pathway_id'],
                    relatedConcepts=record['sub_pathways'],
                ))

            return related_terms

    async def get_super_pathways(self,
                                 pathway_ids: list[str],
                                 ) -> list[RelatedTerm]:
        """
        Get the super-pathways of given pathways.
        :param pathway_ids: The IDs of the pathways to get super-pathways for.
        :return: A list of RelatedTerm instances representing the super-pathways.
        """
        async with self._client.session() as session:
            result = await _execute_query_with_retry(
                query="""
                UNWIND $pathway_ids AS pid
                MATCH (p:Concept:pathway {id: pid})
                OPTIONAL MATCH (p)-[:part_of]->(super:Concept:pathway)
                RETURN pid AS pathway_id, collect(DISTINCT super.id) AS super_pathways
                """,
                session=session,
                parameters={'pathway_ids': pathway_ids},
            )

            related_terms = []

            async for record in result:
                related_terms.append(RelatedTerm(
                    conceptId=record['pathway_id'],
                    relatedConcepts=record['super_pathways'],
                ))

            return related_terms

    async def get_reactions_in_pathway(self,
                                       pathway_ids: list[str],
                                       ) -> list[RelatedTerm]:
        """
        Get the reactions within given pathways.
        :param pathway_ids: The IDs of the pathways to get reactions for.
        :return: A list of RelatedTerm instances representing the reactions.
        """
        async with self._client.session() as session:
            result = await _execute_query_with_retry(
                query="""
                UNWIND $pathway_ids AS pid
                MATCH (p:Concept:pathway {id: pid})
                OPTIONAL MATCH (p)<-[:part_of]-(r:Concept:reaction)
                RETURN pid AS pathway_id, collect(DISTINCT r.id) AS reactions
                """,
                session=session,
                parameters={'pathway_ids': pathway_ids},
            )

            related_terms = []

            async for record in result:
                related_terms.append(RelatedTerm(
                    conceptId=record['pathway_id'],
                    relatedConcepts=record['reactions'],
                ))

            return related_terms

    async def get_pathways_of_reaction(self,
                                       reaction_ids: list[str],
                                       ) -> list[RelatedTerm]:
        """
        Get the pathways that given reactions belongs to.
        :param reaction_ids: The IDs of the reactions to get pathways for.
        :return: A list of RelatedTerm instances representing the pathways.
        """
        async with self._client.session() as session:
            result = await _execute_query_with_retry(
                query="""
                UNWIND $reaction_ids AS rid
                MATCH (r:Concept:reaction {id: rid})
                OPTIONAL MATCH (r)-[:part_of]->(p:Concept:pathway)
                RETURN rid AS reaction_id, collect(DISTINCT p.id) AS pathways
                """,
                session=session,
                parameters={'reaction_ids': reaction_ids},
            )

            related_terms = []

            async for record in result:
                related_terms.append(RelatedTerm(
                    conceptId=record['reaction_id'],
                    relatedConcepts=record['pathways'],
                ))

            return related_terms

    async def get_preceding_reactions(self,
                                      reaction_ids: list[str],
                                      ) -> list[RelatedTerm]:
        """
        Get the preceding reactions of given reactions.
        :param reaction_ids: The IDs of the reactions to get preceding reactions for.
        :return: A list of RelatedTerm instances representing the preceding reactions.
        """
        async with self._client.session() as session:
            result = await _execute_query_with_retry(
                query="""
                UNWIND $reaction_ids AS rid
                MATCH (r:Concept:reaction {id: rid})
                OPTIONAL MATCH (r)-[:preceded_by]->(pre:Concept:reaction)
                RETURN rid AS reaction_id, collect(DISTINCT pre.id) AS preceding_reactions
                """,
                session=session,
                parameters={'reaction_ids': reaction_ids},
            )

            related_terms = []

            async for record in result:
                related_terms.append(RelatedTerm(
                    conceptId=record['reaction_id'],
                    relatedConcepts=record['preceding_reactions'],
                ))

            return related_terms

    async def get_subsequent_reactions(self,
                                       reaction_ids: list[str],
                                       ) -> list[RelatedTerm]:
        """
        Get the subsequent reactions of given reactions.
        :param reaction_ids: The IDs of the reactions to get subsequent reactions for.
        :return: A list of RelatedTerm instances representing the subsequent reactions.
        """
        async with self._client.session() as session:
            result = await _execute_query_with_retry(
                query="""
                UNWIND $reaction_ids AS rid
                MATCH (r:Concept:reaction {id: rid})
                OPTIONAL MATCH (r)<-[:preceded_by]-(sub:Concept:reaction)
                RETURN rid AS reaction_id, collect(DISTINCT sub.id) AS subsequent_reactions
                """,
                session=session,
                parameters={'reaction_ids': reaction_ids},
            )

            related_terms = []

            async for record in result:
                related_terms.append(RelatedTerm(
                    conceptId=record['reaction_id'],
                    relatedConcepts=record['subsequent_reactions'],
                ))

            return related_terms

    async def get_reaction_inputs(self,
                                  reaction_ids: list[str],
                                  ) -> list[RelatedTerm]:
        """
        Get the inputs of given reactions.
        :param reaction_ids: The IDs of the reactions to get inputs for.
        :return: A list of RelatedTerm instances representing the inputs.
        """
        async with self._client.session() as session:
            result = await _execute_query_with_retry(
                query="""
                UNWIND $reaction_ids AS rid
                MATCH (r:Concept:reaction {id: rid})
                OPTIONAL MATCH (r)-[:has_input]->(input:Concept:gene)
                RETURN rid AS reaction_id, collect(DISTINCT input.id) AS inputs
                """,
                session=session,
                parameters={'reaction_ids': reaction_ids},
            )

            related_terms = []

            async for record in result:
                related_terms.append(RelatedTerm(
                    conceptId=record['reaction_id'],
                    relatedConcepts=record['inputs'],
                ))

            return related_terms

    async def get_reaction_outputs(self,
                                   reaction_ids: list[str],
                                   ) -> list[RelatedTerm]:
        """
        Get the outputs of given reactions.
        :param reaction_ids: The IDs of the reactions to get outputs for.
        :return: A list of RelatedTerm instances representing the outputs.
        """
        async with self._client.session() as session:
            result = await _execute_query_with_retry(
                query="""
                UNWIND $reaction_ids AS rid
                MATCH (r:Concept:reaction {id: rid})
                OPTIONAL MATCH (r)-[:has_output]->(output:Concept:gene)
                RETURN rid AS reaction_id, collect(DISTINCT output.id) AS outputs
                """,
                session=session,
                parameters={'reaction_ids': reaction_ids},
            )

            related_terms = []

            async for record in result:
                related_terms.append(RelatedTerm(
                    conceptId=record['reaction_id'],
                    relatedConcepts=record['outputs'],
                ))

            return related_terms

    async def get_gene_input_reactions(self,
                                       gene_ids: list[str],
                                       ) -> list[RelatedTerm]:
        """
        Get the reactions where the product that given genes encode are inputs.
        :param gene_ids: The IDs of the genes to get input reactions for.
        :return: A list of RelatedTerm instances representing the reactions.
        """
        async with self._client.session() as session:
            result = await _execute_query_with_retry(
                query="""
                UNWIND $gene_ids AS gid
                MATCH (g:Concept:gene {id: gid})
                OPTIONAL MATCH (r:Concept:reaction)-[:has_input]->(g)
                RETURN gid AS gene_id, collect(DISTINCT r.id) AS input_reactions
                """,
                session=session,
                parameters={'gene_ids': gene_ids},
            )

            related_terms = []

            async for record in result:
                related_terms.append(RelatedTerm(
                    conceptId=record['gene_id'],
                    relatedConcepts=record['input_reactions'],
                ))

            return related_terms

    async def get_gene_output_reactions(self,
                                        gene_ids: list[str],
                                        ) -> list[RelatedTerm]:
        """
        Get the reactions where the product that given genes encode are output.
        :param gene_ids: The IDs of the genes to get output reactions for.
        :return: A list of RelatedTerm instances representing the reactions.
        """
        async with self._client.session() as session:
            result = await _execute_query_with_retry(
                query="""
                UNWIND $gene_ids AS gid
                MATCH (g:Concept:gene {id: gid})
                OPTIONAL MATCH (r:Concept:reaction)-[:has_output]->(g)
                RETURN gid AS gene_id, collect(DISTINCT r.id) AS output_reactions
                """,
                session=session,
                parameters={'gene_ids': gene_ids},
            )

            related_terms = []

            async for record in result:
                related_terms.append(RelatedTerm(
                    conceptId=record['gene_id'],
                    relatedConcepts=record['output_reactions'],
                ))

            return related_terms


class Neo4jGraphDatabase(GraphDatabase):
    """
    An implementation of the GraphDatabase interface for Neo4j.
    """

    _client: AsyncDriver = None

    def __init__(self,
                 client: AsyncDriver = None,
                 ):
        """
        Initialise the Neo4jGraphDatabase with an AsyncDriver instance.
        :param client: AsyncDriver instance.
        """
        if client is not None:
            self._client = client

    @classmethod
    def set_client(cls,
                   client: AsyncDriver,
                   ):
        """
        Set the Neo4J client for the database.
        :param client: AsyncDriver instance.
        """
        cls._client = client

    async def close(self) -> None:
        """
        Close the Neo4J connection.
        """
        if self._client is not None:
            await self._client.close()
        else:
            raise ValueError('Neo4J client is not set. Cannot close connection.')

    async def _save_vocabulary_graph(self,
                                     prefix: ConceptPrefix,
                                     concepts: Iterable[Concept],
                                     edges: Iterable[tuple[str, str, Optional[str], Optional[str]]],
                                     consume_concepts: bool,
                                     ) -> None:
        async with self._client.session() as session:
            # Insert the concepts first before adding edges
            verbose_print('Inserting concepts into Neo4j...')
            for concept_batch in batch_iterable(concepts, consume=consume_concepts):
                await _execute_query_with_retry(
                    query="""
                    UNWIND $concepts AS concept
                    WITH concept, coalesce(concept.conceptTypes, []) AS types
                    MERGE (n:Concept {id: concept.conceptId, prefix: concept.prefix})

                    WITH n, concept, [t IN types WHERE t IS NOT NULL AND trim(t) <> ""] AS labels
                    SET n:$(labels)
                    WITH n, concept,
                        [k IN $extraProperties WHERE concept[k] IS NOT NULL] AS presentKeys
                    FOREACH (k IN presentKeys | SET n[k] = concept[k])
                    RETURN count(n) AS upserted
                    """,
                    session=session,
                    parameters={
                        'concepts': [concept.model_dump() for concept in concept_batch],
                        'extraProperties': GRAPH_NODE_EXTRA_PROPERTIES,
                    },
                )

            # Insert the edges
            verbose_print(f'Inserting edges into Neo4j...')
            for edge_batch in batch_iterable(edges):
                await _execute_query_with_retry(
                    query="""
                    UNWIND $edges AS edge
                    MERGE (source:Concept {id: edge[0], prefix: $concept_prefix})
                    MERGE (target:Concept {id: edge[1], prefix: $concept_prefix})
                    WITH source, target, edge,
                        coalesce(edge[2], 'related_to') as rel_label,
                        edge[3] AS rel_key
                    MERGE (source)-[rel:$(rel_label)]->(target)
                    WITH rel, rel_key
                    FOREACH (_ IN CASE WHEN rel_key IS NULL THEN [] ELSE [1] END |
                        SET rel.label = reduce(
                            unique_labels = [],
                            item IN (
                                CASE
                                    WHEN rel.label IS NULL THEN []
                                    WHEN rel.label IS TYPED LIST<ANY> THEN rel.label
                                    ELSE [rel.label]
                                END + [rel_key]
                            ) |
                            CASE
                                WHEN item IN unique_labels THEN unique_labels
                                ELSE unique_labels + item
                            END
                        )
                    )
                    RETURN count(rel) AS created
                    """,
                    session=session,
                    parameters={
                        'edges': edge_batch,
                        'concept_prefix': prefix.value,
                    },
                )

    async def get_vocabulary_graph(self,
                                   prefix: ConceptPrefix,
                                   with_similarity: bool = False,
                                   ) -> nx.MultiDiGraph:
        """
        Retrieve the vocabulary graph from the graph database.
        :param prefix: The node prefix of the vocabulary to retrieve.
        :param with_similarity: Whether to include similarity relationships in the graph.
        :return: The vocabulary graph.
        """
        vocabulary_graph = nx.MultiDiGraph()

        async with self._client.session() as session:
            # Retrieve all nodes first from neo4j
            node_count_result = await _execute_query_with_retry(
                query="""
                MATCH (n:Concept {prefix: $prefix})
                RETURN count(n) AS node_count
                """,
                session=session,
                parameters={'prefix': prefix.value},
            )
            node_count = (await node_count_result.single())['node_count']

            nodes_result = await _execute_query_with_retry(
                query="""
                MATCH (n:Concept {prefix: $prefix})
                RETURN n.id AS concept_id
                """,
                session=session,
                parameters={'prefix': prefix.value},
            )

            async for record in aiter_progress(
                nodes_result,
                description='Retrieving nodes',
                total=node_count,
            ):
                concept_id = record['concept_id']
                vocabulary_graph.add_node(concept_id)

            # Retrieve all edges that connects WITHIN the vocabulary
            if with_similarity:
                edge_count_result = await _execute_query_with_retry(
                    query="""
                    MATCH (source:Concept {prefix: $prefix})-[r]->(target:Concept {prefix: $prefix})
                    RETURN count(DISTINCT(r)) AS edge_count
                    """,
                    session=session,
                    parameters={'prefix': prefix.value},
                )
                edge_count = (await edge_count_result.single())['edge_count']

                edges_result = await _execute_query_with_retry(
                    query="""
                    MATCH (source:Concept {prefix: $prefix})-[r]->(target:Concept {prefix: $prefix})
                    RETURN DISTINCT source.id AS source_id, target.id AS target_id, type(r) AS rel_label
                    """,
                    session=session,
                    parameters={'prefix': prefix.value},
                )

                async for record in aiter_progress(
                    edges_result,
                    description='Retrieving edges',
                    total=edge_count,
                ):
                    vocabulary_graph.add_edge(
                        record['source_id'],
                        record['target_id'],
                        label=ConceptRelationshipType(record['rel_label'])
                    )
            else:
                edge_count_result = await _execute_query_with_retry(
                    query="""
                    MATCH (source:Concept {prefix: $prefix})-[r]->(target:Concept {prefix: $prefix})
                    WHERE type(r) <> 'similar_to'
                    RETURN count(DISTINCT(r)) AS edge_count
                    """,
                    session=session,
                    parameters={'prefix': prefix.value},
                )
                edge_count = (await edge_count_result.single())['edge_count']

                edges_result = await _execute_query_with_retry(
                    query="""
                    MATCH (source:Concept {prefix: $prefix})-[r]->(target:Concept {prefix: $prefix})
                    WHERE type(r) <> 'similar_to'
                    RETURN DISTINCT source.id AS source_id, target.id AS target_id, type(r) AS rel_label
                    """,
                    session=session,
                    parameters={'prefix': prefix.value},
                )

                async for record in aiter_progress(
                    edges_result,
                    description='Retrieving edges',
                    total=edge_count,
                ):
                    vocabulary_graph.add_edge(
                        record['source_id'],
                        record['target_id'],
                        label=ConceptRelationshipType(record['rel_label'])
                    )

        return vocabulary_graph

    async def delete_vocabulary_graph(self,
                                      prefix: ConceptPrefix,
                                      ):
        """
        Delete the vocabulary graph from the graph database.
        :param prefix: The node prefix of the vocabulary to delete.
        """
        batch_size = CONFIG.neo4j_delete_batch_size

        # Neither a single CALL {} IN TRANSACTIONS OF N ROWS nor apoc.periodic.commit
        # kept this bounded on constrained instances -- both still hold the *entire*
        # driving MATCH's state open (directly or via APOC's own iteration) across the
        # whole delete. Looping client-side instead, re-issuing a small bounded query
        # as its own fresh auto-commit transaction every round trip, means no state at
        # all is carried between iterations -- peak transaction memory is bounded by
        # one batch of nodes (and their relationships) no matter how large the
        # vocabulary is. DETACH DELETE removes a node's relationships together with it,
        # so this replaces the old two-pass (relationships, then nodes) query, and with
        # it the need to special-case internal (same-prefix-on-both-ends) relationships
        # being matched twice by an undirected pattern.
        async with self._client.session() as session:
            while True:
                result = await _execute_query_with_retry(
                    query="""
                    MATCH (n:Concept {prefix: $prefix})
                    WITH n LIMIT $batch_size
                    DETACH DELETE n
                    RETURN count(n) AS deleted
                    """,
                    session=session,
                    parameters={'prefix': prefix.value, 'batch_size': batch_size},
                )
                if (await result.single())['deleted'] == 0:
                    break

    async def count_terms(self,
                          prefix: ConceptPrefix,
                          ) -> int:
        """
        Count the number of nodes for a given prefix in the graph database.
        :param prefix: The vocabulary prefix to count nodes for.
        :return: The number of nodes with the given prefix.
        """
        async with self._client.session() as session:
            result = await _execute_query_with_retry(
                query="""
                MATCH (n:Concept {prefix: $prefix})
                RETURN count(n) AS term_count
                """,
                session=session,
                parameters={'prefix': prefix.value},
            )

            record = await result.single()
            return record['term_count'] if record is not None else 0

    async def count_internal_relationships(self,
                                           prefix: ConceptPrefix,
                                           ) -> int:
        """
        Count the number of internal relationships within a vocabulary in the graph database.
        :param prefix: The vocabulary prefix to count relationships for
        :return: The number of internal relationships within the vocabulary.
        """
        async with self._client.session() as session:
            result = await _execute_query_with_retry(
                query="""
                MATCH (source:Concept {prefix: $prefix})-[r]->(target:Concept {prefix: $prefix})
                WHERE type(r) <> 'similar_to'
                RETURN count(r) AS relationship_count
                """,
                session=session,
                parameters={'prefix': prefix.value},
            )

            record = await result.single()
            return record['relationship_count'] if record is not None else 0

    async def get_relationship_edges(self,
                                     prefix: ConceptPrefix,
                                     relationship_type: ConceptRelationshipType,
                                     ) -> AsyncIterator[tuple[str, str]]:
        """
        Stream (source_id, target_id) pairs for one specific same-vocabulary relationship
        type, filtered server-side rather than fetching every edge and discarding most of
        them client-side.
        :param prefix: The vocabulary prefix to fetch edges for.
        :param relationship_type: The single relationship type to filter to.
        :return: An async iterator of (source_id, target_id) tuples.
        """
        async with self._client.session() as session:
            result = await _execute_query_with_retry(
                query="""
                MATCH (source:Concept {prefix: $prefix})-[r]->(target:Concept {prefix: $prefix})
                WHERE type(r) = $rel_type
                RETURN source.id AS source_id, target.id AS target_id
                """,
                session=session,
                parameters={'prefix': prefix.value, 'rel_type': relationship_type.value},
            )

            async for record in result:
                yield record['source_id'], record['target_id']

    async def count_similarity_relationships(self,
                                             prefix_from: ConceptPrefix,
                                             prefix_to: ConceptPrefix,
                                             configurations: list[tuple[SimilarityMethod, ConceptPrefix | None]],
                                             ) -> list[tuple[SimilarityMethod, ConceptPrefix | None, int]]:
        """
        Count the number of similarity relationships between two vocabularies in the graph database,
        for each similarity method and corpus configuration.
        :param prefix_from: The source vocabulary prefix.
        :param prefix_to: The target vocabulary prefix.
        :param configurations: A list of tuples containing similarity methods and corpus prefixes
            (or None for intrinsic similarity).
        :return: A list of tuples containing the similarity method, corpus prefix,
            and the number of similarity relationships.
        """
        configuration_mapping = {}
        for method, corpus_prefix in configurations:
            attribute_name = f'{method.value}:{corpus_prefix.value}' if corpus_prefix else method.value
            configuration_mapping[attribute_name] = (method, corpus_prefix)

        counts = []

        async with self._client.session() as session:
            result = await _execute_query_with_retry(
                query="""
                UNWIND $attributes AS attr
                WITH DISTINCT attr
                OPTIONAL MATCH (source:Concept {prefix: $prefix_from})
                    -[r:similar_to]->
                    (target:Concept {prefix: $prefix_to})
                WHERE attr IN keys(r)
                RETURN attr AS attribute, count(r) AS relationship_count
                ORDER BY attribute;
                """,
                session=session,
                parameters={
                    'prefix_from': prefix_from.value,
                    'prefix_to': prefix_to.value,
                    'attributes': list(configuration_mapping.keys()),
                }
            )

            async for record in result:
                attribute = record['attribute']
                relationship_count = record['relationship_count']
                method, corpus_prefix = configuration_mapping[attribute]
                counts.append((method, corpus_prefix, relationship_count))

        return counts

    async def save_annotations(self,
                               annotations: list[Annotation],
                               ):
        """
        Save a list of annotations into the graph database.
        :param annotations: A list of Annotation instances to save.
        """
        async with self._client.session() as session:
            verbose_print(f'Inserting {len(annotations)} annotations into Neo4j...')
            for annotation_batch in batch_iterable(annotations):
                serialized = [annotation.model_dump() for annotation in annotation_batch]
                sourced = [a for a in serialized if (a.get('properties') or {}).get('source')]
                unsourced = [a for a in serialized if not (a.get('properties') or {}).get('source')]
                if sourced:
                    await _execute_query_with_retry(
                        query="""
                        UNWIND $annotations AS annotation
                        MERGE (source:Concept {id: annotation.conceptIdFrom, prefix: annotation.prefixFrom})
                        MERGE (target:Concept {id: annotation.conceptIdTo, prefix: annotation.prefixTo})
                        WITH source,
                            target,
                            coalesce(annotation.annotationType, 'annotated_with') AS rel_type,
                            annotation.properties AS props
                        MERGE (source)-[rel:$(rel_type) {source: props.source}]->(target)
                        SET rel += props
                        RETURN count(rel) AS created
                        """,
                        session=session,
                        parameters={'annotations': sourced},
                    )
                if unsourced:
                    await _execute_query_with_retry(
                        query="""
                        UNWIND $annotations AS annotation
                        MERGE (source:Concept {id: annotation.conceptIdFrom, prefix: annotation.prefixFrom})
                        MERGE (target:Concept {id: annotation.conceptIdTo, prefix: annotation.prefixTo})
                        WITH source,
                            target,
                            coalesce(annotation.annotationType, 'annotated_with') AS rel_type,
                            coalesce(annotation.properties, {}) AS props
                        MERGE (source)-[rel:$(rel_type)]->(target)
                        SET rel += props
                        RETURN count(rel) AS created
                        """,
                        session=session,
                        parameters={'annotations': unsourced},
                    )

    async def get_annotation_graph(self,
                                   prefix_1: ConceptPrefix,
                                   prefix_2: ConceptPrefix,
                                   ) -> nx.DiGraph:
        """
        Retrieve the annotation graph between two vocabularies from the graph database.
        :param prefix_1: The first vocabulary prefix.
        :param prefix_2: The second vocabulary prefix.
        :return: The annotation graph between the two vocabularies.
        """
        annotation_graph = nx.DiGraph()

        async with self._client.session() as session:
            annotation_count_result = await _execute_query_with_retry(
                query="""
                MATCH (source:Concept {prefix: $prefix_1})-[r]-(target:Concept {prefix: $prefix_2})
                RETURN count(DISTINCT(r)) AS annotation_count
                """,
                session=session,
                parameters={
                    'prefix_1': prefix_1.value,
                    'prefix_2': prefix_2.value,
                },
            )
            annotation_count = (await annotation_count_result.single())['annotation_count']

            result = await _execute_query_with_retry(
                query="""
                MATCH (source:Concept {prefix: $prefix_1})-[r]-(target:Concept {prefix: $prefix_2})
                RETURN DISTINCT startNode(r).prefix AS source_prefix,
                    startNode(r).id AS source_id,
                    endNode(r).prefix AS target_prefix,
                    endNode(r).id AS target_id,
                    type(r) AS rel_label,
                    properties(r) AS rel_props
                """,
                session=session,
                parameters={
                    'prefix_1': prefix_1.value,
                    'prefix_2': prefix_2.value,
                },
            )

            async for record in aiter_progress(
                result,
                description='Retrieving annotations',
                total=annotation_count,
            ):
                annotation_graph.add_edge(
                    f'{record["source_prefix"]}:{record["source_id"]}',
                    f'{record["target_prefix"]}:{record["target_id"]}',
                    label=AnnotationType(record['rel_label']),
                    **record['rel_props']
                )

        return annotation_graph

    async def get_annotation_edges(self,
                                   prefix_1: ConceptPrefix,
                                   prefix_2: ConceptPrefix,
                                   annotation_type: AnnotationType | None = None,
                                   ) -> AsyncIterator[tuple[str, str, str, str, AnnotationType]]:
        """Stream cross-vocabulary annotations without materialising a NetworkX graph."""
        async with self._client.session() as session:
            result = await _execute_query_with_retry(
                query="""
                MATCH (source:Concept {prefix: $prefix_1})-[r]-(target:Concept {prefix: $prefix_2})
                WHERE $rel_type IS NULL OR type(r) = $rel_type
                RETURN DISTINCT source.id AS source_id, target.id AS target_id,
                    type(r) AS rel_label
                """,
                session=session,
                parameters={
                    'prefix_1': prefix_1.value,
                    'prefix_2': prefix_2.value,
                    'rel_type': annotation_type.value if annotation_type is not None else None,
                },
            )
            async for record in result:
                yield (
                    prefix_1.value, record['source_id'], prefix_2.value, record['target_id'],
                    AnnotationType(record['rel_label']),
                )

    async def delete_annotations(self,
                                 prefix_1: ConceptPrefix,
                                 prefix_2: ConceptPrefix,
                                 ):
        """
        Delete annotations between two vocabularies from the graph database.
        :param prefix_1: The first vocabulary prefix.
        :param prefix_2: The second vocabulary prefix.
        """
        batch_size = CONFIG.neo4j_delete_batch_size

        # See delete_vocabulary_graph: looping a small bounded query client-side, each
        # iteration its own fresh auto-commit transaction, rather than a single
        # CALL {} IN TRANSACTIONS, keeps peak transaction memory bounded to one batch.
        async with self._client.session() as session:
            while True:
                result = await _execute_query_with_retry(
                    query="""
                    MATCH (:Concept {prefix: $prefix_1})-[r]-(:Concept {prefix: $prefix_2})
                    WITH r LIMIT $batch_size
                    DELETE r
                    RETURN count(r) AS deleted
                    """,
                    session=session,
                    parameters={
                        'prefix_1': prefix_1.value,
                        'prefix_2': prefix_2.value,
                        'batch_size': batch_size,
                    },
                )
                if (await result.single())['deleted'] == 0:
                    break

    async def count_annotations(self,
                                prefix_1: ConceptPrefix,
                                prefix_2: ConceptPrefix,
                                ) -> int:
        """
        Count the number of annotations between two vocabularies in the graph database.
        :param prefix_1: The first vocabulary prefix.
        :param prefix_2: The second vocabulary prefix.
        :return: The number of annotations between the two vocabularies.
        """
        async with self._client.session() as session:
            result = await _execute_query_with_retry(
                query="""
                MATCH (source:Concept {prefix: $prefix_1})-[r]-(target:Concept {prefix: $prefix_2})
                RETURN count(r) AS annotation_count
                """,
                session=session,
                parameters={
                    'prefix_1': prefix_1.value,
                    'prefix_2': prefix_2.value,
                },
            )

            record = await result.single()
            return record['annotation_count'] if record is not None else 0

    async def save_similarity_scores(self,
                                     prefix_from: ConceptPrefix,
                                     prefix_to: ConceptPrefix,
                                     similarity_scores: list[tuple[str, str, float]],
                                     similarity_method: SimilarityMethod,
                                     corpus_prefix: ConceptPrefix | None = None,
                                     ):
        """
        Save similarity scores between two vocabularies into the graph database.
        :param prefix_from: The source vocabulary prefix. Correspond to 'concept_from' in similarity_df.
        :param prefix_to: The target vocabulary prefix. Correspond to 'concept_to' in similarity_df.
        :param similarity_scores: A list of tuple containing similarity scores. In the format of:
            | concept_from | concept_to | similarity |
        :param similarity_method: The similarity method used to generate the scores. Stored as
            property name on the relationship.
        :param corpus_prefix: The corpus vocabulary prefix, if applicable.
        """
        async with self._client.session() as session:
            for scores in batch_iterable(similarity_scores):
                similarities = [
                    {
                        'concept_from': score[0],
                        'concept_to': score[1],
                        'similarity': score[2],
                    }
                    for score in scores
                ]

                await _execute_query_with_retry(
                    query="""
                    UNWIND $similarities AS sim
                    MATCH (source:Concept {id: sim.concept_from, prefix: $prefix_from})
                    MATCH (target:Concept {id: sim.concept_to, prefix: $prefix_to})
                    WITH source, target, sim.similarity AS sim_score, $similarity_property AS similarity_property
                    MERGE (source)-[rel:similar_to]->(target)
                    SET rel[similarity_property] = sim_score
                    RETURN count(rel) AS created
                    """,
                    session=session,
                    parameters={
                        'similarities': similarities,
                        'prefix_from': prefix_from.value,
                        'prefix_to': prefix_to.value,
                        'similarity_property': (
                            f'{similarity_method.value}:{corpus_prefix.value}'
                            if corpus_prefix else similarity_method.value
                        ),
                    },
                )

    async def create_index(self):
        """
        Create indexes in Neo4J
        """
        async with self._client.session() as session:
            await _execute_query_with_retry(
                query="""
                      CREATE INDEX concept_prefix_index IF NOT EXISTS
                          FOR (n:Concept)
                          ON (n.prefix)
                      """,
                session=session,
            )
            await _execute_query_with_retry(
                query="""
                      CREATE INDEX concept_id_index IF NOT EXISTS
                          FOR (n:Concept)
                          ON (n.id)
                      """,
                session=session,
            )
            await _execute_query_with_retry(
                query="""
                CREATE CONSTRAINT concept_prefix_id_unique IF NOT EXISTS
                FOR (n:Concept)
                REQUIRE (n.prefix, n.id) IS UNIQUE
                """,
                session=session,
            )
            # GRAPH_NODE_EXTRA_PROPERTIES fields exist specifically to be filtered on (e.g.
            # scoping UniProt's full, multi-organism release down to organismTaxId='9606')
            # -- at UniProt's scale (250M+ nodes) an unindexed equality filter on these is a
            # full node scan, so each one gets its own index alongside prefix/id above.
            for property_name in GRAPH_NODE_EXTRA_PROPERTIES:
                await _execute_query_with_retry(
                    query=f"""
                          CREATE INDEX concept_{property_name}_index IF NOT EXISTS
                              FOR (n:Concept)
                              ON (n.{property_name})
                          """,
                    session=session,
                )

    async def trace_ancestors_iter(self,
                                   prefix: ConceptPrefix,
                                   concept_ids: list[str],
                                   max_depth: int | None = None,
                                   limit: int | None = None,
                                   ) -> AsyncIterator[RelatedTerm]:
        """
        Trace the given terms to retrieve their ancestors up to the specified depth, and return
        an asynchronous iterator over the results.

        This would only work on ontologies, because it relies on the IS_A relationships.
        Expanding a non-ontology or an ontology that does not have hierarchical relationships
        would return an empty set for each term.
        :param prefix: The prefix of the concepts to trace.
        :param concept_ids: The list of concept IDs to trace.
        :param max_depth: The maximum depth to trace. If None, trace to all depths.
        :param limit: The maximum number of ancestors to return for each term. If None, return all.
        :return: An asynchronous iterator yielding ExpandedTerm instances.
        """
        async with self._client.session() as session:
            if max_depth is None:
                result = await _execute_query_with_retry(
                    query="""
                    MATCH (n:Concept {prefix: $prefix})
                    WHERE n.id IN $concept_ids
                    OPTIONAL MATCH (n)-[:is_a|part_of*]->(ancestor:Concept)
                    WITH n, [a IN collect(DISTINCT ancestor.id) WHERE a IS NOT NULL] AS all_anc
                    WITH n,
                        CASE
                            WHEN $limit IS NULL THEN all_anc
                            ELSE all_anc[0..$limit]
                        END AS ancestors
                    RETURN n.id AS concept_id, ancestors
                    """,
                    session=session,
                    parameters={
                        'prefix': prefix.value,
                        'concept_ids': concept_ids,
                        'limit': limit,
                    },
                )
            else:
                # Variable-length relationship bounds must be literal at parse time in native
                # Cypher (unlike APOC's expandConfig, which accepted maxLevel as a runtime map
                # value), so max_depth is interpolated rather than passed as a parameter.
                result = await _execute_query_with_retry(
                    query=f"""
                    MATCH (n:Concept {{prefix: $prefix}})
                    WHERE n.id IN $concept_ids
                    OPTIONAL MATCH (n)-[:is_a|part_of*1..{int(max_depth)}]->(ancestor:Concept)
                    WITH n, [a IN collect(DISTINCT ancestor.id) WHERE a IS NOT NULL] AS all_anc
                    WITH n,
                        CASE
                            WHEN $limit IS NULL THEN all_anc
                            ELSE all_anc[0..$limit]
                        END AS ancestors
                    RETURN n.id AS concept_id, ancestors
                    """,
                    session=session,
                    parameters={
                        'prefix': prefix.value,
                        'concept_ids': concept_ids,
                        'limit': limit,
                    },
                )

            async for record in result:
                yield RelatedTerm(
                    conceptId=record['concept_id'],
                    relatedConcepts=list(set(record['ancestors'])),
                )

    async def expand_terms_iter(self,
                                prefix: ConceptPrefix,
                                concept_ids: list[str],
                                max_depth: int | None = None,
                                limit: int | None = None,
                                ) -> AsyncIterator[RelatedTerm]:
        """
        Expand the given terms to retrieve their descendants up to the specified depth, and return
        an asynchronous iterator over the results.

        This would only work on ontologies, because it relies on the IS_A relationships.
        Expanding a non-ontology or an ontology that does not have hierarchical relationships
        would return an empty set for each term.
        :param prefix: The prefix of the concepts to expand.
        :param concept_ids: The list of concept IDs to expand.
        :param max_depth: The maximum depth to expand. If None, expand to all depths.
        :param limit: The maximum number of descendants to return for each term. If None, return all.
        :return: An asynchronous iterator yielding ExpandedTerm instances.
        """
        mode = 'unbounded' if max_depth is None else 'bounded'
        start = time.perf_counter()
        first = None
        result_label = 'ok'

        try:
            async with self._client.session() as session:
                if max_depth is None:
                    result = await _execute_query_with_retry(
                        query="""
                        MATCH (n:Concept {prefix: $prefix})
                        WHERE n.id IN $concept_ids
                        OPTIONAL MATCH (n)<-[:is_a|part_of*]-(descendant:Concept)
                        WITH n, [d IN collect(DISTINCT descendant.id) WHERE d IS NOT NULL] AS all_desc
                        WITH n,
                            CASE
                                WHEN $limit IS NULL THEN all_desc
                                ELSE all_desc[0..$limit]
                            END AS descendants
                        RETURN n.id AS concept_id, descendants
                        """,
                        session=session,
                        parameters={
                            'prefix': prefix.value,
                            'concept_ids': concept_ids,
                            'limit': limit,
                        },
                    )
                else:
                    # See trace_ancestors_iter: variable-length bounds must be literal, so
                    # max_depth is interpolated rather than passed as a query parameter.
                    result = await _execute_query_with_retry(
                        query=f"""
                        MATCH (n:Concept {{prefix: $prefix}})
                        WHERE n.id IN $concept_ids
                        OPTIONAL MATCH (n)<-[:is_a|part_of*1..{int(max_depth)}]-(descendant:Concept)
                        WITH n, [d IN collect(DISTINCT descendant.id) WHERE d IS NOT NULL] AS all_desc
                        WITH n,
                            CASE
                                WHEN $limit IS NULL THEN all_desc
                                ELSE all_desc[0..$limit]
                            END AS descendants
                        RETURN n.id AS concept_id, descendants
                        """,
                        session=session,
                        parameters={
                            'prefix': prefix.value,
                            'concept_ids': concept_ids,
                            'limit': limit,
                        },
                    )

                async for record in result:
                    if first is None:
                        first = time.perf_counter()

                    descendants = record['descendants'] or []
                    unique_desc = list(set(descendants))

                    EXPAND_DESC_COUNT.labels(
                        prefix=prefix.value,
                        mode=mode,
                    ).observe(len(unique_desc))

                    yield RelatedTerm(
                        conceptId=record['concept_id'],
                        relatedConcepts=unique_desc,
                    )
        except asyncio.CancelledError:
            result_label = 'cancelled'
            raise
        except Exception as e:
            result_label = 'error'
            GRAPHDB_OP_ERRORS.labels(
                backend='neo4j',
                op='expand',
                prefix=prefix.value,
                error_type=type(e).__name__,
            ).inc()
            raise
        finally:
            end = time.perf_counter()
            GRAPHDB_OP_DURATION.labels(
                backend='neo4j',
                op='expand',
                prefix=prefix.value,
                mode=mode,
                result=result_label,
            ).observe(end - start)
            if first is not None:
                GRAPHDB_OP_TTFR.labels(
                    backend='neo4j',
                    op='expand',
                    prefix=prefix.value,
                    mode=mode,
                    result=result_label,
                ).observe(first - start)

    async def get_replaced_terms_iter(self,
                                      prefix: ConceptPrefix,
                                      concept_ids: list[str],
                                      ) -> AsyncIterator[RelatedTerm]:
        """
        Get the concepts replaced by the given concept IDs as an asynchronous iterator.
        :param prefix: The prefix of the concepts to find replacements for.
        :param concept_ids: The list of concept IDs to find replacements for.
        :return: An asynchronous iterator yielding RelatedTerms instances.
        """
        async with self._client.session() as session:
            result = await _execute_query_with_retry(
                query="""
                MATCH (n:Concept {prefix: $prefix})
                WHERE n.id IN $concept_ids
                OPTIONAL MATCH (n)<-[:replaced_by]-(m:Concept)
                WITH n, [r IN collect(DISTINCT m.id) WHERE r IS NOT NULL] AS replaced_terms
                RETURN n.id AS concept_id, replaced_terms
                """,
                session=session,
                parameters={
                    'prefix': prefix.value,
                    'concept_ids': concept_ids,
                }
            )

            async for record in result:
                yield RelatedTerm(
                    conceptId=record['concept_id'],
                    relatedConcepts=list(set(record['replaced_terms'])),
                )

    async def get_replacing_terms_iter(self,
                                       prefix: ConceptPrefix,
                                       concept_ids: list[str],
                                       ) -> AsyncIterator[RelatedTerm]:
        """
        Get the concepts that replace the given concept IDs as an asynchronous iterator.
        :param prefix: The prefix of the concepts to find replacing terms for.
        :param concept_ids: The list of concept IDs to find replacing terms for.
        :return: An asynchronous iterator yielding RelatedTerms instances.
        """
        async with self._client.session() as session:
            result = await _execute_query_with_retry(
                query="""
                MATCH (n:Concept {prefix: $prefix})
                WHERE n.id IN $concept_ids
                OPTIONAL MATCH (n)-[:replaced_by]->(m:Concept)
                WITH n, [r IN collect(DISTINCT m.id) WHERE r IS NOT NULL] AS replacing_terms
                RETURN n.id AS concept_id, replacing_terms
                """,
                session=session,
                parameters={
                    'prefix': prefix.value,
                    'concept_ids': concept_ids,
                }
            )

            async for record in result:
                yield RelatedTerm(
                    conceptId=record['concept_id'],
                    relatedConcepts=list(set(record['replacing_terms'])),
                )

    async def map_terms_iter(self,
                             prefix: ConceptPrefix,
                             target_prefix: ConceptPrefix,
                             concept_ids: list[str],
                             max_hops: int = 1,
                             limit: int | None = None,
                             ) -> AsyncIterator[RelatedTerm]:
        """
        Map terms from one vocabulary to another as an asynchronous iterator.
        :param prefix: The source prefix.
        :param target_prefix: The target prefix.
        :param concept_ids: The list of concept IDs to map.
        :param max_hops: The maximum number of mapping hops to consider.
        :param limit: The maximum number of mapped terms to return for each concept ID.
        :return: An asynchronous iterator yielding RelatedTerm instances.
        """
        start = time.perf_counter()
        first = None
        result_label = 'ok'

        try:
            async with self._client.session() as session:
                # Binding the target prefix directly on the pattern's end node (rather than
                # filtering after an unconstrained expansion, as apoc.path.expandConfig did)
                # lets the planner prune the traversal towards matching nodes instead of a
                # blind BFS over every Concept type. This also means the old APOC-side
                # NODE_GLOBAL uniqueness (which could silently miss a valid longer mapping if
                # a shorter path already "claimed" one of its intermediate nodes) no longer
                # applies -- results are now complete relative to the max_hops/limit/ordering
                # constraints, which matches the intent of "nearest mappings first" better.
                result = await _execute_query_with_retry(
                    query=f"""
                    MATCH (src:Concept {{prefix: $prefix}})
                    WHERE src.id IN $concept_ids
                    CALL (src) {{
                        MATCH path = (src)-[:annotated_with|has_symbol|exact|broad|narrow|related*1..{int(max_hops)}]-(tgt:Concept {{prefix: $target_prefix}})
                        WHERE ALL(rel IN relationships(path) WHERE startNode(rel).prefix <> endNode(rel).prefix)
                        WITH src, tgt, path, [n IN nodes(path) | n.prefix] AS prefixes
                        WHERE all(i IN range(0, size(prefixes) - 1) WHERE NOT prefixes[i] IN prefixes[(i + 1)..])
                            AND ALL(n IN nodes(path)[1..-2] WHERE n.prefix <> src.prefix AND n.prefix <> $target_prefix)
                        WITH src, tgt, path
                        ORDER BY length(path) ASC
                        LIMIT COALESCE($limit, 1000000)
                        RETURN src.id AS source_id, collect(DISTINCT tgt.id) AS mapped_terms
                    }}
                    RETURN source_id AS concept_id, mapped_terms
                    """,
                    session=session,
                    parameters={
                        'prefix': prefix.value,
                        'target_prefix': target_prefix.value,
                        'concept_ids': concept_ids,
                        'limit': limit,
                    },
                )

                async for record in result:
                    if first is None:
                        first = time.perf_counter()

                    mapped = list(set(record['mapped_terms']))

                    MAP_COUNT.labels(
                        prefix=prefix.value,
                        target_prefix=target_prefix.value,
                    ).observe(len(mapped))

                    yield RelatedTerm(
                        conceptId=record['concept_id'],
                        relatedConcepts=mapped,
                    )
        except asyncio.CancelledError:
            result_label = 'cancelled'
            raise
        except Exception as e:
            result_label = 'error'
            GRAPHDB_OP_ERRORS.labels(
                backend='neo4j',
                op='map',
                prefix=prefix.value,
                target_prefix=target_prefix.value,
                error_type=type(e).__name__,
            ).inc()
            raise
        finally:
            end = time.perf_counter()
            GRAPHDB_OP_DURATION.labels(
                backend='neo4j',
                op='map',
                prefix=prefix.value,
                target_prefix=target_prefix.value,
                mode=('bounded' if limit is not None else 'unbounded'),
                result=result_label,
            ).observe(end - start)
            if first is not None:
                GRAPHDB_OP_TTFR.labels(
                    backend='neo4j',
                    op='map',
                    prefix=prefix.value,
                    target_prefix=target_prefix.value,
                    mode=('bounded' if limit is not None else 'unbounded'),
                    result=result_label,
                ).observe(first - start)

    async def trace_term_iter(self,
                              prefix_start: ConceptPrefix,
                              prefix_end: ConceptPrefix,
                              id_start: str,
                              id_end: str,
                              relationship_type: AnnotationType | ConceptRelationshipType,
                              forward: bool | None = True,
                              max_depth: int = 12,
                              ) -> AsyncIterator[ConceptPath]:
        """
        Trace one or more paths between two terms.

        It returns all available paths without repeating sequence. If a path is a subset of another
        path with order preserved, only the shorter path is returned.
        :param prefix_start: The prefix of the starting concept.
        :param prefix_end: The prefix of the ending concept.
        :param id_start: The ID of the starting concept.
        :param id_end: The ID of the ending concept.
        :param relationship_type: The type of relationship to trace through.
        :param forward: If True, the direction of path must be from start to end; if False, it
            shall be from end to start. If None, direction is ignored, but only the shortest path
            is returned.
        :param max_depth: The maximum depth to trace.
        :return: An asynchronous iterator yielding ConceptPath instances.
        """
        rel_type = relationship_type.value
        # Quantified relationship bounds must be literal at parse time, so max_depth is
        # interpolated into the pattern. The relationship type itself stays a genuine query
        # parameter via Cypher's dynamic relationship type syntax -- note the expression inside
        # $(...) must be `$rel_type` (a parameter reference), not the bare name `rel_type`: that
        # would be parsed as a reference to an already-bound Cypher *variable* of that name
        # (there isn't one here), not a query parameter, and fails with "Variable not defined".
        max_depth_int = int(max_depth)

        if forward is None:
            # Special query, only return one shortest path
            query = f"""
            MATCH (start:Concept {{prefix: $prefix_start, id: $id_start}})
            MATCH (end:Concept   {{prefix: $prefix_end,   id: $id_end}})
            MATCH p = SHORTEST 1 (start)-[:$($rel_type)*1..{max_depth_int}]-(end)
            RETURN
                $id_start AS startConceptId,
                $id_end   AS endConceptId,
                $prefix_start AS startPrefix,
                $prefix_end   AS endPrefix,
                size(nodes(p)) AS length,
                [n IN nodes(p) | {{conceptId: n.id, prefix: n.prefix}}] AS nodes
            """
        else:
            arrow_pattern = (
                f'(start)-[:$($rel_type)*1..{max_depth_int}]->(end)' if forward
                else f'(start)<-[:$($rel_type)*1..{max_depth_int}]-(end)'
            )

            query = f"""
            MATCH (start:Concept {{prefix: $prefix_start, id: $id_start}})
            MATCH (end:Concept   {{prefix: $prefix_end,   id: $id_end}})

            MATCH p = {arrow_pattern}

            WITH collect(p) AS paths

            WITH [p IN paths WHERE
                NOT any(q IN paths WHERE
                    q <> p
                    AND length(q) < length(p)
                    AND reduce(st = {{ok: true, idx: 0}}, n IN nodes(q) |
                        CASE
                            WHEN st.ok = false
                                THEN st
                            ELSE
                                CASE
                                    WHEN head([i IN range(0, size(nodes(p)) - st.idx - 1)
                                        WHERE nodes(p)[st.idx + i] = n | i]) IS NULL
                                        THEN {{ok: false, idx: st.idx}}
                                    ELSE
                                        {{
                                            ok: true,
                                            idx: st.idx + head([i IN range(0, size(nodes(p)) - st.idx - 1)
                                                WHERE nodes(p)[st.idx + i] = n | i]) + 1
                                        }}
                                END
                        END
                    ).ok
                )
            ] AS filtered

            UNWIND filtered AS p
            RETURN
                $id_start AS startConceptId,
                $id_end   AS endConceptId,
                $prefix_start AS startPrefix,
                $prefix_end   AS endPrefix,
                size(nodes(p)) AS length,
                [n IN nodes(p) | {{conceptId: n.id, prefix: n.prefix}}] AS nodes
            ORDER BY length ASC, size(nodes(p)) ASC
            """

        async with self._client.session() as session:
            result = await _execute_query_with_retry(
                query=query,
                session=session,
                parameters={
                    'prefix_start': prefix_start.value,
                    'prefix_end': prefix_end.value,
                    'id_start': id_start,
                    'id_end': id_end,
                    'rel_type': rel_type,
                }
            )

            async for record in result:
                yield ConceptPath(
                    startConceptId=record["startConceptId"],
                    endConceptId=record["endConceptId"],
                    startPrefix=record["startPrefix"],
                    endPrefix=record["endPrefix"],
                    length=int(record["length"]),
                    nodes=[
                        NodeInPath(
                            conceptId=n["conceptId"],
                            prefix=n["prefix"],
                        )
                        for n in record["nodes"]
                    ],
                )

    async def trace_term_aggregate_iter(self,
                                        trace_queries: list[tuple[
                                            ConceptPrefix,
                                            str,
                                            ConceptPrefix,
                                            str,
                                            ConceptRelationshipType | AnnotationType,
                                            bool | None,
                                            int
                                        ]]
                                        ) -> AsyncIterator[ConceptPath]:
        """
        Trace multiple paths between multiple pairs of terms.
        :param trace_queries: A list of tuples containing:
            (prefix_start, id_start, prefix_end, id_end, relationship_type, forward, max_depth)
        :return: An asynchronous iterator yielding ConceptPath instances.
        """
        if not trace_queries:
            return

        prepared = []
        for (prefix_start, id_start, prefix_end, id_end, relationship_type, forward, max_depth) in trace_queries:
            prepared.append({
                'prefix_start': prefix_start.value,
                'id_start': id_start,
                'prefix_end': prefix_end.value,
                'id_end': id_end,
                'rel_type': relationship_type.value,
                'forward': forward,
                'max_depth': max_depth,
            })

        # Quantified relationship bounds must be literal at parse time, so every row in the
        # batch shares one match-time depth cap (the largest max_depth requested); each row's
        # own, possibly smaller, max_depth is still enforced with a WHERE length(p) <= q.max_depth
        # filter. The relationship type stays a genuine per-row parameter via dynamic
        # relationship type syntax (:$(q.rel_type)).
        max_depth_cap = max(int(q['max_depth']) for q in prepared)

        detour_filter = """
              WITH collect(p) AS all_paths
              WITH [p IN all_paths WHERE
                NOT any(alt IN all_paths WHERE
                  alt <> p
                  AND length(alt) < length(p)
                  AND reduce(st = {ok: true, idx: 0}, n IN nodes(alt) |
                    CASE
                      WHEN st.ok = false THEN st
                      ELSE
                        CASE
                          WHEN head([i IN range(0, size(nodes(p)) - st.idx - 1)
                              WHERE nodes(p)[st.idx + i] = n | i]) IS NULL
                            THEN {ok: false, idx: st.idx}
                          ELSE
                            {
                              ok: true,
                              idx: st.idx + head([i IN range(0, size(nodes(p)) - st.idx - 1)
                                  WHERE nodes(p)[st.idx + i] = n | i]) + 1
                            }
                        END
                    END
                  ).ok
                )
              ] AS paths
              RETURN paths"""

        query = f"""
            UNWIND $queries AS q
            MATCH (start:Concept {{prefix: q.prefix_start, id: q.id_start}})
            MATCH (end:Concept   {{prefix: q.prefix_end,   id: q.id_end}})
            WITH q, start, end

            CALL (q, start, end) {{
              WHEN q.forward IS NULL THEN {{
                MATCH p = SHORTEST 1 (start)-[:$(q.rel_type)*1..{max_depth_cap}]-(end)
                RETURN collect(p) AS paths
              }}
              WHEN q.forward = true THEN {{
                MATCH p = (start)-[:$(q.rel_type)*1..{max_depth_cap}]->(end)
                WHERE length(p) <= q.max_depth
                {detour_filter}
              }}
              ELSE {{
                MATCH p = (start)<-[:$(q.rel_type)*1..{max_depth_cap}]-(end)
                WHERE length(p) <= q.max_depth
                {detour_filter}
              }}
            }}

            UNWIND paths AS p
            RETURN
              q.id_start      AS startConceptId,
              q.id_end        AS endConceptId,
              q.prefix_start  AS startPrefix,
              q.prefix_end    AS endPrefix,
              size(nodes(p))  AS length,
              [n IN nodes(p) | {{conceptId: n.id, prefix: n.prefix}}] AS nodes
            ORDER BY startPrefix, startConceptId, endPrefix, endConceptId, length ASC
            """

        async with self._client.session() as session:
            result = await _execute_query_with_retry(
                query=query,
                session=session,
                parameters={"queries": prepared},
            )

            async for record in result:
                yield ConceptPath(
                    startConceptId=record["startConceptId"],
                    endConceptId=record["endConceptId"],
                    startPrefix=record["startPrefix"],
                    endPrefix=record["endPrefix"],
                    length=int(record["length"]),
                    nodes=[
                        NodeInPath(
                            conceptId=n["conceptId"],
                            prefix=n["prefix"],
                        )
                        for n in record["nodes"]
                    ],
                )

    async def get_similar_terms_aggregate_iter(self,
                                               prefix: ConceptPrefix,
                                               similarity_queries: list[tuple[str, float]],
                                               ) -> AsyncIterator[SimilarTermAggregate]:
        """
        Get similar terms for a list of concept IDs as an asynchronous iterator.

        This method is designed for the GraphQL query, where each concept ID may have a
        different similarity threshold, but is always within the same vocabulary prefix.
        It only returns the highest similarity score for each similar term, regardless
        of the corpus or method used to calculate the similarity.
        :param prefix: The prefix of the concepts to find similar terms for.
        :param similarity_queries: A tuple containing the concept ID and the similarity threshold.
        :return: An asynchronous iterator yielding SimilarTerm instances.
        """
        async with self._client.session() as session:
            result = await _execute_query_with_retry(
                query="""
                UNWIND $similarity_queries AS sim_query
                MATCH (n:Concept {prefix: $prefix, id: sim_query.concept_id})
                MATCH (n)-[r:similar_to]-(m:Concept {prefix: $prefix})
                WITH n, m, properties(r) AS props, sim_query.threshold AS threshold
                WITH
                    n,
                    m,
                    [k IN keys(props) WHERE props[k] >= threshold | props[k]] AS scores
                WHERE size(scores) > 0
                WITH
                    n,
                    m,
                    reduce(best = scores[0], s IN scores[1..] | CASE WHEN s > best THEN s ELSE best END) AS highest_score
                ORDER BY n.id, highest_score DESC
                WITH
                    n,
                    collect({
                        id: m.id,
                        highest_score: highest_score
                    }) AS similar_concepts
                RETURN
                    n.id AS concept_id,
                    similar_concepts AS similar_concepts
                ORDER BY concept_id
                """,
                session=session,
                parameters={
                    'prefix': prefix.value,
                    'similarity_queries': [
                        {
                            'concept_id': sim_id,
                            'threshold': threshold,
                        }
                        for sim_id, threshold in similarity_queries
                    ],
                }
            )

            async for record in result:
                concept_id: str = record['concept_id']
                similar_concepts_data: list[dict] = record['similar_concepts']

                similar_concepts: list[tuple[str, float]] = [
                    (sim['id'], sim['highest_score'])
                    for sim in similar_concepts_data
                ]

                yield SimilarTermAggregate(
                    conceptId=concept_id,
                    similarConcepts=similar_concepts,
                )

    @staticmethod
    def _build_similar_term_record(record,
                                   ) -> tuple[str, SimilarTermByPrefix, int]:
        """
        Parse one query result record into a (concept_id, group, group_size) tuple.
        :param record: The raw Neo4j result record.
        :return: A tuple of the concept ID, the SimilarTermByPrefix group for this record, and
            the number of similar concepts in the group.
        """
        concept_id: str = record['concept_id']
        similar_prefix = ConceptPrefix(record['similar_prefix'])
        similar_concepts_data: list[dict] = record['similar_concepts'] or []

        similar_concepts: list[SimilarTermWithScores] = [
            SimilarTermWithScores(
                conceptId=sim['id'],
                similarity_scores=dict(sim['score_pairs']),
            )
            for sim in similar_concepts_data
        ]

        group = SimilarTermByPrefix(
            prefix=similar_prefix,
            similarConcepts=similar_concepts,
        )

        return concept_id, group, len(similar_concepts_data)

    @staticmethod
    def _observe_and_build_similar_term(prefix: ConceptPrefix,
                                        variant: str,
                                        concept_id: str,
                                        groups: list[SimilarTermByPrefix],
                                        total: int,
                                        ) -> SimilarTerm:
        """
        Record grouping metrics for a completed concept's similar-term groups and build the
        SimilarTerm result to yield.
        :param prefix: The prefix of the concepts the similar terms were found for.
        :param variant: The 'same_prefix'/'cross_prefix' metric label variant.
        :param concept_id: The concept ID the groups belong to.
        :param groups: The completed list of SimilarTermByPrefix groups for this concept.
        :param total: The total number of similar concepts across all groups.
        :return: The built SimilarTerm instance.
        """
        SIM_GROUPS.labels(
            prefix=prefix.value,
            variant=variant,
        ).observe(len(groups))
        SIM_TOTAL.labels(
            prefix=prefix.value,
            variant=variant,
        ).observe(total)

        return SimilarTerm(
            conceptId=concept_id,
            similarGroups=groups,
        )

    async def get_similar_terms_iter(self,
                                     prefix: ConceptPrefix,
                                     concept_ids: list[str],
                                     threshold: float = 1.0,
                                     same_prefix: bool = True,
                                     corpus_prefix: ConceptPrefix | None = None,
                                     method: SimilarityMethod | None = None,
                                     limit: int | None = None,
                                     ) -> AsyncIterator[SimilarTerm]:
        """
        Get similar terms for the given concept IDs as an asynchronous iterator.
        :param prefix: The prefix of the concepts to find similar terms for.
        :param concept_ids: The list of concept IDs to find similar terms for.
        :param threshold: The similarity threshold to filter similar terms.
        :param same_prefix: Whether to only consider similar terms within the same prefix.
        :param corpus_prefix: The corpus prefix that was used to calculate the similarity score,
            if applicable.
        :param method: The similarity method to use.
        :param limit: The maximum number of similar terms to return for each concept ID.
        :return: An asynchronous iterator yielding SimilarTerm instances.
        """
        variant = 'same_prefix' if same_prefix else 'cross_prefix'
        start = time.perf_counter()
        first = None
        result_label = 'ok'

        try:
            async with self._client.session() as session:
                if same_prefix:
                    target_prefixes = [prefix.value]
                else:
                    target_prefixes = [p.value for p in ConceptPrefix]

                result = await _execute_query_with_retry(
                    query="""
                    MATCH (n:Concept {prefix: $prefix})
                    WHERE n.id IN $concept_ids
                    MATCH (n)-[r:similar_to]-(m:Concept)
                    WHERE m.prefix IN $target_prefixes
                    WITH n, m, properties(r) AS props
                    WITH
                        n,
                        m,
                        props,
                        [
                            k IN keys(props)
                            WHERE props[k] >= $threshold AND
                            (
                                ($corpus_prefix IS NULL AND $method IS NULL)
                                OR ($corpus_prefix IS NULL AND $method IS NOT NULL
                                    AND (k = $method OR k STARTS WITH $method + ':'))
                                OR ($corpus_prefix IS NOT NULL AND k ENDS WITH ':' + $corpus_prefix)
                            )
                        ] AS valid_keys
                    WHERE size(valid_keys) > 0
                    WITH
                        n,
                        m,
                        [k IN valid_keys | [k, props[k]]] AS score_pairs,
                        reduce(best = props[valid_keys[0]], k IN valid_keys[1..] |
                            CASE WHEN props[k] > best THEN props[k] ELSE best END) AS max_score
                    ORDER BY n.id, max_score DESC
                    WITH
                        n,
                        collect({
                            id: m.id,
                            prefix: m.prefix,
                            score_pairs: score_pairs
                        }) AS sims
                    WITH
                        n,
                        CASE
                            WHEN $limit IS NULL THEN sims
                            ELSE sims[0..$limit]
                        END AS limited_sims
                    UNWIND limited_sims AS sim
                    WITH
                        n,
                        sim.prefix AS similar_prefix,
                        sim
                    WITH
                        n,
                        similar_prefix,
                        collect({
                            id: sim.id,
                            score_pairs: sim.score_pairs
                        }) AS similar_concepts
                    RETURN
                        n.id AS concept_id,
                        similar_prefix AS similar_prefix,
                        similar_concepts AS similar_concepts
                    ORDER BY concept_id, similar_prefix
                    """,
                    session=session,
                    parameters={
                        'prefix': prefix.value,
                        'concept_ids': concept_ids,
                        'target_prefixes': target_prefixes,
                        'threshold': threshold,
                        'method': method.value if method else None,
                        'corpus_prefix': corpus_prefix.value if corpus_prefix else None,
                        'limit': limit,
                    }
                )

                current_concept_id: str | None = None
                current_groups: list[SimilarTermByPrefix] = []
                current_total = 0

                async for record in result:
                    if first is None:
                        first = time.perf_counter()

                    concept_id, group, group_size = self._build_similar_term_record(record)

                    SIM_PER_GROUP.labels(
                        prefix=prefix.value,
                        variant=variant,
                    ).observe(group_size)

                    current_total += group_size

                    if current_concept_id is None:
                        current_concept_id = concept_id
                        current_groups = [group]
                    elif current_concept_id == concept_id:
                        current_groups.append(group)
                    else:
                        yield self._observe_and_build_similar_term(
                            prefix, variant, current_concept_id, current_groups, current_total,
                        )
                        current_concept_id = concept_id
                        current_groups = [group]
                        current_total = group_size

                if current_concept_id is not None:
                    yield self._observe_and_build_similar_term(
                        prefix, variant, current_concept_id, current_groups, current_total,
                    )
        except asyncio.CancelledError:
            result_label = 'cancelled'
            raise
        except Exception as e:
            result_label = 'error'
            GRAPHDB_OP_ERRORS.labels(
                backend='neo4j',
                op='get_similar_terms',
                prefix=prefix.value,
                error_type=type(e).__name__,
            ).inc()
            raise
        finally:
            end = time.perf_counter()
            GRAPHDB_OP_DURATION.labels(
                backend='neo4j',
                op='get_similar_terms',
                prefix=prefix.value,
                mode=variant,
                result=result_label,
            ).observe(end - start)
            if first is not None:
                GRAPHDB_OP_TTFR.labels(
                    backend='neo4j',
                    op='get_similar_terms',
                    prefix=prefix.value,
                    mode=variant,
                    result=result_label,
                ).observe(first - start)

    async def translate_terms_iter(self,
                                   original_ids: list[str],
                                   original_prefix: ConceptPrefix,
                                   constraint_ids: dict[ConceptPrefix, set[str]],
                                   threshold: float = 1.0,
                                   limit: int | None = None,
                                   ) -> AsyncIterator[TranslatedTerm]:
        """
        Translate terms to a subset of the constraint vocabulary as an asynchronous iterator,
        based on the similarity scores.
        :param original_ids: The list of original concept IDs to translate.
        :param original_prefix: The prefix of the original concepts.
        :param constraint_ids: A dictionary mapping constraint vocabulary prefixes to sets of concept IDs.
        :param threshold: The similarity threshold to filter translations.
        :param limit: The maximum number of translations to return for each original concept ID.
        :return: An asynchronous iterator yielding TranslatedTerm instances.
        """
        async with self._client.session() as session:
            result = await _execute_query_with_retry(
                query="""
                UNWIND keys($constraint_ids) AS constraint_prefix

                MATCH (n:Concept {prefix: $original_prefix})
                WHERE n.id IN $original_ids

                MATCH (n)-[r:similar_to]-(m:Concept {prefix: constraint_prefix})
                WHERE m.id IN $constraint_ids[constraint_prefix]

                WITH n, m, properties(r) AS props
                WITH
                    n,
                    m,
                    props,
                    [k IN keys(props) WHERE props[k] >= $threshold] AS valid_keys
                WHERE size(valid_keys) > 0
                WITH
                    n,
                    m,
                    reduce(best = props[valid_keys[0]], k IN valid_keys[1..] |
                        CASE WHEN props[k] > best THEN props[k] ELSE best END) AS max_score
                ORDER BY n.id, max_score DESC

                WITH
                    n,
                    collect({
                        id: m.id,
                        prefix: m.prefix,
                        max_score: max_score
                    }) AS sims
                WITH
                    n,
                    CASE
                        WHEN $limit IS NULL THEN sims
                        ELSE sims[0..$limit]
                    END AS limited_sims
                UNWIND limited_sims AS sim
                RETURN
                    n.id AS original_id,
                    sim.id AS translated_id,
                    sim.prefix AS translated_prefix,
                    sim.max_score AS similarity_score
                ORDER BY original_id, similarity_score DESC
                """,
                session=session,
                parameters={
                    'original_prefix': original_prefix.value,
                    'original_ids': original_ids,
                    'constraint_ids': {k.value: list(v) for k, v in constraint_ids.items()},
                    'threshold': threshold,
                    'limit': limit,
                }
            )

            async for record in result:
                yield TranslatedTerm(
                    conceptId=record['translated_id'],
                    prefix=record['translated_prefix'],
                    score=record['similarity_score'],
                )

    @property
    def reactome(self) -> Neo4jReactomeRepository:
        """
        Get the Reactome repository interface.
        :return: Neo4jReactomeRepository instance.
        """
        return Neo4jReactomeRepository(
            client=self._client,
        )
