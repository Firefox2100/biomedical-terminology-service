import asyncio
from typing import LiteralString, AsyncIterator, Any
import networkx as nx
import pandas as pd
from neo4j import AsyncDriver, AsyncSession
from neo4j.exceptions import TransientError

from bioterms.etc.enums import ConceptPrefix, SimilarityMethod
from bioterms.model.concept import Concept
from bioterms.model.annotation import Annotation
from bioterms.model.expanded_term import ExpandedTerm
from bioterms.model.similar_term import SimilarTermByPrefix, SimilarTerm
from .graph_db import GraphDatabase


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

    @staticmethod
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

        while attempt < backoff_retries:
            try:
                result = await session.run(
                    query,
                    **(parameters or {}),
                )
                return result
            except TransientError as e:
                if attempt < backoff_retries - 1:
                    attempt += 1
                    await asyncio.sleep(backoff_time)
                    backoff_time *= 2
                else:
                    raise e

        raise RuntimeError('Exceeded maximum retry attempts for query execution.')

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

    async def save_vocabulary_graph(self,
                                    concepts: list[Concept],
                                    graph: nx.DiGraph,
                                    ):
        """
        Save the vocabulary graph to the graph database.
        :param concepts: The list of concepts to save. This list is passed in to
            allow for any necessary term metadata to be accessed during graph saving.
        :param graph: The vocabulary graph to save.
        """
        edges = [
            (str(source), str(target), data.get('label'))
            for source, target, data in graph.edges(data=True)
        ]

        async with self._client.session() as session:
            # Insert the concepts first before adding edges
            await self._execute_query_with_retry(
                query="""
                UNWIND $concepts AS concept
                WITH concept, coalesce(concept.conceptTypes, []) AS types
                MERGE (n:Concept {id: concept.conceptId, prefix: concept.prefix})

                WITH n, [t IN types WHERE t IS NOT NULL AND trim(t) <> ""] AS labels
                CALL apoc.create.addLabels(n, labels) YIELD node
                RETURN count(node) AS upserted
                """,
                session=session,
                parameters={
                    'concepts': [concept.model_dump() for concept in concepts],
                },
            )

            # Insert the edges
            await self._execute_query_with_retry(
                query="""
                UNWIND $edges AS edge
                MERGE (source:Concept {id: edge[0], prefix: $concept_prefix})
                MERGE (target:Concept {id: edge[1], prefix: $concept_prefix})
                WITH source, target, edge, coalesce(edge[2], 'related_to') as rel_label
                CALL apoc.merge.relationship(source, rel_label, {}, {}, target) YIELD rel
                RETURN count(rel) AS created
                """,
                session=session,
                parameters={
                    'edges': edges,
                    'concept_prefix': concepts[0].prefix if concepts else '',
                },
            )

    async def get_vocabulary_graph(self,
                                   prefix: ConceptPrefix,
                                   ) -> nx.DiGraph:
        """
        Retrieve the vocabulary graph from the graph database.
        :param prefix: The node prefix of the vocabulary to retrieve.
        :return: The vocabulary graph.
        """
        vocabulary_graph = nx.DiGraph()

        async with self._client.session() as session:
            # Retrieve all nodes first from neo4j
            nodes_result = await self._execute_query_with_retry(
                query="""
                MATCH (n:Concept {prefix: $prefix})
                RETURN n.id AS concept_id
                """,
                session=session,
                parameters={'prefix': prefix.value},
            )

            async for record in nodes_result:
                concept_id = record['concept_id']
                vocabulary_graph.add_node(concept_id)

            # Retrieve all edges that connects WITHIN the vocabulary
            edges_result = await self._execute_query_with_retry(
                query="""
                MATCH (source:Concept {prefix: $prefix})-[r]->(target:Concept {prefix: $prefix})
                RETURN source.id AS source_id, target.id AS target_id, type(r) AS rel_label
                """,
                session=session,
                parameters={'prefix': prefix.value},
            )

            async for record in edges_result:
                source_id = record['source_id']
                target_id = record['target_id']
                rel_label = record['rel_label']
                vocabulary_graph.add_edge(source_id, target_id, label=rel_label)

        return vocabulary_graph

    async def delete_vocabulary_graph(self,
                                      prefix: ConceptPrefix,
                                      ):
        """
        Delete the vocabulary graph from the graph database.
        :param prefix: The node prefix of the vocabulary to delete.
        """
        async with self._client.session() as session:
            await self._execute_query_with_retry(
                query="""
                MATCH (n:Concept {prefix: $prefix})
                DETACH DELETE n
                """,
                session=session,
                parameters={'prefix': prefix.value},
            )

    async def count_terms(self,
                          prefix: ConceptPrefix,
                          ) -> int:
        """
        Count the number of nodes for a given prefix in the graph database.
        :param prefix: The vocabulary prefix to count nodes for.
        :return: The number of nodes with the given prefix.
        """
        async with self._client.session() as session:
            result = await self._execute_query_with_retry(
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
            result = await self._execute_query_with_retry(
                query="""
                MATCH (source:Concept {prefix: $prefix})-[r]->(target:Concept {prefix: $prefix})
                RETURN count(r) AS relationship_count
                """,
                session=session,
                parameters={'prefix': prefix.value},
            )

            record = await result.single()
            return record['relationship_count'] if record is not None else 0

    async def save_annotations(self,
                               annotations: list[Annotation],
                               ):
        """
        Save a list of annotations into the graph database.
        :param annotations: A list of Annotation instances to save.
        """
        async with self._client.session() as session:
            await self._execute_query_with_retry(
                query="""
                UNWIND $annotations AS annotation
                MERGE (source:Concept {id: annotation.conceptIdFrom, prefix: annotation.prefixFrom})
                MERGE (target:Concept {id: annotation.conceptIdTo, prefix: annotation.prefixTo})
                WITH source,
                    target,
                    coalesce(annotation.annotationType, 'annotated_with') AS rel_type,
                    coalesce(annotation.properties, {}) AS props
                CALL apoc.merge.relationship(source, rel_type, {}, props, target) YIELD rel
                RETURN count(rel) AS created
                """,
                session=session,
                parameters={'annotations': [annotation.model_dump() for annotation in annotations]},
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
            result = await self._execute_query_with_retry(
                query="""
                MATCH (source:Concept {prefix: $prefix_1})-[r]-(target:Concept {prefix: $prefix_2})
                RETURN source.id AS source_id, target.id AS target_id, type(r) AS rel_label, properties(r) AS rel_props
                """,
                session=session,
                parameters={
                    'prefix_1': prefix_1.value,
                    'prefix_2': prefix_2.value,
                },
            )

            async for record in result:
                source_id = record['source_id']
                target_id = record['target_id']
                rel_label = record['rel_label']
                rel_props = record['rel_props']

                annotation_graph.add_edge(
                    source_id,
                    target_id,
                    label=rel_label,
                    **rel_props
                )

        return annotation_graph

    async def delete_annotations(self,
                                 prefix_1: ConceptPrefix,
                                 prefix_2: ConceptPrefix,
                                 ):
        """
        Delete annotations between two vocabularies from the graph database.
        :param prefix_1: The first vocabulary prefix.
        :param prefix_2: The second vocabulary prefix.
        """
        async with self._client.session() as session:
            await self._execute_query_with_retry(
                query="""
                MATCH (source:Concept {prefix: $prefix_1})-[r]->(target:Concept {prefix: $prefix_2})
                DELETE r
                """,
                session=session,
                parameters={
                    'prefix_1': prefix_1.value,
                    'prefix_2': prefix_2.value,
                },
            )

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
            result = await self._execute_query_with_retry(
                query="""
                MATCH (source:Concept {prefix: $prefix_1})-[r]->(target:Concept {prefix: $prefix_2})
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
                                     similarity_df: pd.DataFrame,
                                     similarity_method: str,
                                     corpus_prefix: ConceptPrefix | None = None,
                                     ):
        """
        Save similarity scores between two vocabularies into the graph database.
        :param prefix_from: The source vocabulary prefix. Correspond to 'concept_from' in similarity_df.
        :param prefix_to: The target vocabulary prefix. Correspond to 'concept_to' in similarity_df.
        :param similarity_df: A DataFrame containing similarity scores. In the format of:
            | concept_from | concept_to | similarity |
        :param similarity_method: The similarity method used to generate the scores. Stored as
            property name on the relationship.
        :param corpus_prefix: The corpus vocabulary prefix, if applicable.
        """
        async with self._client.session() as session:
            # Similarity data can be millions of rows, so need to batch the inserts
            batch_size = 1000

            for start_idx in range(0, len(similarity_df), batch_size):
                end_idx = start_idx + batch_size
                batch_df = similarity_df.iloc[start_idx:end_idx]

                similarities = [
                    {
                        'concept_from': row['concept_from'],
                        'concept_to': row['concept_to'],
                        'similarity': row['similarity'],
                    }
                    for _, row in batch_df.iterrows()
                ]

                await self._execute_query_with_retry(
                    query="""
                    UNWIND $similarities AS sim
                    MATCH (source:Concept {id: sim.concept_from, prefix: $prefix_from})
                    MATCH (target:Concept {id: sim.concept_to, prefix: $prefix_to})
                    WITH source, target, sim.similarity AS sim_score
                    CALL apoc.merge.relationship(
                        source,
                        'similar_to',
                        {},
                        { $similarity_property: sim_score },
                        target
                    ) YIELD rel
                    RETURN count(rel) AS created
                    """,
                    session=session,
                    parameters={
                        'similarities': similarities,
                        'prefix_from': prefix_from.value,
                        'prefix_to': prefix_to.value,
                        'similarity_property': f'{similarity_method}:{corpus_prefix}'
                            if corpus_prefix else similarity_method,
                    },
                )

    async def create_index(self):
        """
        Create indexes in Neo4J
        """
        async with self._client.session() as session:
            await self._execute_query_with_retry(
                query="""
                CREATE INDEX concept_prefix_index IF NOT EXISTS
                FOR (n:Concept)
                ON (n.prefix)
                """,
                session=session,
            )
            await self._execute_query_with_retry(
                query="""
                CREATE INDEX concept_id_index IF NOT EXISTS
                FOR (n:Concept)
                ON (n.id)
                """,
                session=session,
            )
            await self._execute_query_with_retry(
                query="""
                CREATE CONSTRAINT concept_prefix_id_unique IF NOT EXISTS
                FOR (n:Concept)
                REQUIRE (n.prefix, n.id) IS UNIQUE
                """,
                session=session,
            )

    async def expand_terms_iter(self,
                                prefix: ConceptPrefix,
                                concept_ids: list[str],
                                max_depth: int | None = None,
                                limit: int | None = None,
                                ) -> AsyncIterator[ExpandedTerm]:
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
        async with self._client.session() as session:
            if max_depth is None:
                result = await self._execute_query_with_retry(
                    query="""
                    MATCH (n:Concept {prefix: $prefix})
                    WHERE n.id IN $concept_ids
                    OPTIONAL MATCH (n)<-[:is_a*]-(descendant:Concept)
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
                result = await self._execute_query_with_retry(
                    query="""
                    MATCH (n:Concept {prefix: $prefix})
                    WHERE n.id IN $concept_ids
                    CALL apoc.path.expandConfig(
                        n,
                        {
                            relationshipFilter: 'is_a<',
                            labelFilter: '+Concept',
                            minLevel: 1,
                            maxLevel: $depth,
                            bfs: true,
                            uniqueness: 'NODE_GLOBAL'
                        }
                    ) YIELD path
                    WITH n, [d IN collect(DISTINCT last(nodes(path)).id) WHERE d IS NOT NULL] AS all_desc
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
                        'depth': max_depth,
                    },
                )

            async for record in result:
                yield ExpandedTerm(
                    conceptId=record['concept_id'],
                    descendants=list(set(record['descendants'])),
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
        async with self._client.session() as session:
            if same_prefix:
                target_prefixes = [prefix.value]
            else:
                target_prefixes = [p.value for p in ConceptPrefix]

            result = await self._execute_query_with_retry(
                query="""
                MATCH (n:Concept {prefix: $prefix})
                WHERE n.id IN $concept_ids
                MATCH (n)-[r:similar_to]-(m:Concept)
                WHERE m.prefix IN $target_prefixes
                WITH n, m, apoc.convert.toMap(r) AS props
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
                    apoc.coll.max([k IN valid_keys | props[k]]) AS score
                ORDER BY n.id, score DESC
                WITH
                    n,
                    collect({id: m.id, prefix: m.prefix}) AS sims
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
                    sim.id AS similar_id
                WITH
                    n,
                    similar_prefix,
                    collect(similar_id) AS similar_ids
                RETURN
                    n.id AS concept_id,
                    similar_prefix AS similar_prefix,
                    similar_ids AS similar_ids
                
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

            current_concept_id = None
            current_groups: list[SimilarTermByPrefix] = []

            async for record in result:
                concept_id = record['concept_id']
                similar_prefix = ConceptPrefix(record['similar_prefix'])
                similar_ids = record['similar_ids']

                group = SimilarTermByPrefix(
                    prefix=similar_prefix,
                    similarConcepts=similar_ids,
                )

                if current_concept_id is None:
                    current_concept_id = concept_id
                    current_groups = [group]
                elif current_concept_id == concept_id:
                    current_groups.append(group)
                else:
                    yield SimilarTerm(
                        conceptId=current_concept_id,
                        similarGroups=current_groups,
                    )
                    current_concept_id = concept_id
                    current_groups = [group]

            if current_concept_id is not None:
                yield SimilarTerm(
                    conceptId=current_concept_id,
                    similarGroups=current_groups,
                )
