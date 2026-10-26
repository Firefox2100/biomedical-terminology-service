MCP API Reference
=================

The MCP (Model Context Protocol) API exposes a focused set of BTS operations as typed,
read-only tools for language models and other MCP clients. It is built with
`FastMCP <https://fastmcp.readthedocs.io/>`_ and runs inside the main BTS application.
Use REST or GraphQL when a conventional application API is a better fit.

Connection
==========

The MCP server is mounted at ``/mcp`` and uses FastMCP's streamable HTTP transport. Point
the client at, for example, ``http://localhost:5000/mcp``. It is part of the web process;
there is no separate stdio command in the BTS deployment.

No separate authentication is required at the MCP transport layer; the server shares the same document, graph, and vector database instances as the REST and GraphQL APIs. Any concept readable via REST or GraphQL is readable via the MCP tools.

Supported Vocabularies
======================

The tool parameters accept vocabulary identifiers defined in two Enums:

* **ConceptPrefix** (used by ``auto_complete``, ``search_vocabulary``, ``map_concepts``, ``get_similar_concepts`` and ``translate_concepts_to_constraints``): ``CTV3``, ``ENSEMBL``, ``GO``, ``HGNC``, ``HGNC_SYMBOL``, ``HPO``, ``LOINC``, ``MONDO``, ``NCIT``, ``OHDSI``, ``OMIM``, ``ORDO``, ``REACTOME``, ``RXNORM``, ``SNOMED``, ``UBERON``, ``UNIPROT``. All seventeen supported vocabularies are available.

* **OntologyPrefix** (used only by ``expand_ontology``):
  ``CTV3``, ``GO``, ``HPO``, ``LOINC``, ``MONDO``, ``NCIT``, ``OHDSI``, ``OMIM``, ``ORDO``, ``REACTOME``, ``RXNORM``, ``SNOMED``, ``UBERON``. This is restricted to vocabularies with a traversable ontology or internal hierarchy; atomic identifier tables such as HGNC and UniProt are not included.

Tools
=====

The MCP server exposes six tools. Every tool carries standard MCP tool annotations indicating it is read-only, idempotent, and deterministic (``openWorldHint=false``).

auto_complete
-------------

Runs an exact-match (substring) auto-complete search against a vocabulary's document database.

* ``vocabulary`` (**ConceptPrefix**, required): The vocabulary to search.
* ``query`` (**string**, required): The search string. Case-insensitive; whitespace is matched as-is. Must be at least 3 characters (``BTS_AUTO_COMPLETE_MIN_LENGTH``).
* ``limit`` (**integer**, optional, default ``null``): Maximum number of results. ``null`` returns all matches.

Returns a list of `ConceptUnion` objects (JSON-serialisable concept dictionaries) sorted by relevance.
See :doc:`usage/auto-complete` for the REST API equivalent and version
differences.

search_vocabulary
-----------------

Performs hybrid lexical + semantic search over a vocabulary's concepts (see :doc:`usage/search`).

* ``vocabulary`` (**ConceptPrefix**, required): The vocabulary to search.
* ``query`` (**string**, required): The search query string.
* ``limit`` (**integer**, default ``10``): Maximum number of results.

Returns a list of `ConceptUnion` objects ranked by the hybrid search pipeline. The pipeline
can combine lexical matches, alias and definition embeddings, reciprocal rank fusion, and
reranking when the corresponding data and model are available. See :doc:`usage/search` for
the REST API equivalent.

expand_ontology
---------------

Traverses the ontology hierarchy to return descendants of specified concepts.

* ``ontology`` (**OntologyPrefix**, required): The vocabulary to expand.
* ``concept_ids`` (**list of string**, required): The concept IDs to expand, without the ``prefix:`` domain prefix (e.g. ``"0001250"``).
* ``depth`` (**integer**, optional, default ``null``): Maximum traversal depth. ``1`` returns direct children; ``2`` returns children and grandchildren; ``null`` or omitted means unlimited depth.
* ``limit`` (**integer**, optional, default ``null``): Maximum number of descendants to return per input concept.

Returns a list of :class:`~bioterms.model.related_term.RelatedTerm` objects. See :doc:`usage/expansion` for the REST API equivalent.

map_concepts
------------

Translates concept IDs from one vocabulary to another using loaded annotation paths.

* ``source_vocabulary`` (**ConceptPrefix**, required): The vocabulary of the input IDs.
* ``target_vocabulary`` (**ConceptPrefix**, required): The vocabulary to translate into.
* ``concept_ids`` (**list of string**, required): The source concept IDs.
* ``max_hops`` (**integer**, default ``1``): Maximum annotation hops. ``1`` follows only direct annotations; ``2`` allows one intermediate vocabulary; ``3`` allows two, and so on.
* ``limit`` (**integer**, optional, default ``null``): Maximum results per source concept.

Returns a list of :class:`~bioterms.model.related_term.RelatedTerm` objects. Only annotation pairs that have been loaded will produce results (see :doc:`vocabularies` for supported pairs). See :doc:`usage/mapping` for the REST API equivalent.

get_similar_concepts
--------------------

Retrieves concepts semantically similar to given inputs, based on pre-computed similarity scores (see :doc:`build-database` for calculation instructions).

* ``vocabulary`` (**ConceptPrefix**, required): The vocabulary to search within.
* ``concept_ids`` (**list of string**, required): Concept IDs to find similar concepts for.
* ``threshold`` (**float**, default ``1.0``): Minimum similarity score to include, from 0.0 to 1.0. ``1.0`` is an exact match; lower thresholds accept weaker matches. Note that the server may not have all similarity scores stored for every possible pair; a very low threshold may yield very large responses or performance degradation on vocabularies with many precomputed options.
* ``same_vocabulary`` (**bool**, default ``True``): Restrict results to the same vocabulary as input.
* ``corpus_vocabulary`` (**ConceptPrefix**, optional): Only consider scores computed with this corpus.
* ``method`` (**SimilarityMethod**, optional): Filter by similarity method. One of ``CO_ANNOTATION``, ``RELEVANCE``, or ``WEIGHED_RELEVANCE``.
* ``limit`` (**integer**, optional, default ``null``): Maximum similar concepts per input.

Returns a list of :class:`~bioterms.model.similar_term.SimilarTerm` objects. See :doc:`usage/similarity` for the REST API equivalent and :doc:`similarity-methods/index` for the mathematical background.

translate_concepts_to_constraints
---------------------------------

Translates input concept IDs to the closest matching concepts within a constrained candidate set, optionally spanning multiple vocabularies at once.

* ``vocabulary`` (**ConceptPrefix**, required): The vocabulary of the input (original) IDs.
* ``original_concepts`` (**list of string**, required): The concept IDs to translate.
* ``constraint_concepts`` (**list of string**, required): Candidate IDs. Each must be in ``"prefix:concept_id"`` format (e.g. ``"SNOMED:271000119106"``). Unlike the REST v1 API, constraints can span multiple vocabularies in a single call.
* ``threshold`` (**float**, default ``1.0``): Minimum similarity for a match.
* ``limit`` (**integer**, optional, default ``null``): Maximum translated concepts per input.

Returns a list of :class:`~bioterms.model.translated_term.TranslatedTerm` objects. See :doc:`usage/similarity` (Translating terms) for the REST API equivalent.

Resources
=========

The MCP server exposes one resource URI per vocabulary license: ``license://<prefix>`` (for example ``license://snomed``, ``license://hpo``, etc.).

These correspond to the license files in ``src/bioterms/data/licenses/``. Access them via your MCP client's ``read_resource`` method.

Suitable uses
=============

The tools support bounded terminology tasks such as finding a concept from a phrase,
walking down an ontology hierarchy, following loaded publisher mappings, or comparing a
concept with a supplied candidate set. Results depend on the datasets, annotations,
embeddings, and similarity scores loaded by the operator.

Do not treat an MCP response as clinical advice or as proof that two codes are equivalent.
``map_concepts`` follows the relationships stored in the graph, including multi-hop paths
when requested. The client remains responsible for checking relationship provenance and
whether a result is suitable for its use case.

Connecting a client
===================

Use an MCP client that supports streamable HTTP and configure its server URL as
``https://your-host/mcp``. Tool discovery returns the six tools documented above. Licence
resources are available through the client's resource-reading operation.

The MCP transport does not add its own authentication layer. If a deployment should not
be publicly readable, restrict ``/mcp`` at the reverse proxy or another trusted network
boundary.
