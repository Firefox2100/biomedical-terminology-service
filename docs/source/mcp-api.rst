MCP API Reference
=================

The MCP (Model Context Protocol) API is a specialized interface for integrating the BioMedical Terminology Service with LLMs and AI agents. Built on top of the `fastmcp <https://fastmcp.readthedocs.io/>`_ framework (which itself is built on `MCP <https://modelcontextprotocol.io/>`_), it exposes the service as a set of callable tools — structured functions with typed JSON parameters and return values — rather than HTTP endpoints. It is an alternative to the REST and GraphQL APIs, designed specifically for programmatic tool-calling by language models and agent orchestration frameworks such as LangChain, LlamaIndex, or direct MCP clients.

Connection
==========

The MCP server is mounted at ``/mcp`` inside the FastAPI application. You connect using any standard MCP transport: HTTP SSE, or raw stdio (in-process).

* **HTTP SSE** — Point your MCP client at (for example) ``http://localhost:5000/mcp/sse``.
* **HTTP** — Send MCP protocol messages to ``http://localhost:5000/mcp``.
* **Stdio / in-process** — Instantiate the ``FastMCP`` object from ``bioterms.mcp_api`` directly and run ``mcp.run()``.

No separate authentication is required at the MCP transport layer; the server shares the same document, graph, and vector database instances as the REST and GraphQL APIs. Any concept readable via REST or GraphQL is readable via the MCP tools.

Supported Vocabularies
======================

The tool parameters accept vocabulary identifiers defined in two Enums:

* **ConceptPrefix** (used by ``auto_complete``, ``search_vocabulary``, ``map_concepts``, ``get_similar_concepts`` and ``translate_concepts_to_constraints``): ``CTV3``, ``ENSEMBL``, ``GO``, ``HGNC``, ``HGNC_SYMBOL``, ``HPO``, ``MONDO``, ``NCIT``, ``OHDSI``, ``OMIM``, ``ORDO``, ``REACTOME``, ``SNOMED``, ``UBERON``, ``UNIPROT`` — all fifteen supported vocabularies are available.

* **OntologyPrefix** (used only by ``expand_ontology``):
  ``CTV3``, ``GO``, ``HPO``, ``MONDO``, ``NCIT``, ``OHDSI``, ``OMIM``, ``ORDO``, ``REACTOME``, ``SNOMED``, ``UBERON``. This is restricted to vocabularies with a traversable ontology or internal hierarchy; atomic identifier tables such as HGNC and UniProt are not included.

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

Returns a list of `ConceptUnion` objects ranked by embedding similarity. See :doc:`usage/search` for the REST API equivalent.

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
* ``threshold`` (**float**, default ``1.0``): Minimum similarity score to include (0.0–1.0). ``1.0`` is an exact match; lower thresholds accept weaker matches. Note that the server may not have all similarity scores stored for every possible pair; a very low threshold may yield very large responses or performance degradation on vocabularies with many precomputed options.
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

Use cases
=========

The MCP API is designed for integration with LLMs and AI agents. Typical use cases include:

**Clinical decision support**

An agent queries a physician's diagnosis (via auto-complete or search), expands the HPO concepts to find related OMIM diseases, then maps to SNOMED CT codes for the patient record.

**Literature analysis**

A research agent receives a list of gene names, searches HGNC for the corresponding Ensembl IDs, translates to MONDO diseases, and retrieves literature-relevant gene-disease co-annotations.

**Ontology browsing**

An agent explores a hierarchy by expanding a concept downward (expand_ontology) or mapping it upward through a chain of vocabularies (map_concepts with max_hops > 1).

**Semantic search in constrained space**

An agent takes free-text from a patient report, searches for similar concepts, and narrows results to vocabulary-specific terminology using translation to constraints.

Connecting and using the MCP API
================================

To consume the MCP API, use any MCP-compatible client library (e.g. the ``mcp`` Python SDK, ``@modelcontextprotocol/sdk`` for Node.js, or an MCP-hosting framework such as `fastmcp`).

Example — loading available MCP tools in Python:

.. code-block:: python

   from mcp import ClientSession, StdioServerParameters
   from mcp.client.stdio import stdio_client

   async def main():
       async with stdio_client(StdioServerParameters(
           command="uvicorn",
           args=["bioterms.asgi:application", "--app", "bioterms.mcp_api:mcp"]
       )) as (read, write):
           async with ClientSession(read, write) as session:
               await session.initialize()
               tools = await session.list_tools()
               print(tools)

Connecting via HTTP SSE (for web frameworks or browser-based MCP clients):

.. code-block:: bash

   curl https://your-host/mcp/sse

The MCP server will respond with JSON-RPC messages over the SSE connection.
