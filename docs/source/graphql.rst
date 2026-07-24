=======
GraphQL
=======

In addition to the REST API described under :doc:`usage/index`, the service exposes a GraphQL endpoint that provides the same underlying data with fine-grained, client-selected field access. It is best suited to applications that need to combine several pieces of information (a concept, its parents, and its cross-vocabulary annotations, for example) in a single request without over-fetching.

Endpoint
========

``POST /api/graphql`` accepts GraphQL queries. ``GET /api/graphql`` in a browser serves the GraphiQL explorer, which is useful for interactively browsing the schema and trying queries during development.

Schema availability
====================

The schema is assembled dynamically from whichever vocabularies and annotations are currently loaded (see :doc:`build-database`): a vocabulary that has not been loaded does not appear in the schema at all, rather than appearing with empty results. Because of this, the schema changes whenever data is loaded or deleted. The running service does not pick this up automatically; either send an authenticated ``POST /reload-graphql`` request or restart the service, as described in :doc:`installation`.

Schema structure
=================

The root ``Query`` type always exposes:

.. code-block:: text

    type Query {
        loadedPrefixes: [ConceptPrefix!]!
    }

and gains one additional field per loaded vocabulary, named after its prefix, e.g. ``hpo``, ``mondo``, ``snomed``. Each of these resolves to a per-vocabulary query type with three operations:

.. code-block:: text

    type HpoQuery {
        hpoConcept(conceptId: String!): HpoConceptResponse
        autoComplete(query: String!, limit: Int): HpoConceptListResponse
        search(query: String!, limit: Int): HpoConceptListResponse
    }

``autoComplete`` and ``search`` correspond to the same features described in :doc:`usage/auto-complete` and :doc:`usage/search` respectively. All three operations return an envelope with ``data`` and ``error`` fields, e.g. ``HpoConceptResponse { data: HpoConcept, error: QueryError }``, rather than raising a transport-level error, so a client should always check ``error`` before using ``data``.

Every vocabulary's concept type (``HpoConcept``, ``MondoConcept``, and so on) implements a shared ``Concept`` interface (``prefix``, ``conceptId``, ``label``, ``status``) plus, for hierarchical vocabularies, an ``OntologyConcept`` interface (``children``, ``parents``). Every concept type also exposes two fields that cut across the REST API's endpoint groups:

* ``similarConcepts(threshold: Float)``, corresponding to :doc:`usage/similarity`.
* ``pathsTo(targetPrefix, targetConceptId, relationship, direction, maxDepth)``, corresponding to :doc:`usage/trace`.

Reactome is a special case: instead of one concept type, it exposes a ``ReactomeConcept`` interface implemented by ``ReactomePathway``, ``ReactomeReaction``, and ``ReactomeGene``, with pathway/reaction-specific traversal fields (``subPathways``, ``superPathways``, ``reactions``) in addition to the common ones.

Cross-vocabulary annotations
=============================

When an annotation pair is loaded between two vocabularies (see :doc:`vocabularies`), both concept types are extended with a field for traversing directly to the other vocabulary's annotated concepts, without a separate :doc:`usage/mapping` call:

.. code-block:: text

    extend type HpoConcept {
        annotatedOrdo: [OrdoConcept!]!
    }

    extend type OrdoConcept {
        annotatedHpo: [HpoConcept!]!
    }

The field name follows the pattern ``annotated<OtherVocabulary>``.

Example query
==============

Fetching a concept's label, its parents, and its annotated ORDO concepts in one request (assuming HPO, ORDO, and the HPO-ORDO annotation are all loaded):

.. code-block:: graphql

    query {
      hpo {
        hpoConcept(conceptId: "0001250") {
          error { code message }
          data {
            label
            parents { conceptId label }
            annotatedOrdo { conceptId label }
          }
        }
      }
    }
