============================================
BioMedical Terminology Service Documentation
============================================

The BioMedical Terminology Service (BTS) is a self-hosted system for working with
biomedical vocabularies through one web interface and a consistent set of APIs. It can
search concepts, traverse hierarchies, follow mappings between vocabularies, and compare
concepts with precomputed similarity measures.

BTS keeps document, graph, and vector storage behind separate interfaces. A deployment can
therefore choose the supported database combination that fits its data and operating
environment. REST, GraphQL, FHIR R4, and MCP interfaces expose the same loaded vocabulary
data for different clients.

Start here
==========

* To browse concepts, begin with :doc:`web-interface`.
* To run a server, follow :doc:`installation` and then :doc:`build-database`.
* To integrate an application, see :doc:`usage/index`, :doc:`graphql`, :doc:`fhir`, or
  :doc:`mcp-api`.
* To understand which datasets and mappings BTS knows how to load, see
  :doc:`vocabularies`.

Terminology datasets are not bundled with BTS. Operators are responsible for obtaining
them and complying with their licences. BTS is infrastructure for terminology access and
research; it does not by itself validate a terminology for clinical use.

.. toctree::
    :maxdepth: 2
    :caption: Contents:

    about
    web-interface
    installation
    build-database
    vocabularies
    usage/index
    similarity-methods/index
    graphql
    fhir
    mcp-api
