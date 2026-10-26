=====
Usage
=====

This section documents the public data APIs. It explains what each endpoint returns and
which API generation a client should choose.

The REST API has several generations because BTS retains compatibility with clients built
for earlier Cafe Variome services:

* **Cafe Variome V2 compatibility** predates BTS and mirrors an older PHP service. Keep it
  for existing integrations, but do not choose it for a new client.
* **Cafe Variome V3 compatibility** uses mostly batched ``POST`` requests and returns a
  complete response after the server has collected the results. Existing clients can
  continue to use it.
* **Current REST endpoints** use ``GET`` for read operations where practical. This makes
  successful responses available to a correctly configured proxy cache. Endpoints that
  return a ``StreamingResponse`` begin serialising results without first constructing the
  complete JSON array in application memory. Check each topic for its exact behaviour.

BTS also provides three interfaces that are not REST API versions:

* **GraphQL** supports client-selected fields and queries that combine related data. Its
  schema reflects the vocabularies and annotations loaded at the time it was generated.
  See :doc:`../graphql`.
* **FHIR R4** provides read-only ``CodeSystem`` lookup and validation operations. It is a
  focused compatibility surface, not a general FHIR terminology implementation. See
  :doc:`../fhir`.
* **MCP** exposes six read-only tools and vocabulary licence resources to MCP clients. See
  :doc:`../mcp-api`.

Not every version is available for every feature below; each topic lists the versions it actually has.

.. toctree::
    :maxdepth: 2
    :caption: Usage Topics:

    auto-complete
    search
    expansion
    mapping
    similarity
    trace
    data-management
    monitoring
