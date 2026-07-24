====
FHIR
====

The service exposes a FHIR-compatible terminology server interface, for interoperability with FHIR-based clinical systems that expect to resolve and validate codes against a standard ``CodeSystem`` API. This is a read-only interface: it exposes the already-loaded vocabularies as FHIR resources, it does not accept FHIR resources to load data (use :doc:`usage/data-management` or :doc:`build-database` for that).

Every endpoint is under ``/fhir``. Errors are returned as a FHIR ``OperationOutcome`` resource rather than a plain JSON error body, with an appropriate HTTP status code (``404`` for an unknown code system or code, ``422`` for a malformed request).

Each loaded vocabulary is exposed as one FHIR ``CodeSystem``, identified by the canonical URL ``{BTS_FHIR_CANONICAL_URL}/CodeSystem/{prefix}`` (e.g. ``https://your.deployment.com/fhir/CodeSystem/hpo``). A vocabulary that is not currently loaded has no corresponding ``CodeSystem`` and every operation against it returns ``404``.

Capability statement
======================

``GET /fhir/metadata``

Returns a FHIR ``CapabilityStatement`` describing this server: FHIR version ``4.0.1``, the ``CodeSystem`` resource with ``read``/``search-type`` interactions, and the ``$lookup``/``$validate-code`` operations documented below.

Listing code systems
======================

``GET /fhir/CodeSystem``

Returns a FHIR ``searchset`` ``Bundle`` containing one ``CodeSystem`` entry per currently loaded vocabulary.

``GET /fhir/CodeSystem/{prefix}``

Returns the single ``CodeSystem`` resource for ``{prefix}``, or ``404`` if that vocabulary is not loaded.

Looking up a code
===================

Resolves a single code into its properties (display label, definition, synonyms, and active/inactive status), corresponding to the FHIR ``CodeSystem/$lookup`` operation.

``GET /fhir/CodeSystem/$lookup``

Query parameters:

* ``system`` (required): the code system URL, i.e. ``{BTS_FHIR_CANONICAL_URL}/CodeSystem/{prefix}``.
* ``code`` (required): the concept ID to look up.
* ``property`` (optional, repeatable): which properties to include in the response (``name``, ``code``, ``system``, ``display``, ``inactive``, ``designation``, ``definition``). Omit to include all of them.

``POST /fhir/CodeSystem/$lookup`` accepts the same lookup as a FHIR ``Parameters`` request body instead of query parameters, either as a ``coding`` parameter (a ``Coding`` with ``system`` and ``code``) or as separate ``code``/``system`` parameters; providing both is a ``422`` error, providing neither is also a ``422`` error. The POST form does not support the ``property`` filter, it always returns every property.

Both forms return a FHIR ``Parameters`` resource, e.g.:

.. code-block:: json

    {
      "resourceType": "Parameters",
      "parameter": [
        {"name": "name", "valueString": "hpo"},
        {"name": "code", "valueCode": "0001250"},
        {"name": "system", "valueUri": "https://your.deployment.com/fhir/CodeSystem/hpo"},
        {"name": "display", "valueString": "Narrow face"},
        {"name": "property", "part": [
          {"name": "code", "valueCode": "inactive"},
          {"name": "value", "valueBoolean": false}
        ]},
        {"name": "designation", "part": [{"name": "value", "valueString": "Decreased width of face"}]}
      ]
    }

Validating a code
====================

Checks whether a code exists in a code system, corresponding to the FHIR ``CodeSystem/$validate-code`` operation.

``GET /fhir/CodeSystem/$validate-code``

Query parameters:

* ``system`` (required): the code system URL, as above.
* ``code`` (required): the concept ID to validate.

``POST /fhir/CodeSystem/$validate-code`` accepts the same ``coding`` or ``code``/``system`` ``Parameters`` body pattern as the POST form of ``$lookup``.

Both forms return a FHIR ``Parameters`` resource with a ``result`` boolean. On a successful match:

.. code-block:: json

    {
      "resourceType": "Parameters",
      "parameter": [
        {"name": "result", "valueBoolean": true},
        {"name": "system", "valueUri": "https://your.deployment.com/fhir/CodeSystem/hpo"},
        {"name": "code", "valueCode": "0001250"},
        {"name": "display", "valueString": "Narrow face"}
      ]
    }

When the code is not found, ``result`` is ``false`` and the response additionally includes ``message`` and an ``issues`` parameter carrying an embedded ``OperationOutcome`` describing the failure, rather than a top-level HTTP error, since the code system itself was valid.
