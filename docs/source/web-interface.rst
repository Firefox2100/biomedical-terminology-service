=================
Web interface
=================

The built-in web interface is the simplest way to explore a BTS deployment. Public pages
are server-rendered, work without a separate JavaScript application, and use assets bundled
with BTS rather than third-party content delivery networks.

Browsing vocabularies
=====================

Open ``/vocabularies`` to see the vocabulary register. The page renders from BTS's static
vocabulary configuration, then checks ``GET /api/vocabularies`` in the background to mark
which datasets are loaded. A slow or cold status cache therefore does not hold up the first
page response.

Choose a vocabulary to open ``/vocabularies/{prefix}``. This page provides:

* a server-rendered search form for that vocabulary;
* links to individual concept pages;
* known root concepts for vocabularies with a configured hierarchy; and
* a link to the vocabulary's operational status.

Search starts after three characters by default. The threshold follows
``BTS_AUTO_COMPLETE_MIN_LENGTH`` for API auto-complete, while the current web form enforces
a minimum of three characters. Results come from the document database and are limited to
20 concepts on this page. Use the search API when an application needs hybrid lexical and
vector search, multiple vocabularies, or a configurable result limit.

Concept pages
=============

``/vocabularies/{prefix}/{concept_id}`` shows the stored fields for one concept. Concept
URLs are normal server-rendered pages, so they can be linked, indexed, and opened without
first loading an application shell.

Status pages
============

``/vocabularies/{prefix}/status`` shows counts, loaded annotations, available similarity
data, download timestamps, and the licence text known to BTS. Building this report touches
the cache and graph data and can be slower than browsing concepts, especially after startup.
It is deliberately separate from the main vocabulary page.

Operator sign-in
================

Public browsing does not require an account. Accounts are created from the command line:

.. code-block:: bash

   bioterms-cli user create <username>

After signing in at ``/login``, an operator can create and revoke API keys at ``/api-keys``.
The plaintext value of a new key is displayed once. Give every key a name that identifies
its client or purpose, store the value in a secret manager, and revoke it when it is no
longer needed.

Signed-in operators can also start a cache rebuild and reload the GraphQL schema from the
vocabulary register. These actions return ``202 Accepted``. A cache rebuild requires the
Celery worker described in :doc:`installation`.

The former ``/term-browser`` URL redirects permanently to ``/vocabularies``. Existing
bookmarks continue to work, while the server-rendered vocabulary pages are now the default
browser.
