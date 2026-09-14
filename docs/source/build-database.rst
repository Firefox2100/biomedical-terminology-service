=====================
Constructing Database
=====================

This guild provides instructions on how to build the biomedical terminology database used by the software. Due to the resource-intensive nature of this process, it is recommended to perform the database construction on a separate machine, preferably one with high-performance computing capabilities and a large amount of RAM. Once the database is built, it can be transferred to the server where the software will be deployed.

Setting up Databases
--------------------

Before constructing the database, the appropriate database system must be set up. The software supports multiple database systems, following are the configurations needed for each supported database.

MongoDB
^^^^^^^

The recommended database for storing document data is MongoDB. Follow the official `MongoDB installation guide <https://www.mongodb.com/docs/manual/installation/>`_ to set up MongoDB on your machine. Ensure that the MongoDB server is running before proceeding with the database construction. There's no specific configurations needed for MongoDB, but the WireTiger cache may need to be adjusted based on the available RAM.

SQL (alternative to MongoDB)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As an alternative to MongoDB, the document database can be backed by a SQL database instead, via SQLAlchemy's async engine. **PostgreSQL is the primary/recommended SQL backend** - it gets a native, tested ``ON CONFLICT DO UPDATE`` upsert path and its own dedicated bundle. MySQL/MariaDB and SQLite are also supported (MySQL via its own native ``ON DUPLICATE KEY UPDATE`` upsert; SQLite via its own native ``ON CONFLICT DO UPDATE``, same as PostgreSQL), and any other SQLAlchemy-async-compatible dialect works too through a portable, slower per-row insert-then-update fallback. Concepts are stored one JSON payload column per row (``JSONB`` on PostgreSQL, ``JSON`` elsewhere), alongside plain indexed columns for ``concept_id``, ``label`` and ``search_text`` that back auto-complete and lexical search scoring without needing dialect-specific JSON-patching.

Set ``BTS_DOC_DATABASE_DRIVER=sql`` and ``BTS_SQL_DB_URL`` to a SQLAlchemy async URL to enable it, e.g. ``postgresql+asyncpg://user:password@host:5432/bts``. For PostgreSQL, install the ``postgres`` extra (``pip install .[postgres]``), which bundles the ``asyncpg`` driver - no separate driver package to track down. For MySQL/MariaDB or SQLite, install the plain ``sql`` extra (``pip install .[sql]``) plus an async driver package this project does not bundle, e.g. ``aiomysql``/``asyncmy`` for MySQL/MariaDB or ``aiosqlite`` for SQLite. SQLite is convenient for local development and small deployments but is not recommended for the concurrent write load of a full database build.

Auto-complete search indexing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The auto-complete endpoints (``/api/*/auto-complete``, and the equivalent GraphQL/MCP/FHIR paths) need to find every concept whose label, synonyms, or ID contain each word of the query as a substring - not just as a prefix. The original implementation of this (still used as the fallback below) pre-computes every substring from 3 to 20 characters of each word in a concept's label/synonyms (``Concept.n_grams()``) and stores each one as its own row/array entry, so a query word can be matched with a plain equality lookup. This is simple and portable, but for a vocabulary the size this service targets (SNOMED-scale and up), materialising every substring of every word is a large multiple of the underlying text in extra storage and write I/O, on both the document database and (for MongoDB) the collection itself.

Since this was first built (against plain MongoDB), both of the document database backends have gained a built-in way to do the same kind of substring indexing natively, without exploding the data into a side table/field. Each document database driver now **probes, once per process, whether its connected deployment actually has that native capability available** (a driver being selected does not by itself guarantee the capability - e.g. plain community MongoDB without the ``mongodb-search`` Compose profile, or a PostgreSQL user without permission to install extensions) and uses it when present, falling back to the original n-gram approach otherwise. This choice is cached for the life of the process; if you enable/disable the native capability on a live deployment (e.g. install ``pg_trgm`` after the fact), restart the service to pick it up.

Because the underlying engines don't expose comparable relevance scoring, results are **not** guaranteed to come back in the exact same order across backends - every mode still guarantees an exact substring match per query word (AND across words), and orders ties by shorter label first then concept ID, but how "more relevant first" is approximated differs:

- **MongoDB, native** (Atlas Search/mongot support detected - see the MongoDB section above): an Atlas Search index of type ``autocomplete`` (``tokenization: nGram``, ``minGrams``/``maxGrams`` matching ``Concept.n_grams()``'s own 3-20 range) is created directly on the ``conceptId``/``label``/``synonyms`` document fields (name configurable via ``BTS_MONGODB_TEXT_INDEX_NAME``). No ``nGrams``/``searchText`` field is written to documents at all. Queried via a ``$search`` ``compound.must`` of one ``autocomplete`` clause per query word; ranked by mongot's own relevance score.
- **MongoDB, fallback** (no Atlas Search/mongot): unchanged from the original implementation - the ``nGrams`` array field plus a plain index, matched via ``$all`` and ranked by the query's byte offset within a precomputed ``searchText`` field.
- **PostgreSQL, native**: the `pg_trgm <https://www.postgresql.org/docs/current/pgtrgm.html>`_ extension (enabled automatically via ``CREATE EXTENSION IF NOT EXISTS pg_trgm`` if the connection has privileges to do so) backs a GIN trigram index on the existing ``search_text`` column - no separate n-gram table. Queried with ``ILIKE '%word%'`` per word, index-accelerated by the trigram index for the expensive substring filtering; ranked with the same position-based (``strpos``) score as the fallback path, since trigram ``similarity()`` scores whole-string trigram overlap rather than reliably preferring an earlier/more exact match of the query itself.
- **MySQL, native** (not MariaDB - see below): a ``FULLTEXT ... WITH PARSER ngram`` index on ``search_text``, using MySQL's built-in ngram full-text parser plugin. Queried with ``MATCH ... AGAINST (... IN BOOLEAN MODE)``, requiring every query word (``+word``); the same boolean-mode match score is reused for ranking. The parser's own ``ngram_token_size`` server variable (default 2) controls its internal n-gram length, independent of and coarser than ``Concept.n_grams()``'s 3-20 range - this does not affect correctness, only how much of the index tokenises finer than the words being searched for.
- **SQLite, native**: an `FTS5 virtual table using the built-in trigram tokenizer <https://www.sqlite.org/fts5.html#the_trigram_tokenizer>`_ (SQLite >= 3.34.0), mirrored by hand alongside the concept row on every write (FTS5 has no native upsert/trigger sync). Queried with one ``MATCH`` per word intersected together, index-accelerated the same way as PostgreSQL's trigram index; FTS5 does not expose a relevance score meaningful across an intersection of independent trigram matches, so ranking falls back to the same position-based (``instr``) score, computed over the already-small matched set rather than the full table.
- **MariaDB, and any other SQL dialect**: no native trigram/n-gram full-text capability is probed for, so these always fall back to the portable n-gram table, identically to the original implementation.

The n-gram table/field is still always generated for **offline** vocabulary loading (``--offline``, see below) regardless of native support, since the resulting dump file may be imported into a database backend/version where native support isn't available.

Neo4j
^^^^^

The recommended graph database for storing graph data is Neo4j. Follow the official `Neo4j installation guide <https://neo4j.com/docs/operations-manual/current/installation/>`_ to set up Neo4j on your machine. Ensure that the Neo4j server is running before proceeding with the database construction.

This service utilises the ``APOC`` and ``graph-data-science`` plugins for advanced graph operations, both in building and querying the graph database. Make sure to install these plugins in your Neo4j instance. If using docker, this can be configured via environment variables; otherwise you would need to download the matching release of APOC and GDS plugins from their repositories and place them in the ``plugins`` folder of your Neo4j installation.

PostgreSQL (alternative to Neo4j)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As an alternative to Neo4j, the graph database can be backed by PostgreSQL instead. Set ``BTS_GRAPH_DATABASE_DRIVER=postgresql`` and ``BTS_POSTGRES_GRAPH_DB_URL`` to enable it; requires the ``postgres`` extra (``pip install .[postgres]``). This is a plain relational implementation, not PostgreSQL's own native property-graph query feature (SQL/PGQ, the ``GRAPH_TABLE``/``CREATE PROPERTY GRAPH`` syntax from SQL:2023): that only shipped in PostgreSQL 19, which was still in beta when this driver was written, and its first version only supports fixed-depth pattern matching - it cannot express the unbounded ancestor/descendant traversal or shortest-path queries this service needs. No extension (e.g. Apache AGE) is used either; everything is ordinary tables, indexes, and recursive CTEs, which works on any current PostgreSQL.

Schema, briefly (see the driver's module docstring, ``src/bioterms/database/graph_db/postgres_graph_db.py``, for the exact DDL):

* One set of ``graph_node_<prefix>`` / ``graph_edge_<prefix>`` / ``graph_closure_<prefix>`` tables per vocabulary, keeping each vocabulary's own indexes small even at SNOMED/OHDSI scale (~1M and ~10M concepts respectively). ``graph_closure_<prefix>`` is a precomputed transitive closure over each vocabulary's ``is_a``/``part_of`` edges - the two hottest graph operations, ancestor and descendant lookup, become an indexed read against this table instead of a traversal, at the cost of needing a (re)build step. ``create_index()`` (called automatically as part of ``bioterms-cli vocabulary load``) rebuilds the closure table for every vocabulary prefix that currently has nodes; re-run a vocabulary load (or call it directly) again after any change to that vocabulary's hierarchy edges. OHDSI's ~150 source sub-vocabularies are not further segmented here, since the OHDSI vocabulary loader does not currently capture which sub-vocabulary each concept came from - a reasonable follow-up if OHDSI's own tables become a bottleneck.

  Materialisation only goes ``BTS_POSTGRES_GRAPH_CLOSURE_DEPTH`` hops deep (default 5) rather than to full closure: on a densely polyhierarchical vocabulary like OHDSI (built from those ~150 source vocabularies), the full transitive closure can run into the hundreds of GB, while almost every real ancestor/descendant query only needs a handful of hops. A one-row-per-prefix ``graph_closure_status`` table records whether a given prefix's closure build actually reached natural exhaustion within that depth (in which case the table already holds the complete answer) or was cut off by the cap. A lookup within the materialised depth, or against a prefix whose closure is known-complete, is served straight from the indexed table; anything deeper or unbounded on a cut-off prefix instead runs a live, per-query bounded traversal over ``graph_edge_<prefix>`` (scoped to just the requested concept IDs, so still far cheaper than a full closure rebuild), capped by the hard safety ceiling ``BTS_POSTGRES_GRAPH_CLOSURE_MAX_DEPTH`` (default 500) when the request itself is unbounded. Lower ``BTS_POSTGRES_GRAPH_CLOSURE_DEPTH`` further on a deployment where even the default is too large; this trades slower rare deep/unbounded queries for a smaller closure table.

  Every ``graph_node_<prefix>`` table also carries the same ``GRAPH_NODE_EXTRA_PROPERTIES`` columns as Neo4j (``source_vocabulary_id``, ``reviewed``, ``organism_tax_id``, ``organism_name``, each indexed) - see the Neo4j section above for what they're for. ``_ensure_prefix_schema`` adds any missing ones via ``ALTER TABLE ... ADD COLUMN IF NOT EXISTS`` on every call, so an existing deployment's tables catch up automatically on the next vocabulary load; no manual migration needed.
* One ``graph_annotation`` table for all cross-vocabulary annotation edges and one ``graph_similarity`` table for all similarity scores, each partitioned by source prefix (`PostgreSQL declarative partitioning <https://www.postgresql.org/docs/current/ddl-partitioning.html>`_) rather than split into one table per prefix pair - both need multi-hop, prefix-crossing traversal (``map_terms``) or cross-prefix lookups (``get_similar_terms`` with ``same_prefix=False``, ``translate_terms``) that are only really expressible as a single recursive CTE/query over one table; partitioning still gives most of the per-prefix segmentation benefit.

Like the MongoDB and PostgreSQL vector store options, ``BTS_POSTGRES_GRAPH_DB_URL`` is independent of ``BTS_SQL_DB_URL``/``BTS_POSTGRES_VECTOR_DB_URL`` but can safely be set to the exact same value - graph tables live under their own ``graph_*`` names, so the document store's ``concept_*`` tables and the vector store's columns/tables never collide with them. Setting all three to the same PostgreSQL instance is how to run the document, vector, and graph stores - everything except Redis - on one PostgreSQL server.

Qdrant
^^^^^^

Qdrant only provides a docker image officially. However, it's also possible to compile the source code directly, allowing it to be run on machines without docker support. If compiling from source, the main repository does not come with the web UI, and it must be downloaded separately and placed next to the compiled binary, if you want to use the web UI for management.

MongoDB (alternative to Qdrant)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As an alternative to Qdrant, the vector database can be backed by MongoDB instead, using its ``$vectorSearch`` aggregation stage (powered by ``mongot``, MongoDB's Lucene-based search engine). Set ``BTS_VECTOR_DATABASE_DRIVER=mongodb`` to enable it. Each vocabulary prefix gets its own dedicated ``<prefix>.vectors`` collection, one document per embedding item (a concept's label, a single synonym, or its definition - see "Embedding the concepts" below), rather than per concept, in the same MongoDB database configured via ``BTS_MONGODB_*``. Because of this, it requires a MongoDB deployment with Search support - a plain ``mongod`` without ``mongot`` cannot serve ``$vectorSearch`` queries. The vector search index is filterable on an item's ``kind`` (alias vs. definition), so the two are searched independently; its name and the query candidate pool size can be tuned with ``BTS_MONGODB_VECTOR_INDEX_NAME`` and ``BTS_MONGODB_VECTOR_NUM_CANDIDATES_MULTIPLIER``. This driver works regardless of which document database backs concept storage.

Recommended: **MongoDB Community Server + MongoDB Community Search**, both licensed under the source-available `Server Side Public License (SSPL) <https://www.mongodb.com/legal/licensing/server-side-public-license>`_ - free to self-host, with no Atlas subscription or per-node licensing involved. ``mongot`` requires a replica set (even a single-member one) and authenticates to ``mongod`` as a dedicated user with the ``searchCoordinator`` role; see the `self-managed MongoDB Search docs <https://www.mongodb.com/docs/search/self-managed/current/>`_ for the full deployment guide. The bundled compose files wire this up for local development behind an opt-in ``mongodb-search`` `Compose profile <https://docs.docker.com/compose/how-tos/profiles/>`_, since it is not needed unless you choose the MongoDB vector driver:

.. code-block:: bash

    docker compose -f scripts/docker-compose.dependencies.yaml --profile mongodb-search up

This starts ``mongodb-search`` (mongod, replica set ``rs0``) and ``mongot`` alongside the usual dependencies, exposing MongoDB on ``localhost:8907``. Point ``BTS_MONGODB_HOST``/``BTS_MONGODB_PORT`` at it (``localhost``/``8907``) and set ``BTS_MONGODB_USERNAME=root``, ``BTS_MONGODB_PASSWORD=rootpass``, ``BTS_MONGODB_AUTH_SOURCE=admin`` (or edit the compose file's placeholder credentials first). The equivalent profile also exists in the top-level ``docker-compose.yaml`` for full-stack deployments.

Alternatively, MongoDB Atlas (fully managed) or a self-managed Enterprise Server deployment also support ``$vectorSearch`` and work as drop-in replacements - only the connection settings differ, not the driver code. The ``mongodb/mongodb-atlas-local`` docker image is another option for quick local evaluation, but it is licensed only for local Atlas emulation/testing rather than as a general self-hosted deployment, so it is not used by the bundled compose files.

PostgreSQL/pgvector (alternative to Qdrant)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As a second alternative to Qdrant, the vector database can be backed by PostgreSQL instead, via the `pgvector <https://github.com/pgvector/pgvector>`_ extension. Set ``BTS_VECTOR_DATABASE_DRIVER=postgresql`` to enable it; requires the ``postgres`` extra (``pip install .[postgres]``), which bundles both ``asyncpg`` and the ``pgvector`` Python package. This is the natural choice if you are **already using PostgreSQL for the document database** (``BTS_DOC_DATABASE_DRIVER=sql`` with a PostgreSQL ``BTS_SQL_DB_URL``, see the SQL section above) and would rather not run a second database system just for vectors.

Connection is configured separately via ``BTS_POSTGRES_VECTOR_DB_URL`` (defaults to ``postgresql+asyncpg://localhost:5432/bts``), so the vector store does not have to be the same PostgreSQL instance as the document store. Each vocabulary prefix gets its own dedicated ``concept_<prefix>_vector_item`` table (``item_id``, ``concept_id``, ``kind``, ``text``, ``vector``) - one row per embedding item (a concept's label, a single synonym, or its definition; see "Embedding the concepts" below), not per concept. This table lives alongside whatever the document database maintains, so it works identically whether ``BTS_POSTGRES_VECTOR_DB_URL`` points at a dedicated instance or the exact same PostgreSQL instance/database as ``BTS_SQL_DB_URL``/``BTS_POSTGRES_GRAPH_DB_URL`` - setting all three to the same value is a fully supported way to run the document, vector, and graph stores on one PostgreSQL server.

The extension (``CREATE EXTENSION IF NOT EXISTS vector``) and an HNSW cosine-distance index (plus plain indexes on ``concept_id`` and ``kind``) are created automatically the first time embeddings are written or searched; this requires pgvector **0.5.0 or later** on the server (HNSW support). The bundled compose files provide a ready-to-use PostgreSQL+pgvector image behind an opt-in ``postgres`` `Compose profile <https://docs.docker.com/compose/how-tos/profiles/>`_:

.. code-block:: bash

    docker compose -f scripts/docker-compose.dependencies.yaml --profile postgres up

This exposes PostgreSQL on ``localhost:8911`` with user/password/database all ``bts``/``password``/``bts`` (edit the compose file's placeholder credentials before using it for anything beyond local development). Point both ``BTS_SQL_DB_URL`` and ``BTS_POSTGRES_VECTOR_DB_URL`` at ``postgresql+asyncpg://bts:password@localhost:8911/bts`` to use it as a combined document+vector store, or only one of the two settings to use it for just that role. The image (``pgvector/pgvector``) is a normal PostgreSQL image with the extension pre-installed, so it works equally well as a plain document-database-only PostgreSQL if you only need ``BTS_DOC_DATABASE_DRIVER=sql``. The equivalent profile also exists in the top-level ``docker-compose.yaml`` for full-stack deployments.

Redis
^^^^^

Redis is used as a caching layer to improve performance. Follow the official `Redis installation guide <https://redis.io/docs/getting-started/installation/>`_ to set up Redis on your machine. Ensure that the Redis server is running before proceeding with the database construction.

Building the Database
---------------------

This process is best performed on a separate machine with sufficient resources. Depending on the dataset used, this process may take up to 40GB RAM, and over 100GB disk space. Once the database is built, it can be transferred to the server where the software will be deployed. This software provides a command line tool to facilitate the database construction. It should be installed when doing ``pip install .``, and can be accessed as ``bioterms-cli``. It's written with ``Typer``, so you can run ``bioterms-cli --help`` to see the available commands and options.

Downloading the vocabulary
^^^^^^^^^^^^^^^^^^^^^^^^^^

The CLI provides a command to download the supported vocabularies automatically, except for several (explained below). To download the vocabularies, run:

.. code-block:: bash

    bioterms-cli vocabulary download <the-vocabulary-id>

Some vocabularies require an API key to download. The supported credentials are:

* NHS TRUD API key for CTV3 and SNOMED CT. You need to subscribe to these vocabularies and wait for them to approve the subscription, before the API key can be used to download the files. SNOMED's download also includes its historical Association Reference Set files (SAME_AS/REPLACED_BY/WAS_A/POSSIBLY_EQUIVALENT_TO/etc, loaded as ``snomed_association`` relationships distinguished by SNOMED's own numeric ``refsetId``) - no separate credential or step needed, but if you downloaded SNOMED before this was added, re-run ``vocabulary download snomed --redownload`` to pick them up.
* BioPortal API key for OMIM and ORDO
* NIH UMLS API key for SNOMED-ORDO mapping files

And not all vocabularies can be downloaded this way. Particularly:

* Reactome releases only a Neo4j dump and a SQL dump. They are both complicated to read from plain Python without restoring them into a database first. Therefore, Reactome must be loaded into a Neo4j 4 (note that we use Neo4j 5 for this service, so you may need to install Neo4j 4 separately) instance first, and use the provided script ``scripts/dump_reactome_to_csv.py`` to export the data to CSV files that can be imported into the main database. Reactome's own protein identity is expressed as ``EXACT`` annotations to UniProt (see below), not as a resolved HGNC symbol directly - it does not need UniProt loaded first for correctness (annotation targets are created as bare stub nodes if missing), but loading UniProt first gives those nodes their full properties immediately instead of on UniProt's next load.
* OHDSI standardized vocabularies are not open for public download, and provides no download API. You need to manually download the latest release from Athena, and unzip it to the data folder.
* UMLS system provides no way to fetch the latest release files automatically, so the files downloaded from UMLS are using hard-coded URL. If you need a different version, you need to manually download the files from UMLS and place them in the data folder, or open an issue/pull request to notify us of the desired version.
* UniProt requires no credential and no other vocabulary downloaded first, but it is the **complete** UniProtKB release (Swiss-Prot + TrEMBL, every organism) rather than a subset scoped to any other vocabulary's needs - a partial UniProt cannot be claimed as "supported." Expect it to dominate both download time and disk usage: TrEMBL alone is on the order of 100GB compressed at the time of writing. Both files are kept gzip-compressed on disk and streamed/decompressed on the fly while loading, so disk usage stays close to the download size rather than growing several times larger. Loading (both online and ``--offline``) is fully batched and streamed - memory stays bounded regardless of total release size - but budget real wall-clock time for TrEMBL specifically; parsing Swiss-Prot alone (~575k entries) takes on the order of a minute or two. Organism is not filtered at load time: every entry's NCBI taxonomy ID and organism name are stamped as the ``organismTaxId``/``organismName`` node properties instead (indexed - see below), so scoping to e.g. human (``organismTaxId = '9606'``) is a query-time filter, not a permanent restriction on what was loaded.

Loading the vocabulary
^^^^^^^^^^^^^^^^^^^^^^

The vocabularies can be loaded into the document database and graph database using the following command:

.. code-block:: bash

    bioterms-cli vocabulary load <the-vocabulary-id>

This reads the vocabulary files from the data folder, processes them and holds them in a memory list for concepts, and a memory graph for relationships. If the vocabulary is large, this may take a significant amount of RAM. After processing, the concepts and relationships are written to the document database and graph database respectively. Depending on the size of the vocabulary, this may take a long time. The indices are automatically created during this process.

Additionally, this command supports a ``--offline`` flag, which allows the database to be built without an actual database connection. The results will be written into dump files in the offline directory, which can later be imported into the database using the database import tools provided by the respective database systems. This is useful when building on systems like HPC, which are optimised for computation but not for disk I/O operations, and may have trouble running database processes.

Vocabulary load order
^^^^^^^^^^^^^^^^^^^^^

``bioterms-cli vocabulary load --all`` does **not** load in dependency order - it iterates ``ConceptPrefix`` in its declared enum order, which loads Ensembl before HGNC_SYMBOL and will fail ``ensure_gene_symbol_loaded()``'s check. Load vocabularies individually, in this order:

#. ``hgnc_symbol`` first, always - HGNC, Ensembl, and UniProt all annotate into it and refuse to load without it (``VocabularyNotLoaded``) outside ``--offline`` mode.
#. ``hgnc``, ``ctv3``, ``snomed``, ``hpo``, ``mondo``, ``ncit``, ``omim``, ``ordo``, ``ohdsi`` - independent of each other and of step 1's ordering constraint, any order among these is fine.
#. ``ensembl`` - requires ``hgnc_symbol`` from step 1.
#. ``uniprot`` - requires ``hgnc_symbol`` from step 1. No hard ordering constraint versus Reactome (see the download note above), but loading it before Reactome means Reactome's annotations land on fully-populated nodes immediately.
#. ``reactome`` - see the Reactome download note above for its own two-step (dump-then-CSV) process.

The Read v2 migration overlay (below) is a separate script, not part of this load order, but expects ``ohdsi``, ``ctv3``, and ``snomed`` to already be loaded for a clean result.

Read v2 migration overlay
^^^^^^^^^^^^^^^^^^^^^^^^^

Read v2 is a retired vocabulary that cannot be licensed or downloaded on its own - OHDSI's ``ohdsi`` vocabulary load already includes its ``read`` sub-vocabulary content, but those nodes have no path at all to CTV3 or SNOMED (degree exactly 1, connected only to their own OHDSI standard-concept mapping). NHS's data migration package (TRUD item 9) is still downloadable, though, and contains "Clinically Assured" mapping tables that this project loads as a transient overlay: ``ANNOTATED_WITH`` annotations, tagged ``source: nhs_read_v2_migration_29.0.0``, directly onto the existing OHDSI/CTV3/SNOMED nodes.

This is **not** part of ``bioterms-cli vocabulary download``/``load`` - it is not registered as a vocabulary, has no ``ConceptPrefix`` of its own, and will not be picked up by ``--all``. Run it directly instead, using the same ``BTS_NHS_TRUD_API_KEY`` as CTV3/SNOMED:

.. code-block:: bash

    python scripts/load_read_v2_migration.py

Pass ``--skip-download`` to reuse already-downloaded files under ``data/read_v2_migration/``, or ``--download-only`` to fetch without loading. The script writes directly to the live graph database (no ``--offline`` mode) via the same ``save_annotations`` path as the rest of the service.

Loading the annotations
^^^^^^^^^^^^^^^^^^^^^^^

After multiple vocabularies are loaded, the annotations between them can be loaded using the following command:

.. code-block:: bash

    bioterms-cli annotation download <vocabulary> <another-vocabulary>
    bioterms-cli annotation load <vocabulary> <another-vocabulary>

The order of the vocabulary arguments does not matter. This downloads the mapping (mind the API keys), and loads them into the graph database as relationships between the concepts from the two vocabularies. See :doc:`vocabularies` for the full list of supported annotation pairs and their sources.

The status of a vocabulary or an annotation pair, including whether it is currently loaded and its concept/relationship counts, can be checked at any time:

.. code-block:: bash

    bioterms-cli vocabulary status [<the-vocabulary-id>]
    bioterms-cli annotation status [<vocabulary> <another-vocabulary>]

Omitting the vocabulary/prefix arguments reports the status of every vocabulary or annotation pair.

Embedding the concepts
^^^^^^^^^^^^^^^^^^^^^^

Concept embeddings are used for semantic search on concepts (``/search`` and its GraphQL/MCP equivalents). They are not strictly necessary for the software to function, but highly recommended for an improved user experience. To embed the concepts, run the following command:

.. code-block:: bash

    bioterms-cli vocabulary embed <the-vocabulary-id>

This uses the ``sentence-transformers`` library (default model ``FremyCompany/BioLORD-2023``, adjustable via ``BTS_TRANSFORMER_MODEL_NAME``, but must not be changed once any embedding is stored for a vocabulary - using a different model for different vocabularies is not allowed, and will cause search to silently mix incompatible vector spaces) to generate embeddings for the concepts in the specified vocabulary. Rather than embedding one large string per concept, each concept is broken down into individual **embedding items**, each embedded and stored separately: one per distinct label/synonym string (an "alias" item), plus one for the definition if the concept has one. Search then retrieves against alias items and definition items as two independent recall arms (see the search section below), so a short exact synonym is never diluted by an unrelated definition sentence in the same vector.

This operation also supports the ``--offline`` flag, which writes the embeddings to a special binary dump format (one row per embedding item). This must be imported into the vector database using the CLI itself, via

.. code-block:: bash

    bioterms-cli vocabulary embed <the-vocabulary-id> --restore

Because the binary format is specific to this software. This is necessary to preserve accuracy of the vector and compress efficiently.

Calculating semantic similarities
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This is one of the core features this service offers. Semantic similarities have first-class support in the query engine, and can be used in various scenarios. Multiple different similarity metrics are supported, and can be calculated using the following command:

.. code-block:: bash

    bioterms-cli similarity calculate --target <the-vocabulary-id> --corpus <corpus-vocabulary-id> --method <similarity-method> --threshold <similarity-threshold>

The target is the vocabulary for which the similarities are calculated, and the corpus is the vocabulary against which the similarities are calculated. The corpus may not be required if the metric is intrinsic, in which case it can be omitted. If the corpus is required but omitted, the software understands this as all possible corpus should be used, and it will calculate multiple metrics for different corpus. Similarly, if no method is specified, it will use all methods that can be applied to this vocabulary. The threshold is used to filter out low-similarity results, and can be adjusted based on the desired sensitivity. Because most similarity pairs will have a low score, we recommend setting it to at least 0.2 to get meaningful results, and possibly over 0.7 to get high-confidence results. This operation also supports the ``--offline`` flag, which writes the similarity results to a CSV file in the offline directory. When running offline, ``--annotation-file`` can override the annotation dump used for the target/corpus pair, in case it is not in its default location.

The supported similarity methods are:

* Co-annotation vectors: calculates similarity based on shared annotations between concepts. See :doc:`similarity-methods/co-annotation`.
* Relevance method: calculates similarity based on relevance scores using Information Content (IC) metrics. See :doc:`similarity-methods/relevance`.
* Weighed relevance method: similar to relevance method, assign weights to corpus annotation contribution. See :doc:`similarity-methods/weighed-relevance`.

Because the similarity scores are calculated pair-wise, and for some methods it's not possible to estimate the score without actually calculating it, the workload will grow exponentially with the size of the vocabulary. For example, SNOMED CT has over 1 million concepts with the extensions loaded. Even for just 1 million concepts, there will be almost 500 billion pairs to calculate. For OHDSI which has over 9 million concepts, it's over 40 trillion. This is not practical even for HPC systems. We recommend NOT to calculate similarity for these large vocabularies, and rely on mappings to smaller vocabularies instead.

Deleting a vocabulary
^^^^^^^^^^^^^^^^^^^^^

This service does not support incremental updates of the vocabularies, because it cannot understand the differences between versions. Therefore, to update a vocabulary, it must be deleted first, and then re-loaded. To delete a vocabulary, run the following command:

.. code-block:: bash

    bioterms-cli vocabulary delete <the-vocabulary-id>

Managing the cache
^^^^^^^^^^^^^^^^^^

Vocabulary, annotation, and similarity status are cached to keep repeated status queries fast. The cache is invalidated automatically whenever a vocabulary or annotation is loaded or deleted through the CLI, so this is normally only needed after data is modified out of band, such as after restoring an offline dump directly with database import tools:

.. code-block:: bash

    bioterms-cli cache purge
    bioterms-cli cache rebuild

``purge`` clears all cached status entries so they are recomputed the next time they are requested. ``rebuild`` walks every vocabulary, annotation, and similarity combination and recomputes their cached status immediately; this is the same operation the Celery worker performs when triggered from the web UI's "Rebuild Cache" action.

Loading offline files
^^^^^^^^^^^^^^^^^^^^^

Because some offline files need database tools to import, we provide example queries and commands to facilitate the process.

For ``xxx.doc.dump`` files, they are line-separated JSON documents that can be imported into MongoDB directly using the following command:

.. code-block:: bash

    mongoimport --uri <mongodb-connection-string> --db <database-name> --collection <collection-name> --file <path-to-dump-file>

Neo4j ``LOAD CSV`` reads from Neo4j's configured import directory. Copy the ``*.node_ids.dump``, ``*.graph.dump``, and ``*.annotation.dump`` files there, or adjust the ``file:///`` paths below to match your Neo4j setup. Before importing graph data, create the indexes and uniqueness constraint used by the service:

.. code-block:: cypher

    CREATE INDEX concept_prefix_index IF NOT EXISTS
    FOR (n:Concept)
    ON (n.prefix);

    CREATE INDEX concept_id_index IF NOT EXISTS
    FOR (n:Concept)
    ON (n.id);

    CREATE CONSTRAINT concept_prefix_id_unique IF NOT EXISTS
    FOR (n:Concept)
    REQUIRE (n.prefix, n.id) IS UNIQUE;

A handful of fields beyond id/prefix/type-labels are also promoted to real node properties, because some later phase of graph analysis needs to filter on them directly rather than through the document database - e.g. scoping OHDSI's internal hierarchy to its SNOMED-sourced subset, or scoping UniProt's full, multi-organism release down to human. This is the complete list (``bioterms.model.concept.GRAPH_NODE_EXTRA_PROPERTIES`` in the source, kept in sync with this doc), each worth its own index at UniProt's scale (250M+ nodes, where an unindexed equality filter is a full node scan):

.. code-block:: cypher

    CREATE INDEX concept_sourceVocabularyId_index IF NOT EXISTS
    FOR (n:Concept)
    ON (n.sourceVocabularyId);

    CREATE INDEX concept_reviewed_index IF NOT EXISTS
    FOR (n:Concept)
    ON (n.reviewed);

    CREATE INDEX concept_organismTaxId_index IF NOT EXISTS
    FOR (n:Concept)
    ON (n.organismTaxId);

    CREATE INDEX concept_organismName_index IF NOT EXISTS
    FOR (n:Concept)
    ON (n.organismName);

For ``xxx.node_ids.dump`` files, they are CSV files that contain node IDs, concept types, and the extra properties above for creating graph nodes. The second column is written as a Python-style list string, for example ``['pathway', 'reaction']``. Columns 3-6 are, in order, ``sourceVocabularyId``, ``reviewed`` (``'True'``/``'False'``/empty), ``organismTaxId``, and ``organismName`` - present only for the vocabularies that populate them (OHDSI stamps ``sourceVocabularyId``; UniProt stamps all three of the others), empty otherwise. Import one vocabulary at a time, replacing ``some-prefix`` with the vocabulary prefix in the file name:

.. code-block:: cypher

    CALL apoc.periodic.iterate(
        "
        LOAD CSV FROM 'file:///xxx.node_ids.dump' AS row
        WITH row
        WHERE size(row) >= 1 AND row[0] IS NOT NULL AND trim(row[0]) <> ''
        WITH
            toString(row[0]) AS conceptId,
            CASE WHEN size(row) >= 2 AND row[1] IS NOT NULL THEN trim(row[1]) ELSE '' END AS rawTypes,
            CASE WHEN size(row) >= 3 AND row[2] IS NOT NULL AND trim(row[2]) <> '' THEN trim(row[2]) ELSE null END AS sourceVocabularyId,
            CASE WHEN size(row) >= 4 AND row[3] = 'True' THEN true
                 WHEN size(row) >= 4 AND row[3] = 'False' THEN false
                 ELSE null END AS reviewed,
            CASE WHEN size(row) >= 5 AND row[4] IS NOT NULL AND trim(row[4]) <> '' THEN trim(row[4]) ELSE null END AS organismTaxId,
            CASE WHEN size(row) >= 6 AND row[5] IS NOT NULL AND trim(row[5]) <> '' THEN trim(row[5]) ELSE null END AS organismName
        WITH conceptId, sourceVocabularyId, reviewed, organismTaxId, organismName,
            CASE
                WHEN rawTypes = '' OR rawTypes = '[]' THEN []
                ELSE [
                    label IN split(
                        replace(
                            replace(
                                replace(
                                    replace(rawTypes, '[', ''),
                                    ']', ''
                                ),
                                \"'\", ''
                            ),
                            '\"', ''
                        ),
                        ','
                    )
                    | trim(label)
                ]
            END AS parsedLabels
        RETURN conceptId, [label IN parsedLabels WHERE label <> ''] AS labels,
            sourceVocabularyId, reviewed, organismTaxId, organismName
        ",
        "
        MERGE (n:Concept {prefix: $concept_prefix, id: conceptId})
        WITH n, labels, sourceVocabularyId, reviewed, organismTaxId, organismName
        CALL apoc.create.addLabels(n, labels) YIELD node
        FOREACH (_ IN CASE WHEN sourceVocabularyId IS NULL THEN [] ELSE [1] END | SET node.sourceVocabularyId = sourceVocabularyId)
        FOREACH (_ IN CASE WHEN reviewed IS NULL THEN [] ELSE [1] END | SET node.reviewed = reviewed)
        FOREACH (_ IN CASE WHEN organismTaxId IS NULL THEN [] ELSE [1] END | SET node.organismTaxId = organismTaxId)
        FOREACH (_ IN CASE WHEN organismName IS NULL THEN [] ELSE [1] END | SET node.organismName = organismName)
        RETURN count(node) AS upserted
        ",
        {batchSize: 10000, parallel: true, params: {concept_prefix: 'some-prefix'}}
    );

For ``xxx.graph.dump`` files, they are CSV files that contain internal relationships for the same vocabulary. Each row is ``source_id,target_id,relationship_type,relationship_key``. The relationship key is used by multi-edge vocabularies such as OHDSI and is stored in the Neo4j relationship's ``label`` list property:

.. code-block:: cypher

    CALL apoc.periodic.iterate(
        "
        LOAD CSV FROM 'file:///xxx.graph.dump' AS row
        WITH row
        WHERE size(row) >= 3
        RETURN
            toString(row[0]) AS src,
            toString(row[1]) AS dst,
            CASE
                WHEN row[2] IS NULL OR trim(row[2]) = '' THEN 'related_to'
                ELSE trim(row[2])
            END AS relType,
            CASE
                WHEN size(row) >= 4 AND row[3] IS NOT NULL AND trim(row[3]) <> '' THEN trim(row[3])
                ELSE NULL
            END AS relKey
        ",
        "
        MERGE (source:Concept {prefix: $concept_prefix, id: src})
        MERGE (target:Concept {prefix: $concept_prefix, id: dst})
        CALL apoc.merge.relationship(source, relType, {}, {}, target) YIELD rel
        FOREACH (_ IN CASE WHEN relKey IS NULL THEN [] ELSE [1] END |
            SET rel.label =
                apoc.coll.toSet(
                    apoc.convert.toList(coalesce(rel.label, [])) + [relKey]
                )
        )
        ",
        {batchSize: 10000, parallel: false, params: {concept_prefix: 'some-prefix'}}
    );

Note that parallel execution is disabled for relationship imports to prevent deadlocks.

For ``xxx.annotation.dump`` files, they are CSV files that contain cross-vocabulary relationships for creating relationships in the graph database. Each row is ``source_prefix,source_id,target_prefix,target_id,annotation_type,properties_json``:

.. code-block:: cypher

    CALL apoc.periodic.iterate(
        "
        LOAD CSV FROM 'file:///xxx.annotation.dump' AS row
        WITH row
        WHERE size(row) >= 5
        RETURN
            trim(row[0]) AS prefixFrom,
            toString(row[1]) AS idFrom,
            trim(row[2]) AS prefixTo,
            toString(row[3]) AS idTo,
            CASE WHEN row[4] IS NULL OR trim(row[4]) = '' THEN 'annotated_with' ELSE trim(row[4]) END AS relType,
            CASE
            WHEN size(row) >= 6 AND row[5] IS NOT NULL AND trim(row[5]) <> '' THEN row[5]
            ELSE '{}'
            END AS propsJson
        ",
        "
        MERGE (source:Concept {prefix: prefixFrom, id: idFrom})
        MERGE (target:Concept {prefix: prefixTo,   id: idTo})
        WITH source, target, relType, apoc.convert.fromJsonMap(propsJson) AS props
        CALL apoc.merge.relationship(source, relType, {}, props, target) YIELD rel
        RETURN 1
        ",
        {batchSize: 10000, parallel: false, retries: 20}
    );

These queries may not be the most efficient way to import the data, but they are designed to be robust and handle various edge cases. Adjust the queries as needed based on your specific requirements and database setup, as long as the resulting schema remains consistent with the software's expectations.
