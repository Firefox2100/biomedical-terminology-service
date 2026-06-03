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

Neo4j
^^^^^

The recommended graph database for storing graph data is Neo4j. Follow the official `Neo4j installation guide <https://neo4j.com/docs/operations-manual/current/installation/>`_ to set up Neo4j on your machine. Ensure that the Neo4j server is running before proceeding with the database construction.

This service utilises the ``APOC`` and ``graph-data-science`` plugins for advanced graph operations, both in building and querying the graph database. Make sure to install these plugins in your Neo4j instance. If using docker, this can be configured via environment variables; otherwise you would need to download the matching release of APOC and GDS plugins from their repositories and place them in the ``plugins`` folder of your Neo4j installation.

Qdrant
^^^^^^

Qdrant only provides a docker image officially. However, it's also possible to compile the source code directly, allowing it to be run on machines without docker support. If compiling from source, the main repository does not come with the web UI, and it must be downloaded separately and placed next to the compiled binary, if you want to use the web UI for management.

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

* NHS TRUD API key for CTV3 and SNOMED CT. You need to subscribe to these vocabularies and wait for them to approve the subscription, before the API key can be used to download the files.
* BioPortal API key for OMIM and ORDO
* NIH UMLS API key for SNOMED-ORDO mapping files

And not all vocabularies can be downloaded this way. Particularly:

* Reactome releases only a Neo4j dump and a SQL dump. They are both complicated to read from plain Python without restoring them into a database first. Therefore, Reactome must be loaded into a Neo4j 4 (note that we use Neo4j 5 for this service, so you may need to install Neo4j 4 separately) instance first, and use the provided script ``scripts/dump_reactome_to_csv.py`` to export the data to CSV files that can be imported into the main database.
* OHDSI standardized vocabularies are not open for public download, and provides no download API. You need to manually download the latest release from Athena, and unzip it to the data folder.
* UMLS system provides no way to fetch the latest release files automatically, so the files downloaded from UMLS are using hard-coded URL. If you need a different version, you need to manually download the files from UMLS and place them in the data folder, or open an issue/pull request to notify us of the desired version.

Loading the vocabulary
^^^^^^^^^^^^^^^^^^^^^^

The vocabularies can be loaded into the document database and graph database using the following command:

.. code-block:: bash

    bioterms-cli vocabulary load <the-vocabulary-id>

This reads the vocabulary files from the data folder, processes them and holds them in a memory list for concepts, and a memory graph for relationships. If the vocabulary is large, this may take a significant amount of RAM. After processing, the concepts and relationships are written to the document database and graph database respectively. Depending on the size of the vocabulary, this may take a long time. The indices are automatically created during this process.

Additionally, this command supports a ``--offline`` flag, which allows the database to be built without an actual database connection. The results will be written into dump files in the offline directory, which can later be imported into the database using the database import tools provided by the respective database systems. This is useful when building on systems like HPC, which are optimised for computation but not for disk I/O operations, and may have trouble running database processes.

Loading the annotations
^^^^^^^^^^^^^^^^^^^^^^^

After multiple vocabularies are loaded, the annotations between them can be loaded using the following command:

.. code-block:: bash

    bioterms-cli annotation download <vocabulary> <another-vocabulary>
    bioterms-cli annotation load <vocabulary> <another-vocabulary>

The order of the vocabulary arguments does not matter. This downloads the mapping (mind the API keys), and loads them into the graph database as relationships between the concepts from the two vocabularies.

Embedding the concepts
^^^^^^^^^^^^^^^^^^^^^^

Concept embeddings are used for semantic search on concepts, and calculating semantic similarity between concepts. They are not strictly necessary for the software to function, but highly recommended for an improved user experience. To embed the concepts, run the following command:

.. code-block:: bash

    bioterms-cli vocabulary embed <the-vocabulary-id>

This uses the ``sentence-transformers`` library to generate embeddings for the concepts in the specified vocabulary. The model used by this process is adjustable, but must not be changed once any embedding is stored. Using different model for different vocabulary is not allowed, and will cause all relevant functions to fail. This operation also supports the ``--offline`` flag, which writes the embeddings to a special binary dump format. This must be imported into the vector database using the CLI itself, via

.. code-block:: bash

    bioterms-cli vocabulary embed <the-vocabulary-id> --restore

Because the binary format is specific to this software. This is necessary to preserve accuracy of the vector and compress efficiently.

Calculating semantic similarities
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This is one of the core features this service offers. Semantic similarities have first-class support in the query engine, and can be used in various scenarios. Multiple different similarity metrics are supported, and can be calculated using the following command:

.. code-block:: bash

    bioterms-cli similarity calculate --target <the-vocabulary-id> --corpus <corpus-vocabulary-id> --method <similarity-method> --threshold <similarity-threshold>

The target is the vocabulary for which the similarities are calculated, and the corpus is the vocabulary against which the similarities are calculated. The corpus may not be required if the metric is intrinsic, in which case it can be omitted. If the corpus is required but omitted, the software understands this as all possible corpus should be used, and it will calculate multiple metrics for different corpus. Similarly, if no method is specified, it will use all methods that can be applied to this vocabulary. The threshold is used to filter out low-similarity results, and can be adjusted based on the desired sensitivity. Because most similarity pairs will have a low score, we recommend setting it to at least 0.2 to get meaningful results, and possibly over 0.7 to get high-confidence results. This operation also supports the ``--offline`` flag, which writes the similarity results to a CSV file in the offline directory.

The supported similarity methods are:

* Co-annotation vectors: calculates similarity based on shared annotations between concepts.
* Relevance method: calculates similarity based on relevance scores using Information Content (IC) metrics.
* Weighed relevance method: similar to relevance method, assign weights to corpus annotation contribution.

Because the similarity scores are calculated pair-wise, and for some methods it's not possible to estimate the score without actually calculating it, the workload will grow exponentially with the size of the vocabulary. For example, SNOMED CT has over 1 million concepts with the extensions loaded. Even for just 1 million concepts, there will be almost 500 billion pairs to calculate. For OHDSI which has over 9 million concepts, it's over 40 trillion. This is not practical even for HPC systems. We recommend NOT to calculate similarity for these large vocabularies, and rely on mappings to smaller vocabularies instead.

Deleting a vocabulary
^^^^^^^^^^^^^^^^^^^^^

This service does not support incremental updates of the vocabularies, because it cannot understand the differences between versions. Therefore, to update a vocabulary, it must be deleted first, and then re-loaded. To delete a vocabulary, run the following command:

.. code-block:: bash

    bioterms-cli vocabulary delete <the-vocabulary-id>

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

For ``xxx.node_ids.dump`` files, they are CSV files that contain node IDs and concept types for creating graph nodes. The second column is written as a Python-style list string, for example ``['pathway', 'reaction']``. Import one vocabulary at a time, replacing ``some-prefix`` with the vocabulary prefix in the file name:

.. code-block:: cypher

    CALL apoc.periodic.iterate(
        "
        LOAD CSV FROM 'file:///xxx.node_ids.dump' AS row
        WITH row
        WHERE size(row) >= 1 AND row[0] IS NOT NULL AND trim(row[0]) <> ''
        WITH
            toString(row[0]) AS conceptId,
            CASE WHEN size(row) >= 2 AND row[1] IS NOT NULL THEN trim(row[1]) ELSE '' END AS rawTypes
        WITH conceptId,
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
        RETURN conceptId, [label IN parsedLabels WHERE label <> ''] AS labels
        ",
        "
        MERGE (n:Concept {prefix: $concept_prefix, id: conceptId})
        WITH n, labels
        CALL apoc.create.addLabels(n, labels) YIELD node
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
