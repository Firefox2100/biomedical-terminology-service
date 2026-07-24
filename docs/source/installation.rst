==================
Installation Guide
==================

This guide will help you install the software on your system. Note that only Linux distributions are officially supported, due to the usage of multiprocessing libraries and torch.

Prerequisites
-------------

The following software must be installed on your system before proceeding with the installation:

* A document database for storing and retrieving biomedical terminologies text data. MongoDB is recommended and used throughout this guide; a SQL database (PostgreSQL, MySQL/MariaDB, or SQLite) is also supported as an alternative, see :doc:`build-database`.
* A graph database for storing the relationships between the terminology concepts. For now, only Neo4j is supported.
* A cache database for caching hot data and inter-process communication. For now, only Redis is supported.
* A vector database for storing and searching vector embeddings of biomedical terms. For now, only Qdrant is supported.

For the system running the software, the following requirements must be met:

* Sufficient disk space for databases and application files. Depending on the terminologies you plan to load, this could range from a few gigabytes to over 100GB.
* A minimum of 2 CPU cores and 4GB of RAM for query to work. To serve larger workloads or multiple users or to use large ontology graphs with custom properties, more resources will be needed. 4 cores and 8GB of RAM is recommended for moderate workloads.
* It's not recommended to compile the database directly on a server, because it consumes significant CPU and memory resources. Instead, compile the database on a separate machine, preferably an HPC with both CPU and GPU support, and then transfer the compiled database to the server. Refer to the :doc:`build-database` guide for more details.

If installing on bare metal, ensure the following environment requirements are met, this is not needed if using docker:

* Python 3.11 or higher
* Active internet connection for downloading dependencies
* The ability to install packages with pip, or you may need virtualenv or conda for managing Python environments.

Using Docker (Recommended)
--------------------------

The recommended way to install and run the software is using Docker. This method simplifies the installation process by encapsulating all dependencies within a container, and provides an isolated environment that avoids conflicts with other software on your system.

There are different tags available for different use cases:

* `latest`: This tag tracks the latest stable release of the software. However, staying on this tag means it will be updated automatically even for breaking changes, which may lead to unexpected issues. Use this tag if you want to always have the latest features and fixes, and are okay with manually rebuilding the database if needed.
* A specific version tag (e.g., `v1.2.3`): This tag corresponds to a specific release of the software. Using a version tag ensures that you have a stable and tested version, and it won't change unless you explicitly update it. This is recommended for production environments where stability is crucial.
* `-cpu` suffix: This tag is for CPU-only installations. It is suitable for systems without a compatible GPU or when GPU acceleration is not required. GPU acceleration is used to embed the text data and train graph neural networks, which are needed when compiling the semantic similarity database. When serving web requests, GPU is not required.
* The default tag (without `-cpu` suffix) includes GPU support, or you could use the `-gpu` suffix (they are the same). This version is recommended if you have a compatible NVIDIA GPU and want to leverage GPU acceleration for faster embedding and training times. Note that these images are significantly larger in size, reaching or exceeding 10GB, due to the inclusion of CUDA libraries.

Choose the appropriate tag based on your system capabilities and requirements.

It's recommended to use docker compose to manage the software and its dependencies. A sample `docker-compose.yml` file is provided in the repository to help you get started quickly. The content of the file is as follows:

.. code-block:: yaml
    :caption: docker-compose.yml
    :linenos:

    services:
      # Neo4j as the graph database
      neo4j:
        image: neo4j:5.26.17-community
        restart: unless-stopped
        container_name: neo4j
        healthcheck:
          test: [ "CMD-SHELL", "cypher-shell -u $${NEO4J_AUTH%%/*} -p $${NEO4J_AUTH##*/} 'RETURN 1'" ]
          interval: 30s
          timeout: 10s
          retries: 5
        ports:
          - "7474:7474"   # HTTP
          - "7687:7687"   # Bolt
        environment:
          # The full APOC plugin is required for advanced query and dynamic insertion capabilities
          - NEO4J_AUTH=neo4j/password
          - NEO4J_apoc_export_file_enabled=true
          - NEO4J_apoc_import_file_enabled=true
          - NEO4J_apoc_import_file_use__neo4j__config=true
          - NEO4J_PLUGINS=["apoc", "graph-data-science"]
        volumes:
          - neo4j_data:/data
          - neo4j_plugins:/plugins

      # MongoDB as the primary document database
      mongodb:
        image: mongo:8.2.2
        restart: unless-stopped
        container_name: mongodb
        healthcheck:
          test: [ "CMD", "mongosh", "--eval", "db.adminCommand('ping')" ]
          interval: 30s
          timeout: 10s
          retries: 5
        ports:
          - "27017:27017"
        volumes:
          - mongo_data:/data/db

      # Redis as the caching layer
      redis:
        image: redis:8.4
        restart: unless-stopped
        container_name: redis
        healthcheck:
          test: [ "CMD", "redis-cli", "ping" ]
          interval: 30s
          timeout: 5s
          retries: 5
        ports:
          - "6379:6379"
        command: ["redis-server", "--save", "60", "1", "--loglevel", "warning"]
        volumes:
          - redis_data:/data

      # Qdrant as the vector database
      qdrant:
        image: qdrant/qdrant:latest
        restart: unless-stopped
        container_name: qdrant
        # Qdrant explicitly states that they do not support health checks, and will not include
        # utilities for that in the image. If needed, rely on the docker orchestration platform's
        # built-in health check mechanisms.
        ports:
          - "6333:6333"
          - "6334:6334"
        configs:
          - source: qdrant_config
            target: /qdrant/config/production.yaml
        volumes:
          - qdrant_data:/qdrant/storage

      # Primary service
      bioterms:
        # Defaults to the GPU version; if you prefer lightweight CPU-only, change the image to:
        # image: firefox2100/biomedical-terminology-service:latest-cpu
        image: firefox2100/biomedical-terminology-service:latest
        restart: unless-stopped
        container_name: bioterms
        command: >
          web --host 0.0.0.0 --port 5000 --log-config /app/conf/uvicorn-log.config.yaml
        # This section enables GPU support; remove if not needed
        deploy:
          resources:
            reservations:
              devices:
                - driver: nvidia
                  count: all
                  capabilities: [gpu]
        depends_on:
          neo4j:
            condition: service_healthy
          mongodb:
            condition: service_healthy
          redis:
            condition: service_healthy
          qdrant:
            condition: service_started
        ports:
          - "5000:5000"
        environment:
          # Any of the following environment variables can be adjusted as needed
          # Or can be loaded from docker secrets for better security in production
          - BTS_PROCESS_LIMIT=4
          - BTS_LOGGING_LEVEL=INFO
          - BTS_SECRET_KEY=secret-key
          - BTS_SERVER_HMAC_KEY=your-hmac-key
          - BTS_USE_HTTPS=false
          - BTS_ENABLE_METRICS=true
          - BTS_SERVICE_ROOT_PATH=
          - BTS_OPENAPI_URL=/openapi.json
          - BTS_DOCS_URL=/docs
          - BTS_REDOC_URL=/redoc
          - BTS_ENVIRONMENT=dev
          - BTS_CELERY_BROKER=redis://redis:6379/1
          - BTS_CELERY_BACKEND=redis://redis:6379/2
          - BTS_DOC_DATABASE_DRIVER=mongo
          - BTS_MONGODB_HOST=mongodb
          - BTS_MONGODB_PORT=27017
          - BTS_MONGODB_DB_NAME=bts
          - BTS_MONGODB_USERNAME= # Leave blank if not using authentication
          - BTS_MONGODB_PASSWORD= # Leave blank if not using authentication
          - BTS_GRAPH_DATABASE_DRIVER=neo4j
          - BTS_NEO4J_URI=neo4j://neo4j:7687
          - BTS_NEO4J_DB_NAME=neo4j
          - BTS_NEO4J_USERNAME=neo4j
          - BTS_NEO4J_PASSWORD=password
          - BTS_CACHE_DRIVER=redis
          - BTS_REDIS_HOST=redis
          - BTS_REDIS_PORT=6379
          - BTS_REDIS_DB=0
          - BTS_BIOPORTAL_API_KEY=your-bioportal-api-key
          - BTS_NHS_TRUD_API_KEY=your-nhs-trud-api-key
          - BTS_NIH_UMLS_API_KEY=your-nih-umls-api-key
          - BTS_TRANSFORMER_MODEL_NAME=BAAI/bge-base-en-v1.5
          - BTS_TORCH_DEVICE=cpu
          - BTS_GNN_EPOCHS=100
          - BTS_GNN_HIDDEN_DIM=256
          - BTS_GNN_OUTPUT_DIM=256
          - BTS_GNN_LEARNING_RATE=0.001
          - BTS_VECTOR_DATABASE_DRIVER=qdrant
          - BTS_QDRANT_LOCATION=http://qdrant:6333
        volumes:
          - ./data:/app/data
          - ./config:/app/config

      # celery worker if you need web UI to trigger database operations. Generally not
      # recommended for production deployments, unless the worker is on-demand only.
      bioterms-worker:
        image: firefox2100/biomedical-terminology-service:latest
        restart: unless-stopped
        container_name: bioterms
        command: >
          worker --loglevel=INFO
        # This section enables GPU support; remove if not needed
        deploy:
          resources:
            reservations:
              devices:
                - driver: nvidia
                  count: all
                  capabilities: [ gpu ]
        depends_on:
          neo4j:
            condition: service_healthy
          mongodb:
            condition: service_healthy
          redis:
            condition: service_healthy
          qdrant:
            condition: service_started
        environment:
          - BTS_PROCESS_LIMIT=4
          - BTS_LOGGING_LEVEL=INFO
          - BTS_SECRET_KEY=secret-key
          - BTS_SERVER_HMAC_KEY=your-hmac-key
          - BTS_USE_HTTPS=false
          - BTS_ENABLE_METRICS=true
          - BTS_SERVICE_ROOT_PATH=
          - BTS_OPENAPI_URL=/openapi.json
          - BTS_DOCS_URL=/docs
          - BTS_REDOC_URL=/redoc
          - BTS_ENVIRONMENT=dev
          - BTS_CELERY_BROKER=redis://redis:6379/1
          - BTS_CELERY_BACKEND=redis://redis:6379/2
          - BTS_DOC_DATABASE_DRIVER=mongo
          - BTS_MONGODB_HOST=mongodb
          - BTS_MONGODB_PORT=27017
          - BTS_MONGODB_DB_NAME=bts
          - BTS_MONGODB_USERNAME= # Leave blank if not using authentication
          - BTS_MONGODB_PASSWORD= # Leave blank if not using authentication
          - BTS_GRAPH_DATABASE_DRIVER=neo4j
          - BTS_NEO4J_URI=neo4j://neo4j:7687
          - BTS_NEO4J_DB_NAME=neo4j
          - BTS_NEO4J_USERNAME=neo4j
          - BTS_NEO4J_PASSWORD=password
          - BTS_CACHE_DRIVER=redis
          - BTS_REDIS_HOST=redis
          - BTS_REDIS_PORT=6379
          - BTS_REDIS_DB=0
          - BTS_BIOPORTAL_API_KEY=your-bioportal-api-key
          - BTS_NHS_TRUD_API_KEY=your-nhs-trud-api-key
          - BTS_NIH_UMLS_API_KEY=your-nih-umls-api-key
          - BTS_TRANSFORMER_MODEL_NAME=BAAI/bge-base-en-v1.5
          - BTS_TORCH_DEVICE=cpu
          - BTS_GNN_EPOCHS=100
          - BTS_GNN_HIDDEN_DIM=256
          - BTS_GNN_OUTPUT_DIM=256
          - BTS_GNN_LEARNING_RATE=0.001
          - BTS_VECTOR_DATABASE_DRIVER=qdrant
          - BTS_QDRANT_LOCATION=http://qdrant:6333
        volumes:
          - ./data:/app/data
          - ./config:/app/config

    configs:
      qdrant_config:
        content: |
          log_level: INFO

    volumes:
      neo4j_data:
      neo4j_plugins:
      mongo_data:
      redis_data:
      qdrant_data:

Note that the above configuration is a starting point. You may need to adjust settings such as database credentials, ports, and resource limits based on your specific environment and requirements. The worker service is optional, and is used to handle database related operations such as rebuilding cache, or rebuilding the entire database. ``BTS_SECRET_KEY`` and ``BTS_SERVER_HMAC_KEY`` are placeholders above and should be replaced with generated values before deploying; a HMAC key can be generated with ``docker compose exec bioterms bioterms-cli generate-hmac-key``.

Once the containers are up, create an administrator account to log into the web UI:

.. code-block:: bash

    docker compose exec bioterms bioterms-cli user create <username>

From Source Code
----------------

Using source code is easier for development and building the database. Ensure you have Python 3.11 or higher installed, along with pip for managing Python packages.

1. Clone the repository and install dependencies:

   .. code-block:: bash

       git clone https://github.com/Firefox2100/biomedical-terminology-service.git
       cd biomedical-terminology-service
       pip install .[all]

2. Configure environment variables as needed. The environment variables are read from the shell first, then it will look for a `.env` file in the `./conf/.env` path. This can be overridden by specifying a different path using the `BTS_ENV_FILE` environment variable. An example `example.env` file is provided in the project root directory. ``BTS_SERVER_HMAC_KEY`` is required and has no default; generate one with:

    .. code-block:: bash

         bioterms-cli generate-hmac-key

3. If the database is not yet built, build it first. The web service will start with an empty database, but all query functions will be disabled until the database is built. Refer to the :doc:`build-database` guide for more details. After loading or deleting a vocabulary or annotation, the GraphQL schema needs to be refreshed to reflect the change. Either send an authenticated ``POST /reload-graphql`` request, or restart the web service; a full restart is not required.
4. Create an administrator account, used to log into the web UI and manage API keys:

    .. code-block:: bash

         bioterms-cli user create <username>

5. Start the web service:

    .. code-block:: bash

         uvicorn bioterms.asgi:application --host 127.0.0.1 --port 5000 --log-config ./conf/uvicorn-log.config.yaml
6. (Optional) Start a Celery worker if you need to trigger database operations from the web UI. This is generally not needed for production deployments.

    .. code-block:: bash

         celery -A bioterms.task.app.celery_app worker --loglevel=info
