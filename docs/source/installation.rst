==================
Installation Guide
==================

This guide will help you install the software on your system. Note that only Linux distributions are officially supported, due to the usage of multiprocessing libraries and torch.

Prerequisites
=============

THe following software must be installed on your system before proceeding with the installation:

* A document database for storing and retrieving biomedical terminologies text data. For now, only MongoDB is supported.
* A graph database for storing the relationships between the terminology concepts. For now, only Neo4j is supported.
* A cache database for caching hot data and inter-process communication. For now, only Redis is supported.
* A vector database for storing and searching vector embeddings of biomedical terms. For now, only Qdrant is supported.

For the system running the software, the following requirements must be met:

* Sufficient disk space for databases and application files. Depending on the terminologies you plan to load, this could range from a few gigabytes to over 100GB.
* A minimum of 2 CPU cores and 4GB of RAM for query to work. To serve larger workloads or multiple users or to use large ontology graphs with custom properties, more resources will be needed. 4 cores and 8GB of RAM is recommended for moderate workloads.
* It's not recommended to compile the database directly on a server, because it consumes significant CPU and memory resources. Instead, compile the database on a separate machine, preferably an HPC with both CPU and GPU support, and then transfer the compiled database to the server.

If installing on bare metal, ensure the following environment requirements are met, this is not needed if using docker:

* Python 3.11 or higher
* Active internet connection for downloading dependencies
* The ability to install packages with pip, or you may need virtualenv or conda for managing Python environments.

Using Docker (Recommended)
==========================

The recommended way to install and run the software is using Docker. This method simplifies the installation process by encapsulating all dependencies within a container, and provides an isolated environment that avoids conflicts with other software on your system.

There are different tags available for different use cases:

* `latest`: This tag tracks the latest stable release of the software. However, staying on this tag means it will be updated automatically even for breaking changes, which may lead to unexpected issues. Use this tag if you want to always have the latest features and fixes, and are okay with manually rebuilding the database if needed.
* A specific version tag (e.g., `v1.2.3`): This tag corresponds to a specific release of the software. Using a version tag ensures that you have a stable and tested version, and it won't change unless you explicitly update it. This is recommended for production environments where stability is crucial.
* `-cpu` suffix: This tag is for CPU-only installations. It is suitable for systems without a compatible GPU or when GPU acceleration is not required. GPU acceleration is used to embed the text data and train graph neural networks, which are needed when compiling the semantic similarity database. When serving web requests, GPU is not required.
* The default tag (without `-cpu` suffix) includes GPU support, or you could use the `-gpu` suffix (they are the same). This version is recommended if you have a compatible NVIDIA GPU and want to leverage GPU acceleration for faster embedding and training times. Note that these images are significantly larger in size, reaching or exceeding 10GB, due to the inclusion of CUDA libraries.

Choose the appropriate tag based on your system capabilities and requirements.

It's recommended to use docker compose to manage the software and its dependencies. A sample `docker-compose.yml` file is provided in the repository to help you get started quickly.
