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
