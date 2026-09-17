# Biomedical Terminology Service

[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=Firefox2100_biomedical-terminology-service&metric=bugs)](https://sonarcloud.io/summary/new_code?id=Firefox2100_biomedical-terminology-service) [![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=Firefox2100_biomedical-terminology-service&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=Firefox2100_biomedical-terminology-service) [![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=Firefox2100_biomedical-terminology-service&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=Firefox2100_biomedical-terminology-service) [![Documents](https://app.readthedocs.org/projects/biomedical-terminology-service/badge/?version=latest&style=flat)](https://biomedical-terminology-service.readthedocs.io/en/latest/)

The BioMedical Terminology Service (BTS) is a self-hosted platform that provides one API for searching, traversing, mapping and semantically comparing concepts across major biomedical terminologies, with REST, GraphQL, FHIR and MCP interfaces. It aims to facilitate the integration, retrieval, and research of biomedical terms for various applications in healthcare, research, and data analysis.

A public demonstration server is hosted by the Brookes Lab, University of Leicester. It runs the same codebase as BTS (with different branding) and is maintained by the project authors. It can be accessed at [https://similarity.cafevariome.org](https://similarity.cafevariome.org).

## Features

* Unified access to multiple biomedical vocabularies and ontologies, including SNOMED CT, CTV3, HGNC, HPO, MONDO, NCIt, OMIM, ORDO, Reactome, Ensembl, OHDSI, Uberon, and UniProt.
* Cross-vocabulary annotations and mappings, for normalising subject tagging and translating between term sets.
* Parent-child and other relationship traversal, for use in external queries.
* Auto-complete and full-text search across loaded vocabularies.
* Semantic similarity and translation between concepts, backed by multiple pluggable similarity methods (co-annotation vectors, relevance, weighed relevance).
* REST API with legacy, batched, and streaming versions to suit different client needs, plus a GraphQL endpoint for fine-grained, client-selected field access.
* A FHIR-compatible terminology server interface (`CodeSystem` lookup and validation) for interoperability with FHIR-based clinical systems.
* An MCP server, for use with LLMs and agents.
* Pluggable database backends for document, graph, cache, and vector storage, selectable independently at deployment time.

## Documentation

Full documentation, including installation, database build instructions, per-vocabulary notes, API usage guides, and the mathematical background of the similarity methods, is available at [Read the Docs](https://biomedical-terminology-service.readthedocs.io/en/latest/):

* [About](https://biomedical-terminology-service.readthedocs.io/en/latest/about.html)
* [Installation](https://biomedical-terminology-service.readthedocs.io/en/latest/installation.html)
* [Building the database](https://biomedical-terminology-service.readthedocs.io/en/latest/build-database.html)
* [Supported vocabularies](https://biomedical-terminology-service.readthedocs.io/en/latest/vocabularies.html)
* [Usage guide](https://biomedical-terminology-service.readthedocs.io/en/latest/usage/index.html)
* [GraphQL API](https://biomedical-terminology-service.readthedocs.io/en/latest/graphql.html)
* [FHIR API](https://biomedical-terminology-service.readthedocs.io/en/latest/fhir.html)
* [MCP API](https://biomedical-terminology-service.readthedocs.io/en/latest/mcp-api.html)
* [Similarity methods](https://biomedical-terminology-service.readthedocs.io/en/latest/similarity-methods/index.html)

## Licence, Acknowledgements, and Disclaimer

This software itself is released under [MIT Licence](LICENSE). You should have received a copy of the licence file with this software. If not, see [https://opensource.org/licenses/MIT](https://opensource.org/licenses/MIT).

Third-party libraries and tools used in this project are released under their own licences. They are not included in this repository, and you should refer to their respective documentation for licence information.

The data used in this project is not owned by the authors of this software. They are provided by their own releasing organisations or individuals. As such, they are not included in this repository or its releases, and the users are expected to obtain them separately. For some data, it may be necessary to register for an account, agree to a licence, or pay a fee to access them. The users are expected to comply with the terms and conditions of the data providers when using the data. This software or its author is not affiliated with, or endorsed by, any of these organisations or individuals and cannot help with the process of obtaining the data.

In some countries or regions, usage of this software or the data it intends to work with may be restricted. The users are expected to comply with any applicable laws and regulations in their own countries or regions. The authors of this software are not responsible for any legal issues that may arise from the use of this software or the data it intends to work with.

## Installation

For complete installation instructions, refer to the [Installation Guide](https://biomedical-terminology-service.readthedocs.io/en/latest/installation.html). Below is a quick start using docker-compose:

This software releases pre-built images on [Docker Hub](https://hub.docker.com/r/firefox2100/biomedical-terminology-service). The easiest way to get started is to use the provided `docker-compose.yaml` file. You can run the following command in the terminal:

```bash
git clone https://github.com/Firefox2100/biomedical-terminology-service.git
cd biomedical-terminology-service
# Modify the environment variables in the file
nano docker-compose.yaml
docker compose up -d
```
