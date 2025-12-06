============================
Vocabularies and Annotations
============================

Vocabulary Support
==================

This software aims to support a wide range of biomedical vocabularies and ontologies to provide comprehensive coverage of biomedical concepts, starting from the most commonly adopted ones. Below is a list of vocabularies and their support status:

+------------+------------------+-------------------------------------------------------+
| Vocabulary | Status           | Note                                                  |
+============+==================+=======================================================+
| CTV3       | Supported        | Downloaded from the NHS TRUD API                      |
+------------+------------------+-------------------------------------------------------+
| Ensembl    | Supported        | Downloaded via FTP from Ensembl                       |
+------------+------------------+-------------------------------------------------------+
| HGNC       | Supported        | Downloaded from HGNC release on Google Drive          |
+------------+------------------+-------------------------------------------------------+
| HPO        | Supported        | Downloaded from GitHub release                        |
+------------+------------------+-------------------------------------------------------+
| HPO        | Supported        | Downloaded from GitHub release                        |
+------------+------------------+-------------------------------------------------------+
| NCIT       | Supported        | Downloaded via FTP from NIH                           |
+------------+------------------+-------------------------------------------------------+
| OMIM       | Supported        | Downloaded from BioPortal API                         |
+------------+------------------+-------------------------------------------------------+
| ORDO       | Supported        | Downloaded from BioPortal API                         |
+------------+------------------+-------------------------------------------------------+
| Reactome   | Supported        | Using the official Neo4j dump, this program creates a |
|            |                  | custom CSV dump for import, and released in GitHub    |
+------------+------------------+-------------------------------------------------------+
| SNOMED     | Supported        | Downloaded from the NHS TRUD API                      |
+------------+------------------+-------------------------------------------------------+
| ICD 10     | Not Supported    | On the roadmap for future support                     |
+------------+------------------+-------------------------------------------------------+
| ICD 11     | Will Not Support | ICD 11 does not release the full terminology, only    |
|            |                  | some linearizations.                                  |
+------------+------------------+-------------------------------------------------------+

For other vocabularies not listed here, if you have a use case for their inclusion, please open an issue on the GitHub repository to discuss potential support and implementation.

Specifically, although ICD 10 and ICD 11 are officially endorsed by the WHO and widely used in clinical settings, they are not prioritized for support in this software. ICD uses a flat structure without rich semantic relationships between concepts, providing minimal benefit for research on ontology itself. Additionally, ICD 11 does not release the full terminology, only some linearizations, which limits its utility in this context. If this changes in the future, or if you are willing to contribute to their integration, please reach out via the GitHub repository.

Vocabulary Inclusion Criteria
=============================

For a vocabulary to be considered for inclusion in this software, it should meet the following criteria:

* **Biomedical relevance**: The vocabulary should be relevant to the biomedical domain, covering concepts related to health, diseases, treatments, genetics, or other related fields.
* **Being actively maintained or have large user base**: The vocabulary should be actively maintained with regular updates, or being phased out but have a large user base and significant historical data (like CTV3). Fully outdated vocabularies that are no longer maintained and have minimal usage will not be considered by the developers, but they are open for community contributions.
* **Rich semantic relationships**: The vocabulary should have rich semantic relationships between concepts or with another vocabulary, or provide significant value even with a flat structure (e.g. HGNC symbols are used as canonical gene names).
* **Open access or permissive licensing**: The vocabulary should be openly accessible or have a permissive license that allows for its use and distribution within research contexts. This software and its developers discourage the design and usage of proprietary vocabularies that restrict access to knowledge.

Additionally, vocabularies that are widely adopted in the biomedical community and have a significant impact on research and clinical practice are prioritized for inclusion. The ones that are specific to a country that is not UK or US are less prioritized, unless they have a large user base or are of particular research interest.

Annotations Support
===================

This software also utilises mappings and annotations between the supported vocabularies to enhance the connectivity and semantic richness of the integrated knowledge graph. These annotations help link concepts across different vocabularies, facilitating more comprehensive queries and analyses. Below is a list of annotation sources and their support status:
