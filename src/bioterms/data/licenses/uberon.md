# Licence of Uberon data

This project uses the Uberon multi-species anatomy ontology. It is not affiliated with or
endorsed by the Uberon project or the OBO Foundry.

Uberon is distributed under the Creative Commons Attribution 3.0 license. The service retains
Uberon's concepts and asserted relationships while representing them in its own storage model.

## Data source and links

- [Uberon ontology](http://uberon.org/)
- [Official OBO Foundry entry](https://obofoundry.org/ontology/uberon.html)
- [Release repository](https://github.com/obophenotype/uberon/releases)
- [CC BY 3.0](https://creativecommons.org/licenses/by/3.0/)

## Additional mapping data published under the same licence

The service uses Uberon's OBO-style `hasDbXref` annotations to map Uberon anatomy concepts to
GO, NCIt, and SNOMED CT identifiers. These cross-references are part of the official Uberon
release and are the source of truth for Uberon's bridge products.

- [Uberon bridge and cross-reference documentation](https://github.com/obophenotype/uberon/blob/master/docs/bridges.md)

The bridge data stores identifiers only and does not redistribute GO, NCIt, or SNOMED CT
terminology content. When the mappings are used with those locally loaded releases, the separate
licence terms bundled for those vocabularies also apply to that content.
