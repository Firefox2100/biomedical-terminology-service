# Data export for R-GNN

This part of the code exports data from databases and prepare it for R-GNN training.

## Node exporting

The nodes are exported with the following data:

- Node ID: The unique identifier for each node in the graph.
- Prefix: The vocabulary prefix of the node. This represents the original ontology this node is from.
- Type: The coarse type of the node, including:
  - PHENOTYPE / CLINICAL_FINDING
  - DISEASE / DISORDER
  - PROCEDURE / INTERVENTION
  - DRUG / SUBSTANCE
  - GENE
  - PROTEIN
  - PATHWAY / REACTION / BIO_PROCESS
  - ANATOMY
  - LAB_MEASUREMENT / OBSERVATION / VITAL_SIGN
  - MICROBE / ORGANISM
  - VARIANT / GENOMIC_FEATURE
  - APPLIANCES / DEVICES
  - ADMINISTRATIVE / SOCIAL / ENVIRONMENTAL
  - EVENT / EXPOSURE
  - STRUCTURE / NON_ENTITY
  - OTHER / UNKNOWN
