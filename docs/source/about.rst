=====
About
=====

Why BTS exists
==============

BTS began as a proof of concept: could one self-hosted service provide consistent access
to several biomedical terminologies and calculate useful relationships on top of them?
The project now includes loaders for multiple vocabulary formats, a heterogeneous graph
for concepts and cross-vocabulary annotations, lexical and vector search, and several
similarity methods.

The main audience for the public interface is anyone who needs to look up and explore
concepts. The API and operator documentation are intended for developers, researchers,
and administrators who need repeatable access to terminology data or want to build and
maintain a local deployment.

What BTS does
=============

* Presents supported vocabularies through one browser and API model.
* Stores concept records, graph relationships, and vector embeddings in backends chosen
  by the operator.
* Loads published annotations so clients can follow direct or multi-hop mappings between
  vocabularies.
* Supports hierarchy expansion, constrained translation, and precomputed concept
  similarity in addition to ordinary lookup and search.
* Runs on infrastructure controlled by the operator. No hosted BTS service is required.

What BTS does not do
====================

BTS does not bundle the vocabulary datasets, replace their publishers, or grant rights to
use them. Some loaders require accounts, API keys, or licence acceptance with the original
publisher. Review the terms for every dataset before downloading, loading, or exposing it.
The supported sources and their requirements are listed in :doc:`vocabularies`.

The software and this documentation are released under the MIT License. That licence
applies to BTS, not to data loaded into it. A local deployment is also responsible for its
own access controls, retention policy, and assessment of whether a dataset is suitable for
its intended use.
