# Concept-normalisation reranker: training tooling

Scripts to mine a training dataset for a ColBERT-style late-interaction reranker (see the
project's research notes on why late-interaction rather than a plain cross-encoder fits the
"client knows the disease, not the exact expression" terminology-harmonisation use case) and
fine-tune it, using data auto-mined from this service's own database -- no manual pair
labelling required.

## Service deployment

By default, `train_reranker.py` writes training checkpoints to
`scripts/reranker/runs/production/checkpoint-*` and exports the selected final model to
`scripts/reranker/runs/production/final`. The local service discovers that `final` bundle at
startup when it exists. Use `--output-dir runs/<experiment>` during tests or ablations to keep
their checkpoints and model bundles separate; the service does not auto-load those runs.
The service must be restarted to load a newly trained bundle.

For a different deployment path or a published Hugging Face repository, set the model source
explicitly; PyLate/SentenceTransformers use the same setting for both forms:

```dotenv
# Local bundle
BTS_RERANKER_MODEL=/models/bioterms-sapbert-colbert

# Or, after publishing
# BTS_RERANKER_MODEL=your-org/bioterms-sapbert-colbert

BTS_SEARCH_RETRIEVAL_CANDIDATE_LIMIT=10
BTS_SEARCH_VECTOR_OVERRETRIEVE_FACTOR=1.0
BTS_RERANKER_CANDIDATE_LIMIT=50
BTS_RERANKER_BATCH_SIZE=32
BTS_RERANKER_QUERY_LENGTH=32
BTS_RERANKER_DOCUMENT_LENGTH=64
BTS_RERANKER_MAX_ALIASES=6
```

When the model setting is absent, service search retains its RRF-only behaviour. Retrieval
depth, vector over-retrieval, candidates passed to the reranker, and API results returned are
independent controls. For quantized vector storage, increase
`BTS_SEARCH_VECTOR_OVERRETRIEVE_FACTOR` (for example, `2.0`) to retrieve more vector hits
before fusion without increasing the reranker or response sizes. Exact identifier,
preferred-label, and synonym matches remain pinned and never enter the reranker; if exact
results fill the response limit, the model is not loaded or invoked.

Four scripts, two machines:

- **`build_training_data.py`** -- runs against a fully built and embedded bioterms database
  (needs `BTS_*` config pointing at it, same as the rest of the service). CPU is fine; a GPU
  only speeds up the embedding calls it makes while mining. Produces query-group JSONL
  (identifiers + retrieval metadata only) plus per-vocabulary concept-store JSONL files.
- **`mine_cross_vocab_positives.py`** -- mines aliases across trusted `EXACT` relationship
  edges, using target-vocabulary recall candidates and the same negative-selection path. It
  rejects an alias/mapping pair when no recall arm retrieves its gold concept.
- **`score_candidate_pools.py`** -- runs the trained ColBERT checkpoint over every retained
  candidate, writing a new resumable set of JSONL shards with a named ranking-score channel.
  It never mutates or re-queries the source database data.
- **`train_reranker.py`** -- standalone, no bioterms/database dependency at all, only the ML
  stack (`pylate`, `sentence-transformers`, `datasets`, `torch`) plus the small
  `concept_rendering.py` helper in this folder. Copy the JSONL files the mining script
  produces to your HPC system and run this there.

Concepts, not text, are what gets mined and stored: a query group names its gold concept and
its negative concepts (with per-recall-arm rank/score evidence), and a separate concept store
holds each concept's raw label/synonyms/definition. Rendering the actual training strings
happens in `train_reranker.py` at load time (see "Rendering" below), so the text
representation can change later without re-mining.

## 1. Mining the dataset

Requires the same environment as `bioterms-cli` (i.e. `pip install -e .` from the repo root,
with whichever DB driver extras your deployment uses), pointed at a database that has already
been loaded **and embedded** (`bioterms-cli vocabulary load` / `embed`).

```bash
cd scripts/reranker
python build_training_data.py --output data/part-00.jsonl --skip 0 --limit 100000
```

For incremental database builds, prefer per-vocabulary output so each completed vocabulary can
be mined while other vocabularies are still loading or embedding:

```bash
python build_training_data.py --output-dir data/by-vocabulary \
  --vocabularies hpo mondo ncit --per-vocabulary-limit 100000
```

This writes ``aliases.hpo.jsonl``, ``aliases.mondo.jsonl``, and so on. Cross-vocabulary mining
supports the same layout and routes rows by target vocabulary:

```bash
python mine_cross_vocab_positives.py --output-dir data/by-vocabulary \
  --vocabularies hpo mondo ncit
```

Those files are named ``cross-vocab.<target>.jsonl``. Adding, replacing, or masking a
vocabulary is therefore a training-time choice rather than a dataset recombination step.

This mines the first 100k query units (in a deterministic order -- see the script's module
docstring) into `data/part-00.jsonl`, plus `data/part-00.jsonl.stats.json` and per-vocabulary
concept-store files under `data/concepts/<prefix>.concepts.jsonl`.

Each query-group line looks like:

```json
{
  "schema_version": 2,
  "query_id": "snomed:73211009:alias:1",
  "prefix": "snomed",
  "gold_concept_id": "73211009",
  "query": "sugar diabetes",
  "query_kind": "alias",
  "candidate_pool": [
    {
      "concept_id": "X",
      "role": "negative",
      "sources": ["alias_embedding", "lexical"],
      "ranks": {"alias_embedding": 4, "lexical": 2},
      "scores": {"alias_embedding": 0.84, "lexical": 7.31},
      "rank_band": "very_hard",
      "ranking_scores": {}
    }
  ],
  "negatives": [
    {
      "concept_id": "X",
      "sources": ["alias_embedding", "lexical"],
      "ranks": {"alias_embedding": 4, "lexical": 2},
      "scores": {"alias_embedding": 0.84, "lexical": 7.31},
      "rank_band": "very_hard"
    }
  ]
}
```

`candidate_pool` is the immutable, fully merged and equivalence-filtered recall pool;
`negatives` remains the small legacy selection for backward compatibility. No candidate text
is stored -- only concept identifiers and retrieval evidence. A negative
concept hit by several recall arms is merged into one candidate with all of that evidence
attached (not several separate candidates, and not "first arm wins").

### Audit retrieval before re-mining

The saved schema-v2 pools retain the gold concept's per-arm retrieval ranks. Replay recall
and production-style RRF on CPU without using an embedding model or querying the database:

```bash
python audit_retrieval.py data/v4/raw/cross-vocab.full.jsonl \
  --sample-modulus 20 --output runs/retrieval-audit.json \
  --review-output runs/retrieval-review.jsonl
```

This is conditional on rows the miner kept, and RRF ties are scored optimistically. The
cross-vocabulary miner historically discarded rows whose gold was absent from every arm;
include `skipped_gold_not_retrieved` from its shard statistics when reporting the true
top-50 recall denominator. On a future cross-vocabulary mining run, add
`--retrieval-miss-output data/retrieval-misses.jsonl` to preserve those gold-absent queries
as a separate evaluation manifest; they still do not become reranker training examples.
The document backends now require at least one three-character token for lexical search,
so short-query audits must also check whether vector retrieval alone finds the gold.

For a small live check against the currently configured indexes, including a future
retrieval-miss manifest, use CPU embeddings while GPUs are occupied:

```bash
CUDA_VISIBLE_DEVICES='' python probe_live_retrieval.py \
  --input data/retrieval-misses.jsonl --limit 50 --depths 50 100 \
  --output runs/live-retrieval-probe.json
```

The probe compares item-level arm depths before concept deduplication, as production does.
It is read-only and leaves the service's configured embedding device unchanged.

To mirror the service's optional exact-mapped recall during a new cross-vocabulary mining
run, add `--mapped-recall-limit 5 --mapped-candidate-limit 20`. Mapped concepts are retained
as a separate `exact_mapping` evidence arm, so queries recovered only by mappings become
eligible training examples. Both options are disabled by default to keep older runs exactly
reproducible. Enabling this changes candidate pools and therefore requires re-mining before a
mapped-recall-specific continuation run.

### Model-in-the-loop re-mining

After mining schema-v2 pools, attach scores from the selected SapBERT-ColBERT checkpoint:

```bash
python score_candidate_pools.py \
  --input-dir data/v4/raw \
  --output-dir data/v4/scored \
  --concept-store-dir data/v4/raw/concepts \
  --model runs/sapbert-colbert-v3-full/final \
  --score-key sapbert_colbert_v3
```

The scorer resumes an interrupted shard from its `.partial` file and skips completed output
shards. It adds the gold concept to the pool, scores every candidate, and stores scores under
`candidate_pool[*].ranking_scores.<score-key>`. New score channels (including a future
cross-encoder or LLM-derived teacher) can be produced as separate immutable output datasets;
retrieval/mining does not need to run again.

Train with model-aware hard/mid/easy sampling while keeping the existing contrastive loss:

```bash
python train_reranker.py \
  --train-data-dir data/v4/scored \
  --concept-store-dir data/v4/raw/concepts \
  --base-model runs/sapbert-colbert-v3-full/final \
  --negative-sampling model_stratified \
  --ranking-score-key sapbert_colbert_v3 \
  --training-objective contrastive \
  --output-dir runs/sapbert-colbert-v4-remine
```

Or use the identical scored files for listwise ranking distillation:

```bash
python train_reranker.py \
  --train-data-dir data/v4/scored \
  --concept-store-dir data/v4/raw/concepts \
  --base-model runs/sapbert-colbert-v3-full/final \
  --negative-sampling model_stratified \
  --ranking-score-key sapbert_colbert_v3 \
  --training-objective distillation \
  --distillation-temperature 1.0 \
  --output-dir runs/sapbert-colbert-v4-distilled
```

Omit these three new training flags to reproduce legacy retrieval sampling and hard-label
contrastive training. Thus finding that stored ranking supervision hurts does not require
re-mining.

### Quality-filtered continuation (no re-mining)

The scored candidate pools are immutable. To test the error-analysis-driven cleanup, add
`--drop-ambiguous-training-queries --drop-contextless-training-queries` to a new training
run. The first option removes train queries whose normalised text points to multiple gold
concepts within one target vocabulary; the second removes generic context-dependent answers
(such as "Yes"/"No") and aliases with fewer than three letters/digits. Neither option changes
the held-out split or production vocabulary data. Use `--quality-review-output` to write a
bounded JSONL queue of exclusions for manual inspection; `run_config.json` records the exact
arguments used. These filters are optional so the original v4 experiment remains reproducible.

For error analysis without retraining, `--evaluate-only` loads `--output-dir/final` and scores
the original full held-out candidate sets. `--evaluation-model` can point to another bundle,
and `--evaluation-result` selects a separate metrics file for a same-set comparison. Add
`--prediction-output` to atomically export one JSONL record per query with the gold rank,
top-five IDs/scores, candidate count, exact-match IDs, and query kind. The primary global
metrics remain strict single-gold scores; the additional `by_exactness` breakdown is
diagnostic and does not silently treat unrelated concepts sharing an alias as equivalent.

To prepare a manual cross-vocabulary mapping review from already-scored data (without
re-mining or changing supervision), run `audit_mapping_conflicts.py --input-dir data/v4/scored
--concept-store-dir data/v4/raw/concepts --output runs/mapping_audit.json --review-output
runs/mapping_review.jsonl`. It flags context-dependent aliases, multiple exact candidates,
and a non-exact mapped gold competing with another exact concept. It intentionally does not
auto-drop these rows: an exact alias on an unrelated gene can be the misleading candidate,
not the mapped disease gold.

### `--skip`/`--limit` control quantity only, not vocabulary balance

Vocabularies are mined as contiguous blocks in a fixed order, so the physical line order in a
shard file is **not** a representative mixed-vocabulary sample -- do not treat "the first N
lines" that way. `train_reranker.py` is responsible for building a properly balanced,
correctly split training set out of whatever shard files it's given (see below); `--skip`/
`--limit` here only ever mean "how much to mine and store in this run".

**Important**: in this default global mode, a `--limit` smaller than the units contributed by
the first few vocabularies (in sorted-prefix order) means later vocabularies get **no
coverage at all** -- no amount of downstream vocabulary-balanced sampling can manufacture data
that was never mined. If you need every target vocabulary guaranteed some coverage regardless
of size (e.g. building a vocabulary-balanced experimental dataset from scratch), use
`--per-vocabulary-limit` instead:

```bash
# Up to 50k query units from EACH vocabulary, not 50k total
python build_training_data.py --output data/balanced-00.jsonl --per-vocabulary-limit 50000
```

In this mode `--skip`/`--limit` apply *within* each vocabulary's own sequence (restarting at
0 per vocabulary) rather than to one global sequence, so the same incremental-growth pattern
below still works, just per vocabulary. The two modes write compatible output and can be
combined across different `--train-data` files if you like; you don't have to pick one forever.

### Growing the dataset later

Don't re-run from `--skip 0` with a bigger `--limit` -- that re-mines everything you already
have. Instead, mine the *next* slice into a new file:

```bash
# 100k -> 500k total
python build_training_data.py --output data/part-01.jsonl --skip 100000 --limit 400000

# 500k -> 1M total
python build_training_data.py --output data/part-02.jsonl --skip 500000 --limit 500000
```

A small `.reranker_vocab_unit_counts.json` cache is written next to your output files; a later
run with a large `--skip` uses it to skip whole vocabularies it doesn't need to touch this
time, instead of reloading every concept just to count past them. Delete it if the underlying
vocabularies have changed (reload/re-embed) since it was written. The concept store for a
vocabulary is (re)written whenever that vocabulary is actually processed (not when the
`--skip` cache short-circuits it), so it stays available across runs.

`train_reranker.py` takes however many of these shard files it needs via `--train-data` (list
several) and `--max-groups` (a target sample size on top of that) -- so growing the dataset
for a bigger training run is just adding another `--train-data` file, nothing upstream needs
to change. There's no separate "held-out" mining step any more -- see "Splitting" below.

The trainer can discover per-vocabulary shards dynamically and mask vocabularies while reading:

```bash
python train_reranker.py \
  --train-data-dir data/by-vocabulary \
  --concept-store-dir data/by-vocabulary/concepts \
  --exclude-vocabularies ohdsi uniprot \
  --output-dir runs/without-ohdsi-uniprot
```

Use ``--include-vocabularies`` for an allow-list. Explicit ``--train-data`` files and multiple
``--train-data-dir`` directories can be combined; duplicate file paths are loaded only once.

### Useful flags

- `--vocabularies snomed hpo mondo` -- restrict to specific vocabularies (default: all loaded).
- `--per-vocabulary-limit` -- see above; guarantees every target vocabulary gets mined.
- `--max-queries-per-concept` (default 4) -- caps how many of a concept's aliases become
  queries. Selection is a deterministic *hash* of `(prefix, concept_id, item_id)`, not the
  front of the alias list, so a concept with many synonyms doesn't get proportionally more
  training weight than one with few, and the same abbreviation/legacy/lay-term aliases aren't
  always the ones left out.
- `--negatives-per-query` (default 8) -- target negatives kept per query, selected in three
  passes: (1) source coverage -- at least one negative primarily surfaced by each of
  lexical/alias_embedding/definition_embedding, where available, so a rank-band-favoured arm
  can't silently crowd out the others; (2) rank-band quotas for the remaining budget (roughly
  a third each from ranks 1-5 / 6-20 / 21-50); (3) backfill from the best remaining candidates
  if a band comes up short.
- `--min-negatives-per-query` (default 1) -- drops a query unit entirely if fewer than this
  many negatives were mined for it, since a zero-negative unit can't be used for contrastive
  training at all (`train_reranker.py` would silently drop it anyway) -- filtering it here
  instead keeps `--limit`/`--per-vocabulary-limit` counting only usable units. The number of
  units actually written can be slightly below the requested limit as a result.
- `--candidate-pool` (default 50, up to 100 is practical) -- how deep each individual recall
  arm is queried before merging/filtering; should comfortably cover the deepest rank band.
- `--concurrency` / `--batch-size` -- how hard this hits your document/vector databases while
  mining; turn down if it's competing with production traffic.

### Relationship-based positives

`mine_cross_vocab_positives.py` turns aliases on one side of an `EXACT` annotation into
positives for the mapped concept on the other side. Its candidate sequence is deterministic,
so large runs can be resumed or split into non-overlapping files using the same pattern as
the vocabulary miner:

```bash
python mine_cross_vocab_positives.py \
  --output data/cross-00.jsonl --skip 0 --limit 100000 \
  --max-total-per-direction 50000
python mine_cross_vocab_positives.py \
  --output data/cross-01.jsonl --skip 100000 --limit 100000 \
  --max-total-per-direction 50000
```

Keep the vocabulary list and all query/cap flags identical between shards. A
`.reranker_relationship_unit_counts.json` manifest next to the output lets later shards skip
whole directions. Delete it when the underlying concepts or mappings change.

By default, a cross-vocabulary query is kept only if at least one target-vocabulary recall arm
retrieves its mapped gold. This is both an end-to-end usefulness requirement for a reranker and
a noise filter: a concept can contain aliases for several senses, even when its mapping is
valid for only one of them. `--no-require-gold-retrieved` restores the permissive legacy
behavior for diagnostics, but is not recommended for training data.

Annotation edges are streamed with the relationship type filtered in the database. When
`--max-total-per-direction` is set, the miner first keeps a deterministic bounded mapping
sample (twice the requested direction cap), then loads only those source concepts and performs
the final alias-level hash selection. This is important for OHDSI↔SNOMED: it preserves a
representative, reproducible capped dataset without constructing a multi-million-edge
NetworkX graph or loading every mapped concept into memory. Uncapped mode still intentionally
means “mine every mapping” and therefore has correspondingly large storage/runtime needs.

### Current scope / known limitations

- Positives currently come only from a concept's own label/synonyms. Definitions are
  candidate-concept *evidence* (they feed the definition-embedding recall arm) and are never
  used as a query or listed as an alternative surface form of the concept.
- Cross-vocabulary positives use only `EXACT` annotations. Broader, narrower, and related
  mappings are intentionally not positive labels.
- Negative mining is same-vocabulary only; no graph-neighbour negatives yet. The negative
  shape (`sources`/`ranks`/`scores`) is deliberately left open to add a `"graph_neighbor"`
  source later without a format change.
- `is_valid_negative(...)` rejects the gold itself and same-vocabulary `REPLACED_BY` concepts;
  hierarchy and approximate mappings remain valid hard negatives.

## 2. Training

On the HPC system, install `pylate`, `sentence-transformers`, `datasets`, and a
CUDA-appropriate `torch` build (follow your cluster's usual PyTorch install instructions --
`torch` itself is deliberately not pinned here since it's CUDA-version-specific). Keep
`concept_rendering.py` alongside `train_reranker.py` -- it has no heavy dependencies itself.

```bash
python train_reranker.py \
  --train-data data/part-00.jsonl \
  --concept-store-dir data/concepts \
  --output-dir runs/reranker-100k \
  --epochs 2 --batch-size 32 --bf16
```

`--help` works without the ML stack installed (imports are lazy), so it's safe to sanity-check
the CLI on a login node before submitting a job.

### Rendering happens here, not in the mining script

`--concept-store-dir` points at the concept-store files the miner wrote. For every sampled
query group, the trainer resolves `gold_concept_id` and each negative's `concept_id` against
that store and renders each into a candidate string via `concept_rendering.render_concept`,
picking one of four variants per candidate (label only / label+aliases / label+definition /
label+aliases+definition) via a deterministic hash of `(--seed, sample_index, query_id,
concept_id)` -- a lightweight field-dropout so the model doesn't learn to depend on any one
field always being present. `sample_index` is the candidate's position in the vocabulary-
balanced *sample*, not a property of the group itself, so a group drawn more than once by
sampling-with-replacement renders differently each time rather than as an identical duplicate.
For the gold concept specifically, the query's own alias text is removed from its synonym list
before rendering (the label itself is never removed), so the model isn't trivially rewarded
for matching a string against itself. **This exclusion is a training-only augmentation** --
the held-out evaluators render the gold concept the exact same way as every negative (no
exclusion), since a real inference-time candidate is never scrubbed of the user's query text;
excluding it there would evaluate something harder/different than actual inference. Synonyms
are also always deduplicated case-insensitively,
and capped at `--max-aliases-rendered` (default 6, deterministically selected -- see
`render_concept`) so a synonym-rich concept can't produce an arbitrarily long candidate string.

Only concept identifiers and retrieval metadata are ever loaded from the mined JSONL --
lexical rank, embedding score, retrieval source, and rank-band are used to select and order
negatives, never injected as text into a candidate. Because rendering is a pure function of
the concept store (not baked into the mined JSONL), changing how candidates are represented
later is a `concept_rendering.py` change, not a re-mining job.

### Preferred-label queries are downsampled

When a query's text exactly equals its gold concept's preferred label (case-insensitively),
the positive candidate would otherwise contain an exact copy of the query -- and exact
preferred-label matching is already handled well by lexical retrieval, so training on it at
full weight mostly teaches a copy-detection shortcut. `--preferred-label-query-keep-probability`
(default 0.1) keeps only that fraction of such groups (chosen deterministically from
`(--seed, query_id)`) as easy anchors and drops the rest. This is a training-only sampling
decision -- the renderer never removes the preferred label itself, at inference time or
otherwise.

### Negatives: distinct, never cycled

A training row requires `--negatives-per-query` (default 4) DISTINCT, resolvable negative
concepts -- mined negatives are never repeated/cycled to pad a short list. A group that can't
resolve enough distinct negatives is dropped and counted under
`skipped_insufficient_resolvable_negatives` (reported when flattening). The miner's own
`--min-negatives-per-query` (default 1) is deliberately permissive -- it only guarantees a
mined dataset is *reusable* at all; whether a given group has *enough* negatives for a
particular training run is entirely this script's `--negatives-per-query` call. If you mine
with a lower `--min-negatives-per-query` than you train with, expect some groups to be dropped
here for exactly that reason -- that's normal, not a bug.

### Splitting: concept-grouped, not row-level

There is no `--eval-data` argument. `train_reranker.py` loads the full pool from
`--train-data` and carves out `--eval-fraction` (default 0.02) of it itself, by hashing each
*(prefix, gold_concept_id)* -- never a row/query -- so different aliases of the same concept
can never end up split across train and eval. This is deliberately more robust than mining a
separate held-out shard via a distinct `--skip` range: a concept's query units are not
guaranteed to fall entirely on one side of an arbitrary mining-window boundary, so that
approach cannot make the same guarantee. `--split-seed` controls which concepts land in eval.

### Vocabulary-balanced sampling

After the split, training rows are drawn from the train-split pool via temperature sampling
across vocabularies: `P(vocab) ~ N_vocab^alpha`, `--vocab-sampling-alpha` default `0.5` (`1.0`
= raw proportional sampling, `0.0` = every vocabulary equally likely regardless of size). This
also **is** how dataset size is controlled now -- `--max-groups` is the target sample size
*after* this reweighting, not a raw truncation of the input files (truncating the raw,
vocab-contiguous file order would just grab whichever vocabularies happen to sort first).
Sampling is with replacement, so a small vocabulary can appear more than once per epoch if its
temperature-weighted target exceeds its own pool size -- that's the point, not a bug. The
script prints per-vocabulary diagnostics for this (total draws, unique query groups, unique
gold concepts, and the resulting duplicate count) so it's obvious when a small vocabulary is
being heavily oversampled, rather than a silent surprise.

### Pre-flight validation

Before touching the (heavy) ML stack, the script fails fast on: `--eval-fraction` outside
`(0, 1)`; `--vocab-sampling-alpha < 0`; `--negatives-per-query < 1`;
`--preferred-label-query-keep-probability` outside `[0, 1]`; both `--bf16` and `--fp16`; and
`--gradient-accumulation-steps != 1` (contrastive training -- with either loss -- is not
compatible with the Trainer's ordinary gradient accumulation; see below). After loading data,
it also fails fast if the concept store resolves less than 50% of the loaded groups' gold
concepts (almost always a `--concept-store-dir`/`--train-data` mismatch) or if no evaluation
candidate set could be built at all.

Before a larger run, audit all input shards together for repeated IDs and query strings that
map to multiple gold concepts:

```bash
python audit_training_data.py data/*.jsonl \
  --output data/training_data_audit.json \
  --review-output data/training_data_ambiguity_review.jsonl
```

The audit is read-only. `--drop-ambiguous-training-queries` can exclude these context-free,
multi-gold strings from training while deliberately leaving them in held-out evaluation. This
is a conservative interim measure; where the IDs are genuinely equivalent, representing them
as a multi-positive gold set is preferable to permanently discarding the query.

`--eval-steps N` now runs the primary full-candidate ranking evaluator on a deterministic,
vocabulary-stratified held-out subset (capped by `--periodic-eval-max-groups`, default 1000),
and reloads the checkpoint with the best candidate-set MRR at the end. `--save-steps` must be
a multiple of `--eval-steps`. A value of zero retains the old final-only evaluation behaviour.
Model construction is explicitly seeded before the randomly initialised ColBERT projection is
created, so the step-zero baseline and repeated runs are reproducible.

### Evaluation: candidate-set ranking is the primary FINAL metric

After training completes, the script builds -- for every held-out query -- the full candidate
set (the gold concept plus *every* distinct resolvable mined negative, not just one), renders
each with one fixed variant (`--eval-render-variant`, default `label_aliases_definition`, no
per-candidate dropout AND no query-alias exclusion -- both gold and negatives are rendered
identically to how a real candidate looks at inference, since query-alias exclusion is a
training-only augmentation), scores them with the trained model, and reports Accuracy@1, MRR,
Recall@3, and Recall@5, both globally and per vocabulary, to `runs/<name>/final_eval_result.json`.
This is the number to compare across dataset-size runs.

Note this only scores the *final* model, not intermediate checkpoints -- for a single-epoch
run (the expected V1 usage: train once per dataset size, compare `final_eval_result.json`
across runs) that's exactly what you want. It does not yet drive checkpoint selection within a
longer, multi-epoch run with periodic saves; if you need that, either evaluate saved
checkpoints offline with this same evaluator, or wire it into periodic in-training evaluation
on a fixed subset -- neither is implemented today.

A cheap `ColBERTTripletEvaluator` (gold vs. one negative, also rendered without query-alias
exclusion) also runs, both periodically during training (`--eval-steps`) and once at the end
(`final_eval_triplet_result.json`) -- treat it only as a fast smoke test, never as the primary
evaluation metric, since it never sees the full candidate set a real query would face.

Before training starts, the script also renders every concept in the store with
`--eval-render-variant` and reports the resulting token-length distribution (p50/p90/p95/p99/
max, and the fraction that would be truncated at `--document-length`) using the model's actual
tokenizer -- it does not change `--document-length` automatically, only warns if truncation
looks significant, so check this output before assuming the default is large enough for your
vocabularies.

### The 100k -> 500k -> 1M workflow

Run once per dataset size, compare `runs/<name>/final_eval_result.json` across runs, and only
grow the dataset if it's still improving:

```bash
python train_reranker.py --train-data data/part-00.jsonl --concept-store-dir data/concepts \
  --output-dir runs/100k --max-groups 100000  ...

python train_reranker.py --train-data data/part-00.jsonl data/part-01.jsonl --concept-store-dir data/concepts \
  --output-dir runs/500k --max-groups 500000  ...

python train_reranker.py --train-data data/part-00.jsonl data/part-01.jsonl data/part-02.jsonl --concept-store-dir data/concepts \
  --output-dir runs/1m --max-groups 1000000  ...
```

Keep `--split-seed` fixed across these runs so the held-out concepts stay the same set as the
training pool grows, making the eval numbers comparable.

### On your specific HPC hardware

- **A100-80 nodes**: use `--bf16`. `--batch-size 32` is a reasonable starting point for a
  BioLORD-2023-sized base model at `--query-length 32 --document-length 64`; raise it if
  memory allows.
- **P100-16 nodes**: use `--fp16` instead of `--bf16` (Pascal has no real bf16 support). Pass
  `--cached-loss` if you want a larger effective contrastive batch/negative count than fits in
  16GB at once: raise `--batch-size` (the logical contrastive batch) and use
  `--cached-mini-batch-size` to control how much of it is physically processed at once --
  `--gradient-accumulation-steps` must stay `1` either way (contrastive training doesn't
  support ordinary gradient accumulation with either loss).
- Start with **1-2 GPUs on a single A100 node** (see `slurm/train_reranker.slurm.example`,
  once you've copied and adapted it -- shorter queue wait, and this model size doesn't need
  more). Only move to multi-node once you've confirmed the dataset-size-vs-improvement curve
  is still climbing and want to train faster on a bigger dataset than 1-2 GPUs can chew through
  in reasonable time. The training script itself needs no changes either way -- multi-GPU/
  multi-node support comes entirely from how it's launched (`torchrun`), which the SLURM
  template's commented-out multi-node section demonstrates.
- Pass `--gather-across-devices` whenever training with more than one GPU/process, so in-batch
  negatives are drawn from the whole batch across GPUs, not just each GPU's local slice.

### SLURM

A baseline `sbatch` script lives at `slurm/train_reranker.slurm.example` -- **not tracked in
git** (see `.gitignore`; every cluster's account/partition/module names differ). Copy it,
replace the `ADAPT`-marked placeholders, and submit with `sbatch`. It includes both the
single-node 1-2 GPU launch (the default) and a commented-out multi-node variant.
