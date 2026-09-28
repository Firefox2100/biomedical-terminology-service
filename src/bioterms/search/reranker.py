"""Lazy ColBERT reranking for terminology search candidates."""
import asyncio
import hashlib
from collections.abc import Sequence

from bioterms.etc.consts import CONFIG
from bioterms.model.concept import Concept


_RERANKER = None
_LOAD_LOCK = asyncio.Lock()
_INFERENCE_LOCK = asyncio.Lock()


def _stable_hash_int(*parts: str) -> int:
    digest = hashlib.sha256(':'.join(parts).encode('utf-8')).hexdigest()
    return int(digest[:16], 16)


def render_reranker_candidate(concept: Concept) -> str:
    """Match the label+aliases+definition representation used by training evaluation."""
    synonyms = []
    seen = set()
    for synonym in concept.synonyms or []:
        if not synonym or not synonym.strip():
            continue
        folded = synonym.strip().casefold()
        if folded in seen:
            continue
        seen.add(folded)
        synonyms.append(synonym)

    max_aliases = CONFIG.reranker_max_aliases or None
    if max_aliases is not None and len(synonyms) > max_aliases:
        key = f'{concept.prefix.value}:{concept.concept_id}'
        ranked = sorted(synonyms, key=lambda value: _stable_hash_int(key, value.casefold()))
        keep = {value.casefold() for value in ranked[:max_aliases]}
        synonyms = [value for value in synonyms if value.casefold() in keep]

    parts = []
    if concept.label:
        parts.append(concept.label)
    if synonyms:
        parts.append('(' + '; '.join(synonyms) + ')')
    if concept.definition:
        parts.append(concept.definition)
    return ' '.join(parts).strip()


def reranker_enabled() -> bool:
    return bool(CONFIG.reranker_model)


def _load_reranker():
    from sentence_transformers import MultiVectorEncoder
    model = MultiVectorEncoder(
        model_name_or_path=CONFIG.reranker_model,
        device=CONFIG.torch_device,
        trust_remote_code=True,
    )
    transformer = model[0]
    if transformer.query_length is None and transformer.query_expansion is not None:
        transformer.query_length = transformer.query_expansion['length']
    return model


async def get_reranker():
    """Load a local bundle or Hugging Face repository once, on first semantic search."""
    global _RERANKER
    if _RERANKER is not None:
        return _RERANKER
    async with _LOAD_LOCK:
        if _RERANKER is None:
            _RERANKER = await asyncio.to_thread(_load_reranker)
    return _RERANKER


def _rerank_sync(model, query: str, concepts: Sequence[Concept]) -> list[Concept]:
    texts = [render_reranker_candidate(concept) for concept in concepts]
    query_embeddings = model.encode_query(
        [query], batch_size=CONFIG.reranker_batch_size,
        show_progress_bar=False,
    )
    document_embeddings = model.encode_document(
        texts, batch_size=CONFIG.reranker_batch_size,
        show_progress_bar=False,
    )
    scores = model.similarity(query_embeddings, document_embeddings)[0]
    order = scores.argsort(descending=True).tolist()
    by_id = {concept.concept_id: concept for concept in concepts}
    return [by_id[concepts[index].concept_id] for index in order]


async def rerank_concepts(query: str, concepts: Sequence[Concept]) -> list[Concept]:
    """Rerank non-exact semantic candidates without blocking the ASGI event loop."""
    if len(concepts) < 2 or not reranker_enabled():
        return list(concepts)
    model = await get_reranker()
    # Serialize inference on the shared model. This avoids concurrent GPU forwards from
    # separate requests while still allowing database recall to run concurrently.
    async with _INFERENCE_LOCK:
        return await asyncio.to_thread(_rerank_sync, model, query, concepts)
