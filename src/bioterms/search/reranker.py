"""Lazy ColBERT reranking for terminology search candidates."""
import asyncio
import hashlib
import json
from collections.abc import Sequence
from pathlib import Path

from bioterms.etc.consts import CONFIG, LOGGER
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


def _legacy_bundle_config() -> bool:
    """Detect bundles serialized before the SentenceTransformers 6 module format."""
    source = CONFIG.reranker_model
    config_path = Path(source) / 'config_sentence_transformers.json'
    if not config_path.exists() and not Path(source).exists():
        try:
            from huggingface_hub import hf_hub_download
            config_path = Path(hf_hub_download(source, 'config_sentence_transformers.json'))
        except Exception:
            return False
    if not config_path.exists():
        return False
    try:
        version = json.loads(config_path.read_text(encoding='utf-8')).get('__version__', {}) \
            .get('sentence_transformers', '')
        return bool(version) and int(version.split('.', 1)[0]) < 6
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return False


def _load_reranker():
    from pylate import models
    if _legacy_bundle_config():
        return _load_legacy_reranker(models)
    try:
        return models.ColBERT(
            model_name_or_path=CONFIG.reranker_model,
            device=CONFIG.torch_device,
        )
    except KeyError as error:
        # PyLate 1.6 / SentenceTransformers 6 changed Dense's serialized activation field.
        # Bundles trained with PyLate 1.2 contain the correct projection weights but can hit
        # that upstream compatibility error. Load their trusted, operator-configured modules
        # with SentenceTransformer and wrap them without changing weights.
        if error.args != ('activation_function',):
            raise
        LOGGER.warning(
            'Using the legacy PyLate bundle compatibility loader for reranker %s.',
            CONFIG.reranker_model,
        )
        return _load_legacy_reranker(models)


def _load_legacy_reranker(models):
    from sentence_transformers import SentenceTransformer
    LOGGER.warning(
        'Using the legacy PyLate bundle compatibility loader for reranker %s.',
        CONFIG.reranker_model,
    )
    sentence_model = SentenceTransformer(
        CONFIG.reranker_model,
        device=CONFIG.torch_device,
        trust_remote_code=True,
    )
    model = models.ColBERT(
        modules=list(sentence_model._modules.values()),
        device=CONFIG.torch_device,
        query_length=CONFIG.reranker_query_length,
        document_length=CONFIG.reranker_document_length,
    )
    # PyLate 1.6 still calls the SentenceTransformers 5-era private name while
    # SentenceTransformers 6 renamed it. Keep this compatibility local to the model.
    if not hasattr(model, '_text_length') and hasattr(model, '_input_length'):
        model._text_length = model._input_length
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
    from pylate import rank

    texts = [render_reranker_candidate(concept) for concept in concepts]
    query_embeddings = model.encode(
        [query], is_query=True, batch_size=CONFIG.reranker_batch_size,
        show_progress_bar=False,
    )
    document_embeddings = model.encode(
        [texts], is_query=False, batch_size=CONFIG.reranker_batch_size,
        show_progress_bar=False,
    )
    result = rank.rerank(
        documents_ids=[[concept.concept_id for concept in concepts]],
        queries_embeddings=query_embeddings,
        documents_embeddings=document_embeddings,
        device=str(model.device),
    )[0]
    by_id = {concept.concept_id: concept for concept in concepts}
    return [by_id[str(item['id'])] for item in result if str(item['id']) in by_id]


async def rerank_concepts(query: str, concepts: Sequence[Concept]) -> list[Concept]:
    """Rerank non-exact semantic candidates without blocking the ASGI event loop."""
    if len(concepts) < 2 or not reranker_enabled():
        return list(concepts)
    model = await get_reranker()
    # Serialize inference on the shared model. This avoids concurrent GPU forwards from
    # separate requests while still allowing database recall to run concurrently.
    async with _INFERENCE_LOCK:
        return await asyncio.to_thread(_rerank_sync, model, query, concepts)
