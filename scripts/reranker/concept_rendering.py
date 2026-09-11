"""
Dependency-free concept-text rendering: turns a raw concept record (label/synonyms/
definition) into the candidate string used during training and evaluation. See README.md for
why this lives in its own module (shared with future inference-time rendering) and no
bioterms/torch/database imports are allowed here.
"""
import hashlib
from enum import Enum


class RenderVariant(str, Enum):
    """
    How much of a concept's known text to include when rendering it as a candidate string.
    """
    LABEL_ONLY = 'label_only'
    LABEL_ALIASES = 'label_aliases'
    LABEL_DEFINITION = 'label_definition'
    LABEL_ALIASES_DEFINITION = 'label_aliases_definition'


ALL_VARIANTS: list[RenderVariant] = list(RenderVariant)

DEFAULT_MAX_ALIASES = 6


def _stable_hash_int(*parts: str) -> int:
    """
    Deterministically hash a tuple of strings to an integer, stable across processes/runs.
    :param parts: The strings to hash together (order matters).
    :return: A large non-negative integer.
    """
    digest = hashlib.sha256(':'.join(parts).encode('utf-8')).hexdigest()
    return int(digest[:16], 16)


def render_concept(label: str | None,
                   synonyms: list[str] | None,
                   definition: str | None,
                   variant: RenderVariant,
                   exclude_aliases: set[str] | None = None,
                   max_aliases: int | None = DEFAULT_MAX_ALIASES,
                   alias_selection_key: str | None = None,
                   ) -> str:
    """
    Render a concept's (label, synonyms, definition) into a single candidate string.

    The label is always included and never excluded/capped. Synonyms are deduplicated
    case-insensitively, `exclude_aliases` is removed (case-insensitive), and if more than
    `max_aliases` remain, a deterministic subset is kept via a stable hash of
    `(alias_selection_key, alias)` -- selection is reproducible per concept, not iteration-order.
    :param exclude_aliases: Synonym strings to omit (e.g. the query's own alias text, a
        training-only augmentation -- see README).
    :param max_aliases: Cap on rendered synonyms, or None for no cap.
    :param alias_selection_key: Stable per-concept key (e.g. "<prefix>:<concept_id>") used to
        pick which aliases survive the cap.
    """
    exclude_folded = {a.strip().casefold() for a in (exclude_aliases or set())}

    deduped: list[str] = []
    seen: set[str] = set()
    for synonym in (synonyms or []):
        if not synonym or not synonym.strip():
            continue
        folded = synonym.strip().casefold()
        if folded in exclude_folded or folded in seen:
            continue
        seen.add(folded)
        deduped.append(synonym)

    if max_aliases is not None and len(deduped) > max_aliases:
        key_prefix = alias_selection_key or ''
        ranked = sorted(deduped, key=lambda s: _stable_hash_int(key_prefix, s.casefold()))
        keep = {s.casefold() for s in ranked[:max_aliases]}
        deduped = [s for s in deduped if s.casefold() in keep]

    parts: list[str] = []
    if label:
        parts.append(label)

    if variant in (RenderVariant.LABEL_ALIASES, RenderVariant.LABEL_ALIASES_DEFINITION) and deduped:
        parts.append('(' + '; '.join(deduped) + ')')

    if variant in (RenderVariant.LABEL_DEFINITION, RenderVariant.LABEL_ALIASES_DEFINITION) and definition:
        parts.append(definition)

    return ' '.join(parts).strip()
