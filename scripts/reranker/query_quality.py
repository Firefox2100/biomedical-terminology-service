"""Conservative, context-free query-quality checks for reranker training.

These checks operate on immutable mined/scored groups. They do not remove concepts, aliases,
or mappings from the production databases. Ambiguous text should be resolved with context or
verified multi-positive supervision rather than arbitrarily assigning one target.
"""

from __future__ import annotations

import unicodedata


CONTEXT_DEPENDENT_RESPONSES = frozenset({
    'yes', 'no', 'unknown', 'not known', 'not applicable', 'n/a',
    'none', 'other', 'positive', 'negative', 'present', 'absent',
    'normal', 'abnormal', 'true', 'false',
})


def normalise_query(text: str) -> str:
    return ' '.join(text.casefold().split())


def contextless_query_reason(text: str) -> str | None:
    """Flag aliases with too little intrinsic information to identify one concept."""
    normalised = normalise_query(text)
    if normalised in CONTEXT_DEPENDENT_RESPONSES:
        return 'context_dependent_response'
    letters_or_digits = sum(
        unicodedata.category(character)[0] in ('L', 'N')
        for character in normalised
    )
    if letters_or_digits < 3:
        return 'fewer_than_three_alphanumeric_characters'
    return None


def training_quality_reasons(group: dict,
                             ambiguous_keys: set[tuple[str, str]],
                             drop_ambiguous: bool,
                             drop_contextless: bool,
                             ) -> list[str]:
    reasons = []
    key = (group['prefix'], normalise_query(group['query']))
    if drop_ambiguous and key in ambiguous_keys:
        reasons.append('multiple_gold_concepts')
    if drop_contextless:
        reason = contextless_query_reason(group['query'])
        if reason:
            reasons.append(reason)
    return reasons
