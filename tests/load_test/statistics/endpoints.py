"""Request definitions shared by the statistics scripts."""

from __future__ import annotations

import random
import string

from locust.contrib.fasthttp import FastHttpUser

from load_test.statistics.benchmark import (
    Endpoint,
    RequestSpec,
    all_prefixes,
    concept_ids,
)


def _query(rng: random.Random, maximum: int = 15) -> str:
    return "".join(rng.choices(string.ascii_lowercase, k=rng.randint(3, maximum)))


def _ids(user: FastHttpUser, prefix: str, rng: random.Random, maximum: int) -> list[str]:
    return concept_ids(user, prefix, rng.randint(1, maximum))


AUTO_COMPLETE = (
    Endpoint("auto-complete", "v1", lambda u, p, r: RequestSpec(
        "GET", f"/api/vocabularies/{p}/auto-complete/v1/query/{_query(r)}",
        {"long": str(r.random() < .1).lower()})),
    Endpoint("auto-complete", "v2", lambda u, p, r: RequestSpec(
        "GET", f"/api/vocabularies/{p}/auto-complete/v2",
        {"query": _query(r), "result_threshold": r.randint(20, 100),
         "with_definition": str(r.random() < .3).lower()})),
    Endpoint("auto-complete", "v3", lambda u, p, r: RequestSpec(
        "GET", f"/api/vocabularies/{p}/auto-complete/v3",
        {"query": _query(r), "limit": r.randint(20, 100)})),
)

EXPANSION = (
    Endpoint("expansion", "v1", lambda u, p, r: RequestSpec(
        "POST", f"/api/vocabularies/{p}/expand/v1",
        {"depth": r.randint(0, 10), "result_threshold": r.randint(50, 500)},
        {"termIds": _ids(u, p, r, 100)}), True),
    Endpoint("expansion", "v2", lambda u, p, r: RequestSpec(
        "GET", f"/api/vocabularies/{p}/expand/v2",
        {"concept_ids": _ids(u, p, r, 100), "depth": r.randint(0, 10),
         "limit": r.randint(50, 500)}), True),
)

MAPPING = (
    Endpoint("mapping", "v1", lambda u, p, r: RequestSpec(
        "POST", f"/api/vocabularies/{p}/map/v1/{r.choice(all_prefixes())}",
        {"result_threshold": r.randint(50, 500)},
        {"termIds": _ids(u, p, r, 100)}), True),
    Endpoint("mapping", "v2", lambda u, p, r: RequestSpec(
        "GET", f"/api/vocabularies/{p}/map/v2/{r.choice(all_prefixes())}",
        {"concept_ids": _ids(u, p, r, 100), "max_hops": r.randint(1, 3),
         "limit": r.randint(50, 500)}), True),
)

SEARCH = (Endpoint("search", "v1", lambda u, p, r: RequestSpec(
    "GET", f"/api/vocabularies/{p}/search/v1",
    {"query": _query(r, 40), "limit": r.randint(10, 100)})),)

SIMILARITY = (
    Endpoint("similarity", "v1", lambda u, p, r: RequestSpec(
        "POST", f"/api/vocabularies/{p}/similarity/v1",
        {"result_threshold": r.randint(50, 500)},
        {"termIds": _ids(u, p, r, 20), "threshold": r.uniform(.5, 1)}), True),
    Endpoint("similarity", "v2", lambda u, p, r: RequestSpec(
        "GET", f"/api/vocabularies/{p}/similarity/v2",
        {"concept_ids": ",".join(_ids(u, p, r, 20)),
         "threshold": r.uniform(.5, 1), "same_prefix": str(r.random() < .7).lower(),
         "limit": r.randint(50, 500)}), True),
)

TRANSLATION = (
    Endpoint("translation", "v1", lambda u, p, r: RequestSpec(
        "POST", f"/api/vocabularies/{p}/translate/v1",
        {"result_threshold": r.randint(50, 500)},
        {"termIds": _ids(u, p, r, 100), "constraintIds": _ids(u, p, r, 100),
         "threshold": r.uniform(.5, 1)}), True),
    Endpoint("translation", "v2", lambda u, p, r: RequestSpec(
        "GET", f"/api/vocabularies/{p}/translate/v2",
        {"original_ids": _ids(u, p, r, 100),
         "constraint_concepts": _ids(u, p, r, 100),
         "threshold": r.uniform(.5, 1), "limit": r.randint(50, 500)}), True),
)

ALL_SUITES = {
    "auto-complete": AUTO_COMPLETE,
    "expansion": EXPANSION,
    "mapping": MAPPING,
    "search": SEARCH,
    "similarity": SIMILARITY,
    "translation": TRANSLATION,
}
