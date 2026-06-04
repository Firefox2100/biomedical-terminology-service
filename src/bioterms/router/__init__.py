"""
Router module for Bioterms application.
"""

_ROUTER_IMPORTS = {
    'auto_complete_router': '.auto_complete',
    'data_router': '.data',
    'expand_router': '.expand',
    'fhir_router': '.fhir',
    'map_router': '.map',
    'misc_router': '.misc',
    'search_router': '.search',
    'similarity_router': '.similarity',
    'trace_router': '.trace',
    'ui_router': '.ui',
    'CacheControlMiddleware': '.utils',
}

__all__ = list(_ROUTER_IMPORTS)


def __getattr__(name):
    if name not in _ROUTER_IMPORTS:
        raise AttributeError(f'module {__name__!r} has no attribute {name!r}')

    from importlib import import_module

    module = import_module(_ROUTER_IMPORTS[name], __name__)
    value = getattr(module, name)
    globals()[name] = value

    return value
