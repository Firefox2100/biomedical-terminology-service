"""
Router module for Bioterms application.
"""

from .auto_complete import auto_complete_router
from .data import data_router
from .expand import expand_router
from .map import map_router
from .misc import misc_router
from .search import search_router
from .similarity import similarity_router
from .trace import trace_router
from .ui import ui_router
from .utils import CacheControlMiddleware
