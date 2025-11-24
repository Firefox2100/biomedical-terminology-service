import re
import posixpath
import importlib.resources as pkg_resources
from typing import AsyncIterator
from urllib.parse import urlsplit, urlunsplit
from pydantic import BaseModel
from fastapi import Request
from fastapi.templating import Jinja2Templates

from bioterms.database import DocumentDatabase


_allowed_redirect_destinations = [
    '/',
    '/vocabularies',
    '/login',
]
_allowed_redirect_regex = [
    re.compile(
        r'^/vocabularies/[a-zA-Z0-9_\-]+$'
    )
]


async def response_generator(data_iter: AsyncIterator[BaseModel],
                             ) -> AsyncIterator[bytes]:
    """
    An asynchronous generator that yields JSON-encoded data from an async iterator.
    :param data_iter: An asynchronous iterator yielding Pydantic BaseModel instances.
    :return: An asynchronous iterator yielding bytes.
    """
    yield b'['
    first = True

    async for concept in data_iter:
        if not first:
            yield b',\n'
        else:
            first = False

        yield concept.model_dump_json().encode()

    yield b']'


async def build_nav_links(request: Request, db: DocumentDatabase) -> list[dict]:
    """
    Build the navigation links in the nav bar for the UI.
    :return: A list of navigation link dictionaries.
    """
    path = request.url.path
    username = request.session.get('username')

    if username:
        user = await db.users.get(username)

        if not user:
            username = None
            request.session['username'] = None

    nav_links = [
        {
            'label': 'Home',
            'url': request.url_for('get_home_page'),
            'active': path == '/',
            'external': False,
        },
        {
            'label': 'Vocabularies',
            'url': request.url_for('list_vocabularies'),
            'active': path.startswith('/vocabularies'),
            'external': False,
        },
        {
            'label': 'OpenAPI',
            'url': '/docs',
            'active': False,
            'external': True,
        },
        {
            'label': 'GraphiQL',
            'url': '/api/graphql',
            'active': False,
            'external': True,
        }
    ]

    if username:
        nav_links.append({
            'label': f'Logout ({username})',
            'url': request.url_for('handle_logout'),
            'active': False
        })
    else:
        nav_links.append({
            'label': 'Login',
            'url': str(request.url_for('get_login_page')) + f'?next={path}',
        })

    return nav_links


def get_templates() -> Jinja2Templates:
    """
    Get the Jinja2 templates instance for rendering HTML templates.
    :return:
    """
    template_path = pkg_resources.files('bioterms.data') / 'templates'
    return Jinja2Templates(directory=str(template_path))


TEMPLATES = get_templates()


def sanitise_next_url(next_url: str | None = None,
                      default: str = '/',
                      ) -> str:
    """
    Sanitise the next URL to prevent open redirect vulnerabilities.
    :param next_url: The next URL to sanitise.
    :param default: The default URL to use if the next URL is not valid.
    :return: The sanitised next URL.
    """
    if not next_url:
        return default

    parts = urlsplit(next_url)

    if parts.scheme or parts.netloc:
        return default

    normalised = posixpath.normpath(parts.path or '/')
    if not normalised.startswith('/'):
        normalised = '/' + normalised

    while normalised.startswith('//'):
        normalised = normalised[1:]

    def _is_allowed(path: str) -> bool:
        if path in _allowed_redirect_destinations:
            return True

        return any(regex.match(path) for regex in _allowed_redirect_regex)

    if not _is_allowed(normalised):
        return default

    clean = urlunsplit(('', '', normalised, parts.query, ''))

    return clean
