import re
import posixpath
import importlib.resources as pkg_resources
from typing import AsyncIterator
from urllib.parse import urlsplit, urlunsplit
from pydantic import BaseModel
from fastapi import Request, Depends, HTTPException, status
from fastapi.templating import Jinja2Templates

from bioterms.database import DocumentDatabase, get_active_doc_db


_allowed_redirect_destinations = [
    '/',
    '/vocabularies',
    '/api-keys',
    '/api-keys/new',
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
        nav_links.extend([
            {
                'label': 'API Keys',
                'url': request.url_for('get_api_keys_page'),
                'active': path.startswith('/api-keys'),
            },
            {
                'label': f'Logout ({username})',
                'url': request.url_for('handle_logout'),
                'active': False
            },
        ])
    else:
        nav_links.append({
            'label': 'Login',
            'url': str(request.url_for('get_login_page')) + f'?next={path}',
            'active': False,
        })

    return nav_links


def build_structured_data(base_url: str) -> list[dict]:
    """
    Build structured data for the web page.
    :param base_url: The base URL of the web application.
    :return: A list of structured data dictionaries.
    """
    homepage_url = base_url + '/'

    website_ld = {
        '@context': 'https://schema.org',
        '@type': 'WebSite',
        'name': 'BioMedical Terminology Service',
        'url': homepage_url,
        'description': 'A service for using with biomedical terminologies, '
                       'such as ontologies or vocabularies.',
    }

    organisation_ld = {
        '@context': 'https://schema.org',
        '@type': 'Organization',
        'name': 'BioMedical Terminology Service',
        'url': homepage_url,
        'creator': {
            '@type': 'Person',
            'name': 'Patrick Wang (@Firefox2100)',
        },
    }

    webapp_ld = {
        '@context': 'https://schema.org',
        '@type': 'WebApplication',
        'name': 'BioMedical Terminology Service',
        'url': homepage_url,
        'applicationCategory': 'MedicalApplication',
        'operatingSystem': 'All',
        'description': 'A service for using with biomedical terminologies, '
                       'such as ontologies or vocabularies.',
    }

    structured_data = [
        website_ld,
        organisation_ld,
        webapp_ld,
    ]

    return structured_data


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


async def login_required(request: Request,
                         db: DocumentDatabase = Depends(get_active_doc_db),
                         ) -> str:
    """
    A dependency to ensure that the user is logged in.
    :param request: The FastAPI request object.
    :param db: The DocumentDatabase instance.
    :return: The username of the logged-in user.
    :raises HTTPException: If the user is not logged in.
    """
    username = request.session.get('username')
    if username:
        user = await db.users.get(username)
        if not user:
            request.session['username'] = None
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail='Authentication required',
            )

        return username

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail='Authentication required',
    )
