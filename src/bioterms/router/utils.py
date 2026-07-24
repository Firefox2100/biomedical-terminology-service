"""
Utility functions for FastAPI route handlers.
"""

import re
import posixpath
import hashlib
import hmac
import base64
import importlib.resources as pkg_resources
from datetime import datetime
from typing import AsyncIterator, Optional
from urllib.parse import urlsplit, urlunsplit
from pydantic import BaseModel
from fastapi import FastAPI, Request, Depends, HTTPException, status
from fastapi.templating import Jinja2Templates
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

from bioterms.etc.consts import CONFIG
from bioterms.database import DocumentDatabase, get_active_doc_db, get_active_cache


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
BEARER_SECURITY = HTTPBearer()

HTTP_DATE_FORMAT = '%a, %d %b %Y %H:%M:%S GMT'
VOCABULARY_CACHE_CONTROL = 'public, max-age=86400, stale-while-revalidate=172800'


class CacheControlMiddleware(BaseHTTPMiddleware):
    def __init__(self,
                 app: FastAPI,
                 ):
        super().__init__(app)

    @staticmethod
    def _is_cacheable_vocabulary_path(path: str) -> bool:
        return path.startswith('/api/vocabularies') and not path.endswith('/random')

    @staticmethod
    def _not_modified_response(etag: str,
                               last_modified: datetime,
                               ) -> Response:
        return Response(
            status_code=status.HTTP_304_NOT_MODIFIED,
            headers={
                'ETag': etag,
                'Last-Modified': last_modified.strftime(HTTP_DATE_FORMAT),
                'Cache-Control': VOCABULARY_CACHE_CONTROL,
            }
        )

    def _check_not_modified(self,
                            request: Request,
                            last_modified: datetime,
                            etag: str,
                            ) -> Optional[Response]:
        """
        Check the conditional request headers against the dataset's last-modified time and
        etag, returning a 304 response if the client's cached copy is still fresh.
        """
        if_not_match = request.headers.get('If-None-Match')
        if if_not_match is not None:
            etag_values = [v.strip() for v in if_not_match.split(',')]
            if etag in etag_values or '*' in etag_values:
                return self._not_modified_response(etag, last_modified)

        if_modified_since = request.headers.get('If-Modified-Since')
        if if_modified_since is not None:
            try:
                ims_date = datetime.strptime(if_modified_since, HTTP_DATE_FORMAT)
                if last_modified <= ims_date:
                    return self._not_modified_response(etag, last_modified)
            except Exception:
                pass

        return None

    async def dispatch(self,
                       request: Request,
                       call_next: RequestResponseEndpoint,
                       ) -> Response:
        if request.method not in ('GET', 'HEAD'):
            response = await call_next(request)
            return response

        cache = get_active_cache()
        last_modified = None
        etag = None
        cacheable = self._is_cacheable_vocabulary_path(request.url.path)

        if cacheable:
            last_modified = await cache.get_dataset_last_modified()
            etag = hashlib.sha1(last_modified.isoformat().encode()).hexdigest()

            not_modified = self._check_not_modified(request, last_modified, etag)
            if not_modified is not None:
                return not_modified

        response = await call_next(request)

        if response.status_code != 200 or not cacheable:
            return response

        response.headers['Cache-Control'] = VOCABULARY_CACHE_CONTROL
        response.headers['Last-Modified'] = last_modified.strftime(HTTP_DATE_FORMAT)
        response.headers['ETag'] = etag

        return response


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


async def login_optional(request: Request,
                         db: DocumentDatabase = Depends(get_active_doc_db),
                         ) -> str | None:
    """
    A dependency to optionally get the logged-in user.
    :param request: The FastAPI request object.
    :param db: The DocumentDatabase instance.
    :return: The username of the logged-in user, or None if not logged in.
    """
    username = request.session.get('username')

    if username:
        user = await db.users.get(username)
        if not user:
            request.session['username'] = None
            return None

        return username

    return None


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


async def api_key_required(credentials: HTTPAuthorizationCredentials = Depends(BEARER_SECURITY),
                           db: DocumentDatabase = Depends(get_active_doc_db),
                           ) -> str:
    """
    A dependency to ensure that a valid API key is provided.
    :param credentials: HTTPAuthorizationCredentials instance.
    :param db: The DocumentDatabase instance.
    :return: The username who owns the API key.
    """
    api_key = credentials.credentials

    server_key_bytes = base64.b64decode(CONFIG.server_hmac_key)
    api_key_hash = hmac.new(
        server_key_bytes,
        api_key.encode(),
        hashlib.sha256
    ).hexdigest()

    user = await db.users.get_user_by_api_key(api_key_hash)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail='Invalid API key',
        )

    return user.username
