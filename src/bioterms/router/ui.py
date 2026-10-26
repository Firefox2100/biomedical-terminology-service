"""
UI Router for the BioMedical Terminology Service.

This router handles the web interface endpoints, serving HTML pages for
various functionalities such as home page, login, API key management,
vocabulary listing, and vocabulary details.
"""

import base64
import hashlib
import hmac
import secrets
from typing import Annotated
from urllib.parse import urlencode
from uuid import UUID

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from markdown import markdown

from bioterms.annotation import get_annotation_status
from bioterms.database import (
    Cache,
    DocumentDatabase,
    GraphDatabase,
    get_active_cache,
    get_active_doc_db,
    get_active_graph_db,
)
from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix
from bioterms.model.annotation_status import AnnotationStatus
from bioterms.model.user import UserApiKey
from bioterms.similarity import get_similarity_status
from bioterms.task.cache import rebuild_cache_task
from bioterms.vocabulary import (
    get_vocabulary_config,
    get_vocabulary_license,
    get_vocabulary_status,
)

from .utils import (
    LD_CONTEXT,
    LD_TYPE,
    SCHEMA_ORG,
    TEMPLATES,
    build_nav_links,
    build_structured_data,
    login_optional,
    login_required,
    sanitise_next_url,
)

ui_router = APIRouter(
    tags=['UI'],
)

PRIVATE_RESPONSE_HEADERS = {
    'Cache-Control': 'no-store',
    'Pragma': 'no-cache',
}

# Root concepts used by the progressively enhanced hierarchy browser.  Keeping these in the
# server-rendered UI means the first useful view does not depend on GraphQL introspection or a
# warmed status cache. Vocabularies without a stable hierarchy still get the same search-first UI.
VOCABULARY_ROOTS: dict[ConceptPrefix, list[str]] = {
    ConceptPrefix.CTV3: ['.....'],
    ConceptPrefix.HPO: ['0000001'],
    ConceptPrefix.MONDO: ['0000001', '0021125', '0021178', '0042489'],
    ConceptPrefix.NCIT: [
        'C12218', 'C12219', 'C12913', 'C14250', 'C16612', 'C17828', 'C1908',
        'C20047', 'C20181', 'C20189', 'C20633', 'C22187', 'C22188', 'C26548',
        'C28428', 'C3910', 'C43431', 'C7057', 'C97325',
    ],
    ConceptPrefix.OMIM: ['MTHU000209', 'MTHU000057', 'MTHU000109'],
    ConceptPrefix.ORDO: ['C001', 'C010', 'C041'],
    ConceptPrefix.SNOMED: ['138875005'],
    ConceptPrefix.UBERON: ['0000104', '0000105', '0001062', '0035943'],
}


def _vocabulary_catalogue() -> list[dict]:
    """Return static vocabulary metadata without touching database or cache state."""
    return [get_vocabulary_config(prefix) for prefix in ConceptPrefix]


@ui_router.get(
    '/',
    response_class=HTMLResponse,
    responses={500: {'description': 'Failed to load vocabulary status for the home page.'}},
)
async def get_home_page(request: Request,
                        doc_db: Annotated[DocumentDatabase, Depends(get_active_doc_db)],
                        ):
    """
    Serve the home page of the BioMedical Terminology Service.
    \f
    :param request: Request object.
    :param doc_db: Document database instance.
    :return: An HTML response with the home page content.
    """
    try:
        nav_links = await build_nav_links(request, doc_db)
        base_url = str(request.base_url).rstrip('/')
        structured_data = build_structured_data(base_url)

        return TEMPLATES.TemplateResponse(
            request=request,
            name='home.html',
            context={
                'page_title': 'Home | BioMedical Terminology Service',
                'vocabularies': _vocabulary_catalogue(),
                'nav_links': nav_links,
                'google_site_verification_id': CONFIG.google_site_verification_id,
                'structured_data': structured_data,
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@ui_router.get('/login', response_class=HTMLResponse)
async def get_login_page(request: Request,
                         doc_db: Annotated[DocumentDatabase, Depends(get_active_doc_db)],
                         next_url: Annotated[str | None, Query(alias='next')] = None,
                         error: Annotated[str | None, Query()] = None,
                         ):
    """
    Serve the login page for the BioMedical Terminology Service.
    \f
    :param request: Request object.
    :param next_url: The URL to redirect to after successful login.
    :param error: An optional error message to display on the login page.
    :param doc_db: Document database instance.
    :return: An HTML response with the login page content.
    """
    sanitised_next_url = sanitise_next_url(next_url)

    if request.session.get('username'):
        # User is already logged in, redirect to next_url or home page
        redirect_url = _resolve_redirect_url(request, sanitised_next_url)
        return RedirectResponse(url=redirect_url, status_code=status.HTTP_303_SEE_OTHER)

    nav_links = await build_nav_links(request, doc_db)

    return TEMPLATES.TemplateResponse(
        request=request,
        name='login.html',
        context={
            'page_title': 'Login | BioMedical Terminology Service',
            'next_url': sanitised_next_url,
            'error': error,
            'nav_links': nav_links,
        },
        headers=PRIVATE_RESPONSE_HEADERS,
    )


def _login_redirect(request: Request,
                    next_url: str | None,
                    error: str | None = None,
                    ) -> RedirectResponse:
    """
    Build a redirect back to the login page, preserving the next_url and an optional error.
    :param request: The current request.
    :param next_url: The URL to redirect to after a subsequent successful login, if any.
    :param error: An optional error message to display on the login page.
    :return: A redirect response to the login page.
    """
    params = {}
    if error:
        params['error'] = error

    if next_url:
        params['next'] = next_url

    url = request.url_for('get_login_page')

    if params:
        url = f'{url}?{urlencode(params)}'

    return RedirectResponse(url, status_code=status.HTTP_303_SEE_OTHER)


def _resolve_redirect_url(request: Request,
                          next_url: str | None,
                          ) -> str:
    """
    Resolve an allow-listed local path through named application routes.

    Redirect responses receive only URLs generated by the router, never request data. This
    keeps the trust boundary explicit even if the path allow-list changes independently.

    :param request: The current request, used to generate application URLs.
    :param next_url: The requested local path.
    :return: A router-generated URL, defaulting to the home page.
    """
    destinations = {
        '/': request.url_for('get_home_page').path,
        '/vocabularies': request.url_for('list_vocabularies').path,
        '/api-keys': request.url_for('get_api_keys_page').path,
        '/api-keys/new': request.url_for('create_new_api_key').path,
        '/login': request.url_for('get_login_page').path,
    }
    destinations.update({
        f'/vocabularies/{prefix.value}': request.url_for(
            'get_vocabulary_info',
            prefix=prefix.value,
        ).path
        for prefix in ConceptPrefix
    })

    return str(destinations.get(next_url, destinations['/']))


@ui_router.post('/login', response_class=RedirectResponse)
async def post_login_credentials(request: Request,
                                 username: Annotated[str, Form()],
                                 password: Annotated[str, Form()],
                                 doc_db: Annotated[DocumentDatabase, Depends(get_active_doc_db)],
                                 next_url: Annotated[str | None, Form(alias='next')] = None,
                                 ):
    """
    Process login credentials and authenticate the user.
    \f
    :param request: The incoming request object.
    :param next_url: The URL to redirect to after successful login.
    :param username: The username provided by the user.
    :param password: The password provided by the user.
    :param doc_db: Document database instance.
    :return: A redirect response to the next URL or home page upon successful login,
        or back to the login page with an error message upon failure.
    """
    sanitised_next_url = sanitise_next_url(next_url)

    # Empty credentials never reach this point: FastAPI rejects a blank required Form() field
    # with a 422 before the route runs.
    user = await doc_db.users.get(username)

    if not user or not user.validate_password(password):
        return _login_redirect(request, sanitised_next_url, 'Invalid username or password')

    request.session['username'] = username

    dest = _resolve_redirect_url(request, sanitised_next_url)
    return RedirectResponse(dest, status_code=status.HTTP_303_SEE_OTHER)


@ui_router.post('/logout', response_class=RedirectResponse)
async def handle_logout(request: Request):
    """
    Logout a user
    :param request: Request object
    :return: Redirection to the login page
    """
    request.session.clear()

    return RedirectResponse(
        request.url_for('get_login_page'),
        status_code=status.HTTP_303_SEE_OTHER
    )


@ui_router.get('/api-keys', response_class=HTMLResponse)
async def get_api_keys_page(request: Request,
                            doc_db: Annotated[DocumentDatabase, Depends(get_active_doc_db)],
                            username: Annotated[str, Depends(login_required)],
                            ):
    """
    Serve the API keys management page.
    \f
    :param request: The incoming request object.
    :param doc_db: Document database instance.
    :param username: The username of the logged-in user.
    :return: An HTML response with the API keys management page.
    """
    user = await doc_db.users.get(username)

    nav_links = await build_nav_links(request, doc_db)

    return TEMPLATES.TemplateResponse(
        request=request,
        name='api_keys.html',
        context={
            'page_title': 'API Keys | BioMedical Terminology Service',
            'api_keys': user.api_keys if user and user.api_keys else [],
            'nav_links': nav_links,
        },
        headers=PRIVATE_RESPONSE_HEADERS,
    )


@ui_router.get('/api-keys/new', response_class=HTMLResponse)
async def create_new_api_key(request: Request,
                             doc_db: Annotated[DocumentDatabase, Depends(get_active_doc_db)],
                             _: Annotated[str, Depends(login_required)],
                             ):
    """
    Create a new API key for the logged-in user.
    :param request: The incoming request object.
    :param doc_db: The document database instance.
    :param _: Authentication dependency to ensure the user is logged in.
    :return: A page asking for name for the new API key.
    """
    nav_links = await build_nav_links(request, doc_db)

    return TEMPLATES.TemplateResponse(
        request=request,
        name='create_new_api_key.html',
        context={
            'page_title': 'New API Key | BioMedical Terminology Service',
            'nav_links': nav_links,
        },
        headers=PRIVATE_RESPONSE_HEADERS,
    )


@ui_router.post(
    '/api-keys/new',
    response_class=HTMLResponse,
    responses={404: {'description': 'User not found.'}},
)
async def post_new_api_key(request: Request,
                           doc_db: Annotated[DocumentDatabase, Depends(get_active_doc_db)],
                           username: Annotated[str, Depends(login_required)],
                           name: Annotated[str | None, Form()] = None,
                           ):
    """
    Process the creation of a new API key for the logged-in user.
    :param request: The incoming request object.
    :param name: The name for the new API key.
    :param doc_db: The document database instance.
    :param username: The username of the logged-in user.
    :return: An HTML response displaying the newly created API key.
    """
    user = await doc_db.users.get(username)
    if not user:
        raise HTTPException(status_code=404, detail='User not found')

    # Names are labels, not credentials. Normalize whitespace so they remain useful in
    # tables and logs, and enforce the same limit even when the form is bypassed.
    name = ' '.join((name or '').split())
    if not name or len(name) > 128:
        nav_links = await build_nav_links(request, doc_db)
        return TEMPLATES.TemplateResponse(
            request=request,
            name='create_new_api_key.html',
            context={
                'page_title': 'New API Key | BioMedical Terminology Service',
                'nav_links': nav_links,
                'error': 'Enter a key name between 1 and 128 characters.',
                'key_name': name,
            },
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            headers=PRIVATE_RESPONSE_HEADERS,
        )

    new_key_bytes = secrets.token_bytes(32)
    new_key_str = base64.urlsafe_b64encode(new_key_bytes).decode('ascii').rstrip('=')
    server_key_bytes = base64.b64decode(CONFIG.server_hmac_key)

    hmac_digest = hmac.new(
        key=server_key_bytes,
        msg=new_key_str.encode(),
        digestmod=hashlib.sha256,
    )

    new_api_key = UserApiKey(
        name=name,
        keyHash=hmac_digest.hexdigest(),
    )
    await doc_db.users.save_api_key(
        username=username,
        api_key=new_api_key,
    )

    nav_links = await build_nav_links(request, doc_db)

    return TEMPLATES.TemplateResponse(
        request=request,
        name='display_new_api_key.html',
        context={
            'page_title': 'New API Key | BioMedical Terminology Service',
            'api_key': new_key_str,
            'nav_links': nav_links,
        },
        headers=PRIVATE_RESPONSE_HEADERS,
    )


@ui_router.delete('/api-keys/{key_id}', status_code=status.HTTP_204_NO_CONTENT)
async def delete_api_key(key_id: str,
                         doc_db: Annotated[DocumentDatabase, Depends(get_active_doc_db)],
                         username: Annotated[str, Depends(login_required)],
                         ):
    """
    Delete an API key for the logged-in user.
    :param key_id: The ID of the API key to delete.
    :param doc_db: The document database instance.
    :param username: The username of the logged-in user.
    :return: A redirect response to the API keys management page.
    """
    await doc_db.users.delete_api_key(
        username=username,
        key_id=UUID(key_id),
    )


@ui_router.get(
    '/vocabularies',
    response_class=HTMLResponse,
    responses={500: {'description': 'Failed to load vocabulary statuses.'}},
)
async def list_vocabularies(request: Request,
                            doc_db: Annotated[DocumentDatabase, Depends(get_active_doc_db)],
                            username: Annotated[str | None, Depends(login_optional)],
                            ):
    """
    List all available vocabularies.
    \f
    :param request: Request object.
    :param doc_db: Document database instance.
    :param username: The username of the logged-in user, if any.
    :return: An HTML response with the list of vocabularies.
    """
    try:
        vocabularies = _vocabulary_catalogue()

        nav_links = await build_nav_links(request, doc_db)
        base_url = str(request.base_url).rstrip('/')
        structured_data = build_structured_data(base_url)

        dataset_lds = []
        for v in vocabularies:
            dataset_lds.append({
                LD_TYPE: 'Dataset',
                'name': v['name'],
                'identifier': v['prefix'].value,
                'url': f"{base_url}/vocabularies/{v['prefix'].value}",
                'description': f"{v['name']} vocabulary dataset.",
                'isAccessibleForFree': True,
                'inLanguage': 'en',
            })

        structured_data.append({
            LD_CONTEXT: SCHEMA_ORG,
            LD_TYPE: 'CollectionPage',
            'name': 'Vocabularies',
            'url': base_url + '/vocabularies',
            'description': 'A list of all available vocabularies in the BioMedical '
                           'Terminology Service.',
            'hasPart': dataset_lds,
        })

        return TEMPLATES.TemplateResponse(
            request=request,
            name='vocabularies.html',
            context={
                'request': request,
                'page_title': 'Vocabularies | BioMedical Terminology Service',
                'vocabularies': vocabularies,
                'nav_links': nav_links,
                'structured_data': structured_data,
                'is_admin': username is not None,
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@ui_router.get(
    '/vocabularies/{prefix}',
    response_class=HTMLResponse,
    responses={500: {'description': 'Failed to load vocabulary detail information.'}},
)
async def get_vocabulary_info(prefix: ConceptPrefix,
                              request: Request,
                              doc_db: Annotated[DocumentDatabase, Depends(get_active_doc_db)],
                              query: Annotated[str | None, Query(alias='q', max_length=200)] = None,
                              ):
    """
    Browse and search concepts in a vocabulary without requiring status/cache warm-up.
    \f
    :param prefix: The vocabulary prefix.
    :param request: Request object.
    :param doc_db: Document database instance.
    :param query: Optional server-rendered concept search.
    :return: An HTML response with the vocabulary information.
    """
    try:
        vocabulary = get_vocabulary_config(prefix)
        model_class = vocabulary['conceptClass']
        clean_query = (query or '').strip()
        search_results = []
        if len(clean_query) >= 3:
            search_results = await doc_db.auto_complete_search(
                prefix=prefix,
                query=clean_query,
                limit=20,
                model_class=model_class,
            )

        root_ids = VOCABULARY_ROOTS.get(prefix, [])
        root_concepts = []
        if root_ids:
            root_concepts = await doc_db.get_terms_by_ids(
                prefix=prefix,
                concept_ids=root_ids,
                model_class=model_class,
            )
        roots_by_id = {concept.concept_id: concept for concept in root_concepts}
        roots = [roots_by_id.get(root_id) for root_id in root_ids]
        nav_links = await build_nav_links(request, doc_db)
        base_url = str(request.base_url).rstrip('/')
        structured_data = build_structured_data(base_url)
        page_url = f'{base_url}/vocabularies/{prefix.value}'
        dataset_ld = {
            LD_CONTEXT: SCHEMA_ORG,
            LD_TYPE: ['Dataset', 'DefinedTermSet'],
            'name': vocabulary['name'],
            'identifier': prefix.value,
            'url': page_url,
            'description': f"Browse and search {vocabulary['name']} concepts.",
            'inLanguage': 'en',
            'isAccessibleForFree': True,
        }

        structured_data.extend([
            {
                LD_CONTEXT: SCHEMA_ORG,
                LD_TYPE: 'WebPage',
                'name': f"{vocabulary['name']} Vocabulary",
                'url': page_url,
                'description': f"Browse and search {vocabulary['name']} in the "
                               'BioMedical Terminology Service.',
            },
            dataset_ld,
        ])

        return TEMPLATES.TemplateResponse(
            request=request,
            name='vocabulary_browser.html',
            context={
                'request': request,
                'page_title': f"{vocabulary['name']} Concepts | BioMedical Terminology Service",
                'meta_description': f"Browse and search concepts in {vocabulary['name']}.",
                'vocabulary': vocabulary,
                'query': clean_query,
                'search_results': search_results,
                'roots': roots,
                'nav_links': nav_links,
                'structured_data': structured_data,
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@ui_router.get(
    '/vocabularies/{prefix}/status',
    response_class=HTMLResponse,
    responses={500: {'description': 'Failed to load vocabulary status information.'}},
)
async def get_vocabulary_status_page(
    prefix: ConceptPrefix,
    request: Request,
    cache: Annotated[Cache, Depends(get_active_cache)],
    doc_db: Annotated[DocumentDatabase, Depends(get_active_doc_db)],
    graph_db: Annotated[GraphDatabase, Depends(get_active_graph_db)],
):
    """Render the deliberately separate, potentially expensive vocabulary status view."""
    try:
        vocabulary = await get_vocabulary_status(
            prefix, cache=cache, doc_db=doc_db, graph_db=graph_db,
        )
        similarity = await get_similarity_status(
            prefix, cache=cache, doc_db=doc_db, graph_db=graph_db,
        )
        annotations: list[AnnotationStatus] = []
        for annotation in vocabulary.annotations:
            annotations.append(await get_annotation_status(
                prefix_1=prefix,
                prefix_2=annotation,
                cache=cache,
                graph_db=graph_db,
            ))
        license_str = get_vocabulary_license(prefix)
        license_html = markdown(
            license_str, extensions=['extra', 'sane_lists', 'toc']
        ) if license_str else None
        return TEMPLATES.TemplateResponse(
            request=request,
            name='vocabulary_detail.html',
            context={
                'page_title': f'{vocabulary.name} Status | BioMedical Terminology Service',
                'vocabulary': vocabulary,
                'similarity': similarity,
                'annotations': annotations,
                'nav_links': await build_nav_links(request, doc_db),
                'license_html': license_html,
                'term_browser_url': request.url_for('get_vocabulary_info', prefix=prefix.value),
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@ui_router.get(
    '/vocabularies/{prefix}/{concept_id}',
    response_class=HTMLResponse,
    responses={
        404: {'description': 'Concept not found in the specified vocabulary.'},
        500: {'description': 'Failed to load concept detail information.'},
    },
)
async def get_concept_detail(prefix: ConceptPrefix,
                             concept_id: str,
                             request: Request,
                             cache: Annotated[Cache, Depends(get_active_cache)],
                             doc_db: Annotated[DocumentDatabase, Depends(get_active_doc_db)],
                             graph_db: Annotated[GraphDatabase, Depends(get_active_graph_db)],
                             ):
    """
    Get detailed information about a specific concept within a vocabulary.
    :param prefix: The vocabulary prefix.
    :param concept_id: The ID of the concept.
    :param request: Request object.
    :param cache: Cache instance.
    :param doc_db: Document database instance.
    :param graph_db: Graph database instance.
    :return: An HTML response with the concept details.
    """
    try:
        vocab_config = get_vocabulary_config(prefix)

        concept = await doc_db.get_terms_by_ids(
            prefix=prefix,
            concept_ids=[concept_id],
            model_class=vocab_config['conceptClass']
        )

        if not concept:
            raise HTTPException(status_code=404, detail='Concept not found')

        concept = concept[0]
        nav_links = await build_nav_links(request, doc_db)
        base_url = str(request.base_url).rstrip('/')
        structured_data = build_structured_data(base_url)

        return TEMPLATES.TemplateResponse(
            request=request,
            name='concept_detail.html',
            context={
                'page_title': f'{concept.label} | {prefix.value} Concept | BioMedical Terminology Service',
                'concept': concept,
                'vocabulary_prefix': prefix,
                'nav_links': nav_links,
                'structured_data': structured_data,
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@ui_router.get(
    '/term-browser',
    response_class=HTMLResponse,
    responses={404: {'description': 'Term browser not compiled or deployed.'}},
)
async def get_term_browser():
    """
    Serve the term browser page.
    :return:
    """
    return RedirectResponse('/vocabularies', status_code=status.HTTP_308_PERMANENT_REDIRECT)


@ui_router.post('/rebuild-cache', status_code=status.HTTP_202_ACCEPTED)
async def rebuild_cache_endpoint(_: Annotated[str, Depends(login_required)],
                                 ):
    """
    Trigger a cache rebuild task.
    \f
    :param _: Authentication dependency to ensure the user is logged in.
    :return: No response.
    """
    rebuild_cache_task.delay()


@ui_router.post(
    '/reload-graphql',
    status_code=status.HTTP_202_ACCEPTED,
    responses={500: {'description': 'GraphQL schema reload failed.'}},
)
async def reload_graphql_endpoint(request: Request,
                                  _: Annotated[str, Depends(login_required)],
                                  ):
    """
    Reload the GraphQL sub application, so that the schema matches the latest capability
    :param request: The incoming request object.
    :param _: Authentication dependency to ensure the user is logged in.
    :return: No response.
    """
    graphql_service = request.app.state.graphql_service

    try:
        await graphql_service.reload()
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail="GraphQL schema reload failed",
        ) from e
