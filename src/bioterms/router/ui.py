"""
UI Router for the BioMedical Terminology Service.

This router handles the web interface endpoints, serving HTML pages for
various functionalities such as home page, login, API key management,
vocabulary listing, and vocabulary details.
"""

import secrets
import hmac
import hashlib
import base64
from typing import Annotated
from uuid import UUID
from urllib.parse import urlencode, urlparse, quote
from markdown import markdown
from fastapi import APIRouter, Query, Form, Depends, Request, HTTPException, status
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse

from bioterms.etc.consts import CONFIG, STATIC_FILE_PATH
from bioterms.etc.enums import ConceptPrefix
from bioterms.database import Cache, DocumentDatabase, GraphDatabase, get_active_cache, \
    get_active_doc_db, get_active_graph_db
from bioterms.vocabulary import get_vocabulary_status, get_vocabulary_config, get_vocabulary_license
from bioterms.annotation import get_annotation_status
from bioterms.similarity import get_similarity_status
from bioterms.task.cache import rebuild_cache_task
from bioterms.model.user import UserApiKey
from bioterms.model.annotation_status import AnnotationStatus
from .utils import TEMPLATES, build_nav_links, sanitise_next_url, login_optional, login_required, \
    build_structured_data


ui_router = APIRouter(
    tags=['UI'],
)


@ui_router.get(
    '/',
    response_class=HTMLResponse,
    responses={500: {'description': 'Failed to load vocabulary status for the home page.'}},
)
async def get_home_page(request: Request,
                        doc_db: Annotated[DocumentDatabase, Depends(get_active_doc_db)],
                        graph_db: Annotated[GraphDatabase, Depends(get_active_graph_db)],
                        ):
    """
    Serve the home page of the BioMedical Terminology Service.
    \f
    :param request: Request object.
    :param doc_db: Document database instance.
    :param graph_db: Graph database instance.
    :return: An HTML response with the home page content.
    """
    try:
        loaded_sum = 0
        concept_sum = 0
        for prefix in ConceptPrefix:
            vocab_status = await get_vocabulary_status(
                prefix,
                doc_db=doc_db,
                graph_db=graph_db,
            )

            if vocab_status.loaded:
                loaded_sum += 1
                concept_sum += vocab_status.concept_count

        nav_links = await build_nav_links(request, doc_db)
        base_url = str(request.base_url).rstrip('/')
        structured_data = build_structured_data(base_url)

        return TEMPLATES.TemplateResponse(
            request=request,
            name='home.html',
            context={
                'page_title': 'Home | BioMedical Terminology Service',
                'vocabulary_count': loaded_sum,
                'concept_count': concept_sum,
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
    sanitised_next_url = sanitise_next_url(next_url) if next_url \
        else str(request.url_for('get_home_page'))

    if request.session.get('username'):
        # User is already logged in, redirect to next_url or home page
        redirect_url = sanitised_next_url
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


def _is_safe_relative_path(url: str,
                           request: Request,
                           ) -> bool:
    """
    Check whether a URL is a safe same-origin relative redirect target, distinct from the
    current request path (to avoid redirecting a user back to the page they just posted to).
    :param url: The URL to check.
    :param request: The current request, used to compare against the current path.
    :return: True if the URL is a safe relative redirect target.
    """
    try:
        p = urlparse(url)
        return (not p.scheme and not p.netloc and url.startswith('/')) and (url != request.url.path)
    except Exception:
        return False


@ui_router.post('/login', response_class=RedirectResponse)
async def post_login_credentials(request: Request,
                                 username: Annotated[str, Form()],
                                 password: Annotated[str, Form()],
                                 doc_db: Annotated[DocumentDatabase, Depends(get_active_doc_db)],
                                 next_url: Annotated[str | None, Query(alias='next')] = None,
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
    sanitised_next_url = sanitise_next_url(next_url) if next_url \
        else str(request.url_for('get_home_page'))

    if not username or not password:
        return _login_redirect(request, next_url, 'Please enter username or password correctly')

    user = await doc_db.users.get(username)

    if not user or not user.validate_password(password):
        return _login_redirect(request, next_url, 'Invalid username or password')

    request.session['username'] = username

    if _is_safe_relative_path(sanitised_next_url, request):
        dest = sanitised_next_url
    else:
        dest = str(request.url_for('get_home_page'))

    return RedirectResponse(dest, status_code=status.HTTP_303_SEE_OTHER)


@ui_router.get('/logout', response_class=RedirectResponse)
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
        }
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
        }
    )


@ui_router.post(
    '/api-keys/new',
    response_class=HTMLResponse,
    responses={404: {'description': 'User not found.'}},
)
async def post_new_api_key(request: Request,
                           name: Annotated[str, Form()],
                           doc_db: Annotated[DocumentDatabase, Depends(get_active_doc_db)],
                           username: Annotated[str, Depends(login_required)],
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
        }
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
                            graph_db: Annotated[GraphDatabase, Depends(get_active_graph_db)],
                            username: Annotated[str | None, Depends(login_optional)],
                            ):
    """
    List all available vocabularies.
    \f
    :param request: Request object.
    :param doc_db: Document database instance.
    :param graph_db: Graph database instance.
    :param username: The username of the logged-in user, if any.
    :return: An HTML response with the list of vocabularies.
    """
    try:
        vocab_statuses = []
        for prefix in ConceptPrefix:
            vocab_status = await get_vocabulary_status(
                prefix,
                doc_db=doc_db,
                graph_db=graph_db,
            )
            vocab_statuses.append(vocab_status)

        nav_links = await build_nav_links(request, doc_db)
        base_url = str(request.base_url).rstrip('/')
        structured_data = build_structured_data(base_url)

        dataset_lds = []
        for v in vocab_statuses:
            dataset_lds.append({
                '@type': 'Dataset',
                'name': v.name,
                'identifier': v.prefix.value,
                'url': f'{base_url}/vocabularies/{v.prefix.value}',
                'description': f'{v.name} vocabulary dataset.',
                'variableMeasured': [
                    {
                        '@type': 'PropertyValue',
                        'name': 'concept_count',
                        'value': v.concept_count,
                    },
                    {
                        '@type': 'PropertyValue',
                        'name': 'relationship_count',
                        'value': v.relationship_count,
                    }
                ],
                'isAccessibleForFree': True,
                'inLanguage': 'en',
            })

        structured_data.append({
            '@context': 'https://schema.org',
            '@type': 'CollectionPage',
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
                'vocabularies': vocab_statuses,
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
                              cache: Annotated[Cache, Depends(get_active_cache)],
                              doc_db: Annotated[DocumentDatabase, Depends(get_active_doc_db)],
                              graph_db: Annotated[GraphDatabase, Depends(get_active_graph_db)],
                              ):
    """
    Get information about the specified vocabulary.
    \f
    :param prefix: The vocabulary prefix.
    :param request: Request object.
    :param cache: Cache instance.
    :param doc_db: Document database instance.
    :param graph_db: Graph database instance.
    :return: An HTML response with the vocabulary information.
    """
    try:
        vocab_status = await get_vocabulary_status(
            prefix,
            cache=cache,
            doc_db=doc_db,
            graph_db=graph_db,
        )
        sim_status = await get_similarity_status(
            prefix,
            cache=cache,
            doc_db=doc_db,
            graph_db=graph_db,
        )
        nav_links = await build_nav_links(request, doc_db)
        base_url = str(request.base_url).rstrip('/')
        structured_data = build_structured_data(base_url)
        license_str = get_vocabulary_license(prefix)
        license_html = None
        if license_str:
            license_html = markdown(
                license_str,
                extensions=['extra', 'sane_lists', 'toc']
            )

        annotation_statuses: list[AnnotationStatus] = []
        for annotation in vocab_status.annotations:
            annotation_status = await get_annotation_status(
                prefix_1=vocab_status.prefix,
                prefix_2=annotation,
                cache=cache,
                graph_db=graph_db,
            )

            annotation_statuses.append(annotation_status)

        page_url = f'{base_url}/vocabularies/{vocab_status.prefix.value}'
        dataset_ld = {
            '@context': 'https://schema.org',
            '@type': ['Dataset', 'DefinedTermSet'],
            'name': vocab_status.name,
            'identifier': vocab_status.prefix.value,
            'url': page_url,
            'description': f'{vocab_status.name} vocabulary dataset.',
            'inLanguage': 'en',
            'isAccessibleForFree': True,
            'variableMeasured': [
                {
                    '@type': 'PropertyValue',
                    'name': 'concept_count',
                    'value': vocab_status.concept_count,
                },
                {
                    '@type': 'PropertyValue',
                    'name': 'relationship_count',
                    'value': vocab_status.relationship_count,
                },
            ],
            'additionalProperty': [
                {
                    '@type': 'PropertyValue',
                    'name': 'loaded',
                    'value': vocab_status.loaded,
                },
                {
                    '@type': 'PropertyValue',
                    'name': 'downloaded',
                    'value': vocab_status.file_downloaded,
                },
                {
                    '@type': 'PropertyValue',
                    'name': 'download_time',
                    'value': vocab_status.file_download_time.isoformat()
                        if vocab_status.file_download_time else None,
                },
            ]
        }
        if license_str:
            if license_str.strip().startswith('http'):
                dataset_ld['license'] = license_str.strip()
            else:
                dataset_ld['license'] = {
                    '@type': 'CreativeWork',
                    'text': license_str.strip(),
                }
        if annotation_statuses:
            dataset_ld['hasPart'] = [
                {
                    '@type': 'Dataset',
                    'name': ann.name,
                    'identifier': f'{ann.prefix_source}-{ann.prefix_target}',
                    'variableMeasured': [
                        {
                            '@type': 'PropertyValue',
                            'name': 'relationship_count',
                            'value': ann.relationship_count,
                        }
                    ],
                } for ann in annotation_statuses
            ]

        structured_data.extend([
            {
                '@context': 'https://schema.org',
                '@type': 'WebPage',
                'name': f'{vocab_status.name} Vocabulary',
                'url': page_url,
                'description': f'Details and statistics for the {vocab_status.name} vocabulary '
                               f'in the BioMedical Terminology Service.',
            },
            dataset_ld,
        ])

        if prefix in [
            ConceptPrefix.CTV3, ConceptPrefix.HPO, ConceptPrefix.MONDO, ConceptPrefix.NCIT,
            ConceptPrefix.OMIM, ConceptPrefix.ORDO, ConceptPrefix.SNOMED
        ]:
            params = {
                'ontology': prefix.value,
                'returnTo': f'/vocabularies/{prefix.value}',
            }
            term_browser_url = f'{request.url_for("get_term_browser")}?{urlencode(params)}'
        else:
            term_browser_url = None

        return TEMPLATES.TemplateResponse(
            request=request,
            name='vocabulary_detail.html',
            context={
                'request': request,
                'page_title': f'{vocab_status.name} | BioMedical Terminology Service',
                'vocabulary': vocab_status,
                'similarity': sim_status,
                'annotations': annotation_statuses,
                'nav_links': nav_links,
                'license_html': license_html,
                'term_browser_url': term_browser_url,
                'structured_data': structured_data,
            }
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
    try:
        html_path = STATIC_FILE_PATH / 'term-browser' / 'index.html'
        return FileResponse(html_path)
    except RuntimeError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail='Term browser not compiled or deployed.'
        )


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
        await graphql_service.reload
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail="GraphQL schema reload failed",
        ) from e
