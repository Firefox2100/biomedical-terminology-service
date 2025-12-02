import secrets
import hmac
import hashlib
import base64
from uuid import UUID
from urllib.parse import urlencode, urlparse
from markdown import markdown
from fastapi import APIRouter, Query, Form, Depends, Request, HTTPException, status
from fastapi.responses import HTMLResponse, RedirectResponse

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix
from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, get_active_graph_db
from bioterms.vocabulary import get_vocabulary_status, get_vocabulary_license
from bioterms.annotation import get_annotation_status
from bioterms.model.user import UserApiKey
from bioterms.model.annotation_status import AnnotationStatus
from .utils import TEMPLATES, build_nav_links, sanitise_next_url, login_required, build_structured_data


ui_router = APIRouter(
    tags=['UI'],
)


@ui_router.get('/', response_class=HTMLResponse)
async def get_home_page(request: Request,
                        doc_db: DocumentDatabase = Depends(get_active_doc_db),
                        graph_db: GraphDatabase = Depends(get_active_graph_db),
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
            'home.html',
            {
                'request': request,
                'page_title': 'Home | BioMedical Terminology Service',
                'vocabulary_count': loaded_sum,
                'concept_count': concept_sum,
                'nav_links': nav_links,
                'structured_data': structured_data,
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@ui_router.get('/login', response_class=HTMLResponse)
async def get_login_page(request: Request,
                         next_url: str | None = Query(None, alias='next'),
                         error: str | None = Query(None),
                         doc_db: DocumentDatabase = Depends(get_active_doc_db),
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
    sanitised_next_url = sanitise_next_url(next_url) if next_url else str(request.url_for('get_home_page'))

    if request.session.get('username'):
        # User is already logged in, redirect to next_url or home page
        redirect_url = sanitised_next_url
        return RedirectResponse(url=redirect_url, status_code=status.HTTP_303_SEE_OTHER)

    nav_links = await build_nav_links(request, doc_db)

    return TEMPLATES.TemplateResponse(
        'login.html',
        {
            'request': request,
            'page_title': 'Login | BioMedical Terminology Service',
            'next_url': sanitised_next_url,
            'error': error,
            'nav_links': nav_links,
        },
    )


@ui_router.post('/login', response_class=RedirectResponse)
async def post_login_credentials(request: Request,
                                 next_url: str | None = Query(None, alias='next'),
                                 username: str = Form(...),
                                 password: str = Form(...),
                                 doc_db: DocumentDatabase = Depends(get_active_doc_db),
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
    sanitised_next_url = sanitise_next_url(next_url) if next_url else str(request.url_for('get_home_page'))

    def login_redirect(error: str | None = None):
        params = {}
        if error:
            params['error'] = error

        if next_url:
            params['next'] = next_url

        url = request.url_for('get_login_page')

        if params:
            url = f'{url}?{urlencode(params)}'

        return RedirectResponse(url, status_code=status.HTTP_303_SEE_OTHER)

    if not username or not password:
        return login_redirect('Please enter username or password correctly')

    user = await doc_db.users.get(username)

    if not user or not user.validate_password(password):
        return login_redirect('Invalid username or password')

    request.session['username'] = username

    def is_safe_relative_path(u: str) -> bool:
        try:
            p = urlparse(u)

            return (not p.scheme and not p.netloc and u.startswith('/')) and (u != request.url.path)
        except Exception:
            return False

    if is_safe_relative_path(sanitised_next_url):
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

    return RedirectResponse(request.url_for('get_login_page'), status_code=status.HTTP_303_SEE_OTHER)


@ui_router.get('/api-keys', response_class=HTMLResponse)
async def get_api_keys_page(request: Request,
                            doc_db: DocumentDatabase = Depends(get_active_doc_db),
                            username: str = Depends(login_required),
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
        'api_keys.html',
        {
            'request': request,
            'page_title': 'API Keys | BioMedical Terminology Service',
            'api_keys': user.api_keys if user and user.api_keys else [],
            'nav_links': nav_links,
        }
    )


@ui_router.get('/api-keys/new', response_class=HTMLResponse)
async def create_new_api_key(request: Request,
                             doc_db: DocumentDatabase = Depends(get_active_doc_db),
                             _: str = Depends(login_required),
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
        'create_new_api_key.html',
        {
            'request': request,
            'page_title': 'New API Key | BioMedical Terminology Service',
            'nav_links': nav_links,
        }
    )


@ui_router.post('/api-keys/new', response_class=HTMLResponse)
async def post_new_api_key(request: Request,
                           name: str = Form(...),
                           doc_db: DocumentDatabase = Depends(get_active_doc_db),
                           username: str = Depends(login_required),
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
        'display_new_api_key.html',
        {
            'request': request,
            'page_title': 'New API Key | BioMedical Terminology Service',
            'api_key': new_key_str,
            'nav_links': nav_links,
        }
    )


@ui_router.delete('/api-keys/{key_id}', status_code=status.HTTP_204_NO_CONTENT)
async def delete_api_key(key_id: str,
                         request: Request,
                         doc_db: DocumentDatabase = Depends(get_active_doc_db),
                         username: str = Depends(login_required),
                         ):
    """
    Delete an API key for the logged-in user.
    :param key_id: The ID of the API key to delete.
    :param request: The incoming request object.
    :param doc_db: The document database instance.
    :param username: The username of the logged-in user.
    :return: A redirect response to the API keys management page.
    """
    await doc_db.users.delete_api_key(
        username=username,
        key_id=UUID(key_id),
    )


@ui_router.get('/vocabularies', response_class=HTMLResponse)
async def list_vocabularies(request: Request,
                            doc_db: DocumentDatabase = Depends(get_active_doc_db),
                            graph_db: GraphDatabase = Depends(get_active_graph_db),
                            ):
    """
    List all available vocabularies.
    \f
    :param request: Request object.
    :param doc_db: Document database instance.
    :param graph_db: Graph database instance.
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
            'description': 'A list of all available vocabularies in the BioMedical Terminology Service.',
            'hasPart': dataset_lds,
        })

        return TEMPLATES.TemplateResponse(
            'vocabularies.html',
            {
                'request': request,
                'page_title': 'Vocabularies | BioMedical Terminology Service',
                'vocabularies': vocab_statuses,
                'nav_links': nav_links,
                'structured_data': structured_data,
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@ui_router.get('/vocabularies/{prefix}', response_class=HTMLResponse)
async def get_vocabulary_info(prefix: ConceptPrefix,
                              request: Request,
                              doc_db: DocumentDatabase = Depends(get_active_doc_db),
                              graph_db: GraphDatabase = Depends(get_active_graph_db),
                              ):
    """
    Get information about the specified vocabulary.
    \f
    :param prefix: The vocabulary prefix.
    :param request: Request object.
    :param doc_db: Document database instance.
    :param graph_db: Graph database instance.
    :return: An HTML response with the vocabulary information.
    """
    try:
        vocab_status = await get_vocabulary_status(
            prefix,
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
                'description': f'Details and statistics for the {vocab_status.name} vocabulary in the '
                               f'BioMedical Terminology Service.',
            },
            dataset_ld,
        ])

        return TEMPLATES.TemplateResponse(
            'vocabulary_detail.html',
            {
                'request': request,
                'page_title': f'{vocab_status.name} | BioMedical Terminology Service',
                'vocabulary': vocab_status,
                'annotations': annotation_statuses,
                'nav_links': nav_links,
                'license_html': license_html,
                'structured_data': structured_data,
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
