from typing import Union
from urllib.parse import urlencode, urlparse
from markdown import markdown
from fastapi import APIRouter, Query, Form, Depends, Request, HTTPException, status
from fastapi.responses import HTMLResponse, RedirectResponse

from bioterms.etc.enums import ConceptPrefix
from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, get_active_graph_db
from bioterms.vocabulary import get_vocabulary_status, get_vocabulary_license
from bioterms.annotation import get_annotation_status
from .utils import TEMPLATES, build_nav_links, sanitise_next_url


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

        return TEMPLATES.TemplateResponse(
            'home.html',
            {
                'request': request,
                'page_title': 'Home | BioMedical Terminology Service',
                'vocabulary_count': loaded_sum,
                'concept_count': concept_sum,
                'nav_links': nav_links,
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@ui_router.get('/login', response_class=Union[HTMLResponse, RedirectResponse])
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

        return TEMPLATES.TemplateResponse(
            'vocabularies.html',
            {
                'request': request,
                'page_title': 'Vocabularies | BioMedical Terminology Service',
                'vocabularies': vocab_statuses,
                'nav_links': nav_links,
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


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

        url = request.url_for('get_admin_login_page')

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
        license_str = get_vocabulary_license(prefix)
        license_html = None
        if license_str:
            license_html = markdown(
                license_str,
                extensions=['extra', 'sane_lists', 'toc']
            )

        annotation_statuses = []
        for annotation in vocab_status.annotations:
            annotation_status = await get_annotation_status(
                prefix_1=vocab_status.prefix,
                prefix_2=annotation,
                graph_db=graph_db,
            )

            annotation_statuses.append(annotation_status)

        return TEMPLATES.TemplateResponse(
            'vocabulary_detail.html',
            {
                'request': request,
                'page_title': f'{vocab_status.name} | BioMedical Terminology Service',
                'vocabulary': vocab_status,
                'annotations': annotation_statuses,
                'nav_links': nav_links,
                'license_html': license_html,
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
