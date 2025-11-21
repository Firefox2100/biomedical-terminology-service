from uuid import UUID
from fastapi import APIRouter, Query, Depends, Request, HTTPException
from fastapi.responses import HTMLResponse

from bioterms.etc.enums import ConceptPrefix
from bioterms.database import DocumentDatabase, get_active_doc_db
from bioterms.vocabulary import get_vocabulary_status
from .utils import TEMPLATES, build_nav_links


ui_router = APIRouter(
    tags=['UI'],
)


@ui_router.get('/', response_class=HTMLResponse)
async def get_home_page(request: Request,
                        doc_db: DocumentDatabase = Depends(get_active_doc_db),
                        ):
    """
    Serve the home page of the BioMedical Terminology Service.
    \f
    :return: An HTML response with the home page content.
    """
    try:
        loaded_sum = 0
        concept_sum = 0
        for prefix in ConceptPrefix:
            vocab_status = await get_vocabulary_status(
                prefix,
                doc_db=doc_db,
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


@ui_router.get('/vocabularies', response_class=HTMLResponse)
async def list_vocabularies(request: Request,
                            doc_db: DocumentDatabase = Depends(get_active_doc_db),
                            ):
    """
    List all available vocabularies.
    \f
    :param request: Request object.
    :param doc_db: Document database instance.
    :return: An HTML response with the list of vocabularies.
    """
    try:
        vocab_statuses = []
        for prefix in ConceptPrefix:
            vocab_status = await get_vocabulary_status(
                prefix,
                doc_db=doc_db,
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


@ui_router.get('/vocabularies/{prefix}', response_class=HTMLResponse)
async def get_vocabulary_info(prefix: ConceptPrefix,
                              request: Request,
                              doc_db: DocumentDatabase = Depends(get_active_doc_db),
                              ):
    """
    Get information about the specified vocabulary.
    \f
    :param prefix: The vocabulary prefix.
    :param request: Request object.
    :param doc_db: Document database instance.
    :return: An HTML response with the vocabulary information.
    """
