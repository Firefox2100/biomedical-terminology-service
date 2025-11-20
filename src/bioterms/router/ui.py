from uuid import UUID
from fastapi import APIRouter, Query, Depends, Request
from fastapi.responses import HTMLResponse

from .utils import TEMPLATES


ui_router = APIRouter(
    tags=['UI'],
)


@ui_router.get('/', response_class=HTMLResponse)
async def get_home_page(request: Request):
    """
    Serve the home page of the BioMedical Terminology Service.
    \f
    :return: An HTML response with the home page content.
    """
    return TEMPLATES.TemplateResponse(
        'home.html',
        {
            'request': request,
            'page_title': 'Home | BioMedical Terminology Service',
        }
    )
