from datetime import date
from xml.etree.ElementTree import Element, SubElement, tostring
from fastapi import APIRouter, Request, Depends
from fastapi.responses import Response, PlainTextResponse

from bioterms.etc.enums import ConceptPrefix
from bioterms.database import Cache, get_active_cache


misc_router = APIRouter(
    tags=['Miscellaneous'],
)


@misc_router.get('/sitemap.xml', include_in_schema=False)
async def sitemap(request: Request,
                  cache: Cache = Depends(get_active_cache),
                  ):
    """
    Serve the sitemap.xml file.
    :return: The sitemap.xml file response.
    """
    xml_str = await cache.get_site_map()

    if xml_str is not None:
        return Response(
            content=xml_str,
            media_type='application/xml',
        )

    base_url = str(request.base_url).rstrip('/')

    urlset = Element('urlset', xmlns='https://www.sitemaps.org/schemas/sitemap/0.9')

    def add_url(loc: str,
                changefreq: str = None,
                priority: float = None,
                lastmod: date = None,

                ):
        url_el = SubElement(urlset, 'url')
        loc_el = SubElement(url_el, 'loc')
        loc_el.text = loc

        if lastmod:
            lastmod_el = SubElement(url_el, 'lastmod')
            lastmod_el.text = lastmod.isoformat()

        if changefreq:
            changefreq_el = SubElement(url_el, 'changefreq')
            changefreq_el.text = changefreq

        if priority is not None:
            priority_el = SubElement(url_el, 'priority')
            priority_el.text = f'{priority:.1f}'

    add_url(f'{base_url}/', changefreq='monthly', priority=1.0)
    add_url(f'{base_url}/vocabularies', changefreq='weekly', priority=0.8)

    for prefix in ConceptPrefix:
        add_url(
            f'{base_url}/vocabularies/{prefix.value}',
            changefreq='weekly',
            priority=0.6,
        )

    xml_bytes = tostring(urlset, encoding='utf-8', xml_declaration=True)
    xml_str = xml_bytes.decode('utf-8')
    await cache.save_site_map(xml_str)

    return Response(
        content=xml_str,
        media_type='application/xml',
    )


@misc_router.get('/robots.txt', include_in_schema=False)
async def robots_txt(request: Request):
    """
    Serve the robots.txt file.
    :return: The robots.txt file response.
    """
    base_url = str(request.base_url).rstrip('/')

    content = f"""User-agent: *
Sitemap: {base_url}/sitemap.xml
"""

    return PlainTextResponse(content)


@misc_router.get('/health', include_in_schema=False)
async def health_check():
    """
    Health check endpoint to verify the application is running.
    :return: A JSON response indicating the application status.
    """
    return {'status': 'ok'}
