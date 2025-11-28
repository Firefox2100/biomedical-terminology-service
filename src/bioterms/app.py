import asyncio
import secrets
import traceback
import importlib.resources as pkg_resources
from contextlib import asynccontextmanager
import uvicorn
from starlette.middleware.sessions import SessionMiddleware
from fastapi import FastAPI, Request, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exception_handlers import http_exception_handler as fastapi_http_exception_handler
from asgi_csrf import asgi_csrf

from bioterms import __version__
from bioterms.etc.consts import LOGGER, CONFIG
from bioterms.etc.errors import BtsError
from bioterms.database import get_active_doc_db, get_active_graph_db
from bioterms.graphql_api import create_graphql_app
from bioterms.router import auto_complete_router, data_router, expand_router, similarity_router, ui_router
from bioterms.router.utils import TEMPLATES, build_nav_links


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for the FastAPI application.
    :param app: The FastAPI application instance.
    """
    LOGGER.debug('System configuration loaded: %s', CONFIG.model_dump_json())

    doc_db = await get_active_doc_db()
    graph_db = get_active_graph_db()

    graphql_app = await create_graphql_app()

    try:
        app.mount('/api/graphql', graphql_app)
        yield
    except Exception as e:
        LOGGER.critical(f'Fatal error during application lifespan: {e}', exc_info=True)
        raise e
    finally:
        LOGGER.info('Shutting down application...')

        await doc_db.close()
        await graph_db.close()

        LOGGER.info('Application shutdown complete.')


def create_app() -> FastAPI:
    """
    FastAPI application factory function.
    :return: An instance of FastAPI application.
    """
    app = FastAPI(
        title='BioMedical Terminology Service',
        version=__version__,
        description='A FastAPI service for using with biomedical terminologies, '
                    'such as ontologies or vocabularies.',
        contact={
            'name': 'Firefox2100',
            'url': 'https://www.firefox2100.co.uk/',
            'email': 'wangyunze16@gmail.com',
        },
        license_info={
            'name': 'MIT',
            'url': 'https://github.com/Firefox2100/biomedical-terminology-service/blob/main/LICENSE',
        },
        openapi_tags=[
            {
                'name': 'Auto Complete',
                'description': 'Endpoints for auto-completion of biomedical terms.',
            },
            {
                'name': 'Data Management',
                'description': 'Endpoints for managing vocabulary data within the service.',
            },
            {
                'name': 'Expansion',
                'description': 'Endpoints for expanding biomedical terms to their descendants.',
            },
            {
                'name': 'Similarity',
                'description': 'Endpoints for retrieving similar biomedical terms.',
            },
            {
                'name': 'UI',
                'description': 'Endpoints for serving the web user interface.',
            },
        ],
        root_path=CONFIG.service_root_path,
        openapi_url=CONFIG.openapi_url,
        docs_url=CONFIG.docs_url,
        redoc_url=CONFIG.redoc_url,
        lifespan=lifespan,
    )

    # Backend-only service, CORS set to allow all origins by default
    app.add_middleware(
        CORSMiddleware,
        allow_origins=(),
        allow_credentials=True,
        allow_methods=('GET', 'POST', 'PATCH', 'PUT', 'DELETE', 'OPTIONS'),
        allow_headers=(
            'Authorization',
            'Content-Type',
            'Accept',
            'X-Requested-With',
        ),
    )

    @app.middleware('http')
    async def disable_cors_for_api(request, call_next):
        if request.url.path.startswith('/api'):
            request.scope['cors_exempt'] = True

        response = await call_next(request)

        if request.url.path.startswith('/api'):
            response.headers['Access-Control-Allow-Origin'] = '*'

        return response

    @app.middleware('http')
    async def csp_headers(request, call_next):
        nonce = secrets.token_urlsafe(16)
        request.state.csp_nonce = nonce

        response = await call_next(request)

        if any([
            request.url.path.startswith('/static/'),
            request.url.path.startswith('/api/'),
            request.url.path.startswith('/docs'),
            request.url.path.startswith('/redoc'),
        ]):
            return response

        # Build a strict policy (adjust as you add features)
        policy = "; ".join([
            "default-src 'self'",
            "base-uri 'self'",
            "object-src 'none'",
            "frame-ancestors 'self'",
            "form-action 'self'",
            "img-src 'self' data:",
            "font-src 'self'",
            "style-src 'self'",
            f"script-src 'self' 'nonce-{nonce}'",
            "upgrade-insecure-requests",
        ])

        response.headers['Content-Security-Policy'] = policy
        response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['X-Frame-Options'] = 'DENY'
        response.headers['Permissions-Policy'] = 'geolocation=(), microphone=(), camera=()'

        return response

    app.add_middleware(
        SessionMiddleware,
        secret_key=CONFIG.secret_key,
        same_site='lax',
        https_only=CONFIG.use_https,
    )

    app.include_router(auto_complete_router)
    app.include_router(data_router)
    app.include_router(expand_router)
    app.include_router(similarity_router)
    app.include_router(ui_router)

    static_file_path = pkg_resources.files('bioterms.data') / 'static'
    app.mount('/static', StaticFiles(directory=str(static_file_path)), name='static')

    @app.get('/health', include_in_schema=False)
    async def health_check():
        """
        Health check endpoint to verify the application is running.
        :return: A JSON response indicating the application status.
        """
        return {'status': 'ok'}

    @app.exception_handler(BtsError)
    async def bioterms_exception_handler(request: Request, exc: BtsError):
        """
        Custom exception handler for BtsError exceptions.
        :param request: The request object.
        :param exc: The exception instance.
        :return: A JSON response with the error message and status code.
        """
        LOGGER.exception('BTS Service Exception occurred: %s', exc.message)
        LOGGER.debug('Request body: %s', (await request.body()).decode(errors='ignore'))

        return JSONResponse(
            status_code=exc.status_code,
            content={
                'error': {
                    'message': exc.message,
                }
            }
        )

    @app.exception_handler(HTTPException)
    async def http_exception_handler(request, exc):
        LOGGER.error('HTTP Exception: %s', exc)

        if request.url.path.startswith('/api'):
            return await fastapi_http_exception_handler(request, exc)

        doc_db = await get_active_doc_db()
        nav_links = await build_nav_links(request, doc_db)

        context = {
            'request': request,
            'page_title': f'{exc.status_code} Error | BioMedical Terminology Service',
            'detail': getattr(exc, 'detail', None),
            'nav_links': nav_links,
            'return_url': '/',
            'return_label': 'Back to Home',
        }

        if exc.status_code == 404:
            return TEMPLATES.TemplateResponse(
                '404.html',
                context=context,
                status_code=404
            )

        return TEMPLATES.TemplateResponse(
            '500.html',
            context=context,
            status_code=exc.status_code
        )

    def skip_paths(scope):
        return scope['path'].startswith('/api/')

    app = asgi_csrf(
        app,
        signing_secret=CONFIG.secret_key,
        always_protect={},
        cookie_secure=CONFIG.use_https,
        skip_if_scope=skip_paths,
    )

    return app


def main():
    """
    Main entry point for the application.
    """
    app = create_app()

    # Remove the logging handler added in the application code
    # This makes the log propagate to uvicorn's logging configuration
    if LOGGER.hasHandlers():
        for h in LOGGER.handlers[:]:
            LOGGER.removeHandler(h)

    uvicorn.run(
        app,
        host='0.0.0.0',
        port=5000,
        log_config={
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'default': {
                    "()": "uvicorn.logging.DefaultFormatter",
                    'fmt': '[%(asctime)s] [%(process)d] [%(levelname)s]: %(message)s',
                    'datefmt': '%Y-%m-%d %H:%M:%S %z',
                },
                'access': {
                    '()': 'uvicorn.logging.AccessFormatter',
                    'fmt': '%(levelprefix)s %(client_addr)s - '
                           '"%(request_line)s" %(status_code)s',
                },
            },
            'handlers': {
                'default': {
                    'formatter': 'default',
                    'class': 'logging.StreamHandler',
                    'stream': 'ext://sys.stderr',
                },
                'access': {
                    'formatter': 'access',
                    'class': 'logging.StreamHandler',
                    'stream': 'ext://sys.stdout',
                },
            },
            'loggers': {
                'uvicorn': {
                    'handlers': ['default'],
                    'level': 'INFO',
                    'propagate': False,
                },
                'uvicorn.error': {
                    'handlers': ['default'],
                    'level': 'INFO',
                    'propagate': False,
                },
                'uvicorn.access': {
                    'handlers': ['access'],
                    'level': 'INFO',
                    'propagate': False,
                },
            },
            'root': {
                'handlers': ['default'],
                'level': CONFIG.logging_level.upper(),  # pylint: disable=no-member
            },
        }
    )


if __name__ == '__main__':
    main()
