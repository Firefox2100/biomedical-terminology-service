from contextlib import asynccontextmanager
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from bioterms import __version__
from bioterms.etc.consts import LOGGER, CONFIG
from bioterms.etc.errors import BtsError
from bioterms.database import get_active_doc_db, get_active_graph_db
from bioterms.router import auto_complete_router, expand_router


@asynccontextmanager
async def lifespan(_: FastAPI):
    """
    Lifespan context manager for the FastAPI application.
    :param _: The FastAPI application instance.
    """
    LOGGER.debug('System configuration loaded: %s', CONFIG.model_dump_json())

    doc_db = get_active_doc_db()
    graph_db = get_active_graph_db()

    try:
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
                'name': 'Expansion',
                'description': 'Endpoints for expanding biomedical terms to their descendants.',
            }
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
        allow_origins=['*'],
        allow_credentials=True,
        allow_methods=('GET', 'POST', 'OPTIONS'),
        allow_headers=(
            'Authorization',
            'Content-Type',
            'Accept',
            'X-Requested-With',
        ),
    )

    app.include_router(auto_complete_router)
    app.include_router(expand_router)

    @app.exception_handler(BtsError)
    async def cafe_variome_exception_handler(request: Request, exc: BtsError):
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
