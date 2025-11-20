import importlib.resources as pkg_resources
from typing import AsyncIterator
from pydantic import BaseModel
from fastapi.templating import Jinja2Templates


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


def get_templates() -> Jinja2Templates:
    """
    Get the Jinja2 templates instance for rendering HTML templates.
    :return:
    """
    template_path = pkg_resources.files('bioterms.data') / 'templates'
    return Jinja2Templates(directory=str(template_path))


TEMPLATES = get_templates()
