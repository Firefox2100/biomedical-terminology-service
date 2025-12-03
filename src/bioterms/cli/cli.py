import secrets
import base64
import typer
from typing import Annotated, Optional

from bioterms.etc.consts import CONFIG
import bioterms.cli.annotation as annotation
import bioterms.cli.cache as cache
import bioterms.cli.similarity as similarity
import bioterms.cli.user as user
import bioterms.cli.vocabulary as vocabulary
from bioterms.cli.utils import CONSOLE


def create_cli() -> typer.Typer:
    app = typer.Typer(
        help='Biomedical Terminology Service CLI',
        invoke_without_command=True,
        no_args_is_help=True,
    )

    app.add_typer(annotation.app, name='annotation', help='Manage biomedical annotations.')
    app.add_typer(cache.app, name='cache', help='Manage data cache.')
    app.add_typer(similarity.app, name='similarity', help='Manage similarity computations between biomedical terms.')
    app.add_typer(user.app, name='user', help='Administrator user operations')
    app.add_typer(vocabulary.app, name='vocabulary', help='Manage biomedical vocabularies.')

    @app.command(name='generate-hmac-key', help='Generate a random HMAC key for secure operations.')
    def generate_hmac_key():
        hmac_key = secrets.token_bytes(32)
        hmac_key_b64 = base64.urlsafe_b64encode(hmac_key).decode('ascii')

        CONSOLE.print(f'[green]Generated HMAC key:[/green] {hmac_key_b64}')

    @app.callback()
    def main_callback(version: Annotated[
                          Optional[bool],
                          typer.Option(
                              '--version',
                              '-v',
                              help='Show the version of the Biomedical Terminology Service CLI and exit.'
                          )
                      ] = None,
                      verbose: Annotated[
                          bool,
                          typer.Option(
                              '--verbose',
                              '-V',
                              help='Enable verbose output.'
                          )
                      ] = False,
                      ):
        if version:
            from bioterms import __version__
            CONSOLE.print(f'Biomedical Terminology Service CLI version: {__version__}')
            raise typer.Exit()

        if verbose:
            CONFIG.verbose_print = True

    return app


def main():
    app = create_cli()

    app()


if __name__ == '__main__':
    main()
