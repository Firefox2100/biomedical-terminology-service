import asyncio
import typer

import bioterms.cli.annotation as annotation
import bioterms.cli.user as user
import bioterms.cli.vocabulary as vocabulary


def create_cli() -> typer.Typer:
    app = typer.Typer(help='Biomedical Terminology Service CLI')

    app.add_typer(annotation.app, name='annotation', help='Manage biomedical annotations.')
    app.add_typer(user.app, name='user', help='Administrator user operations')
    app.add_typer(vocabulary.app, name='vocabulary', help='Manage biomedical vocabularies.')

    return app


def main():
    app = create_cli()

    app()


if __name__ == '__main__':
    main()
