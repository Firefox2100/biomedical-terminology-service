from typing import Annotated
import typer
from rich.table import Table

from bioterms.etc.consts import PH
from bioterms.database import get_active_doc_db
from bioterms.model.user import User
from .utils import run_async, CONSOLE


app = typer.Typer()


@app.command(name='create', help='Create a new administrator user')
@run_async
async def create_user(username: Annotated[str, typer.Argument(help='Username of the new user, must be unique')],
                      password: Annotated[str, typer.Option(help='Password of the new user')] = None,
                      ):
    db = await get_active_doc_db()

    if not password:
        password = typer.prompt(
            f'Enter password for new user {username}',
            hide_input=True,
            confirmation_prompt=True
        )

        if not password:
            CONSOLE.print('Password cannot be empty', style='red')
            raise typer.Exit(code=1)

    hashed_password = PH.hash(password)

    user = User(
        username=username,
        password=hashed_password,
    )

    CONSOLE.print('Creating new user with the following details:')
    table = Table('Field', 'Value')
    table.add_row('Username', user.username)
    CONSOLE.print(table)

    await db.users.save(user)

    CONSOLE.print(f'[green]Successfully created new user {user.username}[/green]')


@app.command(name='list', help='List all users')
@run_async
async def list_users():
    db = await get_active_doc_db()

    users = await db.users.filter()

    if not users:
        CONSOLE.print('No users found.')
        return

    table = Table('Username')
    for user in users:
        table.add_row(user.username)

    CONSOLE.print(table)


@app.command(name='delete', help='Delete a user')
@run_async
async def delete_user(username: Annotated[str, typer.Argument(help='Username of the user to delete')],
                      ):
    db = await get_active_doc_db()

    user = await db.users.get(username)
    if not user:
        CONSOLE.print(f'[red]No user found with username {username}[/red]')
        return

    await db.users.delete(username)
    CONSOLE.print(f'[green]Successfully deleted user {username}[/green]')
