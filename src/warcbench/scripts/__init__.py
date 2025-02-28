import click
from warcbench.scripts.extra import goodbye
from warcbench.scripts.example import parse_example


@click.group()
@click.option("-v", "--verbose", count=True, help="Verbosity; repeatable")
@click.pass_context
def cli(ctx, verbose):
    """warcbench command framework, work in progress"""
    ctx.ensure_object(dict)
    ctx.obj["VERBOSE"] = verbose


cli.add_command(goodbye)
cli.add_command(parse_example)


@cli.command()
@click.option("--world", default="World")
@click.pass_context
def hello(ctx, world):
    """Hello!"""
    if ctx.obj["VERBOSE"] > 2:
        click.echo("There's a lot to say.")
    click.echo(f"Hello {world}!")


@cli.command()
def extract():
    """This could extract a file from a response record."""
    raise click.ClickException("Not yet implemented")
