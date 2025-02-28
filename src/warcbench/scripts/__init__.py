import click


@click.group()
def cli():
    """warcbench command framework, work in progress"""
    pass


@cli.command()
def hello():
    """Hello!"""
    click.echo("Hello World!")


@cli.command()
def extract():
    """This could extract a file from a response record."""
    raise click.ClickException("Not yet implemented")
