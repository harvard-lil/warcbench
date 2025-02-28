import click


@click.command()
@click.pass_context
def goodbye(ctx):
    """Trying out a command from another file"""
    click.echo("Goodbye cruel world :)")
    if ctx.obj["VERBOSE"] > 0:
        click.echo("I wish I had more to say.")
