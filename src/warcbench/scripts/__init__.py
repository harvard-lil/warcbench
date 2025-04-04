import click
import json

from warcbench.scripts.extract import extract
from warcbench.scripts.inspect import inspect
from warcbench.scripts.summarize import summarize


@click.group()
@click.option(
    "-o",
    "--out",
    type=click.Choice(["raw", "json", "pprint"], case_sensitive=False),
    default="raw",
)
@click.option("-v", "--verbose", count=True, help="Verbosity; repeatable")
@click.option(
    "-d",
    "--decompression",
    type=click.Choice(["python", "system"], case_sensitive=False),
    default="python",
    show_default=True,
    help="Use native Python or system tools for extracting archives.",
)
@click.option(
    "--gunzip/--no-gunzip",
    default=False,
    show_default=True,
    help="Gunzip the input archive before parsing, if it is gzipped.",
)
@click.pass_context
def cli(ctx, out, verbose, decompression, gunzip):
    """warcbench command framework, work in progress"""
    ctx.ensure_object(dict)
    ctx.obj["OUT"] = out
    ctx.obj["VERBOSE"] = verbose
    ctx.obj["DECOMPRESSION"] = decompression
    ctx.obj["GUNZIP"] = gunzip


@cli.command()
@click.option("--world", default="World")
@click.pass_context
def hello(ctx, world):
    """Hello!"""
    msg = f"Hello {world}!"
    if ctx.obj["OUT"] == "json":
        click.echo(json.dumps({"message": msg}))
    elif ctx.obj["OUT"] == "pprint":
        click.echo(click.style(f"👋 {msg}", fg="green"))
    else:
        click.echo(msg)


cli.add_command(summarize)
cli.add_command(inspect)
cli.add_command(extract)


@cli.command()
def extract_payload():
    """Similar to `extract`, but accepts generic filtering options."""
    raise click.ClickException("Not yet implemented")


@cli.command()
def filter():
    """This could filter records to stdout, or write them into a file."""
    raise click.ClickException("Not yet implemented")


@cli.command()
def compare_parsers():
    """This could parse the same WARC with all parsing strategies, and report if/how the results differ."""
    raise click.ClickException("Not yet implemented")
