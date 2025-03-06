import click
import json
from mimetypes import guess_extension
from pathlib import Path

from warcbench import WARCParser
from warcbench.filters import http_response_content_type_filter
from warcbench.scripts.example import parse_example
from warcbench.utils import open_archive


@click.group()
@click.option(
    "-o",
    "--out",
    type=click.Choice(["raw", "json", "pprint"], case_sensitive=False),
    default="raw",
)
@click.option("-v", "--verbose", count=True, help="Verbosity; repeatable")
@click.pass_context
def cli(ctx, out, verbose):
    """warcbench command framework, work in progress"""
    ctx.ensure_object(dict)
    ctx.obj["OUT"] = out
    ctx.obj["VERBOSE"] = verbose


cli.add_command(parse_example)


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


@cli.command()
@click.argument(
    "filepath",
    type=click.Path(exists=True, readable=True, allow_dash=True, dir_okay=False),
)
@click.pass_context
def parse(ctx, filepath):
    """This counts the records found in the archive, and reports warning and error messages."""
    ctx.obj["FILEPATH"] = filepath

    open_and_parse(
        ctx,
        parser_callbacks=[
            lambda parser: click.echo(
                f"Found {len(parser.records)} records\nWarnings: {parser.warnings}\nError: {parser.error}"
            )
        ],
    )


@cli.command()
@click.argument(
    "filepath",
    type=click.Path(exists=True, readable=True, allow_dash=True, dir_okay=False),
)
@click.argument("mimetype")
@click.option(
    "--basename", help="Base name for output file; defaults to FILEPATH base name."
)
@click.pass_context
def extract(ctx, filepath, mimetype, basename):
    """This extracts files of the given MIMETYPE from the archive at FILEPATH, writing them to {basename}-{recordstart}.{extension}."""
    ctx.obj["FILEPATH"] = filepath

    open_and_parse(
        ctx,
        filters=[
            http_response_content_type_filter(mimetype),
        ],
        record_handlers=[
            extract_file(
                mimetype,
                basename if basename else Path(filepath).name,
                ctx.obj["VERBOSE"],
            )
        ],
    )


def extract_file(mimetype, basename, verbose):
    """A record-handler for file extraction."""

    def f(record):
        if verbose:
            click.echo(
                f"Found a response of type {mimetype} at position {record.start}",
                err=True,
            )
        filename = f"{basename}-{record.start}{guess_extension(mimetype)}"
        Path(filename).parent.mkdir(exist_ok=True, parents=True)
        with open(filename, "wb") as f:
            f.write(record.get_http_body_block())

    return f


def open_and_parse(ctx, filters=[], record_handlers=[], parser_callbacks=[]):
    """This function runs the parser, filtering and running record handlers and parser callbacks as necessary."""
    try:
        with open_archive(ctx.obj["FILEPATH"]) as warc_file:
            parser = WARCParser(
                warc_file,
                filters=filters,
                record_handlers=record_handlers,
                parser_callbacks=parser_callbacks,
            )
            parser.parse()
    except (ValueError, NotImplementedError) as e:
        raise click.ClickException(e)
