import click
import json
import gzip
import zipfile
from mimetypes import guess_extension
from pathlib import Path
from warcbench import WARCParser
from warcbench.filters import http_response_content_type_filter
from warcbench.scripts.example import parse_example


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
@click.argument("filepath", type=click.Path(exists=True, readable=True))
@click.pass_context
def parse(ctx, filepath):
    """This counts the records found in the archive, and reports warning and error messages."""
    ctx.obj["FILEPATH"] = filepath

    parse_and_run(
        ctx,
        lambda p: click.echo(
            f"Found {len(p.records)} records\nWarnings: {p.warnings}\nError: {p.error}"
        ),
        filters=[],
    )


@cli.command()
@click.argument("filepath", type=click.Path(exists=True, readable=True))
@click.argument("mimetype")
@click.option(
    "--basename", help="Base name for output file; defaults to FILEPATH base name."
)
@click.pass_context
def extract(ctx, filepath, mimetype, basename):
    """Using a content type filter, this extracts files of the given MIMETYPE from the archive at FILEPATH, writing them to {basename}-{recordstart}.{extension}."""
    ctx.obj["FILEPATH"] = filepath

    parse_and_run(
        ctx,
        partial_function(
            _extract,
            mimetype,
            basename if basename else Path(filepath).name,
        ),
        filters=[
            http_response_content_type_filter(mimetype),
        ],
    )


def _extract(mimetype, basename, parser):
    """A version of the file extractor using a content type filter."""
    click.echo(len(parser.records))
    for r in parser.records:
        click.echo(
            f"Found a response of type {mimetype} at position {r.start}",
            err=True,
        )
        filename = f"{basename}-{r.start}{guess_extension(mimetype)}"
        Path(filename).parent.mkdir(exist_ok=True, parents=True)
        with open(filename, "wb") as f:
            f.write(r.get_http_body_block())


def partial_function(func, *fixed_args):
    """This closure provides partial application, for building the function passed to parse_and_run(). Note that the function passed in to this function should include `parser` as its *last* argument."""

    def _partial_function(*args):
        return func(*fixed_args, *args)

    return _partial_function


def parse_and_run(ctx, f, filters=[]):
    """This function, not entirely DRY, handles three different archive file types, and runs the passed function on the parser, optionally filtering."""
    input_file = ctx.obj["FILEPATH"]
    if input_file.lower().endswith(".wacz"):
        with (
            click.open_file(input_file, mode="rb") as wacz_file,
            zipfile.Path(wacz_file, "archive/data.warc.gz").open("rb") as warc_gz_file,
            gzip.open(warc_gz_file, "rb") as warc_file,
        ):
            parser = WARCParser(
                warc_file,
                filters=filters,
            )
            parser.parse()
            f(parser)
    elif input_file.lower().endswith(".warc.gz"):
        with (
            click.open_file(input_file, mode="rb") as warc_gz_file,
            gzip.open(warc_gz_file, "rb") as warc_file,
        ):
            parser = WARCParser(
                warc_file,
                filters=filters,
            )
            parser.parse()
            f(parser)
    elif input_file.lower().endswith(".warc"):
        with click.open_file(input_file, mode="rb") as warc_file:
            parser = WARCParser(
                warc_file,
                filters=filters,
            )
            parser.parse()
            f(parser)
    else:
        raise click.ClickException("This doesn't look like a web archive")
