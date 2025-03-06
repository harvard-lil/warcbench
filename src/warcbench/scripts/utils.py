import click
from mimetypes import guess_extension
from pathlib import Path
from warcbench import WARCParser
from warcbench.utils import python_open_archive, system_open_archive


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


def open_and_parse(ctx, filters=None, record_handlers=None, parser_callbacks=None):
    """This function runs the parser, filtering and running record handlers and parser callbacks as necessary."""
    if ctx.obj["DECOMPRESSION"] == "python":
        open_archive = python_open_archive
    elif ctx.obj["DECOMPRESSION"] == "system":
        open_archive = system_open_archive

    try:
        with open_archive(ctx.obj["FILEPATH"]) as warc_file:
            parser = WARCParser(
                warc_file,
                filters=filters,
                record_handlers=record_handlers,
                parser_callbacks=parser_callbacks,
            )
            parser.parse()
    except (ValueError, NotImplementedError, RuntimeError) as e:
        raise click.ClickException(e)
