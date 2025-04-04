import click
from mimetypes import guess_extension
from pathlib import Path
from warcbench import WARCParser, WARCGZParser
from warcbench.utils import FileType, python_open_archive, system_open_archive


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


def open_and_parse(
    ctx,
    record_filters=None,
    member_handlers=None,
    record_handlers=None,
    parser_callbacks=None,
    cache_records_or_members=False,
):
    """This function runs the parser, filtering and running record handlers and parser callbacks as necessary."""
    if ctx.obj["DECOMPRESSION"] == "python":
        open_archive = python_open_archive
    elif ctx.obj["DECOMPRESSION"] == "system":
        open_archive = system_open_archive

    try:
        with open_archive(ctx.obj["FILEPATH"], ctx.obj["GUNZIP"]) as (file, file_type):
            if file_type == FileType.WARC:
                if member_handlers:
                    click.echo(
                        "WARNING: parsing as WARC file, member_handlers will be ignored.",
                        err=True,
                    )
                parser = WARCParser(
                    file,
                    record_filters=record_filters,
                    record_handlers=record_handlers,
                    parser_callbacks=parser_callbacks,
                )
                parse_kwargs = {"cache_records": cache_records_or_members}
            elif file_type == FileType.GZIPPED_WARC:
                parser = WARCGZParser(
                    file,
                    record_filters=record_filters,
                    member_handlers=member_handlers,
                    record_handlers=record_handlers,
                    parser_callbacks=parser_callbacks,
                )
                parse_kwargs = {"cache_members": cache_records_or_members}
            parser.parse(**parse_kwargs)
    except (ValueError, NotImplementedError, RuntimeError) as e:
        raise click.ClickException(e)
