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


def open_and_invoke(
    ctx,
    invoke_method,
    invoke_args=None,
    invoke_kwargs=None,
    record_filters=None,
    member_handlers=None,
    record_handlers=None,
    parser_callbacks=None,
    cache_records_or_members=False,
    extra_parser_kwargs=None,
):
    if not invoke_args:
        invoke_args = []
    if not invoke_kwargs:
        invoke_kwargs = {}
    if not extra_parser_kwargs:
        extra_parser_kwargs = {}

    if ctx.obj["DECOMPRESSION"] == "python":
        open_archive = python_open_archive
    elif ctx.obj["DECOMPRESSION"] == "system":
        open_archive = system_open_archive

    try:
        with open_archive(ctx.obj["FILEPATH"], ctx.obj["GUNZIP"]) as (file, file_type):
            #
            # Validate and configure options
            #
            if invoke_method == "parse":
                cache_records_or_members_kwarg = {
                    FileType.WARC: "cache_records",
                    FileType.GZIPPED_WARC: "cache_members",
                }
                invoke_kwargs[cache_records_or_members_kwarg[file_type]] = (
                    cache_records_or_members
                )

            elif cache_records_or_members:
                raise ValueError(
                    "The option cache_records_or_members=True is only meaningful when invoking parser.parse()."
                )

            #
            # Initialize parser
            #
            if file_type == FileType.WARC:
                if member_handlers and ctx.obj["VERBOSE"]:
                    click.echo(
                        "DEBUG: parsing as WARC file, member_handlers will be ignored.\n",
                        err=True,
                    )
                parser = WARCParser(
                    file,
                    record_filters=record_filters,
                    record_handlers=record_handlers,
                    parser_callbacks=parser_callbacks,
                    **extra_parser_kwargs,
                )
            elif file_type == FileType.GZIPPED_WARC:
                parser = WARCGZParser(
                    file,
                    record_filters=record_filters,
                    member_handlers=member_handlers,
                    record_handlers=record_handlers,
                    parser_callbacks=parser_callbacks,
                    **extra_parser_kwargs,
                )

            return getattr(parser, invoke_method)(*invoke_args, **invoke_kwargs)
    except (ValueError, NotImplementedError, RuntimeError) as e:
        raise click.ClickException(e)


def open_and_parse(
    ctx,
    record_filters=None,
    member_handlers=None,
    record_handlers=None,
    parser_callbacks=None,
    cache_records_or_members=False,
    extra_parser_kwargs=None,
):
    """This function runs the parser, filtering and running record handlers and parser callbacks as necessary."""
    if not extra_parser_kwargs:
        extra_parser_kwargs = {}

    return open_and_invoke(
        ctx,
        "parse",
        record_filters=record_filters,
        member_handlers=member_handlers,
        record_handlers=record_handlers,
        parser_callbacks=parser_callbacks,
        cache_records_or_members=cache_records_or_members,
        extra_parser_kwargs=extra_parser_kwargs,
    )
