import brotli
import click
from mimetypes import guess_extension
from pathlib import Path
from pyzstd import decompress as pyzstd_decompress
from warcbench import WARCParser, WARCGZParser
from warcbench.utils import (
    FileType,
    python_open_archive,
    system_open_archive,
    find_pattern_in_bytes,
)
import zlib


def extract_file(mimetype, basename, decode, verbose):
    """A record-handler for file extraction."""

    def f(record):
        if not (http_body_block := record.get_http_body_block()):
            return
        if verbose:
            click.echo(
                f"Found a response of type {mimetype} at position {record.start}",
                err=True,
            )
        if decode:
            match = find_pattern_in_bytes(
                rb"Content-Encoding:\s*(.*)((\r\n)|$)",
                record.get_http_header_block(),
                case_insensitive=True,
            )
            if match:
                try:
                    encodings = (
                        match.group(1).decode("utf-8", errors="replace").split(" ")
                    )
                except Exception:
                    pass
            try:
                http_body_block = content_decode(http_body_block, encodings)
            except Exception as e:
                click.echo(
                    f"Failed to decode record starting at {record.start}, passing through encoded: {e}"
                )

        filename = f"{basename}-{record.start}{guess_extension(mimetype)}"
        Path(filename).parent.mkdir(exist_ok=True, parents=True)
        with open(filename, "wb") as fh:
            fh.write(http_body_block)

    return f


def content_decode(http_body_block, encodings):
    """This function recursively decodes an HTTP body block, given a list of encodings."""
    if not encodings:
        return http_body_block
    else:
        return content_decode(
            _content_decode(http_body_block, encodings[-1]), encodings[:-1]
        )


def _content_decode(http_body_block, encoding):
    """This function decodes an HTTP body block with a given encoding."""
    # see https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Encoding
    if encoding == "gzip":
        return zlib.decompress(http_body_block, 16 + zlib.MAX_WBITS)
    elif encoding == "deflate":
        return zlib.decompress(http_body_block, -15)
    elif encoding == "br":
        return brotli.decompress(http_body_block)
    elif encoding == "zstd":
        return pyzstd_decompress(http_body_block)
    elif encoding == "dcb":
        click.echo("Passing dcb-encoded body block as is")
    elif encoding == "dcz":
        click.echo("Passing dcz-encoded body block as is")

    return http_body_block


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
