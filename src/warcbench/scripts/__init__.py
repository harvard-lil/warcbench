import click
from collections import defaultdict
import json
from pathlib import Path
from warcbench.filters import http_response_content_type_filter
from warcbench.member_handlers import get_member_offsets
from warcbench.record_handlers import (
    get_record_offsets,
    get_record_headers,
    get_record_http_headers,
)
from warcbench.scripts.utils import extract_file, open_and_parse


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
@click.option(
    "--member-offsets/--no-member-offsets",
    default=True,
    show_default=True,
    help="Include the offsets of each gzipped member.",
)
@click.option(
    "--record-offsets/--no-record-offsets",
    default=True,
    show_default=True,
    help="Include the offsets of each record in the file (uncompressed).",
)
@click.option(
    "--record-headers/--no-record-headers",
    default=True,
    show_default=True,
    help="Include the WARC headers of each record.",
)
@click.option(
    "--record-http-headers/--no-record-http-headers",
    default=True,
    show_default=True,
    help="Include the HTTP headers of any record whose content is an HTTP request or response.",
)
@click.pass_context
def inspect(
    ctx,
    filepath,
    member_offsets,
    record_offsets,
    record_headers,
    record_http_headers,
):
    """Get metadata describing an archive's records."""
    #
    # Handle options
    #

    ctx.obj["FILEPATH"] = filepath
    ctx.obj["MEMBER_OFFSETS"] = member_offsets
    ctx.obj["RECORD_OFFSETS"] = record_offsets
    ctx.obj["RECORD_HEADERS"] = record_headers
    ctx.obj["RECORD_HTTP_HEADERS"] = record_http_headers

    data = defaultdict(list)
    member_handlers = []
    if ctx.obj["MEMBER_OFFSETS"]:
        member_handlers.append(
            get_member_offsets(append_to=data["member_offsets"], print_each=False)
        )

    record_handlers = []
    if ctx.obj["RECORD_OFFSETS"]:
        record_handlers.append(
            get_record_offsets(append_to=data["record_offsets"], print_each=False)
        )
    if ctx.obj["RECORD_HEADERS"]:
        record_handlers.append(
            get_record_headers(append_to=data["record_headers"], print_each=False)
        )
    if ctx.obj["RECORD_HTTP_HEADERS"]:
        record_handlers.append(
            get_record_http_headers(
                append_to=data["record_http_headers"], print_each=False
            )
        )

    #
    # Parse
    #

    open_and_parse(
        ctx, member_handlers=member_handlers, record_handlers=record_handlers
    )

    #
    # Format data for output
    #

    records = []

    if "member_offsets" in data:
        if not records:
            for offsets in data["member_offsets"]:
                records.append({"member_offsets": offsets})
        else:
            for index, offsets in enumerate(data["member_offsets"]):
                records[index]["member_offsets"] = offsets

    if "record_offsets" in data:
        if not records:
            for offsets in data["record_offsets"]:
                records.append({"record_offsets": offsets})
        else:
            for index, offsets in enumerate(data["record_offsets"]):
                records[index]["record_offsets"] = offsets

    if "record_headers" in data:
        if not records:
            for header_set in data["record_headers"]:
                if header_set:
                    records.append(
                        {
                            "record_headers": [
                                line for line in header_set.split("\r\n") if line
                            ]
                        }
                    )
                else:
                    records.append({"record_headers": None})
        else:
            for index, header_set in enumerate(data["record_headers"]):
                if header_set:
                    records[index]["record_headers"] = [
                        line for line in header_set.split("\r\n") if line
                    ]
                else:
                    records[index]["record_headers"] = None

    if "record_http_headers" in data:
        if not records:
            for header_set in data["record_http_headers"]:
                if header_set:
                    records.append(
                        {
                            "record_http_headers": [
                                line for line in header_set.split("\r\n") if line
                            ]
                        }
                    )
                else:
                    records.append({"record_http_headers": None})
        else:
            for index, header_set in enumerate(data["record_http_headers"]):
                if header_set:
                    records[index]["record_http_headers"] = [
                        line for line in header_set.split("\r\n") if line
                    ]
                else:
                    records[index]["record_http_headers"] = None

    #
    # Output
    #

    if ctx.obj["OUT"] == "json":
        click.echo(json.dumps({"records": records}))
    else:
        for record in records:
            if record["member_offsets"]:
                click.echo(
                    f"Member bytes {record['member_offsets'][0]}-{record['member_offsets'][1]}\n"
                )
            if record["record_offsets"]:
                click.echo(
                    f"Record bytes {record['record_offsets'][0]}-{record['record_offsets'][1]}\n"
                )
            if record["record_headers"]:
                for header in record["record_headers"]:
                    click.echo(header)
                click.echo()
            if record["record_http_headers"]:
                for header in record["record_http_headers"]:
                    click.echo(header)
                click.echo()
            click.echo("-" * 40)


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
        record_filters=[
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
