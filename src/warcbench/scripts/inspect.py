import click
from collections import defaultdict
import json

from warcbench.member_handlers import get_member_offsets
from warcbench.record_handlers import (
    get_record_offsets,
    get_record_headers,
    get_record_http_headers,
)
from warcbench.scripts.utils import open_and_parse, format_record_data_for_output


@click.command(short_help="Get detailed record metadata.")
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
    """
    Get detailed metadata describing an archive's records.
    (Use `wb summarize` for a high-level summary.)

    Output can be quite verbose and should be adapted to suit your purposes.
    The default report includes all available metadata; use the options to
    suppress unwanted information.

    ---

    Example:

      \b
      $ wb inspect example.com.warc.gz

      \b
      Member bytes 0-237

      \b
      Record bytes 0-280

      \b
      WARC/1.1
      WARC-Filename: archive.warc
      WARC-Date: 2024-11-04T19:10:55.900Z
      WARC-Type: warcinfo
      WARC-Record-ID: <urn:uuid:a6fd8346-f170-497b-9e26-47a5bde6d86c>
      Content-Type: application/warc-fields
      Content-Length: 57

      \b
      ----------------------------------------
      Member bytes 237-876

      \b
      (etc.)
    """
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
        ctx,
        member_handlers=member_handlers,
        record_handlers=record_handlers,
        extra_parser_kwargs={
            "cache_header_bytes": True,
            "cache_content_block_bytes": True,
        },
    )

    #
    # Output
    #

    records = format_record_data_for_output(data)

    if ctx.obj["OUT"] == "json":
        click.echo(json.dumps({"records": records}))
    else:
        for record in records:
            if record.get("member_offsets"):
                click.echo(
                    f"Member bytes {record['member_offsets'][0]}-{record['member_offsets'][1]}\n"
                )
            if record.get("record_offsets"):
                click.echo(
                    f"Record bytes {record['record_offsets'][0]}-{record['record_offsets'][1]}\n"
                )
            if record.get("record_headers"):
                for header in record["record_headers"]:
                    click.echo(header)
                click.echo()
            if record.get("record_http_headers"):
                for header in record["record_http_headers"]:
                    click.echo(header)
                click.echo()
            click.echo("-" * 40)
