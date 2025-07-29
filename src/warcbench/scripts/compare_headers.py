# Standard library imports
import click
from collections import OrderedDict
import difflib
from http.server import HTTPServer
import json
import socket

# Warcbench imports
from warcbench import WARCParser, WARCGZParser
from warcbench.scripts.utils import CLICachingConfig, get_warc_response_handler
from warcbench.utils import FileType, python_open_archive, system_open_archive

# Typing imports
from typing import Any, Dict, Generator, List, Union, TYPE_CHECKING, cast

if TYPE_CHECKING:
    from warcbench.models import Record


@click.command(short_help="Compare the record headers of two archives.")
@click.argument(
    "filepath1",
    type=click.Path(exists=True, readable=True, allow_dash=True, dir_okay=False),
)
@click.argument(
    "filepath2",
    type=click.Path(exists=True, readable=True, allow_dash=True, dir_okay=False),
)
@click.option(
    "--include-extra-header-field",
    multiple=True,
    help="Extra WARC record header field to compare.",
)
@click.option(
    "--exclude-header-field",
    multiple=True,
    help="WARC record header field to exclude from the comparison.",
)
@click.option(
    "--near-match-field",
    multiple=True,
    help="WARC record header field which may differ, indicating a near match, rather than uniqueness.",
)
@click.option(
    "--output-summary/--no-output-summary",
    default=True,
    show_default=True,
    help="Summarize the number of matching, nearly-matching, and unique records.",
)
@click.option(
    "--output-matching-record-details/--no-output-matching-record-details",
    default=False,
    show_default=True,
    help="Include detailed metadata about matching records in output.",
)
@click.option(
    "--output-near-matching-record-details/--no-output-near-matching-record-details",
    default=False,
    show_default=True,
    help="Include detailed metadata about nearly-matching records in output.",
)
@click.option(
    "--output-near-matching-record-header-diffs/--no-output-near-matching-record-header-diffs",
    default=False,
    show_default=True,
    help="Include a diff of the warc headers of nearly-matching request/response records in output.",
)
@click.option(
    "--output-near-matching-record-http-header-diffs/--no-output-near-matching-record-http-header-diffs",
    default=False,
    show_default=True,
    help="Include a diff of the http headers of nearly-matching request/response records in output.",
)
@click.option(
    "--output-unique-record-details/--no-output-unique-record-details",
    default=False,
    show_default=True,
    help="Include detailed metadata about unique records in output.",
)
@click.option(
    "--serve-near-matching-records/--no-serve-near-matching-records",
    default=False,
    show_default=True,
    help="Serve a website with side-by-side comparisons of nearly-matching records.",
)
@click.option(
    "--server-host",
    type=str,
    default="127.0.0.1",
    show_default=True,
    help="The hostname or IP address for the server to bind to.",
)
@click.option(
    "--server-port",
    type=click.IntRange(1, 65535),
    default=8080,
    show_default=True,
    help="The port on which the server will accept connections (1-65535).",
)
@click.pass_context
def compare_headers(
    ctx,
    filepath1,
    filepath2,
    include_extra_header_field,
    exclude_header_field,
    near_match_field,
    output_summary,
    output_matching_record_details,
    output_near_matching_record_details,
    output_near_matching_record_header_diffs,
    output_near_matching_record_http_header_diffs,
    output_unique_record_details,
    serve_near_matching_records,
    server_host,
    server_port,
):
    """
    Compares the record headers of two archives and reports how they differ.

    \b
    Defaults to comparing only a small subset of header fields:
    - WARC-Type
    - WARC-Target-URI
    - WARC-Payload-Digest
    - Content-Length

    Use `--include-extra-header-field FIELDNAME` (repeatable) to include additional fields.

    Use `--exclude-header-field FIELDNAME` (repeatable) to exclude particular fields.

    Records are sorted first by WARC-Type and then by WARC-Target-URI, and then
    matched, as possible, into pairs.

    Records are then classified as matching, nearly-matching, or unique. By default,
    records are considered nearly-matching when all the compared headers match except
    for WARC-Payload-Digest or Content-Length.

    Use `--near-match-field FIELDNAME` (repeatable) to supply a custom set of fields.

    By default, outputs a summary. Use the `--output-*` options to get more details
    about matching, nearly-matching, or unique records.

    To more easily inspect nearly-matching records and determine whether their differences
    are meaningful, you can output a diff of their http headers. You can also spin up
    a web server that will show side-by-side comparisons of nearly-matching HTTP response
    records in iframes, for visual comparison and/or inspection in your browser's dev tools.

    ---

    Example:

      \b
      $ wb compare-headers before.wacz after.wacz --serve-near-matching-records
      #
      # SUMMARY
      #

      \b
      Matching records: 5
      Nearly-matching records: 3
      Unique records (before.wacz): 0
      Unique records (after.wacz): 0

      \b
      Server started http://127.0.0.1:8080
    """
    ctx.obj["FILEPATH1"] = filepath1
    ctx.obj["FILEPATH2"] = filepath2

    if ctx.obj["DECOMPRESSION"] == "python":
        open_archive = python_open_archive
    elif ctx.obj["DECOMPRESSION"] == "system":
        open_archive = system_open_archive

    #
    # Compile the list of fields to compare
    #

    for field in ["WARC-Type", "WARC-Target-URI"]:
        if field in exclude_header_field:
            raise click.ClickException(
                "WARC-Type and WARC-Target-URI cannot be excluded from comparisons."
            )

    compare_fields = [
        "WARC-Payload-Digest",
        "Content-Length",
        *include_extra_header_field,
    ]
    for field in exclude_header_field:
        try:
            compare_fields.remove(field)
        except ValueError:
            pass

    #
    # Compile the list of fields that count towards nearly matching
    #

    if near_match_field:
        near_match_fields = [*near_match_field]
    else:
        near_match_fields = [
            "WARC-Payload-Digest",
            "Content-Length",
        ]

    # Collect record info

    def collect_records(path, gunzip):
        records: Dict[str, Union[List["Record"], OrderedDict[str, List["Record"]]]] = {}
        with open_archive(path, gunzip) as (file, file_type):
            cache_config = CLICachingConfig(
                parsed_headers=True,
                header_bytes=output_matching_record_details
                or output_near_matching_record_details
                or output_near_matching_record_http_header_diffs
                or output_unique_record_details,
                content_block_bytes=serve_near_matching_records,
            )

            if file_type == FileType.WARC:
                warc_parser = WARCParser(file, cache=cache_config.to_warc_config())
                iterator = warc_parser.iterator()
            elif file_type == FileType.GZIPPED_WARC:
                warcgz_parser = WARCGZParser(
                    file, cache=cache_config.to_warc_gz_config()
                )
                iterator = cast(
                    Generator["Record", None, None],
                    warcgz_parser.iterator(yield_type="records"),
                )

            for record in iterator:
                # Get record type as string for dictionary operations
                record_type = cast(
                    str,
                    record.header.get_field("WARC-Type", decode=True),  # type: ignore[union-attr]
                )
                if record_type == "warcinfo":
                    records.setdefault(record_type, [])
                    cast(List["Record"], records[record_type]).append(record)
                else:
                    records.setdefault(record_type, OrderedDict())
                    # Get target URI as string for dictionary operations
                    target = cast(
                        str,
                        record.header.get_field("WARC-Target-URI", "", decode=True),  # type: ignore[union-attr]
                    )
                    cast(
                        OrderedDict[str, List["Record"]], records[record_type]
                    ).setdefault(target, [])
                    cast(OrderedDict[str, List["Record"]], records[record_type])[
                        target
                    ].append(record)
        return records

    records1 = collect_records(ctx.obj["FILEPATH1"], ctx.obj["GUNZIP"])
    records2 = collect_records(ctx.obj["FILEPATH2"], ctx.obj["GUNZIP"])

    record_types = set(records1.keys())
    record_types.update(records2.keys())

    unique_records1 = []
    unique_records2 = []
    matching_records = []
    near_matching_records = []

    # NB: sets and set operations do not preserve order.
    # Sort, so that the order of output is more stable.
    for record_type in sorted(record_types):
        if record_type == "warcinfo":
            pass
        else:
            urls1 = set(records1[record_type]) if record_type in records1 else set()
            urls2 = set(records2[record_type]) if record_type in records2 else set()

            # NB: sets and set operations do not preserve order.
            # Sort, so that the order of output is more stable.
            common = sorted(urls1.intersection(urls2))
            unique1 = sorted(urls1.difference(urls2))
            unique2 = sorted(urls2.difference(urls1))

            for url in unique1:
                unique_records1.extend(records1[record_type][url])

            for url in unique2:
                unique_records2.extend(records2[record_type][url])

            for url in common:
                url_records1 = records1[record_type][url]
                url_records2 = records2[record_type][url]

                if len(url_records1) != len(url_records2):
                    pass
                else:
                    for record1, record2 in zip(url_records1, url_records2):
                        matches = True
                        near_matches = True
                        for field in compare_fields:
                            if record1.header.get_field(
                                field, "", decode=True
                            ) != record2.header.get_field(field, "", decode=True):
                                matches = False
                                if field not in near_match_fields:
                                    near_matches = False

                        if matches:
                            matching_records.append((record1, record2))
                        elif near_matches:
                            near_matching_records.append((record1, record2))
                        else:
                            unique_records1.append(record1)
                            unique_records2.append(record2)

    if ctx.obj["OUT"] == "json":
        output: Dict[str, Any] = {}

        if output_summary:
            output["summary"] = {
                "matching": len(matching_records),
                "near_matching": len(near_matching_records),
                "unique": {
                    ctx.obj["FILEPATH1"]: len(unique_records1),
                    ctx.obj["FILEPATH2"]: len(unique_records2),
                },
            }

        def format_record_details(record):
            return {
                "start": record.start,
                "end": record.end,
                "headers": record.header.get_parsed_fields(decode=True),
            }

        if output_matching_record_details:
            output["matching"] = [
                {
                    ctx.obj["FILEPATH1"]: format_record_details(record1),
                    ctx.obj["FILEPATH2"]: format_record_details(record2),
                }
                for record1, record2 in matching_records
            ]

        if output_near_matching_record_details:
            output["near_matching"] = [
                {
                    ctx.obj["FILEPATH1"]: format_record_details(record1),
                    ctx.obj["FILEPATH2"]: format_record_details(record2),
                }
                for record1, record2 in near_matching_records
            ]

        if output_near_matching_record_header_diffs:
            output["near_matching_header_diffs"] = [
                list(
                    difflib.ndiff(
                        record1.header.bytes.decode(
                            "utf-8", errors="replace"
                        ).splitlines(keepends=True),
                        record2.header.bytes.decode(
                            "utf-8", errors="replace"
                        ).splitlines(keepends=True),
                    )
                )
                for record1, record2 in near_matching_records
            ]

        if output_near_matching_record_http_header_diffs:
            output["near_matching_http_header_diffs"] = []
            for record1, record2 in near_matching_records:
                record1_headers = record1.get_http_header_block()
                record2_headers = record2.get_http_header_block()
                if record1_headers:
                    record1_headers = record1_headers.decode("utf-8", errors="replace")
                else:
                    record1_headers = ""
                if record2_headers:
                    record2_headers = record2_headers.decode("utf-8", errors="replace")
                else:
                    record2_headers = ""

                output["near_matching_http_header_diffs"].append(
                    list(
                        difflib.ndiff(
                            record1_headers.splitlines(keepends=True),
                            record2_headers.splitlines(keepends=True),
                        )
                    )
                )

        if output_unique_record_details:
            output["unique"] = {}
            output["unique"][ctx.obj["FILEPATH1"]] = [
                format_record_details(record) for record in unique_records1
            ]
            output["unique"][ctx.obj["FILEPATH2"]] = [
                format_record_details(record) for record in unique_records2
            ]

        click.echo(json.dumps(output))

    else:
        if output_summary:
            click.echo("#\n# SUMMARY\n#")
            click.echo()
            click.echo(f"Matching records: {len(matching_records)}")
            click.echo(f"Nearly-matching records: {len(near_matching_records)}")
            click.echo(
                f"Unique records ({ctx.obj['FILEPATH1']}): {len(unique_records1)}"
            )
            click.echo(
                f"Unique records ({ctx.obj['FILEPATH2']}): {len(unique_records2)}"
            )
            click.echo()

        def output_record_details(filepath, record):
            click.echo(f"File {filepath}")
            click.echo(f"Record bytes {record.start}-{record.end} (uncompressed)")
            click.echo()
            click.echo(record.header.bytes.decode("utf-8", errors="replace"))
            click.echo()

        if output_matching_record_details:
            click.echo("#\n# MATCHING RECORD DETAILS\n#")
            click.echo()
            if matching_records:
                for record1, record2 in matching_records:
                    output_record_details(ctx.obj["FILEPATH1"], record1)
                    output_record_details(ctx.obj["FILEPATH2"], record2)
                    click.echo("-" * 40)
            else:
                click.echo("None")

        if output_near_matching_record_details:
            click.echo("#\n# NEARLY-MATCHING RECORD DETAILS\n#")
            click.echo()
            if near_matching_records:
                for record1, record2 in near_matching_records:
                    output_record_details(ctx.obj["FILEPATH1"], record1)
                    output_record_details(ctx.obj["FILEPATH2"], record2)
                    click.echo("-" * 40)
            else:
                click.echo("None")

        if output_near_matching_record_header_diffs:
            click.echo("#\n# NEARLY-MATCHING RECORD HEADER DIFFS\n#")
            click.echo()
            if near_matching_records:
                for record1, record2 in near_matching_records:
                    for line in difflib.ndiff(
                        record1.header.bytes.decode(
                            "utf-8", errors="replace"
                        ).splitlines(keepends=True),
                        record2.header.bytes.decode(
                            "utf-8", errors="replace"
                        ).splitlines(keepends=True),
                    ):
                        click.echo(line.rstrip())
                    click.echo()
                    click.echo("-" * 40)
            else:
                click.echo("None")

        if output_near_matching_record_http_header_diffs:
            click.echo("#\n# NEARLY-MATCHING RECORD HTTP HEADER DIFFS\n#")
            click.echo()
            if near_matching_records:
                for record1, record2 in near_matching_records:
                    record1_headers = record1.get_http_header_block()
                    record2_headers = record2.get_http_header_block()
                    if record1_headers:
                        record1_headers = record1_headers.decode(
                            "utf-8", errors="replace"
                        )
                    if record2_headers:
                        record2_headers = record2_headers.decode(
                            "utf-8", errors="replace"
                        )

                    if not record1_headers and record2_headers:
                        click.echo(record1_headers or "No HTTP headers found.")
                        click.echo(record2_headers or "No HTTP headers found.")
                        click.echo()
                        click.echo("-" * 40)
                        continue

                    for line in difflib.ndiff(
                        record1_headers.splitlines(keepends=True),
                        record2_headers.splitlines(keepends=True),
                    ):
                        click.echo(line.rstrip())
                    click.echo()
                    click.echo("-" * 40)
            else:
                click.echo("None")

        if output_unique_record_details:
            click.echo("#\n# UNIQUE RECORD DETAILS\n#")
            click.echo()
            if unique_records1:
                for record in unique_records1:
                    output_record_details(ctx.obj["FILEPATH1"], record)
                    click.echo("-" * 40)
                click.echo("-" * 40)
            if unique_records2:
                for record in unique_records2:
                    output_record_details(ctx.obj["FILEPATH2"], record)
                    click.echo("-" * 40)
            else:
                click.echo("None")

    if serve_near_matching_records:
        responses = {}
        for index, (record1, record2) in enumerate(near_matching_records):
            if record1.header.get_field("WARC-Type", decode=True) == "response":
                responses[f"/{index + 1}/"] = (index + 1, record1, record2)

        handler = get_warc_response_handler(
            responses, ctx.obj["FILEPATH1"], ctx.obj["FILEPATH2"]
        )
        web_server = HTTPServer((server_host, server_port), handler)
        click.echo("Server started http://%s:%s" % (server_host, server_port), err=True)

        try:
            stop_event = ctx.obj.get("stop_event")
            if stop_event:
                # We should listen for a threading.Event() to be set(),
                # to know when to stop the server.
                # (For example, during testing.)

                # Set a timeout so that web_server.handle_request()
                # doesn't wait indefinitely for the next request to come in,
                # but instead times out, causing us to check stop_event.is_set() frequently
                web_server.socket.settimeout(0.1)  # 100ms
                while not stop_event.is_set():
                    try:
                        web_server.handle_request()
                    except socket.timeout:
                        continue
            else:
                # Otherwise, listen for SIGINT
                web_server.serve_forever()
        except KeyboardInterrupt:
            pass

        web_server.server_close()
        click.echo("Server stopped.", err=True)
