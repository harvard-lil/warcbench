# Standard library imports
import click
from collections import defaultdict
import json
import sys

# Warcbench imports
from warcbench.filters import (
    http_header_filter,
    http_response_content_type_filter,
    http_status_filter,
    http_verb_filter,
    record_content_length_filter,
    record_content_type_filter,
    warc_header_regex_filter,
    warc_named_field_filter,
)
from warcbench.member_handlers import get_member_offsets
from warcbench.record_handlers import (
    get_record_headers,
    get_record_http_body,
    get_record_http_headers,
    get_record_offsets,
)
from warcbench.scripts.utils import (
    CLICachingConfig,
    CLIProcessorConfig,
    dynamically_import,
    format_record_data_for_output,
    open_and_parse,
    output,
    output_record,
)

# Typing imports
from typing import Any, Callable, Dict, List, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from warcbench.models import GzippedMember, Record


class PathOrStdout(click.Path):
    def convert(self, value, param, ctx):
        if value == "-":
            return sys.stdout  # Return stdout if the value is '-'
        return super().convert(value, param, ctx)  # Otherwise, use the default behavior


@click.command(short_help="Filter records; optionally extract to a new archive.")
@click.argument(
    "filepath",
    type=click.Path(exists=True, readable=True, allow_dash=True, dir_okay=False),
)
@click.option(
    "--filter-by-http-header",
    nargs=2,
    help="Find records with WARC-Type: {request, response} and look for the supplied HTTP header name and value.",
)
@click.option(
    "--filter-by-http-response-content-type",
    nargs=1,
    help="Find records with WARC-Type: response, and then filters by Content-Type.",
)
@click.option(
    "--filter-by-http-status-code",
    nargs=1,
    type=int,
    help="Find records with WARC-Type: response, and then filters by HTTP status code.",
)
@click.option(
    "--filter-by-http-verb",
    nargs=1,
    help="Find records with WARC-Type: request, and then filter by HTTP verb.",
)
@click.option(
    "--filter-by-record-content-length",
    nargs=2,
    type=(int, click.Choice(["eq", "lt", "le", "gt", "ge", "ne"], case_sensitive=True)),
    help="Filter by the WARC record's reported Content-Length. Takes a length and an operator.",
)
@click.option(
    "--filter-by-record-content-type",
    nargs=1,
    help="Filter by the WARC record's own Content-Type (e.g. warcinfo, request, response). See related --filter_by_http_response_content_type.",
)
@click.option(
    "--filter-warc-header-with-regex",
    nargs=1,
    help="Filter the bytes of each record's WARC header against the regex produced by encoding this string (utf-8).",
)
@click.option(
    "--filter-by-warc-named-field",
    nargs=2,
    help="Find records with the header WARC-[field_name]: [value].",
)
@click.option(
    "--force-include-warcinfo/--no-force-include-warcinfo",
    default=False,
    help="Force include warcinfo records in output, if present, regardless of other filters.",
)
@click.option(
    "--custom-filter-path",
    nargs=1,
    type=click.Path(exists=True, readable=True, allow_dash=True, dir_okay=False),
    help="Path to a python file with custom filter functions exposed in __all__. Filter functions should take a warcbench.models.Record and return True/False.",
)
@click.option(
    "--output-count/--no-output-count",
    default=True,
    help="Output the number of records matching the filters.",
)
@click.option(
    "--output-member-offsets/--no-output-member-offsets",
    default=False,
    help="Output the offsets of each gzipped member.",
)
@click.option(
    "--output-record-offsets/--no-output-record-offsets",
    default=False,
    help="Output the offsets of each record in the file (uncompressed).",
)
@click.option(
    "--output-warc-headers/--no-output-warc-headers",
    default=False,
    help="Output the WARC headers of each record.",
)
@click.option(
    "--output-http-headers/--no-output-http-headers",
    default=False,
    help="Output the HTTP headers of any record whose content is an HTTP request or response.",
)
@click.option(
    "--output-http-body/--no-output-http-body",
    default=False,
    help="Include the HTTP body of any record whose content is an HTTP request or response.",
)
@click.option(
    "--extract-to-warc",
    type=PathOrStdout(),
    help="Extract records to FILEPATH or - for stdout.",
)
@click.option(
    "--extract-to-gzipped-warc",
    type=PathOrStdout(),
    help="Extract records to FILEPATH or - for stdout. GZIP each record individually, outputting a canonical warc.gz file.",
)
@click.option(
    "--extract-summary-to",
    type=PathOrStdout(),
    help="In addition to extracting records, direct output from any supplied --output-* options to FILEPATH or - for stdout.",
)
@click.option(
    "--custom-record-handler-path",
    nargs=1,
    type=click.Path(exists=True, readable=True, allow_dash=True, dir_okay=False),
    help="Path to a python file with custom record handler functions exposed in __all__. Handler functions should take a warcbench.models.Record.",
)
@click.pass_context
def filter_records(
    ctx,
    filepath,
    filter_by_http_header,
    filter_by_http_response_content_type,
    filter_by_http_status_code,
    filter_by_http_verb,
    filter_by_record_content_length,
    filter_by_record_content_type,
    filter_warc_header_with_regex,
    filter_by_warc_named_field,
    force_include_warcinfo,
    custom_filter_path,
    output_count,
    output_member_offsets,
    output_record_offsets,
    output_warc_headers,
    output_http_headers,
    output_http_body,
    extract_to_warc,
    extract_to_gzipped_warc,
    extract_summary_to,
    custom_record_handler_path,
):
    """
    Applies the specified filters (if any) to the archive's records. If no filters
    are specified, all WARC records are considered to match.

    By default, outputs the number of matching records. Use the `--output-*`
    options to include more detailed information about matching records, or
    `--no-output-count` to suppress the count.

    Can also extract the matching records to a new WARC file
    (`--extract-to-warc`, `--extract-to-gzipped-warc`). To ensure the new
    WARC includes a `WARC-Type: warcinfo` record (if present in the original),
    even if it would otherwise be filtered out by any applied filters, run
    with `--force-include-warcinfo`.

    If extracting records to a new WARC file, by default, no other output
    is produced. To produce a summary report as well, run with `--extract-summary-to`.

    To apply your own, custom filters, use `--custom-filter-path` to specify the
    path to a python file where the custom filter functions are listed, in desired
    order of application, in `__all__`.
    See `tests/assets/custom-filters.py` for an example.
    See the "Filters" section of the README for more information on constructing filters.

    This command also supports custom record handlers, which can be used to do arbitrary
    work on records that pass through the supplied filters. For example, you could use
    record handlers to construct a custom report, or export records one-at-a-time
    to an upstream service.
    Use `--custom-record-handler-path` to specify the path to a python file where the
    custom handler functions are listed, in desired order of application, in `__all__`.
    See `tests/assets/custom-handlers.py` for an example.
    See the "Handlers" section of the README for more information on constructing handlers.

    ---

    Example:

      \b
      $ wb filter-records --filter-by-warc-named-field Type response tests/assets/example.com.warc
      Found 6 records.
    """
    ctx.obj["FILEPATH"] = filepath

    #
    # Collect filters and handlers
    #

    built_in_filters: Dict[str, Callable[..., Callable[["Record"], bool]]] = {
        "filter_by_http_header": http_header_filter,
        "filter_by_http_verb": http_verb_filter,
        "filter_by_http_status_code": http_status_filter,
        "filter_by_http_response_content_type": http_response_content_type_filter,
        "filter_by_record_content_length": record_content_length_filter,
        "filter_by_record_content_type": record_content_type_filter,
        "filter_warc_header_with_regex": warc_header_regex_filter,
        "filter_by_warc_named_field": warc_named_field_filter,
    }

    filters: List[Tuple[Callable[..., Callable[["Record"], bool]], List[Any]]] = []
    member_handlers: List[Callable[["GzippedMember"], None]] = []
    record_handlers: List[Callable[["Record"], None]] = []

    data: Dict[str, Any] = {"count": None, "record_info": defaultdict(list)}

    # Handle output options
    if extract_to_warc and extract_to_gzipped_warc:
        raise click.ClickException(
            "Incompatible options: only one of --extract-to-warc or --extract-to-gzipped-warc may be set."
        )

    if extract_to_warc and extract_summary_to and extract_to_warc == extract_summary_to:
        raise click.ClickException(
            "Incompatible options: --extract-to-warc and --extract-summary-to cannot output to the same destination."
        )

    if (
        extract_to_gzipped_warc
        and extract_summary_to
        and extract_to_gzipped_warc == extract_summary_to
    ):
        raise click.ClickException(
            "Incompatible options: --extract-to-gzipped-warc and --extract-summary-to cannot output to the same destination."
        )

    if extract_to_warc or extract_to_gzipped_warc:
        if extract_summary_to:
            output_to = extract_summary_to
        else:
            output_to = None
    else:
        output_to = sys.stdout

    # Handle options that take arguments
    for flag_name, value in ctx.params.items():
        if flag_name == "extract_to_warc" and value:
            record_handlers.append(output_record(value, gzip=False))

        elif flag_name == "extract_to_gzipped_warc" and value:
            record_handlers.append(output_record(value, gzip=True))

        elif flag_name in built_in_filters and value:
            if isinstance(value, tuple):
                filters.append((built_in_filters[flag_name], [*value]))
            else:
                filters.append((built_in_filters[flag_name], [value]))

        elif flag_name == "custom_filter_path" and value:
            custom_filters = dynamically_import("custom_filters", value)
            if not hasattr(custom_filters, "__all__"):
                raise click.ClickException(f"{value} does not define __all__.")
            for f in custom_filters.__all__:
                filters.append((lambda: (getattr(custom_filters, f)), []))

        elif flag_name == "custom_record_handler_path" and value:
            custom_record_handlers = dynamically_import("custom_record_handlers", value)
            if not hasattr(custom_record_handlers, "__all__"):
                raise click.ClickException(f"{value} does not define __all__.")
            for f in custom_record_handlers.__all__:
                record_handlers.append(getattr(custom_record_handlers, f))

    # Handle options that don't take arguments

    if output_member_offsets:
        member_handlers.append(
            get_member_offsets(
                append_to=data["record_info"]["member_offsets"], print_each=False
            )
        )

    if output_count:
        data["count"] = 0

        def increment_record_count(_):
            data["count"] += 1

        record_handlers.append(increment_record_count)

    if output_record_offsets:
        record_handlers.append(
            get_record_offsets(
                append_to=data["record_info"]["record_offsets"], print_each=False
            )
        )

    if output_warc_headers:
        record_handlers.append(
            get_record_headers(
                append_to=data["record_info"]["record_headers"], print_each=False
            )
        )

    if output_http_headers:
        record_handlers.append(
            get_record_http_headers(
                append_to=data["record_info"]["record_http_headers"], print_each=False
            )
        )

    if output_http_body:
        record_handlers.append(
            get_record_http_body(
                append_to=data["record_info"]["record_http_body"], print_each=False
            )
        )

    # Post process filters
    if force_include_warcinfo:
        record_filters = [
            lambda record: (
                (record.header.get_field("WARC-Type", decode=True) == "warcinfo")
                or all(f(record) for f in [fltr(*args) for fltr, args in filters])
            )
        ]
    else:
        record_filters = [fltr(*args) for fltr, args in filters]

    #
    # Parse
    #

    open_and_parse(
        ctx,
        processor_config=CLIProcessorConfig(
            record_filters=record_filters,
            member_handlers=member_handlers,
            record_handlers=record_handlers,
        ),
        cache_config=CLICachingConfig(
            header_bytes=True,
            parsed_headers=True,
            content_block_bytes=True,
        ),
    )

    #
    # Handle output
    #

    formatted_data = {}

    if data["count"] is not None:
        formatted_data["count"] = data["count"]

    if data["record_info"]:
        formatted_data["records"] = format_record_data_for_output(data["record_info"])

    if data:
        if ctx.obj["OUT"] == "json" and formatted_data:
            output(output_to, json.dumps(formatted_data))
        else:
            if formatted_data.get("count"):
                output(output_to, f"Found {formatted_data['count']} records.")

            for record in formatted_data.get("records", []):
                if record.get("member_offsets"):
                    output(
                        output_to,
                        f"Member bytes {record['member_offsets'][0]}-{record['member_offsets'][1]}\n",
                    )
                if record.get("record_offsets"):
                    output(
                        output_to,
                        f"Record bytes {record['record_offsets'][0]}-{record['record_offsets'][1]}\n",
                    )
                if record.get("record_headers"):
                    for header in record["record_headers"]:
                        output(output_to, header)
                    output(output_to, "")
                if record.get("record_http_headers"):
                    for header in record["record_http_headers"]:
                        output(output_to, header)
                    output(output_to, "")
                if record.get("record_http_body"):
                    output(output_to, record["record_http_body"])
                    output(output_to, "")
                output(output_to, "-" * 40)
