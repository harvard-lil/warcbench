import click
from pathlib import Path

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
from warcbench.scripts.utils import open_and_parse, dynamically_import


@click.command()
@click.argument(
    "filepath",
    type=click.Path(exists=True, readable=True, allow_dash=True, dir_okay=False),
)
@click.option(
    "--gzip/--no-gzip",
    default=False,
    show_default=True,
    help="Individually gzip each WARC record, outputting a canonical warc.gz file.",
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
    "--custom-filter-path",
    nargs=1,
    type=click.Path(exists=True, readable=True, allow_dash=True, dir_okay=False),
    help="Path to a python file with custom filter functions exposed in __all__. Filter functions should take a warcbench.models.Record and return True/False.",
)
@click.pass_context
def filter_records(
    ctx,
    filepath,
    gzip,
    filter_by_http_header,
    filter_by_http_response_content_type,
    filter_by_http_status_code,
    filter_by_http_verb,
    filter_by_record_content_length,
    filter_by_record_content_type,
    filter_warc_header_with_regex,
    filter_by_warc_named_field,
    custom_filter_path,
):
    """"""
    ctx.obj["FILEPATH"] = filepath
    ctx.obj["GZIP"] = gzip

    #
    # Collect filters
    #

    built_in_filters = {
        "filter_by_http_header": http_header_filter,
        "filter_by_http_verb": http_verb_filter,
        "filter_by_http_status_code": http_status_filter,
        "filter_by_http_response_content_type": http_response_content_type_filter,
        "filter_by_record_content_length": record_content_length_filter,
        "filter_by_record_content_type": record_content_type_filter,
        "filter_warc_header_with_regex": warc_header_regex_filter,
        "filter_by_warc_named_field": warc_named_field_filter,
    }

    filters = []
    for flag_name, value in ctx.params.items():
        if flag_name in built_in_filters and value:
            if isinstance(value, tuple):
                filters.append((built_in_filters[flag_name], [*value]))
            else:
                filters.append((built_in_filters[flag_name], [value]))

        if flag_name == "custom_filter_path" and value:
            custom_filters = dynamically_import("custom_filters", value)
            if not hasattr(custom_filters, "__all__"):
                raise click.ClickException(print("{value} does not define __all__."))
            for f in custom_filters.__all__:
                filters.append((lambda: (getattr(custom_filters, f)), []))

    #
    # Parse and extract
    #

    open_and_parse(
        ctx,
        record_filters=[f(*args) for f, args in filters],
        record_handlers=[lambda record: print(record.header.bytes)],
        extra_parser_kwargs={
            "cache_header_bytes": True,
            "cache_parsed_headers": True,
            "cache_content_block_bytes": True,
        },
    )
