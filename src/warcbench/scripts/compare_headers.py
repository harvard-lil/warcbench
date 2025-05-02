import click
from collections import defaultdict
import json

from warcbench import WARCParser, WARCGZParser
from warcbench.utils import FileType, python_open_archive, system_open_archive


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
    "--include-extra-header-fields",
    multiple=True,
    help="Extra WARC record header fields to compare.",
)
@click.option(
    "--exclude-header-fields",
    multiple=True,
    help="WARC record header fields to exclude from the comparison.",
)
@click.option(
    "--near-match-fields",
    multiple=True,
    help="WARC record header fields which may differ, indicating a near match, rather than uniqueness.",
)
@click.option(
    "--output-summary/--no-output-summary",
    default=True,
    help="Summarize the number of matching, nearly-matching, and unique records.",
)
@click.option(
    "--output-matching-record-details/--no-output-matching-record-details",
    default=False,
    help="Include detailed metadata about matching records in output.",
)
@click.option(
    "--output-near-matching-record-details/--no-output-near-matching-record-details",
    default=False,
    help="Include detailed metadata about nearly-matching records in output.",
)
@click.option(
    "--output-near-matching-record-http-header-diffs/--no-output-near-matching-record-http-header-diffs",
    default=False,
    help="Include a diff of the http headers of nearly-matching request/response records in output.",
)
@click.option(
    "--output-unique-record-details/--no-output-unique-record-details",
    default=False,
    help="Include detailed metadata about unique records in output.",
)
@click.option(
    "--extract-near-matching-record-http-bodies-to",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, writable=True),
    help="Extract the decompressed http bodies of nearly-matching records to this directory, as extensionless files.",
)
@click.pass_context
def compare_headers(ctx, filepath1, filepath2, output_summary):
    """
    Compares the record headers of two archives and reports how they differ.

    \b
    Defaults to comparing only a small subset of header fields:
    - WARC-Type
    - WARC-Target-URI
    - WARC-Payload-Digest
    - Content-Type
    - Content-Length

    Use `--include-extra-header-fields FIELDNAME FIELDNAME ...` to include additional fields.

    Use `--exclude-header-fields FIELDNAME FIELDNAME ...` to exclude particular fields.

    Records are sorted first by WARC-Type and then by WARC-Target-URI, and then
    matched, as possible, into pairs.

    Records are then classified as matching, nearly-matching, or unique. By default,
    records are considered nearly-matching when all the compared headers match except
    for WARC-Payload-Digest or Content-Length.

    Use `--near-match-fields FIELDNAME FIELDNAME ...`to supply a custom list.

    By default, outputs a summary. Use the `--output-*` options to get more details
    about matching, nearly-matching, or unique records.

    To more easily inspect nearly-matching records and determine whether their differences
    are meaningful, you can output a diff of their http headers. You can also extract their
    http bodies to a directory of your choice. The extracted files will be extensionless
    for simplicity. Filenames will include the source filename and the record start offset.
    """
    ctx.obj["FILEPATH1"] = filepath1
    ctx.obj["FILEPATH2"] = filepath2

    if ctx.obj["DECOMPRESSION"] == "python":
        open_archive = python_open_archive
    elif ctx.obj["DECOMPRESSION"] == "system":
        open_archive = system_open_archive

    with open_archive(ctx.obj["FILEPATH1"], ctx.obj["GUNZIP"]) as (file, file_type):
        pass

    with open_archive(ctx.obj["FILEPATH2"], ctx.obj["GUNZIP"]) as (file, file_type):
        pass
