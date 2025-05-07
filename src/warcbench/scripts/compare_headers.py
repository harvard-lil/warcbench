import click
from collections import OrderedDict
import difflib
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
    "--output-near-matching-record-header-diffs/--no-output-near-matching-record-header-diffs",
    default=False,
    help="Include a diff of the warc headers of nearly-matching request/response records in output.",
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
    extract_near_matching_record_http_bodies_to,
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
        records = {}
        with open_archive(path, gunzip) as (file, file_type):
            parser_options = {
                "cache_parsed_headers": True,
                "cache_header_bytes": output_matching_record_details
                or output_near_matching_record_details
                or output_near_matching_record_http_header_diffs
                or output_unique_record_details,
                "cache_content_block_bytes": extract_near_matching_record_http_bodies_to,
            }
            if file_type == FileType.WARC:
                parser = WARCParser(file, **parser_options)
                iterator = parser.iterator()
            elif file_type == FileType.GZIPPED_WARC:
                parser = WARCGZParser(file, **parser_options)
                iterator = parser.iterator(yield_type="records")

            for record in iterator:
                record_type = record.header.get_field("WARC-Type", decode=True)
                if record_type == "warcinfo":
                    records.setdefault(record_type, [])
                    records[record_type].append(record)
                else:
                    records.setdefault(record_type, OrderedDict())
                    target = record.header.get_field("WARC-Target-URI", "", decode=True)
                    records[record_type].setdefault(target, [])
                    records[record_type][target].append(record)
        return records

    records1 = collect_records(ctx.obj["FILEPATH1"], ctx.obj["GUNZIP"])
    records2 = collect_records(ctx.obj["FILEPATH2"], ctx.obj["GUNZIP"])

    record_types = set(records1.keys())
    record_types.update(records2.keys())

    unique_records1 = []
    unique_records2 = []
    matching_records = []
    near_matching_records = []

    for record_type in record_types:
        if record_type == "warcinfo":
            pass
        else:
            urls1 = set(records1[record_type]) if record_type in records1 else set()
            urls2 = set(records2[record_type]) if record_type in records2 else set()

            common = urls1.intersection(urls2)
            unique1 = urls1.difference(urls2)
            unique2 = urls2.difference(urls1)

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
        output = {}

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
