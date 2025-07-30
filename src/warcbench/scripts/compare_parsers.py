# Standard library imports
import click
from collections import defaultdict
import json

# Warcbench imports
from warcbench import WARCParser, WARCGZParser
from warcbench.config import WARCParsingConfig, WARCGZParsingConfig
from warcbench.utils import FileType, python_open_archive, system_open_archive

# Typing imports
from typing import Any


@click.command(short_help="Compare all available parsing strategies.")
@click.argument(
    "filepath",
    type=click.Path(exists=True, readable=True, allow_dash=True, dir_okay=False),
)
@click.option(
    "--output-offsets/--no-output-offsets",
    default=False,
    show_default=True,
    help="Include the offsets of members/records in output.",
)
@click.pass_context
def compare_parsers(ctx, filepath, output_offsets):
    """
    This parses a single WARC with all parsing strategies, and reports whether
    the results differ.

    Run with `--output-offsets` to get more detail.
    """
    ctx.obj["FILEPATH"] = filepath
    ctx.obj["OUTPUT_OFFSETS"] = output_offsets

    if ctx.obj["DECOMPRESSION"] == "python":
        open_archive = python_open_archive
    elif ctx.obj["DECOMPRESSION"] == "system":
        open_archive = system_open_archive

    try:
        data: defaultdict[str, defaultdict[str, Any]] = defaultdict(
            lambda: defaultdict(dict)
        )

        #
        # Parse
        #

        with open_archive(ctx.obj["FILEPATH"], ctx.obj["GUNZIP"]) as (file, file_type):
            if file_type == FileType.WARC:
                #
                # Get the offsets
                #
                delimiter_parser = WARCParser(
                    file, parsing_options=WARCParsingConfig(style="delimiter")
                )
                data["record"]["offsets"]["delimiter_parser"] = (
                    delimiter_parser.get_record_offsets(split=True)
                )
                data["warnings"]["delimiter_parser"] = delimiter_parser.warnings
                data["error"]["delimiter_parser"] = delimiter_parser.error

                content_length_parser = WARCParser(
                    file, parsing_options=WARCParsingConfig(style="content_length")
                )
                data["record"]["offsets"]["content_length_parser"] = (
                    content_length_parser.get_record_offsets(split=True)
                )
                data["warnings"]["content_length_parser"] = (
                    content_length_parser.warnings
                )
                data["error"]["content_length_parser"] = content_length_parser.error

                #
                # See if they match
                #
                first_record_parser = next(iter(data["record"]["offsets"]))
                record_offests_match = all(
                    data["record"]["offsets"][parser]
                    == data["record"]["offsets"][first_record_parser]
                    for parser in data["record"]["offsets"]
                )
                data["record"]["all_match"] = record_offests_match

            elif file_type == FileType.GZIPPED_WARC:
                #
                # Get the offsets, parsing as a gzipped warc
                #

                # Member offsets
                data["member"]["offsets"]["gzip_member_decompressing_parser"] = (
                    WARCGZParser(
                        file,
                        parsing_options=WARCGZParsingConfig(
                            decompression_style="member"
                        ),
                        enable_lazy_loading_of_bytes=False,
                    ).get_member_offsets(compressed=True)
                )

                # Record offsets
                gzip_member_decompressing_parser = WARCGZParser(
                    file,
                    parsing_options=WARCGZParsingConfig(decompression_style="member"),
                    enable_lazy_loading_of_bytes=False,
                )
                data["record"]["offsets"]["gzip_member_decompressing_parser"] = (
                    gzip_member_decompressing_parser.get_record_offsets(split=True)
                )
                data["warnings"]["gzip_member_decompressing_parser"] = (
                    gzip_member_decompressing_parser.warnings
                )
                data["error"]["gzip_member_decompressing_parser"] = (
                    gzip_member_decompressing_parser.error
                )

                # Member offsets
                data["member"]["offsets"]["gzip_file_decompressing_parser"] = (
                    WARCGZParser(
                        file,
                        parsing_options=WARCGZParsingConfig(decompression_style="file"),
                    ).get_member_offsets(compressed=True)
                )

                # Record offsets
                gzip_file_decompressing_parser = WARCGZParser(
                    file,
                    parsing_options=WARCGZParsingConfig(decompression_style="file"),
                )
                data["record"]["offsets"]["gzip_file_decompressing_parser"] = (
                    gzip_file_decompressing_parser.get_record_offsets(split=True)
                )
                data["warnings"]["gzip_file_decompressing_parser"] = (
                    gzip_file_decompressing_parser.warnings
                )
                data["error"]["gzip_file_decompressing_parser"] = (
                    gzip_file_decompressing_parser.error
                )

        #
        # If we just parsed a gzipped warc, also get the offsets after decompressing and parsing as a warc
        #

        if file_type == FileType.GZIPPED_WARC:
            with open_archive(ctx.obj["FILEPATH"], gunzip=True) as (file, _):
                #
                # Get the offsets
                #
                delimiter_parser = WARCParser(
                    file, parsing_options=WARCParsingConfig(style="delimiter")
                )
                data["record"]["offsets"]["delimiter_parser"] = (
                    delimiter_parser.get_record_offsets(split=True)
                )
                data["warnings"]["delimiter_parser"] = delimiter_parser.warnings
                data["error"]["delimiter_parser"] = delimiter_parser.error

                content_length_parser = WARCParser(
                    file, parsing_options=WARCParsingConfig(style="content_length")
                )
                data["record"]["offsets"]["content_length_parser"] = (
                    content_length_parser.get_record_offsets(split=True)
                )
                data["warnings"]["content_length_parser"] = (
                    content_length_parser.warnings
                )
                data["error"]["content_length_parser"] = content_length_parser.error

                #
                # See if everything matches
                #
                first_member_parser = next(iter(data["member"]["offsets"]))
                member_offests_match = all(
                    data["member"]["offsets"][parser]
                    == data["member"]["offsets"][first_member_parser]
                    for parser in data["member"]["offsets"]
                )
                data["member"]["all_match"] = member_offests_match

                first_record_parser = next(iter(data["record"]["offsets"]))
                record_offests_match = all(
                    data["record"]["offsets"][parser]
                    == data["record"]["offsets"][first_record_parser]
                    for parser in data["record"]["offsets"]
                )
                data["record"]["all_match"] = record_offests_match

        #
        # Check for warnings and errors
        #

        data["warnings"]["any"] = any(
            bool(warnings) for warnings in data["warnings"].values()
        )
        data["error"]["any"] = any(bool(error) for error in data["error"].values())

        #
        # Output
        #

        if ctx.obj["OUT"] == "json":
            if not ctx.obj["OUTPUT_OFFSETS"]:
                if "record" in data:
                    data["record"].pop("offsets", None)
                if "member" in data:
                    data["member"].pop("offsets", None)
            click.echo(json.dumps(data))
        else:
            click.echo("PARSERS")
            for parser in data["record"]["offsets"]:
                click.echo(f"{' '.join(parser.split('_')).capitalize()}")
            click.echo()

            click.echo("ERRORS")
            if data["error"]["any"]:
                click.echo(json.dumps(data["error"], indent=2))
            else:
                click.echo("None")
            click.echo()

            click.echo("WARNINGS")
            if data["warnings"]["any"]:
                click.echo(json.dumps(data["warnings"], indent=2))
            else:
                click.echo("None")
            click.echo()

            click.echo("COMPARE OFFSETS")
            if data["member"]:
                click.echo(f"All member offsets match: {data['member']['all_match']}")
            click.echo(f"All record offsets match: {data['record']['all_match']}")
            if ctx.obj["OUTPUT_OFFSETS"]:
                if data["member"]:
                    click.echo("\nMEMBER OFFSETS")
                    for parser_name, offsets in data["member"]["offsets"].items():
                        click.echo(
                            f"\n{' '.join(parser_name.split('_')).capitalize()}\n"
                        )
                        for start, end in offsets:
                            click.echo(f"{start}-{end}")

                click.echo("\nRECORD OFFSETS")
                for parser_name, offsets in data["record"]["offsets"].items():
                    click.echo(f"\n{' '.join(parser_name.split('_')).capitalize()}\n")
                    for (
                        header_start,
                        header_end,
                        content_block_start,
                        content_block_end,
                    ) in offsets:
                        click.echo(
                            f"Header {header_start}-{header_end}, Content Block {content_block_start}-{content_block_end}"
                        )
                click.echo()

    except (ValueError, RuntimeError) as e:
        raise click.ClickException(str(e))
