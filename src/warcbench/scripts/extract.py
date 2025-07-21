import click
from pathlib import Path

from warcbench.filters import http_response_content_type_filter
from warcbench.scripts.utils import (
    CLICachingConfig,
    CLIProcessorConfig,
    extract_file,
    open_and_parse,
)


@click.command(short_help="Extract files of MIMETYPE to disk.")
@click.argument(
    "filepath",
    type=click.Path(exists=True, readable=True, allow_dash=True, dir_okay=False),
)
@click.argument("mimetype")
@click.argument("extension")
@click.option(
    "--basename", help="Base name for output file; defaults to FILEPATH base name."
)
@click.option(
    "--decode/--no-decode",
    help="When Content-Encoding for a record is set, whether to decode.",
    default=True,
    show_default=True,
)
@click.pass_context
def extract(ctx, filepath, mimetype, extension, basename, decode):
    """This extracts files of the given MIMETYPE from the archive at FILEPATH, writing them to {basename}-{recordstart}.{EXTENSION}."""
    ctx.obj["FILEPATH"] = filepath

    def log_response_found(record):
        if ctx.obj["VERBOSE"]:
            click.echo(
                f"Found a response of type {mimetype} at position {record.start}",
                err=True,
            )
        return True

    open_and_parse(
        ctx,
        processor_config=CLIProcessorConfig(
            record_filters=[
                http_response_content_type_filter(mimetype),
                log_response_found,
            ],
            record_handlers=[
                extract_file(
                    basename if basename else Path(filepath).name,
                    extension,
                    decode,
                )
            ],
        ),
        cache_config=CLICachingConfig(
            header_bytes=True,
            content_block_bytes=True,
        ),
    )
