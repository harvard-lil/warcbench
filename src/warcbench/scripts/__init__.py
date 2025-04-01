import click
import json
from pathlib import Path
from warcbench.filters import http_response_content_type_filter
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
def inspect():
    """This could report variously detailed information on the WARC's contents."""
    # Possibly replaces the 'parse' command.
    raise click.ClickException("Not yet implemented")


@cli.command()
def compare_parsers():
    """This could parse the same WARC with all parsing strategies, and report if/how the results differ."""
    raise click.ClickException("Not yet implemented")
