import click

from warcbench.scripts.compare_headers import compare_headers
from warcbench.scripts.compare_parsers import compare_parsers
from warcbench.scripts.extract import extract
from warcbench.scripts.filter_records import filter_records
from warcbench.scripts.inspect import inspect
from warcbench.scripts.match_record_pairs import match_record_pairs
from warcbench.scripts.summarize import summarize


@click.group()
@click.option(
    "-o",
    "--out",
    type=click.Choice(["raw", "json"], case_sensitive=False),
    default="raw",
    help="Format subcommand output as a human-readable report (raw) or as JSON.",
)
@click.option("-v", "--verbose", count=True, help="Logging verbosity; repeatable.")
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
@click.version_option(None, "-V", "--version")
@click.help_option("-h", "--help")
@click.pass_context
def cli(ctx, out, verbose, decompression, gunzip):
    """WARCbench command framework"""
    ctx.ensure_object(dict)
    ctx.obj["OUT"] = out
    ctx.obj["VERBOSE"] = verbose
    ctx.obj["DECOMPRESSION"] = decompression
    ctx.obj["GUNZIP"] = gunzip


cli.add_command(summarize)
cli.add_command(inspect)
cli.add_command(filter_records)
cli.add_command(extract)
cli.add_command(compare_headers)
cli.add_command(compare_parsers)
cli.add_command(match_record_pairs)
