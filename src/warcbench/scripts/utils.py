import base64
import click
import html
from http.server import BaseHTTPRequestHandler
import importlib.util
import io
from pathlib import Path
import re
import sys

from warcbench import WARCParser, WARCGZParser
from warcbench.exceptions import DecodingException
from warcbench.patches import patched_gzip
from warcbench.patterns import CRLF
from warcbench.utils import (
    FileType,
    python_open_archive,
    system_open_archive,
)


def dynamically_import(module_name, module_path):
    # Create a module specification
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    # Create a new module based on the specification
    module = importlib.util.module_from_spec(spec)
    # Execute the module in its own namespace
    spec.loader.exec_module(module)
    return module


def extract_file(basename, extension, decode):
    """A record-handler for file extraction."""

    def f(record):
        if decode:
            try:
                http_body_block = record.get_decompressed_http_body()
            except DecodingException as e:
                click.echo(f"Failed to decode block: {e}", err=True)
                http_body_block = record.get_http_body_block()
        else:
            http_body_block = record.get_http_body_block()
        if not http_body_block:
            return

        filename = f"{basename}-{record.start}{'.' + extension if extension else ''}"
        Path(filename).parent.mkdir(exist_ok=True, parents=True)
        with open(filename, "wb") as fh:
            fh.write(http_body_block)

    return f


def output(destination, data_string):
    if not destination:
        return
    elif destination is sys.stdout:
        click.echo(data_string)
    elif destination is sys.stderr:
        click.echo(data_string, err=True)
    elif isinstance(destination, io.IOBase):
        destination.write(data_string)
    else:
        with open(destination, "a") as file:
            file.write(data_string)


def output_record(output_to, gzip=False):
    """
    A record-handler for outputting WARC records
    """

    def f(record):
        if gzip:
            if output_to is sys.stdout:
                with patched_gzip.open(sys.stdout.buffer, "wb") as stdout:
                    stdout.write(record.bytes + CRLF * 2)
            elif output_to is sys.stderr:
                with patched_gzip.open(sys.stderr.buffer, "wb") as stderr:
                    stderr.write(record.bytes + CRLF * 2)
            else:
                with patched_gzip.open(output_to, "ab") as file:
                    file.write(record.bytes + CRLF * 2)
        else:
            if output_to is sys.stdout:
                sys.stdout.buffer.write(record.bytes + CRLF * 2)
            elif output_to is sys.stderr:
                sys.stderr.buffer.write(record.bytes + CRLF * 2)
            else:
                with open(output_to, "ab") as file:
                    file.write(record.bytes + CRLF * 2)

    return f


def format_record_data_for_output(data):
    records = []

    if "member_offsets" in data:
        if not records:
            for offsets in data["member_offsets"]:
                records.append({"member_offsets": offsets})
        else:
            for index, offsets in enumerate(data["member_offsets"]):
                records[index]["member_offsets"] = offsets

    if "record_offsets" in data:
        if not records:
            for offsets in data["record_offsets"]:
                records.append({"record_offsets": offsets})
        else:
            for index, offsets in enumerate(data["record_offsets"]):
                records[index]["record_offsets"] = offsets

    if "record_headers" in data:
        if not records:
            for header_set in data["record_headers"]:
                if header_set:
                    records.append(
                        {
                            "record_headers": [
                                line for line in header_set.split("\r\n") if line
                            ]
                        }
                    )
                else:
                    records.append({"record_headers": None})
        else:
            for index, header_set in enumerate(data["record_headers"]):
                if header_set:
                    records[index]["record_headers"] = [
                        line for line in header_set.split("\r\n") if line
                    ]
                else:
                    records[index]["record_headers"] = None

    if "record_http_headers" in data:
        if not records:
            for header_set in data["record_http_headers"]:
                if header_set:
                    records.append(
                        {
                            "record_http_headers": [
                                line for line in header_set.split("\r\n") if line
                            ]
                        }
                    )
                else:
                    records.append({"record_http_headers": None})
        else:
            for index, header_set in enumerate(data["record_http_headers"]):
                if header_set:
                    records[index]["record_http_headers"] = [
                        line for line in header_set.split("\r\n") if line
                    ]
                else:
                    records[index]["record_http_headers"] = None

    return records


def get_warc_response_handler(pairs, file1, file2):
    def get_warc_record_fields_as_html(record):
        data = bytearray()
        data.extend(b"<p>")
        for field, values in record.header.get_parsed_fields(decode=True).items():
            data.extend(
                bytes(
                    f"""
                {field}: {html.escape(values[0]) if values[0] else values[0]}<br>
            """,
                    "utf-8",
                )
            )
        data.extend(b"</p>")
        return data

    class WARCResponseHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/":
                self.send_response(200)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(
                    bytes(
                        "<html><head><title>Nearly-Matching Records' HTTP Responses</title></head>",
                        "utf-8",
                    )
                )
                self.wfile.write(
                    bytes(
                        f"""
                    <body>
                      <h1>Nearly-Matching Records' HTTP Responses</h1>
                      <p> Comparing:<br><br>
                        {file1}<br>
                        {file2}
                      </p>
                      <ul>
                """,
                        "utf-8",
                    )
                )
                for path, (index, _, _) in self.pairs.items():
                    self.wfile.write(
                        bytes(
                            f"""
                        <li><a href="{path}">Pair {index}</a></li>
                    """,
                            "utf-8",
                        )
                    )
                self.wfile.write(bytes("</ul></body></html>", "utf-8"))
                return

            elif self.path == "/favicon.ico":
                self.send_response(200)
                self.send_header("Content-type", "image/png")
                self.end_headers()
                # This is a PNG of 🛠️
                self.wfile.write(
                    base64.b64decode(
                        b"iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAAHPklEQ"
                        b"VRYR8WXeVDTZxrHv0nIwVHCYqIQaD1QqgJRWXBrhVYEtdqulhmd3d"
                        b"Etf7Cj1bp0drau7Ih2dqaLO9B2qtupDh7YbcW27qql1LaOolZBAUM"
                        b"Aa5BwB0MSIHImIQk59vn9WlljEox/dPedeWdy/H7P83mf++W43W4O"
                        b"gD20d9KOos18/zmXm4QbaB+iXcghgL304Z2fU+MUsvcxAPqfTj4lg"
                        b"8lsxh1VCzT3tGA+CwRCxMiisCA+DjOmTweXy/V4n+RCq9VieGQMiQ"
                        b"kLwOE8ZFiXC/QC87yBAaBv/s3udDrx2el/oX1gAsHhUjwljkRIaDi"
                        b"4PC6s5hGMjxoxSxqG9avTJpVoNBqUf1sJkzsU8xMWY8PyueDxeL4O"
                        b"6GYAGJ/4XMPDwzhbcR7iWUmYGLfB5XKDw+NDAAfmz5TiGZkEoSEhr"
                        b"GJmM6LOfVWBFu0IYmNnAk47Vj0vR3TUdL/W9QvQ1dWFG4oGiCKiMG"
                        b"QcBI/Ph3NiAjGRoch4cRlEIpGX0PKvz2PAygfsNjgmbEhLXojEpIU"
                        b"ez9ltNgiEwsnffAKYycdnzpUjSBiOkVET3MwJyWfdqlt44YXlePnX"
                        b"G7yUm0wmfHr2G/C4fLjJUnOixcjKWunh++8ufAeVZhg7c7LpAD9Ce"
                        b"AE4HA60d3TiWvVNREhkGDTeZ03vdDoQF2FDh54EvPknr6BTqZpR39"
                        b"ID89gYyBdIX5pAwZcwCapUKvHvry8iOeU5bFi1DHy+wBOA8d/31TX"
                        b"49oYKUdPEWJ2+BLebO2HU68nvFEBkgTnSIOh67+GV7M2Iio72sMLt"
                        b"OyrUNqrJ+jYWQEAnXJo0D0lJCejt7cXBw8cQP18OmVSMV9au8naBo"
                        b"kkFizACFpMVldcVWJ+ZgpsV5bCZRiEMDYMoUoJwkRPt6jvYkfcWYm"
                        b"JjPQBqautQo1CBF0SwTFyT26zuIAzdH4DVbkf8nFmsrIy0X0K+aLE"
                        b"3QA/ld6RUChN4KP3iKnhGNS5+eZqEAdGRkQglCOPYEBIS5yJjzXqs"
                        b"yFjpAaBsbMLRIx/j2YVkdg7lOGU3Y1Umdhggl8sJCmNs3ZbrEcCTM"
                        b"eCi4lBdU4t+C1B75nOM2YfBE4VgT8E+XLxUiUZSoLilwOpUORakr8"
                        b"SK9DRIJBIWglHU1NyKysvX0aKsw7PyRaSYLEGxwxZ6gnFYx7E660U"
                        b"kp6Z6gHsE4RgF0Nt/zsfd1laERUZg3bq1yM39/aSSs+e+xOdHj+HV"
                        b"3B3gCwXIXptBwcRHW2c3dGYXfrhVB3XtdWg62hGfKMcvJFK4KHhFg"
                        b"iBkZmVgSUqKZ0X0lQVDQ0MoPfExFsnlyMz0TCOG5OTJMtR9X4XlG3"
                        b"MRxrUgdXECqpo1uG8wQK/+AQa1ClyCW7dpI56OliFcLEbsM0/7q4T"
                        b"eaei3ZP30B+Oq4qIi2K1uzH5uDTpamsgaIZgY6EJ/212IJdNQ8Ld3"
                        b"EBYW9jhRvutAIG9ZrVbsfmsX5i5MRYhsNr4qK4GY48AM6TT8tbg4Y"
                        b"OU+C1EgAEzQKZQNOFD8HuKT01B+5iTiZ8bgUEkJIiljnmRN2Yx8CW"
                        b"LbbK8OHf0jOP3PUly+cB4ONxcyajjHjx3BvHnznkT/k8eAnoLt3vA"
                        b"4qi5fRc/teigbFNBSuebxhZhLxYaBkMlkAUM8kQX0hj7oRm24ea0K"
                        b"WlUDdOq7WLv5t/ikrAwtre3gC0RYLE/EkZLDiIiICAgiIADG7DpSb"
                        b"jDZUXPtBptu/W1qLPhVCv6Yn4+mpibkvJaDMYuVbbXpy5fhHwcPII"
                        b"RmhcetxwIwyu+Rzw2mCdRV3cBAhxqDPV1w8zgoOnyISnQoWwmPUoE"
                        b"qLn6Xih8HQmpEL697CX/fX8gWqqnWlABOyvkebS+0w1Yoq6tZxaN9"
                        b"etzXabH73SIaNpImZTNtfPvr23H5yhXqnEEQBQfjd5t/g7+QhR6dF"
                        b"x8G8gvAzILdpFwzMAZl1XWMkWLzoBHDul4sXZOFrTt3epVVPbXujZ"
                        b"s2UcvWEwOfXBCMP7yxA9u2bfV69gGETwDGpB3dGnToBtFYXQWzkVo"
                        b"qtVIGwEnt8T2KdH+V7tKlS8jLy8O41U7mF5KLgvH2vgJkZ2f7hPAJ"
                        b"0KvT4WZNAwyd7RgZHMToyAhsFjMG9b3YvncPnk9L8+tWBn7//v04c"
                        b"eIEnDRv82l8Fz8Vhg8+eB9pPt7zAmAEXLhQidbGBuTl78IEDaIl7x"
                        b"9Ac3095ixKwq69BX7N+YDKYrEgJycH9fVKdjYQUHpGzZCgtPQ44uL"
                        b"i/LfjB739+Icfoa29DRtf20I9X4ozp76ARt2Cwg8Pst0tkNXW1o4t"
                        b"Wzajr6+fpiQ+mxlvbH+ddc+jQeh1Menu7MTBwiIES2PYK4vVqMebe"
                        b"3Zj1uzZgeiefKaiogL5u/NhJoswAXnqVBmWLEl+WAZ7Mfnxavbf6x"
                        b"L7AHMpuXblKg0UTqzIygy4snlIJ3cyEAqFgkb0LKSnpz/qPvZq9n+"
                        b"/nDJTWwFt5no+g/b/4nreR3o+ol34H3/lbZqbrVMgAAAAAElFTkSuQmCC"
                    )
                )
                return

            elif self.path in self.pairs:
                _, record1, record2 = self.pairs[self.path]

                self.send_response(200)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(
                    bytes(
                        f"""
                    <html>
                    <head>
                      <title>Nearly-Matching Records' HTTP Responses</title>
                        <style>
                          body {{
                            height: 100%;
                          }}
                          .records {{
                            display: flex;
                            height: 100vh;
                          }}
                          .record {{
                            flex: 1;
                          }}
                          iframe {{
                            width: 100%;
                            height: 100%;
                          }}
                      </style>
                    </head>
                    <body>
                      <a href="/"><- Back to index</a>
                      <h1>Target-URI <small>{record1.header.get_field("WARC-Target-URI", decode=True)}</h1>
                      <div class="records">
                        <div class="record">
                          <h2>{file1}</h2>
                """,
                        "utf-8",
                    )
                )
                self.wfile.write(get_warc_record_fields_as_html(record1))
                self.wfile.write(
                    bytes(
                        f"""
                          <iframe src="{self.path}1/" title="Record 1"></iframe>
                        </div>
                        <div class="record">
                          <h2>{file2}</h2>
                """,
                        "utf-8",
                    )
                )
                self.wfile.write(get_warc_record_fields_as_html(record2))
                self.wfile.write(
                    bytes(
                        f"""
                          <iframe src="{self.path}2/" title="Record 2"></iframe>
                        </div>
                      </div>
                    </body>
                    </html>
                """,
                        "utf-8",
                    )
                )

                return

            elif self.path[:-2] in self.pairs:
                pair = self.pairs[self.path[:-2]]
                record = pair[int(self.path[-2:-1])]
                header_lines = (
                    record.get_http_header_block()
                    .decode("utf-8", errors="replace")
                    .splitlines()
                )
                headers = []
                status = 200
                for line in header_lines:
                    split = line.split(":", 1)
                    if len(split) == 1:
                        if line.startswith("HTTP/1.1"):
                            match = re.search(r"HTTP/1.1\s*(\d*)", line)
                            if match:
                                status = int(match.group(1))
                    else:
                        headers.append((split[0], split[1].strip()))
                self.send_response(status)
                for header, value in headers:
                    self.send_header(header, value)
                self.end_headers()
                self.wfile.write(record.get_http_body_block())
                return

            self.send_error(404, "File not found")

    WARCResponseHandler.pairs = pairs
    return WARCResponseHandler


def open_and_invoke(
    ctx,
    invoke_method,
    invoke_args=None,
    invoke_kwargs=None,
    record_filters=None,
    member_handlers=None,
    record_handlers=None,
    parser_callbacks=None,
    cache_records_or_members=False,
    extra_parser_kwargs=None,
):
    if not invoke_args:
        invoke_args = []
    if not invoke_kwargs:
        invoke_kwargs = {}
    if not extra_parser_kwargs:
        extra_parser_kwargs = {}

    if ctx.obj["DECOMPRESSION"] == "python":
        open_archive = python_open_archive
    elif ctx.obj["DECOMPRESSION"] == "system":
        open_archive = system_open_archive

    try:
        with open_archive(ctx.obj["FILEPATH"], ctx.obj["GUNZIP"]) as (file, file_type):
            #
            # Validate and configure options
            #
            if invoke_method == "parse":
                cache_records_or_members_kwarg = {
                    FileType.WARC: "cache_records",
                    FileType.GZIPPED_WARC: "cache_members",
                }
                invoke_kwargs[cache_records_or_members_kwarg[file_type]] = (
                    cache_records_or_members
                )

            elif cache_records_or_members:
                raise ValueError(
                    "The option cache_records_or_members=True is only meaningful when invoking parser.parse()."
                )

            #
            # Initialize parser
            #
            if file_type == FileType.WARC:
                if member_handlers and ctx.obj["VERBOSE"]:
                    click.echo(
                        "DEBUG: parsing as WARC file, member_handlers will be ignored.\n",
                        err=True,
                    )
                parser = WARCParser(
                    file,
                    record_filters=record_filters,
                    record_handlers=record_handlers,
                    parser_callbacks=parser_callbacks,
                    **extra_parser_kwargs,
                )
            elif file_type == FileType.GZIPPED_WARC:
                parser = WARCGZParser(
                    file,
                    record_filters=record_filters,
                    member_handlers=member_handlers,
                    record_handlers=record_handlers,
                    parser_callbacks=parser_callbacks,
                    **extra_parser_kwargs,
                )

            return getattr(parser, invoke_method)(*invoke_args, **invoke_kwargs)
    except (ValueError, NotImplementedError, RuntimeError) as e:
        raise click.ClickException(e)


def open_and_parse(
    ctx,
    record_filters=None,
    member_handlers=None,
    record_handlers=None,
    parser_callbacks=None,
    cache_records_or_members=False,
    extra_parser_kwargs=None,
):
    """This function runs the parser, filtering and running record handlers and parser callbacks as necessary."""
    if not extra_parser_kwargs:
        extra_parser_kwargs = {}

    return open_and_invoke(
        ctx,
        "parse",
        record_filters=record_filters,
        member_handlers=member_handlers,
        record_handlers=record_handlers,
        parser_callbacks=parser_callbacks,
        cache_records_or_members=cache_records_or_members,
        extra_parser_kwargs=extra_parser_kwargs,
    )
