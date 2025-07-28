"""
`patterns` module: Common sequences of bytes expected in WARC files
"""
from typing import Union

CRLF = b"\r\n"
WARC_VERSIONS = [b"WARC/1.0\r\n", b"WARC/1.1\r\n"]

# http://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#content-length-mandatory
CONTENT_LENGTH_PATTERN = rb"Content-Length:\s*(\d+)"

# http://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#content-type
CONTENT_TYPE_PATTERN = rb"Content-Type:\s*(.*)((\r\n)|$)"


def get_warc_named_field_pattern(field_name: str) -> bytes:
    """
    Get a regular expression for finding and extracting the value of any particular "warc named field"
    present in a bytestring of unparsed WARC headers.
    http://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#named-fields
    """
    return b"WARC-" + bytes(field_name, "utf-8") + rb":\s*(.*)((\r\n)|$)"


def get_http_verb_pattern(verb: str) -> bytes:
    """
    Get a regular expression for finding and extracting an HTTP verb from the request line of
    a bytestring of unparsed HTTP headers.
    https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods
    """
    return bytes(f"({verb})", "utf-8") + rb"\s+.*HTTP/.*((\r\n)|$)"


def get_http_status_pattern(status_code: Union[str, int]) -> bytes:
    """
    Get a regular expression for finding and extracting the value of any HTTP status code
    present in the status line of a bytestring of unparsed HTTP headers.
    """
    return rb"HTTP/1.1\s*" + bytes(f"({status_code})", "utf-8")


def get_http_header_pattern(header_name: str) -> bytes:
    """
    Get a regular expression for finding and extracting the value of any HTTP
    present in a bytestring of unparsed HTTP headers.
    """
    return bytes(header_name, "utf-8") + rb":\s*(.+)((\r\n)|$)"
