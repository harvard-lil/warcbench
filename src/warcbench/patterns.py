"""
`patterns` module: Common sequences of bytes expected in WARC files
"""

CRLF = b"\r\n"
WARC_VERSION = b"WARC/1.1\r\n"
CONTENT_LENGTH_PATTERN = rb"Content-Length:\s*(\d+)"
CONTENT_TYPE_PATTERN = rb"Content-Type:\s*(.*)((\r\n)|$)"


def get_warc_named_field_pattern(field_name):
    return b"WARC-" + bytes(field_name, "utf-8") + rb":\s*(.*)((\r\n)|$)"


def get_http_verb_pattern(verb):
    return bytes(f"({verb})", "utf-8") + rb"\s+.*HTTP/.*((\r\n)|$)"


def get_http_status_pattern(status_code):
    return rb"HTTP/1.1\s*" + bytes(f"({status_code})", "utf-8")


def get_http_header_pattern(header_name):
    return bytes(header_name, "utf-8") + rb":\s*(.+)((\r\n)|$)"
