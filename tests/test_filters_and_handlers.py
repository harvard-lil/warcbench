import pytest

from warcbench import WARCParser, WARCGZParser
from warcbench.filters import (
    warc_header_regex_filter,
    record_content_length_filter,
    record_content_type_filter,
    warc_named_field_filter,
    http_verb_filter,
    http_status_filter,
    http_header_filter,
    http_response_content_type_filter,
)


@pytest.mark.parametrize("file", ["warc_file", "gzipped_warc_file"])
@pytest.mark.parametrize("verb,record_count", [("get", 2), ("post", 0)])
def test_http_verb_filter(request, file, verb, record_count):
    file_handle = request.getfixturevalue(file)
    filters = [http_verb_filter(verb)]

    match file:
        case "warc_file":
            parser = WARCParser(file_handle, filters=filters)
        case "gzipped_warc_file":
            parser = WARCGZParser(file_handle, record_filters=filters)

    parser.parse()
    assert len(parser.records) == record_count
    for record in parser.records:
        assert record.get_http_header_block().startswith(verb.upper().encode())


@pytest.mark.parametrize("file", ["warc_file", "gzipped_warc_file"])
@pytest.mark.parametrize("status,record_count", [(200, 5), (404, 1)])
def test_http_status_filter(request, file, status, record_count):
    file_handle = request.getfixturevalue(file)
    filters = [http_status_filter(status)]

    match file:
        case "warc_file":
            parser = WARCParser(file_handle, filters=filters)
        case "gzipped_warc_file":
            parser = WARCGZParser(file_handle, record_filters=filters)

    parser.parse()
    assert len(parser.records) == record_count
    for record in parser.records:
        assert f"HTTP/1.1 {status}".encode() in record.get_http_header_block()


@pytest.mark.parametrize("file", ["warc_file", "gzipped_warc_file"])
@pytest.mark.parametrize(
    "header,value,record_count",
    [
        ("referer", "example.com/", 1),
        ("proxy-connection", "keep-alive", 2),
    ],
)
def test_http_header_filter(request, file, header, value, record_count):
    file_handle = request.getfixturevalue(file)
    filters = [http_header_filter(header, value)]

    match file:
        case "warc_file":
            parser = WARCParser(file_handle, filters=filters)
        case "gzipped_warc_file":
            parser = WARCGZParser(file_handle, record_filters=filters)

    parser.parse()
    assert len(parser.records) == record_count


@pytest.mark.parametrize("file", ["warc_file", "gzipped_warc_file"])
@pytest.mark.parametrize(
    "content_type,record_count",
    [
        ("png", 1),
        ("html", 4),
    ],
)
def test_http_response_content_type_filter(request, file, content_type, record_count):
    file_handle = request.getfixturevalue(file)
    filters = [http_response_content_type_filter(content_type)]

    match file:
        case "warc_file":
            parser = WARCParser(file_handle, filters=filters)
        case "gzipped_warc_file":
            parser = WARCGZParser(file_handle, record_filters=filters)

    parser.parse()
    assert len(parser.records) == record_count
