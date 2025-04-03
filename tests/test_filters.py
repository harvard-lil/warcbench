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

###
### Helpers
###


def parse_and_check_record_count(request, file, filters, record_count):
    file_handle = request.getfixturevalue(file)

    match file:
        case "warc_file":
            parser = WARCParser(file_handle, record_filters=filters)
        case "gzipped_warc_file":
            parser = WARCGZParser(file_handle, record_filters=filters)

    parser.parse()
    assert len(parser.records) == record_count

    return parser.records


#
# Tests
#


@pytest.mark.parametrize("file", ["warc_file", "gzipped_warc_file"])
@pytest.mark.parametrize(
    "regex,record_count",
    [
        ("Scoop-Exchange-Description: Provenance Summary", 1),
        ("WARC/1.[01]", 9),
        (r"WARC-Refers-To-Target-URI:\shttp://example.com/", 4),
    ],
)
def test_warc_header_regex_filter(request, file, regex, record_count):
    filters = [warc_header_regex_filter(regex)]
    parse_and_check_record_count(request, file, filters, record_count)


@pytest.mark.parametrize("file", ["warc_file", "gzipped_warc_file"])
@pytest.mark.parametrize(
    "length,operator,record_count",
    [
        (38979, "eq", 1),
        (38979, "gt", 0),
        (38979, "lt", 8),
    ],
)
def test_record_content_length_filter(request, file, length, operator, record_count):
    filters = [record_content_length_filter(length, operator)]
    parse_and_check_record_count(request, file, filters, record_count)


@pytest.mark.parametrize("file", ["warc_file", "gzipped_warc_file"])
@pytest.mark.parametrize(
    "content_type,record_count",
    [
        ("warc-fields", 1),
        ("http", 8),
        ("application/http; msgtype=request", 2),
        ("application/http; msgtype=response", 6),
    ],
)
def test_record_content_type_filter(request, file, content_type, record_count):
    filters = [record_content_type_filter(content_type)]
    parse_and_check_record_count(request, file, filters, record_count)


@pytest.mark.parametrize("file", ["warc_file", "gzipped_warc_file"])
@pytest.mark.parametrize(
    "field,target,record_count",
    [
        ("type", "warcinfo", 1),
        ("type", "request", 2),
        ("record-id", "<urn:uuid:9831f6b7-247d-45d2-a6a8-21708a194b23>", 1),
    ],
)
def test_warc_named_field_filter(request, file, field, target, record_count):
    filters = [warc_named_field_filter(field, target)]
    parse_and_check_record_count(request, file, filters, record_count)


@pytest.mark.parametrize("file", ["warc_file", "gzipped_warc_file"])
@pytest.mark.parametrize("verb,record_count", [("get", 2), ("post", 0)])
def test_http_verb_filter(request, file, verb, record_count):
    filters = [http_verb_filter(verb)]
    records = parse_and_check_record_count(request, file, filters, record_count)
    for record in records:
        assert record.get_http_header_block().startswith(verb.upper().encode())


@pytest.mark.parametrize("file", ["warc_file", "gzipped_warc_file"])
@pytest.mark.parametrize("status,record_count", [(200, 5), (404, 1)])
def test_http_status_filter(request, file, status, record_count):
    filters = [http_status_filter(status)]
    records = parse_and_check_record_count(request, file, filters, record_count)
    for record in records:
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
    filters = [http_header_filter(header, value)]
    parse_and_check_record_count(request, file, filters, record_count)


@pytest.mark.parametrize("file", ["warc_file", "gzipped_warc_file"])
@pytest.mark.parametrize(
    "content_type,record_count",
    [
        ("png", 1),
        ("html", 4),
    ],
)
def test_http_response_content_type_filter(request, file, content_type, record_count):
    filters = [http_response_content_type_filter(content_type)]
    parse_and_check_record_count(request, file, filters, record_count)
