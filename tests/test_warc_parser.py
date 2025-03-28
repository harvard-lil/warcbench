import pytest

from warcbench import WARCParser


@pytest.mark.parametrize("parsing_style", ["delimiter", "content_length"])
def test_warc_parser_parse(warc_file, expected_warc_offsets, parsing_style):
    parser = WARCParser(warc_file, parsing_style=parsing_style)
    parser.parse()

    assert len(parser.records) == 9
    for record, (start, end) in zip(parser.records, expected_warc_offsets):
        assert record.start == start
        assert record.end == end


@pytest.mark.parametrize("parsing_style", ["delimiter", "content_length"])
def test_warc_parser_iterator(warc_file, expected_warc_offsets, parsing_style):
    parser = WARCParser(warc_file, parsing_style=parsing_style)
    record_count = 0
    for record, (start, end) in zip(parser.iterator(), expected_warc_offsets):
        record_count += 1
        assert record.start == start
        assert record.end == end

    assert record_count == 9
