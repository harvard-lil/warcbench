import pytest

from warcbench import WARCParser


@pytest.mark.parametrize("parsing_style", ["delimiter", "content_length"])
def test_warc_parser_offsets(warc_file, expected_warc_record_offsets, parsing_style):
    parser = WARCParser(warc_file, parsing_style=parsing_style)
    parser.parse()

    assert len(parser.records) == len(expected_warc_record_offsets)
    for record, (start, end) in zip(parser.records, expected_warc_record_offsets):
        assert record.start == start
        assert record.end == end

