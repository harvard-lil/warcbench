import pytest

from warcbench import WARCParser


@pytest.mark.parametrize("parsing_style", ["delimiter", "content_length"])
def test_warc_parser_parse(warc_file, parsing_style):
    parser = WARCParser(warc_file, parsing_style=parsing_style)
    parser.parse()

    assert len(parser.records) == 9
    assert parser.records[-1].start == 76091
    assert parser.records[-1].end == 82943


@pytest.mark.parametrize("parsing_style", ["delimiter", "content_length"])
def test_warc_parser_iterator(warc_file, parsing_style):
    parser = WARCParser(warc_file, parsing_style=parsing_style)
    record_count = 0
    for record in parser.iterator():
        record_count += 1

    assert record_count == 9
    assert record.start == 76091
    assert record.end == 82943
