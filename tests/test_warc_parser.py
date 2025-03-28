import pytest

from warcbench import WARCParser


@pytest.mark.parametrize("parsing_style", ["delimiter", "content_length"])
def test_warc_parser_offsets(warc_file, expected_offsets, parsing_style):
    parser = WARCParser(warc_file, parsing_style=parsing_style)
    parser.parse()

    assert len(parser.records) == len(expected_offsets["warc_records"])
    for record, (start, end) in zip(parser.records, expected_offsets["warc_records"]):
        assert record.start == start
        assert record.end == end


@pytest.mark.parametrize("parsing_style", ["delimiter", "content_length"])
def test_warc_parser_stop_after_nth(warc_file, parsing_style):
    parser = WARCParser(warc_file, parsing_style=parsing_style, stop_after_nth=2)
    parser.parse()
    assert len(parser.records) == 2


def test_warc_parser_check_content_lengths_not_supported_in_content_length_mode(
    warc_file,
):
    with pytest.raises(ValueError):
        parser = WARCParser(
            warc_file, check_content_lengths=True
        )


def test_warc_parser_check_content_lengths_false(warc_file):
    # None, by default, when not checking
    parser = WARCParser(
        warc_file, parsing_style="delimiter", check_content_lengths=False
    )
    parser.parse()
    for record in parser.records:
        assert record.content_length_check_result is None


def test_warc_parser_check_content_lengths_true(warc_file):
    # True, for this valid WARC, when checking
    parser = WARCParser(
        warc_file, parsing_style="delimiter", check_content_lengths=True
    )
    parser.parse()
    for record in parser.records:
        assert record.content_length_check_result is True
