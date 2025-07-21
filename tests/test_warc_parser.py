import pytest

from warcbench import WARCParser
from warcbench.config import WARCParsingConfig, WARCCachingConfig


@pytest.mark.parametrize("parsing_style", ["delimiter", "content_length"])
def test_warc_parser_offsets(warc_file, expected_offsets, parsing_style):
    parser = WARCParser(
        warc_file, parsing_options=WARCParsingConfig(style=parsing_style)
    )
    parser.parse()

    assert len(parser.records) == len(expected_offsets["warc_records"])
    for record, (start, end) in zip(parser.records, expected_offsets["warc_records"]):
        assert record.start == start
        assert record.end == end


@pytest.mark.parametrize("parsing_style", ["delimiter", "content_length"])
def test_warc_parser_stop_after_nth(warc_file, parsing_style):
    parser = WARCParser(
        warc_file,
        parsing_options=WARCParsingConfig(style=parsing_style, stop_after_nth=2),
    )
    parser.parse()
    assert len(parser.records) == 2


def test_warc_parser_check_content_lengths_not_supported_in_content_length_mode(
    warc_file,
):
    with pytest.raises(ValueError) as e:
        WARCParser(
            warc_file,
            parsing_options=WARCParsingConfig(check_content_lengths=True),
        )

    assert (
        "Checking content lengths is only meaningful when parsing in delimiter mode."
        in str(e)
    )


def test_warc_parser_check_content_lengths_false(warc_file):
    # None, by default, when not checking
    parser = WARCParser(
        warc_file,
        parsing_options=WARCParsingConfig(
            style="delimiter", check_content_lengths=False
        ),
    )
    parser.parse()
    for record in parser.records:
        assert record.content_length_check_result is None


def test_warc_parser_check_content_lengths_true(warc_file):
    # True, for this valid WARC, when checking
    parser = WARCParser(
        warc_file,
        parsing_options=WARCParsingConfig(
            style="delimiter", check_content_lengths=True
        ),
    )
    parser.parse()
    for record in parser.records:
        assert record.content_length_check_result is True


@pytest.mark.parametrize("parsing_style", ["delimiter", "content_length"])
def test_warc_parser_records_not_split(warc_file, parsing_style):
    parser = WARCParser(
        warc_file,
        parsing_options=WARCParsingConfig(style=parsing_style, split_records=False),
    )
    parser.parse()

    for record in parser.records:
        assert record.header is None
        assert record.content_block is None


@pytest.mark.parametrize("parsing_style", ["delimiter", "content_length"])
def test_warc_parser_records_split_correctly(
    warc_file, expected_offsets, parsing_style
):
    parser = WARCParser(
        warc_file, parsing_options=WARCParsingConfig(style=parsing_style)
    )
    parser.parse()

    for record, (header_start, header_end), (
        content_block_start,
        content_block_end,
    ) in zip(
        parser.records,
        expected_offsets["record_headers"],
        expected_offsets["record_content_blocks"],
    ):
        assert record.header.start == header_start
        assert record.header.end == header_end
        assert record.content_block.start == content_block_start
        assert record.content_block.end == content_block_end


@pytest.mark.parametrize("parsing_style", ["delimiter", "content_length"])
def test_warc_parser_records_caches_bytes(
    warc_file,
    parsing_style,
    expected_record_last_bytes,
    check_records_start_and_end_bytes,
):
    parser = WARCParser(
        warc_file,
        parsing_options=WARCParsingConfig(style=parsing_style),
        cache=WARCCachingConfig(
            record_bytes=True,
            header_bytes=True,
            content_block_bytes=True,
            unparsable_line_bytes=True,
        ),
    )
    parser.parse()

    check_records_start_and_end_bytes(parser.records, expect_cached_bytes=True)


@pytest.mark.parametrize("parsing_style", ["delimiter", "content_length"])
def test_warc_parser_records_lazy_loads_bytes(
    warc_file,
    parsing_style,
    expected_record_last_bytes,
    check_records_start_and_end_bytes,
):
    parser = WARCParser(
        warc_file,
        parsing_options=WARCParsingConfig(style=parsing_style),
    )
    parser.parse()

    check_records_start_and_end_bytes(parser.records, expect_cached_bytes=False)


@pytest.mark.parametrize("parsing_style", ["delimiter", "content_length"])
def test_warc_parser_get_record_offsets(
    warc_file,
    parsing_style,
    expected_offsets,
):
    parser = WARCParser(
        warc_file,
        parsing_options=WARCParsingConfig(style=parsing_style),
    )
    assert parser.get_record_offsets() == expected_offsets["warc_records"]


@pytest.mark.parametrize("parsing_style", ["delimiter", "content_length"])
def test_warc_parser_get_split_record_offsets(
    warc_file,
    parsing_style,
    expected_offsets,
):
    offsets = [
        (h1, h2, c1, c2)
        for (h1, h2), (c1, c2) in zip(
            expected_offsets["record_headers"],
            expected_offsets["record_content_blocks"],
        )
    ]

    parser = WARCParser(
        warc_file,
        parsing_options=WARCParsingConfig(style=parsing_style),
    )
    assert parser.get_record_offsets(split=True) == offsets
