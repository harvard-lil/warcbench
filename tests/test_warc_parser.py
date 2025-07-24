import pytest
from unittest.mock import patch

from warcbench import WARCParser
from warcbench.config import WARCParsingConfig, WARCCachingConfig
from warcbench.exceptions import AttributeNotInitializedError


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


def test_warc_parser_unsupported_style(warc_file):
    """Test that WARCParser raises ValueError for unsupported styles."""
    with pytest.raises(ValueError) as e:
        WARCParser(
            warc_file,
            parsing_options=WARCParsingConfig(style="unsupported_style"),
        )

    assert "Supported parsing styles: delimiter, content_length" in str(e.value)


@pytest.mark.parametrize("parsing_style", ["delimiter", "content_length"])
def test_warc_parser_iterator_current_record(warc_file, parsing_style):
    """Test that WARCParser's iterator and current_record work correctly."""
    parser = WARCParser(
        warc_file,
        parsing_options=WARCParsingConfig(style=parsing_style),
    )

    iterator = parser.iterator()

    # Get the third record
    for _ in range(2):
        next(iterator)
    third_record = next(iterator)

    # Verify that parser.current_record matches the third record
    assert parser.current_record is third_record


def test_content_length_warc_parser_unparsable_lines(warc_file):
    """
    Test ContentLengthWARCParser when find_content_length_in_bytes always returns None,
    so every line is considered unparsable.
    """
    # Patch find_content_length_in_bytes to always return None
    with patch(
        "warcbench.parsers.warc.find_content_length_in_bytes", return_value=None
    ):
        parser = WARCParser(
            warc_file,
            parsing_options=WARCParsingConfig(style="content_length"),
            cache=WARCCachingConfig(unparsable_lines=True, unparsable_line_bytes=True),
        )
        parser.parse()

        # Check that no records were parsed (since content length is always None)
        assert len(parser.records) == 0

        # Check that unparsable lines were captured
        assert len(parser.unparsable_lines) == 967

        # Check that the list of unparsable lines matches reality
        warc_file.seek(0)
        for unparsable_line in parser.unparsable_lines:
            assert warc_file.tell() == unparsable_line.start
            file_line = warc_file.readline()
            assert warc_file.tell() == unparsable_line.end
            assert unparsable_line.bytes == file_line

        # Check that, after having gone through the whole list of unparsable lines,
        # we're at the end of the file: there's no content that wasn't captured.
        assert not warc_file.read(), (
            "File handle should be at EOF after processing all unparsable lines"
        )


@pytest.mark.parametrize("parsing_style", ["delimiter", "content_length"])
def test_warc_parser_records_access_without_cache_members(warc_file, parsing_style):
    """Test that accessing records without cache_members=True raises the correct error."""
    parser = WARCParser(
        warc_file,
        parsing_options=WARCParsingConfig(style=parsing_style),
    )

    with pytest.raises(AttributeNotInitializedError) as exc_info:
        _ = parser.records

    expected_message = (
        "Call parser.parse(cache_members=True) to load records into RAM and populate parser.records, "
        "or use parser.iterator() to iterate through records without preloading."
    )
    assert str(exc_info.value) == expected_message


@pytest.mark.parametrize("parsing_style", ["delimiter", "content_length"])
def test_warc_parser_unparsable_lines_access_without_cache(warc_file, parsing_style):
    """Test that accessing unparsable_lines without cache_unparsable_lines=True raises the correct error."""
    parser = WARCParser(
        warc_file,
        parsing_options=WARCParsingConfig(style=parsing_style),
    )

    with pytest.raises(AttributeNotInitializedError) as exc_info:
        _ = parser.unparsable_lines

    expected_message = (
        "Pass cache_unparsable_lines=True to WARCParser() to store UnparsableLines "
        "in parser.unparsable_lines."
    )
    assert str(exc_info.value) == expected_message


@pytest.mark.parametrize("parsing_style", ["delimiter", "content_length"])
def test_warc_parser_get_split_record_offsets_without_split_records(
    warc_file, parsing_style
):
    """Test that getting split record offsets without split_records=True raises the correct error."""
    parser = WARCParser(
        warc_file,
        parsing_options=WARCParsingConfig(style=parsing_style, split_records=False),
    )
    parser.parse()

    with pytest.raises(ValueError) as exc_info:
        parser.get_record_offsets(split=True)

    expected_message = "Split record offsets are only available when the parser is initialized with split_records=True."
    assert str(exc_info.value) == expected_message


def test_warc_parser_check_content_lengths_without_split_records(warc_file):
    """Test that using check_content_lengths=True without split_records=True raises the correct error."""
    with pytest.raises(ValueError) as exc_info:
        WARCParser(
            warc_file,
            parsing_options=WARCParsingConfig(
                style="delimiter", check_content_lengths=True, split_records=False
            ),
        )

    expected_message = "To check_content_lengths, you must split records."
    assert str(exc_info.value) == expected_message


@pytest.mark.parametrize(
    "enable_lazy_loading,header_bytes,content_block_bytes,should_raise",
    [
        # Both cache options false, no lazy loading - should raise
        (False, False, False, True),
        # Only header_bytes true - should raise
        (False, True, False, True),
        # Only content_block_bytes true - should raise
        (False, False, True, True),
        # Both cache options false, but lazy loading enabled - should work
        (True, False, False, False),
        # Both cache options true, no lazy loading - should work
        (False, True, True, False),
        # Both cache options true, lazy loading enabled - should work
        (True, True, True, False),
    ],
)
def test_warc_parser_check_content_lengths_cache_configs(
    warc_file, enable_lazy_loading, header_bytes, content_block_bytes, should_raise
):
    """Test different cache configurations with check_content_lengths=True."""
    if should_raise:
        with pytest.raises(ValueError) as exc_info:
            WARCParser(
                warc_file,
                enable_lazy_loading_of_bytes=enable_lazy_loading,
                parsing_options=WARCParsingConfig(
                    style="delimiter", check_content_lengths=True, split_records=True
                ),
                cache=WARCCachingConfig(
                    header_bytes=header_bytes, content_block_bytes=content_block_bytes
                ),
            )

        expected_message = (
            "To check_content_lengths, you must either enable_lazy_loading_of_bytes or "
            "both cache_header_bytes and cache_content_block_bytes."
        )
        assert str(exc_info.value) == expected_message
    else:
        # Should not raise an error
        parser = WARCParser(
            warc_file,
            enable_lazy_loading_of_bytes=enable_lazy_loading,
            parsing_options=WARCParsingConfig(
                style="delimiter", check_content_lengths=True, split_records=True
            ),
            cache=WARCCachingConfig(
                header_bytes=header_bytes, content_block_bytes=content_block_bytes
            ),
        )
        # If we get here without an exception, the test passes
        assert parser is not None


@pytest.mark.parametrize(
    "header_bytes,parsed_headers,content_block_bytes,should_raise",
    [
        # Only header_bytes true - should raise
        (True, False, False, True),
        # Only parsed_headers true - should raise
        (False, True, False, True),
        # Only content_block_bytes true - should raise
        (False, False, True, True),
        # Multiple cache options true - should raise
        (True, True, False, True),
        (True, False, True, True),
        (False, True, True, True),
        (True, True, True, True),
        # All cache options false - should work
        (False, False, False, False),
    ],
)
def test_warc_parser_cache_header_content_without_split_records(
    warc_file, header_bytes, parsed_headers, content_block_bytes, should_raise
):
    """Test that caching header or content block bytes without split_records=True raises the correct error."""
    if should_raise:
        with pytest.raises(ValueError) as exc_info:
            WARCParser(
                warc_file,
                parsing_options=WARCParsingConfig(
                    style="delimiter", split_records=False
                ),
                cache=WARCCachingConfig(
                    header_bytes=header_bytes,
                    parsed_headers=parsed_headers,
                    content_block_bytes=content_block_bytes,
                ),
            )

        expected_message = (
            "To cache or parse header or content block bytes, you must split records."
        )
        assert str(exc_info.value) == expected_message
    else:
        # Should not raise an error
        parser = WARCParser(
            warc_file,
            parsing_options=WARCParsingConfig(style="delimiter", split_records=False),
            cache=WARCCachingConfig(
                header_bytes=header_bytes,
                parsed_headers=parsed_headers,
                content_block_bytes=content_block_bytes,
            ),
        )
        # If we get here without an exception, the test passes
        assert parser is not None
