import pytest

from warcbench import WARCGZParser
from warcbench.config import WARCGZParsingConfig, WARCGZCachingConfig
from warcbench.exceptions import AttributeNotInitializedError


def test_warc_gz_parser_unsupported_style(gzipped_warc_file):
    """Test that WARCGZParser raises ValueError for unsupported styles."""
    with pytest.raises(ValueError) as e:
        WARCGZParser(
            gzipped_warc_file,
            parsing_options=WARCGZParsingConfig(style="unsupported_style"),
        )

    assert "Supported parsing styles: split_gzip_members" in str(e.value)


def test_warc_gz_parser_unsupported_decompression_style(gzipped_warc_file):
    """Test that WARCGZParser raises ValueError for unsupported decompression styles."""
    with pytest.raises(ValueError) as e:
        WARCGZParser(
            gzipped_warc_file,
            enable_lazy_loading_of_bytes=False,
            parsing_options=WARCGZParsingConfig(
                style="split_gzip_members",
                decompression_style="unsupported_decompression_style",
            ),
        )

    assert "Supported decompression styles: member, file" in str(e.value)


@pytest.mark.parametrize(
    "decompression_style,should_raise,expected_message",
    [
        ("file", True, "Decompressing records can only be disabled when decompression style is set to 'member'."),
        ("member", False, None),
    ],
)
def test_warc_gz_parser_decompression_style_validation(gzipped_warc_file, decompression_style, should_raise, expected_message):
    """Test that WARCGZParser validates decompression_style when decompress_and_parse_members=False."""
    if should_raise:
        with pytest.raises(ValueError) as e:
            WARCGZParser(
                gzipped_warc_file,
                enable_lazy_loading_of_bytes=False,
                parsing_options=WARCGZParsingConfig(
                    style="split_gzip_members",
                    decompress_and_parse_members=False,
                    decompression_style=decompression_style,
                    split_records=False,  # Required when decompress_and_parse_members=False
                ),
            )
        assert str(e.value) == expected_message
    else:
        parser = WARCGZParser(
            gzipped_warc_file,
            enable_lazy_loading_of_bytes=False,
            parsing_options=WARCGZParsingConfig(
                style="split_gzip_members",
                decompress_and_parse_members=False,
                decompression_style=decompression_style,
                split_records=False,  # Required when decompress_and_parse_members=False
            ),
        )
        # If we get here without an exception, the test passes
        assert parser is not None


@pytest.mark.parametrize(
    "header_bytes,parsed_headers,content_block_bytes,should_raise",
    [
        (True, False, False, True),
        (False, True, False, True),
        (False, False, True, True),
        (True, True, False, True),
        (True, False, True, True),
        (False, True, True, True),
        (True, True, True, True),
        (False, False, False, False),
    ],
)
def test_warc_gz_parser_cache_options_without_split_records(
    gzipped_warc_file, header_bytes, parsed_headers, content_block_bytes, should_raise
):
    """Test that WARCGZParser validates cache options when split_records=False."""
    if should_raise:
        with pytest.raises(ValueError) as e:
            WARCGZParser(
                gzipped_warc_file,
                enable_lazy_loading_of_bytes=False,
                parsing_options=WARCGZParsingConfig(
                    style="split_gzip_members",
                    split_records=False,
                ),
                cache=WARCGZCachingConfig(
                    header_bytes=header_bytes,
                    parsed_headers=parsed_headers,
                    content_block_bytes=content_block_bytes,
                ),
            )
        expected_message = "To cache or parse header or content block bytes, you must split records."
        assert str(e.value) == expected_message
    else:
        # This should not raise an error
        parser = WARCGZParser(
            gzipped_warc_file,
            enable_lazy_loading_of_bytes=False,
            parsing_options=WARCGZParsingConfig(
                style="split_gzip_members",
                split_records=False,
            ),
            cache=WARCGZCachingConfig(
                header_bytes=header_bytes,
                parsed_headers=parsed_headers,
                content_block_bytes=content_block_bytes,
            ),
        )
        # If we get here without an exception, the test passes
        assert parser is not None


@pytest.mark.parametrize("decompression_style", ["file", "member"])
def test_warc_gz_parser_iterator_current_member(gzipped_warc_file, decompression_style):
    """Test that WARCGZParser's iterator and current_member work correctly."""
    parser = WARCGZParser(
        gzipped_warc_file,
        parsing_options=WARCGZParsingConfig(decompression_style=decompression_style),
        enable_lazy_loading_of_bytes=False,
    )

    iterator = parser.iterator()

    # Get the third member
    for _ in range(2):
        next(iterator)
    third_member = next(iterator)

    # Verify that parser.current_member matches the third member
    assert parser.current_member is third_member


@pytest.mark.parametrize("decompression_style", ["file", "member"])
def test_warc_gz_parser_offsets(
    gzipped_warc_file,
    expected_offsets,
    decompression_style,
):
    parser = WARCGZParser(
        gzipped_warc_file,
        parsing_options=WARCGZParsingConfig(decompression_style=decompression_style),
        enable_lazy_loading_of_bytes=False,
    )
    parser.parse()

    assert len(parser.members) == len(expected_offsets["warc_gz_members"])
    for member, (member_start, member_end), (record_start, record_end) in zip(
        parser.members,
        expected_offsets["warc_gz_members"],
        expected_offsets["warc_records"],
    ):
        assert member.start == member_start
        assert member.end == member_end
        assert member.uncompressed_warc_record.start == record_start
        assert member.uncompressed_warc_record.end == record_end


@pytest.mark.parametrize("decompression_style", ["file", "member"])
def test_warc_gz_parser_stop_after_nth(gzipped_warc_file, decompression_style):
    parser = WARCGZParser(
        gzipped_warc_file,
        parsing_options=WARCGZParsingConfig(
            decompression_style=decompression_style, stop_after_nth=2
        ),
        enable_lazy_loading_of_bytes=False,
    )
    parser.parse()
    assert len(parser.members) == 2


@pytest.mark.parametrize("decompression_style", ["file", "member"])
def test_warc_gz_parser_records_not_split(
    gzipped_warc_file,
    decompression_style,
):
    parser = WARCGZParser(
        gzipped_warc_file,
        parsing_options=WARCGZParsingConfig(
            decompression_style=decompression_style, split_records=False
        ),
        enable_lazy_loading_of_bytes=False,
    )
    parser.parse()

    for record in parser.records:
        assert record.header is None
        assert record.content_block is None


@pytest.mark.parametrize("decompression_style", ["file", "member"])
def test_warc_gz_parser_records_split_correctly(
    gzipped_warc_file,
    expected_offsets,
    decompression_style,
):
    parser = WARCGZParser(
        gzipped_warc_file,
        parsing_options=WARCGZParsingConfig(decompression_style=decompression_style),
        enable_lazy_loading_of_bytes=False,
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


@pytest.mark.parametrize("decompression_style", ["file", "member"])
def test_warc_gz_parser_caches_compressed_and_uncompressed_bytes(
    gzipped_warc_file, decompression_style, check_records_start_and_end_bytes
):
    parser = WARCGZParser(
        gzipped_warc_file,
        parsing_options=WARCGZParsingConfig(decompression_style=decompression_style),
        enable_lazy_loading_of_bytes=False,
        cache=WARCGZCachingConfig(
            member_bytes=True,
            member_uncompressed_bytes=True,
            record_bytes=True,
            header_bytes=True,
            content_block_bytes=True,
        ),
    )
    parser.parse()

    for member in parser.members:
        assert member._bytes
        assert member._uncompressed_bytes

    check_records_start_and_end_bytes(parser.records, expect_cached_bytes=True)


def test_warc_gz_parser_lazy_loads_bytes_in_file_mode(
    gzipped_warc_file, check_records_start_and_end_bytes
):
    parser = WARCGZParser(
        gzipped_warc_file,
        parsing_options=WARCGZParsingConfig(decompression_style="file"),
        enable_lazy_loading_of_bytes=True,
        cache=WARCGZCachingConfig(
            member_uncompressed_bytes=False,
        ),
    )
    parser.parse()

    for member in parser.members:
        assert not member._bytes
        assert not member._uncompressed_bytes

    check_records_start_and_end_bytes(parser.records, expect_cached_bytes=False)


def test_warc_gz_parser_does_not_load_bytes_in_member_mode(
    gzipped_warc_file, check_records_start_and_end_bytes
):
    with pytest.raises(ValueError) as e:
        WARCGZParser(
            gzipped_warc_file,
            parsing_options=WARCGZParsingConfig(decompression_style="member"),
            enable_lazy_loading_of_bytes=True,
            cache=WARCGZCachingConfig(
                member_uncompressed_bytes=False,
            ),
        )

    assert (
        "The lazy loading of bytes is only supported when decompression style is 'file'."
        in str(e)
    )


@pytest.mark.parametrize("decompression_style", ["file", "member"])
def test_warc_gz_parser_get_member_offsets(
    gzipped_warc_file,
    decompression_style,
    expected_offsets,
):
    parser = WARCGZParser(
        gzipped_warc_file,
        parsing_options=WARCGZParsingConfig(decompression_style=decompression_style),
        enable_lazy_loading_of_bytes=False,
    )
    assert parser.get_member_offsets() == expected_offsets["warc_gz_members"]


@pytest.mark.parametrize("decompression_style", ["file", "member"])
def test_warc_gz_parser_get_member_uncompressed_offsets(
    gzipped_warc_file,
    decompression_style,
    expected_offsets,
):
    parser = WARCGZParser(
        gzipped_warc_file,
        parsing_options=WARCGZParsingConfig(decompression_style=decompression_style),
        enable_lazy_loading_of_bytes=False,
    )
    assert (
        parser.get_member_offsets(compressed=False)
        == expected_offsets["warc_gz_members_uncompressed"]
    )


@pytest.mark.parametrize("decompression_style", ["file", "member"])
def test_warc_gz_parser_get_record_offsets(
    gzipped_warc_file,
    decompression_style,
    expected_offsets,
):
    parser = WARCGZParser(
        gzipped_warc_file,
        parsing_options=WARCGZParsingConfig(decompression_style=decompression_style),
        enable_lazy_loading_of_bytes=False,
    )
    assert parser.get_record_offsets() == expected_offsets["warc_records"]


@pytest.mark.parametrize("decompression_style", ["file", "member"])
def test_warc_gz_parser_get_split_record_offsets(
    gzipped_warc_file,
    decompression_style,
    expected_offsets,
):
    offsets = [
        (h1, h2, c1, c2)
        for (h1, h2), (c1, c2) in zip(
            expected_offsets["record_headers"],
            expected_offsets["record_content_blocks"],
        )
    ]

    parser = WARCGZParser(
        gzipped_warc_file,
        parsing_options=WARCGZParsingConfig(decompression_style=decompression_style),
        enable_lazy_loading_of_bytes=False,
    )
    assert parser.get_record_offsets(split=True) == offsets


@pytest.mark.parametrize("decompression_style", ["file", "member"])
def test_warc_gz_parser_members_access_without_cache_members(
    gzipped_warc_file, decompression_style
):
    """Test that accessing members without cache_members=True raises the correct error."""
    parser = WARCGZParser(
        gzipped_warc_file,
        parsing_options=WARCGZParsingConfig(decompression_style=decompression_style),
        enable_lazy_loading_of_bytes=False,
    )

    with pytest.raises(AttributeNotInitializedError) as exc_info:
        _ = parser.members

    expected_message = (
        "Call parser.parse(cache_members=True) to load members into RAM and populate parser.members, "
        "or use parser.iterator() to iterate through members without preloading."
    )
    assert str(exc_info.value) == expected_message


@pytest.mark.parametrize("decompression_style", ["file", "member"])
def test_warc_gz_parser_records_access_without_cache_members(
    gzipped_warc_file, decompression_style
):
    """Test that accessing records without cache_members=True raises the correct error."""
    parser = WARCGZParser(
        gzipped_warc_file,
        parsing_options=WARCGZParsingConfig(decompression_style=decompression_style),
        enable_lazy_loading_of_bytes=False,
    )

    with pytest.raises(AttributeNotInitializedError) as exc_info:
        _ = parser.records

    expected_message = (
        "Call parser.parse(cache_members=True) to load records into RAM and populate parser.records, "
        "or use parser.iterator(yield_type='records') to iterate through successfully "
        "parsed records without preloading."
    )
    assert str(exc_info.value) == expected_message


@pytest.mark.parametrize("decompression_style", ["file", "member"])
def test_warc_gz_parser_get_split_record_offsets_without_split_records(
    gzipped_warc_file, decompression_style
):
    """Test that getting split record offsets without split_records=True raises the correct error."""
    parser = WARCGZParser(
        gzipped_warc_file,
        parsing_options=WARCGZParsingConfig(
            decompression_style=decompression_style, split_records=False
        ),
        enable_lazy_loading_of_bytes=False,
    )
    parser.parse()

    with pytest.raises(ValueError) as exc_info:
        parser.get_record_offsets(split=True)

    expected_message = "Split record offsets are only available when the parser is initialized with split_records=True."
    assert str(exc_info.value) == expected_message


def test_warc_gz_parser_get_record_offsets_without_decompress_and_parse_members(
    gzipped_warc_file,
):
    """Test that getting record offsets without decompress_and_parse_members=True raises the correct error."""
    parser = WARCGZParser(
        gzipped_warc_file,
        parsing_options=WARCGZParsingConfig(
            decompression_style="member",
            decompress_and_parse_members=False,
            split_records=False,
        ),
        enable_lazy_loading_of_bytes=False,
    )
    parser.parse()

    with pytest.raises(ValueError) as exc_info:
        parser.get_record_offsets()

    expected_message = "Record offsets are only available when the parser is initialized with decompress_and_parse_members=True."
    assert str(exc_info.value) == expected_message


@pytest.mark.parametrize(
    "split_records,record_bytes,header_bytes,parsed_headers,content_block_bytes,non_warc_member_bytes,member_uncompressed_bytes,should_raise",
    [
        # split_records=True - should raise
        (True, False, False, False, False, False, False, True),
        # record_bytes=True - should raise
        (False, True, False, False, False, False, False, True),
        # header_bytes=True - should raise
        (False, False, True, False, False, False, False, True),
        # parsed_headers=True - should raise
        (False, False, False, True, False, False, False, True),
        # content_block_bytes=True - should raise
        (False, False, False, False, True, False, False, True),
        # non_warc_member_bytes=True - should raise
        (False, False, False, False, False, True, False, True),
        # member_uncompressed_bytes=True - should raise
        (False, False, False, False, False, False, True, True),
        # Multiple cache options true - should raise
        (True, True, False, False, False, False, False, True),
        (False, True, True, False, False, False, False, True),
        # All cache options false - should work
        (False, False, False, False, False, False, False, False),
    ],
)
def test_warc_gz_parser_cache_options_without_decompress_and_parse_members(
    gzipped_warc_file,
    split_records,
    record_bytes,
    header_bytes,
    parsed_headers,
    content_block_bytes,
    non_warc_member_bytes,
    member_uncompressed_bytes,
    should_raise,
):
    """Test that using cache options without decompress_and_parse_members=True raises the correct error."""
    if should_raise:
        with pytest.raises(ValueError) as exc_info:
            WARCGZParser(
                gzipped_warc_file,
                parsing_options=WARCGZParsingConfig(
                    decompression_style="member",
                    decompress_and_parse_members=False,
                    split_records=split_records,
                ),
                enable_lazy_loading_of_bytes=False,
                cache=WARCGZCachingConfig(
                    record_bytes=record_bytes,
                    header_bytes=header_bytes,
                    parsed_headers=parsed_headers,
                    content_block_bytes=content_block_bytes,
                    non_warc_member_bytes=non_warc_member_bytes,
                    member_uncompressed_bytes=member_uncompressed_bytes,
                ),
            )

        expected_message = "Decompressing records must be enabled, for this parsing and caching configuration."
        assert str(exc_info.value) == expected_message
    else:
        # Should not raise an error
        parser = WARCGZParser(
            gzipped_warc_file,
            parsing_options=WARCGZParsingConfig(
                decompression_style="member",
                decompress_and_parse_members=False,
                split_records=split_records,
            ),
            enable_lazy_loading_of_bytes=False,
            cache=WARCGZCachingConfig(
                record_bytes=record_bytes,
                header_bytes=header_bytes,
                parsed_headers=parsed_headers,
                content_block_bytes=content_block_bytes,
                non_warc_member_bytes=non_warc_member_bytes,
                member_uncompressed_bytes=member_uncompressed_bytes,
            ),
        )
        # If we get here without an exception, the test passes
        assert parser is not None
