import pytest

from warcbench import WARCGZParser
from warcbench.config import WARCGZParsingConfig, WARCGZCachingConfig


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
                decompression_style="unsupported_decompression_style"
            ),
        )
    
    assert "Supported decompression styles: member, file" in str(e.value)


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
