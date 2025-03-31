import pytest

from warcbench import WARCGZParser


@pytest.mark.parametrize("decompression_style", ["file", "member"])
def test_warc_gz_parser_offsets(
    gzipped_warc_file,
    expected_offsets,
    decompression_style,
):
    parser = WARCGZParser(
        gzipped_warc_file,
        decompression_style=decompression_style,
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
        decompression_style=decompression_style,
        enable_lazy_loading_of_bytes=False,
        stop_after_nth=2,
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
        decompression_style=decompression_style,
        enable_lazy_loading_of_bytes=False,
        split_records=False,
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
        decompression_style=decompression_style,
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
        decompression_style=decompression_style,
        enable_lazy_loading_of_bytes=False,
        cache_member_bytes=True,
        cache_member_uncompressed_bytes=True,
        cache_record_bytes=True,
        cache_header_bytes=True,
        cache_content_block_bytes=True,
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
        decompression_style="file",
        enable_lazy_loading_of_bytes=True,
        cache_member_uncompressed_bytes=False,
    )
    parser.parse()

    for member in parser.members:
        assert not member._bytes
        assert not member._uncompressed_bytes

    check_records_start_and_end_bytes(parser.records, expect_cached_bytes=False)


def test_warc_gz_parser_does_not_loads_bytes_in_member_mode(
    gzipped_warc_file, check_records_start_and_end_bytes
):
    with pytest.raises(ValueError) as e:
        WARCGZParser(
            gzipped_warc_file,
            decompression_style="member",
            enable_lazy_loading_of_bytes=True,
            cache_member_uncompressed_bytes=False,
        )

    assert (
        "The lazy loading of bytes is only supported when decompression style is 'file'."
        in str(e)
    )
