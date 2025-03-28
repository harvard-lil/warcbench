import pytest

from warcbench import WARCGZParser


@pytest.mark.parametrize("decompression_style", ["file", "member"])
def test_warcgz_parser_offsets(
    gzipped_warc_file,
    expected_warcgz_member_offsets,
    expected_warc_record_offsets,
    decompression_style,
):
    parser = WARCGZParser(
        gzipped_warc_file,
        decompression_style=decompression_style,
        enable_lazy_loading_of_bytes=False,
    )
    parser.parse()

    assert len(parser.members) == len(expected_warcgz_member_offsets)
    for member, (member_start, member_end), (record_start, record_end) in zip(
        parser.members, expected_warcgz_member_offsets, expected_warc_record_offsets
    ):
        assert member.start == member_start
        assert member.end == member_end
        assert member.uncompressed_warc_record.start == record_start
        assert member.uncompressed_warc_record.end == record_end
