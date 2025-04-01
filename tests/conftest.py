import gzip
from io import BufferedReader
from pathlib import Path
import zipfile

import pytest


@pytest.fixture
def assets_path():
    return Path(__file__).parent / "assets"


@pytest.fixture
def wacz_file(assets_path: Path):
    filepath = assets_path / "example.com.wacz"
    with filepath.open("rb") as wacz:
        yield wacz


@pytest.fixture
def gzipped_warc_file(wacz_file: BufferedReader):
    with zipfile.Path(wacz_file, "archive/data.warc.gz").open("rb") as warc_gz_file:
        yield warc_gz_file


@pytest.fixture
def warc_file(wacz_file: BufferedReader):
    with zipfile.Path(wacz_file, "archive/data.warc.gz").open("rb") as warc_gz_file:
        with gzip.open(warc_gz_file, "rb") as warc_file:
            yield warc_file


@pytest.fixture
def expected_offsets():
    return {
        "warc_gz_members": [
            (0, 237),
            (237, 876),
            (876, 2216),
            (2216, 2829),
            (2829, 4183),
            (4183, 27222),
            (27222, 28294),
            (28294, 49670),
            (49670, 51764),
        ],
        "warc_records": [
            (0, 280),
            (284, 1237),
            (1241, 2736),
            (2740, 3644),
            (3648, 5172),
            (5176, 34535),
            (34539, 36484),
            (36488, 76087),
            (76091, 82943),
        ],
        "record_headers": [
            (0, 221),
            (284, 767),
            (1241, 1727),
            (2740, 3234),
            (3648, 4145),
            (5176, 5790),
            (34539, 35157),
            (36488, 37106),
            (76091, 76685),
        ],
        "record_content_blocks": [
            (223, 280),
            (769, 1237),
            (1729, 2736),
            (3236, 3644),
            (4147, 5172),
            (5792, 34535),
            (35159, 36484),
            (37108, 76087),
            (76687, 82943),
        ],
    }


@pytest.fixture
def expected_record_last_bytes():
    return [
        b"\r\n",
        b"\r\n",
        b"\x00\x00",
        b"\r\n",
        b"\x00\x00",
        b"`\x82",
        b"l>",
        b"F\n",
        b"\n\n",
        b"\r\n",
        b"\r\n",
        b"\x00\x00",
        b"\r\n",
        b"\x00\x00",
        b"`\x82",
        b"l>",
        b"F\n",
        b"\n\n",
    ]


@pytest.fixture
def check_records_start_and_end_bytes(expected_record_last_bytes):
    def f(records, expect_cached_bytes):
        header_prefix = b"WARC/1.1\r\n"
        for record, last_bytes in zip(records, expected_record_last_bytes):
            assert bool(record._bytes) == expect_cached_bytes
            assert record.bytes[:10] == header_prefix
            assert record.bytes[-2:] == last_bytes

            assert bool(record.header._bytes) == expect_cached_bytes
            assert record.header.bytes[:10] == header_prefix
            assert record.header.bytes[-2:] == b"\r\n"

            assert bool(record.content_block._bytes) == expect_cached_bytes
            assert record.content_block.bytes[:2] != b"\r\n"
            assert record.content_block.bytes[-2:] == last_bytes

    return f
