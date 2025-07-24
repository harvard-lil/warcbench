from collections import defaultdict
from io import BufferedReader
import json
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
def warc_file(assets_path: Path):
    filepath = assets_path / "example.com.warc"
    with filepath.open("rb") as warc:
        yield warc


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
        "warc_gz_members_uncompressed": [
            (0, 284),
            (284, 1241),
            (1241, 2740),
            (2740, 3648),
            (3648, 5176),
            (5176, 34539),
            (34539, 36488),
            (36488, 76091),
            (76091, 82947),
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


@pytest.fixture
def sample_inspect_json(assets_path: Path):
    inspect_json = {}
    for file_name in ["example.com.warc", "example.com.wacz", "test-crawl.wacz"]:
        filepath = assets_path / f"{file_name}.inspect.json"
        with filepath.open("r") as json_file:
            inspect_json[file_name] = json.loads(json_file.read())
    return inspect_json


@pytest.fixture
def sample_inspect_txt(assets_path: Path):
    inspect_txt = {}
    for file_name in ["example.com.warc", "example.com.wacz", "test-crawl.wacz"]:
        filepath = assets_path / f"{file_name}.inspect.txt"
        with filepath.open("r") as txt_file:
            inspect_txt[file_name] = txt_file.read()
    return inspect_txt


@pytest.fixture
def sample_match_pairs_json(assets_path: Path):
    pairs_json = {}
    for file_name in ["example.com.warc", "example.com.wacz", "test-crawl.wacz"]:
        filepath = assets_path / f"{file_name}.pairs.json"
        with filepath.open("r") as json_file:
            pairs_json[file_name] = json.loads(json_file.read())
    return pairs_json


@pytest.fixture
def sample_filter_json(assets_path: Path):
    filter_json = defaultdict(dict)
    for file_name in ["example.com.wacz"]:
        filepath = assets_path / f"{file_name}.filter.json"
        verbose_filepath = assets_path / f"{file_name}.filter-verbose.json"
        with filepath.open("r") as json_file:
            filter_json[file_name]["basic"] = json.loads(json_file.read())
        with verbose_filepath.open("r") as json_file:
            filter_json[file_name]["verbose"] = json.loads(json_file.read())
    return dict(filter_json)


@pytest.fixture
def sample_summarize_txt(assets_path: Path):
    summarize_txt = {}
    for file_name in ["example.com.warc", "example.com.wacz", "test-crawl.wacz"]:
        filepath = assets_path / f"{file_name}.summarize.txt"
        with filepath.open("r") as txt_file:
            summarize_txt[file_name] = txt_file.read()
    return summarize_txt


@pytest.fixture
def sample_filter_detailed_txt(assets_path: Path):
    filepath = assets_path / "example.com.wacz.filter-detailed.txt"
    with filepath.open("r") as txt_file:
        return txt_file.read()


@pytest.fixture
def sample_pairs_detailed_txt(assets_path: Path):
    filepath = assets_path / "example.com.wacz.pairs-detailed.txt"
    with filepath.open("r") as txt_file:
        return txt_file.read()


@pytest.fixture
def sample_compare_headers_detailed_txt(assets_path: Path):
    filepath = assets_path / "compare-headers-detailed.txt"
    with filepath.open("r") as txt_file:
        return txt_file.read()


@pytest.fixture
def sample_summarize_json():
    return {
        "example.com.warc": {
            "record_count": 9,
            "record_types": {"request": 2, "response": 6, "warcinfo": 1},
            "domains": ["example.com"],
            "content_types": {
                "application/pdf": 1,
                "image/png": 1,
                "text/html": 3,
                "text/html; charset=UTF-8": 1,
            },
        },
        "example.com.wacz": {
            "record_count": 9,
            "record_types": {"request": 2, "response": 6, "warcinfo": 1},
            "domains": ["example.com"],
            "content_types": {
                "application/pdf": 1,
                "image/png": 1,
                "text/html": 3,
                "text/html; charset=UTF-8": 1,
            },
        },
        "test-crawl.wacz": {
            "record_count": 23,
            "record_types": {"request": 11, "response": 11, "warcinfo": 1},
            "domains": ["www.iana.org", "dict.brave.com", "example.com"],
            "content_types": {
                "text/html": 2,
                "text/html; charset=UTF-8": 1,
                "text/javascript": 2,
                "font/ttf": 1,
                "application/octet-stream": 1,
                "image/svg+xml": 2,
                "image/vnd.microsoft.icon": 1,
                "text/css": 1,
            },
        },
    }


@pytest.fixture
def expected_custom_filter_results():
    return {
        "count": 3,
        "records": [
            {
                "record_headers": [
                    "WARC/1.1",
                    "WARC-Filename: archive.warc",
                    "WARC-Date: 2024-11-04T19:10:55.900Z",
                    "WARC-Type: warcinfo",
                    "WARC-Record-ID: <urn:uuid:a6fd8346-f170-497b-9e26-47a5bde6d86c>",
                    "Content-Type: application/warc-fields",
                    "Content-Length: 57",
                ]
            },
            {
                "record_headers": [
                    "WARC/1.1",
                    "Scoop-Exchange-ID: 5733be1f-60ea-47c8-99be-abc4f8b31846",
                    "WARC-Target-URI: http://example.com/",
                    "WARC-Date: 2024-11-04T19:10:51.248Z",
                    "WARC-Type: request",
                    "WARC-Record-ID: <urn:uuid:ab3ef7b3-0c7e-4a12-9097-96352b6c9e3a>",
                    "Content-Type: application/http; msgtype=request",
                    "WARC-Payload-Digest: sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                    "WARC-Block-Digest: sha256:ee4938cc9bc0d0cd340a97536ec32cebc937815e52bb40b5e3bc37753c5c87d2",
                    "Content-Length: 468",
                ]
            },
            {
                "record_headers": [
                    "WARC/1.1",
                    "Scoop-Exchange-ID: 5733be1f-60ea-47c8-99be-abc4f8b31846",
                    "WARC-Target-URI: http://example.com/",
                    "WARC-Date: 2024-11-04T19:10:51.248Z",
                    "WARC-Type: response",
                    "WARC-Record-ID: <urn:uuid:c4a6a946-252c-48cc-8e7b-85d3aa8ee81b>",
                    "Content-Type: application/http; msgtype=response",
                    "WARC-Payload-Digest: sha256:2682a32f5b99c7d0c9395ccba0464a38856b36472926eaf53fd4f11d5d3364a0",
                    "WARC-Block-Digest: sha256:d6e9fe3f079a51a37a4ecaf6ba5483a2bcc7865e5395dea42c136b9f8e74f3fb",
                    "Content-Length: 1007",
                ]
            },
        ],
    }


@pytest.fixture
def complete_compare_headers_json(assets_path: Path):
    filepath = assets_path / "compare-headers.json"
    with filepath.open("r") as json_file:
        return json.loads(json_file.read())
