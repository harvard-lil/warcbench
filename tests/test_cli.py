from click.testing import CliRunner
import json
from pathlib import Path
import pytest

from warcbench.scripts import cli


@pytest.mark.parametrize(
    "file_name", ["example.com.warc", "example.com.wacz", "test-crawl.wacz"]
)
def test_summarize(file_name, expected_summary):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--out", "json", "summarize", f"tests/assets/{file_name}"]
    )
    assert result.exit_code == 0, result.output
    summary_data = json.loads(result.stdout)
    assert summary_data["record_count"] == expected_summary[file_name]["record_count"]
    assert not summary_data["warnings"]
    assert not summary_data["error"]
    assert summary_data["record_types"] == expected_summary[file_name]["record_types"]
    assert summary_data["domains"] == expected_summary[file_name]["domains"]
    assert summary_data["content_types"] == expected_summary[file_name]["content_types"]


@pytest.mark.parametrize(
    "file_name", ["example.com.warc", "example.com.wacz", "test-crawl.wacz"]
)
def test_inspect(file_name, sample_inspect_json):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--out", "json", "inspect", f"tests/assets/{file_name}"]
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == sample_inspect_json[file_name]


def test_extract(tmp_path):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--verbose",
            "extract",
            "--basename",
            f"{tmp_path}/example",
            "tests/assets/example.com.wacz",
            "image/png",
            "png",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Found a response of type image/png at position 5176" in result.output
    assert Path(f"{tmp_path}/example-5176.png").exists()


def test_extract_gzip_decode(tmp_path):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--verbose",
            "extract",
            "--basename",
            f"{tmp_path}/example",
            "tests/assets/example.com.warc",
            "text/html",
            "html",
        ],
    )
    assert result.exit_code == 0, result.output
    position = 1241
    output_file = Path(f"{tmp_path}/example-{position}.html")
    assert f"Found a response of type text/html at position {position}" in result.output
    assert output_file.exists()
    with open(output_file) as f:
        assert (
            "This domain is for use in illustrative examples in documents." in f.read()
        )


def test_extract_gzip_no_decode(tmp_path):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--verbose",
            "extract",
            "--no-decode",
            "--basename",
            f"{tmp_path}/example",
            "tests/assets/example.com.warc",
            "text/html",
            "html",
        ],
    )
    assert result.exit_code == 0, result.output
    position = 1241
    output_file = Path(f"{tmp_path}/example-{position}.html")
    assert f"Found a response of type text/html at position {position}" in result.output
    assert output_file.exists()
    with open(output_file, "rb") as f:
        # check the magic number
        assert f.read(2) == b"\x1f\x8b"


def test_extract_brotli_decode(tmp_path):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--verbose",
            "extract",
            "--basename",
            f"{tmp_path}/test-crawl",
            "tests/assets/test-crawl.wacz",
            "text/javascript",
            "js",
        ],
    )
    assert result.exit_code == 0, result.output
    position = 334
    output_file = Path(f"{tmp_path}/test-crawl-{position}.js")
    assert (
        f"Found a response of type text/javascript at position {position}"
        in result.output
    )
    assert output_file.exists()
    with open(output_file) as f:
        assert "jQuery" in f.read()
    assert output_file.stat().st_size == 87533


def test_extract_brotli_no_decode(tmp_path):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--verbose",
            "extract",
            "--no-decode",
            "--basename",
            f"{tmp_path}/test-crawl",
            "tests/assets/test-crawl.wacz",
            "text/javascript",
            "js",
        ],
    )
    assert result.exit_code == 0, result.output
    position = 334
    output_file = Path(f"{tmp_path}/test-crawl-{position}.js")
    assert (
        f"Found a response of type text/javascript at position {position}"
        in result.output
    )
    assert output_file.exists()

    # there's no magic number for Brotli, as there is for gzip
    with pytest.raises(UnicodeDecodeError):
        with open(output_file) as f:
            assert "jQuery" not in f.read()  # no-op
    assert output_file.stat().st_size == 27918


def test_extract_zstd_decode(tmp_path):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--verbose",
            "extract",
            "--basename",
            f"{tmp_path}/fb-warc",
            "tests/assets/fb.warc.gz",
            "text/html",
            "html",
        ],
    )
    assert result.exit_code == 0, result.output
    position = 2698
    output_file = Path(f"{tmp_path}/fb-warc-{position}.html")
    assert f"Found a response of type text/html at position {position}" in result.output
    assert output_file.exists()
    with open(output_file) as f:
        assert "html" in f.read()
    assert output_file.stat().st_size == 60071


def test_extract_zstd_no_decode(tmp_path):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--verbose",
            "extract",
            "--no-decode",
            "--basename",
            f"{tmp_path}/fb-warc",
            "tests/assets/fb.warc.gz",
            "text/html",
            "html",
        ],
    )
    assert result.exit_code == 0, result.output
    position = 2698
    output_file = Path(f"{tmp_path}/fb-warc-{position}.html")
    assert f"Found a response of type text/html at position {position}" in result.output
    assert output_file.exists()
    with pytest.raises(UnicodeDecodeError):
        with open(output_file) as f:
            assert "html" not in f.read()  # no-op
    assert output_file.stat().st_size == 23379


def test_compare_parsers_gzipped_warc():
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--out",
            "json",
            "compare-parsers",
            "--output-offsets",
            "tests/assets/example.com.wacz",
        ],
    )
    assert result.exit_code == 0, result.output
    comparison_data = json.loads(result.stdout)

    assert comparison_data["member"]["all_match"] is True
    assert len(comparison_data["member"]["offsets"]) == 2

    assert comparison_data["record"]["all_match"] is True
    assert len(comparison_data["record"]["offsets"]) == 4

    assert comparison_data["warnings"]["any"] is False
    assert comparison_data["error"]["any"] is False


def test_compare_parsers_warc():
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--out",
            "json",
            "compare-parsers",
            "--output-offsets",
            "tests/assets/example.com.warc",
        ],
    )
    assert result.exit_code == 0, result.output
    comparison_data = json.loads(result.stdout)

    assert comparison_data["record"]["all_match"] is True
    assert len(comparison_data["record"]["offsets"]) == 2

    assert comparison_data["warnings"]["any"] is False
    assert comparison_data["error"]["any"] is False


@pytest.mark.parametrize(
    "file_name", ["example.com.warc", "example.com.wacz", "test-crawl.wacz"]
)
def test_match_record_pairs(file_name, sample_match_pairs_json):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--out",
            "json",
            "match-record-pairs",
            "--output-summary-by-uri",
            "--output-record-details",
            "--include-pairs",
            "--include-http-headers",
            f"tests/assets/{file_name}",
        ],
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == sample_match_pairs_json[file_name]
