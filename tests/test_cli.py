from click.testing import CliRunner
import filecmp
import warnings
import json
import os
from pathlib import Path
import pytest
import requests
from tempfile import NamedTemporaryFile
import threading
import time

from warcbench.scripts import cli
from warcbench.utils import decompress_and_get_gzip_file_member_offsets


@pytest.mark.parametrize(
    "file_name", ["example.com.warc", "example.com.wacz", "test-crawl.wacz"]
)
def test_summarize_json(file_name, sample_summarize_json):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--out", "json", "summarize", f"tests/assets/{file_name}"]
    )
    assert result.exit_code == 0, result.output
    summary_data = json.loads(result.stdout)
    assert summary_data["record_count"] == sample_summarize_json[file_name]["record_count"]
    assert not summary_data["warnings"]
    assert not summary_data["error"]
    assert summary_data["record_types"] == sample_summarize_json[file_name]["record_types"]
    assert summary_data["domains"] == sample_summarize_json[file_name]["domains"]
    assert summary_data["content_types"] == sample_summarize_json[file_name]["content_types"]


@pytest.mark.parametrize(
    "file_name", ["example.com.warc", "example.com.wacz", "test-crawl.wacz"]
)
def test_summarize_text(file_name, sample_summarize_txt):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["summarize", f"tests/assets/{file_name}"]
    )
    assert result.exit_code == 0, result.output
    assert result.stdout == sample_summarize_txt[file_name]


@pytest.mark.parametrize(
    "file_name", ["example.com.warc", "example.com.wacz", "test-crawl.wacz"]
)
def test_inspect_json(file_name, sample_inspect_json):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--out", "json", "inspect", f"tests/assets/{file_name}"]
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == sample_inspect_json[file_name]


@pytest.mark.parametrize(
    "file_name", ["example.com.warc", "example.com.wacz", "test-crawl.wacz"]
)
def test_inspect_text(file_name, sample_inspect_txt):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["inspect", f"tests/assets/{file_name}"]
    )
    assert result.exit_code == 0, result.output
    assert result.stdout == sample_inspect_txt[file_name]


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


def test_match_record_pairs_http_headers_without_record_details_error():
    """Test that output-http-headers requires output-record-details."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--out",
            "json",
            "match-record-pairs",
            "--output-http-headers",
            "tests/assets/example.com.wacz",
        ],
    )
    assert result.exit_code != 0
    assert "Please pass --output-record-metadata together with --include-http-headers." in result.output


def test_match_record_pairs_pair_details_without_record_details_error():
    """Test that include-pair-details requires output-record-details."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--out",
            "json",
            "match-record-pairs",
            "--include-pair-details",
            "tests/assets/example.com.wacz",
        ],
    )
    assert result.exit_code != 0
    assert "Please pass --output-record-metadata together with --include-pairs." in result.output


@pytest.mark.parametrize(
    "file_name", ["example.com.warc", "example.com.wacz", "test-crawl.wacz"]
)
def test_match_record_pairs_json(file_name, sample_match_pairs_json):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--out",
            "json",
            "match-record-pairs",
            "--output-summary-by-uri",
            "--output-record-details",
            "--output-http-headers",
            "--include-pair-details",
            f"tests/assets/{file_name}",
        ],
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == sample_match_pairs_json[file_name]


def test_match_record_pairs_detailed_output(sample_pairs_detailed_txt):
    """Test match-record-pairs with all detailed output options enabled."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "match-record-pairs",
            "--output-record-details",
            "--output-http-headers",
            "--include-pair-details",
            "tests/assets/example.com.wacz",
        ],
    )
    assert result.exit_code == 0, result.output
    assert result.stdout == sample_pairs_detailed_txt


def test_filter_records_incompatible_extract_options_error(tmp_path):
    """Test that specifying both extract options raises an error."""
    warc_output = tmp_path / "output.warc"
    gzipped_warc_output = tmp_path / "output.warc.gz"
    
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "filter-records",
            "--extract-to-warc",
            str(warc_output),
            "--extract-to-gzipped-warc",
            str(gzipped_warc_output),
            "tests/assets/example.com.wacz",
        ],
    )
    assert result.exit_code != 0
    assert "Incompatible options: only one of --extract-to-warc or --extract-to-gzipped-warc may be set." in result.output


def test_filter_records_same_destination_error(tmp_path):
    """Test that extract-to-warc and extract-summary-to cannot output to the same destination."""
    same_output = tmp_path / "output.warc"
    
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "filter-records",
            "--extract-to-warc",
            str(same_output),
            "--extract-summary-to",
            str(same_output),
            "tests/assets/example.com.wacz",
        ],
    )
    assert result.exit_code != 0
    assert "Incompatible options: --extract-to-warc and --extract-summary-to cannot output to the same destination." in result.output


def test_filter_records_gzipped_same_destination_error(tmp_path):
    """Test that extract-to-gzipped-warc and extract-summary-to cannot output to the same destination."""
    same_output = tmp_path / "output.warc.gz"
    
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "filter-records",
            "--extract-to-gzipped-warc",
            str(same_output),
            "--extract-summary-to",
            str(same_output),
            "tests/assets/example.com.wacz",
        ],
    )
    assert result.exit_code != 0
    assert "Incompatible options: --extract-to-gzipped-warc and --extract-summary-to cannot output to the same destination." in result.output


def test_filter_records_extract_warc():
    """Extracting all records should result in an identical WARC."""
    filter_into = NamedTemporaryFile("w+b", delete=False)

    try:
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "filter-records",
                "--extract-to-warc",
                filter_into.name,
                "tests/assets/example.com.wacz",
            ],
        )
        assert result.exit_code == 0, result.output
        assert filecmp.cmp(
            filter_into.name, "tests/assets/example.com.warc", shallow=False
        )
    except:
        raise
    finally:
        os.remove(filter_into.name)


def test_filter_records_extract_force_include_warcinfo():
    filter_into = NamedTemporaryFile("w+b", delete=False)

    try:
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--out",
                "json",
                "filter-records",
                "--filter-by-warc-named-field",
                "Target-URI",
                "http://example.com",
                "--output-count",
                "--output-warc-headers",
                "--extract-to-warc",
                filter_into.name,
                "--extract-summary-to",
                "-",
                "--force-include-warcinfo",
                "tests/assets/example.com.wacz",
            ],
        )
        assert result.exit_code == 0, result.output
        results = json.loads(result.stdout)
        assert results["count"] == 5
        assert "warcinfo" in r"\n".join(results["records"][0]["record_headers"])
    except:
        raise
    finally:
        os.remove(filter_into.name)


def test_filter_records_extract_warc_gz():
    """Extracting all records should result in an identical WARC."""
    filter_into = NamedTemporaryFile("w+b", delete=False)
    gunzip_into = NamedTemporaryFile("w+b", delete=False)

    try:
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "filter-records",
                "--extract-to-gzipped-warc",
                filter_into.name,
                "tests/assets/example.com.wacz",
            ],
        )
        assert result.exit_code == 0, result.output

        with (
            open(filter_into.name, "rb") as output_file,
            open(gunzip_into.name, "wb") as gunzipped_file,
        ):
            decompress_and_get_gzip_file_member_offsets(output_file, gunzipped_file)

        assert filecmp.cmp(
            gunzip_into.name, "tests/assets/example.com.warc", shallow=False
        )
    except:
        raise
    finally:
        os.remove(filter_into.name)
        os.remove(gunzip_into.name)


def test_filter_records_basic_output(sample_filter_json):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--out",
            "json",
            "filter-records",
            "tests/assets/example.com.wacz",
        ],
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == sample_filter_json["example.com.wacz"]["basic"]


def test_filter_records_detailed_output(sample_filter_detailed_txt):
    """Test filter-records with all detailed output options enabled."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "filter-records",
            "--output-member-offsets",
            "--output-record-offsets", 
            "--output-warc-headers",
            "--output-http-headers",
            "--output-http-body",
            "tests/assets/example.com.wacz",
        ],
    )
    assert result.exit_code == 0, result.output
    assert result.stdout == sample_filter_detailed_txt


def test_filter_records_custom_filters(expected_custom_filter_results):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--out",
            "json",
            "filter-records",
            "--custom-filter-path",
            "tests/assets/custom-filters.py",
            "--output-warc-headers",
            "tests/assets/example.com.wacz",
        ],
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == expected_custom_filter_results


def test_filter_records_custom_handlers(expected_custom_filter_results):
    path = "/tmp/custom-handler-report.txt"

    assert not os.path.exists(path)

    try:
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "filter-records",
                "--custom-record-handler-path",
                "tests/assets/custom-handlers.py",
                "tests/assets/example.com.wacz",
            ],
        )
        assert result.exit_code == 0, result.output
        assert os.path.exists(path)
        assert filecmp.cmp(
            path, "tests/assets/custom-handler-report.txt", shallow=False
        )
    except:
        raise
    finally:
        if os.path.exists(path):
            os.remove(path)


@pytest.mark.parametrize(
    "flag,args,record_count",
    [
        ("--filter-by-http-header", ["referer", "example.com/"], 1),
        ("--filter-by-http-header", ["proxy-connection", "keep-alive"], 2),
        ("--filter-by-http-response-content-type", ["png"], 1),
        ("--filter-by-http-response-content-type", ["html"], 4),
        ("--filter-by-http-status-code", [200], 5),
        ("--filter-by-http-status-code", [404], 1),
        ("--filter-by-http-verb", ["get"], 2),
        ("--filter-by-http-verb", ["post"], 0),
        ("--filter-by-record-content-length", [38979, "eq"], 1),
        ("--filter-by-record-content-length", [38979, "gt"], 0),
        ("--filter-by-record-content-length", [38979, "lt"], 8),
        ("--filter-by-record-content-type", ["warc-fields"], 1),
        ("--filter-by-record-content-type", ["http"], 8),
        ("--filter-by-record-content-type", ["application/http; msgtype=request"], 2),
        ("--filter-by-record-content-type", ["application/http; msgtype=response"], 6),
        (
            "--filter-warc-header-with-regex",
            ["Scoop-Exchange-Description: Provenance Summary"],
            1,
        ),
        ("--filter-warc-header-with-regex", ["WARC/1.[01]"], 9),
        (
            "--filter-warc-header-with-regex",
            [r"WARC-Refers-To-Target-URI:\shttp://example.com/"],
            4,
        ),
        ("--filter-by-warc-named-field", ["type", "warcinfo"], 1),
        ("--filter-by-warc-named-field", ["type", "request"], 2),
        (
            "--filter-by-warc-named-field",
            ["record-id", "<urn:uuid:9831f6b7-247d-45d2-a6a8-21708a194b23>"],
            1,
        ),
    ],
)
def test_filter_records_filters(flag, args, record_count):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--out",
            "json",
            "filter-records",
            flag,
            *args,
            "tests/assets/example.com.wacz",
        ],
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["count"] == record_count


def test_compare_headers_default():
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--out",
            "json",
            "compare-headers",
            "tests/assets/before.wacz",
            "tests/assets/after.wacz",
        ],
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == {
        "summary": {
            "matching": 5,
            "near_matching": 3,
            "unique": {"tests/assets/before.wacz": 0, "tests/assets/after.wacz": 0},
        }
    }


def test_compare_headers_extra_field():
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--out",
            "json",
            "compare-headers",
            "--include-extra-header-field",
            "WARC-Record-ID",
            "tests/assets/before.wacz",
            "tests/assets/after.wacz",
        ],
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == {
        "summary": {
            "matching": 0,
            "near_matching": 0,
            "unique": {"tests/assets/before.wacz": 8, "tests/assets/after.wacz": 8},
        }
    }


def test_compare_headers_exclude_field():
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--out",
            "json",
            "compare-headers",
            "--exclude-header-field",
            "Content-Length",
            "--exclude-header-field",
            "WARC-Payload-Digest",
            "tests/assets/before.wacz",
            "tests/assets/after.wacz",
        ],
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == {
        "summary": {
            "matching": 8,
            "near_matching": 0,
            "unique": {"tests/assets/before.wacz": 0, "tests/assets/after.wacz": 0},
        }
    }


@pytest.mark.parametrize("field", ["WARC-Type", "WARC-Target-URI"])
def test_compare_headers_exclude_required_field_error(field):
    """Test that excluding required fields raises an error."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--out",
            "json",
            "compare-headers",
            "--exclude-header-field",
            field,
            "tests/assets/before.wacz",
            "tests/assets/after.wacz",
        ],
    )
    assert result.exit_code != 0
    assert "WARC-Type and WARC-Target-URI cannot be excluded from comparisons." in result.output


def test_compare_headers_custom_near_match_field():
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--out",
            "json",
            "compare-headers",
            "--near-match-field",
            "WARC-Target-URI",
            "tests/assets/before.wacz",
            "tests/assets/after.wacz",
        ],
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == {
        "summary": {
            "matching": 5,
            "near_matching": 0,
            "unique": {"tests/assets/before.wacz": 3, "tests/assets/after.wacz": 3},
        }
    }


def test_compare_headers_full_output_json(complete_compare_headers_json):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--out",
            "json",
            "compare-headers",
            "--output-matching-record-details",
            "--output-near-matching-record-details",
            "--output-near-matching-record-header-diffs",
            "--output-near-matching-record-http-header-diffs",
            "--output-unique-record-details",
            "tests/assets/before.wacz",
            "tests/assets/after.wacz",
        ],
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == complete_compare_headers_json


def test_compare_headers_full_output_text(sample_compare_headers_detailed_txt):
    """Test compare-headers with all detailed output options enabled."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "compare-headers",
            "--output-matching-record-details",
            "--output-near-matching-record-details",
            "--output-near-matching-record-header-diffs",
            "--output-near-matching-record-http-header-diffs",
            "--output-unique-record-details",
            "tests/assets/before.wacz",
            "tests/assets/after.wacz",
        ],
    )
    assert result.exit_code == 0, result.output
    assert result.stdout == sample_compare_headers_detailed_txt


def test_compare_headers_serve():
    runner = CliRunner()

    # Run the command in a thread, so that the web server doesn't block.
    # Pass a stop event via the click context object so we can
    # signal the server to shut down when the test is done.

    host = "127.0.0.1"
    port = "9999"
    base_url = f"http://{host}:{port}/"
    stop_event = threading.Event()

    def run_server():
        runner.invoke(
            cli,
            [
                "--out",
                "json",
                "compare-headers",
                "--serve-near-matching-records",
                "--server-host",
                host,
                "--server-port",
                port,
                "tests/assets/before.wacz",
                "tests/assets/after.wacz",
            ],
            obj={"stop_event": stop_event},
        )

    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()

    # Give the server time to start
    time.sleep(1)

    try:
        #
        # Test the main page (index)
        #

        response = requests.get(base_url, timeout=5)
        assert response.status_code == 200
        assert response.headers["Content-Type"] == "text/html"

        html_content = response.text
        assert "Nearly-Matching Records' HTTP Responses" in html_content
        assert 'href="/1/"' in html_content
        assert 'href="/2/"' in html_content
        assert 'href="/3/"' in html_content

        #
        # Test the comparison pages
        #

        for pair_num in [1, 2, 3]:
            pair_url = f"{base_url}{pair_num}/"
            pair_response = requests.get(pair_url, timeout=5)

            assert pair_response.status_code == 200, (
                f"Got HTTP {pair_response.status_code} for pair {pair_num}."
            )
            assert pair_response.headers["Content-Type"] == "text/html"

            pair_html = pair_response.text
            assert "Target-URI" in pair_html
            assert "before.wacz" in pair_html
            assert "after.wacz" in pair_html
            assert 'href="/"' in pair_html  # Back to index link
            assert "<iframe" in pair_html  # Should have iframes for comparison
            assert f'src="/{pair_num}/1/"' in pair_html  # First record iframe
            assert f'src="/{pair_num}/2/"' in pair_html  # Second record iframe

            # Test the iframes
            for record_num in [1, 2]:
                record_url = f"{base_url}{pair_num}/{record_num}/"
                record_response = requests.get(record_url, timeout=5)
                assert record_response.status_code == 200, (
                    f"Got HTTP {record_response.status_code} for record {pair_num}/{record_num}."
                )

        # Test 404
        not_found_response = requests.get(f"{base_url}nonexistent/", timeout=5)
        assert not_found_response.status_code == 404

        # Test favicon
        favicon_response = requests.get(f"{base_url}favicon.ico", timeout=5)
        assert favicon_response.status_code == 200
        assert favicon_response.headers["Content-Type"] == "image/png"
        assert len(favicon_response.content) > 0

    finally:
        stop_event.set()
        server_thread.join(timeout=3)
        if server_thread.is_alive():
            warnings.warn(
                "The server thread in test_compare_headers_serve did not stop."
            )
