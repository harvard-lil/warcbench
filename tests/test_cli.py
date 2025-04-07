from click.testing import CliRunner
import json
from pathlib import Path
import pytest

from warcbench.scripts import cli


@pytest.mark.parametrize("wacz_file", ["example.com.wacz", "test-crawl.wacz"])
def test_summarize(wacz_file, expected_summary):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--out", "json", "summarize", f"tests/assets/{wacz_file}"]
    )
    assert result.exit_code == 0
    summary_data = json.loads(result.stdout)
    assert summary_data["record_count"] == expected_summary[wacz_file]["record_count"]
    assert not summary_data["warnings"]
    assert not summary_data["error"]
    assert summary_data["record_types"] == expected_summary[wacz_file]["record_types"]
    assert summary_data["domains"] == expected_summary[wacz_file]["domains"]
    assert summary_data["content_types"] == expected_summary[wacz_file]["content_types"]


@pytest.mark.parametrize("wacz_file", ["example.com.wacz", "test-crawl.wacz"])
def test_inspect(wacz_file, sample_inspect_json):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--out", "json", "inspect", f"tests/assets/{wacz_file}"]
    )
    assert result.exit_code == 0
    assert json.loads(result.stdout) == sample_inspect_json[wacz_file]


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
        ],
    )
    assert result.exit_code == 0
    assert "Found a response of type image/png at position 5176" in result.output
    assert Path(f"{tmp_path}/example-5176.png").exists()
