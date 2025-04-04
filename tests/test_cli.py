from click.testing import CliRunner
import json
from pathlib import Path

from warcbench.scripts import cli


def test_summarize():
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--out", "json", "summarize", "tests/assets/example.com.wacz"]
    )
    assert result.exit_code == 0
    summary_data = json.loads(result.stdout)
    assert summary_data["record_count"] == 9
    assert not summary_data["warnings"]
    assert not summary_data["error"]
    assert summary_data["record_types"] == {"request": 2, "response": 6, "warcinfo": 1}
    assert summary_data["domains"] == ["example.com"]
    assert summary_data["content_types"] == {
        "application/pdf": 1,
        "image/png": 1,
        "text/html": 3,
        "text/html; charset=UTF-8": 1,
    }


def test_inspect(sample_inspect_json):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--out", "json", "inspect", "tests/assets/example.com.wacz"]
    )
    assert result.exit_code == 0
    assert json.loads(result.stdout) == sample_inspect_json


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
