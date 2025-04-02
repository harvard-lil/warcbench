from click.testing import CliRunner
import json
from pathlib import Path

from warcbench.scripts import cli


def test_parse():
    runner = CliRunner()
    result = runner.invoke(cli, ["parse", "tests/assets/example.com.wacz"])
    assert result.exit_code == 0
    assert "Found 9 records" in result.output


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
