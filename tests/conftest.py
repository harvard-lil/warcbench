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
    with filepath.open("rb") as doc:
        yield doc


@pytest.fixture
def warc_file(wacz_file: BufferedReader):
    with zipfile.Path(wacz_file, "archive/data.warc.gz").open("rb") as warc_gz_file:
        with gzip.open(warc_gz_file, "rb") as warc_file:
            yield warc_file
