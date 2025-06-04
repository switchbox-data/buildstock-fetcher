import shutil
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).parent.parent))

from buildstock_fetch.main import fetch_bldg_data, fetch_bldg_ids


@pytest.fixture(scope="function")
def cleanup_downloads():
    # Setup - clean up any existing files before test
    data_dir = Path("data")
    if data_dir.exists():
        shutil.rmtree(data_dir)

    yield

    # Teardown - clean up downloaded files after test
    if data_dir.exists():
        shutil.rmtree(data_dir)


def test_fetch_bldg_ids():
    assert fetch_bldg_ids("MA") == ["0000007", "0000008", "0000009"]


def test_fetch_bldg_data(cleanup_downloads):
    fetch_bldg_data(["0000007", "0000008"])
    assert Path("data/0000007_upgrade0.zip").exists()
    assert Path("data/0000008_upgrade0.zip").exists()
