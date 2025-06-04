import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from buildstock_fetcher.main import fetch_bldg_data, fetch_bldg_ids


def test_fetch_bldg_ids():
    assert fetch_bldg_ids("MA") == ["0000007", "0000008", "0000009"]


def test_fetch_bldg_data():
    fetch_bldg_data(["0000007", "0000008"])
    assert Path("data/0000007_upgrade0.zip").exists()
    assert Path("data/0000008_upgrade0.zip").exists()
