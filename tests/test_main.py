from buildstock_fetcher.foo import fetch_bldg_ids


def test_fetch_bldg_ids():
    assert fetch_bldg_ids("MA") == ["0000007", "0000008", "0000009"]


def test_fetch_bldg_data():
    pass
