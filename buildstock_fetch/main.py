import os
from pathlib import Path

import requests


def fetch_bldg_ids(state: str) -> list[str]:
    """Summary line.

    Extended description of function.

    Args:
        bar: Description of input argument.

    Returns:
        Description of return value
    """
    if state == "MA":
        return ["0000007", "0000008", "0000009"]

    else:
        raise NotImplementedError(f"State {state} not supported")


def fetch_bldg_data(
    bldg_ids: list[str],
    release_number: str = "1.1",
    release_year: str = "2022",
    res_com: str = "resstock",
    weather: str = "tmy3",
    upgrade_id: str = "0",
) -> list[Path]:
    """Summary line.

    Downloads the data for the given building ids and returns list of paths to the downloaded files.
    """
    data_dir = Path(__file__).parent.parent / "data"
    downloaded_paths = []
    os.makedirs(data_dir, exist_ok=True)

    for bldg_id in bldg_ids:
        url = (
            "https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/"
            f"end-use-load-profiles-for-us-building-stock/{release_year}/{res_com}_{weather}_release_{release_number}/"
            f"building_energy_models/upgrade={upgrade_id}/bldg{bldg_id}-up0{upgrade_id}.zip"
        )

        response = requests.get(url, timeout=30)
        response.raise_for_status()

        output_path = data_dir / f"{bldg_id}_upgrade{upgrade_id}.zip"
        with open(output_path, "wb") as file:
            file.write(response.content)

        downloaded_paths.append(output_path)
    return downloaded_paths


if __name__ == "__main__":  # pragma: no cover
    tmp_ids = fetch_bldg_ids("MA")
    tmp_data = fetch_bldg_data(tmp_ids)
    print(f"Downloaded files: {[str(path) for path in tmp_data]}")
