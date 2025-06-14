import json
import os
from pathlib import Path

import requests

with open("buildstock_releases.json") as file:
    data = json.load(file)

data_dir = Path(__file__).parent.parent / "data"
os.makedirs(data_dir, exist_ok=True)

columns_to_keep = [
    "in.puma",
    "in.nhgis_puma_gisjoin",
    "in.resstock_puma_id",
    "in.state",
    "in.state_name",
    "in.resstock_county_id",
    "bldg_id",
    "upgrade",
]

for release_name, release in data.items():
    release_year = release["release_year"]
    res_com = release["res_com"]
    weather = release["weather"]
    release_number = release["release_number"]
    upgrade_ids = release["upgrade_ids"]

    if release_year == "2021":
        download_url = (
            f"https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/"
            f"end-use-load-profiles-for-us-building-stock/{release_year}/"
            f"{res_com}_{weather}_release_{release_number}/metadata/metadata.parquet"
        )
        response = requests.get(download_url, timeout=30)
        response.raise_for_status()

        output_path = data_dir / f"{release_name}_baseline.parquet"
        with open(output_path, "wb") as file:
            file.write(response.content)
    elif release_year == "2022":
        for upgrade_id in upgrade_ids:
            upgrade_id = int(upgrade_id)
            if upgrade_id == 0:
                download_url = (
                    f"https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/"
                    f"end-use-load-profiles-for-us-building-stock/{release_year}/"
                    f"{res_com}_{weather}_release_{release_number}/metadata/baseline.parquet"
                )
            else:
                ownload_url = (
                    f"https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/"
                    f"end-use-load-profiles-for-us-building-stock/{release_year}/"
                    f"{res_com}_{weather}_release_{release_number}/metadata/upgrade{upgrade_id:02d}.parquet"
                )
                response = requests.get(download_url, timeout=30)
                response.raise_for_status()

                output_path = data_dir / f"{release_name}_upgrade{upgrade_id:02d}.parquet"
                with open(output_path, "wb") as file:
                    file.write(response.content)
