import json
import re
import time
from pathlib import Path
from typing import TypedDict

import boto3
from botocore import UNSIGNED
from botocore.config import Config


class BuildStockRelease(TypedDict):
    release_year: str
    res_com: str
    weather: str
    release_number: str


def resolve_bldgid_sets(
    bucket_name: str = "oedi-data-lake",
    prefix: str = "nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock",
    output_file: str = "buildstock_releases.json",
) -> dict[str, BuildStockRelease]:
    """
    Get URLs containing 'building_energy_models' from the NREL S3 bucket.
    Parses the URL structure to extract available releases.

    Args:
        bucket_name (str): Name of the S3 bucket
        prefix (str): Prefix path in the bucket to search
        output_file (str): Path to output JSON file

    Returns:
        dict[str, BuildStockRelease]: Dictionary of releases with keys following pattern:
            {res/com}_{release_year}_{weather}_{release_number}
            Example: "res_2022_tmy3_1"
    """
    # Initialize S3 client with unsigned requests (for public buckets)
    s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    # Dictionary to store releases
    releases: dict[str, BuildStockRelease] = {}
    # Set to track unique combinations
    seen_combinations = set()

    # Regex pattern to extract components from full release path
    pattern = r"(\d{4})/(res|com)stock_(\w+)_release_(\d+(?:\.\d+)?)"

    # First, list all year directories
    paginator = s3_client.get_paginator("list_objects_v2")
    year_pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix + "/", Delimiter="/")

    for year_page in year_pages:
        # Get common prefixes (directories) at this level
        if "CommonPrefixes" not in year_page:
            continue

        for year_prefix in year_page["CommonPrefixes"]:
            year_path = year_prefix["Prefix"]

            # List contents of the year directory to find release directories
            release_pages = paginator.paginate(Bucket=bucket_name, Prefix=year_path, Delimiter="/")

            for release_page in release_pages:
                if "CommonPrefixes" not in release_page:
                    continue

                for release_prefix in release_page["CommonPrefixes"]:
                    release_path = release_prefix["Prefix"].rstrip("/")

                    # Try to match the pattern against the full path
                    relative_path = release_path.replace(prefix, "").lstrip("/")
                    match = re.match(pattern, relative_path)

                    if match:
                        release_year, res_com_type, weather, release_number = match.groups()
                        # Create a tuple of the components to check uniqueness
                        combination = (release_year, f"{res_com_type}stock", weather, release_number)
                        if combination not in seen_combinations:
                            seen_combinations.add(combination)
                            release_data = {
                                "release_year": release_year,
                                "res_com": f"{res_com_type}stock",
                                "weather": weather,
                                "release_number": release_number,
                            }
                            # Create key following the pattern: {res/com}_{release_year}_{weather}_{release_number}
                            key = f"{res_com_type}_{release_year}_{weather}_{release_number}"
                            releases[key] = release_data

    # Save to JSON file with consistent formatting
    output_path = Path(__file__).parent / output_file
    with open(output_path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(releases, f, indent=2, sort_keys=False, ensure_ascii=False)
        f.write("\n")  # Ensure newline at end of file

    return releases


if __name__ == "__main__":
    start_time = time.time()
    releases = resolve_bldgid_sets()
    elapsed_time = time.time() - start_time
    print(f"\nFound {len(releases)} unique releases in {elapsed_time:.2f} seconds")
