import json
import re
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
    output_file: str = "available_urls.json",
) -> dict[str, BuildStockRelease]:
    """
    Get URLs containing 'building_energy_models' from the NREL S3 bucket (first 5 pages only).
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

    # List objects in the bucket with the given prefix
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    # Regex pattern to extract components
    pattern = r"(\d{4})/(res|com)stock_(\w+)_release_(\d+(?:\.\d+)?)/.*"

    for page_number, page in enumerate(pages):
        if page_number >= 5:  # Only get first 5 pages
            break

        if "Contents" not in page:
            continue

        for obj in page["Contents"]:
            key = obj["Key"]
            # Remove the prefix to get just the part after it
            relative_path = key.replace(prefix, "").lstrip("/")
            if not relative_path:  # Skip if there's nothing after the prefix
                continue

            # Try to match the pattern and extract components
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
    releases = resolve_bldgid_sets()
    print("\nAvailable releases:")
    for key, release in releases.items():
        print(f"\n{key}:")
        for field, value in release.items():
            print(f"  {field}: {value}")
