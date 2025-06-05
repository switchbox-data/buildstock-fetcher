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
) -> list[BuildStockRelease]:
    """
    Get URLs containing 'building_energy_models' from the NREL S3 bucket (first 5 pages only).
    Parses the URL structure to extract available releases.
    Args:
        bucket_name (str): Name of the S3 bucket
        prefix (str): Prefix path in the bucket to search
        output_file (str): Path to output JSON file

    Returns:
        list[BuildStockRelease]: List of dictionaries containing parsed URL data with structure:
            {
                'release_year': str,     # Year of the release (e.g. '2022')
                'res_com': str,          # Either 'resstock' or 'comstock'
                'weather': str,          # Weather type (e.g. 'tmy3')
                'release_number': str,   # Release number (e.g. '1')
            }
    """
    # Initialize S3 client with unsigned requests (for public buckets)
    s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    # List to store all URLs and their metadata
    releases: list[BuildStockRelease] = []
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
            # Only include paths containing building_energy_models
            if "building_energy_model" in key:
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
                        releases.append(release_data)

    # Save to JSON file with consistent formatting
    output_path = Path(__file__).parent / output_file
    with open(output_path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(releases, f, indent=2, sort_keys=False, ensure_ascii=False)
        f.write("\n")  # Ensure newline at end of file

    return releases


if __name__ == "__main__":
    releases = resolve_bldgid_sets()
    print("\nFirst 5 building energy model releases:")
    for release in releases[:5]:
        print("\nRelease:")
        for key, value in release.items():
            print(f"  {key}: {value}")
