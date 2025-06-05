import json

import boto3
from botocore import UNSIGNED
from botocore.config import Config


def resolve_bldgid_sets(
    bucket_name: str = "oedi-data-lake",
    prefix: str = "nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock",
    output_file: str = "available_urls.json",
) -> list[str]:
    """
    Get URLs containing 'building_energy_models' from the NREL S3 bucket (first 5 pages only).

    Args:
        bucket_name (str): Name of the S3 bucket
        prefix (str): Prefix path in the bucket to search
        output_file (str): Path to output JSON file

    Returns:
        List[str]: List of building energy model URLs after the prefix
    """
    # Initialize S3 client with unsigned requests (for public buckets)
    s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    # List to store all URLs
    urls = []

    # List objects in the bucket with the given prefix
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

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
                if relative_path:  # Only add if there's something after the prefix
                    urls.append(relative_path)

    # Save to JSON file
    with open(output_file, "w") as f:
        json.dump(urls, f, indent=2)

    return urls


if __name__ == "__main__":
    urls = resolve_bldgid_sets()
    print("\nFirst 5 building energy model URLs:")
    for url in urls[:5]:
        print(url)
