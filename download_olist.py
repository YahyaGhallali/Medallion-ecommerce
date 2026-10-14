"""Download and extract the Olist Brazilian E-Commerce dataset from Kaggle."""

from __future__ import annotations

import argparse
import base64
import json
import os
import shutil
import tempfile
import urllib.error
import urllib.request
import zipfile
from pathlib import Path


DATASET_URL = (
    "https://www.kaggle.com/api/v1/datasets/download/olistbr/brazilian-ecommerce"
)


def _request() -> urllib.request.Request:
    request = urllib.request.Request(DATASET_URL)
    token = os.environ.get("KAGGLE_API_TOKEN")
    if token:
        request.add_header("Authorization", f"Bearer {token}")
        return request

    username = os.environ.get("KAGGLE_USERNAME")
    key = os.environ.get("KAGGLE_KEY")
    credentials_file = Path.home() / ".kaggle" / "kaggle.json"
    if username and key:
        credentials = (username, key)
    elif credentials_file.exists():
        credentials_data = json.loads(credentials_file.read_text(encoding="utf-8"))
        credentials = (
            credentials_data["username"],
            credentials_data["key"],
        )
    else:
        credentials = None

    if credentials:
        encoded_credentials = base64.b64encode(
            f"{credentials[0]}:{credentials[1]}".encode("utf-8")
        ).decode("ascii")
        request.add_header("Authorization", f"Basic {encoded_credentials}")
    return request


def download_dataset(output_dir: Path) -> list[Path]:
    """Download the dataset zip and return the extracted CSV paths."""
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        with urllib.request.urlopen(_request()) as response:
            with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as archive:
                shutil.copyfileobj(response, archive)
                archive_path = Path(archive.name)
    except urllib.error.HTTPError as error:
        if error.code in (401, 403):
            raise RuntimeError(
                "Kaggle authentication is required. Set KAGGLE_API_TOKEN, "
                "KAGGLE_USERNAME and KAGGLE_KEY, or create ~/.kaggle/kaggle.json."
            ) from error
        raise RuntimeError(f"Kaggle download failed with HTTP {error.code}.") from error

    try:
        with zipfile.ZipFile(archive_path) as archive:
            archive.extractall(output_dir)
            csv_paths = [
                output_dir / member
                for member in archive.namelist()
                if member.lower().endswith(".csv")
            ]
    finally:
        archive_path.unlink(missing_ok=True)

    return csv_paths


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download and extract Olist Brazilian E-Commerce CSV files."
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=Path("data/olist-brazilian-ecommerce"),
        help="Directory where the CSV files will be extracted.",
    )
    args = parser.parse_args()

    csv_paths = download_dataset(args.output_dir)
    print(f"Downloaded {len(csv_paths)} CSV files to {args.output_dir.resolve()}")


if __name__ == "__main__":
    main()
