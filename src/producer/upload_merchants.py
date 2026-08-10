"""Generate merchant master data and upload it to the Unity Catalog volume.

`MerchantGenerator` writes a CSV to the local data directory, which is fine for
the producers -- they read it in-process to enrich transactions. But bronze
ingests merchants through Auto Loader, which watches a volume. This script is
the bridge: generate, then upload to /Volumes/finguard/source/merchants/.

Each run writes a timestamped file rather than overwriting. Auto Loader tracks
which files it has already processed, so a new file is picked up as new rows --
which is exactly what makes merchant attribute changes (risk rating,
blacklisting) visible downstream as SCD2 history.

Usage:
    python upload_merchants.py                 # generate + upload
    python upload_merchants.py --local-only    # generate, skip upload
"""

from __future__ import annotations

import argparse
import base64
import configparser
import json
import os
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from config import load_settings
from merchant_generator import MerchantGenerator

VOLUME_PATH = "/Volumes/finguard/source/merchants/source_data"


def _workspace_credentials() -> tuple[str, str]:
    """Read host/token from the Databricks CLI config, falling back to env."""
    host = os.getenv("DATABRICKS_HOST", "")
    token = os.getenv("DATABRICKS_TOKEN", "")
    if host and token:
        return host.rstrip("/"), token

    cfg_path = Path.home() / ".databrickscfg"
    if not cfg_path.exists():
        raise RuntimeError(
            "no Databricks credentials: set DATABRICKS_HOST/DATABRICKS_TOKEN "
            "or run `databricks configure --token`"
        )

    parser = configparser.ConfigParser()
    parser.read(cfg_path)
    profile = os.getenv("DATABRICKS_PROFILE", "DEFAULT")
    section = parser[profile]
    return section["host"].rstrip("/"), section["token"]


def upload_to_volume(local_path: Path, remote_path: str) -> None:
    host, token = _workspace_credentials()

    # The Files API takes raw bytes on PUT, unlike the workspace import API
    # which expects base64. Overwrite is explicit so a re-run of the same
    # timestamp does not fail.
    url = f"{host}/api/2.0/fs/files{remote_path}?overwrite=true"
    request = urllib.request.Request(
        url,
        data=local_path.read_bytes(),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/octet-stream",
        },
        method="PUT",
    )

    try:
        urllib.request.urlopen(request)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode()[:300]
        raise RuntimeError(f"upload failed ({exc.code}): {detail}") from None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--local-only",
        action="store_true",
        help="generate the CSV but do not upload it",
    )
    args = parser.parse_args()

    settings = load_settings()
    data_dir = settings.base_dir / "data"
    local_path = data_dir / "merchants.csv"

    merchants = MerchantGenerator(
        total_merchants=settings.total_merchants,
        seed=settings.random_seed,
        output_path=local_path,
    ).generate()

    blacklisted = sum(1 for m in merchants if m.is_blacklisted)
    high_risk = sum(1 for m in merchants if m.merchant_risk == "HIGH")
    print(f"generated {len(merchants)} merchants -> {local_path}")
    print(f"  high risk: {high_risk}  blacklisted: {blacklisted}")

    if args.local_only:
        return

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    remote_path = f"{VOLUME_PATH}/merchants_{stamp}.csv"
    upload_to_volume(local_path, remote_path)
    print(f"uploaded -> {remote_path}")


if __name__ == "__main__":
    main()
