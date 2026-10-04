"""Download the Electricity Authority data for the NZ electricity lakehouse.

Saves everything into ~/Downloads/nz-electricity-data/, ready to upload to the
Databricks volume. Only uses Python's standard library, so it runs as-is:

    python3 download_emi_data.py

It's safe to stop and re-run: files already downloaded are skipped.
"""
import gzip
import re
import shutil
import sys
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

BASE = "https://emidatasets.blob.core.windows.net/publicdata"
OUT = Path.home() / "Downloads" / "nz-electricity-data"
PRICES = "Datasets/Wholesale/DispatchAndPricing/FinalEnergyPrices/ByMonth/"
HYDRO = "Datasets/Environment/HydrologicalModellingDataset/3_StorageAndSpill_20241231/3_1_Storage/"
SUPPLY_POINTS = "Datasets/Wholesale/MappingsAndGeospatial/NetworkSupplyPointsTable/"


def list_files(prefix):
    """All file names under a folder of the public container."""
    names, marker = [], ""
    while True:
        url = (f"{BASE}?restype=container&comp=list&prefix={urllib.parse.quote(prefix)}"
               + (f"&marker={marker}" if marker else ""))
        xml = urllib.request.urlopen(url, timeout=120).read().decode()
        names += re.findall(r"<Blob><Name>(.*?)</Name>", xml)
        m = re.search(r"<NextMarker>(.*?)</NextMarker>", xml)
        if not m or not m.group(1):
            return names
        marker = m.group(1)


def download(name, dest, compress):
    if dest.exists():
        return "skip"
    tmp = dest.with_suffix(dest.suffix + ".part")
    url = f"{BASE}/{urllib.parse.quote(name)}"
    with urllib.request.urlopen(url, timeout=600) as r:
        opener = gzip.open if compress else open
        with opener(tmp, "wb") as f:
            shutil.copyfileobj(r, f)
    tmp.rename(dest)
    return "ok"


def main():
    jobs = []
    # 1. Half-hourly prices, one file per month since October 1996 (gzipped)
    for name in list_files(PRICES):
        if name.endswith(".csv"):
            jobs.append((name, OUT / "prices" / (Path(name).name + ".gz"), True))
    # 2. Daily hydro lake storage
    for name in list_files(HYDRO):
        if name.endswith(".csv"):
            jobs.append((name, OUT / "hydro" / Path(name).name, False))
    # 3. The latest table of grid points (maps each point to its region)
    latest = sorted(n for n in list_files(SUPPLY_POINTS) if n.endswith(".csv"))[-1]
    jobs.append((latest, OUT / "reference" / "NetworkSupplyPointsTable.csv", False))

    for d in ("prices", "hydro", "reference"):
        (OUT / d).mkdir(parents=True, exist_ok=True)
    print(f"Downloading {len(jobs)} files to {OUT}")
    done = 0
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(download, *j) for j in jobs]
        for f in futures:
            f.result()
            done += 1
            sys.stdout.write(f"\r{done}/{len(jobs)} files")
            sys.stdout.flush()
    size = sum(p.stat().st_size for p in OUT.rglob("*") if p.is_file()) / 1e6
    print(f"\nDone: {size:,.0f} MB in {OUT}")


if __name__ == "__main__":
    main()
