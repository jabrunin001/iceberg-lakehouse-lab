"""Download the MovieLens small dataset into data/raw/movielens/."""
from __future__ import annotations

import io
import zipfile
from pathlib import Path

import requests

URL = "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"


def expected_files() -> list[str]:
    return ["movies.csv", "ratings.csv", "tags.csv", "links.csv"]


def target_dir() -> Path:
    return Path("data/raw/movielens")


def download() -> Path:
    dest = target_dir()
    dest.mkdir(parents=True, exist_ok=True)
    resp = requests.get(URL, timeout=120)
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        for member in zf.namelist():
            name = Path(member).name
            if name in expected_files():
                (dest / name).write_bytes(zf.read(member))
    return dest


if __name__ == "__main__":
    out = download()
    print(f"Downloaded MovieLens to {out}: {sorted(p.name for p in out.glob('*.csv'))}")
