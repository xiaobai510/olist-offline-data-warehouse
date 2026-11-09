from __future__ import annotations

import argparse
from pathlib import Path
import shutil
import urllib.request


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("url")
    parser.add_argument("target", type=Path)
    args = parser.parse_args()

    request = urllib.request.Request(
        args.url,
        headers={"User-Agent": "Mozilla/5.0 Olist-Realtime-Bootstrap"},
    )
    args.target.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(request, timeout=60) as response:
        with args.target.open("wb") as output:
            shutil.copyfileobj(response, output, length=1024 * 1024)

    if not args.target.exists() or args.target.stat().st_size == 0:
        raise RuntimeError(f"downloaded file is empty: {args.target}")
    print(f"saved {args.target} bytes={args.target.stat().st_size}")


if __name__ == "__main__":
    main()

