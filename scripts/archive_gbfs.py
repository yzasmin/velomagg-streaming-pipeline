"""Archive brute du flux GBFS (sans Docker) : une ligne JSON par interrogation de station_status.

Usage : python scripts/archive_gbfs.py data/archive
Chaque ligne : {"fetched_at": epoch, "status": <réponse station_status>} dans un fichier par jour (UTC).
station_information est archivé une fois par heure. Sert de filet de sécurité et de source de rejeu
(`python -m velomagg.replay`), pas de mesure de latence.
"""

from __future__ import annotations

import json
import sys
import time
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

ROOT = "https://gbfs.theta.fifteen.eu/gbfs/2.2/montpellier/en"


def fetch(name: str) -> dict:
    req = urllib.request.Request(f"{ROOT}/{name}.json",
                                 headers={"User-Agent": "velomagg-streaming-pipeline/0.1 (portfolio)"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.load(resp)


def main(out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    last_info = 0.0
    while True:
        started = time.time()
        ttl = 60
        day = datetime.now(UTC).strftime("%Y-%m-%d")
        try:
            if started - last_info > 3600:
                info = fetch("station_information")
                with open(out / f"station_information_{day}.jsonl", "a", encoding="utf-8") as fh:
                    fh.write(json.dumps({"fetched_at": time.time(), "information": info}) + "\n")
                last_info = started
            fetched_at = time.time()
            status = fetch("station_status")
            ttl = int(status.get("ttl") or 60)
            with open(out / f"station_status_{day}.jsonl", "a", encoding="utf-8") as fh:
                fh.write(json.dumps({"fetched_at": fetched_at, "status": status}) + "\n")
            print(f"{datetime.now(UTC).isoformat()} ok last_updated={status.get('last_updated')}", flush=True)
        except Exception as exc:
            print(f"{datetime.now(UTC).isoformat()} erreur {exc}", flush=True)
        time.sleep(max(30, ttl - (time.time() - started)))


if __name__ == "__main__":
    main(Path(sys.argv[1] if len(sys.argv) > 1 else "data/archive"))
