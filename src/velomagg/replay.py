"""Rejoue les archives brutes (scripts/archive_gbfs.py) dans Redpanda.

Les messages rejoués portent `"replay": true` : le consommateur les écrit avec is_replay = true
et les indicateurs de latence les excluent (leur heure d'insertion n'a rien de temps réel).
Usage : docker compose run --rm -v ./data:/app/data producer python -m velomagg.replay data/archive
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

from confluent_kafka import Producer

from velomagg.config import Settings
from velomagg.gbfs import ValidationError, feed_envelope, status_message
from velomagg.producer import ensure_topics

log = logging.getLogger("replay")


def iter_archive(folder: Path):
    """Produit (topic_logique, message, clé) à partir des fichiers .jsonl d'archive, dans l'ordre."""
    for path in sorted(folder.glob("station_information_*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            rec = json.loads(line)
            last_updated, _, stations = feed_envelope(rec["information"])
            for st in stations:
                yield "information", {**st, "feed_last_updated": last_updated}, str(st["station_id"])
    for path in sorted(folder.glob("station_status_*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            rec = json.loads(line)
            try:
                last_updated, _, stations = feed_envelope(rec["status"])
            except ValidationError as exc:
                log.warning("instantané ignoré : %s", exc)
                continue
            for st in stations:
                msg = {**status_message(st, last_updated, rec["fetched_at"]), "replay": True}
                yield "status", msg, str(st["station_id"])


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    settings = Settings()
    ensure_topics(settings)
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap, "enable.idempotence": True,
                         "linger.ms": 100, "compression.type": "zstd"})
    topics = {"information": settings.topic_information, "status": settings.topic_status}
    n = 0
    for kind, msg, key in iter_archive(Path(sys.argv[1] if len(sys.argv) > 1 else "data/archive")):
        producer.produce(topics[kind], key=key, value=json.dumps(msg))
        n += 1
        if n % 5000 == 0:
            producer.flush(30)
    producer.flush(60)
    log.info("%d messages rejoués", n)


if __name__ == "__main__":
    main()
