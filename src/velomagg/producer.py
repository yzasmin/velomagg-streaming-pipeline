"""Producteur : interroge le flux GBFS Vélomagg à la fréquence `ttl` et publie dans Redpanda.

- station_status : un message par station et par interrogation, clé = station_id.
- station_information : publié au démarrage puis seulement quand la description d'une station change.
"""

from __future__ import annotations

import hashlib
import json
import logging
import signal
import time
import urllib.request

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from velomagg.config import Settings
from velomagg.gbfs import ValidationError, feed_envelope, status_message

log = logging.getLogger("producer")
_running = True


def _stop(*_: object) -> None:
    global _running
    _running = False


def fetch_json(url: str, timeout: float) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "velomagg-streaming-pipeline/0.1 (portfolio)"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.load(resp)


def ensure_topics(settings: Settings) -> None:
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
    existing = set(admin.list_topics(timeout=30).topics)
    wanted = [
        NewTopic(settings.topic_status, num_partitions=3, replication_factor=1),
        NewTopic(settings.topic_information, num_partitions=1, replication_factor=1,
                 config={"cleanup.policy": "compact"}),
    ]
    todo = [t for t in wanted if t.topic not in existing]
    for topic, fut in admin.create_topics(todo).items() if todo else []:
        fut.result()
        log.info("topic créé : %s", topic)


def _digest(station: dict) -> str:
    return hashlib.sha1(json.dumps(station, sort_keys=True).encode()).hexdigest()


def run() -> None:
    settings = Settings()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    ensure_topics(settings)
    producer = Producer({
        "bootstrap.servers": settings.kafka_bootstrap,
        "enable.idempotence": True,  # pas de doublon introduit par les réessais du producteur
        "acks": "all",
        "linger.ms": 50,
        "compression.type": "zstd",
    })
    info_digests: dict[str, str] = {}

    while _running:
        started = time.time()
        ttl = settings.min_poll_interval_s
        try:
            info = fetch_json(f"{settings.gbfs_root}/station_information.json", settings.http_timeout_s)
            info_updated, _, info_stations = feed_envelope(info)
            changed = 0
            for st in info_stations:
                digest = _digest(st)
                sid = str(st.get("station_id"))
                if info_digests.get(sid) != digest:
                    info_digests[sid] = digest
                    producer.produce(settings.topic_information, key=sid,
                                     value=json.dumps({**st, "feed_last_updated": info_updated}))
                    changed += 1

            fetched_at = time.time()
            status = fetch_json(f"{settings.gbfs_root}/station_status.json", settings.http_timeout_s)
            last_updated, ttl, stations = feed_envelope(status)
            for st in stations:
                producer.produce(settings.topic_status, key=str(st.get("station_id")),
                                 value=json.dumps(status_message(st, last_updated, fetched_at)))
            producer.flush(30)
            log.info("publié : %d statuts (last_updated=%d, ttl=%d), %d descriptions modifiées",
                     len(stations), last_updated, ttl, changed)
        except (OSError, ValidationError, json.JSONDecodeError) as exc:
            log.warning("interrogation échouée : %s", exc)

        # Respecte le ttl annoncé par le flux, avec un plancher pour ne pas surcharger l'API.
        wait = max(ttl, settings.min_poll_interval_s) - (time.time() - started)
        while _running and wait > 0:
            time.sleep(min(wait, 1))
            wait -= 1
    producer.flush(10)
    log.info("arrêt du producteur")


if __name__ == "__main__":
    run()
