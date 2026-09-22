"""Consommateur : lit Redpanda, valide, déduplique et écrit dans PostgreSQL par lots.

Garantie : au moins une fois. Les décalages (offsets) ne sont validés qu'après le COMMIT
PostgreSQL ; si le processus tombe entre les deux, le lot est relu et les doublons sont
écartés par la contrainte d'unicité (station_id, last_reported). Résultat : idempotent en base.
"""

from __future__ import annotations

import json
import logging
import signal
import time
from datetime import UTC, datetime

import psycopg
from confluent_kafka import Consumer, KafkaError, Message
from psycopg.types.json import Jsonb

from velomagg.config import Settings
from velomagg.gbfs import (
    StationInformation,
    StationStatus,
    ValidationError,
    dedup_batch,
    parse_information,
    parse_status,
)

log = logging.getLogger("consumer")
_running = True

INSERT_STATUS = """
INSERT INTO raw.station_status (
    station_id, last_reported, num_bikes_available, num_docks_available,
    is_installed, is_renting, is_returning, vehicle_types_available,
    feed_last_updated, fetched_at, kafka_partition, kafka_offset, is_replay
) VALUES (%s, to_timestamp(%s), %s, %s, %s, %s, %s, %s, to_timestamp(%s), to_timestamp(%s), %s, %s, %s)
ON CONFLICT (station_id, last_reported) DO NOTHING
"""

UPSERT_INFORMATION = """
INSERT INTO raw.station_information (station_id, name, lat, lon, capacity, is_virtual_station, feed_last_updated)
VALUES (%s, %s, %s, %s, %s, %s, to_timestamp(%s))
ON CONFLICT (station_id) DO UPDATE SET
    name = EXCLUDED.name, lat = EXCLUDED.lat, lon = EXCLUDED.lon, capacity = EXCLUDED.capacity,
    is_virtual_station = EXCLUDED.is_virtual_station, feed_last_updated = EXCLUDED.feed_last_updated,
    updated_at = clock_timestamp()
"""

INSERT_BATCH = """
INSERT INTO raw.ingest_batches (started_at, finished_at, received, invalid, status_valid,
                                status_inserted, status_duplicates, information_upserted)
VALUES (%s, clock_timestamp(), %s, %s, %s, %s, %s, %s)
"""


def _stop(*_: object) -> None:
    global _running
    _running = False


def split_messages(messages: list[Message], settings: Settings):
    """Répartit les messages par topic et isole ceux qui sont invalides."""
    statuses: list[tuple[StationStatus, Message]] = []
    informations: list[StationInformation] = []
    rejected: list[tuple[Message, str]] = []
    for msg in messages:
        try:
            payload = json.loads(msg.value())
            if msg.topic() == settings.topic_status:
                statuses.append((parse_status(payload), msg))
            elif msg.topic() == settings.topic_information:
                informations.append(parse_information(payload))
        except (ValidationError, json.JSONDecodeError, TypeError) as exc:
            rejected.append((msg, str(exc)))
    return statuses, informations, rejected


def write_batch(conn: psycopg.Connection, messages: list[Message], settings: Settings, started: datetime) -> dict:
    statuses, informations, rejected = split_messages(messages, settings)
    by_key = {rec.dedup_key: msg for rec, msg in statuses}
    unique, in_batch_dups = dedup_batch([rec for rec, _ in statuses])
    inserted = 0
    with conn.transaction(), conn.cursor() as cur:
        for info in informations:
            cur.execute(UPSERT_INFORMATION, (info.station_id, info.name, info.lat, info.lon, info.capacity,
                                             info.is_virtual_station, info.feed_last_updated))
        for rec in unique:
            msg = by_key[rec.dedup_key]
            cur.execute(INSERT_STATUS, (
                rec.station_id, rec.last_reported, rec.num_bikes_available, rec.num_docks_available,
                rec.is_installed, rec.is_renting, rec.is_returning, Jsonb(rec.vehicle_types_available),
                rec.feed_last_updated, rec.fetched_at, msg.partition(), msg.offset(), rec.is_replay,
            ))
            inserted += cur.rowcount
        for msg, error in rejected:
            cur.execute(
                "INSERT INTO raw.rejected_messages (topic, kafka_partition, kafka_offset, payload, error) "
                "VALUES (%s, %s, %s, %s, %s)",
                (msg.topic(), msg.partition(), msg.offset(), (msg.value() or b"").decode("utf-8", "replace"), error),
            )
        duplicates = in_batch_dups + (len(unique) - inserted)
        cur.execute(INSERT_BATCH, (started, len(messages), len(rejected), len(statuses), inserted,
                                   duplicates, len(informations)))
    return {"received": len(messages), "inserted": inserted, "duplicates": duplicates, "invalid": len(rejected)}


def run() -> None:
    settings = Settings()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap,
        "group.id": settings.consumer_group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # validation manuelle après écriture en base
    })
    consumer.subscribe([settings.topic_status, settings.topic_information])
    conn = psycopg.connect(settings.postgres_dsn, autocommit=True)
    log.info("consommateur prêt (groupe %s)", settings.consumer_group)
    try:
        while _running:
            started = datetime.now(UTC)
            messages = consumer.consume(num_messages=settings.batch_size, timeout=settings.batch_timeout_s)
            good = []
            for m in messages:
                if m.error():
                    if m.error().code() != KafkaError._PARTITION_EOF:
                        log.warning("erreur Kafka : %s", m.error())
                    continue
                good.append(m)
            if not good:
                continue
            stats = write_batch(conn, good, settings, started)
            consumer.commit(asynchronous=False)
            log.info("lot écrit : %s", stats)
    finally:
        consumer.close()
        conn.close()
        log.info("arrêt du consommateur")


if __name__ == "__main__":
    while True:
        try:
            run()
            break
        except psycopg.OperationalError as exc:  # base pas encore prête : on réessaie
            logging.getLogger("consumer").warning("PostgreSQL indisponible (%s), nouvel essai dans 5 s", exc)
            time.sleep(5)
