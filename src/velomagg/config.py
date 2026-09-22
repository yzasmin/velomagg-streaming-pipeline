"""Configuration lue dans les variables d'environnement (voir .env.example)."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    gbfs_root: str = _env("GBFS_ROOT", "https://gbfs.theta.fifteen.eu/gbfs/2.2/montpellier/en")
    kafka_bootstrap: str = _env("KAFKA_BOOTSTRAP", "redpanda:9092")
    topic_status: str = _env("TOPIC_STATUS", "velomagg.station_status")
    topic_information: str = _env("TOPIC_INFORMATION", "velomagg.station_information")
    consumer_group: str = _env("CONSUMER_GROUP", "velomagg-postgres-writer")
    batch_size: int = int(_env("BATCH_SIZE", "500"))
    batch_timeout_s: float = float(_env("BATCH_TIMEOUT_S", "5"))
    min_poll_interval_s: int = int(_env("MIN_POLL_INTERVAL_S", "30"))
    http_timeout_s: float = float(_env("HTTP_TIMEOUT_S", "20"))
    postgres_dsn: str = (
        f"host={_env('POSTGRES_HOST', 'postgres')} port={_env('POSTGRES_PORT', '5432')} "
        f"dbname={_env('POSTGRES_DB', 'velomagg')} user={_env('POSTGRES_USER', 'velomagg')} "
        f"password={_env('POSTGRES_PASSWORD', 'velomagg')}"
    )
