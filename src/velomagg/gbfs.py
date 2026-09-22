"""Parsing et validation des fichiers GBFS 2.2 (station_information, station_status).

Fonctions pures, sans réseau ni base : elles sont couvertes par les tests unitaires.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

STATUS_REQUIRED_INT = ("num_bikes_available", "num_docks_available", "last_reported")
STATUS_REQUIRED_BOOL = ("is_installed", "is_renting", "is_returning")


class ValidationError(ValueError):
    """Message qui ne respecte pas le schéma attendu."""


@dataclass(frozen=True)
class StationStatus:
    station_id: str
    num_bikes_available: int
    num_docks_available: int
    is_installed: bool
    is_renting: bool
    is_returning: bool
    last_reported: int
    feed_last_updated: int
    fetched_at: float
    vehicle_types_available: list[dict[str, Any]] = field(default_factory=list)
    is_replay: bool = False

    @property
    def dedup_key(self) -> tuple[str, int]:
        return (self.station_id, self.last_reported)


@dataclass(frozen=True)
class StationInformation:
    station_id: str
    name: str
    lat: float
    lon: float
    capacity: int
    is_virtual_station: bool
    feed_last_updated: int


def _as_bool(value: Any, name: str) -> bool:
    # GBFS 2.2 impose des booléens ; certaines implémentations envoient 0/1.
    if isinstance(value, bool):
        return value
    if value in (0, 1):
        return bool(value)
    raise ValidationError(f"{name} doit être un booléen, reçu {value!r}")


def _as_int(value: Any, name: str, minimum: int | None = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"{name} doit être un entier, reçu {value!r}")
    if minimum is not None and value < minimum:
        raise ValidationError(f"{name} doit être >= {minimum}, reçu {value}")
    return value


def _as_station_id(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"station_id invalide : {value!r}")
    return value.strip()


def feed_envelope(payload: dict[str, Any]) -> tuple[int, int, list[dict[str, Any]]]:
    """Retourne (last_updated, ttl, stations) d'un fichier GBFS de stations."""
    try:
        last_updated = _as_int(payload["last_updated"], "last_updated", minimum=1)
        ttl = _as_int(payload["ttl"], "ttl", minimum=0)
        stations = payload["data"]["stations"]
    except (KeyError, TypeError) as exc:
        raise ValidationError(f"enveloppe GBFS incomplète : {exc}") from exc
    if not isinstance(stations, list):
        raise ValidationError("data.stations doit être une liste")
    return last_updated, ttl, stations


def status_message(station: dict[str, Any], feed_last_updated: int, fetched_at: float) -> dict[str, Any]:
    """Construit le message publié dans Redpanda pour une station (côté producteur)."""
    return {
        **station,
        "feed_last_updated": feed_last_updated,
        "fetched_at": fetched_at,
    }


def parse_status(message: dict[str, Any]) -> StationStatus:
    """Valide un message de statut (côté consommateur) et le convertit."""
    if not isinstance(message, dict):
        raise ValidationError("le message doit être un objet JSON")
    missing = [k for k in ("station_id", *STATUS_REQUIRED_INT, *STATUS_REQUIRED_BOOL) if k not in message]
    if missing:
        raise ValidationError(f"champs manquants : {', '.join(missing)}")
    vehicle_types = message.get("vehicle_types_available") or []
    if not isinstance(vehicle_types, list):
        raise ValidationError("vehicle_types_available doit être une liste")
    fetched_at = message.get("fetched_at")
    if isinstance(fetched_at, bool) or not isinstance(fetched_at, (int, float)):
        raise ValidationError(f"fetched_at invalide : {fetched_at!r}")
    return StationStatus(
        station_id=_as_station_id(message["station_id"]),
        num_bikes_available=_as_int(message["num_bikes_available"], "num_bikes_available"),
        num_docks_available=_as_int(message["num_docks_available"], "num_docks_available"),
        is_installed=_as_bool(message["is_installed"], "is_installed"),
        is_renting=_as_bool(message["is_renting"], "is_renting"),
        is_returning=_as_bool(message["is_returning"], "is_returning"),
        last_reported=_as_int(message["last_reported"], "last_reported", minimum=1),
        feed_last_updated=_as_int(message.get("feed_last_updated"), "feed_last_updated", minimum=1),
        fetched_at=float(fetched_at),
        vehicle_types_available=vehicle_types,
        is_replay=_as_bool(message.get("replay", False), "replay"),
    )


def parse_information(message: dict[str, Any]) -> StationInformation:
    """Valide un message de description de station."""
    if not isinstance(message, dict):
        raise ValidationError("le message doit être un objet JSON")
    try:
        lat = float(message["lat"])
        lon = float(message["lon"])
        name = str(message["name"]).strip()
    except (KeyError, TypeError, ValueError) as exc:
        raise ValidationError(f"station_information invalide : {exc}") from exc
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        raise ValidationError(f"coordonnées hors bornes : {lat}, {lon}")
    if not name:
        raise ValidationError("nom de station vide")
    return StationInformation(
        station_id=_as_station_id(message.get("station_id")),
        name=name,
        lat=lat,
        lon=lon,
        capacity=_as_int(message.get("capacity"), "capacity"),
        is_virtual_station=_as_bool(message.get("is_virtual_station", False), "is_virtual_station"),
        feed_last_updated=_as_int(message.get("feed_last_updated"), "feed_last_updated", minimum=1),
    )


def dedup_batch(records: list[StationStatus]) -> tuple[list[StationStatus], int]:
    """Retire les doublons (station_id, last_reported) à l'intérieur d'un lot.

    Retourne (lot sans doublon, nombre de doublons retirés). Les doublons entre lots
    sont écartés par la contrainte d'unicité en base.
    """
    seen: set[tuple[str, int]] = set()
    unique: list[StationStatus] = []
    for rec in records:
        if rec.dedup_key in seen:
            continue
        seen.add(rec.dedup_key)
        unique.append(rec)
    return unique, len(records) - len(unique)
