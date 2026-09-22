"""Tests unitaires du parsing GBFS et de la déduplication (sans réseau ni base)."""

import json
from pathlib import Path

import pytest

from velomagg.gbfs import (
    ValidationError,
    dedup_batch,
    feed_envelope,
    parse_information,
    parse_status,
    status_message,
)

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def valid_status(**overrides) -> dict:
    base = {
        "station_id": "006",
        "num_bikes_available": 8,
        "num_docks_available": 19,
        "is_installed": True,
        "is_renting": True,
        "is_returning": True,
        "last_reported": 1790069019,
        "vehicle_types_available": [{"vehicle_type_id": "14", "count": 8}],
        "feed_last_updated": 1790069019,
        "fetched_at": 1790069030.5,
    }
    base.update(overrides)
    return base


def test_enveloppe_du_vrai_flux():
    last_updated, ttl, stations = feed_envelope(load("station_status.json"))
    assert last_updated == 1790069019
    assert ttl == 60
    assert len(stations) == 3


def test_enveloppe_incomplete_refusee():
    with pytest.raises(ValidationError):
        feed_envelope({"ttl": 60, "data": {"stations": []}})
    with pytest.raises(ValidationError):
        feed_envelope({"last_updated": 1, "ttl": 60, "data": {"stations": {}}})


def test_aller_retour_producteur_consommateur():
    payload = load("station_status.json")
    last_updated, _, stations = feed_envelope(payload)
    msg = status_message(stations[0], last_updated, 1790069030.5)
    rec = parse_status(json.loads(json.dumps(msg)))
    assert rec.station_id == "006"
    assert rec.num_bikes_available == 8
    assert rec.dedup_key == ("006", 1790069019)
    assert rec.fetched_at == pytest.approx(1790069030.5)


def test_drapeau_rejeu():
    assert parse_status(valid_status()).is_replay is False
    assert parse_status(valid_status(replay=True)).is_replay is True


def test_booleens_0_1_acceptes():
    rec = parse_status(valid_status(is_installed=1, is_renting=0))
    assert rec.is_installed is True and rec.is_renting is False


@pytest.mark.parametrize(
    "override, message",
    [
        ({"num_bikes_available": -1}, ">= 0"),
        ({"num_bikes_available": "8"}, "entier"),
        ({"num_docks_available": True}, "entier"),
        ({"is_renting": "yes"}, "booléen"),
        ({"station_id": ""}, "station_id"),
        ({"last_reported": 0}, ">= 1"),
        ({"fetched_at": None}, "fetched_at"),
        ({"vehicle_types_available": "14"}, "liste"),
    ],
)
def test_statut_invalide_refuse(override, message):
    with pytest.raises(ValidationError, match=message):
        parse_status(valid_status(**override))


def test_champ_manquant_signale():
    msg = valid_status()
    del msg["num_docks_available"]
    with pytest.raises(ValidationError, match="num_docks_available"):
        parse_status(msg)


def test_station_information_du_vrai_flux():
    payload = load("station_information.json")
    last_updated, _, stations = feed_envelope(payload)
    info = parse_information({**stations[0], "feed_last_updated": last_updated})
    assert info.name == "Place Albert 1er - St Charles"
    assert info.capacity == 27
    assert 43 < info.lat < 44


def test_station_information_coordonnees_hors_bornes():
    with pytest.raises(ValidationError, match="hors bornes"):
        parse_information({"station_id": "1", "name": "x", "lat": 95, "lon": 3.8,
                           "capacity": 10, "feed_last_updated": 1})


def test_dedup_dans_un_lot():
    a = parse_status(valid_status())
    b = parse_status(valid_status(num_bikes_available=7))  # même clé : relu ou republié
    c = parse_status(valid_status(last_reported=1790069079))
    unique, dups = dedup_batch([a, b, c])
    assert dups == 1
    assert [r.last_reported for r in unique] == [1790069019, 1790069079]
    assert unique[0].num_bikes_available == 8  # on garde la première occurrence
