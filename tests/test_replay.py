"""Le rejeu d'archive produit des messages valides pour le consommateur, marqués comme rejoués."""

import json
from pathlib import Path

from velomagg.gbfs import parse_information, parse_status
from velomagg.replay import iter_archive

FIXTURES = Path(__file__).parent / "fixtures"


def test_rejeu_archive(tmp_path):
    status = json.loads((FIXTURES / "station_status.json").read_text(encoding="utf-8"))
    info = json.loads((FIXTURES / "station_information.json").read_text(encoding="utf-8"))
    (tmp_path / "station_information_2026-09-22.jsonl").write_text(
        json.dumps({"fetched_at": 1.0, "information": info}) + "\n", encoding="utf-8")
    (tmp_path / "station_status_2026-09-22.jsonl").write_text(
        json.dumps({"fetched_at": 1790069030.0, "status": status}) + "\n", encoding="utf-8")

    items = list(iter_archive(tmp_path))
    kinds = [k for k, _, _ in items]
    assert kinds == ["information"] * 3 + ["status"] * 3  # descriptions avant statuts
    for kind, msg, key in items:
        rec = parse_information(msg) if kind == "information" else parse_status(msg)
        assert rec.station_id == key
    assert all(parse_status(m).is_replay for k, m, _ in items if k == "status")
