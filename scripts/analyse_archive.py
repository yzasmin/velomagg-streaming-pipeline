"""Chiffres tirés de l'archive brute du flux (sans Docker ni PostgreSQL).

Sert de mesure de repli quand le pipeline n'a pas encore tourné : les fichiers produits sont
préfixés `archive_` pour ne jamais être confondus avec les sorties du pipeline (results/*.json
produits par scripts/export_results.py, qui eux viennent de PostgreSQL).

Usage : python scripts/analyse_archive.py data/archive
"""

from __future__ import annotations

import csv
import json
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).parents[1]
PARIS = timezone(timedelta(hours=2))  # Europe/Paris en septembre (UTC+2)


def charger(folder: Path):
    """Retourne (descriptions, instantanés) : un instantané = (last_updated, fetched_at, stations)."""
    descriptions: dict[str, dict] = {}
    for path in sorted(folder.glob("station_information_*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            rec = json.loads(line)
            for st in rec["information"]["data"]["stations"]:
                descriptions[str(st["station_id"])] = st
    instantanes = []
    for path in sorted(folder.glob("station_status_*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            rec = json.loads(line)
            d = rec["status"]
            instantanes.append((int(d["last_updated"]), float(rec["fetched_at"]), d["data"]["stations"]))
    instantanes.sort(key=lambda x: x[1])
    return descriptions, instantanes


def main(folder: Path) -> None:
    descriptions, instantanes = charger(folder)
    out = ROOT / "results"
    out.mkdir(exist_ok=True)

    vus = set()
    doublons = 0  # interrogations qui rapportent un instantané déjà vu : le pipeline les écarterait
    uniques = []
    for last_updated, fetched_at, stations in instantanes:
        if last_updated in vus:
            doublons += 1
            continue
        vus.add(last_updated)
        uniques.append((last_updated, fetched_at, stations))

    horodatages = sorted(vus)
    ecarts = [b - a for a, b in zip(horodatages, horodatages[1:], strict=False)]
    ages = [fetched_at - last_updated for last_updated, fetched_at, _ in uniques]

    par_heure = defaultdict(lambda: {"releves": 0, "remplissage": [], "vide": 0, "plein": 0, "stations": set()})
    par_station = defaultdict(lambda: {"releves": 0, "remplissage": [], "vide": 0, "plein": 0})
    releves = 0
    for last_updated, _, stations in uniques:
        heure = datetime.fromtimestamp(last_updated, PARIS).replace(minute=0, second=0, microsecond=0)
        for st in stations:
            sid = str(st["station_id"])
            velos, bornes = int(st["num_bikes_available"]), int(st["num_docks_available"])
            installee = bool(st.get("is_installed", True))
            remplissage = velos / (velos + bornes) if velos + bornes else None
            for cible in (par_heure[heure.isoformat()], par_station[sid]):
                cible["releves"] += 1
                if remplissage is not None:
                    cible["remplissage"].append(remplissage)
                cible["vide"] += 1 if installee and velos == 0 else 0
                cible["plein"] += 1 if installee and bornes == 0 else 0
            par_heure[heure.isoformat()]["stations"].add(sid)
            releves += 1

    synthese = {
        "source": "archive brute du flux GBFS (scripts/archive_gbfs.py), pipeline non exécuté",
        "stations_decrites": len(descriptions),
        "stations_observees": len(par_station),
        "interrogations": len(instantanes),
        "instantanes_distincts": len(uniques),
        "interrogations_redondantes": doublons,
        "releves_station": releves,
        "debut_utc": datetime.fromtimestamp(horodatages[0], timezone.utc).isoformat(),
        "fin_utc": datetime.fromtimestamp(horodatages[-1], timezone.utc).isoformat(),
        "duree_h": round((horodatages[-1] - horodatages[0]) / 3600, 2),
        "ecart_median_entre_instantanes_s": statistics.median(ecarts) if ecarts else None,
        "ecart_max_entre_instantanes_s": max(ecarts) if ecarts else None,
        "intervalles_superieurs_90s": sum(1 for e in ecarts if e > 90),
        "age_du_flux_a_la_lecture_median_s": round(statistics.median(ages), 2) if ages else None,
        "age_du_flux_a_la_lecture_max_s": round(max(ages), 2) if ages else None,
        "part_releves_vide": round(sum(v["vide"] for v in par_station.values()) / releves, 4),
        "part_releves_pleine": round(sum(v["plein"] for v in par_station.values()) / releves, 4),
        "taux_remplissage_moyen": round(
            statistics.fmean([x for v in par_station.values() for x in v["remplissage"]]), 4),
        "capacite_totale": sum(int(s.get("capacity", 0)) for s in descriptions.values()),
        "releves_velos_superieurs_capacite": sum(
            1 for _, _, stations in uniques for st in stations
            if int(st["num_bikes_available"]) > int(descriptions.get(str(st["station_id"]), {}).get("capacity", 10**6))),
    }
    (out / "archive_synthese.json").write_text(json.dumps(synthese, ensure_ascii=False, indent=2) + "\n",
                                               encoding="utf-8")

    with open(out / "archive_occupation_par_heure.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["heure_locale", "nb_stations", "nb_releves", "taux_remplissage_moyen",
                    "part_releves_vide", "part_releves_pleine"])
        for heure in sorted(par_heure):
            v = par_heure[heure]
            w.writerow([heure[:16].replace("T", " "), len(v["stations"]), v["releves"],
                        round(statistics.fmean(v["remplissage"]), 4) if v["remplissage"] else "",
                        round(v["vide"] / v["releves"], 4), round(v["plein"] / v["releves"], 4)])

    with open(out / "archive_serie_reseau.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["horodatage_local", "velos_disponibles", "bornes_libres", "stations_vides", "stations_pleines"])
        for last_updated, _, stations in uniques:
            velos = sum(int(s["num_bikes_available"]) for s in stations)
            bornes = sum(int(s["num_docks_available"]) for s in stations)
            vides = sum(1 for s in stations if s.get("is_installed", True) and int(s["num_bikes_available"]) == 0)
            pleines = sum(1 for s in stations if s.get("is_installed", True) and int(s["num_docks_available"]) == 0)
            w.writerow([datetime.fromtimestamp(last_updated, PARIS).strftime("%Y-%m-%d %H:%M:%S"),
                        velos, bornes, vides, pleines])

    with open(out / "archive_stations.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["station_id", "nom", "capacite", "nb_releves", "part_temps_vide", "part_temps_pleine",
                    "taux_remplissage_moyen"])
        for sid in sorted(par_station, key=lambda s: -par_station[s]["vide"] / max(par_station[s]["releves"], 1)):
            v = par_station[sid]
            w.writerow([sid, descriptions.get(sid, {}).get("name", ""), descriptions.get(sid, {}).get("capacity", ""),
                        v["releves"], round(v["vide"] / v["releves"], 4), round(v["plein"] / v["releves"], 4),
                        round(statistics.fmean(v["remplissage"]), 4) if v["remplissage"] else ""])
    print(json.dumps(synthese, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main(Path(sys.argv[1] if len(sys.argv) > 1 else "data/archive"))
