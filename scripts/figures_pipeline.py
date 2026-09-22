"""Reproduit les panneaux du tableau de bord Grafana en images, depuis les CSV de results/.

Le tableau de bord provisionné (grafana/dashboards/velomagg.json) n'a pas pu être capturé : la pile
tourne dans l'intégration continue, sans navigateur. Ces quatre panneaux utilisent exactement les mêmes
requêtes (sql/resultats.sql), exportées par scripts/export_results.py.

Usage : python scripts/figures_pipeline.py [dossier_de_sortie]
"""

from __future__ import annotations

import csv
import json
import sys
from datetime import datetime
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt

ROOT = Path(__file__).parents[1]
FOND, TEXTE, ACCENT, SECOND = "#0D0F12", "#ECE8DF", "#F2B84B", "#3FD1BE"


def lire(nom: str) -> list[dict]:
    with open(ROOT / "results" / nom, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def style(ax, titre: str) -> None:
    ax.set_facecolor(FOND)
    for côté in ("top", "right"):
        ax.spines[côté].set_visible(False)
    for côté in ("left", "bottom"):
        ax.spines[côté].set_color("#3A3F46")
    ax.tick_params(colors=TEXTE, labelsize=10)
    ax.grid(axis="y", color="#262B31", linewidth=0.8)
    ax.set_axisbelow(True)
    ax.yaxis.label.set_color(TEXTE)
    ax.set_title(titre, color=TEXTE, fontsize=14, loc="left", pad=10)


def legende(ax) -> None:
    leg = ax.legend(facecolor=FOND, edgecolor="#3A3F46", fontsize=10)
    for texte in leg.get_texts():
        texte.set_color(TEXTE)


def main() -> None:
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "results" / "figures"
    out.mkdir(parents=True, exist_ok=True)
    ingestion = lire("serie_ingestion.csv")
    vides = lire("serie_vides_pleines.csv")
    horaire = lire("occupation_par_heure.csv")
    synthese = json.loads((ROOT / "results" / "synthese.json").read_text(encoding="utf-8"))

    fig = plt.figure(figsize=(16, 9), dpi=100, facecolor=FOND)
    fig.suptitle(
        "Vélomagg : suivi du pipeline, exécution réelle en intégration continue",
        color=TEXTE, fontsize=19, x=0.02, ha="left", y=0.975,
    )
    fig.text(0.02, 0.925,
             f"{synthese['releves_inseres']} relevés insérés, {synthese['doublons_ecartes']} doublons écartés, "
             f"{synthese['stations_observees']} stations, {synthese['lots_ecrits']} lots",
             color="#9AA1A9", fontsize=12, ha="left")

    ax1 = fig.add_subplot(221)
    style(ax1, "Relevés insérés par tranche de 5 minutes")
    x1 = range(len(ingestion))
    ax1.bar(x1, [int(r["releves_inseres"]) for r in ingestion], color=ACCENT, label="rejeu et direct")
    ax1.bar(x1, [int(r["releves_temps_reel"]) for r in ingestion], color=SECOND, label="direct seul")
    ax1.set_xticks(list(x1)[:: max(1, len(ingestion) // 6)])
    ax1.set_xticklabels([ingestion[i]["tranche_5min"][11:] for i in list(x1)[:: max(1, len(ingestion) // 6)]])
    legende(ax1)

    ax2 = fig.add_subplot(222)
    style(ax2, "Latence de bout en bout, ingestion en direct (s)")
    points = [r for r in ingestion if r["latence_bout_en_bout_p50_s"]]
    x2 = range(len(points))
    ax2.plot(x2, [float(r["latence_bout_en_bout_p50_s"]) for r in points], color=ACCENT, marker="o", label="p50")
    ax2.plot(x2, [float(r["latence_bout_en_bout_p95_s"]) for r in points], color=SECOND, marker="o", label="p95")
    ax2.set_xticks(list(x2))
    ax2.set_xticklabels([r["tranche_5min"][11:] for r in points])
    ax2.set_ylim(bottom=0)
    legende(ax2)

    ax3 = fig.add_subplot(223)
    style(ax3, "Part des stations vides et pleines, par instantané")
    t3 = [datetime.strptime(r["instantane_utc"], "%Y-%m-%d %H:%M:%S") for r in vides]
    ax3.plot(t3, [float(r["part_vides"]) * 100 for r in vides], color=ACCENT, linewidth=1.6, label="vides")
    ax3.plot(t3, [float(r["part_pleines"]) * 100 for r in vides], color=SECOND, linewidth=2.4,
             linestyle="--", label="pleines (constamment 0)")
    ax3.set_ylabel("%", fontsize=11)
    ax3.set_ylim(bottom=-0.6)
    ax3.xaxis.set_major_locator(mdates.AutoDateLocator(maxticks=7))
    ax3.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m %Hh"))
    legende(ax3)

    ax4 = fig.add_subplot(224)
    style(ax4, "Taux de remplissage moyen par heure locale (modèle dbt)")
    x4 = range(len(horaire))
    ax4.bar(x4, [float(r["taux_remplissage_moyen"]) * 100 for r in horaire], color=ACCENT)
    ax4.set_xticks(list(x4))
    ax4.set_xticklabels([r["heure_locale"][11:] for r in horaire], rotation=45, ha="right")
    ax4.set_ylabel("%", fontsize=11)

    fig.tight_layout(rect=(0, 0, 1, 0.9))
    fig.savefig(out / "tableau-de-bord.png", facecolor=FOND)
    plt.close(fig)
    print(f"tableau de bord reproduit dans {out / 'tableau-de-bord.png'}")


if __name__ == "__main__":
    main()
