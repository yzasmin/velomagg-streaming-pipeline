"""Graphiques du projet, à partir des CSV de results/ (aucun chiffre saisi à la main).

Usage : python scripts/figures.py [dossier_de_sortie]
Thème sombre du portfolio : fond #0D0F12, texte #ECE8DF, accent engineering #F2B84B.
"""

from __future__ import annotations

import csv
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


def style(ax) -> None:
    ax.set_facecolor(FOND)
    for côté in ("top", "right"):
        ax.spines[côté].set_visible(False)
    for côté in ("left", "bottom"):
        ax.spines[côté].set_color("#3A3F46")
    ax.tick_params(colors=TEXTE, labelsize=13)
    ax.grid(axis="y", color="#262B31", linewidth=0.8)
    ax.set_axisbelow(True)
    ax.yaxis.label.set_color(TEXTE)
    ax.xaxis.label.set_color(TEXTE)


def figure(largeur: int = 1600, hauteur: int = 900):
    fig = plt.figure(figsize=(largeur / 100, hauteur / 100), dpi=100, facecolor=FOND)
    return fig


def serie_reseau(out: Path) -> None:
    lignes = lire("archive_serie_reseau.csv")
    t = [datetime.strptime(ligne["horodatage_local"], "%Y-%m-%d %H:%M:%S") for ligne in lignes]
    velos = [int(ligne["velos_disponibles"]) for ligne in lignes]
    vides = [int(ligne["stations_vides"]) for ligne in lignes]

    fig = figure()
    ax = fig.add_subplot(111)
    style(ax)
    ax.plot(t, velos, color=ACCENT, linewidth=2.6, label="Vélos disponibles sur le réseau")
    ax.set_ylabel("Vélos disponibles", fontsize=14)
    ax2 = ax.twinx()
    ax2.plot(t, vides, color=SECOND, linewidth=2.2, linestyle="--", label="Stations vides")
    ax2.set_ylabel("Stations vides (sur 52)", fontsize=14, color=TEXTE)
    ax2.tick_params(colors=TEXTE, labelsize=13)
    ax2.set_facecolor(FOND)
    for côté in ("top", "right", "left", "bottom"):
        ax2.spines[côté].set_visible(côté == "right")
    ax2.spines["right"].set_color("#3A3F46")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Hh%M"))
    ax.set_title("Vélomagg : disponibilité relevée toutes les 60 secondes",
                 color=TEXTE, fontsize=20, pad=18, loc="left")
    lignes_legende = ax.get_lines() + ax2.get_lines()
    leg = ax.legend(lignes_legende, [courbe.get_label() for courbe in lignes_legende], loc="upper right",
                    facecolor=FOND, edgecolor="#3A3F46", fontsize=13)
    for texte in leg.get_texts():
        texte.set_color(TEXTE)
    fig.tight_layout()
    fig.savefig(out / "serie-reseau.png", facecolor=FOND)
    plt.close(fig)


def stations_vides(out: Path) -> None:
    lignes = sorted(lire("archive_stations.csv"), key=lambda x: -float(x["part_temps_vide"]))[:12]
    noms = [ligne["nom"][:34] or ligne["station_id"] for ligne in lignes][::-1]
    parts = [float(ligne["part_temps_vide"]) * 100 for ligne in lignes][::-1]

    fig = figure()
    ax = fig.add_subplot(111)
    style(ax)
    ax.grid(axis="y", visible=False)
    ax.grid(axis="x", color="#262B31", linewidth=0.8)
    ax.barh(noms, parts, color=ACCENT, height=0.68)
    ax.set_xlabel("Part des relevés sans aucun vélo (%)", fontsize=14)
    ax.set_xlim(0, 105)
    ax.set_title("Douze stations les plus souvent vides", color=TEXTE, fontsize=20, pad=18, loc="left")
    for y, valeur in enumerate(parts):
        ax.text(valeur + 1.5, y, f"{valeur:.0f} %", va="center", color=TEXTE, fontsize=12)
    fig.tight_layout()
    fig.savefig(out / "stations-vides.png", facecolor=FOND)
    plt.close(fig)


def occupation_horaire(out: Path) -> None:
    lignes = lire("archive_occupation_par_heure.csv")
    heures = [ligne["heure_locale"][11:16] for ligne in lignes]
    taux = [float(ligne["taux_remplissage_moyen"]) * 100 for ligne in lignes]
    vide = [float(ligne["part_releves_vide"]) * 100 for ligne in lignes]

    fig = figure()
    ax = fig.add_subplot(111)
    style(ax)
    x = range(len(heures))
    ax.bar([i - 0.19 for i in x], taux, width=0.38, color=ACCENT, label="Taux de remplissage moyen")
    ax.bar([i + 0.19 for i in x], vide, width=0.38, color=SECOND, label="Part des relevés sans vélo")
    ax.set_xticks(list(x))
    ax.set_xticklabels(heures)
    ax.set_ylabel("Pourcentage", fontsize=14)
    ax.set_title("Remplissage du réseau par heure locale", color=TEXTE, fontsize=20, pad=18, loc="left")
    leg = ax.legend(facecolor=FOND, edgecolor="#3A3F46", fontsize=13)
    for texte in leg.get_texts():
        texte.set_color(TEXTE)
    fig.tight_layout()
    fig.savefig(out / "occupation-horaire.png", facecolor=FOND)
    plt.close(fig)


def main() -> None:
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "results" / "figures"
    out.mkdir(parents=True, exist_ok=True)
    serie_reseau(out)
    stations_vides(out)
    occupation_horaire(out)
    print(f"figures écrites dans {out}")


if __name__ == "__main__":
    main()
