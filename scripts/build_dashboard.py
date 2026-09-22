"""Génère grafana/dashboards/velomagg.json (le JSON est commité ; ce script sert à le modifier proprement).

Usage : python scripts/build_dashboard.py
"""

from __future__ import annotations

import json
from pathlib import Path

DS = {"type": "grafana-postgresql-datasource", "uid": "velomagg-pg"}


def target(sql: str, fmt: str = "time_series") -> dict:
    return {"datasource": DS, "editorMode": "code", "format": fmt, "rawQuery": True, "rawSql": sql.strip(), "refId": "A"}


def stat(pid: int, title: str, sql: str, x: int, unit: str = "short", decimals: int = 0) -> dict:
    return {
        "id": pid, "type": "stat", "title": title, "datasource": DS,
        "gridPos": {"h": 4, "w": 6, "x": x, "y": 0},
        "targets": [target(sql, "table")],
        "fieldConfig": {"defaults": {"unit": unit, "decimals": decimals, "color": {"mode": "fixed", "fixedColor": "#F2B84B"}},
                        "overrides": []},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "colorMode": "value", "graphMode": "none", "textMode": "value"},
    }


def series(pid: int, title: str, sql: str, grid: dict, unit: str, draw: str = "line") -> dict:
    return {
        "id": pid, "type": "timeseries", "title": title, "datasource": DS, "gridPos": grid,
        "targets": [target(sql)],
        "fieldConfig": {"defaults": {"unit": unit, "custom": {"drawStyle": draw, "lineWidth": 2, "fillOpacity": 10,
                                                              "spanNulls": False}}, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                    "tooltip": {"mode": "multi"}},
    }


panels = [
    stat(1, "Messages de statut reçus", """
        select coalesce(sum(status_valid), 0) as "messages"
        from raw.ingest_batches where $__timeFilter(finished_at)""", 0),
    stat(2, "Doublons écartés", """
        select coalesce(sum(status_duplicates), 0) as "doublons"
        from raw.ingest_batches where $__timeFilter(finished_at)""", 6),
    stat(3, "Messages rejetés (schéma)", """
        select count(*) as "rejetés" from raw.rejected_messages where $__timeFilter(rejected_at)""", 12),
    stat(4, "Stations suivies (10 dernières min)", """
        select count(distinct station_id) as "stations" from raw.station_status
        where ingested_at > now() - interval '10 minutes'""", 18),
    series(5, "Relevés insérés par tranche de 5 min", """
        select $__timeGroupAlias(ingested_at, '5m'), count(*) as "relevés insérés"
        from raw.station_status where $__timeFilter(ingested_at) and not is_replay
        group by 1 order by 1""", {"h": 8, "w": 12, "x": 0, "y": 4}, "short", "bars"),
    series(6, "Latence de bout en bout (last_reported vers insertion)", """
        select $__timeGroupAlias(ingested_at, '5m'),
          percentile_cont(0.5) within group (order by extract(epoch from ingested_at - last_reported)) as "p50",
          percentile_cont(0.95) within group (order by extract(epoch from ingested_at - last_reported)) as "p95",
          percentile_cont(0.95) within group (order by extract(epoch from ingested_at - fetched_at)) as "p95 pipeline seul"
        from raw.station_status where $__timeFilter(ingested_at) and not is_replay
        group by 1 order by 1""", {"h": 8, "w": 12, "x": 12, "y": 4}, "s"),
    series(7, "Part des stations vides et pleines", """
        select last_reported as "time",
          avg(case when is_installed and num_bikes_available = 0 then 1.0 else 0 end) as "vides",
          avg(case when is_installed and num_docks_available = 0 then 1.0 else 0 end) as "pleines"
        from raw.station_status where $__timeFilter(last_reported)
        group by 1 order by 1""", {"h": 8, "w": 12, "x": 0, "y": 12}, "percentunit"),
    {
        "id": 8, "type": "barchart", "title": "Taux de remplissage moyen par heure (modèle dbt)", "datasource": DS,
        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 12},
        "targets": [target("""
            select to_char(heure_locale, 'DD/MM HH24"h"') as "heure", taux_remplissage_moyen as "remplissage"
            from marts.agg_reseau_horaire order by heure_locale""", "table")],
        "fieldConfig": {"defaults": {"unit": "percentunit", "min": 0, "max": 1,
                                     "color": {"mode": "fixed", "fixedColor": "#F2B84B"}}, "overrides": []},
        "options": {"xField": "heure", "orientation": "vertical", "showValue": "never",
                    "legend": {"showLegend": False, "displayMode": "list", "placement": "bottom"},
                    "xTickLabelRotation": -45},
    },
]

dashboard = {
    "uid": "velomagg-pipeline", "title": "Vélomagg : suivi du pipeline", "timezone": "Europe/Paris",
    "schemaVersion": 39, "version": 1, "editable": True, "refresh": "1m",
    "time": {"from": "now-6h", "to": "now"}, "tags": ["velomagg", "gbfs"], "panels": panels,
}

out = Path(__file__).parents[1] / "grafana" / "dashboards" / "velomagg.json"
out.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print(f"écrit : {out}")
