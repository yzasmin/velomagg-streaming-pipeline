"""Exécute sql/resultats.sql et écrit chaque bloc dans results/ (JSON ou CSV).

Usage (pipeline lancé, port PostgreSQL publié sur 127.0.0.1:55432) :
    uv run python scripts/export_results.py
"""

from __future__ import annotations

import csv
import json
import os
import re
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import psycopg

ROOT = Path(__file__).parents[1]


def blocks(sql: str):
    for m in re.finditer(r"-- name: (\S+)\n(.*?)(?=\n-- name: |\Z)", sql, re.S):
        yield m.group(1), m.group(2).strip().rstrip(";")


def plain(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def main() -> None:
    dsn = (f"host={os.environ.get('POSTGRES_HOST', '127.0.0.1')} port={os.environ.get('POSTGRES_PORT', '55432')} "
           f"dbname={os.environ.get('POSTGRES_DB', 'velomagg')} user={os.environ.get('POSTGRES_USER', 'velomagg')} "
           f"password={os.environ.get('POSTGRES_PASSWORD', 'change-moi')}")
    out = ROOT / "results"
    out.mkdir(exist_ok=True)
    with psycopg.connect(dsn) as conn:
        for name, query in blocks((ROOT / "sql" / "resultats.sql").read_text(encoding="utf-8")):
            cur = conn.execute(query)
            cols = [c.name for c in cur.description]
            rows = [[plain(v) for v in r] for r in cur.fetchall()]
            if name.endswith(".json"):
                data = dict(zip(cols, rows[0], strict=True)) if len(rows) == 1 else [dict(zip(cols, r, strict=True)) for r in rows]
                (out / name).write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            else:
                with open(out / name, "w", newline="", encoding="utf-8") as fh:
                    w = csv.writer(fh)
                    w.writerow(cols)
                    w.writerows(rows)
            print(f"{name} : {len(rows)} ligne(s)")


if __name__ == "__main__":
    main()
