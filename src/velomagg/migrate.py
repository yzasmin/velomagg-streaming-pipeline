"""Applique les migrations SQL versionnées de `migrations/` (V001__nom.sql, V002__nom.sql, ...).

Chaque fichier est appliqué une seule fois, dans sa propre transaction, et enregistré
avec sa somme de contrôle dans `public.schema_migrations`. Un fichier déjà appliqué
puis modifié fait échouer la commande : on ajoute une nouvelle migration, on ne réécrit pas l'histoire.
"""

from __future__ import annotations

import hashlib
import logging
import re
import sys
import time
from pathlib import Path

import psycopg

from velomagg.config import Settings

log = logging.getLogger("migrate")
PATTERN = re.compile(r"^V(\d{3})__([a-z0-9_]+)\.sql$")


def discover(folder: Path) -> list[tuple[int, str, Path]]:
    """Liste les migrations triées par version ; refuse les noms invalides et les versions en double."""
    found: dict[int, tuple[int, str, Path]] = {}
    for path in folder.glob("*.sql"):
        m = PATTERN.match(path.name)
        if not m:
            raise ValueError(f"nom de migration invalide : {path.name}")
        version = int(m.group(1))
        if version in found:
            raise ValueError(f"version en double : {version}")
        found[version] = (version, m.group(2), path)
    return [found[v] for v in sorted(found)]


def checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def apply(conn: psycopg.Connection, folder: Path) -> int:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS public.schema_migrations (
            version integer PRIMARY KEY,
            name text NOT NULL,
            checksum text NOT NULL,
            applied_at timestamptz NOT NULL DEFAULT now()
        )""")
    applied = {v: c for v, c in conn.execute("SELECT version, checksum FROM public.schema_migrations")}
    count = 0
    for version, name, path in discover(folder):
        digest = checksum(path)
        if version in applied:
            if applied[version] != digest:
                raise RuntimeError(f"V{version:03d} a été modifiée après application")
            continue
        with conn.transaction():
            conn.execute(path.read_text(encoding="utf-8"))
            conn.execute("INSERT INTO public.schema_migrations (version, name, checksum) VALUES (%s, %s, %s)",
                         (version, name, digest))
        log.info("migration appliquée : V%03d %s", version, name)
        count += 1
    return count


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    folder = Path(sys.argv[1] if len(sys.argv) > 1 else "migrations")
    for attempt in range(30):
        try:
            with psycopg.connect(Settings().postgres_dsn, autocommit=True) as conn:
                n = apply(conn, folder)
            log.info("%d migration(s) appliquée(s)", n)
            return
        except psycopg.OperationalError as exc:
            log.warning("PostgreSQL indisponible (essai %d) : %s", attempt + 1, exc)
            time.sleep(2)
    sys.exit(1)


if __name__ == "__main__":
    main()
