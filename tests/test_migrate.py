"""Tests de la découverte des migrations versionnées (sans base)."""

from pathlib import Path

import pytest

from velomagg.migrate import checksum, discover

MIGRATIONS = Path(__file__).parents[1] / "migrations"


def test_migrations_du_depot_ordonnees():
    versions = [v for v, _, _ in discover(MIGRATIONS)]
    assert versions == sorted(versions)
    assert versions[0] == 1


def test_nom_invalide_refuse(tmp_path):
    (tmp_path / "001_init.sql").write_text("select 1;")
    with pytest.raises(ValueError, match="nom de migration invalide"):
        discover(tmp_path)


def test_version_en_double_refusee(tmp_path):
    (tmp_path / "V001__a.sql").write_text("select 1;")
    (tmp_path / "V001__b.sql").write_text("select 2;")
    with pytest.raises(ValueError, match="double"):
        discover(tmp_path)


def test_checksum_insensible_aux_fins_de_ligne(tmp_path):
    unix = tmp_path / "V001__a.sql"
    win = tmp_path / "V002__b.sql"
    unix.write_bytes(b"select 1;\nselect 2;\n")
    win.write_bytes(b"select 1;\r\nselect 2;\r\n")
    assert checksum(unix) == checksum(win)
