#!/bin/sh
# Exécute `dbt build` (modèles + tests + instantané de fraîcheur) à intervalle régulier.
set -u
while true; do
  echo "=== dbt build $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  dbt source freshness || true
  dbt build || echo "dbt build a signalé des erreurs (voir ci-dessus)"
  sleep "${DBT_INTERVAL_S:-900}"
done
