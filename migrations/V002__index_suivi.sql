-- Index pour les requêtes du tableau de bord (fenêtres temporelles) et le modèle incrémental dbt.
CREATE INDEX station_status_ingested_at_idx ON raw.station_status (ingested_at);
CREATE INDEX ingest_batches_finished_at_idx ON raw.ingest_batches (finished_at);
