-- Schéma brut alimenté par le consommateur. Les modèles dbt lisent ce schéma.
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE raw.station_information (
    station_id          text PRIMARY KEY,
    name                text NOT NULL,
    lat                 double precision NOT NULL,
    lon                 double precision NOT NULL,
    capacity            integer NOT NULL CHECK (capacity >= 0),
    is_virtual_station  boolean NOT NULL DEFAULT false,
    feed_last_updated   timestamptz NOT NULL,
    updated_at          timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE raw.station_status (
    status_id                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    station_id               text NOT NULL,
    last_reported            timestamptz NOT NULL,
    num_bikes_available      integer NOT NULL CHECK (num_bikes_available >= 0),
    num_docks_available      integer NOT NULL CHECK (num_docks_available >= 0),
    is_installed             boolean NOT NULL,
    is_renting               boolean NOT NULL,
    is_returning             boolean NOT NULL,
    vehicle_types_available  jsonb NOT NULL DEFAULT '[]'::jsonb,
    feed_last_updated        timestamptz NOT NULL,
    fetched_at               timestamptz NOT NULL,
    kafka_partition          integer NOT NULL,
    kafka_offset             bigint NOT NULL,
    -- true pour un relevé rejoué depuis une archive : exclu des mesures de latence
    is_replay                boolean NOT NULL DEFAULT false,
    -- clock_timestamp() et non now() : heure réelle de l'insertion, pas du début de transaction.
    ingested_at              timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT station_status_dedup UNIQUE (station_id, last_reported)
);

CREATE TABLE raw.ingest_batches (
    batch_id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    started_at            timestamptz NOT NULL,
    finished_at           timestamptz NOT NULL,
    received              integer NOT NULL,
    invalid               integer NOT NULL,
    status_valid          integer NOT NULL,
    status_inserted       integer NOT NULL,
    status_duplicates     integer NOT NULL,
    information_upserted  integer NOT NULL
);

CREATE TABLE raw.rejected_messages (
    rejected_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic            text NOT NULL,
    kafka_partition  integer NOT NULL,
    kafka_offset     bigint NOT NULL,
    payload          text NOT NULL,
    error            text NOT NULL,
    rejected_at      timestamptz NOT NULL DEFAULT clock_timestamp()
);
