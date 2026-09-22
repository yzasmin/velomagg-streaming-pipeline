select
    status_id,
    station_id,
    last_reported,
    num_bikes_available,
    num_docks_available,
    is_installed,
    is_renting,
    is_returning,
    feed_last_updated,
    fetched_at,
    ingested_at,
    is_replay,
    -- latence de bout en bout : publication de la mesure par l'opérateur, puis ligne en base.
    -- Nulle pour les relevés rejoués depuis une archive (pas de sens en temps réel).
    case when not is_replay
         then extract(epoch from ingested_at - last_reported)::numeric(10, 3)
    end as latence_bout_en_bout_s,
    -- part propre au pipeline : lecture du flux, puis ligne en base (Redpanda + consommateur)
    case when not is_replay
         then extract(epoch from ingested_at - fetched_at)::numeric(10, 3)
    end as latence_pipeline_s
from {{ source('raw', 'station_status') }}
