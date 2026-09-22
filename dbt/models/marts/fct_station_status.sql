{{ config(materialized='incremental', unique_key='status_id', on_schema_change='fail') }}

select
    s.status_id,
    s.station_id,
    s.last_reported,
    (s.last_reported at time zone '{{ var("fuseau") }}') as releve_heure_locale,
    date_trunc('hour', s.last_reported at time zone '{{ var("fuseau") }}') as heure_locale,
    s.num_bikes_available,
    s.num_docks_available,
    d.capacite as capacity,
    s.is_installed,
    s.is_renting,
    s.is_returning,
    -- taux de remplissage : vélos / (vélos + bornes libres) ; les bornes hors service sont exclues
    case when s.num_bikes_available + s.num_docks_available > 0
         then s.num_bikes_available::numeric / (s.num_bikes_available + s.num_docks_available)
    end as taux_remplissage,
    (s.is_installed and s.num_bikes_available = 0) as est_vide,
    (s.is_installed and s.num_docks_available = 0) as est_pleine,
    s.latence_bout_en_bout_s,
    s.latence_pipeline_s,
    s.is_replay,
    s.ingested_at
from {{ ref('stg_station_status') }} as s
left join {{ ref('dim_station') }} as d using (station_id)
{% if is_incremental() %}
where s.status_id > (select coalesce(max(status_id), 0) from {{ this }})
{% endif %}
