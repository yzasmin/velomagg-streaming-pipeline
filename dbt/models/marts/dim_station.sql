select
    station_id,
    nom,
    lat,
    lon,
    capacite,
    station_virtuelle,
    updated_at as description_maj_le
from {{ ref('stg_station_information') }}
