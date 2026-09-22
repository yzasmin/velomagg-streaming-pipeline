select
    station_id,
    name as nom,
    lat,
    lon,
    capacity as capacite,
    is_virtual_station as station_virtuelle,
    feed_last_updated,
    updated_at
from {{ source('raw', 'station_information') }}
