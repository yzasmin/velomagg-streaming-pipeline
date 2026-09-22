-- Aucun relevé ne doit annoncer plus de vélos disponibles que la capacité de la station.
select status_id, station_id, num_bikes_available, capacity
from {{ ref('fct_station_status') }}
where num_bikes_available > capacity
