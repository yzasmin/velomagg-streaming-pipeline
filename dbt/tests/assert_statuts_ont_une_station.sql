-- Chaque relevé doit correspondre à une station décrite dans station_information.
select s.station_id, count(*) as n
from {{ ref('stg_station_status') }} s
left join {{ ref('dim_station') }} d using (station_id)
where d.station_id is null
group by s.station_id
