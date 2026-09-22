-- Vue réseau : une ligne par heure locale, toutes stations confondues.
select
    heure_locale,
    count(distinct station_id) as nb_stations,
    count(*) as nb_releves,
    avg(taux_remplissage)::numeric(6, 4) as taux_remplissage_moyen,
    avg(num_bikes_available)::numeric(8, 2) as velos_moyens_par_station,
    avg(case when est_vide then 1 else 0 end)::numeric(6, 4) as part_releves_vide,
    avg(case when est_pleine then 1 else 0 end)::numeric(6, 4) as part_releves_pleine
from {{ ref('fct_station_status') }}
group by heure_locale
