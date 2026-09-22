-- Part des relevés où chaque station était vide ou pleine, sur toute la période collectée.
select
    d.station_id,
    d.nom,
    d.capacite,
    count(*) as nb_releves,
    min(f.last_reported) as premier_releve,
    max(f.last_reported) as dernier_releve,
    avg(case when f.est_vide then 1 else 0 end)::numeric(6, 4) as part_temps_vide,
    avg(case when f.est_pleine then 1 else 0 end)::numeric(6, 4) as part_temps_pleine,
    avg(f.taux_remplissage)::numeric(6, 4) as taux_remplissage_moyen
from {{ ref('fct_station_status') }} as f
inner join {{ ref('dim_station') }} as d using (station_id)
group by d.station_id, d.nom, d.capacite
