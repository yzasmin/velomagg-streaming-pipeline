-- Requêtes qui produisent les chiffres publiés (exécutées par scripts/export_results.py).
-- Chaque bloc commence par « -- name: <fichier> » ; la sortie va dans results/<fichier>.

-- name: synthese.json
select
    (select count(*) from raw.station_information) as stations_decrites,
    (select count(distinct station_id) from raw.station_status where not is_replay) as stations_observees,
    (select min(ingested_at) from raw.station_status where not is_replay) as debut_collecte,
    (select max(ingested_at) from raw.station_status where not is_replay) as fin_collecte,
    (select round(extract(epoch from max(ingested_at) - min(ingested_at)) / 3600.0, 2)
       from raw.station_status where not is_replay) as duree_collecte_h,
    (select coalesce(sum(received), 0) from raw.ingest_batches) as messages_recus,
    (select coalesce(sum(status_valid), 0) from raw.ingest_batches) as messages_statut_valides,
    (select coalesce(sum(status_inserted), 0) from raw.ingest_batches) as releves_inseres,
    (select coalesce(sum(status_duplicates), 0) from raw.ingest_batches) as doublons_ecartes,
    (select coalesce(sum(invalid), 0) from raw.ingest_batches) as messages_invalides,
    (select count(*) from raw.ingest_batches) as lots_ecrits,
    (select count(*) from raw.station_status where not is_replay) as lignes_station_status,
    (select count(distinct last_reported) from raw.station_status where not is_replay) as instantanes_distincts;

-- name: latence.json
select
    count(*) as n,
    round(percentile_cont(0.50) within group (order by extract(epoch from ingested_at - last_reported))::numeric, 2) as bout_en_bout_p50_s,
    round(percentile_cont(0.95) within group (order by extract(epoch from ingested_at - last_reported))::numeric, 2) as bout_en_bout_p95_s,
    round(max(extract(epoch from ingested_at - last_reported))::numeric, 2) as bout_en_bout_max_s,
    round(percentile_cont(0.50) within group (order by extract(epoch from ingested_at - fetched_at))::numeric, 3) as pipeline_p50_s,
    round(percentile_cont(0.95) within group (order by extract(epoch from ingested_at - fetched_at))::numeric, 3) as pipeline_p95_s,
    round(percentile_cont(0.50) within group (order by extract(epoch from fetched_at - last_reported))::numeric, 2) as age_flux_a_la_lecture_p50_s
from raw.station_status
where not is_replay;

-- name: instantanes_manques.json
-- Écarts entre deux valeurs successives de last_updated du flux : > 90 s signifie au moins une mise à jour non captée.
with s as (
    select distinct feed_last_updated from raw.station_status where not is_replay
), g as (
    select extract(epoch from feed_last_updated - lag(feed_last_updated) over (order by feed_last_updated)) as ecart_s
    from s
)
select
    count(*) as intervalles,
    count(*) filter (where ecart_s > 90) as intervalles_superieurs_90s,
    round(percentile_cont(0.5) within group (order by ecart_s)::numeric, 1) as ecart_median_s,
    round(max(ecart_s)::numeric, 1) as ecart_max_s
from g
where ecart_s is not null;

-- name: serie_ingestion.csv
-- Panneaux « relevés insérés » et « latence » du tableau de bord, par tranche de 5 minutes.
select
    to_char(date_trunc('minute', ingested_at)
            - make_interval(mins => extract(minute from ingested_at)::int % 5),
            'YYYY-MM-DD HH24:MI') as tranche_5min,
    count(*) as releves_inseres,
    count(*) filter (where not is_replay) as releves_temps_reel,
    round(percentile_cont(0.50) within group (
        order by case when not is_replay then extract(epoch from ingested_at - last_reported) end)::numeric, 2)
        as latence_bout_en_bout_p50_s,
    round(percentile_cont(0.95) within group (
        order by case when not is_replay then extract(epoch from ingested_at - last_reported) end)::numeric, 2)
        as latence_bout_en_bout_p95_s,
    round(percentile_cont(0.95) within group (
        order by case when not is_replay then extract(epoch from ingested_at - fetched_at) end)::numeric, 3)
        as latence_pipeline_p95_s
from raw.station_status
group by 1
order by 1;

-- name: serie_vides_pleines.csv
-- Panneau « part des stations vides et pleines », un point par instantané du flux.
select
    to_char(last_reported, 'YYYY-MM-DD HH24:MI:SS') as instantane_utc,
    count(*) as stations,
    round(avg(case when is_installed and num_bikes_available = 0 then 1.0 else 0 end), 4) as part_vides,
    round(avg(case when is_installed and num_docks_available = 0 then 1.0 else 0 end), 4) as part_pleines
from raw.station_status
group by 1
order by 1;

-- name: occupation_par_heure.csv
select
    to_char(heure_locale, 'YYYY-MM-DD HH24:00') as heure_locale,
    nb_stations,
    nb_releves,
    taux_remplissage_moyen,
    velos_moyens_par_station,
    part_releves_vide,
    part_releves_pleine
from marts.agg_reseau_horaire
order by heure_locale;

-- name: occupation_par_heure_du_jour.csv
select
    extract(hour from heure_locale)::int as heure_du_jour,
    count(*) as heures_observees,
    round(avg(taux_remplissage_moyen), 4) as taux_remplissage_moyen,
    round(avg(part_releves_vide), 4) as part_releves_vide,
    round(avg(part_releves_pleine), 4) as part_releves_pleine
from marts.agg_reseau_horaire
group by 1
order by 1;

-- name: stations_vides_pleines.csv
select station_id, nom, capacite, nb_releves, part_temps_vide, part_temps_pleine, taux_remplissage_moyen
from marts.stations_vides_pleines
order by part_temps_vide desc, part_temps_pleine desc;

-- name: vides_pleines_global.json
select
    round(avg(case when est_vide then 1.0 else 0 end), 4) as part_releves_vide,
    round(avg(case when est_pleine then 1.0 else 0 end), 4) as part_releves_pleine,
    round(avg(taux_remplissage), 4) as taux_remplissage_moyen,
    count(*) filter (where num_bikes_available > capacity) as releves_velos_superieurs_capacite
from marts.fct_station_status;
