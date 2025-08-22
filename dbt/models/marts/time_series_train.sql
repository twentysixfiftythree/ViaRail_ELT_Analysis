{{ config(
    schema='viarail_dataset',
    materialized='incremental',
   unique_key=['train_instance_id', 'station_code', 'stop_number']
   )
    }}

SELECT DISTINCT f.train_id, f.train_instance_id, train_instance_date, arrival_scheduled, arrival_eta AS arrival_time, f.train_stop_id,  d.station_code, d.stop_number FROM {{ref("core_increment_time_fact")}} f
LEFT JOIN {{ref("core_increment_route_stop_dim")}} d ON d.train_stop_id = f.train_stop_id
WHERE eta = "ARR"
ORDER BY train_instance_date DESC, train_instance_id, d.stop_number ASC
