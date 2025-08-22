{{ config(
    schema='viarail_dataset',
    materialized='incremental',
    unique_key='train_stop_record_id'
    )
    }}

SELECT DISTINCT f.train_stop_record_id, f.train_id, f.train_instance_id, f.arrival_eta, train_instance_date, arrival_scheduled, f.train_stop_id,  d.station_code, d.stop_number FROM {{ref("core_increment_time_fact")}} f
LEFT JOIN {{ref("core_increment_route_stop_dim")}} d ON d.train_stop_id = f.train_stop_id
WHERE eta = 'ARR'
