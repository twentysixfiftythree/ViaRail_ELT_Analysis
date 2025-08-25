{{ config(
    schema='viarail_dataset',
    materialized='incremental',
    unique_key='train_stop_record_id'
) }}

WITH ranked_stops AS (
    SELECT 
        f.train_stop_record_id, 
        f.train_id, 
        f.train_instance_id, 
        f.arrival_eta, 
        f.departure_estimated, 
        f.departure_scheduled, 
        f.train_instance_date, 
        f.arrival_scheduled, 
        f.train_stop_id,  
        d.station_code, 
        d.stop_number,
        ROW_NUMBER() OVER (
            PARTITION BY f.train_instance_id, f.train_stop_id 
            ORDER BY f.arrival_scheduled
        ) AS rn
    FROM {{ref("core_increment_time_fact")}} f
    LEFT JOIN {{ref("core_increment_route_stop_dim")}} d 
        ON d.train_stop_id = f.train_stop_id
    WHERE eta = 'ARR'
),

one_row_per_stop AS (
    SELECT 
        train_stop_record_id, 
        train_id, 
        train_instance_id, 
        arrival_eta, 
        departure_estimated, 
        departure_scheduled, 
        train_instance_date, 
        arrival_scheduled, 
        train_stop_id,  
        station_code, 
        stop_number
    FROM ranked_stops
    WHERE rn = 1
),

with_delays AS (
    SELECT
        *,
        TIMESTAMP_DIFF(arrival_eta, arrival_scheduled, SECOND) / 60.0 AS delay_minutes
    FROM one_row_per_stop
),

with_lag AS (
    SELECT
        *,
        LAG(delay_minutes) OVER (
            PARTITION BY train_instance_id 
            ORDER BY stop_number
        ) AS previous_delay_minutes
    FROM with_delays
),

final_with_diff AS (
    SELECT
        *,
        CASE 
            WHEN previous_delay_minutes IS NULL THEN delay_minutes
            ELSE delay_minutes - previous_delay_minutes
        END AS delay_diff_minutes
    FROM with_lag
)

SELECT
    train_stop_record_id,
    train_id,
    train_instance_id,
    arrival_eta,
    departure_estimated,
    departure_scheduled,
    train_instance_date,
    arrival_scheduled,
    train_stop_id,
    station_code,
    stop_number,
    delay_minutes,
    previous_delay_minutes,
    delay_diff_minutes
FROM final_with_diff
ORDER BY train_instance_id, stop_number
