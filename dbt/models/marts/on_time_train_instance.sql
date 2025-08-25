{{ config( schema='viarail_dataset', materialized='incremental', unique_key=['train_instance_id'] ) }}

WITH ranked_trains AS (
  SELECT
    f.train_instance_id,
    f.train_id,
    f.arrival_eta,
    f.arrival_scheduled,
    f.departure_estimated,
    f.departure_scheduled,
    f.stop_number,
    d.from_loc AS departing_location,
    d.to_loc AS final_arrival_destination,
    ROW_NUMBER() OVER (
      PARTITION BY f.train_instance_id
      ORDER BY f.stop_number DESC, f.arrival_eta DESC
    ) AS rn
  FROM {{ref('time_series_train')}} f
  LEFT JOIN `viarail_dataset.core_increment_train_dim` d ON d.train_id = f.train_id
)
SELECT
  train_instance_id,
  train_id,
  arrival_eta,
  arrival_scheduled,
  departing_location,
  final_arrival_destination,
  departure_estimated,
  departure_scheduled
FROM ranked_trains
WHERE rn = 1
ORDER BY train_instance_id
