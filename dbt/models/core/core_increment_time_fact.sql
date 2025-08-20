{{ config(
    schema='viarail_dataset',
    materialized='incremental',
    unique_key='train_stop_record_id',
        partition_by={
      "field": "train_instance_date",
      "data_type": "date"
    },
    cluster_by=[
        "train_instance_id"
    ],
    partition_expiration_days=None
) }}


-- collected at is from the current loaded data into core
--changed to core
SELECT * FROM {{source('core', 'train_time_fact')}}
{% if is_incremental() %}
WHERE collected_at > (SELECT MAX(collected_at) FROM {{this}})
{% endif %}
