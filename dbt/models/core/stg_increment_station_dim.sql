{{ config(
    schema='viarail_dataset',
    materialized='incremental',
    unique_key='train_stop_record_id'
) }}


-- collected at is from the current loaded data into core
SELECT * FROM {{source('staging', 'train_time_fact')}}
{% if is_incremental() %}
WHERE collected_at > (SELECT MAX(collected_at) FROM {{this}})
{% endif %}
