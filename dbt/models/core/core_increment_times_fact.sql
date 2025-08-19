{{ config(
    schema='viarail_dataset',
    materialized='incremental',
    unique_key='train_stop_record_id'
) }}


WITH incremental_data AS (
    SELECT * FROM {{ source('staging', 'train_time_fact') }}
    {% if is_incremental() %}
    WHERE collected_at > (SELECT MAX(collected_at) FROM {{ this }})
    {% endif %}
)

SELECT 
    train_stop_record_id,
    collected_at,
    COUNT(*) AS duplicate_count
FROM incremental_data
GROUP BY train_stop_record_id, collected_at
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, train_stop_record_id, collected_at
