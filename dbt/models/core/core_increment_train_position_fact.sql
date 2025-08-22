{{ config(
    schema='viarail_dataset',
    materialized='incremental',
    unique_key='train_stop_record_id')
    }}
-- add unique key
SELECT * FROM {{source('core', 'train_position_fact')}}