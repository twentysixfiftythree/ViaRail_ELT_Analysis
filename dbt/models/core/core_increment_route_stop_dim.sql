{{ config(
    schema='viarail_dataset',
    materialized='incremental',
    unique_key='train_stop_id')
    }}

SELECT * FROM {{source('core', 'route_stop_dim')}}