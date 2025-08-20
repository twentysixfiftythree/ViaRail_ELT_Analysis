{{ config(
    schema='viarail_dataset',
    materialized='incremental',
    unique_key='code')
    }}

SELECT * FROM {{source('core', 'station_dim')}}