{{ config(
    schema='viarail_dataset',
    materialized='incremental',
    unique_key='?')
    }}

SELECT * FROM {{source('core', 'train_position_fact')}}