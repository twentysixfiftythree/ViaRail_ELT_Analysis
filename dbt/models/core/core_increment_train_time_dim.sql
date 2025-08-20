{{ config(
    schema='viarail_dataset',
    materialized='incremental',
    unique_key='train_instance_date')
    }}

SELECT * FROM {{source('core', 'time_dim')}}