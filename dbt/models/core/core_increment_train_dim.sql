{{ config(
    schema='viarail_dataset',
    materialized='incremental',
    unique_key='train_id')
    }}

SELECT * FROM {{source('core', 'train_dim')}}