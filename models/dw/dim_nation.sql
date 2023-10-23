{{
    config(
        materialized = 'incremental',
        unique_key = 'n_nationkey'
    )
}}

select * from {{source('tpch_sf100','nation')}}
{% if is_incremental() %}
    where n_nationkey not in (select n_nationkey from {{this}})
{% endif %}