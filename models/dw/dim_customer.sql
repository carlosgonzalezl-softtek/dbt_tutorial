{{
    config(
        materialized='table'
    )
}}

SELECT * FROM {{source("tpch_sf100","customer")}}