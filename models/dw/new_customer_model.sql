{{
    config(materialized='table')
}}

select * from {{source('source_data','customer')}}