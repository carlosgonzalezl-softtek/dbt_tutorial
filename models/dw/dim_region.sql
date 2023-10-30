{{
    config(
        materialized='table'
    )
}}

WITH FINAL AS (
    select 
        md5(REG.R_REGIONKEY)        AS REGION_ID,
        REG.R_REGIONKEY             AS REGION_SRC_NUM,
        REG.R_NAME                  AS REGION_NAME,
        current_timestamp()         AS AUDT_CREATE_DATE
    from {{ source('tpch_sf100', 'region') }} REG
)

SELECT * FROM FINAL