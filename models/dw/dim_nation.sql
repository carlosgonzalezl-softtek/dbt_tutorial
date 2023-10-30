{{
    config(
        materialized='table'
    )
}}

WITH FINAL AS (
    select 
        md5(NAT.N_NATIONKEY)        AS NATION_ID,
        NAT.N_NATIONKEY             AS NATION_SRC_NUM,
        NAT.N_NAME                  AS NATION_NAME,
        md5(NAT.N_REGIONKEY)        AS REGION_ID,
        NAT.N_REGIONKEY             AS REGION_SRC_NUM,
        NAT.N_COMMENT               AS NATION_COMMENT,
        current_timestamp()         AS AUDT_CREATE_DATE
    from {{ source('tpch_sf100', 'nation') }} NAT
)

SELECT * FROM FINAL