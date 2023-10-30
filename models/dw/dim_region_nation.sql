{{
    config(
        materialized='view'
    )
}}

SELECT reg.region_id, reg.region_src_num, reg.region_name, nat.nation_id, nat.nation_src_num, nat.nation_name
FROM {{ref('dim_region')}} reg
    JOIN {{ref('dim_nation')}} nat using (region_id)